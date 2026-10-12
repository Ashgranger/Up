"""Comprehensive Test Suite for Level 4 Advanced Market-Making Research Architecture.

Validates the 6 Level 4 components:
1. Joint Decision State & Receding-Horizon Control: x_t = (b_t, q_t, c_t, ell_t) and multi-step MDP rollouts.
2. Optimal Stopping & Active Liquidity Withdrawal: 3-state controller (Normal, Reduced-Risk, Withdrawal).
3. Mean-Field Game & Strategic Competitor Dynamics: Competitor population response m_t(a_t) and penny war avoidance.
4. Causal Market-Impact Identification: tau(a, a') = E[Y | do(a)] - E[Y | do(a')] and selection bias correction.
5. Tail-Risk-Aware Stochastic Control & 6-Component Risk Budget Allocator: CVaR penalization and dynamic throttling.
6. End-to-End Level 4 Integrated Quoting Engine: Verifies robust quote generation with all Level 4 modules active.
"""
import asyncio
import os
import unittest
from decimal import Decimal as D

import sim
from latent_state import MarketRegime, BeliefState
from level4_control import (
    DecisionState,
    LatencyState,
    OptimalStoppingWithdrawalController,
    QuotingMode,
    RecedingHorizonQuoteController,
)
from mean_field_competition import MeanFieldCompetitionEngine
from causal_impact import CausalMarketImpactEstimator
from tail_risk_budget import RiskBudgetAllocator
from utils import BUY, SELL, BPS, fmt


class TestLevel4MarketMaker(unittest.IsolatedAsyncioTestCase):

    async def test_01_decision_state_and_receding_horizon(self):
        """1. Joint Decision State & Receding-Horizon Optimization:
        Constructs x_t = (b_t, q_t, c_t, ell_t) and evaluates multi-period expected utility rollouts."""
        latency = LatencyState(estimated_rtt_ms=12.5, inflight_orders_count=0)
        belief = BeliefState()

        state = DecisionState(
            belief=belief,
            epistemic_uncertainty=0.15,
            inventory_usd=D("20.0"),
            inventory_qty=D("0.00025"),
            context_markout_rmse=0.45,
            context_brier_score=0.08,
            latency=latency,
            timestamp=100.0,
        )

        controller = RecedingHorizonQuoteController(horizon_steps=3, dt_s=1.0)
        rollout = controller.evaluate_receding_horizon(
            x=state,
            candidate_bid_px=D("79995.0"),
            candidate_ask_px=D("80005.0"),
            candidate_bid_qty=D("0.00025"),
            candidate_ask_qty=D("0.00025"),
            mid=D("80000.0"),
            fair_val=D("80000.0"),
            p_fill_bid=0.65,
            p_fill_ask=0.65,
            expected_markout_bid=D("0.10"),
            expected_markout_ask=D("0.10"),
            maker_fee_bps=D("0.0"),
            max_pos_usd=D("100.0"),
            skew_rate_bps=D("0.80"),
        )

        self.assertIn("cumulative_utility_bps", rollout)
        self.assertIn("terminal_value_bps", rollout)
        self.assertGreater(rollout["cumulative_utility_bps"], D("-10.0"))
        print(f"✓ test_01_decision_state_and_receding_horizon passed: Multi-step utility ({rollout['cumulative_utility_bps']:.2f} bps) calculated.")

    async def test_02_optimal_stopping_and_active_withdrawal(self):
        """2. Optimal Stopping & Active Liquidity Withdrawal:
        Verifies 3-state controller transitions: Normal -> Reduced-Risk -> Withdrawal -> Recovery."""
        controller = OptimalStoppingWithdrawalController(withdrawal_hold_s=3.0)
        latency = LatencyState(estimated_rtt_ms=15.0, inflight_orders_count=0)

        # 1. Normal state: healthy market conditions
        s_norm = DecisionState(
            belief=BeliefState(),
            epistemic_uncertainty=0.10,
            inventory_usd=D("0"),
            inventory_qty=D("0"),
            context_markout_rmse=0.40,
            context_brier_score=0.08,
            latency=latency,
            timestamp=100.0,
        )
        mode1, r1 = controller.evaluate_modes(
            s_norm, expected_spread_capture_bps=D("1.5"), expected_adverse_move_bps=D("0.5"),
            market_spread_bps=D("2.0"), vol_bps=D("4.0"), unrealized_pnl_bps=D("0"), now=100.0
        )
        self.assertEqual(mode1, QuotingMode.NORMAL_QUOTING)

        # 2. Reduced-Risk state: elevated volatility or epistemic uncertainty
        s_reduced = DecisionState(
            belief=BeliefState(),
            epistemic_uncertainty=0.60,
            inventory_usd=D("0"),
            inventory_qty=D("0"),
            context_markout_rmse=0.40,
            context_brier_score=0.08,
            latency=latency,
            timestamp=105.0,
        )
        mode2, r2 = controller.evaluate_modes(
            s_reduced, expected_spread_capture_bps=D("1.5"), expected_adverse_move_bps=D("0.5"),
            market_spread_bps=D("2.0"), vol_bps=D("5.0"), unrealized_pnl_bps=D("0"), now=105.0
        )
        self.assertEqual(mode2, QuotingMode.REDUCED_RISK_QUOTING)

        # 3. Withdrawal state: latency stress or catastrophic adverse move (V_Q < V_W)
        s_withdraw = DecisionState(
            belief=BeliefState(),
            epistemic_uncertainty=0.90,  # Extreme epistemic breakdown
            inventory_usd=D("50"),
            inventory_qty=D("0.000625"),
            context_markout_rmse=5.0,
            context_brier_score=0.40,
            latency=LatencyState(estimated_rtt_ms=120.0, inflight_orders_count=4),
            timestamp=110.0,
        )
        mode3, r3 = controller.evaluate_modes(
            s_withdraw, expected_spread_capture_bps=D("0.5"), expected_adverse_move_bps=D("3.5"),
            market_spread_bps=D("1.0"), vol_bps=D("15.0"), unrealized_pnl_bps=D("-40.0"), now=110.0
        )
        self.assertEqual(mode3, QuotingMode.WITHDRAWAL)
        self.assertIn("latency_stress", r3)

        # 4. Immediate re-evaluation during cooldown remains in WITHDRAWAL
        mode4, r4 = controller.evaluate_modes(
            s_norm, expected_spread_capture_bps=D("1.5"), expected_adverse_move_bps=D("0.5"),
            market_spread_bps=D("2.0"), vol_bps=D("4.0"), unrealized_pnl_bps=D("0"), now=111.0
        )
        self.assertEqual(mode4, QuotingMode.WITHDRAWAL)

        # 5. After cooldown (dt > 3.0s), transitions back to REDUCED_RISK_QUOTING
        mode5, r5 = controller.evaluate_modes(
            s_norm, expected_spread_capture_bps=D("1.5"), expected_adverse_move_bps=D("0.5"),
            market_spread_bps=D("2.0"), vol_bps=D("4.0"), unrealized_pnl_bps=D("0"), now=114.5
        )
        self.assertEqual(mode5, QuotingMode.REDUCED_RISK_QUOTING)
        print("✓ test_02_optimal_stopping_and_active_withdrawal passed: Complete 3-state lifecycle verified.")

    async def test_03_mean_field_competition(self):
        """3. Mean-Field Game & Strategic Competitor Dynamics:
        Models competitor response m_t(a_t), discounts strategic fill probability, and avoids pennying wars."""
        engine = MeanFieldCompetitionEngine(base_elasticity=0.65)

        # Test passive quote (no competitor retaliation triggered)
        p_passive, sc_passive, m_passive, why_passive = engine.evaluate_strategic_quote(
            side=BUY,
            candidate_price=D("79998.0"),
            is_touch_improvement=False,
            nominal_spread_capture_bps=D("1.5"),
            nominal_fill_prob=0.50,
            nominal_markout_bps=D("0.20"),
            tick=D("0.1"),
        )
        self.assertEqual(p_passive, 0.50)
        self.assertEqual(m_passive, D("0.20"))

        # Inject aggressive competitor responses: competitors repenny 9 out of 10 times
        for i in range(10):
            engine.record_quote_interaction(BUY, improved_ticks=1, was_undercut_within_window=True, now=10.0 + i)

        self.assertEqual(engine.state.competitive_intensity_regime, "HYPER_COMPETITIVE")
        self.assertTrue(engine.should_avoid_pennying_war(spread_ticks=D("1.5")))

        # Under hyper-competition, strategic fill probability and markout are discounted
        p_strat, sc_strat, m_strat, why_strat = engine.evaluate_strategic_quote(
            side=BUY,
            candidate_price=D("80000.0"),
            is_touch_improvement=True,
            nominal_spread_capture_bps=D("1.0"),
            nominal_fill_prob=0.80,
            nominal_markout_bps=D("0.10"),
            tick=D("0.1"),
        )
        self.assertLess(p_strat, 0.50, "Strategic fill probability must reflect competitive priority loss")
        self.assertLess(m_strat, D("0.0"), "Strategic markout must penalize toxic sweep-through fills")
        print(f"✓ test_03_mean_field_competition passed: Strategic fill prob ({0.80} -> {p_strat:.3f}) and markout penalized.")

    async def test_04_causal_impact_identification(self):
        """4. Causal Market-Impact Identification:
        Estimates true causal treatment effect tau(a, a') using do-calculus and separates selection bias."""
        estimator = CausalMarketImpactEstimator(base_causal_slope=0.00015)

        # Generate observations across low-flow (unconfounded) and high-flow (confounded) states
        for i in range(20):
            estimator.record_outcome(
                side=BUY, action_size_usd=20.0 + (i % 5) * 5.0, action_distance_bps=1.0,
                is_interventional=True, cross_flow_usd=500.0, vol_bps=3.5,
                queue_ahead_usd=6000.0, regime_index=0, did_fill=True,
                realized_markout_bps=0.40 - 0.00018 * (20.0 + (i % 5) * 5.0),
                realized_pnl_usd=0.015, now=10.0 + i
            )
        for i in range(20):
            estimator.record_outcome(
                side=BUY, action_size_usd=20.0, action_distance_bps=1.0,
                is_interventional=False, cross_flow_usd=20000.0, vol_bps=6.0,
                queue_ahead_usd=4000.0, regime_index=1, did_fill=True,
                realized_markout_bps=-1.80, realized_pnl_usd=-0.03, now=30.0 + i
            )

        causal_m, sel_bias = estimator.predict_causal_markout(candidate_size_usd=40.0, baseline_markout_bps=0.30)
        self.assertGreater(float(sel_bias), 1.0, "Selection bias must identify informed counterparty adverse choice")
        self.assertLess(float(causal_m), 0.30, "Causal markout must incorporate endogenous price concession")
        print(f"✓ test_04_causal_impact_identification passed: Causal slope ({estimator.estimated_causal_impact_bps_per_usd:.6f}) and selection bias ({sel_bias:.2f} bps) isolated.")

    async def test_05_tail_risk_budget_allocation(self):
        """5. Tail-Risk-Aware Stochastic Control & 6-Component Risk Budget Allocation:
        Evaluates J(pi) with CVaR and throttles quoting size when model uncertainty or latency consumes risk quota."""
        allocator = RiskBudgetAllocator(total_budget_usd=10.0, cvar_lambda=0.50, vol_inventory_penalty_eta=0.005)

        # Baseline conditions: low uncertainty, low latency
        sz_norm, inv_norm, reason_norm = allocator.evaluate_budget_utilization(
            inventory_usd=10.0, vol_bps=4.0, epistemic_uncertainty=0.05, latency_rtt_ms=15.0, is_shock_regime=False
        )
        self.assertGreaterEqual(float(sz_norm), 0.90)

        # Stress conditions: high uncertainty (0.75) and elevated latency (55ms)
        sz_stress, inv_stress, reason_stress = allocator.evaluate_budget_utilization(
            inventory_usd=45.0, vol_bps=12.0, epistemic_uncertainty=0.75, latency_rtt_ms=55.0, is_shock_regime=False
        )
        self.assertLess(float(sz_stress), 0.65, "Risk allocator must throttle size under uncertainty/latency stress")
        self.assertIn("uncertainty_drain", reason_stress)

        # Risk-sensitive objective J(pi)
        j_benign = allocator.compute_risk_sensitive_objective(
            expected_pnl_bps=D("1.50"), cvar_tail_loss_bps=D("-0.50"), pos_usd=D("10.0"), vol_bps=D("4.0")
        )
        j_shock = allocator.compute_risk_sensitive_objective(
            expected_pnl_bps=D("1.50"), cvar_tail_loss_bps=D("-4.00"), pos_usd=D("80.0"), vol_bps=D("15.0")
        )
        self.assertGreater(j_benign, D("0"))
        self.assertLess(j_shock, D("-10.0"), "Risk-sensitive objective must heavily penalize tail CVaR and inventory")
        print(f"✓ test_05_tail_risk_budget_allocation passed: Budget throttling ({sz_norm} -> {sz_stress}) and CVaR penalty verified.")

    async def test_06_joint_optimization_end_to_end(self):
        """6. End-to-End Level 4 Integrated Quoting Engine:
        Runs bot with active withdrawal, mean-field competition, and risk-budget allocation enabled."""
        bot, s, clock = sim.make(
            EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100,
            ENABLE_BELIEF_STATE_QUOTING="1",
            ENABLE_LEVEL4_CONTROL="1",
            ENABLE_ACTIVE_WITHDRAWAL="1",
            ENABLE_RISK_BUDGET_ALLOCATOR="1",
            ENABLE_MEAN_FIELD_COMPETITION="1",
            ENABLE_CAUSAL_IMPACT="1",
        )

        # 1. Normal market conditions: bot quotes both sides cleanly
        await sim.step(bot, s, clock, "80000.0", "80010.0", bsz="2", asz="2")
        buy_order = bot.om.get_order_by_slot(0, BUY)
        sell_order = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNotNone(buy_order, "Buy quote should be placed in normal mode")
        self.assertIsNotNone(sell_order, "Sell quote should be placed in normal mode")

        # 2. Inject extreme latency and epistemic uncertainty to trigger ACTIVE WITHDRAWAL
        bot.engine.latency_state.estimated_rtt_ms = 150.0
        bot.engine.latency_state.inflight_orders_count = 5

        # In next step, Active Withdrawal Controller should cancel quotes and withdraw completely
        await sim.step(bot, s, clock, "80000.0", "80010.0", bsz="2", asz="2", dt=0.5)
        withdrawn_buy = bot.om.get_order_by_slot(0, BUY)
        withdrawn_sell = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNone(withdrawn_buy, "Buy quotes must be withdrawn under extreme latency stress")
        self.assertIsNone(withdrawn_sell, "Sell quotes must be withdrawn under extreme latency stress")
        self.assertEqual(bot.engine.withdrawal_controller.current_mode, QuotingMode.WITHDRAWAL)

        # 3. Restore latency to healthy state and advance clock past withdrawal hold
        bot.engine.latency_state.estimated_rtt_ms = 15.0
        bot.engine.latency_state.inflight_orders_count = 0
        clock.t += 6.0  # Pass withdrawal hold cooldown

        await sim.step(bot, s, clock, "80000.0", "80010.0", bsz="2", asz="2", dt=0.5)
        recovered_buy = bot.om.get_order_by_slot(0, BUY)
        self.assertIsNotNone(recovered_buy, "Bot should resume quoting after cooldown and recovery")
        print(f"✓ test_06_joint_optimization_end_to_end passed: Active withdrawal, full quote cancel, and recovery verified.")


if __name__ == "__main__":
    unittest.main()
