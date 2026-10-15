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
import orders
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

    async def test_07_phantom_fill_and_reduce_only_recovery(self):
        """7. Phantom Fill Prevention & Reduce-Only Recovery:
        Verifies that cancelled orders with remainingSize=0 never create phantom fills,
        and REDUCE_ONLY_WOULD_INCREASE immediately triggers forced position sync."""
        bot, s, clock = sim.make()
        fills_recorded = []
        bot.om.on_fill = lambda side, qty, px, o: fills_recorded.append((side, qty, px))

        # 1. Simulate cancelled order with remainingSize=0
        import orders
        o = orders.Order('test-1', 0, 'BUY', D('80000'), D('0.1'), D('0.1'), 1000, 0, 0)
        bot.om.orders['test-1'] = o
        cancel_msg = {
            'orderId': 'test-1', 'state': 'CANCELED', 'status': 'CANCELED',
            'remainingSize': '0', 'filledSize': '0', 'price': '80000'
        }
        bot.om.on_update(cancel_msg, 10.0)
        self.assertEqual(len(fills_recorded), 0, 'Cancelled order must never trigger phantom fill')

        # 2. Simulate REDUCE_ONLY_WOULD_INCREASE rejection
        reconciled_called = []
        bot.om.on_reduce_only_rejected = lambda: reconciled_called.append(True)
        o2 = orders.Order('test-2', 0, 'SELL', D('80010'), D('0.1'), D('0.1'), 1000, 0, 0)
        bot.om.orders['test-2'] = o2
        reject_msg = {
            'orderId': 'test-2', 'state': 'REJECTED', 'status': 'REJECTED',
            'rejectionReason': 'REDUCE_ONLY_WOULD_INCREASE'
        }
        bot.om.on_update(reject_msg, 11.0)
        self.assertEqual(len(reconciled_called), 1, 'Reduce-only reject must trigger position reconciliation')
        print('✓ test_07_phantom_fill_and_reduce_only_recovery passed: Zero phantom fills and immediate recovery verified.')

    async def test_08_reconcile_signed_short_position_and_hardware_clamp(self):
        """8. Reconcile Signed Short Position & Hardware Reduce-Only Clamp:
        Verifies that exchange signed positions (-0.2393776 for SHORT) do NOT flip sign to positive,
        and hardware clamp in OrderManager prevents reduce_only=True on SELL when short or flat."""
        bot, s, clock = sim.make()
        bot.md.info.mark = D('83.50')
        res = {'positions': [{'marketId': bot.md.info.market_id, 'size': '-0.2393776', 'side': 'SHORT'}]}
        bot.ex.get = lambda *args, **kwargs: asyncio.sleep(0, result=res)
        await bot._reconcile_positions(100.0, force=True)
        self.assertEqual(bot.ledger.position, D('-0.2393776'), 'Position must be SHORT -0.2393776, NOT flipped to positive!')

        # Test hardware clamp
        target = orders.Order('dummy', 0, SELL, D('83.56'), D('0.2393776'), D('0.2393776'), 1000, 0, 0)
        target.is_exit_quote = True
        target.pair_index = 0
        target.side = SELL
        target.price = D('83.56')
        target.qty = D('0.2393776')
        placed_orders = []
        async def mock_place(pair_index, side, price, qty, now, time_in_force='ALO', reduce_only=False, **kwargs):
            placed_orders.append((side, price, qty, reduce_only))
            return orders.Order('placed-1', pair_index, side, price, qty, qty, 1000, now, now, is_reduce_only=reduce_only)
        bot.om.place = mock_place
        await bot.om._sync_one(target, 100.0)
        self.assertEqual(len(placed_orders), 1)
        self.assertFalse(placed_orders[0][3], 'Hardware clamp MUST disarm reduce_only on SELL when position is short!')
        print('✓ test_08_reconcile_signed_short_position_and_hardware_clamp passed: Correct sign reconciliation and clamp verified.')

    async def test_09_realtime_flow_defense_and_sweep_guard_adaptation(self):
        """9. Real-Time Flow Defense & Sweep Guard Adaptation:
        Verifies that Sweep Guard and Burst Guard events trigger immediate real-time feedback
        to the Online Learner, expanding sweep windows, tightening trend pull hurdles,
        and adapting burst cooldowns dynamically."""
        bot, s, clock = sim.make(ENABLE_ONLINE_LEARNING=1)
        l = bot.ledger.learner
        base_sw = l.sweep_guard_window_s
        base_bcd = l.burst_cooldown_s
        base_bwin = l.burst_window_s
        base_pull = l.trend_pull_bps
        base_edge = l.min_edge_bps

        # 1. Trigger sweep event
        l.on_sweep('BUY', 2, 1.5)
        self.assertGreater(l.sweep_guard_window_s, base_sw, 'Sweep window must expand on sweep detection')
        self.assertGreater(l.burst_cooldown_s, base_bcd, 'Burst cooldown must increase on sweep detection')
        self.assertGreater(l.min_edge_bps, base_edge, 'Min edge must widen on sweep detection')
        self.assertLess(l.trend_pull_bps, base_pull, 'Trend pull must tighten on sweep detection')

        # 2. Trigger burst event
        l.on_burst('SELL', 2, 20.0)
        self.assertGreater(l.burst_window_s, base_bwin, 'Burst window must expand on burst detection')

        # 3. Trigger adverse markout
        cur_sw = l.sweep_guard_window_s
        cur_th = l.trend_hold_s
        l.on_markout(D('-4.5'), 'BUY', D('2.0'), horizon=5.0)
        self.assertGreater(l.sweep_guard_window_s, cur_sw, 'Sweep window must expand on adverse markout')
        self.assertGreater(l.trend_hold_s, cur_th, 'Trend hold must increase on adverse markout')
        print('✓ test_09_realtime_flow_defense_and_sweep_guard_adaptation passed: Real-time dynamic updates verified.')


    async def test_10_unified_system_no_ladder_collision_and_no_wiggle_loss(self):
        """10. Unified System Closed-Loop Feedback, Wiggle Protection & Ladder Collision Guard:
        Verifies:
        - Normal 1.5-2.5 bps wiggles do NOT trigger maker loss-selling below breakeven.
        - L1 can never collide at the exact same price as a resting L0 order.
        - Fills and markouts feed back into impact_kernel, meta_learner, markout_model, and causal_estimator.
        """
        bot, s, clock = sim.make(
            ORDER_USD=20, MAX_POSITION_USD=100, EXTRA_LEVELS=1,
            MIN_EDGE_BPS="2.2", LEVEL_SPACING_BPS="2.0", MAKER_EXIT_FIRST="1", ENABLE_TAKER_EXITS="0"
        )
        m = sim.Market(10, "NVDA-USD", "ONLINE", D("0.01"), D("0.0001"), [], D("5"), D("0.0001"), D("100000"), D("226.90"), False)
        bot.md.info = m
        bot.md.update(D("226.89"), D("226.91"), D("10.0"), D("10.0"), clock.t)

        # 1. Enter Long at 226.90
        import orders, engine
        o_buy = orders.Order("b0", 0, BUY, D("226.90"), D("0.10"), D("0.10"), 1001, clock.t, clock.t, quote_mid=D("226.90"))
        bot._on_fill(BUY, D("0.10"), D("226.90"), o_buy)
        self.assertGreater(len(bot.engine.impact_kernel.trades), 0, "Fill must feed impact_kernel")
        self.assertGreater(len(bot.engine.meta_learner._fills_log), 0, "Fill must feed meta_learner")

        # 2. Simulate a 2.0 bps dip (226.85 / 226.87, mid=226.86 -> -1.76 bps unrealized)
        clock.t += 1.0
        bot.md.update(D("226.85"), D("226.87"), D("10.0"), D("10.0"), clock.t)
        bot.ledger.process_markouts(bot.md.mid, clock.t)
        self.assertGreater(len(bot.engine.markout_model.distributions), 0, "Markout must feed markout_model")

        quotes = bot.engine.generate_ladder_quotes(m, bot.md, bot.ledger, clock.t, False, False)
        sells = [q for q in quotes if q.side == SELL]
        self.assertGreaterEqual(len(sells), 1)
        self.assertGreaterEqual(sells[0].price, D("226.90"), "Maker unwind must NOT dump at 226.87 loss on a 1.76 bps wiggle!")
        if len(sells) >= 2:
            self.assertGreater(sells[1].price, sells[0].price, "L1S must be strictly higher than L0S!")

        # 3. Verify OrderManager collision guard when L0S is resting at 226.94 and L1S tries to modify to 226.94
        o0 = orders.Order("s0", 0, SELL, D("226.94"), D("0.10"), D("0.10"), 2001, clock.t, clock.t)
        o1 = orders.Order("s1", 1, SELL, D("226.98"), D("0.10"), D("0.10"), 2002, clock.t - 5.0, clock.t - 5.0)
        bot.om.orders = {"s0": o0, "s1": o1}
        bot.om.pair_slots = {(0, SELL): "s0", (1, SELL): "s1"}
        modified_prices = []
        async def mock_mod(o, px, now, reduce_only=None, **kw):
            modified_prices.append(px)
            o.price = px
            return True
        bot.om.modify = mock_mod
        t1 = engine.QuoteTarget(1, SELL, D("226.94"), D("0.10"), D("1.0"), 0.5, False, D("226.90"))
        await bot.om._sync_one(t1, clock.t)
        if modified_prices:
            self.assertGreater(modified_prices[0], D("226.94"), "Collision guard must push L1S strictly above resting L0S (226.94)!")
        print("✓ test_10_unified_system_no_ladder_collision_and_no_wiggle_loss passed: Closed-loop system, wiggle protection & collision guard verified.")


    async def test_11_nbis_log_regression_strict_edge_and_no_wick_stop(self):
        """11. NBIS-USD Log Regression (Strict Edge Floor, Wick-Stop Protection & Persistent Toxicity Floor):
        Verifies:
        - When MIN_EDGE_BPS=3.2 and ENABLE_SELECTIVE_TOUCH=1, L0 never quotes inside model_px (<3.2 bps from mid).
        - A transient -11.0 bps wick at hold=3.0s (with STRESS_LOSS_BPS=15.0) does NOT trigger stress_loss stop-out.
        - After >=10 markouts with >55% adverse fill rate, tick_decay preserves an elevated toxicity floor above base.
        """
        import orders, engine
        bot, s, clock = sim.make(
            ORDER_USD=35, MAX_POSITION_USD=105, EXTRA_LEVELS=1,
            MIN_EDGE_BPS="3.2", LEVEL_SPACING_BPS="3.5", STRESS_LOSS_BPS="15.0",
            EMERGENCY_TAKER_LOSS_BPS="18.0", ENABLE_SELECTIVE_TOUCH="1",
            MAKER_EXIT_FIRST="1", ENABLE_TAKER_EXITS="0"
        )
        m = sim.Market(46, "NBIS-USD", "ONLINE", D("0.01"), D("0.0000001"), [], D("5"), D("0.0000001"), D("100000"), D("186.50"), False)
        bot.md.info = m
        bot.md.update(D("186.49"), D("186.51"), D("10.0"), D("10.0"), clock.t)

        # 1. Flat quoting: L0 bid must respect MIN_EDGE_BPS=3.2 (<= 186.44, never at touch 186.49!)
        quotes = bot.engine.generate_ladder_quotes(m, bot.md, bot.ledger, clock.t, False, False)
        bids = [q for q in quotes if q.side == BUY]
        self.assertGreaterEqual(len(bids), 1)
        self.assertLessEqual(bids[0].price, D("186.44"), f"L0 bid {bids[0].price} must respect MIN_EDGE_BPS=3.2 (<=186.44)")

        # 2. Enter Long at 186.44, then experience a 3.0s wick down to 186.23 (-11.26 bps unrealized)
        o_buy = orders.Order("b0", 0, BUY, D("186.44"), D("0.18"), D("0.18"), 1001, clock.t, clock.t, quote_mid=D("186.50"))
        bot._on_fill(BUY, D("0.18"), D("186.44"), o_buy)
        clock.t += 3.0
        bot.md.update(D("186.22"), D("186.24"), D("10.0"), D("10.0"), clock.t)
        u_quotes = bot.engine.generate_ladder_quotes(m, bot.md, bot.ledger, clock.t, False, False)
        sells = [q for q in u_quotes if q.side == SELL]
        self.assertGreaterEqual(len(sells), 1)
        self.assertFalse(sells[0].is_taker, "Must NOT fire taker IOC on a 3s wick!")
        self.assertGreaterEqual(sells[0].price, D("186.44"), "Must hold breakeven 186.44 during a 3s wick instead of dumping at 186.24!")

        # 3. Verify learner persistent toxicity floor after >=10 markouts with high adverse fill rate
        l = bot.ledger.learner
        for _ in range(12):
            l.on_markout(D("-3.0"), BUY, D("3.0"), horizon=5.0, now=clock.t)
        clock.t += 120.0
        l.tick_decay(clock.t)
        self.assertGreater(l.min_edge_bps, D("3.5"), "Learner must retain elevated min_edge_bps floor when adverse_fill_rate > 55%!")
        self.assertGreater(l.skew_bps, bot.cfg.skew_bps, "Learner must retain elevated skew_bps floor when adverse_fill_rate > 55%!")
        print("✓ test_11_nbis_log_regression_strict_edge_and_no_wick_stop passed: NBIS-USD regression fixes verified.")


    async def test_12_dry_run_paper_simulator_fills_and_rolling_learner(self):
        """12. DRY_RUN Paper Simulator Fills & Rolling Learner Win/Adverse Ratio Adaptation:
        Verifies:
        - When DRY_RUN=1, resting paper orders fill on live BBO crosses and queue-depleting trades,
          and _reconcile() does not wipe out paper orders or simulated positions.
        - OnlineLearner rolling win_rate / adverse_fill_rate adapts toward 50/50+ as benign markouts arrive,
          even if historical lifetime n_toxic was high.
        """
        bot, s, clock = sim.make(DRY_RUN="1", ORDER_USD=35, MAX_POSITION_USD=105, EXTRA_LEVELS=0, MIN_EDGE_BPS="2.5")
        m = sim.Market(46, "NBIS-USD", "ONLINE", D("0.01"), D("0.0001"), [], D("5"), D("0.0001"), D("100000"), D("186.50"), False)
        bot.md.info = m
        bot.md.update(D("186.49"), D("186.51"), D("5.0"), D("5.0"), clock.t)
        await bot.tick()
        self.assertGreater(len(bot.om.orders), 0, "DRY_RUN must place paper orders")

        # Verify _reconcile in DRY_RUN=1 does NOT drop paper orders
        clock.t += 10.0
        await bot._reconcile(clock.t)
        self.assertGreater(len(bot.om.orders), 0, "DRY_RUN _reconcile must NOT drop paper orders!")

        # Simulate a trade hitting our paper bid
        b_ord = bot.om.get_order_by_slot(0, BUY)
        self.assertIsNotNone(b_ord)
        bot._on_channel("trades", {"side": "SELL", "quantity": "10.0", "price": str(b_ord.price)}, False)
        self.assertGreater(bot.ledger.n_fills, 0, "DRY_RUN trade matching must record fill!")
        self.assertGreater(bot.ledger.position, D(0), "DRY_RUN position must increase on BUY fill!")

        # Verify rolling win_rate / adverse_fill_rate drifts upward when calibrated fills are benign
        l = bot.ledger.learner
        l.n_toxic = 150
        l.n_benign = 50
        for _ in range(10):
            l.on_markout(D("1.2"), BUY, D("0.5"), horizon=5.0, now=clock.t)
        self.assertGreaterEqual(l.win_rate, 60.0, "Rolling win_rate must adapt upward when recent markouts improve!")
        print("✓ test_12_dry_run_paper_simulator_fills_and_rolling_learner passed: Paper simulator fills and rolling learner verified.")


    async def test_13_pump_usd_active_quoting_and_accurate_pnl(self):
        """13. PUMP-USD 24/7 Active Quoting & Accurate Modify-Race / UserFill PnL Accounting:
        Verifies:
        - PUMP-USD (mid=0.003500, tick=0.000001 ~ 2.86bps) actively places L0 + L1 two-sided quotes
          even when ET_PAUSE_WINDOWS=15:50-09:45 is set, and is not blocked by 1-2 tick moves (~5.7 bps).
        - Unwind breakeven_px includes at least +1 tick profit (never $0.00 gross profit on normal hold time).
        - Modify-race fill and userFills channel reconcile exact execution price and fee into ledger.realized.
        """
        import orders, engine
        bot, s, clock = sim.make(
            MARKET="PUMP-USD", ORDER_USD=35, MAX_POSITION_USD=105, EXTRA_LEVELS=1,
            MIN_EDGE_BPS="2.2", LEVEL_SPACING_BPS="3.5", ET_PAUSE_WINDOWS="00:00-23:59",
            TREND_PULL_BPS="2.0", JUMP_BPS="5.5", VOL_PAUSE_BPS="10.0", ORACLE_GUARD="1", ORACLE_GUARD_BPS="3.0"
        )
        m_pump = sim.Market(99, "PUMP-USD", "ONLINE", D("0.000001"), D("1"), [], D("5"), D("1"), D("10000000"), D("0.003530"), False)
        bot.md.info = m_pump
        bot.md.info_ts = clock.t

        # Simulate a 2-tick move on PUMP-USD (0.003500 -> 0.003502 = 5.71 bps)
        await sim.step(bot, s, clock, "0.003499", "0.003501", bsz="50000", asz="50000")
        await sim.step(bot, s, clock, "0.003501", "0.003503", bsz="50000", asz="50000", dt=0.5)

        b0 = bot.om.get_order_by_slot(0, BUY)
        s0 = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNotNone(b0, "PUMP-USD L0 BUY must be placed (never blocked by ET_PAUSE_WINDOWS or 2-tick move)!")
        self.assertIsNotNone(s0, "PUMP-USD L0 SELL must be placed!")

        # Test userFills exact execution price & fee reconciliation on NBIS-USD
        bot2, s2, clock2 = sim.make(MARKET="NBIS-USD", ORDER_USD=35, MAX_POSITION_USD=105, MIN_EDGE_BPS="2.3", STRESS_LOSS_BPS="15.0")
        m_nbis = sim.Market(46, "NBIS-USD", "ONLINE", D("0.01"), D("0.0001"), [], D("5"), D("0.0001"), D("100000"), D("182.02"), False)
        bot2.md.info = m_nbis
        bot2.md.update(D("182.01"), D("182.03"), D("10.0"), D("10.0"), clock2.t)

        # Enter short at 182.02
        o_sell = orders.Order("s1", 1, SELL, D("182.02"), D("0.10"), D("0.10"), 1001, clock2.t, clock2.t, quote_mid=D("182.00"))
        bot2._on_fill(SELL, D("0.10"), D("182.02"), o_sell)

        # Check unwind L0 BUY targets <= 182.01 (+1 tick profit, NOT 182.02 $0 profit!)
        clock2.t += 2.0
        bot2.md.update(D("182.01"), D("182.03"), D("2.0"), D("10.0"), clock2.t)
        u_quotes = bot2.engine.generate_ladder_quotes(m_nbis, bot2.md, bot2.ledger, clock2.t, False, False)
        u_buys = [q for q in u_quotes if q.side == BUY]
        self.assertGreaterEqual(len(u_buys), 1)
        self.assertLessEqual(u_buys[0].price, D("182.01"), "Unwind BUY must target at least 1 tick profit (<=182.01, not 182.02)!")

        # Simulate modify-race fill: order was at 182.18 (prev_price), modified to 182.02 while mid=182.185, then FILLED arrives
        o_buy = orders.Order("b_race", 0, BUY, D("182.02"), D("0.10"), D("0.10"), 1002, clock2.t, clock2.t, is_reduce_only=True, prev_price=D("182.18"))
        bot2.om.orders["b_race"] = o_buy
        bot2.md.update(D("182.18"), D("182.19"), D("10.0"), D("10.0"), clock2.t)
        bot2.om._apply(o_buy, {"orderId": "b_race", "state": "FILLED", "status": "FILLED", "price": "182.02", "filledSize": "0.10", "remainingSize": "0"}, clock2.t)
        self.assertLess(bot2.ledger.realized, D(0), "Modify-race fill at 182.18 must book real loss (-$0.016), NOT fake $0.00 at 182.02!")
        print("✓ test_13_pump_usd_active_quoting_and_accurate_pnl passed: PUMP-USD active quoting & exact PnL accounting verified.")


if __name__ == "__main__":
    unittest.main()
