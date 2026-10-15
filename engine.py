"""Level 7 Quantitative Market-Making Engine."""
from __future__ import annotations

import logging
import time
import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, List, Tuple

from config import Config
from market import Market, MarketData
from ledger import Ledger
from latent_state import MarketRegime, BeliefState
from level4_control import (
    DecisionState,
    QuotingMode,
    LatencyState,
    OptimalStoppingWithdrawalController,
    RecedingHorizonQuoteController,
)
from mean_field_competition import MeanFieldCompetitionEngine
from causal_impact import CausalMarketImpactEstimator
from tail_risk_budget import RiskBudgetAllocator
from optimizer import MarketImpactKernel, CrossAssetHedger, RobustQuoteOptimizer
from markout_model import ConditionalMarkoutModel, CounterfactualExecutionEngine
from meta_learner import HierarchicalMetaLearner
from utils import BPS, BUY, SELL, ZERO, ONE, clamp, q_down, q_up, fmt

log = logging.getLogger("mm")


@dataclass
class QuoteTarget:
    pair_index: int
    side: str
    price: Decimal
    qty: Decimal
    expected_value_bps: Decimal
    fill_probability: float
    is_exit_quote: bool
    is_taker: bool = False
    quote_mid: Optional[Decimal] = None
    est_px: Optional[Decimal] = None


class MarketMakingEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._chase_top_until: float = 0.0
        self._chase_bottom_until: float = 0.0

        from optimizer import MarketImpactKernel, CrossAssetHedger, RobustQuoteOptimizer
        from markout_model import ConditionalMarkoutModel, CounterfactualExecutionEngine
        from meta_learner import HierarchicalMetaLearner

        self.impact_kernel = MarketImpactKernel(
            eta_0_bps_per_usd=getattr(cfg, "impact_eta_bps_per_usd", 0.0002),
            gamma_decay_s=getattr(cfg, "impact_gamma_decay_s", 0.15)
        )
        self.hedger = CrossAssetHedger(
            correlation_rho=getattr(cfg, "cross_asset_rho", 0.95),
            hedge_taker_fee_bps=float(getattr(cfg, "taker_fee_bps", Decimal("2.0")))
        )
        self.robust_optimizer = RobustQuoteOptimizer(
            self.impact_kernel,
            self.hedger,
            robust_epsilon=getattr(cfg, "robust_epsilon", 0.20)
        )
        self.markout_model = ConditionalMarkoutModel(
            prior_weight=getattr(cfg, "empirical_prior_weight", 5)
        )
        self.counterfactual_engine = CounterfactualExecutionEngine(self.markout_model)
        self.meta_learner = HierarchicalMetaLearner(cfg)
        self.last_hedge_recommendation: Optional[Tuple[float, float, str, float]] = None

        # Level 4 Advanced Research Components
        from level4_control import OptimalStoppingWithdrawalController, RecedingHorizonQuoteController, LatencyState
        from mean_field_competition import MeanFieldCompetitionEngine
        from causal_impact import CausalMarketImpactEstimator
        from tail_risk_budget import RiskBudgetAllocator

        self.latency_state = LatencyState()
        self.withdrawal_controller = OptimalStoppingWithdrawalController(
            withdrawal_hold_s=getattr(cfg, "withdrawal_hold_s", 5.0)
        )
        self.receding_horizon_controller = RecedingHorizonQuoteController()
        self.competition_engine = MeanFieldCompetitionEngine(
            base_elasticity=getattr(cfg, "competitor_base_elasticity", 0.60)
        )
        self.causal_estimator = CausalMarketImpactEstimator()
        self.risk_allocator = RiskBudgetAllocator(
            total_budget_usd=getattr(cfg, "risk_total_budget_usd", 10.0),
            cvar_lambda=getattr(cfg, "risk_cvar_lambda", 0.50)
        )

    def compute_fair_value(self, md: MarketData, now: float, ledger: Optional[Ledger] = None) -> Decimal:
        base_mid = md.mid
        if base_mid is None:
            return ZERO

        if not self.cfg.enable_orderbook_intel:
            return md.micro if (self.cfg.use_micro and md.micro) else base_mid

        micro = md.micro if md.micro else base_mid
        half_spr = (md.ask - md.bid) / Decimal("2") if (md.bid and md.ask) else ZERO

        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        alpha = l.obi_alpha if l else self.cfg.obi_alpha
        beta = l.tfi_beta if l else self.cfg.tfi_beta

        obi = md.obi
        obi_shift = half_spr * obi * alpha

        tfi = md.trade_flow_imbalance(10.0, now)
        tfi_shift = half_spr * tfi * beta

        cross_shift = ZERO
        cross_obi_shift = ZERO
        if self.cfg.enable_cross_exchange and md.cross.venues:
            cross_div = md.cross.lead_lag_divergence_bps(base_mid, now)
            if cross_div != ZERO:
                cross_shift = base_mid * (cross_div / BPS) * self.cfg.cross_lead_lag_weight
            cross_obi = md.cross.cross_obi(now)
            cross_obi_shift = half_spr * cross_obi * Decimal("0.5")
            cross_tfi = md.cross.cross_tfi(5.0, now)
            if cross_tfi != ZERO:
                cross_obi_shift += half_spr * cross_tfi * Decimal(str(self.cfg.cross_flow_weight))

        basis_shift = ZERO
        if hasattr(md, "reference_basis_bps"):
            basis = md.reference_basis_bps()
            if basis != ZERO:
                basis_shift = -base_mid * (basis / BPS) * Decimal("0.05")

        if getattr(self.cfg, "enable_belief_state_quoting", False) and hasattr(md, "latent_estimator"):
            regime_shift = md.latent_estimator.compute_regime_adjusted_fair_shift(half_spr, obi, tfi, alpha, beta)
            fair_val = micro + regime_shift + cross_shift + cross_obi_shift + basis_shift
        else:
            fair_val = micro + obi_shift + tfi_shift + cross_shift + cross_obi_shift + basis_shift
        if md.bid and md.ask and md.bid < md.ask:
            fair_val = clamp(fair_val, md.bid, md.ask)
        return fair_val

    def fill_probability(self, distance_bps: Decimal, ledger: Optional[Ledger] = None) -> float:
        d = float(max(ZERO, distance_bps))
        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        kappa = float(l.fill_prob_kappa if l else self.cfg.fill_prob_kappa)
        return math.exp(-kappa * d)

    def expected_adverse_move(self, side: str, md: MarketData, ledger: Ledger, now: float) -> Decimal:
        base_tox = ledger.side_tox_bps(side)
        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        trend_win = l.trend_window_s if (l and hasattr(l, "trend_window_s")) else self.cfg.trend_window_s
        trend_widen = l.trend_widen if l else self.cfg.trend_widen

        ret_5s = md.ret_bps(trend_win, now)
        tick_now = (md.info.tick_for(md.mid) if hasattr(md.info, "tick_for") else md.info.tick) if (md.info and md.mid) else Decimal("0")
        tick_noise_bps = (tick_now / md.mid * BPS * Decimal("2.0")) if (md.mid and md.mid > ZERO and tick_now > ZERO) else ZERO
        # Only penalize return exceeding normal 2-tick discretization noise when not in a large jump
        eff_ret = max(ZERO, abs(ret_5s) - tick_noise_bps) if abs(ret_5s) < Decimal("10.0") else abs(ret_5s)
        momentum_risk = ZERO
        if side == BUY and ret_5s < 0:
            momentum_risk = eff_ret * trend_widen
        elif side == SELL and ret_5s > 0:
            momentum_risk = eff_ret * trend_widen

        tfi = md.trade_flow_imbalance(10.0, now)
        flow_risk = ZERO
        if side == BUY and tfi < Decimal("-0.2"):
            flow_risk = abs(tfi) * Decimal("1.5")
        elif side == SELL and tfi > Decimal("0.2"):
            flow_risk = tfi * Decimal("1.5")

        cross_risk = ZERO
        if self.cfg.enable_cross_exchange and md.cross.venues:
            cr = md.cross
            cross_velo = cr.cross_velocity_bps(3.0, now)
            if side == BUY and cross_velo < -self.cfg.cross_velocity_threshold_bps:
                cross_risk = abs(cross_velo) * Decimal("2.0")
            elif side == SELL and cross_velo > self.cfg.cross_velocity_threshold_bps:
                cross_risk = cross_velo * Decimal("2.0")
            # stale-quote risk: external venues already moved away from our side, Arcus has not yet
            div = cr.lead_lag_divergence_bps(md.mid, now)
            mult = Decimal(str(self.cfg.cross_div_adverse_mult))
            if side == BUY and div < ZERO:
                cross_risk += abs(div) * mult
            elif side == SELL and div > ZERO:
                cross_risk += div * mult
            # external aggressive flow + forced liquidations against this side
            ctfi = cr.cross_tfi(3.0, now)
            if side == BUY and ctfi < Decimal("-0.2"):
                cross_risk += abs(ctfi) * Decimal("1.5")
            elif side == SELL and ctfi > Decimal("0.2"):
                cross_risk += ctfi * Decimal("1.5")
            liq_down, liq_up = cr.liq_pressure_usd(5.0, now)
            liq_thr = Decimal(str(max(self.cfg.cross_liq_usd, 1.0)))
            if side == BUY and liq_down > 0:
                cross_risk += min(Decimal("4"), Decimal(str(liq_down)) / liq_thr * Decimal("2"))
            elif side == SELL and liq_up > 0:
                cross_risk += min(Decimal("4"), Decimal(str(liq_up)) / liq_thr * Decimal("2"))

        total_adverse = base_tox + momentum_risk + flow_risk + cross_risk
        return total_adverse

    def compute_reservation_price(self, fair_value: Decimal, position_usd: Decimal,
                                  vol_bps: Decimal, ledger: Optional[Ledger] = None,
                                  target_inventory_usd: Decimal = ZERO) -> Decimal:
        if self.cfg.max_position_usd <= 0:
            return fair_value
        net_pos = position_usd - target_inventory_usd
        q = clamp(net_pos / self.cfg.max_position_usd, Decimal("-1"), Decimal("1"))
        
        q_eff = Decimal(str(math.copysign(math.pow(abs(float(q)), 1.3), float(q))))
        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        skew_rate = l.skew_bps if l else self.cfg.skew_bps
        gamma_rate = l.gamma_risk_aversion if l else self.cfg.gamma_risk_aversion

        inv_skew_bps = q_eff * skew_rate
        if vol_bps > 0:
            inv_skew_bps += q_eff * vol_bps * gamma_rate

        res_price = fair_value * (ONE - inv_skew_bps / BPS)
        return res_price
    def compute_target_inventory_usd(self, md: MarketData, now: float, ledger: Optional[Ledger] = None) -> Decimal:
        """Computes optimal target inventory based on short-term alpha and funding carry."""
        if not self.cfg.enable_smart_inventory_mgmt:
            return ZERO
        
        alpha_bps = ZERO
        obi = md.obi
        tfi = md.trade_flow_imbalance(2.0, now)
        flow_signal = Decimal("0.6") * obi + Decimal("0.4") * tfi
        alpha_bps += flow_signal * Decimal("1.5")

        if self.cfg.enable_cross_exchange and md.cross.venues and md.mid:
            div = md.cross.lead_lag_divergence_bps(md.mid, now)
            alpha_bps += div * Decimal("0.5")

        if getattr(self.cfg, "enable_funding_carry", True) and md.info and getattr(md.info, "funding_rate", ZERO) != ZERO:
            funding_rate = md.info.funding_rate
            horizon_s = min(30.0, float(self.cfg.max_hold_s))
            # Horizon-scaled funding yield (28800s in 8 hours)
            funding_yield_bps = (Decimal(str(horizon_s)) / Decimal("28800.0")) * funding_rate * BPS
            funding_bias = -funding_yield_bps * getattr(self.cfg, "funding_weight", Decimal("0.5"))
            funding_bias = clamp(funding_bias, Decimal("-0.15"), Decimal("0.15"))
            alpha_bps += funding_bias

        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        gamma = l.gamma_risk_aversion if l else self.cfg.gamma_risk_aversion
        denom = max(Decimal("0.05"), gamma * (ONE + md.vol_bps / Decimal("10.0")))
        target_usd = (alpha_bps / denom) * (self.cfg.order_usd / Decimal("10.0"))

        max_target = min(self.cfg.order_usd * Decimal("0.25"), self.cfg.max_position_usd * Decimal("0.10"))
        return clamp(target_usd, -max_target, max_target)

    def _adv_obi_persist(self, key: str, cond: bool, now: float) -> float:
        """Seconds the book has leaned against our open position (resets when it stops or we were flat)."""
        st = getattr(self, "_adv_obi", None)
        if st is None:
            st = self._adv_obi = {}
        since, seen = st.get(key, (now, now))
        if (not cond) or (now - seen) > 3.0:
            since = now
        st[key] = (since, now)
        return now - since

    def _exit_style(self, side: str, loss_bps: Decimal, md: MarketData, now: float, ledger: Ledger):
        """Decide how to get out when a taker-exit rule fires. Returns ("taker"|"maker", reason).
        taker allowed only if ENABLE_TAKER_EXITS=1 or the loss reached TAKER_HARD_STOP_BPS (catastrophe stop, 0=off).
        maker (post-only, reduce-only, joined at the best bid/ask) is used when
          - taker exits are disabled, or
          - MAKER_EXIT_FIRST=1 and (loss is only MAKER_EXIT_SLACK_BPS beyond the stress stop, or the touch is likely to fill).
        """
        cfg = self.cfg
        hard = cfg.taker_hard_stop_bps > ZERO and loss_bps >= cfg.taker_hard_stop_bps
        if hard:
            return ("taker", "hard_stop")
        if not cfg.enable_taker_exits:
            return ("maker", "taker_disabled")
        if not cfg.maker_exit_first:
            return ("taker", "default")
        l = ledger.learner if (ledger and hasattr(ledger, "learner") and cfg.enable_online_learning) else None
        stress_limit = l.stress_loss_bps if (l and hasattr(l, "stress_loss_bps")) else cfg.stress_loss_bps
        if loss_bps <= stress_limit + cfg.maker_exit_slack_bps:
            return ("maker", "slightly_beyond_stop")
        try:
            px = md.bid if side == BUY else md.ask          # BUY covers a short at the best bid; SELL exits a long at the best ask
            p = self.queue_fill_probability(side, px, md, now, horizon_s=cfg.queue_horizon_s, ledger=ledger)
            if p >= cfg.maker_exit_min_prob:
                return ("maker", "high_fill_prob=%.2f" % p)
        except Exception:
            pass
        return ("taker", "loss_beyond_slack_low_fill_prob")

    def _log_exit_style(self, side, style, why, rule, loss_bps) -> None:
        t = time.time()
        if t - getattr(self, "_last_exit_style_ts", 0.0) < 1.0:
            return
        self._last_exit_style_ts = t
        log.info("EXIT_STYLE %s -> %s (%s) rule=%s loss=%.2fbps", side, style.upper(), why, rule, float(loss_bps))

    def _log_taker_why(self, side, why, unreal_bps, emerg_loss_bps, adv_score, pos_ratio, mid, ledger) -> None:
        t = time.time()
        if t - getattr(self, "_last_why_ts", 0.0) < 1.0:
            return
        self._last_why_ts = t
        log.info("TAKER_WHY exit=%s rule=%s unreal=%.2fbps (stress<-%s emerg<-%.2f) adv_score=%.2f pos_ratio=%.2f avg_cost=%s mid=%s hold=%.1fs",
                 side, why, float(unreal_bps), self.cfg.stress_loss_bps, float(emerg_loss_bps), float(adv_score),
                 float(pos_ratio), ledger.avg_cost, mid, ledger.hold_s(ledger.last_now or 0) if hasattr(ledger, "hold_s") else 0.0)

    def dynamic_size_mult(self, side: str, pos_usd: Decimal, md: MarketData, ledger: Ledger,
                          regime: str, now: float) -> Decimal:
        """Per-side size multiplier in [DYN_SIZE_MIN, 1] applied to ADDING quotes only (unwinds keep position size).
        = inventory schedule x realized-edge (Kelly-style) x volatility x drawdown.
          inventory : linear shrink of the side that would add to the position, reaching DYN_INV_MIN at
                      DYN_INV_CAP_FRAC of MAX_POSITION_USD (inventory-limit sizing, GLFT/AS practice)
          edge      : recent per-side markout (bps, incl. captured half-spread) / DYN_EDGE_REF_BPS; <=0 -> floor
                      (fractional-Kelly logic: size follows edge, zero edge -> minimum size)
          vol       : DYN_VOL_REF_BPS / realized vol when vol exceeds the reference
          drawdown  : shrink as session loss approaches SESSION_MAX_LOSS_USD (weight DYN_DD_WEIGHT)
        """
        cfg = self.cfg
        if not cfg.enable_dynamic_sizing:
            return ONE
        floor = cfg.dyn_size_min
        # 1) inventory schedule (only the side that adds to an existing position)
        m_inv = ONE
        if cfg.max_position_usd > ZERO:
            f = pos_usd / cfg.max_position_usd
            adds = (side == BUY and f > ZERO) or (side == SELL and f < ZERO)
            if adds:
                af = min(ONE, abs(f) / max(Decimal("0.05"), cfg.dyn_inv_cap_frac))
                m_inv = ONE - af * (ONE - cfg.dyn_inv_min)
        # 2) realized edge of THIS side (markout vs fill price, 5s) -> Kelly-style
        buf = ledger.markouts_buy if side == BUY else ledger.markouts_sell
        vals = [x[1] if isinstance(x, tuple) else x for x in buf]
        if len(vals) < cfg.dyn_edge_min_n:
            m_edge = cfg.dyn_warmup_mult
            edge = None
        else:
            edge = sum(vals, ZERO) / Decimal(len(vals))
            m_edge = clamp(edge / max(Decimal("0.05"), cfg.dyn_edge_ref_bps), floor, ONE)
        # 3) volatility
        vol = md.vol_bps
        m_vol = ONE if vol <= cfg.dyn_vol_ref_bps else clamp(cfg.dyn_vol_ref_bps / vol, floor, ONE)
        if regime == "REGIME_D_TOXIC":
            m_vol = m_vol * Decimal("0.8")
        # 4) drawdown scaling
        m_dd = ONE
        try:
            if cfg.session_max_loss_usd > ZERO and md.mid:
                pnl = ledger.total_pnl(md.mid) - getattr(ledger, "dyn_pnl_base", ZERO)
                if pnl < ZERO:
                    m_dd = clamp(ONE - cfg.dyn_dd_weight * (-pnl / cfg.session_max_loss_usd), floor, ONE)
        except Exception:
            m_dd = ONE
        m = clamp(m_inv * m_edge * m_vol * m_dd, floor, ONE)
        last = getattr(self, "_dyn_log", {})
        if now - last.get(side, -1e9) >= 15.0 and m < Decimal("0.95"):
            last[side] = now
            self._dyn_log = last
            log.info("DYNSIZE %s x%.2f (inv=%.2f edge=%s vol=%.2f dd=%.2f)", side, float(m), float(m_inv),
                     ("%+.2fbps->%.2f" % (float(edge), float(m_edge))) if edge is not None else "warmup",
                     float(m_vol), float(m_dd))
        return m

    def calculate_vwap_cross_cost(self, side: str, qty: Decimal, md: MarketData) -> Tuple[Decimal, Decimal]:
        """Calculates actual VWAP price and crossing cost in bps by walking the L2 book."""
        mid = md.mid
        if not mid or mid <= ZERO or qty <= ZERO:
            return (mid or ZERO, ZERO)

        depth = md.depth_levels("BUY" if side == SELL else "SELL")
        if not depth:
            touch = md.bid if side == SELL else md.ask
            if not touch:
                return (mid, ZERO)
            slip = abs(touch - mid) / mid * BPS
            return (touch, slip + self.cfg.taker_fee_bps)

        rem = qty
        total_cost = ZERO
        for row in depth:
            px = row[0] if isinstance(row[0], Decimal) else Decimal(str(row[0]))
            sz = row[1] if isinstance(row[1], Decimal) else Decimal(str(row[1]))
            filled = min(rem, sz)
            total_cost += filled * px
            rem -= filled
            if rem <= ZERO:
                break

        if rem > ZERO:
            worst_px = Decimal(str(depth[-1][0]))
            penalty = Decimal("1.005") if side == BUY else Decimal("0.995")
            total_cost += rem * worst_px * penalty

        vwap = total_cost / qty
        crossing_slip_bps = abs(vwap - mid) / mid * BPS
        total_cross_bps = crossing_slip_bps + self.cfg.taker_fee_bps
        return (vwap, total_cross_bps)

    def queue_fill_probability(self, side: str, price: Decimal, md: MarketData, now: float,
                               horizon_s: float = 2.0, ledger: Optional[Ledger] = None) -> float:
        """Queue-Aware Fill Probability: Models queue depletion hazard rate lambda = consumption_rate / (queue_ahead + order_size)."""
        d = float(max(ZERO, (abs(md.mid - price) / md.mid * BPS) if md.mid else ZERO))
        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        kappa = float(l.fill_prob_kappa if l else self.cfg.fill_prob_kappa)
        dist_penalty = math.exp(-kappa * d)

        if not getattr(self.cfg, "enable_queue_model", True):
            return dist_penalty

        q_ahead = float(md.queue_ahead(side, price)) if hasattr(md, "queue_ahead") else float(md.bid_sz or 1)
        cons = float(md.consumption_rate(side, 2.0, now)) if hasattr(md, "consumption_rate") else 0.0
        my_sz = float(self.cfg.order_usd / (price if price > 0 else Decimal(1)))
        if cons <= 0.0 and hasattr(md, "consumption_rate"):
            cons = max(my_sz * 0.08, 0.35 * float(md.consumption_rate(side, 15.0, now)))

        denom = max(my_sz * 0.1, q_ahead + my_sz)
        lambda_fill = cons / denom
        p_queue = 1.0 - math.exp(-max(0.001, lambda_fill) * horizon_s)

        blended_p = 0.65 * p_queue + 0.35 * dist_penalty
        return float(max(0.02, min(0.98, blended_p)))



    def compute_optimal_quote_surface(self, md: MarketData, pos_usd: Decimal, now: float) -> Dict[str, Any]:
        """Exposes the precomputed / online optimal quote surface f(q, alpha, sigma, ...)."""
        mid = md.mid or ZERO
        fair_val = self.compute_fair_value(md, now)
        belief = getattr(md, "belief_state", None)
        if belief is None:
            from latent_state import BeliefState
            belief = BeliefState()
        return self.robust_optimizer.compute_optimal_surface(
            mid=mid,
            fair_val=fair_val,
            pos_usd=pos_usd,
            max_pos_usd=self.cfg.max_position_usd,
            vol_bps=md.vol_bps,
            belief=belief,
            base_edge_bps=self.cfg.min_edge_bps,
            skew_rate_bps=self.cfg.skew_bps,
            order_usd=self.cfg.order_usd,
            tick=Decimal("0.1"),
            step=Decimal("0.00001"),
            now=now
        )

    def generate_ladder_quotes(
        self,
        m: Market,
        md: MarketData,
        ledger: Ledger,
        now: float,
        buy_blocked: bool,
        sell_blocked: bool,
        existing_slots: Optional[set] = None
    ) -> List[QuoteTarget]:
        if not md.bid or not md.ask or md.bid >= md.ask or not md.mid:
            return []

        mid = md.mid
        tick = m.tick_for(mid)
        step = m.step
        spr_bps = md.spread_bps
        spr_ticks = (md.ask - md.bid) / tick if (tick > ZERO and md.ask and md.bid) else Decimal("10")
        tick_bps = (tick / mid) * BPS if mid > ZERO else Decimal("0.1")
        # Never let the emergency taker stop sit inside tick noise (coarse-tick tokens: 1 tick can be ~2bps)
        emerg_loss_bps = max(self.cfg.emergency_taker_loss_bps, tick_bps * Decimal("4"))
        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        min_edge = l.min_edge_bps if l else self.cfg.min_edge_bps
        max_edge = l.max_edge_bps if l else self.cfg.max_edge_bps
        vol_k = l.vol_k if l else self.cfg.vol_k
        tox_mult = l.tox_mult if l else self.cfg.tox_mult
        tox_spread_mult = l.regime_toxic_spread_mult if l else self.cfg.regime_toxic_spread_mult
        spacing = l.level_spacing_bps if l else self.cfg.level_spacing_bps
        size_mult_base = l.level_size_mult if l else self.cfg.level_size_mult
        min_ev_base = l.min_ev_bps if l else self.cfg.min_ev_bps

        fair_val = self.compute_fair_value(md, now, ledger=ledger)
        # Dust (< exchange min order size) can never be traded out: treat as flat so it cannot lock the bot in unwind mode.
        pos_eff = ledger.position if abs(ledger.position) >= m.min_size else ZERO
        pos_usd = pos_eff * mid
        target_inv_usd = self.compute_target_inventory_usd(md, now, ledger=ledger)
        res_price = self.compute_reservation_price(fair_val, pos_usd, md.vol_bps, ledger=ledger, target_inventory_usd=target_inv_usd)

        regime = md.detect_regime(now, ledger.tox_bps)
        is_toxic = (regime == "REGIME_D_TOXIC")

        # Only treat as ultra-tight touch-quoting market when user has NOT requested a wider min_edge (> 2.0 bps) or wider spread capture (> 0.5 bps)
        guar_sc_bps = getattr(self.cfg, "guarantee_spread_capture_bps", Decimal("0.5")) if self.cfg.guarantee_spread_capture else ZERO
        learner_active_widen = bool(l and getattr(l, "total_updates", 0) > 0 and l.min_edge_bps > self.cfg.min_edge_bps)
        is_liquid_market = (
            (spr_bps <= Decimal("1.2") or spr_ticks <= Decimal("2.5"))
            and (self.cfg.min_edge_bps <= Decimal("2.0"))
            and (guar_sc_bps <= Decimal("1.0"))
            and (not learner_active_widen)
            and (not is_toxic)
        )

        # Precompute per-tick adverse move estimates once (avoids 10x redundant calls in inner loop)
        adv_bps_buy = self.expected_adverse_move(BUY, md, ledger, now)
        adv_bps_sell = self.expected_adverse_move(SELL, md, ledger, now)

        base_edge_bps = min_edge + vol_k * md.vol_bps
        if is_toxic:
            base_edge_bps = base_edge_bps * tox_spread_mult

        # Level 2 & 3 Belief-State & Robust Optimizer Adjustments
        belief_spr_mult, belief_sz_mult, unc_penalty_bps = ONE, ONE, ZERO
        if getattr(self.cfg, "enable_belief_state_quoting", False) and hasattr(md, "latent_estimator"):
            belief_spr_mult, belief_sz_mult, unc_penalty_bps = md.latent_estimator.compute_regime_quote_multipliers()

        meta_spr_mult, meta_sz_mult, is_degraded, degraded_why = ONE, ONE, False, ""
        if getattr(self.cfg, "enable_meta_learner", False):
            self.meta_learner.update_context(float(md.vol_bps), float(md.spread_bps), now)
            meta_spr_mult, meta_sz_mult, is_degraded, degraded_why = self.meta_learner.get_hierarchical_adjustments(md.belief_state, now)
            if is_degraded and now - getattr(self, "_last_deg_log", 0.0) > 15.0:
                self._last_deg_log = now
                log.warning("CALIBRATION DEGRADED (%s): widening quotes and haircutting sizes", degraded_why)

        impact_pen_buy, impact_pen_sell = ZERO, ZERO
        if getattr(self.cfg, "enable_market_impact_kernel", False):
            impact_pen_buy = self.impact_kernel.predict_impact_penalty("BUY", float(self.cfg.order_usd), now)
            impact_pen_sell = self.impact_kernel.predict_impact_penalty("SELL", float(self.cfg.order_usd), now)

        if getattr(self.cfg, "enable_cross_asset_hedger", False):
            hedge_h, p_var, hedge_status = self.hedger.evaluate_optimal_hedge(float(pos_usd), 0.0)
            if hedge_status == "hedge_recommended" and abs(hedge_h) > 0:
                self.last_hedge_recommendation = (hedge_h, p_var, hedge_status, now)

        # --- Level 4 Optimal Stopping & Active Liquidity Withdrawal ---
        q_mode = QuotingMode.NORMAL_QUOTING
        if getattr(self.cfg, "enable_active_withdrawal", False):
            unreal_bps = (mid - ledger.avg_cost) / ledger.avg_cost * BPS if (ledger.avg_cost and ledger.avg_cost > ZERO and pos_eff != ZERO) else ZERO
            d_state = DecisionState(
                belief=md.belief_state,
                epistemic_uncertainty=float(md.belief_state.uncertainty),
                inventory_usd=pos_usd,
                inventory_qty=pos_eff,
                context_markout_rmse=self.meta_learner.context.markout_rmse_bps,
                context_brier_score=self.meta_learner.context.fill_prob_brier_score,
                latency=self.latency_state,
                timestamp=now,
            )
            q_mode, q_reason = self.withdrawal_controller.evaluate_modes(
                d_state,
                expected_spread_capture_bps=spr_bps * Decimal("0.5"),
                expected_adverse_move_bps=max(adv_bps_buy, adv_bps_sell),
                market_spread_bps=spr_bps,
                vol_bps=md.vol_bps,
                unrealized_pnl_bps=unreal_bps,
                now=now,
            )
            if q_mode == QuotingMode.WITHDRAWAL:
                log.info("LEVEL 4 ACTIVE WITHDRAWAL ENGAGED: %s (canceling quotes)", q_reason)
                return []

        # --- Level 4 Risk-Budget Allocation ---
        risk_budget_sz_mult = ONE
        if getattr(self.cfg, "enable_risk_budget_allocator", False):
            is_shock = (md.belief_state.dominant_regime == MarketRegime.LIQUIDITY_VACUUM)
            sz_mult_budget, _, budget_why = self.risk_allocator.evaluate_budget_utilization(
                inventory_usd=float(pos_usd),
                vol_bps=float(md.vol_bps),
                epistemic_uncertainty=float(md.belief_state.uncertainty),
                latency_rtt_ms=float(self.latency_state.estimated_rtt_ms),
                is_shock_regime=is_shock,
            )
            risk_budget_sz_mult = sz_mult_budget

        # Unified System Spread & Size Multipliers (combining Belief State, Meta-Learner, Withdrawal Mode, and Risk Budget)
        mode_spr_mult = Decimal("1.15") if q_mode == QuotingMode.REDUCED_RISK_QUOTING else ONE
        mode_sz_mult = Decimal("0.75") if q_mode == QuotingMode.REDUCED_RISK_QUOTING else ONE
        sys_spr_mult = max(belief_spr_mult, meta_spr_mult) * mode_spr_mult
        sys_sz_mult = clamp(belief_sz_mult * meta_sz_mult * risk_budget_sz_mult * mode_sz_mult, Decimal("0.15"), ONE)

        if sys_spr_mult > ONE or unc_penalty_bps > ZERO:
            base_edge_bps = base_edge_bps * sys_spr_mult + unc_penalty_bps

        if self.cfg.enable_cross_exchange and md.cross.venues:
            dispersion = md.cross.cross_dispersion_bps()
            if dispersion > Decimal("2.0"):
                dispersion_widen = (dispersion / Decimal("2.0")) * self.cfg.cross_dispersion_widen_mult
                base_edge_bps = base_edge_bps * (ONE + dispersion_widen / Decimal("10.0"))

        if is_liquid_market and sys_spr_mult <= Decimal("1.15"):
            target_liquid_edge = max(tick_bps / Decimal("2.0"), spr_bps / Decimal("2.0"))
            base_edge_bps = min(base_edge_bps, target_liquid_edge)
        else:
            if self.cfg.guarantee_spread_capture:
                min_capture_edge = (Decimal("2.0") * self.cfg.maker_fee_bps) + guar_sc_bps
                base_edge_bps = max(base_edge_bps, min_capture_edge)
            base_edge_bps = clamp(base_edge_bps, min_edge, max_edge)

        # Microstructure Flow & Asymmetric Quote Shading (Stoikov & Cartea-Jaimungal)
        obi = md.obi
        tfi = md.trade_flow_imbalance(10.0, now)
        flow_bias = Decimal("0.6") * obi + Decimal("0.4") * tfi
        half_spr = (md.ask - md.bid) / Decimal("2") if (md.bid and md.ask) else ZERO
        obi_alpha = l.obi_alpha if l else self.cfg.obi_alpha

        bid_asym_shift = max(ZERO, -flow_bias) * obi_alpha * half_spr
        ask_asym_shift = -max(ZERO, -flow_bias) * Decimal("0.3") * half_spr
        if flow_bias > 0:
            ask_asym_shift = flow_bias * obi_alpha * half_spr
            bid_asym_shift = -flow_bias * Decimal("0.3") * half_spr

        buy_tox = ledger.side_tox_bps(BUY)
        sell_tox = ledger.side_tox_bps(SELL)
        max_tox_addon = Decimal("2.5")
        buy_tox_penalty = min(max_tox_addon, buy_tox * tox_mult) if self.cfg.enable_online_learning else ZERO
        sell_tox_penalty = min(max_tox_addon, sell_tox * tox_mult) if self.cfg.enable_online_learning else ZERO

        quotes: List[QuoteTarget] = []
        stress_loss_limit = l.stress_loss_bps if l else self.cfg.stress_loss_bps
        max_hold_time = l.max_hold_s if l else self.cfg.max_hold_s

        hold_now = ledger.hold_s(now)
        min_stress_hold_s = 0.0 if self.cfg.stress_loss_bps <= Decimal("6.0") else min(20.0, float(max_hold_time) * 0.35)
        unreal_tot_bps = (ledger.unrealized(mid) / abs(pos_usd) * BPS) if pos_usd != 0 else ZERO
        is_stressed = (pos_usd != 0 and (
            hold_now > max_hold_time or
            (unreal_tot_bps < -stress_loss_limit and (hold_now >= min_stress_hold_s or unreal_tot_bps < -(stress_loss_limit * Decimal("1.6"))))
        ))
        trend_win = l.trend_window_s if (l and hasattr(l, 'trend_window_s')) else self.cfg.trend_window_s
        ret_5s = md.ret_bps(trend_win, now)
        trend_pull = l.trend_pull_bps if l else self.cfg.trend_pull_bps
        chase_cooldown = getattr(self.cfg, "chase_cooldown_s", 3.0)

        chase_trig_bps = max(Decimal("0.8"), tick_bps * Decimal("2.5"))
        chase_hold_bps = max(Decimal("0.6"), tick_bps * Decimal("2.0"))
        chase_top_trigger = (ret_5s > chase_trig_bps and (is_toxic or flow_bias > Decimal("0.30")))
        if chase_top_trigger:
            self._chase_top_until = now + chase_cooldown
        chasing_top = (now < getattr(self, "_chase_top_until", 0.0) or (ret_5s > chase_hold_bps and flow_bias > Decimal("0.20")))

        chase_bottom_trigger = (ret_5s < -chase_trig_bps and (is_toxic or flow_bias < Decimal("-0.30")))
        if chase_bottom_trigger:
            self._chase_bottom_until = now + chase_cooldown
        chasing_bottom = (now < getattr(self, "_chase_bottom_until", 0.0) or (ret_5s < -chase_hold_bps and flow_bias < Decimal("-0.20")))

                # Adverse flow detection for long (facing selling pressure) and short (facing buying pressure)
        has_adverse_selling = (tfi <= Decimal("-0.5") or (tfi <= Decimal("-0.2") and obi <= Decimal("-0.5")) or (obi <= Decimal("-0.7")) or (flow_bias <= Decimal("-0.4")))
        has_adverse_buying = (tfi >= Decimal("0.5") or (tfi >= Decimal("0.2") and obi >= Decimal("0.5")) or (obi >= Decimal("0.7")) or (flow_bias >= Decimal("0.4")))
        severe_sell_pressure = (flow_bias < Decimal("-0.50") or (is_toxic and md.obi < Decimal("-0.55")) or (has_adverse_selling and ret_5s < Decimal("-0.5")))
        severe_buy_pressure = (flow_bias > Decimal("0.50") or (is_toxic and md.obi > Decimal("0.55")) or (has_adverse_buying and ret_5s > Decimal("0.5")))

        depth_widen_buy = ZERO
        depth_widen_sell = ZERO
        depth_cut_buy = ZERO
        depth_cut_sell = ZERO
        if getattr(self.cfg, "use_depth_imbalance", False):
            levels = getattr(self.cfg, "imbalance_levels", 7)
            depth_obi = md.multi_depth_obi(levels)
            widen_bps = getattr(self.cfg, "imbalance_widen_bps", Decimal("4.0"))
            size_cut = getattr(self.cfg, "imbalance_size_cut", Decimal("0.3"))
            if depth_obi < Decimal("-0.20"):
                depth_widen_buy = widen_bps * abs(depth_obi)
                depth_cut_buy = size_cut * abs(depth_obi)
            elif depth_obi > Decimal("0.20"):
                depth_widen_sell = widen_bps * depth_obi
                depth_cut_sell = size_cut * depth_obi

        total_levels = 1 + max(0, self.cfg.extra_levels)

        remaining_buy_usd = max(ZERO, self.cfg.max_position_usd - pos_usd)
        remaining_sell_usd = max(ZERO, self.cfg.max_position_usd + pos_usd)
        l0_bid_px = None
        l0_ask_px = None
        last_bid_px = None
        last_ask_px = None

        # Precompute per-tick dynamic sizing and microstructure indicators once
        dyn_buy = self.dynamic_size_mult(BUY, pos_usd, md, ledger, regime, now) * sys_sz_mult
        dyn_sell = self.dynamic_size_mult(SELL, pos_usd, md, ledger, regime, now) * sys_sz_mult
        fragility_buy = md.liquidity_fragility(BUY, now) if hasattr(md, "liquidity_fragility") else ZERO
        fragility_sell = md.liquidity_fragility(SELL, now) if hasattr(md, "liquidity_fragility") else ZERO
        is_exh_buy = md.is_exhaustion(BUY, now) if hasattr(md, "is_exhaustion") else False
        is_exh_sell = md.is_exhaustion(SELL, now) if hasattr(md, "is_exhaustion") else False
        cons_buy_2s = Decimal(str(md.consumption_rate(BUY, 2.0, now))) if hasattr(md, "consumption_rate") else ZERO
        cons_sell_2s = Decimal(str(md.consumption_rate(SELL, 2.0, now))) if hasattr(md, "consumption_rate") else ZERO
        avoid_penny_war = (
            getattr(self.cfg, "enable_mean_field_competition", False)
            and self.competition_engine.should_avoid_pennying_war(spr_ticks)
        )

        for k in range(total_levels):
            k_spacing = Decimal(str(k)) * max(tick_bps, spacing)
            if is_toxic:
                k_spacing = k_spacing * Decimal("2.0")
            level_edge = base_edge_bps + k_spacing

            size_mult = Decimal(str(math.pow(float(size_mult_base), k)))
            level_usd = max(self.cfg.order_usd * size_mult, m.min_notional)
            level_edge_buy = level_edge + depth_widen_buy + impact_pen_buy
            level_usd_buy = max(level_usd * (ONE - depth_cut_buy) * dyn_buy, m.min_notional)
            level_edge_sell = level_edge + depth_widen_sell + impact_pen_sell
            level_usd_sell = max(level_usd * (ONE - depth_cut_sell) * dyn_sell, m.min_notional)

            # --- BUY SIDE --- #
            is_unwind_buy = (pos_usd < 0)
            if is_unwind_buy:
                # UNWIND SHORT: Exit short with minimum profit target or breakeven shading
                if k == 0:
                    qty = q_down(abs(ledger.position), step)
                    unreal_bps = (ledger.avg_cost - mid) / ledger.avg_cost * BPS if (ledger.avg_cost and ledger.avg_cost > ZERO) else ZERO
                    pos_ratio = -pos_usd / self.cfg.max_position_usd if self.cfg.max_position_usd > ZERO else ZERO

                    has_adverse_flow = has_adverse_buying
                    adv_score = max(ZERO, tfi) * Decimal("1.5") + max(ZERO, obi) * Decimal("1.5")
                    if ret_5s > ZERO:
                        adv_score += ret_5s * Decimal("0.5")
                    if self.cfg.enable_cross_exchange and md.cross.venues:
                        cross_velo = md.cross.cross_velocity_bps(3.0, now)
                        if cross_velo > ZERO:
                            adv_score += cross_velo * Decimal("0.5")
                    adv_score += (sell_tox / Decimal("5.0"))

                    trigger_taker = False
                    taker_why = ""
                    if self.cfg.enable_smart_inventory_mgmt:
                        if unreal_bps < -stress_loss_limit and (hold_now >= min_stress_hold_s or unreal_bps < -(stress_loss_limit * Decimal("1.6"))):
                            trigger_taker = True
                            taker_why = "stress_loss"
                        elif unreal_bps < -emerg_loss_bps and (has_adverse_flow or adv_score >= self.cfg.emergency_taker_score_threshold):
                            trigger_taker = True
                            taker_why = "emergency_loss_and_flow"
                        elif pos_ratio >= Decimal("0.80") and unreal_bps < -emerg_loss_bps and has_adverse_flow:
                            trigger_taker = True
                            taker_why = "pos_ratio_80_adverse_flow"
                        elif ret_5s >= Decimal("3.0") and obi >= Decimal("0.70") and tfi >= Decimal("0.50") and unreal_bps < -emerg_loss_bps:
                            trigger_taker = True
                            taker_why = "trend_cascade"
                        elif (self.cfg.adv_obi_exit and self._adv_obi_persist("S", obi >= self.cfg.adv_obi_thresh, now) >= self.cfg.adv_obi_secs
                              and unreal_bps < -self.cfg.adv_obi_loss_bps):
                            trigger_taker = True
                            taker_why = "adverse_obi_persist"

                    force_touch_exit = False
                    if trigger_taker and qty >= m.min_size:
                        style, style_why = self._exit_style(BUY, -unreal_bps, md, now, ledger)
                        self._log_exit_style(BUY, style, style_why, taker_why, -unreal_bps)
                        if style != "taker":
                            trigger_taker = False
                            force_touch_exit = True
                    if trigger_taker and qty >= m.min_size:
                        self._log_taker_why("BUY", taker_why, unreal_bps, emerg_loss_bps, adv_score, pos_ratio, mid, ledger)
                    if trigger_taker and md.ask and qty >= m.min_size:
                        vwap, cross_cost = self.calculate_vwap_cross_cost(BUY, qty, md)
                        slip_buffer = max(Decimal("2") * tick, q_up(md.ask * self.cfg.taker_slip_bps / BPS, tick))
                        taker_px = q_up(max(md.ask, vwap) + slip_buffer, tick)
                        est_px = min(max(md.ask, vwap), taker_px)   # what a CLOB IOC really pays: walk of the book
                        quotes.append(QuoteTarget(
                            pair_index=0, side=BUY, price=taker_px, qty=qty,
                            expected_value_bps=-cross_cost, fill_probability=1.0,
                            is_exit_quote=True, is_taker=True, quote_mid=mid, est_px=est_px
                        ))
                    else:
                        min_profit_bps = max(self.cfg.exit_min_profit_bps, Decimal("1.5"))
                        hold_time = ledger.hold_s(now)
                        max_hold = self.cfg.max_hold_s
                        if hold_time <= 15.0:
                            dynamic_min_profit_bps = min_profit_bps
                        else:
                            decay_span = max(15.0, max_hold * 0.6)
                            decay = max(Decimal("0.0"), Decimal("1.0") - Decimal(str((hold_time - 15.0) / decay_span)))
                            dynamic_min_profit_bps = max(min_profit_bps * decay, Decimal("0.6"))

                        min_profit_px = ledger.avg_cost * (ONE - dynamic_min_profit_bps / BPS) if (ledger.avg_cost and ledger.avg_cost > ZERO) else md.bid
                        if is_liquid_market or self.cfg.min_edge_bps <= Decimal("2.0") or hold_time > max_hold * 0.70:
                            breakeven_px = ledger.avg_cost * (ONE - self.cfg.maker_fee_bps / BPS) if (ledger.avg_cost and ledger.avg_cost > ZERO) else md.bid
                        else:
                            min_be_bps = max(self.cfg.maker_fee_bps, tick_bps)
                            breakeven_px = min(ledger.avg_cost - tick, q_down(ledger.avg_cost * (ONE - min_be_bps / BPS), tick)) if (ledger.avg_cost and ledger.avg_cost > ZERO) else md.bid

                        scratch_hold_thresh = min(max_hold * 0.4, 25.0)
                        is_scratch_time = (hold_time > scratch_hold_thresh)
                        extreme_buy_cascade = (self.cfg.emergency_taker_loss_bps <= Decimal("6.0") or hold_time >= min_stress_hold_s) and (tfi >= Decimal("0.75") and obi >= Decimal("0.70") and unreal_bps <= -Decimal("0.8"))
                        should_maker_scratch = self.cfg.enable_smart_inventory_mgmt and (force_touch_exit or has_adverse_flow or pos_ratio >= Decimal("0.60") or is_stressed or is_scratch_time or extreme_buy_cascade)

                        if should_maker_scratch:
                            # Only concede below breakeven_px on genuine stress, taker-override, or extreme cascade (never on 1.5 bps noise!)
                            if force_touch_exit or is_stressed or extreme_buy_cascade:
                                cand_px = md.bid
                                if self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and (md.bid + tick) <= breakeven_px:
                                    cand_px = md.bid + tick
                            else:
                                cand_px = min(md.bid, breakeven_px)
                                if self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and (md.bid + tick) <= breakeven_px:
                                    cand_px = md.bid + tick
                        else:
                            if md.bid <= breakeven_px:
                                cand_px = md.bid
                                if self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and (md.bid + tick) <= breakeven_px:
                                    cand_px = md.bid + tick
                            else:
                                cand_px = min(md.bid, min_profit_px)
                                if self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and (md.bid + tick) <= min_profit_px:
                                    cand_px = md.bid + tick

                        cand_px = min(cand_px, md.ask - tick)
                        cand_px = q_down(cand_px, tick)
                        if md.ask and cand_px >= md.ask:
                            cand_px = md.ask - tick
                        if is_liquid_market:
                            cand_px = min(cand_px, md.bid)
                        else:
                            min_cap = tick if should_maker_scratch else max(tick, q_down(mid * (getattr(self.cfg, "guarantee_spread_capture_bps", Decimal("0.5")) if self.cfg.guarantee_spread_capture else Decimal("0.1")) / BPS, tick))
                            cand_px = min(cand_px, q_down(mid - min_cap, tick))
                        if cand_px > ZERO and qty >= m.min_size:
                            quotes.append(QuoteTarget(
                                pair_index=0, side=BUY, price=cand_px, qty=qty,
                                expected_value_bps=Decimal("1.0"), fill_probability=0.9,
                                is_exit_quote=True, quote_mid=mid
                            ))
                            l0_bid_px = cand_px
            else:
                # ADDING LONG: Quote as long as inventory has room and side is not blocked
                severe_sell_pressure = (md.obi <= Decimal("-0.88") or (flow_bias < Decimal("-0.50") and (tfi <= Decimal("-0.15") or ret_5s <= -tick_bps)) or (is_toxic and md.obi < Decimal("-0.55")) or (has_adverse_selling and ret_5s < -max(Decimal("0.5"), tick_bps)))
                toxic_extra_level = (is_toxic and k > 0)
                # INVENTORY ROTATION & ANTI-CHASING: If already long, suppress touch L0 buy!
                already_long = (pos_usd >= self.cfg.order_usd * Decimal("0.5"))
                suppress_buy = (already_long and (k == 0 or (has_adverse_selling and ret_5s < Decimal("-0.5")) or severe_sell_pressure)) or (chasing_top and k == 0)
                if getattr(self.cfg, "enable_onesided_touch", True) and k == 0 and is_toxic and flow_bias <= Decimal("-0.40"):
                    suppress_buy = True
                can_add = (not buy_blocked) and (not severe_sell_pressure) and (not toxic_extra_level) and (not suppress_buy) and (remaining_buy_usd >= level_usd)
                if can_add:
                    if k == 0 and getattr(self.cfg, "enable_selective_touch", True):
                        fragility = fragility_buy
                        is_fragile = bool(fragility >= getattr(self.cfg, "fragility_threshold", Decimal("0.60")))
                        is_exh = is_exh_buy

                        penny_active = self.cfg.penny and getattr(self.cfg, "aggressive_touch", True) and not avoid_penny_war
                        touch_px = (md.bid + tick) if (penny_active and (md.ask - md.bid) > Decimal("2") * tick and not is_toxic and flow_bias >= Decimal("-0.2")) else md.bid
                        if touch_px >= mid:
                            touch_px = md.bid

                        model_px = (res_price - bid_asym_shift) * (ONE - (level_edge_buy + buy_tox_penalty) / BPS)
                        model_px = q_down(min(model_px, md.bid), tick)

                        strict_edge_floor = (self.cfg.min_edge_bps > Decimal("2.0")) or learner_active_widen
                        if is_liquid_market and pos_usd <= ZERO and buy_tox < Decimal("1.0") and sys_spr_mult <= Decimal("1.10"):
                            best_cand_px = (md.bid - tick) if (flow_bias < Decimal("-0.20") or md.obi < Decimal("-0.20")) else md.bid
                            cand_options = [best_cand_px]
                        elif strict_edge_floor:
                            cand_options = [model_px, q_down(model_px - tick, tick)]
                        else:
                            cand_options = [touch_px, md.bid - tick, model_px]
                        if getattr(self.cfg, "enable_belief_state_quoting", False) and hasattr(md, "belief_state"):
                            dom = md.belief_state.dominant_regime
                            if dom in (MarketRegime.PERSISTENT_SELL, MarketRegime.LIQUIDITY_VACUUM, MarketRegime.UNSTABLE_STALE):
                                cand_options = [model_px, q_down(model_px - tick, tick)] if strict_edge_floor else [md.bid - tick, model_px]
                        best_cand_px = cand_options[0]
                        best_ev = Decimal("-999999")
                        best_p_fill = 0.5

                        for c_px in set(cand_options):
                            c_px = min(c_px, md.bid + (tick if penny_active else ZERO))
                            if c_px >= mid:
                                c_px = md.bid
                            c_px = q_down(c_px, tick)
                            if c_px <= ZERO:
                                continue
                            c_cap = (fair_val - c_px) / fair_val * BPS
                            c_adv = adv_bps_buy
                            if is_exh:
                                c_adv = max(ZERO, c_adv - Decimal("0.8"))
                            if is_fragile and c_px >= md.bid:
                                c_adv += Decimal("2.0") * fragility

                            fee_bps = self.cfg.maker_fee_bps
                            skew_rate = l.skew_bps if l else self.cfg.skew_bps
                            inv_cost_bps = max(ZERO, (pos_usd - target_inv_usd) / self.cfg.max_position_usd) * skew_rate if pos_usd > ZERO else ZERO
                            pred_m = l.predict_markout(BUY, regime, k, self.cfg.queue_horizon_s) if (l and hasattr(l, "predict_markout")) else ZERO
                            if getattr(self.cfg, "enable_causal_impact", False):
                                pred_m, _ = self.causal_estimator.predict_causal_markout(float(level_usd_buy), float(pred_m))
                            if getattr(self.cfg, "enable_belief_state_quoting", False) and hasattr(md, "belief_state"):
                                cf_cand = self.counterfactual_engine.evaluate_candidate(
                                    pair_index=k, side=BUY, price=c_px, qty=level_usd_buy / c_px,
                                    fair_val=fair_val, mid=mid, queue_ahead_qty=md.queue_ahead(BUY, c_px),
                                    consumption_rate_qty_s=cons_buy_2s,
                                    belief=md.belief_state, pos_usd=pos_usd, target_inv_usd=target_inv_usd,
                                    max_pos_usd=self.cfg.max_position_usd, skew_rate_bps=skew_rate,
                                    maker_fee_bps=fee_bps, impact_penalty_bps=impact_pen_buy,
                                    horizon_s=self.cfg.queue_horizon_s
                                )
                                c_p = cf_cand.p_fill
                                c_ev = cf_cand.net_ev_bps - Decimal(str(c_p)) * c_adv
                            else:
                                c_p = self.queue_fill_probability(BUY, c_px, md, now, self.cfg.queue_horizon_s, ledger=ledger)
                                if getattr(self.cfg, "enable_mean_field_competition", False):
                                    c_p, c_cap, pred_m, _ = self.competition_engine.evaluate_strategic_quote(
                                        BUY, c_px, bool(c_px > md.bid), c_cap, c_p, pred_m, tick
                                    )
                                c_ev = Decimal(str(c_p)) * (c_cap + pred_m - c_adv) - fee_bps - inv_cost_bps - impact_pen_buy


                            if c_ev > best_ev:
                                best_ev = c_ev
                                best_cand_px = c_px
                                best_p_fill = c_p

                        cand_px = best_cand_px
                        p_fill = best_p_fill
                        ev_bps = best_ev
                        l0_bid_px = cand_px
                        last_bid_px = cand_px
                    elif k == 0:
                        penny_active = self.cfg.penny and getattr(self.cfg, "aggressive_touch", True) and not avoid_penny_war
                        if penny_active and (md.ask - md.bid) > Decimal("2") * tick and not is_toxic and flow_bias >= Decimal("-0.2"):
                            cand_px = md.bid + tick
                            if cand_px >= mid:
                                cand_px = md.bid
                        else:
                            cand_px = min(md.bid, (res_price - bid_asym_shift) * (ONE - (level_edge_buy + buy_tox_penalty) / BPS))
                        cand_px = min(cand_px, md.ask - tick)
                        cand_px = q_down(cand_px, tick)
                        if md.ask and cand_px >= md.ask:
                            cand_px = md.ask - tick
                        dist_bps = (md.ask - cand_px) / mid * BPS
                        p_fill = self.queue_fill_probability(BUY, cand_px, md, now, self.cfg.queue_horizon_s, ledger=ledger)
                        capture_bps = (fair_val - cand_px) / fair_val * BPS
                        adv_bps = adv_bps_buy
                        fee_bps = self.cfg.maker_fee_bps
                        skew_rate = l.skew_bps if l else self.cfg.skew_bps
                        inv_cost_bps = max(ZERO, (pos_usd - target_inv_usd) / self.cfg.max_position_usd) * skew_rate if pos_usd > ZERO else ZERO
                        pred_m = l.predict_markout(BUY, regime, 0, self.cfg.queue_horizon_s) if (l and hasattr(l, "predict_markout")) else ZERO
                        net_u_buy0 = (capture_bps + pred_m - adv_bps) - fee_bps - inv_cost_bps - impact_pen_buy
                        ev_bps = Decimal(str(p_fill)) * (capture_bps + pred_m - adv_bps) - fee_bps - inv_cost_bps - impact_pen_buy
                        if net_u_buy0 > min_ev_base:
                            ev_bps = max(ev_bps, net_u_buy0 * Decimal("0.55"))
                        l0_bid_px = cand_px
                        last_bid_px = cand_px
                    else:
                        anchor = l0_bid_px if l0_bid_px is not None else md.bid
                        spacing_px = max(tick * Decimal(str(k)), q_down(mid * k_spacing / BPS, tick))
                        cand_px = q_down(anchor - spacing_px, tick)
                        step_px = max(tick, q_down(mid * max(tick_bps, spacing) / BPS, tick))
                        if last_bid_px is not None and cand_px > last_bid_px - step_px:
                            cand_px = q_down(last_bid_px - step_px, tick)
                        if cand_px >= md.ask:
                            cand_px = md.ask - tick
                        last_bid_px = cand_px
                        dist_bps = (md.ask - cand_px) / mid * BPS
                        p_fill = self.queue_fill_probability(BUY, cand_px, md, now, self.cfg.queue_horizon_s, ledger=ledger)
                        capture_bps = (fair_val - cand_px) / fair_val * BPS
                        adv_bps = adv_bps_buy
                        fee_bps = self.cfg.maker_fee_bps
                        skew_rate = l.skew_bps if l else self.cfg.skew_bps
                        inv_cost_bps = max(ZERO, (pos_usd - target_inv_usd) / self.cfg.max_position_usd) * skew_rate if pos_usd > ZERO else ZERO
                        pred_m = l.predict_markout(BUY, regime, k, self.cfg.queue_horizon_s) if (l and hasattr(l, "predict_markout")) else ZERO
                        net_u_buy = (capture_bps + pred_m - adv_bps) - fee_bps - inv_cost_bps - impact_pen_buy
                        ev_bps = Decimal(str(p_fill)) * (capture_bps + pred_m - adv_bps) - fee_bps - inv_cost_bps - impact_pen_buy
                        if net_u_buy > min_ev_base:
                            ev_bps = max(ev_bps, net_u_buy * Decimal("0.55"))

                    if is_liquid_market:
                        cand_px = min(cand_px, md.bid)
                    else:
                        min_cap_bps = getattr(self.cfg, "guarantee_spread_capture_bps", Decimal("0.5")) if self.cfg.guarantee_spread_capture else Decimal("0.1")
                        min_dist = max(tick, q_down(mid * min_cap_bps / BPS, tick))
                        cand_px = min(cand_px, q_down(mid - min_dist, tick))

                    if cand_px > ZERO:
                        qty = q_down(level_usd_buy / cand_px, step)
                        if qty < m.min_size and (m.min_size * cand_px <= remaining_buy_usd * Decimal("1.05")):
                            qty = m.min_size
                        if qty >= m.min_size:
                            eff_min_ev_base = min(min_ev_base, spr_bps * Decimal("0.25")) if is_liquid_market else min_ev_base
                            min_ev = max(ZERO, eff_min_ev_base - self.cfg.ev_hysteresis_bps) if (existing_slots and (k, BUY) in existing_slots) else eff_min_ev_base
                            unit_net = ((abs(fair_val - cand_px) / fair_val * BPS) - (adv_bps_buy if cand_px < mid else adv_bps_sell) - self.cfg.maker_fee_bps)
                            anti_freeze_ok = (self.cfg.min_edge_bps > Decimal("2.0") and unit_net >= max(Decimal("1.2"), min_ev * Decimal("2.5")) and not is_toxic)
                            if (not self.cfg.enable_adaptive_ev) or (ev_bps >= min_ev) or anti_freeze_ok:
                                quotes.append(QuoteTarget(
                                    pair_index=k, side=BUY, price=cand_px, qty=qty,
                                    expected_value_bps=ev_bps, fill_probability=p_fill,
                                    is_exit_quote=False, quote_mid=mid
                                ))
                                remaining_buy_usd -= (qty * cand_px)

            # --- SELL SIDE --- #
            is_unwind_sell = (pos_usd > 0)
            if is_unwind_sell:
                # UNWIND LONG: Exit long with minimum profit target or breakeven shading
                if k == 0:
                    qty = q_down(abs(ledger.position), step)
                    unreal_bps = (mid - ledger.avg_cost) / ledger.avg_cost * BPS if (ledger.avg_cost and ledger.avg_cost > ZERO) else ZERO
                    pos_ratio = pos_usd / self.cfg.max_position_usd if self.cfg.max_position_usd > ZERO else ZERO

                    has_adverse_flow = has_adverse_selling
                    adv_score = max(ZERO, -tfi) * Decimal("1.5") + max(ZERO, -obi) * Decimal("1.5")
                    if ret_5s < ZERO:
                        adv_score += abs(ret_5s) * Decimal("0.5")
                    if self.cfg.enable_cross_exchange and md.cross.venues:
                        cross_velo = md.cross.cross_velocity_bps(3.0, now)
                        if cross_velo < ZERO:
                            adv_score += abs(cross_velo) * Decimal("0.5")
                    adv_score += (buy_tox / Decimal("5.0"))

                    trigger_taker = False
                    taker_why = ""
                    if self.cfg.enable_smart_inventory_mgmt:
                        if unreal_bps < -stress_loss_limit and (hold_now >= min_stress_hold_s or unreal_bps < -(stress_loss_limit * Decimal("1.6"))):
                            trigger_taker = True
                            taker_why = "stress_loss"
                        elif unreal_bps < -emerg_loss_bps and (has_adverse_flow or adv_score >= self.cfg.emergency_taker_score_threshold):
                            trigger_taker = True
                            taker_why = "emergency_loss_and_flow"
                        elif pos_ratio >= Decimal("0.80") and unreal_bps < -emerg_loss_bps and has_adverse_flow:
                            trigger_taker = True
                            taker_why = "pos_ratio_80_adverse_flow"
                        elif ret_5s <= -Decimal("3.0") and obi <= Decimal("-0.70") and tfi <= Decimal("-0.50") and unreal_bps < -emerg_loss_bps:
                            trigger_taker = True
                            taker_why = "trend_cascade"
                        elif (self.cfg.adv_obi_exit and self._adv_obi_persist("L", obi <= -self.cfg.adv_obi_thresh, now) >= self.cfg.adv_obi_secs
                              and unreal_bps < -self.cfg.adv_obi_loss_bps):
                            trigger_taker = True
                            taker_why = "adverse_obi_persist"

                    force_touch_exit = False
                    if trigger_taker and qty >= m.min_size:
                        style, style_why = self._exit_style(SELL, -unreal_bps, md, now, ledger)
                        self._log_exit_style(SELL, style, style_why, taker_why, -unreal_bps)
                        if style != "taker":
                            trigger_taker = False
                            force_touch_exit = True
                    if trigger_taker and qty >= m.min_size:
                        self._log_taker_why("SELL", taker_why, unreal_bps, emerg_loss_bps, adv_score, pos_ratio, mid, ledger)
                    if trigger_taker and md.bid and qty >= m.min_size:
                        vwap, cross_cost = self.calculate_vwap_cross_cost(SELL, qty, md)
                        slip_buffer = max(Decimal("2") * tick, q_down(md.bid * self.cfg.taker_slip_bps / BPS, tick))
                        taker_px = q_down(min(md.bid, vwap) - slip_buffer, tick)
                        est_px = max(min(md.bid, vwap), taker_px)
                        quotes.append(QuoteTarget(
                            pair_index=0, side=SELL, price=taker_px, qty=qty,
                            expected_value_bps=-cross_cost, fill_probability=1.0,
                            is_exit_quote=True, is_taker=True, quote_mid=mid, est_px=est_px
                        ))
                    else:
                        min_profit_bps = max(self.cfg.exit_min_profit_bps, Decimal("1.5"))
                        hold_time = ledger.hold_s(now)
                        max_hold = self.cfg.max_hold_s
                        if hold_time <= 15.0:
                            dynamic_min_profit_bps = min_profit_bps
                        else:
                            decay_span = max(15.0, max_hold * 0.6)
                            decay = max(Decimal("0.0"), Decimal("1.0") - Decimal(str((hold_time - 15.0) / decay_span)))
                            dynamic_min_profit_bps = max(min_profit_bps * decay, Decimal("0.6"))

                        min_profit_px = ledger.avg_cost * (ONE + dynamic_min_profit_bps / BPS) if (ledger.avg_cost and ledger.avg_cost > ZERO) else md.ask
                        if is_liquid_market or self.cfg.min_edge_bps <= Decimal("2.0") or hold_time > max_hold * 0.70:
                            breakeven_px = ledger.avg_cost * (ONE + self.cfg.maker_fee_bps / BPS) if (ledger.avg_cost and ledger.avg_cost > ZERO) else md.ask
                        else:
                            min_be_bps = max(self.cfg.maker_fee_bps, tick_bps)
                            breakeven_px = max(ledger.avg_cost + tick, q_up(ledger.avg_cost * (ONE + min_be_bps / BPS), tick)) if (ledger.avg_cost and ledger.avg_cost > ZERO) else md.ask

                        scratch_hold_thresh = min(max_hold * 0.4, 25.0)
                        is_scratch_time = (hold_time > scratch_hold_thresh)
                        extreme_sell_cascade = (self.cfg.emergency_taker_loss_bps <= Decimal("6.0") or hold_time >= min_stress_hold_s) and (tfi <= Decimal("-0.75") and obi <= Decimal("-0.70") and unreal_bps <= -Decimal("0.8"))
                        should_maker_scratch = self.cfg.enable_smart_inventory_mgmt and (force_touch_exit or has_adverse_flow or pos_ratio >= Decimal("0.60") or is_stressed or is_scratch_time or extreme_sell_cascade)

                        if should_maker_scratch:
                            # Only concede below breakeven_px on genuine stress, taker-override, or extreme cascade (never on 1.5 bps noise!)
                            if force_touch_exit or is_stressed or extreme_sell_cascade:
                                cand_px = md.ask
                                if self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and (md.ask - tick) >= breakeven_px:
                                    cand_px = md.ask - tick
                            else:
                                cand_px = max(md.ask, breakeven_px)
                                if self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and (md.ask - tick) >= breakeven_px:
                                    cand_px = md.ask - tick
                        else:
                            if md.ask >= breakeven_px:
                                cand_px = md.ask
                                if self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and (md.ask - tick) >= breakeven_px:
                                    cand_px = md.ask - tick
                            else:
                                cand_px = max(md.ask, min_profit_px)
                                if self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and (md.ask - tick) >= min_profit_px:
                                    cand_px = md.ask - tick

                        cand_px = max(cand_px, md.bid + tick)
                        cand_px = q_up(cand_px, tick)
                        if md.bid and cand_px <= md.bid:
                            cand_px = md.bid + tick
                        if is_liquid_market:
                            cand_px = max(cand_px, md.ask)
                        else:
                            min_cap = tick if should_maker_scratch else max(tick, q_up(mid * (getattr(self.cfg, "guarantee_spread_capture_bps", Decimal("0.5")) if self.cfg.guarantee_spread_capture else Decimal("0.1")) / BPS, tick))
                            cand_px = max(cand_px, q_up(mid + min_cap, tick))
                        if cand_px > ZERO and qty >= m.min_size:
                            quotes.append(QuoteTarget(
                                pair_index=0, side=SELL, price=cand_px, qty=qty,
                                expected_value_bps=Decimal("1.0"), fill_probability=0.9,
                                is_exit_quote=True, quote_mid=mid
                            ))
                            l0_ask_px = cand_px
            else:
                # ADDING SHORT: Quote as long as inventory has room and side is not blocked
                severe_buy_pressure = (md.obi >= Decimal("0.88") or (flow_bias > Decimal("0.50") and (tfi >= Decimal("0.15") or ret_5s >= tick_bps)) or (is_toxic and md.obi > Decimal("0.55")) or (has_adverse_buying and ret_5s > max(Decimal("0.5"), tick_bps)))
                toxic_extra_level = (is_toxic and k > 0)
                # INVENTORY ROTATION & ANTI-CHASING: If already short, suppress touch L0 sell!
                already_short = (-pos_usd >= self.cfg.order_usd * Decimal("0.5"))
                suppress_sell = (already_short and (k == 0 or (has_adverse_buying and ret_5s > Decimal("0.5")) or severe_buy_pressure)) or (chasing_bottom and k == 0)
                if getattr(self.cfg, "enable_onesided_touch", True) and k == 0 and is_toxic and flow_bias >= Decimal("0.40"):
                    suppress_sell = True
                can_add = (not sell_blocked) and (not severe_buy_pressure) and (not toxic_extra_level) and (not suppress_sell) and (remaining_sell_usd >= level_usd)
                if can_add:
                    if k == 0 and getattr(self.cfg, "enable_selective_touch", True):
                        fragility = fragility_sell
                        is_fragile = bool(fragility >= getattr(self.cfg, "fragility_threshold", Decimal("0.60")))
                        is_exh = is_exh_sell

                        penny_active = self.cfg.penny and getattr(self.cfg, "aggressive_touch", True) and not avoid_penny_war
                        touch_px = (md.ask - tick) if (penny_active and (md.ask - md.bid) > Decimal("2") * tick and not is_toxic and flow_bias <= Decimal("0.2")) else md.ask
                        if touch_px <= mid:
                            touch_px = md.ask

                        model_px = (res_price + ask_asym_shift) * (ONE + (level_edge_sell + sell_tox_penalty) / BPS)
                        model_px = q_up(max(model_px, md.ask), tick)

                        strict_edge_floor = (self.cfg.min_edge_bps > Decimal("2.0")) or learner_active_widen
                        if is_liquid_market and pos_usd >= ZERO and sell_tox < Decimal("1.0") and sys_spr_mult <= Decimal("1.10"):
                            best_cand_px = (md.ask + tick) if (flow_bias > Decimal("0.20") or md.obi > Decimal("0.20")) else md.ask
                            cand_options = [best_cand_px]
                        elif strict_edge_floor:
                            cand_options = [model_px, q_up(model_px + tick, tick)]
                        else:
                            cand_options = [touch_px, md.ask + tick, model_px]
                        if getattr(self.cfg, "enable_belief_state_quoting", False) and hasattr(md, "belief_state"):
                            dom = md.belief_state.dominant_regime
                            if dom in (MarketRegime.PERSISTENT_BUY, MarketRegime.LIQUIDITY_VACUUM, MarketRegime.UNSTABLE_STALE):
                                cand_options = [model_px, q_up(model_px + tick, tick)] if strict_edge_floor else [md.ask + tick, model_px]
                        best_cand_px = cand_options[0]
                        best_ev = Decimal("-999999")
                        best_p_fill = 0.5

                        for c_px in set(cand_options):
                            c_px = max(c_px, md.ask - (tick if penny_active else ZERO))
                            if c_px <= mid:
                                c_px = md.ask
                            c_px = q_up(c_px, tick)
                            if c_px <= ZERO:
                                continue
                            c_cap = (c_px - fair_val) / fair_val * BPS
                            c_adv = adv_bps_sell
                            if is_exh:
                                c_adv = max(ZERO, c_adv - Decimal("0.8"))
                            if is_fragile and c_px <= md.ask:
                                c_adv += Decimal("2.0") * fragility

                            fee_bps = self.cfg.maker_fee_bps
                            skew_rate = l.skew_bps if l else self.cfg.skew_bps
                            inv_cost_bps = max(ZERO, -(pos_usd - target_inv_usd) / self.cfg.max_position_usd) * skew_rate if pos_usd < ZERO else ZERO
                            pred_m = l.predict_markout(SELL, regime, k, self.cfg.queue_horizon_s) if (l and hasattr(l, "predict_markout")) else ZERO
                            if getattr(self.cfg, "enable_causal_impact", False):
                                pred_m, _ = self.causal_estimator.predict_causal_markout(float(level_usd_sell), float(pred_m))
                            if getattr(self.cfg, "enable_belief_state_quoting", False) and hasattr(md, "belief_state"):
                                cf_cand = self.counterfactual_engine.evaluate_candidate(
                                    pair_index=k, side=SELL, price=c_px, qty=level_usd_sell / c_px,
                                    fair_val=fair_val, mid=mid, queue_ahead_qty=md.queue_ahead(SELL, c_px),
                                    consumption_rate_qty_s=cons_sell_2s,
                                    belief=md.belief_state, pos_usd=pos_usd, target_inv_usd=target_inv_usd,
                                    max_pos_usd=self.cfg.max_position_usd, skew_rate_bps=skew_rate,
                                    maker_fee_bps=fee_bps, impact_penalty_bps=impact_pen_sell,
                                    horizon_s=self.cfg.queue_horizon_s
                                )
                                c_p = cf_cand.p_fill
                                c_ev = cf_cand.net_ev_bps - Decimal(str(c_p)) * c_adv
                            else:
                                c_p = self.queue_fill_probability(SELL, c_px, md, now, self.cfg.queue_horizon_s, ledger=ledger)
                                if getattr(self.cfg, "enable_mean_field_competition", False):
                                    c_p, c_cap, pred_m, _ = self.competition_engine.evaluate_strategic_quote(
                                        SELL, c_px, bool(c_px < md.ask), c_cap, c_p, pred_m, tick
                                    )
                                c_ev = Decimal(str(c_p)) * (c_cap + pred_m - c_adv) - fee_bps - inv_cost_bps - impact_pen_sell


                            if c_ev > best_ev:
                                best_ev = c_ev
                                best_cand_px = c_px
                                best_p_fill = c_p

                        cand_px = best_cand_px
                        p_fill = best_p_fill
                        ev_bps = best_ev
                        l0_ask_px = cand_px
                        last_ask_px = cand_px
                    elif k == 0:
                        penny_active = self.cfg.penny and getattr(self.cfg, "aggressive_touch", True) and not avoid_penny_war
                        if penny_active and (md.ask - md.bid) > Decimal("2") * tick and not is_toxic and flow_bias <= Decimal("0.2"):
                            cand_px = md.ask - tick
                            if cand_px <= mid:
                                cand_px = md.ask
                        else:
                            cand_px = max(md.ask, (res_price + ask_asym_shift) * (ONE + (level_edge_sell + sell_tox_penalty) / BPS))
                        cand_px = max(cand_px, md.bid + tick)
                        cand_px = q_up(cand_px, tick)
                        if md.bid and cand_px <= md.bid:
                            cand_px = md.bid + tick
                        dist_bps = (cand_px - md.bid) / mid * BPS
                        p_fill = self.queue_fill_probability(SELL, cand_px, md, now, self.cfg.queue_horizon_s, ledger=ledger)
                        capture_bps = (cand_px - fair_val) / fair_val * BPS
                        adv_bps = adv_bps_sell
                        fee_bps = self.cfg.maker_fee_bps
                        skew_rate = l.skew_bps if l else self.cfg.skew_bps
                        inv_cost_bps = max(ZERO, -(pos_usd - target_inv_usd) / self.cfg.max_position_usd) * skew_rate if pos_usd < ZERO else ZERO
                        pred_m = l.predict_markout(SELL, regime, 0, self.cfg.queue_horizon_s) if (l and hasattr(l, "predict_markout")) else ZERO
                        net_u_sell0 = (capture_bps + pred_m - adv_bps) - fee_bps - inv_cost_bps - impact_pen_sell
                        ev_bps = Decimal(str(p_fill)) * (capture_bps + pred_m - adv_bps) - fee_bps - inv_cost_bps - impact_pen_sell
                        if net_u_sell0 > min_ev_base:
                            ev_bps = max(ev_bps, net_u_sell0 * Decimal("0.55"))
                        l0_ask_px = cand_px
                        last_ask_px = cand_px
                    else:
                        anchor = l0_ask_px if l0_ask_px is not None else md.ask
                        spacing_px = max(tick * Decimal(str(k)), q_up(mid * k_spacing / BPS, tick))
                        cand_px = q_up(anchor + spacing_px, tick)
                        step_px = max(tick, q_up(mid * max(tick_bps, spacing) / BPS, tick))
                        if last_ask_px is not None and cand_px < last_ask_px + step_px:
                            cand_px = q_up(last_ask_px + step_px, tick)
                        if cand_px <= md.bid:
                            cand_px = md.bid + tick
                        last_ask_px = cand_px
                        dist_bps = (cand_px - md.bid) / mid * BPS
                        p_fill = self.queue_fill_probability(SELL, cand_px, md, now, self.cfg.queue_horizon_s, ledger=ledger)
                        capture_bps = (cand_px - fair_val) / fair_val * BPS
                        adv_bps = adv_bps_sell
                        fee_bps = self.cfg.maker_fee_bps
                        skew_rate = l.skew_bps if l else self.cfg.skew_bps
                        inv_cost_bps = max(ZERO, -(pos_usd - target_inv_usd) / self.cfg.max_position_usd) * skew_rate if pos_usd < ZERO else ZERO
                        pred_m = l.predict_markout(SELL, regime, k, self.cfg.queue_horizon_s) if (l and hasattr(l, "predict_markout")) else ZERO
                        net_u_sell = (capture_bps + pred_m - adv_bps) - fee_bps - inv_cost_bps - impact_pen_sell
                        ev_bps = Decimal(str(p_fill)) * (capture_bps + pred_m - adv_bps) - fee_bps - inv_cost_bps - impact_pen_sell
                        if net_u_sell > min_ev_base:
                            ev_bps = max(ev_bps, net_u_sell * Decimal("0.55"))

                    if is_liquid_market:
                        cand_px = max(cand_px, md.ask)
                    else:
                        min_cap_bps = getattr(self.cfg, "guarantee_spread_capture_bps", Decimal("0.5")) if self.cfg.guarantee_spread_capture else Decimal("0.1")
                        min_dist = max(tick, q_up(mid * min_cap_bps / BPS, tick))
                        cand_px = max(cand_px, q_up(mid + min_dist, tick))

                    if cand_px > ZERO:
                        qty = q_down(level_usd_sell / cand_px, step)
                        if qty < m.min_size and (m.min_size * cand_px <= remaining_sell_usd * Decimal("1.05")):
                            qty = m.min_size
                        if qty >= m.min_size:
                            eff_min_ev_base = min(min_ev_base, spr_bps * Decimal("0.25")) if is_liquid_market else min_ev_base
                            min_ev = max(ZERO, eff_min_ev_base - self.cfg.ev_hysteresis_bps) if (existing_slots and (k, SELL) in existing_slots) else eff_min_ev_base
                            unit_net = ((abs(fair_val - cand_px) / fair_val * BPS) - (adv_bps_buy if cand_px < mid else adv_bps_sell) - self.cfg.maker_fee_bps)
                            anti_freeze_ok = (self.cfg.min_edge_bps > Decimal("2.0") and unit_net >= max(Decimal("1.2"), min_ev * Decimal("2.5")) and not is_toxic)
                            if (not self.cfg.enable_adaptive_ev) or (ev_bps >= min_ev) or anti_freeze_ok:
                                quotes.append(QuoteTarget(
                                    pair_index=k, side=SELL, price=cand_px, qty=qty,
                                    expected_value_bps=ev_bps, fill_probability=p_fill,
                                    is_exit_quote=False, quote_mid=mid
                                ))
                                remaining_sell_usd -= (qty * cand_px)

        return quotes
