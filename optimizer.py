"""Level 2 & Level 3 Constrained Stochastic Optimizer, Robust Control, and Cross-Asset Hedger.

Implements:
1. Endogenous Market Impact Response Kernel:
   dS_t = alpha_t dt + sigma_t dW_t + int_0^t K(t-s) u_s ds dt
   where K(s) = eta_0 * exp(-gamma * s) is the transient impact kernel with exponential decay.
2. Cross-Asset Hedging and Portfolio Risk Optimizer:
   q = [q_primary, q_hedge],  R(q) = q^T * Sigma * q
   max_{a, h} E[Delta W] - lambda_risk * q^T * Sigma * q - C_hedge(h)
   accounting for basis risk, correlation rho, funding rate differentials, and execution costs.
3. Distributionally Robust Stochastic Optimizer & Optimal Quote Surface:
   (p_b*, p_a*, v_b*, v_a*) = f(q, alpha, sigma, depth, queue, impact, uncertainty)
   solving max_a min_{P in U} E_{P} [ Q(b_t, a) ] under ambiguity set U.
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any

from latent_state import MarketRegime, BeliefState
from utils import BPS, BUY, SELL, ZERO, ONE, clamp


@dataclass
class MarketImpactKernel:
    """Transient & permanent market impact tracking with exponential decay kernel."""
    eta_0_bps_per_usd: float = 0.0002   # Instantaneous impact coefficient (bps / USD)
    gamma_decay_s: float = 0.15         # Decay rate (half-life ~ 4.6 seconds)
    eta_perm_bps_per_usd: float = 0.00005 # Permanent impact coefficient
    trades: deque = field(default_factory=lambda: deque(maxlen=200)) # (timestamp, signed_usd)

    def record_fill(self, side: str, qty_usd: float, now: float) -> None:
        """Records a fill executed by the market maker."""
        signed = qty_usd if side.upper() == BUY else -qty_usd
        self.trades.append((now, signed))

    def current_cumulative_impact_bps(self, now: float) -> float:
        """Computes int_0^t K(t-s) u_s ds in basis points."""
        impact = 0.0
        while self.trades and (now - self.trades[0][0] > 60.0):
            self.trades.popleft()

        for ts, signed_usd in self.trades:
            dt = max(0.0, now - ts)
            decay = math.exp(-self.gamma_decay_s * dt)
            impact += self.eta_0_bps_per_usd * signed_usd * decay

        return impact

    def predict_impact_penalty(self, side: str, order_usd: float, now: float) -> Decimal:
        """Computes impact penalty on quoting or aggressive trading."""
        cum_impact = self.current_cumulative_impact_bps(now)
        instant_impact = self.eta_0_bps_per_usd * order_usd

        # If quoting BUY when cumulative impact is already positive (we bought a lot),
        # our buying pushes price higher, increasing entry cost
        if side.upper() == BUY:
            total_impact = max(0.0, instant_impact + max(0.0, cum_impact))
        else:
            total_impact = max(0.0, instant_impact + max(0.0, -cum_impact))

        return Decimal(str(round(total_impact, 4)))


@dataclass
class CrossAssetPosition:
    primary_usd: float = 0.0
    hedge_usd: float = 0.0


class CrossAssetHedger:
    """Joint portfolio risk management and optimal hedge rebalancing engine."""

    def __init__(
        self,
        risk_aversion_lambda: float = 0.001,
        sigma_primary_bps: float = 8.0,
        sigma_hedge_bps: float = 7.5,
        correlation_rho: float = 0.95,
        hedge_taker_fee_bps: float = 2.0,
        hedge_slippage_bps: float = 0.5,
    ):
        self.risk_aversion_lambda = risk_aversion_lambda
        self.sigma_primary = sigma_primary_bps
        self.sigma_hedge = sigma_hedge_bps
        self.rho = correlation_rho
        self.hedge_taker_fee = hedge_taker_fee_bps
        self.hedge_slippage = hedge_slippage_bps

    def update_correlation(self, rho: float, sigma_primary: float, sigma_hedge: float) -> None:
        self.rho = max(-1.0, min(1.0, rho))
        self.sigma_primary = max(0.1, sigma_primary)
        self.sigma_hedge = max(0.1, sigma_hedge)

    def portfolio_variance(self, q_primary_usd: float, q_hedge_usd: float) -> float:
        """Computes R(q) = q^T Sigma q."""
        var1 = (q_primary_usd * self.sigma_primary) ** 2
        var2 = (q_hedge_usd * self.sigma_hedge) ** 2
        cov = 2.0 * q_primary_usd * q_hedge_usd * self.rho * self.sigma_primary * self.sigma_hedge
        return var1 + var2 + cov

    def evaluate_optimal_hedge(
        self,
        q_primary_usd: float,
        current_hedge_usd: float,
        min_rebalance_threshold_usd: float = 20.0,
    ) -> Tuple[float, float, str]:
        """Calculates optimal hedge trade h* minimizing risk penalty plus execution cost.

        Analytical minimum of:
        lambda * [ (q_p*s1)^2 + ((q_h + h)*s2)^2 + 2*q_p*(q_h + h)*rho*s1*s2 ] + C_hedge(h)
        Ignoring friction: h_ideal = -q_h - rho * (s1/s2) * q_p.
        """
        if abs(q_primary_usd) < 5.0 and abs(current_hedge_usd) < 5.0:
            return 0.0, 0.0, "flat"

        beta = self.rho * (self.sigma_primary / self.sigma_hedge)
        target_hedge_usd = -beta * q_primary_usd
        desired_h = target_hedge_usd - current_hedge_usd

        if abs(desired_h) < min_rebalance_threshold_usd:
            return 0.0, self.portfolio_variance(q_primary_usd, current_hedge_usd), "within_band"

        # Compare risk reduction vs cost of hedging
        current_var = self.portfolio_variance(q_primary_usd, current_hedge_usd)
        new_var = self.portfolio_variance(q_primary_usd, current_hedge_usd + desired_h)

        risk_reduction_usd = self.risk_aversion_lambda * (current_var - new_var) / float(BPS)
        hedge_cost_usd = abs(desired_h) * (self.hedge_taker_fee + self.hedge_slippage) / float(BPS)

        if risk_reduction_usd > hedge_cost_usd:
            return desired_h, new_var, "hedge_recommended"
        else:
            return 0.0, current_var, "hedge_too_expensive"


class RobustQuoteOptimizer:
    """Computes the optimal quote surface under distributionally robust minimax control."""

    def __init__(
        self,
        impact_kernel: MarketImpactKernel,
        hedger: CrossAssetHedger,
        robust_epsilon: float = 0.20,     # Size of distributional ambiguity ball
    ):
        self.impact_kernel = impact_kernel
        self.hedger = hedger
        self.robust_epsilon = robust_epsilon

    def compute_optimal_surface(
        self,
        mid: Decimal,
        fair_val: Decimal,
        pos_usd: Decimal,
        max_pos_usd: Decimal,
        vol_bps: Decimal,
        belief: BeliefState,
        base_edge_bps: Decimal,
        skew_rate_bps: Decimal,
        order_usd: Decimal,
        tick: Decimal,
        step: Decimal,
        now: float,
    ) -> Dict[str, Any]:
        """Calculates optimal quote surface (p_b*, p_a*, v_b*, v_a*) mapping state to quotes.

        Solves distributionally robust optimization:
        max_a min_{P in U} E_P [ Q(b_t, a) ]
        where ambiguity set U distorts nominal belief toward adverse states (Vacuum/Trend).
        """
        # 1. Distort belief distribution toward worst-case (minimax robust step)
        eps = self.robust_epsilon
        worst_regime = MarketRegime.LIQUIDITY_VACUUM
        if pos_usd > ZERO:
            # Holding long inventory: worst case is sudden downward trend or vacuum
            worst_regime = MarketRegime.PERSISTENT_SELL
        elif pos_usd < ZERO:
            # Holding short inventory: worst case is sudden upward trend or vacuum
            worst_regime = MarketRegime.PERSISTENT_BUY

        # Worst-case belief
        rob_probs = dict(belief.probabilities)
        for r in rob_probs:
            rob_probs[r] = (1.0 - eps) * rob_probs[r]
        rob_probs[worst_regime] = rob_probs.get(worst_regime, 0.0) + eps

        rob_belief = BeliefState(probabilities=rob_probs, timestamp=now)

        # 2. Cumulative Market Impact Concession
        cum_impact_bps = Decimal(str(round(self.impact_kernel.current_cumulative_impact_bps(now), 4)))

        # 3. Model Uncertainty and Entropy Scaling
        unc = belief.uncertainty
        uncertainty_spread_widen = Decimal(str(round(1.0 + 1.2 * unc, 3)))
        uncertainty_size_dampen = Decimal(str(round(max(0.2, 1.0 - 0.6 * unc), 3)))

        # 4. Inventory Skew (Avellaneda-Stoikov / Guéant-Tapia style)
        inv_ratio = (pos_usd / max_pos_usd) if max_pos_usd > ZERO else ZERO
        inv_ratio = clamp(inv_ratio, Decimal("-1.0"), Decimal("1.0"))
        # Inventory penalty shifts reservation price
        gamma_risk = Decimal("0.30")
        inv_skew_bps = inv_ratio * skew_rate_bps * (ONE + gamma_risk * (vol_bps / Decimal("5.0")))

        # Reservation price
        res_price = fair_val - (mid * inv_skew_bps / BPS)
        # Apply market impact offset
        res_price -= (mid * cum_impact_bps / (BPS * Decimal("2.0")))

        # 5. Optimal Half-Spreads
        base_half_spread = (base_edge_bps + Decimal("0.3") * vol_bps) * uncertainty_spread_widen

        # Asymmetric half spreads depending on robust belief
        p_buy_toxic = Decimal(str(round(rob_belief.get(MarketRegime.PERSISTENT_BUY), 3)))
        p_sell_toxic = Decimal(str(round(rob_belief.get(MarketRegime.PERSISTENT_SELL), 3)))

        # If buyers dominate, widen ask quote aggressively to avoid getting picked off
        half_spread_ask = base_half_spread + (p_buy_toxic * Decimal("2.5") * vol_bps)
        # If sellers dominate, widen bid quote aggressively to avoid getting picked off
        half_spread_bid = base_half_spread + (p_sell_toxic * Decimal("2.5") * vol_bps)

        opt_bid_px = res_price * (ONE - half_spread_bid / BPS)
        opt_ask_px = res_price * (ONE + half_spread_ask / BPS)

        # 6. Optimal Sizes
        base_qty = order_usd / mid if mid > ZERO else Decimal("0.001")
        # Bid size scales down when holding long inventory or under uncertainty
        bid_inv_mult = max(Decimal("0.1"), ONE - inv_ratio) if inv_ratio > ZERO else ONE
        ask_inv_mult = max(Decimal("0.1"), ONE + inv_ratio) if inv_ratio < ZERO else ONE

        opt_bid_qty = base_qty * bid_inv_mult * uncertainty_size_dampen
        opt_ask_qty = base_qty * ask_inv_mult * uncertainty_size_dampen

        # 7. Check Cross-Asset Hedge recommendation
        hedge_h, p_var, hedge_status = self.hedger.evaluate_optimal_hedge(float(pos_usd), 0.0)

        return {
            "res_price": res_price,
            "opt_bid_px": opt_bid_px,
            "opt_ask_px": opt_ask_px,
            "opt_bid_qty": opt_bid_qty,
            "opt_ask_qty": opt_ask_qty,
            "cum_impact_bps": cum_impact_bps,
            "uncertainty": unc,
            "hedge_action_usd": hedge_h,
            "hedge_status": hedge_status,
            "robust_belief": rob_belief.as_dict(),
        }
