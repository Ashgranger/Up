"""Level 2 & Level 3 Conditional Fill and Markout Model & Counterfactual Execution Engine.

Implements:
1. Multi-Horizon Conditional Post-Fill Price Drift Distribution:
   E[Delta S_{t+tau} | fill at price p, side, regime, queue_pos] for tau in {0.5s, 1.0s, 2.0s, 5.0s}.
2. Distributional Tail-Risk Modeling:
   Variance, Value-at-Risk (VaR_95), and Conditional Value-at-Risk (CVaR_95 / Expected Shortfall)
   capturing asymmetric adverse fill penalty distributions.
3. Counterfactual Execution Evaluator:
   Simulates expected outcomes for alternative quote prices, sizes, and queue positions:
   Q(b_t, a) = E_{z ~ b_t} [ P(fill | a, z) * (SpreadCapture(a) + E[Markout | fill, a, z] - Fee - TailRisk)
                           - InventoryRisk(q, a, z) - MarketImpact(a) ].
4. Bayesian Shrinkage of empirical sample markouts toward regime priors.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any

from latent_state import MarketRegime, BeliefState
from utils import BPS, BUY, SELL, ZERO, ONE, clamp


@dataclass
class MarkoutStats:
    """Statistical distribution of realized markouts for a specific state conditioning."""
    n: int = 0
    mean_bps: float = 0.0
    var_bps: float = 0.0
    cvar_95_bps: float = 0.0          # Expected shortfall (mean of worst 5% adverse outcomes)
    history: deque = field(default_factory=lambda: deque(maxlen=200))

    def record(self, markout_bps: float) -> None:
        self.history.append(markout_bps)
        self.n = len(self.history)
        if self.n == 1:
            self.mean_bps = markout_bps
            self.var_bps = 0.0
            self.cvar_95_bps = min(0.0, markout_bps)
            return

        vals = list(self.history)
        self.mean_bps = sum(vals) / self.n
        variance = sum((x - self.mean_bps) ** 2 for x in vals) / (self.n - 1)
        self.var_bps = variance

        # Compute empirical CVaR (tail loss)
        sorted_vals = sorted(vals)
        k_tail = max(1, int(math.ceil(0.05 * self.n)))
        worst_tail = sorted_vals[:k_tail]
        self.cvar_95_bps = sum(worst_tail) / len(worst_tail)


@dataclass
class CounterfactualCandidate:
    """Evaluated quote candidate under alternative pricing and placement."""
    pair_index: int
    side: str
    price: Decimal
    qty: Decimal
    p_fill: float
    spread_capture_bps: Decimal
    expected_markout_bps: Decimal
    cvar_tail_risk_bps: Decimal
    fee_bps: Decimal
    inv_cost_bps: Decimal
    impact_penalty_bps: Decimal
    net_ev_bps: Decimal
    quote_mid: Decimal

    def as_dict(self) -> Dict[str, Any]:
        return {
            "pair_index": self.pair_index,
            "side": self.side,
            "price": float(self.price),
            "qty": float(self.qty),
            "p_fill": round(self.p_fill, 4),
            "spread_capture_bps": round(float(self.spread_capture_bps), 4),
            "expected_markout_bps": round(float(self.expected_markout_bps), 4),
            "cvar_tail_risk_bps": round(float(self.cvar_tail_risk_bps), 4),
            "net_ev_bps": round(float(self.net_ev_bps), 4),
        }


class ConditionalMarkoutModel:
    """Tracks and predicts conditional markouts across horizons, regimes, and queue depth."""

    def __init__(self, prior_weight: int = 5):
        self.prior_weight = prior_weight
        # Table of distributions: (side, regime_name, horizon_s) -> MarkoutStats
        self.distributions: Dict[Tuple[str, str, float], MarkoutStats] = {}

        # Prior expectations per regime (in basis points)
        # Positive = favorable post-fill price drift, Negative = adverse selection
        self.priors: Dict[Tuple[str, str], float] = {
            (BUY, MarketRegime.NORMAL_TWO_SIDED.value): 0.05,
            (SELL, MarketRegime.NORMAL_TWO_SIDED.value): 0.05,
            (BUY, MarketRegime.PERSISTENT_BUY.value): 1.50,     # Buying into an uptrend is favorable
            (SELL, MarketRegime.PERSISTENT_BUY.value): -2.50,   # Selling into an uptrend is severely toxic
            (BUY, MarketRegime.PERSISTENT_SELL.value): -2.50,   # Buying into a downtrend is severely toxic
            (SELL, MarketRegime.PERSISTENT_SELL.value): 1.50,   # Selling into a downtrend is favorable
            (BUY, MarketRegime.LIQUIDITY_VACUUM.value): -1.80,  # Vacuum has high adverse slippage
            (SELL, MarketRegime.LIQUIDITY_VACUUM.value): -1.80,
            (BUY, MarketRegime.UNSTABLE_STALE.value): -3.00,    # Stale quotes are picked off
            (SELL, MarketRegime.UNSTABLE_STALE.value): -3.00,
        }

    def record_realization(self, side: str, regime: str, horizon_s: float, markout_bps: float) -> None:
        """Records realized markout from filled orders."""
        key = (side.upper(), regime, round(horizon_s, 1))
        if key not in self.distributions:
            self.distributions[key] = MarkoutStats()
        self.distributions[key].record(markout_bps)

    def predict_markout(
        self,
        side: str,
        belief: BeliefState,
        horizon_s: float = 2.0,
    ) -> Tuple[Decimal, Decimal]:
        """Returns (expected_markout_bps, cvar_tail_risk_bps) weighted across belief state."""
        h = round(horizon_s, 1)
        tot_mean = 0.0
        tot_cvar = 0.0

        for regime, prob in belief.probabilities.items():
            if prob <= 0:
                continue
            key = (side.upper(), regime.value, h)
            stats = self.distributions.get(key)
            prior = self.priors.get((side.upper(), regime.value), 0.0)

            if stats and stats.n > 0:
                # Bayesian shrinkage: w = n / (n + prior_weight)
                w = float(stats.n) / (float(stats.n) + float(self.prior_weight))
                regime_mean = w * stats.mean_bps + (1.0 - w) * prior
                regime_cvar = w * stats.cvar_95_bps + (1.0 - w) * (prior - 1.5)
            else:
                regime_mean = prior
                regime_cvar = prior - 1.5

            tot_mean += prob * regime_mean
            tot_cvar += prob * regime_cvar

        return (
            Decimal(str(round(tot_mean, 4))),
            Decimal(str(round(tot_cvar, 4))),
        )


class CounterfactualExecutionEngine:
    """Evaluates candidate quote prices and sizes against belief-state execution dynamics."""

    def __init__(self, markout_model: ConditionalMarkoutModel):
        self.markout_model = markout_model

    def evaluate_candidate(
        self,
        pair_index: int,
        side: str,
        price: Decimal,
        qty: Decimal,
        fair_val: Decimal,
        mid: Decimal,
        queue_ahead_qty: Decimal,
        consumption_rate_qty_s: Decimal,
        belief: BeliefState,
        pos_usd: Decimal,
        target_inv_usd: Decimal,
        max_pos_usd: Decimal,
        skew_rate_bps: Decimal,
        maker_fee_bps: Decimal,
        impact_penalty_bps: Decimal = ZERO,
        tail_risk_weight: Decimal = Decimal("0.05"),
        horizon_s: float = 2.0,
    ) -> CounterfactualCandidate:
        """Evaluates counterfactual Q(b_t, a) for a candidate quote."""
        if price <= ZERO or mid <= ZERO or fair_val <= ZERO:
            return CounterfactualCandidate(
                pair_index=pair_index, side=side, price=price, qty=qty, p_fill=0.0,
                spread_capture_bps=ZERO, expected_markout_bps=ZERO, cvar_tail_risk_bps=ZERO,
                fee_bps=maker_fee_bps, inv_cost_bps=ZERO, impact_penalty_bps=ZERO,
                net_ev_bps=Decimal("-9999"), quote_mid=mid
            )

        # 1. Spread capture
        if side == BUY:
            spread_capture_bps = (fair_val - price) / fair_val * BPS
        else:
            spread_capture_bps = (price - fair_val) / fair_val * BPS

        # 2. Queue-aware counterfactual fill probability P(fill | price, queue, belief)
        # Hazard rate lambda_fill = consumption / (queue_ahead + order_size)
        order_sz = float(qty)
        ahead = float(queue_ahead_qty)
        cons = max(0.001, float(consumption_rate_qty_s))
        dist_bps = float(abs(mid - price) / mid * BPS)

        denom = max(order_sz * 0.1, ahead + order_sz)
        lambda_queue = cons / denom
        p_queue = 1.0 - math.exp(-lambda_queue * horizon_s)

        # Distance penalty based on market arrival
        kappa = 0.50
        p_dist = math.exp(-kappa * dist_bps)

        # Weight queue vs distance: near the touch, queue priority dominates; deep in book, distance dominates
        queue_weight = 0.70 if dist_bps < 1.5 else 0.35
        base_p_fill = queue_weight * p_queue + (1.0 - queue_weight) * p_dist
        if dist_bps <= 1.0:
            base_p_fill = max(0.40, base_p_fill)

        # Belief state modulation on fill probability
        # In persistent buy, buy orders fill slower (everyone is buying), sell orders fill faster
        p_buy_reg = belief.get(MarketRegime.PERSISTENT_BUY)
        p_sell_reg = belief.get(MarketRegime.PERSISTENT_SELL)
        p_vac_reg = belief.get(MarketRegime.LIQUIDITY_VACUUM)

        if side == BUY:
            p_fill = base_p_fill * (1.0 - 0.3 * p_buy_reg + 0.4 * p_sell_reg)
        else:
            p_fill = base_p_fill * (1.0 + 0.4 * p_buy_reg - 0.3 * p_sell_reg)

        if p_vac_reg > 0.3:
            p_fill *= max(0.2, 1.0 - p_vac_reg)

        p_fill = float(max(0.01, min(0.99, p_fill)))

        # 3. Expected conditional markout & CVaR tail risk
        pred_markout_bps, cvar_tail_bps = self.markout_model.predict_markout(side, belief, horizon_s)

        # 4. Inventory risk penalty: penalize the side that adds to an existing exposure
        inv_cost_bps = ZERO
        if max_pos_usd > ZERO:
            if side == BUY and pos_usd > target_inv_usd:
                inv_cost_bps = max(ZERO, (pos_usd - target_inv_usd) / max_pos_usd) * skew_rate_bps
            elif side == SELL and pos_usd < target_inv_usd:
                inv_cost_bps = max(ZERO, -(pos_usd - target_inv_usd) / max_pos_usd) * skew_rate_bps

        # 5. Net counterfactual expected value Q(b_t, a)
        # Tail risk penalty: if cvar_tail_bps is negative (losses), penalize it
        adverse_tail_pen = max(ZERO, -cvar_tail_bps) * tail_risk_weight

        net_ev = (
            Decimal(str(p_fill)) * (spread_capture_bps + pred_markout_bps - maker_fee_bps - adverse_tail_pen)
            - inv_cost_bps
            - impact_penalty_bps
        )

        return CounterfactualCandidate(
            pair_index=pair_index,
            side=side,
            price=price,
            qty=qty,
            p_fill=p_fill,
            spread_capture_bps=spread_capture_bps,
            expected_markout_bps=pred_markout_bps,
            cvar_tail_risk_bps=cvar_tail_bps,
            fee_bps=maker_fee_bps,
            inv_cost_bps=inv_cost_bps,
            impact_penalty_bps=impact_penalty_bps,
            net_ev_bps=net_ev,
            quote_mid=mid,
        )
