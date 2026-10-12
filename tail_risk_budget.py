"""Level 4: Tail-Risk-Aware Stochastic Control and Multi-Component Risk Budget Allocator.

Implements:
1. Risk-Sensitive Stochastic Objective:
   J(pi) = E[PnL_T] - lambda_cvar * CVaR_alpha(-PnL_T) - eta_inv * E[ int_0^T q_t^2 * sigma_t^2 dt ]
   penalizing:
   * Poor average profitability
   * Losses in the worst tail of outcomes (CVaR at alpha=0.95)
   * Inventory exposure carried during volatile periods
2. Multi-Component Risk-Budget Allocator:
   Allocates total risk budget B_total across 6 interacting components:
   * B_normal: Normal market making quoting quota
   * B_inventory: Directional inventory carry quota
   * B_hedging: Cross-asset basis and hedge slippage quota
   * B_latency: Execution latency and cancel-race quota
   * B_uncertainty: Epistemic model uncertainty reserve
   * B_shock: Tail liquidity shock / gap risk reserve
3. Dynamic Budget Throttling:
   When estimated model state becomes unreliable, or latency surges,
   the quoting controller consumes less risk budget, scaling down quote sizes and tightening inventory limits.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any

from utils import BPS, BUY, SELL, ZERO, ONE, clamp


@dataclass
class RiskBudgetBreakdown:
    """Allocation and current utilization across the 6 risk dimensions."""
    total_budget_usd: float = 10.0

    # Quotas (sum to total_budget_usd)
    normal_mm_quota: float = 3.5
    inventory_carry_quota: float = 2.5
    hedging_quota: float = 1.0
    latency_quota: float = 1.0
    uncertainty_reserve: float = 1.0
    shock_reserve: float = 1.0

    # Current consumed risk (USD equivalent)
    normal_mm_consumed: float = 0.0
    inventory_carry_consumed: float = 0.0
    hedging_consumed: float = 0.0
    latency_consumed: float = 0.0
    uncertainty_consumed: float = 0.0
    shock_consumed: float = 0.0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "total_budget": round(self.total_budget_usd, 2),
            "normal_mm": f"{self.normal_mm_consumed:.2f}/{self.normal_mm_quota:.2f}",
            "inventory": f"{self.inventory_carry_consumed:.2f}/{self.inventory_carry_quota:.2f}",
            "hedging": f"{self.hedging_consumed:.2f}/{self.hedging_quota:.2f}",
            "latency": f"{self.latency_consumed:.2f}/{self.latency_quota:.2f}",
            "uncertainty": f"{self.uncertainty_consumed:.2f}/{self.uncertainty_reserve:.2f}",
            "shock": f"{self.shock_consumed:.2f}/{self.shock_reserve:.2f}",
        }


class RiskBudgetAllocator:
    """Dynamically throttles quoting quotas based on model certainty, volatility, and latency."""

    def __init__(
        self,
        total_budget_usd: float = 10.0,
        cvar_lambda: float = 0.50,
        vol_inventory_penalty_eta: float = 0.005,
    ):
        self.budget = RiskBudgetBreakdown(total_budget_usd=total_budget_usd)
        self.cvar_lambda = cvar_lambda
        self.vol_inventory_eta = vol_inventory_penalty_eta

    def evaluate_budget_utilization(
        self,
        inventory_usd: float,
        vol_bps: float,
        epistemic_uncertainty: float,
        latency_rtt_ms: float,
        is_shock_regime: bool,
    ) -> Tuple[Decimal, Decimal, str]:
        """Calculates budget consumption across all 6 dimensions.

        Returns:
        (size_budget_multiplier, max_allowed_inventory_usd, throttle_reason)
        """
        # 1. Update Consumed Risk per Bucket:
        # Inventory carry risk: q * (vol_bps / BPS)
        self.budget.inventory_carry_consumed = abs(inventory_usd) * (vol_bps / float(BPS))

        # Latency risk: increases if RTT > 30ms
        latency_ratio = max(0.0, (latency_rtt_ms - 20.0) / 50.0)
        self.budget.latency_consumed = min(self.budget.latency_quota, latency_ratio * self.budget.latency_quota)

        # Uncertainty consumption: scales directly with epistemic ambiguity
        self.budget.uncertainty_consumed = min(self.budget.uncertainty_reserve, epistemic_uncertainty * self.budget.uncertainty_reserve)

        # Shock consumption: consumed if shock/vacuum regime is active
        self.budget.shock_consumed = self.budget.shock_reserve if is_shock_regime else 0.0

        # Normal MM risk: active resting quotes
        self.budget.normal_mm_consumed = 0.50

        # 2. Dynamic Throttling:
        # If uncertainty or latency consume their buffers, take quota away from normal MM & inventory!
        unallocated_reserve_loss = (self.budget.uncertainty_consumed / max(0.1, self.budget.uncertainty_reserve)) * 0.40
        latency_loss = (self.budget.latency_consumed / max(0.1, self.budget.latency_quota)) * 0.30
        shock_loss = 0.50 if is_shock_regime else 0.0

        total_haircut = min(0.85, unallocated_reserve_loss + latency_loss + shock_loss)
        size_mult = max(0.15, 1.0 - total_haircut)

        # Directional inventory cap throttles down during stress
        max_inv_cap = 100.0 * (1.0 - 0.5 * total_haircut)

        reasons = []
        if unallocated_reserve_loss > 0.15:
            reasons.append(f"uncertainty_drain({unallocated_reserve_loss:.2f})")
        if latency_loss > 0.10:
            reasons.append(f"latency_drain({latency_loss:.2f})")
        if shock_loss > 0:
            reasons.append("liquidity_shock_drain")

        reason_str = ", ".join(reasons) if reasons else "normal_budget"
        return (
            Decimal(str(round(size_mult, 3))),
            Decimal(str(round(max_inv_cap, 2))),
            reason_str,
        )

    def compute_risk_sensitive_objective(
        self,
        expected_pnl_bps: Decimal,
        cvar_tail_loss_bps: Decimal,
        pos_usd: Decimal,
        vol_bps: Decimal,
        horizon_s: float = 2.0,
    ) -> Decimal:
        """Evaluates J(pi) = E[PnL] - lambda * CVaR_alpha(-PnL) - eta * q^2 * sigma^2 * dt."""
        pnl = expected_pnl_bps
        # cvar_tail_loss_bps is negative for losses (e.g. -2.5 bps)
        tail_penalty = max(ZERO, -cvar_tail_loss_bps) * Decimal(str(self.cvar_lambda))

        # Inventory variance penalty: eta * (q/100)^2 * (vol/5)^2 * (horizon / 2)
        inv_ratio = abs(pos_usd) / Decimal("100.0") if pos_usd != ZERO else ZERO
        vol_ratio = vol_bps / Decimal("5.0")
        horizon_ratio = Decimal(str(horizon_s / 2.0))
        inv_vol_penalty = Decimal(str(self.vol_inventory_eta)) * (inv_ratio ** 2) * (vol_ratio ** 2) * horizon_ratio * BPS

        j_objective = pnl - tail_penalty - inv_vol_penalty
        return j_objective
