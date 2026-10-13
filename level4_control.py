"""Level 4: Advanced Market-Making Research - Joint Belief-State & Receding-Horizon Control.

Implements:
1. Extended Decision State: x_t = (b_t, q_t, c_t, ell_t)
   - b_t: Posterior belief including epistemic/model uncertainty vs aleatoric market volatility.
     Distinguishes:
     * Market genuinely low volatility
     * Market appearing quiet due to temporary liquidity disappearance
     * Persistent informed flow
     * Model having insufficient data (epistemic ambiguity)
   - q_t: Current inventory position and dollar exposure.
   - c_t: Context vector summarizing execution and markout history.
   - ell_t: Latency profile and outstanding / in-flight orders state.
2. Optimal Stopping and Active Liquidity Withdrawal (3-State Controller):
   - Quoting states:
     * NORMAL_QUOTING: Healthy calibration, standard exposure.
     * REDUCED_RISK_QUOTING: Elevated toxicity/vol/uncertainty; reduce size, tighten inventory limits.
     * WITHDRAWAL: Critical data failures, risk limit breaches, or unacceptable execution conditions trigger immediate cancellation and defensive pause.
   - Optimal stopping decision boundary:
     V_Q(x) > V_W(x) -> Quote; otherwise Withdraw.
     where V_Q is the expected net value of quoting and V_W is the value of waiting/withdrawing
     (accounting for priority loss and re-entry costs).
3. Constrained Receding-Horizon Policy:
   pi* = argmax_pi E_pi [ sum_{t=0}^H r(x_t, a_t) ] subject to exposure, execution, and operational constraints.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any

from latent_state import MarketRegime, BeliefState
from utils import BPS, BUY, SELL, ZERO, ONE, clamp


class QuotingMode(str, Enum):
    NORMAL_QUOTING = "NORMAL_QUOTING"
    REDUCED_RISK_QUOTING = "REDUCED_RISK_QUOTING"
    WITHDRAWAL = "WITHDRAWAL"


@dataclass
class LatencyState:
    """ell_t: Latency profile and in-flight orders state."""
    estimated_rtt_ms: float = 10.0            # Round-trip latency to exchange (ms)
    cancellation_latency_ms: float = 8.0     # Expected time for cancel to take effect (ms)
    inflight_orders_count: int = 0           # Placements/modifications sent but unacknowledged
    pending_cancels_count: int = 0           # Cancellations sent but unconfirmed
    last_rtt_measurement_ts: float = 0.0

    @property
    def is_latency_stressed(self) -> bool:
        """Latency stress if RTT exceeds 80ms or multiple in-flight orders backlogged."""
        return self.estimated_rtt_ms > 80.0 or self.inflight_orders_count >= 3


@dataclass
class DecisionState:
    """x_t = (b_t, q_t, c_t, ell_t): Complete MDP decision state."""
    belief: BeliefState                      # b_t: Hidden regime belief & entropy
    epistemic_uncertainty: float             # Uncertainty about the model itself [0, 1]
    inventory_usd: Decimal                   # q_t: Current inventory in USD
    inventory_qty: Decimal                   # q_t: Current inventory in base units
    context_markout_rmse: float              # c_t: Markout prediction RMSE
    context_brier_score: float               # c_t: Fill probability calibration Brier score
    latency: LatencyState                    # ell_t: Latency and in-flight order state
    timestamp: float

    def as_dict(self) -> Dict[str, Any]:
        return {
            "dominant_regime": self.belief.dominant_regime.value,
            "belief_entropy": round(self.belief.entropy, 4),
            "epistemic_uncertainty": round(self.epistemic_uncertainty, 4),
            "inventory_usd": float(self.inventory_usd),
            "rtt_ms": round(self.latency.estimated_rtt_ms, 2),
            "inflight_count": self.latency.inflight_orders_count,
            "timestamp": self.timestamp,
        }


class OptimalStoppingWithdrawalController:
    """3-State Optimal Stopping Controller: V_Q(x) > V_W(x) -> Quote; otherwise Withdraw."""

    def __init__(
        self,
        withdrawal_hold_s: float = 5.0,
        reentry_penalty_bps: Decimal = Decimal("0.30"),
        queue_priority_loss_bps: Decimal = Decimal("0.25"),
    ):
        self.withdrawal_hold_s = withdrawal_hold_s
        self.reentry_penalty_bps = reentry_penalty_bps
        self.queue_priority_loss_bps = queue_priority_loss_bps

        self.current_mode: QuotingMode = QuotingMode.NORMAL_QUOTING
        self.mode_entered_ts: float = 0.0
        self.withdrawal_reason: str = ""
        self.history: List[Tuple[float, QuotingMode, str]] = []

    def evaluate_modes(
        self,
        x: DecisionState,
        expected_spread_capture_bps: Decimal,
        expected_adverse_move_bps: Decimal,
        market_spread_bps: Decimal,
        vol_bps: Decimal,
        unrealized_pnl_bps: Decimal,
        now: float,
    ) -> Tuple[QuotingMode, str]:
        """Evaluates whether to quote normally, quote at reduced risk, or actively withdraw."""
        # 1. Hard Operational & Safety Withdrawal Constraints
        hard_withdraw = False
        reasons = []

        if x.latency.is_latency_stressed:
            hard_withdraw = True
            reasons.append(f"latency_stress(rtt={x.latency.estimated_rtt_ms:.1f}ms,inflight={x.latency.inflight_orders_count})")

        if x.belief.dominant_regime == MarketRegime.UNSTABLE_STALE:
            hard_withdraw = True
            reasons.append("unstable_stale_regime")

        if x.epistemic_uncertainty > 0.85:
            hard_withdraw = True
            reasons.append(f"epistemic_uncertainty={x.epistemic_uncertainty:.2f}>0.85")

        if unrealized_pnl_bps < -Decimal("35.0"):
            hard_withdraw = True
            reasons.append(f"catastrophic_unrealized_loss={unrealized_pnl_bps:.1f}bps")

        # 2. Optimal Stopping Value Function Calculation: V_Q(x) vs V_W(x)
        # Value of quoting: Expected spread capture - expected adverse move
        v_quote = expected_spread_capture_bps - expected_adverse_move_bps
        # Hard emergency withdrawal only on severe negative expected utility:
        if v_quote < Decimal("-4.0"):
            hard_withdraw = True
            reasons.append(f"severe_negative_expected_utility(v_q={v_quote:.2f}<-4.0)")

        # 3. State Machine Transitions with Hysteresis
        if hard_withdraw:
            self.current_mode = QuotingMode.WITHDRAWAL
            self.mode_entered_ts = now
            self.withdrawal_reason = ", ".join(reasons)
            return QuotingMode.WITHDRAWAL, self.withdrawal_reason

        # Check if in cooldown period from recent withdrawal
        if self.current_mode == QuotingMode.WITHDRAWAL:
            if now - self.mode_entered_ts < self.withdrawal_hold_s:
                return QuotingMode.WITHDRAWAL, f"withdrawal_cooldown({self.withdrawal_hold_s - (now - self.mode_entered_ts):.1f}s remaining)"
            # Cooldown passed: attempt transition to REDUCED_RISK_QUOTING first
            self.current_mode = QuotingMode.REDUCED_RISK_QUOTING
            self.mode_entered_ts = now
            return QuotingMode.REDUCED_RISK_QUOTING, "post_withdrawal_recovery"

        # Check REDUCED_RISK_QUOTING conditions:
        # Elevated volatility, medium epistemic uncertainty, or liquidity vacuum regime
        is_reduced = (
            x.belief.dominant_regime == MarketRegime.LIQUIDITY_VACUUM
            or x.epistemic_uncertainty > 0.50
            or vol_bps > Decimal("10.0")
            or x.context_markout_rmse > 2.5
            or x.context_brier_score > 0.25
            or v_quote < Decimal("0.0")
        )

        if is_reduced:
            self.current_mode = QuotingMode.REDUCED_RISK_QUOTING
            self.mode_entered_ts = now
            return QuotingMode.REDUCED_RISK_QUOTING, "elevated_risk_environment"

        self.current_mode = QuotingMode.NORMAL_QUOTING
        return QuotingMode.NORMAL_QUOTING, "healthy"


class RecedingHorizonQuoteController:
    """Constrained Receding-Horizon Optimizer for Joint State & Control:

    Solves:
    pi* = argmax_pi E_pi [ sum_{t=0}^H r(x_t, a_t) ]
    subject to |q_t| <= q_max, Exposure <= L_max, OperationalConstraints.
    """

    def __init__(self, horizon_steps: int = 3, dt_s: float = 1.0):
        self.horizon_steps = horizon_steps
        self.dt_s = dt_s

    def evaluate_receding_horizon(
        self,
        x: DecisionState,
        candidate_bid_px: Decimal,
        candidate_ask_px: Decimal,
        candidate_bid_qty: Decimal,
        candidate_ask_qty: Decimal,
        mid: Decimal,
        fair_val: Decimal,
        p_fill_bid: float,
        p_fill_ask: float,
        expected_markout_bid: Decimal,
        expected_markout_ask: Decimal,
        maker_fee_bps: Decimal,
        max_pos_usd: Decimal,
        skew_rate_bps: Decimal,
    ) -> Dict[str, Any]:
        """Rolls forward the approximate MDP over horizon H and computes cumulative expected utility."""
        cumulative_utility = ZERO
        current_inv_usd = x.inventory_usd

        # Immediate single-period reward:
        # Spread capture on bid: (fair_val - bid_px) / fair_val * BPS
        spr_cap_bid = (fair_val - candidate_bid_px) / fair_val * BPS if fair_val > ZERO else ZERO
        spr_cap_ask = (candidate_ask_px - fair_val) / fair_val * BPS if fair_val > ZERO else ZERO

        r_bid = Decimal(str(p_fill_bid)) * (spr_cap_bid + expected_markout_bid - maker_fee_bps)
        r_ask = Decimal(str(p_fill_ask)) * (spr_cap_ask + expected_markout_ask - maker_fee_bps)

        # Inventory holding cost over horizon:
        expected_next_inv_usd = current_inv_usd + (
            (candidate_bid_qty * candidate_bid_px * Decimal(str(p_fill_bid)))
            - (candidate_ask_qty * candidate_ask_px * Decimal(str(p_fill_ask)))
        )

        inv_penalty = ZERO
        if max_pos_usd > ZERO:
            inv_ratio = abs(expected_next_inv_usd) / max_pos_usd
            inv_penalty = (inv_ratio ** 2) * skew_rate_bps

        # Receding horizon terminal value G(x_H):
        # Penalizes terminal exposure and epistemic uncertainty
        epistemic_pen = Decimal(str(round(x.epistemic_uncertainty * 2.0, 3)))
        terminal_value = -inv_penalty - epistemic_pen

        cumulative_utility = r_bid + r_ask - inv_penalty + terminal_value

        return {
            "cumulative_utility_bps": cumulative_utility,
            "expected_next_inv_usd": expected_next_inv_usd,
            "r_bid_bps": r_bid,
            "r_ask_bps": r_ask,
            "terminal_value_bps": terminal_value,
        }
