"""Level 2 & Level 3 Meta-Learning, Context Vector, and Hierarchical Safety Controller.

Implements:
1. Meta-Learning Context Vector:
   c_t = g(o_{t-n:t}, f_{t-n:t}, m_{t-n:t})
   summarizing recent market observations (o), execution outcomes (f), and markouts (m).
2. Hierarchical Strategy Structure:
   - Layer 1: Global baseline model (prior parameters)
   - Layer 2: Regime-specific parameter modulation (specialized per HMM state)
   - Layer 3: Online calibration layer (updates fill probability and markout models)
   - Layer 4: Safety Controller & Calibration Degradation Guard
     (reduces exposure and widens spreads when calibration deteriorates or prediction errors spike).
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
class ContextVector:
    """Context summary c_t = g(o_{t-n:t}, f_{t-n:t}, m_{t-n:t})."""
    rolling_vol_bps: float = 5.0
    rolling_spread_bps: float = 1.0
    rolling_fill_rate_per_min: float = 0.0
    rolling_adverse_fraction: float = 0.0      # Fraction of recent fills that were toxic
    mean_markout_1s_bps: float = 0.0
    fill_prob_brier_score: float = 0.10        # Mean squared error of predicted P(fill) vs actual
    markout_rmse_bps: float = 0.50             # RMSE of markout predictions
    consecutive_toxic_fills: int = 0
    calibration_status: str = "HEALTHY"        # "HEALTHY", "WARNING", "DEGRADED"
    timestamp: float = 0.0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "vol_bps": round(self.rolling_vol_bps, 2),
            "spread_bps": round(self.rolling_spread_bps, 2),
            "fill_rate_pm": round(self.rolling_fill_rate_per_min, 2),
            "adverse_frac": round(self.rolling_adverse_fraction, 3),
            "mean_markout": round(self.mean_markout_1s_bps, 2),
            "brier_score": round(self.fill_prob_brier_score, 4),
            "markout_rmse": round(self.markout_rmse_bps, 3),
            "toxic_streak": self.consecutive_toxic_fills,
            "calibration_status": self.calibration_status,
        }


class HierarchicalSafetyController:
    """Layer 4 Safety Controller: detects calibration degradation and imposes hard guardrails."""

    def __init__(
        self,
        brier_threshold: float = 0.35,
        markout_rmse_threshold_bps: float = 3.5,
        max_toxic_streak: int = 3,
    ):
        self.brier_threshold = brier_threshold
        self.markout_rmse_threshold = markout_rmse_threshold_bps
        self.max_toxic_streak = max_toxic_streak

        self.in_degraded_mode: bool = False
        self.degraded_until_ts: float = 0.0
        self.degraded_reason: str = ""

    def evaluate_safety(self, context: ContextVector, now: float) -> Tuple[bool, Decimal, Decimal, str]:
        """Returns (is_degraded, spread_safety_multiplier, size_safety_haircut, reason).

        - spread_safety_multiplier >= 1.0 (widens quotes to protect against model uncertainty)
        - size_safety_haircut <= 1.0 (scales down order sizes)
        """
        # Check trigger conditions
        reasons = []
        if context.fill_prob_brier_score > self.brier_threshold:
            reasons.append(f"brier_score={context.fill_prob_brier_score:.3f}>{self.brier_threshold}")
        if context.markout_rmse_bps > self.markout_rmse_threshold:
            reasons.append(f"markout_rmse={context.markout_rmse_bps:.2f}bps>{self.markout_rmse_threshold}")
        if context.consecutive_toxic_fills >= self.max_toxic_streak:
            reasons.append(f"toxic_streak={context.consecutive_toxic_fills}>={self.max_toxic_streak}")

        if reasons:
            self.in_degraded_mode = True
            self.degraded_until_ts = now + 15.0  # 15-second safety cooldown
            self.degraded_reason = ", ".join(reasons)
        elif now >= self.degraded_until_ts:
            self.in_degraded_mode = False
            self.degraded_reason = ""

        if self.in_degraded_mode:
            # Under degraded calibration: widen spread by 1.8x, cut size to 0.30x
            return True, Decimal("1.80"), Decimal("0.30"), self.degraded_reason

        return False, ONE, ONE, "calibrated"


class HierarchicalMetaLearner:
    """Manages the 4-layer hierarchical adaptation architecture."""

    def __init__(self, cfg=None):
        self.cfg = cfg
        self.safety_controller = HierarchicalSafetyController()
        self.context = ContextVector()

        # Observation tracking buffers (window ~ 60s)
        self._obs_window_s = 60.0
        self._fills_log: deque = deque(maxlen=200)       # (ts, side, price, qty, is_toxic)
        self._predictions_log: deque = deque(maxlen=200) # (ts, pred_p_fill, actual_filled)
        self._markouts_log: deque = deque(maxlen=200)    # (ts, pred_markout, actual_markout)
        self._last_context_update_ts = 0.0

        # Layer 1: Global base parameters
        self.global_params: Dict[str, float] = {
            "min_edge_bps": 1.5,
            "max_edge_bps": 8.0,
            "skew_bps": 0.8,
            "vol_k": 0.4,
            "tox_mult": 1.0,
            "kappa_fill": 0.50,
        }

        # Layer 2: Regime-specific parameter profiles
        self.regime_params: Dict[MarketRegime, Dict[str, float]] = {
            MarketRegime.NORMAL_TWO_SIDED: {
                "spread_mult": 1.0,
                "size_mult": 1.0,
                "skew_mult": 1.0,
                "pull_touch": 0.0,
            },
            MarketRegime.PERSISTENT_BUY: {
                "spread_mult": 1.25,
                "size_mult": 0.85,
                "skew_mult": 1.4,
                "pull_touch": 1.0,      # Suppress/retreat ask touch
            },
            MarketRegime.PERSISTENT_SELL: {
                "spread_mult": 1.25,
                "size_mult": 0.85,
                "skew_mult": 1.4,
                "pull_touch": 1.0,      # Suppress/retreat bid touch
            },
            MarketRegime.LIQUIDITY_VACUUM: {
                "spread_mult": 2.20,
                "size_mult": 0.35,
                "skew_mult": 1.8,
                "pull_touch": 1.0,
            },
            MarketRegime.UNSTABLE_STALE: {
                "spread_mult": 2.80,
                "size_mult": 0.15,
                "skew_mult": 2.0,
                "pull_touch": 1.0,
            },
        }

    def record_fill_prediction(self, pred_p_fill: float, did_fill: bool, now: float) -> None:
        """Records quote fill prediction vs outcome for Brier score calibration tracking."""
        self._predictions_log.append((now, pred_p_fill, 1.0 if did_fill else 0.0))

    def record_markout_prediction(self, pred_markout_bps: float, actual_markout_bps: float, now: float) -> None:
        """Records conditional markout prediction vs realization for RMSE calibration tracking."""
        self._markouts_log.append((now, pred_markout_bps, actual_markout_bps))

    def record_fill(self, side: str, price: float, qty_usd: float, is_toxic: bool, now: float) -> None:
        """Records fill event."""
        self._fills_log.append((now, side, price, qty_usd, is_toxic))

    def update_context(
        self,
        current_vol_bps: float,
        current_spread_bps: float,
        now: float,
    ) -> ContextVector:
        """Computes current context vector c_t = g(o, f, m)."""
        # Prune old records
        cutoff = now - self._obs_window_s
        while self._fills_log and self._fills_log[0][0] < cutoff:
            self._fills_log.popleft()
        while self._predictions_log and self._predictions_log[0][0] < cutoff:
            self._predictions_log.popleft()
        while self._markouts_log and self._markouts_log[0][0] < cutoff:
            self._markouts_log.popleft()

        # Fill metrics
        n_fills = len(self._fills_log)
        fill_rate_pm = (n_fills / (self._obs_window_s / 60.0))
        n_toxic = sum(1 for _, _, _, _, tox in self._fills_log if tox)
        adverse_frac = (n_toxic / n_fills) if n_fills > 0 else 0.0

        # Consecutive toxic fills
        toxic_streak = 0
        for _, _, _, _, tox in reversed(self._fills_log):
            if tox:
                toxic_streak += 1
            else:
                break

        # Brier score for fill probability: mean((pred - actual)^2)
        if self._predictions_log:
            brier = sum((p - a) ** 2 for _, p, a in self._predictions_log) / len(self._predictions_log)
        else:
            brier = 0.08  # Default baseline

        # Markout RMSE: sqrt(mean((pred - actual)^2))
        if self._markouts_log:
            mse = sum((p - a) ** 2 for _, p, a in self._markouts_log) / len(self._markouts_log)
            rmse = math.sqrt(mse)
            mean_m = sum(a for _, _, a in self._markouts_log) / len(self._markouts_log)
        else:
            rmse = 0.40
            mean_m = 0.0

        cal_status = "HEALTHY"
        if brier > 0.35 or rmse > 3.5 or toxic_streak >= 3:
            cal_status = "DEGRADED"
        elif brier > 0.25 or rmse > 2.0 or toxic_streak >= 2:
            cal_status = "WARNING"

        self.context = ContextVector(
            rolling_vol_bps=current_vol_bps,
            rolling_spread_bps=current_spread_bps,
            rolling_fill_rate_per_min=fill_rate_pm,
            rolling_adverse_fraction=adverse_frac,
            mean_markout_1s_bps=mean_m,
            fill_prob_brier_score=brier,
            markout_rmse_bps=rmse,
            consecutive_toxic_fills=toxic_streak,
            calibration_status=cal_status,
            timestamp=now,
        )
        return self.context

    def get_hierarchical_adjustments(
        self,
        belief: BeliefState,
        now: float,
    ) -> Tuple[Decimal, Decimal, bool, str]:
        """Synthesizes all 4 layers into final quote modifiers:

        Returns:
        (composite_spread_mult, composite_size_mult, is_safety_degraded, status_str)
        """
        # Layer 1 + 2: Belief-weighted regime parameters
        regime_spr_mult = 0.0
        regime_sz_mult = 0.0
        for r, p in belief.probabilities.items():
            params = self.regime_params.get(r, self.regime_params[MarketRegime.NORMAL_TWO_SIDED])
            regime_spr_mult += p * params["spread_mult"]
            regime_sz_mult += p * params["size_mult"]

        # Layer 4: Safety Controller hard limits
        is_deg, safety_spr, safety_sz, reason = self.safety_controller.evaluate_safety(self.context, now)

        final_spr = Decimal(str(round(regime_spr_mult * float(safety_spr), 3)))
        final_sz = Decimal(str(round(regime_sz_mult * float(safety_sz), 3)))

        return final_spr, final_sz, is_deg, reason
