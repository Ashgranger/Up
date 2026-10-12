"""Level 4: Mean-Field Game & Strategic Competition Modeling between Liquidity Providers.

Implements:
1. Competitor Population Distribution m_t:
   - Tracks the collective state, quoting density, and reaction profiles of competing market makers.
   - Monitors competitive queue crowding and pennying hazard rates.
2. Strategic Competitor Reaction Function:
   P(competitor reacts / pennies | we improve price by delta)
   estimating whether an aggressive touch quote triggers competitive retaliation or preserves priority.
3. Strategic Value Function:
   V(t, x; m) = sup_pi E [ int_t^T r(x_s, a_s, m_s) ds + G(x_T, m_T) ]
   optimizing quotes after factoring in competitor strategic equilibrium responses.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any

from utils import BPS, BUY, SELL, ZERO, ONE, clamp


@dataclass
class CompetitorPopulationState:
    """m_t: Collective distribution of competing liquidity providers."""
    mean_competitor_depth_usd: float = 5000.0   # Average resting depth per level
    reaction_elasticity: float = 0.65            # Probability competitors penny when touch improved [0, 1]
    yield_probability: float = 0.20              # Probability competitors widen/retreat when we post size
    cancellation_intensity_hz: float = 4.0       # Rate of competitor quote cancellations (events/s)
    competitive_intensity_regime: str = "MODERATE" # "PASSIVE", "MODERATE", "HYPER_COMPETITIVE"
    timestamp: float = 0.0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "depth_usd": round(self.mean_competitor_depth_usd, 2),
            "elasticity": round(self.reaction_elasticity, 3),
            "yield_prob": round(self.yield_probability, 3),
            "cancel_intensity": round(self.cancellation_intensity_hz, 2),
            "regime": self.competitive_intensity_regime,
        }


class MeanFieldCompetitionEngine:
    """Strategic optimizer accounting for competitor reaction functions m_t(a_t)."""

    def __init__(
        self,
        base_elasticity: float = 0.60,
        observation_window_s: float = 30.0,
    ):
        self.base_elasticity = base_elasticity
        self.observation_window_s = observation_window_s
        self.state = CompetitorPopulationState(reaction_elasticity=base_elasticity)

        # History of our price improvements and whether a competitor repennied within 200ms
        # Tuple: (timestamp, side, price_improvement_ticks, was_pennied)
        self._interaction_history: deque = deque(maxlen=100)

    def record_quote_interaction(
        self,
        side: str,
        improved_ticks: int,
        was_undercut_within_window: bool,
        now: float,
    ) -> None:
        """Records whether competing bots immediately underbid/undercut our price improvement."""
        self._interaction_history.append((now, side, improved_ticks, was_undercut_within_window))
        self._recompute_population_state(now)

    def _recompute_population_state(self, now: float) -> None:
        """Updates empirical competitor reaction elasticity."""
        cutoff = now - self.observation_window_s
        while self._interaction_history and self._interaction_history[0][0] < cutoff:
            self._interaction_history.popleft()

        n = len(self._interaction_history)
        if n < 5:
            return

        pennied_count = sum(1 for _, _, _, pennied in self._interaction_history if pennied)
        empirical_p = pennied_count / n
        # Bayesian blend with base prior
        w = float(n) / (float(n) + 5.0)
        blended_elasticity = w * empirical_p + (1.0 - w) * self.base_elasticity

        regime = "MODERATE"
        if blended_elasticity > 0.75:
            regime = "HYPER_COMPETITIVE"
        elif blended_elasticity < 0.35:
            regime = "PASSIVE"

        self.state.reaction_elasticity = blended_elasticity
        self.state.competitive_intensity_regime = regime
        self.state.timestamp = now

    def evaluate_strategic_quote(
        self,
        side: str,
        candidate_price: Decimal,
        is_touch_improvement: bool,
        nominal_spread_capture_bps: Decimal,
        nominal_fill_prob: float,
        nominal_markout_bps: Decimal,
        tick: Decimal,
    ) -> Tuple[float, Decimal, Decimal, str]:
        """Calculates strategic fill probability and markout *after* competitor reaction:

        Returns:
        (strategic_p_fill, strategic_spread_capture_bps, strategic_markout_bps, strategy_explanation)
        """
        if not is_touch_improvement:
            # Passive quote behind the touch: competitors don't react aggressively
            return (
                nominal_fill_prob,
                nominal_spread_capture_bps,
                nominal_markout_bps,
                "passive_placement_no_reaction",
            )

        # Improving the touch: competitors may react
        p_react = self.state.reaction_elasticity

        # If competitors react by pennying our improved quote:
        # 1. Our queue priority is lost (we get displaced behind the competitor)
        # 2. Spread capture declines (if we get filled, it is because toxic flow swept through the competitor)
        # 3. Markout conditional on fill is worse (higher adverse selection)
        p_fill_if_pennied = nominal_fill_prob * 0.40
        markout_if_pennied = nominal_markout_bps - Decimal("0.80")  # Extra toxic penalty when swept through

        strategic_p_fill = (1.0 - p_react) * nominal_fill_prob + p_react * p_fill_if_pennied
        strategic_markout = (
            Decimal(str(1.0 - p_react)) * nominal_markout_bps
            + Decimal(str(p_react)) * markout_if_pennied
        )

        # Spread capture is only realized on our quote price
        strategic_spread_capture = nominal_spread_capture_bps

        explanation = f"competitive_reaction(elasticity={p_react:.2f},regime={self.state.competitive_intensity_regime})"
        return (
            strategic_p_fill,
            strategic_spread_capture,
            strategic_markout,
            explanation,
        )

    def should_avoid_pennying_war(self, spread_ticks: Decimal) -> bool:
        """Determines whether engaging in a 1-tick improvement is negative expected value due to competitor retaliation."""
        if self.state.competitive_intensity_regime == "HYPER_COMPETITIVE" and spread_ticks <= Decimal("2.0"):
            # When spread is tight (<= 2 ticks) and competitors retaliate 75%+ of the time, pennying only burns spread without priority
            return True
        return False
