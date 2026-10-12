"""Level 2 & Level 3 Latent-State and Belief-State Estimator.

Implements:
1. Discrete Market Regime Enumeration (5 regimes: Normal, Persistent Buy, Persistent Sell,
   Liquidity Vacuum, Unstable/Stale).
2. Continuous Latent State Vector: z_t = (alpha_t, lambda_t, nu_t, theta_t)
   - alpha_t: short-horizon fair-value drift rate (bps/s)
   - lambda_t: arrival intensity of marketable orders (buy and sell intensities)
   - nu_t: volatility / liquidity-regime state
   - theta_t: adverse-selection / toxicity state (bps)
3. Recursive Bayesian Hidden Markov Model (HMM) Filter for Belief Distribution:
   b_t(z) = P(z_t = z | o_{1:t}, a_{1:t-1})
4. Shannon Entropy and Model Uncertainty Metrics: H(b_t) in [0, 1].
5. Regime-Conditioned Asymmetric Fair-Value and Quote Adjustment Functions:
   ensuring the same observed imbalance produces differentiated, regime-optimal quote responses.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any

from utils import BPS, BUY, SELL, ZERO, ONE, clamp


class MarketRegime(str, Enum):
    NORMAL_TWO_SIDED = "REGIME_NORMAL_TWO_SIDED"
    PERSISTENT_BUY = "REGIME_PERSISTENT_BUY"
    PERSISTENT_SELL = "REGIME_PERSISTENT_SELL"
    LIQUIDITY_VACUUM = "REGIME_LIQUIDITY_VACUUM"
    UNSTABLE_STALE = "REGIME_UNSTABLE_STALE"


REGIME_NAMES = [
    MarketRegime.NORMAL_TWO_SIDED,
    MarketRegime.PERSISTENT_BUY,
    MarketRegime.PERSISTENT_SELL,
    MarketRegime.LIQUIDITY_VACUUM,
    MarketRegime.UNSTABLE_STALE,
]
NUM_REGIMES = len(REGIME_NAMES)


@dataclass
class LatentState:
    """Continuous latent state z_t = (alpha_t, lambda_t, nu_t, theta_t)."""
    alpha_bps_s: float = 0.0          # Short-horizon fair-value drift (bps/s)
    lambda_buy: float = 1.0           # Arrival intensity of aggressive buy orders (events/s)
    lambda_sell: float = 1.0          # Arrival intensity of aggressive sell orders (events/s)
    nu_vol_bps: float = 5.0           # Volatility / liquidity-regime state (bps)
    theta_buy_bps: float = 1.0        # Adverse-selection / toxicity conditional on buy fill (bps)
    theta_sell_bps: float = 1.0       # Adverse-selection / toxicity conditional on sell fill (bps)
    timestamp: float = 0.0

    def as_dict(self) -> Dict[str, float]:
        return {
            "alpha_bps_s": self.alpha_bps_s,
            "lambda_buy": self.lambda_buy,
            "lambda_sell": self.lambda_sell,
            "nu_vol_bps": self.nu_vol_bps,
            "theta_buy_bps": self.theta_buy_bps,
            "theta_sell_bps": self.theta_sell_bps,
            "timestamp": self.timestamp,
        }


@dataclass
class BeliefState:
    """Discrete belief distribution over hidden market regimes b_t(R_k) = P(R_k | o_{1:t})."""
    probabilities: Dict[MarketRegime, float] = field(default_factory=dict)
    entropy: float = 0.0              # Normalized Shannon entropy in [0, 1]
    dominant_regime: MarketRegime = MarketRegime.NORMAL_TWO_SIDED
    dominant_probability: float = 1.0
    timestamp: float = 0.0

    def __post_init__(self):
        if not self.probabilities:
            # Default prior: strong bias toward normal two-sided
            p_norm = 0.80
            p_rest = 0.20 / (NUM_REGIMES - 1)
            self.probabilities = {
                MarketRegime.NORMAL_TWO_SIDED: p_norm,
                MarketRegime.PERSISTENT_BUY: p_rest,
                MarketRegime.PERSISTENT_SELL: p_rest,
                MarketRegime.LIQUIDITY_VACUUM: p_rest,
                MarketRegime.UNSTABLE_STALE: p_rest,
            }
        self.recompute_metrics()

    def get(self, regime: MarketRegime) -> float:
        return self.probabilities.get(regime, 0.0)

    def recompute_metrics(self) -> None:
        """Computes normalized entropy and dominant regime."""
        total = sum(self.probabilities.values())
        if total <= 0:
            p_even = 1.0 / NUM_REGIMES
            self.probabilities = {r: p_even for r in REGIME_NAMES}
            total = 1.0
        else:
            self.probabilities = {r: p / total for r, p in self.probabilities.items()}

        ent = 0.0
        best_p = -1.0
        best_r = MarketRegime.NORMAL_TWO_SIDED
        for r, p in self.probabilities.items():
            if p > 0:
                ent -= p * math.log(p)
            if p > best_p:
                best_p = p
                best_r = r

        # Normalized entropy against max entropy log(K)
        max_ent = math.log(NUM_REGIMES)
        self.entropy = max(0.0, min(1.0, ent / max_ent if max_ent > 0 else 0.0))
        self.dominant_regime = best_r
        self.dominant_probability = best_p

    @property
    def uncertainty(self) -> float:
        """Model uncertainty metric [0, 1]. Combines entropy and margin against certainty."""
        return self.entropy

    def as_dict(self) -> Dict[str, Any]:
        return {
            "probs": {r.value: round(p, 4) for r, p in self.probabilities.items()},
            "entropy": round(self.entropy, 4),
            "dominant": self.dominant_regime.value,
            "dominant_prob": round(self.dominant_probability, 4),
            "timestamp": self.timestamp,
        }


class LatentStateEstimator:
    """Bayesian HMM filter and state-space estimator for latent market state."""

    def __init__(self, cfg=None):
        self.cfg = cfg
        # Regime transition matrix T[from, to]
        # Persistent regimes have high self-transition probability (0.88 - 0.94)
        self.transitions: Dict[MarketRegime, Dict[MarketRegime, float]] = {
            MarketRegime.NORMAL_TWO_SIDED: {
                MarketRegime.NORMAL_TWO_SIDED: 0.90,
                MarketRegime.PERSISTENT_BUY: 0.035,
                MarketRegime.PERSISTENT_SELL: 0.035,
                MarketRegime.LIQUIDITY_VACUUM: 0.02,
                MarketRegime.UNSTABLE_STALE: 0.01,
            },
            MarketRegime.PERSISTENT_BUY: {
                MarketRegime.NORMAL_TWO_SIDED: 0.07,
                MarketRegime.PERSISTENT_BUY: 0.88,
                MarketRegime.PERSISTENT_SELL: 0.01,
                MarketRegime.LIQUIDITY_VACUUM: 0.03,
                MarketRegime.UNSTABLE_STALE: 0.01,
            },
            MarketRegime.PERSISTENT_SELL: {
                MarketRegime.NORMAL_TWO_SIDED: 0.07,
                MarketRegime.PERSISTENT_BUY: 0.01,
                MarketRegime.PERSISTENT_SELL: 0.88,
                MarketRegime.LIQUIDITY_VACUUM: 0.03,
                MarketRegime.UNSTABLE_STALE: 0.01,
            },
            MarketRegime.LIQUIDITY_VACUUM: {
                MarketRegime.NORMAL_TWO_SIDED: 0.12,
                MarketRegime.PERSISTENT_BUY: 0.03,
                MarketRegime.PERSISTENT_SELL: 0.03,
                MarketRegime.LIQUIDITY_VACUUM: 0.80,
                MarketRegime.UNSTABLE_STALE: 0.02,
            },
            MarketRegime.UNSTABLE_STALE: {
                MarketRegime.NORMAL_TWO_SIDED: 0.30,
                MarketRegime.PERSISTENT_BUY: 0.05,
                MarketRegime.PERSISTENT_SELL: 0.05,
                MarketRegime.LIQUIDITY_VACUUM: 0.10,
                MarketRegime.UNSTABLE_STALE: 0.50,
            },
        }

        # Initialize prior belief
        self.belief: BeliefState = BeliefState()
        self.state: LatentState = LatentState()
        self.last_update_ts: float = 0.0

        # Continuous state-space tracking (Kalman / EWMA smoothed)
        self._alpha_ewma: float = 0.0
        self._vol_ewma: float = 5.0
        self._lambda_buy_ewma: float = 1.0
        self._lambda_sell_ewma: float = 1.0
        self._theta_buy_ewma: float = 1.0
        self._theta_sell_ewma: float = 1.0

    def update(
        self,
        tfi_1s: float,
        tfi_5s: float,
        tfi_10s: float,
        obi: float,
        flow_accel: float,
        vol_bps: float,
        spread_bps: float,
        fragility_buy: float,
        fragility_sell: float,
        feed_age_s: float,
        is_jump: bool,
        cross_div_bps: float,
        cross_vel_bps: float,
        now: float,
    ) -> Tuple[BeliefState, LatentState]:
        """Performs a Bayesian forward HMM belief update and continuous latent state inference."""
        dt = max(0.01, now - self.last_update_ts) if self.last_update_ts > 0 else 0.25
        self.last_update_ts = now

        # 1. State Prior Propagation: b_pred = T^T * b_prev
        prior_probs: Dict[MarketRegime, float] = {r: 0.0 for r in REGIME_NAMES}
        for r_from in REGIME_NAMES:
            p_prev = self.belief.probabilities[r_from]
            for r_to in REGIME_NAMES:
                prior_probs[r_to] += p_prev * self.transitions[r_from][r_to]

        # 2. Emission Likelihood Computation log L(o_t | R_k)
        log_likelihoods: Dict[MarketRegime, float] = {}

        # Log-likelihood kernels
        # R0: Normal Two-Sided
        # Expects: balanced TFI near 0, balanced OBI near 0, low-to-moderate vol, normal spread, fresh feed
        ll_norm = (
            -0.5 * ((tfi_5s / 0.25) ** 2)
            - 0.5 * ((obi / 0.30) ** 2)
            - 0.5 * ((flow_accel / 0.30) ** 2)
            - 0.5 * ((max(0.0, vol_bps - 4.0) / 4.0) ** 2)
            - 0.5 * ((cross_div_bps / 1.5) ** 2)
            - (2.0 if is_jump else 0.0)
            - (3.0 if feed_age_s > 1.0 else 0.0)
        )
        log_likelihoods[MarketRegime.NORMAL_TWO_SIDED] = ll_norm

        # R1: Persistent Buyer-Initiated Flow
        # Expects: positive TFI, positive OBI, positive flow accel, positive cross-div/vel
        flow_buy = 0.5 * tfi_5s + 0.3 * tfi_1s + 0.2 * obi
        ll_buy = (
            -0.5 * (((flow_buy - 0.65) / 0.25) ** 2)
            - 0.5 * (((cross_div_bps - 1.0) / 1.5) ** 2)
            - 0.5 * ((fragility_sell / 0.5) ** 2)  # asks being eaten
            - (3.0 if feed_age_s > 1.0 else 0.0)
        )
        log_likelihoods[MarketRegime.PERSISTENT_BUY] = ll_buy

        # R2: Persistent Seller-Initiated Flow
        # Expects: negative TFI, negative OBI, negative flow accel, negative cross-div/vel
        flow_sell = 0.5 * tfi_5s + 0.3 * tfi_1s + 0.2 * obi
        ll_sell = (
            -0.5 * (((flow_sell - (-0.65)) / 0.25) ** 2)
            - 0.5 * (((cross_div_bps - (-1.0)) / 1.5) ** 2)
            - 0.5 * ((fragility_buy / 0.5) ** 2)   # bids being eaten
            - (3.0 if feed_age_s > 1.0 else 0.0)
        )
        log_likelihoods[MarketRegime.PERSISTENT_SELL] = ll_sell

        # R3: Liquidity Vacuum (High volatility, withdrawal of liquidity)
        # Expects: elevated vol (vol_bps >= 8.0), wide spread, elevated fragility on both sides
        ll_vac = (
            -0.5 * (((vol_bps - 12.0) / 6.0) ** 2)
            - 0.5 * (((spread_bps - 5.0) / 3.0) ** 2)
            - 0.5 * (((max(fragility_buy, fragility_sell) - 0.70) / 0.25) ** 2)
            + (1.5 if is_jump else 0.0)
            - (6.0 if vol_bps < 7.0 else 0.0)
        )
        log_likelihoods[MarketRegime.LIQUIDITY_VACUUM] = ll_vac

        # R4: Unstable / Stale Feed Condition
        # Expects: high feed age, price jump cooldown, extreme cross divergence
        ll_stale = (
            -0.5 * (((feed_age_s - 2.5) / 1.0) ** 2)
            + (3.5 if is_jump else -1.5)
            + (3.0 if feed_age_s > 1.5 else -2.0)
            + (2.0 if abs(cross_div_bps) > 4.0 else -1.0)
        )
        log_likelihoods[MarketRegime.UNSTABLE_STALE] = ll_stale

        # 3. Posterior Belief Update: log b(k) = log prior(k) + log L(k)
        log_posts = {}
        for r in REGIME_NAMES:
            p_prior = max(1e-9, prior_probs[r])
            log_posts[r] = math.log(p_prior) + log_likelihoods[r]

        # Stable softmax (log-sum-exp)
        max_lp = max(log_posts.values())
        post_unnorm = {r: math.exp(lp - max_lp) for r, lp in log_posts.items()}
        sum_post = sum(post_unnorm.values())
        new_probs = {r: post_unnorm[r] / sum_post for r in REGIME_NAMES}

        self.belief.probabilities = new_probs
        self.belief.timestamp = now
        self.belief.recompute_metrics()

        # 4. Continuous Latent State Estimation: z_t = (alpha_t, lambda_t, nu_t, theta_t)
        # alpha_t (fair-value drift rate in bps/s):
        # Driven by persistent buy/sell regimes + short-term momentum
        alpha_instant = (
            self.belief.get(MarketRegime.PERSISTENT_BUY) * 3.5
            - self.belief.get(MarketRegime.PERSISTENT_SELL) * 3.5
            + 0.5 * cross_div_bps
            + 0.3 * flow_accel
        )
        # In a liquidity vacuum, fair-value drift is highly noisy; damp it toward 0
        vac_weight = self.belief.get(MarketRegime.LIQUIDITY_VACUUM)
        if vac_weight > 0.3:
            alpha_instant *= max(0.1, 1.0 - vac_weight)
        if self.belief.get(MarketRegime.UNSTABLE_STALE) > 0.4:
            alpha_instant = 0.0

        # EWMA update of alpha
        self._alpha_ewma = 0.80 * self._alpha_ewma + 0.20 * alpha_instant

        # Arrival intensities lambda_buy, lambda_sell (events / sec)
        base_lambda = max(0.2, 1.0 + 0.1 * vol_bps)
        l_buy_inst = base_lambda * (1.0 + 0.8 * max(0.0, tfi_5s) + 1.2 * self.belief.get(MarketRegime.PERSISTENT_BUY))
        l_sell_inst = base_lambda * (1.0 + 0.8 * max(0.0, -tfi_5s) + 1.2 * self.belief.get(MarketRegime.PERSISTENT_SELL))
        if vac_weight > 0.4:
            l_buy_inst *= 0.3
            l_sell_inst *= 0.3

        self._lambda_buy_ewma = 0.85 * self._lambda_buy_ewma + 0.15 * l_buy_inst
        self._lambda_sell_ewma = 0.85 * self._lambda_sell_ewma + 0.15 * l_sell_inst

        # nu_t (volatility state):
        self._vol_ewma = 0.90 * self._vol_ewma + 0.10 * vol_bps

        # theta_t (adverse selection conditional on fill in bps):
        # If someone sells to our bid (buy fill), toxicity is high when seller flow is persistent or vacuum
        th_buy_inst = 1.0 + 3.0 * self.belief.get(MarketRegime.PERSISTENT_SELL) + 2.5 * vac_weight + 1.5 * fragility_buy
        # If someone buys from our ask (sell fill), toxicity is high when buyer flow is persistent or vacuum
        th_sell_inst = 1.0 + 3.0 * self.belief.get(MarketRegime.PERSISTENT_BUY) + 2.5 * vac_weight + 1.5 * fragility_sell

        self._theta_buy_ewma = 0.85 * self._theta_buy_ewma + 0.15 * th_buy_inst
        self._theta_sell_ewma = 0.85 * self._theta_sell_ewma + 0.15 * th_sell_inst

        self.state = LatentState(
            alpha_bps_s=self._alpha_ewma,
            lambda_buy=self._lambda_buy_ewma,
            lambda_sell=self._lambda_sell_ewma,
            nu_vol_bps=self._vol_ewma,
            theta_buy_bps=self._theta_buy_ewma,
            theta_sell_bps=self._theta_sell_ewma,
            timestamp=now,
        )

        return self.belief, self.state

    def compute_regime_adjusted_fair_shift(
        self,
        base_half_spread: Decimal,
        obi: Decimal,
        tfi: Decimal,
        alpha_coeff: Decimal,
        beta_coeff: Decimal,
    ) -> Decimal:
        """Computes asymmetric regime-aware fair value shift.

        Key principle: The exact same observed order-book imbalance must NOT produce
        the same quote adjustment across all regimes.
        - In Normal Two-Sided: standard moderate microprice/flow adjustment.
        - In Persistent Buy: upward shift amplified; ask protected.
        - In Persistent Sell: downward shift amplified; bid protected.
        - In Liquidity Vacuum: imbalance is uninformative noise; dampen shift, widen spread instead.
        - In Unstable/Stale: shift zeroed out due to unreliability.
        """
        raw_shift = base_half_spread * (alpha_coeff * obi + beta_coeff * tfi)

        p_norm = Decimal(str(self.belief.get(MarketRegime.NORMAL_TWO_SIDED)))
        p_buy = Decimal(str(self.belief.get(MarketRegime.PERSISTENT_BUY)))
        p_sell = Decimal(str(self.belief.get(MarketRegime.PERSISTENT_SELL)))
        p_vac = Decimal(str(self.belief.get(MarketRegime.LIQUIDITY_VACUUM)))
        p_stale = Decimal(str(self.belief.get(MarketRegime.UNSTABLE_STALE)))

        # In Persistent Buy, upward shift is amplified (1.5x), downward shift dampened (0.5x)
        buy_regime_shift = raw_shift * (Decimal("1.5") if raw_shift > ZERO else Decimal("0.5")) + base_half_spread * Decimal("0.4")
        # In Persistent Sell, downward shift is amplified (1.5x), upward shift dampened (0.5x)
        sell_regime_shift = raw_shift * (Decimal("1.5") if raw_shift < ZERO else Decimal("0.5")) - base_half_spread * Decimal("0.4")
        # In Liquidity Vacuum, damp shift by 70% (noise)
        vac_regime_shift = raw_shift * Decimal("0.3")
        # In Stale/Unstable, zero shift
        stale_regime_shift = ZERO

        adjusted_shift = (
            p_norm * raw_shift
            + p_buy * buy_regime_shift
            + p_sell * sell_regime_shift
            + p_vac * vac_regime_shift
            + p_stale * stale_regime_shift
        )
        return adjusted_shift

    def compute_regime_quote_multipliers(self) -> Tuple[Decimal, Decimal, Decimal]:
        """Returns (spread_multiplier, size_multiplier, uncertainty_penalty_bps).

        - Under Liquidity Vacuum: spread widens significantly (e.g. 2.0x), size is shrunk.
        - Under Stale/Unstable: spread widens (2.5x), size is minimized.
        - Under high model uncertainty H(b_t): spread widens and size scales down.
        """
        p_norm = self.belief.get(MarketRegime.NORMAL_TWO_SIDED)
        p_buy = self.belief.get(MarketRegime.PERSISTENT_BUY)
        p_sell = self.belief.get(MarketRegime.PERSISTENT_SELL)
        p_vac = self.belief.get(MarketRegime.LIQUIDITY_VACUUM)
        p_stale = self.belief.get(MarketRegime.UNSTABLE_STALE)
        ent = self.belief.entropy

        # Spread multiplier
        spr_mult = (
            1.0 * p_norm
            + 1.25 * (p_buy + p_sell)
            + 2.20 * p_vac
            + 2.60 * p_stale
            + 0.50 * ent
        )

        # Size multiplier
        sz_mult = (
            1.0 * p_norm
            + 0.85 * (p_buy + p_sell)
            + 0.35 * p_vac
            + 0.15 * p_stale
        ) * max(0.3, 1.0 - 0.4 * ent)

        # Uncertainty penalty in bps (added to reservation spread)
        unc_pen_bps = 0.5 * ent + 1.5 * p_stale + 1.2 * p_vac

        return (
            Decimal(str(round(max(1.0, spr_mult), 3))),
            Decimal(str(round(max(0.1, min(1.0, sz_mult)), 3))),
            Decimal(str(round(unc_pen_bps, 3))),
        )
