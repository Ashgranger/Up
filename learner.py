"""Level 6/7 online learner: adapts quoting parameters from markout feedback and persists them.

When disabled, every property returns the static config value, so callers can always read
parameters through this object without branching on the feature flag.
"""
from __future__ import annotations

import json
import logging
import math
import os
import tempfile
from decimal import Decimal
from typing import Any, Dict, Optional

from utils import BPS, ZERO, clamp

log = logging.getLogger("learner")


class OnlineLearner:
    """Level 7+ Autonomous Online Learning Engine.
    Dynamically modulates ALL market making environment parameters, microstructural
    asymmetry margins, risk penalties, and inventory aversion based on real-time execution feedback."""

    def __init__(self, cfg, enabled=None):
        self.cfg = cfg
        self.enabled = bool(getattr(cfg, "enable_online_learning", False)) if enabled is None else bool(enabled)
        self.state_path = getattr(cfg, "learning_state_path", "learning_state.json")

        # Base configuration defaults (loaded from environment)
        self.base = {
            "min_edge_bps": Decimal(str(cfg.min_edge_bps)),
            "max_edge_bps": Decimal(str(cfg.max_edge_bps)),
            "skew_bps": Decimal(str(cfg.skew_bps)),
            "level_spacing_bps": Decimal(str(cfg.level_spacing_bps)),
            "level_size_mult": Decimal(str(cfg.level_size_mult)),
            "vol_k": Decimal(str(cfg.vol_k)),
            "tox_mult": Decimal(str(cfg.tox_mult)),
            "min_ev_bps": Decimal(str(cfg.min_ev_bps)),
            "obi_alpha": Decimal(str(cfg.obi_alpha)),
            "tfi_beta": Decimal(str(cfg.tfi_beta)),
            "fill_prob_kappa": Decimal(str(cfg.fill_prob_kappa)),
            "gamma_risk_aversion": Decimal(str(cfg.gamma_risk_aversion)),
            "regime_toxic_spread_mult": Decimal(str(cfg.regime_toxic_spread_mult)),
            "trend_pull_bps": Decimal(str(cfg.trend_pull_bps)),
            "trend_widen": Decimal(str(cfg.trend_widen)),
            "exit_min_profit_bps": Decimal(str(cfg.exit_min_profit_bps)),
            "stress_loss_bps": Decimal(str(cfg.stress_loss_bps)),
            "max_hold_s": Decimal(str(cfg.max_hold_s)),
            "burst_fills": Decimal(str(cfg.burst_fills)),
            "burst_cooldown_s": Decimal(str(cfg.burst_cooldown_s)),
            "sweep_guard_fills": Decimal(str(cfg.sweep_guard_fills)),
            "sweep_guard_window_s": Decimal(str(cfg.sweep_guard_window_s)),
        }

        self._last_decay_ts: Optional[float] = None

        # Hard mathematical & safety bounds [min_val, max_val]
        self.bounds = {
            "min_edge_bps": (Decimal("0.2"), Decimal("6.0")),
            "max_edge_bps": (Decimal("2.0"), Decimal("20.0")),
            "skew_bps": (Decimal("0.5"), Decimal("35.0")),
            "level_spacing_bps": (Decimal("1.0"), Decimal("15.0")),
            "level_size_mult": (Decimal("0.10"), Decimal("0.95")),
            "vol_k": (Decimal("0.1"), Decimal("4.0")),
            "tox_mult": (Decimal("0.2"), Decimal("2.5")),
            "min_ev_bps": (Decimal("0.05"), Decimal("0.40")),
            "obi_alpha": (Decimal("0.05"), Decimal("1.5")),
            "tfi_beta": (Decimal("0.05"), Decimal("2.0")),
            "fill_prob_kappa": (Decimal("0.05"), Decimal("1.5")),
            "gamma_risk_aversion": (Decimal("0.01"), Decimal("1.5")),
            "regime_toxic_spread_mult": (Decimal("1.1"), Decimal("4.0")),
            "trend_pull_bps": (Decimal("0.5"), Decimal("10.0")),
            "trend_widen": (Decimal("0.2"), Decimal("4.0")),
            "exit_min_profit_bps": (Decimal("0.5"), Decimal("10.0")),
            "stress_loss_bps": (Decimal("10.0"), Decimal("60.0")),
            "max_hold_s": (Decimal("60.0"), Decimal("1200.0")),
            "burst_fills": (Decimal("2"), Decimal("5")),
            "burst_cooldown_s": (Decimal("10.0"), Decimal("90.0")),
            "sweep_guard_fills": (Decimal("2"), Decimal("4")),
            "sweep_guard_window_s": (Decimal("0.5"), Decimal("2.5")),
        }

        # Current live parameters initialized to base values
        self.params: Dict[str, Decimal] = dict(self.base)

        # Performance & learning statistics
        self.n_markouts = 0
        self.n_toxic = 0
        self.n_benign = 0
        self.n_fills = 0
        self.total_learned_updates = 0
        self.cumulative_spread_captured = Decimal("0")

        if self.enabled:
            self.load()

    # --- Property Accessors for Engine & Bot --- #
    @property
    def min_edge_bps(self) -> Decimal:
        return self.params["min_edge_bps"] if self.enabled else self.base["min_edge_bps"]

    @property
    def max_edge_bps(self) -> Decimal:
        return self.params["max_edge_bps"] if self.enabled else self.base["max_edge_bps"]

    @property
    def skew_bps(self) -> Decimal:
        return self.params["skew_bps"] if self.enabled else self.base["skew_bps"]

    @property
    def level_spacing_bps(self) -> Decimal:
        return self.params["level_spacing_bps"] if self.enabled else self.base["level_spacing_bps"]

    @property
    def level_size_mult(self) -> Decimal:
        return self.params["level_size_mult"] if self.enabled else self.base["level_size_mult"]

    @property
    def vol_k(self) -> Decimal:
        return self.params["vol_k"] if self.enabled else self.base["vol_k"]

    @property
    def tox_mult(self) -> Decimal:
        return self.params["tox_mult"] if self.enabled else self.base["tox_mult"]

    @property
    def min_ev_bps(self) -> Decimal:
        return self.params["min_ev_bps"] if self.enabled else self.base["min_ev_bps"]

    @property
    def obi_alpha(self) -> Decimal:
        return self.params["obi_alpha"] if self.enabled else self.base["obi_alpha"]

    @property
    def tfi_beta(self) -> Decimal:
        return self.params["tfi_beta"] if self.enabled else self.base["tfi_beta"]

    @property
    def fill_prob_kappa(self) -> Decimal:
        return self.params["fill_prob_kappa"] if self.enabled else self.base["fill_prob_kappa"]

    @property
    def gamma_risk_aversion(self) -> Decimal:
        return self.params["gamma_risk_aversion"] if self.enabled else self.base["gamma_risk_aversion"]

    @property
    def regime_toxic_spread_mult(self) -> Decimal:
        return self.params["regime_toxic_spread_mult"] if self.enabled else self.base["regime_toxic_spread_mult"]

    @property
    def trend_pull_bps(self) -> Decimal:
        return self.params["trend_pull_bps"] if self.enabled else self.base["trend_pull_bps"]

    @property
    def trend_widen(self) -> Decimal:
        return self.params["trend_widen"] if self.enabled else self.base["trend_widen"]

    @property
    def exit_min_profit_bps(self) -> Decimal:
        return self.params["exit_min_profit_bps"] if self.enabled else self.base["exit_min_profit_bps"]

    @property
    def stress_loss_bps(self) -> Decimal:
        return self.params["stress_loss_bps"] if self.enabled else self.base["stress_loss_bps"]

    @property
    def max_hold_s(self) -> float:
        return float(self.params["max_hold_s"]) if self.enabled else float(self.base["max_hold_s"])

    @property
    def burst_fills(self) -> int:
        return int(self.params["burst_fills"]) if self.enabled else int(self.base["burst_fills"])

    @property
    def burst_cooldown_s(self) -> float:
        return float(self.params["burst_cooldown_s"]) if self.enabled else float(self.base["burst_cooldown_s"])

    @property
    def sweep_guard_fills(self) -> int:
        return int(self.params["sweep_guard_fills"]) if self.enabled else int(self.base["sweep_guard_fills"])

    @property
    def sweep_guard_window_s(self) -> float:
        return float(self.params["sweep_guard_window_s"]) if self.enabled else float(self.base["sweep_guard_window_s"])

    def tick_decay(self, now: float) -> None:
        """Gradually relaxes learned parameters toward base config during idle/no-fill periods."""
        if not self.enabled:
            return
        if self._last_decay_ts is None:
            self._last_decay_ts = now
            return
        dt = now - self._last_decay_ts
        if dt <= 0:
            return
        self._last_decay_ts = now

        decay_factor = Decimal(str(math.exp(-dt / 45.0)))
        for k in self.params:
            if k in self.base:
                diff = self.params[k] - self.base[k]
                self.params[k] = self.base[k] + diff * decay_factor
        self._clamp_all()

    decay_idle = tick_decay

    def _clamp_all(self) -> None:
        for k in self.params:
            if k in self.bounds:
                lo, hi = self.bounds[k]
                self.params[k] = clamp(self.params[k], lo, hi)
        if self.params["max_edge_bps"] < self.params["min_edge_bps"] + Decimal("2.0"):
            self.params["max_edge_bps"] = min(self.bounds["max_edge_bps"][1], self.params["min_edge_bps"] + Decimal("2.0"))

    def on_markout(self, m_bps: Decimal, side: str, tox_bps: Decimal) -> None:
        """Adapts edges, spreads, spacing, EV cutoffs, and adverse defenses based on markout evaluation."""
        if not self.enabled:
            return

        self.n_markouts += 1
        self.total_learned_updates += 1

        if m_bps < 0:
            # Adverse selection detected (toxic fill where price moved against us)
            self.n_toxic += 1
            severity = min(Decimal("3.0"), abs(m_bps) / Decimal("5.0"))

            # 1. Widen quoting edges & ladder defenses (bounded safely)
            self.params["min_edge_bps"] += Decimal("0.20") * severity
            self.params["max_edge_bps"] += Decimal("0.40") * severity
            self.params["level_spacing_bps"] += Decimal("0.15") * severity
            self.params["level_size_mult"] -= Decimal("0.02") * severity

            # 2. Sharpen toxicity scaling, momentum penalties & EV hurdle
            self.params["tox_mult"] += Decimal("0.05") * severity
            self.params["min_ev_bps"] += Decimal("0.02") * severity
            self.params["regime_toxic_spread_mult"] += Decimal("0.03") * severity
            self.params["trend_widen"] += Decimal("0.03") * severity
            self.params["vol_k"] += Decimal("0.02") * severity

            # 3. Increase exit profit expectation to recover adverse costs
            self.params["exit_min_profit_bps"] += Decimal("0.05") * severity

            # 4. Tighten burst protection
            self.params["burst_cooldown_s"] += Decimal("1.5") * severity

            log.info("LEARN [ADVERSE MARKOUT %.2fbps] -> Widened min_edge=%.2fbps, spacing=%.2fbps, tox_mult=%.2f, min_ev=%.2fbps",
                     float(m_bps), float(self.params["min_edge_bps"]), float(self.params["level_spacing_bps"]),
                     float(self.params["tox_mult"]), float(self.params["min_ev_bps"]))
        else:
            # Profitable, benign markout
            self.n_benign += 1
            decay = Decimal("0.10")

            self.params["min_edge_bps"] -= (self.params["min_edge_bps"] - self.base["min_edge_bps"]) * decay
            self.params["max_edge_bps"] -= (self.params["max_edge_bps"] - self.base["max_edge_bps"]) * decay
            self.params["level_spacing_bps"] -= (self.params["level_spacing_bps"] - self.base["level_spacing_bps"]) * decay
            self.params["level_size_mult"] += (self.base["level_size_mult"] - self.params["level_size_mult"]) * decay
            self.params["tox_mult"] -= (self.params["tox_mult"] - self.base["tox_mult"]) * decay
            self.params["min_ev_bps"] -= (self.params["min_ev_bps"] - self.base["min_ev_bps"]) * decay
            self.params["regime_toxic_spread_mult"] -= (self.params["regime_toxic_spread_mult"] - self.base["regime_toxic_spread_mult"]) * decay
            self.params["vol_k"] -= (self.params["vol_k"] - self.base["vol_k"]) * decay
            self.params["trend_widen"] -= (self.params["trend_widen"] - self.base["trend_widen"]) * decay

        self._clamp_all()
        self.save()

    def on_fill(self, side: str, price: Decimal, mid: Decimal, pos_usd: Decimal, hold_s: float) -> None:
        """Adapts inventory skew, risk aversion, and fill-probability kappa upon execution."""
        if not self.enabled:
            return

        self.n_fills += 1
        self.total_learned_updates += 1

        # 1. Fill distance calibration for fill probability model P(fill) = exp(-kappa * dist)
        if mid and mid > 0:
            dist_bps = abs(price - mid) / mid * BPS
            if dist_bps > Decimal("1.5"):
                self.params["fill_prob_kappa"] -= Decimal("0.005")
            elif dist_bps < Decimal("0.2"):
                self.params["fill_prob_kappa"] += Decimal("0.002")

        # 2. Inventory holding duration & skew adaptation
        max_pos = Decimal(str(self.cfg.max_position_usd))
        pos_ratio = abs(pos_usd) / max_pos if max_pos > 0 else ZERO

        if hold_s > 45.0 or pos_ratio > Decimal("0.5"):
            self.params["skew_bps"] += Decimal("0.35")
            self.params["gamma_risk_aversion"] += Decimal("0.02")
            log.info("LEARN [INVENTORY STAGNANT hold=%.1fs pos_ratio=%.2f] -> Increased skew=%.2fbps, gamma=%.3f",
                     hold_s, float(pos_ratio), float(self.params["skew_bps"]), float(self.params["gamma_risk_aversion"]))
        elif pos_ratio < Decimal("0.15"):
            self.params["skew_bps"] -= (self.params["skew_bps"] - self.base["skew_bps"]) * Decimal("0.05")
            self.params["gamma_risk_aversion"] -= (self.params["gamma_risk_aversion"] - self.base["gamma_risk_aversion"]) * Decimal("0.05")

        self._clamp_all()
        self.save()

    def on_flow_correlation(self, obi: Decimal, tfi: Decimal, ret_bps: Decimal) -> None:
        """Adapts order-book and trade-flow imbalance weights based on forward price prediction accuracy."""
        if not self.enabled:
            return

        if abs(obi) > Decimal("0.2") and abs(ret_bps) > Decimal("0.1"):
            if (obi > 0 and ret_bps > 0) or (obi < 0 and ret_bps < 0):
                self.params["obi_alpha"] += Decimal("0.02")
            else:
                self.params["obi_alpha"] -= Decimal("0.02")

        if abs(tfi) > Decimal("0.2") and abs(ret_bps) > Decimal("0.1"):
            if (tfi > 0 and ret_bps > 0) or (tfi < 0 and ret_bps < 0):
                self.params["tfi_beta"] += Decimal("0.02")
            else:
                self.params["tfi_beta"] -= Decimal("0.02")

        self._clamp_all()

    def on_spread_turnover(self, realized_bps: Decimal) -> None:
        """Adapts exit profit target based on realized turnover profitability."""
        if not self.enabled:
            return
        if realized_bps < Decimal("0.5"):
            self.params["exit_min_profit_bps"] += Decimal("0.15")
        elif realized_bps > Decimal("2.0"):
            self.params["exit_min_profit_bps"] -= (self.params["exit_min_profit_bps"] - self.base["exit_min_profit_bps"]) * Decimal("0.05")
        self._clamp_all()
        self.save()

    def get_summary(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "total_updates": self.total_learned_updates,
            "markouts": self.n_markouts,
            "toxic_fills": self.n_toxic,
            "benign_fills": self.n_benign,
            "fills": self.n_fills,
            "params": {k: f"{v:.4f}" for k, v in self.params.items()},
        }

    def save(self, path: Optional[str] = None) -> bool:
        """Persists learned parameters atomically to disk."""
        target_path = path or self.state_path
        if not target_path or target_path == os.devnull:
            return False

        data = {
            "version": 1,
            "market": getattr(self.cfg, "market", "UNKNOWN"),
            "enabled": self.enabled,
            "total_updates": self.total_learned_updates,
            "n_markouts": self.n_markouts,
            "n_toxic": self.n_toxic,
            "n_benign": self.n_benign,
            "n_fills": self.n_fills,
            "params": {k: str(v) for k, v in self.params.items()},
        }

        try:
            dir_name = os.path.dirname(os.path.abspath(target_path)) or "."
            fd, tmp_file = tempfile.mkstemp(dir=dir_name, prefix="learning_tmp_")
            with os.fdopen(fd, "w") as fp:
                json.dump(data, fp, indent=2)
            os.replace(tmp_file, target_path)
            return True
        except Exception:
            return False

    def load(self, path: Optional[str] = None) -> bool:
        """Loads previously saved learned parameters."""
        target_path = path or self.state_path
        if not os.path.exists(target_path):
            return False

        try:
            with open(target_path, "r") as fp:
                data = json.load(fp)

            if isinstance(data, dict) and "params" in data:
                saved_market = data.get("market")
                current_market = getattr(self.cfg, "market", None)
                if saved_market and current_market and saved_market != current_market:
                    log.info("Learned state was for %s, current market is %s — resetting to base configuration",
                             saved_market, current_market)
                    return False
                for k, v in data["params"].items():
                    if k in self.params:
                        self.params[k] = Decimal(str(v))
                self.n_markouts = int(data.get("n_markouts", 0))
                self.n_toxic = int(data.get("n_toxic", 0))
                self.n_benign = int(data.get("n_benign", 0))
                self.n_fills = int(data.get("n_fills", 0))
                self.total_learned_updates = int(data.get("total_updates", 0))
                self._clamp_all()
                return True
        except Exception:
            pass
        return False
