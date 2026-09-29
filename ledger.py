"""Fill-driven accounting: the bot's own source of truth for position and PnL.

Definitions (all in USD unless noted):
  spread_capture = sum(qty * (mid_at_fill - buy_price))  and  sum(qty * (sell_price - mid_at_fill))
                   -> what the quotes earned versus fair value at the instant of each fill
  realized       = closed-lot PnL by average cost (minus fees)
  unrealized     = (mark - avg_cost) * position
  inventory_pnl  = total - spread_capture   -> what price movement did to the inventory we carried
  markout        = mid N seconds after a fill vs the fill price (bps, + = good). Persistently
                   negative markout == adverse selection == the edge is too thin.
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from learner import OnlineLearner
from utils import BPS, BUY, ONE, ZERO


@dataclass
class Fill:
    ts: float
    side: str
    qty: Decimal
    price: Decimal
    mid: Decimal
    edge_bps: Decimal
    position: Decimal
    realized_delta: Decimal
    fid: int = 0              # sequential id; journal markout lines reference it


class Ledger:
    def __init__(self, cfg):
        self.cfg = cfg
        self.position = ZERO
        self.avg_cost = ZERO
        self.realized = ZERO
        self.fees = ZERO
        self.spread_capture = ZERO
        self.spread_edge_bps_sum = ZERO
        self.volume_usd = ZERO
        self.n_fills = 0
        self.n_buys = 0
        self.n_sells = 0
        self.fills: deque = deque(maxlen=500)
        self.opened_ts: Optional[float] = None
        self.last_fill_ts = -1e9
        self._pending_markouts: list = []
        self._journal_markouts: list = []       # (due_ts, horizon_s, Fill) - multi-horizon, journal only
        # Markouts are (ts, bps) so old samples fade (see _calc_weighted_markout). Kept per side
        # too: a toxic bid side must not make the ask side quote wider for no reason.
        self.markouts: deque = deque(maxlen=cfg.markout_window * 2)
        self.markouts_buy: deque = deque(maxlen=cfg.markout_window)
        self.markouts_sell: deque = deque(maxlen=cfg.markout_window)
        self._mismatch = 0
        self.last_now: float = time.time()
        self.current_now: Optional[float] = None
        self.learner = OnlineLearner(cfg)      # Level 6/7 online parameter learning

    # ---- state helpers --------------------------------------------------- #
    def is_flat(self, mid: Decimal, min_notional: Decimal) -> bool:
        """Positions below the venue's minimum order notional are untradeable dust == flat."""
        return abs(self.position * mid) < max(min_notional, Decimal(1))

    def hold_s(self, now: float) -> float:
        return now - self.opened_ts if self.opened_ts is not None else 0.0

    def unrealized(self, mark: Decimal) -> Decimal:
        return (mark - self.avg_cost) * self.position if self.position != 0 else ZERO

    def total_pnl(self, mark: Decimal) -> Decimal:
        return self.realized + self.unrealized(mark)

    def inventory_pnl(self, mark: Decimal) -> Decimal:
        return self.total_pnl(mark) - self.spread_capture

    @property
    def avg_edge_bps(self) -> Decimal:
        return (self.spread_edge_bps_sum / self.n_fills) if self.n_fills else ZERO

    # ---- fills ------------------------------------------------------------- #
    def on_fill(self, side: str, qty: Decimal, price: Decimal, mid: Decimal, now: float,
                min_notional: Decimal) -> Fill:
        signed = qty if side == BUY else -qty
        was_flat = self.is_flat(mid, min_notional)
        realized_delta = ZERO
        if self.position == 0 or (self.position > 0) == (signed > 0):
            total = abs(self.position) + qty
            self.avg_cost = (self.avg_cost * abs(self.position) + price * qty) / total
            self.position += signed
        else:
            closed = min(abs(self.position), qty)
            direction = 1 if self.position > 0 else -1
            realized_delta = (price - self.avg_cost) * closed * direction
            new_pos = self.position + signed
            if new_pos == 0:
                self.avg_cost = ZERO
            elif (new_pos > 0) != (self.position > 0):
                self.avg_cost = price  # flipped through zero: the remainder opens at this price
            self.position = new_pos
        fee = qty * price * self.cfg.maker_fee_bps / BPS
        realized_delta -= fee
        self.fees += fee
        self.realized += realized_delta

        edge = (mid - price) if side == BUY else (price - mid)
        edge_bps = edge / mid * BPS if mid else ZERO
        self.spread_capture += edge * qty
        self.spread_edge_bps_sum += edge_bps
        self.volume_usd += qty * price
        self.n_fills += 1
        self.n_buys += side == BUY
        self.n_sells += side != BUY
        self.last_fill_ts = now

        now_flat = self.is_flat(mid, min_notional)
        if was_flat and not now_flat:
            self.opened_ts = now
        elif now_flat:
            self.opened_ts = None

        f = Fill(now, side, qty, price, mid, edge_bps, self.position, realized_delta, fid=self.n_fills)
        self.fills.append(f)
        self._pending_markouts.append((now + self.cfg.markout_horizon_s, f))
        for h in self.cfg.markout_horizons_s:
            self._journal_markouts.append((now + h, h, f))
        self.last_now = now
        self.learner.on_fill(side, price, mid, self.position * mid, self.hold_s(now))
        return f

    def matured_journal_markouts(self, mid: Decimal, now: float) -> list:
        """[(fid, horizon_s, markout_bps)] for every horizon that has now elapsed. Kept separate from
        the single-horizon toxicity estimate above (which drives quoting) - this only feeds the
        journal, so collecting more horizons can never change trading behavior."""
        out, keep = [], []
        for item in self._journal_markouts:
            due, h, f = item
            if due <= now:
                m = (mid - f.price) if f.side == BUY else (f.price - mid)
                out.append((f.fid, h, float(m / f.price * BPS)))
            else:
                keep.append(item)
        self._journal_markouts = keep
        return out

    # ---- markouts (adverse-selection measurement) -------------------------- #
    def _now(self) -> float:
        if self.current_now is not None:
            return float(self.current_now)
        return float(self.last_now)

    def _calc_weighted_markout(self, buf: deque) -> Decimal:
        """Exponentially time-weighted mean markout (tau 25s); samples older than 60s are dropped."""
        if not buf:
            return ZERO
        weighted_sum = weight_total = ZERO
        now = self._now()
        for ts, val in buf:
            dt = max(0.0, now - float(ts))
            if dt >= 60.0:
                continue
            w = Decimal(str(math.exp(-dt / 25.0)))
            weighted_sum += val * w
            weight_total += w
        if weight_total < Decimal("0.01"):
            return ZERO
        return weighted_sum / weight_total

    def process_markouts(self, mid: Decimal, now: float) -> None:
        self.last_now = now
        while self._pending_markouts and self._pending_markouts[0][0] <= now:
            _, f = self._pending_markouts.pop(0)
            m = (mid - f.price) if f.side == BUY else (f.price - mid)
            m_bps = m / f.price * BPS
            self.markouts.append((now, m_bps))
            (self.markouts_buy if f.side == BUY else self.markouts_sell).append((now, m_bps))
            self.learner.on_markout(m_bps, f.side, self.tox_bps)

    @property
    def avg_markout_bps(self) -> Decimal:
        return self._calc_weighted_markout(self.markouts)

    @property
    def tox_bps(self) -> Decimal:
        """How much the recent fills lost after the fact (>= 0), time-weighted."""
        if not self.markouts:
            return ZERO
        return max(ZERO, -self.avg_markout_bps)

    def side_tox_bps(self, side: str) -> Decimal:
        """Toxicity of one side's fills; falls back to the overall figure until that side has data."""
        buf = self.markouts_buy if side == BUY else self.markouts_sell
        if not buf:
            return self.tox_bps
        return max(ZERO, -self._calc_weighted_markout(buf))

    # ---- reconciliation with the exchange ------------------------------------ #
    def reconcile(self, ex_pos: Decimal, now: float, mid: Decimal, min_notional: Decimal) -> bool:
        """Adopt the exchange position only after two consecutive, quiet disagreements
        (a single stale read right after a fill must not flip our books - it did in v1)."""
        tol = (min_notional / mid) * Decimal("0.25") if mid else Decimal("1e-8")
        if abs(ex_pos - self.position) <= tol:
            self._mismatch = 0
            return False
        if now - self.last_fill_ts < 4.0:
            return False
        self._mismatch += 1
        if self._mismatch < 2:
            return False
        old = self.position
        self.position = ex_pos
        self._mismatch = 0
        if old == 0 or (old > 0) != (ex_pos > 0) or self.avg_cost == 0:
            self.avg_cost = mid  # unknown true cost -> adopt at current mid
        self.opened_ts = None if self.is_flat(mid, min_notional) else now
        return True
