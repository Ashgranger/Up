"""Market metadata (REST) and live top-of-book state (WebSocket bbo channel)."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from utils import BPS, ZERO


@dataclass
class Market:
    market_id: int
    name: str
    status: str
    tick: Decimal
    step: Decimal
    tiers: list
    min_notional: Decimal
    min_size: Decimal
    max_size: Decimal
    mark: Decimal
    is_outside_rth: bool

    @classmethod
    def from_api(cls, d: dict) -> "Market":
        return cls(
            market_id=int(d["marketId"]),
            name=d["marketDisplayName"],
            status=str(d.get("status", "ONLINE")).upper(),
            tick=Decimal(d["tickSize"]),
            step=Decimal(d["stepSize"]),
            tiers=list(d.get("tickTiers") or []),
            min_notional=Decimal(d.get("minOrderNotional") or "0"),
            min_size=Decimal(d.get("minOrderSize") or "0"),
            max_size=Decimal(d.get("maxOrderSize") or "0"),
            mark=Decimal(d.get("markPrice") or "0"),
            is_outside_rth=bool(d.get("isOutsideRth")),
        )

    def tick_for(self, price: Decimal) -> Decimal:
        """Prices must be multiples of the tick of their band (tickTiers)."""
        for t in self.tiers:
            up = t.get("upToPrice")
            if up is None or price < Decimal(up):
                return Decimal(t["tick"])
        return self.tick


class MarketData:
    """Top of book + short-horizon statistics used by the strategy guards."""

    HISTORY_S = 120.0

    def __init__(self, cfg):
        self.cfg = cfg
        self.info: Optional[Market] = None
        self.info_ts = 0.0
        self.bid: Optional[Decimal] = None
        self.ask: Optional[Decimal] = None
        self.bid_sz: Optional[Decimal] = None
        self.ask_sz: Optional[Decimal] = None
        self.ts = 0.0
        self.jump_until = 0.0
        self._hist: deque = deque()  # (t, mid)
        self._vol_ewma = ZERO
        # Real depth (l2OrderbookUpdates: snapshot + incremental, pushed on change - see bot.py's
        # on_book). bbo alone only carries the touch; this lets the micro price lean on real size
        # a few levels deep instead of just what's sitting at the top.
        self.bids: dict = {}          # price -> size
        self.asks: dict = {}
        self.book_seq: Optional[int] = None
        self.book_ts = 0.0
        # Level 5: recent public trades (ts, SIDE, size, price) for trade-flow imbalance
        self._trades: deque = deque()

    # ---- updates --------------------------------------------------------- #
    def clear_book(self) -> None:
        self.bid = self.ask = self.bid_sz = self.ask_sz = None

    @staticmethod
    def _levels(rows) -> list:
        """(price, size) pairs from a level array. Field names aren't nailed down for this
        channel from docs alone (unlike bbo, which real logs already confirmed uses
        price/size) - accepts a couple of plausible shapes defensively; verify against a
        live testnet subscribe before relying on this in production."""
        out = []
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            px = r.get("price", r.get("px"))
            sz = r.get("size", r.get("sz"))
            if px is None or sz is None:
                continue
            try:
                out.append((Decimal(str(px)), Decimal(str(sz))))
            except Exception:
                continue
        return out

    @staticmethod
    def _seq(c: dict) -> Optional[int]:
        for k in ("sequence", "lastSequenceId", "seq", "sequenceId", "updateId"):
            if k in c:
                try:
                    return int(c[k])
                except (TypeError, ValueError):
                    pass
        return None

    def on_book(self, c, snapshot: bool, now: float) -> bool:
        """Apply an l2OrderbookUpdates frame. Returns True if the sequence went backwards and
        the caller should force a fresh subscribe (local book is no longer trustworthy) -
        forward gaps are fine (a later snapshot/update self-heals them), a reversal isn't."""
        if not isinstance(c, dict):
            return False
        seq = self._seq(c)
        if snapshot:
            self.bids = dict(self._levels(c.get("bids")))
            self.asks = dict(self._levels(c.get("asks")))
            self.book_seq, self.book_ts = seq, now
            return False
        if seq is not None and self.book_seq is not None and seq < self.book_seq:
            return True
        for px, sz in self._levels(c.get("bids")):
            if sz == 0:
                self.bids.pop(px, None)
            else:
                self.bids[px] = sz
        for px, sz in self._levels(c.get("asks")):
            if sz == 0:
                self.asks.pop(px, None)
            else:
                self.asks[px] = sz
        if seq is not None:
            self.book_seq = seq
        self.book_ts = now
        return False

    def depth_micro(self, levels: int, now: float) -> Optional[Decimal]:
        """Micro price from real cumulative depth a few levels deep, instead of just the size
        sitting at the touch. None if the book isn't populated or has gone stale (the bbo
        channel can keep working fine even if this one degrades - don't silently trust a
        frozen depth book forever)."""
        if not self.bids or not self.asks or now - self.book_ts > self.cfg.stale_s:
            return None
        bids = sorted(self.bids.items(), key=lambda x: -x[0])[:levels]
        asks = sorted(self.asks.items(), key=lambda x: x[0])[:levels]
        bid_sz, ask_sz = sum(s for _, s in bids), sum(s for _, s in asks)
        if not bids or not asks or bid_sz + ask_sz <= 0:
            return None
        return (bids[0][0] * ask_sz + asks[0][0] * bid_sz) / (bid_sz + ask_sz)

    def depth_imbalance(self, levels: int, now: float) -> Optional[Decimal]:
        """Signed book-pressure reading in [-1, 1] from real depth a few levels deep:
        (bid_depth - ask_depth) / (bid_depth + ask_depth). Positive = more resting size on the
        bid side = buying pressure = price more likely to tick UP next, which makes a resting
        ASK the side about to get run over (sell right before a rally) - not the bid. This is a
        LEADING complement to ret_bps (which only reacts after price has already moved): the aim
        is to widen/shrink the endangered side on the pressure itself, not wait for the move.
        None on the same terms as depth_micro (unpopulated or stale book) - never guess from a
        book that might no longer reflect reality."""
        if not self.bids or not self.asks or now - self.book_ts > self.cfg.stale_s:
            return None
        bid_sz = sum(s for _, s in sorted(self.bids.items(), key=lambda x: -x[0])[:levels])
        ask_sz = sum(s for _, s in sorted(self.asks.items(), key=lambda x: x[0])[:levels])
        total = bid_sz + ask_sz
        if total <= 0:
            return None
        return (bid_sz - ask_sz) / total

    def on_trade(self, side: str, size: Decimal, price: Decimal, now: float) -> None:
        """Record a public trade. `side` is the taker/aggressor side as reported by the feed."""
        self._trades.append((now, side.upper(), size, price))
        while self._trades and now - self._trades[0][0] > 60.0:
            self._trades.popleft()

    def trade_flow_imbalance(self, window_s: float, now: float) -> Decimal:
        """(buy_vol - sell_vol) / total over the window, in [-1, 1]. 0 with no trades."""
        buy_vol = sell_vol = ZERO
        for t, side, sz, _ in self._trades:
            if now - t <= window_s:
                if side in ("BUY", "BID"):
                    buy_vol += sz
                else:
                    sell_vol += sz
        total = buy_vol + sell_vol
        if total <= 0:
            return ZERO
        return (buy_vol - sell_vol) / total

    def update(self, bid: Decimal, ask: Decimal, bid_sz: Optional[Decimal],
               ask_sz: Optional[Decimal], now: float) -> None:
        prev = self.mid
        self.bid, self.ask, self.bid_sz, self.ask_sz, self.ts = bid, ask, bid_sz, ask_sz, now
        mid = self.mid
        if prev and mid and bid < ask:
            if abs(mid - prev) / prev * BPS >= self.cfg.jump_bps:
                self.jump_until = now + self.cfg.jump_cooldown_s
        if bid < ask:  # ignore crossed snapshots in the stats
            self._hist.append((now, mid))
            while self._hist and now - self._hist[0][0] > self.HISTORY_S:
                self._hist.popleft()
            move = self.move_bps(self.cfg.vol_window_s, now)
            self._vol_ewma = move if self._vol_ewma == 0 else self._vol_ewma * Decimal("0.9") + move * Decimal("0.1")

    # ---- derived --------------------------------------------------------- #
    @property
    def mid(self) -> Optional[Decimal]:
        return (self.bid + self.ask) / 2 if self.bid is not None and self.ask is not None else None

    def micro(self, now: float) -> Optional[Decimal]:
        """Size-weighted mid: leans toward the thin side (where price goes next). Prefers real
        depth a few levels in (see depth_micro) over just the size sitting at the touch."""
        d = self.depth_micro(3, now)
        if d is not None:
            return d
        if self.bid is None or self.ask is None:
            return None
        if self.bid_sz and self.ask_sz and (self.bid_sz + self.ask_sz) > 0:
            return (self.bid * self.ask_sz + self.ask * self.bid_sz) / (self.bid_sz + self.ask_sz)
        return self.mid

    @property
    def obi(self) -> Decimal:
        """Top-of-book order-book imbalance in [-1, 1] (+ = more resting bid size)."""
        if self.bid_sz is None or self.ask_sz is None:
            return ZERO
        total = self.bid_sz + self.ask_sz
        if total <= 0:
            return ZERO
        return (self.bid_sz - self.ask_sz) / total

    @property
    def spread_bps(self) -> Decimal:
        m = self.mid
        return (self.ask - self.bid) / m * BPS if m else ZERO

    def jump_active(self, now: float) -> bool:
        return now < self.jump_until

    def _window(self, window_s: float, now: float):
        return [m for t, m in self._hist if now - t <= window_s]

    def ret_bps(self, window_s: float, now: float) -> Decimal:
        """Signed mid return over the window (negative = falling)."""
        w = self._window(window_s, now)
        if len(w) < 2 or w[0] == 0:
            return ZERO
        return (w[-1] - w[0]) / w[0] * BPS

    def move_bps(self, window_s: float, now: float) -> Decimal:
        """High-low range of mid over the window."""
        w = self._window(window_s, now)
        if len(w) < 2 or w[0] == 0:
            return ZERO
        return (max(w) - min(w)) / w[-1] * BPS

    @property
    def vol_bps(self) -> Decimal:
        return self._vol_ewma

    def detect_regime(self, now: float, tox_bps: Decimal) -> str:
        return classify_regime(self.cfg, tox_bps, self.trade_flow_imbalance(10.0, now), self.obi, self.vol_bps)


def classify_regime(cfg, tox_bps: Decimal, tfi: Decimal, obi: Decimal, vol_bps: Decimal) -> str:
    """Pure regime classifier (shared by MarketData and the strategy so both always agree)."""
    if tox_bps >= cfg.regime_toxic_threshold_bps:
        return "REGIME_D_TOXIC"
    if abs(tfi) >= cfg.regime_flow_threshold or abs(obi) >= cfg.regime_flow_threshold:
        return "REGIME_C_TREND"
    if vol_bps >= cfg.regime_vol_threshold_bps:
        return "REGIME_B_HIGH_VOL"
    return "REGIME_A_QUIET"
