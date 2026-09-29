"""Market metadata and live state + Level 5 Intelligence."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, List, Tuple

from utils import BPS, ZERO, ONE, clamp


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
            tick=Decimal(str(d["tickSize"])),
            step=Decimal(str(d["stepSize"])),
            tiers=list(d.get("tickTiers") or []),
            min_notional=Decimal(str(d.get("minOrderNotional") or "0")),
            min_size=Decimal(str(d.get("minOrderSize") or "0")),
            max_size=Decimal(str(d.get("maxOrderSize") or "0")),
            mark=Decimal(str(d.get("markPrice") or "0")),
            is_outside_rth=bool(d.get("isOutsideRth")),
        )

    def tick_for(self, price: Decimal) -> Decimal:
        for t in self.tiers:
            up = t.get("upToPrice")
            if up is None or price < Decimal(str(up)):
                return Decimal(str(t["tick"]))
        return self.tick


@dataclass
class VenueState:
    venue: str
    bid: Decimal
    ask: Decimal
    bid_sz: Decimal
    ask_sz: Decimal
    ts: float

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")

    @property
    def obi(self) -> Decimal:
        total = self.bid_sz + self.ask_sz
        if total <= 0:
            return ZERO
        return (self.bid_sz - self.ask_sz) / total


class CrossVenueTracker:
    """Tracks cross-exchange market states (Binance, Bybit, OKX, Coinbase, etc.)
    and generates lead/lag, price velocity, dispersion, and cross-venue OBI features.
    """
    def __init__(self):
        self.venues: dict[str, VenueState] = {}
        self._history: deque = deque()

    def update_venue(self, venue: str, bid: Decimal, ask: Decimal,
                     bid_sz: Decimal, ask_sz: Decimal, now: float) -> None:
        st = VenueState(venue, bid, ask, bid_sz, ask_sz, now)
        self.venues[venue] = st
        self._history.append((now, venue, st.mid))
        while self._history and now - self._history[0][0] > 60.0:
            self._history.popleft()

    def cross_fair_value(self) -> Optional[Decimal]:
        if not self.venues:
            return None
        mids = [v.mid for v in self.venues.values() if v.mid > ZERO]
        if not mids:
            return None
        return sum(mids) / Decimal(str(len(mids)))

    def cross_velocity_bps(self, window_s: float, now: float) -> Decimal:
        """Rate of change of the external benchmark mid price over window_s."""
        if not self._history:
            return ZERO
        recent = [m for t, v, m in self._history if now - t <= window_s]
        if len(recent) < 2 or recent[0] == ZERO:
            return ZERO
        return (recent[-1] - recent[0]) / recent[0] * BPS

    def cross_dispersion_bps(self) -> Decimal:
        """Measures price disagreement across venues in bps (max mid - min mid / avg mid)."""
        if len(self.venues) < 2:
            return ZERO
        mids = [v.mid for v in self.venues.values() if v.mid > ZERO]
        if len(mids) < 2:
            return ZERO
        avg_mid = sum(mids) / Decimal(str(len(mids)))
        if avg_mid <= ZERO:
            return ZERO
        return (max(mids) - min(mids)) / avg_mid * BPS

    def cross_obi(self) -> Decimal:
        """Consolidated average order book imbalance across external venues."""
        if not self.venues:
            return ZERO
        obis = [v.obi for v in self.venues.values()]
        return sum(obis) / Decimal(str(len(obis)))

    def lead_lag_divergence_bps(self, local_mid: Optional[Decimal]) -> Decimal:
        """Divergence between external benchmark fair value and local mid price."""
        if local_mid is None or local_mid <= ZERO:
            return ZERO
        cf = self.cross_fair_value()
        if cf is None:
            return ZERO
        return (cf - local_mid) / local_mid * BPS


class MarketData:
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
        self._hist: deque = deque()
        self._trades: deque = deque()
        self._vol_ewma = ZERO
        self._book_depth_bids: list = []
        self._book_depth_asks: list = []
        self.cross = CrossVenueTracker()

    def clear_book(self) -> None:
        self.bid = self.ask = self.bid_sz = self.ask_sz = None
        self._book_depth_bids.clear()
        self._book_depth_asks.clear()

    def update(self, bid: Decimal, ask: Decimal, bid_sz: Optional[Decimal],
               ask_sz: Optional[Decimal], now: float) -> None:
        prev = self.mid
        self.bid, self.ask, self.bid_sz, self.ask_sz, self.ts = bid, ask, bid_sz, ask_sz, now
        mid = self.mid
        if prev and mid and bid < ask:
            if abs(mid - prev) / prev * BPS >= self.cfg.jump_bps:
                self.jump_until = now + self.cfg.jump_cooldown_s
        if bid < ask:
            self._hist.append((now, mid))
            while self._hist and now - self._hist[0][0] > self.HISTORY_S:
                self._hist.popleft()
            move = self.move_bps(self.cfg.vol_window_s, now)
            self._vol_ewma = move if self._vol_ewma == 0 else self._vol_ewma * Decimal("0.9") + move * Decimal("0.1")

    def on_trade(self, side: str, size: Decimal, price: Decimal, now: float) -> None:
        self._trades.append((now, side.upper(), size, price))
        while self._trades and now - self._trades[0][0] > 60.0:
            self._trades.popleft()

    def on_depth(self, bids: list, asks: list, now: float) -> None:
        self._book_depth_bids = bids
        self._book_depth_asks = asks

    def update_cross_venue(self, venue: str, bid: Decimal, ask: Decimal,
                           bid_sz: Decimal, ask_sz: Decimal, now: float) -> None:
        self.cross.update_venue(venue, bid, ask, bid_sz, ask_sz, now)

    @property
    def mid(self) -> Optional[Decimal]:
        return (self.bid + self.ask) / 2 if self.bid is not None and self.ask is not None else None

    @property
    def micro(self) -> Optional[Decimal]:
        if self.bid is None or self.ask is None:
            return None
        if self.bid_sz and self.ask_sz and (self.bid_sz + self.ask_sz) > 0:
            return (self.bid * self.ask_sz + self.ask * self.bid_sz) / (self.bid_sz + self.ask_sz)
        return self.mid

    @property
    def obi(self) -> Decimal:
        if self.bid_sz is None or self.ask_sz is None:
            return ZERO
        total = self.bid_sz + self.ask_sz
        if total <= 0:
            return ZERO
        return (self.bid_sz - self.ask_sz) / total

    def trade_flow_imbalance(self, window_s: float, now: float) -> Decimal:
        buy_vol = ZERO
        sell_vol = ZERO
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

    @property
    def spread_bps(self) -> Decimal:
        m = self.mid
        return (self.ask - self.bid) / m * BPS if m else ZERO

    def jump_active(self, now: float) -> bool:
        return now < self.jump_until

    def _window(self, window_s: float, now: float):
        return [m for t, m in self._hist if now - t <= window_s]

    def ret_bps(self, window_s: float, now: float) -> Decimal:
        w = self._window(window_s, now)
        if len(w) < 2 or w[0] == 0:
            return ZERO
        return (w[-1] - w[0]) / w[0] * BPS

    def move_bps(self, window_s: float, now: float) -> Decimal:
        w = self._window(window_s, now)
        if len(w) < 2 or w[0] == 0:
            return ZERO
        return (max(w) - min(w)) / w[-1] * BPS

    @property
    def vol_bps(self) -> Decimal:
        return self._vol_ewma

    def detect_regime(self, now: float, tox_bps: Decimal) -> str:
        if tox_bps >= self.cfg.regime_toxic_threshold_bps:
            return "REGIME_D_TOXIC"
        tfi = abs(self.trade_flow_imbalance(10.0, now))
        obi = abs(self.obi)
        if tfi >= self.cfg.regime_flow_threshold or obi >= self.cfg.regime_flow_threshold:
            return "REGIME_C_TREND"
        if self.vol_bps >= self.cfg.regime_vol_threshold_bps:
            return "REGIME_B_HIGH_VOL"
        return "REGIME_A_QUIET"
