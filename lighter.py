"""Lighter RH (Robinhood Chain) integration: public data feed + optional hedge executor.

Docs used (apidocs.lighter.xyz):
  * Lighter RH is a SEPARATE exchange: REST https://api.rh.lighter.xyz, WS wss://api.rh.lighter.xyz/stream,
    chain id 466324 (the SDK picks it from the URL). Accounts / API keys / nonces / market ids are per exchange.
  * Market data is public over the WebSocket (does not use the REST/sendTx budget):
        order_book/{id}   snapshot on subscribe, then deltas every 50 ms; size "0" removes a level;
                          update.begin_nonce must equal the previous update's nonce, else resubscribe.
        market_stats/{id}, trade/{id}.  Client must send a frame at least every 2 minutes ({"type":"ping"}).
  * Standard account: 0% maker / 0% taker, taker latency 300 ms, maker/cancel 200 ms,
    sendTx + REST share a budget of 60 requests per minute (HTTP and WS sendTx are one bucket).
  * Orders use integer sizes/prices (x 10^size_decimals / 10^price_decimals). A taker order's price is the WORST
    acceptable price (slippage cap). Market order = order_type 1, IOC, order_expiry 0.
  * sendTx code 200 only means "accepted": real position comes from the account_all_positions channel.

Modes:  HEDGE_MODE=paper (default; also forced when DRY_RUN=1) simulates the hedge against the live Lighter book
        with HEDGE_LATENCY_MS delay, so you can measure the real hedged edge before risking anything.
        HEDGE_MODE=live sends real IOC market orders through the official `lighter` Python SDK (pip install lighter-sdk).
"""
from __future__ import annotations

import asyncio
import json
import math
import logging
import time
import urllib.request
from collections import deque
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any, Callable, Optional

from feeds import VenueFeed
from utils import BPS, BUY, SELL, ZERO, ONE

log = logging.getLogger("lighter")
D = Decimal


# --------------------------------------------------------------------------- #
class LighterBook:
    """Order book kept in sync from the order_book channel (nonce-continuity checked)."""

    def __init__(self) -> None:
        self.bids: dict = {}
        self.asks: dict = {}
        self.nonce: int = -1
        self.ts: float = 0.0

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.nonce = -1

    def _merge(self, ob: dict) -> None:
        for side, levels in ((self.bids, ob.get("bids") or []), (self.asks, ob.get("asks") or [])):
            for lv in levels:
                p, s = str(lv["price"]), str(lv["size"])
                if D(s) == 0:
                    side.pop(p, None)
                else:
                    side[p] = s

    def snapshot(self, ob: dict, now: float) -> None:
        self.bids.clear()
        self.asks.clear()
        self._merge(ob)
        self.nonce = int(ob.get("nonce", -1))
        self.ts = now

    def update(self, ob: dict, now: float) -> bool:
        """False => gap detected (caller must resubscribe)."""
        if self.nonce < 0:
            return False
        bn = ob.get("begin_nonce")
        if bn is not None and int(bn) != self.nonce:
            self.nonce = -1
            return False
        self._merge(ob)
        self.nonce = int(ob.get("nonce", self.nonce))
        self.ts = now
        return True

    def top(self, n: int = 10):
        bids = sorted(((D(p), D(s)) for p, s in self.bids.items()), key=lambda x: -x[0])[:n]
        asks = sorted(((D(p), D(s)) for p, s in self.asks.items()), key=lambda x: x[0])[:n]
        return bids, asks

    def bbo(self):
        """(bid, ask, bid_sz, ask_sz) or None if the book is empty/crossed."""
        if not self.bids or not self.asks:
            return None
        bp = max(self.bids, key=lambda p: D(p))
        ap = min(self.asks, key=lambda p: D(p))
        b, a = D(bp), D(ap)
        if b >= a:
            return None
        return b, a, D(self.bids[bp]), D(self.asks[ap])

    def vwap(self, side: str, qty: Decimal):
        """Average price to TAKE qty: BUY lifts asks, SELL hits bids. Returns (px, filled_qty) or (None, 0)."""
        bids, asks = self.top(50)
        levels = asks if side == BUY else bids
        need, cost = qty, ZERO
        for p, s in levels:
            take = min(need, s)
            cost += take * p
            need -= take
            if need <= 0:
                break
        filled = qty - need
        return ((cost / filled) if filled > 0 else None), filled


# --------------------------------------------------------------------------- #
def resolve_market_sync(rest_url: str, symbol: str, market_id: str = "") -> dict:
    """One REST call (counts 1 of the 60/min budget). Returns market_id + decimals + minimums."""
    url = rest_url.rstrip("/") + "/api/v1/orderBookDetails"
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read().decode())
    rows = data.get("order_book_details") or []
    sym = symbol.upper()
    for row in rows:
        if market_id and str(row.get("market_id")) == str(market_id):
            return row
        if not market_id and str(row.get("symbol", "")).upper() in (sym, sym + "-USD", sym + "/USD", sym + "USD"):
            return row
    raise RuntimeError(f"Lighter market '{symbol}' not found in orderBookDetails ({len(rows)} markets)")


class LighterFeed(VenueFeed):
    """Public WS feed -> sink.on_external_* (same contract as Binance/Bybit feeds) + optional account positions."""
    name = "LIGHTER"
    idle_timeout_s = 20.0

    def __init__(self, sink: Any, cfg: Any, connect: Optional[Callable] = None, now: Callable[[], float] = time.monotonic):
        symbol = cfg.lighter_symbol or cfg.market.upper().split("-")[0].split("/")[0]
        super().__init__(sink, symbol, cfg.lighter_ws_url, connect)
        self.cfg = cfg
        self.now = now
        self.book = LighterBook()
        self.market: Optional[dict] = None
        self.market_id: Optional[int] = int(cfg.lighter_market_id) if str(cfg.lighter_market_id).strip() else None
        self.size_decimals = 4
        self.price_decimals = 2
        self.min_base_amount = ZERO
        self.mark: Optional[Decimal] = None
        self.index_price: Optional[Decimal] = None
        self.position: Optional[Decimal] = None        # signed, from account_all_positions (live mode)
        self.position_ts: float = 0.0
        self._ws = None
        self._need_resub = False
        self.last_bbo_ts: float = 0.0
        self.on_position: Optional[Callable[[Decimal], None]] = None
        self.trade_cbs: list = []

    # -- market resolution ------------------------------------------------ #
    def apply_market_row(self, row: dict) -> None:
        self.market = row
        self.market_id = int(row["market_id"])
        self.size_decimals = int(row.get("size_decimals", self.size_decimals))
        self.price_decimals = int(row.get("price_decimals", self.price_decimals))
        self.min_base_amount = D(str(row.get("min_base_amount", "0") or "0"))
        st = str(row.get("status", "active")).lower()
        if st != "active":
            log.warning("[LIGHTER] market %s status=%s (only 'active' markets should be traded)", self.symbol, st)

    async def ensure_market(self) -> None:
        if self.market is not None:
            return
        row = await asyncio.to_thread(resolve_market_sync, self.cfg.lighter_url, self.symbol,
                                      str(self.market_id) if self.market_id is not None else "")
        self.apply_market_row(row)
        log.info("[LIGHTER] market %s -> id=%s size_dec=%s price_dec=%s min_base=%s", self.symbol, self.market_id,
                 self.size_decimals, self.price_decimals, self.min_base_amount)

    # -- connection hooks -------------------------------------------------- #
    async def on_open(self, ws) -> None:
        self._ws = ws
        await self.ensure_market()
        self.book.reset()
        mid = self.market_id
        for ch in (f"order_book/{mid}", f"market_stats/{mid}", f"trade/{mid}"):
            await ws.send(json.dumps({"type": "subscribe", "channel": ch}))
        if self.cfg.lighter_account_index >= 0 and self.cfg.hedge_enabled and self.cfg.hedge_mode == "live":
            await ws.send(json.dumps({"type": "subscribe",
                                      "channel": f"account_all_positions/{self.cfg.lighter_account_index}"}))

    async def keepalive(self, ws) -> None:
        last_ping = time.monotonic()
        while True:
            await asyncio.sleep(0.25)
            if self._need_resub:
                self._need_resub = False
                ch = f"order_book/{self.market_id}"
                log.warning("[LIGHTER] order book gap -> resubscribing")
                await ws.send(json.dumps({"type": "unsubscribe", "channel": ch}))
                await ws.send(json.dumps({"type": "subscribe", "channel": ch}))
            if time.monotonic() - last_ping > 45.0:      # server closes after 2 min without a frame
                last_ping = time.monotonic()
                await ws.send(json.dumps({"type": "ping"}))

    # -- message handling --------------------------------------------------- #
    def handle(self, raw) -> None:
        msg = json.loads(raw) if not isinstance(raw, dict) else raw
        t = msg.get("type", "")
        now = self.now()
        if t == "ping":
            return
        if t in ("subscribed/order_book", "update/order_book"):
            ob = msg.get("order_book") or {}
            if t.startswith("subscribed"):
                self.book.snapshot(ob, now)
            elif not self.book.update(ob, now):
                self._need_resub = True
                return
            self._publish(now)
        elif t == "update/market_stats":
            ms = msg.get("market_stats") or {}
            if ms.get("mark_price"):
                self.mark = D(str(ms["mark_price"]))
            if ms.get("index_price"):
                self.index_price = D(str(ms["index_price"]))
        elif t == "update/trade":
            for tr in msg.get("trades") or []:
                # is_maker_ask=True => the resting order was an ask => the aggressor BOUGHT
                side = BUY if tr.get("is_maker_ask") else SELL
                sz, px = D(str(tr["size"])), D(str(tr["price"]))
                self.sink.on_external_trade(self.name, side, sz, px)
                for cb in self.trade_cbs:
                    cb(side, sz, px)
        elif t in ("subscribed/account_all_positions", "update/account_all_positions"):
            pos = (msg.get("positions") or {}).get(str(self.market_id))
            if pos is not None:
                q = D(str(pos.get("position", "0"))) * D(int(pos.get("sign", 1) or 1))
                self.position, self.position_ts = q, now
                if self.on_position:
                    self.on_position(q)

    def _publish(self, now: float) -> None:
        bbo = self.book.bbo()
        if bbo is None:
            return
        b, a, bs, asz = bbo
        self.last_bbo_ts = now
        self.sink.on_external_venue_bbo(self.name, b, a, bs, asz)
        bids, asks = self.book.top(10)
        self.sink.on_external_depth(self.name, [(str(p), str(s)) for p, s in bids], [(str(p), str(s)) for p, s in asks])


# --------------------------------------------------------------------------- #
class LighterHedger:
    """Keeps (Arcus position + Lighter position) ~ 0 by hedging the NET exposure in batches (request-budget aware)."""

    def __init__(self, cfg: Any, feed: LighterFeed, get_arcus_position: Callable[[], Decimal],
                 get_mid: Callable[[], Optional[Decimal]], client: Any = None,
                 now: Callable[[], float] = time.monotonic) -> None:
        self.cfg = cfg
        self.feed = feed
        self.get_arcus_position = get_arcus_position
        self.get_mid = get_mid
        self.client = client
        self.now = now
        self.enabled = bool(cfg.hedge_enabled)
        self.mode = "paper" if (cfg.dry_run or cfg.hedge_mode != "live") else "live"
        # state
        self.position: Decimal = ZERO          # our Lighter position (paper: simulated, live: confirmed by WS)
        self.avg_px: Decimal = ZERO
        self.realized: Decimal = ZERO
        self.tx_times: deque = deque()
        self.last_hedge_ts: float = 0.0
        self.exposure_since: Optional[float] = None
        self.inflight: Decimal = ZERO
        self.inflight_ts: float = 0.0
        self.fails: int = 0
        self.halted: str = ""
        self.n_hedges = 0
        self.hedged_notional = ZERO
        self._busy = False
        # basis / cost estimators (time-based EMAs, robust to book glitches)
        self.basis_ema: Optional[float] = None      # Lighter mid minus Arcus mid, bps (persistent level ~ +9 in the 10-08 sample)
        self.basis_now: Optional[float] = None
        self.hl_ema: Optional[float] = None         # Lighter half-spread in bps = cost of crossing it
        self.slip_ema: float = 0.0                  # measured execution slippage vs the touch at decision time, bps
        self._obs_ts: Optional[float] = None
        self.cost_n = 0
        self.cost_notional = ZERO
        self.cost_cross_usd = ZERO
        self.cost_slip_usd = ZERO
        self._last_report = 0.0
        self._coid = int(time.time()) % 10_000_000 * 1000
        self.style = cfg.hedge_style if cfg.hedge_style in ("taker", "maker_first") else "taker"
        self._trades: deque = deque(maxlen=2000)     # (seq, aggressor_side, size, price)
        self._trade_seq = 0
        self.maker_qty = ZERO
        self.taker_qty = ZERO
        self.maker_attempts = 0
        self.cost_ema = {"maker": None, "taker": None}   # measured cost vs Lighter mid (bps) by execution style
        self.maker_filled_attempts = 0
        feed.trade_cbs.append(self._on_trade)
        if self.mode == "live":
            feed.on_position = self._on_live_position

    # -- helpers --------------------------------------------------------------- #
    def _on_live_position(self, q: Decimal) -> None:
        self.position = q
        self.inflight = ZERO

    def _on_trade(self, side: str, size: Decimal, price: Decimal) -> None:
        self._trade_seq += 1
        self._trades.append((self._trade_seq, side, size, price))

    def eff_arcus(self) -> Decimal:
        p = self.get_arcus_position()
        return p

    def budget_used(self, now: float) -> int:
        while self.tx_times and now - self.tx_times[0] > 60.0:
            self.tx_times.popleft()
        return len(self.tx_times)

    def budget_ok(self, now: float) -> bool:
        return self.budget_used(now) < min(60, int(self.cfg.hedge_max_tx_per_min))

    def feed_fresh(self, now: float) -> bool:
        return self.feed.last_bbo_ts > 0 and (now - self.feed.last_bbo_ts) <= self.cfg.hedge_feed_stale_s

    def delta(self) -> Decimal:
        """Signed Lighter quantity still needed: target = -ratio * arcus_position."""
        return -self.cfg.hedge_ratio * self.eff_arcus() - (self.position + self.inflight)

    def net_exposure(self) -> Decimal:
        return self.eff_arcus() + self.position + self.inflight

    def combined_pnl(self, arcus_pnl: Decimal) -> Decimal:
        bbo = self.feed.book.bbo()
        mark = ((bbo[0] + bbo[1]) / 2) if bbo else self.avg_px
        unreal = (mark - self.avg_px) * self.position if self.position != 0 else ZERO
        return arcus_pnl + self.realized + unreal

    # -- basis / cost model ----------------------------------------------------------- #
    def observe(self, now: float, arcus_bid: Optional[Decimal], arcus_ask: Optional[Decimal]) -> None:
        """Update the inter-venue basis and the Lighter crossing-cost estimates (call every loop)."""
        bbo = self.feed.book.bbo()
        if bbo is None or not arcus_bid or not arcus_ask or not self.feed_fresh(now):
            return
        b, a, _, _ = bbo
        lmid = (b + a) / 2
        amid = (arcus_bid + arcus_ask) / 2
        bnow = float((lmid - amid) / amid * BPS)
        hl = float((a - b) / 2 / lmid * BPS)
        self.basis_now = bnow
        if self._obs_ts is None or self.basis_ema is None:
            self.basis_ema, self.hl_ema = bnow, min(hl, 50.0)
        else:
            dt = max(0.0, now - self._obs_ts)
            alpha = 1.0 - math.exp(-dt * math.log(2) / max(1.0, self.cfg.hedge_basis_halflife_s))
            if abs(bnow - self.basis_ema) <= self.cfg.hedge_basis_glitch_bps:       # ignore glitches (seen up to 240 bps)
                self.basis_ema += alpha * (bnow - self.basis_ema)
            # the Lighter spread flickers (autocorr ~0.07 at 1s): use a SHORTER average, never the instantaneous touch
            a2 = 1.0 - math.exp(-dt * math.log(2) / max(1.0, self.cfg.hedge_basis_halflife_s / 5.0))
            self.hl_ema += a2 * (min(hl, 50.0) - self.hl_ema)
        self._obs_ts = now

    def basis_dev_bps(self) -> float:
        """Current basis minus its average (>0: Lighter is richer than usual)."""
        if self.basis_now is None or self.basis_ema is None:
            return 0.0
        return self.basis_now - self.basis_ema

    def expected_cost_bps(self) -> float:
        """Expected cost of ONE hedge relative to Lighter mid.
        taker style : smoothed half-spread + measured slippage.
        maker_first : after >= 5 posts, blend what we really measured: P(fill) x maker cost + (1-P) x taker-fallback cost."""
        taker = (self.hl_ema if self.hl_ema is not None else 0.0) + max(0.0, self.slip_ema)
        if self.style == "maker_first" and self.maker_attempts >= 5:
            p = self.maker_filled_attempts / max(1, self.maker_attempts)
            mk = self.cost_ema.get("maker")
            tk = self.cost_ema.get("taker")
            mk = mk if mk is not None else 0.0
            tk = tk if tk is not None else taker
            return max(0.0, p * mk + (1.0 - p) * max(tk, 0.0))
        return taker

    def edge_floors(self) -> tuple:
        """(buy_floor, sell_floor) in bps FROM FAIR VALUE that an Arcus quote needs so that, after hedging on Lighter,
        the cycle still earns HEDGE_TARGET_EDGE_BPS. Derivation: hedged edge = e +/- dev - cost per hedge, and every
        Arcus fill is hedged twice over its life (entry + exit) -> cost x HEDGE_COST_MULT (default 2).
        BUY Arcus/SELL Lighter gains when Lighter is richer (dev>0); SELL Arcus/BUY Lighter loses."""
        cost = self.expected_cost_bps() * float(self.cfg.hedge_cost_mult)
        dev = self.basis_dev_bps() * float(self.cfg.hedge_anchor_weight)
        tgt = float(self.cfg.hedge_target_edge_bps)
        return D(str(round(max(0.0, cost + tgt - dev), 3))), D(str(round(max(0.0, cost + tgt + dev), 3)))

    def record_cost(self, side: str, qty: Decimal, exec_px: Decimal, bbo, now: float, style: str = "taker") -> None:
        b, a, _, _ = bbo
        lmid = (a + b) / 2
        touch = a if side == BUY else b
        sgn = ONE if side == BUY else -ONE                    # cost is positive when we pay above / receive below
        cross_bps = float((exec_px - lmid) * sgn / lmid * BPS)
        slip_bps = float((exec_px - touch) * sgn / lmid * BPS)
        notional = qty * exec_px
        self.cost_n += 1
        self.cost_notional += notional
        self.cost_cross_usd += (exec_px - lmid) * sgn * qty
        self.cost_slip_usd += (exec_px - touch) * sgn * qty
        prev = self.cost_ema.get(style)
        self.cost_ema[style] = cross_bps if prev is None else prev + 0.2 * (cross_bps - prev)
        if style == "taker":
            self.slip_ema += 0.2 * (max(-5.0, min(20.0, slip_bps)) - self.slip_ema)
        log.info("HEDGE_COST %s %s %s @ %s | vs L_mid=%+.2fbps (spread %.2f + slip %+.2f) | basis=%s dev=%+.2f",
                 style, side, qty, exec_px, cross_bps, cross_bps - slip_bps, slip_bps,
                 ("%+.2f" % self.basis_now) if self.basis_now is not None else "-", self.basis_dev_bps())

    def report(self, now: float, force: bool = False) -> str:
        if not force and now - self._last_report < 60.0:
            return ""
        self._last_report = now
        n = self.cost_n
        avg = float((self.cost_cross_usd / self.cost_notional) * BPS) if self.cost_notional else 0.0
        fl = self.edge_floors()
        return (f"HEDGE_STATS n={n} notional=${float(self.cost_notional):.0f} avg_cost={avg:.2f}bps "
                f"cost_usd=${float(self.cost_cross_usd):.4f} | basis_avg={self.basis_ema if self.basis_ema is not None else 0:+.2f} "
                f"dev={self.basis_dev_bps():+.2f} Lhalf_spread_ema={self.hl_ema if self.hl_ema is not None else 0:.2f} "
                f"slip_ema={self.slip_ema:.2f} | maker_share={self.maker_share():.0%} (posts={self.maker_attempts}, "
                f"filled={self.maker_filled_attempts}) | edge_floor buy={fl[0]} sell={fl[1]}bps")

    def maker_share(self) -> float:
        tot = self.maker_qty + self.taker_qty
        return float(self.maker_qty / tot) if tot > 0 else 0.0

    # -- gating used by the quoting loop ------------------------------------------ #
    def add_block_reason(self, now: float) -> str:
        if not self.enabled:
            return ""
        if self.halted:
            return f"halted:{self.halted}"
        if not self.feed_fresh(now):
            return "lighter_feed_stale"
        mid = self.get_mid()
        if mid and abs(self.net_exposure()) * mid > self.cfg.hedge_max_unhedged_usd:
            return "unhedged_limit"
        if not self.budget_ok(now) and abs(self.delta()) * (mid or ONE) >= self.cfg.hedge_min_usd:
            return "tx_budget"
        return ""

    def locked_edge_bps(self, side: str, arcus_px: Decimal, mid: Decimal, neutral: bool = True) -> Optional[Decimal]:
        """Profit locked in (bps of mid) if we trade at arcus_px on Arcus and hedge on Lighter.
        raw      : uses today's Lighter touch (includes the persistent inter-venue basis, which is NOT earned: it is paid back
                   when the pair is unwound).
        neutral  : Lighter prices are converted to Arcus-equivalent by removing the average basis and the crossing cost
                   uses the smoothed half-spread (the touch flickers faster than a 300 ms taker can react)."""
        bbo = self.feed.book.bbo()
        if bbo is None or not mid:
            return None
        b, a, _, _ = bbo
        if not neutral or self.basis_ema is None:
            raw = (arcus_px - a) if side == SELL else (b - arcus_px)
            return raw / mid * BPS
        lmid = (a + b) / 2
        equiv_mid = lmid - mid * D(str(self.basis_ema)) / BPS           # Lighter mid expressed in Arcus terms
        cost = D(str(self.expected_cost_bps()))
        edge = ((arcus_px - equiv_mid) if side == SELL else (equiv_mid - arcus_px)) / mid * BPS
        return edge - cost

    # -- sizing / conversion --------------------------------------------------------- #
    def _qty_to_int(self, qty: Decimal) -> int:
        return int((qty * (D(10) ** self.feed.size_decimals)).to_integral_value(rounding=ROUND_DOWN))

    def _px_to_int(self, px: Decimal, round_up: bool) -> int:
        return int((px * (D(10) ** self.feed.price_decimals)).to_integral_value(rounding=ROUND_UP if round_up else ROUND_DOWN))

    def _min_qty(self) -> Decimal:
        step = D(1) / (D(10) ** self.feed.size_decimals)
        return max(step, self.feed.min_base_amount)

    # -- main tick --------------------------------------------------------------------- #
    async def tick(self, now: float) -> None:
        if not self.enabled or self._busy or self.halted:
            return
        if self.inflight != 0 and now - self.inflight_ts > self.cfg.hedge_confirm_timeout_s and self.mode == "live":
            self.fails += 1
            log.warning("[HEDGE] no position confirmation after %.1fs (fails=%d) - resetting in-flight %s",
                        now - self.inflight_ts, self.fails, self.inflight)
            self.inflight = ZERO
            if self.fails >= self.cfg.hedge_max_fails:
                self.halted = "confirm_failures"
                log.error("[HEDGE] HALTED after %d unconfirmed hedges - adds are blocked; check the Lighter account", self.fails)
            return
        if self.inflight != 0 and self.mode == "live":
            return
        if not self.feed_fresh(now) or not self.budget_ok(now):
            return
        mid = self.get_mid()
        bbo = self.feed.book.bbo()
        if not mid or bbo is None:
            return
        delta = self.delta()
        usd = abs(delta) * mid
        if abs(delta) < self._min_qty():
            self.exposure_since = None
            return
        if self.exposure_since is None:
            self.exposure_since = now
        aged = (now - self.exposure_since) >= self.cfg.hedge_max_age_s
        if usd < self.cfg.hedge_min_usd and not aged:
            return
        side = BUY if delta > 0 else SELL
        self._busy = True
        try:
            await self._send_hedge(side, abs(delta), bbo, now)
        finally:
            self._busy = False

    async def _send_hedge(self, side: str, qty: Decimal, bbo, now: float) -> None:
        if self.style == "maker_first":
            qty = await self._hedge_maker_first(side, qty, bbo, now)
            if qty <= 0:
                return
            now = self.now()
            bbo = self.feed.book.bbo() or bbo
        await self._hedge_taker(side, qty, bbo, now)

    def _maker_price(self, side: str, bbo) -> tuple:
        """Resting price inside the Lighter spread (never crossing). Returns (price, queue_ahead)."""
        b, a, _, _ = bbo
        tick = D(1) / (D(10) ** self.feed.price_decimals)
        imp = max(ZERO, min(ONE, self.cfg.hedge_maker_improve))
        spread = a - b
        if side == SELL:                       # hedge SELL rests as an ask
            p = a - spread * imp
            p = (p / tick).to_integral_value(rounding=ROUND_UP) * tick
            p = max(p, b + tick)
            if p >= a:
                return a, D(self.feed.book.asks.get(str(a), "0"))
            return p, ZERO
        p = b + spread * imp                   # hedge BUY rests as a bid
        p = (p / tick).to_integral_value(rounding=ROUND_DOWN) * tick
        p = min(p, a - tick)
        if p <= b:
            return b, D(self.feed.book.bids.get(str(b), "0"))
        return p, ZERO

    async def _hedge_maker_first(self, side: str, qty: Decimal, bbo, now: float) -> Decimal:
        """Post-only inside the Lighter spread (0% maker fee), wait up to HEDGE_MAKER_TIMEOUT_S, return the qty still unhedged.
        Paper: fills only when a REAL trade print on Lighter reaches our price (and, if we only joined the touch, after the
        queue ahead of us traded). Live: post-only order via the SDK; position confirmation comes from the account channel."""
        px, queue_ahead = self._maker_price(side, bbo)
        qty = (qty * (D(10) ** self.feed.size_decimals)).to_integral_value(rounding=ROUND_DOWN) / (D(10) ** self.feed.size_decimals)
        if qty < self._min_qty():
            return ZERO
        self.maker_attempts += 1
        self.tx_times.append(now)
        self.last_hedge_ts = now
        loop = asyncio.get_event_loop()
        lat = self.cfg.hedge_maker_latency_ms / 1000.0
        if self.mode == "live":
            return await self._maker_live(side, qty, px, bbo, now)
        await asyncio.sleep(lat)                               # Standard accounts: maker orders are delayed ~200 ms
        start_seq = self._trade_seq
        deadline = loop.time() + self.cfg.hedge_maker_timeout_s
        remaining, traded_at_px = qty, ZERO
        while remaining > 0 and loop.time() < deadline:
            for seq, tside, tsize, tpx in list(self._trades):
                if seq <= start_seq:
                    continue
                start_seq = seq
                if (side == SELL and tside == BUY and tpx >= px) or (side == BUY and tside == SELL and tpx <= px):
                    sz = tsize
                    if tpx == px and queue_ahead > 0:           # only joined the touch: the queue ahead trades first
                        eat = min(queue_ahead, sz)
                        queue_ahead -= eat
                        sz -= eat
                    fill = min(remaining, sz)
                    if fill > 0:
                        remaining -= fill
                        self.record_cost(side, fill, px, bbo, now, "maker")
                        self._apply_fill(side, fill, px, now)
                        self.maker_qty += fill
                        if remaining <= 0:
                            break
            await asyncio.sleep(0.02)
        self.tx_times.append(now)                                # the cancel
        if remaining < qty:
            self.maker_filled_attempts += 1
        log.info("[HEDGE maker] %s %s @ %s inside spread: filled %s / %s%s", side, qty, px, qty - remaining, qty,
                 "" if remaining <= 0 else " -> crossing the rest")
        return remaining

    async def _maker_live(self, side: str, qty: Decimal, px: Decimal, bbo, now: float) -> Decimal:
        if self.client is None:
            self.halted = "no_client"
            log.error("[HEDGE live] no Lighter client")
            return ZERO
        loop = asyncio.get_event_loop()
        self._coid += 1
        coid = self._coid
        sgn = ONE if side == BUY else -ONE
        pos0 = self.position
        self.inflight, self.inflight_ts = qty * sgn, now
        reduce_only = (self.position < 0 and side == BUY) or (self.position > 0 and side == SELL)
        if reduce_only and qty > abs(self.position):
            reduce_only = False
        try:
            c = self.client
            _, resp, err = await c.create_order(
                self.feed.market_id, coid, self._qty_to_int(qty), self._px_to_int(px, round_up=(side == SELL)), side == SELL,
                getattr(c, "ORDER_TYPE_LIMIT", 0), getattr(c, "ORDER_TIME_IN_FORCE_POST_ONLY", 2), reduce_only)
        except Exception as e:
            err, resp = str(e), None
        if err or (resp is not None and getattr(resp, "code", 200) != 200):
            self.inflight = ZERO
            self.fails += 1
            log.error("[HEDGE live maker] post-only %s %s @ %s rejected: %s (fails=%d)", side, qty, px, err or getattr(resp, "message", resp), self.fails)
            if self.fails >= self.cfg.hedge_max_fails:
                self.halted = "send_failures"
            return qty                                           # fall back to the taker path
        deadline = loop.time() + self.cfg.hedge_maker_timeout_s + self.cfg.hedge_maker_latency_ms / 1000.0
        while loop.time() < deadline and abs(self.position - pos0) < qty:
            await asyncio.sleep(0.05)
        self.tx_times.append(now)
        try:                                                     # cancel whatever is left of the order
            await self.client.cancel_order(self.feed.market_id, coid)
        except Exception as e:
            log.warning("[HEDGE live maker] cancel failed: %s", e)
        await asyncio.sleep(self.cfg.hedge_cancel_settle_s)      # let the position channel report a late fill
        self.inflight = ZERO
        filled = min(qty, abs(self.position - pos0))
        self.maker_qty += filled
        if filled > 0:
            self.maker_filled_attempts += 1
            self.record_cost(side, filled, px, bbo, now, "maker")
        log.info("[HEDGE live maker] %s %s @ %s: filled %s -> crossing the rest", side, qty, px, filled)
        return qty - filled

    async def _hedge_taker(self, side: str, qty: Decimal, bbo, now: float) -> None:
        b, a, _, _ = bbo
        slip = self.cfg.hedge_slippage_bps / BPS
        qty = (qty * (D(10) ** self.feed.size_decimals)).to_integral_value(rounding=ROUND_DOWN) / (D(10) ** self.feed.size_decimals)
        if qty < self._min_qty():
            return
        # worst acceptable price (slippage cap), never beyond the book if depth is too thin
        worst = (a * (ONE + slip)) if side == BUY else (b * (ONE - slip))
        reduce_only = (self.position < 0 and side == BUY) or (self.position > 0 and side == SELL)
        if reduce_only and qty > abs(self.position):
            reduce_only = False   # flips the position: cannot be reduce-only
        self.tx_times.append(now)
        self.last_hedge_ts = now
        if self.mode == "paper":
            await asyncio.sleep(self.cfg.hedge_latency_ms / 1000.0)
            px, filled = self.feed.book.vwap(side, qty)          # book AFTER the simulated delay
            if px is None or (side == BUY and px > worst) or (side == SELL and px < worst):
                log.warning("[HEDGE paper] %s %s would be cancelled (price beyond slippage cap %s, vwap=%s)",
                            side, qty, worst, px)
                self.fails += 1
                return
            self.record_cost(side, filled, px, bbo, now)
            self._apply_fill(side, filled, px, now)
            self.taker_qty += filled
            return
        # ---- live ----
        if self.client is None:
            self.halted = "no_client"
            log.error("[HEDGE live] no Lighter client (pip install lighter-sdk; set LIGHTER_API_PRIVATE_KEY / account index)")
            return
        self._coid += 1
        sgn = ONE if side == BUY else -ONE
        self.inflight, self.inflight_ts = qty * sgn, now
        try:
            _, resp, err = await self.client.create_market_order(
                self.feed.market_id, self._coid, self._qty_to_int(qty),
                self._px_to_int(worst, round_up=(side == BUY)), side == SELL, reduce_only)
        except Exception as e:  # network / SDK error
            err, resp = str(e), None
        if err or (resp is not None and getattr(resp, "code", 200) != 200):
            self.inflight = ZERO
            self.fails += 1
            log.error("[HEDGE live] %s %s rejected: %s (fails=%d)", side, qty, err or getattr(resp, "message", resp), self.fails)
            if self.fails >= self.cfg.hedge_max_fails:
                self.halted = "send_failures"
                log.error("[HEDGE] HALTED after %d failed hedges - adds are blocked", self.fails)
            return
        self.n_hedges += 1
        self.taker_qty += qty
        self.hedged_notional += qty * (a if side == BUY else b)
        log.info("[HEDGE live] sent %s %s worst=%s reduce_only=%s (waiting for position confirmation)", side, qty, worst, reduce_only)

    def _apply_fill(self, side: str, qty: Decimal, px: Decimal, now: float) -> None:
        sgn = ONE if side == BUY else -ONE
        old = self.position
        new = old + sgn * qty
        if old == 0 or (old > 0) == (sgn > 0):
            tot = abs(old) + qty
            self.avg_px = (self.avg_px * abs(old) + px * qty) / tot if tot else px
        else:
            closed = min(abs(old), qty)
            self.realized += (px - self.avg_px) * closed * (ONE if old > 0 else -ONE)
            if abs(new) > 0 and (new > 0) != (old > 0):
                self.avg_px = px
            elif new == 0:
                self.avg_px = ZERO
        self.position = new
        self.n_hedges += 1
        self.hedged_notional += qty * px
        self.exposure_since = None
        log.info("[HEDGE paper] %s %s @ %s | lighter_pos=%s net_exposure=%s realized=$%.4f", side, qty, px,
                 self.position, self.net_exposure(), float(self.realized))

    def describe(self, now: float) -> str:
        bbo = self.feed.book.bbo()
        age = (now - self.feed.last_bbo_ts) if self.feed.last_bbo_ts else -1
        return (f"hedge[{self.mode}] L_pos={self.position:.4f} net={self.net_exposure():.4f} "
                f"tx={self.budget_used(now)}/{min(60, int(self.cfg.hedge_max_tx_per_min))} "
                f"L_bbo={'%s/%s' % (bbo[0], bbo[1]) if bbo else '-'} age={age:.1f}s"
                f"{' HALTED:' + self.halted if self.halted else ''}")


def make_live_client(cfg: Any):
    """Official SDK client (pip install lighter-sdk). Chain id 466324 is chosen automatically for api.rh.lighter URLs."""
    import lighter  # type: ignore
    key = cfg.lighter_api_private_key
    if not key or cfg.lighter_account_index < 0:
        raise RuntimeError("LIGHTER_API_PRIVATE_KEY and LIGHTER_ACCOUNT_INDEX are required for HEDGE_MODE=live")
    client = lighter.SignerClient(url=cfg.lighter_url, account_index=cfg.lighter_account_index,
                                  api_private_keys={cfg.lighter_api_key_index: key})
    err = client.check_client()
    if err is not None:
        raise RuntimeError(f"Lighter API key check failed: {err}")
    return client
