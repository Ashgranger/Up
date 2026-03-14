"""
BTC 5-Minute Up/Down Fill-Detection Bot  (WebSocket edition)
=============================================================

Key upgrade: monitoring is now event-driven via Polymarket WebSocket.
  - REST polling (old): ~2s latency per price update
  - WebSocket (new):    <50ms latency, server pushes every top-of-book change

Architecture:
  - One persistent WebSocket connection runs the entire time
  - Subscribes to UP+DOWN tokens as soon as market is found
  - best_bid_ask events update shared state instantly
  - price_change events used as fast fallback for fills
  - book snapshot on subscribe gives immediate baseline
  - REST fetches only used for market discovery (Gamma API)

Strategy per cycle:
  1. Compute next 5-min market timestamp
  2. Pre-subscribe to tokens ~4 min before start via WS
  3. Place UP@0.47 and DOWN@0.47 at order_time
  4. WS events fire fill detection in real-time until market end
  5. Stats accumulated across all cycles

Fill conditions (per leg):
  bid <= 0.45   OR   ask <= 0.47

Run:
  python btc_5min_bot.py

Env:
  DRY_RUN=true   (default — no real orders)
  DRY_RUN=false  + PRIVATE_KEY, API_KEY, API_SECRET, API_PASSPHRASE, FUNDER_ADDRESS
"""

import asyncio
import json
import os
import time
import logging
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv

load_dotenv()

# ─── Config ────────────────────────────────────────────────────────────────────

GAMMA_API        = "https://gamma-api.polymarket.com"
CLOB_API         = "https://clob.polymarket.com"
WS_MARKET_URL    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

LIMIT_PRICE      = 0.47
FILL_ASK_MAX     = 0.47   # Fill assumed when ask drops to/below this
ORDER_SIZE       = 10
PRE_MARKET_SECS  = 240        # Subscribe + place orders 4 min before start
CYCLE_SECS       = 300
WS_PING_INTERVAL = 10         # Polymarket requires PING every 10s
WS_RECONNECT_DELAY = 2
MONITOR_TICK     = 0.05       # 50ms inner loop — reacts within one event loop tick

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("btc5m")

# ─── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class MarketInfo:
    slug:       str
    condition:  str
    up_token:   str
    down_token: str
    start_ts:   int
    end_ts:     int


@dataclass
class LegState:
    name:        str
    filled:      bool  = False
    fill_reason: str   = ""
    fill_time:   float = 0.0
    bid:         float = 0.0
    ask:         float = 1.0
    updates:     int   = 0    # WS price updates received


@dataclass
class CycleResult:
    cycle:         int
    slug:          str
    start_ts:      int
    up:            LegState = field(default_factory=lambda: LegState("UP"))
    down:          LegState = field(default_factory=lambda: LegState("DOWN"))
    orders_placed: bool = False
    skip_reason:   str  = ""

    @property
    def both_filled(self) -> bool:
        return self.up.filled and self.down.filled

    def summary(self) -> str:
        up_s   = f"FILLED({self.up.fill_reason})"   if self.up.filled   else "not filled"
        down_s = f"FILLED({self.down.fill_reason})"  if self.down.filled else "not filled"
        return (f"Cycle {self.cycle:>3} | {self.slug[-20:]} | "
                f"UP={up_s:<22} DOWN={down_s}  "
                f"[WS: UP={self.up.updates} DN={self.down.updates}]")


# ─── Stats ─────────────────────────────────────────────────────────────────────

class Stats:
    def __init__(self):
        self.cycles     = 0
        self.up_fills   = 0
        self.down_fills = 0
        self.both_fills = 0
        self.results: list[CycleResult] = []

    def record(self, r: CycleResult):
        self.cycles += 1
        if r.up.filled:   self.up_fills   += 1
        if r.down.filled: self.down_fills += 1
        if r.both_filled: self.both_fills += 1
        self.results.append(r)

    def print_summary(self):
        print("\n" + "="*75)
        print(f"  CUMULATIVE STATS  ({self.cycles} cycles)")
        print("="*75)
        for r in self.results:
            m = " ✓✓" if r.both_filled else (" ✓ " if (r.up.filled or r.down.filled) else "   ")
            print(f"  {m} {r.summary()}")
        print("-"*75)
        n = max(self.cycles, 1)
        print(f"  Total cycles : {self.cycles}")
        print(f"  UP filled    : {self.up_fills}  ({self.up_fills/n*100:.0f}%)")
        print(f"  DOWN filled  : {self.down_fills}  ({self.down_fills/n*100:.0f}%)")
        print(f"  Both filled  : {self.both_fills}  ({self.both_fills/n*100:.0f}%)")
        print("="*75 + "\n")


stats = Stats()

# ─── Shared WebSocket price state ──────────────────────────────────────────────

class WSPriceState:
    """
    Thread-safe price store updated by ws_engine, read by monitor_ws.
    Uses asyncio.Event to wake up monitor instantly on any price change.
    """
    def __init__(self):
        self._prices: dict[str, tuple[float, float]] = {}
        self._lock = asyncio.Lock()
        self.changed = asyncio.Event()  # set on every update, cleared by monitor

    async def update(self, token_id: str, bid: float, ask: float):
        async with self._lock:
            prev = self._prices.get(token_id)
            self._prices[token_id] = (bid, ask)
        if prev != (bid, ask):          # only wake monitor on actual change
            self.changed.set()

    def get(self, token_id: str) -> tuple[float, float]:
        return self._prices.get(token_id, (0.0, 1.0))


ws_prices = WSPriceState()

# Outgoing message queue — anything put here gets sent over the WS
_ws_out: asyncio.Queue = asyncio.Queue()
_subscribed: set[str]  = set()


async def ws_subscribe(tokens: list[str]):
    new = [t for t in tokens if t not in _subscribed]
    if not new:
        return
    _subscribed.update(new)
    await _ws_out.put(json.dumps({
        "assets_ids": new,
        "type": "market",
        "custom_feature_enabled": True,   # enables best_bid_ask events
    }))
    log.debug(f"WS subscribe: {[t[-8:] for t in new]}")


async def ws_unsubscribe(tokens: list[str]):
    for t in tokens:
        _subscribed.discard(t)
    await _ws_out.put(json.dumps({
        "assets_ids": tokens,
        "operation": "unsubscribe",
    }))

# ─── WebSocket engine (background task) ────────────────────────────────────────

def _extract_prices(raw: str) -> list[tuple[str, float, float]]:
    """
    Parse WS message → list of (token_id, bid, ask).
    Handles best_bid_ask (fastest), price_change, book (snapshot).
    Polymarket sends events as a JSON array — we iterate over each item.
    """
    results = []
    try:
        parsed = json.loads(raw)
    except Exception:
        return results

    # Polymarket wraps all events in a list: [{event_type: ...}, ...]
    msgs = parsed if isinstance(parsed, list) else [parsed]

    for msg in msgs:
        if not isinstance(msg, dict):
            continue
        etype = msg.get("event_type")
        _parse_one(msg, etype, results)

    return results


def _parse_one(msg: dict, etype: str, results: list):
    if etype == "best_bid_ask":
        # Dedicated top-of-book event — lowest latency path
        token = msg.get("asset_id") or msg.get("token_id")
        if token:
            bid = float(msg.get("best_bid") or 0)
            ask = float(msg.get("best_ask") or 1)
            results.append((token, bid, ask))

    elif etype == "price_change":
        for pc in msg.get("price_changes", []):
            token = pc.get("asset_id")
            if not token:
                continue
            bid_s = pc.get("best_bid", "")
            ask_s = pc.get("best_ask", "")
            if bid_s and ask_s:
                bid = float(bid_s)
                ask = float(ask_s)
                if bid > 0 or ask < 1:
                    results.append((token, bid, ask))

    elif etype == "book":
        # Full book snapshot sent right after subscribing
        token = msg.get("asset_id")
        if token:
            bids = msg.get("bids", [])
            asks = msg.get("asks", [])
            bid = float(bids[0]["price"]) if bids else 0.0
            ask = float(asks[0]["price"]) if asks else 1.0
            results.append((token, bid, ask))


async def ws_engine(stop: asyncio.Event):
    """
    Runs forever (until stop is set). Reconnects on any error.
    Pumps incoming price data into ws_prices.
    Drains _ws_out queue for subscribe/unsubscribe messages.
    """
    while not stop.is_set():
        try:
            log.info("WS connecting...")
            async with aiohttp.ClientSession() as sess:
                async with sess.ws_connect(
                    WS_MARKET_URL,
                    receive_timeout=30,
                    heartbeat=None,
                ) as ws:
                    log.info("WS connected ✓")

                    # Re-subscribe on reconnect
                    if _subscribed:
                        await ws.send_str(json.dumps({
                            "assets_ids": list(_subscribed),
                            "type": "market",
                            "custom_feature_enabled": True,
                        }))

                    # Sender coroutine drains the outgoing queue
                    async def _sender():
                        while True:
                            msg = await _ws_out.get()
                            try:
                                await ws.send_str(msg)
                            except Exception:
                                await _ws_out.put(msg)   # re-queue for next connection
                                return

                    # Pinger keeps connection alive
                    async def _pinger():
                        while True:
                            await asyncio.sleep(WS_PING_INTERVAL)
                            try:
                                await ws.send_str("PING")
                            except Exception:
                                return

                    t_send = asyncio.create_task(_sender())
                    t_ping = asyncio.create_task(_pinger())

                    try:
                        async for wsmsg in ws:
                            if stop.is_set():
                                break
                            if wsmsg.type == aiohttp.WSMsgType.TEXT:
                                if wsmsg.data == "PONG":
                                    continue
                                for token, bid, ask in _extract_prices(wsmsg.data):
                                    await ws_prices.update(token, bid, ask)
                            elif wsmsg.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                log.warning(f"WS msg type={wsmsg.type} — reconnecting")
                                break
                    finally:
                        t_send.cancel()
                        t_ping.cancel()

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning(f"WS engine error: {e}")

        if not stop.is_set():
            await asyncio.sleep(WS_RECONNECT_DELAY)

    log.info("WS engine stopped")

# ─── Helpers ───────────────────────────────────────────────────────────────────

def now_ts() -> int:
    return int(time.time())

def ts_to_hms(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S UTC")

def next_5min_start() -> int:
    """Return the next 5-min boundary whose order window hasn't passed yet."""
    now = now_ts()
    candidate = (now // 300) * 300 + 300
    # If order window (start - PRE_MARKET_SECS) is already behind us, skip ahead
    while candidate - PRE_MARKET_SECS < now:
        candidate += 300
    return candidate

def slug_for_ts(ts: int) -> str:
    return f"btc-updown-5m-{ts}"

async def http_get(session: aiohttp.ClientSession, url: str, params: dict = None):
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
        r.raise_for_status()
        return await r.json()

# ─── Market discovery (REST) ───────────────────────────────────────────────────

async def fetch_market(session: aiohttp.ClientSession, ts: int) -> Optional[MarketInfo]:
    slug = slug_for_ts(ts)
    try:
        data = await http_get(session, f"{GAMMA_API}/markets", params={"slug": slug})
    except Exception as e:
        log.debug(f"Gamma error ({slug}): {e}")
        return None

    market = data[0] if isinstance(data, list) else data
    if not market:
        return None

    raw_tokens = market.get("clobTokenIds", [])
    if isinstance(raw_tokens, str):
        raw_tokens = json.loads(raw_tokens)
    if not raw_tokens or len(raw_tokens) < 2:
        return None

    raw_outcomes = market.get("outcomes", '["Up","Down"]')
    if isinstance(raw_outcomes, str):
        raw_outcomes = json.loads(raw_outcomes)

    up_idx, down_idx = 0, 1
    for i, o in enumerate(raw_outcomes):
        s = str(o).lower()
        if s in ("up", "yes"):    up_idx   = i
        elif s in ("down", "no"): down_idx = i

    return MarketInfo(
        slug       = slug,
        condition  = market.get("conditionId", ""),
        up_token   = raw_tokens[up_idx],
        down_token = raw_tokens[down_idx],
        start_ts   = ts,
        end_ts     = ts + 300,
    )


async def wait_for_market(session: aiohttp.ClientSession, ts: int) -> MarketInfo:
    log.info(f"Waiting for market: {slug_for_ts(ts)}")
    while True:
        m = await fetch_market(session, ts)
        if m:
            log.info(f"  UP   token: ...{m.up_token[-12:]}")
            log.info(f"  DOWN token: ...{m.down_token[-12:]}")
            return m
        await asyncio.sleep(5)

# ─── Fill logic ────────────────────────────────────────────────────────────────

def check_fill(bid: float, ask: float) -> tuple[bool, str]:
    # Fill assumed only when ask <= FILL_ASK_MAX (someone willing to sell at/below our limit)
    if 0 < ask <= FILL_ASK_MAX:
        return True, f"ask<={FILL_ASK_MAX}"
    return False, ""

# ─── Order placement ───────────────────────────────────────────────────────────

async def place_orders(session: aiohttp.ClientSession, market: MarketInfo) -> bool:
    if DRY_RUN:
        log.info(f"  [DRY RUN] BUY UP@{LIMIT_PRICE}   size={ORDER_SIZE}  ...{market.up_token[-8:]}")
        log.info(f"  [DRY RUN] BUY DOWN@{LIMIT_PRICE}  size={ORDER_SIZE}  ...{market.down_token[-8:]}")
        return True
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType, ApiCreds
        from py_clob_client.order_builder.constants import BUY

        creds = ApiCreds(
            api_key        = os.environ["API_KEY"],
            api_secret     = os.environ["API_SECRET"],
            api_passphrase = os.environ["API_PASSPHRASE"],
        )
        client = ClobClient(
            host           = CLOB_API,
            key            = os.environ["PRIVATE_KEY"],
            chain_id       = 137,
            creds          = creds,
            signature_type = 2,
            funder         = os.environ["FUNDER_ADDRESS"],
        )
        opts = {"tick_size": "0.01", "neg_risk": False}
        loop = asyncio.get_event_loop()
        up_r, dn_r = await asyncio.gather(
            loop.run_in_executor(None, lambda: client.create_and_post_order(
                OrderArgs(token_id=market.up_token,   price=LIMIT_PRICE,
                          size=ORDER_SIZE, side=BUY),
                options=opts, order_type=OrderType.GTC)),
            loop.run_in_executor(None, lambda: client.create_and_post_order(
                OrderArgs(token_id=market.down_token, price=LIMIT_PRICE,
                          size=ORDER_SIZE, side=BUY),
                options=opts, order_type=OrderType.GTC)),
        )
        log.info(f"  UP   order: {up_r.get('orderID','?')}  status={up_r.get('status','?')}")
        log.info(f"  DOWN order: {dn_r.get('orderID','?')}  status={dn_r.get('status','?')}")
        return True
    except Exception as e:
        log.error(f"Order placement failed: {e}")
        return False

# ─── WS-driven monitor ─────────────────────────────────────────────────────────

async def monitor_ws(cycle_num: int, market: MarketInfo) -> CycleResult:
    """
    Event-driven monitoring.  Instead of sleeping N seconds between polls,
    the inner loop wakes immediately whenever ws_prices.changed fires —
    which happens on every best_bid_ask / price_change WS event (~ms latency).
    Falls back to MONITOR_TICK (50ms) timeout if no event arrives.
    """
    result = CycleResult(cycle=cycle_num, slug=market.slug, start_ts=market.start_ts)
    up   = LegState("UP")
    down = LegState("DOWN")

    log.info(f"  WS monitoring → {ts_to_hms(market.end_ts)}  (fill: ask<={FILL_ASK_MAX})")

    last_status_log = now_ts()

    while now_ts() < market.end_ts:
        # Wait for a WS price update OR MONITOR_TICK timeout — whichever comes first
        try:
            await asyncio.wait_for(ws_prices.changed.wait(), timeout=MONITOR_TICK)
            ws_prices.changed.clear()
        except asyncio.TimeoutError:
            pass

        remaining = market.end_ts - now_ts()
        t = time.time()

        up_bid,  up_ask  = ws_prices.get(market.up_token)
        dn_bid,  dn_ask  = ws_prices.get(market.down_token)

        # Count actual price changes (not just wakeups)
        if (up_bid, up_ask) != (up.bid, up.ask):
            up.bid, up.ask = up_bid, up_ask
            up.updates += 1

        if (dn_bid, dn_ask) != (down.bid, down.ask):
            down.bid, down.ask = dn_bid, dn_ask
            down.updates += 1

        # Fill checks
        if not up.filled:
            ok, reason = check_fill(up_bid, up_ask)
            if ok:
                up.filled = True; up.fill_reason = reason; up.fill_time = t
                phase = "PRE" if now_ts() < market.start_ts else "LIVE"
                log.info(f"  ★ UP FILLED  [{phase} +{(t - market.start_ts):.1f}s / {remaining}s left]  "
                         f"{reason}  bid={up_bid:.4f} ask={up_ask:.4f}")

        if not down.filled:
            ok, reason = check_fill(dn_bid, dn_ask)
            if ok:
                down.filled = True; down.fill_reason = reason; down.fill_time = t
                phase = "PRE" if now_ts() < market.start_ts else "LIVE"
                log.info(f"  ★ DOWN FILLED [{phase} +{(t - market.start_ts):.1f}s / {remaining}s left]  "
                         f"{reason}  bid={dn_bid:.4f} ask={dn_ask:.4f}")

        # Periodic status — every 30s
        if now_ts() - last_status_log >= 30:
            phase = "PRE" if now_ts() < market.start_ts else "LIVE"
            u = "filled" if up.filled   else f"bid={up_bid:.4f} ask={up_ask:.4f}"
            d = "filled" if down.filled else f"bid={dn_bid:.4f} ask={dn_ask:.4f}"
            log.info(f"  [{phase} {remaining:>3}s left]  UP: {u}  DOWN: {d}  "
                     f"WS events: UP={up.updates} DN={down.updates}")
            last_status_log = now_ts()

        if up.filled and down.filled:
            log.info("  ✓ Both legs filled — exiting monitor early")
            break

    result.up   = up
    result.down = down
    return result

# ─── One full cycle ────────────────────────────────────────────────────────────

async def run_cycle(session: aiohttp.ClientSession, cycle_num: int):
    next_ts    = next_5min_start()
    order_time = next_ts - PRE_MARKET_SECS

    log.info("")
    log.info(f"{'─'*64}")
    log.info(f"  CYCLE {cycle_num}  |  {slug_for_ts(next_ts)}")
    log.info(f"  Market start : {ts_to_hms(next_ts)}   end: {ts_to_hms(next_ts+300)}")
    log.info(f"  Order time   : {ts_to_hms(order_time)}")
    log.info(f"{'─'*64}")

    wait = order_time - now_ts()
    if wait > 0:
        log.info(f"  Sleeping {wait:.0f}s...")
        await asyncio.sleep(wait)

    # Discover market via REST
    market = await wait_for_market(session, next_ts)

    # Subscribe BEFORE placing orders — book snapshot arrives immediately
    await ws_subscribe([market.up_token, market.down_token])
    log.info("  WS subscribed — book snapshot incoming...")
    await asyncio.sleep(0.3)   # brief settle for snapshot

    up_b, up_a = ws_prices.get(market.up_token)
    dn_b, dn_a = ws_prices.get(market.down_token)
    log.info(f"  Initial prices — UP: bid={up_b:.4f} ask={up_a:.4f}  "
             f"DOWN: bid={dn_b:.4f} ask={dn_a:.4f}")

    # Place orders
    log.info(f"  Placing orders ({'DRY RUN' if DRY_RUN else 'LIVE'})...")
    ok = await place_orders(session, market)

    if not ok:
        result = CycleResult(cycle=cycle_num, slug=market.slug,
                             start_ts=market.start_ts, skip_reason="placement failed")
        stats.record(result)
        await ws_unsubscribe([market.up_token, market.down_token])
        return

    # Monitor via WS events
    result = await monitor_ws(cycle_num, market)
    result.orders_placed = True
    stats.record(result)

    # Clean up subscription
    await ws_unsubscribe([market.up_token, market.down_token])

    # Cycle summary
    log.info(f"  ── Cycle {cycle_num} result ──")
    for leg in (result.up, result.down):
        status = f"FILLED ({leg.fill_reason})" if leg.filled else "NOT FILLED"
        log.info(f"  {leg.name:<4} : {status:<28}  "
                 f"last bid={leg.bid:.4f} ask={leg.ask:.4f}  WS={leg.updates}")
    stats.print_summary()

    # Wait until this market fully ends before computing the next target.
    # Without this, next_5min_start() could return the same market again.
    gap = market.end_ts - now_ts()
    if gap > 0:
        log.info(f"  Market ends in {gap}s — waiting...")
        await asyncio.sleep(gap + 2)   # +2s buffer past market close

# ─── Main ──────────────────────────────────────────────────────────────────────

async def main():
    log.info("=" * 64)
    log.info("  BTC 5-Min Up/Down Bot  —  WebSocket Edition")
    log.info(f"  Mode       : {'DRY RUN (no real orders)' if DRY_RUN else '*** LIVE TRADING ***'}")
    log.info(f"  Limit      : {LIMIT_PRICE}   Fill condition: ask<={FILL_ASK_MAX}")
    log.info(f"  Size       : {ORDER_SIZE} shares/leg   Pre-market: {PRE_MARKET_SECS}s")
    log.info(f"  WS latency : event-driven (~ms vs 2s polling)")
    log.info("=" * 64)

    stop = asyncio.Event()
    ws_task = asyncio.create_task(ws_engine(stop))

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        cycle = 1
        try:
            while True:
                try:
                    await run_cycle(session, cycle)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.error(f"Cycle {cycle} error: {e}", exc_info=True)
                cycle += 1
                await asyncio.sleep(1)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass

    stop.set()
    ws_task.cancel()
    try:
        await ws_task
    except asyncio.CancelledError:
        pass

    log.info("Stopped.")
    stats.print_summary()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
