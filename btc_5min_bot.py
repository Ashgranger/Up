"""
BTC 5-Minute Up/Down Fill-Detection Bot
========================================
Strategy per cycle:
  - Place UP@0.47 and DOWN@0.47 limit orders 4 min before market starts
  - Monitor via WebSocket until market ends (9 min window total)
  - Fill assumed when: ask <= 0.47 for either leg

WebSocket design (bulletproof):
  - Uses 'websockets' library (not aiohttp WS — aiohttp WS is flaky)
  - Single persistent connection with auto-reconnect
  - Sends subscribe message within 1s of connect (server closes after ~10s idle)
  - PING every 8s
  - On reconnect: re-subscribes all active tokens automatically
  - If no prices after subscribe: REST fallback for initial snapshot

Run:
  pip install websockets aiohttp python-dotenv
  python3 btc_5min_bot.py

Env: DRY_RUN=true (default). Set DRY_RUN=false + credentials for live trading.
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

GAMMA_API       = "https://gamma-api.polymarket.com"
CLOB_API        = "https://clob.polymarket.com"
WS_URL          = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

LIMIT_PRICE     = 0.47
FILL_ASK_MAX    = 0.47   # Fill assumed when ask drops to/below this
ORDER_SIZE      = 10
PRE_MARKET_SECS = 240    # Place orders 4 min before market start
PING_INTERVAL   = 8      # Send PING every 8s (server closes after ~10s idle)
RECONNECT_DELAY = 1      # Seconds before reconnect attempt

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("btc5m")

# ─── Shared price state ────────────────────────────────────────────────────────

class PriceStore:
    def __init__(self):
        self._data: dict[str, tuple[float, float]] = {}
        self.updated = asyncio.Event()

    def set(self, token: str, bid: float, ask: float):
        prev = self._data.get(token)
        self._data[token] = (bid, ask)
        if prev != (bid, ask):
            self.updated.set()
            self.updated.clear()

    def get(self, token: str) -> tuple[float, float]:
        return self._data.get(token, (0.0, 1.0))

    def clear(self, token: str):
        self._data.pop(token, None)


prices = PriceStore()

# Active subscriptions — ws_engine re-sends these on every reconnect
_active_tokens: set[str] = set()

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
    updates:     int   = 0


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
    def both_filled(self):
        return self.up.filled and self.down.filled

    def summary(self):
        u = f"FILLED({self.up.fill_reason})"   if self.up.filled   else "not filled"
        d = f"FILLED({self.down.fill_reason})"  if self.down.filled else "not filled"
        return (f"Cycle {self.cycle:>3} | {self.slug[-20:]} | "
                f"UP={u:<22} DOWN={d}  [WS: UP={self.up.updates} DN={self.down.updates}]")


# ─── Stats ─────────────────────────────────────────────────────────────────────

class Stats:
    def __init__(self):
        self.cycles = self.up_fills = self.down_fills = self.both_fills = 0
        self.results: list[CycleResult] = []

    def record(self, r: CycleResult):
        self.cycles += 1
        if r.up.filled:   self.up_fills   += 1
        if r.down.filled: self.down_fills += 1
        if r.both_filled: self.both_fills += 1
        self.results.append(r)

    def print(self):
        print("\n" + "="*72)
        print(f"  STATS  ({self.cycles} cycles)")
        print("="*72)
        for r in self.results:
            m = " ✓✓" if r.both_filled else (" ✓ " if (r.up.filled or r.down.filled) else "   ")
            print(f"  {m} {r.summary()}")
        print("-"*72)
        n = max(self.cycles, 1)
        print(f"  Total: {self.cycles}  UP: {self.up_fills}({self.up_fills/n*100:.0f}%)"
              f"  DOWN: {self.down_fills}({self.down_fills/n*100:.0f}%)"
              f"  Both: {self.both_fills}({self.both_fills/n*100:.0f}%)")
        print("="*72 + "\n")


stats = Stats()

# ─── Helpers ───────────────────────────────────────────────────────────────────

def now_ts() -> int:
    return int(time.time())

def ts_hms(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S UTC")

def next_market_start() -> int:
    now = now_ts()
    candidate = (now // 300) * 300 + 300
    while candidate - PRE_MARKET_SECS < now:
        candidate += 300
    return candidate

def slug_for(ts: int) -> str:
    return f"btc-updown-5m-{ts}"

async def http_get(session: aiohttp.ClientSession, url: str, params=None):
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
        r.raise_for_status()
        return await r.json()

# ─── WS message parser ─────────────────────────────────────────────────────────

def parse_ws(raw: str) -> list[tuple[str, float, float]]:
    """Parse WS message → list of (token, bid, ask)."""
    out = []
    try:
        msgs = json.loads(raw)
    except Exception:
        return out
    if isinstance(msgs, dict):
        msgs = [msgs]
    for msg in msgs:
        if not isinstance(msg, dict):
            continue
        et = msg.get("event_type")
        if et == "best_bid_ask":
            t = msg.get("asset_id") or msg.get("token_id")
            if t:
                out.append((t, float(msg.get("best_bid") or 0), float(msg.get("best_ask") or 1)))
        elif et == "price_change":
            for pc in msg.get("price_changes", []):
                t = pc.get("asset_id")
                b, a = pc.get("best_bid"), pc.get("best_ask")
                if t and b and a:
                    out.append((t, float(b), float(a)))
        elif et == "book":
            t = msg.get("asset_id")
            if t:
                bids = msg.get("bids", [])
                asks = msg.get("asks", [])
                b = float(bids[0]["price"]) if bids else 0.0
                a = float(asks[0]["price"]) if asks else 1.0
                out.append((t, b, a))
    return out

# ─── WebSocket engine ──────────────────────────────────────────────────────────

async def ws_engine(stop: asyncio.Event):
    """
    Persistent WS using 'websockets' library.
    Reconnects automatically. Re-subscribes all active tokens on reconnect.
    Sends subscribe message immediately on connect to prevent server-side close.
    """
    try:
        import websockets
    except ImportError:
        log.error("'websockets' not installed. Run: pip install websockets")
        return

    while not stop.is_set():
        try:
            log.info("WS connecting...")
            async with websockets.connect(
                WS_URL,
                ping_interval=None,   # we handle PING manually
                ping_timeout=None,
                close_timeout=5,
                open_timeout=10,
            ) as ws:
                log.info("WS connected ✓")

                # Send subscribe immediately — server closes after ~10s if nothing sent
                sub_tokens = list(_active_tokens)
                await ws.send(json.dumps({
                    "assets_ids": sub_tokens,
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                if sub_tokens:
                    log.info(f"WS subscribed {len(sub_tokens)} tokens")
                else:
                    log.debug("WS sent empty subscribe (keepalive)")

                last_ping = time.time()

                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=4.0)
                    except asyncio.TimeoutError:
                        # No message in 4s — send PING to stay alive
                        await ws.send("PING")
                        last_ping = time.time()
                        log.debug("WS PING (keepalive)")
                        continue
                    except Exception as e:
                        log.warning(f"WS recv error: {e}")
                        break

                    if time.time() - last_ping >= PING_INTERVAL:
                        await ws.send("PING")
                        last_ping = time.time()

                    if raw == "PONG":
                        log.debug("WS PONG")
                        continue

                    for token, bid, ask in parse_ws(raw):
                        prices.set(token, bid, ask)

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning(f"WS error: {type(e).__name__}: {e}")

        if not stop.is_set():
            log.info(f"WS reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)

    log.info("WS stopped")


async def subscribe(tokens: list[str]):
    """Register tokens so ws_engine re-subs them on reconnect."""
    _active_tokens.update(tokens)


async def unsubscribe(tokens: list[str]):
    """Remove tokens from active set and clear cached prices."""
    for t in tokens:
        _active_tokens.discard(t)
        prices.clear(t)

# ─── Market discovery ──────────────────────────────────────────────────────────

async def fetch_market(session: aiohttp.ClientSession, ts: int) -> Optional[MarketInfo]:
    slug = slug_for(ts)
    try:
        data = await http_get(session, f"{GAMMA_API}/markets", params={"slug": slug})
    except Exception as e:
        log.debug(f"Gamma error ({slug}): {e}")
        return None

    m = data[0] if isinstance(data, list) else data
    if not m:
        return None

    tokens = m.get("clobTokenIds", [])
    if isinstance(tokens, str):
        tokens = json.loads(tokens)
    if not tokens or len(tokens) < 2:
        return None

    outcomes = m.get("outcomes", '["Up","Down"]')
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    ui, di = 0, 1
    for i, o in enumerate(outcomes):
        s = str(o).lower()
        if s in ("up", "yes"):    ui = i
        elif s in ("down", "no"): di = i

    return MarketInfo(
        slug      = slug,
        condition = m.get("conditionId", ""),
        up_token  = tokens[ui],
        down_token= tokens[di],
        start_ts  = ts,
        end_ts    = ts + 300,
    )


async def wait_for_market(session: aiohttp.ClientSession, ts: int) -> MarketInfo:
    log.info(f"Waiting for market: {slug_for(ts)}")
    while True:
        m = await fetch_market(session, ts)
        if m:
            log.info(f"  UP   token: ...{m.up_token[-12:]}")
            log.info(f"  DOWN token: ...{m.down_token[-12:]}")
            return m
        await asyncio.sleep(5)

# ─── REST price fallback ────────────────────────────────────────────────────────

async def fetch_prices_rest(session: aiohttp.ClientSession, market: MarketInfo):
    """Fetch book via REST and seed PriceStore — used when WS snapshot is slow."""
    for token, name in [(market.up_token, "UP"), (market.down_token, "DOWN")]:
        try:
            data = await http_get(session, f"{CLOB_API}/book", params={"token_id": token})
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            bid = float(bids[0]["price"]) if bids else 0.0
            ask = float(asks[0]["price"]) if asks else 1.0
            prices.set(token, bid, ask)
            log.info(f"  REST snapshot {name}: bid={bid:.4f} ask={ask:.4f}")
        except Exception as e:
            log.warning(f"  REST snapshot {name} failed: {e}")

# ─── Fill check ────────────────────────────────────────────────────────────────

def check_fill(ask: float) -> tuple[bool, str]:
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
            api_key=os.environ["API_KEY"],
            api_secret=os.environ["API_SECRET"],
            api_passphrase=os.environ["API_PASSPHRASE"],
        )
        client = ClobClient(CLOB_API, key=os.environ["PRIVATE_KEY"], chain_id=137,
                            creds=creds, signature_type=2, funder=os.environ["FUNDER_ADDRESS"])
        opts = {"tick_size": "0.01", "neg_risk": False}
        loop = asyncio.get_event_loop()
        up_r, dn_r = await asyncio.gather(
            loop.run_in_executor(None, lambda: client.create_and_post_order(
                OrderArgs(token_id=market.up_token,   price=LIMIT_PRICE, size=ORDER_SIZE, side=BUY),
                options=opts, order_type=OrderType.GTC)),
            loop.run_in_executor(None, lambda: client.create_and_post_order(
                OrderArgs(token_id=market.down_token, price=LIMIT_PRICE, size=ORDER_SIZE, side=BUY),
                options=opts, order_type=OrderType.GTC)),
        )
        log.info(f"  UP   order: {up_r.get('orderID','?')}  status={up_r.get('status','?')}")
        log.info(f"  DOWN order: {dn_r.get('orderID','?')}  status={dn_r.get('status','?')}")
        return True
    except Exception as e:
        log.error(f"Order placement failed: {e}")
        return False

# ─── Monitor ───────────────────────────────────────────────────────────────────

async def monitor(session: aiohttp.ClientSession, cycle_num: int, market: MarketInfo) -> CycleResult:
    result = CycleResult(cycle=cycle_num, slug=market.slug, start_ts=market.start_ts)
    up   = LegState("UP")
    down = LegState("DOWN")

    log.info(f"  Monitoring → {ts_hms(market.end_ts)}  (fill: ask<={FILL_ASK_MAX})")
    last_log      = now_ts()
    last_rest_poll = 0   # timestamp of last REST poll during stale-price recovery

    while now_ts() < market.end_ts:
        # Wait for WS update or 0.5s timeout
        try:
            await asyncio.wait_for(prices.updated.wait(), timeout=0.5)
        except asyncio.TimeoutError:
            pass

        up_bid,  up_ask  = prices.get(market.up_token)
        dn_bid,  dn_ask  = prices.get(market.down_token)

        # Prices look like empty-book junk (0.01/0.99) — poll REST every 15s
        # until real liquidity appears (ask < 0.90 means real quotes exist)
        prices_stale = (up_ask >= 0.90 and dn_ask >= 0.90)
        if prices_stale and now_ts() - last_rest_poll >= 15:
            log.info("  Prices stale (empty book) — polling REST...")
            await fetch_prices_rest(session, market)
            last_rest_poll = now_ts()
            up_bid, up_ask = prices.get(market.up_token)
            dn_bid, dn_ask = prices.get(market.down_token)

        # Track changes
        if (up_bid, up_ask) != (up.bid, up.ask):
            up.bid, up.ask = up_bid, up_ask
            up.updates += 1
        if (dn_bid, dn_ask) != (down.bid, down.ask):
            down.bid, down.ask = dn_bid, dn_ask
            down.updates += 1

        t = time.time()
        remaining = market.end_ts - now_ts()

        if not up.filled:
            ok, reason = check_fill(up_ask)
            if ok:
                up.filled = True; up.fill_reason = reason; up.fill_time = t
                phase = "PRE" if now_ts() < market.start_ts else "LIVE"
                log.info(f"  ★ UP FILLED [{phase} {remaining}s left]  {reason}  "
                         f"bid={up_bid:.4f} ask={up_ask:.4f}")

        if not down.filled:
            ok, reason = check_fill(dn_ask)
            if ok:
                down.filled = True; down.fill_reason = reason; down.fill_time = t
                phase = "PRE" if now_ts() < market.start_ts else "LIVE"
                log.info(f"  ★ DOWN FILLED [{phase} {remaining}s left]  {reason}  "
                         f"bid={dn_bid:.4f} ask={dn_ask:.4f}")

        if up.filled and down.filled:
            log.info("  ✓ Both filled — done early")
            break

        if now_ts() - last_log >= 30:
            phase = "PRE" if now_ts() < market.start_ts else "LIVE"
            u_s = "filled" if up.filled   else f"bid={up_bid:.4f} ask={up_ask:.4f}"
            d_s = "filled" if down.filled else f"bid={dn_bid:.4f} ask={dn_ask:.4f}"
            log.info(f"  [{phase} {remaining:>3}s]  UP: {u_s}  DOWN: {d_s}  "
                     f"WS: UP={up.updates} DN={down.updates}")
            last_log = now_ts()

    result.up   = up
    result.down = down
    return result

# ─── One cycle ─────────────────────────────────────────────────────────────────

async def run_cycle(session: aiohttp.ClientSession, cycle_num: int):
    next_ts    = next_market_start()
    order_time = next_ts - PRE_MARKET_SECS

    log.info("")
    log.info(f"{'─'*64}")
    log.info(f"  CYCLE {cycle_num}  |  {slug_for(next_ts)}")
    log.info(f"  Market: {ts_hms(next_ts)} → {ts_hms(next_ts+300)}   Orders @ {ts_hms(order_time)}")
    log.info(f"{'─'*64}")

    # Sleep until order time
    wait = order_time - now_ts()
    if wait > 0:
        log.info(f"  Sleeping {wait:.0f}s...")
        await asyncio.sleep(wait)

    # Discover market
    market = await wait_for_market(session, next_ts)

    # Register tokens with WS engine
    await subscribe([market.up_token, market.down_token])
    log.info("  Waiting for WS book snapshot (up to 8s)...")

    # Wait up to 8s for real prices to arrive via WS
    deadline = time.time() + 8
    while time.time() < deadline:
        await asyncio.sleep(0.3)
        ub, ua = prices.get(market.up_token)
        db, da = prices.get(market.down_token)
        if ua < 1.0 or da < 1.0:
            break

    ua_now = prices.get(market.up_token)[1]
    da_now = prices.get(market.down_token)[1]

    # If still no WS prices — fetch via REST
    if ua_now >= 1.0 and da_now >= 1.0:
        log.warning("  WS snapshot not received — using REST fallback")
        await fetch_prices_rest(session, market)

    ub, ua = prices.get(market.up_token)
    db, da = prices.get(market.down_token)
    log.info(f"  Prices — UP: bid={ub:.4f} ask={ua:.4f}  DOWN: bid={db:.4f} ask={da:.4f}")

    # Place orders
    log.info(f"  Placing orders ({'DRY RUN' if DRY_RUN else 'LIVE'})...")
    ok = await place_orders(session, market)

    if not ok:
        result = CycleResult(cycle=cycle_num, slug=market.slug,
                             start_ts=market.start_ts, skip_reason="placement failed")
        stats.record(result)
        await unsubscribe([market.up_token, market.down_token])
        return

    # Monitor
    result = await monitor(session, cycle_num, market)
    result.orders_placed = True
    stats.record(result)

    # Cycle summary
    log.info(f"  ── Cycle {cycle_num} result ──")
    for leg in (result.up, result.down):
        s = f"FILLED ({leg.fill_reason})" if leg.filled else "NOT FILLED"
        log.info(f"  {leg.name:<4}: {s:<28} bid={leg.bid:.4f} ask={leg.ask:.4f}  WS={leg.updates}")
    stats.print()

    # Clean up and wait for market to end
    await unsubscribe([market.up_token, market.down_token])
    gap = market.end_ts - now_ts()
    if gap > 0:
        log.info(f"  Waiting {gap}s for market to close...")
        await asyncio.sleep(gap + 2)

# ─── Main ──────────────────────────────────────────────────────────────────────

async def main():
    # Check websockets is available
    try:
        import websockets
    except ImportError:
        print("ERROR: websockets not installed. Run: pip install websockets")
        return

    log.info("=" * 64)
    log.info("  BTC 5-Min Up/Down Bot")
    log.info(f"  Mode : {'DRY RUN' if DRY_RUN else '*** LIVE TRADING ***'}")
    log.info(f"  Fill : ask<={FILL_ASK_MAX}   Limit: {LIMIT_PRICE}   Size: {ORDER_SIZE}")
    log.info(f"  Pre-market: {PRE_MARKET_SECS}s   PING every {PING_INTERVAL}s")
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
    stats.print()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
