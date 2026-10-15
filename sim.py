"""Arcus exchange simulator driving the Level 7 Market Maker."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import types
from decimal import Decimal as D

# Stub websockets if not installed in offline environment
if "websockets" not in sys.modules:
    try:
        import websockets  # noqa
    except ImportError:
        stub = types.ModuleType("websockets")
        class ConnectionClosed(Exception): pass
        stub.ConnectionClosed = ConnectionClosed
        sys.modules["websockets"] = stub

import config as C
from bot import MarketMaker
from market import Market
from utils import fmt

ADDR = "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
MKT = Market(1, "BTC-USD", "ONLINE", D("0.1"), D("0.00000001"), [], D("5"), D("0.00001"), D("100"), D("0"), False)


def mkcfg(**env):
    base = {
        "ARCUS_ENV": "testnet",
        "ARCUS_WALLET_ADDRESS": ADDR,
        "ARCUS_API_SIGNING_KEY": "11" * 32,
        "DRY_RUN": "0",
        "JOURNAL_PATH": os.devnull,
        "LEARNING_STATE_PATH": os.devnull,
        "MAX_ACTIONS_PER_MIN": "100000",
        "MIN_REQUOTE_S": "0.5",
        "EXTRA_LEVELS": "1",
        "ENABLE_ADAPTIVE_EV": "1",
        "ENABLE_ORDERBOOK_INTEL": "1",
        "ENABLE_ONLINE_LEARNING": "1",
    }
    base.update({k: str(v) for k, v in env.items()})
    old = {k: os.environ.get(k) for k in base}
    os.environ.update(base)
    try:
        return C.Config.from_env()
    finally:
        for k, v in old.items():
            if v is None: os.environ.pop(k, None)
            else: os.environ[k] = v


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class SimWS:
    def __init__(self, bot):
        self.bot = bot
        self.orders = {}
        self.seq = 0
        self.bid = self.ask = None
        self.bid_sz = self.ask_sz = D("1")
        self.position = D(0)
        self.cash = D(0)
        self.rejects = 0
        self.posts = []
        self.max_open_per_side = 0

    def _reply(self, obj):
        asyncio.get_running_loop().call_soon(self.bot.ex.handle_message, json.dumps(obj))

    def _push(self, o, state, status, reason=None, rem=None):
        c = {
            "orderId": o["id"], "state": state, "status": status, "side": o["side"],
            "price": fmt(o["price"]), "remainingSize": fmt(o["rem"] if rem is None else rem)
        }
        if reason:
            c["rejectionReason" if state == "REJECTED" else "cancelReason"] = reason
        self._reply({"type": "channel_data", "channel": "orders", "id": ADDR, "contents": c})

    def _crosses(self, side, px):
        return (side == "BUY" and px >= self.ask) or (side == "SELL" and px <= self.bid)

    async def send(self, raw):
        m = json.loads(raw)
        if m["type"] == "post":
            self._post(m)
        elif m["type"] == "get":
            self._get(m)

    def _post(self, m):
        r = m["request"]
        t = r["type"]
        p = r["payload"]
        self.posts.append(t)
        if t == "placeOrder":
            self.seq += 1
            oid = f"sim-{self.seq}"
            o = {"id": oid, "side": p["orderSide"], "price": D(p["price"]), "rem": D(p["quantity"])}
            self._reply({"id": m["id"], "status": 202, "result": {"orderId": oid, "status": "ACK"}})
            if self._crosses(o["side"], o["price"]):
                tif = p.get("timeInForce", "ALO")
                if tif == "IOC":
                    q = o["rem"]
                    signed = q if o["side"] == "BUY" else -q
                    self.position += signed
                    self.cash -= signed * o["price"]
                    o["rem"] = D(0)
                    self._push(o, "FILLED", "FILLED", rem=D(0))
                    self._reply({"type": "channel_data", "channel": "positions", "id": ADDR,
                                 "contents": {"positions": [{"marketId": 1, "side": "LONG" if self.position >= 0 else "SHORT", "size": fmt(self.position)}]}})
                else:
                    self.rejects += 1
                    self._push(o, "REJECTED", "REJECTED", "POST_ONLY_WOULD_CROSS")
            else:
                self.orders[oid] = o
                self._push(o, "OPEN", "OPEN")
                for s in ("BUY", "SELL"):
                    self.max_open_per_side = max(self.max_open_per_side, sum(1 for x in self.orders.values() if x["side"] == s))
        elif t == "modifyOrder":
            o = self.orders.get(p["orderId"])
            if o is None:
                self._reply({"id": m["id"], "status": 404, "error": {"type": "ORDER_NOT_FOUND"}})
                return
            self._reply({"id": m["id"], "status": 202, "result": {"status": "ACK"}})
            o["price"] = D(p["price"])
            if self._crosses(o["side"], o["price"]):
                self.rejects += 1
                self.orders.pop(o["id"])
                self._push(o, "REJECTED", "REJECTED", "POST_ONLY_WOULD_CROSS")
            else:
                self._push(o, "OPEN", "OPEN")
        elif t == "cancelOrder":
            o = self.orders.pop(p["orderId"], None)
            if o is None:
                self._reply({"id": m["id"], "status": 404, "error": {"type": "ORDER_NOT_FOUND"}})
                return
            self._reply({"id": m["id"], "status": 202, "result": {"status": "ACK"}})
            self._push(o, "CANCELED", "CANCELED", rem=o["rem"])
        elif t == "cancelAllOrders":
            for o in list(self.orders.values()):
                self._push(o, "CANCELED", "CANCELED")
            self.orders.clear()
            self._reply({"id": m["id"], "status": 202, "result": {"status": "ACK"}})
        else:
            self._reply({"id": m["id"], "status": 202, "result": {"status": "ACK"}})

    def _get(self, m):
        t = m["request"]["type"]
        if t == "bbo":
            res = self._bbo()
        elif t == "positions":
            res = {"positions": {"1": {"marketId": 1, "side": "LONG" if self.position >= 0 else "SHORT", "size": fmt(self.position)}}}
        else:
            res = {"openOrders": [{"orderId": o["id"]} for o in self.orders.values()], "recentClosedOrders": []}
        self._reply({"id": m["id"], "status": 200, "result": res})

    def _bbo(self):
        return {"bestBid": {"price": fmt(self.bid), "size": fmt(self.bid_sz)},
                "bestAsk": {"price": fmt(self.ask), "size": fmt(self.ask_sz)}}

    def set_book(self, bid, ask, bsz="1", asz="1"):
        self.bid, self.ask = D(str(bid)), D(str(ask))
        self.bid_sz, self.ask_sz = D(str(bsz)), D(str(asz))
        for o in list(self.orders.values()):
            if (o["side"] == "BUY" and self.ask <= o["price"]) or (o["side"] == "SELL" and self.bid >= o["price"]):
                q = o["rem"]
                signed = q if o["side"] == "BUY" else -q
                self.position += signed
                self.cash -= signed * o["price"]
                self.orders.pop(o["id"])
                o["rem"] = D(0)
                self._push(o, "FILLED", "FILLED", rem=D(0))
                self._reply({"type": "channel_data", "channel": "positions", "id": ADDR,
                             "contents": {"positions": [{"marketId": 1, "side": "LONG" if self.position >= 0 else "SHORT", "size": fmt(self.position)}]}})
        self._reply({"type": "channel_data", "channel": "bbo", "id": "BTC-USD",
                     "contents": {"bestBid": {"price": fmt(self.bid), "size": fmt(self.bid_sz)},
                                  "bestAsk": {"price": fmt(self.ask), "size": fmt(self.ask_sz)}}})

    def push_trade(self, side: str, size: str, price: str):
        self._reply({"type": "channel_data", "channel": "trades", "id": "BTC-USD",
                     "contents": {"side": side, "quantity": size, "price": price}})

    def taker(self, side: str):
        if side == "BUY":
            cand = sorted([o for o in self.orders.values() if o["side"] == "SELL" and o["price"] <= self.ask],
                          key=lambda o: o["price"])
        else:
            cand = sorted([o for o in self.orders.values() if o["side"] == "BUY" and o["price"] >= self.bid],
                          key=lambda o: o["price"], reverse=True)
        for o in cand:
            q = o["rem"]
            signed = q if o["side"] == "BUY" else -q
            self.position += signed
            self.cash -= signed * o["price"]
            self.orders.pop(o["id"])
            o["rem"] = D(0)
            self._push(o, "FILLED", "FILLED", rem=D(0))
            self._reply({"type": "channel_data", "channel": "positions", "id": ADDR,
                         "contents": {"positions": [{"marketId": 1, "side": "LONG" if self.position >= 0 else "SHORT", "size": fmt(self.position)}]}})

    def pnl(self, mid):
        return self.cash + self.position * mid


def make(**env):
    cfg = mkcfg(**env)
    bot = MarketMaker(cfg)
    clock = Clock()
    bot.now = clock
    bot.md.info = MKT
    bot.md.info_ts = clock.t
    sim = SimWS(bot)
    bot.ex.ws = sim
    bot.om.maybe_orders = False
    return bot, sim, clock


async def step(bot, sim, clock, bid, ask, bsz="1", asz="1", dt=0.25, tick=True, keep_info=True):
    clock.t += dt
    if keep_info:
        bot.md.info_ts = clock.t
    sim.set_book(bid, ask, bsz, asz)
    for _ in range(3):
        await asyncio.sleep(0)
    if tick:
        await bot.tick()
    for _ in range(3):
        await asyncio.sleep(0)


async def run_standalone_sim(steps: int = 300, market_name: str = "NBIS-USD", base_px: float = 186.50) -> dict:
    """Runs an interactive multi-regime simulation of the full MarketMaker bot with live fills and learner updates."""
    import random, math, logging
    from utils import setup_logging, BUY, SELL
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    rng = random.Random(42)

    bot, s, clock = make(
        MARKET=market_name,
        ORDER_USD=os.getenv("ORDER_USD", "35"),
        MAX_POSITION_USD=os.getenv("MAX_POSITION_USD", "105"),
        EXTRA_LEVELS=os.getenv("EXTRA_LEVELS", "2"),
        MIN_EDGE_BPS=os.getenv("MIN_EDGE_BPS", "2.8"),
        LEVEL_SPACING_BPS=os.getenv("LEVEL_SPACING_BPS", "3.0"),
        SKEW_BPS=os.getenv("SKEW_BPS", "5.5"),
        MIN_EV_BPS=os.getenv("MIN_EV_BPS", "0.35"),
        MAKER_EXIT_FIRST=os.getenv("MAKER_EXIT_FIRST", "1"),
        ENABLE_TAKER_EXITS=os.getenv("ENABLE_TAKER_EXITS", "0"),
        ENABLE_SELECTIVE_TOUCH=os.getenv("ENABLE_SELECTIVE_TOUCH", "0"),
        STATUS_S="5.0",
    )
    tick = D("0.01") if base_px < 5000 else D("0.1")
    step_sz = D("0.0001")
    m = Market(46, market_name, "ONLINE", tick, step_sz, [], D("5"), step_sz, D("100000"), D(str(base_px)), False)
    bot.md.info = m
    s.mkt_id = m.market_id

    mid = base_px
    anchor = base_px
    for i in range(steps):
        # Mean-reverting Ornstein-Uhlenbeck process + occasional micro-bursts
        regime_phase = (i // 60) % 3
        vol = 0.04 if regime_phase != 1 else 0.09
        drift = -0.18 * (mid - anchor) + rng.gauss(0.0, vol)
        mid = max(1.0, round(mid + drift, 2 if tick == D("0.01") else 1))
        half_spr = float(tick) if rng.random() < 0.7 else float(tick) * 2.0
        bid_f = round(mid - half_spr, 2 if tick == D("0.01") else 1)
        ask_f = round(mid + half_spr, 2 if tick == D("0.01") else 1)
        if ask_f <= bid_f:
            ask_f = round(bid_f + float(tick) * 2.0, 2 if tick == D("0.01") else 1)

        bsz = f"{rng.uniform(3.0, 18.0):.2f}"
        asz = f"{rng.uniform(3.0, 18.0):.2f}"
        m.mark = D(str(round((bid_f + ask_f) * 0.5, 3)))

        # Push realistic L2 depth & trade prints
        bids_l2 = [[str(round(bid_f - k * float(tick), 2)), f"{rng.uniform(4.0, 20.0):.2f}"] for k in range(8)]
        asks_l2 = [[str(round(ask_f + k * float(tick), 2)), f"{rng.uniform(4.0, 20.0):.2f}"] for k in range(8)]
        bot.md.on_depth(bids_l2, asks_l2, clock.t)

        t_side = BUY if rng.random() < 0.5 else SELL
        t_px = ask_f if t_side == BUY else bid_f
        s.push_trade(t_side, f"{rng.uniform(0.2, 2.5):.3f}", str(t_px))

        await step(bot, s, clock, str(bid_f), str(ask_f), bsz=bsz, asz=asz, dt=0.5)

        # Simulate marketable retail/informed order flow hitting our quotes when competitive
        if s.orders and rng.random() < 0.45:
            hit_side = BUY if rng.random() < 0.5 else SELL
            cands = [o for o in s.orders.values() if o["side"] != hit_side]
            if cands:
                # Prefer filling closest order to mid
                cands.sort(key=lambda o: abs(float(o["price"]) - mid))
                best_o = cands[0]
                dist_bps = abs(float(best_o["price"]) - mid) / mid * 10000.0
                if rng.random() < math.exp(-0.22 * max(0.0, dist_bps - 1.0)):
                    q = best_o["rem"]
                    signed = q if best_o["side"] == "BUY" else -q
                    s.position += signed
                    s.cash -= signed * best_o["price"]
                    s.orders.pop(best_o["id"], None)
                    best_o["rem"] = D(0)
                    s._push(best_o, "FILLED", "FILLED", rem=D(0))
                    for _ in range(3):
                        await asyncio.sleep(0)

        bot._status_log(clock.t)

    summary = bot.ledger.learner.get_summary()
    final_mid = bot.md.mid or D(str(mid))
    res = {
        "market": market_name,
        "steps": steps,
        "fills": bot.ledger.n_fills,
        "buys": bot.ledger.n_buys,
        "sells": bot.ledger.n_sells,
        "volume_usd": float(bot.ledger.volume_usd),
        "spread_capture_usd": float(bot.ledger.spread_capture),
        "avg_edge_bps": float(bot.ledger.avg_edge_bps),
        "realized_pnl_usd": float(bot.ledger.realized),
        "total_pnl_usd": float(bot.ledger.total_pnl(final_mid)),
        "win_rate_pct": summary["win_rate"],
        "adverse_fill_rate_pct": summary["adverse_fill_rate"],
        "learner_updates": summary["total_updates"],
    }
    print("\n" + "=" * 72)
    print(f"SIMULATION COMPLETE | Market: {res['market']} | Steps: {res['steps']}")
    print(f"Fills: {res['fills']} ({res['buys']}B / {res['sells']}S) | Volume: ${res['volume_usd']:,.2f}")
    print(f"Spread Capture: ${res['spread_capture_usd']:+.4f} (avg {res['avg_edge_bps']:.2f} bps)")
    print(f"Realized PnL:   ${res['realized_pnl_usd']:+.4f} | Total PnL: ${res['total_pnl_usd']:+.4f}")
    print(f"Learner Updates: {res['learner_updates']} | Calibrated Win Rate: {res['win_rate_pct']:.1f}% | Adverse Rate: {res['adverse_fill_rate_pct']:.1f}%")
    print("=" * 72)
    return res


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Run standalone Arcus MM simulation")
    ap.add_argument("--steps", type=int, default=300, help="Number of simulation steps")
    ap.add_argument("--market", default=os.getenv("MARKET", "NBIS-USD"), help="Market symbol")
    ap.add_argument("--price", type=float, default=186.50, help="Initial mid price")
    args = ap.parse_args()
    asyncio.run(run_standalone_sim(steps=args.steps, market_name=args.market, base_px=args.price))
