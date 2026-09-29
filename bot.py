"""Orchestrator: wires exchange <-> market data <-> strategy <-> orders <-> ledger."""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
import urllib.error
from collections import deque
from decimal import Decimal
from typing import Any, Optional

import websockets

from config import Config
from exchange import Exchange
from ledger import Ledger
from market import Market, MarketData
from orders import OrderManager
from signer import Signer
from strategy import Snapshot, Strategy
from utils import BPS, BUY, SELL, ZERO, Fatal, fmt

log = logging.getLogger("bot")
_NOTE_NUM = re.compile(r"[-+]?\d+\.?\d*")  # strips drifting bps figures for PLAN-log dedup


# --------------------------------------------------------------------------- #
# positions payload helpers (frame shapes are only partly documented -> defensive)
# --------------------------------------------------------------------------- #
def extract_positions(c: Any) -> list:
    if isinstance(c, list):
        return [r for r in c if isinstance(r, dict)]
    if isinstance(c, dict):
        if "positions" in c:
            p = c["positions"]
            if isinstance(p, dict):
                return [r for r in p.values() if isinstance(r, dict)]
            if isinstance(p, list):
                return [r for r in p if isinstance(r, dict)]
            return []
        if "marketId" in c:
            return [c]
    return []


def parse_size(row: dict) -> Decimal:
    size = Decimal(str(row.get("size", "0")))
    side = str(row.get("side", "")).upper()
    if side == "FLAT":
        return ZERO
    if side == "SHORT" and size > 0:
        size = -size
    return size


class MarketMaker:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.now = time.monotonic          # injectable clock (tests)
        self.signer = Signer(cfg.signing_key, cfg.address, cfg.account_index)
        self.ex = Exchange(cfg, self.on_channel)
        self.md = MarketData(cfg)
        self.ledger = Ledger(cfg)
        self.strategy = Strategy(cfg)
        self.om = OrderManager(cfg, self.ex, self.signer, lambda: self.md.info, self.on_fill)
        self.stop_evt = asyncio.Event()
        self.wake = asyncio.Event()
        self.halted = False
        self.cooldown = {BUY: 0.0, SELL: 0.0}
        self._recent: deque = deque(maxlen=200)      # (ts, side) of recent fills (burst + sweep guards)
        self._last_status_regime = "REGIME_A_QUIET"
        self._last_reason = ""
        self._last_notes: tuple = ()
        self.dms_ok = True
        self._journal = None

    # ------------------------------------------------------------------------ #
    # inbound data
    # ------------------------------------------------------------------------ #
    def on_channel(self, ch: str, contents: Any, snapshot: bool) -> None:
        now = self.now()
        if ch == "bbo":
            self.on_bbo(contents, now)
        elif ch == "l2OrderbookUpdates":
            self.on_book(contents, snapshot, now)
        elif ch == "trades":
            if not snapshot:                          # a snapshot may replay old history: don't stamp it as 'now'
                for tr in (contents if isinstance(contents, list) else [contents]):
                    self.on_trade(tr, now)
        elif ch == "orders":
            if not snapshot:
                for c in (contents if isinstance(contents, list) else [contents]):
                    self.om.on_update(c, now)
        elif ch == "positions":
            self.on_positions(contents, snapshot, now)

    def on_book(self, c: Any, snapshot: bool, now: float) -> None:
        for row in (c if isinstance(c, list) else [c]):
            if self.md.on_book(row, snapshot, now):
                log.warning("l2OrderbookUpdates sequence went backwards - resubscribing for a fresh snapshot")
                asyncio.ensure_future(self.ex.subscribe("l2OrderbookUpdates", self.cfg.market))
                return
        self.wake.set()

    def on_trade(self, tr: Any, now: float) -> None:
        """Public trade tape -> trade-flow imbalance (Level 5). Defensive about field names."""
        if not isinstance(tr, dict):
            return
        try:
            side = str(tr.get("side") or tr.get("orderSide") or tr.get("takerSide") or "BUY").upper()
            sz = Decimal(str(tr.get("size") or tr.get("quantity") or "0"))
            px = Decimal(str(tr.get("price") or "0"))
        except Exception:
            return
        if sz > 0:
            self.md.on_trade(side, sz, px, now)

    def on_bbo(self, c: Any, now: float) -> None:
        if not isinstance(c, dict) or "bestBid" not in c:
            return
        bb, ba = c.get("bestBid"), c.get("bestAsk")
        if not bb or not ba:
            self.md.clear_book()
            return

        def size(x) -> Optional[Decimal]:
            try:
                return Decimal(str(x["size"])) if x.get("size") not in (None, "") else None
            except Exception:
                return None

        self.md.update(Decimal(str(bb["price"])), Decimal(str(ba["price"])), size(bb), size(ba), now)
        self.wake.set()

    def on_positions(self, c: Any, snapshot: bool, now: float) -> None:
        m, mid = self.md.info, self.md.mid
        if m is None or mid is None:
            return
        found = None
        for row in extract_positions(c):
            try:
                same = int(row.get("marketId", -1)) == m.market_id
            except (TypeError, ValueError):
                same = False
            if same or row.get("marketDisplayName") == self.cfg.market:
                found = parse_size(row)
        if found is None and snapshot:
            found = ZERO
        if found is not None and self.ledger.reconcile(found, now, mid, m.min_notional):
            log.warning("LEDGER RESYNC: position now %s (exchange truth)", fmt(found))

    def on_fill(self, side: str, qty: Decimal, price: Decimal, order) -> None:
        now = self.now()
        mid = self.md.mid or price
        f = self.ledger.on_fill(side, qty, price, mid, now, self.md.info.min_notional)
        log.info("FILL %s %s @ %s | edge %+.2fbps | position %s | realized %+.5f",
                 side, fmt(qty), fmt(price), f.edge_bps, fmt(f.position), f.realized_delta)
        self._write_journal(f, order)
        self._recent.append((now, side))
        horizon = max(self.cfg.burst_window_s, self.ledger.learner.sweep_guard_window_s)
        while self._recent and now - self._recent[0][0] > horizon:
            self._recent.popleft()
        L = self.ledger.learner                     # live parameters (== config values when learning is off)
        burst_limit, burst_cooldown = L.burst_fills, L.burst_cooldown_s
        sweep_limit, sweep_window = L.sweep_guard_fills, L.sweep_guard_window_s

        same_side = sum(1 for t, sd in self._recent if sd == side and now - t <= self.cfg.burst_window_s)
        if same_side >= burst_limit:
            self.cooldown[side] = max(self.cooldown[side], now + burst_cooldown)
            log.warning("BURST GUARD: %d %s fills in %.0fs -> pausing adding %s quotes %.0fs",
                        same_side, side, self.cfg.burst_window_s, side, burst_cooldown)
            self._pull_side(side, now)

        rapid = sum(1 for t, sd in self._recent if sd == side and now - t <= sweep_window)
        if rapid >= sweep_limit:
            self.cooldown[side] = max(self.cooldown[side], now + burst_cooldown)
            log.warning("SWEEP GUARD: %d %s fills within %.1fs -> emergency pull of %s adds for %.0fs",
                        rapid, side, sweep_window, side, burst_cooldown)
            self._pull_side(side, now)
        self.wake.set()

    def _pull_side(self, side: str, now: float) -> None:
        """Cancel that side's ADD orders right now (exits stay live). Fire-and-forget: on_fill runs
        inside the websocket reader, which must not block on an RPC."""
        try:
            asyncio.ensure_future(self.om.cancel_side(side, now))
        except RuntimeError:                          # no running loop (offline unit use)
            pass

    def _jwrite(self, rec: dict) -> None:
        try:
            if self._journal is None:
                self._journal = open(self.cfg.journal_path, "a", buffering=1)
            self._journal.write(json.dumps(rec) + "\n")
        except OSError:
            pass

    def _write_journal(self, f, order=None) -> None:
        """One line per fill, with the market state AT the fill (so a few hundred fills can later be
        sliced by level / book pressure / trend / volatility) - see analyze.py. Markouts follow as
        separate {"type":"markout"} lines once each horizon elapses, joined by fid."""
        md, now = self.md, f.ts
        self._jwrite({
            "type": "fill", "fid": f.fid, "tag": self.cfg.run_tag, "t": time.time(), "side": f.side,
            "qty": fmt(f.qty), "price": fmt(f.price), "mid": fmt(f.mid), "edge_bps": float(f.edge_bps),
            "position": fmt(f.position), "realized_delta": float(f.realized_delta), "paper": self.cfg.dry_run,
            "level": getattr(order, "level", None), "role": getattr(order, "role", None),
            "imbalance": float(md.depth_imbalance(self.cfg.imbalance_levels, now) or 0),
            "ret_bps": float(md.ret_bps(self.cfg.trend_window_s, now)),
            "move_bps": float(md.move_bps(self.cfg.vol_window_s, now)),
            "spread_bps": float(md.spread_bps), "tox_bps": float(self.ledger.tox_bps),
            "obi": float(md.obi), "tfi": float(md.trade_flow_imbalance(10.0, now)),
            "regime": md.detect_regime(now, self.ledger.tox_bps)})

    # ------------------------------------------------------------------------ #
    # quoting
    # ------------------------------------------------------------------------ #
    def can_quote(self, now: float) -> tuple[bool, str]:
        cfg, m, md = self.cfg, self.md.info, self.md
        if now < self.om.paused_until:
            return False, "rate-limited / error pause"
        if now - md.info_ts > 90:
            return False, "market metadata stale"
        if m.status != "ONLINE":
            return False, f"market {m.status}"
        if m.is_outside_rth and not cfg.quote_outside_rth:
            return False, "outside regular trading hours"
        if md.bid is None or md.ask is None or now - md.ts > cfg.stale_s:
            return False, "no/stale BBO"
        if md.bid >= md.ask:
            return False, "crossed book"
        if md.spread_bps > cfg.max_market_spread_bps:
            return False, "market spread too wide"
        if m.mark > 0 and abs(md.mid - m.mark) / m.mark * BPS > cfg.max_oracle_dev_bps:
            return False, "mid deviates from mark"
        return True, ""

    async def paper_fills(self, now: float) -> None:
        """Paper mode: an order fills when the market trades through its price (conservative)."""
        md = self.md
        for o in list(self.om.orders.values()):
            if o.cancelling_since is not None:
                continue
            if (o.side == BUY and md.ask <= o.price) or (o.side == SELL and md.bid >= o.price):
                self.om.on_update({"orderId": o.order_id, "state": "FILLED", "status": "FILLED",
                                   "remainingSize": "0", "price": fmt(o.price)}, now)

    def snapshot(self, now: float) -> Snapshot:
        md, lg, c = self.md, self.ledger, self.cfg
        return Snapshot(
            now=now, market=md.info, bid=md.bid, ask=md.ask, mid=md.mid, micro=md.micro(now),
            position=lg.position, avg_cost=lg.avg_cost, hold_s=lg.hold_s(now),
            ret_bps=md.ret_bps(c.trend_window_s, now), move_bps=md.move_bps(c.vol_window_s, now),
            vol_bps=md.vol_bps, tox_bps=lg.tox_bps,
            imbalance=md.depth_imbalance(c.imbalance_levels, now) or ZERO,   # None (no/stale book) -> neutral
            cooldown_until=dict(self.cooldown),
            jump_active=md.jump_active(now), halted=self.halted,
            obi=md.obi, tfi=md.trade_flow_imbalance(10.0, now),
            buy_tox_bps=lg.side_tox_bps(BUY), sell_tox_bps=lg.side_tox_bps(SELL),
            params=lg.learner, live_levels=self.om.live_levels())

    def _check_risk(self, mid: Decimal) -> None:
        if self.halted:
            return
        total = self.ledger.total_pnl(mid)
        if total <= -self.cfg.session_max_loss_usd:
            self.halted = True
            log.error("HALT: session PnL %+.4f <= -%s. Quotes pulled; %s", total, self.cfg.session_max_loss_usd,
                      "working a limit-only exit until flat, then stopping." if self.cfg.halt_exit
                      else "cancelling everything and stopping.")

    async def tick(self) -> None:
        now, md = self.now(), self.md
        self.ledger.last_now = self.ledger.current_now = now      # markout weighting uses the bot clock
        self.ledger.learner.tick_decay(now)                       # learned params relax back when idle
        ok, reason = self.can_quote(now)
        if reason != self._last_reason:
            if ok:
                log.info("quoting resumed")
            else:
                log.warning("NOT QUOTING: %s", reason)
            self._last_reason = reason
        if not ok:
            for o in list(self.om.orders.values()):
                await self.om.cancel(o, now)
            return
        mid = md.mid
        self.ledger.process_markouts(mid, now)
        for fid, h, bps in self.ledger.matured_journal_markouts(mid, now):
            self._jwrite({"type": "markout", "fid": fid, "h": h, "bps": bps})
        if self.cfg.dry_run:
            await self.paper_fills(now)
        if self.cfg.enable_online_learning:                       # did OBI/TFI predict the move?
            self.ledger.learner.on_flow_correlation(md.obi, md.trade_flow_imbalance(10.0, now),
                                                     md.ret_bps(5.0, now))
        self._check_risk(mid)
        flat = self.ledger.is_flat(mid, md.info.min_notional)
        if self.halted and (not self.cfg.halt_exit or flat):
            await self.om.cancel_all(force=True)
            log.error("halted and %s - stopping.", "flat" if flat else "exit disabled")
            self.log_pnl(now, final=True)
            self.stop_evt.set()
            return
        plan = self.strategy.plan(self.snapshot(now))
        # Trend-guard hysteresis: once "trend" pulls an adding side, keep it pulled for
        # TREND_HOLD_S even if the return snaps back under threshold next tick. Without this,
        # a side sitting right at TREND_PULL_BPS cancels/re-places/re-rejects every tick in a
        # choppy market - this is what drove the 74 cancels / 13 rejects in ~4 minutes seen in
        # arcus_live_log.txt (12:28-12:32). Reuses the existing burst-fill cooldown mechanism,
        # so strategy.py's blocked() sees it automatically on the next tick.
        for side, code in plan.blocked.items():
            if code == "trend":
                self.cooldown[side] = max(self.cooldown[side], now + self.cfg.trend_hold_s)
        notes = tuple(plan.notes)
        sig = tuple(_NOTE_NUM.sub("#", n) for n in notes)  # ignore drifting bps values for dedup
        if sig != self._last_notes:
            if notes:
                log.info("PLAN edge=%.1fbps skew=%+.1fbps | %s", plan.edge_bps, plan.skew_bps, "; ".join(notes))
            self._last_notes = sig
        await asyncio.gather(
            self.manage_side(BUY, ([plan.bid] if plan.bid else []) + plan.extra_bids, now, plan.stress),
            self.manage_side(SELL, ([plan.ask] if plan.ask else []) + plan.extra_asks, now, plan.stress))

    async def manage_side(self, side: str, targets: list, now: float, stress: bool = False) -> None:
        """targets[0] (if present) is the touch quote - it re-quotes at TOUCH_MIN_REQUOTE_S (fast:
        track the book as quickly as it pushes updates) and, if it's a 'reduce' order, gets the
        immediate urgent-retreat behavior. targets[1:] are the ladder levels (see Config.extra_levels)
        - same chase/retreat logic, just throttled to MIN_REQUOTE_S since they're not meant to chase
        every tick (that would only burn the action budget for levels that aren't trying to be fastest
        to the touch anyway). Live orders are paired to targets nearest-mid-first so a level keeps its
        own resting order across ticks instead of being torn down whenever the ladder shape shifts."""
        mid = self.md.mid or ZERO
        live = sorted(self.om.side_orders(side), key=lambda o: abs(o.price - mid))
        for i in range(max(len(live), len(targets))):
            await self._manage_one(side, live[i] if i < len(live) else None,
                                    targets[i] if i < len(targets) else None, now, stress, level=i)

    async def _manage_one(self, side: str, o, target, now: float, stress: bool, level: int = 0) -> None:
        om, cfg = self.om, self.cfg
        level = target.level if target is not None else (o.level if o is not None else level)
        min_requote_s = cfg.touch_min_requote_s if level == 0 else cfg.min_requote_s
        if o is not None and o.cancelling_since is not None:
            if now - o.cancelling_since > 5:
                o.cancelling_since = None            # cancel never confirmed -> retry
            return
        if target is None:
            if o:
                await om.cancel(o, now)
            return
        if now < om.reject_until[side]:               # post-only reject back-off (no retry storms)
            return
        if o is None:
            await om.place(side, target.price, target.qty, now, level=level, role=target.role)
            return
        if o.filled_any or abs(o.qty - target.qty) > target.qty * Decimal("0.25"):
            await om.cancel(o, now)                   # partially filled / resized -> clean re-place
            return
        drift = abs(target.price - o.price) / o.price * BPS
        if drift == 0:
            return
        retreat = target.price < o.price if side == BUY else target.price > o.price
        if retreat:
            if drift >= cfg.retreat_bps:              # run away from the market immediately
                await om.modify(o, target.price, now, urgent=True)
        elif drift >= (cfg.retreat_bps if stress else cfg.requote_bps) and now - o.last_action >= min_requote_s:
            await om.modify(o, target.price, now)     # chase (fast at level 0, throttled beyond it)

    # ------------------------------------------------------------------------ #
    # reporting
    # ------------------------------------------------------------------------ #
    def log_pnl(self, now: float, final: bool = False) -> None:
        lg, md = self.ledger, self.md
        mid = md.mid or lg.avg_cost
        if not mid:
            return
        pos_usd = lg.position * mid
        log.info("%s mid=%s pos=%s ($%+.2f) cost=%s | open: %s", "FINAL " if final else "STATUS",
                 fmt(mid), fmt(lg.position), pos_usd, fmt(lg.avg_cost) if lg.avg_cost else "-", self.om.describe(now))
        regime = md.detect_regime(now, lg.tox_bps)
        log.info("REGIME %s | obi=%s tfi=%s vol=%sbps spread=%sbps | tox buy/sell=%s/%sbps | cooldown B/S=%s/%s",
                 regime, fmt(md.obi), fmt(md.trade_flow_imbalance(10.0, now)), fmt(md.vol_bps), fmt(md.spread_bps),
                 fmt(lg.side_tox_bps(BUY)), fmt(lg.side_tox_bps(SELL)),
                 "on" if self.cooldown[BUY] > now else "-", "on" if self.cooldown[SELL] > now else "-")
        if self.cfg.enable_online_learning:
            sm = lg.learner.get_summary()
            p = {k: float(v) for k, v in sm["params"].items()}
            log.info("LEARN  [updates=%d toxic=%d benign=%d] edge=%.2f-%.2fbps skew=%.2fbps spacing=%.2fbps "
                     "mult=%.2f vol_k=%.2f tox_mult=%.2f min_ev=%.2fbps obi_a=%.2f tfi_b=%.2f kappa=%.2f "
                     "trend_pull=%.1fbps exit_min=%.2fbps",
                     sm["total_updates"], sm["toxic_fills"], sm["benign_fills"], p["min_edge_bps"],
                     p["max_edge_bps"], p["skew_bps"], p["level_spacing_bps"], p["level_size_mult"], p["vol_k"],
                     p["tox_mult"], p["min_ev_bps"], p["obi_alpha"], p["tfi_beta"], p["fill_prob_kappa"],
                     p["trend_pull_bps"], p["exit_min_profit_bps"])
        log.info("PNL    fills=%d (B%d/S%d) vol=$%.0f | SPREAD_CAPTURE=$%+.4f (avg %+.2fbps/fill) | "
                 "realized=$%+.4f unreal=$%+.4f TOTAL=$%+.4f | inventory_pnl=$%+.4f | markout(%ds)=%+.2fbps | "
                 "place/mod/cancel/rej=%d/%d/%d/%d",
                 lg.n_fills, lg.n_buys, lg.n_sells, lg.volume_usd, lg.spread_capture, lg.avg_edge_bps,
                 lg.realized, lg.unrealized(mid), lg.total_pnl(mid), lg.inventory_pnl(mid),
                 self.cfg.markout_horizon_s, lg.avg_markout_bps,
                 self.om.n_place, self.om.n_modify, self.om.n_cancel, self.om.n_reject)

    # ------------------------------------------------------------------------ #
    # background loops
    # ------------------------------------------------------------------------ #
    async def quote_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self.wake.wait(), self.cfg.loop_s)
            except asyncio.TimeoutError:
                pass
            self.wake.clear()
            try:
                await self.tick()
            except (Fatal, websockets.ConnectionClosed):
                raise
            except asyncio.TimeoutError:
                log.warning("RPC timeout")
            except Exception:
                log.exception("tick error")
            await asyncio.sleep(0.1)

    async def heartbeat_loop(self) -> None:
        """bbo only pushes on top-of-book change; poll it so a quiet book isn't 'stale'."""
        while True:
            await asyncio.sleep(self.cfg.heartbeat_s)
            try:
                r = await self.ex.get("bbo", {"market": self.cfg.market}, 5)
                if isinstance(r, dict):
                    self.on_bbo(r, self.now())
            except (asyncio.TimeoutError, KeyError):
                log.warning("bbo heartbeat failed")

    async def reconcile_loop(self) -> None:
        pay = {"address": self.cfg.address, "accountIndex": self.cfg.account_index, "market": self.cfg.market}
        while True:
            await asyncio.sleep(self.cfg.reconcile_s)
            if self.cfg.dry_run:
                continue
            try:
                now = self.now()
                res = await self.ex.get("positions", pay)
                if res is not None:
                    self.on_positions(res, True, now)
                res = await self.ex.get("orders", {**pay, "status": ["OPEN"]})
                if isinstance(res, dict) and isinstance(res.get("openOrders"), list):
                    await self.om.reconcile(res["openOrders"], now)
            except (asyncio.TimeoutError, KeyError):
                log.warning("reconcile failed")

    async def dms_loop(self) -> None:
        while self.dms_ok:
            await asyncio.sleep(15)
            try:
                await self.schedule_cancel(45)
            except asyncio.TimeoutError:
                log.warning("scheduleCancel timeout")

    async def market_loop(self) -> None:
        while True:
            await asyncio.sleep(20)
            try:
                await self.refresh_market()
            except Exception as e:
                log.warning("market refresh failed: %s", e)

    async def status_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.status_s)
            self.log_pnl(self.now())

    # ------------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------------ #
    async def refresh_market(self) -> None:
        rows = await self.ex.fetch_markets(self.cfg.market)
        self.md.info = Market.from_api(rows[0])
        self.md.info_ts = self.now()

    async def schedule_cancel(self, lead_s: Optional[int]) -> None:
        """Dead man's switch: the gateway cancels everything if we stop refreshing it."""
        body = {"address": self.cfg.address, "accountIndex": self.cfg.account_index}
        if lead_s is not None:
            body["time"] = int(time.time() * 1_000_000) + lead_s * 1_000_000
        resp = await self.ex.write(self.signer.legacy("scheduleCancel", body))
        if resp.get("status") not in (200, 202) or "error" in resp:
            if self.dms_ok:
                log.warning("scheduleCancel rejected (%s) - dead man's switch DISABLED", json.dumps(resp)[:200])
            self.dms_ok = False

    async def startup(self) -> None:
        cfg = self.cfg
        if self.om.maybe_orders:
            await self.om.cancel_all()               # clean slate: nothing rests that we don't know about
        await self.ex.subscribe("bbo", cfg.market)
        await self.ex.subscribe("l2OrderbookUpdates", cfg.market)
        await self.ex.subscribe("trades", cfg.market)         # public tape -> trade-flow imbalance
        acct = {"accountIndex": cfg.account_index, "market": cfg.market}
        await self.ex.subscribe("orders", cfg.address, snapshot=False, **acct)
        await self.ex.subscribe("positions", cfg.address, **acct)
        if not cfg.dry_run:
            await self.schedule_cancel(45)
        deadline = time.monotonic() + 15
        while self.md.ts == 0 and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
        if self.md.ts == 0:
            raise ConnectionError("no BBO received after subscribe")
        if not cfg.dry_run:                          # adopt whatever we're already holding
            res = await self.ex.get("positions", {"address": cfg.address, **acct})
            m, mid, now = self.md.info, self.md.mid, self.now()
            pos = ZERO
            for row in extract_positions(res):
                if int(row.get("marketId", -1)) == m.market_id:
                    pos = parse_size(row)
            if abs(pos * mid) >= m.min_notional and self.ledger.is_flat(mid, m.min_notional):
                self.ledger.position, self.ledger.avg_cost, self.ledger.opened_ts = pos, mid, now
                log.warning("ADOPTED existing position %s at mid %s (true cost unknown)", fmt(pos), fmt(mid))
        m = self.md.info
        log.info("connected; market=%s id=%s tick=%s step=%s minNotional=%s", m.name, m.market_id,
                 m.tick, m.step, m.min_notional)

    async def shutdown_orders(self) -> None:
        if self.cfg.enable_online_learning:
            if self.ledger.learner.save():
                log.info("Saved online learning state to %s", self.cfg.learning_state_path)
        try:
            await asyncio.wait_for(self.om.cancel_all(force=True), 8)
            if self.dms_ok and not self.cfg.dry_run:
                await asyncio.wait_for(self.schedule_cancel(None), 5)   # disarm
        except Exception as e:
            log.warning("shutdown cleanup failed: %s (dead man's switch should still fire)", e)

    async def session(self) -> None:
        await self.refresh_market()
        self.md.clear_book()
        self.md.ts = 0.0
        async with websockets.connect(self.ex.ws_url, ping_interval=15, ping_timeout=15,
                                      max_size=2 ** 23) as ws:
            self.ex.ws = ws
            tasks = [asyncio.create_task(self.ex.reader())]
            try:
                await self.startup()
                loops = [self.quote_loop, self.heartbeat_loop, self.reconcile_loop, self.market_loop,
                         self.status_loop] + ([] if self.cfg.dry_run else [self.dms_loop])
                tasks += [asyncio.create_task(c()) for c in loops]
                tasks.append(asyncio.create_task(self.stop_evt.wait()))
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                if self.stop_evt.is_set():
                    await self.shutdown_orders()
                    return
                try:
                    for t in done:
                        t.result()
                except Fatal:
                    await self.shutdown_orders()     # never leave quotes resting on a fatal stop
                    raise
                raise ConnectionError("session ended")
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self.ex.ws = None
                self.om.orders.clear()

    async def run(self) -> None:
        backoff = 1.0
        while not self.stop_evt.is_set():
            try:
                await self.session()
                backoff = 1.0
            except Fatal as e:
                log.error("FATAL: %s", e)
                break
            except (websockets.ConnectionClosed, OSError, ConnectionError, asyncio.TimeoutError,
                    urllib.error.URLError) as e:
                log.warning("connection problem: %r", e)
            except Exception:
                log.exception("session crashed")
            if self.stop_evt.is_set():
                break
            delay = backoff + random.random()
            log.info("reconnecting in %.1fs", delay)
            try:
                await asyncio.wait_for(self.stop_evt.wait(), delay)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 30)
        self.log_pnl(self.now(), final=True)
