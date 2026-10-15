"""Order book of *our own* orders with Multi-Pair Individual Order Management."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Optional, Dict, Tuple, List

from exchange import Exchange
from market import Market
from signer import Signer
from utils import BUY, SELL, fmt, bps_diff, q_down, q_up, ZERO

log = logging.getLogger("orders")
GTT_DAYS = 40


@dataclass
class Order:
    order_id: str
    pair_index: int
    side: str
    price: Decimal
    qty: Decimal
    remaining: Decimal
    good_til_us: int
    created: float
    last_action: float
    filled_any: bool = False
    cancelling_since: Optional[float] = None
    is_taker: bool = False
    is_reduce_only: bool = False
    ev_bps: Decimal = Decimal(0)
    quote_mid: Optional[Decimal] = None
    est_px: Optional[Decimal] = None
    queue_ahead: Decimal = Decimal(0)
    cum_filled: Decimal = Decimal(0)


class OrderManager:
    def __init__(self, cfg, ex: Exchange, signer: Signer, get_market: Callable[[], Market],
                 on_fill: Callable[[str, Decimal, Decimal, Order], None]):
        self.cfg, self.ex, self.signer = cfg, ex, signer
        self.get_market = get_market
        self.on_fill = on_fill
        self.ledger = None
        self.md = None
        
        self.orders: dict[str, Order] = {}
        self._last_taker: dict = {}
        self.pair_slots: dict[Tuple[int, str], str] = {}
        self._unmatched: dict[str, tuple] = {}
        self.reject_until = {BUY: 0.0, SELL: 0.0}
        self._reject_n = {BUY: 0, SELL: 0}
        self._actions: deque = deque()
        self.paused_until = 0.0
        self._recently_closed: deque = deque(maxlen=200)
        self._consec_errors = 0
        self.last_place_ts = 0.0
        self.maybe_orders = True
        self.on_reduce_only_rejected = None
        self.n_place = self.n_modify = self.n_cancel = self.n_reject = self.n_actions = 0

    def side_orders(self, side: str) -> list[Order]:
        return sorted((o for o in self.orders.values() if o.side == side), key=lambda o: o.price, reverse=(side == BUY))

    def get_order_by_slot(self, pair_index: int, side: str) -> Optional[Order]:
        oid = self.pair_slots.get((pair_index, side))
        return self.orders.get(oid) if oid else None

    def open_qty(self, side: str) -> Decimal:
        return sum((o.remaining for o in self.orders.values() if o.side == side), Decimal(0))

    def describe(self, now: float) -> str:
        if not self.orders:
            return "none"
        desc = []
        for (pair_idx, side), oid in sorted(self.pair_slots.items()):
            o = self.orders.get(oid)
            if o:
                desc.append(f"L{pair_idx}{'B' if side == BUY else 'S'} {fmt(o.remaining)}@{fmt(o.price)}")
        return " | ".join(desc) if desc else "none"

    def _budget(self, now: float) -> bool:
        while self._actions and now - self._actions[0] > 60:
            self._actions.popleft()
        if len(self._actions) >= self.cfg.max_actions_per_min:
            return False
        self._actions.append(now)
        self.n_actions += 1
        return True

    @staticmethod
    def _ok(resp: dict) -> bool:
        return resp.get("status") in (200, 202) and "error" not in resp

    def _error(self, what: str, resp: dict, now: float) -> None:
        err = resp.get("error")
        log.warning("%s failed: status=%s %s", what, resp.get("status"), json.dumps(err)[:300])
        self._consec_errors += 1
        retry = err.get("retryAfterMs") if isinstance(err, dict) else None
        retry = retry or resp.get("retryAfterMs")
        if resp.get("status") == 429 and retry:
            self.paused_until = now + float(retry) / 1000 + 0.1
        code = err.get("code") if isinstance(err, dict) else None
        if code == 23000:
            log.warning("VOLUME QUOTA EXHAUSTED (code 23000) - backing off order actions for 10s")
            self.paused_until = max(self.paused_until, now + 10.0)
            self._actions.clear()
        elif code == 21104:
            log.warning("INVALID NONCE (code 21104) - resynchronizing nonce generator")
            if hasattr(self.signer, "next_ts"):
                self.signer._last_ts = max(time.time_ns(), getattr(self.signer, "_last_ts", 0) + 50_000_000)
            self.paused_until = max(self.paused_until, now + 1.5)
        if self._consec_errors >= 8:
            log.error("too many consecutive errors - pausing 30s")
            self.paused_until = now + 30
            self._consec_errors = 0

    def _backoff(self, side: str, now: float) -> None:
        self._reject_n[side] += 1
        self.reject_until[side] = now + min(0.25 * 2 ** (self._reject_n[side] - 1), 4.0)

    async def place(self, pair_index: int, side: str, px: Decimal, qty: Decimal, now: float,
                    time_in_force: str = "ALO", reduce_only: bool = False,
                    quote_mid: Optional[Decimal] = None,
                    est_px: Optional[Decimal] = None) -> Optional[Order]:
        if now < self.paused_until or not self._budget(now):
            return None
        m = self.get_market()
        tick = m.tick_for(px) if hasattr(m, "tick_for") else m.tick
        is_taker = (time_in_force == "IOC")
        if is_taker:
            px = q_up(px, tick) if side == BUY else q_down(px, tick)
        else:
            px = q_down(px, tick) if side == BUY else q_up(px, tick)
            md = getattr(self, "md", None)
            if md is not None and md.bid and md.ask and md.bid < md.ask:
                if side == BUY and px >= md.ask:
                    px = q_down(md.ask - tick, tick)
                elif side == SELL and px <= md.bid:
                    px = q_up(md.bid + tick, tick)
        qty = q_down(qty, m.step)
        if qty < m.min_size or (m.min_notional > 0 and px * qty < m.min_notional):
            return None
        good_til = int(time.time() * 1_000_000) + GTT_DAYS * 86_400 * 1_000_000
        req = self.signer.place(m, side, px, qty, good_til,
                                time_in_force=time_in_force, reduce_only=reduce_only)
        self.maybe_orders = True
        self.last_place_ts = now
        resp = await self.ex.write(req)
        res = resp.get("result") or {}
        if not self._ok(resp) or not res.get("orderId") or str(res.get("status")).upper() == "REJECTED":
            self._error(f"place L{pair_index} {side}", resp if not self._ok(resp) else
                        {"status": resp.get("status"), "error": res}, now)
            if not is_taker:
                self._backoff(side, now)
            return None
        self._consec_errors = 0
        self._reject_n[side] = 0
        self.n_place += 1
        oid = str(res["orderId"])
        q_ahead = ZERO
        if self.cfg.dry_run and not is_taker and self.md is not None and hasattr(self.md, "queue_ahead"):
            try:
                q_ahead = max(ZERO, self.md.queue_ahead(side, px) * Decimal("0.25"))
            except Exception:
                q_ahead = ZERO
        o = Order(oid, pair_index, side, px, qty, qty, good_til, now, now,
                  is_taker=is_taker, is_reduce_only=reduce_only, quote_mid=quote_mid, est_px=est_px,
                  queue_ahead=q_ahead)
        self.orders[oid] = o
        if not is_taker:
            self.pair_slots[(pair_index, side)] = oid
        log.info("PLACE L%d %s %s @ %s (taker=%s)", pair_index, side, fmt(qty), fmt(px), is_taker)
        early = self._unmatched.pop(oid, None)
        if early:
            self._apply(o, early[1], now)
        if self.cfg.dry_run and is_taker:
            fill_px = est_px or ((self.md.ask if side == BUY else self.md.bid) if self.md else None) or px
            self._apply(o, {
                "orderId": oid, "state": "FILLED", "status": "FILLED",
                "price": str(px), "avgFillPrice": str(fill_px),
                "filledSize": str(qty), "remainingSize": "0"
            }, now)
        return o

    async def modify(self, o: Order, px: Decimal, now: float, urgent: bool = False,
                     reduce_only: Optional[bool] = None) -> bool:
        if now < self.paused_until or not self._budget(now):
            if urgent:
                await self.cancel(o, now)
            return False
        m = self.get_market()
        tick = m.tick_for(px) if hasattr(m, "tick_for") else m.tick
        px = q_down(px, tick) if o.side == BUY else q_up(px, tick)
        md = getattr(self, "md", None)
        if md is not None and md.bid and md.ask and md.bid < md.ask:
            if o.side == BUY and px >= md.ask:
                px = q_down(md.ask - tick, tick)
            elif o.side == SELL and px <= md.bid:
                px = q_up(md.bid + tick, tick)
        r_only = o.is_reduce_only if reduce_only is None else reduce_only
        if px == o.price and r_only == o.is_reduce_only:
            return True
        req = self.signer.modify(m, o.order_id, o.side, px, o.qty, o.good_til_us, reduce_only=r_only)
        resp = await self.ex.write(req)
        if not self._ok(resp):
            self._error(f"modify L{o.pair_index} {o.side}", resp, now)
            err = resp.get("error")
            code = err.get("code") if isinstance(err, dict) else None
            # If quota exhausted or invalid nonce, sending cancel will just fail and spam; let order rest
            if code not in (23000, 21104):
                await self.cancel(o, now)
            return False
        self._consec_errors = 0
        self.n_modify += 1
        log.info("MODIFY L%d %s %s -> %s (reduce_only=%s)", o.pair_index, o.side, fmt(o.price), fmt(px), r_only)
        if self.cfg.dry_run and px != o.price and self.md is not None and hasattr(self.md, "queue_ahead"):
            try:
                o.queue_ahead = max(ZERO, self.md.queue_ahead(o.side, px) * Decimal("0.25"))
            except Exception:
                o.queue_ahead = ZERO
        o.price, o.last_action, o.is_reduce_only = px, now, r_only
        return True

    async def cancel(self, o: Order, now: float) -> None:
        if o.cancelling_since is not None and now - o.cancelling_since < 5:
            return
        o.cancelling_since = now
        self._recently_closed.append((now, o.order_id))
        self._budget(now)
        resp = await self.ex.write(self.signer.cancel(self.get_market(), o.order_id))
        if self.cfg.dry_run or "ORDER_NOT_FOUND" in json.dumps(resp):
            self._remove_order(o.order_id)
        elif not self._ok(resp):
            o.cancelling_since = None
            self._error(f"cancel L{o.pair_index} {o.side}", resp, now)
        else:
            self.n_cancel += 1

    def _remove_order(self, order_id: str) -> None:
        o = self.orders.pop(order_id, None)
        if o:
            self._recently_closed.append((time.time(), order_id))
            slot = (o.pair_index, o.side)
            if self.pair_slots.get(slot) == order_id:
                self.pair_slots.pop(slot, None)

    async def _gather(self, coros) -> None:
        coros = list(coros)
        if not coros:
            return
        if len(coros) == 1:
            await coros[0]
            return
        for r in await asyncio.gather(*coros, return_exceptions=True):
            if isinstance(r, asyncio.CancelledError):
                raise r
            if isinstance(r, Exception):
                log.warning("parallel order action failed: %r", r)

    async def cancel_side(self, side: str, now: float) -> None:
        coros = []
        for slot, oid in list(self.pair_slots.items()):
            if slot[1] == side:
                o = self.orders.get(oid)
                if o:
                    coros.append(self.cancel(o, now))
        await self._gather(coros)

    async def cancel_all(self, force: bool = False) -> None:
        if not force and not self.orders and not self.maybe_orders:
            return
        m = self.get_market()
        now = time.time()
        await self._gather([self.cancel(o, now) for o in list(self.orders.values())])

        if (force or self.maybe_orders) and not self.cfg.dry_run and getattr(self.ex, "is_connected", False):
            try:
                res = await self.ex.get("orders", {"address": self.cfg.address, "accountIndex": self.cfg.account_index,
                                                    "marketId": m.market_id})
                if res and "openOrders" in res:
                    reqs = []
                    for r in res["openOrders"]:
                        if isinstance(r, dict):
                            r_mkt = r.get("marketId")
                            if r_mkt is not None and int(r_mkt) != m.market_id:
                                continue
                            oid = str(r.get("orderId") or r.get("id"))
                            if oid and oid != "None" and oid not in self.orders:
                                reqs.append(self.ex.write(self.signer.cancel(m, oid)))
                    await self._gather(reqs)
            except Exception:
                pass

        self.orders.clear()
        self.pair_slots.clear()
        self.maybe_orders = False

    async def sync_quotes(self, targets: list, now: float, blocked_sides: Optional[set] = None) -> None:
        active_slots = set()
        side_groups: dict = {}
        for t in targets:
            slot = (t.pair_index, t.side)
            side_groups.setdefault(t.side, []).append(t)
            if not getattr(t, "is_taker", False):
                active_slots.add(slot)

        async def run_side(ts: list) -> None:
            # Process L0 -> L1 -> L2 -> L3 in strict order per side so deeper levels never collide with resting closer levels
            ts_sorted = sorted(ts, key=lambda x: (0 if getattr(x, "is_taker", False) else 1, x.pair_index))
            for t in ts_sorted:
                await self._sync_one(t, now)

        coros = [run_side(ts) for ts in side_groups.values()]
        for slot, oid in list(self.pair_slots.items()):
            if slot not in active_slots:
                o = self.orders.get(oid)
                if o:
                    coros.append(self.cancel(o, now))
        await self._gather(coros)

    async def _sync_one(self, t, now: float) -> None:
        slot = (t.pair_index, t.side)
        existing = self.get_order_by_slot(t.pair_index, t.side)

        if getattr(t, "is_taker", False):
            last = self._last_taker.get(t.side)
            if last is not None and now - last < 1.5:
                return            # previous IOC still settling: never double-send a taker
            self._last_taker[t.side] = now
            if existing:
                await self.cancel(existing, now)
                self.pair_slots.pop(slot, None)
            taker_r_only = True
            ledger = getattr(self, "ledger", None)
            if ledger is not None:
                if (t.side == SELL and ledger.position <= ZERO) or (t.side == BUY and ledger.position >= ZERO):
                    taker_r_only = False
            self.reject_until[t.side] = max(self.reject_until[t.side], now + 0.50)
            await self.place(t.pair_index, t.side, t.price, t.qty, now,
                             time_in_force="IOC", reduce_only=taker_r_only,
                             quote_mid=getattr(t, "quote_mid", None),
                             est_px=getattr(t, "est_px", None))
            return

        m = self.get_market()
        tick = m.tick_for(t.price) if hasattr(m, "tick_for") else m.tick
        target_px = t.price
        # LADDER COLLISION GUARD: Ensure level k > 0 is strictly deeper than all resting levels < k on the same side
        if t.pair_index > 0:
            for o in self.orders.values():
                if o.side == t.side and not o.is_taker and o.cancelling_since is None and o.pair_index < t.pair_index:
                    if t.side == BUY and target_px >= o.price:
                        target_px = q_down(o.price - tick, tick)
                    elif t.side == SELL and target_px <= o.price:
                        target_px = q_up(o.price + tick, tick)
            t.price = target_px

        is_exit = bool(getattr(t, "is_exit_quote", False))
        # HARDWARE CLAMP: reduce_only is ONLY valid if we hold an opposing position!
        # A SELL can ONLY reduce a LONG position (pos > 0).
        # A BUY can ONLY reduce a SHORT position (pos < 0).
        ledger = getattr(self, "ledger", None)
        if ledger is not None:
            if t.side == SELL and ledger.position <= ZERO:
                is_exit = False
            elif t.side == BUY and ledger.position >= ZERO:
                is_exit = False

        if existing is None:
            if now >= self.reject_until[t.side]:
                o_new = await self.place(t.pair_index, t.side, t.price, t.qty, now,
                                         time_in_force="ALO", reduce_only=is_exit,
                                         quote_mid=getattr(t, "quote_mid", None))
                if o_new:
                    o_new.ev_bps = getattr(t, "expected_value_bps", Decimal(0))
            return

        # Check if unwind quantity mismatch (accumulated further fills: must resize!)
        qty_mismatch = abs(t.qty - existing.remaining) >= (m.step * Decimal("0.5"))
        if is_exit and qty_mismatch:
            await self.cancel(existing, now)
            self.pair_slots.pop(slot, None)
            await self.place(t.pair_index, t.side, t.price, t.qty, now,
                             time_in_force="ALO", reduce_only=is_exit,
                             quote_mid=getattr(t, "quote_mid", None))
            return

        drift = abs(bps_diff(t.price, existing.price))
        is_advancing = (t.side == BUY and t.price > existing.price) or (t.side == SELL and t.price < existing.price)
        is_retreating = not is_advancing

        should_modify = False
        urgent = False

        if getattr(existing, "is_reduce_only", False) != is_exit:
            should_modify = True
            urgent = True

        tick_bps = (m.tick / existing.price) * Decimal("10000") if existing.price > 0 else Decimal("0.1")
        eff_retreat = max(self.cfg.retreat_bps, tick_bps * Decimal("1.2"))
        eff_requote = max(self.cfg.requote_bps, tick_bps * Decimal("1.5"))
        time_since_last = now - existing.last_action
        min_requote_delay = getattr(self.cfg, "touch_min_requote_s", self.cfg.min_requote_s) if existing.pair_index == 0 else self.cfg.min_requote_s

        # Don't modify on sub-tick or 1-tick jiggles unless drift exceeds threshold or min delay passed
        if is_retreating and (drift >= eff_retreat) and (is_exit or time_since_last >= 0.25 or drift >= eff_retreat * Decimal("2.5")):
            should_modify = True
            urgent = True
        elif is_advancing and (drift >= eff_requote) and (time_since_last >= min_requote_delay):
            queue_reset_cost = getattr(self.cfg, "queue_reset_cost_bps", Decimal("0.20"))
            ev_gain = getattr(t, "expected_value_bps", Decimal(0)) - getattr(existing, "ev_bps", Decimal(0))
            if ev_gain >= queue_reset_cost or drift >= (eff_requote * Decimal("1.5")):
                should_modify = True

        if should_modify:
            if await self.modify(existing, t.price, now, urgent=urgent, reduce_only=is_exit):
                existing.ev_bps = getattr(t, "expected_value_bps", Decimal(0))
                existing.quote_mid = getattr(t, "quote_mid", None)

    def on_update(self, c, now: float) -> None:
        if not isinstance(c, dict) or not c.get("orderId"):
            return
        mkt_id = c.get("marketId")
        if mkt_id is not None:
            try:
                if int(mkt_id) != self.get_market().market_id:
                    return
            except Exception:
                pass
        oid = str(c["orderId"])
        o = self.orders.get(oid)
        if o is None:
            self._unmatched[oid] = (now, c)
            if len(self._unmatched) > 200:
                self._unmatched = {k: v for k, v in self._unmatched.items() if v[0] > now - 30}
            return
        self._apply(o, c, now)

    def _apply(self, o: Order, c: dict, now: float) -> None:
        if c.get("cancelReason") == "MODIFY_CANCELED":
            return
        state = str(c.get("state") or "").upper()
        status = str(c.get("status") or "").upper()
        filled = (state == "FILLED" or status == "FILLED")
        px = o.price
        try:
            if c.get("price"):
                px = Decimal(str(c["price"]))
            # takers: record the real execution price when the exchange reports it, not our limit
            for k in ("avgPrice", "averagePrice", "avgFillPrice", "lastFillPrice", "fillPrice"):
                v = c.get(k)
                if v and Decimal(str(v)) > 0:
                    px = Decimal(str(v))
                    break
        except Exception:
            pass
        if o.is_taker and (filled or state == "PARTIALLY_FILLED"):
            real = any(c.get(k) and Decimal(str(c[k])) > 0
                       for k in ("avgPrice", "averagePrice", "avgFillPrice", "lastFillPrice", "fillPrice"))
            # Always dump the raw update once per taker event so the true execution-price field can be confirmed.
            log.info("TAKER_RAW limit=%s est=%s real_px_field=%s raw=%s", fmt(o.price), fmt(o.est_px) if o.est_px else "-",
                     real, json.dumps(c, default=str)[:500])
            if not real and o.est_px and getattr(self.cfg, "taker_fill_price_mode", "est") == "est":
                px = o.est_px   # exchange sent no execution price: book the book-walk estimate, NOT our far-through limit
        fill_qty = Decimal(0)
        is_partially_filled = (state == "PARTIALLY_FILLED" or status == "PARTIALLY_FILLED")

        # 1. Prefer cumulative filled size if reported by exchange (Arcus sends "filledSize")
        cum_filled_raw = c.get("filledSize") or c.get("cumFilled") or c.get("executedQty")
        if cum_filled_raw is not None:
            try:
                cum_filled = Decimal(str(cum_filled_raw))
                prev_filled = getattr(o, "cum_filled", Decimal(0))
                if cum_filled > prev_filled:
                    fill_qty = cum_filled - prev_filled
                    o.cum_filled = cum_filled
                    o.remaining = max(Decimal(0), o.qty - cum_filled)
                else:
                    fill_qty = Decimal(0)
            except Exception:
                fill_qty = Decimal(0)
        elif filled or is_partially_filled:
            # Only compute delta from remainingSize if state explicitly confirms an execution!
            rem = c.get("remainingSize")
            if rem is not None:
                try:
                    new_rem = Decimal(str(rem))
                    if new_rem < o.remaining:
                        fill_qty = o.remaining - new_rem
                    o.remaining = new_rem
                except Exception:
                    rem = None
            if filled and (rem is None or fill_qty == Decimal(0)):
                fill_qty, o.remaining = o.remaining, Decimal(0)

        # 2. Never book fills if order was canceled or rejected without execution!
        if (state in ("CANCELED", "REJECTED", "EXPIRED") or status in ("CANCELED", "REJECTED", "EXPIRED")) and not filled and not is_partially_filled:
            fill_qty = Decimal(0)

        if fill_qty > 0:
            o.filled_any = True
            self.on_fill(o.side, fill_qty, px, o)
        if state == "OPEN" or status == "OPEN":
            self._reject_n[o.side] = 0

        # IOC terminal handling per Arcus documentation
        is_ioc_done = o.is_taker and (state in ("PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED") or
                                     status in ("PARTIALLY_FILLED", "FILLED", "CANCELED", "MARGIN_CANCELED", "REJECTED") or
                                     o.remaining == Decimal(0))
        if filled or is_ioc_done:
            self._remove_order(o.order_id)
        elif state in ("CANCELED", "REJECTED") or status in ("CANCELED", "MARGIN_CANCELED", "REJECTED"):
            reason = c.get("rejectionReason") or c.get("cancelReason") or ""
            if "REDUCE_ONLY" in str(reason).upper():
                log.warning("ORDER L%d %s REJECTED (%s): exchange position mismatch! Triggering position reconciliation.",
                            o.pair_index, o.side, reason)
                if hasattr(self, "on_reduce_only_rejected") and callable(self.on_reduce_only_rejected):
                    try:
                        self.on_reduce_only_rejected(o.side)
                    except TypeError:
                        try:
                            self.on_reduce_only_rejected()
                        except Exception:
                            pass
                    except Exception:
                        pass
            elif (state == "REJECTED" or status == "REJECTED") and not o.is_taker:
                self.n_reject += 1
                if "POST_ONLY" in str(reason).upper() or "WOULD_CROSS" in str(reason).upper():
                    self.reject_until[o.side] = max(self.reject_until[o.side], now + 0.15)
                else:
                    self._backoff(o.side, now)
            if not o.is_taker and (now - o.last_action < 1.0):
                self.reject_until[o.side] = max(self.reject_until[o.side], now + 0.25)
            log.info("ORDER L%d %s %s %s", o.pair_index, o.side, state or status, reason)
            self._remove_order(o.order_id)

    async def reconcile(self, rows: list, now: float) -> None:
        m = self.get_market()
        market_rows = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            r_mkt = r.get("marketId")
            if r_mkt is not None:
                try:
                    if int(r_mkt) != m.market_id:
                        continue
                except (ValueError, TypeError):
                    pass
            market_rows.append(r)

        open_ids = {str(r.get("orderId") or r.get("id")) for r in market_rows}
        for o in list(self.orders.values()):
            if o.order_id not in open_ids and now - o.last_action > 5 and o.cancelling_since is None:
                log.warning("dropping ghost L%d %s order %s", o.pair_index, o.side, o.order_id)
                self._remove_order(o.order_id)
        mine = set(self.orders)
        if now - self.last_place_ts < 3:
            return
        recent_closed = {oid for ts, oid in self._recently_closed if now - ts < 15.0}
        reqs = []
        for oid in (open_ids - mine - recent_closed):
            if oid and oid != "None":
                self._recently_closed.append((now, oid))
                log.warning("cancelling orphan order %s for market %s (id %d)", oid, m.name, m.market_id)
                reqs.append(self.ex.write(self.signer.cancel(m, oid)))
        await self._gather(reqs)


    def on_dry_bbo(self, bid: Decimal, ask: Decimal, now: float) -> None:
        """Simulates fills in DRY_RUN=1 when live BBO crosses or touches our resting paper orders."""
        if not self.cfg.dry_run or not self.orders:
            return
        for o in list(self.orders.values()):
            if o.is_taker or o.cancelling_since is not None or o.remaining <= ZERO:
                continue
            if (o.side == BUY and ask <= o.price) or (o.side == SELL and bid >= o.price):
                new_cum = o.qty
                self._apply(o, {
                    "orderId": o.order_id, "state": "FILLED", "status": "FILLED",
                    "price": str(o.price), "avgFillPrice": str(o.price),
                    "filledSize": str(new_cum), "remainingSize": "0"
                }, now)

    def on_dry_trade(self, trade_side: str, trade_sz: Decimal, trade_px: Decimal, now: float) -> None:
        """Simulates queue-aware fills in DRY_RUN=1 when live market trades hit or cross our paper orders."""
        if not self.cfg.dry_run or not self.orders or trade_sz <= ZERO:
            return
        rem_flow = trade_sz
        t_side = trade_side.upper()
        # Sort resting orders by price priority
        candidates = sorted(
            [o for o in self.orders.values() if not o.is_taker and o.cancelling_since is None and o.remaining > ZERO],
            key=lambda x: x.price if x.side == SELL else -x.price
        )
        for o in candidates:
            if rem_flow <= ZERO:
                break
            if o.side == BUY:
                # Trade printed at or below our bid price
                if trade_px < o.price or (trade_px == o.price and t_side in ("SELL", "ASK")):
                    if trade_px < o.price:
                        o.queue_ahead = ZERO
                    elif o.queue_ahead > ZERO:
                        deplete = min(o.queue_ahead, rem_flow)
                        o.queue_ahead -= deplete
                        rem_flow -= deplete
                    if rem_flow > ZERO and o.queue_ahead <= ZERO:
                        fill_sz = min(o.remaining, rem_flow)
                        rem_flow -= fill_sz
                        new_cum = getattr(o, "cum_filled", ZERO) + fill_sz
                        new_rem = max(ZERO, o.qty - new_cum)
                        st = "FILLED" if new_rem <= ZERO else "PARTIALLY_FILLED"
                        self._apply(o, {
                            "orderId": o.order_id, "state": st, "status": st,
                            "price": str(o.price), "avgFillPrice": str(o.price),
                            "filledSize": str(new_cum), "remainingSize": str(new_rem)
                        }, now)
            else:
                # Trade printed at or above our ask price
                if trade_px > o.price or (trade_px == o.price and t_side in ("BUY", "BID")):
                    if trade_px > o.price:
                        o.queue_ahead = ZERO
                    elif o.queue_ahead > ZERO:
                        deplete = min(o.queue_ahead, rem_flow)
                        o.queue_ahead -= deplete
                        rem_flow -= deplete
                    if rem_flow > ZERO and o.queue_ahead <= ZERO:
                        fill_sz = min(o.remaining, rem_flow)
                        rem_flow -= fill_sz
                        new_cum = getattr(o, "cum_filled", ZERO) + fill_sz
                        new_rem = max(ZERO, o.qty - new_cum)
                        st = "FILLED" if new_rem <= ZERO else "PARTIALLY_FILLED"
                        self._apply(o, {
                            "orderId": o.order_id, "state": st, "status": st,
                            "price": str(o.price), "avgFillPrice": str(o.price),
                            "filledSize": str(new_cum), "remainingSize": str(new_rem)
                        }, now)
