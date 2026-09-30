"""Order book of *our own* orders. Nothing rests on the exchange that this class doesn't know about.

Sources of truth, in order: place ACK -> `orders` channel updates -> periodic `get orders` reconcile
(ghosts are dropped, unknown orders are cancelled). All orders are LIMIT + ALO (post-only).
"""
from __future__ import annotations

import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Optional

from exchange import Exchange
from market import Market
from signer import Signer
from utils import BUY, SELL, fmt

log = logging.getLogger("orders")
GTT_DAYS = 40  # goodTilTime must be >= ~1 month ahead on EVERY order (docs)


@dataclass
class Order:
    order_id: str
    side: str
    price: Decimal
    qty: Decimal
    remaining: Decimal
    good_til_us: int
    created: float
    last_action: float
    filled_any: bool = False
    cancelling_since: Optional[float] = None
    level: int = 0            # 0 = touch quote, 1+ = ladder level (tagged so each fill can be analysed by level)
    role: str = ""            # "add" | "reduce"


class OrderManager:
    def __init__(self, cfg, ex: Exchange, signer: Signer, get_market: Callable[[], Market],
                 on_fill: Callable[[str, Decimal, Decimal, Order], None]):
        self.cfg, self.ex, self.signer = cfg, ex, signer
        self.get_market = get_market
        self.on_fill = on_fill
        self.orders: dict[str, Order] = {}
        self._unmatched: dict[str, tuple] = {}       # updates that beat their own ACK
        self._recently_closed: deque = deque(maxlen=200)   # (ts, order_id): never "orphan-cancel" these
        self.reject_until = {BUY: 0.0, SELL: 0.0}
        self._reject_n = {BUY: 0, SELL: 0}
        self._actions: deque = deque()
        self.paused_until = 0.0
        self._consec_errors = 0
        self.last_place_ts = 0.0
        self.maybe_orders = True   # unknown leftovers (previous run) -> clean slate at start
        self.n_place = self.n_modify = self.n_cancel = self.n_reject = self.n_actions = 0

    # ---- views ------------------------------------------------------------ #
    def side_orders(self, side: str) -> list:
        return sorted((o for o in self.orders.values() if o.side == side and o.role != "take"),
                      key=lambda o: o.created)

    def live_levels(self) -> frozenset:
        """{(level, side)} of every resting order (feeds the strategy's EV hysteresis)."""
        return frozenset((o.level, o.side) for o in self.orders.values()
                         if o.cancelling_since is None and o.role != "take")

    def _forget(self, order_id: str) -> None:
        """Drop an order from our book and remember it briefly, so a slightly stale `get orders`
        that still lists it is not mistaken for an unknown orphan."""
        if self.orders.pop(order_id, None) is not None:
            self._recently_closed.append((time.time(), order_id))

    def open_qty(self, side: str) -> Decimal:
        return sum((o.remaining for o in self.orders.values() if o.side == side and o.role != "take"), Decimal(0))

    def describe(self, now: float) -> str:
        if not self.orders:
            return "none"
        return " | ".join(f"{'B' if o.side == BUY else 'S'} {fmt(o.remaining)}@{fmt(o.price)} ({now - o.created:.0f}s)"
                          for o in sorted(self.orders.values(), key=lambda o: o.side))

    # ---- helpers ------------------------------------------------------------ #
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
        if self._consec_errors >= 8:
            log.error("too many consecutive errors - pausing 30s")
            self.paused_until = now + 30
            self._consec_errors = 0

    def _backoff(self, side: str, now: float) -> None:
        self._reject_n[side] += 1
        self.reject_until[side] = now + min(0.25 * 2 ** (self._reject_n[side] - 1), 4.0)

    # ---- actions -------------------------------------------------------------- #
    async def place(self, side: str, px: Decimal, qty: Decimal, now: float,
                    level: int = 0, role: str = "") -> Optional[Order]:
        if now < self.paused_until or not self._budget(now):
            return None
        good_til = int(time.time() * 1_000_000) + GTT_DAYS * 86_400 * 1_000_000
        req = self.signer.place(self.get_market(), side, px, qty, good_til)
        self.maybe_orders = True
        self.last_place_ts = now
        resp = await self.ex.write(req)
        res = resp.get("result") or {}
        if not self._ok(resp) or not res.get("orderId") or str(res.get("status")).upper() == "REJECTED":
            self._error(f"place {side}", resp if not self._ok(resp) else
                        {"status": resp.get("status"), "error": res}, now)
            self._backoff(side, now)
            return None
        self._consec_errors = 0
        self.n_place += 1
        o = Order(res["orderId"], side, px, qty, qty, good_til, now, now, level=level, role=role)
        self.orders[o.order_id] = o
        log.info("PLACE %s %s @ %s", side, fmt(qty), fmt(px))
        early = self._unmatched.pop(o.order_id, None)
        if early:
            self._apply(o, early[1], now)
        return o

    async def take(self, side: str, px_limit: Decimal, qty: Decimal, now: float) -> Optional[Order]:
        """Bounded taker cut: IOC + reduce-only LIMIT at `px_limit` (worst acceptable price). Bypasses the
        requote budget on purpose - getting out of a losing position must not queue behind quote churn -
        but still honours a 429 pause."""
        if now < self.paused_until:
            return None
        good_til = int(time.time() * 1_000_000) + GTT_DAYS * 86_400 * 1_000_000
        req = self.signer.place_ioc(self.get_market(), side, px_limit, qty, good_til, reduce_only=True)
        self.n_actions += 1
        resp = await self.ex.write(req)
        res = resp.get("result") or {}
        if not self._ok(resp) or not res.get("orderId") or str(res.get("status")).upper() == "REJECTED":
            self._error(f"take {side}", resp if not self._ok(resp) else {"status": resp.get("status"), "error": res}, now)
            return None
        self._consec_errors = 0
        self.n_place += 1
        o = Order(res["orderId"], side, px_limit, qty, qty, good_til, now, now, level=0, role="take")
        self.orders[o.order_id] = o
        log.warning("TAKE %s %s IOC limit %s (reduce-only)", side, fmt(qty), fmt(px_limit))
        early = self._unmatched.pop(o.order_id, None)
        if early:
            self._apply(o, early[1], now)
        return o

    async def modify(self, o: Order, px: Decimal, now: float, urgent: bool = False) -> bool:
        if now < self.paused_until or not self._budget(now):
            if urgent:                       # can't afford a reprice while running from the market -> pull it
                await self.cancel(o, now)
            return False
        req = self.signer.modify(self.get_market(), o.order_id, o.side, px, o.qty, o.good_til_us)
        resp = await self.ex.write(req)
        if not self._ok(resp):
            self._error(f"modify {o.side}", resp, now)
            await self.cancel(o, now)   # unknown state -> pull it, re-place cleanly
            return False
        self._consec_errors = 0
        self.n_modify += 1
        log.info("MODIFY %s %s -> %s", o.side, fmt(o.price), fmt(px))
        o.price, o.last_action = px, now
        return True

    async def cancel(self, o: Order, now: float) -> None:
        if o.cancelling_since is not None and now - o.cancelling_since < 5:
            return
        o.cancelling_since = now
        self._recently_closed.append((time.time(), o.order_id))
        self._budget(now)
        resp = await self.ex.write(self.signer.cancel(self.get_market(), o.order_id))
        if self.cfg.dry_run or "ORDER_NOT_FOUND" in json.dumps(resp):
            self._forget(o.order_id)
        elif not self._ok(resp):
            o.cancelling_since = None
            self._error(f"cancel {o.side}", resp, now)
        else:
            self.n_cancel += 1   # the CANCELED order update removes it

    async def cancel_side(self, side: str, now: float, adds_only: bool = True) -> None:
        """Pull a side immediately (burst / sweep guards). By default only ADD-role orders: an exit
        that happens to sit on that side must stay live so the position can always be unwound."""
        for o in list(self.orders.values()):
            if o.side == side and o.role != "take" and (not adds_only or o.role != "reduce"):
                await self.cancel(o, now)

    async def cancel_all(self, force: bool = False) -> None:
        if not force and not self.orders and not self.maybe_orders:
            return                      # nothing of ours can be resting -> don't spam cancelAllOrders
        m = self.get_market()
        body = {"address": self.cfg.address, "accountIndex": self.cfg.account_index, "marketId": m.market_id}
        resp = await self.ex.write(self.signer.legacy("cancelAllOrders", body))
        log.info("CANCEL-ALL sent (status %s)", resp.get("status"))
        self.orders.clear()
        self.maybe_orders = False

    # ---- inbound order updates ---------------------------------------------------- #
    def on_update(self, c, now: float) -> None:
        if not isinstance(c, dict) or not c.get("orderId"):
            return
        o = self.orders.get(c["orderId"])
        if o is None:
            self._unmatched[c["orderId"]] = (now, c)
            if len(self._unmatched) > 200:
                self._unmatched = {k: v for k, v in self._unmatched.items() if v[0] > now - 30}
            return
        self._apply(o, c, now)

    def _apply(self, o: Order, c: dict, now: float) -> None:
        if c.get("cancelReason") == "MODIFY_CANCELED":
            return  # first leg of a modify; same orderId is re-placed next
        state = str(c.get("state") or "").upper()
        status = str(c.get("status") or "").upper()
        filled = state == "FILLED" or status == "FILLED"
        px = o.price
        try:
            if c.get("price"):
                px = Decimal(str(c["price"]))
        except Exception:
            pass
        fill_qty = Decimal(0)
        rem = c.get("remainingSize")
        if rem is not None:
            try:
                new_rem = Decimal(str(rem))
                if new_rem < o.remaining:
                    fill_qty = o.remaining - new_rem
                o.remaining = new_rem
            except Exception:
                rem = None
        if filled and rem is None:
            fill_qty, o.remaining = o.remaining, Decimal(0)
        if fill_qty > 0:
            o.filled_any = True
            self.on_fill(o.side, fill_qty, px, o)
        if state == "OPEN" or status == "OPEN":
            self._reject_n[o.side] = 0
        if filled:
            self._forget(o.order_id)
        elif state in ("CANCELED", "REJECTED") or status in ("CANCELED", "MARGIN_CANCELED", "REJECTED"):
            reason = c.get("rejectionReason") or c.get("cancelReason") or ""
            if state == "REJECTED" or status == "REJECTED":
                self.n_reject += 1
                if o.role != "take":                       # an IOC that found nothing is not a post-only reject storm
                    self._backoff(o.side, now)
            log.info("ORDER %s %s %s", o.side, state or status, reason)
            self._forget(o.order_id)

    # ---- reconcile with exchange truth --------------------------------------------- #
    async def reconcile(self, rows: list, now: float) -> None:
        open_ids = {str(r.get("orderId") or r.get("id")) for r in rows}
        for o in list(self.orders.values()):
            if o.order_id not in open_ids and now - o.last_action > 5 and o.cancelling_since is None:
                log.warning("dropping ghost %s order %s", o.side, o.order_id)
                self._forget(o.order_id)
        mine = set(self.orders)
        if now - self.last_place_ts < 3:
            return
        wall = time.time()
        recent_closed = {oid for ts, oid in self._recently_closed if wall - ts < 15.0}
        for oid in open_ids - mine - recent_closed:
            if oid and oid != "None":
                self._recently_closed.append((wall, oid))
                log.warning("cancelling orphan order %s", oid)
                await self.ex.write(self.signer.cancel(self.get_market(), oid))
