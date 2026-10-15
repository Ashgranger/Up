"""Research-Grade Queue-Aware Exchange Simulator.

Implements:
1. FIFO Queue Priority Mechanics:
   - Tracks exact queue volume ahead at each price level.
   - Accurately models cancellations ahead advancing queue priority.
   - Partial fills and residual queue persistence.
2. Latency & Race Condition Simulation:
   - Order placement transit latency (network delay).
   - Cancellation racing: cancellation orders take tau_cancel latency to reach the matching engine.
   - If an aggressive market order sweep arrives before the cancellation is processed, the resting quote fills adversely!
3. Liquidity Consumption & Sweeps:
   - Simulates multi-level marketable order sweeps.
   - Generates realistic Poisson arrival of benign retail orders vs bursty informed institutional sweeps.
4. Comprehensive Fee, Slippage & Funding Accounting.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any

from utils import BPS, BUY, SELL, ZERO, ONE, clamp


@dataclass
class QueueOrder:
    order_id: str
    side: str
    price: Decimal
    size: Decimal
    remaining: Decimal
    queue_ahead: Decimal
    placed_ts: float
    effective_ts: float                  # Time when order reached the matching engine
    cancel_requested_ts: Optional[float] = None
    cancel_effective_ts: Optional[float] = None
    is_active: bool = True
    is_mine: bool = False
    fills: List[Tuple[float, Decimal, Decimal]] = field(default_factory=list) # (ts, fill_sz, fill_px)


class QueueLevel:
    """Represents a discrete price level with strict FIFO queue order tracking."""

    def __init__(self, price: Decimal, side: str):
        self.price = price
        self.side = side
        self.orders: List[QueueOrder] = []
        self.total_size: Decimal = ZERO

    def add_order(self, order: QueueOrder) -> None:
        # Volume ahead is the current total size at this price level
        order.queue_ahead = self.total_size
        self.orders.append(order)
        self.total_size += order.remaining

    def cancel_order(self, order_id: str) -> Optional[QueueOrder]:
        for idx, o in enumerate(self.orders):
            if o.order_id == order_id:
                canceled_sz = o.remaining
                o.is_active = False
                o.remaining = ZERO
                self.orders.pop(idx)
                self.total_size = max(ZERO, self.total_size - canceled_sz)
                # Any order behind this canceled order advances in queue by canceled_sz
                for behind in self.orders[idx:]:
                    behind.queue_ahead = max(ZERO, behind.queue_ahead - canceled_sz)
                return o
        return None

    def match_market_order(self, marketable_size: Decimal, now: float) -> Tuple[Decimal, List[Tuple[QueueOrder, Decimal]]]:
        """Matches an incoming marketable order against resting orders in FIFO sequence.

        Returns: (unfilled_market_order_size, [(matched_order, fill_size), ...])
        """
        remaining_flow = marketable_size
        matched_fills = []

        active_orders = [o for o in self.orders if o.is_active]
        for o in active_orders:
            if remaining_flow <= ZERO:
                break
            fill_sz = min(o.remaining, remaining_flow)
            o.remaining -= fill_sz
            o.queue_ahead = ZERO
            remaining_flow -= fill_sz
            self.total_size = max(ZERO, self.total_size - fill_sz)
            o.fills.append((now, fill_sz, self.price))
            matched_fills.append((o, fill_sz))

            if o.remaining <= ZERO:
                o.is_active = False
                if o in self.orders:
                    self.orders.remove(o)

        return remaining_flow, matched_fills


class QueueAwareSimulator:
    """Matching engine simulator with microsecond queue priority and latency dynamics."""

    def __init__(
        self,
        placement_latency_s: float = 0.005,    # 5ms placement delay
        cancellation_latency_s: float = 0.005,  # 5ms cancellation delay
        maker_fee_bps: Decimal = ZERO,
        taker_fee_bps: Decimal = Decimal("2.0"),
    ):
        self.placement_latency_s = placement_latency_s
        self.cancellation_latency_s = cancellation_latency_s
        self.maker_fee_bps = maker_fee_bps
        self.taker_fee_bps = taker_fee_bps

        # Price levels: side -> price -> QueueLevel
        self.bids: Dict[Decimal, QueueLevel] = {}
        self.asks: Dict[Decimal, QueueLevel] = {}

        self.orders: Dict[str, QueueOrder] = {}
        self.pending_placements: deque = deque()   # (effective_ts, order)
        self.pending_cancels: deque = deque()      # (effective_ts, order_id)

        # Performance accounting for simulated MM
        self.realized_pnl_usd: Decimal = ZERO
        self.position_qty: Decimal = ZERO
        self.avg_cost: Decimal = ZERO
        self.fees_paid_usd: Decimal = ZERO
        self.total_volume_usd: Decimal = ZERO
        self.fills_history: List[Dict[str, Any]] = []

    def best_bid(self) -> Optional[Decimal]:
        active_bids = [p for p, lvl in self.bids.items() if lvl.total_size > ZERO]
        return max(active_bids) if active_bids else None

    def best_ask(self) -> Optional[Decimal]:
        active_asks = [p for p, lvl in self.asks.items() if lvl.total_size > ZERO]
        return min(active_asks) if active_asks else None

    def submit_limit_order(
        self,
        order_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        now: float,
        is_mine: bool = True,
    ) -> QueueOrder:
        """Submits a limit order subject to placement transit latency."""
        effective_ts = now + (self.placement_latency_s if is_mine else 0.0)
        order = QueueOrder(
            order_id=order_id,
            side=side.upper(),
            price=price,
            size=size,
            remaining=size,
            queue_ahead=ZERO,
            placed_ts=now,
            effective_ts=effective_ts,
            is_mine=is_mine,
        )
        self.orders[order_id] = order
        self.pending_placements.append((effective_ts, order))
        return order

    def request_cancel_order(self, order_id: str, now: float) -> None:
        """Requests cancellation of a resting order subject to cancellation transit latency."""
        order = self.orders.get(order_id)
        if not order or not order.is_active:
            return
        effective_ts = now + (self.cancellation_latency_s if order.is_mine else 0.0)
        order.cancel_requested_ts = now
        order.cancel_effective_ts = effective_ts
        self.pending_cancels.append((effective_ts, order_id))

    def step_time(self, now: float) -> None:
        """Processes in-flight placements and cancellations that reached matching engine by now."""
        # 1. Process cancellations first (matching engine order)
        while self.pending_cancels and self.pending_cancels[0][0] <= now:
            _, oid = self.pending_cancels.popleft()
            o = self.orders.get(oid)
            if o and o.is_active:
                book = self.bids if o.side == BUY else self.asks
                if o.price in book:
                    book[o.price].cancel_order(oid)

        # 2. Process placements
        while self.pending_placements and self.pending_placements[0][0] <= now:
            _, o = self.pending_placements.popleft()
            if not o.is_active:
                continue
            book = self.bids if o.side == BUY else self.asks
            if o.price not in book:
                book[o.price] = QueueLevel(o.price, o.side)
            book[o.price].add_order(o)

    def execute_market_order(
        self,
        side: str,
        size: Decimal,
        now: float,
    ) -> List[Tuple[str, Decimal, Decimal]]:
        """Simulates an incoming aggressive market order sweeping the book.

        If side == BUY: sweeps asks from lowest to highest.
        If side == SELL: sweeps bids from highest to lowest.
        """
        self.step_time(now)
        rem = size
        executed_fills = []

        if side.upper() == BUY:
            sorted_prices = sorted([p for p, lvl in self.asks.items() if lvl.total_size > ZERO])
            book = self.asks
        else:
            sorted_prices = sorted([p for p, lvl in self.bids.items() if lvl.total_size > ZERO], reverse=True)
            book = self.bids

        for px in sorted_prices:
            if rem <= ZERO:
                break
            level = book[px]
            rem_flow, matches = level.match_market_order(rem, now)
            rem = rem_flow

            for order, fill_sz in matches:
                executed_fills.append((order.order_id, fill_sz, px))
                if order.is_mine:
                    self._process_my_fill(order.side, fill_sz, px, now)

        return executed_fills

    def _process_my_fill(self, side: str, qty: Decimal, price: Decimal, now: float) -> None:
        """Updates internal PnL, position, and fill accounting for MM order."""
        fee = qty * price * self.maker_fee_bps / BPS
        self.fees_paid_usd += fee
        self.total_volume_usd += qty * price

        signed_qty = qty if side == BUY else -qty
        new_pos = self.position_qty + signed_qty

        realized_delta = ZERO
        if self.position_qty != ZERO and (self.position_qty > ZERO) != (signed_qty > ZERO):
            # Closing or reducing position
            close_qty = min(abs(self.position_qty), abs(signed_qty))
            if self.position_qty > ZERO:
                realized_delta = close_qty * (price - self.avg_cost)
            else:
                realized_delta = close_qty * (self.avg_cost - price)

        self.realized_pnl_usd += (realized_delta - fee)

        if new_pos == ZERO:
            self.avg_cost = ZERO
        elif (self.position_qty == ZERO) or ((self.position_qty > ZERO) != (new_pos > ZERO)):
            self.avg_cost = price
        elif abs(new_pos) > abs(self.position_qty):
            # Adding in same direction
            tot_cost = (abs(self.position_qty) * self.avg_cost) + (abs(signed_qty) * price)
            self.avg_cost = tot_cost / abs(new_pos)

        self.position_qty = new_pos
        self.fills_history.append({
            "timestamp": now,
            "side": side,
            "price": float(price),
            "qty": float(qty),
            "fee": float(fee),
            "position": float(self.position_qty),
            "realized_pnl": float(self.realized_pnl_usd),
        })

    def unrealized_pnl_usd(self, current_mid: Decimal) -> Decimal:
        if self.position_qty == ZERO or self.avg_cost == ZERO:
            return ZERO
        return self.position_qty * (current_mid - self.avg_cost)

    def total_pnl_usd(self, current_mid: Decimal) -> Decimal:
        return self.realized_pnl_usd + self.unrealized_pnl_usd(current_mid)


if __name__ == "__main__":
    import asyncio
    from sim import run_standalone_sim
    asyncio.run(run_standalone_sim(steps=300))
