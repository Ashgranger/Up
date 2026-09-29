"""Request signing per docs.arcus.xyz > Authentication."""
from __future__ import annotations

import time
from decimal import Decimal
from cryptography.hazmat.primitives.asymmetric import ed25519

from market import Market
from utils import canonical, fmt, to_int


class Signer:
    OP_PLACE, OP_CANCEL, OP_MODIFY = 1, 2, 3
    SIDE = {"BUY": 0, "SELL": 1}
    TIF_ALO = 3

    def __init__(self, key_hex: str, address: str, account_index: int):
        self.priv = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(key_hex))
        from cryptography.hazmat.primitives import serialization
        pub = self.priv.public_key()
        if hasattr(pub, 'public_bytes_raw'):
            self.api_key = pub.public_bytes_raw().hex()
        else:
            self.api_key = pub.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw).hex()
        self.address = address
        self.addr_lc = address.lower()
        self.ai = account_index
        self._last_ts = 0

    def next_ts(self) -> int:
        self._last_ts = max(time.time_ns(), self._last_ts + 1)
        return self._last_ts

    def _typed(self, op: int, ts: int, market_id: int, **fields) -> str:
        d = {"ad": self.addr_lc, "ai": self.ai, "ct": ts, "m": market_id, "op": op, "v": 1}
        d.update(fields)
        return canonical(d)

    def _envelope(self, req_type: str, body: dict, message: str, ts: int) -> dict:
        return {"type": req_type, "payload": body, "apiKey": self.api_key,
                "timestamp": str(ts), "signature": self.priv.sign(message.encode()).hex()}

    def place(self, m: Market, side: str, px: Decimal, qty: Decimal, good_til_us: int) -> dict:
        ts = self.next_ts()
        msg = self._typed(self.OP_PLACE, ts, m.market_id, g=good_til_us * 1000,
                          p=to_int(px, m.tick), q=to_int(qty, m.step), r=0,
                          s=self.SIDE[side], t=self.TIF_ALO)
        body = {"address": self.address, "accountIndex": self.ai, "marketId": m.market_id,
                "orderSide": side, "orderType": "LIMIT", "timeInForce": "ALO",
                "goodTilTime": str(good_til_us), "quantity": fmt(qty), "price": fmt(px),
                "timestamp": ts}
        return self._envelope("placeOrder", body, msg, ts)

    def cancel(self, m: Market, order_id: str) -> dict:
        ts = self.next_ts()
        msg = self._typed(self.OP_CANCEL, ts, m.market_id, id=order_id)
        body = {"address": self.address, "accountIndex": self.ai, "marketId": m.market_id,
                "kind": "orderId", "orderId": order_id, "timestamp": ts}
        return self._envelope("cancelOrder", body, msg, ts)

    def modify(self, m: Market, order_id: str, side: str, px: Decimal, qty: Decimal,
               good_til_us: int) -> dict:
        ts = self.next_ts()
        msg = self._typed(self.OP_MODIFY, ts, m.market_id, g=good_til_us * 1000, id=order_id,
                          p=to_int(px, m.tick), q=to_int(qty, m.step), r=0,
                          s=self.SIDE[side], t=self.TIF_ALO)
        body = {"address": self.address, "accountIndex": self.ai, "marketId": m.market_id,
                "orderId": order_id, "side": side, "quantity": fmt(qty), "price": fmt(px),
                "timeInForce": "ALO", "reduceOnly": False, "goodTilTime": str(good_til_us)}
        return self._envelope("modifyOrder", body, msg, ts)

    def legacy(self, action: str, body: dict) -> dict:
        ts = self.next_ts()
        return self._envelope(action, body, f"{ts}{action}{canonical(body)}", ts)
