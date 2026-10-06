"""Predictive layer for the Arcus market maker (pure python, no numpy).

Four online-learned models, all fed by the bot's own live data and persisted to disk:

  FILL     P(fill within H | quote, X)          logistic   (label: did our resting quote fill in H seconds)
  ADVERSE  P(post-fill drift < -thr | fill, X)  logistic   (label: mid drift 2s after OUR fill)
  DRIFT    E[post-fill drift | fill, X]  2s/5s  NLMS ridge (bps, + = favourable for the side that filled)
  HOLD     E[future return of an OPEN position over H | X] and P(big loss)
           trained on unconditional both-direction samples (so it learns from the first minute, even
           while flat) plus open-position extras (hold time, unrealised bps, inventory ratio)

Everything is signed by side: feature > 0 means "market is moving in favour of that side's position".
Hand-set PRIOR weights make the models sane on day one; online SGD with L2-toward-prior then adapts them
to this market. The engine blends model output with the old heuristics by a warm-up weight, so a model
that has seen little data cannot dominate.

Expected PnL of a quote (bps of notional):
    EV = P(fill) * ( capture + E[drift | fill] ) - fee - inventory_cost
and a quote is only shown when EV clears the engine's min_ev gate.
"""
from __future__ import annotations

import json
import logging
import math
import os
import tempfile
import time
from collections import deque
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("predictor")

BUY, SELL = "BUY", "SELL"

FEATURES = [
    "obi1", "obi5", "obi10", "micro", "tfi250", "tfi1", "tfi5", "tfi10", "tfi_acc",
    "ret1", "ret5", "vol", "spread", "trade_int", "fragility", "consume", "queue",
    "dist", "inv", "xvel", "xdiv", "basis", "funding", "tox",
    # open-position extras (zero for unconditional samples)
    "hold_t", "pos_ratio", "unreal",
    "bias",
]
NF = len(FEATURES)
IX = {n: i for i, n in enumerate(FEATURES)}


def _vec(**kw) -> List[float]:
    v = [0.0] * NF
    for k, x in kw.items():
        v[IX[k]] = x
    return v


# ---- hand-set priors (bps / logit units on the fixed-scale features above) ---------------------------------
PRIOR_DRIFT = _vec(obi1=0.30, obi5=0.25, obi10=0.10, micro=0.35, tfi250=0.12, tfi1=0.25, tfi5=0.15,
                   tfi10=0.05, tfi_acc=0.10, ret1=0.12, ret5=0.10, vol=-0.05, fragility=-0.40, consume=-0.08,
                   tox=-0.15, bias=-0.30)
PRIOR_ADV = _vec(obi1=-0.80, obi5=-0.50, obi10=-0.20, micro=-0.70, tfi250=-0.40, tfi1=-0.90, tfi5=-0.40,
                 tfi_acc=-0.20, ret1=-0.30, ret5=-0.25, vol=0.30, fragility=0.80, consume=0.15, tox=0.40,
                 bias=-0.20)
PRIOR_FILL = _vec(obi1=-0.30, tfi1=-0.80, tfi250=-0.30, trade_int=0.40, consume=0.80, queue=-0.50,
                  dist=-0.90, spread=0.10, bias=-0.60)
PRIOR_HOLD = _vec(obi1=0.25, obi5=0.20, micro=0.30, tfi250=0.10, tfi1=0.25, tfi5=0.15, ret1=0.12, ret5=0.10,
                  fragility=-0.30, hold_t=-0.05, tox=-0.10)
PRIOR_HOLD_LOSS = [-x * 1.4 for x in PRIOR_HOLD]
PRIOR_HOLD_LOSS[IX["bias"]] = -1.2


def _clip(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _f(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


class OnlineModel:
    """Logistic (kind='logit') or NLMS ridge (kind='ridge') regression with L2 pulled toward a prior."""

    def __init__(self, kind: str, prior: List[float], lr: float, l2: float = 0.002, ytol: float = 8.0):
        self.kind = kind
        self.prior = list(prior)
        self.w = list(prior)
        self.lr = lr
        self.l2 = l2
        self.ytol = ytol
        self.n = 0
        self.ema_loss = 0.0
        self.ema_pred = 0.0
        self.ema_obs = 0.0
        self.ema_hit = 0.5
        self.res_var = 4.0   # ridge residual variance (bps^2)

    def raw(self, x: List[float]) -> float:
        return sum(a * b for a, b in zip(self.w, x))

    def predict(self, x: List[float]) -> float:
        z = self.raw(x)
        if self.kind == "logit":
            z = _clip(z, -12.0, 12.0)
            return _clip(1.0 / (1.0 + math.exp(-z)), 0.01, 0.99)
        return _clip(z, -self.ytol, self.ytol)

    def update(self, x: List[float], y: float) -> None:
        p = self.predict(x)
        nrm = 1.0 + sum(v * v for v in x)
        lr = self.lr / math.sqrt(1.0 + self.n / 3000.0)
        if self.kind == "logit":
            g = (y - p)
            loss = -(y * math.log(p) + (1 - y) * math.log(1 - p))
        else:
            g = _clip(y - p, -self.ytol, self.ytol)
            loss = g * g
            self.res_var = 0.995 * self.res_var + 0.005 * max(0.05, g * g)
            if y != 0.0:   # a static market (no move) says nothing about direction
                self.ema_hit = 0.99 * self.ema_hit + 0.01 * (1.0 if (p * y) > 0 else 0.0)
        step = lr * g / nrm
        for i in range(NF):
            self.w[i] += step * x[i] - lr * self.l2 * (self.w[i] - self.prior[i])
        self.n += 1
        a = 0.01 if self.n > 100 else 1.0 / self.n
        self.ema_loss += a * (loss - self.ema_loss)
        self.ema_pred += a * (p - self.ema_pred)
        self.ema_obs += a * (y - self.ema_obs)

    def to_dict(self) -> dict:
        return {"kind": self.kind, "w": self.w, "n": self.n, "res_var": self.res_var,
                "ema_loss": self.ema_loss, "ema_pred": self.ema_pred, "ema_obs": self.ema_obs,
                "ema_hit": self.ema_hit}

    def load_dict(self, d: dict) -> None:
        w = d.get("w")
        if isinstance(w, list) and len(w) == NF:
            self.w = [float(a) for a in w]
            self.n = int(d.get("n", 0))
            self.res_var = float(d.get("res_var", 4.0))
            self.ema_loss = float(d.get("ema_loss", 0.0))
            self.ema_pred = float(d.get("ema_pred", 0.0))
            self.ema_obs = float(d.get("ema_obs", 0.0))
            self.ema_hit = float(d.get("ema_hit", 0.5))


def _ncdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


class Predictor:
    def __init__(self, cfg):
        self.cfg = cfg
        self.enabled = bool(getattr(cfg, "enable_predictor", True))
        self.path = getattr(cfg, "predictor_path", "predictor_state.json")
        self.m_fill = OnlineModel("logit", PRIOR_FILL, lr=0.30)
        self.m_adv = OnlineModel("logit", PRIOR_ADV, lr=0.30)
        self.m_d2 = OnlineModel("ridge", PRIOR_DRIFT, lr=0.20)
        self.m_d5 = OnlineModel("ridge", PRIOR_DRIFT, lr=0.20, ytol=12.0)
        self.m_hold = OnlineModel("ridge", PRIOR_HOLD, lr=0.15, ytol=12.0)
        self.m_hloss = OnlineModel("logit", PRIOR_HOLD_LOSS, lr=0.25)
        self.models = {"fill": self.m_fill, "adv": self.m_adv, "d2": self.m_d2, "d5": self.m_d5,
                       "hold": self.m_hold, "hloss": self.m_hloss}
        self.fill_h = float(getattr(cfg, "pred_fill_horizon_s", 2.0))
        self.hold_h = float(getattr(cfg, "pred_hold_horizon_s", 5.0))
        self.adv_thr = float(getattr(cfg, "pred_adv_thresh_bps", 0.4))
        self.loss_thr = float(getattr(cfg, "pred_hold_loss_bps", 1.5))
        self.sample_s = float(getattr(cfg, "pred_sample_s", 0.5))
        self.weight = float(getattr(cfg, "pred_weight", 0.8))
        self._pend_fill: deque = deque()     # (ts, side, level, vec, deadline)
        self._last_fill_sample: Dict[tuple, float] = {}
        self._pend_fill_obs: List[tuple] = []  # (deadline, h, vec, side_sign, mid0)  post-fill drift
        self._pend_hold: deque = deque()     # (deadline, vec, sign, mid0)
        self._ring = {BUY: deque(maxlen=40), SELL: deque(maxlen=40)}  # (ts, vec)
        self._last_ring = 0.0
        self._last_hold_sample = 0.0
        self._last_save = time.time()
        self._dirty = False
        self._cache: Dict[tuple, dict] = {}
        self._cache_now = -1.0
        self.last_fill_pred: dict = {}
        self.load()

    # ------------------------------------------------------------------ features
    def features(self, md, side: str, now: float, price: Optional[Decimal] = None, ledger=None,
                 extras: bool = False, pos_usd: float = 0.0, unreal_bps: float = 0.0, hold_s: float = 0.0
                 ) -> List[float]:
        s = 1.0 if side == BUY else -1.0
        try:
            mid = md.mid
            v = [0.0] * NF
            v[IX["obi1"]] = s * _f(md.obi)
            v[IX["obi5"]] = s * _f(md.multi_depth_obi(5))
            v[IX["obi10"]] = s * _f(md.multi_depth_obi(10))
            v[IX["micro"]] = _clip(s * _f(md.micro_spread_bps()) / 1.5, -3, 3)
            v[IX["tfi250"]] = s * _f(md.tfi_horizon(0.25, now))
            v[IX["tfi1"]] = s * _f(md.tfi_horizon(1.0, now))
            v[IX["tfi5"]] = s * _f(md.tfi_horizon(5.0, now))
            v[IX["tfi10"]] = s * _f(md.tfi_horizon(10.0, now))
            v[IX["tfi_acc"]] = s * _clip(_f(md.trade_flow_acceleration(now)), -1.5, 1.5)
            v[IX["ret1"]] = _clip(s * _f(md.ret_bps(1.0, now)) / 2.0, -3, 3)
            v[IX["ret5"]] = _clip(s * _f(md.ret_bps(5.0, now)) / 4.0, -3, 3)
            v[IX["vol"]] = _clip(_f(md.vol_bps) / 5.0, 0, 3)
            v[IX["spread"]] = _clip(_f(md.spread_bps) / 3.0, 0, 3)
            try:
                _, _, bu, su = md.trade_rates(1.0, now)
                v[IX["trade_int"]] = _clip(math.log1p(_f(bu) + _f(su)) / 8.0, 0, 2)
            except Exception:
                pass
            v[IX["fragility"]] = _clip(_f(md.liquidity_fragility(side, now)), 0, 2)
            v[IX["consume"]] = _clip(math.log1p(_f(md.consumption_rate(side, 2.0, now))) / 5.0, 0, 2)
            if price is not None and mid:
                q = _f(md.queue_ahead(side, price))
                v[IX["queue"]] = _clip(math.log1p(q) / 6.0, 0, 2)
                v[IX["dist"]] = _clip(abs(_f(mid) - _f(price)) / _f(mid) * 1e4 / 3.0, 0, 3)
            cfg = self.cfg
            if cfg.max_position_usd and _f(cfg.max_position_usd) > 0:
                v[IX["inv"]] = _clip(s * pos_usd / _f(cfg.max_position_usd), -1, 1)
            try:
                if getattr(cfg, "enable_cross_exchange", False) and md.cross.venues:
                    v[IX["xvel"]] = _clip(s * _f(md.cross.cross_velocity_bps(3.0, now)) / 3.0, -3, 3)
                    v[IX["xdiv"]] = _clip(s * _f(md.cross.lead_lag_divergence_bps(mid, now)) / 3.0, -3, 3)
            except Exception:
                pass
            v[IX["basis"]] = _clip(s * _f(md.reference_basis_bps()) / 5.0, -3, 3)
            v[IX["funding"]] = _clip(s * _f(getattr(md, "funding_rate", 0)) * 1e4, -2, 2)
            if ledger is not None:
                v[IX["tox"]] = _clip(_f(ledger.side_tox_bps(side)) / 2.0, 0, 3)
            if extras:
                v[IX["hold_t"]] = _clip(hold_s / 30.0, 0, 3)
                v[IX["pos_ratio"]] = _clip(abs(pos_usd) / max(1.0, _f(cfg.max_position_usd)), 0, 1)
                v[IX["unreal"]] = _clip(unreal_bps / 5.0, -3, 3)
            v[IX["bias"]] = 1.0
            return v
        except Exception:
            v = [0.0] * NF
            v[IX["bias"]] = 1.0
            return v

    # ------------------------------------------------------------------ weights
    def _w(self, n: int, full: int, floor: float = 0.35) -> float:
        return self.weight * (floor + (1.0 - floor) * min(1.0, n / max(1, full)))

    # ------------------------------------------------------------------ quote-time predictions
    def quote_eval(self, side: str, price: Decimal, md, ledger, now: float) -> Optional[dict]:
        if not self.enabled:
            return None
        if now != self._cache_now:
            self._cache.clear()
            self._cache_now = now
        key = ("q", side, str(price))
        hit = self._cache.get(key)
        if hit is not None:
            return hit
        pos_usd = _f(ledger.position) * _f(md.mid) if md.mid else 0.0
        x = self.features(md, side, now, price=price, ledger=ledger, pos_usd=pos_usd)
        d2 = self.m_d2.predict(x)
        d5 = self.m_d5.predict(x)
        out = {
            "p_fill": self.m_fill.predict(x),
            "p_adv": self.m_adv.predict(x),
            "drift": d2, "drift5": d5,
            "sigma5": math.sqrt(max(0.05, self.m_d5.res_var)),
            "w_fill": self._w(self.m_fill.n, int(getattr(self.cfg, "pred_fill_full_n", 600))),
            "w_drift": self._w(self.m_d2.n, int(getattr(self.cfg, "pred_warmup_fills", 40))),
        }
        self._cache[key] = out
        return out

    def p_pnl_positive(self, q: dict, capture_bps: float, fee_bps: float) -> float:
        """P(this fill's net PnL > 0) assuming gaussian residual around predicted 5s drift."""
        mu = capture_bps + q["drift5"] - fee_bps
        return _ncdf(mu / q["sigma5"])

    def side_quality(self, side: str, md, ledger, now: float) -> float:
        """Level-0 P(positive pnl fill) for this side, used for size scaling."""
        if not self.enabled or not md.bid or not md.ask:
            return 1.0
        px = md.bid if side == BUY else md.ask
        q = self.quote_eval(side, px, md, ledger, now)
        if q is None:
            return 1.0
        cap = _f(md.spread_bps) / 2.0
        return self.p_pnl_positive(q, cap, _f(self.cfg.maker_fee_bps))

    # ------------------------------------------------------------------ open-position outlook
    def hold_outlook(self, pos_side: str, md, ledger, now: float, unreal_bps: float, hold_s: float
                     ) -> Optional[Tuple[float, float, float]]:
        """pos_side=BUY for a long, SELL for a short. Returns (E[ret over H] bps in my favour, P(loss>thr), weight)."""
        if not self.enabled:
            return None
        pos_usd = _f(ledger.position) * _f(md.mid) if md.mid else 0.0
        key = ("h", pos_side)
        if now != self._cache_now:
            self._cache.clear()
            self._cache_now = now
        hit = self._cache.get(key)
        if hit is not None:
            return hit
        x = self.features(md, pos_side, now, ledger=ledger, extras=True, pos_usd=pos_usd,
                          unreal_bps=unreal_bps, hold_s=hold_s)
        out = (self.m_hold.predict(x), self.m_hloss.predict(x),
               self._w(self.m_hold.n, int(getattr(self.cfg, "pred_hold_full_n", 400))))
        self._cache[key] = out
        return out

    # ------------------------------------------------------------------ data collection
    def register_quotes(self, targets, md, ledger, now: float) -> None:
        """Create fill-label samples for each live ADDING quote (one per side/level every sample_s)."""
        if not self.enabled:
            return
        pos_usd = _f(ledger.position) * _f(md.mid) if md.mid else 0.0
        for t in targets:
            if getattr(t, "is_taker", False) or getattr(t, "is_exit_quote", False):
                continue
            key = (t.side, t.pair_index)
            if now - self._last_fill_sample.get(key, -1e9) < self.sample_s:
                continue
            self._last_fill_sample[key] = now
            x = self.features(md, t.side, now, price=t.price, ledger=ledger, pos_usd=pos_usd)
            self._pend_fill.append((now, t.side, t.pair_index, x, now + self.fill_h))
        while len(self._pend_fill) > 4000:
            self._pend_fill.popleft()

    def on_tick(self, md, ledger, now: float) -> None:
        if not self.enabled or not md.mid:
            return
        mid = _f(md.mid)
        # 1) ring buffer of pre-fill snapshots (touch price) every 100ms
        if now - self._last_ring >= 0.1 and md.bid and md.ask:
            self._last_ring = now
            pos_usd = _f(ledger.position) * mid
            self._ring[BUY].append((now, self.features(md, BUY, now, price=md.bid, ledger=ledger, pos_usd=pos_usd)))
            self._ring[SELL].append((now, self.features(md, SELL, now, price=md.ask, ledger=ledger, pos_usd=pos_usd)))
        # 2) expire fill samples -> label 0
        while self._pend_fill and self._pend_fill[0][4] <= now:
            _, _, _, x, _ = self._pend_fill.popleft()
            self.m_fill.update(x, 0.0)
            self._dirty = True
        # 3) unconditional + open-position hold samples (1/s)
        if now - self._last_hold_sample >= 1.0 and md.bid and md.ask:
            self._last_hold_sample = now
            pos = _f(ledger.position)
            pos_usd = pos * mid
            open_pos = abs(pos_usd) >= 5.0
            unreal = 0.0
            hold_s = 0.0
            if open_pos and _f(ledger.avg_cost) > 0:
                unreal = (mid - _f(ledger.avg_cost)) / _f(ledger.avg_cost) * 1e4 * (1 if pos > 0 else -1)
                hold_s = ledger.hold_s(now)
            dead = (not open_pos) and _f(md.vol_bps) < 0.01
            for side in () if dead else (BUY, SELL):
                is_mine = open_pos and ((side == BUY) == (pos > 0))
                x = self.features(md, side, now, ledger=ledger, extras=is_mine, pos_usd=pos_usd,
                                  unreal_bps=unreal, hold_s=hold_s)
                self._pend_hold.append((now + self.hold_h, x, 1.0 if side == BUY else -1.0, mid))
        while self._pend_hold and self._pend_hold[0][0] <= now:
            dl, x, sgn, mid0 = self._pend_hold.popleft()
            if now - dl > 3.0 * self.hold_h or mid0 <= 0:
                continue
            ret = sgn * (mid - mid0) / mid0 * 1e4
            if ret == 0.0 and x[IX["hold_t"]] == 0.0:
                continue
            self.m_hold.update(x, ret)
            self.m_hloss.update(x, 1.0 if ret < -self.loss_thr else 0.0)
            self._dirty = True
        # 4) post-fill drift observations (adverse / drift models)
        if self._pend_fill_obs:
            keep = []
            for item in self._pend_fill_obs:
                dl, h, x, sgn, mid0 = item
                if dl > now:
                    keep.append(item)
                    continue
                if mid0 <= 0 or now - dl > 5.0:
                    continue
                d = sgn * (mid - mid0) / mid0 * 1e4
                if h == 2.0:
                    self.m_d2.update(x, d)
                    self.m_adv.update(x, 1.0 if d < -self.adv_thr else 0.0)
                else:
                    self.m_d5.update(x, d)
                self._dirty = True
            self._pend_fill_obs = keep
        # 5) persistence
        if self._dirty and time.time() - self._last_save > 30.0:
            self.save()

    def on_fill(self, side: str, level: int, price: Decimal, md, ledger, now: float, is_maker: bool = True) -> None:
        if not self.enabled or not is_maker or not md.mid:
            return
        mid = _f(md.mid)
        # fill label = 1 for pending samples on this side/level
        keep = deque()
        hit = 0
        while self._pend_fill:
            it = self._pend_fill.popleft()
            ts, sd, lv, x, dl = it
            if sd == side and lv == level and ts >= now - self.fill_h and hit < 4:
                self.m_fill.update(x, 1.0)
                hit += 1
            else:
                keep.append(it)
        self._pend_fill = keep
        # pre-fill snapshot (>=250ms old if possible) for the adverse/drift models
        snap = None
        for ts, x in self._ring[side]:
            if ts <= now - 0.25:
                snap = x
        if snap is None and self._ring[side]:
            snap = self._ring[side][0][1]
        if snap is None:
            pos_usd = _f(ledger.position) * mid
            snap = self.features(md, side, now, price=price, ledger=ledger, pos_usd=pos_usd)
        snap = list(snap)
        sgn = 1.0 if side == BUY else -1.0
        self._pend_fill_obs.append((now + 2.0, 2.0, snap, sgn, mid))
        self._pend_fill_obs.append((now + 5.0, 5.0, snap, sgn, mid))
        q = {"p_fill": self.m_fill.predict(snap), "p_adv": self.m_adv.predict(snap),
             "drift": self.m_d2.predict(snap), "drift5": self.m_d5.predict(snap)}
        self.last_fill_pred = q
        self._dirty = True

    # ------------------------------------------------------------------ persistence / diagnostics
    def save(self) -> None:
        if not self.path or self.path == os.devnull:
            return
        try:
            d = {"v": 1, "nf": NF, "features": FEATURES, "models": {k: m.to_dict() for k, m in self.models.items()}}
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(self.path)) or ".", suffix=".tmp")
            with os.fdopen(fd, "w") as fh:
                json.dump(d, fh)
            os.replace(tmp, self.path)
            self._last_save = time.time()
            self._dirty = False
        except Exception as e:
            log.debug("predictor save failed: %s", e)

    def load(self) -> None:
        if not self.path or self.path == os.devnull or not os.path.exists(self.path):
            return
        try:
            with open(self.path) as fh:
                d = json.load(fh)
            if d.get("nf") != NF or d.get("features") != FEATURES:
                log.warning("predictor state has a different feature set - starting from priors")
                return
            for k, m in self.models.items():
                if k in d.get("models", {}):
                    m.load_dict(d["models"][k])
        except Exception as e:
            log.warning("predictor load failed: %s", e)

    def summary(self) -> str:
        m = self.models
        return ("PRED n[fill=%d adv=%d d2=%d hold=%d] fill(pred=%.3f obs=%.3f) adv(pred=%.2f obs=%.2f) "
                "drift2 hit=%.0f%% hold hit=%.0f%% sigma5=%.2fbps") % (
            m["fill"].n, m["adv"].n, m["d2"].n, m["hold"].n,
            m["fill"].ema_pred, m["fill"].ema_obs, m["adv"].ema_pred, m["adv"].ema_obs,
            100 * m["d2"].ema_hit, 100 * m["hold"].ema_hit, math.sqrt(max(0.05, m["d5"].res_var)))
