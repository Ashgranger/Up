"""Quote engine. Pure function of a Snapshot -> Plan (no I/O), so it is unit-testable.

How a professional-style maker quotes, in short:
  * fair value  = micro price, shifted by order-book imbalance (OBI) and trade-flow imbalance (TFI),
                  kept inside the book (Level 5)
  * reservation = fair value skewed against inventory (non-linear in position) plus a
                  volatility-scaled risk-aversion term (Stoikov-style)
  * edge        = min_edge + vol_k*vol, widened in a TOXIC regime, plus a per-side toxicity penalty
  * placement   = penny inside the touch when the spread is wide and flow is calm, otherwise at our edge
  * EV filter   = every add-quote must clear an expected-value hurdle:
                     EV = P(fill) * (capture - expected adverse move) - fee - inventory cost   (Level 4)
  * asymmetric  = the side about to be run over by flow is shaded away; the L0 add-quote is
    shading       suppressed when already loaded on that side or when chasing a move
  * exits       = reduce-side quote never worse than cost + a profit floor that DECAYS with hold time
                  towards breakeven; underwater beyond STRESS_LOSS_BPS -> exit at the touch
  * guards      = only the side that ADDS exposure is ever pulled/widened; the reducing side stays
                  live so we can always get out
All tunables are read through `params` (the online learner when learning is on, the static config
otherwise), so the same code path serves both modes.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

import inventory
from learner import OnlineLearner
from market import Market, classify_regime
from utils import BPS, BUY, SELL, ZERO, ONE, Fatal, bps_diff, clamp, q_down, q_up

MAX_TOX_ADDON_BPS = Decimal("2.5")     # cap on the per-side toxicity penalty added to the edge
FLOW_EV_RISK = Decimal("1.5")          # bps of expected adverse move per unit of opposing TFI


@dataclass
class Snapshot:
    now: float
    market: Market
    bid: Decimal
    ask: Decimal
    mid: Decimal
    micro: Decimal
    position: Decimal
    avg_cost: Decimal
    hold_s: float
    ret_bps: Decimal            # signed short-window return (negative = falling)
    move_bps: Decimal           # short-window high-low range
    vol_bps: Decimal            # smoothed range
    tox_bps: Decimal            # recent negative markout (adverse selection)
    imbalance: Decimal = ZERO   # signed multi-level book depth pressure, -1..1 (MarketData.depth_imbalance)
    cooldown_until: dict = field(default_factory=lambda: {BUY: 0.0, SELL: 0.0})
    jump_active: bool = False
    halted: bool = False
    # ---- Level 5-7 inputs ------------------------------------------------------------------
    obi: Decimal = ZERO                 # top-of-book imbalance, -1..1 (+ = more bid size)
    tfi: Decimal = ZERO                 # trade-flow imbalance over 10s, -1..1 (+ = buyers aggressing)
    buy_tox_bps: Optional[Decimal] = None    # per-side toxicity (None -> use tox_bps)
    sell_tox_bps: Optional[Decimal] = None
    params: Any = None                  # OnlineLearner (or None -> static config values)
    live_levels: frozenset = frozenset()     # {(level, side)} currently resting (EV hysteresis)
    severe_latched: dict = field(default_factory=lambda: {BUY: False, SELL: False})   # bot-held pressure latch


@dataclass
class Target:
    price: Decimal
    qty: Decimal
    role: str                   # "add" | "reduce"
    level: int = 0              # ladder slot: 0 = touch, 1+ = further out
    ev_bps: Optional[Decimal] = None
    p_fill: Optional[float] = None


@dataclass
class Plan:
    bid: Optional[Target]
    ask: Optional[Target]
    edge_bps: Decimal = ZERO
    skew_bps: Decimal = ZERO
    stress: bool = False
    notes: list = field(default_factory=list)
    # side -> guard code ("trend" | "vol" | "jump" | "burst" | "stress" | None) for the ADDING side only,
    # structured (not parsed from `notes`) so the bot can arm a hysteresis cooldown on "trend".
    blocked: dict = field(default_factory=lambda: {BUY: None, SELL: None})
    # extra levels beyond the touch quote, nearest-first (see Config.extra_levels).
    extra_bids: list = field(default_factory=list)
    extra_asks: list = field(default_factory=list)
    regime: str = "REGIME_A_QUIET"
    fair: Decimal = ZERO
    inv: Any = None                       # inventory.InvDecision (state, risk, taker cut request, ...)
    severe: dict = field(default_factory=lambda: {BUY: False, SELL: False})   # raw severe-pressure flags


class Strategy:
    def __init__(self, cfg):
        self.cfg = cfg
        # Used when a Snapshot carries no live parameter object: the static config, via a learner
        # that is switched off (its properties then return the config values verbatim).
        self._static = OnlineLearner(cfg, enabled=False)

    # ------------------------------------------------------------------------------------- #
    def plan(self, s: Snapshot) -> Plan:
        c, m = self.cfg, s.market
        P = s.params if s.params is not None else self._static
        notes: list = []
        tb, ta = m.tick_for(s.bid), m.tick_for(s.ask)
        mid = s.mid
        pos_usd = s.position * mid
        flat = abs(pos_usd) < max(m.min_notional, Decimal(1))
        long_ = (not flat) and s.position > 0
        short_ = (not flat) and s.position < 0
        spread = s.ask - s.bid

        # ---- stress: position is hurting -> stop adding, get out at the touch ----------------
        stress = s.halted
        if s.halted:
            notes.append("HALTED")
        if not flat and s.avg_cost > 0:
            mark = s.bid if long_ else s.ask                       # what we could actually get
            pnl_bps = bps_diff(mark, s.avg_cost) * (1 if long_ else -1)
            if pnl_bps <= -P.stress_loss_bps:
                stress = True
                notes.append(f"stress: position {pnl_bps:.1f}bps underwater")

        # ---- exit profit floor: decays towards breakeven the longer we hold -------------------
        # (schedule anchored on MAX_HOLD_S: 0.5x -> 0.8bps, 1.5x -> 0.2bps; = 60s/180s at 120s)
        floor_bps = max(P.exit_min_profit_bps, ONE)
        max_hold = Decimal(str(P.max_hold_s))
        hold = Decimal(str(s.hold_s))
        if hold > max_hold * Decimal("1.5"):
            floor_bps = Decimal("0.2")
        elif hold > max_hold * Decimal("0.5"):
            floor_bps = Decimal("0.8")
        if not flat and s.hold_s >= P.max_hold_s:
            notes.append(f"held {s.hold_s:.0f}s: exit floor decayed to {floor_bps}bps")

        # ---- order-flow inputs (zeroed when ENABLE_ORDERBOOK_INTEL=0) --------------------------
        intel = c.enable_orderbook_intel
        obi = s.obi if intel else ZERO
        tfi = s.tfi if intel else ZERO
        flow_bias = Decimal("0.6") * obi + Decimal("0.4") * tfi
        half_spr = spread / Decimal(2)

        # ---- inventory manager: risk-scored exit aggressiveness (uses every signal we have) ------
        buy_tox0 = s.buy_tox_bps if s.buy_tox_bps is not None else s.tox_bps
        sell_tox0 = s.sell_tox_bps if s.sell_tox_bps is not None else s.tox_bps
        inv = inventory.assess(
            c, P, position=s.position, avg_cost=s.avg_cost, bid=s.bid, ask=s.ask, mid=mid, hold_s=s.hold_s,
            ret_bps=s.ret_bps, vol_bps=s.vol_bps, obi=obi, tfi=tfi,
            imb=clamp(s.imbalance, -ONE, ONE) if c.use_depth_imbalance else ZERO,
            side_tox_bps=(buy_tox0 if long_ else sell_tox0), flat=flat, halted=s.halted)
        if inv.state != inventory.NORMAL:
            notes.append(f"inventory {inv.state} risk={inv.risk:.2f} flow_against={inv.flow_against:+.2f} "
                         f"pnl={inv.pnl_bps:+.1f}bps exit gives up {inv.give_bps:.2f}bps")
            floor_bps = floor_bps - inv.give_bps           # may go below zero: accept a small loss to get out

        # ---- regime, fair value, reservation price --------------------------------------------
        vol = s.vol_bps
        regime = classify_regime(c, s.tox_bps, tfi, obi, vol)
        toxic = regime == "REGIME_D_TOXIC"
        base_micro = s.micro if (c.use_micro and s.micro) else mid
        if intel:
            fair = base_micro + half_spr * obi * P.obi_alpha + half_spr * tfi * P.tfi_beta
            if s.bid < s.ask:
                fair = clamp(fair, s.bid, s.ask)
        else:
            fair = base_micro
        if toxic:
            notes.append("toxic regime: wider edge, no extra add levels")

        q = clamp(pos_usd / c.max_position_usd, -ONE, ONE) if c.max_position_usd > 0 else ZERO
        q_eff = Decimal(str(math.copysign(math.pow(abs(float(q)), 1.3), float(q))))   # non-linear
        inv_skew_bps = q_eff * P.skew_bps + (q_eff * vol * P.gamma_risk_aversion if vol > 0 else ZERO)
        res = fair * (ONE - inv_skew_bps / BPS)

        edge = P.min_edge_bps + P.vol_k * vol
        if toxic:
            edge = edge * P.regime_toxic_spread_mult
        edge = clamp(edge, P.min_edge_bps, P.max_edge_bps)

        # asymmetric shading: shade the side flow is about to run over (bid on selling flow,
        # ask on buying flow), nudge the other side slightly the opposite way
        bid_asym = max(ZERO, -flow_bias) * P.obi_alpha * half_spr
        ask_asym = -max(ZERO, -flow_bias) * Decimal("0.3") * half_spr
        if flow_bias > 0:
            ask_asym = flow_bias * P.obi_alpha * half_spr
            bid_asym = -flow_bias * Decimal("0.3") * half_spr

        buy_tox = s.buy_tox_bps if s.buy_tox_bps is not None else s.tox_bps
        sell_tox = s.sell_tox_bps if s.sell_tox_bps is not None else s.tox_bps
        buy_pen = min(MAX_TOX_ADDON_BPS, buy_tox * P.tox_mult)
        sell_pen = min(MAX_TOX_ADDON_BPS, sell_tox * P.tox_mult)
        if buy_tox > 0 or sell_tox > 0:
            notes.append(f"toxic fills: +{max(buy_pen, sell_pen):.1f}bps edge on the hit side")

        chasing_top = s.ret_bps > Decimal("0.8") and (toxic or flow_bias > Decimal("0.3"))
        chasing_bottom = s.ret_bps < Decimal("-0.8") and (toxic or flow_bias < Decimal("-0.3"))
        raw_sev_sell = flow_bias < Decimal("-0.50") or (toxic and obi < Decimal("-0.55"))
        raw_sev_buy = flow_bias > Decimal("0.50") or (toxic and obi > Decimal("0.55"))
        # the bot latches these for SEVERE_HOLD_S so a value flickering around the threshold does not
        # cancel/re-place the side every tick
        severe_sell_pressure = raw_sev_sell or s.severe_latched.get(BUY, False)
        severe_buy_pressure = raw_sev_buy or s.severe_latched.get(SELL, False)

        # multi-level depth pressure (leading), on top of the OBI/TFI model above: only ever the
        # ADD side is widened/shrunk, never the exit
        bid_role = "reduce" if short_ else "add"
        ask_role = "reduce" if long_ else "add"
        imb = clamp(s.imbalance, -ONE, ONE) if c.use_depth_imbalance else ZERO
        bid_danger, ask_danger = max(ZERO, -imb), max(ZERO, imb)
        bid_imb_mult = ONE - c.imbalance_size_cut * bid_danger if bid_role == "add" else ONE
        ask_imb_mult = ONE - c.imbalance_size_cut * ask_danger if ask_role == "add" else ONE
        bid_imb_extra = bid_danger * c.imbalance_widen_bps if bid_role == "add" else ZERO
        ask_imb_extra = ask_danger * c.imbalance_widen_bps if ask_role == "add" else ZERO
        if c.use_depth_imbalance and abs(imb) >= Decimal("0.4"):
            notes.append(f"book pressure {imb:+.2f} -> {'ask' if imb > 0 else 'bid'} widened/shrunk")

        # ---- sizes --------------------------------------------------------------------------
        base = q_down(c.order_usd / mid, m.step)
        if base < m.min_size or base * mid < m.min_notional or (m.max_size and base > m.max_size):
            raise Fatal(f"ORDER_USD={c.order_usd} -> qty {base}; market needs size>={m.min_size}, "
                        f"notional>={m.min_notional}, size<={m.max_size}")

        def sized_ok(qty: Decimal, px: Decimal) -> bool:
            return qty >= m.min_size and qty * px >= m.min_notional and (not m.max_size or qty <= m.max_size)

        def reduce_qty() -> Decimal:
            if stress:
                qty = abs(s.position)
            else:
                qty = max(min(abs(s.position), base), abs(s.position) * inv.reduce_frac)
            return q_down(min(qty, abs(s.position)), m.step)

        # ---- guards for ADDING sides --------------------------------------------------------
        def blocked(side: str) -> tuple:
            """(human_text, code); code None when not blocked, else stress/burst/jump/vol/trend."""
            if stress:
                return "stress", "stress"
            if s.cooldown_until.get(side, 0.0) > s.now:
                return "fill-burst/sweep cooldown", "burst"
            if s.jump_active:
                return "price jump", "jump"
            if s.move_bps >= c.vol_pause_bps:
                return f"vol pause ({s.move_bps:.1f}bps range)", "vol"
            if side == BUY and s.ret_bps <= -P.trend_pull_bps:
                return f"falling {s.ret_bps:.1f}bps", "trend"
            if side == SELL and s.ret_bps >= P.trend_pull_bps:
                return f"rising {s.ret_bps:.1f}bps", "trend"
            return None, None

        # ---- expected-value model ---------------------------------------------------------------
        def adverse_bps(side: str) -> Decimal:
            base_tox = buy_tox if side == BUY else sell_tox
            momentum = ZERO
            if side == BUY and s.ret_bps < 0:
                momentum = abs(s.ret_bps) * P.trend_widen
            elif side == SELL and s.ret_bps > 0:
                momentum = s.ret_bps * P.trend_widen
            flow = ZERO
            if side == BUY and tfi < Decimal("-0.2"):
                flow = abs(tfi) * FLOW_EV_RISK
            elif side == SELL and tfi > Decimal("0.2"):
                flow = tfi * FLOW_EV_RISK
            return base_tox + momentum + flow

        def ev_of(side: str, px: Decimal) -> tuple:
            if side == BUY:
                capture = (fair - px) / fair * BPS
                dist = (s.ask - px) / mid * BPS
                inv_cost = max(ZERO, pos_usd / c.max_position_usd) * P.skew_bps
            else:
                capture = (px - fair) / fair * BPS
                dist = (px - s.bid) / mid * BPS
                inv_cost = max(ZERO, -pos_usd / c.max_position_usd) * P.skew_bps
            p_fill = math.exp(-float(P.fill_prob_kappa) * float(max(ZERO, dist)))
            ev = Decimal(str(p_fill)) * (capture - adverse_bps(side)) - c.maker_fee_bps - inv_cost
            return ev, p_fill

        blocked_map = {BUY: None, SELL: None}

        # ---- one ADD-role quote at ladder slot k ---------------------------------------------------
        def add_target(side: str, k: int, remaining: list, same_dir: bool, beyond: Optional[Decimal],
                       why_not: list) -> Optional[Target]:
            """Price/size/EV-check the add quote for slot k. None (with a reason in why_not) if it
            should not exist. `remaining` = [USD of exposure still allowed], updated when accepted."""
            buy = side == BUY
            if k > 0 and toxic:
                why_not.append("toxic: no extra add levels")
                return None
            if buy and severe_sell_pressure:
                why_not.append("severe selling pressure")
                return None
            if (not buy) and severe_buy_pressure:
                why_not.append("severe buying pressure")
                return None
            if same_dir and inv.block_adds:
                why_not.append(f"inventory {inv.util:.0%} of cap / risk {inv.risk:.2f}: no more adds this way")
                return None
            if k == 0:
                loaded = (pos_usd >= c.order_usd * Decimal("0.5")) if buy else (-pos_usd >= c.order_usd * Decimal("0.5"))
                chasing = chasing_top if buy else chasing_bottom
                if same_dir and loaded:
                    why_not.append("already loaded: touch add suppressed (unwind first)")
                    return None
                if chasing:
                    why_not.append("chasing a move: touch add suppressed")
                    return None
            spacing = Decimal(k) * P.level_spacing_bps * (Decimal(2) if toxic else ONE)
            pen = buy_pen if buy else sell_pen
            imb_extra = bid_imb_extra if buy else ask_imb_extra
            lvl_edge = edge + spacing + pen + imb_extra
            if buy:
                formula = (res - bid_asym) * (ONE - lvl_edge / BPS)
                joined = s.bid + (tb if (c.penny and spread >= 2 * tb) else ZERO)
                if k == 0 and c.aggressive_touch:
                    px = joined
                elif k == 0 and c.penny and spread > 2 * tb and (not toxic) and flow_bias >= Decimal("-0.2"):
                    px = s.bid + tb
                else:
                    px = formula
                if k == 0 and imb_extra > 0:
                    px = min(px, formula)                 # book pressure: the edge price caps a penny/touch quote
                px = q_down(min(px, s.ask - tb), tb)
                if px >= s.ask:
                    px = s.ask - tb
                if beyond is not None and px >= beyond:
                    why_not.append(f"L{k} collapsed onto the level in front")
                    return None
            else:
                formula = (res + ask_asym) * (ONE + lvl_edge / BPS)
                joined = s.ask - (ta if (c.penny and spread >= 2 * ta) else ZERO)
                if k == 0 and c.aggressive_touch:
                    px = joined
                elif k == 0 and c.penny and spread > 2 * ta and (not toxic) and flow_bias <= Decimal("0.2"):
                    px = s.ask - ta
                else:
                    px = formula
                if k == 0 and imb_extra > 0:
                    px = max(px, formula)
                px = q_up(max(px, s.bid + ta), ta)
                if px <= s.bid:
                    px = s.bid + ta
                if beyond is not None and px <= beyond:
                    why_not.append(f"L{k} collapsed onto the level in front")
                    return None
            if px <= 0:
                return None
            scale = (ONE - Decimal("0.5") * abs(q)) * inv.add_scale if same_dir else ONE      # smaller when loaded
            usd = c.order_usd * scale * (bid_imb_mult if buy else ask_imb_mult) * (P.level_size_mult ** k)
            usd = max(usd, m.min_notional)
            qty = q_down(usd / px, m.step)
            if qty * px < m.min_notional:
                qty = q_up(m.min_notional / px, m.step)
            if not sized_ok(qty, px):
                why_not.append("size below minimum")
                return None
            if qty * px > remaining[0]:
                why_not.append("max position")
                return None
            ev, p_fill = ev_of(side, px)
            min_ev = P.min_ev_bps
            if (k, side) in s.live_levels:
                min_ev = max(ZERO, min_ev - c.ev_hysteresis_bps)        # don't flicker a resting level
            aggressive_touch_quote = k == 0 and c.aggressive_touch       # explicit "join the touch" override
            if c.enable_adaptive_ev and not aggressive_touch_quote and ev < min_ev:
                why_not.append(f"EV {ev:.2f}bps < {min_ev:.2f}")
                return None
            remaining[0] -= qty * px
            return Target(px, qty, "add", level=k, ev_bps=ev, p_fill=p_fill)

        # ---- REDUCE-role (exit) quotes --------------------------------------------------------------
        def exit_price(side: str) -> Decimal:
            if side == SELL:                                     # long -> sell
                mp_px = s.avg_cost * (ONE + floor_bps / BPS) if s.avg_cost > 0 else s.ask
                if stress:
                    px = s.ask
                    if c.penny and spread > 2 * ta:
                        px = s.ask - ta
                else:
                    px = max(s.ask, mp_px)
                    if c.penny and spread > 2 * ta and (s.ask - ta) >= mp_px:
                        px = s.ask - ta
                px = q_up(max(px, s.bid + ta), ta)
                return s.bid + ta if px <= s.bid else px
            mp_px = s.avg_cost * (ONE - floor_bps / BPS) if s.avg_cost > 0 else s.bid
            if stress:
                px = s.bid
                if c.penny and spread > 2 * tb:
                    px = s.bid + tb
            else:
                px = min(s.bid, mp_px)
                if c.penny and spread > 2 * tb and (s.bid + tb) <= mp_px:
                    px = s.bid + tb
            px = q_down(min(px, s.ask - tb), tb)
            return s.ask - tb if px >= s.ask else px

        def reduce_ladder(side: str, px0: Decimal, tick: Decimal, first_qty: Decimal) -> list:
            """Scale-out exits beyond the touch exit, each at a better price (never past the position)."""
            out: list = []
            if c.extra_levels <= 0:
                return out
            remaining = abs(s.position) - first_qty
            prev = px0
            sign = 1 if side == BUY else -1
            for i in range(1, c.extra_levels + 1):
                if remaining <= 0:
                    break
                lvl_edge = edge + Decimal(i) * P.level_spacing_bps
                raw = res * (ONE - sign * lvl_edge / BPS)
                px_i = q_down(raw, tick) if side == BUY else q_up(raw, tick)
                if (side == BUY and px_i >= prev) or (side == SELL and px_i <= prev):
                    break
                take = q_down(min(remaining, base * (P.level_size_mult ** i)), m.step)
                if take <= 0 or not sized_ok(take, px_i):
                    break
                remaining -= take
                out.append(Target(px_i, take, "reduce", level=i))
                prev = px_i
            return out

        def continue_adding(side: str, targets: list) -> list:
            """Leftover ladder slots (the reduce ladder often needs only a few) become fresh add quotes,
            position-capped from a flat baseline, subject to the SAME guards/EV as normal adds, never in
            stress. They sit beyond every reduce level, so the exit itself is never repriced or resized."""
            used = max(0, len(targets) - 1)                     # targets[0] is the level-0 exit
            if not targets or not c.continue_add_after_reduce or used >= c.extra_levels or stress:
                return targets
            why, code = blocked(side)
            blocked_map[side] = code
            if why:
                notes.append(f"no continued {'bid' if side == BUY else 'ask'}: {why}")
                return targets
            remaining = [c.max_position_usd]
            beyond, more = targets[-1].price, []
            for k in range(used + 1, c.extra_levels + 1):
                t = add_target(side, k, remaining, False, beyond, [])
                if t is not None:
                    more.append(t)
                    beyond = t.price
            return targets + more

        def add_side(side: str, same_dir: bool) -> list:
            why, code = blocked(side)
            blocked_map[side] = code
            label = "bid" if side == BUY else "ask"
            if why:
                notes.append(f"no {label}: {why}")
                return []
            remaining = [max(ZERO, c.max_position_usd - pos_usd) if side == BUY
                         else max(ZERO, c.max_position_usd + pos_usd)]
            out, beyond = [], None
            for k in range(0, c.extra_levels + 1):
                reasons: list = []
                t = add_target(side, k, remaining, same_dir, beyond, reasons)
                if t is None:
                    if k == 0 and reasons:
                        notes.append(f"no {label} L0: {reasons[-1]}")
                    continue
                out.append(t)
                beyond = t.price
            return out

        def reduce_side(side: str, tick: Decimal) -> list:
            px = exit_price(side)
            qty = reduce_qty()
            if not sized_ok(qty, px):
                return []
            out = [Target(px, qty, "reduce", level=0)]
            out += reduce_ladder(side, px, tick, qty)
            return continue_adding(side, out)

        bid_t = reduce_side(BUY, tb) if bid_role == "reduce" else add_side(BUY, long_)
        ask_t = reduce_side(SELL, ta) if ask_role == "reduce" else add_side(SELL, short_)

        # ---- never let our own bid and ask cross each other (adds give way to exits) ---------------
        def prune(bids: list, asks: list) -> tuple:
            for _ in range(2):
                if bids and asks and max(t.price for t in bids) >= min(t.price for t in asks):
                    hi_bid = max(t.price for t in bids)
                    lo_ask = min(t.price for t in asks)
                    bids = [t for t in bids if t.role == "reduce" or t.price < lo_ask]
                    asks = [t for t in asks if t.role == "reduce" or t.price > hi_bid]
                    notes.append("own quotes would cross: add quote dropped")
            return bids, asks

        bid_t, ask_t = prune(bid_t, ask_t)

        def split(ts: list) -> tuple:
            """(touch, extras): touch is the level-0 target when present; everything else is 'extra'."""
            head = next((t for t in ts if t.level == 0), None)
            return head, [t for t in ts if t is not head]

        bid, extra_bids = split(bid_t)
        ask, extra_asks = split(ask_t)
        return Plan(bid, ask, edge, inv_skew_bps, stress, notes, blocked_map, extra_bids, extra_asks,
                    regime=regime, fair=fair, inv=inv, severe={BUY: raw_sev_sell, SELL: raw_sev_buy})
