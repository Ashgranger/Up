"""Inventory manager: decides HOW HARD to get out of a position, from all the data the bot has.

Pure function (no I/O) so it is unit-testable. It answers three questions each tick:

  1. How dangerous is the position right now?   -> risk score 0..1.5 built from
       flow against us (OBI + trade-flow + multi-level depth imbalance), trend against us,
       inventory size vs cap, toxic fills on the side that built it, and how long we've held
       it while under water.
  2. How much price may the MAKER exit give up?  -> `give_bps`: the exit target slides from
       "cost + profit floor" down to the touch (and a slightly worse-than-cost exit) as risk rises.
       This is the main tool: a passive ALO exit at the touch costs no taker fee.
       `reduce_frac` says what share of the position the touch exit should work, and
       `block_adds`/`add_scale` stop the bot from building more of what it already can't sell.
  3. Is it worth PAYING the taker fee to cut?    -> only when the expected further loss
       (flow + trend + toxicity + vol, in bps) beats the cost of crossing (half spread + taker fee)
       by a margin, the maker exit has had time to work (the bot enforces the wait), or on a hard stop /
       emergency. A cut is an IOC reduce-only LIMIT with a bounded slippage price - never a market order.

Position sign convention: dir = +1 long (adverse = falling), -1 short (adverse = rising).
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from utils import BPS, ONE, ZERO, clamp

NORMAL, CAUTION, PRESSURE, STRESS = "NORMAL", "CAUTION", "PRESSURE", "STRESS"
HOLD, REDUCE, CUT = "HOLD", "REDUCE", "CUT"


@dataclass
class InvDecision:
    state: str = NORMAL               # NORMAL | CAUTION | PRESSURE | STRESS
    action: str = HOLD                # HOLD | REDUCE (maker) | CUT (taker)
    risk: Decimal = ZERO
    util: Decimal = ZERO              # |position| / MAX_POSITION_USD, 0..1
    flow_against: Decimal = ZERO      # -1..1, + = flow pushing price against the position
    exp_loss_bps: Decimal = ZERO      # expected further adverse move (bps)
    pnl_bps: Decimal = ZERO           # mark-to-exit-side PnL of the position (bps of cost)
    cross_cost_bps: Decimal = ZERO    # half spread + taker fee: what a cut costs
    give_bps: Decimal = ZERO          # how far below the profit floor the maker exit may go
    reduce_frac: Decimal = ZERO       # share of the position the touch exit works (0 = base size)
    block_adds: bool = False          # no more exposure in the position's direction
    add_scale: Decimal = ONE          # size multiplier for same-direction adds
    taker: bool = False               # a taker cut is warranted (bot still applies wait/cooldown/caps)
    taker_frac: Decimal = ZERO        # share of the position to cut
    emergency: bool = False           # skip the maker-wait before cutting
    reason: str = ""


def assess(c, P, *, position: Decimal, avg_cost: Decimal, bid: Decimal, ask: Decimal, mid: Decimal,
           hold_s: float, ret_bps: Decimal, vol_bps: Decimal, obi: Decimal, tfi: Decimal, imb: Decimal,
           side_tox_bps: Decimal, flat: bool, halted: bool = False) -> InvDecision:
    d = InvDecision()
    if flat or not c.enable_inventory_mgr or mid <= 0:
        return d
    direction = ONE if position > 0 else -ONE
    pos_usd = abs(position * mid)
    d.util = clamp(pos_usd / c.max_position_usd, ZERO, ONE) if c.max_position_usd > 0 else ZERO

    # ---- direction of the pressure, in the position's frame ------------------------------
    raw = Decimal("0.4") * obi + Decimal("0.35") * tfi + Decimal("0.25") * imb
    d.flow_against = clamp(-direction * raw, -ONE, ONE)
    trend_against = max(ZERO, -direction * ret_bps)
    mark = bid if position > 0 else ask                         # what we could actually get right now
    d.pnl_bps = ((mark - avg_cost) / avg_cost * BPS * direction) if avg_cost > 0 else ZERO
    spread_bps = (ask - bid) / mid * BPS if mid else ZERO

    # ---- expected further loss if we do nothing (bps) ------------------------------------
    d.exp_loss_bps = (max(ZERO, d.flow_against) * c.inv_flow_bps + trend_against * Decimal("0.5")
                      + side_tox_bps + vol_bps * c.inv_vol_k)
    d.cross_cost_bps = spread_bps / 2 + c.taker_fee_bps

    # ---- risk score ------------------------------------------------------------------------
    trend_norm = min(ONE, trend_against / P.trend_pull_bps) if P.trend_pull_bps > 0 else ZERO
    tox_norm = min(ONE, side_tox_bps / c.regime_toxic_threshold_bps) if c.regime_toxic_threshold_bps > 0 else ZERO
    stale = min(ONE, Decimal(str(hold_s)) / Decimal(str(max(P.max_hold_s, 1.0)))) if d.pnl_bps < 0 else ZERO
    d.risk = (Decimal("0.45") * max(ZERO, d.flow_against) + Decimal("0.25") * trend_norm
              + Decimal("0.30") * d.util + Decimal("0.15") * stale + Decimal("0.20") * tox_norm)
    if d.pnl_bps <= -P.stress_loss_bps or halted:
        d.risk = max(d.risk, Decimal("0.8"))

    if d.risk >= Decimal("0.75"):
        d.state = STRESS
    elif d.risk >= Decimal("0.5"):
        d.state = PRESSURE
    elif d.risk >= Decimal("0.25"):
        d.state = CAUTION

    # ---- maker tools ---------------------------------------------------------------------------
    if d.state != NORMAL:
        d.action = REDUCE
        d.give_bps = clamp((d.risk - Decimal("0.25")) / Decimal("0.75"), ZERO, ONE) * c.inv_max_give_bps
    d.reduce_frac = {NORMAL: ZERO, CAUTION: ZERO, PRESSURE: Decimal("0.5"), STRESS: ONE}[d.state]
    d.block_adds = d.util >= c.inv_add_block_util or d.risk >= Decimal("0.5")
    d.add_scale = clamp(ONE - Decimal("0.5") * d.risk, Decimal("0.25"), ONE)

    # ---- taker cut: only when the expected loss beats the cost of crossing --------------------
    if not c.enable_taker_exit:
        d.reason = "taker exit disabled"
        return d
    saving = d.exp_loss_bps - d.cross_cost_bps
    if d.pnl_bps <= -c.inv_hard_stop_bps:
        d.taker, d.taker_frac, d.emergency = True, ONE, True
        d.reason = f"hard stop: {d.pnl_bps:.1f}bps underwater"
    elif d.util >= c.inv_emergency_util and d.flow_against >= Decimal("0.3"):
        d.taker, d.taker_frac, d.emergency = True, ONE, True
        d.reason = f"inventory {d.util:.0%} of cap with flow against"
    elif (d.risk >= c.inv_cut_risk and d.flow_against >= c.inv_cut_flow and saving >= c.inv_cut_min_saving_bps
          and d.pnl_bps < c.inv_cut_max_pnl_bps):
        d.taker, d.taker_frac = True, Decimal("0.5") if d.util < Decimal("0.8") else ONE
        d.reason = f"expected further loss {d.exp_loss_bps:.1f}bps > cross cost {d.cross_cost_bps:.1f}bps"
    elif (hold_s > P.max_hold_s * 1.5 and d.pnl_bps <= -c.inv_cut_min_loss_bps
          and d.flow_against >= Decimal("0.2") and saving > 0):
        d.taker, d.taker_frac = True, Decimal("0.5")
        d.reason = f"stale losing inventory ({hold_s:.0f}s, {d.pnl_bps:.1f}bps)"
    if d.taker:
        d.action = CUT
    return d
