"""Offline tests: `python tests/test_bot.py` (no network, no pytest needed)."""
import asyncio, random, re, sys, os
from decimal import Decimal as D

sys.path.insert(0, os.path.dirname(__file__))
from sim import *          # noqa: E402  (also sets sys.path / stubs)
from strategy import Snapshot, Strategy   # noqa: E402
from ledger import Ledger                 # noqa: E402
from market import MarketData             # noqa: E402
from signer import Signer                 # noqa: E402
from utils import Fatal, canonical        # noqa: E402
from cryptography.hazmat.primitives.asymmetric import ed25519  # noqa: E402

FAILS = []
def check(name, cond, extra=""):
    print(("  ok   " if cond else "  FAIL ") + name + (f"  [{extra}]" if extra and not cond else ""))
    if not cond: FAILS.append(name)

def snap(cfg, **kw):
    base = dict(now=100.0, market=MKT, bid=D("81349.0"), ask=D("81349.1"), mid=D("81349.05"), micro=D("81349.05"),
                position=D(0), avg_cost=D(0), hold_s=0.0, ret_bps=D(0), move_bps=D(0), vol_bps=D(0), tox_bps=D(0))
    base.update(kw); return Snapshot(**base)

# --------------------------------------------------------------------------------------------- #
print("1. signing (unchanged, proven live in the logs)")
s = Signer("11" * 32, ADDR, 0)
req = s.place(MKT, "BUY", D("100000.1"), D("0.0005"), 4102444800000000)
pub = ed25519.Ed25519PublicKey.from_public_bytes(bytes.fromhex(req["apiKey"]))
signed = s._typed(1, int(req["timestamp"]), 1, g=4102444800000000 * 1000, p=1000001, q=50000, r=0, s=0, t=3)
try: pub.verify(bytes.fromhex(req["signature"]), signed.encode()); ok = True
except Exception: ok = False
check("place signature verifies", ok)
leg = s.legacy("cancelAllOrders", {"address": ADDR, "accountIndex": 0, "marketId": 1})
try: pub.verify(bytes.fromhex(leg["signature"]), (leg["timestamp"] + "cancelAllOrders" + canonical(leg["payload"])).encode()); ok = True
except Exception: ok = False
check("legacy signature verifies", ok)

# --------------------------------------------------------------------------------------------- #
print("2. strategy: the v1 failure (long $60, mid 81349) can no longer produce a crossing quote")
cfg = mkcfg(MAX_POSITION_USD=100)
st = Strategy(cfg)
pos = D("0.0007368")
p = st.plan(snap(cfg, position=pos, avg_cost=D("81368.6"), hold_s=5))
check("ask never <= best bid", p.ask is not None and p.ask.price > D("81349.0"), str(p.ask))
check("bid never >= best ask", p.bid is None or p.bid.price < D("81349.1"))
check("exit floor respected (>= cost + 0.5bp) when not stressed", p.stress or p.ask.price >= D("81368.6") * D("1.00005") - D("0.1"))
p = st.plan(snap(cfg, bid=D("81250.0"), ask=D("81250.1"), mid=D("81250.05"), micro=D("81250.05"), position=pos, avg_cost=D("81368.6"), hold_s=20))
check("underwater 14bps -> stress: exit at touch, no adding", p.stress and p.ask.price <= D("81250.1") and p.bid is None, str((p.bid, p.ask, p.notes)))
check("stress exit sells whole position", p.ask.qty >= D("0.0007"))
p = st.plan(snap(cfg, position=D("0.00037"), avg_cost=D("81349.0"), hold_s=cfg.max_hold_s + 1))
p_old = st.plan(snap(cfg, position=D("0.00037"), avg_cost=D("81349.0"), hold_s=5))
p_mid = st.plan(snap(cfg, position=D("0.00037"), avg_cost=D("81349.0"), hold_s=cfg.max_hold_s * 0.6))
p_late = st.plan(snap(cfg, position=D("0.00037"), avg_cost=D("81349.0"), hold_s=cfg.max_hold_s * 1.6))
check("exit floor decays with hold time (fresh > 0.5x max_hold > 1.5x max_hold)",
      (not p_late.stress) and p_old.ask.price > p_mid.ask.price >= p_late.ask.price, str((p_old.ask, p_mid.ask, p_late.ask)))
check("...and never below cost (breakeven+0.2bps)", p_late.ask.price >= D("81349.0") * D("1.00002") - D("0.1"), str(p_late.ask))

print("3. strategy: guards only ever pull the ADDING side")
p = st.plan(snap(cfg, ret_bps=D("-4")))
check("falling fast: no bid, ask still quoted", p.bid is None and p.ask is not None, str(p.notes))
p = st.plan(snap(cfg, ret_bps=D("4")))
check("rising fast: no ask, bid still quoted", p.ask is None and p.bid is not None)
p = st.plan(snap(cfg, position=D("0.00037"), avg_cost=D("81349.0"), ret_bps=D("-4")))
check("long + falling: bid pulled, reducing ask kept", p.bid is None and p.ask is not None and p.ask.role == "reduce")
p = st.plan(snap(cfg, position=D("-0.00037"), avg_cost=D("81349.0"), ret_bps=D("-4")))
check("short + falling: reducing bid kept (price moving our way)", p.bid is not None and p.bid.role == "reduce")
p = st.plan(snap(cfg, move_bps=D("9")))
check("vol pause pulls adding sides", p.bid is None and p.ask is None)
p = st.plan(snap(cfg, cooldown_until={"BUY": 200.0, "SELL": 0.0}))
check("burst cooldown pulls only that side", p.bid is None and p.ask is not None)
p = st.plan(snap(cfg, jump_active=True))
check("price jump pulls both adding sides", p.bid is None and p.ask is None)
p = st.plan(snap(cfg, position=D("0.00074")))       # $60 == MAX_POSITION default
cfg60 = mkcfg()
p = Strategy(cfg60).plan(snap(cfg60, position=D("0.00074")))
check("at max position: no more buying", p.bid is None and p.ask is not None)
p0 = st.plan(snap(cfg)); p1 = st.plan(snap(cfg, tox_bps=D("3")))
check("toxic fills widen the edge", p1.edge_bps > p0.edge_bps and p1.bid.price < p0.bid.price)
p = st.plan(snap(cfg, position=D("0.00002"), avg_cost=D("81000")))   # $1.6 dust
check("dust position is treated as flat", p.bid is not None and p.ask is not None and p.bid.role == "add")
cfg_noev = mkcfg(MAX_POSITION_USD=100, ENABLE_ADAPTIVE_EV="0")
wide = Strategy(cfg_noev).plan(snap(cfg_noev, bid=D("100.0"), ask=D("100.5"), mid=D("100.25"), micro=D("100.25")))
check("wide spread: penny inside the touch (EV filter off)", wide.bid.price == D("100.1") and wide.ask.price == D("100.4"), str((wide.bid, wide.ask)))
wide_ev = st.plan(snap(cfg, bid=D("100.0"), ask=D("100.5"), mid=D("100.25"), micro=D("100.25")))
check("wide spread: EV filter drops quotes that are too far from the far touch to ever fill", wide_ev.bid is None and wide_ev.ask is None, str((wide_ev.bid, wide_ev.ask, wide_ev.notes)))
try: Strategy(mkcfg(ORDER_USD=1)).plan(snap(cfg)); ok = False
except Fatal: ok = True
check("ORDER_USD below venue minimum -> Fatal", ok)

print("3b. reduce-ladder: scale-out exits, and aggressive-touch mode")
cfgL = mkcfg(EXTRA_LEVELS=2, LEVEL_SPACING_BPS="4", LEVEL_SIZE_MULT="0.6", MAX_POSITION_USD="200", ORDER_USD="20")
stL = Strategy(cfgL)
p = stL.plan(snap(cfgL, position=D("0.0025"), avg_cost=D("81000")))   # long, well inside max position
check("reduce ladder: touch + extra levels all on the ask (reduce) side", p.ask is not None and len(p.extra_asks) == 2, str((p.ask, p.extra_asks)))
check("reduce ladder: every level is 'reduce', never 'add'", p.ask.role == "reduce" and all(t.role == "reduce" for t in p.extra_asks))
prices = [p.ask.price] + [t.price for t in p.extra_asks]
check("reduce ladder: each level is a better (higher) exit than the last", all(prices[i] < prices[i+1] for i in range(2)), str(prices))
check("reduce ladder: no bid ladder while long (bid side is guard/role 'add', not reduce)", not p.extra_bids)
total_reduce = p.ask.qty + sum(t.qty for t in p.extra_asks)
check("reduce ladder: never offers to sell more than the position held", total_reduce <= D("0.0025") + D("1e-9"), str(total_reduce))
p_stress = stL.plan(snap(cfgL, bid=D("81250.0"), ask=D("81250.1"), mid=D("81250.05"), micro=D("81250.05"),
                          position=D("0.0025"), avg_cost=D("81368.6")))  # far underwater -> stress
check("in stress, level 0 alone takes the whole position -> nothing left to ladder", p_stress.stress and not p_stress.extra_asks, str((p_stress.stress, p_stress.extra_asks)))

cfgA = mkcfg(AGGRESSIVE_TOUCH="1", MIN_EDGE_BPS="20")   # edge would normally sit far behind the touch
stA = Strategy(cfgA)
p = stA.plan(snap(cfgA))
check("aggressive_touch: bid joins the actual best bid despite a wide min_edge", p.bid.price == D("81349.0"), str(p.bid))
check("aggressive_touch: ask joins the actual best ask despite a wide min_edge", p.ask.price == D("81349.1"), str(p.ask))
p = stA.plan(snap(cfgA, position=D("0.0025"), avg_cost=D("81348.0")))
check("aggressive_touch never overrides the exit-profit floor on a reduce order",
      p.ask.price >= D("81348.0") * (1 + cfgA.exit_min_profit_bps / D(10000)) - D("0.001"), str(p.ask))

print("3d. book-pressure (depth imbalance): leading widen/shrink of the endangered ADD side")
from market import MarketData
mdI = MarketData(mkcfg())
check("empty book -> no imbalance reading (never guess)", mdI.depth_imbalance(5, 100.0) is None)
mdI.on_book({"bids": [{"price": "100.0", "size": "9"}, {"price": "99.9", "size": "9"}],
             "asks": [{"price": "100.1", "size": "1"}, {"price": "100.2", "size": "1"}]}, True, 100.0)
imb_up = mdI.depth_imbalance(5, 100.0)
check("bid-heavy book -> strongly positive (buying pressure)", imb_up is not None and imb_up == D("0.8"), str(imb_up))
mdI.on_book({"bids": [{"price": "100.0", "size": "1"}, {"price": "99.9", "size": "1"}],
             "asks": [{"price": "100.1", "size": "9"}, {"price": "100.2", "size": "9"}]}, True, 100.0)
check("ask-heavy book -> strongly negative (selling pressure)", mdI.depth_imbalance(5, 100.0) == D("-0.8"))
check("stale depth book -> None, not a frozen-book guess", mdI.depth_imbalance(5, 100.0 + mkcfg().stale_s + 1) is None)

cfgP = mkcfg(MIN_EDGE_BPS="2", IMBALANCE_WIDEN_BPS="3", IMBALANCE_SIZE_CUT="0.3", EXTRA_LEVELS=0)
stP = Strategy(cfgP)
p0 = stP.plan(snap(cfgP))
pu = stP.plan(snap(cfgP, imbalance=D("1")))       # full buying pressure -> ASK endangered
pd = stP.plan(snap(cfgP, imbalance=D("-1")))      # full selling pressure -> BID endangered
check("buying pressure: ask is pushed further out than neutral", pu.ask.price > p0.ask.price, str((pu.ask.price, p0.ask.price)))
check("buying pressure: the bid is NOT widened (buying into a rally is fine)", pu.bid.price == p0.bid.price, str((pu.bid.price, p0.bid.price)))
check("buying pressure: ask size shrinks, bid size unchanged", pu.ask.qty < p0.ask.qty and pu.bid.qty == p0.bid.qty, str((pu.ask.qty, p0.ask.qty)))
check("selling pressure: bid is pushed further out, ask untouched", pd.bid.price < p0.bid.price and pd.ask.price == p0.ask.price)
check("selling pressure: bid size shrinks by ~IMBALANCE_SIZE_CUT", pd.bid.qty <= p0.bid.qty * D("0.71") and pd.bid.qty >= p0.bid.qty * D("0.69"), str((pd.bid.qty, p0.bid.qty)))
cfgN = mkcfg(MIN_EDGE_BPS="2", USE_DEPTH_IMBALANCE="0", EXTRA_LEVELS=0)
pn = Strategy(cfgN).plan(snap(cfgN, imbalance=D("1")))
check("USE_DEPTH_IMBALANCE=0 -> signal ignored entirely", pn.ask.price == p0.ask.price and pn.ask.qty == p0.ask.qty)
# the exit side is never widened/shrunk by book pressure (same invariant as every other guard)
pl = stP.plan(snap(cfgP, position=D("0.0002"), avg_cost=D("81000"), imbalance=D("1")))
pl0 = stP.plan(snap(cfgP, position=D("0.0002"), avg_cost=D("81000")))
check("holding a long under buying pressure: the reduce (exit) order is untouched", pl.ask == pl0.ask, str((pl.ask, pl0.ask)))

print("3c. continue adding after a small reduce fill (don't let a bad fill idle the whole ladder)")
cfgC = mkcfg(EXTRA_LEVELS=3, LEVEL_SPACING_BPS="4", LEVEL_SIZE_MULT="0.6", MAX_POSITION_USD="200", ORDER_USD="20")
stC = Strategy(cfgC)
# a SMALL long (from one bad fill) needs only level 0 to fully exit -> 2 of the 3 ladder slots are free
snapshot = snap(cfgC, position=D("0.0001"), avg_cost=D("81000"))
p = stC.plan(snapshot)
check("small position: level 0 reduce order alone would cover it", p.ask is not None and p.ask.qty >= snapshot.position - D("1e-9"), str(p.ask))
add_levels = [t for t in p.extra_asks if t.role == "add"]
check("leftover ladder budget is used for fresh 'add' quotes, not left idle", len(add_levels) > 0, str(p.extra_asks))
check("every fresh add level sits further out than the reduce (exit) quote", all(t.price > p.ask.price for t in add_levels), str((p.ask.price, [t.price for t in add_levels])))
check("total extra levels never exceed EXTRA_LEVELS budget", len(p.extra_asks) <= cfgC.extra_levels, str(p.extra_asks))
worst_short = -snapshot.position * snapshot.mid + sum(t.qty * t.price for t in add_levels)
check("fresh add levels alone stay within MAX_POSITION_USD from a flat baseline", worst_short <= cfgC.max_position_usd + D("1"), str(worst_short))
# isolation: the reduce order/levels must be BYTE-IDENTICAL whether or not continuation is enabled
cfgOff = mkcfg(EXTRA_LEVELS=3, LEVEL_SPACING_BPS="4", LEVEL_SIZE_MULT="0.6", MAX_POSITION_USD="200", ORDER_USD="20", CONTINUE_ADD_AFTER_REDUCE="0")
p_off = Strategy(cfgOff).plan(snapshot)
reduce_only = [t for t in p.extra_asks if t.role == "reduce"]
check("continuation ON/OFF never changes the reduce (exit) order itself", p.ask == p_off.ask, str((p.ask, p_off.ask)))
check("continuation ON/OFF never changes the reduce ladder levels themselves", reduce_only == p_off.extra_asks, str((reduce_only, p_off.extra_asks)))
check("with continuation off, the leftover ladder budget is simply unused", not p_off.extra_asks, str(p_off.extra_asks))
# stress overrides continuation entirely - only the exit should be working
p_stress2 = stC.plan(snap(cfgC, bid=D("81250.0"), ask=D("81250.1"), mid=D("81250.05"), micro=D("81250.05"),
                          position=D("0.0001"), avg_cost=D("81368.6")))
check("stress: no continuation even though the (small) reduce ladder had room", p_stress2.stress and not p_stress2.extra_asks, str(p_stress2.extra_asks))
# a guard that would block fresh SELL adds also blocks the continuation, but never the reduce order
cfgG = mkcfg(EXTRA_LEVELS=3, LEVEL_SPACING_BPS="4", LEVEL_SIZE_MULT="0.6", MAX_POSITION_USD="200", ORDER_USD="20", TREND_PULL_BPS="0.01")
p_g = Strategy(cfgG).plan(snap(cfgG, position=D("0.0001"), avg_cost=D("81000"), ret_bps=D("5")))  # "rising" -> blocks fresh SELL adds
check("a guard that would block fresh sell-adds also blocks the continuation", not any(t.role == "add" for t in p_g.extra_asks), str(p_g.extra_asks))
check("...but the reduce order itself is still live (guards never touch the exit)", p_g.ask is not None and p_g.ask.role == "reduce", str(p_g.ask))

# --------------------------------------------------------------------------------------------- #
print("4. ledger: reproduce the uploaded log-2 numbers")
FILLS = """BUY 0.00036852 81421.5
SELL 0.00036852 81418.4""".splitlines()
log2 = [("SELL",.00036852,81418.4),("BUY",.00036852,81421.5),("SELL",.00036844,81446),("BUY",.00036843,81450.2),
        ("BUY",.00036831,81439.6),("SELL",.00036834,81436.5),("SELL",.00036838,81448.7),("BUY",.00036839,81452),
        ("BUY",.00036832,81437.2),("SELL",.00036832,81435.1),("SELL",.00036838,81448.9),("BUY",.0003684,81473.5),
        ("SELL",.00036832,81458.3),("BUY",.00036826,81479.3),("BUY",.00036834,81433.8),("SELL",.00036825,81430.5),
        ("SELL",.00036839,81446.1),("BUY",.00036842,81449.5),("BUY",.00036834,81432.5),("BUY",.00036841,81354.1),
        ("BUY",.00036877,81319.2)]
lg = Ledger(mkcfg()); t = 0
for side, q, px in log2:
    t += 10; lg.on_fill(side, D(str(q)), D(str(px)), D(str(px)), t, D(5))
check("position ~0.001106 BTC long", abs(lg.position - D("0.00110557")) < D("0.0000002"), str(lg.position))
check("realized ~ -$0.025 (matches pnl.py on the log)", abs(lg.realized - D("-0.0251")) < D("0.001"), str(lg.realized))
check("unrealized at 81250.75 ~ -$0.13", abs(lg.unrealized(D("81250.75")) - D("-0.1303")) < D("0.003"), str(lg.unrealized(D("81250.75"))))
lg2 = Ledger(mkcfg()); lg2.on_fill("BUY", D("0.001"), D("100"), D("100.5"), 1, D(5)); lg2.on_fill("SELL", D("0.001"), D("101"), D("100.5"), 2, D(5))
check("spread capture = qty*(mid-buy) + qty*(sell-mid)", lg2.spread_capture == D("0.001") and lg2.realized == D("0.001"), str((lg2.spread_capture, lg2.realized)))
lg3 = Ledger(mkcfg()); lg3.on_fill("SELL", D("0.001"), D("100"), D("100"), 1, D(5)); lg3.on_fill("BUY", D("0.002"), D("99"), D("99"), 2, D(5))
check("short->long flip books the closed part only", lg3.position == D("0.001") and lg3.avg_cost == D("99") and lg3.realized == D("0.001"), str((lg3.position, lg3.avg_cost, lg3.realized)))
lg4 = Ledger(mkcfg()); lg4.on_fill("BUY", D("0.001"), D("100000"), D("100000"), 0, D(5))
for i in range(3): lg4.on_fill("BUY", D("0.0001"), D("100000"), D("100000"), 0, D(5))
lg4.process_markouts(D("99990"), 100)
check("markouts measure adverse move after fills (-1bp)", len(lg4.markouts) == 4 and abs(lg4.avg_markout_bps + 1) < D("0.01") and lg4.tox_bps > 0)
lg5 = Ledger(mkcfg()); lg5.on_fill("SELL", D("0.0004"), D("81000"), D("81000"), 100, D(5))
r1 = lg5.reconcile(D(0), 101, D("81000"), D(5)); r2 = lg5.reconcile(D(0), 110, D("81000"), D(5)); r3 = lg5.reconcile(D(0), 116, D("81000"), D(5))
check("stale 'flat' read right after a fill does NOT flip the ledger (v1 flapping)", not r1 and not r2 and r3 and lg5.position == 0)

# --------------------------------------------------------------------------------------------- #
# l2OrderbookUpdates depth book (market.py) - field names for this channel aren't nailed down
# from docs alone (see market.py's _levels docstring), so these lock in the parsing/apply/resync
# contract regardless of exactly which shape the live feed turns out to use.
md_cfg = mkcfg()
md = MarketData(md_cfg)
resync = md.on_book({"bids": [{"price": "100.0", "size": "2"}, {"price": "99.9", "size": "3"}],
                     "asks": [{"price": "100.1", "size": "1"}, {"price": "100.2", "size": "6"}],
                     "sequence": 10}, True, 1000.0)
check("snapshot applied, no resync needed", not resync and md.bids[D("100.0")] == D("2") and md.asks[D("100.1")] == D("1"))
check("depth_micro leans toward the thin side (bigger ask size -> pulls micro down)",
      md.depth_micro(2, 1000.0) < D("100.05"), str(md.depth_micro(2, 1000.0)))
resync = md.on_book({"bids": [{"price": "100.0", "size": "0"}], "asks": [], "sequence": 11}, False, 1001.0)
check("incremental size=0 removes the level", not resync and D("100.0") not in md.bids)
resync = md.on_book({"bids": [{"price": "99.8", "size": "5"}], "asks": [], "sequence": 5}, False, 1002.0)
check("sequence going backwards signals resync, and does NOT corrupt the book", resync and D("99.8") not in md.bids)
check("depth_micro returns None once stale", md.depth_micro(2, 1002.0 + md_cfg.stale_s + 1) is None)
md2 = MarketData(md_cfg)
check("depth_micro is None with an empty book (falls back to bbo-size weighting in .micro)", md2.depth_micro(2, 1000.0) is None)

# --------------------------------------------------------------------------------------------- #
async def scenario_falling_knife():
    print("5. end-to-end: the log-2 disaster (long $60, market falls 14bps) against the simulator")
    bot, sim, clk = make(SESSION_MAX_LOSS_USD=100, EXTRA_LEVELS=0, ENABLE_TAKER_EXIT=0)  # isolates the limit-only exit path (taker cut: test_inventory.py)
    bot.ledger.position, bot.ledger.avg_cost, bot.ledger.opened_ts = D("0.0007368"), D("81368.6"), clk.t
    sim.position = D("0.0007368")
    px = D("81349.0"); adds_after_fall = 0
    for i in range(400):                                 # 100 virtual seconds
        px -= D("0.28")
        await step(bot, sim, clk, px, px + D("0.1"))
        if i == 399: pass
    buys = [o for o in sim.orders.values() if o["side"] == "BUY"]
    check("zero POST_ONLY_WOULD_CROSS rejects (v1 had 122)", sim.rejects == 0, str(sim.rejects))
    check("bot did not keep buying into the fall", sim.position <= D("0.0007368") + D("1e-12"), str(sim.position))
    check("no resting bid while falling/stressed", not buys, str(buys))
    asks = [o for o in sim.orders.values() if o["side"] == "SELL"]
    check("exit ask re-pegged to the touch (limit only)", len(asks) == 1 and asks[0]["price"] <= px * D("1.00005"), str((asks, px)))
    check("never more than one order per side", sim.max_open_per_side <= 1)
    # market bounces -> exit fills
    for i in range(40):
        px += D("0.6"); await step(bot, sim, clk, px, px + D("0.1"))
    check("exit filled on the bounce; flat", abs(sim.position) < D("0.00001"), str(sim.position))

async def scenario_halt():
    print("6. end-to-end: SESSION_MAX_LOSS halts, exits with limit orders only, then stops")
    bot, sim, clk = make(SESSION_MAX_LOSS_USD="0.05")
    bot.ledger.position, bot.ledger.avg_cost, bot.ledger.opened_ts = D("0.0007368"), D("81368.6"), clk.t
    sim.position = D("0.0007368")
    px = D("81349.0")
    for i in range(400):
        px -= D("0.28"); await step(bot, sim, clk, px, px + D("0.1"))
    check("halted", bot.halted)
    check("only SELL (reduce) orders while halted", all(o["side"] == "SELL" for o in sim.orders.values()))
    for i in range(60):
        px += D("0.8"); await step(bot, sim, clk, px, px + D("0.1"))
        if bot.stop_evt.is_set(): break
    check("stopped once flat", bot.stop_evt.is_set() and abs(sim.position) < D("0.00001"), f"{bot.stop_evt.is_set()} {sim.position}")

async def scenario_random_walk(seed, steps=6000, sigma=0.35):
    rnd = random.Random(seed)
    bot, sim, clk = make(SESSION_MAX_LOSS_USD=100, MIN_EDGE_BPS="1.0", EXTRA_LEVELS=0)  # isolates level-0 accounting from the ladder (see scenario_ladder)
    px = D("81300.0"); worst = D(0)
    for i in range(steps):
        move = rnd.gauss(0, sigma)
        if rnd.random() < 0.02: move += rnd.choice((-1, 1)) * rnd.uniform(8, 30)   # sweeps / news jumps
        px = (px + D(str(round(move, 1)))).quantize(D("0.1"))
        await step(bot, sim, clk, px, px + D("0.1"))
        worst = max(worst, abs(sim.position * px))
    mid = px + D("0.05")
    return bot, sim, worst, mid

async def scenario_invariants():
    print("7. end-to-end: 25 min random walk x3 seeds - accounting, limits, no order leaks")
    for seed in (1, 2, 3):
        bot, sim, worst, mid = await scenario_random_walk(seed)
        led = bot.ledger.total_pnl(mid); simp = sim.pnl(mid)
        check(f"seed {seed}: ledger PnL == simulator PnL ({led:+.5f} vs {simp:+.5f}) on {bot.ledger.n_fills} fills", abs(led - simp) < D("0.0000001"), f"{led} {simp}")
        check(f"seed {seed}: position == exchange position", abs(bot.ledger.position - sim.position) < D("0.00000001"))
        check(f"seed {seed}: max |position| ${worst:.0f} <= cap + 1 lot", worst <= D("60") + D("31"), str(worst))
        check(f"seed {seed}: rejects {sim.rejects} <= 3", sim.rejects <= 3, str(sim.rejects))
        check(f"seed {seed}: one order per side", sim.max_open_per_side <= 1)
        check(f"seed {seed}: bot's open orders == exchange's", {o.order_id for o in bot.om.orders.values()} == set(sim.orders), f"{set(bot.om.orders)} {set(sim.orders)}")
        check(f"seed {seed}: spread capture tracked", bot.ledger.n_fills == 0 or bot.ledger.spread_capture != 0)

async def scenario_reject_backoff():
    print("8. post-only reject -> back-off, no retry storm")
    bot, sim, clk = make()
    await step(bot, sim, clk, D("81000.0"), D("81000.1"))
    sim.bid, sim.ask = D("81005.0"), D("81005.1")          # book jumped but bot hasn't seen the frame yet
    o = await bot.om.place("SELL", D("81002.0"), D("0.0004"), clk.t)
    for _ in range(3): await asyncio.sleep(0)
    check("crossed order rejected by exchange and removed", sim.rejects == 1 and not any(x.price == D("81002.0") for x in bot.om.orders.values()))
    check("back-off armed for that side", bot.om.reject_until["SELL"] > clk.t)
    n = sim.rejects
    for i in range(8):
        await step(bot, sim, clk, D("81005.0"), D("81005.1"), dt=0.05)
    check("no retry storm inside the back-off window", sim.rejects == n, str(sim.rejects - n))

async def scenario_paper():
    print("9. paper mode: sends nothing, still accounts fills")
    bot, sim, clk = make(DRY_RUN="1")
    class Boom:
        async def send(self, raw): raise SystemExit("paper mode sent something: " + raw[:80])
    bot.ex.ws = Boom()
    px = D("81300.0")
    for i in range(400):
        px = px + (D("60") if (i % 30) == 0 else D("-0.83")) * (1 if (i // 30) % 2 == 0 else -1)
        bot.on_bbo({"bestBid": {"price": fmt(px), "size": "1"}, "bestAsk": {"price": fmt(px + D("0.1")), "size": "1"}}, clk.t)
        clk.t += 0.25; bot.md.info_ts = clk.t
        await bot.tick()
    check("paper fills recorded", bot.ledger.n_fills > 0, str(bot.ledger.n_fills))
    check("paper positions bounded", abs(bot.ledger.position * px) <= D("60") + D("31"))

async def scenario_burst_guard():
    print("10. adverse-fill burst guard + sweep guard (L7: counted per side inside a time window)")
    bot, sim, clk = make(MAX_POSITION_USD=200, EXTRA_LEVELS=0)
    await step(bot, sim, clk, D("81000.0"), D("81000.1"))
    for k in range(3):                                   # three BUY fills, 3s apart (no sweep), inside 15s
        clk.t += 3.0
        bot.on_fill("BUY", D("0.0003"), D("81000.0"), None)
    check("3 same-side fills inside BURST_WINDOW_S -> that side paused", bot.cooldown["BUY"] > clk.t, str(bot.cooldown))
    check("...only that side", bot.cooldown["SELL"] <= clk.t)
    bot2, sim2, clk2 = make(MAX_POSITION_USD=200, EXTRA_LEVELS=0)
    await step(bot2, sim2, clk2, D("81000.0"), D("81000.1"))
    bot2.on_fill("SELL", D("0.0003"), D("81000.1"), None); clk2.t += 0.4
    bot2.on_fill("SELL", D("0.0003"), D("81000.1"), None)
    check("2 same-side fills inside SWEEP_GUARD_WINDOW_S -> sweep guard pauses that side", bot2.cooldown["SELL"] > clk2.t, str(bot2.cooldown))
    check("...and the other side keeps quoting", bot2.cooldown["BUY"] <= clk2.t)

async def scenario_wide_spread():
    print("11. wide-spread market (6bps, tick 0.01), balanced flow: does it actually capture spread?")
    from market import Market
    mk = Market(1, "XYZ-USD", "ONLINE", D("0.01"), D("0.001"), [], D("5"), D("0.001"), D("10000"), D("0"), False)
    res = []
    for seed in (11, 12, 13):
        rnd = random.Random(seed)
        bot, sim, clk = make(SESSION_MAX_LOSS_USD=100, MIN_EDGE_BPS="1.0", ORDER_USD=30, MAX_POSITION_USD=60)
        bot.md.info = mk; bot.cfg.market = "XYZ-USD"
        fair = D("100.00")
        for i in range(8000):
            fair = (fair + D(str(round(rnd.gauss(0, 0.004), 3)))).quantize(D("0.001"))
            bid = (fair - D("0.03")).quantize(D("0.01")); ask = (fair + D("0.03")).quantize(D("0.01"))
            await step(bot, sim, clk, bid, ask, dt=0.25, tick=False)
            if rnd.random() < 0.15: sim.taker("BUY" if rnd.random() < 0.5 else "SELL"); await asyncio.sleep(0); await asyncio.sleep(0)
            await bot.tick(); await asyncio.sleep(0)
        mid = (sim.bid + sim.ask) / 2
        led = bot.ledger
        res.append((led.n_fills, led.spread_capture, led.total_pnl(mid), led.avg_edge_bps, bot.om.n_reject))
        check(f"seed {seed}: ledger == simulator ({led.total_pnl(mid):+.4f})", abs(led.total_pnl(mid) - sim.pnl(mid)) < D("0.000001"))
    print("   (fills, spread_capture$, total_pnl$, avg_edge_bps, rejects):")
    for r in res: print("   ", r[0], f"{r[1]:+.4f}", f"{r[2]:+.4f}", f"{r[3]:+.2f}", r[4])
    check("quotes rest at/near the touch (avg edge captured > 0 bps)", all(r[3] > 0 for r in res))
    check("spread capture positive in benign flow", all(r[1] > 0 for r in res))

async def scenario_ladder():
    print("12. multi-level quoting (EXTRA_LEVELS)")
    bot, sim, clk = make(EXTRA_LEVELS=2, LEVEL_SPACING_BPS="4", LEVEL_SIZE_MULT="0.6",
                         MIN_EDGE_BPS="1.0", MAX_POSITION_USD="60", ORDER_USD="20", SESSION_MAX_LOSS_USD=100)
    for i in range(20):                                    # let quotes settle (multiple ticks to place all levels)
        await step(bot, sim, clk, D("81000.0") + D(str(i)) * D("0.001"), D("81000.1") + D(str(i)) * D("0.001"))
    buys = sorted((o for o in bot.om.orders.values() if o.side == "BUY"), key=lambda o: -o.price)
    sells = sorted((o for o in bot.om.orders.values() if o.side == "SELL"), key=lambda o: o.price)
    check("3 resting levels per side (touch + 2 extra)", len(buys) == 3 and len(sells) == 3, f"{len(buys)} {len(sells)}")
    check("each buy level is further from mid than the last", all(buys[i].price > buys[i+1].price for i in range(2)), str([b.price for b in buys]))
    check("each sell level is further from mid than the last", all(sells[i].price < sells[i+1].price for i in range(2)), str([s.price for s in sells]))
    check("outer levels are smaller (LEVEL_SIZE_MULT < 1)", buys[2].qty < buys[0].qty and sells[2].qty < sells[0].qty)
    worst_long = bot.ledger.position * D("81000") + sum(o.remaining * o.price for o in buys)
    worst_short = -bot.ledger.position * D("81000") + sum(o.remaining * o.price for o in sells)
    check("cumulative ladder exposure (if every level fills) stays within MAX_POSITION_USD",
          worst_long <= D("61") and worst_short <= D("61"), f"{worst_long} {worst_short}")
    # a sustained adverse move should pull the WHOLE buy side (touch + every extra level), not just
    # level 0 - keep the price falling for the whole window so the trend-hold hysteresis stays armed
    # (a flat price would legitimately let it expire after TREND_HOLD_S and resume quoting - that's
    # correct behavior, not what this checks)
    px = D("80950.0")
    for i in range(10):
        px -= D("2.0")
        await step(bot, sim, clk, px, px + D("0.1"), dt=1.0)
    check("trend guard pulls the entire ladder on the blocked side, not just the touch quote",
          not [o for o in bot.om.orders.values() if o.side == "BUY"],
          str([o.price for o in bot.om.orders.values() if o.side == "BUY"]))

async def scenario_touch_speed():
    print("13. touch level re-quotes fast; ladder levels stay throttled")
    bot, sim, clk = make(EXTRA_LEVELS=1, LEVEL_SPACING_BPS="4", LEVEL_SIZE_MULT="0.6", MIN_EDGE_BPS="1.0",
                         MAX_POSITION_USD="60", ORDER_USD="20", MIN_REQUOTE_S="5", TOUCH_MIN_REQUOTE_S="0.2",
                         REQUOTE_BPS="0.1", TREND_PULL_BPS="50", VOL_PAUSE_BPS="50", SESSION_MAX_LOSS_USD=100)
    await step(bot, sim, clk, D("81000.0"), D("81000.1"))
    buy0 = min((o for o in bot.om.orders.values() if o.side == "BUY"), key=lambda o: abs(o.price - D("81000.05")))
    outer0 = max((o for o in bot.om.orders.values() if o.side == "BUY"), key=lambda o: abs(o.price - D("81000.05")))
    t0_id, o_id = buy0.order_id, outer0.order_id
    t0_last, o_last = buy0.last_action, outer0.last_action
    # mid rises: the BUY side CHASES (moves toward the market - not urgent, throttled by
    # min_requote_s at every level except the touch, which uses TOUCH_MIN_REQUOTE_S). A SELL-side
    # move here would be a "retreat" instead (untrottled at every level, by design - see
    # _manage_one), so this only isolates cleanly on the chasing side.
    await step(bot, sim, clk, D("81005.0"), D("81005.1"), dt=1.0)
    buy0b = bot.om.orders.get(t0_id)
    outerb = bot.om.orders.get(o_id)
    check("touch level re-quoted within 1s (TOUCH_MIN_REQUOTE_S=0.2s)",
          buy0b is not None and buy0b.last_action > t0_last, str(buy0b))
    check("ladder level did NOT re-quote yet (MIN_REQUOTE_S=5s)",
          outerb is not None and outerb.last_action == o_last, str(outerb))


async def scenario_journal():
    print("14. data-collection journal: features at fill, level/role tags, multi-horizon markouts")
    import json, tempfile, os, subprocess, sys as _sys
    path = os.path.join(tempfile.mkdtemp(), "j.jsonl")
    bot, sim, clk = make(JOURNAL_PATH=path, RUN_TAG="t1", MARKOUT_HORIZONS_S="1,5", EXTRA_LEVELS=1,
                         MIN_EDGE_BPS="1.0", MAX_POSITION_USD="60", ORDER_USD="20", SESSION_MAX_LOSS_USD=100,
                         TREND_PULL_BPS="100", VOL_PAUSE_BPS="100")
    px = D("81000.0")
    for i in range(30):
        await step(bot, sim, clk, px, px + D("0.1"))
    px -= D("40.0")                           # one gap down through the bids (bot can't retreat in time) -> fills
    await step(bot, sim, clk, px, px + D("0.1"), dt=1.0)
    for i in range(12):                       # let the 1s/5s markouts mature
        await step(bot, sim, clk, px, px + D("0.1"), dt=1.0)
    bot._journal and bot._journal.flush()
    lines = [json.loads(l) for l in open(path)]
    fills = [l for l in lines if l.get("type") == "fill"]
    marks = [l for l in lines if l.get("type") == "markout"]
    check("fills were journaled", len(fills) >= 1, str(len(lines)))
    f0 = fills[0]
    check("fill line carries level, role, tag and market state",
          all(k in f0 for k in ("fid", "tag", "level", "role", "imbalance", "ret_bps", "move_bps", "spread_bps")) and f0["tag"] == "t1", str(f0))
    check("level/role are actually populated from the order that filled", f0["level"] in (0, 1) and f0["role"] in ("add", "reduce"), str(f0))
    check("every fill gets a markout at each configured horizon",
          all({m["h"] for m in marks if m["fid"] == f["fid"]} == {1.0, 5.0} for f in fills), str(marks[:4]))
    r = subprocess.run([_sys.executable, "analyze.py", path, "--h", "5"], capture_output=True, text=True)
    check("analyze.py on a tiny journal says 'keep collecting' instead of crashing",
          r.returncode != 0 and "not enough matured markouts" in (r.stdout + r.stderr) and "Traceback" not in r.stderr, (r.stdout + r.stderr)[-200:])
    # synthetic journal with a KNOWN answer: 300 fills, bids lose ~2bps, asks gain ~1bps, sd ~3
    rnd = random.Random(7)
    syn = os.path.join(os.path.dirname(path), "syn.jsonl")
    with open(syn, "w") as fh:
        for i in range(300):
            side = "BUY" if i % 2 == 0 else "SELL"
            mean = -2.0 if side == "BUY" else 1.0
            fh.write(json.dumps({"type": "fill", "fid": i, "tag": "A", "side": side, "level": i % 2, "role": "add",
                                 "edge_bps": 1.0, "imbalance": rnd.uniform(-1, 1), "ret_bps": rnd.uniform(-3, 3)}) + "\n")
            fh.write(json.dumps({"type": "markout", "fid": i, "h": 5.0, "bps": rnd.gauss(mean, 3.0)}) + "\n")
    r = subprocess.run([_sys.executable, "analyze.py", syn, "--h", "5", "--min-effect", "0.5"], capture_output=True, text=True)
    out = r.stdout
    buy_line = [l for l in out.splitlines() if l.strip().startswith("BUY")][0]
    sell_line = [l for l in out.splitlines() if l.strip().startswith("SELL")][0]
    check("analyze.py flags a genuinely toxic side as BAD (n=150, mean -2bps)", r.returncode == 0 and "BAD" in buy_line, buy_line)
    check("analyze.py flags a genuinely good side as GOOD (n=150, mean +1bps)", "GOOD" in sell_line, sell_line)
    need = int(out.split("you need about ")[1].split()[0])
    check("required-sample-size estimate is in the right ballpark (1.96*sd/0.5)^2 ~ 230 for sd~3", 150 <= need <= 450, str(need))

async def main():
    await scenario_wide_spread()
    await scenario_falling_knife(); await scenario_halt(); await scenario_invariants()
    await scenario_reject_backoff(); await scenario_paper(); await scenario_burst_guard()
    await scenario_ladder(); await scenario_touch_speed(); await scenario_journal()

asyncio.run(main())
print("\nFAILED: " + ", ".join(FAILS) if FAILS else "\nALL TESTS PASSED")
sys.exit(1 if FAILS else 0)
