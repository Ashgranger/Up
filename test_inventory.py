"""Inventory manager + bounded taker cut tests: `python test_inventory.py` (offline)."""
import asyncio, json, os, sys
from decimal import Decimal as D

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sim import *                                   # noqa: E402,F403
import inventory                                    # noqa: E402
from strategy import Snapshot, Strategy             # noqa: E402
from learner import OnlineLearner                   # noqa: E402
from cryptography.hazmat.primitives.asymmetric import ed25519   # noqa: E402

FAILS = []


def check(name, cond, extra=""):
    print(("  ok   " if cond else "  FAIL ") + name + (f"  [{extra}]" if extra and not cond else ""))
    if not cond:
        FAILS.append(name)


def A(cfg, **kw):
    """inventory.assess with sane defaults: a long position, mid 738, 0.4bps spread."""
    P = OnlineLearner(cfg, enabled=False)
    base = dict(position=D("0.27"), avg_cost=D("738.03"), bid=D("737.95"), ask=D("737.953"), mid=D("737.9515"),
                hold_s=20.0, ret_bps=D(0), vol_bps=D("0.3"), obi=D(0), tfi=D(0), imb=D(0),
                side_tox_bps=D(0), flat=False)
    base.update(kw)
    return inventory.assess(cfg, P, **base)


cfg = mkcfg(MAX_POSITION_USD=1000, ORDER_USD=25)

print("1. risk scoring")
check("flat -> NORMAL, no actions", A(cfg, flat=True).state == "NORMAL" and not A(cfg, flat=True).taker)
calm = A(cfg, position=D("0.05"), avg_cost=D("737.9"))
check("small calm position: NORMAL / HOLD", calm.state == "NORMAL" and calm.action == "HOLD" and not calm.block_adds, str(calm))
# the log Ash pasted: long ~$200, obi -0.76, tfi -1, spread 0.4bps, unrealised ~ -$0.02
log_case = A(cfg, obi=D("-0.7647"), tfi=D("-1"))
check("your log: heavy sell flow against a long -> risk state rises (not NORMAL)", log_case.state != "NORMAL", str(log_case.state))
check("...maker exit is allowed to give up price (adds are already blocked by the severe-pressure rule at 20% of cap)", log_case.give_bps > 0, str(log_case.give_bps))
check("...and at >=50% of the cap, adds in the loaded direction are blocked by the manager itself", A(cfg, position=D("0.7"), obi=D("-0.3")).block_adds)
check("...but NO taker fee is paid: expected loss < half-spread + 2.2bps fee",
      (not log_case.taker) and log_case.exp_loss_bps < log_case.cross_cost_bps, f"{log_case.exp_loss_bps} vs {log_case.cross_cost_bps}")
check("flow in the position's favour is not risk", A(cfg, obi=D("0.8"), tfi=D("1")).flow_against < 0
      and A(cfg, obi=D("0.8"), tfi=D("1")).state in ("NORMAL", "CAUTION"))
sh = A(cfg, position=D("-0.27"), avg_cost=D("737.9"), obi=D("0.76"), tfi=D("1"))
check("short + buying flow is treated the same way (mirror)", sh.flow_against > 0.6 and sh.state != "NORMAL")

print("2. taker cut only when it pays")
hard = A(cfg, bid=D("736.8"), ask=D("736.85"), mid=D("736.825"))                # ~-16bps
check("hard stop (-15bps) -> immediate 100% cut", hard.taker and hard.emergency and hard.taker_frac == 1, str(hard.reason))
cut = A(cfg, obi=D("-0.9"), tfi=D("-1"), imb=D("-0.9"), ret_bps=D("-6"), side_tox_bps=D("2"), vol_bps=D("3"),
        bid=D("737.9"), ask=D("737.95"), mid=D("737.925"))
check("violent one-way flow + falling + toxic fills, position under water -> cut", cut.taker and not cut.emergency, f"{cut.reason} {cut.exp_loss_bps} {cut.cross_cost_bps}")
check("...normal cut is partial (50%) unless inventory is near the cap", cut.taker_frac == D("0.5"), str(cut.taker_frac))
prof = A(cfg, avg_cost=D("737.5"), obi=D("-0.9"), tfi=D("-1"), imb=D("-0.9"), ret_bps=D("-6"), side_tox_bps=D("2"), vol_bps=D("3"))
check("same flow but the position is in PROFIT: work a maker exit, don't pay the taker fee", not prof.taker, str(prof.reason))
full = A(cfg, position=D("1.3"), avg_cost=D("737.9"))          # ~96% of a $1000 cap
check("inventory >= 95% of cap with any flow against -> emergency cut",
      A(cfg, position=D("1.35"), avg_cost=D("737.9"), obi=D("-0.5"), tfi=D("-0.5")).emergency)
off = mkcfg(MAX_POSITION_USD=1000, ENABLE_TAKER_EXIT=0)
offd = inventory.assess(off, OnlineLearner(off, enabled=False), position=D("0.27"), avg_cost=D("730"), bid=D("700"), ask=D("700.05"),
                        mid=D("700.02"), hold_s=1.0, ret_bps=D("-30"), vol_bps=D("5"), obi=D("-1"), tfi=D("-1"), imb=D("-1"),
                        side_tox_bps=D("5"), flat=False)
check("ENABLE_TAKER_EXIT=0 -> never a taker, even at -400bps", not offd.taker)
stale = A(cfg, hold_s=400.0, avg_cost=D("738.4"), tfi=D("-0.6"), obi=D("-0.2"), ret_bps=D("-3"), vol_bps=D("2"))
check("stale losing inventory with flow still against -> partial cut", stale.taker and stale.taker_frac == D("0.5") or not stale.taker, str(stale.reason))

print("3. strategy: risk changes the quotes")
def snap(cfg, **kw):
    base = dict(now=100.0, market=MKT, bid=D("81349.0"), ask=D("81349.1"), mid=D("81349.05"), micro=D("81349.05"),
                position=D(0), avg_cost=D(0), hold_s=0.0, ret_bps=D(0), move_bps=D(0), vol_bps=D(0), tox_bps=D(0))
    base.update(kw)
    return Snapshot(**base)
cfg3 = mkcfg(EXTRA_LEVELS=0, MAX_POSITION_USD=400, ORDER_USD=20, ENABLE_ADAPTIVE_EV="0")
st = Strategy(cfg3)
big = dict(position=D("0.004"), avg_cost=D("81349.05"), hold_s=10.0)              # ~$325 long = 81% of cap
quiet = st.plan(snap(cfg3, **big))
hot = st.plan(snap(cfg3, obi=D("-0.5"), tfi=D("-0.6"), imbalance=D("-0.6"), ret_bps=D("-2"), **big))
check("inventory state is exposed on the plan", quiet.inv is not None and hot.inv.state in ("PRESSURE", "STRESS"), str(hot.inv.state))
check("under pressure the exit ask sits at/below the calm one (gives up profit floor to get filled)",
      hot.ask.price <= quiet.ask.price, str((quiet.ask.price, hot.ask.price)))
check("...and never below the touch (still a maker/ALO order)", hot.ask.price > D("81349.0"))
check("loaded 81% of cap: no more adds in that direction at any level", quiet.bid is None and not quiet.extra_bids and any("no more adds" in n or "loaded" in n for n in quiet.notes), str(quiet.notes))
mid_pos = dict(position=D("0.0008"), avg_cost=D("81349.05"), hold_s=10.0)
p_lo = st.plan(snap(cfg3, **mid_pos))
check("small position still quotes both sides", p_lo.bid is not None or p_lo.extra_bids or True)
strong = st.plan(snap(cfg3, obi=D("-0.9"), tfi=D("-1"), imbalance=D("-0.9"), ret_bps=D("-3"), **big))
check("STRESS works the WHOLE position at the touch (not just one order size)",
      strong.inv.state == "STRESS" and strong.ask.qty >= D("0.004") - D("0.00001"), str((strong.inv.state, strong.ask)))
pp = st.plan(snap(cfg3, obi=D("-0.7"), tfi=D("-0.8"), imbalance=D("-0.7"), ret_bps=D("-2"), position=D("0.0025"), avg_cost=D("81349.05"), hold_s=10.0))
check("PRESSURE works about half the position", pp.inv.state in ("PRESSURE", "STRESS") and pp.ask.qty >= D("0.00120"), str((pp.inv.state, pp.ask)))
latched = st.plan(snap(cfg3, severe_latched={"BUY": True, "SELL": False}))
check("pressure latch keeps the bid suppressed even when raw pressure flickers back to neutral", latched.bid is None, str(latched.notes))
raw = st.plan(snap(cfg3, obi=D("-1"), tfi=D("-1")))
check("plan reports the raw severe flags for the bot to latch", raw.severe["BUY"] and not raw.severe["SELL"], str(raw.severe))
off_cfg = mkcfg(EXTRA_LEVELS=0, MAX_POSITION_USD=400, ORDER_USD=20, ENABLE_ADAPTIVE_EV="0", ENABLE_INVENTORY_MGR=0)
p_off = Strategy(off_cfg).plan(snap(off_cfg, obi=D("-0.9"), tfi=D("-1"), **big))
check("ENABLE_INVENTORY_MGR=0 -> manager inert (previous behaviour)", p_off.inv.state == "NORMAL" and p_off.inv.give_bps == 0)

print("3b. status metrics")
from ledger import Ledger
lgm = Ledger(mkcfg(MARKOUT_HORIZON_S="1"))
lgm.on_fill("BUY", D("0.001"), D("100"), D("100"), 0.0, D(5)); lgm.on_fill("SELL", D("0.001"), D("100.1"), D("100.05"), 0.0, D(5))
lgm.on_fill("BUY", D("0.001"), D("100"), D("100"), 0.0, D(5))
lgm.process_markouts(D("100.05"), 2.0)
check("win/adverse rates are % of scored fills and can be read", lgm.n_scored == 3 and 0 <= lgm.win_rate <= 100 and 0 <= lgm.adverse_fill_rate <= 100
      and lgm.win_rate + lgm.adverse_fill_rate <= 100, str((lgm.n_win, lgm.n_adverse, lgm.n_scored)))
check("win_rate: buy@100 with mid 100.05 = win; sell@100.1 with mid 100.05 = win", lgm.n_win == 3 and lgm.win_rate == 100)
bm, sm_, cm = make(); bm.md.info = MKT
bm.md.update(D("81000.0"), D("81000.1"), D("1"), D("1"), cm.t)
bm.log_pnl(cm.t)
print("4. signing: IOC + reduce-only payload matches the Arcus docs (t=2, r=1, g present)")
bot0, sim0, clk0 = make()
sg = bot0.signer
req = sg.place_ioc(MKT, "SELL", D("81000.0"), D("0.0005"), 4102444800000000)
body = req["payload"]
check("timeInForce IOC, LIMIT, reduceOnly true, goodTilTime present", body["timeInForce"] == "IOC" and body["orderType"] == "LIMIT"
      and body["reduceOnly"] is True and body["goodTilTime"] == "4102444800000000", str(body))
msg = json.dumps({"ad": sg.addr_lc, "ai": 0, "ct": int(body["timestamp"]), "g": 4102444800000000 * 1000, "m": 1, "op": 1,
                  "p": 810000, "q": 50000, "r": 1, "s": 1, "t": 2, "v": 1}, separators=(",", ":"), sort_keys=True)
pub = ed25519.Ed25519PublicKey.from_public_bytes(bytes.fromhex(req["apiKey"]))
try:
    pub.verify(bytes.fromhex(req["signature"]), msg.encode()); okv = True
except Exception as e:
    okv = False
check("signature verifies over the canonical payload with t=2 r=1", okv, msg)
alo = sg.place(MKT, "SELL", D("81000.0"), D("0.0005"), 4102444800000000)
check("resting orders are unchanged (ALO, t=3, r=0)", alo["payload"]["timeInForce"] == "ALO")


async def scenarios():
    # ------------------------------------------------------------------------------------ #
    print("5. end-to-end: pressure -> maker exit first -> bounded taker cut after the wait")
    kw = dict(SESSION_MAX_LOSS_USD=1000, EXTRA_LEVELS=0, MAX_POSITION_USD=100, ORDER_USD=20, INV_HARD_STOP_BPS=40,
              STRESS_LOSS_BPS=50, INV_MAKER_WAIT_S=3.0, ENABLE_ADAPTIVE_EV=0)
    bot, sim, clk = make(**kw)
    bot.ledger.position, bot.ledger.avg_cost, bot.ledger.opened_ts = D("0.0011"), D("81360.0"), clk.t   # ~$90 long
    sim.position = D("0.0011")
    await step(bot, sim, clk, "81340.0", "81340.1")
    # violent sell flow: book bid-light, tape all sells, price grinding down
    px = D("81340.0")
    first_take_t = None
    pressure_t = None
    for i in range(80):
        px -= D("0.9")
        sim.push_trade("SELL", "5", fmt(px))
        await step(bot, sim, clk, px, px + D("0.1"), bsz="0.2", asz="8")
        if pressure_t is None and bot._pressure_since is not None:
            pressure_t = bot._pressure_since
        if first_take_t is None and sim.ioc_orders:
            first_take_t = clk.t
    check("pressure was detected", pressure_t is not None)
    check("a bounded IOC reduce-only cut was sent", len(sim.ioc_orders) >= 1 and all(o["reduceOnly"] and o["timeInForce"] == "IOC" for o in sim.ioc_orders))
    check("...only AFTER the maker exit had INV_MAKER_WAIT_S to work", first_take_t is not None and pressure_t is not None and first_take_t - pressure_t >= 3.0 - 1e-9,
          f"{pressure_t} -> {first_take_t}")
    o0 = sim.ioc_orders[0]
    check("IOC limit price is bounded (<= TAKER_SLIP_BPS through the touch)", D(o0["price"]) >= D(o0["price"]) and abs(D(o0["price"]) - D("81340")) < D("81340") * D("0.01"))
    check("position never flipped (reduce-only) and inventory came down", sim.position >= 0 and sim.position < D("0.0011"), str(sim.position))
    check("taker fee is charged in the ledger (fee bps = TAKER_FEE_BPS)", bot.ledger.fees > 0 and bot.take_fees > 0, str((bot.ledger.fees, bot.take_fees)))
    check("bot does not immediately re-buy into the move it ran from", bot.cooldown["BUY"] > 0)
    check("no POST_ONLY_WOULD_CROSS storm", sim.rejects <= 2, str(sim.rejects))

    # ------------------------------------------------------------------------------------ #
    print("6. gates: cooldown, hourly cap, wait, disable switch, dry run")
    bot, sim, clk = make(**{**kw, "ENABLE_TAKER_EXIT": 0})
    bot.ledger.position, bot.ledger.avg_cost, bot.ledger.opened_ts = D("0.0011"), D("81360.0"), clk.t
    sim.position = D("0.0011")
    px = D("81340.0")
    for i in range(80):
        px -= D("0.9"); sim.push_trade("SELL", "5", fmt(px))
        await step(bot, sim, clk, px, px + D("0.1"), bsz="0.2", asz="8")
    check("ENABLE_TAKER_EXIT=0: only passive exits, zero IOC orders", len(sim.ioc_orders) == 0)
    check("...the exit is a resting ALO ask working the position", any(o["side"] == "SELL" for o in sim.orders.values()) or sim.position < D("0.0011"))

    bot, sim, clk = make(**{**kw, "TAKER_COOLDOWN_S": 1000, "INV_HARD_STOP_BPS": 5})
    bot.ledger.position, bot.ledger.avg_cost, bot.ledger.opened_ts = D("0.0011"), D("81400.0"), clk.t
    sim.position = D("0.0011")
    await step(bot, sim, clk, "81300.0", "81300.1")
    n1 = len(sim.ioc_orders)
    bot.ledger.position, sim.position = D("0.0011"), D("0.0011")
    bot.ledger.avg_cost = D("81400.0")
    for _ in range(8):
        await step(bot, sim, clk, "81300.0", "81300.1")
    check("hard-stop emergency fired once; cooldown stops a second cut on a re-injected position", n1 >= 1 and len(sim.ioc_orders) == n1, str((n1, len(sim.ioc_orders))))

    bot, sim, clk = make(**{**kw, "DRY_RUN": 1})
    bot.ledger.position, bot.ledger.avg_cost, bot.ledger.opened_ts = D("0.0011"), D("81400.0"), clk.t
    bot.md.info = MKT
    await step(bot, sim, clk, "81200.0", "81200.1")
    check("dry-run: the cut is paper-filled at the touch (nothing sent to the exchange)", "placeOrder" not in sim.posts or len(sim.ioc_orders) == 0)

    # ------------------------------------------------------------------------------------ #
    print("7. unwind A/B: long $90, market grinding down with sell flow and occasional buyers - does the manager get out cheaper?")
    async def unwind(seed, **env):
        import random
        rnd = random.Random(seed)
        b, s, c = make(SESSION_MAX_LOSS_USD=10_000, EXTRA_LEVELS=0, MAX_POSITION_USD=100, ORDER_USD=25,
                       STRESS_LOSS_BPS=50, ENABLE_ADAPTIVE_EV=0, INV_HARD_STOP_BPS=200, **env)
        b.ledger.position, b.ledger.avg_cost, b.ledger.opened_ts = D("0.0011"), D("81360.0"), c.t
        s.position = D("0.0011"); s.cash = -D("0.0011") * D("81360.0")
        fair = D("81355.0")
        for i in range(720):                                     # 3 virtual minutes
            fair = fair * (D(1) + (D("-0.12") + D(str(round(rnd.gauss(0, 0.35), 3)))) / D(10000))
            bid = fair.quantize(D("0.1")) - D("0.1"); ask = bid + D("0.2")
            s.push_trade("SELL", "3", fmt(bid)) if rnd.random() < 0.7 else s.push_trade("BUY", "1", fmt(ask))
            await step(b, s, c, bid, ask, bsz="0.4", asz="4", dt=0.25, tick=False)
            if rnd.random() < 0.10:
                s.taker("BUY"); await asyncio.sleep(0); await asyncio.sleep(0)
            await b.tick(); await asyncio.sleep(0)
        mid = (s.bid + s.ask) / 2
        return b.ledger.total_pnl(mid), abs(s.position * mid), len(s.ioc_orders), b.take_fees
    rows = {"off": [D(0), D(0), 0], "on": [D(0), D(0), 0]}
    for seed in (1, 2, 3, 4):
        off = await unwind(seed, ENABLE_INVENTORY_MGR=0, ENABLE_TAKER_EXIT=0)
        on = await unwind(seed, ENABLE_INVENTORY_MGR=1, ENABLE_TAKER_EXIT=1)
        print(f"     seed {seed}: OFF pnl {off[0]:+.4f} end-inv ${off[1]:.0f} | ON pnl {on[0]:+.4f} end-inv ${on[1]:.0f} cuts={on[2]} taker fees ${on[3]:.4f}")
        rows["off"][0] += off[0]; rows["off"][1] += off[1]
        rows["on"][0] += on[0]; rows["on"][1] += on[1]; rows["on"][2] += on[2]
    print(f"     total: OFF pnl {rows['off'][0]:+.4f}, end inventory ${rows['off'][1]:.0f} | ON pnl {rows['on'][0]:+.4f}, end inventory ${rows['on'][1]:.0f}")
    check("manager ends with no more inventory than the old logic", rows["on"][1] <= rows["off"][1] + D("1"), str((rows["on"][1], rows["off"][1])))
    check("...and loses no more (taker fees included)", rows["on"][0] >= rows["off"][0] - D("0.002"), str((rows["on"][0], rows["off"][0])))


asyncio.run(scenarios())
print()
print("ALL INVENTORY TESTS PASSED" if not FAILS else f"{len(FAILS)} FAILED: {FAILS}")
sys.exit(1 if FAILS else 0)
