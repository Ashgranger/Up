"""Level 4-7 feature tests for the merged bot: `python test_level7.py` (offline, no pytest needed)."""
import asyncio, json, os, random, sys, tempfile
from decimal import Decimal as D

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sim import *                                   # noqa: E402,F403  (make, step, mkcfg, MKT, D ...)
import config as C                                  # noqa: E402
from strategy import Snapshot, Strategy             # noqa: E402
from ledger import Ledger                           # noqa: E402
from learner import OnlineLearner                   # noqa: E402
from market import Market, MarketData, classify_regime   # noqa: E402
from orders import Order                            # noqa: E402

FAILS = []


def check(name, cond, extra=""):
    print(("  ok   " if cond else "  FAIL ") + name + (f"  [{extra}]" if extra and not cond else ""))
    if not cond:
        FAILS.append(name)


MK = Market(1, "XYZ-USD", "ONLINE", D("0.01"), D("0.001"), [], D("5"), D("0.001"), D("100000"), D("0"), False)


def snap(cfg, **kw):
    base = dict(now=100.0, market=MKT, bid=D("81349.0"), ask=D("81349.1"), mid=D("81349.05"), micro=D("81349.05"),
                position=D(0), avg_cost=D(0), hold_s=0.0, ret_bps=D(0), move_bps=D(0), vol_bps=D(0), tox_bps=D(0))
    base.update(kw)
    return Snapshot(**base)


def wide(cfg, **kw):
    """10-tick-wide book on a 0.01-tick market: room for penny quotes and flow shading."""
    return snap(cfg, market=MK, bid=D("100.00"), ask=D("100.10"), mid=D("100.05"), micro=D("100.05"), **kw)


# ------------------------------------------------------------------------------------------------ #
print("1. config: the L7 environment parses exactly")
ENV = dict(MAX_HOLD_S=120, SESSION_MAX_LOSS_USD="2.0", HALT_EXIT=1, TREND_WINDOW_S="5.0", TREND_PULL_BPS="6.0",
           TREND_WIDEN="1.0", TREND_HOLD_S="2.0", VOL_WINDOW_S="5.0", VOL_PAUSE_BPS="25.0", JUMP_BPS="8.0",
           JUMP_COOLDOWN_S="2.0", BURST_FILLS=3, BURST_WINDOW_S="15.0", BURST_COOLDOWN_S="12.0",
           SWEEP_GUARD_FILLS=2, SWEEP_GUARD_WINDOW_S="1.0", REQUOTE_BPS="1.0", RETREAT_BPS="0.4",
           MIN_REQUOTE_S="1.5", MAX_ACTIONS_PER_MIN=1060, LOOP_S="0.25", HEARTBEAT_S="5.0", RECONCILE_S="5.0",
           STATUS_S="15.0", STALE_S="15.0", MAX_MARKET_SPREAD_BPS="40.0", MAX_ORACLE_DEV_BPS="150.0",
           QUOTE_OUTSIDE_RTH=1)
cfg = mkcfg(**ENV)
got = dict(max_hold_s=cfg.max_hold_s, loss=cfg.session_max_loss_usd, halt=cfg.halt_exit, tw=cfg.trend_window_s,
           tp=cfg.trend_pull_bps, tw2=cfg.trend_widen, th=cfg.trend_hold_s, vw=cfg.vol_window_s, vp=cfg.vol_pause_bps,
           jb=cfg.jump_bps, jc=cfg.jump_cooldown_s, bf=cfg.burst_fills, bw=cfg.burst_window_s, bc=cfg.burst_cooldown_s,
           sf=cfg.sweep_guard_fills, sw=cfg.sweep_guard_window_s, rq=cfg.requote_bps, rt=cfg.retreat_bps,
           mr=cfg.min_requote_s, ma=cfg.max_actions_per_min, lp=cfg.loop_s, hb=cfg.heartbeat_s, rc=cfg.reconcile_s,
           st=cfg.status_s, sl=cfg.stale_s, ms=cfg.max_market_spread_bps, mo=cfg.max_oracle_dev_bps,
           rth=cfg.quote_outside_rth)
want = dict(max_hold_s=120.0, loss=D("2.0"), halt=True, tw=5.0, tp=D("6.0"), tw2=D("1.0"), th=2.0, vw=5.0,
            vp=D("25.0"), jb=D("8.0"), jc=2.0, bf=3, bw=15.0, bc=12.0, sf=2, sw=1.0, rq=D("1.0"), rt=D("0.4"),
            mr=1.5, ma=1060, lp=0.25, hb=5.0, rc=5.0, st=15.0, sl=15.0, ms=D("40.0"), mo=D("150.0"), rth=True)
bad = {k: (got[k], want[k]) for k in want if got[k] != want[k]}
check("every variable in the L7 env block is read into Config", not bad, str(bad))
d = mkcfg()
check("L4-L7 model defaults present (EV, OBI/TFI, kappa, gamma, regimes, state path)",
      d.min_ev_bps == D("0.2") and d.enable_adaptive_ev and d.enable_orderbook_intel and d.enable_online_learning
      and d.obi_alpha == D("1.0") and d.tfi_beta == D("1.5") and d.fill_prob_kappa == D("0.25")
      and d.gamma_risk_aversion == D("0.1") and d.ev_hysteresis_bps == D("0.1"))

# ------------------------------------------------------------------------------------------------ #
print("2. order-book intelligence: OBI/TFI fair value, clamped inside the book")
cfg = mkcfg(ENABLE_ADAPTIVE_EV="0", EXTRA_LEVELS=0)
st = Strategy(cfg)
p0 = st.plan(wide(cfg))
pobi = st.plan(wide(cfg, obi=D("0.5")))
check("neutral book: fair == micro", p0.fair == D("100.05"), str(p0.fair))
check("bid-heavy book (OBI +0.5): fair shifts up by 0.5 half-spreads", pobi.fair == D("100.075"), str(pobi.fair))
check("fair value never leaves [bid, ask]", st.plan(wide(cfg, obi=D("1"), tfi=D("1"))).fair == D("100.10"))
check("buying flow (TFI +0.8) shades the ASK away from the market",
      st.plan(wide(cfg, tfi=D("0.8"))).ask.price > p0.ask.price, str((st.plan(wide(cfg, tfi=D("0.8"))).ask, p0.ask)))
cfg_off = mkcfg(ENABLE_ADAPTIVE_EV="0", EXTRA_LEVELS=0, ENABLE_ORDERBOOK_INTEL="0")
check("ENABLE_ORDERBOOK_INTEL=0 -> OBI/TFI ignored", Strategy(cfg_off).plan(wide(cfg_off, obi=D("0.5"), tfi=D("0.8"))).fair == D("100.05"))

md = MarketData(mkcfg())
md.on_trade("BUY", D("3"), D("100"), 10.0); md.on_trade("SELL", D("1"), D("100"), 10.5)
check("trade-flow imbalance = (buy-sell)/total", md.trade_flow_imbalance(10.0, 11.0) == D("0.5"))
check("trades older than the window are ignored", md.trade_flow_imbalance(10.0, 30.0) == D(0))
md.update(D("100"), D("100.1"), D("9"), D("1"), 11.0)
check("top-of-book OBI = (bid_sz-ask_sz)/total", md.obi == D("0.8"))

# ------------------------------------------------------------------------------------------------ #
print("3. regimes")
c = mkcfg()
check("quiet", classify_regime(c, D(0), D(0), D(0), D(0)) == "REGIME_A_QUIET")
check("high vol", classify_regime(c, D(0), D(0), D(0), D(6)) == "REGIME_B_HIGH_VOL")
check("trend (flow)", classify_regime(c, D(0), D("0.5"), D(0), D(0)) == "REGIME_C_TREND")
check("toxic beats everything", classify_regime(c, D("3"), D("0.9"), D("0.9"), D(9)) == "REGIME_D_TOXIC")
cfgT = mkcfg(EXTRA_LEVELS=2, ENABLE_ADAPTIVE_EV="0")
stT = Strategy(cfgT)
pq, pt = stT.plan(snap(cfgT)), stT.plan(snap(cfgT, tox_bps=D("3")))
check("toxic regime widens the edge", pt.edge_bps > pq.edge_bps, str((pq.edge_bps, pt.edge_bps)))
check("toxic regime drops the extra add levels", len(pq.extra_bids) == 2 and not pt.extra_bids and not pt.extra_asks,
      str((len(pq.extra_bids), len(pt.extra_bids))))
check("toxic regime is reported on the plan", pt.regime == "REGIME_D_TOXIC" and pq.regime == "REGIME_A_QUIET")

# ------------------------------------------------------------------------------------------------ #
print("4. reservation price: non-linear inventory skew + volatility risk aversion")
cfg = mkcfg(ENABLE_ADAPTIVE_EV="0", EXTRA_LEVELS=0, MAX_POSITION_USD=200, ORDER_USD=20)
st = Strategy(cfg)
sm = st.plan(snap(cfg, position=D("0.0005"), avg_cost=D("81349.05")))     # ~$40 long = 20% of cap
bg = st.plan(snap(cfg, position=D("0.0020"), avg_cost=D("81349.05")))     # ~$160 long = 80% of cap
check("skew grows with inventory", bg.skew_bps > sm.skew_bps > 0, str((sm.skew_bps, bg.skew_bps)))
check("...faster than linearly (q^1.3: 4x the inventory -> ~6x the skew)", bg.skew_bps / sm.skew_bps > D("5"), str(bg.skew_bps / sm.skew_bps))
pv = st.plan(snap(cfg, position=D("0.0020"), avg_cost=D("81349.05"), vol_bps=D("5")))
check("volatility adds risk-aversion skew (gamma * vol * q)", pv.skew_bps > bg.skew_bps, str((bg.skew_bps, pv.skew_bps)))

# ------------------------------------------------------------------------------------------------ #
print("5. add-quote gating: pressure, anti-chasing, inventory rotation")
cfg = mkcfg(EXTRA_LEVELS=1, MAX_POSITION_USD=200, ORDER_USD=20)
st = Strategy(cfg)
ps = st.plan(snap(cfg, obi=D("-1"), tfi=D("-1")))
check("severe selling pressure: no bid adds at all, ask still quoted", ps.bid is None and not ps.extra_bids and ps.ask is not None,
      str((ps.bid, ps.extra_bids, ps.notes)))
pb = st.plan(snap(cfg, obi=D("1"), tfi=D("1")))
check("severe buying pressure: no ask adds at all, bid still quoted", pb.ask is None and not pb.extra_asks and pb.bid is not None)
pl = st.plan(snap(cfg, position=D("0.000246"), avg_cost=D("81349.05")))     # ~$20 long: already loaded
check("already long: touch (L0) bid suppressed, deeper level kept", pl.bid is None and len(pl.extra_bids) == 1
      and pl.extra_bids[0].level == 1, str((pl.bid, pl.extra_bids)))
check("...while the exit (reduce) ask stays live", pl.ask is not None and pl.ask.role == "reduce")
pch = st.plan(snap(cfg, ret_bps=D("1.5"), tox_bps=D("3")))
check("chasing a rally in a toxic regime: L0 bid suppressed", pch.bid is None, str((pch.bid, pch.notes)))
pcalm = st.plan(snap(cfg, ret_bps=D("1.5")))
check("same rally in a calm regime: still quoting the bid", pcalm.bid is not None)

# ------------------------------------------------------------------------------------------------ #
print("6. expected-value filter + hysteresis")
cfg0 = mkcfg(ENABLE_ADAPTIVE_EV="0", EXTRA_LEVELS=0)
ev0 = Strategy(cfg0).plan(snap(cfg0)).bid.ev_bps
check("every add quote carries its EV and fill probability", ev0 is not None and Strategy(cfg0).plan(snap(cfg0)).bid.p_fill is not None)
cfgE = mkcfg(MIN_EV_BPS=str(ev0 + D("0.05")), EV_HYSTERESIS_BPS="0.1", EXTRA_LEVELS=0)
stE = Strategy(cfgE)
check("EV just under MIN_EV_BPS -> quote not placed", stE.plan(snap(cfgE)).bid is None)
check("...but an already-resting level keeps its place (hysteresis)",
      stE.plan(snap(cfgE, live_levels=frozenset({(0, "BUY")}))).bid is not None)
cfgW = mkcfg(EXTRA_LEVELS=0)
pw = Strategy(cfgW).plan(snap(cfgW, market=MK, bid=D("100.00"), ask=D("100.50"), mid=D("100.25"), micro=D("100.25")))
check("quote too far from the far touch to ever fill -> EV filter drops it", pw.bid is None and pw.ask is None)
cfgM = mkcfg(EXTRA_LEVELS=0, TREND_PULL_BPS=50)
pm2 = Strategy(cfgM).plan(snap(cfgM, ret_bps=D("-6")))
check("falling market: expected adverse move eats the EV of a bid (no trend pull needed)", pm2.bid is None, str(pm2.notes))

# ------------------------------------------------------------------------------------------------ #
print("7. exits: profit floor decays with hold time, stress exits at the touch")
cfg = mkcfg(EXTRA_LEVELS=0, MAX_HOLD_S=120)
st = Strategy(cfg)
pos = D("0.0004")
a0 = st.plan(snap(cfg, position=pos, avg_cost=D("81349.0"), hold_s=10)).ask.price
a1 = st.plan(snap(cfg, position=pos, avg_cost=D("81349.0"), hold_s=70)).ask.price
a2 = st.plan(snap(cfg, position=pos, avg_cost=D("81349.0"), hold_s=200)).ask.price
check("floor: fresh 1.0bps > 0.8bps after 0.5x hold > 0.2bps after 1.5x hold", a0 >= a1 >= a2 and a0 > a2, str((a0, a1, a2)))
check("...but never below cost", a2 > D("81349.0"))
ps = st.plan(snap(cfg, bid=D("81100.0"), ask=D("81100.1"), mid=D("81100.05"), micro=D("81100.05"), position=pos, avg_cost=D("81300")))
check("underwater beyond STRESS_LOSS_BPS -> exit at the touch, no adding", ps.stress and ps.ask.price <= D("81100.1") and ps.bid is None)

# ------------------------------------------------------------------------------------------------ #
print("8. per-side toxicity + time-weighted markouts")
lg = Ledger(mkcfg(MARKOUT_HORIZON_S="1"))
lg.on_fill("BUY", D("0.001"), D("100"), D("100"), 0.0, D(5))
lg.on_fill("SELL", D("0.001"), D("100"), D("100"), 0.0, D(5))
lg.process_markouts(D("99.98"), 2.0)              # mid fell 2bps: bad for the buy, good for the sell
check("buy side is toxic, sell side is not", lg.side_tox_bps("BUY") > 0 and lg.side_tox_bps("SELL") == 0,
      str((lg.side_tox_bps("BUY"), lg.side_tox_bps("SELL"))))
lg.current_now = 100.0
check("markouts fade: 60s+ old samples stop counting", lg.tox_bps == 0 and lg.side_tox_bps("BUY") == 0)
lg2 = Ledger(mkcfg())
check("a side with no samples borrows the overall figure (0 with none)", lg2.side_tox_bps("BUY") == 0)

# ------------------------------------------------------------------------------------------------ #
print("9. online learner")
tmp = tempfile.mkdtemp()
path = os.path.join(tmp, "learn.json")
cfgL = mkcfg(LEARNING_STATE_PATH=path)
L = OnlineLearner(cfgL)
base_edge, base_ev = L.min_edge_bps, L.min_ev_bps
L.on_markout(D("-10"), "BUY", D("3"))
check("adverse markout widens edges, spacing, tox_mult and EV hurdle",
      L.min_edge_bps > base_edge and L.min_ev_bps > base_ev and L.tox_mult > D("1") and L.level_spacing_bps > D("4"))
wide_edge = L.min_edge_bps
L.on_markout(D("2"), "BUY", D("0"))
check("benign markout relaxes them back towards base", base_edge < L.min_edge_bps < wide_edge)
L.tick_decay(0.0); L.tick_decay(600.0)
check("idle decay returns everything to base", abs(L.min_edge_bps - base_edge) < D("0.001"), str(L.min_edge_bps))
L.on_markout(D("-10"), "BUY", D("3"))
check("state is persisted atomically", os.path.exists(path) and json.load(open(path))["params"]["min_edge_bps"])
L2 = OnlineLearner(cfgL)
check("...and reloaded by the next run", L2.min_edge_bps == L.min_edge_bps and L2.n_markouts == L.n_markouts, str((L2.min_edge_bps, L.min_edge_bps)))
cfgOther = mkcfg(LEARNING_STATE_PATH=path, MARKET="ETH-USD")
check("state saved for another market is not loaded", OnlineLearner(cfgOther).min_edge_bps == cfgOther.min_edge_bps)
Loff = OnlineLearner(mkcfg(ENABLE_ONLINE_LEARNING="0"))
Loff.on_markout(D("-10"), "BUY", D("3"))
check("ENABLE_ONLINE_LEARNING=0 -> params are exactly the config", Loff.min_edge_bps == Loff.base["min_edge_bps"] and Loff.n_markouts == 0)
L3 = OnlineLearner(mkcfg(MAX_POSITION_USD=100))
L3.on_fill("BUY", D("100"), D("100"), D("90"), 60.0)
check("stagnant / heavy inventory raises skew and risk aversion", L3.skew_bps > D("3") and L3.gamma_risk_aversion > D("0.1"))
L4 = OnlineLearner(mkcfg()); a0 = L4.obi_alpha
L4.on_flow_correlation(D("0.6"), D("0"), D("1")); L4.on_flow_correlation(D("0.6"), D("0"), D("1"))
check("OBI weight grows when imbalance predicted the move", L4.obi_alpha > a0)
L4.on_flow_correlation(D("0.6"), D("0"), D("-1")); L4.on_flow_correlation(D("0.6"), D("0"), D("-1")); L4.on_flow_correlation(D("0.6"), D("0"), D("-1"))
check("...and shrinks when it did not", L4.obi_alpha < a0 + D("0.04"))
learned = OnlineLearner(mkcfg(ENABLE_ADAPTIVE_EV="0", EXTRA_LEVELS=0))
learned.on_markout(D("-15"), "BUY", D("3")); learned.on_markout(D("-15"), "BUY", D("3"))
cfgS = mkcfg(ENABLE_ADAPTIVE_EV="0", EXTRA_LEVELS=0)
p_static = Strategy(cfgS).plan(snap(cfgS))
p_learn = Strategy(cfgS).plan(snap(cfgS, params=learned))
check("the strategy quotes wider once the learner has seen toxic fills", p_learn.bid.price < p_static.bid.price and p_learn.ask.price > p_static.ask.price,
      str((p_static.bid, p_learn.bid)))


async def scenarios():
    # -------------------------------------------------------------------------------------------- #
    print("10. trades channel -> trade-flow imbalance; subscription")
    bot, sim, clk = make(EXTRA_LEVELS=0)
    subs = []
    orig = sim.send

    async def send(raw):
        m = json.loads(raw)
        if m["type"] == "subscribe":
            subs.append(m["channel"])
        await orig(raw)
    sim.send = send
    await step(bot, sim, clk, "81000.0", "81000.1")
    await bot.startup()
    check("bot subscribes to the public trades channel", "trades" in subs, str(subs))
    sim.push_trade("BUY", "3", "81000.1"); sim.push_trade("BUY", "1", "81000.1"); sim.push_trade("SELL", "1", "81000.0")
    for _ in range(4):
        await asyncio.sleep(0)
    check("trades feed the imbalance (3 buys+1, 1 sell -> +0.6)", bot.md.trade_flow_imbalance(10.0, clk.t) == D("0.6"),
          str(bot.md.trade_flow_imbalance(10.0, clk.t)))
    bot.ex.handle_message(json.dumps({"type": "subscribed", "channel": "trades", "id": "BTC-USD",
                                       "contents": [{"side": "SELL", "size": "50", "price": "1"}]}))
    check("a trades snapshot (history replay) is not stamped as fresh flow", bot.md.trade_flow_imbalance(10.0, clk.t) == D("0.6"))

    # -------------------------------------------------------------------------------------------- #
    print("11. guards: burst/sweep pull only ADD orders, never an exit")
    bot, sim, clk = make(EXTRA_LEVELS=0)
    await step(bot, sim, clk, "81000.0", "81000.1")
    add = Order("add-1", "BUY", D("80900.0"), D("0.0003"), D("0.0003"), 0, clk.t, clk.t, level=0, role="add")
    red = Order("red-1", "BUY", D("80950.0"), D("0.0003"), D("0.0003"), 0, clk.t, clk.t, level=0, role="reduce")
    bot.om.orders = {"add-1": add, "red-1": red}
    await bot.om.cancel_side("BUY", clk.t)
    check("cancel_side removed the add order", "add-1" not in bot.om.orders)
    check("...and left the reducing (exit) order alone", "red-1" in bot.om.orders)
    bot.ledger.learner.enabled = True
    bot.ledger.learner.params["burst_fills"] = D("3")
    for _ in range(3):
        clk.t += 2.0
        bot.on_fill("BUY", D("0.0001"), D("81000.0"), None)
    check("burst threshold comes from the learner (live) parameters", bot.cooldown["BUY"] > clk.t)

    # -------------------------------------------------------------------------------------------- #
    print("12. RTH pause (QUOTE_OUTSIDE_RTH) does not spam cancelAllOrders")
    mk_rth = Market(18, "HOOD-USD", "ONLINE", D("0.01"), D("0.0000001"), [], D("5"), D("0.0000001"), D("10000"), D("118.575"), True)
    bot, sim, clk = make(EXTRA_LEVELS=0, QUOTE_OUTSIDE_RTH="0")
    bot.md.info = mk_rth; bot.md.info_ts = clk.t
    await step(bot, sim, clk, "118.56", "118.59")
    sim.posts.clear()
    for _ in range(10):
        clk.t += 0.25; bot.md.info_ts = clk.t
        await bot.tick()
    check("outside RTH with QUOTE_OUTSIDE_RTH=0: nothing sent", len(sim.posts) == 0 and not bot.can_quote(clk.t)[0], str(sim.posts))
    bot2, sim2, clk2 = make(EXTRA_LEVELS=0, QUOTE_OUTSIDE_RTH="1")
    bot2.md.info = mk_rth; bot2.md.info_ts = clk2.t
    await step(bot2, sim2, clk2, "118.56", "118.59")
    check("QUOTE_OUTSIDE_RTH=1: quotes outside RTH", bot2.can_quote(clk2.t)[0] and len(bot2.om.orders) >= 1)
    bot3, sim3, clk3 = make(EXTRA_LEVELS=0)
    check("cancel_all is skipped when nothing of ours can be resting", (await bot3.om.cancel_all()) is None and "cancelAllOrders" not in sim3.posts)

    # -------------------------------------------------------------------------------------------- #
    print("13. end-to-end with a live tape: accounting exact, learning state saved on stop")
    state = os.path.join(tempfile.mkdtemp(), "state.json")
    for seed in (5, 6):
        rnd = random.Random(seed)
        bot, sim, clk = make(SESSION_MAX_LOSS_USD=100, MIN_EDGE_BPS="1.0", ORDER_USD=30, MAX_POSITION_USD=90,
                             LEARNING_STATE_PATH=state, EXTRA_LEVELS=1)
        bot.md.info = MK; bot.cfg.market = "XYZ-USD"
        fair = D("100.00")
        for i in range(3000):
            fair = (fair + D(str(round(rnd.gauss(0, 0.004), 3)))).quantize(D("0.001"))
            bid = (fair - D("0.03")).quantize(D("0.01")); ask = (fair + D("0.03")).quantize(D("0.01"))
            await step(bot, sim, clk, bid, ask, dt=0.25, tick=False)
            if rnd.random() < 0.3:
                sim.push_trade("BUY" if rnd.random() < 0.5 else "SELL", "1", fmt(fair))
            if rnd.random() < 0.15:
                sim.taker("BUY" if rnd.random() < 0.5 else "SELL")
                await asyncio.sleep(0); await asyncio.sleep(0)
            await bot.tick(); await asyncio.sleep(0)
        mid = (sim.bid + sim.ask) / 2
        led = bot.ledger
        check(f"seed {seed}: ledger PnL == simulator PnL ({led.total_pnl(mid):+.4f})", abs(led.total_pnl(mid) - sim.pnl(mid)) < D("1e-6"),
              f"{led.total_pnl(mid)} vs {sim.pnl(mid)}")
        check(f"seed {seed}: position == exchange position", abs(led.position - sim.position) < D("1e-9"))
        check(f"seed {seed}: exposure never above MAX_POSITION_USD (+1 order)", abs(led.position * mid) <= D("90") + D("31"))
        check(f"seed {seed}: the learner actually ran ({led.learner.total_learned_updates} updates)", led.learner.total_learned_updates > 0)
    await bot.shutdown_orders()
    check("learning state written on shutdown", os.path.exists(state) and json.load(open(state))["params"])


asyncio.run(scenarios())
print()
print("ALL LEVEL-7 TESTS PASSED" if not FAILS else f"{len(FAILS)} FAILED: {FAILS}")
sys.exit(1 if FAILS else 0)
