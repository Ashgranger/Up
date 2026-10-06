"""Tests for the predictive layer (fill / adverse / drift / hold-loss)."""
import os
import random
import tempfile
import unittest
from decimal import Decimal as D

import sim
from market import Market
from predictor import Predictor, OnlineModel, FEATURES, NF, IX, BUY, SELL
from utils import BUY as B, SELL as S


class TestPredictor(unittest.IsolatedAsyncioTestCase):

    def _bot(self, **env):
        bot, s, clock = sim.make(ORDER_USD=20, MAX_POSITION_USD=100, EXTRA_LEVELS=0, **env)
        m = sim.Market(1, "BTC-USD", "ONLINE", D("0.1"), D("0.0001"), [], D("5"), D("0.0001"), D("100000"), D("80000.0"), False)
        bot.md.info = m
        return bot, m, clock

    def test_01_online_ridge_learns_signal(self):
        random.seed(1)
        mdl = OnlineModel("ridge", [0.0] * NF, lr=0.2)
        for _ in range(4000):
            x = [0.0] * NF
            x[IX["obi1"]] = random.uniform(-1, 1)
            x[IX["bias"]] = 1.0
            y = 1.5 * x[IX["obi1"]] + random.gauss(0, 0.5)
            mdl.update(x, y)
        self.assertGreater(mdl.w[IX["obi1"]], 0.9)
        self.assertGreater(mdl.ema_hit, 0.7)

    def test_02_online_logit_learns_and_stays_bounded(self):
        random.seed(2)
        mdl = OnlineModel("logit", [0.0] * NF, lr=0.3)
        for _ in range(4000):
            x = [0.0] * NF
            x[IX["tfi1"]] = random.uniform(-1, 1)
            x[IX["bias"]] = 1.0
            y = 1.0 if (x[IX["tfi1"]] + random.gauss(0, 0.3)) < -0.2 else 0.0
            mdl.update(x, y)
        lo = [0.0] * NF; lo[IX["tfi1"]] = -1.0; lo[IX["bias"]] = 1.0
        hi = [0.0] * NF; hi[IX["tfi1"]] = 1.0; hi[IX["bias"]] = 1.0
        self.assertGreater(mdl.predict(lo), 0.8)
        self.assertLess(mdl.predict(hi), 0.2)
        for p in (mdl.predict(lo), mdl.predict(hi)):
            self.assertTrue(0.0 < p < 1.0)

    async def test_03_persistence_roundtrip(self):
        bot, m, clock = self._bot()
        with tempfile.TemporaryDirectory() as td:
            cfg = bot.cfg
            cfg.predictor_path = os.path.join(td, "p.json")
            p1 = Predictor(cfg)
            p1.m_d2.w[IX["obi1"]] = 0.777
            p1.m_d2.n = 55
            p1.save()
            p2 = Predictor(cfg)
            self.assertAlmostEqual(p2.m_d2.w[IX["obi1"]], 0.777)
            self.assertEqual(p2.m_d2.n, 55)

    async def test_04_features_are_side_signed(self):
        bot, m, clock = self._bot()
        bot.md.update(D("80000.0"), D("80000.8"), D("30.0"), D("3.0"), clock.t)
        pr = bot.predictor
        xb = pr.features(bot.md, BUY, clock.t, price=bot.md.bid, ledger=bot.ledger)
        xs = pr.features(bot.md, SELL, clock.t, price=bot.md.ask, ledger=bot.ledger)
        self.assertGreater(xb[IX["obi1"]], 0)
        self.assertLess(xs[IX["obi1"]], 0)
        self.assertEqual(xb[IX["bias"]], 1.0)

    async def test_05_fill_and_post_fill_labels_train_models(self):
        bot, m, clock = self._bot()
        pr = bot.predictor
        md = bot.md
        md.update(D("80000.0"), D("80000.8"), D("10.0"), D("10.0"), clock.t)
        pr.on_tick(md, bot.ledger, clock.t)
        targets = bot.engine.generate_ladder_quotes(m, md, bot.ledger, clock.t, False, False)
        pr.register_quotes(targets, md, bot.ledger, clock.t)
        self.assertGreater(len(pr._pend_fill), 0)
        n0 = pr.m_fill.n
        bid = [t for t in targets if t.side == B][0]
        pr.on_fill(B, bid.pair_index, bid.price, md, bot.ledger, clock.t + 0.3)
        self.assertGreater(pr.m_fill.n, n0, "fill label=1 must train fill model")
        # mid falls after our bid fills -> adverse label
        for dt in (1.0, 2.2, 5.3):
            md.update(D("79990.0"), D("79990.8"), D("10.0"), D("10.0"), clock.t + dt)
            pr.on_tick(md, bot.ledger, clock.t + dt)
        self.assertGreaterEqual(pr.m_adv.n, 1)
        self.assertGreaterEqual(pr.m_d2.n, 1)
        self.assertGreaterEqual(pr.m_d5.n, 1)
        # unfilled samples expire -> label 0
        n1 = pr.m_fill.n
        pr.on_tick(md, bot.ledger, clock.t + 20.0)
        self.assertGreaterEqual(pr.m_fill.n, n1)

    async def test_06_adverse_veto_blocks_toxic_quote(self):
        bot, m, clock = self._bot(MIN_EV_BPS="0.0")
        md = bot.md
        md.update(D("80000.0"), D("80000.8"), D("10.0"), D("10.0"), clock.t)
        base = bot.engine.generate_ladder_quotes(m, md, bot.ledger, clock.t, False, False)
        self.assertTrue(any(q.side == B for q in base))
        pr = bot.predictor
        # force a fully-warmed, very pessimistic model: every BUY fill is predicted adverse by -6bps
        for mdl in (pr.m_d2, pr.m_d5):
            mdl.n = 10_000
            mdl.w = [0.0] * NF
            mdl.w[IX["bias"]] = -6.0
        pr.m_adv.n = 10_000
        pr.m_adv.w = [0.0] * NF
        pr.m_adv.w[IX["bias"]] = 6.0
        pr.m_fill.n = 10_000
        clock.t += 1.0
        md.update(D("80000.0"), D("80000.8"), D("10.0"), D("10.0"), clock.t)
        after = bot.engine.generate_ladder_quotes(m, md, bot.ledger, clock.t, False, False)
        self.assertFalse(any(q.side == B and not q.is_exit_quote for q in after),
                         "toxic prediction must withhold adding quotes")

    async def test_07_predictive_exit_on_bad_hold_outlook(self):
        bot, m, clock = self._bot(STRESS_LOSS_BPS="50", EMERGENCY_TAKER_LOSS_BPS="50")
        md = bot.md
        from orders import Order
        md.update(D("80000.0"), D("80000.8"), D("10.0"), D("10.0"), clock.t)
        q = bot.engine.generate_ladder_quotes(m, md, bot.ledger, clock.t, False, False)
        b = [x for x in q if x.side == B][0]
        bot._on_fill(B, b.qty, b.price, Order("b1", 0, B, b.price, b.qty, b.qty, 0, clock.t, clock.t, quote_mid=D("80000.4")))
        self.assertGreater(bot.ledger.position, 0)
        pr = bot.predictor
        for mdl in (pr.m_hold, pr.m_hloss):
            mdl.n = 10_000
        pr.m_hold.w = [0.0] * NF; pr.m_hold.w[IX["bias"]] = -9.0
        pr.m_hloss.w = [0.0] * NF; pr.m_hloss.w[IX["bias"]] = 9.0
        clock.t += 1.0
        md.update(D("79999.0"), D("79999.8"), D("10.0"), D("10.0"), clock.t)
        out = bot.engine.generate_ladder_quotes(m, md, bot.ledger, clock.t, False, False)
        sells = [x for x in out if x.side == S and x.is_exit_quote]
        self.assertTrue(sells and sells[0].is_taker, "predicted large loss must trigger a predictive taker exit")

    async def test_08_good_outlook_does_not_force_exit(self):
        bot, m, clock = self._bot()
        md = bot.md
        md.update(D("80000.0"), D("80000.8"), D("10.0"), D("10.0"), clock.t)
        pr = bot.predictor
        pr.m_hold.n = 10_000; pr.m_hloss.n = 10_000
        pr.m_hold.w = [0.0] * NF; pr.m_hold.w[IX["bias"]] = 3.0
        pr.m_hloss.w = [0.0] * NF; pr.m_hloss.w[IX["bias"]] = -6.0
        taker, bad, good = bot.engine._pred_hold_flags(BUY, md, bot.ledger, clock.t, D("-0.5"), md.mid)
        self.assertFalse(taker); self.assertFalse(bad); self.assertTrue(good)

    async def test_09_disabled_predictor_matches_legacy(self):
        bot, m, clock = self._bot(ENABLE_PREDICTOR="0")
        md = bot.md
        md.update(D("80000.0"), D("80000.8"), D("10.0"), D("10.0"), clock.t)
        ev, p = bot.engine._pred_ev(B, D("80000.0"), D("0.5"), 0.4, D("0.2"), D("0.1"), D("0"), D("0"), md, bot.ledger, clock.t)
        self.assertEqual(p, 0.4)
        self.assertEqual(ev, D("0.4") * (D("0.5") + D("0.1") - D("0.2")))

    async def test_10_full_loop_runs_with_predictor(self):
        bot, s, clock = sim.make(EXTRA_LEVELS=1, ORDER_USD=20, MAX_POSITION_USD=100)
        for i in range(30):
            o = 80000.0 + (i % 5) * 2.0
            await sim.step(bot, s, clock, str(o), str(o + 0.8), bsz="10", asz="10")
        self.assertGreater(bot.predictor.m_hold.n, 0, "hold model trains from unconditional samples")
        self.assertIn("PRED", bot.predictor.summary())


if __name__ == "__main__":
    unittest.main()
