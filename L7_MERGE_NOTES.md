# Level 7 features merged into the second bot

New: learner.py (OnlineLearner, persisted to LEARNING_STATE_PATH). Rewritten: strategy.py (L7 model inside Snapshot->Plan).
Changed: config.py, market.py (trades/OBI/TFI/regimes), ledger.py (time-weighted + per-side markouts, learner hooks),
orders.py (cancel_side, recently-closed orphan guard, cancel_all(force)), bot.py (trades channel, burst+sweep guards,
learner tick/save, REGIME/LEARN status lines). Removed: engine.py (dead code, superseded by strategy.py).
Kept from bot 2: WS reconnect, dead man's switch, reduce ladder, continue-add, depth imbalance, journal + analyze.py.

Behaviour changes to know about
- MAX_HOLD_S no longer forces a stress exit; the exit profit floor decays 1.0 -> 0.8 -> 0.2bps (0.5x / 1.5x of MAX_HOLD_S).
  Stress (exit at touch) is now only STRESS_LOSS_BPS underwater or a session halt.
- Add quotes must pass the EV filter (MIN_EV_BPS); AGGRESSIVE_TOUCH bypasses it for L0.
- Sweep guard (SWEEP_GUARD_*) and burst guard cancel only ADD orders on that side; exits stay live.
- Set ENABLE_ONLINE_LEARNING=0 to freeze all parameters at the .env values.
- .env.example has your L7 values (SESSION_MAX_LOSS_USD=2.0, MAX_ACTIONS_PER_MIN=1060, QUOTE_OUTSIDE_RTH=1, ...).
  Credentials are NOT included: copy to .env and fill in.
Tests: python test_bot.py ; python test_level7.py
