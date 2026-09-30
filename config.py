"""All tunables live here (loaded from environment / .env)."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal

from utils import Fatal

ENVS = {
    "mainnet": {"rest": "https://api.arcus.xyz", "ws": "wss://api.arcus.xyz/v1/ws"},
    "testnet": {"rest": "https://api.testnet.arcus.xyz", "ws": "wss://api.testnet.arcus.xyz/v1/ws"},
}


def _e(name: str, default):
    v = os.getenv(name)
    return default if v is None or v.strip() == "" else v.strip()


def _d(name: str, default: str) -> Decimal:
    return Decimal(str(_e(name, default)))


def _b(name: str, default: str) -> bool:
    return str(_e(name, default)).lower() in ("1", "true", "yes", "y", "on")


@dataclass
class Config:
    # --- connection -------------------------------------------------------- #
    env_name: str
    address: str
    signing_key: str
    account_index: int
    market: str
    dry_run: bool                 # True = paper trading: real data, simulated fills, no orders sent

    # --- sizing / inventory ------------------------------------------------ #
    order_usd: Decimal            # size of each quote (USD notional)
    max_position_usd: Decimal     # hard cap on |position| (worst case incl. the quote that would fill)
    skew_bps: Decimal             # reservation-price shift at full inventory

    # --- ladder: extra quote levels beyond the touch ------------------------ #
    # The touch-level quote (above) always exists alone. These add EXTRA resting orders further
    # from fair value - on the ADDING side, further-out add levels (never while that side is
    # guard-blocked); on the REDUCING/exit side, further-out SCALE-OUT levels at progressively
    # better profit targets instead of dumping the whole position on one exit order. Every level,
    # add or reduce, is a real live order working the same edge/exit-floor logic as the touch quote
    # - "every ladder captures spread like the first pair does". Cumulative exposure (add side) or
    # remaining position (reduce side) is tracked level-by-level so neither the position cap nor
    # the actual position size can be exceeded.
    extra_levels: int             # additional levels per side beyond the touch quote (0 = off)
    level_spacing_bps: Decimal    # each extra level sits this much further from fair value than the last
    level_size_mult: Decimal      # add level i size = touch size * mult**i; reduce level i takes
                                   # min(remaining position, touch size * mult**i)
    continue_add_after_reduce: bool  # when the reduce ladder (above) doesn't need every EXTRA_LEVELS
                                   # slot to fully exit the current position, use what's left for
                                   # fresh 'add' quotes instead of leaving it idle - so the bot keeps
                                   # capturing spread while a bad fill's exit is still working,
                                   # without ever repricing or resizing that exit. Position-capped
                                   # like any other add level.

    # --- edge (how far from fair value we quote) --------------------------- #
    min_edge_bps: Decimal         # minimum distance from fair value, each side
    max_edge_bps: Decimal
    maker_fee_bps: Decimal        # Base tier = 0 (verify on your account); rebates would be negative
    vol_k: Decimal                # edge += vol_k * short-window range (bps)
    tox_mult: Decimal             # edge += tox_mult * (negative avg markout, bps)
    use_micro: bool               # fair value = size-weighted micro price instead of plain mid
    penny: bool                   # step 1 tick inside the touch when spread >= 2 ticks
    aggressive_touch: bool        # ADD-role touch quote joins the book's actual best bid/ask outright,
                                   # ignoring min_edge_bps (reduce/exit-floor protection still applies).
                                   # Trades edge for fill probability - the ladder levels behind it are
                                   # what's still expected to actually capture spread; see below.
    touch_min_requote_s: float     # touch level (index 0) re-quotes at this cadence instead of
                                   # min_requote_s, so it can track the book as fast as it pushes
                                   # updates. Ladder levels keep min_requote_s - they're not meant
                                   # to chase every tick, and doing so would burn the action budget
                                   # for no benefit (see MAX_ACTIONS_PER_MIN).

    # --- exits / stress ---------------------------------------------------- #
    exit_min_profit_bps: Decimal  # never quote the exit worse than cost + this (unless stressed)
    stress_loss_bps: Decimal      # position underwater by this much -> stop adding, exit at the touch
    max_hold_s: float             # position older than this -> same as stress

    # --- adverse-selection guards ------------------------------------------ #
    trend_window_s: float
    trend_pull_bps: Decimal       # move against the adding side by this in window -> pull that side
    trend_widen: Decimal          # partial: widen the exposed side by this * move
    trend_hold_s: float           # once trend pulls a side, keep it pulled at least this long
                                   # (damps flicker: without this, a side can cancel/re-place every
                                   # tick right at the threshold - that's most of the churn/rejects
                                   # in the 12:28-12:32 stretch of arcus_live_log.txt)
    use_depth_imbalance: bool      # widen/shrink the endangered ADD side using resting book depth
                                   # (l2OrderbookUpdates), not just realized price movement. This is
                                   # a LEADING complement to trend_widen above (which only reacts
                                   # after ret_bps has already moved): heavy bid-side depth predicts
                                   # price ticking up, which makes the ASK the side about to be
                                   # adversely selected - so it gets widened/shrunk on the pressure
                                   # itself. Falls back to doing nothing if the depth book is
                                   # unpopulated or stale (see MarketData.depth_imbalance) - it never
                                   # guesses from a book it can't trust.
    imbalance_levels: int          # how many price levels deep to sum bid/ask resting size over
    imbalance_widen_bps: Decimal   # extra edge added to the endangered side at full (100%) imbalance
    imbalance_size_cut: Decimal    # fraction the endangered side's size shrinks by at full imbalance
                                   # (0 = don't shrink size, only widen; 1 = can shrink to nothing)
    vol_window_s: float
    vol_pause_bps: Decimal        # range in window >= this -> pull adding sides
    jump_bps: Decimal             # single-update mid jump >= this -> pull adding sides briefly
    jump_cooldown_s: float
    burst_fills: int              # N same-side consecutive fills ...
    burst_window_s: float         # ... within this many seconds ...
    burst_cooldown_s: float       # ... -> pause that adding side
    sweep_guard_fills: int        # N same-side fills within sweep_guard_window_s -> emergency pull + cooldown
    sweep_guard_window_s: float
    markout_horizon_s: float      # measure mid this long after each fill
    markout_window: int           # number of recent fills used for the toxicity estimate
    markout_horizons_s: tuple     # data-collection horizons written to the journal (1s/5s/30s...)
    run_tag: str                  # label written on every journal line, to compare settings later

    # --- Level 4-7 quantitative models ------------------------------------- #
    min_ev_bps: Decimal           # a quote is only placed if its expected value (bps) clears this
    enable_adaptive_ev: bool      # False = skip the EV filter entirely
    ev_hysteresis_bps: Decimal    # an already-resting level only needs (min_ev - this) to stay up
    enable_orderbook_intel: bool  # fair value shifted by book imbalance (OBI) + trade flow (TFI),
                                   # plus asymmetric quote shading on the endangered side
    obi_alpha: Decimal            # fair-value shift per unit OBI, in half-spreads (also shading strength)
    tfi_beta: Decimal             # fair-value shift per unit TFI, in half-spreads
    fill_prob_kappa: Decimal      # P(fill) = exp(-kappa * distance_bps) in the EV model
    gamma_risk_aversion: Decimal  # extra inventory skew per bp of short-term vol
    enable_online_learning: bool  # adapt the parameters above (and edges/guards) from markouts
    regime_vol_threshold_bps: Decimal
    regime_flow_threshold: Decimal
    regime_toxic_threshold_bps: Decimal   # tox_bps >= this -> REGIME_D_TOXIC (wider, no extra add levels)
    regime_toxic_spread_mult: Decimal
    learning_state_path: str

    # --- inventory manager + taker cut (inventory.py) --------------------------- #
    enable_inventory_mgr: bool    # risk-scored exits: maker exit slides to the touch, adds blocked, size scaled
    inv_flow_bps: Decimal         # expected adverse move (bps) at flow_against = 1.0
    inv_vol_k: Decimal            # bps of expected loss per bp of short-term vol
    inv_max_give_bps: Decimal     # max the maker exit may give up below the profit floor at full risk
    inv_add_block_util: Decimal   # >= this share of MAX_POSITION_USD: no more adds in the loaded direction
    severe_hold_s: float          # latch time for the 'severe pressure' add-suppression (kills cancel churn)
    enable_taker_exit: bool       # allow IOC reduce-only cuts (taker fee) - strict conditions, see below
    taker_fee_bps: Decimal        # taker fee (0.022% = 2.2 bps); used in the cut-vs-hold expected-value test
    taker_slip_bps: Decimal       # IOC limit price is this far through the touch (bounded slippage)
    inv_cut_risk: Decimal         # risk score needed for a normal cut
    inv_cut_flow: Decimal         # flow_against needed for a normal cut
    inv_cut_min_saving_bps: Decimal   # expected loss must exceed cross cost by this much
    inv_cut_max_pnl_bps: Decimal  # normal cuts only when position PnL (bps) is below this (0 = only when losing)
    inv_cut_min_loss_bps: Decimal # stale-inventory cut needs at least this loss
    inv_hard_stop_bps: Decimal    # unconditional taker exit at this loss
    inv_emergency_util: Decimal   # >= this share of cap with flow against -> immediate cut
    inv_maker_wait_s: float       # a maker exit must have been working this long before a normal cut
    taker_cooldown_s: float
    taker_max_per_hour: int

    # --- risk -------------------------------------------------------------- #
    session_max_loss_usd: Decimal  # total PnL <= -this -> halt, work exit at touch, stop
    halt_exit: bool               # True: keep working a limit exit until flat before stopping

    # --- execution --------------------------------------------------------- #
    requote_bps: Decimal          # ADVANCING a quote needs a drift of at least this
    retreat_bps: Decimal          # RETREATING from the market is immediate above this drift
    min_requote_s: float
    max_actions_per_min: int
    loop_s: float
    heartbeat_s: float
    reconcile_s: float
    status_s: float
    stale_s: float
    max_market_spread_bps: Decimal
    max_oracle_dev_bps: Decimal
    quote_outside_rth: bool
    journal_path: str

    @classmethod
    def from_env(cls) -> "Config":
        env_name = str(_e("ARCUS_ENV", "testnet")).lower()
        if env_name not in ENVS:
            raise Fatal(f"ARCUS_ENV must be one of {list(ENVS)}")
        address = str(_e("ARCUS_WALLET_ADDRESS", ""))
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", address):
            raise Fatal("ARCUS_WALLET_ADDRESS must be your 0x master wallet address")
        key = str(_e("ARCUS_API_SIGNING_KEY", "")).removeprefix("0x")
        if not re.fullmatch(r"[0-9a-fA-F]{64}", key):
            raise Fatal("ARCUS_API_SIGNING_KEY must be the 64-hex Ed25519 private key")
        dry = _b("DRY_RUN", "1")
        market = str(_e("MARKET", "BTC-USD"))
        cfg = cls(
            env_name=env_name, address=address, signing_key=key,
            account_index=int(_e("ARCUS_ACCOUNT_INDEX", 0)), market=market, dry_run=dry,
            order_usd=_d("ORDER_USD", "30"),
            max_position_usd=_d("MAX_POSITION_USD", "60"),
            skew_bps=_d("SKEW_BPS", "3"),
            extra_levels=int(_e("EXTRA_LEVELS", 1)),
            level_spacing_bps=_d("LEVEL_SPACING_BPS", "4"),
            level_size_mult=_d("LEVEL_SIZE_MULT", "0.6"),
            continue_add_after_reduce=_b("CONTINUE_ADD_AFTER_REDUCE", "1"),
            min_edge_bps=_d("MIN_EDGE_BPS", "2"),
            max_edge_bps=_d("MAX_EDGE_BPS", "12"),
            maker_fee_bps=_d("MAKER_FEE_BPS", "0"),
            vol_k=_d("VOL_K", "0.5"),
            tox_mult=_d("TOX_MULT", "1"),
            use_micro=_b("USE_MICRO", "1"),
            penny=_b("PENNY", "1"),
            aggressive_touch=_b("AGGRESSIVE_TOUCH", "0"),
            touch_min_requote_s=float(_e("TOUCH_MIN_REQUOTE_S", 0.2)),
            exit_min_profit_bps=_d("EXIT_MIN_PROFIT_BPS", "0.5"),
            stress_loss_bps=_d("STRESS_LOSS_BPS", "4"),
            max_hold_s=float(_e("MAX_HOLD_S", 120)),
            trend_window_s=float(_e("TREND_WINDOW_S", 5)),
            trend_pull_bps=_d("TREND_PULL_BPS", "2.5"),
            trend_widen=_d("TREND_WIDEN", "1"),
            trend_hold_s=float(_e("TREND_HOLD_S", 3)),
            use_depth_imbalance=_b("USE_DEPTH_IMBALANCE", "1"),
            imbalance_levels=int(_e("IMBALANCE_LEVELS", 5)),
            imbalance_widen_bps=_d("IMBALANCE_WIDEN_BPS", "3"),
            imbalance_size_cut=_d("IMBALANCE_SIZE_CUT", "0.3"),
            vol_window_s=float(_e("VOL_WINDOW_S", 5)),
            vol_pause_bps=_d("VOL_PAUSE_BPS", "8"),
            jump_bps=_d("JUMP_BPS", "6"),
            jump_cooldown_s=float(_e("JUMP_COOLDOWN_S", 2)),
            burst_fills=int(_e("BURST_FILLS", 3)),
            burst_window_s=float(_e("BURST_WINDOW_S", 15)),
            burst_cooldown_s=float(_e("BURST_COOLDOWN_S", 20)),
            sweep_guard_fills=int(_e("SWEEP_GUARD_FILLS", 2)),
            sweep_guard_window_s=float(_e("SWEEP_GUARD_WINDOW_S", 1.0)),
            min_ev_bps=_d("MIN_EV_BPS", "0.2"),
            enable_adaptive_ev=_b("ENABLE_ADAPTIVE_EV", "1"),
            ev_hysteresis_bps=_d("EV_HYSTERESIS_BPS", "0.1"),
            enable_orderbook_intel=_b("ENABLE_ORDERBOOK_INTEL", "1"),
            obi_alpha=_d("OBI_ALPHA", "1.0"),
            tfi_beta=_d("TFI_BETA", "1.5"),
            fill_prob_kappa=_d("FILL_PROB_KAPPA", "0.25"),
            gamma_risk_aversion=_d("GAMMA_RISK_AVERSION", "0.1"),
            enable_online_learning=_b("ENABLE_ONLINE_LEARNING", "1"),
            regime_vol_threshold_bps=_d("REGIME_VOL_THRESHOLD_BPS", "5.0"),
            regime_flow_threshold=_d("REGIME_FLOW_THRESHOLD", "0.35"),
            regime_toxic_threshold_bps=_d("REGIME_TOXIC_THRESHOLD_BPS", "1.5"),
            regime_toxic_spread_mult=_d("REGIME_TOXIC_SPREAD_MULT", "1.5"),
            learning_state_path=str(_e("LEARNING_STATE_PATH", "learning_state.json")),
            enable_inventory_mgr=_b("ENABLE_INVENTORY_MGR", "1"),
            inv_flow_bps=_d("INV_FLOW_BPS", "3.0"),
            inv_vol_k=_d("INV_VOL_K", "0.5"),
            inv_max_give_bps=_d("INV_MAX_GIVE_BPS", "1.5"),
            inv_add_block_util=_d("INV_ADD_BLOCK_UTIL", "0.5"),
            severe_hold_s=float(_e("SEVERE_HOLD_S", 1.5)),
            enable_taker_exit=_b("ENABLE_TAKER_EXIT", "1"),
            taker_fee_bps=_d("TAKER_FEE_BPS", "2.2"),
            taker_slip_bps=_d("TAKER_SLIP_BPS", "2.0"),
            inv_cut_risk=_d("INV_CUT_RISK", "0.75"),
            inv_cut_flow=_d("INV_CUT_FLOW", "0.5"),
            inv_cut_min_saving_bps=_d("INV_CUT_MIN_SAVING_BPS", "0.5"),
            inv_cut_max_pnl_bps=_d("INV_CUT_MAX_PNL_BPS", "0.0"),
            inv_cut_min_loss_bps=_d("INV_CUT_MIN_LOSS_BPS", "2.0"),
            inv_hard_stop_bps=_d("INV_HARD_STOP_BPS", "15.0"),
            inv_emergency_util=_d("INV_EMERGENCY_UTIL", "0.95"),
            inv_maker_wait_s=float(_e("INV_MAKER_WAIT_S", 3.0)),
            taker_cooldown_s=float(_e("TAKER_COOLDOWN_S", 6.0)),
            taker_max_per_hour=int(_e("TAKER_MAX_PER_HOUR", 20)),
            markout_horizon_s=float(_e("MARKOUT_HORIZON_S", 5)),
            markout_window=int(_e("MARKOUT_WINDOW", 10)),
            markout_horizons_s=tuple(float(x) for x in str(_e("MARKOUT_HORIZONS_S", "1,5,30")).split(",") if x.strip()),
            run_tag=str(_e("RUN_TAG", "untagged")),
            session_max_loss_usd=_d("SESSION_MAX_LOSS_USD", "0.35"),
            halt_exit=_b("HALT_EXIT", "1"),
            requote_bps=_d("REQUOTE_BPS", "1"),
            retreat_bps=_d("RETREAT_BPS", "0.4"),
            min_requote_s=float(_e("MIN_REQUOTE_S", 2)),
            max_actions_per_min=int(_e("MAX_ACTIONS_PER_MIN", 40)),
            loop_s=float(_e("LOOP_S", 0.25)),
            heartbeat_s=float(_e("HEARTBEAT_S", 5)),
            reconcile_s=float(_e("RECONCILE_S", 5)),
            status_s=float(_e("STATUS_S", 15)),
            stale_s=float(_e("STALE_S", 15)),
            max_market_spread_bps=_d("MAX_MARKET_SPREAD_BPS", "30"),
            max_oracle_dev_bps=_d("MAX_ORACLE_DEV_BPS", "150"),
            quote_outside_rth=_b("QUOTE_OUTSIDE_RTH", "0"),
            journal_path=str(_e("JOURNAL_PATH", f"fills_{'paper' if dry else 'live'}_{market}.jsonl")),
        )
        if cfg.max_position_usd < cfg.order_usd:
            raise Fatal("MAX_POSITION_USD must be >= ORDER_USD")
        return cfg
