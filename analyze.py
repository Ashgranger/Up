#!/usr/bin/env python3
"""Analyse fill journals (fills_*.jsonl) written by the bot.

    python analyze.py fills_live_HOOD-USD.jsonl [more files...] [--h 5] [--min-effect 0.5]

Answers "is there a real edge yet, and how many more fills do I need?". For every slice it prints
n, mean markout (bps, + = good for us, measured from the FILL PRICE to the mid h seconds later)
and a 95% confidence interval. A slice only counts as significant when its whole interval sits on
one side of zero - with n=10 almost nothing will, and that's the honest answer, not a bug.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict


def load(paths):
    fills, marks = {}, defaultdict(dict)
    for path in paths:
        with open(path) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = (path, r.get("fid"))
                if r.get("type") == "markout":
                    marks[key][r["h"]] = r["bps"]
                elif r.get("type") == "fill" or "side" in r:      # old-format lines have no "type"
                    r["_key"] = key
                    fills[key] = r
    return fills, marks


def stats(xs):
    n = len(xs)
    if n == 0:
        return 0, 0.0, 0.0, 0.0
    mean = sum(xs) / n
    sd = math.sqrt(sum((x - mean) ** 2 for x in xs) / (n - 1)) if n > 1 else 0.0
    se = sd / math.sqrt(n) if n > 0 else 0.0
    return n, mean, sd, se


def row(label, xs):
    n, mean, sd, se = stats(xs)
    if n < 2:
        return f"  {label:<34} n={n:<4}  (too few)"
    lo, hi = mean - 1.96 * se, mean + 1.96 * se
    flag = "GOOD" if lo > 0 else ("BAD " if hi < 0 else "    ")
    return f"  {label:<34} n={n:<4} mean={mean:+6.2f}bps  95%CI[{lo:+6.2f},{hi:+6.2f}]  sd={sd:5.2f}  {flag}"


def pressure_against(f):
    """Book pressure pointing at the side we just filled on: a BUY is endangered by selling pressure
    (negative imbalance), a SELL by buying pressure (positive). Positive result = against us."""
    imb = f.get("imbalance") or 0.0
    return -imb if f["side"] == "BUY" else imb


def bucket(v, cuts, names):
    for c, nm in zip(cuts, names):
        if v < c:
            return nm
    return names[-1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--h", type=float, default=5.0, help="markout horizon (s) to slice on")
    ap.add_argument("--min-effect", type=float, default=0.5, help="smallest edge (bps) you care to detect")
    a = ap.parse_args()

    fills, marks = load(a.files)
    if not fills:
        sys.exit("no fills found")
    rows = []
    for key, f in fills.items():
        m = marks.get(key, {}).get(a.h)
        if m is not None:
            rows.append((f, m))
    print(f"{len(fills)} fills, {len(rows)} with a {a.h:g}s markout\n")
    if len(rows) < 2:
        sys.exit("not enough matured markouts yet - keep collecting (and check MARKOUT_HORIZONS_S includes %g)" % a.h)

    print("ALL FILLS, by horizon")
    hs = sorted({h for d in marks.values() for h in d})
    for h in hs:
        print(row(f"markout @ {h:g}s", [d[h] for k, d in marks.items() if h in d and k in fills]))
    print(row("edge at fill (vs mid, bps)", [float(f.get("edge_bps", 0)) for f, _ in rows]))

    def group(title, keyfn):
        print(f"\n{title}  (markout @ {a.h:g}s)")
        g = defaultdict(list)
        for f, m in rows:
            g[keyfn(f)].append(m)
        for k in sorted(g, key=str):
            print(row(str(k), g[k]))

    group("BY RUN TAG", lambda f: f.get("tag", "untagged"))
    group("BY SIDE", lambda f: f["side"])
    group("BY LEVEL (0 = touch)", lambda f: f"level {f.get('level')}")
    group("BY ROLE", lambda f: f.get("role") or "?")
    group("BY BOOK PRESSURE AGAINST THE FILL SIDE",
          lambda f: bucket(pressure_against(f), (-0.3, 0.3), ("with us (<-0.3)", "neutral", "against us (>0.3)")))
    group("BY RECENT TREND (bps in window)",
          lambda f: bucket(abs(f.get("ret_bps") or 0), (1.0, 2.5), ("calm (<1)", "moving (1-2.5)", "fast (>2.5)")))

    n, mean, sd, se = stats([m for _, m in rows])
    need = math.ceil((1.96 * sd / a.min_effect) ** 2) if sd > 0 else 0
    print(f"\nSAMPLE SIZE: observed per-fill sd = {sd:.2f}bps.")
    print(f"To detect a {a.min_effect:g}bps edge at 95% confidence you need about {need} fills per setting "
          f"(you have {n}{' - enough' if n >= need else ', ' + str(need - n) + ' to go'}).")
    print("Slices are smaller than the total - the per-slice counts above are what limit what you can conclude.")
    print("Caution: markout is measured vs the mid, so it ignores the cost of actually exiting; treat a positive")
    print("number as 'no adverse selection detected', not 'guaranteed profit'.")


if __name__ == "__main__":
    main()
