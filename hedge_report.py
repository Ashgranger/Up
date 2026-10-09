"""Usage: python hedge_report.py bot.log
Real hedge cost by execution style from the bot's own log (notional-weighted, bps of the Lighter mid at the moment the exposure appeared).
New format:  HEDGE_COST <style> <side> <qty> @ <px> | total=+x bps = spread +s + drift +d
Old format:  HEDGE_COST <style> <side> <qty> @ <px> | vs L_mid=+x bps  (spread only: the reference was taken AFTER the maker wait, so drift is missing)
Also prints maker fill statistics and, if present, the Arcus-only vs hedged P&L from the HEDGE status lines."""
import re, sys, collections
agg = collections.defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, 0, 0])   # notional, total*n, spread*n, drift*n, n, old_format_n
posts = fills = 0
last_pnl = None
for l in open(sys.argv[1], errors="ignore"):
    m = re.search(r"HEDGE_COST (\w+) (BUY|SELL) (\S+) @ (\S+) \| total=([+-][\d.]+)bps = spread ([+-][\d.]+) \+ drift ([+-][\d.]+)", l)
    if m:
        n = float(m[3]) * float(m[4]); a = agg[m[1]]
        a[0] += n; a[1] += float(m[5]) * n; a[2] += float(m[6]) * n; a[3] += float(m[7]) * n; a[4] += 1
        continue
    m = re.search(r"HEDGE_COST (\w+) (BUY|SELL) (\S+) @ (\S+) \| vs L_mid=([+-][\d.]+)bps", l)
    if m:
        n = float(m[3]) * float(m[4]); a = agg[m[1]]
        a[0] += n; a[1] += float(m[5]) * n; a[2] += float(m[5]) * n; a[4] += 1; a[5] += 1
        continue
    if "[HEDGE maker]" in l and "filled" in l:
        posts += 1
        f = re.search(r"filled ([\d.]+) / ([\d.]+)", l)
        if f and float(f[1]) > 0:
            fills += 1
    m = re.search(r"combined_pnl=\$(\S+) \(arcus \$(\S+)\)", l)
    if m:
        last_pnl = (float(m[1]), float(m[2]))
tn = tt = 0.0
for st, (n, t, s, d, k, old) in agg.items():
    note = "  (old log format: spread only, wait-drift not included)" if old == k else ""
    print(f"{st:6s} hedges={k:4d} notional=${n:9.0f}  total={t/n:+.2f}  spread={s/n:+.2f}  drift={d/n:+.2f} bps{note}")
    tn += n; tt += t
if tn:
    print(f"ALL    total={tt/tn:+.2f} bps   (break-even is about 0.3 bps; Arcus edge ~0.46, first-second drift ~0.14)")
if posts:
    print(f"maker posts={posts} filled={fills} ({fills/posts:.0%})")
if last_pnl:
    print(f"last status: Arcus-only P&L ${last_pnl[1]:+.4f}  vs  combined (Arcus + Lighter hedge) ${last_pnl[0]:+.4f}")
