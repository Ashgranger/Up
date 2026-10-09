"""Usage: python hedge_report.py bot.log   -> real hedge cost by style (maker vs taker), notional-weighted, bps vs Lighter mid.
Negative = you gained. This is the number to compare with your Arcus edge."""
import re, sys, collections
agg = collections.defaultdict(lambda: [0.0, 0.0, 0])      # style -> [sum(cost_bps*notional), notional, n]
for l in open(sys.argv[1], errors="ignore"):
    m = re.search(r"HEDGE_COST (\w+) (BUY|SELL) (\S+) @ (\S+) \| vs L_mid=([+-]?[\d.]+)bps", l)
    if m:
        st, q, px, c = m[1], float(m[3]), float(m[4]), float(m[5])
        a = agg[st]; a[0] += c * q * px; a[1] += q * px; a[2] += 1
tot = [0.0, 0.0]
for st, (s, n, k) in agg.items():
    print(f"{st:6s} hedges={k:4d} notional=${n:9.0f} avg_cost={s/n:+.2f} bps")
    tot[0] += s; tot[1] += n
if tot[1]:
    print(f"ALL    avg_cost={tot[0]/tot[1]:+.2f} bps   (target 0.25-0.30; Arcus edge is ~0.46, adverse ~0.14 in the first second)")
