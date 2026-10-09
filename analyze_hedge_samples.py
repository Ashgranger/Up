"""Usage: python analyze_hedge_samples.py hedge_samples.csv
Columns: ts, buy_hedge_bps (Arcus BUY -> Lighter SELL), sell_hedge_bps (Arcus SELL -> Lighter BUY); positive = COST, negative = gain.
Prints: basis (Lighter - Arcus), symmetric hedge cost per fill, glitch share, persistence, basis-change risk, break-even edge."""
import csv, statistics as st, sys
rows = list(csv.DictReader(open(sys.argv[1])))
ts = [float(r["ts"]) for r in rows]; buy = [float(r["buy_hedge_bps"]) for r in rows]; sell = [float(r["sell_hedge_bps"]) for r in rows]
ok = [i for i in range(len(ts)) if -40 < buy[i] < 30 and sell[i] < 40]
b = [(sell[i] - buy[i]) / 2 for i in ok]; c = [(sell[i] + buy[i]) / 2 for i in ok]
q = lambda x, p: sorted(x)[min(len(x) - 1, int(p * len(x)))]
print(f"samples {len(ts)} ({(ts[-1]-ts[0])/60:.0f} min), glitches dropped {len(ts)-len(ok)}")
print(f"basis  mean {st.mean(b):+.2f}  median {st.median(b):+.2f}  sd {st.pstdev(b):.2f} bps")
print(f"cost/hedge  mean {st.mean(c):.2f}  median {st.median(c):.2f}  p5 {q(c,.05):.2f}  p95 {q(c,.95):.2f} bps")
print(f"round trip (entry+exit hedge) ~ {2*st.mean(c):.2f} bps; Arcus edge from mid needed ~ {2*(st.mean(c)+0.2)+0.5:.2f} bps")
