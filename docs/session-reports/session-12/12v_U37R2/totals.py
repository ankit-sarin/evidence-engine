"""12v-U37R2 step 3b — per-case and pooled STATED-only rates, and R697(e)'s rule, from score.py's rows.
usage, from the repository root: totals.py <scores.json>   (stdout: totals JSON; the verdict line last)
scores.json is 12v_U37R/score.py's output over this run's send log (one row per send). Only `scored` rows
count. A send whose shape is `call_failed` is out of its arm's denominator (and said so); an
`unparseable` reply stays in the denominator and is not STATED-only.
R697(e), fixed before the sends:
  (i)   CLOSE INT-r12-1 on U37 if the new arm's pooled STATED-only rate is at most half the old arm's
        and no case has a higher rate under the new template than under the old;
  (iii) REVERT U37 if the new arm's pooled rate exceeds the old arm's;
  (ii)  RETAIN U37 on neutral evidence otherwise. Ties on the pooled rate are (ii).
Rates are compared as exact fractions."""
import collections, json, sys
from fractions import Fraction

rows = [r for r in json.load(open(sys.argv[1]))["rows"] if r["role"] == "scored"]
per = collections.defaultdict(lambda: {"old": [], "new": []})
for r in rows:
    per[r["case"]][r["arm"]].append(r["shape"])
cases, pooled = {}, {"old": [0, 0], "new": [0, 0]}
for c, arms in per.items():
    out = {}
    for arm, shapes in arms.items():
        n = sum(1 for s in shapes if s != "call_failed"); k = sum(1 for s in shapes if s == "stated_only")
        out[arm] = {"stated_only": k, "n": n, "failed_calls": len(shapes) - n, "shapes": shapes,
                    "rate": str(Fraction(k, n)) if n else None}
        pooled[arm][0] += k; pooled[arm][1] += n
    cases[c] = out
rate = {a: Fraction(k, n) for a, (k, n) in pooled.items()}
worse = [c for c, o in cases.items() if o["new"]["n"] and o["old"]["n"]
         and Fraction(o["new"]["stated_only"], o["new"]["n"]) > Fraction(o["old"]["stated_only"], o["old"]["n"])]
if rate["new"] > rate["old"]:
    verdict = "(iii) REVERT"
elif rate["new"] <= rate["old"] / 2 and not worse and rate["new"] != rate["old"]:
    verdict = "(i) CLOSE"
else:
    verdict = "(ii) RETAIN"
line = (f"VERDICT {verdict}: pooled STATED-only old {pooled['old'][0]}/{pooled['old'][1]}, new "
        f"{pooled['new'][0]}/{pooled['new'][1]}; new <= old/2: {rate['new'] <= rate['old'] / 2}; "
        f"cases with a higher new rate: {worse or 'none'}")
print(json.dumps({"cases": cases, "pooled": {a: {"stated_only": k, "n": n, "rate": str(Fraction(k, n))}
                                             for a, (k, n) in pooled.items()},
                  "cases_new_higher": worse, "verdict": verdict, "verdict_line": line}, indent=1, sort_keys=True))
print(line, file=sys.stderr)
