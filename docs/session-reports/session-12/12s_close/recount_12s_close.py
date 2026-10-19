"""12s-CLOSE — the R510 recount at the session-12s closeout, from the plan. Read-only.
usage, from the repository root:
    recount_12s_close.py <plan at d7bafc4> <plan at 8b2c388> docs/plan/ENGINE_REFACTOR_PLAN.md
  (the first two files are `git show d7bafc4:…` and `git show 8b2c388:…` of the plan, each checked by
  its sha256: the first by recount_12r_close.py, the second below)
Writes recount_12s_close.json beside itself and prints it.
Baseline: 12r_close/recount_12r_close.py run on the plan at 8b2c388 — the last plan before this
closeout's table, on which its bounds and assertions hold (the chain form of R672(f)). It must
reproduce its committed JSON byte for byte, and through it the chain back to recount_12l_r624.py
(272, 275, 276). It writes its own JSON beside itself, so run this script in a scratch clone before a
commit (R673(b)).
Deltas, read from the closeout's plan: every row of the "L (12s additions, R675, R677)" table enters
with the leading digit of its Class cell and the first package its Owning-package cell names
(R675(e): INT-s12-1, Class 2, P1; R677(d): INT-s12-2, Class 2, P6); INT-i12-12's class moves 1 → 2 by
the dated note in its row (R677(b): "the class cell not edited; the recount reads the note"), its
package unchanged (P1). P1's fix units stay 35: INT-s12-1's unit is assigned at its Phase A.
R510 for the session: rows in 2, rows out 0, one class move."""
import contextlib, hashlib, io, json, os, re, sys
PRIOR_12R, PRIOR, PLAN = sys.argv[1], sys.argv[2], sys.argv[3]; D = "docs/session-reports/session-12"
PRIOR_SHA256 = "95951ec04d317afb32fe33dbf9dd84b437121b5ef35883885cc419ffd856f709"   # the plan at 8b2c388
assert hashlib.sha256(open(PRIOR, "rb").read()).hexdigest() == PRIOR_SHA256, "the baseline file is not the plan at 8b2c388"
BASE = f"{D}/12r_close/recount_12r_close.py"; BASE_JSON = f"{D}/12r_close/recount_12r_close.json"
committed = open(BASE_JSON, encoding="utf-8").read()
g = {"__file__": os.path.abspath(BASE)}; argv = sys.argv; sys.argv = ["recount_12r_close.py", PRIOR_12R, PRIOR]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    try: exec(open(BASE, encoding="utf-8").read(), g)
    except SystemExit as e: assert e.code == 0, "recount_12r_close.py no longer reproduces"
sys.argv = argv
assert open(BASE_JSON, encoding="utf-8").read() == committed, "recount_12r_close.py rewrote its committed JSON differently"
assert buf.getvalue() == committed, "recount_12r_close.py's output is not its committed JSON"
base = json.loads(buf.getvalue())
assert base["now"] == {"rows": 276, "class_1": 95, "class_2": 50, "class_3": 131, "without_class": 0}
own, pk, cells, fix_units = dict(g["own"]), dict(g["pk"]), g["cells"], g["fix_units"]

text = open(PLAN, encoding="utf-8").read()
step2 = text[text.index("\n### L. "):text.index("\n### Not engine defects")]
TABLE = "**L (12s additions, R675, R677).**"
assert step2.count(TABLE) == 1, "the 12s table is not in section L once"
add = step2.split(TABLE)[1].split("**The pre-tag list, recounted 2026-10-19 (12s-CLOSE")[0]
opened = []
for line in add.split("\n"):
    if line.startswith("| INT-"):
        c = cells(line); assert c[0] not in own
        o = {"id": c[0], "class": c[2][0], "package": re.search(r"P\d", c[3]).group(0),
             "unit": (re.search(r"U\d\d", c[3]) or [None])[0], "ruling": c[4]}
        opened.append(o); own[o["id"]] = o["class"]; pk[o["id"]] = o["package"]
rows = [l for l in step2.split("\n") if l.startswith("| INT-i12-12 |")]
assert len(rows) == 1, f"{len(rows)} INT-i12-12 rows in section L"
NOTE = "*Note 2026-10-19 (12s-CLOSE, R677(b)):* moves from Class 1 to Class 2"
assert NOTE in rows[0], "INT-i12-12's moving note is not in its row"
assert cells(rows[0])[2] == "1", "INT-i12-12's class cell was edited"
before = {"class": own["INT-i12-12"], "package": pk["INT-i12-12"]}
assert before == {"class": "1", "package": "P1"}, before
own["INT-i12-12"] = "2"
moves = [{"id": "INT-i12-12", "from": before, "to": {"class": "2", "package": "P1"}, "ruling": "R677(b)"}]

from collections import Counter
per = Counter(own.values())
now = {"rows": len(own), "class_1": per["1"], "class_2": per["2"], "class_3": per["3"], "without_class": per["none"]}
table = {}
for rid, c in own.items(): table.setdefault(pk[rid], Counter())[c] += 1
by_package = {p: {"class_1": t["1"], "class_2": t["2"], "class_3": t["3"], "rows": sum(t.values())} for p, t in sorted(table.items())}
expected = {"rows": 278, "class_1": 94, "class_2": 53, "class_3": 131, "without_class": 0,
            "rows_opened": ["INT-s12-1", "INT-s12-2"], "P1": {"class_1": 25, "class_2": 22, "class_3": 12, "rows": 59},
            "P6_rows": 11, "P7_rows": 117, "fix_units": 35}
holds = (now == {k: expected[k] for k in ("rows", "class_1", "class_2", "class_3", "without_class")}
         and [o["id"] for o in opened] == expected["rows_opened"]
         and [(o["class"], o["package"], o["unit"]) for o in opened] == [("2", "P1", None), ("2", "P6", None)]
         and by_package["P1"] == expected["P1"] and by_package["P6"]["rows"] == expected["P6_rows"]
         and by_package["P7"]["rows"] == expected["P7_rows"] and len(fix_units) == expected["fix_units"])
out = json.dumps({"baseline_12r_close": base["now"], "baseline_file_sha256": PRIOR_SHA256,
                  "rows_opened": {"count": len(opened), "rows": opened}, "class_moves": moves, "now": now,
                  "r510_session_12s": {"rows_in": len(opened), "rows_in_ids": [o["id"] for o in opened], "rows_out": 0,
                                       "class_moves": len(moves)},
                  "by_package_and_class": {"whole_list": by_package}, "fix_units": len(fix_units),
                  "expected": expected, "expected_holds": holds}, indent=1, sort_keys=True, ensure_ascii=False)
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "recount_12s_close.json"), "w", encoding="utf-8") as f:
    f.write(out + "\n")
print(out)
sys.exit(0 if holds else 1)
