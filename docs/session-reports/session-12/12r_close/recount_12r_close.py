"""12r-CLOSE — the R510 recount at the session-12r closeout, from the plan. Read-only.
usage, from the repository root:
    recount_12r_close.py <plan at d7bafc4> docs/plan/ENGINE_REFACTOR_PLAN.md
  (the first file is `git show d7bafc4:docs/plan/ENGINE_REFACTOR_PLAN.md`, checked by its sha256 below)
Writes recount_12r_close.json beside itself and prints it.
Baseline: 12q_close/recount_12q_close.py run on the plan at d7bafc4 — the file it was written for, before this
closeout's table (it asserts that no section-L table follows "L (12q additions, R659)", so it is run on its own
file, as 12q's recount ran 12p's on the pre-table file). It must reproduce its committed JSON byte for byte, and
through it the chain back to recount_12l_r624.py (272, then 275).
Deltas, read from the closeout's plan: every row of the "L (12r additions, R668, R671)" table enters with the
leading digit of its Class cell and the first package its Owning-package cell names (R671(b): INT-r12-1, Class 2,
P1); INT-h12-2's class and package move by the dated note in its row (R670(c), R671(a): "the class cell is not
edited; the recount reads the note"). P1's fix units stay 35: INT-r12-1's unit is assigned at its Phase A
(R671(b)). R510 for the session: rows in 1, rows out 0, one class move."""
import contextlib, hashlib, io, json, os, re, sys
PRIOR, PLAN = sys.argv[1], sys.argv[2]; D = "docs/session-reports/session-12"
PRIOR_SHA256 = "f2e4dc60d5dd4eb396083475d221a45e5600ccb91849181766a56de730cb9b8d"   # the plan at c748129 = d7bafc4
assert hashlib.sha256(open(PRIOR, "rb").read()).hexdigest() == PRIOR_SHA256, "the baseline file is not the plan at d7bafc4"
BASE = f"{D}/12q_close/recount_12q_close.py"; BASE_JSON = f"{D}/12q_close/recount_12q_close.json"
committed = open(BASE_JSON, encoding="utf-8").read()
g = {"__file__": os.path.abspath(BASE)}; argv = sys.argv; sys.argv = ["recount_12q_close.py", PRIOR]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    try: exec(open(BASE, encoding="utf-8").read(), g)
    except SystemExit as e: assert e.code == 0, "recount_12q_close.py no longer reproduces"
sys.argv = argv
assert open(BASE_JSON, encoding="utf-8").read() == committed, "recount_12q_close.py rewrote its committed JSON differently"
assert buf.getvalue() == committed, "recount_12q_close.py's output is not its committed JSON"
base = json.loads(buf.getvalue())
assert base["now"] == {"rows": 275, "class_1": 95, "class_2": 50, "class_3": 130, "without_class": 0}
inner = g["g"]                                   # recount_12q_r659.py's globals, as 12q_close ran it
own, pk, cells, fix_units = dict(inner["own"]), dict(inner["pk"]), inner["cells"], inner["fix_units"]

text = open(PLAN, encoding="utf-8").read()
step2 = text[text.index("\n### L. "):text.index("\n### Not engine defects")]
TABLE = "**L (12r additions, R668, R671).**"
assert step2.count(TABLE) == 1, "the 12r table is not in section L once"
add = step2.split(TABLE)[1].split("**The pre-tag list, recounted 2026-10-19 (12r-CLOSE")[0]
opened = []
for line in add.split("\n"):
    if line.startswith("| INT-"):
        c = cells(line); assert c[0] not in own
        o = {"id": c[0], "class": c[2][0], "package": re.search(r"P\d", c[3]).group(0),
             "unit": (re.search(r"U\d\d", c[3]) or [None])[0], "ruling": c[4]}
        opened.append(o); own[o["id"]] = o["class"]; pk[o["id"]] = o["package"]
rows = [l for l in step2.split("\n") if l.startswith("| INT-h12-2 |")]
assert len(rows) == 1, f"{len(rows)} INT-h12-2 rows in section L"
NOTE = "*Note 2026-10-19 (12r-CLOSE, R670(c), R671(a)):* moves from Class 2 to Class 3 and from P1 to P7"
assert NOTE in rows[0], "INT-h12-2's moving note is not in its row"
assert cells(rows[0])[2] == "2", "INT-h12-2's class cell was edited"
before = {"class": own["INT-h12-2"], "package": pk["INT-h12-2"]}
assert before == {"class": "2", "package": "P1"}, before
own["INT-h12-2"], pk["INT-h12-2"] = "3", "P7"
moves = [{"id": "INT-h12-2", "from": before, "to": {"class": "3", "package": "P7"}, "ruling": "R670(c), R671(a)"}]

from collections import Counter
per = Counter(own.values())
now = {"rows": len(own), "class_1": per["1"], "class_2": per["2"], "class_3": per["3"], "without_class": per["none"]}
table = {}
for rid, c in own.items(): table.setdefault(pk[rid], Counter())[c] += 1
by_package = {p: {"class_1": t["1"], "class_2": t["2"], "class_3": t["3"], "rows": sum(t.values())} for p, t in sorted(table.items())}
expected = {"rows": 276, "class_1": 95, "class_2": 50, "class_3": 131, "without_class": 0,
            "rows_opened": ["INT-r12-1"], "P1": {"class_1": 26, "class_2": 20, "class_3": 12, "rows": 58},
            "P7_rows": 117, "fix_units": 35}
holds = (now == {k: expected[k] for k in ("rows", "class_1", "class_2", "class_3", "without_class")}
         and [o["id"] for o in opened] == expected["rows_opened"]
         and all(o["class"] == "2" and o["package"] == "P1" and o["unit"] is None for o in opened)
         and by_package["P1"] == expected["P1"] and by_package["P7"]["rows"] == expected["P7_rows"]
         and len(fix_units) == expected["fix_units"])
out = json.dumps({"baseline_12q_close": base["now"], "baseline_file_sha256": PRIOR_SHA256,
                  "rows_opened": {"count": len(opened), "rows": opened}, "class_moves": moves, "now": now,
                  "r510_session_12r": {"rows_in": len(opened), "rows_in_ids": [o["id"] for o in opened], "rows_out": 0,
                                       "class_moves": len(moves)},
                  "by_package_and_class": {"whole_list": by_package}, "fix_units": len(fix_units),
                  "expected": expected, "expected_holds": holds}, indent=1, sort_keys=True, ensure_ascii=False)
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "recount_12r_close.json"), "w", encoding="utf-8") as f:
    f.write(out + "\n")
print(out)
sys.exit(0 if holds else 1)
