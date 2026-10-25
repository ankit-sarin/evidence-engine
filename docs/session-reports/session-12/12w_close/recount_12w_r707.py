"""12w-CLOSE item 0 — the R510 recount with the "L (12w additions, R707)" table, from the plan. Read-only.
usage, from the repository root:
    recount_12w_r707.py <plan at d7bafc4> <plan at 8b2c388> <plan at 1e15b50> <plan at 8659035> docs/plan/ENGINE_REFACTOR_PLAN.md
  (the first four files are `git show <commit>:…` of the plan, each checked by its sha256: the first three
  by the scripts below, the fourth here)
Writes recount_12w_r707.json beside itself and prints it.
Baseline: 12v_U37A/recount_12v_r694.py run on the plan at 8659035 — the last plan before this table —
which must reproduce its committed JSON byte for byte, and through it the chain back to
recount_12l_r624.py (272, 275, 276, 278, 279). It writes its own JSON beside itself, so run this script in
a scratch clone before a commit (R673(b)).
Delta, read from the given plan: every row of the "L (12w additions, R707)" table enters with the leading
digit of its Class cell and the first package its Owning-package cell names (R707(e): INT-w12-1, Class 2,
P1). No class move. The fix_units field is the baseline script's unit list, unchanged (35); the dated
lines count 37, done 19 (R701(c), R707(d)). R510 for session 12w: rows in 1, rows out 0, no class move."""
import contextlib, hashlib, io, json, os, re, sys
PRIOR_12R, PRIOR_12S, PRIOR_12V, PRIOR, PLAN = sys.argv[1:6]; D = "docs/session-reports/session-12"
PRIOR_SHA256 = "af7de56383ef8608bb2250d94e7cf73d17c024e34ea0e909200d33a5bdb5e1b4"   # the plan at 8659035
assert hashlib.sha256(open(PRIOR, "rb").read()).hexdigest() == PRIOR_SHA256, "the baseline file is not the plan at 8659035"
BASE = f"{D}/12v_U37A/recount_12v_r694.py"; BASE_JSON = f"{D}/12v_U37A/recount_12v_r694.json"
committed = open(BASE_JSON, encoding="utf-8").read()
g = {"__file__": os.path.abspath(BASE)}; argv = sys.argv; sys.argv = ["recount_12v_r694.py", PRIOR_12R, PRIOR_12S, PRIOR_12V, PRIOR]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    try: exec(open(BASE, encoding="utf-8").read(), g)
    except SystemExit as e: assert e.code == 0, "recount_12v_r694.py no longer reproduces"
sys.argv = argv
assert open(BASE_JSON, encoding="utf-8").read() == committed, "recount_12v_r694.py rewrote its committed JSON differently"
assert buf.getvalue() == committed, "recount_12v_r694.py's output is not its committed JSON"
base = json.loads(buf.getvalue())
assert base["now"] == {"rows": 279, "class_1": 94, "class_2": 54, "class_3": 131, "without_class": 0}
own, pk, cells, fix_units = dict(g["own"]), dict(g["pk"]), g["cells"], g["fix_units"]

text = open(PLAN, encoding="utf-8").read()
step2 = text[text.index("\n### L. "):text.index("\n### Not engine defects")]
TABLE = "**L (12w additions, R707).**"
assert step2.count(TABLE) == 1, "the 12w table is not in section L once"
add = step2.split(TABLE)[1].split("**The pre-tag list, recounted 2026-10-25 (12w-CLOSE")[0]
opened = []
for line in add.split("\n"):
    if line.startswith("| INT-"):
        c = cells(line); assert c[0] not in own
        o = {"id": c[0], "class": c[2][0], "package": re.search(r"P\d", c[3]).group(0),
             "unit": (re.search(r"U\d\d", c[3]) or [None])[0], "ruling": c[4]}
        opened.append(o); own[o["id"]] = o["class"]; pk[o["id"]] = o["package"]

from collections import Counter
per = Counter(own.values())
now = {"rows": len(own), "class_1": per["1"], "class_2": per["2"], "class_3": per["3"], "without_class": per["none"]}
table = {}
for rid, c in own.items(): table.setdefault(pk[rid], Counter())[c] += 1
by_package = {p: {"class_1": t["1"], "class_2": t["2"], "class_3": t["3"], "rows": sum(t.values())} for p, t in sorted(table.items())}
expected = {"rows": 280, "class_1": 94, "class_2": 55, "class_3": 131, "without_class": 0,
            "rows_opened": ["INT-w12-1"], "P1": {"class_1": 25, "class_2": 24, "class_3": 12, "rows": 61},
            "P6_rows": 11, "P7_rows": 117, "fix_units": 35}
holds = (now == {k: expected[k] for k in ("rows", "class_1", "class_2", "class_3", "without_class")}
         and [o["id"] for o in opened] == expected["rows_opened"]
         and [(o["class"], o["package"], o["unit"]) for o in opened] == [("2", "P1", "U39")]
         and by_package["P1"] == expected["P1"] and by_package["P6"]["rows"] == expected["P6_rows"]
         and by_package["P7"]["rows"] == expected["P7_rows"] and len(fix_units) == expected["fix_units"])
out = json.dumps({"baseline_12v_u37b": base["now"], "baseline_file_sha256": PRIOR_SHA256,
                  "rows_opened": {"count": len(opened), "rows": opened}, "class_moves": [], "now": now,
                  "r510_session_12w": {"rows_in": len(opened), "rows_in_ids": [o["id"] for o in opened], "rows_out": 0,
                                       "class_moves": 0},
                  "by_package_and_class": {"whole_list": by_package}, "fix_units": len(fix_units),
                  "fix_units_dated_lines": {"units": 37, "done": 19},
                  "expected": expected, "expected_holds": holds}, indent=1, sort_keys=True, ensure_ascii=False)
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "recount_12w_r707.json"), "w", encoding="utf-8") as f:
    f.write(out + "\n")
print(out)
sys.exit(0 if holds else 1)
