"""12q-P12-2B item 0 — the R510 recount after R659(d), from the plan. Read-only.
usage, from the repository root: recount_12q_r659.py docs/plan/ENGINE_REFACTOR_PLAN.md
Writes recount_12q_r659.json beside itself and prints it.
Baseline: 12p_close/recount_12p_close.py run on the plan given (272 rows; it must still reproduce
its committed JSON byte for byte, and through it the chain back to recount_12l_r624.py).
Delta: every row of the "L (12q additions, R659)" table enters with the leading digit of its Class
cell and the first package its Owning-package cell names. R659 moves no class. The three rows are
in P7; no P1 row enters, so P1's fix units stay the package plan's (R612(m)) and no placed-step
exception is needed."""
import contextlib, io, json, os, re, sys
from collections import Counter
PLAN = sys.argv[1]; D = "docs/session-reports/session-12"
BASE = f"{D}/12p_close/recount_12p_close.py"; BASE_JSON = f"{D}/12p_close/recount_12p_close.json"
committed = open(BASE_JSON, encoding="utf-8").read()
g = {"__file__": os.path.abspath(BASE)}; argv = sys.argv; sys.argv = ["recount_12p_close.py", PLAN]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    try: exec(open(BASE, encoding="utf-8").read(), g)
    except SystemExit as e: assert e.code == 0, "recount_12p_close.py no longer reproduces"
sys.argv = argv
assert open(BASE_JSON, encoding="utf-8").read() == committed, "recount_12p_close.py rewrote its committed JSON differently"
assert buf.getvalue() == committed, "recount_12p_close.py's output is not its committed JSON"
base = json.loads(buf.getvalue())
assert base["now"] == {"rows": 272, "class_1": 95, "class_2": 50, "class_3": 127, "without_class": 0}
own, pk, cells, step2, fix_units = dict(g["own"]), dict(g["pk"]), g["cells"], g["step2"], g["fix_units"]
L = step2[step2.index("\n### L. "):step2.index("\n### Not engine defects")]
add = L.split("**L (12q additions, R659).**")[1].split("**The pre-tag list, recounted 2026-10-17 (12q-P12-2B")[0]
opened = []
for line in add.split("\n"):
    if line.startswith("| INT-"):
        c = cells(line); assert c[0] not in own
        o = {"id": c[0], "class": c[2][0], "package": re.search(r"P\d", c[3]).group(0),
             "unit": (re.search(r"U\d\d", c[3]) or [None])[0], "ruling": c[4]}
        opened.append(o); own[o["id"]] = o["class"]; pk[o["id"]] = o["package"]
per = Counter(own.values())
now = {"rows": len(own), "class_1": per["1"], "class_2": per["2"], "class_3": per["3"], "without_class": per["none"]}
table = {}
for rid, c in own.items(): table.setdefault(pk[rid], Counter())[c] += 1
by_package = {p: {"class_1": t["1"], "class_2": t["2"], "class_3": t["3"], "rows": sum(t.values())} for p, t in sorted(table.items())}
new_p1 = {o["id"]: o["unit"] for o in opened if o["package"] == "P1"}
# R659(d): 275 rows, 95 / 50 / 130, 0 without a class; three rows opened, all Class 3 in P7, none
# riding with U13; P1 unchanged at 58 rows (26 / 20 / 12) and 35 fix units; P7 116.
expected = {"rows": 275, "class_1": 95, "class_2": 50, "class_3": 130, "rows_opened": ["INT-q12-1", "INT-q12-2", "INT-q12-3"],
            "P1": {"class_1": 26, "class_2": 20, "class_3": 12, "rows": 58}, "P7_rows": 116, "fix_units": 35}
holds = (all(now[k] == expected[k] for k in ("rows", "class_1", "class_2", "class_3")) and now["without_class"] == 0
         and [o["id"] for o in opened] == expected["rows_opened"]
         and all(o["class"] == "3" and o["package"] == "P7" and o["unit"] is None and o["ruling"] == "R659(d)" for o in opened)
         and by_package["P1"] == expected["P1"] and by_package["P7"]["rows"] == expected["P7_rows"]
         and len(fix_units) == expected["fix_units"] and not new_p1)
by_class = Counter(o["class"] for o in opened)
out = json.dumps({"baseline_12p_close": base["now"], "rows_opened": {"count": len(opened), "class_1": by_class["1"], "class_2": by_class["2"], "class_3": by_class["3"], "rows": opened},
                  "class_moves": [], "now": now, "r510": {"rows_in": len(opened), "rows_in_opened": len(opened), "rows_in_existing_rows_entering": 0, "rows_out": 0},
                  "by_package_and_class": {"whole_list": by_package},
                  "P1_units": {"fix_units": len(fix_units), "new_P1_rows_and_the_unit_each_joins": new_p1},
                  "expected_by_R659d": expected, "expected_holds": holds}, indent=1, sort_keys=True, ensure_ascii=False)
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "recount_12q_r659.json"), "w", encoding="utf-8") as f:
    f.write(out + "\n")
print(out)
sys.exit(0 if holds else 1)
