"""12l item 0 — the R510 recount after R613, from the plan. Read-only.
usage, from the repository root: recount_12l_r613.py docs/plan/ENGINE_REFACTOR_PLAN.md   (stdout: JSON)
Baseline: 12l_R612/recount_12l.py run on the plan given (249 rows; it must still reproduce).
Delta: every row of the "L (12l additions, R613)" table enters with the leading digit of its Class
cell and the first package its Owning-package cell names. R613 moves no class. A new P1 row joins
the unit its cell names, so P1's fix units stay the package plan's (R612(m))."""
import contextlib, io, json, re, sys
from collections import Counter
PLAN = sys.argv[1]; D = "docs/session-reports/session-12"
g = {}; argv = sys.argv; sys.argv = ["recount_12l.py", PLAN]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    try: exec(open(f"{D}/12l_R612/recount_12l.py", encoding="utf-8").read(), g)
    except SystemExit as e: assert e.code == 0, "recount_12l.py no longer reproduces"
sys.argv = argv
base = json.loads(buf.getvalue())
assert base["now"] == {"rows": 249, "class_1": 95, "class_2": 46, "class_3": 108, "without_class": 0}
own, pk, cells, step2, fix_units = dict(g["own"]), dict(g["pk"]), g["cells"], g["step2"], g["fix_units"]
L = step2[step2.index("\n### L. "):step2.index("\n### Not engine defects")]
add = L.split("**L (12l additions, R613).**")[1].split("**The pre-tag list, recounted 2026-10-11 (12l item 0, R613")[0]
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
unit_ids = {u[0] for u in fix_units}
new_p1 = {o["id"]: o["unit"] for o in opened if o["package"] == "P1"}
expected = {"rows": 251, "class_1": 95, "class_2": 46, "class_3": 110, "P1": {"rows": 50, "class_1": 26, "class_2": 16, "class_3": 8}, "P1_units": 35}
i5 = (all(now[k] == expected[k] for k in ("rows", "class_1", "class_2", "class_3")) and now["without_class"] == 0
      and by_package["P1"] == expected["P1"] and len(fix_units) == 35 and all(u in unit_ids for u in new_p1.values()))
by_class = Counter(o["class"] for o in opened)
print(json.dumps({"baseline_12l_R612": base["now"], "rows_opened": {"count": len(opened), "class_1": by_class["1"], "class_2": by_class["2"], "class_3": by_class["3"], "rows": opened},
                  "class_moves": [], "now": now, "r510": {"rows_in": len(opened), "rows_in_opened": len(opened), "rows_in_existing_rows_entering": 0, "rows_out": 0},
                  "by_package_and_class": {"whole_list": by_package},
                  "P1_units": {"fix_units": len(fix_units), "new_P1_rows_and_the_unit_each_joins": new_p1},
                  "expected_by_the_brief_I5": expected, "I5_holds": i5}, indent=1, sort_keys=True, ensure_ascii=False))
sys.exit(0 if i5 else 1)
