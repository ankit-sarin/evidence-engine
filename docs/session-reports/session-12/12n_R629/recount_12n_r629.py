"""12n-U12-C2B item 0 — the R510 recount after R629, from the plan. Read-only.
usage, from the repository root: recount_12n_r629.py docs/plan/ENGINE_REFACTOR_PLAN.md   (stdout: JSON)
Baseline: 12m_R627/recount_12m_r627.py run on the plan given (268 rows; it must still reproduce,
and through it the chain back to recount_12l_r624.py).
Delta: every row of the "L (12n additions, R629)" table enters with the leading digit of its Class
cell and the first package its Owning-package cell names. R629 moves no class. A new P1 row joins
the unit its cell names, so P1's fix units stay the package plan's (R612(m))."""
import contextlib, io, json, re, sys
from collections import Counter
PLAN = sys.argv[1]; D = "docs/session-reports/session-12"
g = {}; argv = sys.argv; sys.argv = ["recount_12m_r627.py", PLAN]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    try: exec(open(f"{D}/12m_R627/recount_12m_r627.py", encoding="utf-8").read(), g)
    except SystemExit as e: assert e.code == 0, "recount_12m_r627.py no longer reproduces"
sys.argv = argv
base = json.loads(buf.getvalue())
assert base["now"] == {"rows": 268, "class_1": 95, "class_2": 50, "class_3": 123, "without_class": 0}
assert base == json.load(open(f"{D}/12m_R627/recount_12m_r627.json", encoding="utf-8")), "the committed R627 JSON is not reproduced"
own, pk, cells, step2, fix_units = dict(g["own"]), dict(g["pk"]), g["cells"], g["step2"], g["fix_units"]
L = step2[step2.index("\n### L. "):step2.index("\n### Not engine defects")]
add = L.split("**L (12n additions, R629).**")[1].split("**The pre-tag list, recounted 2026-10-13 (12n-R629")[0]
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
# I2 as the brief states it: 269 rows, 95 / 50 / 124, P1 57, P7 111.
expected = {"rows": 269, "class_1": 95, "class_2": 50, "class_3": 124, "P1_rows": 57, "P7_rows": 111}
i2 = (all(now[k] == expected[k] for k in ("rows", "class_1", "class_2", "class_3")) and now["without_class"] == 0
      and by_package["P1"]["rows"] == expected["P1_rows"] and by_package["P7"]["rows"] == expected["P7_rows"]
      and all(u in unit_ids for u in new_p1.values()))
by_class = Counter(o["class"] for o in opened)
print(json.dumps({"baseline_12m_R627": base["now"], "rows_opened": {"count": len(opened), "class_1": by_class["1"], "class_2": by_class["2"], "class_3": by_class["3"], "rows": opened},
                  "class_moves": [], "now": now, "r510": {"rows_in": len(opened), "rows_in_opened": len(opened), "rows_in_existing_rows_entering": 0, "rows_out": 0},
                  "by_package_and_class": {"whole_list": by_package},
                  "P1_units": {"fix_units": len(fix_units), "new_P1_rows_and_the_unit_each_joins": new_p1},
                  "expected_by_the_brief_I2": expected, "I2_holds": i2}, indent=1, sort_keys=True, ensure_ascii=False))
sys.exit(0 if i2 else 1)
