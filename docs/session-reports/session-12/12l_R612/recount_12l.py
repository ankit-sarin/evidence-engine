"""12l-R612 — the R510 recount of the pre-tag list after R612, from the plan. Read-only.
usage, from the repository root: recount_12l.py docs/plan/ENGINE_REFACTOR_PLAN.md   (stdout: JSON)

Baseline: 12k_0c/recount_12k.py run on the plan given (its per-row class, package and kind for the
238 rows of the 12k recount; it must still reproduce that count).
Deltas, all read from the plan:
  - every row of the "L (12l additions)" table enters with the leading digit of its Class cell and
    the first package its Owning-package cell names;
  - a dated note "*Note 2026-10-11 (12l-R612, R612(l); D11):* class moved by P1's package ruling,
    Class X … to Class Y" moves the class of the row that carries it. A ruled move may lower a class
    (R607(a): "class confirmed or moved with evidence"; R587 and R599 for the conditional rows) —
    unlike a candidate note, which under R597(d) never does.
Units: P1's fix units are the package plan's (12l_P1A/p1_plan.py, UNITS with rows), as R612(m)
accepts them; a new P1 row joins the unit its Owning-package cell names. They are counted here
from that file and the two cells, not re-derived by 12k-EST's merge rules.
"""
import contextlib, io, json, re, sys
from collections import Counter

PLAN = sys.argv[1]
D = "docs/session-reports/session-12"
g = {}; argv = sys.argv; sys.argv = ["recount_12k.py", PLAN]
with contextlib.redirect_stdout(io.StringIO()) as buf:
    exec(open(f"{D}/12k_0c/recount_12k.py", encoding="utf-8").read(), g)
sys.argv = argv
base = json.loads(buf.getvalue())
own, pk, kind, cells, step2 = dict(g["own"]), {k: v[0] for k, v in g["pk"].items()}, dict(g["kind"]), g["cells"], g["step2"]
assert base["now"] == {"rows": 238, "class_1": 98, "class_2": 44, "class_3": 96, "without_class": 0}, base["now"]
before = dict(own)

# ---- rows opened
L = step2[step2.index("\n### L. "):step2.index("\n### Not engine defects")]
add = L.split("**L (12l additions).**")[1].split("**The pre-tag list, recounted 2026-10-11 (12l-R612")[0]
opened = []
for line in add.split("\n"):
    if line.startswith("| INT-"):
        c = cells(line)
        assert c[0] not in own, c[0]
        o = {"id": c[0], "class": c[2][0], "conditional": c[2].startswith("1 (conditional"),
             "package": re.search(r"P\d", c[3]).group(0), "unit": (re.search(r"U\d\d", c[3]) or [None])[0], "ruling": c[4]}
        opened.append(o); own[o["id"]] = o["class"]; pk[o["id"]] = o["package"]; kind[o["id"]] = "opened at 12l-R612"

# ---- class moves
MV = re.compile(r"\*Note 2026-10-11 \(12l-R612, (R612\(l\)[^)]*)\):\* class moved by P1's package ruling, Class (\d)(?: \(conditional\))? to Class (\d)")
moves = []
for line in step2.split("\n"):
    if line.startswith("| "):
        rid = cells(line)[0]
        for m in MV.finditer(line):
            assert rid in before and before[rid] == m.group(2), (rid, before.get(rid), m.group(2))
            moves.append({"id": rid, "before": m.group(2), "after": m.group(3), "package": pk[rid], "row": kind[rid], "ruling": m.group(1)})
            own[rid] = m.group(3)

per = Counter(own.values())
now = {"rows": len(own), "class_1": per["1"], "class_2": per["2"], "class_3": per["3"], "without_class": per["none"]}
table = {}
for rid, c in own.items(): table.setdefault(pk[rid], Counter())[c] += 1
by_package = {p: {"class_1": t["1"], "class_2": t["2"], "class_3": t["3"], "rows": sum(t.values())} for p, t in sorted(table.items())}

# ---- P1's units, from the package plan R612(m) accepts
sys.path.insert(0, f"{D}/12l_P1A"); sys.dont_write_bytecode = True
import p1_plan
fix_units = [u for u in p1_plan.UNITS if u[2]]
unit_ids = {u[0] for u in fix_units}
plan_rows = {r for u in fix_units for r in u[2]}
p1_before = {r for r in before if pk[r] == "P1"}
new_p1 = [o for o in opened if o["package"] == "P1"]
units = {"fix_units_in_the_package_plan": len(fix_units), "placed_steps_not_fix_units": [u[0] for u in p1_plan.UNITS if not u[2]],
         "package_plan_rows_equal_P1_rows_before_R612": plan_rows == p1_before,
         "new_P1_rows_and_the_unit_each_joins": {o["id"]: o["unit"] for o in new_p1},
         "every_new_P1_row_joins_an_existing_unit": all(o["unit"] in unit_ids for o in new_p1),
         "fix_units_after_R612": len(fix_units)}

by_class = Counter(o["class"] for o in opened)
expected = {"rows": 249, "class_1": 95, "class_2": 46, "class_3": 108, "P1": {"rows": 49, "class_1": 26, "class_2": 16, "class_3": 7}, "P1_units": 35}
i1 = (all(now[k] == expected[k] for k in ("rows", "class_1", "class_2", "class_3")) and now["without_class"] == 0
      and by_package["P1"] == expected["P1"] and units["fix_units_after_R612"] == 35
      and units["package_plan_rows_equal_P1_rows_before_R612"] and units["every_new_P1_row_joins_an_existing_unit"])
print(json.dumps({
    "baseline_12k_item_0c": base["now"],
    "rows_opened": {"count": len(opened), "class_1": by_class["1"], "class_2": by_class["2"], "class_3": by_class["3"],
                    "class_1_conditional": sum(o["conditional"] for o in opened), "rows": opened},
    "class_moves": moves,
    "now": now,
    "r510": {"rows_in": len(opened), "rows_in_opened": len(opened), "rows_in_existing_rows_entering": 0, "rows_out": 0},
    "by_package_and_class": {"rule": "first-named package, as recount_12k.py reads it; a row opened here by its Owning-package cell", "whole_list": by_package},
    "P1_units": units,
    "expected_by_the_brief_I1": expected, "I1_holds": i1,
}, indent=1, sort_keys=True, ensure_ascii=False))
sys.exit(0 if i1 else 1)
