"""12p-CLOSE — the R510 recount after R650(e), from the plan. Read-only (R651(a)).
usage, from the repository root: recount_12p_close.py docs/plan/ENGINE_REFACTOR_PLAN.md
Writes recount_12p_close.json beside itself and prints it.
Baseline: 12p_open/recount_12p_item0.py run on the plan given (271 rows; it must still reproduce
its committed JSON byte for byte, and through it the chain back to recount_12l_r624.py).
Delta: every row of the "L (12p additions, R650)" table enters with the leading digit of its Class
cell and the first package its Owning-package cell names. R650 moves no class. A new P1 row joins
the unit its cell names, so P1's fix units stay the package plan's (R612(m)). One exception, for
one row only (R650(e), R652(b)): INT-p12-2 rides with the placed step U13 as its telemetry
prerequisite, not as a fix unit; a second row naming a placed step is not accepted here."""
import contextlib, io, json, os, re, sys
from collections import Counter
PLAN = sys.argv[1]; D = "docs/session-reports/session-12"
ITEM0 = f"{D}/12p_open/recount_12p_item0.py"; ITEM0_JSON = f"{D}/12p_open/recount_12p_item0.json"
committed = open(ITEM0_JSON, encoding="utf-8").read()
g = {"__file__": os.path.abspath(ITEM0)}; argv = sys.argv; sys.argv = ["recount_12p_item0.py", PLAN]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    try: exec(open(ITEM0, encoding="utf-8").read(), g)
    except SystemExit as e: assert e.code == 0, "recount_12p_item0.py no longer reproduces"
sys.argv = argv
assert open(ITEM0_JSON, encoding="utf-8").read() == committed, "recount_12p_item0.py rewrote its committed JSON differently"
assert buf.getvalue() == committed, "recount_12p_item0.py's output is not its committed JSON"
base = json.loads(buf.getvalue())
assert base["now"] == {"rows": 271, "class_1": 95, "class_2": 50, "class_3": 126, "without_class": 0}
own, pk, cells, step2, fix_units = dict(g["own"]), dict(g["pk"]), g["cells"], g["step2"], g["fix_units"]
L = step2[step2.index("\n### L. "):step2.index("\n### Not engine defects")]
add = L.split("**L (12p additions, R650).**")[1].split("**The pre-tag list, recounted 2026-10-15 (12p-CLOSE")[0]
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
# R650(e), R652(b): INT-p12-2 rides with the placed step U13 (12l_P1A/p1_plan.py: no rows, not a fix
# unit), for this row only. The 35 fix units are unchanged; any other row must join a fix unit.
sys.dont_write_bytecode = True; sys.path.insert(0, f"{D}/12l_P1A"); import p1_plan
placed = {u[0] for u in p1_plan.UNITS if not u[2]}
EXCEPTION = {"INT-p12-2": "U13"}
assert "U13" in placed and "U13" not in unit_ids
via_placed = {r: u for r, u in new_p1.items() if u not in unit_ids}
exception_holds = via_placed == EXCEPTION and all(u in placed for u in via_placed.values())
units_hold = all(u in unit_ids for r, u in new_p1.items() if r not in EXCEPTION) and exception_holds
# I3 / R651(a) / R652(b): 272 rows, 95 / 50 / 127, 0 without a class; rows_opened 1, INT-p12-2; P1 58 rows.
expected = {"rows": 272, "class_1": 95, "class_2": 50, "class_3": 127, "rows_opened": ["INT-p12-2"], "P1_rows": 58, "P1": {"class_1": 26, "class_2": 20, "class_3": 12, "rows": 58}, "P7_rows": 113}
holds = (all(now[k] == expected[k] for k in ("rows", "class_1", "class_2", "class_3")) and now["without_class"] == 0
         and [o["id"] for o in opened] == expected["rows_opened"] and by_package["P1"] == expected["P1"] and by_package["P7"]["rows"] == expected["P7_rows"]
         and len(fix_units) == 35 and units_hold)
by_class = Counter(o["class"] for o in opened)
out = json.dumps({"baseline_12p_item0": base["now"], "rows_opened": {"count": len(opened), "class_1": by_class["1"], "class_2": by_class["2"], "class_3": by_class["3"], "rows": opened},
                  "class_moves": [], "now": now, "r510": {"rows_in": len(opened), "rows_in_opened": len(opened), "rows_in_existing_rows_entering": 0, "rows_out": 0},
                  "by_package_and_class": {"whole_list": by_package},
                  "P1_units": {"fix_units": len(fix_units), "new_P1_rows_and_the_unit_each_joins": new_p1,
                                "rows_riding_with_a_placed_step": via_placed, "placed_step_exception": {"rows": EXCEPTION, "rulings": ["R650(e)", "R652(b)"], "holds": exception_holds}},
                  "expected_by_I3_R651a_R652b": expected, "expected_holds": holds}, indent=1, sort_keys=True, ensure_ascii=False)
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "recount_12p_close.json"), "w", encoding="utf-8") as f:
    f.write(out + "\n")
print(out)
sys.exit(0 if holds else 1)
