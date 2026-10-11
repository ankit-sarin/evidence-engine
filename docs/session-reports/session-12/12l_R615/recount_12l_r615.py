"""12l item 0 — the R510 recount after R615, from the plan. Read-only.
usage, from the repository root: recount_12l_r615.py docs/plan/ENGINE_REFACTOR_PLAN.md   (stdout: JSON)
R615 opens no row and moves no class, so the count is 12l_R614/recount_12l_r614.py's on the plan
given (254 rows); this script runs it, requires that it still reproduces, and states I6."""
import contextlib, io, json, sys
PLAN = sys.argv[1]; D = "docs/session-reports/session-12"
g = {}; argv = sys.argv; sys.argv = ["recount_12l_r614.py", PLAN]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    try: exec(open(f"{D}/12l_R614/recount_12l_r614.py", encoding="utf-8").read(), g)
    except SystemExit as e: assert e.code == 0, "recount_12l_r614.py no longer reproduces"
sys.argv = argv
base = json.loads(buf.getvalue())
expected = {"rows": 254, "class_1": 95, "class_2": 47, "class_3": 112, "without_class": 0}
i6 = base["now"] == expected
print(json.dumps({"baseline_12l_R614": base["now"], "rows_opened": {"count": 0, "rows": []}, "class_moves": [],
                  "now": base["now"], "r510": {"rows_in": 0, "rows_out": 0},
                  "by_package_and_class": base["by_package_and_class"], "P1_units": {"fix_units": base["P1_units"]["fix_units"]},
                  "expected_by_the_brief_I6": expected, "I6_holds": i6}, indent=1, sort_keys=True, ensure_ascii=False))
sys.exit(0 if i6 else 1)
