"""12q-CLOSE — the R510 recount at the session-12q closeout, from the plan. Read-only.
usage, from the repository root: recount_12q_close.py docs/plan/ENGINE_REFACTOR_PLAN.md
Writes recount_12q_close.json beside itself and prints it.
Baseline: 12q_P12-2B/recount_12q_r659.py run on the plan given (275 rows; it must still reproduce
its committed JSON byte for byte, and through it the chain back to recount_12l_r624.py).
Delta: none. The closeout opens no row and closes none out of the count: no section-L table after
"L (12q additions, R659)" exists. INT-p12-2 is CLOSED at b3f0072 (R660(a)) and stays counted until
P1's close (R607(d)); the script asserts the closing note is in its row and the row is still counted.
R510 for the session: rows in 3 (INT-q12-1, INT-q12-2, INT-q12-3; all opened, R659(d)), rows out 0."""
import contextlib, io, json, os, sys
PLAN = sys.argv[1]; D = "docs/session-reports/session-12"
BASE = f"{D}/12q_P12-2B/recount_12q_r659.py"; BASE_JSON = f"{D}/12q_P12-2B/recount_12q_r659.json"
committed = open(BASE_JSON, encoding="utf-8").read()
g = {"__file__": os.path.abspath(BASE)}; argv = sys.argv; sys.argv = ["recount_12q_r659.py", PLAN]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    try: exec(open(BASE, encoding="utf-8").read(), g)
    except SystemExit as e: assert e.code == 0, "recount_12q_r659.py no longer reproduces"
sys.argv = argv
assert open(BASE_JSON, encoding="utf-8").read() == committed, "recount_12q_r659.py rewrote its committed JSON differently"
assert buf.getvalue() == committed, "recount_12q_r659.py's output is not its committed JSON"
base = json.loads(buf.getvalue())
own, step2 = dict(g["own"]), g["step2"]
L = step2[step2.index("\n### L. "):step2.index("\n### Not engine defects")]
tables = [line.split("**")[1] for line in L.split("\n") if line.startswith("**L (")]
after = tables[tables.index("L (12q additions, R659).") + 1:]
row = [line for line in L.split("\n") if line.startswith("| INT-p12-2 |")]
closed = len(row) == 1 and "*Note 2026-10-17 (12q-P12-2B, R660(a)):* CLOSED at `b3f0072`" in row[0]
now = base["now"]
expected = {"rows": 275, "class_1": 95, "class_2": 50, "class_3": 130, "without_class": 0,
            "P1": {"class_1": 26, "class_2": 20, "class_3": 12, "rows": 58}, "P7_rows": 116, "fix_units": 35,
            "session_rows_in": ["INT-q12-1", "INT-q12-2", "INT-q12-3"]}
by_package = base["by_package_and_class"]["whole_list"]
holds = (now == {k: expected[k] for k in ("rows", "class_1", "class_2", "class_3", "without_class")}
         and base["expected_holds"] is True and after == [] and closed and "INT-p12-2" in own
         and by_package["P1"] == expected["P1"] and by_package["P7"]["rows"] == expected["P7_rows"]
         and base["P1_units"]["fix_units"] == expected["fix_units"]
         and [r["id"] for r in base["rows_opened"]["rows"]] == expected["session_rows_in"])
out = json.dumps({"baseline_12q_r659": now, "rows_opened_at_close": 0, "class_moves": [], "now": now,
                  "section_L_tables_after_R659": after,
                  "INT-p12-2": {"closed_note_present": closed, "still_counted": "INT-p12-2" in own, "class": own.get("INT-p12-2")},
                  "r510_session_12q": {"rows_in": len(expected["session_rows_in"]), "rows_in_ids": expected["session_rows_in"], "rows_out": 0},
                  "P1": by_package["P1"], "P7_rows": by_package["P7"]["rows"], "fix_units": base["P1_units"]["fix_units"],
                  "expected": expected, "expected_holds": holds}, indent=1, sort_keys=True, ensure_ascii=False)
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "recount_12q_close.json"), "w", encoding="utf-8") as f:
    f.write(out + "\n")
print(out)
sys.exit(0 if holds else 1)
