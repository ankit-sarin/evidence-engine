"""12ai-CLOSE — the R510 recount with the "L (12ai additions, R819)" table, from the plan. Read-only.
usage, from the repository root:
    recount_12ai_close.py <plan at d7bafc4> <plan at 8b2c388> <plan at 1e15b50> <plan at 8659035> <plan at 52be363> <plan at 95873b5> <plan at 2f73fd6> <plan at df3e721> <plan at 4884bc3> docs/plan/ENGINE_REFACTOR_PLAN.md
  (the first nine files are `git show <commit>:…` of the plan; the ninth is the plan 12af's recount ran on, its close's item 0, so that
  script reproduces its JSON)
Writes recount_12ai_close.json beside itself and prints it.
Baseline: 12af_close/recount_12af_close.py run on those nine plans, which must reproduce its committed JSON byte for byte (sha256
3657ce2f…15a5, the 287 of the chain), and through it the chain back to recount_12l_r624.py. It writes its own JSON beside itself, so run this
script in a scratch clone before a commit (R673(b)).
Delta, read from the given plan: every row of the "L (12ai additions, R819)" table enters with the leading digit of its Class cell and the
first package its Owning-package cell names, as 12af_close/recount_12af_close.py reads its table (R819(b): INT-ai12-1; R819(i): INT-ai12-2).
No class move. Its own expectation (R824(a), R821(a)): 289 rows, Class 1 / 2 / 3 = 94 / 59 / 136, 0 unclassed; rows opened
[INT-ai12-1, INT-ai12-2] at ("2", "P1") and ("2", "P4"); P1 and P4 one row more than the baseline's, P5, P6 and P7 as the baseline's; the
fix_units field the baseline's; the plan's last dated fix-unit line 39 / 35; INT-ai12-1, INT-i12-71 and INT-j12-51 CLOSED and counted
(R820(e), R821(c)). R510 for session 12ai: rows in 2, rows out 0, no class move."""
import contextlib, hashlib, io, json, os, re, sys
ARGS = sys.argv[1:10]; PLAN = sys.argv[10]; D = "docs/session-reports/session-12"
BASE = f"{D}/12af_close/recount_12af_close.py"; BASE_JSON = f"{D}/12af_close/recount_12af_close.json"
BASE_SHA256 = "3657ce2f219c9fc130d5248295e99071855245d87dc2b7ab6d80e9b5c16d15a5"  # the 287 of the chain, as the 12af and 12ah closes record it
committed = open(BASE_JSON, encoding="utf-8").read()
assert hashlib.sha256(committed.encode()).hexdigest() == BASE_SHA256, "the 12af JSON is not the chain's 287"
g = {"__file__": os.path.abspath(BASE)}; argv = sys.argv; sys.argv = ["recount_12af_close.py", *ARGS]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    try: exec(open(BASE, encoding="utf-8").read(), g)
    except SystemExit as e: assert e.code == 0, "recount_12af_close.py no longer reproduces"
sys.argv = argv
assert open(BASE_JSON, encoding="utf-8").read() == committed, "recount_12af_close.py rewrote its committed JSON differently"
assert buf.getvalue() == committed, "recount_12af_close.py's output is not its committed JSON"
base = json.loads(buf.getvalue())
assert base["now"] == {"rows": 287, "class_1": 94, "class_2": 57, "class_3": 136, "without_class": 0}
own, pk, cells = dict(g["own"]), dict(g["pk"]), g["cells"]
fix_units_n = base["fix_units"]

text = open(PLAN, encoding="utf-8").read()
step2 = text[text.index("\n### L. "):text.index("\n### Not engine defects")]
TABLE = "**L (12ai additions, R819).**"
assert step2.count(TABLE) == 1, "the 12ai table is not in section L once"
opened = []
for line in step2.split(TABLE)[1].split("\n"):
    if line.startswith("| INT-"):
        c = cells(line); assert c[0] not in own
        o = {"id": c[0], "class": c[2][0], "package": re.search(r"P\d", c[3]).group(0), "ruling": c[4]}
        opened.append(o); own[o["id"]] = o["class"]; pk[o["id"]] = o["package"]

dated = re.findall(r"^\*Dated [^\n]*P1's fix units (\d+), done (\d+) ", text, flags=re.M)[-1]
from collections import Counter
per = Counter(own.values())
now = {"rows": len(own), "class_1": per["1"], "class_2": per["2"], "class_3": per["3"], "without_class": per["none"]}
table = {}
for rid, c in own.items(): table.setdefault(pk[rid], Counter())[c] += 1
by_package = {p: {"class_1": t["1"], "class_2": t["2"], "class_3": t["3"], "rows": sum(t.values())} for p, t in sorted(table.items())}
bp = base["by_package_and_class"]["whole_list"]
expected = {"rows": 289, "class_1": 94, "class_2": 59, "class_3": 136, "without_class": 0, "rows_opened": ["INT-ai12-1", "INT-ai12-2"],
            "P1_rows": bp["P1"]["rows"] + 1, "P4_rows": bp["P4"]["rows"] + 1, "P5_rows": bp["P5"]["rows"], "P6_rows": bp["P6"]["rows"],
            "P7_rows": bp["P7"]["rows"], "fix_units": base["fix_units"], "dated_units": 39, "dated_done": 35}
holds = (now == {k: expected[k] for k in ("rows", "class_1", "class_2", "class_3", "without_class")}
         and [o["id"] for o in opened] == expected["rows_opened"]
         and [(o["class"], o["package"]) for o in opened] == [("2", "P1"), ("2", "P4")]
         and all(by_package[p]["rows"] == expected[f"{p}_rows"] for p in ("P1", "P4", "P5", "P6", "P7"))
         and fix_units_n == expected["fix_units"]
         and all(r in own for r in ("INT-ai12-1", "INT-i12-71", "INT-j12-51"))
         and (int(dated[0]), int(dated[1])) == (expected["dated_units"], expected["dated_done"]))
out = json.dumps({"baseline_12af": base["now"], "baseline_json_sha256": hashlib.sha256(committed.encode()).hexdigest(),
                  "rows_opened": {"count": len(opened), "rows": opened}, "class_moves": [], "now": now,
                  "r510_session_12ai": {"rows_in": len(opened), "rows_in_ids": [o["id"] for o in opened], "rows_out": 0, "class_moves": 0},
                  "closed_this_session_still_counted": ["INT-ai12-1", "INT-i12-71", "INT-j12-51"],
                  "by_package_and_class": {"whole_list": by_package}, "fix_units": fix_units_n,
                  "fix_units_literal_12w": base["fix_units_literal_12w"],
                  "fix_units_dated_lines": {"units": int(dated[0]), "done": int(dated[1])},
                  "expected": expected, "expected_holds": holds}, indent=1, sort_keys=True, ensure_ascii=False)
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "recount_12ai_close.json"), "w", encoding="utf-8") as f:
    f.write(out + "\n")
print(out)
sys.exit(0 if holds else 1)
