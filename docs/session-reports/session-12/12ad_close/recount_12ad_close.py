"""12ad-CLOSE — the R510 recount with the "L (12ad additions, R780)" table, from the plan. Read-only.
usage, from the repository root:
    recount_12ad_close.py <plan at d7bafc4> <plan at 8b2c388> <plan at 1e15b50> <plan at 8659035> <plan at 52be363> <plan at 95873b5> docs/plan/ENGINE_REFACTOR_PLAN.md
  (the first six files are `git show <commit>:…` of the plan; the sixth is the plan 12ac's recount ran on, so that script reproduces its JSON)
Writes recount_12ad_close.json beside itself and prints it.
Baseline: 12ac_close/recount_12ac_close.py run on the plan at 95873b5, which must reproduce its committed JSON byte for byte, and through it
the chain back to recount_12l_r624.py (272, 275, 276, 278, 279, 280, 281, 281). It writes its own JSON beside itself, so run this script in a
scratch clone before a commit (R673(b)).
Delta, read from the given plan: every row of the "L (12ad additions, R780)" table enters with the leading digit of its Class cell and the first
package its Owning-package cell names, as 12ab_close/recount_12ab_close.py reads its table (R782(g): INT-ad12-1, -2, -3). No class move. The
fix_units field is the baseline script's unit list, unchanged (35); the 12w script's literal stays 37 / 19; the third reading is the plan's last
dated fix-unit line, read here (R714(c)). R510 for session 12ad: rows in 3, rows out 0, no class move; E-ROOT, a section K row, CLOSED and counted."""
import contextlib, hashlib, io, json, os, re, sys
P1, P2, P3, P4, P12AB, P12AC, PLAN = sys.argv[1:8]; D = "docs/session-reports/session-12"
BASE = f"{D}/12ac_close/recount_12ac_close.py"; BASE_JSON = f"{D}/12ac_close/recount_12ac_close.json"
BASE_SHA256 = "2b003d6b"  # the 281 of the chain, as the 12ac closure quotes it
committed = open(BASE_JSON, encoding="utf-8").read()
assert hashlib.sha256(committed.encode()).hexdigest().startswith(BASE_SHA256), "the 12ac JSON is not the chain's 281"
assert hashlib.sha256(open(P12AC, "rb").read()).hexdigest().startswith("eb2a0108"), "the sixth plan is not the plan at 95873b5"
g = {"__file__": os.path.abspath(BASE)}; argv = sys.argv; sys.argv = ["recount_12ac_close.py", P1, P2, P3, P4, P12AB, P12AC]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    try: exec(open(BASE, encoding="utf-8").read(), g)
    except SystemExit as e: assert e.code == 0, "recount_12ac_close.py no longer reproduces"
sys.argv = argv
assert open(BASE_JSON, encoding="utf-8").read() == committed, "recount_12ac_close.py rewrote its committed JSON differently"
assert buf.getvalue() == committed, "recount_12ac_close.py's output is not its committed JSON"
base = json.loads(buf.getvalue())
assert base["now"] == {"rows": 281, "class_1": 94, "class_2": 56, "class_3": 131, "without_class": 0}
own, pk, fix_units, cells = dict(g["own"]), dict(g["pk"]), g["fix_units"], g["g"]["cells"]

text = open(PLAN, encoding="utf-8").read()
step2 = text[text.index("\n### L. "):text.index("\n### Not engine defects")]
TABLE = "**L (12ad additions, R780).**"
assert step2.count(TABLE) == 1, "the 12ad table is not in section L once"
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
expected = {"rows": 284, "class_1": 94, "class_2": 56, "class_3": 134, "without_class": 0,
            "rows_opened": ["INT-ad12-1", "INT-ad12-2", "INT-ad12-3"], "P1": {"class_1": 25, "class_2": 25, "class_3": 12, "rows": 62},
            "P6_rows": 12, "P7_rows": 119, "fix_units": 35, "dated_units": 37, "dated_done": 26}
holds = (now == {k: expected[k] for k in ("rows", "class_1", "class_2", "class_3", "without_class")}
         and [o["id"] for o in opened] == expected["rows_opened"]
         and [(o["class"], o["package"]) for o in opened] == [("3", "P7"), ("3", "P6"), ("3", "P7")]
         and by_package["P1"] == expected["P1"] and by_package["P6"]["rows"] == expected["P6_rows"]
         and by_package["P7"]["rows"] == expected["P7_rows"] and len(fix_units) == expected["fix_units"]
         and "E-ROOT" in own and (int(dated[0]), int(dated[1])) == (expected["dated_units"], expected["dated_done"]))
out = json.dumps({"baseline_12ac": base["now"], "baseline_json_sha256": hashlib.sha256(committed.encode()).hexdigest(),
                  "rows_opened": {"count": len(opened), "rows": opened}, "class_moves": [], "now": now,
                  "r510_session_12ad": {"rows_in": len(opened), "rows_in_ids": [o["id"] for o in opened], "rows_out": 0, "class_moves": 0},
                  "closed_this_session_still_counted": ["E-ROOT"],
                  "by_package_and_class": {"whole_list": by_package}, "fix_units": len(fix_units),
                  "fix_units_literal_12w": base["fix_units_literal_12w"],
                  "fix_units_dated_lines": {"units": int(dated[0]), "done": int(dated[1])},
                  "expected": expected, "expected_holds": holds}, indent=1, sort_keys=True, ensure_ascii=False)
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "recount_12ad_close.json"), "w", encoding="utf-8") as f:
    f.write(out + "\n")
print(out)
sys.exit(0 if holds else 1)
