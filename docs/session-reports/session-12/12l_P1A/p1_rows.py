"""12l-P1A step 1 — the P1 row table, by script, from the plan.

usage, from the repository root:
    p1_rows.py docs/plan/ENGINE_REFACTOR_PLAN.md <out.json> <out.md>

Runs 12k_est/est_12k.py (which runs 12k_0c/recount_12k.py) on the plan given, into a
temporary directory, and keeps the rows whose first-named package is P1. Adds, per row,
the rulings and dated notes its plan line carries. Reads only; writes its two outputs.
Reconciles against the committed recount_12k.json and est_12k.json and exits 1 on any
difference in P1's rows or class counts (brief I1).
"""
import json, re, subprocess, sys, tempfile, os
from collections import Counter

PLAN, OUT_JSON, OUT_MD = sys.argv[1:4]
D = "docs/session-reports/session-12"
with tempfile.TemporaryDirectory() as t:
    subprocess.run([sys.executable, f"{D}/12k_est/est_12k.py", PLAN, f"{t}/e.json", f"{t}/e.md"],
                   check=True, stdout=subprocess.DEVNULL)
    est = json.load(open(f"{t}/e.json", encoding="utf-8"))
committed = json.load(open(f"{D}/12k_est/est_12k.json", encoding="utf-8"))
recount = json.load(open(f"{D}/12k_0c/recount_12k.json", encoding="utf-8"))

# the row's own plan line, from recount_12k.py's reader (est_12k.json does not carry it)
import contextlib, io
g = {}; argv = sys.argv; sys.argv = ["recount_12k.py", PLAN]
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(f"{D}/12k_0c/recount_12k.py", encoding="utf-8").read(), g)
sys.argv = argv
rowline = g["rowline"]

rows = est["rows"]
p1 = [r for r in rows if rows[r]["package"] == "P1"]
units = {}
for u in est["section_2_fix_units"]["multi_row_units"]:
    if u["package"] == "P1":
        for r in u["rows"]:
            units[r] = {"lead": u["lead"], "rows": u["rows"], "bases": u["bases"]}

NOTE = re.compile(r"\*Note (\d{4}-\d\d-\d\d) \(([^)]*(?:\([a-z]\))?[^)]*)\):\*")
out = []
for rid in p1:
    r = rows[rid]; line = rowline(rid)
    notes = [{"date": m.group(1), "source": m.group(2),
              "text": line[m.end():(n.start() if (n := NOTE.search(line, m.end())) else len(line))].strip(" |")}
             for m in NOTE.finditer(line)]
    out.append({
        "id": rid, "class": r["class"], "kind": r["kind"],
        "class_cell": r["class_cell"], "package_cell": r["package_cell"],
        "packages_named": r["packages_named"],
        "rulings": sorted(set(re.findall(r"\bR\d{2,3}\b", line)), key=lambda x: int(x[1:])),
        "dated_notes": notes,
        "findings": r["findings"],
        "pin_statement_12k_est": r["pin"],
        "pin_stated_on_row": "(stated)" in r["pin"],
        "conditional": r["conditional"], "condition": r["condition"],
        "phase_a_may_move_class": r["phase_a_may_move_class"],
        "phase_a_text": r["phase_a_text"],
        "unit_12k_est": units.get(rid, {"lead": rid, "rows": [rid], "bases": []}),
        "plan_line": line,
    })

cls = Counter(o["class"] for o in out)
n_units = len({o["unit_12k_est"]["lead"] for o in out})
want = recount["by_package_and_class"]["whole_list"]["P1"]
cp1 = sorted(r for r in committed["rows"] if committed["rows"][r]["package"] == "P1")
recon = {
    "rows": len(out), "class_1": cls["1"], "class_2": cls["2"], "class_3": cls["3"],
    "units_12k_est": n_units,
    "recount_12k_json_P1": want,
    "same_row_set_as_committed_est_12k_json": sorted(p1) == cp1,
    "rows_whose_est_record_differs_from_committed": [r for r in p1 if rows[r] != committed["rows"].get(r)],
    "pin_statement_counts": dict(Counter(o["pin_statement_12k_est"] for o in out)),
    "rows_with_no_pin_statement_of_any_kind": sorted(o["id"] for o in out if o["pin_statement_12k_est"].startswith("pin-effect unknown")),
    "conditional_class_1": sorted(o["id"] for o in out if o["conditional"]),
}
ok = (recon["same_row_set_as_committed_est_12k_json"]
      and [cls["1"], cls["2"], cls["3"]] == [want["class_1"], want["class_2"], want["class_3"]]
      and len(out) == 47 and [cls["1"], cls["2"], cls["3"]] == [29, 14, 4])
recon["I1_holds"] = ok
json.dump({"plan": PLAN, "reconciliation": recon, "rows": out}, open(OUT_JSON, "w", encoding="utf-8"),
          indent=1, ensure_ascii=False, sort_keys=True)
with open(OUT_MD, "w", encoding="utf-8") as f:
    f.write("| row | class | conditional | packages named | pin statement in the plan (12k-EST reading) | rulings on the row | dated notes | 12k-EST unit |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
    for o in out:
        f.write(f"| {o['id']} | {o['class']} | {'yes' if o['conditional'] else ''} | {', '.join(o['packages_named'])} | {o['pin_statement_12k_est']} | "
                f"{', '.join(o['rulings'])} | {len(o['dated_notes'])} | {o['unit_12k_est']['lead'] if len(o['unit_12k_est']['rows']) > 1 else ''} |\n")
print(json.dumps({k: v for k, v in recon.items()}, indent=1, ensure_ascii=False))
sys.exit(0 if ok else 1)
