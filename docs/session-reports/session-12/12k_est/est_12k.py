"""12k-EST — read-only re-estimate inputs for the path to the tag (R604). Reads the plan, the two
internal-review read-outs, recount_12k.py and git history; writes nothing but its own outputs.
usage, from the repository root:
    est_12k.py docs/plan/ENGINE_REFACTOR_PLAN.md <out.json> <tables.md>

Every count below is derived here. The rules are mechanical and stated where they are applied;
where the plan does not state something the row is marked, not guessed (E2).
"""
import contextlib, io, json, re, subprocess, sys
from collections import Counter, defaultdict

PLAN, OUT_JSON, OUT_MD = sys.argv[1], sys.argv[2], sys.argv[3]
RC = "docs/session-reports/session-12/12k_0c/recount_12k.py"
g = {}
argv = sys.argv; sys.argv = ["recount_12k.py", PLAN]
with contextlib.redirect_stdout(io.StringIO()) as buf:
    exec(open(RC, encoding="utf-8").read(), g)
sys.argv = argv
recount = json.loads(buf.getvalue())
own, pk, kind, covmap, rowline, cells, step2, text = (g[k] for k in ("own", "pk", "kind", "covmap", "rowline", "cells", "step2", "text"))
IDS = list(own)
PKGS = ["P1", "P2", "P3", "P4", "P5", "P6", "P7"]

# ---------------------------------------------------------------- E1
e1 = {"rows": len(own), "by_class": dict(Counter(own.values())),
      "table": {p: [sum(1 for r in IDS if pk[r][0] == p and own[r] == c) for c in "123"] for p in PKGS}}
want = recount["by_package_and_class"]["whole_list"]
e1["equals_recount_json"] = (len(own) == recount["now"]["rows"] and
                             all(e1["table"][p] == [want[p]["class_1"], want[p]["class_2"], want[p]["class_3"]] for p in PKGS))
assert e1["equals_recount_json"] and e1["rows"] == 238

# ---------------------------------------------------------------- per-row facts
W1N = re.compile(r"\*Note 2026-10-10 \(12j item 0c, [^)]*(?:\([a-z]\))?[^)]*\):\* Wave 1 row (\d+)(?:, items? .*?)? \((?:L|W1)[^)]*\) extends this row — Class (\d)(.*?), (P\d)")
W2N = re.compile(r"\*Note 2026-10-11 \(12k item 0c, [^)]*(?:\([a-z]\))?[^)]*\):\* (?:Wave 2 row (\d+) \([^)]*\) extends this row|"
                 r"Wave 2 adjacent items? ([\d and]+) \(seen by [^)]*\) (?:is|are) noted on this row) — Class (\d), (P\d)(; conditional)?")
R601_SENT = "Pre-tag: closes on its minimum loud-failure fix (R601); full fix: requirement note on J1."
rows = {}
for rid in IDS:
    line = rowline(rid); c = cells(line); cl = rowline(rid, covmap)
    cov = cells(cl)[1] if cl else ""
    r = {"id": rid, "class": own[rid], "package": pk[rid][0], "package_source": pk[rid][1], "kind": kind[rid], "line": line}
    w1 = [{"finding": f"W1 row {m.group(1)}", "class": m.group(2), "package": m.group(4), "conditional": "(conditional" in m.group(3)} for m in W1N.finditer(line)]
    w2 = [{"finding": (f"W2 row {m.group(1)}" if m.group(1) else f"W2 adjacent item {m.group(2)}"), "class": m.group(3), "package": m.group(4),
           "conditional": bool(m.group(5))} for m in W2N.finditer(line)]
    r["findings"] = w1 + w2
    sectionL = kind[rid] in ("section L", "opened at 12k item 0c")
    by_note = kind[rid].startswith("entered")
    if sectionL:
        r["class_cell"], r["package_cell"] = c[2], c[3]
        r["evidence_cell"] = c[5] if len(c) > 5 else ""
        ptxt = c[3] + " || " + cov
    elif by_note:
        r["class_cell"] = r["package_cell"] = r["evidence_cell"] = ""
        ptxt = " || ".join(m.group(0) for m in list(W1N.finditer(line)) + list(W2N.finditer(line)))
    else:
        k = re.search(r"\*Note 2026-10-08 \(12g item 0c,[^)]*\):\* 12f triage[^|]*?(?=\*Note|$)", line)
        r["class_cell"] = r["evidence_cell"] = ""; r["package_cell"] = k.group(0) if k else ""
        ptxt = r["package_cell"] + " || " + cov
    r["ptxt"] = ptxt
    named = set(re.findall(r"P[1-7]\b", ptxt)) | {f["package"] for f in r["findings"]}
    r["packages_named"] = sorted(named)
    # conditional Class 1 (R587 / R599): the Class cell says so, or the note that gives a by-note row its class does
    cond, condition = False, ""
    if r["class"] == "1":
        if sectionL and re.match(r"1 \((conditional|latent)", r["class_cell"]):
            cond = True
            k = re.search(r'Condition, as the read-out states it — evidence level: "(.*?)"(?:; class / package cell: "(.*?)")?\)', r["class_cell"])
            condition = (k.group(1) + (f' — cell: {k.group(2)}' if k.group(2) else "")) if k else r["class_cell"]
        elif by_note:
            top = [f for f in r["findings"] if f["class"] == "1"]
            if top and all(f["conditional"] for f in top):
                cond = True; condition = "conditional by its note(s): " + ", ".join(f["finding"] for f in top)
    r["conditional"], r["condition"] = cond, condition
    r["may_weigh_R600"] = '"may weigh"' in r["package_cell"]
    r["phase_a_may_move_class"] = cond or r["may_weigh_R600"] or any(f["conditional"] for f in r["findings"])
    r["r601"] = (R601_SENT in r["package_cell"]) or ("the page/workbook halves close on their minimum loud-failure fix (R601)" in line)
    # pin effect, as the plan states it (E2). Sources: the package cell, the coverage-map line, the classed notes.
    src = ptxt + (" || " + " ".join(m.group(0) + line[m.end():m.end() + 400] for m in W1N.finditer(line)) if r["findings"] else "")
    neg = bool(re.search(r"not pin-affecting|outside the pin|audit-side", src))
    pos = bool(re.search(r"(?<!not )pin-affecting", src)) or "before the freeze smoke" in src
    wave2_only = kind[rid] == "opened at 12k item 0c" or (kind[rid] == "entered at 12k by a note")
    if pos and neg: r["pin"] = "pin-affecting in part (stated)"
    elif pos: r["pin"] = "pin-affecting (stated)"
    elif neg: r["pin"] = "not pin-affecting (stated)"
    elif wave2_only: r["pin"] = "not pin-affecting by the readers' reading (R599 block; 12j_review_w2.md)"
    elif rid.startswith("INT-i12-") or kind[rid] == "entered at 12j by a note":
        r["pin"] = "not among the rows Wave 1 names pin-affecting (12i_review_w1.md; R589) — no statement on the row"
    else: r["pin"] = "pin-effect unknown (the row states nothing)"
    r["migration"] = bool(re.search(r"\bP6\b|migration 02\d|\b02[34]\b", ptxt))
    r["phase_a_text"] = [s.strip() for s in re.split(r"(?<=[.;])\s+|\s\|\|\s", ptxt) if "Phase A" in s]
    rows[rid] = r

# ---------------------------------------------------------------- 1. package x class
sec1 = {}
for p in PKGS:
    rs = [rows[r] for r in IDS if rows[r]["package"] == p]
    sec1[p] = {"rows": len(rs), "class_1": sum(r["class"] == "1" for r in rs), "class_2": sum(r["class"] == "2" for r in rs),
               "class_3": sum(r["class"] == "3" for r in rs),
               "class_1_conditional": [{"id": r["id"], "condition": r["condition"]} for r in rs if r["conditional"]],
               "multi_package": [{"id": r["id"], "packages": r["packages_named"]} for r in rs if len(r["packages_named"]) > 1],
               "phase_a_may_move_class": [r["id"] for r in rs if r["phase_a_may_move_class"]],
               "ids": {c: [r["id"] for r in rs if r["class"] == c] for c in "123"}}

# ---------------------------------------------------------------- 2. fix units (union-find over stated bases only)
parent = {r: r for r in IDS}
def find(x):
    while parent[x] != x:
        parent[x] = parent[parent[x]]; x = parent[x]
    return x
edges, partial_rides = [], []
def union(a, b, basis):
    """Merge only rows of one first-named package: a ride across packages is a part of a row, listed apart."""
    if a == b or a not in parent or b not in parent: return
    if rows[a]["package"] != rows[b]["package"]:
        if not any({a, b} == {x["a"], x["b"]} for x in partial_rides):
            partial_rides.append({"a": a, "a_package": rows[a]["package"], "b": b, "b_package": rows[b]["package"], "basis": basis})
        return
    edges.append({"a": a, "b": b, "basis": basis})
    parent[find(a)] = find(b)
# (a) one Wave 1/2 finding placed on several rows, where every row carrying it is on the list by notes only
bynote = {r for r in IDS if kind[r].startswith("entered")}
carriers = defaultdict(list)
for r in IDS:
    for f in rows[r]["findings"]: carriers[f["finding"]].append(r)
crosslinks = []
for f, rs in carriers.items():
    rs = list(dict.fromkeys(rs))
    if len(rs) > 1:
        if all(x in bynote for x in rs):
            for x in rs[1:]: union(rs[0], x, f"one finding on several rows ({f}; R597(c)); every carrier is on the list by notes only")
        else:
            crosslinks.append({"finding": f, "rows": rs, "why_not_merged": "a carrier has content of its own"})
# (b) "with <row>" in the row's package cell, coverage-map line or 12g note (the plan's ride-together statements)
idre = re.compile(r"(?<![\w-])(" + "|".join(re.escape(i) for i in sorted(IDS, key=len, reverse=True)) + r")(?![\w-])")
for r in IDS:
    # an alternative the read-out offered and the ruling did not take ("(or P6 with …)") is not a ride
    for seg in re.findall(r"(?<!\(or P\d )\bwith ([^—;.|]*)", rows[r]["ptxt"] if not kind[r].startswith("entered") else ""):
        for other in idre.findall(seg):
            union(r, other, f'"with {other}" in the package cell / coverage line / 12g note of {r}')
    for m in re.finditer(r"\((INT-[\w-]+)\) goes with it", rows[r]["line"]):
        union(r, m.group(1), f'"({m.group(1)}) goes with it" in a dated note on {r}')
    if "in the INT-h12-2 batch" in rows[r]["line"] and r != "INT-h12-2":
        union(r, "INT-h12-2", "R589: lands in the INT-h12-2 batch; one comparison rehearsal covers them together")
# (c) rulings that name a ride-together not written as "with <row>" on the row
for a, b, basis in [("E-TEST", "INT-g6-1", "R543: E-TEST lands with or immediately after INT-g6-1"),
                    ("A-7", "C56", "R531/R533/R542: one run-fault predicate lands first, with the extract-path handlers"),
                    ("INT-g3-1", "B-F15", "R538: INT-g3-1 rides with B-F15's text-cell helper")]:
    if find(a) != find(b): union(a, b, basis)
_lines = {r: rows[r]["line"] for r in IDS}
comp = defaultdict(list)
for r in IDS: comp[find(r)].append(r)
units = []
for members in comp.values():
    lead = sorted(members, key=lambda x: (rows[x]["class"], IDS.index(x)))[0]
    units.append({"lead": lead, "package": rows[lead]["package"], "class": rows[lead]["class"], "rows": members,
                  "packages_of_members": sorted({rows[m]["package"] for m in members}),
                  "r601_minimum": any(rows[m]["r601"] for m in members),
                  "bases": sorted({e["basis"] for e in edges if e["a"] in members})})
for r in IDS: rows[r]["unit"] = [u["lead"] for u in units if r in u["rows"]][0]
sec2 = {p: {c: {"rows": sum(1 for r in IDS if rows[r]["package"] == p and rows[r]["class"] == c),
                "units": sum(1 for u in units if u["package"] == p and u["class"] == c)} for c in "123"} for p in PKGS}
multi_units = [u for u in units if len(u["rows"]) > 1]

# ---------------------------------------------------------------- 3. ordering constraints
pin = defaultdict(lambda: defaultdict(list))
for r in IDS: pin[rows[r]["pin"]][rows[r]["package"]].append(r)
mig = [{"id": r, "package": rows[r]["package"], "packages_named": rows[r]["packages_named"],
        "text": [s for s in re.split(r"\s\|\|\s", rows[r]["ptxt"]) if re.search(r"\bP6\b|02[34]", s)][:2]} for r in IDS if rows[r]["migration"]]
FT = re.compile(r"ft_adjudication_html|ft_screening_adjudicator|_import_ft|both files|review_workbook")
r601_rows = [{"id": r, "ft_page_or_importer": bool(FT.search(rows[r]["evidence_cell"])) or r == "INT-i12-77",
              "where": re.sub(r"; evidence level.*", "", rows[r]["evidence_cell"])[:260]} for r in IDS if rows[r]["r601"]]
def ruling(n):
    k = re.search(rf"^\| 20\d\d-\d\d-\d\d \| ({n} — .*?) \| PI", text, re.M)
    return k.group(1) if k else None
quoted = {n: ruling(n) for n in ("R537", "R539", "R540", "R542", "R543", "R546", "R549", "R561", "R571", "R589", "R590", "R591", "R592", "R601", "R603")}

# ---------------------------------------------------------------- 4. the Class 3 batch
def files_of(r):
    row = rows[r]
    # the text cell and the "where" part of the Evidence cell; the read-out and reader files a row cites are not what it touches
    where = re.search(r"row \d+ \([^;]*; (.*?); evidence level", row["evidence_cell"])
    src = (where.group(1) if where else "") + " " + (cells(_lines[r])[1] if len(cells(_lines[r])) > 1 else "")
    fs = [f for f in re.findall(r"[\w/]*\w+\.(?:py|md|yaml|toml|txt)\b", src)
          if not re.search(r"12[a-k]_|triage|readers/|^(?:[A-Z]\d*|L\d\d|F\d|W1-\w+)\.md$", f)]
    k = re.search(r"row \d+ \([^;]*; (.*?); evidence level", row["evidence_cell"])       # Wave 1 rows name a module, not a file
    mods = []
    if k and not re.search(r"\.py", k.group(1)):
        for tok in re.split(r"[;,/]| and ", k.group(1)):
            m = re.match(r"\s*((?:elicitation |cloud/)?[a-z_]+)\.[A-Za-z_]", tok)          # a dotted module.function name only
            if m: mods.append(m.group(1).strip())
    out = list(dict.fromkeys([re.sub(r"\.py$", "", f.split("/")[-1]) for f in fs] + mods))      # a .py file and its module are one key
    return out
PINFILE = re.compile(r"^(units|prompts|pipeline|materialize|extractor|effective_config|review_spec|codebook|run_manifest|contracts)(\.py)?$|elicitation (units|prompts|pipeline|materialize|contracts)")
c3 = []
for r in IDS:
    if rows[r]["class"] == "3":
        fs = files_of(r)
        c3.append({"id": r, "package": rows[r]["package"], "first_file": fs[0] if fs else "(no file or module named in the row)", "files_named": fs,
                   "touches_pin_file": sorted({f for f in fs if PINFILE.search(f)}), "pin": rows[r]["pin"], "unit": rows[r]["unit"]})
p7 = [x for x in c3 if x["package"] == "P7"]
riding = [x for x in c3 if x["package"] != "P7"]
byfile = defaultdict(list)
for x in p7: byfile[x["first_file"]].append(x["id"])
sec4 = {"class_3_rows": len(c3), "p7_rows": len(p7), "class_3_riding_elsewhere": {p: [x["id"] for x in riding if x["package"] == p] for p in PKGS if p != "P7"},
        "p7_non_class_3": [r for r in IDS if rows[r]["package"] == "P7" and rows[r]["class"] != "3"],
        "p7_by_first_named_file": dict(sorted(byfile.items(), key=lambda kv: (-len(kv[1]), kv[0]))),
        "p7_files": len([f for f in byfile if not f.startswith("(")]), "p7_rows_naming_no_file": byfile.get("(no file or module named in the row)", []),
        "p7_rows_naming_several_files": {x["id"]: len(x["files_named"]) for x in p7 if len(x["files_named"]) > 1},
        "p7_rows_touching_a_pin_file": {x["id"]: x["touches_pin_file"] for x in p7 if x["touches_pin_file"]},
        "class_3_units_in_p7": sum(1 for u in units if u["package"] == "P7" and u["class"] == "3")}

# ---------------------------------------------------------------- 5. Phase A load
log = text[text.index("\n## Decision log"):]
phase_a_rulings = []
for m in re.finditer(r"^\| 20\d\d-\d\d-\d\d \| (R(5[4-9]\d|60[0-4])) — (.*?) \| PI", log, re.M):
    for s in re.split(r"(?<=[.;:])\s+(?=[-(A-Z])", m.group(3)):
        if "Phase A" in s:
            phase_a_rulings.append({"ruling": m.group(1), "packages": sorted(set(re.findall(r"P[1-7]\b", s))), "text": s[:420]})
phase_a_rows = {p: [{"id": r, "text": rows[r]["phase_a_text"]} for r in IDS if rows[r]["package"] == p and rows[r]["phase_a_text"]] for p in PKGS}
sec5 = {"by_ruling": phase_a_rulings, "rows_naming_a_phase_a": phase_a_rows,
        "conditional_rows_whose_trigger_a_phase_a_establishes": {p: len(sec1[p]["class_1_conditional"]) for p in PKGS}}

# ---------------------------------------------------------------- 6. effort basis (git + the plan's session records)
def git(*a): return subprocess.run(["git", *a], capture_output=True, text=True, check=True).stdout.strip()
SESS = [("12a", "694d568", "77fe1a2", "2026-10-01T00:08:00+00:00", ["C40", "C47", "E-DEADKI", "E-EXEC", "E-STATE"], "R332–R370"),
        ("12c", "77fe1a2", "ad01226", "2026-10-01T12:18:00+00:00", ["E-PIN", "C41", "D21", "E-PASS2", "E-SENTINEL", "C52", "C53"], "R371–R428"),
        ("12d", "ad01226", "caf2034", "2026-10-06T03:46:00+00:00", ["D23 (code)", "D24", "D26 (a)", "B23"], "R429–R463"),
        ("12e", "caf2034", "c22c15d", "2026-10-06T19:28:00+00:00", ["C54", "C55", "C57", "C58"], "R464–R506")]
from datetime import datetime
sessions = []
for name, a, b, opened, closed, rul in SESS:
    assert f"opened {opened[:10]} {opened[11:16]} UTC" in text, name                  # the open time is the plan's
    for x in closed: assert x.split(" ")[0] in text
    commits = git("log", "--format=%h", f"{a}..{b}").split()
    code = git("log", "--format=%h", f"{a}..{b}", "--", "engine", "tests", "scripts", "analysis").split()
    end = git("log", "-1", "--format=%cI", b)
    hours = (datetime.fromisoformat(end) - datetime.fromisoformat(opened)).total_seconds() / 3600
    lo, hi = map(int, re.findall(r"\d+", rul))
    sessions.append({"session": name, "commits": len(commits), "commits_touching_code_or_tests": len(code), "rows_closed": len(closed), "rows": closed,
                     "rulings_issued": hi - lo + 1, "ruling_range": rul, "opened_utc": opened, "closeout_commit_utc": end, "wall_hours_open_to_closeout_commit": round(hours, 2),
                     "gate_runs": "not stated as a count in the record; at least one per code commit (plan rule: standard gate before every commit)"})
rps = [s["rows_closed"] for s in sessions]
hpr = [round(s["wall_hours_open_to_closeout_commit"] / s["rows_closed"], 2) for s in sessions]
import math
ranges = {}
for p in PKGS:
    u12 = sec2[p]["1"]["units"] + sec2[p]["2"]["units"]
    if p in ("P6", "P7"):
        ranges[p] = {"class_1_2_units": u12, "sessions": None}
    else:
        ranges[p] = {"class_1_2_units": u12, "sessions": [math.ceil(u12 / max(rps)), math.ceil(u12 / min(rps))],
                     "label": "INFERRED", "basis": f"Class 1 and 2 fix units ÷ rows closed per fix-bearing session, 12a–12e (min {min(rps)}, max {max(rps)})"}
sec6 = {"sessions_measured": sessions, "rows_closed_per_session": {"min": min(rps), "max": max(rps), "total": sum(rps), "sessions": len(rps)},
        "wall_hours_per_row": hpr, "per_package": ranges, "gate_wall_seconds_12k": 792.72}

# ---------------------------------------------------------------- 7. Run 7
new_rows = [r for r in IDS if r.startswith(("INT-i12-", "INT-j12-")) or kind[r].startswith("entered")]
sec7 = {"rows_opened_or_entered_at_12j_12k": len(new_rows),
        "of_which_stated_pin_affecting": [r for r in new_rows if rows[r]["pin"].startswith("pin-affecting")],
        "R561": quoted["R561"]}

# ---------------------------------------------------------------- 8. C12 / C37
sec8 = {}
for rid in ("C12", "C37"):
    k = re.search(r"\*Note 2026-10-11 \(12k item 0c, R602\):\* Wave 2 adjacent item 5 .*?(?= \*Note|\s\|$)", rowline(rid))
    sec8[rid] = k.group(0)

for r in rows.values(): r.pop("line"); r.pop("ptxt")
out = {"e1": e1, "section_1_rows_by_package_and_class": sec1,
       "section_2_fix_units": {"by_package_and_class": sec2, "units": len(units), "multi_row_units": multi_units, "edges": edges,
                               "cross_links_not_merged": crosslinks, "rides_across_packages_not_merged": partial_rides, "r601_rows_stay_separate_units": [r["id"] for r in r601_rows]},
       "section_3_ordering": {"pin": {k: dict(v) for k, v in pin.items()}, "pin_counts": {k: sum(len(x) for x in v.values()) for k, v in pin.items()},
                              "migration_rows": mig, "r601_rows": r601_rows, "rulings_quoted": quoted},
       "section_4_class_3_batch": sec4, "section_5_phase_a": sec5, "section_6_effort_basis": sec6, "section_7_run_7": sec7,
       "section_8_c12_c37_notes": sec8, "rows": rows}
json.dump(out, open(OUT_JSON, "w", encoding="utf-8"), indent=1, sort_keys=True, ensure_ascii=False)

# ---------------------------------------------------------------- generated tables
L = []
def t(header, body):
    L.append("| " + " | ".join(header) + " |"); L.append("|" + "|".join(" --- " for _ in header) + "|")
    for b in body: L.append("| " + " | ".join(str(x) for x in b) + " |")
    L.append("")
L.append("### T1 — rows and fix units by package and class\n")
t(["package", "rows C1", "rows C2", "rows C3", "rows", "units C1", "units C2", "units C3", "units", "C1 conditional", "rows naming >1 package", "class may move at Phase A"],
  [[p, sec1[p]["class_1"], sec1[p]["class_2"], sec1[p]["class_3"], sec1[p]["rows"], sec2[p]["1"]["units"], sec2[p]["2"]["units"], sec2[p]["3"]["units"],
    sum(sec2[p][c]["units"] for c in "123"), len(sec1[p]["class_1_conditional"]), len(sec1[p]["multi_package"]), len(sec1[p]["phase_a_may_move_class"])] for p in PKGS]
  + [["all", *(sum(sec1[p][k] for p in PKGS) for k in ("class_1", "class_2", "class_3", "rows")), *(sum(sec2[p][c]["units"] for p in PKGS) for c in "123"), len(units),
      sum(len(sec1[p]["class_1_conditional"]) for p in PKGS), sum(len(sec1[p]["multi_package"]) for p in PKGS), sum(len(sec1[p]["phase_a_may_move_class"]) for p in PKGS)]])
L.append("### T2 — rows by package and class (IDs)\n")
t(["package", "Class 1", "Class 2", "Class 3"], [[p, ", ".join(sec1[p]["ids"]["1"]), ", ".join(sec1[p]["ids"]["2"]), ", ".join(sec1[p]["ids"]["3"])] for p in PKGS])
L.append("### T3 — conditional Class 1 rows, with the condition as the plan carries it\n")
t(["package", "row", "condition"], [[p, x["id"], x["condition"][:300]] for p in PKGS for x in sec1[p]["class_1_conditional"]])
L.append("### T4 — rows that name more than one package\n")
t(["first-named package", "row", "every package named"], [[p, x["id"], ", ".join(x["packages"])] for p in PKGS for x in sec1[p]["multi_package"]])
L.append("### T5 — rows whose class a Phase A may move (R587, R599, R600)\n")
t(["package", "rows"], [[p, ", ".join(sec1[p]["phase_a_may_move_class"])] for p in PKGS])
L.append("### T6 — fix units of more than one row, with the basis\n")
t(["lead row (package, class)", "rows", "packages of the members", "basis"],
  [[f'{u["lead"]} ({u["package"]}, {u["class"]})', ", ".join(u["rows"]), ", ".join(u["packages_of_members"]), " // ".join(u["bases"])] for u in sorted(multi_units, key=lambda u: (u["package"], u["class"]))])
L.append("### T7 — one finding on several rows, not merged (a carrier has content of its own)\n")
t(["finding", "rows"], [[x["finding"], ", ".join(x["rows"])] for x in crosslinks])
L.append("### T7b — rides across packages, not merged (a part of one row lands with a row of another package)\n")
t(["row", "its package", "rides with", "that row's package", "basis"], [[x["a"], x["a_package"], x["b"], x["b_package"], x["basis"]] for x in partial_rides])
L.append("### T8 — pin effect as the plan states it\n")
t(["statement", "rows", "by package"], [[k, sum(len(x) for x in v.values()), "; ".join(f"{p}: {', '.join(v[p])}" if len(v[p]) <= 12 else f"{p}: {len(v[p])} rows" for p in PKGS if p in v)] for k, v in pin.items()])
L.append("### T9 — rows naming P6 or a migration\n")
t(["row", "first-named package", "packages named", "the plan's words"], [[x["id"], x["package"], ", ".join(x["packages_named"]), " // ".join(s[:200] for s in x["text"])] for x in mig])
L.append("### T10 — R601's rows (loud-failure minimum pre-tag)\n")
t(["row", "FT page or FT importer (by the files its Evidence cell names)", "where"], [[x["id"], "yes" if x["ft_page_or_importer"] else "no", x["where"]] for x in r601_rows])
L.append("### T11 — P7 by first-named file or module\n")
t(["file or module", "rows", "IDs"], [[f, len(v), ", ".join(v)] for f, v in sec4["p7_by_first_named_file"].items()])
L.append("### T12 — Class 3 rows riding outside P7 (R543)\n")
t(["package", "rows"], [[p, ", ".join(v)] for p, v in sec4["class_3_riding_elsewhere"].items()])
L.append("### T13 — P7 rows that name a pin-affecting file\n")
t(["row", "file(s)", "pin statement on the row"], [[k, ", ".join(v), rows[k]["pin"]] for k, v in sec4["p7_rows_touching_a_pin_file"].items()])
L.append("### T14 — Phase A items by ruling\n")
t(["ruling", "packages named", "text"], [[x["ruling"], ", ".join(x["packages"]), x["text"]] for x in phase_a_rulings])
L.append("### T15 — rows whose package cell names a Phase A\n")
t(["package", "row", "text"], [[p, x["id"], " // ".join(s[:220] for s in x["text"])] for p in PKGS for x in phase_a_rows[p]])
L.append("### T16 — fix-bearing sessions, measured\n")
t(["session", "commits", "touching code or tests", "rows closed", "rulings issued", "opened (plan)", "closeout commit (git)", "wall h", "h per row"],
  [[s["session"], s["commits"], s["commits_touching_code_or_tests"], f'{s["rows_closed"]} ({", ".join(s["rows"])})', f'{s["rulings_issued"]} ({s["ruling_range"]})',
    s["opened_utc"][:16], s["closeout_commit_utc"][:16], s["wall_hours_open_to_closeout_commit"], h] for s, h in zip(sessions, hpr)])
L.append("### T17 — per-package range in sessions\n")
t(["package", "Class 1 and 2 fix units", "sessions", "label and basis"],
  [[p, v["class_1_2_units"], (f'{v["sessions"][0]}–{v["sessions"][1]}' if v["sessions"] else "no measured basis"), (v.get("label", "") + " — " + v.get("basis", "")) if v["sessions"] else "see the text"] for p, v in ranges.items()])
open(OUT_MD, "w", encoding="utf-8").write("\n".join(L) + "\n")
print(json.dumps({"e1": e1["equals_recount_json"], "units": len(units), "multi_row_units": len(multi_units), "edges": len(edges),
                  "pin_counts": out["section_3_ordering"]["pin_counts"], "class3": sec4["class_3_rows"], "p7": sec4["p7_rows"]}, ensure_ascii=False))
