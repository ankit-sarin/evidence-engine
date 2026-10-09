"""12i scoping read, step 3c — coverage of each census unit by outside assessments A and B.

usage, from the repository root:
    coverage.py <repo_root> <out_dir>
reads  <out_dir>/census.json (census.py), <out_dir>/judgement.json (hand-written; optional on a
       first pass), docs/session-reports/session-12/outside/assessment_{A,B}*.md,
       docs/session-reports/session-12/12f_triage_rows.md, 12f_triage.md, 12f_C56-A.md
writes <out_dir>/coverage.json, coverage.csv, coverage_table.md, coverage_summary.md,
       matches.json (every script match, and every ambiguous token left for judgement)

Grades, per unit and per assessment:
  F  a finding is located to the unit      C  inside a reviewed area, no finding located to it
  N  not mentioned
Marks:  S  set by this script from the assessment text     J  set in judgement.json, with a reason

SCRIPT RULES (S cells) — the assessment text is cut into parts; a part is a FINDING part
(B: each "### Fnn" section; A: each bullet of sections 1–4, each "Fresh-Pass" gap, each row of
the implementation table) or a NON-FINDING part (everything else: B's overall assessment, scope,
repair sequence, reproduction record; A's introduction and "Architecture Overview" paragraphs).
  1. PATH: a token ending ".py" in the text. It matches a tracked file when the file's path ends
     with the token at a path-component boundary. A token matching exactly one tracked file
     (engine/, scripts/, analysis/ or tests/) is a match; a token matching several is AMBIGUOUS
     and is left to judgement. Dotted module names (engine.x.y) are resolved the same way.
  2. NAME: an identifier in the text (containing "_", or CamelCase, or followed by "(") that is
     the name of a function, class, method or module-level assignment in the tree. Defined in
     exactly one tracked file -> a match; in several -> AMBIGUOUS, left to judgement.
     A NAME match is skipped when the same part already PATH-matches the file. An identifier
     that is a component of a path token ("…/advance_stage.py") is not a NAME.
  3. A match inside a FINDING part grades the unit F and cites the part's ID. A match only inside
     NON-FINDING parts grades it C.
  4. DIR: a directory token ("engine/migrations/") grades every unit under it C unless a file-level
     match already graded it.
  5. No match of any kind -> N.
JUDGEMENT (J cells) — judgement.json holds (a) area rules: a quoted area phrase of the assessment
mapped to unit globs, grading matching units C (or N, to record a deliberate non-mapping) where the
script grade is N; and (b) per-unit overrides. Every J cell carries its reason. A J entry never
lowers a script F silently: an override of a script grade records the script's grade beside it.

12F column — "which files the 12f read re-read at HEAD". 12f recorded quoted code by content
anchor, not a list of files read, so the column records only what is on the page:
  Q<n>  the unit's path occurs n times in 12f_triage_rows.md (a 12f row quotes or names it)
  H<n>  the unit's path occurs n times in 12f_C56-A.md only (the broad-handler census: handlers
        read, not the file)
  not recorded   neither. This is NOT "not read": 12f kept no file list.
"""
import ast, csv, fnmatch, json, os, re, subprocess, sys
from collections import Counter, defaultdict

root, out = os.path.abspath(sys.argv[1]), sys.argv[2]
S12 = os.path.join(root, "docs/session-reports/session-12")
census = json.load(open(os.path.join(out, "census.json")))
units = [u["unit"] for u in census["units"]]
U = {u["unit"]: u for u in census["units"]}
jpath = os.path.join(out, "judgement.json")
judgement = json.load(open(jpath)) if os.path.exists(jpath) else {"area_rules": [], "overrides": []}

tracked = [t for t in subprocess.run(["git", "-C", root, "ls-files", "engine", "scripts", "analysis",
                                      "tests"], capture_output=True, text=True,
                                     check=True).stdout.split("\n") if t.endswith(".py")]

# ---- definition index -------------------------------------------------------------------
defs = defaultdict(set)          # name -> files defining it
for p in tracked:
    try:
        tree = ast.parse(open(os.path.join(root, p), encoding="utf-8").read())
    except SyntaxError:
        continue
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defs[n.name].add(p)
    for n in tree.body:
        tg = []
        if isinstance(n, ast.Assign):
            tg = n.targets
        elif isinstance(n, ast.AnnAssign):
            tg = [n.target]
        for t in tg:
            if isinstance(t, ast.Name):
                defs[t.id].add(p)

# ---- cut the assessments into parts -------------------------------------------------------


def clean(s):
    return s.replace("\\_", "_").replace("\\[", "[").replace("\\]", "]").replace("\\$", "$")


def parts_B(text):
    parts, cur, cur_id, finding = [], [], "B:front", False
    for line in text.split("\n"):
        m = re.match(r"### (F\d\d) — ", line)
        h2 = re.match(r"## (.+)", line)
        h3 = re.match(r"### (.+)", line)
        if m or h2 or (h3 and not m):
            if cur:
                parts.append((cur_id, finding, "\n".join(cur)))
            cur = []
            if m:
                cur_id, finding = m.group(1), True
            else:
                cur_id, finding = "B:" + (h2 or h3).group(1).strip()[:40], False
        cur.append(line)
    if cur:
        parts.append((cur_id, finding, "\n".join(cur)))
    return parts


def parts_A(text):
    parts, cur, cur_id, finding = [], [], "A:intro", False
    sec, bullet, fresh, trow = None, 0, False, 0

    def flush():
        if cur:
            parts.append((cur_id, finding, "\n".join(cur)))
    for line in text.split("\n"):
        new = None
        m = re.match(r"## (\d)\. ", line)
        if m:
            sec, bullet = m.group(1), 0
            new = ("A:§%s heading" % sec, False)
        elif line.startswith("# Fresh-Pass"):
            fresh = True
            new = ("A:fresh-pass heading", False)
        elif line.startswith("# Targeted Implementation Plan"):
            fresh, sec = False, "T"
            new = ("A:table heading", False)
        elif re.match(r"### Architecture Overview", line):
            new = ("A:§%s overview" % sec, False)
        elif re.match(r"### Key Findings", line):
            new = ("A:§%s findings heading" % sec, False)
        elif fresh and re.match(r"### (\d)\. ", line):
            new = ("A-FP%s" % re.match(r"### (\d)\. ", line).group(1), True)
        elif sec == "T" and line.startswith("| **"):
            trow += 1
            if trow > 1:                       # row 1 is the header
                new = ("A-T%d" % (trow - 1), True)
        elif sec and sec != "T" and not fresh and line.startswith("- **"):
            bullet += 1
            new = ("A-§%s.%d" % (sec, bullet), True)
        if new:
            flush()
            cur.clear()
            cur_id, finding = new
        cur.append(line)
    flush()
    return parts


A_text = clean(open(os.path.join(S12, "outside/assessment_A_architectural.md"), encoding="utf-8").read())
B_text = clean(open(os.path.join(S12, "outside/assessment_B_17_findings.md"), encoding="utf-8").read())
PARTS = {"A": parts_A(A_text), "B": parts_B(B_text)}

# ---- matching -----------------------------------------------------------------------------
PATH_RE = re.compile(r"(?<![\w/.])((?:[A-Za-z0-9_]+/)*[A-Za-z0-9_]+\.py)\b")
DOTTED_RE = re.compile(r"\b((?:engine|scripts|analysis)(?:\.[A-Za-z_][A-Za-z0-9_]*)+)")
DIR_RE = re.compile(r"(?<![\w/.])((?:engine|scripts|analysis)(?:/[A-Za-z0-9_]+)*)/(?![\w])")
IDENT_RE = re.compile(r"(?<![\w./])([A-Za-z_][A-Za-z0-9_]{3,})(?!\.py)(?![\w/])(\()?")


def path_candidates(tok):
    return [p for p in tracked if p == tok or p.endswith("/" + tok)]


def dotted_candidates(tok):
    parts = tok.split(".")
    for n in range(len(parts), 1, -1):          # longest resolvable prefix
        base = "/".join(parts[:n])
        c = [p for p in tracked if p in (base + ".py", base + "/__init__.py")]
        if c:
            return c
    return []


def is_namey(name, called):
    return called or "_" in name.strip("_") or (name[0].isupper() and re.search(r"[a-z][A-Z]", name))


matches, ambiguous = [], []       # dict rows
for who, parts in PARTS.items():
    for pid, finding, text in parts:
        hit_files = set()
        for m in PATH_RE.finditer(text):
            tok = m.group(1)
            c = path_candidates(tok)
            row = {"assessment": who, "part": pid, "finding": finding, "kind": "PATH", "token": tok,
                   "candidates": c}
            if len(c) == 1:
                matches.append(dict(row, file=c[0]))
                hit_files.add(c[0])
            elif len(c) > 1:
                ambiguous.append(row)
            else:
                ambiguous.append(dict(row, note="no tracked file has this name"))
        for m in DOTTED_RE.finditer(text):
            c = dotted_candidates(m.group(1))
            if len(c) == 1 and c[0] not in hit_files:
                matches.append({"assessment": who, "part": pid, "finding": finding, "kind": "DOTTED",
                                "token": m.group(1), "candidates": c, "file": c[0]})
                hit_files.add(c[0])
        for m in DIR_RE.finditer(text):
            d = m.group(1) + "/"
            if any(p.startswith(d) for p in tracked):
                matches.append({"assessment": who, "part": pid, "finding": finding, "kind": "DIR",
                                "token": d, "candidates": [], "file": d})
        seen_names = set()
        for m in IDENT_RE.finditer(text):
            name, called = m.group(1), bool(m.group(2))
            if name in seen_names or name not in defs or not is_namey(name, called):
                continue
            seen_names.add(name)
            c = sorted(defs[name])
            row = {"assessment": who, "part": pid, "finding": finding, "kind": "NAME", "token": name,
                   "candidates": c}
            if len(c) == 1:
                if c[0] not in hit_files:
                    matches.append(dict(row, file=c[0]))
            else:
                if not (set(c) & hit_files):
                    ambiguous.append(row)
                else:
                    ambiguous.append(dict(row, note="one candidate is PATH-matched in the same part"))

# ---- grades -------------------------------------------------------------------------------
cells = {u: {"A": None, "B": None} for u in units}
for who in ("A", "B"):
    per = defaultdict(list)
    dirs = []
    for m in matches:
        if m["assessment"] != who:
            continue
        if m["kind"] == "DIR":
            dirs.append(m)
        elif m["file"] in cells:
            per[m["file"]].append(m)
    for u in units:
        ms = per.get(u, [])
        f = [m for m in ms if m["finding"]]
        if f:
            ids = []
            for m in f:
                tag = "%s (%s `%s`)" % (m["part"], m["kind"].lower(), m["token"])
                if tag not in ids:
                    ids.append(tag)
            cells[u][who] = {"grade": "F", "mark": "S", "cite": "; ".join(ids)}
        elif ms:
            cells[u][who] = {"grade": "C", "mark": "S", "cite": "named outside a finding: " + "; ".join(
                sorted({"%s (%s `%s`)" % (m["part"], m["kind"].lower(), m["token"]) for m in ms}))}
        else:
            d = [m for m in dirs if u.startswith(m["file"])]
            if d:
                cells[u][who] = {"grade": "C", "mark": "S", "cite": "directory named: " + "; ".join(
                    sorted({"%s (`%s`)" % (m["part"], m["token"]) for m in d}))}
            else:
                cells[u][who] = {"grade": "N", "mark": "S", "cite": ""}

rank = {"N": 0, "C": 1, "F": 2}
for r in judgement.get("area_rules", []):
    for u in units:
        if any(fnmatch.fnmatch(u, g) for g in r["units"]) and not any(
                fnmatch.fnmatch(u, g) for g in r.get("except", [])):
            c = cells[u][r["assessment"]]
            if c["mark"] == "S" and c["grade"] == "N":
                cells[u][r["assessment"]] = {"grade": r["grade"], "mark": "J",
                                             "cite": "%s: %s" % (r["id"], r["reason"])}
for o in judgement.get("overrides", []):
    c = cells[o["unit"]][o["assessment"]]
    cite = o["reason"]
    if c["mark"] == "S" and c["grade"] != "N" and c["grade"] != o["grade"]:
        cite += " [script grade was %s: %s]" % (c["grade"], c["cite"])
    elif c["mark"] == "S" and c["grade"] == o["grade"] and c["cite"]:
        cite += " [script: %s]" % c["cite"]
    cells[o["unit"]][o["assessment"]] = {"grade": o["grade"], "mark": "J", "cite": cite}

# ---- 12F column ---------------------------------------------------------------------------
rows_txt = open(os.path.join(S12, "12f_triage_rows.md"), encoding="utf-8").read()
c56_txt = open(os.path.join(S12, "12f_C56-A.md"), encoding="utf-8").read()


def occurrences(text, path):
    return len(re.findall(r"(?<![\w/])" + re.escape(path) + r"(?![\w])", text))


for u in units:
    q, h = occurrences(rows_txt, u), occurrences(c56_txt, u)
    cells[u]["12F"] = ("Q%d" % q) if q else (("H%d" % h) if h else "not recorded")
    cells[u]["12F_rows"], cells[u]["12F_c56"] = q, h

# ---- outputs ------------------------------------------------------------------------------
table = []
for u in units:
    a, b = cells[u]["A"], cells[u]["B"]
    best = max(a["grade"], b["grade"], key=lambda g: rank[g])
    table.append({"unit": u, "package": U[u]["package"], "lines": U[u]["lines"], "RUN7": U[u]["RUN7"],
                  "RUN7_hops": U[u].get("RUN7_hops"), "FT": U[u]["FT"], "MIG": U[u]["MIG"],
                  "A": a["grade"], "A_mark": a["mark"], "A_cite": a["cite"],
                  "B": b["grade"], "B_mark": b["mark"], "B_cite": b["cite"],
                  "either": best, "12F": cells[u]["12F"]})

json.dump({"head": census["head"], "table": table}, open(os.path.join(out, "coverage.json"), "w"),
          indent=1, sort_keys=True)
json.dump({"matches": matches, "ambiguous": ambiguous,
           "parts": {w: [[pid, f, len(t)] for pid, f, t in ps] for w, ps in PARTS.items()}},
          open(os.path.join(out, "matches.json"), "w"), indent=1, sort_keys=True)
cols = ["unit", "lines", "RUN7", "FT", "MIG", "A", "A_mark", "B", "B_mark", "either", "12F", "A_cite",
        "B_cite"]
with open(os.path.join(out, "coverage.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(cols)
    for r in table:
        w.writerow([r[c] for c in cols])


def esc(s):
    return s.replace("|", "\\|")


with open(os.path.join(out, "coverage_table.md"), "w", encoding="utf-8") as f:
    f.write("| unit | lines | RUN7 | FT | MIG | A | B | 12F | A: cite or reason | B: cite or reason |\n")
    f.write("| --- | ---: | --- | --- | --- | --- | --- | --- | --- | --- |\n")
    for r in table:
        f.write("| `%s` | %d | %s | %s | %s | %s·%s | %s·%s | %s | %s | %s |\n" % (
            r["unit"], r["lines"], r["RUN7"], r["FT"], r["MIG"], r["A"], r["A_mark"], r["B"],
            r["B_mark"], r["12F"], esc(r["A_cite"]) or "—", esc(r["B_cite"]) or "—"))

R7 = ["top", "lazy", "-"]


def grid(key):
    g = {gr: {r: [0, 0] for r in R7} for gr in "FCN"}
    for r in table:
        c = g[r[key]][r["RUN7"]]
        c[0] += 1
        c[1] += r["lines"]
    return g


with open(os.path.join(out, "coverage_summary.md"), "w", encoding="utf-8") as f:
    for key, title in (("A", "Assessment A"), ("B", "Assessment B"),
                       ("either", "Either assessment (the higher of the two grades)")):
        g = grid(key)
        f.write("**%s** — units (lines)\n\n" % title)
        f.write("| grade | RUN7 top | RUN7 lazy | not on the Run 7 import graph | all |\n")
        f.write("| --- | ---: | ---: | ---: | ---: |\n")
        for gr in "FCN":
            tot = [sum(g[gr][r][0] for r in R7), sum(g[gr][r][1] for r in R7)]
            f.write("| %s | %s | %s | %s | %s |\n" % (gr, *["%d (%s)" % (g[gr][r][0], format(g[gr][r][1], ","))
                                                         for r in R7], "%d (%s)" % (tot[0], format(tot[1], ","))))
        tot = {r: [sum(g[gr][r][0] for gr in "FCN"), sum(g[gr][r][1] for gr in "FCN")] for r in R7}
        f.write("| all | %s | %d (%s) |\n\n" % (
            " | ".join("%d (%s)" % (tot[r][0], format(tot[r][1], ",")) for r in R7),
            sum(tot[r][0] for r in R7), format(sum(tot[r][1] for r in R7), ",")))
    marks = Counter((w, r[w], r[w + "_mark"]) for r in table for w in ("A", "B"))
    f.write("**Cells by mark** — " + "; ".join("%s %s·%s %d" % (w, g, m, n)
                                                for (w, g, m), n in sorted(marks.items())) + "\n\n")
    f12 = Counter(r["12F"][0] if r["12F"] != "not recorded" else "not recorded" for r in table)
    l12 = Counter()
    for r in table:
        l12[r["12F"][0] if r["12F"] != "not recorded" else "not recorded"] += r["lines"]
    f.write("**12F column** — Q %d units (%s lines), H %d (%s), not recorded %d (%s)\n\n" % (
        f12["Q"], format(l12["Q"], ","), f12["H"], format(l12["H"], ","), f12["not recorded"],
        format(l12["not recorded"], ",")))
    for gr, label in (("N", "N by both"), ("C", "C at best (no finding located by either)")):
        sel = [r for r in table if r["either"] == gr and r["RUN7"] != "-"]
        f.write("**On the Run 7 import graph, %s** — %d units, %s lines (top %d / %s; lazy %d / %s)\n\n" % (
            label, len(sel), format(sum(r["lines"] for r in sel), ","),
            sum(1 for r in sel if r["RUN7"] == "top"),
            format(sum(r["lines"] for r in sel if r["RUN7"] == "top"), ","),
            sum(1 for r in sel if r["RUN7"] == "lazy"),
            format(sum(r["lines"] for r in sel if r["RUN7"] == "lazy"), ",")))
        for r in sel:
            f.write("- `%s` — %d lines, RUN7 %s, A %s·%s, B %s·%s, 12F %s\n" % (
                r["unit"], r["lines"], r["RUN7"], r["A"], r["A_mark"], r["B"], r["B_mark"], r["12F"]))
        f.write("\n")

print("units", len(table), "| matches", len(matches), "| ambiguous", len(ambiguous))
print(Counter((r["A"], r["A_mark"]) for r in table), Counter((r["B"], r["B_mark"]) for r in table))
