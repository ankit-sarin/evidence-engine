"""12i scoping read, step 3b — census of code units at HEAD and the AST import graph. Read-only.

usage, from the repository root:
    census.py <repo_root> <out_dir>
writes <out_dir>/census.json and <out_dir>/census.csv ; prints a short summary.

Units: every git-tracked .py file under engine/ and scripts/ (one row each), with line count
(newline count, as `wc -l`) and package path. analysis/ is listed at directory level only, plus
every analysis/ module that an engine/ or scripts/ unit imports (by AST).

Import graph: every `import` / `from … import …` node of every tracked .py under engine/,
scripts/ and analysis/, at any nesting depth, resolved to tracked files:
  - `import a.b.c`            -> a/b/c.py or a/b/c/__init__.py, plus every parent package __init__
  - `from a.b import c`       -> a/b/c.py (if c is a submodule) else a/b.py or a/b/__init__.py
  - relative imports are resolved against the importing file's package
  - an edge is "module-level" unless the import node sits inside a function or lambda body
One DECLARED dynamic edge is added, because the AST cannot see it:
  engine/migrations/runner.py -> every engine/migrations/NNN_*.py
  (runner.py: `importlib.import_module(f"engine.migrations.{migration_id}")`).
String references that look like launches (`-m engine.x.y`, `scripts/x.py`) are listed under
"string_refs" and are NOT edges.

Reachability: transitive closure over all edges (module-level and in-function) from each entry
point. RUN7 = reachable from scripts/run_pipeline.py. "top" = reachable over module-level edges
only (what merely importing the entry point executes). RUN7_hops = the fewest in-function import
edges on any path from scripts/run_pipeline.py (0 for "top"; empty when unreachable).
"""
import ast, csv, json, os, re, subprocess, sys

root, out = os.path.abspath(sys.argv[1]), sys.argv[2]
os.makedirs(out, exist_ok=True)

tracked = subprocess.run(["git", "-C", root, "ls-files", "engine", "scripts", "analysis"],
                         capture_output=True, text=True, check=True).stdout.split("\n")
tracked = [t for t in tracked if t]
py = sorted(t for t in tracked if t.endswith(".py"))
pyset = set(py)
head = subprocess.run(["git", "-C", root, "rev-parse", "HEAD"], capture_output=True, text=True,
                      check=True).stdout.strip()


def nlines(p):
    with open(os.path.join(root, p), "rb") as f:
        return f.read().count(b"\n")


def resolve(dotted):
    """dotted module name -> tracked file, or None."""
    base = dotted.replace(".", "/")
    if base + ".py" in pyset:
        return base + ".py"
    if base + "/__init__.py" in pyset:
        return base + "/__init__.py"
    return None


def parents(dotted):
    parts = dotted.split(".")
    for i in range(1, len(parts)):
        r = resolve(".".join(parts[:i]))
        if r and r.endswith("__init__.py"):
            yield r


def pkg_of(path):
    """dotted package of a file (for relative imports)."""
    d = os.path.dirname(path).replace("/", ".")
    return d


class V(ast.NodeVisitor):
    def __init__(self, path):
        self.path, self.depth, self.edges, self.strings = path, 0, [], []

    def _fn(self, n):
        self.depth += 1
        self.generic_visit(n)
        self.depth -= 1
    visit_FunctionDef = visit_AsyncFunctionDef = visit_Lambda = _fn

    def add(self, dotted):
        r = resolve(dotted)
        level = "module" if self.depth == 0 else "function"
        if r:
            self.edges.append((r, level))
        for p in parents(dotted):
            self.edges.append((p, level))
        return r

    def visit_Import(self, n):
        for a in n.names:
            self.add(a.name)

    def visit_ImportFrom(self, n):
        mod = n.module or ""
        if n.level:
            base = pkg_of(self.path).split(".")
            base = base[:len(base) - (n.level - 1)] if n.level > 1 else base
            mod = ".".join([b for b in base if b] + ([mod] if mod else []))
        if not mod:
            return
        for a in n.names:
            if not self.add(mod + "." + a.name):   # `from pkg import submodule`?
                self.add(mod)
            else:
                self.add(mod)

    def visit_Constant(self, n):
        if isinstance(n.value, str) and len(n.value) < 400:
            self.strings.append(n.value)


edges, string_refs, parse_errors = {}, [], []
for p in py:
    try:
        tree = ast.parse(open(os.path.join(root, p), encoding="utf-8").read())
    except SyntaxError as e:
        parse_errors.append([p, str(e)])
        continue
    v = V(p)
    v.visit(tree)
    e = {}
    for tgt, level in v.edges:
        if tgt == p:
            continue
        if e.get(tgt) != "module":
            e[tgt] = level
    edges[p] = e
    for s in v.strings:
        for m in re.finditer(r"\b((?:engine|analysis|scripts)(?:\.[A-Za-z_][A-Za-z0-9_]*)+)\b", s):
            r = resolve(m.group(1))
            if r and r != p:
                string_refs.append([p, r, s[:120]])
        for m in re.finditer(r"\b(scripts/[A-Za-z0-9_]+\.py)\b", s):
            if m.group(1) in pyset and m.group(1) != p:
                string_refs.append([p, m.group(1), s[:120]])

# the declared dynamic edge
mig = [p for p in py if re.match(r"engine/migrations/\d{3}_.*\.py$", p)]
for m in mig:
    edges["engine/migrations/runner.py"].setdefault(m, "dynamic")


def closure(start, levels):
    seen, stack = set(), [start]
    while stack:
        x = stack.pop()
        if x in seen:
            continue
        seen.add(x)
        for tgt, level in edges.get(x, {}).items():
            if level in levels:
                stack.append(tgt)
    # running `python a/b/c.py` or `-m a.b.c` also executes the parent package __init__ files
    return seen


ALL, TOP = {"module", "function", "dynamic"}, {"module", "dynamic"}

# Entry points. RUN7 is the brief's; FT and MIG are the two the brief names; OTHER is every
# further command CLAUDE.md's "Running" block names that resolves to a unit under engine/ or
# scripts/ (analysis/ entry points are outside the census).
ENTRY = {
    "RUN7": ["scripts/run_pipeline.py"],
    "FT": ["engine/agents/ft_screener.py"],
    "MIG": ["engine/migrations/__main__.py"],
    "OTHER": [
        "scripts/screen_expanded.py",
        "engine/acquisition/check_oa.py",
        "engine/acquisition/download.py",
        "engine/acquisition/verify_downloads.py",
        "engine/acquisition/pdf_quality_check.py",
        "engine/acquisition/pdf_quality_import.py",
        "engine/validators/extraction_validator.py",
        "engine/tools/db_fingerprint.py",
        "engine/tools/inventory.py",
        "engine/utils/ollama_preflight.py",
        "scripts/run_cloud_extraction.py",
        "engine/validators/distribution_monitor.py",
        "scripts/backfill_cloud_spans.py",
        "scripts/q8_validation.py",
        "scripts/q8_validation_fast.py",
        "engine/adjudication/advance_stage.py",
    ],
}
missing_entry = [e for g in ENTRY.values() for e in g if e not in pyset]


def entry_closure(e, levels):
    c = closure(e, levels)
    parts = e[:-3].split("/")
    for i in range(1, len(parts)):
        init = "/".join(parts[:i]) + "/__init__.py"
        if init in pyset:
            c |= closure(init, levels)
    return c


def lazy_hops(start):
    """fewest in-function import edges on any path from the entry point (0 = import-time)."""
    from collections import deque
    dist, dq = {}, deque()
    seeds = [start]
    parts = start[:-3].split("/")
    for i in range(1, len(parts)):
        init = "/".join(parts[:i]) + "/__init__.py"
        if init in pyset:
            seeds.append(init)
    for s_ in seeds:
        dist[s_] = 0
        dq.append(s_)
    while dq:
        x = dq.popleft()
        for tgt, level in edges.get(x, {}).items():
            w = 1 if level == "function" else 0
            if tgt not in dist or dist[x] + w < dist[tgt]:
                dist[tgt] = dist[x] + w
                (dq.append if w else dq.appendleft)(tgt)
    return dist


reach = {}
for group, es in ENTRY.items():
    for e in es:
        if e in pyset:
            reach[e] = {"all": entry_closure(e, ALL), "top": entry_closure(e, TOP)}

hops7 = lazy_hops("scripts/run_pipeline.py")
units = [p for p in py if p.startswith(("engine/", "scripts/"))]
rows = []
for p in units:
    def flag(group):
        a = any(p in reach[e]["all"] for e in ENTRY[group] if e in reach)
        t = any(p in reach[e]["top"] for e in ENTRY[group] if e in reach)
        return "top" if t else ("lazy" if a else "-")
    other = sorted(e for e in ENTRY["OTHER"] if e in reach and p in reach[e]["all"])
    parts = p.split("/")
    package = "/".join(parts[:2]) if parts[0] == "engine" and len(parts) > 2 else parts[0]
    rows.append({"unit": p, "package": package, "lines": nlines(p),
                 "RUN7": flag("RUN7"), "RUN7_hops": hops7.get(p), "FT": flag("FT"), "MIG": flag("MIG"),
                 "OTHER_n": len(other), "OTHER": other,
                 "imported_by_n": sum(1 for q in units if p in edges.get(q, {}))})

# analysis/ at directory level, and the analysis modules engine/ or scripts/ import
an = {}
for p in py:
    if p.startswith("analysis/"):
        d = "/".join(p.split("/")[:2]) if "/" in p[len("analysis/"):] else "analysis"
        a = an.setdefault(d, {"files": 0, "lines": 0})
        a["files"] += 1
        a["lines"] += nlines(p)
an_imported = {}
for q in units:
    for tgt, level in edges.get(q, {}).items():
        if tgt.startswith("analysis/"):
            an_imported.setdefault(tgt, []).append([q, level])
run7_all = reach["scripts/run_pipeline.py"]["all"]
an_rows = [{"module": m, "lines": nlines(m), "imported_by": v,
            "RUN7": "top" if m in reach["scripts/run_pipeline.py"]["top"]
            else ("lazy" if m in run7_all else "-")}
           for m, v in sorted(an_imported.items())]
non_py = sorted(t for t in tracked if t.startswith(("engine/", "scripts/")) and not t.endswith(".py"))

res = {"head": head, "units": rows, "entry_points": ENTRY, "missing_entry": missing_entry,
       "parse_errors": parse_errors,
       "analysis_dirs": an, "analysis_imported_by_engine_or_scripts": an_rows,
       "analysis_reachable_run7": sorted(m for m in run7_all if m.startswith("analysis/")),
       "non_py_tracked": [[t, nlines(t)] for t in non_py],
       "string_refs": sorted({(a, b) for a, b, _ in string_refs
                              if a.startswith(("engine/", "scripts/"))}),
       "edges": {k: v for k, v in edges.items()},
       "totals": {"units": len(rows), "lines": sum(r["lines"] for r in rows),
                  "engine_units": sum(1 for r in rows if r["unit"].startswith("engine/")),
                  "engine_lines": sum(r["lines"] for r in rows if r["unit"].startswith("engine/")),
                  "scripts_units": sum(1 for r in rows if r["unit"].startswith("scripts/")),
                  "scripts_lines": sum(r["lines"] for r in rows if r["unit"].startswith("scripts/")),
                  "run7_units": sum(1 for r in rows if r["RUN7"] != "-"),
                  "run7_lines": sum(r["lines"] for r in rows if r["RUN7"] != "-"),
                  "run7_top_units": sum(1 for r in rows if r["RUN7"] == "top")}}
json.dump(res, open(os.path.join(out, "census.json"), "w"), indent=1, sort_keys=True)
with open(os.path.join(out, "census.csv"), "w", newline="") as f:
    w = csv.writer(f)
    cols = ("unit", "package", "lines", "RUN7", "RUN7_hops", "FT", "MIG", "OTHER_n", "imported_by_n")
    w.writerow(cols)
    for r in rows:
        w.writerow([r[k] for k in cols])
print(json.dumps(res["totals"]), "missing_entry", missing_entry, "parse_errors", parse_errors)
