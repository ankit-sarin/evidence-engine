"""12af-U40A — AST census for U40's Phase A ("The cloud path writes claim events"; B9, C28; Wave 2 row 25), in
12ae_U28A/census_u28a.py's form. Read-only: AST only, no scanned module is imported, no database is opened.
usage, from the repository root: python3 -I census_u40a.py   (stdout: sections, then counts)
Sites, across engine/, scripts/ and analysis/ (each with its path class: run7 = the static import closure of scripts/run_pipeline.py,
engine/agents/ft_screener.py and engine/migrations/runner.py, plus engine/migrations/*.py; cloud = the closure of
scripts/run_cloud_extraction.py):
  (a) calls of the event writer: write_extraction_events, and of the two row writers it uses, write_field_event and write_paper_event;
  (b) calls of each reader function of the brief's R4 — is_assigned, live_claim_events, effective_value, registered_arms,
      eligible_paper_ids, iter_grid — and of four neighbours the trace needs: effective_state, grid_cells, corpus_id_sql, live_claims;
  (c) the files outside engine/core/effective.py that NAME iter_grid, effective_value, live_claim_events or registered_arms (a Name,
      an Attribute or an import alias: the brief's R5 grep, done by AST);
  (d) SQL string literals naming field_events, cloud_extractions or cloud_evidence_spans, classed SELECT / INSERT / DDL / other
      (a literal is the whole Constant, or an f-string's literal parts joined); engine/migrations/ is listed apart;
  (e) arm tests: a registered_arms call with kind=; an attribute read of .enabled_arms / .cloud_arms / .arm_kind / .provider; a
      .startswith on a receiver whose source names "arm"; a Compare whose source names "arm" against a string literal; string
      literals "human_", "model", "human_extractor", "local", or starting "cloud:" in a Compare or a call argument.
Tests: functions in tests/ that name store_result, write_extraction_events, or any consumer function the read-out marks CHANGES
(the list CHANGES below), and get_pending_papers, which a §7 option names (ALSO), with the fixtures each test function takes as parameters and the module-level helpers it calls.
Declared completeness: direct names, attribute access, import aliases, keyword arguments, literal strings and f-string literal
parts. It does not see a name bound by a variable or passed as a value, SQL built by concatenation across statements, a reader
reached through a wrapper the census does not name, or behaviour injected by a fixture."""
import ast, collections, re
from pathlib import Path

WRITER = {"write_extraction_events", "write_field_event", "write_paper_event"}
READERS = {"is_assigned", "live_claim_events", "effective_value", "registered_arms", "eligible_paper_ids", "iter_grid",
           "effective_state", "grid_cells", "corpus_id_sql", "live_claims"}
R5 = {"iter_grid", "effective_value", "live_claim_events", "registered_arms"}
TABLES = ("field_events", "cloud_extractions", "cloud_evidence_spans")
#: consumer functions the read-out marks CHANGES under A1 (its §3 table); tests naming one are listed under "tests coupled".
CHANGES = {"store_result", "write_extraction_events", "audit_run", "_advance_extraction_workflow", "_processing_states",
           "_build_evidence_rows", "export_evidence_csv", "export_evidence_excel", "export_evidence_docx", "generate_prisma_flow",
           "validate_prisma_counts", "get_pipeline_stats", "incomplete_extractions", "record_selection_refusals", "load_arm",
           "run_concordance", "run_all_pairs", "load_grid", "_fetch_spans_for_paper", "validate_all", "arm_cells",
           "check_distribution", "run_post_extraction_check", "arm_population", "run_distribution_check"}
#: named by an option in the read-out's §7 (resume moved to selection), listed with the tests coupled but not marked CHANGES.
ALSO = {"get_pending_papers"}


def scopes(tree):
    out = {}
    def walk(node, name):
        for ch in ast.iter_child_nodes(node):
            n = f"{name}.{ch.name}" if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) else name
            out[id(ch)] = n; walk(ch, n)
    walk(tree, "<module>"); return out


def line(src, n): return src.splitlines()[n - 1].strip()[:160]


def mod_path(name):
    p = Path(*name.split("."))
    for c in (p.with_suffix(".py"), p / "__init__.py"):
        if c.exists(): return c
    return None


def closure(starts):
    seen, todo = set(), list(starts)
    while todo:
        p = todo.pop()
        if p in seen or not p.exists(): continue
        seen.add(p); t = ast.parse(p.read_text(encoding="utf-8"))
        for n in ast.walk(t):
            names = []
            if isinstance(n, ast.Import): names = [a.name for a in n.names]
            elif isinstance(n, ast.ImportFrom) and n.module and n.level == 0:
                names = [n.module] + [f"{n.module}.{a.name}" for a in n.names]
            for nm in names:
                if nm.split(".")[0] in ("engine", "analysis"):
                    m = mod_path(nm)
                    if m: todo.append(m)
    return seen


RUN7 = closure([Path("scripts/run_pipeline.py"), Path("engine/agents/ft_screener.py"), Path("engine/migrations/runner.py")]) \
    | set(Path("engine/migrations").glob("*.py"))
CLOUD = closure([Path("scripts/run_cloud_extraction.py")])


def klass(p):
    s = str(p); area = "analysis" if s.startswith("analysis") else "scripts" if s.startswith("scripts") else "engine"
    tags = (["run7"] if Path(s) in RUN7 else []) + (["cloud"] if Path(s) in CLOUD else [])
    return "/".join(tags + [area])


def literal_text(n):
    if isinstance(n, ast.Constant) and isinstance(n.value, str): return n.value
    if isinstance(n, ast.JoinedStr):
        return "".join(v.value for v in n.values if isinstance(v, ast.Constant) and isinstance(v.value, str))
    return None


def sql_class(s):
    u = s.upper()
    if re.search(r"\bCREATE\b|\bALTER\b|\bDROP\b", u): return "DDL"
    if re.search(r"\bINSERT\b", u): return "INSERT"
    if re.search(r"\bUPDATE\b|\bDELETE\b", u): return "UPDATE/DELETE"
    if re.search(r"\bSELECT\b", u): return "SELECT"
    return "other (prose or a name)"


print("== the static closures")
print(f"modules in the run7 closure: {len(RUN7)}; in the run_cloud_extraction closure: {len(CLOUD)}")
for f in ("engine/core/extraction_events.py", "engine/core/events.py", "engine/core/effective.py", "engine/cloud/base.py",
          "engine/validators/distribution_monitor.py", "engine/agents/auditor.py", "engine/analysis/concordance.py"):
    print(f"  {f}: run7 {Path(f) in RUN7}; cloud {Path(f) in CLOUD}")

sites = collections.defaultdict(set); namers = collections.defaultdict(set)
JOINED = set()  # ids of Constant nodes inside a JoinedStr, so they are not counted twice
for root in ("engine", "scripts", "analysis"):
    for p in sorted(Path(root).rglob("*.py")):
        src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
        mig = str(p).startswith("engine/migrations/")
        for n in ast.walk(t):
            if isinstance(n, ast.JoinedStr):
                for v in n.values: JOINED.add(id(v))
        for n in ast.walk(t):
            rec = lambda: (klass(p), f"{p}:{n.lineno}", sc.get(id(n), ""), line(src, n.lineno))
            if isinstance(n, ast.Call):
                f = ast.unparse(n.func); tail = f.rsplit(".", 1)[-1]
                if tail in WRITER: sites[f"(a) call {tail}"].add(rec())
                if tail in READERS: sites[f"(b) call {tail}"].add(rec())
                if tail == "registered_arms" and any(k.arg == "kind" for k in n.keywords):
                    sites["(e) registered_arms(kind=…)"].add(rec())
                if tail == "startswith" and isinstance(n.func, ast.Attribute) and "arm" in ast.unparse(n.func.value).lower():
                    sites["(e) .startswith on an arm"].add(rec())
                for a in n.args:
                    s = literal_text(a)
                    if s is not None and (s in ("human_", "model", "human_extractor", "local") or s.startswith("cloud:")):
                        sites["(e) an arm literal as a call argument"].add(rec())
            if isinstance(n, (ast.Name, ast.Attribute, ast.alias)):
                nm = n.id if isinstance(n, ast.Name) else n.attr if isinstance(n, ast.Attribute) else (n.name.rsplit(".", 1)[-1])
                if nm in R5 and str(p) != "engine/core/effective.py":
                    namers[str(p)].add(nm)
            if isinstance(n, ast.Attribute) and n.attr in ("enabled_arms", "cloud_arms", "arm_kind", "provider"):
                sites[f"(e) attribute .{n.attr}"].add(rec())
            if isinstance(n, ast.Compare):
                u = ast.unparse(n)
                if "arm" in u.lower() and any(literal_text(c) is not None for c in [n.left, *n.comparators]):
                    sites["(e) a compare of an arm against a literal"].add(rec())
            s = literal_text(n) if isinstance(n, (ast.Constant, ast.JoinedStr)) and id(n) not in JOINED else None
            if s is not None:
                for tb in TABLES:
                    if re.search(rf"\b{tb}\b", s):
                        key = f"(d) {'[migrations] ' if mig else ''}{tb} — {sql_class(s)}"
                        sites[key].add(rec())

print("\n== (c) files outside engine/core/effective.py naming iter_grid, effective_value, live_claim_events or registered_arms")
for f in sorted(namers): print(f"  [{klass(Path(f))}] {f}: {', '.join(sorted(namers[f]))}")
print(f"  files: {len(namers)}")

print("\n== sites (engine, scripts, analysis)")
for key in sorted(sites):
    rs = sorted(sites[key]); print(f"\n-- {key}: {len(rs)}")
    for r in rs: print(f"  [{r[0]}] {r[1]} {r[2]} | {r[3]}")

# tests coupled
T = collections.defaultdict(set)
for p in sorted(Path("tests").rglob("*.py")):
    src = p.read_text(encoding="utf-8"); t = ast.parse(src)
    helpers = {n.name for n in t.body if isinstance(n, ast.FunctionDef) and not n.name.startswith("test_")}
    fixtures = {n.name for n in ast.walk(t) if isinstance(n, ast.FunctionDef)
                and any("fixture" in ast.unparse(d) for d in n.decorator_list)}
    for fn in ast.walk(t):
        if not (isinstance(fn, ast.FunctionDef) and fn.name.startswith("test_")): continue
        used = set()
        for n in ast.walk(fn):
            if isinstance(n, ast.Name): used.add(n.id)
            elif isinstance(n, ast.Attribute): used.add(n.attr)
            elif isinstance(n, ast.Constant) and isinstance(n.value, str):
                used |= {x for x in CHANGES if x in n.value}
        hit = sorted(used & (CHANGES | ALSO))
        if not hit: continue
        params = [a.arg for a in fn.args.args if a.arg != "self"]
        called = sorted(used & helpers)
        for h in hit:
            T[h].add((f"{p}:{fn.lineno}", fn.name, ",".join(params) or "-", ",".join(called) or "-",
                      ",".join(sorted(set(params) & fixtures)) or "-"))
print("\n== tests coupled (functions in tests/ naming store_result, the writer, or a CHANGES consumer)")
for key in sorted(T):
    rs = sorted(T[key]); files = collections.Counter(r[0].split(":")[0] for r in rs)
    print(f"\n-- {key}: {len(rs)} test functions / {len(files)} files")
    print("   by file: " + ", ".join(f"{a.replace('tests/', '')} {b}" for a, b in files.most_common()))
    for r in rs: print(f"  {r[0]} {r[1]} | params: {r[2]} | module helpers called: {r[3]} | file-local fixtures: {r[4]}")

print("\n== counts")
print(f"files naming the R5 functions: {len(namers)}")
for key in sorted(sites): print(f"sites {key}: {len(sites[key])}")
for key in sorted(T): print(f"tests {key}: {len(T[key])} / {len({r[0].split(':')[0] for r in T[key]})}")
