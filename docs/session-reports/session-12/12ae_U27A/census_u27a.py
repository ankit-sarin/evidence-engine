"""12ae-U27A — AST census of U27's sites (C56's cloud handlers in engine/cloud/) and of the tests coupled to them, in
12ae_U26A/census_u26a.py's form. Read-only: AST only, no scanned module is imported, no database is opened, no key is read.
usage, from the repository root: census_u27a.py   (stdout: sections, then counts)
Sites: every `except` handler under engine/cloud/ with its caught type(s), its enclosing function and whether its body re-raises
(a bare `raise`, a `raise X from`, or none); and every caller of OpenAIExtractor, AnthropicExtractor, store_result, send,
extract_with_completeness and run_arm across engine/, scripts/ and analysis/, with its path class (run7 = the static import closure of
scripts/run_pipeline.py, engine/agents/ft_screener.py and engine/migrations/runner.py, plus engine/migrations/*.py; cloud = the closure of
scripts/run_cloud_extraction.py).
Tests: the classes of R722(a), R723(c), R724(a), R725(b) as 12ac_U22A.md §4 labels them, adapted to the cloud handlers: (i) direct names and
from-imports of the cloud modules' names; (ii) attribute access, and patch / setattr whose target string names engine.cloud. or a cloud
extractor attribute (patch.object on an extractor instance or class); (iii) the stats keys and log literals the handlers write ("failed",
"store_result failed", "failed after 3 attempts"); (iv) exact compares on a stats["failed"] / stats["extracted"] value; (v) the seams — calls
to .run(, store_result(, send(, extract_with_completeness( in tests, and every patch whose side_effect is a run-fault class (sqlite3.*,
ConnectionError, httpx.Connect*, CeilingUnavailable, UndeclaredCall, UndeclaredOverride, a RunRefused subclass, RunAborted, EventRefused);
(vi) a raise of a run-fault class in a test (fakes and wrappers); (vii) the module-level skips of tests/test_cloud_extraction.py (B14).
Declared completeness: direct names, attribute access, from-imports, patch-target strings, setattr on any receiver, literal strings and
keyword arguments. It does not see a side_effect bound to a variable, a patch whose target is not a literal, a fault injected by a fixture's
behaviour, or a name bound by a variable."""
import ast, collections, re
from pathlib import Path
NAMES = {"OpenAIExtractor", "AnthropicExtractor", "CloudExtractorBase", "store_result", "extract_with_completeness", "send", "_create",
         "run_arm", "select_cloud_arms", "get_pending_papers", "run_distribution_check", "parse_response_to_spans"}
LIT = {"failed", "store_result failed", "extracted"}
RUNFAULT = re.compile(r"\b(sqlite3\.\w+|ConnectionError|httpx\.Connect\w*|CeilingUnavailable|UndeclaredCall|UndeclaredOverride|RunRefused|"
                      r"ModelDigestChanged|ServerEnvChanged|LoadedContextMismatch|StageNotInRun|RunAborted|EventRefused|OperationalError)\b")
def scopes(tree):
    out = {}
    def walk(node, name):
        for ch in ast.iter_child_nodes(node):
            n = f"{name}.{ch.name}" if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) else name
            out[id(ch)] = n; walk(ch, n)
    walk(tree, "<module>"); return out
def line(src, n): return src.splitlines()[n - 1].strip()[:150]
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
RUN7 = closure([Path("scripts/run_pipeline.py"), Path("engine/agents/ft_screener.py"), Path("engine/migrations/runner.py")]) | set(Path("engine/migrations").glob("*.py"))
CLOUD = closure([Path("scripts/run_cloud_extraction.py")])
def klass(p):
    s = str(p); area = "analysis" if s.startswith("analysis") else "scripts" if s.startswith("scripts") else "engine"
    tags = (["run7"] if Path(s) in RUN7 else []) + (["cloud"] if Path(s) in CLOUD else [])
    return "/".join(tags + [area])
print("== the static closures"); print(f"modules in the run7 closure: {len(RUN7)}; in the run_cloud_extraction closure: {len(CLOUD)}")
cloudmods = sorted(Path("engine/cloud").glob("*.py"))
print("engine/cloud modules in the run7 closure: " + (", ".join(str(p) for p in cloudmods if p in RUN7) or "none"))
print("\n== every except handler under engine/cloud/")
rows = []
for P in cloudmods:
    src = P.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
    for n in ast.walk(t):
        if isinstance(n, ast.ExceptHandler):
            caught = ast.unparse(n.type) if n.type is not None else "(bare)"
            raises = [ast.unparse(r)[:60] or "raise" for r in ast.walk(ast.Module(body=n.body, type_ignores=[])) if isinstance(r, ast.Raise)]
            rows.append((str(P), n.lineno, sc.get(id(n), ""), caught, raises or ["(none)"], line(src, n.lineno)))
for r in sorted(rows): print(f"  {r[0]}:{r[1]} {r[2]} | catches {r[3]} | raises in body: {r[4]} | {r[5]}")
sites = collections.defaultdict(list)
for root in ("engine", "scripts", "analysis"):
    for p in sorted(Path(root).rglob("*.py")):
        s2 = p.read_text(encoding="utf-8"); t2 = ast.parse(s2); sc2 = scopes(t2)
        for n in ast.walk(t2):
            if isinstance(n, ast.Call):
                tail = ast.unparse(n.func).rsplit(".", 1)[-1]
                if tail in ("OpenAIExtractor", "AnthropicExtractor", "store_result", "send", "extract_with_completeness", "run_arm") or \
                   (tail == "run" and "extractor" in ast.unparse(n.func).lower()):
                    sites[f"call {tail}"].append((klass(p), f"{p}:{n.lineno}", sc2.get(id(n), ""), line(s2, n.lineno)))
            if isinstance(n, ast.Name) and n.id in ("OpenAIExtractor", "AnthropicExtractor") and not str(p).startswith("engine/cloud"):
                sites["name OpenAIExtractor/AnthropicExtractor outside engine/cloud"].append((klass(p), f"{p}:{n.lineno}", sc2.get(id(n), ""), line(s2, n.lineno)))
print("\n== callers (engine, scripts, analysis)")
for key in sorted(sites):
    rs = sorted(set(sites[key])); print(f"\n-- {key}: {len(rs)}")
    for r in rs: print(f"  [{r[0]}] {r[1]} {r[2]} | {r[3]}")
T = collections.defaultdict(list)
for p in sorted(Path("tests").rglob("*.py")):
    s3 = p.read_text(encoding="utf-8"); t3 = ast.parse(s3); sc3 = scopes(t3)
    for n in ast.walk(t3):
        if not hasattr(n, "lineno"): continue
        rec = (f"{p}:{n.lineno}", sc3.get(id(n), ""), line(s3, n.lineno))
        if isinstance(n, ast.Name) and n.id in NAMES: T["(i) direct names"].append(rec)
        if isinstance(n, ast.ImportFrom) and n.module and n.module.startswith("engine.cloud"): T["(i) from-imports of engine.cloud.*"].append(rec)
        if isinstance(n, ast.Import) and any(a.name.startswith("engine.cloud") for a in n.names): T["(i) from-imports of engine.cloud.*"].append(rec)
        if isinstance(n, ast.Attribute) and n.attr in NAMES: T["(ii) attribute access to a cloud name"].append(rec)
        if isinstance(n, ast.Call):
            f = ast.unparse(n.func); tail = f.rsplit(".", 1)[-1]
            if tail in ("setattr", "patch", "object") or f.endswith("patch"):
                vals = [a.value for a in n.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]
                tgt = ast.unparse(n.args[0]) if n.args else ""
                if any(v.startswith("engine.cloud") for v in vals) or (tail in ("object", "setattr") and (
                        re.search(r"extractor|Extractor|base_mod|oe|ae|cloud", tgt) or any(v in NAMES for v in vals))):
                    T["(ii) patch or setattr on engine.cloud / a cloud extractor"].append(rec)
                se = [k for k in n.keywords if k.arg == "side_effect"]
                if se and RUNFAULT.search(ast.unparse(se[0].value)) and (any(v.startswith("engine.cloud") for v in vals) or tail in ("object", "setattr")):
                    T["(v) a patch whose side_effect is a run-fault class"].append(rec + (ast.unparse(se[0].value)[:60],))
            if tail in ("run", "store_result", "send", "extract_with_completeness", "run_arm") and (tail != "run" or re.search(r"extractor|ext\b|oe|ae", f, re.I)):
                T[f"(v) seam call {tail}"].append(rec)
            if tail in ("skipif", "skip") or f.endswith("pytest.skip"):
                if "test_cloud_extraction" in str(p): T["(vii) skips in tests/test_cloud_extraction.py"].append(rec)
        if isinstance(n, ast.Assign) and any(ast.unparse(tg) == "pytestmark" for tg in n.targets):
            T["(vii) module pytestmark"].append(rec)
        if isinstance(n, ast.Raise) and n.exc is not None and RUNFAULT.search(ast.unparse(n.exc)) and re.search(r"cloud|extract", str(p)):
            T["(vi) a raise of a run-fault class in a cloud/extract test"].append(rec + (ast.unparse(n.exc)[:60],))
        if isinstance(n, ast.Constant) and isinstance(n.value, str) and n.value in LIT and re.search(r"cloud", str(p)):
            T["(iii) a stats key or log literal in a cloud test"].append(rec + (n.value,))
        if isinstance(n, ast.Compare) and any(isinstance(o, ast.Eq) for o in n.ops):
            u = ast.unparse(n)
            if re.search(r"\[['\"](failed|extracted)['\"]\]", u): T["(iv) exact compare on stats['failed'|'extracted']"].append(rec)
print("\n== tests")
for key in sorted(T):
    rs = sorted(set(T[key])); files = len({r[0].split(":")[0] for r in rs})
    print(f"\n-- {key}: {len(rs)} hits / {files} files")
    c = collections.Counter(r[0].split(":")[0] for r in rs)
    print("   by file: " + ", ".join(f"{a.replace('tests/', '')} {b}" for a, b in c.most_common(25)))
    for r in rs[:200]: print(f"  {r[0]} {r[1]} | {r[2]}" + (f" | {r[3]}" if len(r) > 3 else ""))
print("\n== counts")
print(f"engine/cloud except handlers: {len(rows)}")
for key in sorted(sites): print(f"callers {key}: {len(set(sites[key]))}")
for key in sorted(T):
    rs = set(T[key]); print(f"tests {key}: {len(rs)} / {len({r[0].split(':')[0] for r in rs})}")
