"""12ae-U26A — AST census of U26's sites (C56's parse handlers in engine/parsers/pdf_parser.py) and of the tests coupled to them, in
12ae_U25A/census_u25a.py's form. Read-only: AST only, no scanned module is imported, no database is opened.
usage, from the repository root: census_u26a.py   (stdout: sections, then counts)
Sites: every `except` handler in engine/parsers/pdf_parser.py with its caught type(s), its enclosing function and whether its body re-raises
(a bare `raise`, a `raise X from`, or none); and every caller of parse_all_pdfs, parse_pdf, reparse_papers and _commit_attempts across
engine/, scripts/ and analysis/, with its path class (run7 = the static import closure of scripts/run_pipeline.py,
engine/agents/ft_screener.py and engine/migrations/runner.py, plus engine/migrations/*.py).
Tests: the classes of R722(a), R723(c), R724(a), R725(b) as 12ac_U22A.md §4 labels them, adapted to the parse handlers: (i) direct names and
from-imports of the parser's names; (ii) attribute access, and patch / setattr whose target string names engine.parsers.pdf_parser; (iii) the
parse reason-code constants and literals; (iv) exact compares naming a parse reason code or `parse_failed`; (v) the seams — calls to
parse_all_pdfs / parse_pdf / reparse_papers in tests, and every patch on pdf_parser whose side_effect is a run-fault class (sqlite3.*,
ConnectionError, httpx.Connect*, CeilingUnavailable, UndeclaredCall, UndeclaredOverride, a RunRefused subclass, RunAborted, EventRefused);
(vi) a connection wrapper that raises inside parse_attempts' INSERT (test_parse_gate.py's form).
Declared completeness: direct names, attribute access, from-imports, patch-target strings, setattr on any receiver, literal strings and
keyword arguments. It does not see a side_effect bound to a variable, a patch whose target is not a literal, a fault injected by a fixture's
behaviour, or a name bound by a variable."""
import ast, collections, re
from pathlib import Path
NAMES = {"parse_all_pdfs", "parse_pdf", "reparse_papers", "_commit_attempts", "_insert_attempts", "parse_with_vision", "_run_parser",
         "_next_parser", "ParseFailed", "select_attempt", "assess"}
REASONS = {"REASON_PARSE_VISION_EXHAUSTED", "REASON_PARSE_UNCLASSIFIED_ERROR", "REASON_PARSE_CASCADE_EMPTY", "REASON_PARSE_PYMUPDF_EXHAUSTED",
           "REASON_PARSE_FILE_UNREADABLE"}
REASON_LIT = {"parse_vision_exhausted", "parse_unclassified_error", "parse_cascade_empty", "parse_pymupdf_exhausted", "parse_file_unreadable", "parse_failed"}
RUNFAULT = re.compile(r"\b(sqlite3\.\w+|ConnectionError|httpx\.Connect\w*|CeilingUnavailable|UndeclaredCall|UndeclaredOverride|RunRefused|"
                      r"ModelDigestChanged|ServerEnvChanged|LoadedContextMismatch|StageNotInRun|RunAborted|EventRefused)\b")
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
def klass(p):
    s = str(p); area = "analysis" if s.startswith("analysis") else "scripts" if s.startswith("scripts") else "engine"
    return f"run7/{area}" if Path(s) in RUN7 else area
print("== the static closure"); print(f"modules in the run7 closure: {len(RUN7)}")
P = Path("engine/parsers/pdf_parser.py"); src = P.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
print("\n== every except handler in engine/parsers/pdf_parser.py")
rows = []
for n in ast.walk(t):
    if isinstance(n, ast.ExceptHandler):
        caught = ast.unparse(n.type) if n.type is not None else "(bare)"
        raises = [ast.unparse(r)[:60] or "raise" for r in ast.walk(ast.Module(body=n.body, type_ignores=[])) if isinstance(r, ast.Raise)]
        rows.append((n.lineno, sc.get(id(n), ""), caught, raises or ["(none)"], line(src, n.lineno)))
for r in sorted(rows): print(f"  pdf_parser.py:{r[0]} {r[1]} | catches {r[2]} | raises in body: {r[3]} | {r[4]}")
sites = collections.defaultdict(list)
for root in ("engine", "scripts", "analysis"):
    for p in sorted(Path(root).rglob("*.py")):
        s2 = p.read_text(encoding="utf-8"); t2 = ast.parse(s2); sc2 = scopes(t2)
        for n in ast.walk(t2):
            if isinstance(n, ast.Call):
                tail = ast.unparse(n.func).rsplit(".", 1)[-1]
                if tail in ("parse_all_pdfs", "parse_pdf", "reparse_papers", "_commit_attempts"):
                    kw = {k.arg for k in n.keywords}
                    sites[f"call {tail}" + (", run_id passed" if "run_id" in kw else ", no run_id" if tail == "parse_all_pdfs" else "")].append(
                        (klass(p), f"{p}:{n.lineno}", sc2.get(id(n), ""), line(s2, n.lineno)))
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
        if isinstance(n, ast.ImportFrom) and (n.module == "engine.parsers.pdf_parser"): T["(i) from-imports of pdf_parser"].append(rec)
        if isinstance(n, ast.Attribute) and n.attr in NAMES: T["(ii) attribute access to a parser name"].append(rec)
        if isinstance(n, ast.Call):
            f = ast.unparse(n.func); tail = f.rsplit(".", 1)[-1]
            if tail in ("setattr", "patch", "object") or f.endswith("patch"):
                vals = [a.value for a in n.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]
                if any("engine.parsers.pdf_parser" in v for v in vals) or (tail == "object" and n.args and ast.unparse(n.args[0]) in ("pp", "pdf_parser", "P")):
                    T["(ii) patch or setattr on engine.parsers.pdf_parser"].append(rec)
                    se = [k for k in n.keywords if k.arg == "side_effect"]
                    if se and RUNFAULT.search(ast.unparse(se[0].value)): T["(v) a pdf_parser patch whose side_effect is a run-fault class"].append(rec + (ast.unparse(se[0].value)[:60],))
            if tail in ("parse_all_pdfs", "parse_pdf", "reparse_papers"): T[f"(v) seam call {tail}"].append(rec)
        if isinstance(n, ast.Raise) and n.exc is not None and RUNFAULT.search(ast.unparse(n.exc)):
            T["(vi) a raise of a run-fault class in a test (fakes and wrappers)"].append(rec + (ast.unparse(n.exc)[:60],))
        if isinstance(n, ast.Name) and n.id in REASONS: T["(iii) a parse reason-code constant"].append(rec + (n.id,))
        if isinstance(n, ast.Attribute) and n.attr in REASONS: T["(iii) a parse reason-code constant"].append(rec + (n.attr,))
        if isinstance(n, ast.Constant) and isinstance(n.value, str) and n.value in REASON_LIT: T["(iii) a parse reason-code literal"].append(rec + (n.value,))
        if isinstance(n, ast.Compare) and any(isinstance(o, ast.Eq) for o in n.ops):
            u = ast.unparse(n)
            if any(r in u for r in REASONS | REASON_LIT): T["(iv) exact compare naming a parse reason code or parse_failed"].append(rec)
print("\n== tests")
for key in sorted(T):
    rs = sorted(set(T[key])); files = len({r[0].split(":")[0] for r in rs})
    print(f"\n-- {key}: {len(rs)} hits / {files} files")
    c = collections.Counter(r[0].split(":")[0] for r in rs)
    print("   by file: " + ", ".join(f"{a.replace('tests/', '')} {b}" for a, b in c.most_common(20)))
    for r in rs[:200]: print(f"  {r[0]} {r[1]} | {r[2]}" + (f" | {r[3]}" if len(r) > 3 else ""))
print("\n== counts")
print(f"pdf_parser.py except handlers: {len(rows)}")
for key in sorted(sites): print(f"callers {key}: {len(set(sites[key]))}")
for key in sorted(T):
    rs = set(T[key]); print(f"tests {key}: {len(rs)} / {len({r[0].split(':')[0] for r in rs})}")
