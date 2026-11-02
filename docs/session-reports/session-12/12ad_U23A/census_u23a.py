"""12ad-U23A — AST census of U23's sites (E-ROOT, N30: spec, data and git roots) and of the tests coupled to them, in
12ac_U22A/census_u22a.py's form. Read-only: AST only, no scanned module is imported, no database is opened.
usage, from the repository root: census_u23a.py   (stdout: sections, then counts)
Sites: engine/, scripts/, analysis/ — definitions and reads of SPEC_ROOT, DATA_ROOT, REPO_ROOT, PROJECT_ROOT; relative roots written as
Path("...") or as a literal beginning data/ or review_specs/; __file__-derived roots; the parameters that bypass the constants and who
passes them; the callers of the derivers and of ReviewDatabase. Each row carries the path class: run7 = in the static import closure
(lazy imports included) of scripts/run_pipeline.py, engine/agents/ft_screener.py or engine/migrations/runner.py, plus engine/migrations/*.py,
which the runner loads by file; else acquisition, adjudication, analysis, scripts, tools, or engine-other.
Tests: the classes of R722(a), R723(c), R724(a), R725(b) as 12ac_U22A's §4 labels them, (iv) with R772(d)'s set(...) == form; plus chdir
sites, ReviewDatabase constructions by whether data_root is passed, and working-directory-relative literals.
Declared completeness: direct names, attribute access, from-imports, patch-target strings, setattr on any receiver, literal strings and keyword
arguments. It does not see a name assembled at runtime, a path built by joining variables, a patch whose target is not a literal, or a
test coupled only through a fixture's behaviour (the conftest guard is read by hand, not by this script)."""
import ast, collections, re
from pathlib import Path
ROOTS = {"SPEC_ROOT", "DATA_ROOT", "REPO_ROOT", "PROJECT_ROOT", "LIVE_DATA_ROOT"}
DERIVERS = {"spec_path_for", "data_root_for", "load_spec_for", "codebook_path_for", "load_codebook_for", "load_codebook_beside", "git_state", "canonical_path", "resolve_stored_path", "resolved_review_db_path", "is_live_data_path"}
PARAMS = {"data_root", "repo_root", "override", "spec_path", "review_root", "codebook_path"}
BODY = {"run_uid", "review_id", "run_kind", "git", "engine_state", "spec_hash", "codebook", "libraries", "host", "ollama_host", "started_at", "cloud_arms", "payload_description", "stages", "pins"}
COLS = {"git_commit", "git_dirty", "git_tag", "engine_state"}
LIT = re.compile(r"^(\./)?(data|review_specs)(/|$)")
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
    s = str(p)
    tag = "run7" if Path(s) in RUN7 else None
    area = "acquisition" if s.startswith("engine/acquisition") else "adjudication" if s.startswith("engine/adjudication") else "analysis" if s.startswith("analysis") else "scripts" if s.startswith("scripts") else "tools" if s.startswith("engine/tools") else "migrations" if s.startswith("engine/migrations") else "engine-other"
    return f"run7/{area}" if tag else area
print("== the static closure"); print(f"modules in the run7 closure: {len(RUN7)}"); print(" ".join(sorted(str(p) for p in RUN7)))
sites = collections.defaultdict(list)
def is_file_root(v):
    u = ast.unparse(v)
    return "__file__" in u
for root in ("engine", "scripts", "analysis"):
    for p in sorted(Path(root).rglob("*.py")):
        src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t); k = klass(p)
        for n in ast.walk(t):
            if not hasattr(n, "lineno"): continue
            rec = (k, f"{p}:{n.lineno}", sc.get(id(n), ""), line(src, n.lineno))
            if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name):
                nm = n.targets[0].id
                if nm in ROOTS or (nm.endswith(("ROOT", "_DIR")) and is_file_root(n.value)):
                    sites[f"definition {nm} ({'module file' if is_file_root(n.value) else 'cwd' if 'Path(' in ast.unparse(n.value) and not is_file_root(n.value) else '?'})"].append(rec)
                elif is_file_root(n.value) and ("resolve" in ast.unparse(n.value) or "parent" in ast.unparse(n.value)):
                    sites[f"definition other __file__-derived ({nm})"].append(rec)
            if isinstance(n, ast.ImportFrom) and any(a.name in ROOTS for a in n.names):
                sites["import of a root name"].append(rec)
            if isinstance(n, ast.Name) and n.id in ROOTS and isinstance(n.ctx, ast.Load):
                sites[f"read {n.id}"].append(rec)
            if isinstance(n, ast.Call):
                f = ast.unparse(n.func); tail = f.rsplit(".", 1)[-1]
                if tail == "Path" and n.args and isinstance(n.args[0], ast.Constant) and isinstance(n.args[0].value, str) and not n.args[0].value.startswith(("/", "~")):
                    sites["relative Path(\"...\") literal (cwd)"].append(rec)
                if tail in DERIVERS: sites[f"call {tail}"].append(rec)
                if tail == "ReviewDatabase":
                    kw = [k2.arg for k2 in n.keywords]; pos = len(n.args)
                    sites["call ReviewDatabase, data_root passed" if ("data_root" in kw or pos >= 2) else "call ReviewDatabase, no data_root (DATA_ROOT, cwd)"].append(rec)
                for k2 in n.keywords:
                    if k2.arg in PARAMS: sites[f"keyword passed {k2.arg}="].append(rec)
                if tail in ("chdir", "getcwd", "cwd"): sites["cwd read or chdir in code"].append(rec)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                a = [x.arg for x in n.args.args + n.args.kwonlyargs if x.arg in PARAMS]
                if a: sites[f"parameter {','.join(a)}"].append(rec)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and LIT.match(n.value):
                sites["literal beginning data/ or review_specs/"].append(rec)
            if isinstance(n, ast.JoinedStr) and n.values and isinstance(n.values[0], ast.Constant) and isinstance(n.values[0].value, str) and LIT.match(n.values[0].value):
                sites["f-string beginning data/ or review_specs/"].append(rec)
print("\n== sites (engine, scripts, analysis)")
for key in sorted(sites):
    rows = sorted(set(sites[key])); print(f"\n-- {key}: {len(rows)}")
    for r in rows: print(f"  [{r[0]}] {r[1]} {r[2]} | {r[3]}")
T = collections.defaultdict(list)
ALLP = sorted(Path("tests").rglob("*.py"))
for p in ALLP:
    src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
    for n in ast.walk(t):
        if not hasattr(n, "lineno"): continue
        rec = (f"{p}:{n.lineno}", sc.get(id(n), ""), line(src, n.lineno))
        if isinstance(n, ast.Name) and n.id in ROOTS | DERIVERS: T["(i) direct names"].append(rec + (n.id,))
        if isinstance(n, ast.ImportFrom) and any(a.name in ROOTS | DERIVERS for a in n.names): T["(i) from-imports"].append(rec)
        if isinstance(n, ast.Attribute) and n.attr in ROOTS: T["(ii) attribute access on an alias (e.g. db_module.DATA_ROOT)"].append(rec + (ast.unparse(n),))
        if isinstance(n, ast.Attribute) and n.attr in DERIVERS: T["(ii) attribute access to a deriver"].append(rec + (ast.unparse(n),))
        if isinstance(n, ast.Call):
            f = ast.unparse(n.func); tail = f.rsplit(".", 1)[-1]
            if tail in ("setattr", "patch", "object") or f.endswith("patch"):
                vals = [a.value for a in n.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]
                if any(any(x in v for x in ROOTS | DERIVERS | {"ReviewDatabase.__init__"}) for v in vals):
                    T["(ii) patch or setattr naming a root or deriver"].append(rec)
            if tail == "chdir": T["chdir sites"].append(rec)
            if tail == "ReviewDatabase":
                kw = [k2.arg for k2 in n.keywords]; pos = len(n.args)
                T["ReviewDatabase, data_root passed" if ("data_root" in kw or pos >= 2) else "ReviewDatabase, no data_root"].append(rec)
            if tail == "open_run" and any(k2.arg == "git" for k2 in n.keywords): T["(v) open_run(git=...) seam"].append(rec)
        if isinstance(n, ast.Compare) and any(isinstance(o, ast.Eq) for o in n.ops):
            sides = [n.left] + n.comparators; u = " ".join(ast.unparse(s) for s in sides)
            lits = {x.value for s in sides for x in ast.walk(s) if isinstance(x, ast.Constant) and isinstance(x.value, str)}
            setform = any(isinstance(s, (ast.Set, ast.Dict)) for s in sides) or any(isinstance(s, ast.Call) and ast.unparse(s.func) in ("set", "sorted", "frozenset") for s in sides)
            if setform and (lits & (BODY | COLS)): T["(iv) exact set/dict/sorted compare over a manifest body key or git column (set(...) == included, R772(d))"].append(rec)
            if any(isinstance(s, ast.Call) and ast.unparse(s.func) == "set" for s in sides) and any(isinstance(x, ast.Name) and x.id.endswith("KEYS") for s in sides for x in ast.walk(s)): T["(iv) set(...) == a named key set (R772(d)'s form, the literal elsewhere)"].append(rec)
            if "Path(" in u and any(LIT.match(v) for v in lits): T["(iv) exact compare to a relative Path"].append(rec)
            if any(LIT.match(v) for v in lits): T["(iv) exact compare to a data/ or review_specs/ literal"].append(rec)
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            if n.value in COLS: T["(iii) manifest git-column literal"].append(rec + (n.value,))
            if n.value == "git": T["(iii) manifest body 'git' key literal"].append(rec)
            if LIT.match(n.value): T["(vi) literal beginning data/ or review_specs/"].append(rec + (n.value[:60],))
            if re.search(r"\b(git_commit|git_dirty|git_tag)\b", n.value) and n.value not in COLS: T["(iii) SQL or text naming a git column"].append(rec)
        if isinstance(n, ast.JoinedStr) and n.values and isinstance(n.values[0], ast.Constant) and isinstance(n.values[0].value, str) and LIT.match(n.values[0].value):
            T["(vi) f-string beginning data/ or review_specs/"].append(rec)
print("\n== tests")
for key in sorted(T):
    rows = sorted(set(T[key])); files = len({r[0].split(":")[0] for r in rows})
    print(f"\n-- {key}: {len(rows)} hits / {files} files")
    c = collections.Counter(r[0].split(":")[0] for r in rows)
    print("   by file: " + ", ".join(f"{a.replace('tests/', '')} {b}" for a, b in c.most_common(14)))
    limit = 400 if key.startswith(("chdir", "(ii)", "(iv)", "ReviewDatabase, no data", "(v)")) else 60
    for r in rows[:limit]: print(f"  {r[0]} {r[1]} | {r[2]}")
    if len(rows) > limit: print(f"  … {len(rows) - limit} more")
print("\n== counts")
for key in sorted(sites): print(f"sites {key}: {len(set(sites[key]))}")
for key in sorted(T):
    rows = set(T[key]); print(f"tests {key}: {len(rows)} / {len({r[0].split(':')[0] for r in rows})}")
