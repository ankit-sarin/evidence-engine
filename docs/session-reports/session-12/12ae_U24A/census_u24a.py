"""12ae-U24A — AST census of U24's sites (C49: every call reaching the preflight probe, by whether it passes spec=) and of the tests
coupled to them, in 12ad_U23A/census_u23a.py's form. Read-only: AST only, no scanned module is imported, no database is opened.
usage, from the repository root: census_u24a.py   (stdout: sections, then counts)
Sites: engine/, scripts/, analysis/, tests/ — every call whose callee's last name is require_preflight, preflight_check, check_model or
ollama_preflight's main, each with whether it passes spec= (keyword, or a positional beyond the declared ones), and every definition of
those names. Each row carries the path class: run7 = in the static import closure (lazy imports included) of scripts/run_pipeline.py,
engine/agents/ft_screener.py or engine/migrations/runner.py, plus engine/migrations/*.py; else acquisition, adjudication, analysis,
scripts, tools, utils-cli or engine-other. A call to check_model that is not ollama_preflight's own (another module's check_model, e.g.
the vision parser's) is listed under its own heading so it is not mistaken for a probe.
Tests: the classes of R722(a), R723(c), R724(a), R725(b) as 12ac_U22A.md §4 labels them, adapted to the probe: (i) direct names and
from-imports of the probe's names; (ii) attribute access to them, and patch / setattr whose target string names them; (iii) the
'preflight' stage literal and 'preflight:<model>' keys; (iv) exact compares whose literals include the probe's option keys
(temperature, num_predict, keep_alive) or the 'preflight' key, in a set/dict/sorted form or against a dict literal; (v) the seams —
calls to run_screening, run_verification, run_ft_screening and the FT invocation in tests, and open_run / open_screening_manifest
calls passing preflight_models= or with_preflight=; (vi) PREFLIGHT_MESSAGES and the probe's prompt text.
Declared completeness: direct names, attribute access, from-imports, patch-target strings, setattr on any receiver, literal strings and
keyword arguments. It does not see a name assembled at runtime, a patch whose target is not a literal, a probe reached only through a
fixture's behaviour, or a call through a variable bound to the function (an alias such as pf = ollama_preflight is seen only where the
attribute name itself is one of the probe's names)."""
import ast, collections, re
from pathlib import Path
PROBE = {"require_preflight", "preflight_check", "check_model"}
NAMES = PROBE | {"PREFLIGHT_MESSAGES", "PreflightConfig", "ollama_preflight"}
SEAMS = {"run_screening", "run_verification", "run_ft_screening", "run_ft_verification", "run_ft_invocation", "open_screening_manifest"}
OPTKEYS = {"temperature", "num_predict", "keep_alive"}
DECLARED_POS = {"require_preflight": 3, "preflight_check": 2, "check_model": 2}
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
    if s.startswith("tests"): return "tests"
    area = ("acquisition" if s.startswith("engine/acquisition") else "adjudication" if s.startswith("engine/adjudication") else
            "analysis" if s.startswith("analysis") else "scripts" if s.startswith("scripts") else "tools" if s.startswith("engine/tools") else
            "migrations" if s.startswith("engine/migrations") else "engine-other")
    return f"run7/{area}" if Path(s) in RUN7 else area
def imports_probe_module(t):
    """Whether this module binds check_model from engine.utils.ollama_preflight (directly or as an attribute of that module)."""
    for n in ast.walk(t):
        if isinstance(n, ast.ImportFrom) and n.module == "engine.utils.ollama_preflight" and any(a.name == "check_model" for a in n.names): return True
    return False
print("== the static closure"); print(f"modules in the run7 closure: {len(RUN7)}")
sites = collections.defaultdict(list)
for root in ("engine", "scripts", "analysis", "tests"):
    for p in sorted(Path(root).rglob("*.py")):
        src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t); k = klass(p)
        own_cm = str(p) == "engine/utils/ollama_preflight.py" or imports_probe_module(t)
        for n in ast.walk(t):
            if not hasattr(n, "lineno"): continue
            rec = (k, f"{p}:{n.lineno}", sc.get(id(n), ""), line(src, n.lineno))
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in PROBE:
                kwo = [x.arg for x in n.args.kwonlyargs]
                sites[f"definition {n.name} (keyword-only: {kwo})"].append(rec)
            if isinstance(n, ast.Call):
                f = ast.unparse(n.func); tail = f.rsplit(".", 1)[-1]
                kw = {k2.arg for k2 in n.keywords}
                if tail in PROBE:
                    if tail == "check_model" and not (own_cm or "ollama_preflight" in f or "pf." in f):
                        sites["call check_model of another module (not the probe)"].append(rec); continue
                    passes = "spec" in kw or len(n.args) > DECLARED_POS[tail]
                    sites[f"call {tail}, {'passes spec=' if passes else 'omits spec='}"].append(rec)
                if tail == "main" and ("ollama_preflight" in f or (str(p) == "engine/utils/ollama_preflight.py")):
                    sites["call ollama_preflight.main"].append(rec)
                if tail == "open_run" and "preflight_models" in kw: sites["call open_run with preflight_models="].append(rec)
                if tail == "open_screening_manifest": sites["call open_screening_manifest"].append(rec)
                if tail == "resolve_run" and "preflight_models" in kw: sites["call resolve_run with preflight_models="].append(rec)
                if tail == "stage_config" and n.args and isinstance(n.args[0], ast.Constant) and n.args[0].value == "preflight":
                    sites[f"call stage_config('preflight', …), {'spec passed' if len(n.args) > 1 or 'spec' in kw else 'no spec'}"].append(rec)
print("\n== sites (engine, scripts, analysis, tests)")
for key in sorted(sites):
    rows = sorted(set(sites[key])); print(f"\n-- {key}: {len(rows)}")
    for r in rows: print(f"  [{r[0]}] {r[1]} {r[2]} | {r[3]}")
T = collections.defaultdict(list)
for p in sorted(Path("tests").rglob("*.py")):
    src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
    for n in ast.walk(t):
        if not hasattr(n, "lineno"): continue
        rec = (f"{p}:{n.lineno}", sc.get(id(n), ""), line(src, n.lineno))
        if isinstance(n, ast.Name) and n.id in NAMES: T["(i) direct names"].append(rec)
        if isinstance(n, ast.ImportFrom) and (n.module == "engine.utils.ollama_preflight" or any(a.name in NAMES for a in n.names)): T["(i) from-imports"].append(rec)
        if isinstance(n, ast.Import) and any(a.name == "engine.utils.ollama_preflight" for a in n.names): T["(i) from-imports"].append(rec)
        if isinstance(n, ast.Attribute) and n.attr in NAMES - {"ollama_preflight"}: T["(ii) attribute access to a probe name"].append(rec)
        if isinstance(n, ast.Call):
            f = ast.unparse(n.func); tail = f.rsplit(".", 1)[-1]
            if tail in ("setattr", "patch", "object") or f.endswith("patch"):
                vals = [a.value for a in n.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]
                if any(any(x in v for x in NAMES) for v in vals): T["(ii) patch or setattr naming a probe name"].append(rec)
                elif any(isinstance(a, ast.Attribute) and isinstance(a.value, ast.Name) and a.value.id in ("pf", "ollama_preflight") for a in n.args) or any(isinstance(a, ast.Name) and a.id in ("pf",) for a in n.args):
                    T["(ii) patch.object / setattr on the probe module"].append(rec)
            if tail in SEAMS: T[f"(v) seam call {tail}"].append(rec)
            if tail in ("open_run", "resolve_run") and any(k2.arg == "preflight_models" for k2 in n.keywords): T["(v) open_run / resolve_run with preflight_models="].append(rec)
            if tail == "open_screening_manifest" or any(k2.arg == "with_preflight" for k2 in n.keywords): T["(v) with_preflight= / open_screening_manifest"].append(rec)
            if tail in PROBE: T[f"(v) direct call {tail}, {'spec=' if any(k2.arg == 'spec' for k2 in n.keywords) else 'no spec='}"].append(rec)
        if isinstance(n, ast.Compare) and any(isinstance(o, (ast.Eq, ast.In)) for o in n.ops):
            sides = [n.left] + n.comparators
            lits = {x.value for s in sides for x in ast.walk(s) if isinstance(x, ast.Constant) and isinstance(x.value, str)}
            setform = any(isinstance(s, (ast.Set, ast.Dict)) for s in sides) or any(isinstance(s, ast.Call) and ast.unparse(s.func) in ("set", "sorted", "frozenset", "dict") for s in sides)
            if setform and (lits & OPTKEYS) and ("num_predict" in lits or "keep_alive" in lits): T["(iv) exact set/dict compare naming num_predict or keep_alive"].append(rec)
            if setform and any(v == "preflight" or v.startswith("preflight:") for v in lits): T["(iv) exact set/dict compare naming a preflight stage key"].append(rec)
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            if n.value == "preflight": T["(iii) the 'preflight' stage literal"].append(rec)
            if n.value.startswith("preflight:"): T["(iii) a 'preflight:<model>' key literal"].append(rec + (n.value,))
            if n.value == "num_predict": T["(iii) the 'num_predict' option literal"].append(rec)
            if n.value == "Respond with OK": T["(vi) the probe's prompt text"].append(rec)
        if isinstance(n, ast.JoinedStr) and n.values and isinstance(n.values[0], ast.Constant) and isinstance(n.values[0].value, str) and n.values[0].value.startswith("preflight:"):
            T["(iii) an f-string 'preflight:{…}' key"].append(rec)
print("\n== tests")
for key in sorted(T):
    rows = sorted(set(T[key])); files = len({r[0].split(":")[0] for r in rows})
    print(f"\n-- {key}: {len(rows)} hits / {files} files")
    c = collections.Counter(r[0].split(":")[0] for r in rows)
    print("   by file: " + ", ".join(f"{a.replace('tests/', '')} {b}" for a, b in c.most_common(20)))
    for r in rows[:400]: print(f"  {r[0]} {r[1]} | {r[2]}")
print("\n== counts")
for key in sorted(sites): print(f"sites {key}: {len(set(sites[key]))}")
for key in sorted(T):
    rows = set(T[key]); print(f"tests {key}: {len(rows)} / {len({r[0].split(':')[0] for r in rows})}")
