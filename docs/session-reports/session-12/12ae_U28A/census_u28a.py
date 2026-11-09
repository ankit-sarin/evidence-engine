"""12ae-U28A — AST census of U28's sites (the cloud distribution gate, the cloud store and the cloud write boundary) and of the tests
coupled to them, in 12ae_U27A/census_u27a.py's form. Read-only: AST only, no scanned module is imported, no database is opened.
usage, from the repository root: census_u28a.py   (stdout: sections, then counts)
Sites: every caller across engine/, scripts/ and analysis/ of run_distribution_check, run_post_extraction_check, check_distribution,
assert_no_collapse, arm_population, _query_values, _query_all_fields, parse_response_to_spans and store_result, with its path class (run7 =
the static import closure of scripts/run_pipeline.py, engine/agents/ft_screener.py and engine/migrations/runner.py, plus
engine/migrations/*.py; cloud = the closure of scripts/run_cloud_extraction.py); every write to field_events / a call to the event writer's
claim functions under engine/cloud/.
Tests: the classes of R722(a), R723(c), R724(a), R725(b) as 12ac_U22A.md §4 labels them, adapted: (i) direct names and from-imports of
these names; (ii) attribute access, and patch / setattr whose target names engine.validators.distribution_monitor, engine.cloud.base or one
of the names; (iii) the status literals ("COLLAPSED", "LOW_VARIANCE", "OK") and summary keys ("skipped", "skip_reason") in a test that names
a monitor function; (iv) exact compares on a status or skipped value; (v) the seams — calls to the names in tests; (vi) a test asserting
an empty or missing source_snippet stored or refused (literal "source_snippet" beside an empty string).
Declared completeness: direct names, attribute access, from-imports, patch-target strings, setattr on any receiver, literal strings and
keyword arguments. It does not see a name bound by a variable, a patch whose target is not a literal, or behaviour injected by a fixture."""
import ast, collections, re
from pathlib import Path
NAMES = {"run_distribution_check", "run_post_extraction_check", "check_distribution", "assert_no_collapse", "arm_population", "_query_values",
         "_query_all_fields", "parse_response_to_spans", "store_result", "DistributionCollapseError"}
MONITOR = {"run_distribution_check", "run_post_extraction_check", "check_distribution", "assert_no_collapse", "arm_population",
           "_query_values", "_query_all_fields"}
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
print("engine/validators/distribution_monitor.py in run7: " + str(Path("engine/validators/distribution_monitor.py") in RUN7)
      + "; in cloud: " + str(Path("engine/validators/distribution_monitor.py") in CLOUD))
sites = collections.defaultdict(list)
for root in ("engine", "scripts", "analysis"):
    for p in sorted(Path(root).rglob("*.py")):
        s2 = p.read_text(encoding="utf-8"); t2 = ast.parse(s2); sc2 = scopes(t2)
        for n in ast.walk(t2):
            if isinstance(n, ast.Call):
                tail = ast.unparse(n.func).rsplit(".", 1)[-1]
                if tail in NAMES:
                    sites[f"call {tail}"].append((klass(p), f"{p}:{n.lineno}", sc2.get(id(n), ""), line(s2, n.lineno)))
                if str(p).startswith("engine/cloud") and tail in ("write_field_event", "write_claim", "write_extraction_events", "record_claim"):
                    sites["the event writer called under engine/cloud"].append((klass(p), f"{p}:{n.lineno}", sc2.get(id(n), ""), line(s2, n.lineno)))
            if str(p).startswith("engine/cloud") and isinstance(n, ast.Constant) and isinstance(n.value, str) and "field_events" in n.value:
                sites["a literal naming field_events under engine/cloud"].append((klass(p), f"{p}:{n.lineno}", sc2.get(id(n), ""), line(s2, n.lineno)))
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and re.search(r"INSERT INTO cloud_(extractions|evidence_spans)", n.value):
                sites["an INSERT into the cloud tables"].append((klass(p), f"{p}:{n.lineno}", sc2.get(id(n), ""), line(s2, n.lineno)))
print("\n== callers and writers (engine, scripts, analysis)")
for key in sorted(sites):
    rs = sorted(set(sites[key])); print(f"\n-- {key}: {len(rs)}")
    for r in rs: print(f"  [{r[0]}] {r[1]} {r[2]} | {r[3]}")
T = collections.defaultdict(list)
for p in sorted(Path("tests").rglob("*.py")):
    s3 = p.read_text(encoding="utf-8"); t3 = ast.parse(s3); sc3 = scopes(t3)
    names_monitor = any(m in s3 for m in MONITOR)
    for n in ast.walk(t3):
        if not hasattr(n, "lineno"): continue
        rec = (f"{p}:{n.lineno}", sc3.get(id(n), ""), line(s3, n.lineno))
        if isinstance(n, ast.Name) and n.id in NAMES: T["(i) direct names"].append(rec)
        if isinstance(n, ast.ImportFrom) and n.module in ("engine.validators.distribution_monitor", "engine.cloud.base"): T["(i) from-imports"].append(rec)
        if isinstance(n, ast.Attribute) and n.attr in NAMES: T["(ii) attribute access to a name"].append(rec)
        if isinstance(n, ast.Call):
            f = ast.unparse(n.func); tail = f.rsplit(".", 1)[-1]
            if tail in ("setattr", "patch", "object") or f.endswith("patch"):
                vals = [a.value for a in n.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]
                if any(v.startswith(("engine.validators.distribution_monitor", "engine.cloud.base")) or v in NAMES or
                       any(v.endswith("." + nm) for nm in NAMES) for v in vals):
                    T["(ii) patch or setattr on the monitor / base / a name"].append(rec)
            if tail in NAMES: T[f"(v) seam call {tail}"].append(rec)
        if names_monitor and isinstance(n, ast.Constant) and isinstance(n.value, str) and n.value in ("COLLAPSED", "LOW_VARIANCE", "OK", "skipped", "skip_reason"):
            T["(iii) a status literal or summary key in a monitor test"].append(rec + (n.value,))
        if names_monitor and isinstance(n, ast.Compare) and any(isinstance(o, (ast.Eq, ast.Is, ast.IsNot, ast.NotEq)) for o in n.ops):
            u = ast.unparse(n)
            if re.search(r"status|skipped|skip_reason|collapsed", u): T["(iv) exact compare on a status / skipped value"].append(rec)
        if isinstance(n, ast.Dict):
            ks = [k.value for k in n.keys if isinstance(k, ast.Constant)]
            if "source_snippet" in ks:
                v = n.values[ks.index("source_snippet")] if ks.index("source_snippet") < len(n.values) else None
                if isinstance(v, ast.Constant) and v.value in ("", None):
                    T["(vi) a span dict with an empty or None source_snippet"].append(rec)
print("\n== tests")
for key in sorted(T):
    rs = sorted(set(T[key])); files = len({r[0].split(":")[0] for r in rs})
    print(f"\n-- {key}: {len(rs)} hits / {files} files")
    c = collections.Counter(r[0].split(":")[0] for r in rs)
    print("   by file: " + ", ".join(f"{a.replace('tests/', '')} {b}" for a, b in c.most_common(30)))
    for r in rs[:200]: print(f"  {r[0]} {r[1]} | {r[2]}" + (f" | {r[3]}" if len(r) > 3 else ""))
print("\n== counts")
for key in sorted(sites): print(f"sites {key}: {len(set(sites[key]))}")
for key in sorted(T):
    rs = set(T[key]); print(f"tests {key}: {len(rs)} / {len({r[0].split(':')[0] for r in rs})}")
