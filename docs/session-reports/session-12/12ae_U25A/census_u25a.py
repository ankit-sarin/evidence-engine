"""12ae-U25A — AST census of U25's sites (INT-i12-31: the codebook loader and the readers of a loaded valid_values value or sentinel) and of
the tests coupled to them, in 12ae_U24A/census_u24a.py's form. Read-only: AST only, no scanned module is imported, no database is opened.
usage, from the repository root: census_u25a.py   (stdout: sections, then counts)
Sites: engine/, scripts/, analysis/ — calls to the codebook entry points (load_codebook, load_codebook_beside, load_codebook_for, _parse) and
to the readers of the loaded values (enum_values, is_absence_sentinel, absence_sentinel_set, absence_sentinels, raw, _build_field_block,
build_extraction_prompt, compute_semantic_hash), each with its path class (run7 = the static import closure of scripts/run_pipeline.py,
engine/agents/ft_screener.py and engine/migrations/runner.py, plus engine/migrations/*.py).
Tests: the classes of R722(a), R723(c), R724(a), R725(b) as 12ac_U22A.md §4 labels them, adapted to the codebook: (i) direct names and
from-imports of the codebook module's names; (ii) attribute access to them, patch / setattr whose target string names them; (iii) the
codebook's key literals ("valid_values", "absence_sentinels", "canonical_absence_sentinel"); (iv) exact compares against a codebook's
semantic_hash or sha256; (v) fixtures that write codebook YAML text — every string constant (or f-string's literal parts) containing
"valid_values:" or "absence_sentinels:", each scanned for an unquoted YAML 1.1 bool/null scalar as a `value:` or as a list item, and every
yaml.dump / yaml.safe_dump call; (vi) copies of the live codebook (a literal naming extraction_codebook.yaml).
Declared completeness: direct names, attribute access, from-imports, patch-target strings, setattr on any receiver, literal strings and
keyword arguments. It does not see a codebook text assembled at runtime from non-literal pieces, a fixture reached only through another
fixture's behaviour, or a name bound by a variable; the token scan reads `value: X` and `- X` lines only, so a scalar in flow style
(`[Yes, No]`) is listed under its own heading by a separate pattern."""
import ast, collections, re
from pathlib import Path
ENTRY = {"load_codebook", "load_codebook_beside", "load_codebook_for", "_parse"}
READERS = {"enum_values", "is_absence_sentinel", "absence_sentinel_set", "absence_sentinels", "raw", "_build_field_block",
           "build_extraction_prompt", "compute_semantic_hash", "_validate_valid_values", "_validate_top_level", "_validate_fields"}
NAMES = ENTRY | READERS | {"Codebook", "CodebookError", "CodebookIdentityError", "clear_cache", "compute_codebook_sha256", "CODEBOOK_FILENAME"}
KEYS = {"valid_values", "absence_sentinels", "canonical_absence_sentinel"}
TOK = r"(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF|~|null|Null|NULL)"
LINE_TOK = re.compile(rf"^\s*(?:-\s*)?(?:value:\s*)?{TOK}\s*(?:#.*)?$|^\s*-?\s*value:\s*{TOK}\s*(?:#.*)?$")
FLOW_TOK = re.compile(rf"\[[^\]]*\b{TOK}\b[^\]]*\]")
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
    area = ("acquisition" if s.startswith("engine/acquisition") else "adjudication" if s.startswith("engine/adjudication") else
            "analysis" if s.startswith("analysis") else "scripts" if s.startswith("scripts") else "tools" if s.startswith("engine/tools") else
            "migrations" if s.startswith("engine/migrations") else "engine-other")
    return f"run7/{area}" if Path(s) in RUN7 else area
print("== the static closure"); print(f"modules in the run7 closure: {len(RUN7)}")
sites = collections.defaultdict(list)
for root in ("engine", "scripts", "analysis"):
    for p in sorted(Path(root).rglob("*.py")):
        src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t); k = klass(p)
        for n in ast.walk(t):
            if not hasattr(n, "lineno"): continue
            rec = (k, f"{p}:{n.lineno}", sc.get(id(n), ""), line(src, n.lineno))
            if isinstance(n, ast.Call):
                tail = ast.unparse(n.func).rsplit(".", 1)[-1]
                if tail in ENTRY: sites[f"call {tail}"].append(rec)
                if tail in READERS: sites[f"call {tail}"].append(rec)
                if tail in ("safe_load", "load") and "yaml" in ast.unparse(n.func): sites["call yaml.safe_load / yaml.load"].append(rec)
            if isinstance(n, ast.Attribute) and n.attr in {"absence_sentinels", "absence_sentinel_set", "raw"} and isinstance(n.ctx, ast.Load):
                sites[f"attribute read .{n.attr}"].append(rec)
            if isinstance(n, ast.Subscript) and isinstance(n.slice, ast.Constant) and n.slice.value in ("valid_values", "absence_sentinels", "value"):
                sites[f"subscript [{n.slice.value!r}]"].append(rec)
print("\n== sites (engine, scripts, analysis)")
for key in sorted(sites):
    rows = sorted(set(sites[key])); print(f"\n-- {key}: {len(rows)}")
    for r in rows: print(f"  [{r[0]}] {r[1]} {r[2]} | {r[3]}")
T = collections.defaultdict(list)
fixtures = []
for p in sorted(Path("tests").rglob("*.py")):
    src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
    for n in ast.walk(t):
        if not hasattr(n, "lineno"): continue
        rec = (f"{p}:{n.lineno}", sc.get(id(n), ""), line(src, n.lineno))
        if isinstance(n, ast.Name) and n.id in NAMES: T["(i) direct names"].append(rec)
        if isinstance(n, ast.ImportFrom) and (n.module == "engine.core.codebook" or any(a.name in NAMES for a in n.names)): T["(i) from-imports"].append(rec)
        if isinstance(n, ast.Attribute) and n.attr in NAMES: T["(ii) attribute access to a codebook name"].append(rec)
        if isinstance(n, ast.Call):
            f = ast.unparse(n.func); tail = f.rsplit(".", 1)[-1]
            if tail in ("setattr", "patch", "object") or f.endswith("patch"):
                vals = [a.value for a in n.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]
                if any(any(x in v for x in NAMES | {"engine.core.codebook"}) for v in vals): T["(ii) patch or setattr naming a codebook name"].append(rec)
            if tail in ("dump", "safe_dump") and "yaml" in f: T["(v) yaml.dump / yaml.safe_dump (quotes bool-like strings itself)"].append(rec)
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            if n.value in KEYS: T["(iii) a codebook key literal"].append(rec)
            if "extraction_codebook.yaml" in n.value: T["(vi) a literal naming extraction_codebook.yaml"].append(rec)
        if isinstance(n, ast.Compare) and any(isinstance(o, ast.Eq) for o in n.ops):
            u = ast.unparse(n)
            if "semantic_hash" in u or ".sha256" in u: T["(iv) exact compare against semantic_hash or sha256"].append(rec)
        texts = []
        if isinstance(n, ast.Constant) and isinstance(n.value, str): texts = [n.value]
        elif isinstance(n, ast.JoinedStr): texts = ["".join(v.value for v in n.values if isinstance(v, ast.Constant) and isinstance(v.value, str))]
        for tx in texts:
            if "valid_values:" in tx or "absence_sentinels:" in tx:
                bad = [l.strip() for l in tx.splitlines() if LINE_TOK.match(l)]
                flow = FLOW_TOK.findall(tx)
                fixtures.append((rec[0], rec[1], bad, flow))
                T["(v) codebook YAML text constants"].append(rec)
                if bad: T["(v) ... of which with an unquoted YAML 1.1 bool/null as `value:` or list item"].append(rec + (str(bad[:4]),))
                if flow: T["(v) ... of which with a bool/null token inside a flow sequence"].append(rec + (str(flow[:3]),))
print("\n== tests")
for key in sorted(T):
    rows = sorted(set(T[key])); files = len({r[0].split(":")[0] for r in rows})
    print(f"\n-- {key}: {len(rows)} hits / {files} files")
    c = collections.Counter(r[0].split(":")[0] for r in rows)
    print("   by file: " + ", ".join(f"{a.replace('tests/', '')} {b}" for a, b in c.most_common(20)))
    for r in rows[:300]: print(f"  {r[0]} {r[1]} | {r[2]}" + (f" | {r[3]}" if len(r) > 3 else ""))
print("\n== counts")
for key in sorted(sites): print(f"sites {key}: {len(set(sites[key]))}")
for key in sorted(T):
    rows = set(T[key]); print(f"tests {key}: {len(rows)} / {len({r[0].split(':')[0] for r in rows})}")
