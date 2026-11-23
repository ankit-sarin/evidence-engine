"""12aj-U31A — census for U31's Phase A ("Cloud response repair does not invent a value"; INT-j12-18, INT-l12-2), in
12af_U30A/census_u30a.py's form. Read-only: AST (and, where said, plain text lines); no scanned module is imported, no database is
opened, neither repair script is run.
usage, from the repository root: python3 -I census_u31a.py   (stdout: sections, then counts)
  (a) every call of parse_response_to_spans under engine/, scripts/, tests/ and analysis/, with its enclosing function and the
      receiver's source;
  (b) every write and read of `_last_salvage` under the same four trees (AST attribute nodes, by context Store / Load, and a string
      constant naming it);
  (c) every use of `canonical_absence_sentinel` in engine/cloud/*.py, scripts/run_cloud_extraction.py and the two repair scripts;
  (d) every import of ExtractionOutput or EvidenceSpan under the four trees, and every use of either name in engine/core/effective_config.py;
  (e) every reference to reparse_cloud_spans or backfill_cloud_spans under engine/, scripts/, tests/ and analysis/ (PLAIN TEXT lines,
      comments and strings included), and, for the record, in CLAUDE.md, docs/architecture/ and docs/inventory/ (counts per file);
  (f) for each of the two repair scripts: which of run_arm's path elements its source names (AST names, attributes and string
      constants): open_run, close_run, record_call, hold_experiment_lock, select_cloud_arms, enabled_arms, enforce_completeness,
      drop_unexpected, enforce_citations, write_extraction_events, store_result, init_cloud_tables; and its connect / execute / commit
      calls, its SQL verbs, its argparse flags and how it builds the CloudExtractorBase it parses with;
  (g) test functions coupled to the null, the missing-key and the coercion behaviour: in test files that name engine.cloud, every test
      function whose body calls parse_response_to_spans or extract_paper, or names _last_salvage, canonical_absence_sentinel or
      empty_string_to_null, or holds a span literal (a dict with a "field_name" key and at least one of "value", "source_snippet",
      "confidence", "tier") whose "value" is absent, None, empty or not a string, or whose "source_snippet" key is absent;
      each with its parameters (fixtures), the non-string / absent "value" kinds it holds, and its assert statements.
Declared completeness: direct names, attributes, literals and (for (e)) text lines; a call made through a variable or reflection, a
span built outside a dict literal (a comprehension over a helper, a fixture file), or a reference in a tree the lists do not name is
not seen. (g) reports what a test's own body holds; a behaviour reached only through a shared helper is not attributed."""
import ast, collections, re
from pathlib import Path

TREES = ["engine", "scripts", "tests", "analysis"]
SCRIPTS = [Path("scripts/reparse_cloud_spans.py"), Path("scripts/backfill_cloud_spans.py")]
NEEDLE = re.compile(r"reparse_cloud_spans|backfill_cloud_spans")
ELEMENTS = ["open_run", "close_run", "record_call", "hold_experiment_lock", "select_cloud_arms", "enabled_arms", "enforce_completeness",
            "drop_unexpected", "enforce_citations", "write_extraction_events", "store_result", "init_cloud_tables"]


def scopes(tree):
    out = {}
    def walk(node, name):
        for ch in ast.iter_child_nodes(node):
            n = f"{name}.{ch.name}" if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) else name
            out[id(ch)] = n; walk(ch, n)
    walk(tree, "<module>"); return out


def line(src, n): return src.splitlines()[n - 1].strip()[:150]


def files(trees): return sorted(p for t in trees for p in Path(t).rglob("*.py") if "__pycache__" not in p.parts)


rows = collections.defaultdict(list)
for p in files(TREES):
    src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
    for n in ast.walk(t):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "parse_response_to_spans":
            rows["(a) calls of parse_response_to_spans"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | receiver {ast.unparse(n.func.value)} | {line(src, n.lineno)}")
        if isinstance(n, ast.Attribute) and n.attr == "_last_salvage":
            kind = "WRITE" if isinstance(n.ctx, ast.Store) else "read"
            rows["(b) _last_salvage"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | {kind} | {line(src, n.lineno)}")
        if isinstance(n, ast.Constant) and isinstance(n.value, str) and n.value == "_last_salvage":
            rows["(b) _last_salvage"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | string constant | {line(src, n.lineno)}")
        if isinstance(n, (ast.ImportFrom, ast.Import)):
            names = [a.name for a in n.names]
            hit = [x for x in names if x in ("ExtractionOutput", "EvidenceSpan")]
            if hit:
                rows["(d) imports of ExtractionOutput / EvidenceSpan"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | {', '.join(hit)} | {line(src, n.lineno)}")
    for i, l in enumerate(src.splitlines(), 1):
        if NEEDLE.search(l) and p not in SCRIPTS:
            rows["(e) references to the two scripts (text lines; engine/, scripts/, tests/, analysis/; the scripts' own lines apart)"].append(f"{p}:{i} | {l.strip()[:150]}")
        elif NEEDLE.search(l):
            rows["(e') the scripts naming themselves or each other"].append(f"{p}:{i} | {l.strip()[:150]}")
for p in sorted(Path("engine/cloud").glob("*.py")) + [Path("scripts/run_cloud_extraction.py"), *SCRIPTS]:
    src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
    for n in ast.walk(t):
        if isinstance(n, ast.Attribute) and n.attr == "canonical_absence_sentinel":
            rows["(c) canonical_absence_sentinel on the cloud path"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | {line(src, n.lineno)}")
p = Path("engine/core/effective_config.py"); src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
for n in ast.walk(t):
    if isinstance(n, ast.Name) and n.id in ("ExtractionOutput", "EvidenceSpan"):
        rows["(d') uses of either model in engine/core/effective_config.py"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | {line(src, n.lineno)}")
for extra in [Path("CLAUDE.md"), *sorted(Path("docs/architecture").rglob("*")), *sorted(Path("docs/inventory").rglob("*"))]:
    if extra.is_file():
        hits = [i for i, l in enumerate(extra.read_text(encoding="utf-8", errors="replace").splitlines(), 1) if NEEDLE.search(l)]
        if hits:
            rows["(e'') references outside the four trees (count, lines)"].append(f"{extra} | {len(hits)} | lines {hits}")
for p in SCRIPTS:
    src = p.read_text(encoding="utf-8"); t = ast.parse(src)
    names = {x.id for x in ast.walk(t) if isinstance(x, ast.Name)} | {x.attr for x in ast.walk(t) if isinstance(x, ast.Attribute)}
    strs = [x.value for x in ast.walk(t) if isinstance(x, ast.Constant) and isinstance(x.value, str)]
    present = [e for e in ELEMENTS if e in names or any(e in s for s in strs)]
    rows[f"(f) {p}"].append(f"run_arm path elements named: {present or 'none'}; absent: {[e for e in ELEMENTS if e not in present]}")
    for n in ast.walk(t):
        if isinstance(n, ast.Call):
            f = ast.unparse(n.func)
            if f.rsplit(".", 1)[-1] in ("connect", "commit", "rollback", "add_argument") or "CloudExtractorBase" in f or f.endswith("parse_response_to_spans"):
                rows[f"(f) {p}"].append(f":{n.lineno} {line(src, n.lineno)}")
            if f.rsplit(".", 1)[-1] == "execute" and n.args and isinstance(n.args[0], ast.Constant):
                rows[f"(f) {p}"].append(f":{n.lineno} execute — SQL verb(s): {sorted(set(re.findall(r'\b(SELECT|INSERT|UPDATE|DELETE|PRAGMA|CREATE|DROP)\b', n.args[0].value)))}")
        if isinstance(n, ast.Assign) and any(isinstance(tg, ast.Name) and tg.id == "db_path" for tg in n.targets):
            rows[f"(f) {p}"].append(f":{n.lineno} {line(src, n.lineno)}")


def value_kinds(fn):
    kinds = []
    for d in (x for x in ast.walk(fn) if isinstance(x, ast.Dict)):
        keys = [k.value if isinstance(k, ast.Constant) else None for k in d.keys]
        if "field_name" not in keys or not ({"value", "source_snippet", "confidence", "tier"} & set(keys)):
            continue   # a span literal: a dict naming a field and at least one other span key
        if "value" not in keys:
            kinds.append("no value key")
        else:
            v = d.values[keys.index("value")]
            if isinstance(v, ast.Constant):
                if v.value is None: kinds.append("None")
                elif isinstance(v.value, bool): kinds.append("bool")
                elif isinstance(v.value, (int, float)): kinds.append(type(v.value).__name__)
                elif v.value == "": kinds.append("empty string")
            elif isinstance(v, (ast.List, ast.Tuple)): kinds.append("list")
            elif isinstance(v, ast.Dict): kinds.append("dict")
        if "source_snippet" not in keys:
            kinds.append("no source_snippet key")
    return sorted(set(kinds))


for p in sorted(Path("tests").rglob("*.py")):
    src = p.read_text(encoding="utf-8")
    if "engine.cloud" not in src:
        continue
    t = ast.parse(src)
    for fn in (n for n in ast.walk(t) if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")):
        names = {x.attr for x in ast.walk(fn) if isinstance(x, ast.Attribute)} | {x.id for x in ast.walk(fn) if isinstance(x, ast.Name)}
        strs = {x.value for x in ast.walk(fn) if isinstance(x, ast.Constant) and isinstance(x.value, str)}
        why = sorted(names & {"parse_response_to_spans", "extract_paper", "_last_salvage", "canonical_absence_sentinel"}) + sorted(strs & {"empty_string_to_null"})
        kinds = value_kinds(fn)
        if not (why or kinds):
            continue
        key = "(g) tests coupled — holding a null, missing-key, non-string or empty value, or a missing snippet" if kinds else "(g') tests that call the parser or extract_paper with string values only"
        asserts = [ast.unparse(a.test)[:110] for a in ast.walk(fn) if isinstance(a, ast.Assert)]
        rows[key].append(f"{p}:{fn.lineno} {fn.name} | names {why} | value kinds {kinds} | params {[a.arg for a in fn.args.args if a.arg != 'self']}"
                         + ("".join(f"\n      assert {a}" for a in asserts) if kinds else f" | asserts {len(asserts)}"))
for k in sorted(rows):
    print(f"\n-- {k}: {len(rows[k])}")
    for r in rows[k]: print("  " + r)
print("\n== counts")
for k in sorted(rows): print(f"{k}: {len(rows[k])}")
