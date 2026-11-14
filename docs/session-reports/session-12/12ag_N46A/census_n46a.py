"""12ag-N46A — AST census for N46's Phase A read-out: who reads the legacy cloud tables, in 12af_U30A/census_u30a.py's form.
Read-only: AST only, no scanned module is imported, no database is opened.
usage, from the repository root: python3 -I census_n46a.py   (stdout: sections, then counts)
Files: every .py under engine/, scripts/, analysis/ and tests/; engine/migrations/ is listed apart in (a) and left out of (b)-(e).
  (a) every string literal, docstrings left out (a str constant, or an f-string's literal parts joined) naming `cloud_evidence_spans` or
      `cloud_extractions`, with its enclosing function and its SQL verb (the first of CREATE / INSERT / SELECT / UPDATE /
      DELETE / DROP / ALTER / PRAGMA / WITH found in it, else "name only");
  (b) the reads: (a)'s SELECT and WITH literals, each marked for whether it names `cloud_evidence_spans`, names
      `extracted_data`, filters on `field_name` (a `field_name` after a WHERE / AND / ON, or `field_name IN`), and aggregates
      (COUNT( / GROUP BY);
  (c) the writes outside the cloud store: (a)'s INSERT / UPDATE / DELETE literals not in engine/cloud/, tests/ apart;
  (d) every subscript or .get() with the key "extracted_data" (a read of the column's value after it is fetched), with
      `json.loads` in the same function marked;
  (e) per file, the functions of (b) and (d), so the read-out can quote each reader; tests are counted apart;
  (f) test functions coupled to the seam: naming store_result, run_arm, drop_unexpected, enforce_completeness or
      check_completeness, or naming a cloud table in a literal; each marked for whether the function also names a field outside
      a plain codebook set (a literal 'Title', 'field_1', 'invented_field', 'bogus_field' or 'junk').
Declared completeness: direct literals only. SQL assembled from variables, a table name passed as a parameter, a read through
pandas or the ORM-less sqlite3 Row API by a name not in a literal, or a module outside these trees, is not seen."""
import ast, collections, re
from pathlib import Path

FILES = [p for d in ("engine", "scripts", "analysis", "tests") for p in sorted(Path(d).rglob("*.py"))]
VERBS = ("CREATE", "INSERT", "SELECT", "UPDATE", "DELETE", "DROP", "ALTER", "PRAGMA", "WITH")
TABLE = re.compile(r"\bcloud_(evidence_spans|extractions)\b")


def scopes(tree):
    out = {}
    def walk(node, name):
        for ch in ast.iter_child_nodes(node):
            n = f"{name}.{ch.name}" if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) else name
            out[id(ch)] = n; walk(ch, n)
    walk(tree, "<module>"); return out


def literals(tree):
    """(node, text): str constants not inside an f-string, and each f-string's literal parts joined; docstrings left out."""
    inside = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and n.body and \
                isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant) and isinstance(n.body[0].value.value, str):
            inside.add(id(n.body[0].value))
    for n in ast.walk(tree):
        if isinstance(n, ast.JoinedStr):
            for v in n.values: inside.add(id(v))
            yield n, "".join(v.value if isinstance(v, ast.Constant) else "{…}" for v in n.values)
    for n in ast.walk(tree):
        if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in inside:
            yield n, n.value


def verb(s):
    u = s.upper(); hits = [(m.start(), v) for v in VERBS for m in [re.search(r"\b" + v + r"\b", u)] if m]
    return min(hits)[1] if hits else "name only"


rows = collections.defaultdict(list); readers = collections.defaultdict(set)
for p in FILES:
    src = p.read_text(encoding="utf-8")
    if "cloud_evidence_spans" not in src and "cloud_extractions" not in src and "extracted_data" not in src:
        continue
    t = ast.parse(src); sc = scopes(t); mig = str(p).startswith("engine/migrations/")
    for n, s in literals(t):
        if not TABLE.search(s):
            continue
        v = verb(s); fn = sc.get(id(n), "<module>"); one = " ".join(s.split())[:170]
        rows["(a) literals naming a cloud table" + (" — engine/migrations/" if mig else "")].append(f"{p}:{n.lineno} {fn} | {v} | {one}")
        if mig:
            continue
        if v in ("SELECT", "WITH"):
            u = " ".join(s.split())
            marks = [m for m, ok in (("spans", "cloud_evidence_spans" in u), ("extracted_data", "extracted_data" in u),
                                     ("by field_name", bool(re.search(r"\b(WHERE|AND|ON)\b[^;]*\bfield_name\b\s*(=|IN\b|<>|!=)", u, re.I))),
                                     ("aggregates", bool(re.search(r"COUNT\(|GROUP BY", u, re.I)))) if ok]
            rows["(b) reads (SELECT / WITH)"].append(f"{p}:{n.lineno} {fn} | {', '.join(marks) or '-'} | {one}")
            readers[str(p)].add(fn)
        if v in ("INSERT", "UPDATE", "DELETE") and not str(p).startswith("engine/cloud/"):
            rows["(c) writes outside engine/cloud/" + (" — tests/" if str(p).startswith("tests/") else "")].append(f"{p}:{n.lineno} {fn} | {v} | {one}")
    if str(p).startswith("engine/migrations/"):
        continue
    for n in ast.walk(t):
        key = None
        if isinstance(n, ast.Subscript) and isinstance(n.slice, ast.Constant) and n.slice.value == "extracted_data":
            key = "subscript"
        elif isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "get" and n.args and \
                isinstance(n.args[0], ast.Constant) and n.args[0].value == "extracted_data":
            key = ".get"
        if key:
            fn = sc.get(id(n), "<module>")
            fnode = next((f for f in ast.walk(t) if isinstance(f, (ast.FunctionDef, ast.AsyncFunctionDef)) and sc.get(id(f), "").endswith(fn.rsplit(".", 1)[-1]) and f.lineno <= n.lineno <= f.end_lineno), None)
            loads = fnode is not None and "json.loads" in ast.unparse(fnode)
            rows["(d) value reads of 'extracted_data'"].append(f"{p}:{n.lineno} {fn} | {key} | json.loads in function: {loads} | {src.splitlines()[n.lineno - 1].strip()[:120]}")
            readers[str(p)].add(fn)
SEAM = {"store_result", "run_arm", "drop_unexpected", "enforce_completeness", "check_completeness"}
EXTRA = {"Title", "field_1", "invented_field", "bogus_field", "junk"}
for p in sorted(Path("tests").rglob("*.py")):
    src = p.read_text(encoding="utf-8"); t = ast.parse(src)
    for fn in (n for n in ast.walk(t) if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")):
        names = {x.attr for x in ast.walk(fn) if isinstance(x, ast.Attribute)} | {x.id for x in ast.walk(fn) if isinstance(x, ast.Name)}
        lits = {x.value for x in ast.walk(fn) if isinstance(x, ast.Constant) and isinstance(x.value, str)}
        hit = sorted(names & SEAM) + (["cloud table literal"] if any(TABLE.search(s) for s in lits) else [])
        if hit:
            extra = sorted(EXTRA & (lits | names))
            rows["(f) tests coupled to the seam"].append(f"{p}:{fn.lineno} {fn.name} | {hit} | extra name: {extra or '-'}")
for k in sorted(rows):
    print(f"\n-- {k}: {len(rows[k])}")
    for r in rows[k]: print("  " + r)
print("\n-- (e) reader functions by file (from (b) and (d))")
for f in sorted(readers): print(f"  {f}: {sorted(readers[f])}")
print("\n== counts")
for k in sorted(rows): print(f"{k}: {len(rows[k])}")
nt = [f for f in readers if not f.startswith("tests/")]
print(f"(e) files with a reader: {len(readers)} ({len(nt)} outside tests/, {len(readers) - len(nt)} in tests/); "
      f"reader functions outside tests/: {sum(len(readers[f]) for f in nt)}")
