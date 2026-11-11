"""12af-U40B — AST census for U40's build (B9, C28; R798(f)), in 12af_U40A/census_u40a.py's form, run before the edit.
Read-only: AST only, no scanned module is imported, no database is opened.
usage, from the repository root: python3 -I census_u40b.py   (stdout: sections, then counts)
  (1) every test function in tests/ that reaches CloudExtractorBase.store_result — directly (a call whose attribute is store_result),
      through a transport's run (a call `<x>.run(...)` in a function that names OpenAIExtractor, AnthropicExtractor or a fixture/helper
      returning one, or `super().run(` in a class the test defines), or through a module-level helper or a file-local fixture that
      does either — with the fixtures it takes and whether it expects a store: "store" when a store_result call or a run call sits
      outside every `with pytest.raises(...)`, "refusal" when every such call sits inside one; for a run call the expectation is the
      selected run's to decide ("run: decided by the selected run"), since the census does not see what a mocked provider returns;
  (2) every test function naming write_extraction_events, with its file;
  (3) every production call of write_extraction_events with its keywords (the AST census R798(c) pins: paper_event passed by
      engine/cloud/base.py only — at HEAD, before the edit, no call passes it).
Declared completeness: direct names, attributes, module-level helpers and file-local fixtures one level deep, and the helpers they
call (closure within the file). It does not see a method reached through a variable bound elsewhere, a fixture from conftest.py, or
behaviour injected by a patch."""
import ast, collections
from pathlib import Path

TRANSPORTS = {"OpenAIExtractor", "AnthropicExtractor"}


def raises_ranges(fn):
    out = []
    for n in ast.walk(fn):
        if isinstance(n, ast.With) and any("raises" in ast.unparse(i.context_expr) for i in n.items):
            out.append((n.lineno, n.end_lineno))
    return out


def inside(n, rr): return any(a <= n.lineno <= b for a, b in rr)


def reach(fn):
    """('store', lines_outside, lines_inside) for store_result calls; ('run', ...) for transport run calls."""
    names = {x.id for x in ast.walk(fn) if isinstance(x, ast.Name)} | {x.attr for x in ast.walk(fn) if isinstance(x, ast.Attribute)}
    rr = raises_ranges(fn); out = collections.defaultdict(lambda: [0, 0])
    for n in ast.walk(fn):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
            kind = None
            if n.func.attr == "store_result": kind = "store_result"
            elif n.func.attr == "run" and (names & TRANSPORTS or ast.unparse(n.func.value) == "super()"): kind = "run"
            if kind: out[kind][1 if inside(n, rr) else 0] += 1
    return dict(out)


rows, wee = [], []
for p in sorted(Path("tests").rglob("*.py")):
    src = p.read_text(encoding="utf-8"); t = ast.parse(src)
    funcs = {n.name: n for n in ast.walk(t) if isinstance(n, ast.FunctionDef)}
    classes = [n for n in ast.walk(t) if isinstance(n, ast.ClassDef)]
    fixtures = {n.name for n in funcs.values() if any("fixture" in ast.unparse(d) for d in n.decorator_list)}
    helpers = {k for k, n in funcs.items() if not k.startswith("test_")}
    direct = {k: reach(n) for k, n in funcs.items()}
    # a class a test file defines that subclasses a transport and calls super().run: its methods count as transport runs
    for c in classes:
        if any(ast.unparse(b) in TRANSPORTS for b in c.bases):
            for n in c.body:
                if isinstance(n, ast.FunctionDef) and any(isinstance(x, ast.Call) and ast.unparse(x.func) == "super().run" for x in ast.walk(n)):
                    direct[n.name] = {"run": [1, 0]}
    def closure(name, seen=()):
        r = {k: list(v) for k, v in direct.get(name, {}).items()}
        n = funcs.get(name)
        if n is None: return r, []
        via = []
        used = {x.id for x in ast.walk(n) if isinstance(x, ast.Name)} | {x.attr for x in ast.walk(n) if isinstance(x, ast.Attribute)}
        if name.startswith("test_"): used |= {a.arg for a in n.args.args}
        for h in sorted((used & (helpers | set(direct))) - {name} - set(seen)):
            sub, subvia = closure(h, seen + (name,))
            if sub:
                via.append(h); via += subvia
                for k, v in sub.items():
                    r.setdefault(k, [0, 0]); r[k][0] += v[0]; r[k][1] += v[1]
        return r, via
    for fn in (n for n in ast.walk(t) if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")):
        r, via = closure(fn.name)
        if r:
            params = [a.arg for a in fn.args.args if a.arg != "self"]
            if "store_result" in r and r["store_result"][0]: exp = "store"
            elif "store_result" in r: exp = "refusal"
            else: exp = "run: decided by the selected run"
            if "run" in r and "store_result" in r and r["store_result"][0]: exp = "store (and run: decided by the selected run)"
            rows.append((f"{p}:{fn.lineno}", fn.name, ",".join(params) or "-", ",".join(sorted(set(params) & fixtures)) or "-",
                         ",".join(dict.fromkeys(via)) or "-", {k: tuple(v) for k, v in r.items()}, exp))
        if any(isinstance(x, (ast.Name, ast.Attribute)) and (getattr(x, "id", None) or getattr(x, "attr", None)) == "write_extraction_events"
               for x in ast.walk(fn)):
            wee.append((str(p), f"{p}:{fn.lineno}", fn.name))

print("== (1) test functions reaching store_result (counts are (outside pytest.raises, inside))")
for r in rows: print(f"  {r[0]} {r[1]} | params: {r[2]} | file-local fixtures: {r[3]} | via: {r[4]} | reach: {r[5]} | expects: {r[6]}")
byf = collections.Counter(r[0].split(":")[0] for r in rows)
print("  by file: " + ", ".join(f"{a} {b}" for a, b in byf.most_common()))
print("\n== (2) test functions naming write_extraction_events")
fs = collections.Counter(w[0] for w in wee)
for f, n in sorted(fs.items()): print(f"  {f}: {n}")
print("\n== (3) production calls of write_extraction_events (engine/, scripts/, analysis/) and their keywords")
for root in ("engine", "scripts", "analysis"):
    for p in sorted(Path(root).rglob("*.py")):
        for n in ast.walk(ast.parse(p.read_text(encoding="utf-8"))):
            if isinstance(n, ast.Call) and ast.unparse(n.func).rsplit(".", 1)[-1] == "write_extraction_events":
                print(f"  {p}:{n.lineno} keywords: {[k.arg for k in n.keywords]}")
print("\n== counts")
print(f"tests reaching store_result: {len(rows)} / {len(byf)} files; expecting a store (direct): "
      f"{sum(1 for r in rows if r[6].startswith('store'))}; refusal only: {sum(1 for r in rows if r[6] == 'refusal')}; "
      f"run only: {sum(1 for r in rows if r[6].startswith('run'))}")
print(f"tests naming write_extraction_events: {len(wee)} / {len(fs)} files")
