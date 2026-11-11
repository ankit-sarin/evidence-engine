"""12af-U29A — census for U29's Phase A ("A cloud call that raises still records its call"; C24 for Wave 2 row 28; N47), in
12af_U40A/census_u40a.py's form. Parts (a)–(e) are AST only: no scanned module is imported and no database is opened. Part (f) runs the
cloud test files under pytest with a plugin (written to a temp directory, never into the repository) that wraps
CloudExtractorBase.send and .store_result at session start and records, per test, whether the instance a call is made on lacks each
attribute a U40 guard reads by getattr — the reach of the three getattr guards and of the stage-name fallback (R799(f)). The conftest
fences stay on; nothing is sent; no live database is opened (the tests' own temp directories only).
usage, from the repository root: .venv/bin/python -I census_u29a.py   (stdout: sections, then counts)
  (a) calls named record_call in engine/, scripts/, analysis/, classed by the module the name resolves to in that file
      (engine.core.run_manifest — run_calls; engine.core.extraction_telemetry — the JSONL row), and calls of record_active_ollama_call;
  (b) SQL string literals naming run_calls (SELECT / INSERT / DDL / other), engine/migrations/ listed apart;
  (c) constructions of the SDK clients (a call whose callee ends `.OpenAI`, `.Anthropic`, or is the bare name OpenAI / Anthropic), in
      engine/, scripts/, analysis/ and tests/, with their keyword arguments;
  (d) test functions coupled to the cloud send or the call record: naming `send`, `record_call`, `last_request_hash`, `_create`,
      `run_calls` (in a string), with their parameters and file-local fixtures;
  (e) tests that build a cloud instance without a provider or without its constructor: `object.__new__(<cloud class>)`; a direct call of
      `CloudExtractorBase(...)` (PROVIDER ""); a cloud method called unbound on another object (`CloudExtractorBase.<m>(<x>, …)`); a class
      subclassing a cloud class whose __init__ does not call super().__init__ / the transport's own __init__;
  (f) the runtime reach, as above.
Declared completeness: (a)–(e) see direct names, attributes, import aliases and literal strings (f-string literal parts joined); they do
not see a name bound through a variable, a call made by reflection, or a fixture from conftest.py. (f) sees only the test files it runs
(listed in its output) and only calls that reach the two methods through the class attribute it wraps."""
import ast, collections, os, re, subprocess, sys, tempfile, textwrap
from pathlib import Path

CLOUD_CLASSES = {"CloudExtractorBase", "OpenAIExtractor", "AnthropicExtractor"}


def literal_text(n):
    if isinstance(n, ast.Constant) and isinstance(n.value, str): return n.value
    if isinstance(n, ast.JoinedStr):
        return "".join(v.value for v in n.values if isinstance(v, ast.Constant) and isinstance(v.value, str))
    return None


def scopes(tree):
    out = {}
    def walk(node, name):
        for ch in ast.iter_child_nodes(node):
            n = f"{name}.{ch.name}" if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) else name
            out[id(ch)] = n; walk(ch, n)
    walk(tree, "<module>"); return out


def line(src, n): return src.splitlines()[n - 1].strip()[:150]


def sql_class(s):
    u = s.upper()
    if re.search(r"\bCREATE\b|\bALTER\b|\bDROP\b", u): return "DDL"
    if re.search(r"\bINSERT\b", u): return "INSERT"
    if re.search(r"\bUPDATE\b|\bDELETE\b", u): return "UPDATE/DELETE"
    if re.search(r"\bSELECT\b", u): return "SELECT"
    return "other (prose or a name)"


def files(*roots):
    for r in roots:
        yield from sorted(Path(r).rglob("*.py"))


rows = collections.defaultdict(list)
for p in files("engine", "scripts", "analysis", "tests"):
    src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t); top = str(p).split("/")[0]
    # name -> module, for record_call
    origin = {}
    for n in ast.walk(t):
        if isinstance(n, ast.ImportFrom) and n.module:
            for a in n.names:
                origin[a.asname or a.name] = f"{n.module}.{a.name}"
        if isinstance(n, ast.Import):
            for a in n.names:
                origin[a.asname or a.name.split(".")[0]] = a.name
    joined = {id(v) for n in ast.walk(t) if isinstance(n, ast.JoinedStr) for v in n.values}
    for n in ast.walk(t):
        if isinstance(n, ast.Call):
            f = ast.unparse(n.func); tail = f.rsplit(".", 1)[-1]
            if top != "tests" and tail in ("record_call", "record_active_ollama_call"):
                head = f.rsplit(".", 1)[0] if "." in f else f
                mod = origin.get(head, origin.get(f, "(local or unresolved)"))
                if mod == "(local or unresolved)" and str(p) in ("engine/core/run_manifest.py", "engine/core/extraction_telemetry.py"):
                    mod = str(p)[:-3].replace("/", ".") + " (same module)"
                rows[f"(a) {tail} → {mod}"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | {line(src, n.lineno)}")
            if (tail in ("OpenAI", "Anthropic") and f in ("OpenAI", "Anthropic", "openai.OpenAI", "anthropic.Anthropic")):
                rows[f"(c) SDK client construction {tail} [{top}]"].append(
                    f"{p}:{n.lineno} {sc.get(id(n), '')} | keywords {[k.arg for k in n.keywords]} | {line(src, n.lineno)}")
            if top == "tests":
                if f == "object.__new__" and n.args and ast.unparse(n.args[0]) in CLOUD_CLASSES:
                    rows["(e) object.__new__ of a cloud class"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | {line(src, n.lineno)}")
                if f == "CloudExtractorBase":
                    rows["(e) CloudExtractorBase(...) — PROVIDER \"\""].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | {line(src, n.lineno)}")
                if isinstance(n.func, ast.Attribute) and ast.unparse(n.func.value) in CLOUD_CLASSES and n.func.attr not in ("__new__",) \
                        and n.args:
                    rows["(e) a cloud method called unbound on another object"].append(
                        f"{p}:{n.lineno} {sc.get(id(n), '')} | {line(src, n.lineno)}")
        if isinstance(n, (ast.Constant, ast.JoinedStr)) and id(n) not in joined and top != "tests":
            s = literal_text(n)
            if s is not None and re.search(r"\brun_calls\b", s):
                mig = str(p).startswith("engine/migrations/")
                rows[f"(b) {'[migrations] ' if mig else ''}run_calls — {sql_class(s)}"].append(
                    f"{p}:{n.lineno} {sc.get(id(n), '')} | {line(src, n.lineno)}")
        if top == "tests" and isinstance(n, ast.ClassDef) and any(ast.unparse(b) in CLOUD_CLASSES for b in n.bases):
            init = next((b for b in n.body if isinstance(b, ast.FunctionDef) and b.name == "__init__"), None)
            if init is not None:
                calls = {ast.unparse(c.func) for c in ast.walk(init) if isinstance(c, ast.Call)}
                bases = {ast.unparse(b) for b in n.bases}
                full = any(c in ("super().__init__",) or c == f"{b}.__init__" for c in calls for b in bases if b != "CloudExtractorBase")
                if not full:
                    rows["(e) a subclass of a cloud class whose __init__ skips its base's own constructor"].append(
                        f"{p}:{n.lineno} class {n.name}({', '.join(bases)}) | __init__ calls {sorted(calls)}")
    if top == "tests":
        fixtures = {n.name for n in ast.walk(t) if isinstance(n, ast.FunctionDef)
                    and any("fixture" in ast.unparse(d) for d in n.decorator_list)}
        for fn in (n for n in ast.walk(t) if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")):
            used = {x.attr for x in ast.walk(fn) if isinstance(x, ast.Attribute)} | {x.id for x in ast.walk(fn) if isinstance(x, ast.Name)}
            strs = " ".join(s for s in (literal_text(x) for x in ast.walk(fn) if isinstance(x, (ast.Constant, ast.JoinedStr))) if s)
            hit = sorted((used & {"send", "record_call", "last_request_hash", "_create", "record_active_ollama_call"})
                         | ({"run_calls"} if re.search(r"\brun_calls\b", strs) else set()))
            if hit:
                params = [a.arg for a in fn.args.args if a.arg != "self"]
                rows[f"(d) tests naming {'/'.join(hit)}"].append(
                    f"{p}:{fn.lineno} {fn.name} | params: {','.join(params) or '-'} | file-local fixtures: "
                    f"{','.join(sorted(set(params) & fixtures)) or '-'}")

for key in sorted(rows):
    print(f"\n-- {key}: {len(rows[key])}")
    for r in rows[key]: print("  " + r)

# (f) runtime reach of the U40 guards
CLOUD_TESTS = sorted({r.split(":")[0] for k, v in rows.items() if k.startswith(("(d)", "(e)")) for r in v
                      if r.startswith("tests/")} | {"tests/test_cloud_extraction.py", "tests/test_u27_cloud_run_faults.py",
                                                     "tests/test_u28_cloud_gate_and_boundary.py", "tests/test_u40_cloud_claim_events.py",
                                                     "tests/test_parsed_text_resolver.py", "tests/test_completeness_guard.py",
                                                     "tests/test_corpus_authority.py", "tests/test_request_capture.py",
                                                     "tests/test_run_manifest.py"})
PLUGIN = textwrap.dedent('''
    import json, os
    import pytest
    OUT = os.environ["U29A_GUARD_OUT"]
    _cur = {"id": None}
    def pytest_sessionstart(session):
        from engine.cloud import base
        cls = base.CloudExtractorBase
        orig_send, orig_store = cls.send, cls.store_result
        def rec(method, self):
            row = {"test": _cur["id"], "method": method, "class": type(self).__name__,
                   "lacks__request_chain": not hasattr(self, "_request_chain"),
                   "lacks__text_ref": not hasattr(self, "_text_ref"),
                   "lacks_stage_cfg": not hasattr(self, "stage_cfg")}
            with open(OUT, "a") as f: f.write(json.dumps(row) + "\\n")
        def send(self, *a, **k):
            rec("send", self); return orig_send(self, *a, **k)
        def store_result(self, *a, **k):
            rec("store_result", self); return orig_store(self, *a, **k)
        cls.send, cls.store_result = send, store_result
    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_call(item):
        _cur["id"] = item.nodeid
        yield
''')
with tempfile.TemporaryDirectory() as d:
    Path(d, "u29a_guard_plugin.py").write_text(PLUGIN)
    out = Path(d, "guard.jsonl")
    env = dict(os.environ, U29A_GUARD_OUT=str(out), PYTHONPATH=os.pathsep.join([d, os.environ.get("PYTHONPATH", "")]))
    r = subprocess.run([sys.executable, "-m", "pytest", *CLOUD_TESTS, "-q", "-p", "no:cacheprovider", "-p", "u29a_guard_plugin",
                        "-m", "not network and not ollama and not integration"], capture_output=True, text=True, env=env)
    tail = [l for l in r.stdout.splitlines() if re.search(r"passed|failed|error", l)][-1:]
    recs = [__import__("json").loads(l) for l in out.read_text().splitlines()] if out.exists() else []
print(f"\n-- (f) runtime reach of the U40 guards: {len(CLOUD_TESTS)} files run: {CLOUD_TESTS}")
print(f"   pytest: {tail}")
print(f"   calls recorded: {len(recs)} (send {sum(1 for x in recs if x['method'] == 'send')}, "
      f"store_result {sum(1 for x in recs if x['method'] == 'store_result')})")
G = {"base.py:148 send — getattr(self, \"_request_chain\", None)": ("send", "lacks__request_chain"),
     "base.py:431 store_result — getattr(self, \"_text_ref\", None)": ("store_result", "lacks__text_ref"),
     "base.py:432 store_result — getattr(self, \"_request_chain\", None)": ("store_result", "lacks__request_chain"),
     "base.py:434/444 store_result — getattr(self, \"stage_cfg\", None) and the f\"cloud:{arm}\" fallback": ("store_result", "lacks_stage_cfg")}
for label, (m, k) in G.items():
    tests = sorted({(x["test"], x["class"]) for x in recs if x["method"] == m and x[k]})
    print(f"   {label}: default taken by {len(tests)} test(s)")
    for tid, c in tests: print(f"     {tid} [{c}]")

print("\n== counts")
for key in sorted(rows): print(f"{key}: {len(rows[key])}")
