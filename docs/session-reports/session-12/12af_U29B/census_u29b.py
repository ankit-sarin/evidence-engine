"""12af-U29B — census for U29's build (C24 for Wave 2 row 28; N47; R800(h)), in 12af_U29A/census_u29a.py's form, run before the edit.
(a)–(b) are AST only (no scanned module imported, no database opened); (c) runs the cloud test files under pytest with a plugin written to
a temp directory (never into the repository) that wraps CloudExtractorBase.send and .store_result at session start and records, per test
and call: the instance's class, whether a run is open on it (run_id set), the type of what send returned or raised, and, for store_result,
whether the instance has a provider (stage_cfg present). The conftest fences stay on; nothing is sent; no live database is opened.
usage, from the repository root: .venv/bin/python -I census_u29b.py   (stdout: sections, then counts)
  (a) every test function that names send / extract_paper / .run( on a cloud class or a fixture yielding one / store_result, with its
      parameters, and the run_calls assertions it makes (a string literal naming run_calls in the function);
  (b) every test that constructs or patches an SDK client (openai.OpenAI / anthropic.Anthropic, by name, attribute, or patch string), and
      the constructor keywords it asserts (a Compare naming api_key or max_retries);
  (c) the runtime record: every test that reached send, with the fake's return shape and whether a run was open; every test that stored
      through a provider-less instance (R800(g)).
Declared completeness: (a)–(b) see direct names, attributes and literal strings; not reflection, names bound through variables, or
conftest fixtures. (c) sees only the files it runs (listed in its output) and calls that reach the two methods through the class
attribute it wraps."""
import ast, collections, json, os, re, subprocess, sys, tempfile, textwrap
from pathlib import Path

CLOUD = {"CloudExtractorBase", "OpenAIExtractor", "AnthropicExtractor"}


def lit(n):
    if isinstance(n, ast.Constant) and isinstance(n.value, str): return n.value
    if isinstance(n, ast.JoinedStr):
        return "".join(v.value for v in n.values if isinstance(v, ast.Constant) and isinstance(v.value, str))
    return None


rows = collections.defaultdict(list); files_hit = set()
for p in sorted(Path("tests").rglob("*.py")):
    src = p.read_text(encoding="utf-8"); t = ast.parse(src)
    mentions_cloud = any(isinstance(x, (ast.Name, ast.Attribute)) and (getattr(x, "id", None) or getattr(x, "attr", None)) in CLOUD
                         for x in ast.walk(t)) or "cloud." in src
    for fn in (n for n in ast.walk(t) if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")):
        attrs = {x.attr for x in ast.walk(fn) if isinstance(x, ast.Attribute)}
        strs = [s for s in (lit(x) for x in ast.walk(fn) if isinstance(x, (ast.Constant, ast.JoinedStr))) if s]
        params = [a.arg for a in fn.args.args if a.arg != "self"]
        hit = sorted(attrs & {"send", "extract_paper", "store_result", "extract_with_completeness"}
                     | ({"run"} if mentions_cloud and "run" in attrs else set()))
        rc = [s.strip()[:110] for s in strs if re.search(r"\brun_calls\b", s)]
        if hit and mentions_cloud:
            rows["(a) tests naming the cloud send path"].append(f"{p}:{fn.lineno} {fn.name} | names {hit} | params {params} | run_calls SQL {rc}")
            files_hit.add(str(p))
        sdk = sorted({s for s in strs if re.search(r"openai\.OpenAI|anthropic\.Anthropic", s)}
                     | {ast.unparse(x) for x in ast.walk(fn) if isinstance(x, ast.Attribute) and ast.unparse(x) in ("openai.OpenAI", "anthropic.Anthropic")})
        kw = [ast.unparse(x)[:140] for x in ast.walk(fn) if isinstance(x, ast.Compare) and re.search(r"api_key|max_retries", ast.unparse(x))]
        if sdk or kw:
            rows["(b) tests constructing or patching an SDK client"].append(f"{p}:{fn.lineno} {fn.name} | {sdk} | keyword asserts {kw}")
            files_hit.add(str(p))
    # decorators / fixtures patching SDKs at module level
    for n in ast.walk(t):
        if isinstance(n, ast.FunctionDef) and not n.name.startswith("test_"):
            s = ast.unparse(n)
            if re.search(r"openai\.OpenAI|anthropic\.Anthropic|\"OpenAI\"|\"Anthropic\"", s):
                kw = [ast.unparse(x)[:140] for x in ast.walk(n) if isinstance(x, ast.Compare) and re.search(r"api_key|max_retries", ast.unparse(x))]
                rows["(b) fixtures / helpers patching an SDK client"].append(f"{p}:{n.lineno} {n.name} | keyword asserts {kw}")
                files_hit.add(str(p))

for k in sorted(rows):
    print(f"\n-- {k}: {len(rows[k])}")
    for r in rows[k]: print("  " + r)

RUN = sorted(files_hit | {"tests/test_cloud_extraction.py", "tests/test_u27_cloud_run_faults.py", "tests/test_u28_cloud_gate_and_boundary.py",
                          "tests/test_u40_cloud_claim_events.py", "tests/test_run_manifest.py", "tests/test_request_capture.py",
                          "tests/test_completeness_guard.py", "tests/test_parsed_text_resolver.py", "tests/test_corpus_authority.py"})
PLUGIN = textwrap.dedent('''
    import json, os
    import pytest
    OUT = os.environ["U29B_OUT"]; _cur = {"id": None}
    def _w(row):
        with open(OUT, "a") as f: f.write(json.dumps(row) + "\\n")
    def pytest_sessionstart(session):
        from engine.cloud import base
        cls = base.CloudExtractorBase
        orig_send, orig_store = cls.send, cls.store_result
        def send(self, *a, **k):
            row = {"test": _cur["id"], "m": "send", "class": type(self).__name__, "in_run": getattr(self, "run_id", None) is not None}
            try:
                r = orig_send(self, *a, **k)
                row["returned"] = f"{type(r).__module__}.{type(r).__qualname__}"
                return r
            except BaseException as e:
                row["raised"] = f"{type(e).__module__}.{type(e).__qualname__}"; raise
            finally:
                _w(row)
        def store_result(self, *a, **k):
            _w({"test": _cur["id"], "m": "store_result", "class": type(self).__name__, "provider": hasattr(self, "stage_cfg")})
            return orig_store(self, *a, **k)
        cls.send, cls.store_result = send, store_result
    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_call(item):
        _cur["id"] = item.nodeid
        yield
''')
with tempfile.TemporaryDirectory() as d:
    Path(d, "u29b_plugin.py").write_text(PLUGIN); out = Path(d, "rec.jsonl")
    env = dict(os.environ, U29B_OUT=str(out), PYTHONPATH=os.pathsep.join([d, os.environ.get("PYTHONPATH", "")]))
    r = subprocess.run([sys.executable, "-m", "pytest", *RUN, "-q", "-p", "no:cacheprovider", "-p", "u29b_plugin",
                        "-m", "not network and not ollama and not integration"], capture_output=True, text=True, env=env)
    tail = [l for l in r.stdout.splitlines() if re.search(r"passed|failed|error", l)][-1:]
    recs = [json.loads(l) for l in out.read_text().splitlines()] if out.exists() else []
print(f"\n-- (c) runtime record over {len(RUN)} files: {RUN}\n   pytest: {tail}")
sends = collections.defaultdict(lambda: collections.Counter())
for x in recs:
    if x["m"] == "send":
        sends[(x["test"], x["class"])][(x["in_run"], x.get("returned") or ("raised " + x.get("raised", "?")))] += 1
print(f"   tests that reached send: {len(sends)}")
for (tid, c), cnt in sorted(sends.items()):
    print(f"     {tid} [{c}] " + "; ".join(f"in_run={k[0]} {k[1]} ×{v}" for k, v in sorted(cnt.items())))
pl = sorted({(x["test"], x["class"]) for x in recs if x["m"] == "store_result" and not x["provider"]})
print(f"   tests that called store_result on a provider-less instance (R800(g)): {len(pl)}")
for tid, c in pl: print(f"     {tid} [{c}]")
inrun = sorted({tid for (tid, c), cnt in sends.items() if any(k[0] for k in cnt)})
print(f"   tests whose send ran inside a run (the accessor reads their response): {len(inrun)}")
for tid in inrun: print(f"     {tid}")
print("\n== counts")
for k in sorted(rows): print(f"{k}: {len(rows[k])}")
print(f"send-reaching tests: {len(sends)}; in a run: {len(inrun)}; provider-less stores: {len(pl)}")
