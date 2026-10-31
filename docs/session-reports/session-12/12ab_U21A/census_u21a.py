"""12ab-U21A — AST census of U21's sites (B-F14, N23, N24, F7) and of the tests coupled to them, in
12aa_U20A/census_u20a.py's form. Read-only: AST only, no scanned module is imported, no database is opened.
usage, from the repository root: census_u21a.py   (stdout: sections, then counts)
Sites: engine/, and scripts/ and analysis/ reported apart. Tests: the five classes (R722(a), R723(c), R724(a),
R725(b)) plus hard-coded outcome and message strings. Declared completeness: direct names, attribute calls, patch-target
strings, setattr on an alias named oc, _oc, ollama_client or client_mod, literal strings. It does not see a name
assembled at runtime, a patch whose target is not a literal, or a test coupled only through a fixture's behaviour."""
import ast, collections, re, sys
from pathlib import Path
SEND = {"ollama_chat", "chat", "generate", "embed", "embeddings"}
SEND_OWNERS = {"_client", "ollama", "client"}
LANE = {"MODEL_TIMEOUTS", "DEFAULT_MAX_RETRIES", "DEFAULT_RETRY_DELAY", "_HTTP_READ_TIMEOUT", "_HTTP_CONNECT_TIMEOUT", "_httpx_timeout",
        "_DEFAULT_WALL_TIMEOUT", "_wall_timeout_for_model", "ThreadPoolExecutor", "FuturesTimeoutError", "_restart_ollama_and_retry",
        "restart_disabled", "RESTART_OPT_OUT_ENV", "foreign_lock_held", "record_active_ollama_call", "record_call", "restart_ollama",
        "RUN_CALL_OUTCOMES", "_build_client"}
ARG_PINS = {"max_retries", "retry_delay", "wall_timeout", "effective_timeout"}
OUTCOMES = ("completed", "refused_input_overflow", "refused_ceiling_unavailable", "refused_input_truncated", "refused_input_dropped")
MSG = ("timed out after", "wall-clock", "HTTP timeout", "Post-restart", "restart refused", "Restart refused", "restart failed", "All %d retries", "cannot recover", "outcome_detail", "model_call_failed")
ALIASES = {"oc", "_oc", "ollama_client", "client_mod"}
def scopes(tree):
    out = {}
    def walk(node, name):
        for ch in ast.iter_child_nodes(node):
            n = f"{name}.{ch.name}" if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) else name
            out[id(ch)] = n; walk(ch, n)
    walk(tree, "<module>"); return out
def line(src, n): return src.splitlines()[n - 1].strip()[:150]
sites = collections.defaultdict(list)
for root in ("engine", "scripts", "analysis"):
    for p in sorted(Path(root).rglob("*.py")):
        src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
        for n in ast.walk(t):
            if isinstance(n, ast.Call):
                f = ast.unparse(n.func); tail = f.rsplit(".", 1)[-1]; owner = f.rsplit(".", 1)[0] if "." in f else ""
                if f == "ollama_chat" or tail == "ollama_chat":
                    sites[root, "send: ollama_chat"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
                elif tail in SEND and owner.rsplit(".", 1)[-1] in SEND_OWNERS:
                    sites[root, "send: direct client call"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
                if tail in ("run", "Popen", "check_call", "check_output", "call") and "subprocess" in f:
                    a = ast.unparse(n.args[0]) if n.args else ""
                    if "systemctl" in a and "restart" in a:
                        sites[root, "restart: systemctl restart"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
                if tail in ("submit",) and n.args and "chat" in ast.unparse(n.args[0]):
                    sites[root, "submit: client chat to an executor"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
                if tail == "ThreadPoolExecutor" or f == "ThreadPoolExecutor":
                    sites[root, "executor created"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
                if tail == "shutdown":
                    sites[root, "executor shutdown"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
                if tail in ("record_active_ollama_call", "record_call"):
                    sites[root, "run_calls write"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and re.search(r"/api/(generate|chat|embed)\b", n.value):
                sites[root, "literal model endpoint"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
tests = collections.defaultdict(list)
for p in sorted(Path("tests").rglob("*.py")):
    src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
    for n in ast.walk(t):
        if isinstance(n, ast.Call):
            f = ast.unparse(n.func)
            if f.endswith("setattr") and len(n.args) >= 2:
                a = ast.unparse(n.args[0]); b = ast.unparse(n.args[1]).strip("'\"")
                if a in ALIASES and b == "_client": tests["ii_module_alias_client_replaced"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
                elif b == "ollama_chat": tests["v_whole_function_ollama_chat"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
                elif b in LANE: tests["i_patch_name"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
                elif b in ("sleep", "run") and a in ALIASES | {"time", "subprocess"}: tests["i_patch_name"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
                elif re.search(r"ollama_client\.", ast.unparse(n.args[0])): tests["i_patch_string"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
            if f in ("patch", "patch.object", "mock.patch", "mock.patch.object") and n.args:
                s = " ".join(ast.unparse(x) for x in n.args[:2])
                if re.search(r"ollama_chat\b", s): tests["v_whole_function_ollama_chat"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
                elif re.search(r"ollama_client\.(_client|time|subprocess|ThreadPoolExecutor|_restart_ollama_and_retry|foreign_lock_held|restart_disabled|MODEL_TIMEOUTS|_wall_timeout_for_model|DEFAULT_)", s) or re.search("|".join(sorted(LANE)), s):
                    tests["i_patch_string"].append((str(p), n.lineno, sc.get(id(n), ""), line(src, n.lineno)))
            for kw in n.keywords:
                if kw.arg in ARG_PINS and ast.unparse(n.func).rsplit(".", 1)[-1] in ("ollama_chat", "_restart_ollama_and_retry", "chat"):
                    tests["iii_signature_kw"].append((str(p), n.lineno, sc.get(id(n), ""), f"{kw.arg}={ast.unparse(kw.value)}"))
            if f in ("pytest.raises",) and n.args and ast.unparse(n.args[0]) in ("TimeoutError", "httpx.TimeoutException", "httpx.ReadTimeout", "httpx.ConnectError", "RuntimeError", "ConnectionError"):
                tests["iv_exception_class"].append((str(p), n.lineno, sc.get(id(n), ""), ast.unparse(n.args[0])))
        if isinstance(n, ast.Name) and n.id in LANE: tests["i_direct_name"].append((str(p), n.lineno, sc.get(id(n), ""), n.id))
        if isinstance(n, ast.Attribute) and n.attr in LANE: tests["i_direct_name"].append((str(p), n.lineno, sc.get(id(n), ""), n.attr))
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            v = n.value
            if v == "error" or v in OUTCOMES or re.search(r"\boutcome\b", v) and ("run_calls" in v or "SELECT" in v):
                if "run_calls" in src or "outcome" in src: tests["iii_outcome_string"].append((str(p), n.lineno, sc.get(id(n), ""), v[:90].replace("\n", " ")))
            for m in MSG:
                if m in v: tests["vi_message_string"].append((str(p), n.lineno, sc.get(id(n), ""), v[:90].replace("\n", " "))); break
            if "run_calls" in v and "SELECT" in v.upper(): tests["iii_run_calls_query"].append((str(p), n.lineno, sc.get(id(n), ""), " ".join(v.split())[:110]))
print("# SITES"); 
for (root, k), v in sorted(sites.items()):
    print(f"## {root} / {k}: {len(v)}")
    for x in v: print("  ", *x)
print("\n# TESTS")
for k, v in sorted(tests.items()):
    print(f"## {k}: {len(v)} hits in {len({x[0] for x in v})} files")
    c = collections.Counter(x[0] for x in v)
    print("   by file:", ", ".join(f"{a} {b}" for a, b in c.most_common()))
    for x in v: print("  ", *x)
print("\n# COUNTS")
for (root, k), v in sorted(sites.items()): print(f"site {root} / {k}: {len(v)}")
for k, v in sorted(tests.items()): print(f"test {k}: {len(v)} / {len({x[0] for x in v})} files")
