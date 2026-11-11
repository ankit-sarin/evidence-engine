"""12af-U30A — AST census for U30's Phase A ("An interrupted cloud run rolls back and closes interrupted"; INT-i12-71), in
12af_U29A/census_u29a.py's form. Read-only: AST only, no scanned module is imported, no database is opened.
usage, from the repository root: python3 -I census_u30a.py   (stdout: sections, then counts)
The cloud path's files: engine/cloud/*.py, scripts/run_cloud_extraction.py, and the functions of engine/core/run_manifest.py and
engine/core/extraction_events.py the cloud path calls (record_call, close_run, open_run, write_extraction_events).
  (a) every .commit() and .rollback() call in those files and functions, with its enclosing function and the receiver's source;
  (b) every exception handler in those files and functions, by the class(es) it catches, and whether its body re-raises;
  (c) every call of close_run in engine/ and scripts/, with the status argument as written;
  (d) every interrupt-handling construct in engine/ and scripts/: interrupt_signals(), `except (KeyboardInterrupt, …RunInterrupted)`,
      signal.signal(…), exit_process(…);
  (e) test functions coupled to run_arm's close, to store_result's rollback and to get_pending_papers: naming run_arm, close_run with a
      cloud run, store_result, get_pending_papers, get_progress, or KeyboardInterrupt/RunInterrupted together with a cloud class, with
      their parameters.
Declared completeness: direct names, attributes and literals; a call made through a variable or reflection, or a handler in a module
the file list does not name, is not seen."""
import ast, collections
from pathlib import Path

CLOUD_FILES = sorted(Path("engine/cloud").glob("*.py")) + [Path("scripts/run_cloud_extraction.py")]
CORE = {Path("engine/core/run_manifest.py"): {"record_call", "close_run", "open_run"},
        Path("engine/core/extraction_events.py"): {"write_extraction_events"}}


def scopes(tree):
    out = {}
    def walk(node, name):
        for ch in ast.iter_child_nodes(node):
            n = f"{name}.{ch.name}" if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) else name
            out[id(ch)] = n; walk(ch, n)
    walk(tree, "<module>"); return out


def line(src, n): return src.splitlines()[n - 1].strip()[:140]


rows = collections.defaultdict(list)
targets = [(p, None) for p in CLOUD_FILES] + list(CORE.items())
for p, only in targets:
    src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
    nodes = []
    if only is None:
        nodes = list(ast.walk(t))
    else:
        for fn in ast.walk(t):
            if isinstance(fn, ast.FunctionDef) and fn.name in only:
                nodes += list(ast.walk(fn))
    for n in nodes:
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in ("commit", "rollback"):
            rows[f"(a) .{n.func.attr}()"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | receiver {ast.unparse(n.func.value)} | {line(src, n.lineno)}")
        if isinstance(n, ast.ExceptHandler):
            caught = ast.unparse(n.type) if n.type is not None else "(bare)"
            reraise = any(isinstance(x, ast.Raise) for x in ast.walk(n))
            rows["(b) handlers on the cloud path"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | except {caught} | re-raises: {reraise}")
for p in sorted(Path("engine").rglob("*.py")) + sorted(Path("scripts").rglob("*.py")):
    src = p.read_text(encoding="utf-8"); t = ast.parse(src); sc = scopes(t)
    for n in ast.walk(t):
        if isinstance(n, ast.Call):
            f = ast.unparse(n.func)
            if f.rsplit(".", 1)[-1] == "close_run" and not str(p).endswith("run_manifest.py"):
                status = ast.unparse(n.args[2]) if len(n.args) > 2 else next((ast.unparse(k.value) for k in n.keywords if k.arg == "status"), "(default 'completed')")
                rows["(c) close_run calls"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | status {status} | {line(src, n.lineno)}")
            if f.rsplit(".", 1)[-1] in ("interrupt_signals", "exit_process") or f == "signal.signal":
                rows["(d) interrupt constructs"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | {f}(…) | {line(src, n.lineno)}")
        if isinstance(n, ast.ExceptHandler) and n.type is not None and ("KeyboardInterrupt" in ast.unparse(n.type) or "RunInterrupted" in ast.unparse(n.type)):
            rows["(d) interrupt constructs"].append(f"{p}:{n.lineno} {sc.get(id(n), '')} | except {ast.unparse(n.type)}")
CLOUD = {"CloudExtractorBase", "OpenAIExtractor", "AnthropicExtractor"}
for p in sorted(Path("tests").rglob("*.py")):
    src = p.read_text(encoding="utf-8"); t = ast.parse(src)
    cloudfile = "engine.cloud" in src or "run_cloud_extraction" in src
    for fn in (n for n in ast.walk(t) if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")):
        names = {x.attr for x in ast.walk(fn) if isinstance(x, ast.Attribute)} | {x.id for x in ast.walk(fn) if isinstance(x, ast.Name)}
        hit = sorted(names & {"run_arm", "store_result", "get_pending_papers", "get_progress"})
        if cloudfile and names & {"close_run", "KeyboardInterrupt", "RunInterrupted", "is_closed"}:
            hit += sorted(names & {"close_run", "KeyboardInterrupt", "RunInterrupted", "is_closed"})
        if hit and cloudfile:
            rows["(e) tests coupled"].append(f"{p}:{fn.lineno} {fn.name} | names {hit} | params {[a.arg for a in fn.args.args if a.arg != 'self']}")
for k in sorted(rows):
    print(f"\n-- {k}: {len(rows[k])}")
    for r in rows[k]: print("  " + r)
print("\n== counts")
for k in sorted(rows): print(f"{k}: {len(rows[k])}")
