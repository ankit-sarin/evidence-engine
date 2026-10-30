"""12z-U19A — AST census of U19's sites (INT-j12-17) and of the tests coupled to them, in
12s_U14A/census_u14a.py's form. Read-only: AST only, no scanned module is imported, no database
is opened. usage, from the repository root: census_u19a.py   (stdout: one line per site, then counts)
Why a separate census: docs/inventory/ (engine.tools.inventory) records entry points and
configuration-authority readers and censuses no test file (B11, R676(d)(iv)), so it cannot answer
"which test patches check_ollama_env" or "which test pins the manifest's key set".
Five classes (R722(a), R723(c), R724(a), R725(b)): a name or a patch-target string naming a site;
a module alias of a site's module patched by attribute; a record key set (the manifest body's
existing key names); a class membership set (the run-fault tuple, RUN_REFUSED_MEMBERS); a whole-function
replacement. The classification of each test hit is done in the read-out from these lines."""
import ast, sys
from collections import Counter
from pathlib import Path
ROOTS = ("engine", "scripts", "analysis", "tests")
# The sites the row, its reader and the unit entry name (3a).
NAMES = {"_get_ollama_env", "check_ollama_env", "require_preflight", "_diff_env", "_CORRECTNESS_ENV",
         "_PERFORMANCE_ENV", "preflight_check", "restart_ollama", "_restart_ollama_and_retry",
         "RESTART_EVERY_N", "server_context_length", "SERVICE_UNIT_FILE", "SERVICE_DROPIN_DIR",
         "restart_disabled", "foreign_lock_held", "open_run", "run_fault_classes", "is_run_fault",
         "RUN_REFUSED_MEMBERS", "RunRefused"}
# Modules whose attributes a test may patch through an alias.
MODULES = {"engine.utils.ollama_preflight", "engine.agents.extractor", "engine.utils.ollama_client",
           "engine.core.run_manifest"}
# The manifest body's existing key names (open_run's `manifest` dict at HEAD), R724(a)'s pattern.
BODY_KEYS = {"run_uid", "review_id", "run_kind", "git", "engine_state", "spec_hash", "codebook",
             "libraries", "host", "ollama_host", "started_at", "cloud_arms", "payload_description",
             "stages", "pins", "lineage_check", "selection_bound", "inputs"}
STRINGS = ("systemctl", "Environment=", "OLLAMA_KV_CACHE_TYPE", "OLLAMA_FLASH_ATTENTION",
           "OLLAMA_MAX_LOADED_MODELS", "OLLAMA_NUM_PARALLEL", "OLLAMA_CONTEXT_LENGTH", "override.conf")
def scopes(tree):
    out = {}
    def walk(node, name):
        for ch in ast.iter_child_nodes(node):
            n = f"{name}.{ch.name}" if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) else name
            out[id(ch)] = n; walk(ch, n)
    walk(tree, "<module>"); return out
rows, unparsed = [], []
for root in ROOTS:
    for p in sorted(Path(root).rglob("*.py")):
        try: tree = ast.parse(p.read_text(encoding="utf-8"))
        except SyntaxError: unparsed.append(str(p)); continue
        sc = scopes(tree)
        for node in ast.walk(tree):
            where = sc.get(id(node), "<module>"); kind = None
            if isinstance(node, ast.Name) and node.id in NAMES: kind = f"name {node.id}"
            elif isinstance(node, ast.Attribute) and node.attr in NAMES: kind = f"name .{node.attr}"
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in NAMES:
                kind = f"def {node.name}"
            elif isinstance(node, ast.alias) and (node.name in NAMES | MODULES or (node.asname or "") in NAMES):
                kind = f"import {node.name}" + (f" as {node.asname}" if node.asname else "")
            elif isinstance(node, ast.ImportFrom) and node.module in {m.rsplit(".", 1)[0] for m in MODULES}:
                al = [a for a in node.names if f"{node.module}.{a.name}" in MODULES]
                if al: kind = "import module " + ", ".join(f"{node.module}.{a.name}" + (f" as {a.asname}" if a.asname else "") for a in al)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str) and len(node.value) < 400:
                s = node.value
                hit = [n for n in NAMES if n in s and ("." in s or s == n)]
                if hit: kind = f"string-name {sorted(hit)} {s[:100]!r}"
                elif any(t in s for t in STRINGS): kind = f"string {[t for t in STRINGS if t in s]} {s[:100]!r}"
                elif str(p).startswith("tests") and s in BODY_KEYS: kind = f"body-key {s!r}"
            if kind: rows.append((str(p), getattr(node, "lineno", 0), where, kind))
for r in rows: print(f"{r[0]}:{r[1]}  [{r[2]}]  {r[3]}")
c = Counter(r[0].split("/")[0] for r in rows)
print(f"# sites {len(rows)} by root {dict(sorted(c.items()))}; files {len({r[0] for r in rows})}; unparsed {unparsed}")
