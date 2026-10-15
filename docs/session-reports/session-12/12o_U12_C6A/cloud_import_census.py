"""12o-U12-C6A — every importer of engine.cloud, by AST. Read-only: parses, never imports.

Reports each Import / ImportFrom naming engine.cloud (any form, incl. `from engine import cloud`),
its scope (module-level or inside a def), and every string constant that names engine.cloud
(importlib strings, mock.patch targets). Run from the repo root.
"""
import ast, json, sys
from pathlib import Path

ROOTS = ["engine", "scripts", "analysis", "tests"]

def scope_map(tree):
    out = {}
    def walk(node, fn):
        for ch in ast.iter_child_nodes(node):
            f = ch.name if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef)) else fn
            out[id(ch)] = fn
            walk(ch, f)
    walk(tree, None)
    return out

rows, strings = [], []
for root in ROOTS:
    for p in sorted(Path(root).rglob("*.py")):
        tree = ast.parse(p.read_text(encoding="utf-8"), filename=str(p))
        sc = scope_map(tree)
        for n in ast.walk(tree):
            if isinstance(n, ast.Import):
                for a in n.names:
                    if a.name == "engine.cloud" or a.name.startswith("engine.cloud."):
                        rows.append(dict(file=str(p), line=n.lineno, stmt=f"import {a.name}", names=[a.asname or a.name], scope=sc.get(id(n)) or "module"))
            elif isinstance(n, ast.ImportFrom) and n.level == 0 and n.module:
                m = n.module
                hit = m == "engine.cloud" or m.startswith("engine.cloud.") or (m == "engine" and any(a.name == "cloud" for a in n.names))
                if hit:
                    rows.append(dict(file=str(p), line=n.lineno, stmt=f"from {m} import " + ", ".join(a.name for a in n.names), names=[a.name for a in n.names], scope=sc.get(id(n)) or "module"))
            elif isinstance(n, ast.ImportFrom) and n.level and str(p).startswith("engine/cloud/"):
                rows.append(dict(file=str(p), line=n.lineno, stmt=f"from {'.'*n.level}{n.module or ''} import ...", names=[a.name for a in n.names], scope=sc.get(id(n)) or "module"))
            elif isinstance(n, ast.Constant) and isinstance(n.value, str) and "engine.cloud" in n.value and len(n.value) < 200:
                strings.append(dict(file=str(p), line=n.lineno, value=n.value, scope=sc.get(id(n)) or "module"))
json.dump({"imports": rows, "strings": strings}, sys.stdout, indent=1)
