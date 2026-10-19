"""12s-U14A — AST census of the locator's names and of every read of `located` / `bridged` /
`locator_version`. Read-only: AST only, no scanned module is imported, no database is opened.
usage, from the repository root: census_u14a.py   (stdout: one line per site, then counts)
Why a separate census: docs/inventory/ (engine.tools.inventory) records entry points and
configuration-authority readers; it has no call graph, so it cannot answer "who calls locate"."""
import ast, sys
from collections import Counter
from pathlib import Path
ROOTS = ("engine", "scripts", "analysis", "tests")
NAMES = {"locate", "locate_payload", "LOCATOR_VERSION", "FUZZY_THRESHOLD", "INVALID_SNIPPET_RE",
         "_has_invalid_snippet", "LocateResult", "LOCATOR_ACTOR", "audit_span"}
FIELDS = ("located", "bridged", "locator_version")
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
            elif isinstance(node, ast.alias) and (node.name in NAMES or (node.asname or "") in NAMES):
                kind = f"import {node.name}" + (f" as {node.asname}" if node.asname else "")
            elif isinstance(node, ast.Attribute) and node.attr in FIELDS:
                kind = f"attr .{node.attr} ({'store' if isinstance(node.ctx, ast.Store) else 'load'})"
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                hit = [f for f in FIELDS if f in node.value]
                if hit and len(node.value) < 400: kind = f"string {hit} {node.value[:90]!r}"
                elif hit: kind = f"string {hit} <{len(node.value)} chars>"
            elif isinstance(node, ast.keyword) and node.arg in FIELDS: kind = f"kwarg {node.arg}="
            if kind: rows.append((str(p), getattr(node, "lineno", 0), where, kind))
for r in rows: print(f"{r[0]}:{r[1]}  [{r[2]}]  {r[3]}")
print("files with a site:", len({r[0] for r in rows}), "| sites:", len(rows), "| unparsed:", unparsed or "none")
