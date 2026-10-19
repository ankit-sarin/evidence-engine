"""12s-U15A — AST census of the readers and writers of the processing axis's completion facts:
`audited_ai`, `analysis_ready`, `COMPLETED_PROCESSING_STATES`, `effective_state(...).processing`,
`processing_reason`, `audit_run`, `eligible_paper_ids`. Read-only: AST only, nothing imported, no
database opened. The 12s_U14A census form (R676(d)(iv)).
usage, from the repository root: census_u15a.py   (stdout: one line per site, then counts)"""
import ast
from pathlib import Path
ROOTS = ("engine", "scripts", "analysis", "tests")
NAMES = {"analysis_ready", "COMPLETED_PROCESSING_STATES", "audit_run", "eligible_paper_ids",
         "effective_state", "processing_reason", "FAILURE_STATES", "is_failure"}
ATTRS = {"analysis_ready", "processing", "processing_reason"}
STRINGS = ("audited_ai", "analysis_ready", "studies_included")
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
            elif isinstance(node, ast.Attribute) and node.attr in NAMES | ATTRS:
                kind = f"attr .{node.attr} ({'store' if isinstance(node.ctx, ast.Store) else 'load'})"
            elif isinstance(node, ast.alias) and node.name in NAMES: kind = f"import {node.name}"
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                hit = [s for s in STRINGS if s in node.value]
                if hit: kind = f"string {hit} " + (repr(node.value[:90]) if len(node.value) < 300 else f"<{len(node.value)} chars>")
            if kind: rows.append((str(p), getattr(node, "lineno", 0), where, kind))
for r in rows: print(f"{r[0]}:{r[1]}  [{r[2]}]  {r[3]}")
print("files with a site:", len({r[0] for r in rows}), "| sites:", len(rows), "| unparsed:", unparsed or "none")
