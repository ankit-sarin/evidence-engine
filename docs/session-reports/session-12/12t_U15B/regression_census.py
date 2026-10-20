"""12t-U15B — the T9 regression set: every test function that reaches audit_run, directly or through a
module-level helper in its own file (run, _stage_audit callers). AST only, nothing imported.
usage, from the repository root: regression_census.py   (stdout: one id per line, then a count)"""
import ast
from pathlib import Path
TARGETS = {"audit_run", "_stage_audit"}
ids = []
for p in sorted(Path("tests").glob("test_*.py")):
    tree = ast.parse(p.read_text(encoding="utf-8"))
    funcs = {n.name: n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    def calls(fn):
        out = set()
        for n in ast.walk(fn):
            if isinstance(n, ast.Call):
                f = n.func
                out.add(f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None))
        return out
    reach = {k for k, f in funcs.items() if calls(f) & TARGETS}
    changed = True
    while changed:
        changed = False
        for k, f in funcs.items():
            if k not in reach and calls(f) & reach:
                reach.add(k); changed = True
    ids += [f"{p}::{k}" for k in funcs if k.startswith("test_") and k in reach]
print("\n".join(ids)); print(f"# {len(ids)} test functions")
