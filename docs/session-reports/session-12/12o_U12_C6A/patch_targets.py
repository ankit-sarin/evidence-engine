"""12o-U12-C6A step 4 — every mock.patch target under tests/ that resolves through engine.cloud.
AST only. Prints file:line, the callee, the target, and a summary count."""
import ast
from collections import Counter
from pathlib import Path
rows = []
for p in sorted(Path("tests").rglob("*.py")):
    for n in ast.walk(ast.parse(p.read_text(encoding="utf-8"))):
        if isinstance(n, ast.Call) and n.args and isinstance(n.args[0], ast.Constant) \
                and isinstance(n.args[0].value, str) and n.args[0].value.startswith("engine.cloud"):
            f = ast.unparse(n.func)
            rows.append((str(p), n.lineno, f, n.args[0].value))
for r in rows: print(f"{r[0]}:{r[1]}  {r[2]}  {r[3]!r}")
pt = [r for r in rows if r[2].split(".")[-1] == "patch"]
print("patch() targets:", len(pt), "| by target:", dict(Counter(r[3] for r in pt)))
print("other calls with an engine.cloud string first argument:", [(r[0], r[1], r[2]) for r in rows if r not in pt])
