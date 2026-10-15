"""12o-U12-C7 step 1(b) — every top-level third-party import name under engine/, tracked scripts/,
analysis/ and tests/, by AST; mapped to distributions by importlib.metadata.packages_distributions().
Read-only: parses, never imports a scanned file. Run from the repo root with the project interpreter."""
import ast, json, subprocess, sys
import importlib.metadata as md
from collections import defaultdict
from pathlib import Path

FIRST_PARTY = {"engine", "analysis", "tests", "scripts", "conftest", "_live_db_guard"}
tracked = set(subprocess.run(["git", "ls-files", "*.py"], capture_output=True, text=True, check=True).stdout.split())
where = defaultdict(set)
local_names = set()
for root in ("engine", "scripts", "analysis", "tests"):
    for p in sorted(Path(root).rglob("*.py")):
        if str(p) not in tracked:
            continue
        local_names.add(p.stem)
        for n in ast.walk(ast.parse(p.read_text(encoding="utf-8"), filename=str(p))):
            if isinstance(n, ast.Import):
                names = [a.name.split(".")[0] for a in n.names]
            elif isinstance(n, ast.ImportFrom) and n.level == 0 and n.module:
                names = [n.module.split(".")[0]]
            else:
                continue
            for name in names:
                where[name].add(root)
p2d = md.packages_distributions()
rows = []
for name in sorted(where):
    if name in sys.stdlib_module_names or name in FIRST_PARTY or name == "__future__":
        continue
    dists = sorted(set(p2d.get(name, [])))
    rows.append({"import": name, "distributions": dists,
                 "versions": {d: md.version(d) for d in dists},
                 "where": sorted(where[name]),
                 "local_module_of_same_name": name in local_names})
json.dump(rows, sys.stdout, indent=1)
