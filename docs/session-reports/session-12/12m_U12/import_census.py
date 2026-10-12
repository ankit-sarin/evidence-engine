"""12m-U12-READ step 6 — third-party top-level imports under engine/, scripts/, analysis/, tests/
against requirements.txt. Read-only: AST for the imports, importlib.metadata for the installed
distribution and version (no install, no import of the packages themselves).
usage, from the repository root, with the project interpreter: import_census.py   (stdout: JSON)"""
import ast, json, os, re, sys
from importlib import metadata
ROOTS = ("engine", "scripts", "analysis", "tests")
local = {"engine", "scripts", "analysis", "tests"}
for r in ROOTS:                                  # helper modules imported by bare name (sys.path edits)
    for d, _, fs in os.walk(r):
        local |= {f[:-3] for f in fs if f.endswith(".py")} | {os.path.basename(d)}
uses = {}
for r in ROOTS:
    for d, _, fs in os.walk(r):
        for f in fs:
            if not f.endswith(".py"): continue
            p = os.path.join(d, f)
            for n in ast.walk(ast.parse(open(p, encoding="utf-8").read())):
                names = ([a.name for a in n.names] if isinstance(n, ast.Import)
                         else [n.module] if isinstance(n, ast.ImportFrom) and not n.level and n.module else [])
                for nm in names:
                    top = nm.split(".")[0]
                    if top in sys.stdlib_module_names or top in local or top == "__future__": continue
                    uses.setdefault(top, {}).setdefault(r, set()).add(p)
canon = lambda s: re.sub(r"[-_.]+", "-", s).lower()
declared = {}
for line in open("requirements.txt", encoding="utf-8"):
    body = line.split("#")[0].strip()
    if not body or not re.match(r"[A-Za-z0-9]", body): continue
    m = re.match(r"([A-Za-z0-9_.\-]+)\s*(.*)", body)
    declared[canon(m[1])] = {"name": m[1], "spec": m[2].strip(), "pinned": m[2].strip().startswith("==")}
dists = metadata.packages_distributions()
def version(d):
    try: return metadata.version(d)
    except metadata.PackageNotFoundError: return None
imports = []
for top in sorted(uses, key=str.lower):
    ds = sorted({canon(d) for d in dists.get(top, [])})
    hit = [d for d in ds if d in declared]
    imports.append({"import": top, "distribution": ds, "installed": {d: version(d) for d in ds},
                    "declared": bool(hit), "pinned": any(declared[d]["pinned"] for d in hit),
                    "roots": {r: len(ps) for r, ps in sorted(uses[top].items())},
                    "example": sorted(next(iter(sorted(uses[top].items())))[1])[0]})
imported_dists = {d for i in imports for d in i["distribution"]}
decl = [{"requirement": v["name"], "spec": v["spec"], "pinned": v["pinned"], "installed": version(v["name"]),
         "imported_under": sorted({r for i in imports if k in i["distribution"] for r in i["roots"]})}
        for k, v in declared.items()]
print(json.dumps({"declared": decl, "declared_count": len(decl),
                  "declared_unpinned": [d["requirement"] for d in decl if not d["pinned"]],
                  "declared_not_imported": [d["requirement"] for d in decl if not d["imported_under"]],
                  "imported_undeclared": [i["import"] for i in imports if not i["declared"]],
                  "imports": imports}, indent=1))
