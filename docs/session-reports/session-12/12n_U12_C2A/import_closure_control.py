"""12n-U12-C2A A3 control — for every entry point the lock census puts in category "none" or
"cloud only", does its transitive IMPORT closure (function-level imports included) contain a module
that can call the local server? An entry listed here is not thereby a caller: it is a candidate the
reference resolution may have missed, to be read by hand. usage: import_closure_control.py lock_census.json"""
import ast, json, os, sys
ROOTS = ("engine", "scripts", "analysis")
WATCH = {"engine.utils.ollama_client", "engine.utils.ollama_preflight", "engine.agents.extractor", "ollama"}
mods = {}
for root in ROOTS:
    for d, _, fs in os.walk(root):
        for f in fs:
            if f.endswith(".py"):
                p = os.path.join(d, f); n = p[:-3].replace("/", ".")
                pkg = n.endswith(".__init__"); n = n[:-9] if pkg else n
                mods[n] = (p, pkg)
def imports(n):
    p, pkg = mods[n]; out = set()
    for x in ast.walk(ast.parse(open(p, encoding="utf-8").read())):
        if isinstance(x, ast.Import):
            for a in x.names: out.add(a.name)
        elif isinstance(x, ast.ImportFrom):
            if x.level:
                parts = n.split(".") if pkg else n.split(".")[:-1]
                parts = parts[:len(parts) - (x.level - 1)]; base = ".".join(parts + ([x.module] if x.module else []))
            else: base = x.module or ""
            out.add(base)
            for a in x.names: out.add(f"{base}.{a.name}")
    res = set()
    for i in out:
        parts = i.split(".")
        for k in range(len(parts), 0, -1):
            c = ".".join(parts[:k])
            if c in mods or c in WATCH: res.add(c)       # the module and every parent package
    return res
cache = {}
def closure(n):
    seen = set(); todo = [n]
    while todo:
        k = todo.pop()
        if k in seen or k not in mods: seen.add(k); continue
        seen.add(k)
        if k not in cache: cache[k] = imports(k)
        todo += list(cache[k])
    return seen
rows = json.load(open(sys.argv[1]))["rows"]
for r in rows:
    if r["category"] in ("none", "cloud only"):
        n = r["entry"][:-3].replace("/", ".")
        hit = sorted(closure(n) & WATCH)
        if hit: print(r["entry"], "|", r["category"], "|", ", ".join(hit))
