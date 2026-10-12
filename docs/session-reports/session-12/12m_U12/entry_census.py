"""12m-U12-READ step 4 — entry points under engine/, scripts/, analysis/ and what each can reach.
Read-only, AST only; imports nothing from the repository. usage, from the repository root:
    entry_census.py            (stdout: JSON)
An ENTRY POINT is a module with an `if __name__ == "__main__":` block; scripts/*.sh are listed.
REACH follows resolved references from the module's top-level code (its `__main__` block included):
  * a bare name -> the function or class of that name in the same module, or the object a
    `from m import name` binds (re-exports followed);
  * `alias.name` where alias is an imported repository module -> that module's function or class;
  * a class -> all its methods; `self.name` / `cls.name` -> a method of that name in the same module;
  * any other `x.name` -> methods called `name` on classes of the caller's module and of the
    repository modules it imports directly (an over-approximation, one import hop only).
A reference counts whether called or passed (executor.submit(f), callbacks). Dynamic dispatch the
AST cannot see (importlib, a dict of classes built elsewhere, the migration runner's
module.run_migration) is NOT followed: a "." is "no resolved path", to be read against the grep
of call sites in the read-out. Every `ollama_chat` caller can also reach the last-resort restart
(`_restart_ollama_and_retry` is called from inside `ollama_chat`), so that is not a separate column."""
import ast, json, os
ROOTS = ("engine", "scripts", "analysis")
SINK = {"local_call": {("engine.utils.ollama_client", "ollama_chat")},
        "restart_proactive": {("engine.agents.extractor", "restart_ollama")},
        "preflight_probe": {("engine.utils.ollama_preflight", "check_model")},
        "cloud_call": {("engine.cloud.openai_extractor", "send"), ("engine.cloud.anthropic_extractor", "send")},
        "holds_lock": {("engine.utils.ollama_lock", "hold_experiment_lock")},
        "probes_lock": {("engine.utils.ollama_lock", "foreign_lock_held"),
                        ("engine.utils.ollama_lock", "check_experiment_lock")}}
mods = {}
for root in ROOTS:
    for d, _, fs in os.walk(root):
        for f in sorted(fs):
            if f.endswith(".py"):
                p = os.path.join(d, f); name = p[:-3].replace("/", ".")
                pkg = name.endswith(".__init__")
                if pkg: name = name[:-9]
                mods[name] = {"path": p, "tree": ast.parse(open(p, encoding="utf-8").read()), "pkg": pkg}
def absolute(name, node):
    if not node.level: return node.module or ""
    parts = name.split(".") if mods[name]["pkg"] else name.split(".")[:-1]
    parts = parts[:len(parts) - (node.level - 1)]
    return ".".join(parts + ([node.module] if node.module else []))
for name, m in mods.items():
    t = m["tree"]; m["names"] = {}; m["aliases"] = {}; m["defs"] = {}; m["classes"] = {}; m["methods"] = {}
    for x in ast.walk(t):
        if isinstance(x, ast.Import):
            for a in x.names:
                if a.name in mods: m["aliases"][a.asname or a.name.split(".")[-1] if a.asname else a.name] = a.name
                if a.asname is None and "." in a.name and a.name in mods: m["aliases"][a.name] = a.name
        elif isinstance(x, ast.ImportFrom):
            base = absolute(name, x)
            for a in x.names:
                full = f"{base}.{a.name}"
                if full in mods: m["aliases"][a.asname or a.name] = full
                elif base in mods: m["names"][a.asname or a.name] = (base, a.name)
    for x in t.body:
        if isinstance(x, (ast.FunctionDef, ast.AsyncFunctionDef)): m["defs"][x.name] = x
        elif isinstance(x, ast.ClassDef):
            m["classes"][x.name] = x
            for y in ast.walk(x):
                if isinstance(y, (ast.FunctionDef, ast.AsyncFunctionDef)): m["methods"].setdefault(y.name, []).append(y)
    m["direct"] = set(m["aliases"].values()) | {b for b, _ in m["names"].values()}
def dotted(n):
    parts = []
    while isinstance(n, ast.Attribute): parts.append(n.attr); n = n.value
    return (n.id, parts[::-1]) if isinstance(n, ast.Name) else (None, None)
def resolve_name(mod, nm, depth=0):
    m = mods[mod]
    if nm in m["defs"]: return [("f", mod, nm)]
    if nm in m["classes"]: return [("c", mod, nm)]
    if nm in m["names"] and depth < 6:
        b, n = m["names"][nm]; return resolve_name(b, n, depth + 1)
    return []
def refs(mod, node):
    out = []; m = mods[mod]
    for n in ast.walk(node):
        if isinstance(n, ast.Name): out += resolve_name(mod, n.id)
        elif isinstance(n, ast.Attribute):
            head, parts = dotted(n)
            if head is None: 
                out += [("m", mm, n.attr) for mm in {mod} | m["direct"] if n.attr in mods[mm]["methods"]]; continue
            for k in range(len(parts), 0, -1):          # alias.sub.func
                cand = ".".join([head] + parts[:k - 1])
                target = m["aliases"].get(cand) or (m["aliases"].get(head) and ".".join([m["aliases"][head]] + parts[:k - 1]))
                if target in mods: out += resolve_name(target, parts[k - 1]); break
            else:
                pass
            if head in ("self", "cls"):
                if n.attr in m["methods"]: out.append(("m", mod, n.attr))
            elif head not in m["aliases"]:
                out += [("m", mm, n.attr) for mm in {mod} | m["direct"] if n.attr in mods[mm]["methods"]]
    return out
def body_of(kind, mod, nm):
    m = mods[mod]
    if kind == "f": return [m["defs"][nm]]
    if kind == "c": return [m["classes"][nm]]
    return m["methods"][nm]
rows = []
for name, m in sorted(mods.items()):
    t = m["tree"]
    if not any(isinstance(x, ast.If) and isinstance(x.test, ast.Compare) and isinstance(x.test.left, ast.Name)
               and x.test.left.id == "__name__" for x in t.body): continue
    todo = []
    for x in t.body:
        if not isinstance(x, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Import, ast.ImportFrom)):
            todo += refs(name, x)
    seen = set()
    while todo:
        k = todo.pop()
        if k in seen: continue
        seen.add(k)
        for b in body_of(*k): todo += refs(k[1], b)
    reached = {(mod, nm) for _, mod, nm in seen}
    row = {"entry": m["path"]}
    for col, targets in SINK.items(): row[col] = bool(reached & targets)
    rows.append(row)
sh = sorted(os.path.join(d, f) for r in ROOTS for d, _, fs in os.walk(r) for f in fs if f.endswith(".sh"))
print(json.dumps({"python_entry_points": len(rows), "shell_scripts": sh, "rows": rows}, indent=1))
