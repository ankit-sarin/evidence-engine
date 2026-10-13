"""12n-U12-C2A A3/A4/A5 — the lock census: every entry point under engine/, scripts/, analysis/,
what it can reach, and whether the path runs inside `with hold_experiment_lock(...)`.
Read-only, AST only; imports nothing from the repository. usage, from the repository root:
    lock_census.py [--stages scripts.run_pipeline] > lock_census.json

It extends docs/session-reports/session-12/12m_U12/entry_census.py (same module table, same
reference resolution, same over-approximation) with three things:

  1. LOCK CONTEXT. A reference is "locked" when it sits lexically inside a `with` statement whose
     context expression calls `hold_experiment_lock`, or is reached from a reference that is.
     Search states are (object, locked). A sink reached in a state with locked=False has an
     UNLOCKED static path; a sink reached only with locked=True is locked on every resolved path.
  2. CHAINS. Each reported reach carries its shortest chain, preferring exactly resolved edges
     (a bare name, an imported name, `alias.name`, `self.name`) over the method-name
     over-approximation, which is marked "~" in the chain.
  3. MORE SINKS: the last-resort restart, the proactive restart, the preflight probe and its two
     wrappers, Ollama reads that load no model, the cloud transports, and subprocess / process-pool
     sites (for A5).

An ENTRY POINT is a module with an `if __name__ == "__main__":` block (a package's `__main__.py`
included). Shell scripts are listed, not analysed. pyproject.toml declares no console script.

What the AST cannot see (read these against the read-out, not as absences):
  * a lock decision made by a parameter: `run_extraction(experiment_lock=False)` reaches
    `_run_extraction_unlocked` outside the `with`. That one branch is reported separately
    ("unlocked_only_via_optout": the unlocked path disappears when the branch is ignored);
  * dynamic dispatch (importlib, getattr, a class chosen from a dict built in another module);
  * a lock taken by the caller's caller in another process (a shell wrapper, a parent process);
  * the method-name over-approximation can invent a path ("~" edges): one import hop, any class.
"""
import ast, heapq, json, os, sys

ROOTS = ("engine", "scripts", "analysis")
LOCK = ("engine.utils.ollama_lock", "hold_experiment_lock")
OPTOUT_EDGE = (("engine.agents.extractor", "run_extraction"), ("engine.agents.extractor", "_run_extraction_unlocked"))
SINK = {
    "local_call": {("engine.utils.ollama_client", "ollama_chat")},
    "restart_last_resort": {("engine.utils.ollama_client", "_restart_ollama_and_retry")},
    "restart_proactive": {("engine.agents.extractor", "restart_ollama")},
    "preflight_probe": {("engine.utils.ollama_preflight", "check_model")},
    "preflight_wrappers": {("engine.utils.ollama_preflight", "require_preflight"),
                           ("engine.utils.ollama_preflight", "preflight_check")},
    "ollama_read": {("engine.utils.ollama_client", "fetch_model_digest"),
                    ("engine.utils.ollama_client", "get_model_context_length")},
    "cloud_call": {("engine.cloud.openai_extractor", "OpenAIExtractor"),
                   ("engine.cloud.anthropic_extractor", "AnthropicExtractor"),
                   ("engine.cloud.openai_extractor", "send"), ("engine.cloud.anthropic_extractor", "send")},
    "probes_lock": {("engine.utils.ollama_lock", "foreign_lock_held"),
                    ("engine.utils.ollama_lock", "check_experiment_lock")},
}
PROC_CALLS = {"subprocess.run", "subprocess.Popen", "subprocess.call", "subprocess.check_call",
              "subprocess.check_output", "os.system", "os.fork", "os.execv", "os.execvp",
              "multiprocessing.Pool", "multiprocessing.Process", "Pool", "Process",
              "ProcessPoolExecutor", "concurrent.futures.ProcessPoolExecutor"}

mods = {}
for root in ROOTS:
    for d, _, fs in os.walk(root):
        for f in sorted(fs):
            if f.endswith(".py"):
                p = os.path.join(d, f); name = p[:-3].replace("/", ".")
                pkg = name.endswith(".__init__")
                if pkg: name = name[:-9]
                src = open(p, encoding="utf-8").read()
                mods[name] = {"path": p, "tree": ast.parse(src), "pkg": pkg, "src": src}

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

def is_lock_with(mod, node):
    """A `with` whose context expression calls hold_experiment_lock (bare, imported or alias.attr)."""
    for item in node.items:
        e = item.context_expr
        if isinstance(e, ast.Call):
            f = e.func
            if isinstance(f, ast.Name) and (LOCK in [(r[1], r[2]) for r in resolve_name(mod, f.id)] or f.id == LOCK[1]):
                return True
            if isinstance(f, ast.Attribute) and f.attr == LOCK[1]:
                return True
    return False

def node_refs(mod, n):
    """References made by ONE node (not its children): [(target, exact)]."""
    m = mods[mod]; out = []
    if isinstance(n, ast.Name):
        out += [(r, True) for r in resolve_name(mod, n.id)]
    elif isinstance(n, ast.Attribute):
        head, parts = dotted(n)
        if head is None:
            return [(("m", mm, n.attr), False) for mm in {mod} | m["direct"] if n.attr in mods[mm]["methods"]]
        for k in range(len(parts), 0, -1):
            cand = ".".join([head] + parts[:k - 1])
            target = m["aliases"].get(cand) or (m["aliases"].get(head) and ".".join([m["aliases"][head]] + parts[:k - 1]))
            if target in mods:
                out += [(r, True) for r in resolve_name(target, parts[k - 1])]; break
        if head in ("self", "cls"):
            if n.attr in m["methods"]: out.append((("m", mod, n.attr), True))
        elif head not in m["aliases"]:
            out += [(("m", mm, n.attr), False) for mm in {mod} | m["direct"] if n.attr in mods[mm]["methods"]]
    return out

def refs(mod, node, locked=False):
    """[(target, exact, locked, lineno)] for every reference under `node`, tracking lock context."""
    out = []
    def walk(n, locked):
        if isinstance(n, (ast.With, ast.AsyncWith)) and is_lock_with(mod, n):
            for item in n.items: walk(item, locked)        # the context expression itself is outside
            for b in n.body: walk(b, True)
            return
        for tgt, exact in node_refs(mod, n):
            out.append((tgt, exact, locked, getattr(n, "lineno", 0)))
        for c in ast.iter_child_nodes(n): walk(c, locked)
    walk(node, locked)
    return out

def body_of(kind, mod, nm):
    m = mods[mod]
    if kind == "f": return [m["defs"][nm]]
    if kind == "c": return [m["classes"][nm]]
    return m["methods"][nm]

def proc_sites(mod, node):
    out = []
    for n in ast.walk(node):
        if isinstance(n, ast.Call):
            head, parts = dotted(n.func) if isinstance(n.func, ast.Attribute) else (getattr(n.func, "id", None), [])
            if head is None: continue
            nm = ".".join([head] + parts)
            if nm in PROC_CALLS:
                arg = ast.unparse(n.args[0])[:70] if n.args else ""
                out.append(f"{mods[mod]['path']}:{nm}({arg})")
    return out

def search(start_refs, skip_optout=False):
    """Dijkstra over (object, locked). start_refs: [(target, exact, locked, lineno)] from a root.
    Returns {state: (cost, parent_state, exact)}."""
    best = {}; heap = []; n = 0
    for tgt, exact, locked, _ in start_refs:
        heapq.heappush(heap, (1 if exact else 1000, n, (tgt, locked), None, exact)); n += 1
    while heap:
        cost, _, state, parent, exact = heapq.heappop(heap)
        if state in best: continue
        best[state] = (cost, parent, exact)
        (kind, mod, nm), locked = state
        for b in body_of(kind, mod, nm):
            for tgt, ex, lk, _ in refs(mod, b, locked):
                if skip_optout and not lk and (mod, nm) == OPTOUT_EDGE[0] and (tgt[1], tgt[2]) == OPTOUT_EDGE[1]:
                    continue
                st = (tgt, lk)
                if st not in best:
                    heapq.heappush(heap, (cost + (1 if ex else 1000), n, st, state, ex)); n += 1
    return best

def chain(best, state):
    out = []
    while state is not None:
        cost, parent, exact = best[state]
        (kind, mod, nm), locked = state
        out.append(("" if exact else "~") + f"{mod}::{nm}" + ("[L]" if locked else ""))
        state = parent
    return " -> ".join(out[::-1])

def reach(best, targets):
    """{'unlocked': chain|None, 'locked': chain|None} for the cheapest state on each side."""
    res = {"unlocked": None, "locked": None}
    for locked in (False, True):
        cands = [(best[s][0], s) for s in best if s[1] == locked and (s[0][1], s[0][2]) in targets]
        if cands: res["locked" if locked else "unlocked"] = chain(best, min(cands)[1])
    return res

def analyse(start_refs):
    best = search(start_refs); best2 = search(start_refs, skip_optout=True)
    row = {}
    for col, targets in SINK.items():
        r = reach(best, targets); r2 = reach(best2, targets)
        r["unlocked_only_via_optout"] = bool(r["unlocked"]) and not r2["unlocked"]
        row[col] = r
    lock_sites = set()
    procs = set()
    for ((kind, mod, nm), locked) in best:
        for b in body_of(kind, mod, nm):
            for n in ast.walk(b):
                if isinstance(n, (ast.With, ast.AsyncWith)) and is_lock_with(mod, n):
                    lock_sites.add(f"{mods[mod]['path']}::{nm}")
            for s in proc_sites(mod, b): procs.add(s + ("" if not locked else " [L]"))
    row["lock_sites_reached"] = sorted(lock_sites)
    row["process_sites_reached"] = sorted(procs)
    return row

def is_entry(t):
    return any(isinstance(x, ast.If) and isinstance(x.test, ast.Compare) and isinstance(x.test.left, ast.Name)
               and x.test.left.id == "__name__" for x in t.body)

def top_refs(name):
    out = []
    for x in mods[name]["tree"].body:
        if not isinstance(x, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Import, ast.ImportFrom)):
            out += refs(name, x)
    return out

def category(row):
    lc, rp, cc = row["local_call"], row["restart_proactive"], row["cloud_call"]
    local = bool(lc["unlocked"] or lc["locked"]) or bool(rp["unlocked"] or rp["locked"])
    if local:
        unl = (lc["unlocked"] and not lc["unlocked_only_via_optout"]) or (rp["unlocked"] and not rp["unlocked_only_via_optout"])
        lk = lc["locked"] or rp["locked"] or lc["unlocked_only_via_optout"] or rp["unlocked_only_via_optout"]
        if unl and lk: return "local: part locked, part unlocked"
        if unl: return "local: no lock on any resolved path"
        return "local: every resolved path locked"
    if cc["unlocked"] or cc["locked"]: return "cloud only"
    if row["ollama_read"]["unlocked"] or row["ollama_read"]["locked"]: return "ollama read only (no model call)"
    return "none"

rows = []
for name, m in sorted(mods.items()):
    if not is_entry(m["tree"]): continue
    row = {"entry": m["path"], "tree": m["path"].split("/")[0]}
    row.update(analyse(top_refs(name)))
    row["category"] = category(row)
    row["frozen_marker"] = [l.strip()[:120] for l in m["src"].split("\n") if "frozen study" in l.lower() or "FROZEN" in l][:2]
    rows.append(row)

stages = {}
if "--stages" in sys.argv:
    smod = sys.argv[sys.argv.index("--stages") + 1]
    for fn, node in sorted(mods[smod]["defs"].items()):
        if fn.startswith("_stage_") or fn in ("run_pipeline", "_open_run_manifest", "main"):
            r = analyse(refs(smod, node)); stages[fn] = {k: r[k] for k in ("local_call", "restart_proactive", "preflight_probe", "preflight_wrappers", "ollama_read", "lock_sites_reached")}

sh = sorted(os.path.join(d, f) for r in ROOTS for d, _, fs in os.walk(r) for f in fs if f.endswith(".sh"))
counts = {}
for r in rows: counts.setdefault(r["tree"], {}).setdefault(r["category"], 0); counts[r["tree"]][r["category"]] += 1
print(json.dumps({"python_entry_points": len(rows), "counts_by_tree_and_category": counts,
                  "shell_scripts": sh, "rows": rows, "stages": stages}, indent=1))
