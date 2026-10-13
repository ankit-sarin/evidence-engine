"""Static census of entry points and their paths to a local model call or an Ollama restart.

The helper behind `test_every_model_calling_entry_point_holds_the_experiment_lock`
(R625(b), R629(c)). AST only: it imports nothing from the tree it reads and names no
entry point. Promoted from `docs/session-reports/session-12/12n_U12_C2A/lock_census.py`.

An ENTRY POINT is a module under one of `TREES` with an `if __name__ == "__main__":`
block, a package's `__main__.py` included.

REACH follows resolved references from the module's top-level code:
  * a bare name -> the function or class of that name in the same module, or the object
    a `from m import name` binds (re-exports followed);
  * `alias.name` where alias is an imported module of the tree -> that module's function
    or class;
  * a class -> all its methods; `self.name` / `cls.name` -> a method of that name in the
    same module;
  * any other `x.name` -> methods called `name` on classes of the caller's module and of
    the modules it imports directly (an over-approximation; marked `~` in a chain).
A reference counts whether it is called or passed.

LOCK CONTEXT. A reference is locked when it sits lexically inside a `with` statement whose
context expression calls `hold_experiment_lock`, or is reached from a reference that is.
Search states are (object, locked); a sink reached in a state with locked=False has an
unlocked static path, and that path is what the census reports.

What it cannot see: dynamic dispatch (importlib, getattr, a class chosen from a table
built in another module), a lock decided by a parameter's value, and a lock held by a
parent process. `test_lock_census.py` guards the sink list itself.
"""

from __future__ import annotations

import ast
import heapq
from pathlib import Path

TREES = ("engine", "scripts", "analysis")
LOCK_MODULE = "engine.utils.ollama_lock"
LOCK_NAME = "hold_experiment_lock"
#: The one route to a local model call, and the two sites that restart the service.
SINKS = {
    ("engine.utils.ollama_client", "ollama_chat"): "local model call",
    ("engine.utils.ollama_client", "_restart_ollama_and_retry"): "last-resort Ollama restart",
    ("engine.agents.extractor", "restart_ollama"): "proactive Ollama restart",
}


def _load(root: Path, trees) -> dict:
    mods = {}
    for tree in trees:
        for path in sorted((root / tree).rglob("*.py")):
            rel = path.relative_to(root).as_posix()
            name = rel[:-3].replace("/", ".")
            pkg = name.endswith(".__init__")
            if pkg:
                name = name[:-9]
            mods[name] = {"path": rel, "tree": ast.parse(path.read_text(encoding="utf-8")), "pkg": pkg}
    for name, m in mods.items():
        m.update(names={}, aliases={}, defs={}, classes={}, methods={})
        for x in ast.walk(m["tree"]):
            if isinstance(x, ast.Import):
                for a in x.names:
                    if a.name in mods:
                        m["aliases"][a.asname or a.name] = a.name
            elif isinstance(x, ast.ImportFrom):
                base = _absolute(mods, name, x)
                for a in x.names:
                    full = f"{base}.{a.name}"
                    if full in mods:
                        m["aliases"][a.asname or a.name] = full
                    elif base in mods:
                        m["names"][a.asname or a.name] = (base, a.name)
        for x in m["tree"].body:
            if isinstance(x, (ast.FunctionDef, ast.AsyncFunctionDef)):
                m["defs"][x.name] = x
            elif isinstance(x, ast.ClassDef):
                m["classes"][x.name] = x
                for y in ast.walk(x):
                    if isinstance(y, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        m["methods"].setdefault(y.name, []).append(y)
        m["direct"] = set(m["aliases"].values()) | {b for b, _ in m["names"].values()}
    return mods


def _absolute(mods, name, node) -> str:
    if not node.level:
        return node.module or ""
    parts = name.split(".") if mods[name]["pkg"] else name.split(".")[:-1]
    parts = parts[:len(parts) - (node.level - 1)]
    return ".".join(parts + ([node.module] if node.module else []))


def _dotted(n):
    parts = []
    while isinstance(n, ast.Attribute):
        parts.append(n.attr)
        n = n.value
    return (n.id, parts[::-1]) if isinstance(n, ast.Name) else (None, None)


def _resolve(mods, mod, nm, depth=0):
    m = mods[mod]
    if nm in m["defs"]:
        return [("f", mod, nm)]
    if nm in m["classes"]:
        return [("c", mod, nm)]
    if nm in m["names"] and depth < 6:
        base, name = m["names"][nm]
        return _resolve(mods, base, name, depth + 1)
    return []


def _is_lock_with(mods, mod, node) -> bool:
    """A `with` whose context expression calls hold_experiment_lock — the one declared wrapper."""
    for item in node.items:
        e = item.context_expr
        if not isinstance(e, ast.Call):
            continue
        f = e.func
        if isinstance(f, ast.Name) and (LOCK_MODULE, LOCK_NAME) in [(r[1], r[2]) for r in _resolve(mods, mod, f.id)]:
            return True
        if isinstance(f, ast.Attribute) and f.attr == LOCK_NAME:
            head, parts = _dotted(f)
            if head is not None and mods[mod]["aliases"].get(".".join([head] + parts[:-1])) == LOCK_MODULE:
                return True
    return False


def _node_refs(mods, mod, n):
    """References made by one node, not its children: [(target, exact)]."""
    m = mods[mod]
    out = []
    if isinstance(n, ast.Name):
        out += [(r, True) for r in _resolve(mods, mod, n.id)]
    elif isinstance(n, ast.Attribute):
        head, parts = _dotted(n)
        if head is None:
            return [(("m", mm, n.attr), False) for mm in {mod} | m["direct"] if n.attr in mods[mm]["methods"]]
        for k in range(len(parts), 0, -1):
            target = m["aliases"].get(".".join([head] + parts[:k - 1])) or (
                m["aliases"].get(head) and ".".join([m["aliases"][head]] + parts[:k - 1]))
            if target in mods:
                out += [(r, True) for r in _resolve(mods, target, parts[k - 1])]
                break
        if head in ("self", "cls"):
            if n.attr in m["methods"]:
                out.append((("m", mod, n.attr), True))
        elif head not in m["aliases"]:
            out += [(("m", mm, n.attr), False) for mm in {mod} | m["direct"] if n.attr in mods[mm]["methods"]]
    return out


def _refs(mods, mod, node, locked):
    """[(target, exact, locked)] for every reference under `node`, tracking lock context."""
    out = []

    def walk(n, locked):
        if isinstance(n, (ast.With, ast.AsyncWith)) and _is_lock_with(mods, mod, n):
            for item in n.items:      # the context expression itself is outside the lock
                walk(item, locked)
            for b in n.body:
                walk(b, True)
            return
        for target, exact in _node_refs(mods, mod, n):
            out.append((target, exact, locked))
        for child in ast.iter_child_nodes(n):
            walk(child, locked)

    walk(node, locked)
    return out


def _bodies(mods, kind, mod, nm):
    m = mods[mod]
    if kind == "f":
        return [m["defs"][nm]]
    if kind == "c":
        return [m["classes"][nm]]
    return m["methods"][nm]


def _search(mods, start):
    """Cheapest path to every (object, locked) state; exact edges cost 1, `~` edges 1000."""
    best, heap, n = {}, [], 0
    for target, exact, locked in start:
        heapq.heappush(heap, (1 if exact else 1000, n, (target, locked), None, exact))
        n += 1
    while heap:
        cost, _, state, parent, exact = heapq.heappop(heap)
        if state in best:
            continue
        best[state] = (cost, parent, exact)
        (kind, mod, nm), locked = state
        for body in _bodies(mods, kind, mod, nm):
            for target, ex, lk in _refs(mods, mod, body, locked):
                if (target, lk) not in best:
                    heapq.heappush(heap, (cost + (1 if ex else 1000), n, (target, lk), state, ex))
                    n += 1
    return best


def _chain(best, state) -> str:
    out = []
    while state is not None:
        _, parent, exact = best[state]
        (_, mod, nm), locked = state
        out.append(("" if exact else "~") + f"{mod}::{nm}" + ("[locked]" if locked else ""))
        state = parent
    return " -> ".join(out[::-1])


def _is_entry(tree) -> bool:
    return any(isinstance(x, ast.If) and isinstance(x.test, ast.Compare)
               and isinstance(x.test.left, ast.Name) and x.test.left.id == "__name__"
               for x in tree.body)


def census(root: Path, trees=TREES) -> list[dict]:
    """One row per entry point: {"entry", "unlocked": {sink label: chain}, "locked": {…}}.

    `unlocked` is what the acceptance test fails on: a sink with a resolved static path
    that is outside every `with hold_experiment_lock(...)`.
    """
    mods = _load(Path(root), trees)
    rows = []
    for name, m in sorted(mods.items()):
        if not _is_entry(m["tree"]):
            continue
        start = []
        for x in m["tree"].body:
            if not isinstance(x, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Import, ast.ImportFrom)):
                start += _refs(mods, name, x, False)
        best = _search(mods, start)
        row = {"entry": m["path"], "unlocked": {}, "locked": {}}
        for (mod, nm), label in SINKS.items():
            for locked in (False, True):
                hits = [(best[s][0], i, s) for i, s in enumerate(best)
                        if s[1] is locked and (s[0][1], s[0][2]) == (mod, nm)]
                if hits:
                    row["locked" if locked else "unlocked"][label] = _chain(best, min(hits)[2])
        rows.append(row)
    return rows


def sinks_defined(root: Path, trees=TREES) -> dict:
    """{sink: True/False} — whether each sink is still a top-level function of its module."""
    mods = _load(Path(root), trees)
    return {f"{mod}::{nm}": mod in mods and nm in mods[mod]["defs"] for (mod, nm) in SINKS}
