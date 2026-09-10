"""Generated inventory of entry points and configuration-authority readers.

INVENTORY-01. This replaces hand-built inventories (GENERALIZE-READOUT-01 S3,
SPEC-AUTH-01 Phase 1 R3/R4) with something that regenerates from disk, because
a hand-built inventory is a measurement with no expiry date on it: it is
correct the day it is written and silently wrong from the next commit.

**AST only. This module never imports and never executes a scanned file, and
never opens a database.** Not a performance choice — importing a scanned module
runs its module-level code, and several of the scanned files construct paths,
open sockets, or (via `ReviewDatabase.__init__`, which mkdirs) create
directories under `data/` as a side effect of being imported. An inventory tool
that modified the tree it inventories would be worse than no tool.

What it cannot resolve, it reports under UNPARSED. It never guesses.

Usage:
    python -m engine.tools.inventory --check    # drift check, exit 1 on drift
    python -m engine.tools.inventory --write    # regenerate the committed files
"""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

SCAN_ROOTS = ("engine", "scripts", "analysis")
SPEC_DIR = "review_specs"
DATA_DIR = "data"

OUT_MD = Path("docs/inventory/entry_points.md")
OUT_JSON = Path("docs/inventory/entry_points.json")

# Calls that read a configuration authority.
SPEC_FUNCS = ("load_spec_for", "load_review_spec", "spec_path_for", "data_root_for")
RESOLVER_FUNCS = ("load_spec_for", "spec_path_for", "data_root_for")

# A string is flagged as path construction if it carries one of these. Broad on
# purpose: the point is to see what grep could not, so help text and docstrings
# are reported too — with their role, so a reader can tell prose from a path.
PATH_MARKERS = ("review_specs", "data/", ".yaml")


# ── Review ids ───────────────────────────────────────────────────────


def review_ids(repo_root: Path) -> tuple[list[str], dict[str, str]]:
    """The review ids on disk, and every data/ subdirectory NOT counted as one.

    Spec stems are reviews by definition. A data/ subdirectory is counted only
    if it holds a review.db — checked by existence, never opened.

    The filter is not cosmetic. `ReviewDatabase.__init__` mkdirs its root, so
    any run or test that constructed one without a temp data_root left a
    directory behind; data/ therefore contains debris that is not a review, and
    treating every subdirectory as a review id would flag the strings "data",
    "review" and "logs" throughout the codebase. Both sets are reported so the
    exclusions are visible rather than assumed.
    """
    ids = set()
    for spec in sorted((repo_root / SPEC_DIR).glob("*.yaml")):
        ids.add(spec.stem)

    excluded: dict[str, str] = {}
    data_root = repo_root / DATA_DIR
    if data_root.is_dir():
        for child in sorted(data_root.iterdir()):
            if not child.is_dir():
                continue
            if (child / "review.db").exists():
                ids.add(child.name)
            else:
                # The REASON only, never the contents. It used to name the
                # first three files in the directory, which made the committed
                # inventory a function of what happened to be sitting in
                # data/backups — so writing one backup file turned the drift
                # test red, with a message that printed two identical key
                # lists and no way to see what had moved.
                excluded[child.name] = "no review.db"
    return sorted(ids), excluded


# ── AST helpers ──────────────────────────────────────────────────────


def _scoped_nodes(tree: ast.Module):
    """Yield (node, scope) for every node, scope being the dotted def path."""

    def walk(node, scope):
        for child in ast.iter_child_nodes(node):
            yield child, scope
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                inner = f"{scope}.{child.name}" if scope else child.name
                yield from walk(child, inner)
            else:
                yield from walk(child, scope)

    yield from walk(tree, "")


def _docstring_ids(tree: ast.Module) -> set[int]:
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = getattr(node, "body", None)
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                if isinstance(body[0].value.value, str):
                    out.add(id(body[0].value))
    return out


_PROSE_KWARGS = {"help", "description", "epilog", "metavar", "usage"}


def _fstring_part_ids(tree: ast.Module) -> set[int]:
    """String Constants that are PIECES of an f-string, not strings in their own right.

    Without this the f-string `f"review_specs/{review}_v1.yaml"` is reported
    three times: once whole, and once for each literal fragment the parser
    split it into. Three hits for one path is not a more thorough inventory,
    it is a wrong count.
    """
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            for part in ast.walk(node):
                if part is not node:
                    out.add(id(part))
    return out


def _prose_ids(tree: ast.Module) -> set[int]:
    """String nodes that are argparse prose, not constructed values."""
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg in _PROSE_KWARGS:
                    out.add(id(kw.value))
    return out


#: Callees whose string arguments are things a human reads, not paths a program
#: uses. Structural, like the argparse-prose rule: the classification comes from
#: the call being made, never from what the string looks like.
_MESSAGE_CALLEES = {
    "print", "warning", "info", "error", "debug", "critical", "exception", "log",
}


def _message_ids(tree: ast.Module) -> set[int]:
    """String nodes passed to an exception constructor or a logging call.

    `raise ReviewSpecError(f"... e.g. review_specs/<review_id>.yaml")` mentions
    a path in order to tell an operator the convention. Counting it as a
    constructed path would make "spec paths built by hand" a number that can
    never reach zero, and a metric with a floor above its target teaches the
    reader to ignore it.
    """
    out = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _callee_name(node)
        if name in _MESSAGE_CALLEES or name.endswith(("Error", "Exception")):
            for arg in node.args:
                out.add(id(arg))
    return out


def _callee_name(node: ast.Call) -> str:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _dotted(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except Exception:  # pragma: no cover - ast.unparse is total on valid trees
        return "<unrenderable>"


def _render_string_node(node: ast.AST) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return _dotted(node)
    return ""


# ── Per-file analysis ────────────────────────────────────────────────


def analyze_file(path: Path, rel: str, ids: set[str]) -> dict:
    source = path.read_text(encoding="utf-8", errors="replace")
    result = {
        "path": rel,
        "entry_point": False,
        "has_main_guard": False,
        "flags": [],
        "spec_access": [],
        "review_database": [],
        "order": {},
        "yaml_loads": [],
        "path_construction": [],
        "literal_review_ids": [],
        "review_id_constants": [],
        "unparsed": [],
    }

    try:
        tree = ast.parse(source, filename=rel)
    except SyntaxError as exc:
        result["unparsed"].append(
            {"line": exc.lineno or 0, "reason": "SyntaxError: %s" % exc.msg}
        )
        return result

    doc_ids = _docstring_ids(tree)
    prose_ids = _prose_ids(tree)
    fstring_parts = _fstring_part_ids(tree)
    message_ids = _message_ids(tree)

    # module-level constants whose value is a review id
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Constant):
            if isinstance(stmt.value.value, str) and stmt.value.value in ids:
                for target in stmt.targets:
                    if isinstance(target, ast.Name):
                        result["review_id_constants"].append(
                            {"name": target.id, "line": stmt.lineno, "value": stmt.value.value}
                        )

    spec_lines_by_scope: dict[str, list[int]] = {}
    db_lines_by_scope: dict[str, list[int]] = {}

    for node, scope in _scoped_nodes(tree):
        if isinstance(node, ast.If):
            test = _dotted(node.test)
            if "__name__" in test and "__main__" in test:
                result["has_main_guard"] = True

        if isinstance(node, ast.Call):
            name = _callee_name(node)

            if name == "add_argument":
                literal = [
                    a.value for a in node.args
                    if isinstance(a, ast.Constant) and isinstance(a.value, str)
                ]
                dynamic = [
                    _dotted(a) for a in node.args
                    if not (isinstance(a, ast.Constant) and isinstance(a.value, str))
                ]
                if dynamic:
                    result["unparsed"].append({
                        "line": node.lineno,
                        "reason": "add_argument with a non-literal flag name: %s"
                                  % "; ".join(dynamic),
                    })
                if literal:
                    kw = {k.arg: k.value for k in node.keywords}
                    if "default" not in kw:
                        default = "unset"
                    elif isinstance(kw["default"], ast.Constant):
                        default = repr(kw["default"].value)
                    else:
                        default = "dynamic: %s" % _dotted(kw["default"])
                    req = kw.get("required")
                    if req is None:
                        required = False
                    elif isinstance(req, ast.Constant):
                        required = bool(req.value)
                    else:
                        required = "dynamic: %s" % _dotted(req)
                    result["flags"].append({
                        "names": literal,
                        "line": node.lineno,
                        "default": default,
                        "required": required,
                        "dest": kw["dest"].value if isinstance(kw.get("dest"), ast.Constant) else None,
                    })

            if name in SPEC_FUNCS:
                result["spec_access"].append(
                    {"func": name, "line": node.lineno, "scope": scope or "<module>"}
                )
                spec_lines_by_scope.setdefault(scope, []).append(node.lineno)

            if name == "ReviewDatabase":
                kw = {k.arg for k in node.keywords}
                result["review_database"].append({
                    "line": node.lineno,
                    "scope": scope or "<module>",
                    "data_root_passed": "data_root" in kw,
                })
                db_lines_by_scope.setdefault(scope, []).append(node.lineno)

            if name in ("safe_load", "load") and isinstance(node.func, ast.Attribute):
                if _dotted(node.func).split(".")[0].endswith("yaml"):
                    target = _dotted(node.args[0]) if node.args else "<no argument>"
                    result["yaml_loads"].append({
                        "line": node.lineno, "call": name, "target": target,
                        "scope": scope or "<module>",
                    })

        if isinstance(node, (ast.Constant, ast.JoinedStr)):
            if id(node) in fstring_parts:
                continue
            text = _render_string_node(node)
            if text and any(marker in text for marker in PATH_MARKERS):
                if id(node) in doc_ids:
                    role = "docstring"
                elif id(node) in prose_ids:
                    role = "help_text"
                elif id(node) in message_ids:
                    role = "message"
                else:
                    role = "code"
                result["path_construction"].append({
                    "line": node.lineno,
                    "kind": "f-string" if isinstance(node, ast.JoinedStr) else "literal",
                    "role": role,
                    "text": text if len(text) <= 160 else text[:157] + "...",
                })
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value in ids:
                if id(node) in doc_ids:
                    role = "docstring"
                elif id(node) in prose_ids:
                    role = "help_text"
                elif id(node) in message_ids:
                    role = "message"
                else:
                    role = "code"
                result["literal_review_ids"].append(
                    {"line": node.lineno, "value": node.value, "role": role}
                )

    for scope in sorted(set(spec_lines_by_scope) & set(db_lines_by_scope)):
        first_spec = min(spec_lines_by_scope[scope])
        first_db = min(db_lines_by_scope[scope])
        result["order"][scope or "<module>"] = (
            "spec_first" if first_spec < first_db else "db_first"
        )

    result["entry_point"] = bool(result["flags"]) or result["has_main_guard"]
    for key in ("flags", "spec_access", "review_database", "yaml_loads",
                "path_construction", "literal_review_ids", "review_id_constants",
                "unparsed"):
        result[key].sort(key=lambda d: (d.get("line", 0), str(d)))
    return result


# ── Inventory ────────────────────────────────────────────────────────


def build_inventory(repo_root: Path = REPO_ROOT) -> dict:
    ids, excluded = review_ids(repo_root)
    id_set = set(ids)

    files: dict[str, dict] = {}
    for root in SCAN_ROOTS:
        base = repo_root / root
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            rel = path.relative_to(repo_root).as_posix()
            files[rel] = analyze_file(path, rel, id_set)

    entry_points = {r: f for r, f in files.items() if f["entry_point"]}
    spec_bearing = {
        r: f for r, f in entry_points.items()
        if any("--spec" in fl["names"] for fl in f["flags"])
    }
    name_bearing = {
        r: f for r, f in entry_points.items()
        if any(n in ("--review", "--name") for fl in f["flags"] for n in fl["names"])
    }
    constructs_db = {r: f for r, f in entry_points.items() if f["review_database"]}

    summary = {
        "files_scanned": len(files),
        "entry_points": len(entry_points),
        "entry_points_with_spec_flag": len(spec_bearing),
        "entry_points_with_review_name_flag": len(name_bearing),
        "entry_points_name_only": len(
            [r for r in name_bearing if r not in spec_bearing]
        ),
        "entry_points_constructing_reviewdatabase": len(constructs_db),
        "name_only_constructing_reviewdatabase": len(
            [r for r in constructs_db if r in name_bearing and r not in spec_bearing]
        ),
        "files_calling_resolver": len(
            [r for r, f in files.items()
             if any(s["func"] in RESOLVER_FUNCS for s in f["spec_access"])]
        ),
        "files_calling_load_review_spec_directly": len(
            [r for r, f in files.items()
             if any(s["func"] == "load_review_spec" for s in f["spec_access"])]
        ),
        "raw_yaml_load_sites": sum(len(f["yaml_loads"]) for f in files.values()),
        "files_with_raw_yaml_loads": len([r for r, f in files.items() if f["yaml_loads"]]),
        "review_id_constants": sum(len(f["review_id_constants"]) for f in files.values()),
        "literal_review_id_sites_in_code": sum(
            len([h for h in f["literal_review_ids"] if h["role"] == "code"])
            for f in files.values()
        ),
        "path_construction_sites_in_code": sum(
            len([h for h in f["path_construction"] if h["role"] == "code"])
            for f in files.values()
        ),
        "db_before_spec_scopes": sum(
            len([s for s, o in f["order"].items() if o == "db_first"])
            for f in files.values()
        ),
        "fstring_spec_path_sites": sum(
            len([h for h in f["path_construction"]
                 if h["role"] == "code" and h["kind"] == "f-string"
                 and "review_specs" in h["text"]])
            for f in files.values()
        ),
        "default_review_named_constants": sum(
            len([c for c in f["review_id_constants"] if c["name"] == "DEFAULT_REVIEW"])
            for f in files.values()
        ),
        "unparsed_sites": sum(len(f["unparsed"]) for f in files.values()),
    }

    return {
        "review_ids": ids,
        "data_dirs_excluded": excluded,
        "summary": summary,
        "files": files,
    }


def _commit_hash(repo_root: Path) -> str:
    try:
        out = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        return out.stdout.strip() or "unknown"
    except Exception:  # pragma: no cover - git absent
        return "unknown"


# ── Rendering ────────────────────────────────────────────────────────


def _fmt_flags(f: dict) -> str:
    parts = []
    for fl in f["flags"]:
        names = "/".join(fl["names"])
        if fl["required"] is True:
            parts.append(f"`{names}` required")
        elif fl["default"] == "unset":
            parts.append(f"`{names}`")
        else:
            parts.append(f"`{names}`={fl['default']}")
    return "; ".join(parts) or "—"


def render_markdown(inv: dict, commit: str) -> str:
    s = inv["summary"]
    L = []
    L.append("# Entry-point and authority-reader inventory")
    L.append("")
    L.append("**GENERATED — DO NOT EDIT.** Regenerate with "
             "`python -m engine.tools.inventory --write`.")
    L.append(f"Generated at commit `{commit}` by `engine/tools/inventory.py`, "
             f"AST only — no scanned module is imported and no database is opened.")
    L.append("A drift test at the standard gate fails if this file's JSON twin "
             "stops matching the tree.")
    L.append("")
    L.append("## Summary")
    L.append("")
    L.append("| count | value |")
    L.append("|---|---:|")
    for k, v in s.items():
        L.append(f"| {k.replace('_', ' ')} | {v} |")
    L.append("")
    L.append(f"Review ids on disk: {', '.join('`%s`' % i for i in inv['review_ids'])}")
    if inv["data_dirs_excluded"]:
        L.append("")
        L.append("`data/` subdirectories NOT counted as reviews (no `review.db`):")
        L.append("")
        L.append("| directory | why |")
        L.append("|---|---|")
        for name, why in sorted(inv["data_dirs_excluded"].items()):
            L.append(f"| `{name}` | {why} |")
    L.append("")
    L.append(_render_reconciliation(s))
    L.append("")

    files = inv["files"]

    L.append("## Entry points")
    L.append("")
    L.append("| file | flags | spec access | ReviewDatabase | order |")
    L.append("|---|---|---|---|---|")
    for rel in sorted(r for r, f in files.items() if f["entry_point"]):
        f = files[rel]
        spec = ", ".join(sorted({s["func"] for s in f["spec_access"]})) or "—"
        db = ", ".join(str(d["line"]) for d in f["review_database"]) or "—"
        order = ", ".join(f"{k}:{v}" for k, v in sorted(f["order"].items())) or "—"
        L.append(f"| `{rel}` | {_fmt_flags(f)} | {spec} | {db} | {order} |")
    L.append("")

    L.append("## Raw YAML loads (outside the spec loader)")
    L.append("")
    L.append("| file | line | call | target |")
    L.append("|---|---:|---|---|")
    for rel in sorted(files):
        for y in files[rel]["yaml_loads"]:
            L.append(f"| `{rel}` | {y['line']} | `{y['call']}` | `{y['target']}` |")
    L.append("")

    L.append("## Path construction in code")
    L.append("")
    L.append("Strings carrying `review_specs`, `data/` or `.yaml` outside docstrings, "
             "argparse prose, and messages passed to an exception or a logger. This "
             "is the class a filename grep cannot see: an f-string that builds the "
             "path from a variable.")
    L.append("")
    L.append("| file | line | kind | text |")
    L.append("|---|---:|---|---|")
    for rel in sorted(files):
        for h in files[rel]["path_construction"]:
            if h["role"] != "code":
                continue
            L.append(f"| `{rel}` | {h['line']} | {h['kind']} | `{h['text']}` |")
    L.append("")

    L.append("## Literal review ids in code")
    L.append("")
    L.append("| file | line | value |")
    L.append("|---|---:|---|")
    for rel in sorted(files):
        for h in files[rel]["literal_review_ids"]:
            if h["role"] != "code":
                continue
            L.append(f"| `{rel}` | {h['line']} | `{h['value']}` |")
    L.append("")

    L.append("## Module constants holding a review id")
    L.append("")
    L.append("| file | line | name | value |")
    L.append("|---|---:|---|---|")
    for rel in sorted(files):
        for c in files[rel]["review_id_constants"]:
            L.append(f"| `{rel}` | {c['line']} | `{c['name']}` | `{c['value']}` |")
    L.append("")

    L.append("## UNPARSED")
    L.append("")
    unparsed = [(rel, u) for rel in sorted(files) for u in files[rel]["unparsed"]]
    if not unparsed:
        L.append("None. Every scanned file parsed, and every argparse flag name was a "
                 "string literal.")
    else:
        L.append("| file | line | reason |")
        L.append("|---|---:|---|")
        for rel, u in unparsed:
            L.append(f"| `{rel}` | {u['line']} | {u['reason']} |")
    L.append("")
    return "\n".join(L)


# ── Reconciliation ───────────────────────────────────────────────────

#: The hand-built figures this tool replaces, from GENERALIZE-READOUT-01 S3 and
#: the SPEC-AUTH-01 Phase 1 read-out. Held as literals because a reconciliation
#: against a recomputed baseline reconciles nothing. Each row is
#: (label, key, baseline, note when they differ).
BASELINE = [
    ("argparse entry points naming a review",
     "entry_points_with_review_name_flag", 74,
     "the hand scan keyed on --review/--name, which is what this counts"),
    ("of those, spec-bearing", "entry_points_with_spec_flag", 29, ""),
    ("of those, name-only", "entry_points_name_only", 45, ""),
    ("entry points constructing ReviewDatabase",
     "entry_points_constructing_reviewdatabase", 35,
     "the hand scan enumerated files by argparse FLAG, so it could not see an "
     "entry point that constructs a database without a --review/--name flag; "
     "the tool finds those through the __main__ guard instead"),
    ("name-only, constructing ReviewDatabase",
     "name_only_constructing_reviewdatabase", 20, ""),
    ("raw yaml load sites", "raw_yaml_load_sites", 13, ""),
    ("f-string spec-path builders", "fstring_spec_path_sites", 19,
     "SPEC-AUTH-01 moved every one of these onto the resolver; a non-zero value "
     "here means a hand-built spec path has come back"),
    ("DEFAULT_REVIEW constants", "default_review_named_constants", 7, ""),
]


def _render_reconciliation(summary: dict) -> str:
    L = ["## Reconciliation against the hand-built inventories", ""]
    L.append("Baselines are the figures measured by hand in GENERALIZE-READOUT-01 "
             "and SPEC-AUTH-01 Phase 1. A difference is not automatically a defect "
             "— SPEC-AUTH-01 changed several of these deliberately — but every one "
             "is named here rather than left for a reader to notice.")
    L.append("")
    L.append("| figure | hand-built | measured now | |")
    L.append("|---|---:|---:|---|")
    for label, key, baseline, note in BASELINE:
        now = summary.get(key, "—")
        if now == baseline:
            verdict = "matches"
        else:
            verdict = note or "differs — unexplained, investigate"
        L.append(f"| {label} | {baseline} | {now} | {verdict} |")
    L.append("")
    L.append("Two figures deliberately have no baseline row. **entry points** "
             f"({summary.get('entry_points')}) counts anything with argparse flags or a "
             "`__main__` guard, which is a wider net than the hand scan's "
             "review-naming CLIs. And the hand-built note that 12 of the 13 raw YAML "
             "loads are codebook readers is a semantic judgement about what a file "
             "MEANS; this tool reports the call site and its target expression and "
             "makes no such claim.")
    return "\n".join(L)


# ── CLI ──────────────────────────────────────────────────────────────


def write_inventory(repo_root: Path = REPO_ROOT) -> tuple[Path, Path]:
    inv = build_inventory(repo_root)
    commit = _commit_hash(repo_root)
    md_path = repo_root / OUT_MD
    json_path = repo_root / OUT_JSON
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_markdown(inv, commit) + "\n")
    json_path.write_text(json.dumps(
        {"meta": {"generated_at_commit": commit,
                  "tool": "engine/tools/inventory.py",
                  "scan_roots": list(SCAN_ROOTS)},
         "data": inv},
        indent=2, sort_keys=True) + "\n")
    return md_path, json_path


def committed_inventory(repo_root: Path = REPO_ROOT) -> dict | None:
    path = repo_root / OUT_JSON
    if not path.exists():
        return None
    return json.loads(path.read_text())["data"]


def drift(repo_root: Path = REPO_ROOT) -> str | None:
    """Return a message naming the first difference, or None if in sync.

    Only `data` is compared. `meta` carries the generating commit hash, which
    moves with every commit and would make the check fail on its own success.
    """
    committed = committed_inventory(repo_root)
    if committed is None:
        return "no committed inventory at %s — run --write" % OUT_JSON
    fresh = build_inventory(repo_root)

    if committed.get("review_ids") != fresh["review_ids"]:
        return ("review ids changed: committed %s, on disk %s"
                % (committed.get("review_ids"), fresh["review_ids"]))
    if committed.get("data_dirs_excluded") != fresh["data_dirs_excluded"]:
        was, now = committed.get("data_dirs_excluded", {}), fresh["data_dirs_excluded"]
        detail = [
            "%s: committed %r, on disk %r" % (k, was.get(k), now.get(k))
            for k in sorted(set(was) | set(now)) if was.get(k) != now.get(k)
        ]
        return "data/ subdirectories changed: " + "; ".join(detail)

    cf, ff = committed.get("files", {}), fresh["files"]
    for rel in sorted(set(cf) | set(ff)):
        if rel not in cf:
            return "%s: new file, not in the committed inventory" % rel
        if rel not in ff:
            return "%s: in the committed inventory but gone from disk" % rel
        if cf[rel] != ff[rel]:
            for key in sorted(set(cf[rel]) | set(ff[rel])):
                if cf[rel].get(key) != ff[rel].get(key):
                    return ("%s: %r differs — committed %r, on disk %r"
                            % (rel, key, cf[rel].get(key), ff[rel].get(key)))
            return "%s differs" % rel
    if committed.get("summary") != fresh["summary"]:
        return "summary differs: committed %r, on disk %r" % (
            committed.get("summary"), fresh["summary"])
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--write", action="store_true",
                       help="Regenerate the committed inventory files")
    group.add_argument("--check", action="store_true",
                       help="Compare the tree against the committed inventory")
    args = parser.parse_args(argv)

    if args.write:
        md, js = write_inventory()
        print("wrote %s" % md.relative_to(REPO_ROOT))
        print("wrote %s" % js.relative_to(REPO_ROOT))
        return 0

    message = drift()
    if message:
        print("INVENTORY DRIFT: %s" % message, file=sys.stderr)
        print("Regenerate with: python -m engine.tools.inventory --write", file=sys.stderr)
        return 1
    print("inventory in sync")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
