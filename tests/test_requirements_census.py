"""requirements.txt declares exactly the third-party distributions the tree imports.

U12 commit 7 (R625(a)(9), R641(e), R642; row B-F17 (a)–(e)). Before it the file declared
three distributions nothing imports (chromadb, sentence-transformers, gradio) and missed
eight the tree does import (requests, httpx, PyMuPDF, fonttools, pandas, fpdf2,
scikit-learn, pytest). The row's acceptance clause asks for an AST census of
`engine/`, `scripts/`, `analysis/`; this one adds `tests/` (R642(c)).

The census is taken from the tree at test time, never from a list written here: every
top-level import name in a tracked `.py` file under the four trees, less the standard
library and the repository's own modules, mapped to its distribution by
`importlib.metadata.packages_distributions()`. Nothing scanned is imported.
"""

from __future__ import annotations

import ast
import importlib.metadata as md
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
TREES = ("engine", "scripts", "analysis", "tests")
FIRST_PARTY = frozenset(TREES)

#: Declared distributions that no scanned file imports. Empty at commit 7: every declared
#: distribution is imported (R642(b)). An entry needs a comment saying why the engine needs a
#: distribution it never imports — a plugin loaded by name, say — or it is a removal.
DECLARED_NOT_IMPORTED: dict[str, str] = {}


def _normalise(name: str) -> str:
    """PEP 503."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _tracked_sources() -> list[Path]:
    out = subprocess.run(["git", "ls-files", "--", *(f"{t}/*.py" for t in TREES)],
                         cwd=REPO, capture_output=True, text=True, check=True).stdout
    return [REPO / line for line in out.splitlines()]


def _imported_distributions() -> tuple[dict[str, set[str]], list[str]]:
    """{distribution: import names}, and the problems found on the way."""
    sources = _tracked_sources()
    local = {p.stem for p in sources}
    names: dict[str, set[str]] = {}
    for path in sources:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), filename=str(path))):
            if isinstance(node, ast.Import):
                found = [a.name.split(".")[0] for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                found = [node.module.split(".")[0]]
            else:
                continue
            for name in found:
                names.setdefault(name, set()).add(str(path.relative_to(REPO)))
    mapping = md.packages_distributions()
    dists: dict[str, set[str]] = {}
    problems = []
    for name, files in sorted(names.items()):
        if name in sys.stdlib_module_names or name in FIRST_PARTY or name == "__future__":
            continue
        candidates = sorted(set(mapping.get(name, [])))
        if not candidates:
            if name not in local:
                problems.append(f"{name}: no installed distribution provides it ({sorted(files)[0]})")
            continue
        if len(candidates) > 1:
            problems.append(f"{name}: provided by several distributions {candidates}")
            continue
        dists.setdefault(_normalise(candidates[0]), set()).add(name)
    return dists, problems


def _declared() -> list[str]:
    declared = []
    for line in (REPO / "requirements.txt").read_text(encoding="utf-8").splitlines():
        requirement = line.split("#", 1)[0].strip()
        if requirement:
            declared.append(_normalise(re.split(r"[\s\[<>=!~;]", requirement, maxsplit=1)[0]))
    return declared


def test_requirements_declare_exactly_the_imported_distributions():
    imported, problems = _imported_distributions()
    declared = _declared()
    assert problems == []
    assert sorted(d for d in set(declared) if declared.count(d) > 1) == [], "declared twice"
    undeclared = sorted(set(imported) - set(declared))
    assert undeclared == [], f"imported but not declared: {undeclared}"
    unimported = sorted(set(declared) - set(imported) - set(DECLARED_NOT_IMPORTED))
    assert unimported == [], f"declared but imported nowhere: {unimported}"
