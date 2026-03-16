"""Verify all scripts/ can resolve every engine.* import they reference.

Scans every .py file under scripts/ for import statements that reference
engine.* modules.  For each discovered name, attempts the actual import
and asserts it resolves to a real object (callable, class, constant, etc.).
"""

import ast
import importlib
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _collect_engine_imports() -> list[tuple[str, str, str]]:
    """Parse every script and return (script_name, module, name) triples.

    Handles two forms:
        from engine.foo.bar import baz, qux   -> (script, "engine.foo.bar", "baz"), ...
        import engine.foo.bar                  -> (script, "engine.foo.bar", None)
        import engine.foo.bar as alias         -> (script, "engine.foo.bar", None)
    """
    results: list[tuple[str, str, str]] = []

    for py_file in sorted(SCRIPTS_DIR.glob("*.py")):
        try:
            tree = ast.parse(py_file.read_text(), filename=str(py_file))
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if not module.startswith("engine"):
                    continue
                for alias in node.names:
                    results.append((py_file.name, module, alias.name))

            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("engine"):
                        # Plain `import engine.x.y [as z]` — just verify module
                        results.append((py_file.name, alias.name, None))

    return results


_ENGINE_IMPORTS = _collect_engine_imports()


@pytest.mark.parametrize(
    "script,module,name",
    _ENGINE_IMPORTS,
    ids=[
        f"{s}::{m}.{n}" if n else f"{s}::{m}"
        for s, m, n in _ENGINE_IMPORTS
    ],
)
def test_engine_import_resolves(script: str, module: str, name: str | None):
    """Each engine.* import referenced in a script must resolve at runtime."""
    try:
        mod = importlib.import_module(module)
    except ImportError as exc:
        pytest.fail(
            f"{script} imports from '{module}' but the module cannot be imported: {exc}"
        )

    if name is None:
        # Plain `import engine.x.y` — module existence is sufficient
        return

    obj = getattr(mod, name, None)
    if obj is None:
        available = [a for a in dir(mod) if not a.startswith("_")]
        pytest.fail(
            f"{script} imports '{name}' from '{module}' but it does not exist. "
            f"Available names: {', '.join(available)}"
        )
