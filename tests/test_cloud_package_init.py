"""A fresh review database needs no provider SDK.

U12 commit 6 (R625(a)(4), R640; row B-F17 (g)). Migration 014 imports
`engine.cloud.schema` inside `run_migration`, which runs the package
`engine/cloud/__init__.py` first. That `__init__` imported both extractor modules, and
they import `anthropic` and `openai` at module level — so constructing a fresh
`ReviewDatabase` without the SDKs raised `MigrationError` from 014
(`12o_U12_C6A.md` §3). The package init now imports nothing; callers name submodules.

(i) and (ii) run in a fresh interpreter: in this process the SDKs may already be in
`sys.modules` (`test_cloud_extraction.py` imports both), so blocking them here would
prove nothing. The child gets the repository on `sys.path` explicitly — the suite
provides no PYTHONPATH — and builds its database under `tmp_path`, a fresh path on
every run, because a failed migration leaves the receipts written before it.
"""

from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path

from engine.migrations.runner import discover, kind_of

REPO = Path(__file__).resolve().parents[1]
PACKAGE_INIT = REPO / "engine" / "cloud" / "__init__.py"

#: Prepended to every child: both SDKs blocked before any engine import.
_BLOCK_SDKS = """
import json, sys
sys.modules["openai"] = None
sys.modules["anthropic"] = None
sys.path.insert(0, sys.argv[1])
"""


def _run_blocked(body: str, *args: str) -> dict:
    done = subprocess.run(
        [sys.executable, "-I", "-c", _BLOCK_SDKS + body, str(REPO), *args],
        cwd=REPO, capture_output=True, text=True, timeout=120,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_fresh_review_database_needs_no_provider_sdk(tmp_path):
    """(i) Construction succeeds with both SDKs blocked and receipts every schema
    migration. The expected set is derived from the runner — every numbered file that is
    not a data migration, which a fresh database skips — not written down: 18 at 12o
    (the C6A control, `12o_U12_C6A/reproduce_control.out`)."""
    out = _run_blocked(
        """
import sqlite3
from engine.core.database import ReviewDatabase
db = ReviewDatabase(sys.argv[3], data_root=__import__("pathlib").Path(sys.argv[2]))
conn = sqlite3.connect(f"file:{db.db_path}?mode=ro", uri=True)
print(json.dumps({"receipts": sorted(r[0] for r in conn.execute(
    "SELECT migration_id FROM schema_migrations"))}))
""",
        str(tmp_path), "sdkfree_review",
    )
    expected = sorted(mid for mid, _ in discover() if kind_of(mid) != "data")
    assert out["receipts"] == expected


def test_cloud_schema_and_base_import_without_either_sdk():
    """(ii) The modules 014 and the resolver reach load no provider SDK."""
    out = _run_blocked(
        """
import engine.cloud.schema, engine.cloud.base
print(json.dumps({"loaded": [m for m in ("openai", "anthropic")
                             if sys.modules.get(m) is not None]}))
""",
    )
    assert out["loaded"] == []


def test_the_cloud_package_init_imports_nothing():
    """(iii) Static: no Import or ImportFrom node anywhere in the package init."""
    tree = ast.parse(PACKAGE_INIT.read_text(encoding="utf-8"))
    imports = [ast.unparse(n) for n in ast.walk(tree)
               if isinstance(n, (ast.Import, ast.ImportFrom))]
    assert imports == []
