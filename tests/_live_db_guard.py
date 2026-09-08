"""Shared names for the live-data fence in `conftest.py`.

These live in their own module for one reason: pytest loads `tests/conftest.py`
as the top-level module `conftest` (there is no `tests/__init__.py`), while a
test that writes `from tests.conftest import ...` imports a *second* copy under
a different name. The exception class would then exist twice and
`pytest.raises` would not match the one the fence actually raised. Importing
both sides from here gives exactly one class object.

The rationale for the fence itself is in `conftest.py`.
"""

from __future__ import annotations

from pathlib import Path

# tests/_live_db_guard.py -> tests/ -> repository root.
REPO_ROOT = Path(__file__).resolve().parent.parent
LIVE_DATA_ROOT = (REPO_ROOT / "data").resolve()

violations: list[str] = []


class LiveDatabaseBlocked(BaseException):
    """A test tried to construct a ReviewDatabase under the real data/ tree.

    Deliberately a BaseException, for the same reason as ServiceCallBlocked:
    a caller catching `Exception` must not be able to swallow this and go
    green.
    """


def resolved_review_db_path(review_name, data_root, default_root) -> Path:
    """Mirror ReviewDatabase.__init__'s own path arithmetic, without opening.

    `(data_root or DATA_ROOT) / review_name / "review.db"`, resolved to an
    absolute path. `Path.resolve()` is non-strict, so a path that does not
    exist yet still resolves.
    """
    root = Path(data_root) if data_root is not None else Path(default_root)
    return (root / str(review_name) / "review.db").resolve()


def is_live_data_path(candidate: Path) -> bool:
    """True when the resolved path lies inside the repository's real data/."""
    return candidate.is_relative_to(LIVE_DATA_ROOT)


def refuse(candidate: Path, nodeid: str) -> None:
    """Record and raise. Never opens anything."""
    message = (
        f"BLOCKED: {nodeid} tried to open a live review database:\n"
        f"  {candidate}\n"
        f"That path is inside {LIVE_DATA_ROOT}, which holds production review "
        f"data. `ReviewDatabase.__init__` opens read-write and runs "
        f"executescript(_SCHEMA) plus migrations 006-009 on every "
        f"construction, so an open writes to the corpus whenever the schema is "
        f"behind the code (JUDGE-DBGUARD-01).\n"
        f"Fix: pass a temp root — ReviewDatabase(name, data_root=tmp_path) — "
        f"and copy in the rows the test needs, as "
        f"tests/analysis/paper1/test_judge_pass2.py does."
    )
    violations.append(message)
    raise LiveDatabaseBlocked(message)
