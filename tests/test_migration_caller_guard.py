"""R225/C22 — `_called_from_migration`'s file-location guard, tested directly.

Applied migration 017 calls `write_paper_event(..., run_marker='pre-manifest')`
and its text is checksummed (R35), so the caller's FILE LOCATION is the only
identity `_called_from_migration` can check. No test touched this predicate
before this file (10a-C9 A4 census: zero hits).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from engine.core import events

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


def _load_module(path: Path, name: str):
    """Import a standalone .py file by path, the way engine.migrations.NNN_* modules
    are imported — a real module object with its own __file__, not exec'd inline
    (exec'd code has no co_filename `_called_from_migration` could resolve)."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def conn(tmp_path):
    from engine.core.database import ReviewDatabase
    db = ReviewDatabase("guard", data_root=tmp_path)
    db._conn.execute(
        "INSERT INTO papers (id, title, source, created_at, updated_at) "
        "VALUES (1, 't', 's', 'n', 'n')")
    db._conn.commit()
    yield db._conn
    db.close()


_SEED_KWARGS = dict(
    event_type="state_at_migration", paper_id=1, to_state="eligible",
    actor_kind="engine", actor_role="system", actor_name="guard-test",
    run_marker="pre-manifest", run_id=None,
)


def _wrapper_in_test_file(conn, **kwargs):
    """Defined HERE, in this test file — outside engine/migrations/. Calling
    this directly makes *this file* the direct caller of write_paper_event."""
    return events.write_paper_event(conn, **kwargs)


# ── T8 — a caller outside engine/migrations/ is refused ───────────────
def test_T8_a_caller_outside_migrations_is_refused_with_the_r68_message(conn):
    """C22/R82: this test file's own frame is the direct caller of
    write_paper_event (via the local wrapper above), and this file is not
    under engine/migrations/."""
    with pytest.raises(events.RunLinkRefused, match="only by a migration"):
        _wrapper_in_test_file(conn, **_SEED_KWARGS)
    assert conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0] == 0


# ── T9 — a module under a monkeypatched _MIGRATIONS_DIR is accepted ───
def test_T9_a_module_under_migrations_dir_is_accepted(conn, tmp_path, monkeypatch):
    """C22/R82: a real module file, physically placed under a directory this
    test monkeypatches _MIGRATIONS_DIR to — the same identity a real numbered
    migration module has, without needing one on disk under engine/migrations/."""
    fake_migrations = tmp_path / "fake_migrations"
    fake_migrations.mkdir()
    mod_path = fake_migrations / "999_fake_seed.py"
    mod_path.write_text(
        "from engine.core import events\n"
        "def seed(conn, **kwargs):\n"
        "    return events.write_paper_event(conn, **kwargs)\n"
    )
    monkeypatch.setattr(events, "_MIGRATIONS_DIR", fake_migrations.resolve())
    fake_seed = _load_module(mod_path, "fake_migration_999")

    event_id = fake_seed.seed(conn, **_SEED_KWARGS)
    assert event_id is not None
    row = conn.execute(
        "SELECT run_id, run_marker FROM paper_events WHERE event_id = ?",
        (event_id,)).fetchone()
    assert tuple(row) == (None, "pre-manifest")


# ── T10 — depth sensitivity: the DIRECT caller's frame, not any transitive one ──
def test_T10_a_migrations_dir_module_two_frames_up_is_not_enough(conn, tmp_path, monkeypatch):
    """C22/R82: proves `_called_from_migration` checks `sys._getframe(2)` — the
    IMMEDIATE caller of `write_paper_event` — not "is a module under
    engine/migrations/ anywhere on the call stack".

    Here a real module under the monkeypatched `_MIGRATIONS_DIR` calls a
    wrapper *defined in this test file* (outside `_MIGRATIONS_DIR`), and that
    wrapper is what directly calls `write_paper_event`. A migrations-dir
    module IS on the stack — as the caller of the caller — but the DIRECT
    caller (this file's `_wrapper_in_test_file`) is not, so the write is
    refused exactly as T8's. If the predicate instead searched the whole
    stack for a migrations-dir frame (the bug this depth pins against), this
    call would wrongly succeed."""
    fake_migrations = tmp_path / "fake_migrations"
    fake_migrations.mkdir()
    mod_path = fake_migrations / "997_indirect_seed.py"
    mod_path.write_text(
        "def seed_via(wrapper, conn, **kwargs):\n"
        "    return wrapper(conn, **kwargs)\n"
    )
    monkeypatch.setattr(events, "_MIGRATIONS_DIR", fake_migrations.resolve())
    fake_indirect = _load_module(mod_path, "fake_migration_997")

    with pytest.raises(events.RunLinkRefused, match="only by a migration"):
        fake_indirect.seed_via(_wrapper_in_test_file, conn, **_SEED_KWARGS)
    assert conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0] == 0
