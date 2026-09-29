"""The I16 guard: runner.run refuses pending migrations on a non-fresh
(receipt-bearing) database unless apply_pending=True (R222/R222a).

A fresh database (no schema_migrations table, or the table exists with zero
rows) is unaffected by any test here — see test_migration_runner.py's existing
suite, in particular test_receipts_list_every_executed_migration_in_order,
which this file does not duplicate.
"""

from __future__ import annotations

import hashlib
import sqlite3
import sys
from pathlib import Path

import pytest

from engine.core.database import ReviewDatabase
from engine.migrations import runner


def _receipts(path):
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return [dict(zip(("id", "sha", "at", "mode", "ver", "note"), r))
                for r in conn.execute(
                    "SELECT migration_id, file_sha256, applied_at, mode, "
                    "runner_version, note FROM schema_migrations "
                    "ORDER BY migration_id")]
    finally:
        conn.close()


def _sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


@pytest.fixture
def fresh(tmp_path):
    """A receipt-bearing database built the way production builds one — every
    real migration already applied, exactly the T2/T4/T5 starting point."""
    db = ReviewDatabase("scratch_review", data_root=tmp_path)
    path = db.db_path
    db._conn.close()
    return path


def _inject_fake_migration(monkeypatch, tmp_path, *, migration_id, kind,
                           body, name_suffix=""):
    """I3: present a pending migration by monkeypatching discovery, backed by
    a real file on disk (file_sha256 reads it) and a real module in
    sys.modules (importlib.import_module finds it there before the filesystem).
    """
    fake_path = tmp_path / f"{migration_id}{name_suffix}.py"
    fake_path.write_text(body)

    import types
    module_name = f"engine.migrations.{migration_id}"
    fake_module = types.ModuleType(module_name)
    ns: dict = {}
    exec(compile(body, str(fake_path), "exec"), ns)
    fake_module.run_migration = ns["run_migration"]
    monkeypatch.setitem(sys.modules, module_name, fake_module)

    monkeypatch.setitem(runner.KINDS, migration_id.split("_", 1)[0], kind)

    # Real data migrations (002, 003) are excluded from the injected list
    # entirely — never just filtered by include_data — because 003 reads a
    # fixed source directory naming surgical_autonomy (S9; "what must NOT be
    # re-run") and must never execute against this scratch database, which an
    # include_data=True test in this module would otherwise trigger for real.
    real_discover = runner.discover
    monkeypatch.setattr(
        runner, "discover",
        lambda: [(m, p) for m, p in real_discover() if runner.kind_of(m) != "data"]
        + [(migration_id, fake_path)],
    )
    return fake_path


_FAKE_SCHEMA_BODY = (
    "def run_migration(db_path):\n"
    "    import sqlite3\n"
    "    conn = sqlite3.connect(db_path)\n"
    "    conn.execute('CREATE TABLE IF NOT EXISTS fake_pending_marker "
    "(id INTEGER PRIMARY KEY)')\n"
    "    conn.commit()\n"
    "    conn.close()\n"
    "    return {'created': ['fake_pending_marker']}\n"
)

_FAKE_DATA_BODY = (
    "def run_migration(db_path):\n"
    "    import sqlite3\n"
    "    conn = sqlite3.connect(db_path)\n"
    "    conn.execute('CREATE TABLE IF NOT EXISTS fake_data_marker "
    "(id INTEGER PRIMARY KEY)')\n"
    "    conn.commit()\n"
    "    conn.close()\n"
    "    return {'created': ['fake_data_marker']}\n"
)


# ── T1 ──────────────────────────────────────────────────────────────


def test_T1_fresh_database_applies_pending_migrations_unchanged(tmp_path):
    """R222: a fresh database (no receipts) is unaffected by the guard — it
    applies every pending migration exactly as before this commit. The full
    receipt-list assertion is test_migration_runner.py::
    test_receipts_list_every_executed_migration_in_order; this is the
    regression witness that construction itself does not raise."""
    db = ReviewDatabase("scratch_review", data_root=tmp_path)
    recs = _receipts(db.db_path)
    db._conn.close()

    schema_ids = sorted(m for m, _ in runner.discover() if runner.kind_of(m) == "schema")
    assert sorted(r["id"] for r in recs) == schema_ids
    assert all(r["mode"] == "executed" for r in recs)


# ── T2 ──────────────────────────────────────────────────────────────


def test_T2_non_fresh_database_with_pending_migration_refuses(fresh, tmp_path, monkeypatch):
    """R222: a receipt-bearing database with a pending schema migration raises
    PendingMigrations before any write; the file is untouched."""
    fake_id = "997_fake_pending_schema"
    _inject_fake_migration(
        monkeypatch, fresh.parent, migration_id=fake_id, kind="schema",
        body=_FAKE_SCHEMA_BODY,
    )

    before = _sha256(fresh)
    with pytest.raises(runner.PendingMigrations) as exc:
        ReviewDatabase("scratch_review", data_root=tmp_path)
    after = _sha256(fresh)

    assert before == after, "the database must be byte-identical before and after the raise"
    message = str(exc.value)
    assert fake_id in message
    assert f"python -m engine.migrations {fresh.resolve()} --apply-pending" in message
    assert "--include-data" not in message, "no data migration is pending here"


# ── T3 ──────────────────────────────────────────────────────────────


def test_T3_apply_pending_true_applies_the_fake_migration(fresh, tmp_path, monkeypatch):
    """R222: apply_pending=True on the same database applies what T2 refused;
    a subsequent ReviewDatabase construction then succeeds."""
    fake_id = "997_fake_pending_schema"
    _inject_fake_migration(
        monkeypatch, fresh.parent, migration_id=fake_id, kind="schema",
        body=_FAKE_SCHEMA_BODY,
    )

    result = runner.run(fresh, apply_pending=True)
    assert result["executed"] == [fake_id]

    recs = {r["id"] for r in _receipts(fresh)}
    assert fake_id in recs

    db = ReviewDatabase("scratch_review", data_root=tmp_path)
    db._conn.close()


# ── T4 ──────────────────────────────────────────────────────────────


def test_T4_non_fresh_database_with_nothing_pending_succeeds(fresh, tmp_path):
    """R222 regression witness: every live-style open — a receipt-bearing
    database with nothing pending — is unaffected by the guard."""
    db = ReviewDatabase("scratch_review", data_root=tmp_path)
    db._conn.close()
    assert _receipts(fresh) == _receipts(fresh)  # constructed twice, no error


# ── T5 ──────────────────────────────────────────────────────────────


def test_T5_data_migration_pending_include_data_false_is_skipped_not_raised(
    fresh, tmp_path, monkeypatch,
):
    """R222/data kind: a pending data migration is not 'pending' for the guard
    unless include_data is requested — matching today's skip behaviour."""
    fake_id = "998_fake_pending_data"
    _inject_fake_migration(
        monkeypatch, fresh.parent, migration_id=fake_id, kind="data",
        body=_FAKE_DATA_BODY,
    )

    db = ReviewDatabase("scratch_review", data_root=tmp_path)
    db._conn.close()
    assert fake_id not in {r["id"] for r in _receipts(fresh)}, (
        "a data migration must not be applied without include_data"
    )


def test_T5_data_migration_pending_include_data_true_refuses(fresh, monkeypatch):
    """R222/data kind: with include_data=True the data migration IS pending,
    so apply_pending=False still refuses, naming --include-data in the remedy."""
    fake_id = "998_fake_pending_data"
    _inject_fake_migration(
        monkeypatch, fresh.parent, migration_id=fake_id, kind="data",
        body=_FAKE_DATA_BODY,
    )

    with pytest.raises(runner.PendingMigrations) as exc:
        runner.run(fresh, include_data=True)
    message = str(exc.value)
    assert fake_id in message
    assert "--include-data" in message


def test_T5_data_migration_pending_both_true_applies(fresh, monkeypatch):
    """R222/data kind: apply_pending=True and include_data=True together apply
    the pending data migration."""
    fake_id = "998_fake_pending_data"
    _inject_fake_migration(
        monkeypatch, fresh.parent, migration_id=fake_id, kind="data",
        body=_FAKE_DATA_BODY,
    )

    result = runner.run(fresh, include_data=True, apply_pending=True)
    assert result["executed"] == [fake_id]


# ── T6 — the CLI ──────────────────────────────────────────────────────


def test_T6_cli_without_apply_pending_exits_nonzero_and_names_the_remedy(
    fresh, monkeypatch,
):
    """R222a: the CLI without --apply-pending exits non-zero and prints the
    pending id and the remedy invocation; the live fence is not implicated —
    this is a temporary path, never data/surgical_autonomy/review.db."""
    fake_id = "997_fake_pending_schema"
    _inject_fake_migration(
        monkeypatch, fresh.parent, migration_id=fake_id, kind="schema",
        body=_FAKE_SCHEMA_BODY,
    )

    from engine.migrations.__main__ import main
    import io, contextlib

    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        code = main([str(fresh)])
    assert code != 0
    assert fake_id in err.getvalue()
    assert "--apply-pending" in err.getvalue()


def test_T6_cli_with_apply_pending_exits_zero_and_applies(fresh, tmp_path, monkeypatch):
    """R222a: the CLI with --apply-pending exits 0, prints the executed id,
    and a subsequent ReviewDatabase construction then succeeds."""
    fake_id = "997_fake_pending_schema"
    _inject_fake_migration(
        monkeypatch, fresh.parent, migration_id=fake_id, kind="schema",
        body=_FAKE_SCHEMA_BODY,
    )

    from engine.migrations.__main__ import main
    import io, contextlib

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = main([str(fresh), "--apply-pending"])
    assert code == 0
    printed = out.getvalue()
    assert fake_id in printed
    assert "'executed'" in printed

    db = ReviewDatabase("scratch_review", data_root=tmp_path)
    db._conn.close()
