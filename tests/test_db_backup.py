"""Tests for WAL-aware DB backup and restore (SAFE-GROUND-01).

The fixture below is the point of this file. It used to close its connection
and never enable WAL, which is not the shape `ReviewDatabase` runs in — and
under that friendlier shape every assertion passed against a backup mechanism
that, in production, produced a database with no tables in it. A fixture more
permissive than reality proves a path production cannot reach.
"""

import sqlite3
from pathlib import Path

import pytest

from engine.core.database import ReviewDatabase
from engine.tools.db_fingerprint import compare, fingerprint
from engine.utils import db_backup as dbb
from engine.utils.db_backup import (
    BackupVerificationError,
    RestoreRefused,
    auto_backup,
    restore,
)


@pytest.fixture
def temp_db(tmp_path):
    """A database in the shape production runs in: WAL, connection held open.

    The connection is deliberately NOT closed and NO checkpoint is taken, so
    the committed row below lives only in the `-wal`. That is the condition the
    old `shutil.copy2` could not survive.
    """
    db_path = tmp_path / "review.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
    conn.execute("INSERT INTO t VALUES (1, 'hello')")
    conn.commit()
    yield db_path
    conn.close()


# ── The defect this module exists to close ──────────────────────────


def test_backup_includes_uncheckpointed_wal_rows(tmp_path):
    """A row committed on an open WAL connection, never checkpointed, is in the backup."""
    db_path = tmp_path / "review.db"
    live = sqlite3.connect(str(db_path))
    try:
        live.execute("PRAGMA journal_mode=WAL")
        live.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        live.commit()
        live.execute("INSERT INTO t (v) VALUES ('committed-but-in-the-wal')")
        live.commit()  # committed; no checkpoint; `live` stays open

        wal = Path(str(db_path) + "-wal")
        assert wal.exists() and wal.stat().st_size > 0, (
            "the fixture must leave content in the -wal, or it is not testing "
            "the condition it was written for"
        )

        result = auto_backup(db_path, "wal-test")

        # The backup is self-contained: no sidecar is written beside it, and
        # none is needed to read it.
        assert not Path(str(result.path) + "-wal").exists()
        assert not Path(str(result.path) + "-shm").exists()

        backup = sqlite3.connect(f"file:{result.path}?mode=ro", uri=True)
        try:
            rows = backup.execute("SELECT v FROM t").fetchall()
        finally:
            backup.close()
        assert rows == [("committed-but-in-the-wal",)]

        # Still none AFTER reading it. This is the property that matters and
        # the one the first draft got wrong: the backup API copies the source
        # header, so an unswitched backup of a WAL database is a WAL database
        # and merely opening it litters the directory it sits in — which is
        # the live data directory.
        assert not Path(str(result.path) + "-wal").exists()
        assert not Path(str(result.path) + "-shm").exists()

        journal = sqlite3.connect(f"file:{result.path}?mode=ro", uri=True)
        try:
            assert journal.execute("PRAGMA journal_mode").fetchone()[0].lower() == "delete"
        finally:
            journal.close()

        # Taking the backup must not have checkpointed the source.
        assert wal.stat().st_size > 0
    finally:
        live.close()


def test_backup_production_shape(tmp_path):
    """A real ReviewDatabase — WAL on, connection held open — backs up complete."""
    db = ReviewDatabase("scratch_review", data_root=tmp_path)
    try:
        assert db._conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"

        db._conn.execute(
            "INSERT INTO papers (pmid, doi, title, abstract, authors, journal, "
            "year, source, status, created_at, updated_at) "
            "VALUES ('1','10.1/x','A trial','abs','Doe J','J Surg',2024,"
            "'pubmed','INGESTED','2026-01-01','2026-01-01')"
        )
        db._conn.execute(
            "INSERT INTO extractions (paper_id, extraction_schema_hash, "
            "extracted_data, reasoning_trace, model, extracted_at, low_yield) "
            "VALUES (1,'h','{}','','m','2026-01-01',0)"
        )
        db._conn.execute(
            "INSERT INTO evidence_spans (extraction_id, field_name, value, "
            "source_snippet, confidence, tier, audit_status) "
            "VALUES (1,'study_type','RCT','snippet',0.9,1,'verified')"
        )
        db._conn.commit()

        result = auto_backup(db._conn, "production-shape")

        counts = {
            name: result.fingerprint["tables"][name]["row_count"]
            for name in ("papers", "extractions", "evidence_spans")
        }
        assert counts == {"papers": 1, "extractions": 1, "evidence_spans": 1}, counts

        backup = sqlite3.connect(f"file:{result.path}?mode=ro", uri=True)
        try:
            assert backup.execute("SELECT title FROM papers").fetchall() == [("A trial",)]
            assert backup.execute("SELECT model FROM extractions").fetchall() == [("m",)]
            assert backup.execute(
                "SELECT value FROM evidence_spans"
            ).fetchall() == [("RCT",)]
        finally:
            backup.close()
    finally:
        db._conn.close()


def test_backup_fingerprint_mismatch_raises(tmp_path, monkeypatch):
    """A backup that does not match its source is deleted, not returned."""
    db_path = tmp_path / "review.db"
    live = sqlite3.connect(str(db_path))
    try:
        live.execute("PRAGMA journal_mode=WAL")
        live.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        live.execute("INSERT INTO t (v) VALUES ('x')")
        live.commit()

        real = dbb.fingerprint
        calls = {"n": 0}

        def fake_fingerprint(target):
            fp = real(target)
            calls["n"] += 1
            if calls["n"] == 2:  # the backup's, taken after the source's
                fp = dict(fp)
                fp["overall_sha256"] = "0" * 64
                fp["tables"] = dict(fp["tables"])
                fp["tables"]["t"] = dict(fp["tables"]["t"], row_count=99)
            return fp

        monkeypatch.setattr(dbb, "fingerprint", fake_fingerprint)

        before = set(tmp_path.iterdir())
        with pytest.raises(BackupVerificationError) as exc:
            auto_backup(db_path, "mismatch")

        assert "row_count" in str(exc.value) or "overall_sha256" in str(exc.value)
        assert set(tmp_path.iterdir()) == before, (
            "a backup that failed verification must leave no file behind"
        )
    finally:
        live.close()


# ── Restore ─────────────────────────────────────────────────────────


def test_restore_roundtrip(tmp_path):
    """backup -> restore to a new path -> fingerprints equal."""
    db_path = tmp_path / "review.db"
    live = sqlite3.connect(str(db_path))
    live.execute("PRAGMA journal_mode=WAL")
    live.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    for v in ("a", "b", "c"):
        live.execute("INSERT INTO t (v) VALUES (?)", (v,))
    live.commit()
    source_fp = fingerprint(live)
    result = auto_backup(live, "roundtrip")
    live.close()

    target = tmp_path / "restored.db"
    final_fp = restore(result.path, target, expected_fingerprint=result.fingerprint)

    assert compare(source_fp, final_fp) == []
    assert compare(result.fingerprint, final_fp) == []
    assert final_fp["overall_sha256"] == source_fp["overall_sha256"]

    check = sqlite3.connect(f"file:{target}?mode=ro", uri=True)
    try:
        assert check.execute("SELECT v FROM t ORDER BY id").fetchall() == [
            ("a",), ("b",), ("c",)
        ]
    finally:
        check.close()


def test_restore_refuses_open_target(tmp_path):
    """A target any connection still holds open is refused before anything is written."""
    db_path = tmp_path / "review.db"
    live = sqlite3.connect(str(db_path))
    live.execute("PRAGMA journal_mode=WAL")
    live.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    live.execute("INSERT INTO t (v) VALUES ('original')")
    live.commit()
    result = auto_backup(live, "refusal")

    target = tmp_path / "target.db"
    holder = sqlite3.connect(str(target))
    holder.execute("PRAGMA journal_mode=WAL")
    holder.execute("CREATE TABLE other (x)")
    holder.commit()
    holder.execute("SELECT count(*) FROM other").fetchone()  # idle, but open

    try:
        with pytest.raises(RestoreRefused, match="open elsewhere"):
            restore(result.path, target)
        assert not list(target.parent.glob(f"{target.name}.restore-tmp-*"))
        assert holder.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall() == [("other",)], "the target must be untouched"
    finally:
        holder.close()
        live.close()

    # And once the holder closes, the same restore succeeds.
    restore(result.path, target)
    check = sqlite3.connect(f"file:{target}?mode=ro", uri=True)
    try:
        assert check.execute("SELECT v FROM t").fetchall() == [("original",)]
    finally:
        check.close()


# ── The three original assertions, kept ─────────────────────────────
#
# They no longer stand alone: each one passed against the defect the tests
# above close. They are retained because the filename convention and the
# readability of the copy are still contracts.


def test_backup_creates_valid_copy(temp_db):
    """Backup file is a readable SQLite DB with identical content."""
    result = auto_backup(temp_db, "pre-test")

    assert result.path.exists()
    conn = sqlite3.connect(f"file:{result.path}?mode=ro", uri=True)
    try:
        row = conn.execute("SELECT val FROM t WHERE id = 1").fetchone()
    finally:
        conn.close()
    assert row[0] == "hello"


def test_backup_filename_format(temp_db):
    """Backup filename includes the reason and a timestamp."""
    result = auto_backup(temp_db, "pre-cleanup")

    name = result.path.name
    assert name.startswith("review.db.bak-pre-cleanup-")
    timestamp_part = name.split("pre-cleanup-")[1]
    assert len(timestamp_part) == 15  # e.g. 20260316-041500


def test_backup_returns_a_verified_result(temp_db):
    """The return value carries the proof, not just the path.

    The old signature returned a bare Path and all three callers discarded it,
    so nothing downstream could tell a verified backup from a file that merely
    exists.
    """
    result = auto_backup(temp_db, "pre-reset")

    assert isinstance(result, dbb.BackupResult)
    assert result.path.exists()
    assert len(result.overall_sha256) == 64
    assert result.table_count >= 1
    assert compare(result.fingerprint, fingerprint(result.path)) == []
    with pytest.raises(Exception):  # frozen dataclass — the proof cannot be edited
        result.path = Path("/tmp/somewhere-else")
