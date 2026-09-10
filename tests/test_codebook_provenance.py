"""CODEBOOK-AUTH-01 Phase 2 — codebook provenance (T5).

Migration 012 on a temp copy of the live database, and the write sites that
record the two hashes beside the spec-derived schema hash.
"""

from __future__ import annotations

import importlib
import shutil
import sqlite3
from pathlib import Path

import pytest

from engine.core.codebook import load_codebook_for

MIG = importlib.import_module("engine.migrations.012_codebook_provenance")

REPO_ROOT = Path(__file__).resolve().parent.parent
LIVE_DB = REPO_ROOT / "data" / "surgical_autonomy" / "review.db"


@pytest.fixture
def db_copy(tmp_path):
    """A copy. The live database is never opened by this suite."""
    if not LIVE_DB.exists():
        pytest.skip("live review.db not present")
    dest = tmp_path / "review.db"
    shutil.copy2(LIVE_DB, dest)
    return dest


# ── T5: the migration ────────────────────────────────────────────────


def test_migration_adds_both_columns_to_all_three_tables(db_copy):
    result = MIG.run_migration(str(db_copy))
    assert result["table_absent"] == []
    assert set(result["added"]) == {
        f"{t}.{c}" for t in MIG.TARGETS for c in MIG.COLUMNS
    }
    conn = sqlite3.connect(str(db_copy))
    try:
        for table in MIG.TARGETS:
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            assert set(MIG.COLUMNS) <= cols, table
    finally:
        conn.close()


def test_existing_rows_are_null_not_backfilled(db_copy):
    MIG.run_migration(str(db_copy))
    conn = sqlite3.connect(str(db_copy))
    try:
        for table in MIG.TARGETS:
            total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            nulls = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE codebook_hash IS NULL"
            ).fetchone()[0]
            assert nulls == total, table
    finally:
        conn.close()


def test_migration_is_idempotent(db_copy):
    first = MIG.run_migration(str(db_copy))
    second = MIG.run_migration(str(db_copy))
    assert first["added"] and second["added"] == []
    assert set(second["already_present"]) == set(first["added"])


def test_missing_target_table_is_skipped_not_created(tmp_path):
    """A migration that invented an empty table would hide a lane that was
    never set up."""
    p = tmp_path / "bare.db"
    conn = sqlite3.connect(str(p))
    conn.execute("CREATE TABLE extractions (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    result = MIG.run_migration(str(p))
    assert set(result["table_absent"]) == {"cloud_extractions", "review_runs"}
    conn = sqlite3.connect(str(p))
    try:
        names = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert "review_runs" not in names
    finally:
        conn.close()


def test_migration_requires_a_path():
    with pytest.raises(ValueError):
        MIG.run_migration(None)


def test_the_migration_is_wired_not_hand_run():
    """010 and 011 are hand-run; this one is not.

    An extraction written into a database without these columns records no
    codebook at all, and that gap is indistinguishable from an unedited one.
    """
    src = (REPO_ROOT / "engine/core/database.py").read_text()
    assert "012_codebook_provenance" in src


# ── The write sites ──────────────────────────────────────────────────


def test_a_new_database_has_the_columns(tmp_path):
    from engine.core.database import ReviewDatabase

    db = ReviewDatabase("prov_test", data_root=tmp_path)
    try:
        for table in ("extractions", "review_runs"):
            cols = {r[1] for r in db._conn.execute(f"PRAGMA table_info({table})")}
            assert set(MIG.COLUMNS) <= cols, table
    finally:
        db.close()


def test_cloud_tables_carry_the_columns_when_created_fresh(tmp_path):
    """cloud_extractions is created outside ReviewDatabase's migration path."""
    from engine.cloud.schema import init_cloud_tables

    p = tmp_path / "review.db"
    conn = sqlite3.connect(str(p))
    conn.execute("CREATE TABLE papers (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    init_cloud_tables(str(p))
    conn = sqlite3.connect(str(p))
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(cloud_extractions)")}
        assert set(MIG.COLUMNS) <= cols
    finally:
        conn.close()


def test_cloud_tables_gain_the_columns_when_they_predate_them(tmp_path):
    from engine.cloud.schema import init_cloud_tables

    p = tmp_path / "review.db"
    conn = sqlite3.connect(str(p))
    conn.executescript("""
        CREATE TABLE papers (id INTEGER PRIMARY KEY);
        CREATE TABLE cloud_extractions (
            id INTEGER PRIMARY KEY, paper_id INTEGER, arm TEXT,
            model_string TEXT, extracted_at TEXT NOT NULL);
    """)
    conn.commit()
    conn.close()

    init_cloud_tables(str(p))
    conn = sqlite3.connect(str(p))
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(cloud_extractions)")}
        assert set(MIG.COLUMNS) <= cols
    finally:
        conn.close()


def test_the_two_hashes_answer_different_questions():
    """A comment moves one and not the other. That is the point of having both."""
    cb = load_codebook_for("surgical_autonomy")
    assert cb.semantic_hash != cb.sha256
    assert len(cb.semantic_hash) == 64 and len(cb.sha256) == 64


def test_add_extraction_atomic_stores_both(tmp_path):
    from engine.core.database import ReviewDatabase
    from engine.search.models import Citation

    db = ReviewDatabase("prov_write", data_root=tmp_path)
    try:
        db.add_papers([Citation(title="T", authors=[], year=2026, source="pubmed")])
        pid = db._conn.execute("SELECT id FROM papers").fetchone()[0]
        db.add_extraction_atomic(
            paper_id=pid, schema_hash="spec-hash", extracted_data={},
            reasoning_trace="", model="m",
            spans=[{"field_name": "f", "value": "v",
                    "source_snippet": "s", "confidence": 0.9, "tier": 1}],
            codebook_hash="cb-semantic", codebook_sha256="cb-bytes",
        )
        row = db._conn.execute(
            "SELECT extraction_schema_hash, codebook_hash, codebook_sha256 "
            "FROM extractions"
        ).fetchone()
        assert tuple(row) == ("spec-hash", "cb-semantic", "cb-bytes")
    finally:
        db.close()


def test_the_columns_default_to_null_when_not_supplied(tmp_path):
    """NULL means nobody recorded it. It never means 'unchanged'."""
    from engine.core.database import ReviewDatabase
    from engine.search.models import Citation

    db = ReviewDatabase("prov_null", data_root=tmp_path)
    try:
        db.add_papers([Citation(title="T", authors=[], year=2026, source="pubmed")])
        pid = db._conn.execute("SELECT id FROM papers").fetchone()[0]
        db.add_extraction_atomic(
            paper_id=pid, schema_hash="h", extracted_data={},
            reasoning_trace="", model="m",
            spans=[{"field_name": "f", "value": "v",
                    "source_snippet": "s", "confidence": 0.9, "tier": 1}],
        )
        row = db._conn.execute(
            "SELECT codebook_hash, codebook_sha256 FROM extractions").fetchone()
        assert tuple(row) == (None, None)
    finally:
        db.close()
