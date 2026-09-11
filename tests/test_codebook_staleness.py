"""SCHEMA-DERIVE-01 C5/C6 — provenance decisions read codebook_hash.

T4 the NULL-aware staleness predicate · T5 parity on codebook_hash ·
T6 migration 013 on a temp copy.
"""

from __future__ import annotations

import hashlib
import importlib
import shutil
import sqlite3
from pathlib import Path

import pytest

from engine.core.codebook import load_codebook_beside
from engine.core.database import ReviewDatabase
from engine.search.models import Citation

MIG013 = importlib.import_module("engine.migrations.013_drop_schema_hash_not_null")
REPO_ROOT = Path(__file__).resolve().parent.parent
LIVE_DB = REPO_ROOT / "data" / "surgical_autonomy" / "review.db"


def _paper(db, pmid="1"):
    db.add_papers([Citation(title="T", source="pubmed", pmid=pmid)])
    return db._conn.execute(
        "SELECT id FROM papers WHERE pmid = ?", (pmid,)).fetchone()["id"]


# ── T4: NULL is stale ────────────────────────────────────────────────


def test_null_codebook_hash_is_stale(tmp_path):
    """R3. Every extraction predating migration 012 has no codebook_hash, and
    "nobody recorded it" is not "it matches" — a bare `!= ?` would treat all
    190 live rows as current, the opposite of the truth."""
    db = ReviewDatabase("stale_null", data_root=tmp_path)
    try:
        pid = _paper(db)
        db.update_status(pid, "ABSTRACT_SCREENED_IN")
        db.update_status(pid, "PDF_ACQUIRED")
        db.update_status(pid, "PARSED")
        db.update_status(pid, "EXTRACTED")
        db.add_extraction(pid, "a-retired-spec-hash", {}, "t", "m")  # no codebook_hash
        assert len(db.get_stale_extractions("current")) == 1
    finally:
        db.close()


def test_matching_codebook_hash_is_not_stale(tmp_path):
    db = ReviewDatabase("stale_match", data_root=tmp_path)
    try:
        pid = _paper(db)
        for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"):
            db.update_status(pid, s)
        db.add_extraction(pid, None, {}, "t", "m", codebook_hash="current")
        assert db.get_stale_extractions("current") == []
    finally:
        db.close()


def test_differing_codebook_hash_is_stale(tmp_path):
    db = ReviewDatabase("stale_diff", data_root=tmp_path)
    try:
        pid = _paper(db)
        for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"):
            db.update_status(pid, s)
        db.add_extraction(pid, None, {}, "t", "m", codebook_hash="older")
        assert len(db.get_stale_extractions("current")) == 1
    finally:
        db.close()


def test_the_count_predicate_agrees_with_the_row_predicate(tmp_path):
    """check_stale_extractions and get_stale_extractions must not disagree."""
    from engine.utils.extraction_cleanup import check_stale_extractions

    db = ReviewDatabase("stale_count", data_root=tmp_path)
    try:
        for i, h in enumerate((None, "older", "current")):
            pid = _paper(db, pmid=str(i))
            for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"):
                db.update_status(pid, s)
            db.add_extraction(pid, None, {}, "t", "m", codebook_hash=h)
        assert check_stale_extractions(db, "current") == 2
        assert len(db.get_stale_extractions("current")) == 2
    finally:
        db.close()


def test_the_predicate_is_spelled_so_null_matches():
    """Grep-level, because the defect is a missing OR, not a wrong value."""
    for rel in ("engine/core/database.py", "engine/utils/extraction_cleanup.py"):
        src = (REPO_ROOT / rel).read_text()
        assert "codebook_hash IS NULL OR" in src, rel


def test_the_current_hash_is_the_codebooks(tmp_path):
    from engine.utils.extraction_cleanup import get_current_schema_hash

    db = ReviewDatabase("hash_src", data_root=tmp_path)
    try:
        beside = load_codebook_beside(db.db_path)
        assert beside.semantic_hash == get_current_schema_hash(
            "hash_src", codebook_path=beside.path)
    finally:
        db.close()


# ── T5: parity reads codebook_hash and still only warns ──────────────


def test_parity_reads_codebook_hash_and_warns_without_blocking(tmp_path, caplog):
    import logging

    from engine.analysis.concordance import check_schema_parity

    p = tmp_path / "review.db"
    conn = sqlite3.connect(str(p))
    conn.executescript("""
        CREATE TABLE extractions (id INTEGER PRIMARY KEY, paper_id INTEGER,
                                  codebook_hash TEXT);
        CREATE TABLE cloud_extractions (id INTEGER PRIMARY KEY, paper_id INTEGER,
                                        arm TEXT, codebook_hash TEXT);
        INSERT INTO extractions (paper_id, codebook_hash) VALUES (1, 'aaa');
        INSERT INTO cloud_extractions (paper_id, arm, codebook_hash)
            VALUES (1, 'openai', 'bbb');
    """)
    conn.commit()
    conn.close()

    with caplog.at_level(logging.WARNING):
        result = check_schema_parity(str(p), ["local", "openai"])

    assert result["local"] == {"aaa"} and result["openai"] == {"bbb"}
    assert any("mismatch" in m.lower() for m in caplog.messages)


def test_parity_reports_null_as_none_recorded(tmp_path):
    """An arm extracted before migration 012 must not silently agree."""
    from engine.analysis.concordance import check_schema_parity

    p = tmp_path / "review.db"
    conn = sqlite3.connect(str(p))
    conn.executescript("""
        CREATE TABLE extractions (id INTEGER PRIMARY KEY, paper_id INTEGER,
                                  codebook_hash TEXT);
        CREATE TABLE cloud_extractions (id INTEGER PRIMARY KEY, paper_id INTEGER,
                                        arm TEXT, codebook_hash TEXT);
        INSERT INTO extractions (paper_id, codebook_hash) VALUES (1, NULL);
    """)
    conn.commit()
    conn.close()

    assert check_schema_parity(str(p), ["local"])["local"] == {"none-recorded"}


# ── T6: migration 013 on a temp copy ─────────────────────────────────


@pytest.fixture
def db_copy(tmp_path):
    if not LIVE_DB.exists():
        pytest.skip("live review.db not present")
    dest = tmp_path / "review.db"
    shutil.copy2(LIVE_DB, dest)
    return dest


def _content(path):
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return {
            t: (lambda rows: (len(rows), hashlib.sha256(repr(rows).encode()).hexdigest()))(
                conn.execute(f"SELECT * FROM {t} ORDER BY id").fetchall())
            for t in ("extractions", "cloud_extractions", "review_runs")
        }
    finally:
        conn.close()


def test_013_lifts_not_null_preserving_every_row(db_copy):
    """Asserts the OUTCOME, not the delta.

    The delta depends on whether the live database has already been migrated —
    it has, since MIGRATE-013 — so a test that asserted `rebuilt` held the two
    constrained columns passed only until the migration it describes was
    applied. Same lesson as 012's.
    """
    before = _content(db_copy)
    result = MIG013.run_migration(str(db_copy))
    after = _content(db_copy)

    every = {f"{t}.{c}" for t, c in MIG013.TARGETS}
    assert set(result["rebuilt"]) | set(result["already_nullable"]) == every
    # cloud_extractions was NEVER constrained, so it is never rebuilt — a table
    # rebuild to remove a constraint it does not have is risk with no benefit.
    assert "cloud_extractions.extraction_schema_hash" in result["already_nullable"]
    assert "cloud_extractions.extraction_schema_hash" not in result["rebuilt"]
    assert before == after, "row content changed"
    assert before["extractions"][0] == 190


def test_013_rebuilds_a_database_that_still_has_the_constraint(tmp_path):
    """The delta assertion, on a database built WITH the NOT NULL.

    Keeps the "it really does lift two" claim under test now that the live
    database — and every copy of it — is already migrated.
    """
    p = tmp_path / "pre.db"
    conn = sqlite3.connect(str(p))
    conn.executescript("""
        CREATE TABLE extractions (
            id INTEGER PRIMARY KEY,
            extraction_schema_hash TEXT NOT NULL,
            extracted_data TEXT);
        CREATE TABLE review_runs (
            id INTEGER PRIMARY KEY,
            extraction_hash TEXT NOT NULL,
            started_at TEXT);
        INSERT INTO extractions (extraction_schema_hash, extracted_data)
            VALUES ('h', '{}');
        INSERT INTO review_runs (extraction_hash, started_at) VALUES ('h', 'now');
    """)
    conn.commit()
    conn.close()

    result = MIG013.run_migration(str(p))
    assert set(result["rebuilt"]) == {
        "extractions.extraction_schema_hash", "review_runs.extraction_hash"}
    assert result["absent"] == ["cloud_extractions"]

    conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    try:
        for table, column in (("extractions", "extraction_schema_hash"),
                              ("review_runs", "extraction_hash")):
            info = {r[1]: r[3] for r in conn.execute(f"PRAGMA table_info({table})")}
            assert info[column] == 0
        assert conn.execute("SELECT COUNT(*) FROM extractions").fetchone()[0] == 1
    finally:
        conn.close()


def test_013_leaves_the_columns_in_place(db_copy):
    """The columns are the historical record of 190 + 379 extractions made
    while the spec hash was the authority. Only the constraint goes."""
    MIG013.run_migration(str(db_copy))
    conn = sqlite3.connect(f"file:{db_copy}?mode=ro", uri=True)
    try:
        for table, column in MIG013.TARGETS:
            cols = {r[1]: r[3] for r in conn.execute(f"PRAGMA table_info({table})")}
            assert column in cols, f"{table}.{column} was dropped"
            assert cols[column] == 0, f"{table}.{column} still NOT NULL"
    finally:
        conn.close()


def test_013_is_idempotent(db_copy):
    MIG013.run_migration(str(db_copy))
    second = MIG013.run_migration(str(db_copy))
    assert second["rebuilt"] == []
    assert len(second["already_nullable"]) == len(MIG013.TARGETS)


def test_013_is_wired():
    src = (REPO_ROOT / "engine/core/database.py").read_text()
    assert "013_drop_schema_hash_not_null" in src
