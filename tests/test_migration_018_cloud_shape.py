"""Migration 018 — cloud tables to the fresh shape; `audit_adjudication` dropped.

C10 (NOT NULL on `confidence`/`tier`), R16 (no `UNIQUE(paper_id, arm)`) and R32
(the table goes, and so does its provisioning) are one change because they are
one structural gap: after 018 a fresh database and the live one differ in nothing
structural, which is what makes G3 an acceptance gate of the Phase 3 live write.

Every gate runs on a CONSTRUCTED fixture built from the shape that is actually on
live — nullable cloud span columns, a `UNIQUE(paper_id, arm)`, and an
`audit_adjudication` whose `span_id` points at the phantom `_evidence_spans_old`.
A fixture built from the *fresh* shape would have nothing for 018 to do.
"""

from __future__ import annotations

import importlib
import sqlite3

import pytest

m018 = importlib.import_module("engine.migrations.018_cloud_shape_and_audit_adjudication")


# ── the pre-018 (live-shaped) fixture ─────────────────────────────────

PRE_018 = """
CREATE TABLE papers (id INTEGER PRIMARY KEY, title TEXT NOT NULL);
CREATE TABLE evidence_spans (id INTEGER PRIMARY KEY);

CREATE TABLE cloud_extractions (
    id                      INTEGER PRIMARY KEY,
    paper_id                INTEGER NOT NULL REFERENCES papers(id),
    arm                     TEXT NOT NULL,
    model_string            TEXT NOT NULL,
    extracted_data          TEXT,
    reasoning_trace         TEXT,
    prompt_text             TEXT,
    input_tokens            INTEGER,
    output_tokens           INTEGER,
    reasoning_tokens        INTEGER,
    cost_usd                REAL,
    extraction_schema_hash  TEXT,
    extracted_at            TEXT NOT NULL, codebook_hash TEXT, codebook_sha256 TEXT,
    UNIQUE(paper_id, arm)
);

CREATE TABLE cloud_evidence_spans (
    id                      INTEGER PRIMARY KEY,
    cloud_extraction_id     INTEGER NOT NULL REFERENCES cloud_extractions(id),
    field_name              TEXT NOT NULL,
    value                   TEXT,
    source_snippet          TEXT,
    confidence              REAL,
    tier                    INTEGER, notes TEXT,
    UNIQUE(cloud_extraction_id, field_name)
);

-- exactly the live shape: the FK points at a table that does not exist (A11)
CREATE TABLE audit_adjudication (
    id       INTEGER PRIMARY KEY,
    span_id  INTEGER REFERENCES "_evidence_spans_old"(id),
    paper_id INTEGER REFERENCES papers(id),
    field_name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


def _pre_018_db(tmp_path, nulls=False):
    path = tmp_path / "pre018.db"
    conn = sqlite3.connect(path)
    conn.executescript(PRE_018)
    conn.execute("INSERT INTO papers (id, title) VALUES (1, 't'), (2, 'u')")
    conn.execute(
        "INSERT INTO cloud_extractions (id, paper_id, arm, model_string, extracted_at) "
        "VALUES (1, 1, 'openai_o4_mini_high', 'm', '2026-01-01'), "
        "       (2, 2, 'anthropic_sonnet_4_6', 'm', '2026-01-01')")
    conf, tier = (None, None) if nulls else (0.9, 1)
    conn.execute(
        "INSERT INTO cloud_evidence_spans "
        "(id, cloud_extraction_id, field_name, value, source_snippet, confidence, tier, notes) "
        "VALUES (1, 1, 'study_type', 'RCT', 's', ?, ?, NULL), "
        "       (2, 2, 'sample_size', '51', 's2', 0.8, 2, 'n')", (conf, tier))
    conn.commit()
    conn.close()
    return path


def _info(path, table):
    conn = sqlite3.connect(path)
    try:
        return {r[1]: r for r in conn.execute(f"PRAGMA table_info({table})")}
    finally:
        conn.close()


# ── T2 / G-set: the three postconditions ─────────────────────────────

def test_confidence_and_tier_become_not_null(tmp_path):
    path = _pre_018_db(tmp_path)
    assert _info(path, "cloud_evidence_spans")["confidence"][3] == 0
    m018.run_migration(str(path))
    info = _info(path, "cloud_evidence_spans")
    assert info["confidence"][3] == 1
    assert info["tier"][3] == 1


def test_the_unique_paper_arm_constraint_is_gone(tmp_path):
    path = _pre_018_db(tmp_path)
    conn = sqlite3.connect(path)
    assert m018._has_unique_paper_arm(conn)
    conn.close()

    m018.run_migration(str(path))

    conn = sqlite3.connect(path)
    try:
        assert not m018._has_unique_paper_arm(conn)
        # and a second claim by the same arm on the same paper is now WRITABLE,
        # which is the point of R16 — supersession within an arm (S3d)
        conn.execute(
            "INSERT INTO cloud_extractions (id, paper_id, arm, model_string, extracted_at) "
            "VALUES (3, 1, 'openai_o4_mini_high', 'm', '2026-02-02')")
        conn.commit()
    finally:
        conn.close()


def test_audit_adjudication_is_dropped(tmp_path):
    path = _pre_018_db(tmp_path)
    m018.run_migration(str(path))
    conn = sqlite3.connect(path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
            "AND name='audit_adjudication'").fetchone()[0] == 0
        # A11 is closed: no object anywhere still references the phantom
        refs = [r[0] for r in conn.execute(
            "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL")]
        assert not any("_evidence_spans_old" in s for s in refs)
    finally:
        conn.close()


# ── no row content changed ───────────────────────────────────────────

def test_no_row_content_changes(tmp_path):
    """R27's contract, checked the way `db_fingerprint` checks it: the same
    columns, in the same PRAGMA order, over the same rowids, with the same
    values."""
    path = _pre_018_db(tmp_path)

    def snapshot():
        conn = sqlite3.connect(path)
        try:
            out = {}
            for t, cols in (("cloud_evidence_spans", m018.CLOUD_EVIDENCE_SPANS_COLUMNS),
                            ("cloud_extractions", m018.CLOUD_EXTRACTIONS_COLUMNS)):
                order = [r[1] for r in conn.execute(f"PRAGMA table_info({t})")]
                assert order == list(cols), f"{t} column order moved"
                sel = ", ".join(f'"{c}"' for c in cols)
                out[t] = conn.execute(
                    f"SELECT {sel} FROM {t} ORDER BY rowid").fetchall()
            return out
        finally:
            conn.close()

    before = snapshot()
    result = m018.run_migration(str(path))
    assert result["spans"] == 2 and result["extractions"] == 2
    assert result["audit_adjudication_dropped"] is True
    assert snapshot() == before


def test_the_unique_on_extraction_and_field_survives(tmp_path):
    path = _pre_018_db(tmp_path)
    m018.run_migration(str(path))
    conn = sqlite3.connect(path)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO cloud_evidence_spans "
                "(cloud_extraction_id, field_name, confidence, tier) "
                "VALUES (1, 'study_type', 0.5, 1)")
    finally:
        conn.close()


# ── refusals and idempotence ─────────────────────────────────────────

def test_a_null_confidence_stops_the_migration_rather_than_being_backfilled(tmp_path):
    """`init_cloud_tables` would have written confidence=0.0 and tier=1 —
    inventing a confidence nobody measured. R27 says no row content changed."""
    path = _pre_018_db(tmp_path, nulls=True)
    with pytest.raises(RuntimeError, match="will not invent one"):
        m018.run_migration(str(path))
    # nothing moved
    assert _info(path, "cloud_evidence_spans")["confidence"][3] == 0
    conn = sqlite3.connect(path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='audit_adjudication'"
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_a_mid_rebuild_failure_leaves_everything_as_it_was(tmp_path):
    path = _pre_018_db(tmp_path)
    with pytest.raises(RuntimeError, match="forced mid-rebuild failure"):
        m018.run_migration(str(path), _fail_after_copy=True)
    conn = sqlite3.connect(path)
    try:
        assert m018._has_unique_paper_arm(conn)
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='audit_adjudication'"
        ).fetchone()[0] == 1
        assert not [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE name LIKE '%__018'")]
    finally:
        conn.close()
    assert _info(path, "cloud_evidence_spans")["confidence"][3] == 0


def test_the_migration_is_idempotent_by_postcondition(tmp_path):
    path = _pre_018_db(tmp_path)
    assert m018.run_migration(str(path))["status"] == "executed"
    assert m018.run_migration(str(path))["status"] == "already_applied"


# ── the fresh database ───────────────────────────────────────────────

def test_a_fresh_review_database_reaches_the_post_018_shape(tmp_path):
    from engine.core.database import ReviewDatabase

    db = ReviewDatabase(str(tmp_path / "rev"))
    try:
        conn = db._conn
        assert "018_cloud_shape_and_audit_adjudication" in {
            r[0] for r in conn.execute("SELECT migration_id FROM schema_migrations")}
        assert not m018._has_unique_paper_arm(conn)
        assert m018._notnull(conn, "cloud_evidence_spans", "confidence")
        assert m018._notnull(conn, "cloud_evidence_spans", "tier")
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
            "AND name='audit_adjudication'").fetchone()[0] == 0
    finally:
        db.close()


def test_audit_adjudication_does_not_come_back_across_two_constructions(tmp_path):
    """The R32 census reproducer, promoted to a test.

    A DROP alone did not hold: `ensure_adjudication_table` recreated the table on
    every open (measured: present 1 -> 0 -> 1). This is the regression test for
    that, and it fails the moment anyone re-adds the DDL.
    """
    from engine.core.database import ReviewDatabase

    root = str(tmp_path / "rev")
    for construction in (1, 2):
        db = ReviewDatabase(root)
        try:
            assert db._conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
                "AND name='audit_adjudication'").fetchone()[0] == 0, (
                f"audit_adjudication reappeared at construction {construction}")
        finally:
            db.close()


def test_the_other_adjudication_tables_are_still_provisioned(tmp_path):
    """I12: removing one DDL string must not remove the other three."""
    from engine.core.database import ReviewDatabase

    db = ReviewDatabase(str(tmp_path / "rev"))
    try:
        names = {r[0] for r in db._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"abstract_screening_adjudication", "ft_screening_adjudication",
                "workflow_state"} <= names
    finally:
        db.close()
