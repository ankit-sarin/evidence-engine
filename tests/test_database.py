"""Tests for the SQLite review database."""

import json
import sqlite3

import pytest

from engine.core.database import (
    RETIRED_TOKENS, SCREENING_TOKENS, STATUSES, ReviewDatabase, RetiredTransition,
)
from engine.search.models import Citation


@pytest.fixture()
def db(tmp_path):
    """Create a fresh ReviewDatabase in a temp directory."""
    rdb = ReviewDatabase("test_review", data_root=tmp_path)
    yield rdb
    rdb.close()


def _cit(**kw):
    defaults = dict(title="Study A", source="pubmed", pmid="111", doi="10.1/a")
    defaults.update(kw)
    return Citation(**defaults)


# ── Table Creation ───────────────────────────────────────────────────


def test_tables_exist(db):
    tables = {
        r[0]
        for r in db._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    expected = {
        "papers",
        "abstract_screening_decisions",
        "full_text_assets",
        "extractions",
        "evidence_spans",
        "review_runs",
    }
    assert expected.issubset(tables)


def test_directories_created(db, tmp_path):
    base = tmp_path / "test_review"
    assert (base / "pdfs").is_dir()
    assert (base / "parsed_text").is_dir()
    assert (base / "vector_store").is_dir()


def test_wal_mode(db):
    mode = db._conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"


# ── Add Papers & Dedup ───────────────────────────────────────────────


def test_add_papers(db):
    cits = [_cit(pmid=str(i), title=f"Study {i}") for i in range(5)]
    added = db.add_papers(cits)
    assert added == 5


def test_no_duplicates_on_readd(db):
    cits = [_cit(pmid="99", title="Dup Study")]
    db.add_papers(cits)
    added_again = db.add_papers(cits)
    assert added_again == 0

    rows = db.get_papers_by_status("INGESTED")
    pmid_99 = [r for r in rows if r["pmid"] == "99"]
    assert len(pmid_99) == 1


# ── Full Lifecycle Walk ──────────────────────────────────────────────


def test_full_lifecycle(db):
    db.add_papers([_cit(pmid="LC1", title="Lifecycle Paper")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    # INGESTED → ABSTRACT_SCREENED_IN
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    assert db.get_papers_by_status("ABSTRACT_SCREENED_IN")[0]["id"] == pid

    # ABSTRACT_SCREENED_IN → PDF_ACQUIRED
    db.update_status(pid, "PDF_ACQUIRED")

    # PDF_ACQUIRED → PARSED
    db.update_status(pid, "PARSED")

    # PARSED → FT_ELIGIBLE
    db.update_status(pid, "FT_ELIGIBLE")
    assert db.get_papers_by_status("FT_ELIGIBLE")[0]["id"] == pid

    # B5 (R160b, R200 row 33): the forward walk ends at FT_ELIGIBLE; the
    # extraction stage is the event store's, so →EXTRACTED is refused by name.
    with pytest.raises(RetiredTransition, match="EXTRACTED"):
        db.update_status(pid, "EXTRACTED")
    assert db.get_papers_by_status("FT_ELIGIBLE")[0]["id"] == pid


def test_screened_out_lifecycle(db):
    db.add_papers([_cit(pmid="SO1", title="Screened Out")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "ABSTRACT_SCREENED_OUT")
    assert db.get_papers_by_status("ABSTRACT_SCREENED_OUT")[0]["id"] == pid


def test_flagged_then_resolved(db):
    db.add_papers([_cit(pmid="FL1", title="Flagged")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "ABSTRACT_SCREEN_FLAGGED")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    assert db.get_papers_by_status("ABSTRACT_SCREENED_IN")[0]["id"] == pid


# ── Invalid Transitions ─────────────────────────────────────────────


def test_invalid_transition_raises(db):
    db.add_papers([_cit(pmid="IT1", title="Bad Transition")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    # R160b rewrite (R200 row 35): a retired target is refused by name.
    with pytest.raises(RetiredTransition, match="INGESTED → EXTRACTED"):
        db.update_status(pid, "EXTRACTED")


def test_invalid_status_raises(db):
    db.add_papers([_cit(pmid="IS1", title="Invalid Status")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    with pytest.raises(ValueError, match="Invalid status"):
        db.update_status(pid, "NONEXISTENT")


def test_screened_out_is_terminal(db):
    db.add_papers([_cit(pmid="T1", title="Terminal")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "ABSTRACT_SCREENED_OUT")
    with pytest.raises(ValueError, match="Invalid transition"):
        db.update_status(pid, "ABSTRACT_SCREENED_IN")


# ── Screening Decisions ─────────────────────────────────────────────


def test_abstract_screening_decisions(db):
    db.add_papers([_cit(pmid="SD1", title="Screen Me")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    d1 = db.add_screening_decision(pid, 1, "include", "Relevant study", "qwen3:8b")
    d2 = db.add_screening_decision(pid, 2, "include", "Confirmed relevant", "qwen3:8b")
    assert d1 > 0
    assert d2 > d1

    rows = db._conn.execute(
        "SELECT * FROM abstract_screening_decisions WHERE paper_id = ?", (pid,)
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["pass_number"] == 1
    assert rows[1]["pass_number"] == 2


# ── Staleness Detection ─────────────────────────────────────────────


# ── Evidence Spans & Audit ───────────────────────────────────────────


# ── Atomic Extraction ─────────────────────────────────────────────────


# ── Reset for Re-Audit ──────────────────────────────────────────────


# ── Reject Paper ────────────────────────────────────────────────────


# ── Min Status Gate ─────────────────────────────────────────────────


# ── Pipeline Stats ───────────────────────────────────────────────────


def test_pipeline_stats(db):
    """R165 (9c-C4, B5): the end-of-run counts come from the event reader.

    Both axes through `effective_state`; processing counted over ELIGIBLE papers
    only; the screening summary as a sub-dict; no extraction or span totals."""
    from engine.core import events
    from _event_store_fixture import fixture_run, seed_eligibility

    cits = [_cit(pmid=str(i), title=f"Stat {i}") for i in range(10)]
    db.add_papers(cits)
    papers = db.get_papers_by_status("INGESTED")
    for p in papers[:3]:
        db.update_status(p["id"], "ABSTRACT_SCREENED_IN")
    for p in papers[3:5]:
        db.update_status(p["id"], "ABSTRACT_SCREENED_OUT")

    conn = db._conn
    ids = [p["id"] for p in papers]
    run_id = fixture_run(conn)

    def processing(pid, event_type, to_state, reason_code=None):
        events.write_paper_event(
            conn, event_type=event_type, paper_id=pid, to_state=to_state,
            actor_kind="engine", actor_role="system", actor_name="fixture",
            reason_code=reason_code, run_id=run_id)

    for pid in ids[:4]:
        seed_eligibility(conn, pid)                               # 4 eligible
    seed_eligibility(conn, ids[4], to_state="abstract_out")       # 1 out
    processing(ids[0], "extracted", "extracted")
    processing(ids[1], "audited", "audited_ai")
    processing(ids[2], "extraction_failed", "extraction_failed", "no_fields_returned")
    # ids[3] eligible with no processing event; ids[5] processed but NOT eligible
    processing(ids[5], "extracted", "extracted")

    stats = db.get_pipeline_stats()
    assert set(stats) == {"total_papers", "screening", "eligibility", "processing",
                          "processing_failures", "analysis_ready"}
    assert stats["total_papers"] == 10
    # B5 (H5, R194): the nine screening tokens, zeros included, plus `eligible`
    # from the eligibility axis.
    assert stats["screening"] == {
        "INGESTED": 5, "ABSTRACT_SCREENED_IN": 3, "ABSTRACT_SCREEN_FLAGGED": 0,
        "ABSTRACT_SCREENED_OUT": 2, "PDF_ACQUIRED": 0, "PDF_EXCLUDED": 0,
        "PARSED": 0, "FT_FLAGGED": 0, "FT_SCREENED_OUT": 0, "eligible": 4}
    assert stats["eligibility"] == {"eligible": 4, "abstract_out": 1,
                                    "no_recorded_state": 5}
    # ids[5]'s `extracted` is not counted: it is not in the corpus.
    assert stats["processing"] == {"extracted": 1, "audited_ai": 1,
                                   "extraction_failed": 1, "no_recorded_state": 1}
    assert stats["processing_failures"] == {"no_fields_returned": 1}
    assert stats["analysis_ready"] == 2


def test_screening_summary_reports_screening_tokens_and_the_corpus(db):
    """H5 (R194): a paper at AI_AUDIT_COMPLETE on status and eligible on events
    is counted once, as `eligible`; the extraction token is not a key."""
    from engine.core.database import SCREENING_TOKENS
    from _event_store_fixture import seed_eligibility

    db.add_papers([_cit(pmid="1", title="Included"), _cit(pmid="2", title="Out")])
    included, out = [p["id"] for p in db.get_papers_by_status("INGESTED")]
    # Raw SQL on papers.status: the frozen token a Run-6 paper carries.
    db._conn.execute("UPDATE papers SET status = 'AI_AUDIT_COMPLETE' WHERE id = ?",
                     (included,))
    db.update_status(out, "ABSTRACT_SCREENED_OUT")
    seed_eligibility(db._conn, included)

    summary = db.get_screening_summary()
    assert set(summary) == set(SCREENING_TOKENS) | {"eligible"}
    assert "AI_AUDIT_COMPLETE" not in summary
    assert summary["eligible"] == 1
    assert summary["ABSTRACT_SCREENED_OUT"] == 1
    assert sum(v for k, v in summary.items() if k != "eligible") == 1


def test_screening_tokens_have_one_home():
    """PRISMA and the screening summary read the same set, by identity."""
    from engine.core import database
    from engine.exporters import prisma
    assert prisma.SCREENING_TOKENS is database.SCREENING_TOKENS
    assert len(database.SCREENING_TOKENS) == 9


def test_statuses_partition_into_screening_retired_and_ft_eligible():
    """R197: every status is a screening token, a retired token, or FT_ELIGIBLE
    (the corpus status with no processing), and the three are disjoint."""
    corpus = {"FT_ELIGIBLE"}
    assert set(STATUSES) == SCREENING_TOKENS | RETIRED_TOKENS | corpus
    assert not SCREENING_TOKENS & RETIRED_TOKENS
    assert not SCREENING_TOKENS & corpus
    assert not RETIRED_TOKENS & corpus


# ── Reset for Re-Extraction ──────────────────────────────────────────


# ── Cleanup Orphaned Spans ────────────────────────────────────────────


# ── Context Manager ──────────────────────────────────────────────────


def test_context_manager_opens_and_closes(tmp_path):
    """ReviewDatabase works as a context manager; connection open inside, closed after."""
    with ReviewDatabase("ctx_test", data_root=tmp_path) as db:
        # Connection should be live inside the block
        row = db._conn.execute("SELECT 1").fetchone()
        assert row[0] == 1
        # Basic operation works
        db.add_papers([_cit(pmid="CM1", title="Context Mgr")])
        assert len(db.get_papers_by_status("INGESTED")) == 1

    # After exiting, _conn should be None (closed)
    assert db._conn is None


def test_context_manager_closes_on_exception(tmp_path):
    """Connection is closed even when an exception is raised inside the with block."""
    with pytest.raises(RuntimeError, match="boom"):
        with ReviewDatabase("ctx_exc", data_root=tmp_path) as db:
            db.add_papers([_cit(pmid="CE1", title="Exception")])
            raise RuntimeError("boom")

    assert db._conn is None


def test_close_is_idempotent(tmp_path):
    """Calling .close() twice does not raise."""
    db = ReviewDatabase("ctx_idem", data_root=tmp_path)
    db.add_papers([_cit(pmid="CI1", title="Idempotent")])
    db.close()
    db.close()  # Second call should be a no-op
    assert db._conn is None


def test_manual_usage_still_works(tmp_path):
    """Existing non-context-manager usage (db = ...; db.close()) works identically."""
    db = ReviewDatabase("ctx_manual", data_root=tmp_path)
    db.add_papers([_cit(pmid="MU1", title="Manual")])
    papers = db.get_papers_by_status("INGESTED")
    assert len(papers) == 1
    assert papers[0]["title"] == "Manual"
    db.close()
    assert db._conn is None


# ── Migration Error Filtering ───────────────────────────────────────


def test_migration_existing_column_succeeds_silently(tmp_path):
    """Adding an already-existing column is silently ignored (idempotent)."""
    import sqlite3

    # First creation adds all columns via migrations
    db1 = ReviewDatabase("mig_test", data_root=tmp_path)
    db1.close()

    # Second creation re-runs migrations — should not raise
    db2 = ReviewDatabase("mig_test", data_root=tmp_path)
    # Verify the DB is functional
    db2.add_papers([_cit(pmid="M1", title="Migration OK")])
    assert len(db2.get_papers_by_status("INGESTED")) == 1
    db2.close()


def test_migration_syntax_error_raises(tmp_path):
    """A migration with a syntax error raises OperationalError instead of being swallowed."""
    import sqlite3
    from engine.core import database as db_mod

    # Create a valid DB first
    db = ReviewDatabase("mig_err", data_root=tmp_path)
    db.close()

    # Patch _SIMPLE_MIGRATIONS to include a bad SQL statement
    bad_migrations = ["CREAT TABLE bad_syntax (id INTEGER PRIMARY KEY)"]
    original = db_mod._SIMPLE_MIGRATIONS

    try:
        db_mod._SIMPLE_MIGRATIONS = bad_migrations
        with pytest.raises(sqlite3.OperationalError):
            ReviewDatabase("mig_err", data_root=tmp_path)
    finally:
        db_mod._SIMPLE_MIGRATIONS = original


# ── Admin Reset ──────────────────────────────────────────────────────


def test_normal_pipeline_cannot_use_admin_transition(tmp_path):
    """update_status still rejects AI_AUDIT_COMPLETE → PARSED — now by name,
    because the source is a retired token (R160b rewrite, R200 row 36)."""
    db = ReviewDatabase("admin_guard", data_root=tmp_path)
    db.add_papers([_cit(pmid="AG1", title="Guard")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    # Raw SQL on papers.status: retires at the screeners' cut-over (R163 precedent).
    db._conn.execute("UPDATE papers SET status = 'AI_AUDIT_COMPLETE' WHERE id = ?", (pid,))
    db._conn.commit()

    with pytest.raises(RetiredTransition, match="AI_AUDIT_COMPLETE → PARSED"):
        db.update_status(pid, "PARSED")
    assert db.get_papers_by_status("AI_AUDIT_COMPLETE")[0]["id"] == pid
    db.close()


# ── L3: NOT NULL constraints ──────────────────────────────────────────


def test_cloud_null_confidence_raises_integrity_error(tmp_path):
    """L3: cloud_evidence_spans rejects NULL confidence."""
    from engine.cloud.schema import init_cloud_tables

    db = ReviewDatabase("test_cloud_l3", data_root=tmp_path)
    init_cloud_tables(str(db.db_path))

    # Need a paper for the FK
    db.add_papers([_cit(pmid="CL3_1", title="Cloud L3")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]

    ce_id = db._conn.execute(
        """INSERT INTO cloud_extractions
           (paper_id, arm, model_string, extracted_at)
           VALUES (?, 'test_arm', 'model', '2026-01-01')""",
        (pid,),
    ).lastrowid
    db._conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        db._conn.execute(
            """INSERT INTO cloud_evidence_spans
               (cloud_extraction_id, field_name, value, confidence, tier)
               VALUES (?, 'f', 'v', NULL, 1)""",
            (ce_id,),
        )
    db.close()


def test_cloud_null_tier_raises_integrity_error(tmp_path):
    """L3: cloud_evidence_spans rejects NULL tier."""
    from engine.cloud.schema import init_cloud_tables

    db = ReviewDatabase("test_cloud_l3b", data_root=tmp_path)
    init_cloud_tables(str(db.db_path))

    db.add_papers([_cit(pmid="CL3_2", title="Cloud L3b")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]

    ce_id = db._conn.execute(
        """INSERT INTO cloud_extractions
           (paper_id, arm, model_string, extracted_at)
           VALUES (?, 'test_arm', 'model', '2026-01-01')""",
        (pid,),
    ).lastrowid
    db._conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        db._conn.execute(
            """INSERT INTO cloud_evidence_spans
               (cloud_extraction_id, field_name, value, confidence, tier)
               VALUES (?, 'f', 'v', 0.9, NULL)""",
            (ce_id,),
        )
    db.close()


# ── Atomic update_status ──────────────────────────────────────────────


def test_update_status_atomic_valid_transition(tmp_path):
    """update_status commits atomically on a valid transition."""
    db = ReviewDatabase("atomic_ok", data_root=tmp_path)
    db.add_papers([_cit(pmid="AO1", title="Atomic")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    db.update_status(pid, "ABSTRACT_SCREENED_IN")

    # Verify committed — reopen DB and check
    db.close()
    db2 = ReviewDatabase("atomic_ok", data_root=tmp_path)
    assert db2.get_papers_by_status("ABSTRACT_SCREENED_IN")[0]["id"] == pid
    db2.close()


def test_update_status_invalid_transition_still_raises(tmp_path):
    """update_status still raises ValueError on invalid transitions (existing behavior)."""
    db = ReviewDatabase("atomic_err", data_root=tmp_path)
    db.add_papers([_cit(pmid="AE1", title="Invalid")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]

    # R160b rewrite (R200 row 37): refused by name; the row is unchanged.
    with pytest.raises(RetiredTransition, match="successor: engine.core.events"):
        db.update_status(pid, "EXTRACTED")

    # Paper should still be INGESTED
    assert db.get_papers_by_status("INGESTED")[0]["id"] == pid
    db.close()
