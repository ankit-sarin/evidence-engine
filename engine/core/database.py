"""SQLite database manager — one database per review, full provenance.

RETENTION POLICY: All fetched paper data (metadata, abstract, screening
traces, verification traces) is retained permanently regardless of
screening outcome. ABSTRACT_SCREENED_OUT is a label, not a deletion.
The database is the single source of truth for all papers ever evaluated.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from engine.search.models import Citation
from engine.utils.db_backup import auto_backup

logger = logging.getLogger(__name__)

DATA_ROOT = Path("data")

# ── Paper Lifecycle ──────────────────────────────────────────────────

STATUSES = (
    "INGESTED",
    "ABSTRACT_SCREENED_IN",
    "ABSTRACT_SCREENED_OUT",
    "ABSTRACT_SCREEN_FLAGGED",
    "PDF_ACQUIRED",
    "PDF_EXCLUDED",
    "PARSED",
    # Full-text screening statuses
    "FT_ELIGIBLE",
    "FT_SCREENED_OUT",
    "FT_FLAGGED",
    "EXTRACT_FAILED",
    "EXTRACTED",
    "AI_AUDIT_COMPLETE",
    "HUMAN_AUDIT_COMPLETE",
    "REJECTED",
)

ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "INGESTED": {"ABSTRACT_SCREENED_IN", "ABSTRACT_SCREENED_OUT", "ABSTRACT_SCREEN_FLAGGED"},
    "ABSTRACT_SCREENED_IN": {"PDF_ACQUIRED", "ABSTRACT_SCREEN_FLAGGED"},
    "ABSTRACT_SCREEN_FLAGGED": {"ABSTRACT_SCREENED_IN", "ABSTRACT_SCREENED_OUT"},
    "PDF_ACQUIRED": {"PARSED", "PDF_EXCLUDED"},
    "PDF_EXCLUDED": set(),  # Terminal — papers here do not advance
    "PARSED": {"FT_ELIGIBLE", "FT_SCREENED_OUT", "FT_FLAGGED", "EXTRACTED", "EXTRACT_FAILED"},
    "FT_ELIGIBLE": {"EXTRACTED", "EXTRACT_FAILED", "FT_FLAGGED"},
    "FT_FLAGGED": {"FT_ELIGIBLE", "FT_SCREENED_OUT"},
    "EXTRACT_FAILED": {"PARSED", "FT_ELIGIBLE", "EXTRACTED"},
    "EXTRACTED": {"AI_AUDIT_COMPLETE"},
    "AI_AUDIT_COMPLETE": {"HUMAN_AUDIT_COMPLETE", "REJECTED"},
    # Terminal states with no forward transitions
    "ABSTRACT_SCREENED_OUT": set(),
    "FT_SCREENED_OUT": set(),
    "HUMAN_AUDIT_COMPLETE": {"REJECTED"},
    "REJECTED": set(),
}

# Ordered status levels for min_status_gate comparisons
_STATUS_ORDER = {
    "PARSED": 0,
    "ABSTRACT_SCREENED_OUT": 1,
    "EXTRACTED": 2,
    "AI_AUDIT_COMPLETE": 3,
    "HUMAN_AUDIT_COMPLETE": 4,
}

# ── Schema DDL ───────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS papers (
    id              INTEGER PRIMARY KEY,
    pmid            TEXT UNIQUE,
    doi             TEXT,
    title           TEXT NOT NULL,
    abstract        TEXT,
    authors         TEXT,          -- JSON array
    journal         TEXT,
    year            INTEGER,
    source          TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'INGESTED',
    rejected_reason TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_papers_status ON papers(status);
CREATE INDEX IF NOT EXISTS idx_papers_doi    ON papers(doi);

CREATE TABLE IF NOT EXISTS abstract_screening_decisions (
    id              INTEGER PRIMARY KEY,
    paper_id        INTEGER NOT NULL REFERENCES papers(id),
    pass_number     INTEGER NOT NULL CHECK (pass_number IN (1, 2)),
    decision        TEXT NOT NULL CHECK (decision IN ('include', 'exclude', 'uncertain')),
    rationale       TEXT,
    model           TEXT,
    decided_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_abstract_screening_paper ON abstract_screening_decisions(paper_id);

CREATE TABLE IF NOT EXISTS abstract_verification_decisions (
    id              INTEGER PRIMARY KEY,
    paper_id        INTEGER NOT NULL REFERENCES papers(id),
    decision        TEXT NOT NULL CHECK (decision IN ('include', 'exclude')),
    rationale       TEXT,
    model           TEXT,
    decided_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_abstract_verification_paper ON abstract_verification_decisions(paper_id);

CREATE TABLE IF NOT EXISTS full_text_assets (
    id                  INTEGER PRIMARY KEY,
    paper_id            INTEGER NOT NULL REFERENCES papers(id),
    pdf_path            TEXT,
    pdf_hash            TEXT,
    parsed_text_path    TEXT,
    parsed_text_version INTEGER NOT NULL DEFAULT 1,
    parser_used         TEXT,
    parsed_at           TEXT
);

CREATE TABLE IF NOT EXISTS extractions (
    id                      INTEGER PRIMARY KEY,
    paper_id                INTEGER NOT NULL REFERENCES papers(id),
    extraction_schema_hash  TEXT NOT NULL,
    extracted_data          TEXT NOT NULL,  -- JSON
    reasoning_trace         TEXT,
    model                   TEXT,
    model_digest            TEXT,
    auditor_model_digest    TEXT,
    low_yield               INTEGER NOT NULL DEFAULT 0,  -- boolean: 1 if below threshold
    extracted_at            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS evidence_spans (
    id              INTEGER PRIMARY KEY,
    extraction_id   INTEGER NOT NULL REFERENCES extractions(id),
    field_name      TEXT NOT NULL,
    value           TEXT NOT NULL,
    source_snippet  TEXT,
    confidence      REAL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    audit_status    TEXT NOT NULL DEFAULT 'pending'
                    CHECK (audit_status IN (
                        'pending', 'verified', 'contested',
                        'flagged', 'invalid_snippet'
                    )),
    auditor_model   TEXT,
    audit_rationale TEXT,
    audited_at      TEXT
);

CREATE INDEX IF NOT EXISTS idx_spans_extraction ON evidence_spans(extraction_id);

CREATE TABLE IF NOT EXISTS ft_screening_decisions (
    id              INTEGER PRIMARY KEY,
    paper_id        INTEGER NOT NULL REFERENCES papers(id),
    model           TEXT NOT NULL,
    decision        TEXT NOT NULL CHECK (decision IN ('FT_ELIGIBLE', 'FT_EXCLUDE')),
    reason_code     TEXT NOT NULL,
    rationale       TEXT,
    confidence      REAL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    decided_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ft_screening_paper ON ft_screening_decisions(paper_id);

CREATE TABLE IF NOT EXISTS ft_verification_decisions (
    id              INTEGER PRIMARY KEY,
    paper_id        INTEGER NOT NULL REFERENCES papers(id),
    model           TEXT NOT NULL,
    decision        TEXT NOT NULL CHECK (decision IN ('FT_ELIGIBLE', 'FT_FLAGGED')),
    rationale       TEXT,
    confidence      REAL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    decided_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ft_verification_paper ON ft_verification_decisions(paper_id);

CREATE TABLE IF NOT EXISTS review_runs (
    id                  INTEGER PRIMARY KEY,
    review_spec_hash    TEXT NOT NULL,
    screening_hash      TEXT NOT NULL,
    extraction_hash     TEXT NOT NULL,
    started_at          TEXT NOT NULL,
    completed_at        TEXT,
    status              TEXT NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'completed', 'failed')),
    log                 TEXT NOT NULL DEFAULT '[]'  -- JSON array of events
);
"""

# Migrations for existing databases
_VERIFICATION_TABLE = """
CREATE TABLE IF NOT EXISTS abstract_verification_decisions (
    id              INTEGER PRIMARY KEY,
    paper_id        INTEGER NOT NULL REFERENCES papers(id),
    decision        TEXT NOT NULL CHECK (decision IN ('include', 'exclude')),
    rationale       TEXT,
    model           TEXT,
    decided_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_abstract_verification_paper ON abstract_verification_decisions(paper_id);
"""

_SIMPLE_MIGRATIONS = [
    "ALTER TABLE papers ADD COLUMN rejected_reason TEXT",
    "ALTER TABLE papers ADD COLUMN ee_identifier TEXT",
    "ALTER TABLE papers ADD COLUMN oa_status TEXT",
    "ALTER TABLE papers ADD COLUMN pdf_url TEXT",
    "ALTER TABLE papers ADD COLUMN download_status TEXT DEFAULT 'pending' CHECK (download_status IN ('pending', 'success', 'failed', 'manual'))",
    "ALTER TABLE papers ADD COLUMN pdf_local_path TEXT",
    "ALTER TABLE papers ADD COLUMN acquisition_date TEXT",
    "ALTER TABLE extractions ADD COLUMN low_yield INTEGER NOT NULL DEFAULT 0",
    # Migration 004: PDF quality check columns
    "ALTER TABLE papers ADD COLUMN pdf_exclusion_reason TEXT",
    "ALTER TABLE papers ADD COLUMN pdf_exclusion_detail TEXT",
    "ALTER TABLE papers ADD COLUMN pdf_quality_check_status TEXT",
    "ALTER TABLE papers ADD COLUMN pdf_ai_language TEXT",
    "ALTER TABLE papers ADD COLUMN pdf_ai_content_type TEXT",
    "ALTER TABLE papers ADD COLUMN pdf_ai_confidence REAL",
    # Migration 005: model digest columns
    "ALTER TABLE extractions ADD COLUMN model_digest TEXT",
    "ALTER TABLE extractions ADD COLUMN auditor_model_digest TEXT",
    # Migration 006: PDF content hash on papers table
    "ALTER TABLE papers ADD COLUMN pdf_content_hash TEXT",
]

_EVIDENCE_SPANS_REBUILD = """
-- Rebuild evidence_spans to update CHECK constraint for new audit states
ALTER TABLE evidence_spans RENAME TO _evidence_spans_old;

CREATE TABLE evidence_spans (
    id              INTEGER PRIMARY KEY,
    extraction_id   INTEGER NOT NULL REFERENCES extractions(id),
    field_name      TEXT NOT NULL,
    value           TEXT NOT NULL,
    source_snippet  TEXT,
    confidence      REAL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    audit_status    TEXT NOT NULL DEFAULT 'pending'
                    CHECK (audit_status IN (
                        'pending', 'verified', 'contested',
                        'flagged', 'invalid_snippet'
                    )),
    auditor_model   TEXT,
    audit_rationale TEXT,
    audited_at      TEXT
);

INSERT INTO evidence_spans
    SELECT * FROM _evidence_spans_old;

DROP TABLE _evidence_spans_old;

CREATE INDEX IF NOT EXISTS idx_spans_extraction ON evidence_spans(extraction_id);
"""


# ── ReviewDatabase ───────────────────────────────────────────────────


class ReviewDatabase:
    """SQLite state machine for a single systematic review."""

    def __init__(self, review_name: str, data_root: Path | None = None):
        root = (data_root or DATA_ROOT) / review_name
        root.mkdir(parents=True, exist_ok=True)
        (root / "pdfs").mkdir(exist_ok=True)
        (root / "parsed_text").mkdir(exist_ok=True)
        (root / "vector_store").mkdir(exist_ok=True)

        self.db_path = root / "review.db"
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._run_migrations()

    def _run_migrations(self) -> None:
        """Apply schema migrations, skipping those already applied."""
        # Ensure adjudication table exists
        from engine.adjudication.schema import ensure_adjudication_table
        ensure_adjudication_table(self._conn)

        for sql in _SIMPLE_MIGRATIONS:
            try:
                self._conn.execute(sql)
                self._conn.commit()
            except sqlite3.OperationalError:
                pass  # column/table already exists

        # Ensure abstract_verification_decisions table exists (for pre-existing databases)
        self._conn.executescript(_VERIFICATION_TABLE)
        self._conn.commit()

        # Rebuild evidence_spans if CHECK constraint is outdated
        row = self._conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND name='evidence_spans'"
        ).fetchone()
        if row and "contested" not in row[0]:
            self._conn.executescript(_EVIDENCE_SPANS_REBUILD)
            self._conn.commit()
            logger.info("Migrated evidence_spans: added contested/invalid_snippet states")

    # ── Papers ───────────────────────────────────────────────

    def add_papers(self, citations: list[Citation]) -> int:
        """Bulk insert citations, skip duplicates by pmid. Returns count added."""
        now = _now()
        added = 0
        for cit in citations:
            # Skip if PMID already exists
            if cit.pmid:
                row = self._conn.execute(
                    "SELECT id FROM papers WHERE pmid = ?", (cit.pmid,)
                ).fetchone()
                if row:
                    continue

            try:
                self._conn.execute(
                    """INSERT INTO papers
                       (pmid, doi, title, abstract, authors, journal, year,
                        source, status, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'INGESTED', ?, ?)""",
                    (
                        cit.pmid,
                        cit.doi,
                        cit.title,
                        cit.abstract,
                        json.dumps(cit.authors),
                        cit.journal,
                        cit.year,
                        cit.source,
                        now,
                        now,
                    ),
                )
                added += 1
            except sqlite3.IntegrityError:
                # UNIQUE constraint on pmid — skip
                continue

        self._conn.commit()
        logger.info("Added %d/%d papers (duplicates skipped)", added, len(citations))
        return added

    def update_status(self, paper_id: int, new_status: str) -> None:
        """Transition a paper to a new lifecycle status."""
        if new_status not in STATUSES:
            raise ValueError(f"Invalid status: {new_status}")

        row = self._conn.execute(
            "SELECT status FROM papers WHERE id = ?", (paper_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"Paper {paper_id} not found")

        current = row["status"]
        allowed = ALLOWED_TRANSITIONS.get(current, set())
        if new_status not in allowed:
            raise ValueError(
                f"Invalid transition: {current} → {new_status} "
                f"(allowed: {allowed or 'none'})"
            )

        self._conn.execute(
            "UPDATE papers SET status = ?, updated_at = ? WHERE id = ?",
            (new_status, _now(), paper_id),
        )
        self._conn.commit()

    def get_papers_by_status(self, status: str) -> list[dict]:
        """Return all papers with the given status."""
        rows = self._conn.execute(
            "SELECT * FROM papers WHERE status = ?", (status,)
        ).fetchall()
        return [dict(r) for r in rows]

    def reject_paper(self, paper_id: int, reason: str) -> None:
        """Reject a paper from the review, preserving its row and identifiers.

        Sets status to REJECTED and records the rejection reason.
        Wraps in a single transaction.
        """
        row = self._conn.execute(
            "SELECT status FROM papers WHERE id = ?", (paper_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"Paper {paper_id} not found")

        current = row["status"]
        allowed = ALLOWED_TRANSITIONS.get(current, set())
        if "REJECTED" not in allowed:
            raise ValueError(
                f"Cannot reject paper {paper_id}: transition {current} → REJECTED "
                f"not allowed (allowed: {allowed or 'none'})"
            )

        try:
            self._conn.execute("BEGIN")
            self._conn.execute(
                """UPDATE papers
                   SET status = 'REJECTED', rejected_reason = ?, updated_at = ?
                   WHERE id = ?""",
                (reason, _now(), paper_id),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def reset_for_reaudit(self) -> dict:
        """Reset all audit state so the auditor can be re-run from scratch.

        Administrative override. Intentional bypass of state machine. Valid use
        cases: auditor logic changes, schema updates, prompt refinements.
        Never called during normal pipeline operation.

        Atomic: either both updates succeed or neither does.
        Returns counts of papers and spans reset.
        """
        try:
            self._conn.execute("BEGIN")
            span_result = self._conn.execute(
                """UPDATE evidence_spans
                   SET audit_status = 'pending',
                       auditor_model = NULL,
                       audit_rationale = NULL,
                       audited_at = NULL
                   WHERE audit_status != 'pending'"""
            )
            spans_reset = span_result.rowcount

            paper_result = self._conn.execute(
                """UPDATE papers
                   SET status = 'EXTRACTED', updated_at = ?
                   WHERE status IN (
                       'AI_AUDIT_COMPLETE', 'HUMAN_AUDIT_COMPLETE',
                       'AUDITED'
                   )""",
                (_now(),),
            )
            papers_reset = paper_result.rowcount

            self._conn.execute("COMMIT")
            logger.info(
                "Re-audit reset: %d papers → EXTRACTED, %d spans → pending",
                papers_reset, spans_reset,
            )
            return {"papers_reset": papers_reset, "spans_reset": spans_reset}
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def reset_for_reextraction(self) -> dict:
        """Reset all audit, extraction, and span state for full re-extraction.

        Administrative override. Steps papers back to PARSED and deletes all
        extraction records and evidence spans for those papers. Valid use
        cases: extractor logic changes, schema updates. Never called during
        normal pipeline operation. SCREENED_OUT and REJECTED papers are
        unaffected.

        Single transaction — all four phases succeed or none do:
        1. Audited papers → EXTRACTED (collapses audit states)
        2. Delete spans for EXTRACTED papers
        3. Delete extraction records for EXTRACTED papers
        4. EXTRACTED → PARSED

        Returns counts of papers reset, spans deleted, and extractions deleted.
        """
        auto_backup(self.db_path, "pre-reset")

        try:
            self._conn.execute("BEGIN")

            # Phase 1: Audited papers → EXTRACTED
            self._conn.execute(
                """UPDATE papers
                   SET status = 'EXTRACTED', updated_at = ?
                   WHERE status IN (
                       'AI_AUDIT_COMPLETE', 'HUMAN_AUDIT_COMPLETE',
                       'AUDITED'
                   )""",
                (_now(),),
            )

            # Phase 2: Delete spans for papers being reset
            span_result = self._conn.execute(
                """DELETE FROM evidence_spans
                   WHERE extraction_id IN (
                       SELECT id FROM extractions
                       WHERE paper_id IN (
                           SELECT id FROM papers WHERE status = 'EXTRACTED'
                       )
                   )"""
            )
            spans_deleted = span_result.rowcount

            # Phase 3: Delete extraction records for papers being reset
            ext_result = self._conn.execute(
                """DELETE FROM extractions
                   WHERE paper_id IN (
                       SELECT id FROM papers WHERE status = 'EXTRACTED'
                   )"""
            )
            extractions_deleted = ext_result.rowcount

            # Phase 4: EXTRACTED → PARSED (administrative override)
            paper_result = self._conn.execute(
                """UPDATE papers
                   SET status = 'PARSED', updated_at = ?
                   WHERE status = 'EXTRACTED'""",
                (_now(),),
            )
            papers_reset = paper_result.rowcount

            self._conn.execute("COMMIT")
            logger.info(
                "Re-extraction reset: %d papers → PARSED, "
                "%d spans deleted, %d extractions deleted",
                papers_reset, spans_deleted, extractions_deleted,
            )
            return {
                "papers_reset": papers_reset,
                "spans_deleted": spans_deleted,
                "extractions_deleted": extractions_deleted,
            }
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def min_status_gate(self, paper_id: int, min_status: str) -> bool:
        """Return True if paper meets or exceeds the minimum status level.

        Order: PARSED < ABSTRACT_SCREENED_OUT < EXTRACTED < AI_AUDIT_COMPLETE
               < HUMAN_AUDIT_COMPLETE.
        """
        if min_status not in _STATUS_ORDER:
            raise ValueError(f"Unknown status for gate check: {min_status}")

        row = self._conn.execute(
            "SELECT status FROM papers WHERE id = ?", (paper_id,)
        ).fetchone()
        if row is None:
            return False

        current = row["status"]
        if current not in _STATUS_ORDER:
            return False

        return _STATUS_ORDER[current] >= _STATUS_ORDER[min_status]

    # ── Screening ────────────────────────────────────────────

    def add_screening_decision(
        self,
        paper_id: int,
        pass_number: int,
        decision: str,
        rationale: str,
        model: str,
    ) -> int:
        """Record a screening decision. Returns the decision id."""
        cur = self._conn.execute(
            """INSERT INTO abstract_screening_decisions
               (paper_id, pass_number, decision, rationale, model, decided_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (paper_id, pass_number, decision, rationale, model, _now()),
        )
        self._conn.commit()
        return cur.lastrowid

    def add_verification_decision(
        self,
        paper_id: int,
        decision: str,
        rationale: str,
        model: str,
    ) -> int:
        """Record a verification screening decision. Returns the decision id."""
        cur = self._conn.execute(
            """INSERT INTO abstract_verification_decisions
               (paper_id, decision, rationale, model, decided_at)
               VALUES (?, ?, ?, ?, ?)""",
            (paper_id, decision, rationale, model, _now()),
        )
        self._conn.commit()
        return cur.lastrowid

    def add_ft_screening_decision(
        self,
        paper_id: int,
        model: str,
        decision: str,
        reason_code: str,
        rationale: str,
        confidence: float,
    ) -> int:
        """Record a full-text screening decision. Returns the decision id."""
        cur = self._conn.execute(
            """INSERT INTO ft_screening_decisions
               (paper_id, model, decision, reason_code, rationale, confidence, decided_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (paper_id, model, decision, reason_code, rationale, confidence, _now()),
        )
        self._conn.commit()
        return cur.lastrowid

    def add_ft_verification_decision(
        self,
        paper_id: int,
        model: str,
        decision: str,
        rationale: str,
        confidence: float,
    ) -> int:
        """Record a full-text verification decision. Returns the decision id."""
        cur = self._conn.execute(
            """INSERT INTO ft_verification_decisions
               (paper_id, model, decision, rationale, confidence, decided_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (paper_id, model, decision, rationale, confidence, _now()),
        )
        self._conn.commit()
        return cur.lastrowid

    def get_screening_summary(self) -> dict:
        """Counts per paper status for screening-related states."""
        rows = self._conn.execute(
            "SELECT status, COUNT(*) as cnt FROM papers GROUP BY status"
        ).fetchall()
        return {r["status"]: r["cnt"] for r in rows}

    # ── Extractions ──────────────────────────────────────────

    def add_extraction(
        self,
        paper_id: int,
        schema_hash: str,
        extracted_data: dict,
        reasoning_trace: str,
        model: str,
    ) -> int:
        """Record an extraction. Returns the extraction id."""
        cur = self._conn.execute(
            """INSERT INTO extractions
               (paper_id, extraction_schema_hash, extracted_data,
                reasoning_trace, model, extracted_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                paper_id,
                schema_hash,
                json.dumps(extracted_data),
                reasoning_trace,
                model,
                _now(),
            ),
        )
        self._conn.commit()
        return cur.lastrowid

    def add_extraction_atomic(
        self,
        paper_id: int,
        schema_hash: str,
        extracted_data: dict,
        reasoning_trace: str,
        model: str,
        spans: list[dict],
        model_digest: str | None = None,
        auditor_model_digest: str | None = None,
    ) -> int:
        """Atomically insert extraction + all evidence spans in one transaction.

        If any insert fails, the entire operation rolls back — no partial
        extraction records are left in the database.

        Each span dict must have: field_name, value, source_snippet, confidence.
        Returns the extraction id.
        """
        try:
            self._conn.execute("BEGIN")
            cur = self._conn.execute(
                """INSERT INTO extractions
                   (paper_id, extraction_schema_hash, extracted_data,
                    reasoning_trace, model, model_digest,
                    auditor_model_digest, extracted_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    paper_id,
                    schema_hash,
                    json.dumps(extracted_data),
                    reasoning_trace,
                    model,
                    model_digest,
                    auditor_model_digest,
                    _now(),
                ),
            )
            ext_id = cur.lastrowid

            for s in spans:
                self._conn.execute(
                    """INSERT INTO evidence_spans
                       (extraction_id, field_name, value, source_snippet,
                        confidence, audit_status)
                       VALUES (?, ?, ?, ?, ?, 'pending')""",
                    (ext_id, s["field_name"], s["value"],
                     s["source_snippet"], s["confidence"]),
                )

            self._conn.execute("COMMIT")
            return ext_id
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def get_stale_extractions(self, current_hash: str) -> list[dict]:
        """Return papers whose latest extraction hash differs from current."""
        rows = self._conn.execute(
            """SELECT p.*, e.extraction_schema_hash
               FROM papers p
               JOIN extractions e ON e.paper_id = p.id
               WHERE e.extraction_schema_hash != ?
               AND e.id = (
                   SELECT MAX(e2.id) FROM extractions e2
                   WHERE e2.paper_id = p.id
               )""",
            (current_hash,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Evidence Spans ───────────────────────────────────────

    def add_evidence_span(
        self,
        extraction_id: int,
        field_name: str,
        value: str,
        source_snippet: str,
        confidence: float,
    ) -> int:
        """Record an evidence span. Returns the span id."""
        cur = self._conn.execute(
            """INSERT INTO evidence_spans
               (extraction_id, field_name, value, source_snippet,
                confidence, audit_status)
               VALUES (?, ?, ?, ?, ?, 'pending')""",
            (extraction_id, field_name, value, source_snippet, confidence),
        )
        self._conn.commit()
        return cur.lastrowid

    def update_audit(
        self, span_id: int, status: str, model: str, rationale: str
    ) -> None:
        """Update audit status on an evidence span."""
        self._conn.execute(
            """UPDATE evidence_spans
               SET audit_status = ?, auditor_model = ?,
                   audit_rationale = ?, audited_at = ?
               WHERE id = ?""",
            (status, model, rationale, _now(), span_id),
        )
        self._conn.commit()

    # ── Pipeline Stats ───────────────────────────────────────

    def get_pipeline_stats(self) -> dict:
        """Full pipeline counts: papers by status + extraction/span totals."""
        stats = dict(self.get_screening_summary())
        stats["total_papers"] = self._conn.execute(
            "SELECT COUNT(*) FROM papers"
        ).fetchone()[0]
        stats["total_extractions"] = self._conn.execute(
            "SELECT COUNT(*) FROM extractions"
        ).fetchone()[0]
        stats["total_evidence_spans"] = self._conn.execute(
            "SELECT COUNT(*) FROM evidence_spans"
        ).fetchone()[0]
        stats["spans_verified"] = self._conn.execute(
            "SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'verified'"
        ).fetchone()[0]
        stats["spans_flagged"] = self._conn.execute(
            "SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'flagged'"
        ).fetchone()[0]
        stats["spans_contested"] = self._conn.execute(
            "SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'contested'"
        ).fetchone()[0]
        stats["spans_invalid_snippet"] = self._conn.execute(
            "SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'invalid_snippet'"
        ).fetchone()[0]
        return stats

    # ── Cleanup ──────────────────────────────────────────────

    def cleanup_orphaned_spans(self) -> int:
        """Remove orphaned spans from prior extraction runs.

        Intended for use after re-extraction completes: deletes spans whose
        extraction_id is no longer the latest for that paper. Does not affect
        the current extraction's spans.

        Not intended for pre-extraction cleanup — reset_for_reextraction()
        handles that case by deleting all extractions and spans for papers
        being reset as part of its atomic transaction.

        Returns the number of deleted rows.
        """
        auto_backup(self.db_path, "pre-orphan-cleanup")

        result = self._conn.execute(
            """DELETE FROM evidence_spans
               WHERE extraction_id NOT IN (
                   SELECT MAX(e.id) FROM extractions e GROUP BY e.paper_id
               )"""
        )
        deleted = result.rowcount
        self._conn.commit()
        logger.info("Cleaned up %d orphaned spans", deleted)
        return deleted

    def close(self) -> None:
        self._conn.close()


# ── Helpers ──────────────────────────────────────────────────────────


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
