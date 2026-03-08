"""SQLite database manager — one database per review, full provenance."""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from engine.search.models import Citation

logger = logging.getLogger(__name__)

DATA_ROOT = Path("data")

# ── Paper Lifecycle ──────────────────────────────────────────────────

STATUSES = (
    "INGESTED",
    "SCREENED_IN",
    "SCREENED_OUT",
    "SCREEN_FLAGGED",
    "PDF_ACQUIRED",
    "PARSED",
    "EXTRACT_FAILED",
    "EXTRACTED",
    "AI_AUDIT_COMPLETE",
    "HUMAN_AUDIT_COMPLETE",
    "REJECTED",
)

ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "INGESTED": {"SCREENED_IN", "SCREENED_OUT", "SCREEN_FLAGGED"},
    "SCREENED_IN": {"PDF_ACQUIRED", "SCREEN_FLAGGED"},
    "SCREEN_FLAGGED": {"SCREENED_IN", "SCREENED_OUT"},
    "PDF_ACQUIRED": {"PARSED"},
    "PARSED": {"EXTRACTED", "EXTRACT_FAILED"},
    "EXTRACT_FAILED": {"PARSED", "EXTRACTED"},
    "EXTRACTED": {"AI_AUDIT_COMPLETE"},
    "AI_AUDIT_COMPLETE": {"HUMAN_AUDIT_COMPLETE", "REJECTED"},
    # Terminal states with no forward transitions
    "SCREENED_OUT": set(),
    "HUMAN_AUDIT_COMPLETE": set(),
    "REJECTED": set(),
}

# Ordered status levels for min_status_gate comparisons
_STATUS_ORDER = {
    "PARSED": 0,
    "SCREENED_OUT": 1,
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

CREATE TABLE IF NOT EXISTS screening_decisions (
    id              INTEGER PRIMARY KEY,
    paper_id        INTEGER NOT NULL REFERENCES papers(id),
    pass_number     INTEGER NOT NULL CHECK (pass_number IN (1, 2)),
    decision        TEXT NOT NULL CHECK (decision IN ('include', 'exclude', 'uncertain')),
    rationale       TEXT,
    model           TEXT,
    decided_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_screening_paper ON screening_decisions(paper_id);

CREATE TABLE IF NOT EXISTS verification_decisions (
    id              INTEGER PRIMARY KEY,
    paper_id        INTEGER NOT NULL REFERENCES papers(id),
    decision        TEXT NOT NULL CHECK (decision IN ('include', 'exclude')),
    rationale       TEXT,
    model           TEXT,
    decided_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_verification_paper ON verification_decisions(paper_id);

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
CREATE TABLE IF NOT EXISTS verification_decisions (
    id              INTEGER PRIMARY KEY,
    paper_id        INTEGER NOT NULL REFERENCES papers(id),
    decision        TEXT NOT NULL CHECK (decision IN ('include', 'exclude')),
    rationale       TEXT,
    model           TEXT,
    decided_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_verification_paper ON verification_decisions(paper_id);
"""

_SIMPLE_MIGRATIONS = [
    "ALTER TABLE papers ADD COLUMN rejected_reason TEXT",
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
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._run_migrations()

    def _run_migrations(self) -> None:
        """Apply schema migrations, skipping those already applied."""
        for sql in _SIMPLE_MIGRATIONS:
            try:
                self._conn.execute(sql)
                self._conn.commit()
            except sqlite3.OperationalError:
                pass  # column/table already exists

        # Ensure verification_decisions table exists (for pre-existing databases)
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

        Order: PARSED < SCREENED_OUT < EXTRACTED < AI_AUDIT_COMPLETE
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
            """INSERT INTO screening_decisions
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
            """INSERT INTO verification_decisions
               (paper_id, decision, rationale, model, decided_at)
               VALUES (?, ?, ?, ?, ?)""",
            (paper_id, decision, rationale, model, _now()),
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
