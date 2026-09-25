"""SQLite database manager — one database per review, full provenance.

RETENTION POLICY: All fetched paper data (metadata, abstract, screening
traces, verification traces) is retained permanently regardless of
screening outcome. ABSTRACT_SCREENED_OUT is a label, not a deletion.
The database is the single source of truth for all papers ever evaluated.
"""

from collections.abc import Collection
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

CREATE TABLE IF NOT EXISTS parse_attempts (
    id                  INTEGER PRIMARY KEY,
    paper_id            INTEGER NOT NULL REFERENCES papers(id),
    pdf_hash            TEXT,
    parsed_text_version INTEGER NOT NULL,
    attempt_index       INTEGER NOT NULL,
    parser_used         TEXT NOT NULL,
    passed              INTEGER NOT NULL DEFAULT 0,
    failures            TEXT,           -- JSON array of [criterion, value, threshold]
    metrics             TEXT,           -- JSON object, the full metric mapping
    elapsed_s           REAL,
    accepted            INTEGER NOT NULL DEFAULT 0,
    skipped_reason      TEXT,           -- non-NULL => parser was never run
    created_at          TEXT NOT NULL,
    font_audit          TEXT            -- JSON object, engine.parsers.font_audit
);

CREATE INDEX IF NOT EXISTS idx_parse_attempts_paper
    ON parse_attempts(paper_id, parsed_text_version);

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
    confidence      REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    tier            INTEGER NOT NULL DEFAULT 1 CHECK (tier >= 1 AND tier <= 4),
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
    # FONT-AUDIT-02: per-attempt font-exposure audit summary, JSON. NULL means
    # the attempt was not audited (it produced no text, or predates the column);
    # it never means "audited and clean" -- see engine/parsers/font_audit.py.
    "ALTER TABLE parse_attempts ADD COLUMN font_audit TEXT",
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
    confidence      REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    tier            INTEGER NOT NULL DEFAULT 1 CHECK (tier >= 1 AND tier <= 4),
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
    (id, extraction_id, field_name, value, source_snippet,
     confidence, tier, audit_status, auditor_model,
     audit_rationale, audited_at)
    SELECT id, extraction_id, field_name, value, source_snippet,
           confidence, 1, audit_status, auditor_model,
           audit_rationale, audited_at
    FROM _evidence_spans_old;

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
        """Bring this database up to date, recording what ran.

        MIGRATIONS-01. This used to be eighteen inline `ALTER TABLE` statements
        that duplicated migrations 004 and 005, five unconditional `importlib`
        imports, two ad-hoc rebuilds, and no record of any of it — while 002,
        003, 010 and 011 were never run here at all. A database could not say
        which migrations it had, and a second review could not reach the same
        schema.

        The inline base stays: `_SCHEMA` (run by `__init__`), the adjudication
        tables, the verification table and the `evidence_spans` CHECK rebuild
        are the floor the numbered migrations start from, and several of them
        assume it. What moved is everything numbered — the runner applies those
        in order and writes a receipt for each.
        """
        from engine.adjudication.schema import ensure_adjudication_table
        from engine.migrations import runner

        ensure_adjudication_table(self._conn)

        for sql in _SIMPLE_MIGRATIONS:
            try:
                self._conn.execute(sql)
                self._conn.commit()
            except sqlite3.OperationalError as exc:
                msg = str(exc).lower()
                if "already exists" in msg or "duplicate column" in msg:
                    pass  # column/table already exists — safe to ignore
                else:
                    logger.error("Migration failed — %s: %s", sql.strip()[:60], exc)
                    raise

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

        # The numbered migrations, in order, each with a receipt. Data
        # migrations are skipped: 003 would import one review's corpus into
        # another review's database, and 002 renames labels a fresh database
        # has no rows to carry.
        self._conn.close()
        try:
            result = runner.run(self.db_path)
        finally:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.execute("PRAGMA foreign_keys=ON")
        if result["executed"]:
            logger.info("Migrations executed: %s", ", ".join(result["executed"]))

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
        """Transition a paper to a new lifecycle status.

        When called outside a transaction, wraps validation + update in
        BEGIN IMMEDIATE to prevent races. When called inside an existing
        transaction (e.g., from reject_paper), participates in that
        transaction without starting a nested one.
        """
        if new_status not in STATUSES:
            raise ValueError(f"Invalid status: {new_status}")

        own_txn = not self._conn.in_transaction
        try:
            if own_txn:
                self._conn.execute("BEGIN IMMEDIATE")

            row = self._conn.execute(
                "SELECT status FROM papers WHERE id = ?", (paper_id,)
            ).fetchone()
            if row is None:
                if own_txn:
                    self._conn.execute("ROLLBACK")
                raise ValueError(f"Paper {paper_id} not found")

            current = row["status"]
            allowed = ALLOWED_TRANSITIONS.get(current, set())
            if new_status not in allowed:
                if own_txn:
                    self._conn.execute("ROLLBACK")
                raise ValueError(
                    f"Invalid transition: {current} → {new_status} "
                    f"(allowed: {allowed or 'none'})"
                )

            self._conn.execute(
                "UPDATE papers SET status = ?, updated_at = ? WHERE id = ?",
                (new_status, _now(), paper_id),
            )
            if own_txn:
                self._conn.execute("COMMIT")
        except ValueError:
            raise
        except Exception:
            if own_txn:
                self._conn.execute("ROLLBACK")
            raise

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
        *,
        reason_codes: Collection[str],
    ) -> int:
        """Record a full-text screening decision. Returns the decision id.

        `reason_codes` is the review's effective vocabulary
        (`spec.eligibility.reason_codes()`); a code outside it is refused before
        anything is written. The column carries no CHECK constraint because the
        vocabulary belongs to the spec, not the schema, so this is where it is
        enforced. Rows already stored are not re-validated.
        """
        if reason_code not in reason_codes:
            raise ValueError(
                f"reason_code {reason_code!r} is not in this review's reason-code "
                f"vocabulary {sorted(reason_codes)}. A code outside it names no rule "
                "the review declares, so the stored decision could not be traced to "
                "a criterion."
            )
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
        schema_hash: str | None,
        extracted_data: dict,
        reasoning_trace: str,
        model: str,
        codebook_hash: str | None = None,
        codebook_sha256: str | None = None,
    ) -> int:
        """Record an extraction. Returns the extraction id."""
        cur = self._conn.execute(
            """INSERT INTO extractions
               (paper_id, extraction_schema_hash, extracted_data,
                reasoning_trace, model, extracted_at,
                codebook_hash, codebook_sha256)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                paper_id,
                schema_hash,
                json.dumps(extracted_data),
                reasoning_trace,
                model,
                _now(),
                codebook_hash,
                codebook_sha256,
            ),
        )
        self._conn.commit()
        return cur.lastrowid

    # ── Evidence Spans ───────────────────────────────────────

    def add_evidence_span(
        self,
        extraction_id: int,
        field_name: str,
        value: str,
        source_snippet: str,
        confidence: float,
        tier: int = 1,
    ) -> int:
        """Record an evidence span. Returns the span id."""
        cur = self._conn.execute(
            """INSERT INTO evidence_spans
               (extraction_id, field_name, value, source_snippet,
                confidence, tier, audit_status)
               VALUES (?, ?, ?, ?, ?, ?, 'pending')""",
            (extraction_id, field_name, value, source_snippet, confidence, tier),
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
        """End-of-run counts, from the event reader beside the screening summary.

        R165 (9c-C4): the extraction and span totals and the four audit_status
        buckets are gone — no run writes those tables after the cut-over. Both
        axes come from `effective_state`, one call per paper; `no_recorded_state`
        is reported as it is (the screeners write no events yet), not filtered.

        - `screening`: `get_screening_summary()`, `papers.status` counts (the
          screeners are not cut over).
        - `eligibility`: every paper's eligibility token.
        - `processing`: the processing token of each ELIGIBLE paper.
        - `processing_failures`: the reason code of each eligible paper whose
          processing token is a failure.
        - `analysis_ready`: eligible papers whose processing completed.
        """
        from collections import Counter
        from engine.core.effective import effective_state

        eligibility: Counter = Counter()
        processing: Counter = Counter()
        failures: Counter = Counter()
        analysis_ready = 0
        paper_ids = [r[0] for r in self._conn.execute("SELECT id FROM papers ORDER BY id")]
        for pid in paper_ids:
            state = effective_state(self._conn, pid)
            eligibility[state.eligibility] += 1
            if state.eligibility != "eligible":
                continue
            processing[state.processing] += 1
            if state.processing_reason is not None:
                failures[state.processing_reason] += 1
            analysis_ready += state.analysis_ready
        return {
            "total_papers": len(paper_ids),
            "screening": self.get_screening_summary(),
            "eligibility": dict(eligibility),
            "processing": dict(processing),
            "processing_failures": dict(failures),
            "analysis_ready": analysis_ready,
        }

    # ── Context Manager ─────────────────────────────────────

    def __enter__(self) -> "ReviewDatabase":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


# ── Helpers ──────────────────────────────────────────────────────────


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
