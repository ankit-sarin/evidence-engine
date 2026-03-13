"""Database schema for adjudication (screening + audit review)."""

import sqlite3


_ADJUDICATION_TABLE = """
CREATE TABLE IF NOT EXISTS abstract_screening_adjudication (
    id                      INTEGER PRIMARY KEY,
    paper_id                INTEGER REFERENCES papers(id),
    external_key            TEXT,
    title                   TEXT NOT NULL,
    adjudication_decision   TEXT CHECK (adjudication_decision IN ('INCLUDE', 'EXCLUDE')),
    adjudication_source     TEXT NOT NULL DEFAULT 'human'
                            CHECK (adjudication_source IN ('human', 'automated')),
    adjudication_reason     TEXT,
    adjudication_category   TEXT,
    adjudication_timestamp  TEXT,
    created_at              TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_adjudication_paper
    ON abstract_screening_adjudication(paper_id);
CREATE INDEX IF NOT EXISTS idx_adjudication_ext_key
    ON abstract_screening_adjudication(external_key);
CREATE INDEX IF NOT EXISTS idx_adjudication_decision
    ON abstract_screening_adjudication(adjudication_decision);
"""

_AUDIT_ADJUDICATION_TABLE = """
CREATE TABLE IF NOT EXISTS audit_adjudication (
    id                      INTEGER PRIMARY KEY,
    span_id                 INTEGER REFERENCES evidence_spans(id),
    paper_id                INTEGER REFERENCES papers(id),
    field_name              TEXT NOT NULL,
    original_value          TEXT,
    human_decision          TEXT CHECK (human_decision IN ('accept', 'override', 'reject_paper')),
    override_value          TEXT,
    reviewer_notes          TEXT,
    adjudication_timestamp  TEXT,
    created_at              TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_adj_span
    ON audit_adjudication(span_id);
CREATE INDEX IF NOT EXISTS idx_audit_adj_paper
    ON audit_adjudication(paper_id);
"""


_FT_ADJUDICATION_TABLE = """
CREATE TABLE IF NOT EXISTS ft_screening_adjudication (
    id                      INTEGER PRIMARY KEY,
    paper_id                INTEGER REFERENCES papers(id),
    title                   TEXT NOT NULL,
    reason_code             TEXT,
    primary_rationale       TEXT,
    verifier_rationale      TEXT,
    adjudication_decision   TEXT CHECK (adjudication_decision IN ('FT_ELIGIBLE', 'FT_SCREENED_OUT')),
    adjudication_reason     TEXT,
    adjudication_timestamp  TEXT,
    created_at              TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ft_adj_paper
    ON ft_screening_adjudication(paper_id);
"""


def ensure_adjudication_table(conn: sqlite3.Connection) -> None:
    """Create all adjudication tables if they don't exist."""
    conn.executescript(_ADJUDICATION_TABLE)
    conn.executescript(_AUDIT_ADJUDICATION_TABLE)
    conn.executescript(_FT_ADJUDICATION_TABLE)
    conn.commit()

    # Also ensure workflow_state table
    from engine.adjudication.workflow import ensure_workflow_table
    ensure_workflow_table(conn)
