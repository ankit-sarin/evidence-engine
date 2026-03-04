"""Cloud extraction database schema — parallel tables for concordance study."""

import sqlite3

_CLOUD_SCHEMA = """
CREATE TABLE IF NOT EXISTS cloud_extractions (
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
    extracted_at            TEXT NOT NULL,
    UNIQUE(paper_id, arm)
);

CREATE TABLE IF NOT EXISTS cloud_evidence_spans (
    id                      INTEGER PRIMARY KEY,
    cloud_extraction_id     INTEGER NOT NULL REFERENCES cloud_extractions(id),
    field_name              TEXT NOT NULL,
    value                   TEXT,
    source_snippet          TEXT,
    confidence              REAL,
    tier                    INTEGER,
    UNIQUE(cloud_extraction_id, field_name)
);
"""


def init_cloud_tables(db_path: str) -> None:
    """Create cloud extraction tables if they don't exist."""
    conn = sqlite3.connect(db_path)
    conn.executescript(_CLOUD_SCHEMA)
    conn.commit()
    conn.close()
