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
    confidence              REAL NOT NULL,
    tier                    INTEGER NOT NULL,
    notes                   TEXT,
    UNIQUE(cloud_extraction_id, field_name)
);
"""


def init_cloud_tables(db_path: str) -> None:
    """Create cloud extraction tables if they don't exist."""
    conn = sqlite3.connect(db_path)
    conn.executescript(_CLOUD_SCHEMA)
    # Migrate: add notes column if missing (for pre-existing DBs)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(cloud_evidence_spans)").fetchall()}
    if "notes" not in cols:
        conn.execute("ALTER TABLE cloud_evidence_spans ADD COLUMN notes TEXT")

    # Migrate: add NOT NULL to confidence and tier if missing (pre-existing DBs)
    col_info = conn.execute("PRAGMA table_info(cloud_evidence_spans)").fetchall()
    col_map = {r[1]: r for r in col_info}  # name -> (cid, name, type, notnull, default, pk)
    conf_notnull = col_map.get("confidence", (0, "", "", 0, None, 0))[3]
    tier_notnull = col_map.get("tier", (0, "", "", 0, None, 0))[3]
    if not conf_notnull or not tier_notnull:
        # Backfill NULLs before adding constraint
        conn.execute("UPDATE cloud_evidence_spans SET confidence = 0.0 WHERE confidence IS NULL")
        conn.execute("UPDATE cloud_evidence_spans SET tier = 1 WHERE tier IS NULL")
        # Rebuild table with NOT NULL constraints
        conn.executescript("""
            ALTER TABLE cloud_evidence_spans RENAME TO _cloud_evidence_spans_old;

            CREATE TABLE cloud_evidence_spans (
                id                      INTEGER PRIMARY KEY,
                cloud_extraction_id     INTEGER NOT NULL REFERENCES cloud_extractions(id),
                field_name              TEXT NOT NULL,
                value                   TEXT,
                source_snippet          TEXT,
                confidence              REAL NOT NULL,
                tier                    INTEGER NOT NULL,
                notes                   TEXT,
                UNIQUE(cloud_extraction_id, field_name)
            );

            INSERT INTO cloud_evidence_spans
                SELECT id, cloud_extraction_id, field_name, value, source_snippet,
                       confidence, tier, notes
                FROM _cloud_evidence_spans_old;

            DROP TABLE _cloud_evidence_spans_old;
        """)

    conn.commit()
    conn.close()
