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

-- The post-002 names. This file used to carry the PRE-rename names
-- (idx_adjudication_*), and ReviewDatabase calls it on every construction, so
-- the names migration 002 had renamed away were recreated every time and the
-- live database accumulated six indices on three columns (MIGRATIONS-01).
-- Migration 015 drops the duplicates; this is what stops them coming back.
CREATE INDEX IF NOT EXISTS idx_abstract_adjudication_paper
    ON abstract_screening_adjudication(paper_id);
CREATE INDEX IF NOT EXISTS idx_abstract_adjudication_ext_key
    ON abstract_screening_adjudication(external_key);
CREATE INDEX IF NOT EXISTS idx_abstract_adjudication_decision
    ON abstract_screening_adjudication(adjudication_decision);
"""

# The `audit_adjudication` DDL was removed by R32 (READERS-01 Phase 2a).
#
# It created the table on EVERY `ReviewDatabase` construction, which is why the
# table could not simply be dropped: migration 018's DROP was undone by the next
# open (measured: present 1 -> 0 -> 1). The R32 census found no production
# reader and three unreachable INSERT sites, so the table goes and its
# provisioning goes with it. Human audit decisions become `field_events`
# (`human_accepted` / `human_corrected` / `human_withdrew`) through the importer
# built in session 12 — R32 reverses only the SEQUENCING half of R18's A11
# Option B, not the route.
#
# Inventory row C12: the three surviving tables here are still created outside
# the receipted runner — the self-provisioning pattern R14 retired for
# `human_extractions`. Moving them into numbered migrations is S3c work for a
# later session; 018 deliberately did not widen to them.


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
    conn.executescript(_FT_ADJUDICATION_TABLE)
    conn.commit()

    # Also ensure workflow_state table
    from engine.adjudication.workflow import ensure_workflow_table
    ensure_workflow_table(conn)
