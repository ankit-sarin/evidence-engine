"""Shared fixture builder for the corpus-eligibility authority tests (CORPUS-PRED-01).

Underscore-prefixed so pytest does not collect it as a test module. It is imported
both by `tests/test_corpus_authority.py` and by the one-shot pre-patch capture that
produced the pinned baselines in that file, so the bytes proving "unchanged" and the
bytes asserting it come from the same builder.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

# The 15 members of engine.core.database.STATUSES, in declaration order. Held here
# as a literal on purpose: a fixture that imported the definition it exists to probe
# would silently follow it and stop being a fixture.
FIXTURE_STATUSES = (
    "INGESTED",
    "ABSTRACT_SCREENED_IN",
    "ABSTRACT_SCREENED_OUT",
    "ABSTRACT_SCREEN_FLAGGED",
    "PDF_ACQUIRED",
    "PDF_EXCLUDED",
    "PARSED",
    "FT_ELIGIBLE",
    "FT_SCREENED_OUT",
    "FT_FLAGGED",
    "EXTRACT_FAILED",
    "EXTRACTED",
    "AI_AUDIT_COMPLETE",
    "HUMAN_AUDIT_COMPLETE",
    "REJECTED",
)

# One paper per status, id = 1-based index into FIXTURE_STATUSES. These carry no
# parsed text, so they exercise the predicate/SQL agreement without entering the
# sampler's pool.
STATUS_PROBE_IDS = {s: i + 1 for i, s in enumerate(FIXTURE_STATUSES)}

# Mirrors analysis.eval.schema_eval2.CARRIED and the real statuses measured in
# review.db: seven AI_AUDIT_COMPLETE, three FT_SCREENED_OUT (547, 629, 799).
FIXTURE_CARRIED = (39, 386, 466, 498, 547, 629, 691, 694, 708, 799)
FIXTURE_CARRIED_NON_CORPUS = (547, 629, 799)

_POOL_STATUS_CYCLE = ("AI_AUDIT_COMPLETE", "EXTRACTED", "FT_ELIGIBLE",
                      "HUMAN_AUDIT_COMPLETE", "FT_SCREENED_OUT", "EXTRACT_FAILED")
_STUDY_TYPES = ("rct", "cohort", "bench", "simulation")

_SCHEMA = """
CREATE TABLE papers (
    id INTEGER PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL,
    authors TEXT, year INTEGER, source TEXT NOT NULL,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE extractions (id INTEGER PRIMARY KEY, paper_id INTEGER NOT NULL);
CREATE TABLE evidence_spans (
    id INTEGER PRIMARY KEY, extraction_id INTEGER NOT NULL,
    field_name TEXT NOT NULL, value TEXT
);
CREATE TABLE cloud_extractions (
    id INTEGER PRIMARY KEY, paper_id INTEGER NOT NULL, arm TEXT NOT NULL,
    cost_usd REAL
);
"""


def _add_paper(conn: sqlite3.Connection, pid: int, status: str) -> None:
    conn.execute(
        "INSERT INTO papers (id, title, status, source, created_at, updated_at) "
        "VALUES (?, ?, ?, 'fixture', '2026-01-01', '2026-01-01')",
        (pid, f"paper {pid}", status),
    )


def build_fixture(root: Path) -> Path:
    """Build a self-contained review dir. Returns the review dir path.

    Deterministic in every respect: ids, statuses, parsed-text lengths and
    study_type labels are all functions of the paper id.
    """
    root = Path(root)
    parsed = root / "parsed_text"
    parsed.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(root / "review.db")
    conn.executescript(_SCHEMA)

    # 1. One paper per status, no parsed text.
    for status, pid in STATUS_PROBE_IDS.items():
        _add_paper(conn, pid, status)

    # 2. The carried tuple, with the measured real-world statuses.
    for pid in FIXTURE_CARRIED:
        _add_paper(conn, pid,
                   "FT_SCREENED_OUT" if pid in FIXTURE_CARRIED_NON_CORPUS
                   else "AI_AUDIT_COMPLETE")

    # 3. A pool of 60 papers cycling through corpus and non-corpus statuses.
    pool_ids = list(range(1000, 1060))
    for i, pid in enumerate(pool_ids):
        _add_paper(conn, pid, _POOL_STATUS_CYCLE[i % len(_POOL_STATUS_CYCLE)])

    # 4. Parsed text for carried + pool only. Length is a deterministic function
    #    of the id so the sampler's length strata are stable.
    for i, pid in enumerate(list(FIXTURE_CARRIED) + pool_ids):
        (parsed / f"{pid}_v1.md").write_text("x" * (500 + (pid % 37) * 250))

    # 5. study_type spans for every other paper, so "unknown" is also exercised.
    ext_id = 0
    for i, pid in enumerate(list(FIXTURE_CARRIED) + pool_ids):
        if i % 2:
            continue
        ext_id += 1
        conn.execute("INSERT INTO extractions (id, paper_id) VALUES (?, ?)", (ext_id, pid))
        conn.execute(
            "INSERT INTO evidence_spans (extraction_id, field_name, value) "
            "VALUES (?, 'study_type', ?)",
            (ext_id, _STUDY_TYPES[i % len(_STUDY_TYPES)]),
        )

    # 6. A couple of cloud extractions so get_pending_papers has something to exclude.
    for pid in (1000, 1001):
        conn.execute(
            "INSERT INTO cloud_extractions (paper_id, arm, cost_usd) VALUES (?, 'openai', 1.5)",
            (pid,),
        )

    conn.commit()
    conn.close()
    return root
