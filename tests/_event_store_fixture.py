"""Build an S2 event store for tests whose fixtures predate it.

READERS-01 Phase 2a. Concordance, the evidence-table exporter, the judge loader
and the distribution monitor read through `engine/core/effective.py` now, and
their direct-table paths are gone (R30). Every test fixture that declared values
by INSERTing into `evidence_spans` / `cloud_evidence_spans` / `human_extractions`
therefore declares them somewhere no reader looks.

The tests themselves are not wrong — they pin distributions, exports and
alignments that are still the contract. Only the STORE moved. So the store moves
here, in one helper, rather than thirty-one test bodies being rewritten by hand
(B5: a test that pinned a behaviour is rewritten to the corrected behaviour).

Underscore-prefixed so pytest does not collect it.
"""

from __future__ import annotations

import importlib
import sqlite3
from pathlib import Path

from engine.core import events

m016 = importlib.import_module("engine.migrations.016_event_store")
m019 = importlib.import_module("engine.migrations.019_paper_state_axes")

_PAPERS_DDL = """
CREATE TABLE IF NOT EXISTS papers (
    id INTEGER PRIMARY KEY, title TEXT NOT NULL DEFAULT 't',
    source TEXT NOT NULL DEFAULT 'fixture', status TEXT NOT NULL DEFAULT 'EXTRACTED',
    created_at TEXT NOT NULL DEFAULT 'x', updated_at TEXT NOT NULL DEFAULT 'x',
    pmid TEXT, doi TEXT, authors TEXT, year INTEGER, journal TEXT
);
"""


_DONE: set[str] = set()


def ensure_event_store(db_path: str | Path) -> None:
    """Create `papers` (if absent) and the event store at the post-019 shape.

    Memoised per path: `add_values` is called repeatedly by a single fixture and
    running 016 + 019 on each call made the suite markedly slower for no effect
    (both are idempotent).
    """
    db_path = str(db_path)
    if db_path in _DONE:
        return
    conn = sqlite3.connect(db_path)
    conn.executescript(_PAPERS_DDL)
    if not conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE name='evidence_spans'"
    ).fetchone()[0]:
        conn.execute("CREATE TABLE evidence_spans (id INTEGER PRIMARY KEY)")
    m016.create_schema(conn)
    conn.commit()
    conn.close()
    m019.run_migration(db_path)
    _DONE.add(db_path)


def add_values(db_path: str | Path, arm: str, field_name: str,
               values, *, arm_kind: str = "model", start_paper: int = 1,
               located: bool = True) -> None:
    """Declare `values` for `field_name` on `arm`, one paper each.

    `values[i]` belongs to paper `start_paper + i`. A `None` declares that the
    arm recorded nothing for that paper — which the reader returns as `missing`,
    the same fact the old fixtures expressed by omitting the row.
    """
    ensure_event_store(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        if not conn.execute(
            "SELECT COUNT(*) FROM arms WHERE arm_name = ?", (arm,)
        ).fetchone()[0]:
            events.register_arm(conn, arm, arm_kind)

        for i, value in enumerate(values):
            pid = start_paper + i
            conn.execute(
                "INSERT OR IGNORE INTO papers (id) VALUES (?)", (pid,))
            if not conn.execute(
                "SELECT COUNT(*) FROM paper_events WHERE paper_id = ?", (pid,)
            ).fetchone()[0]:
                events.write_paper_event(
                    conn, event_type="state_at_migration", paper_id=pid,
                    to_state="eligible", actor_kind="engine", actor_role="system",
                    actor_name="fixture")
            if value is None:
                continue
            uid = events.mint_extraction_uid()
            events.write_field_event(
                conn, event_type="asserted", paper_id=pid, field_name=field_name,
                arm=arm, value=value, extraction_uid=uid, source_snippet=value,
                actor_kind="model", actor_role="extractor", actor_name="fixture")
            if located:
                events.write_field_event(
                    conn, event_type="citation_located", paper_id=pid,
                    field_name=field_name, arm=arm, extraction_uid=uid,
                    actor_kind="engine", actor_role="system", actor_name="locator",
                    payload={"located": True, "snippet": value})
        conn.commit()
    finally:
        conn.close()


def mirror_legacy_into_events(db, *, arm: str = "local") -> int:
    """Mirror a legacy-shaped fixture's rows into the event store. TEST ONLY.

    Some fixtures declare their world through `ReviewDatabase.add_extraction` /
    `add_evidence_span` / `update_status` — the engine's own write path before
    the event store existed. Those calls are still the clearest way to say what
    the fixture contains, and rewriting them by hand into `write_field_event`
    calls would change what the test READS without changing what it MEANS.

    So this translates instead: the latest extraction's spans become `asserted`
    (+ `citation_located`) field events, and a corpus status becomes one
    `eligible` paper event. It is the shape migration 017 seeded on live, with
    field values added — which 017 deliberately did not do (R25: seed, do not
    migrate). **That is why this lives in `tests/` and not in `engine/`:** it is
    a fixture convenience, not a supported import path, and nothing in the
    engine may grow a dependency on it.

    Returns the number of field events written.
    """
    conn = db._conn
    ensure_event_store(db.db_path)

    if not conn.execute(
        "SELECT COUNT(*) FROM arms WHERE arm_name = ?", (arm,)
    ).fetchone()[0]:
        events.register_arm(conn, arm, "model")

    corpus = ("FT_ELIGIBLE", "EXTRACTED", "AI_AUDIT_COMPLETE",
              "HUMAN_AUDIT_COMPLETE", "EXTRACT_FAILED")
    for (pid,) in conn.execute(
        f"SELECT id FROM papers WHERE status IN "
        f"({', '.join('?' * len(corpus))}) ORDER BY id", corpus
    ).fetchall():
        if not conn.execute(
            "SELECT COUNT(*) FROM paper_events WHERE paper_id = ?", (pid,)
        ).fetchone()[0]:
            events.write_paper_event(
                conn, event_type="state_at_migration", paper_id=pid,
                to_state="eligible", actor_kind="engine", actor_role="system",
                actor_name="fixture")

    rows = conn.execute(
        """SELECT e.paper_id, es.field_name, es.value, es.source_snippet
           FROM evidence_spans es
           JOIN extractions e ON e.id = es.extraction_id
           WHERE e.id = (SELECT MAX(e2.id) FROM extractions e2
                         WHERE e2.paper_id = e.paper_id)
           ORDER BY e.paper_id, es.id"""
    ).fetchall()

    n = 0
    for paper_id, field_name, value, snippet in rows:
        if value is None:
            continue
        uid = events.mint_extraction_uid()
        events.write_field_event(
            conn, event_type="asserted", paper_id=paper_id,
            field_name=field_name, arm=arm, value=value,
            extraction_uid=uid, source_snippet=snippet,
            actor_kind="model", actor_role="extractor", actor_name="fixture")
        events.write_field_event(
            conn, event_type="citation_located", paper_id=paper_id,
            field_name=field_name, arm=arm, extraction_uid=uid,
            actor_kind="engine", actor_role="system", actor_name="locator",
            payload={"located": bool(snippet), "snippet": snippet or ""})
        n += 1
    conn.commit()
    return n
