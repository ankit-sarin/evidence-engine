"""Migration 019 — the two-axis paper-state vocabulary (R29, R39).

Every gate here runs on a CONSTRUCTED fixture, never on live data: under R25 the
live store carries 190 identical `state_at_migration` rows and nothing else, so a
gate evaluated there could not fail for any reason the migration is about
(READERS-01 contradiction 7, accepted as a standing constraint).

The pre-019 fixture is built from migration 016's own DDL rather than from a
hand-typed copy, so a fixture that stopped resembling the table it stands in for
would fail to build rather than quietly test a shape that never existed.
"""

from __future__ import annotations

import importlib
import sqlite3

import pytest

m016 = importlib.import_module("engine.migrations.016_event_store")
m019 = importlib.import_module("engine.migrations.019_paper_state_axes")

from engine.core import paper_state


# ── the pre-019 fixture ───────────────────────────────────────────────

def _pre_019_db(tmp_path, events=()):
    """A database in the 016 shape: papers, the event store, the triggers."""
    path = tmp_path / "pre019.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE papers (
            id INTEGER PRIMARY KEY, title TEXT NOT NULL, source TEXT NOT NULL,
            status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE evidence_spans (id INTEGER PRIMARY KEY);
        """
    )
    m016.create_schema(conn)
    conn.execute(
        "INSERT INTO papers (id,title,source,status,created_at,updated_at) "
        "VALUES (1,'t','manual','FT_ELIGIBLE','2026-01-01','2026-01-01')"
    )
    for uid, etype, to_state, reason in events:
        conn.execute(
            "INSERT INTO paper_events (event_uid,event_type,occurred_at,recorded_at,"
            "actor_kind,actor_role,actor_name,paper_id,to_state,reason_code) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (uid, etype, "2026-01-01", "2026-01-01", "engine", "system", "seed",
             1, to_state, reason),
        )
    conn.commit()
    conn.close()
    return path


def _objects(path, kind, tbl="paper_events"):
    conn = sqlite3.connect(path)
    try:
        return sorted(r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type=? AND tbl_name=?", (kind, tbl)))
    finally:
        conn.close()


# ── G2: seeded events preserved row-for-row ──────────────────────────

def test_the_seeded_eligibility_events_are_preserved_row_for_row(tmp_path):
    """The 190-equivalent: every seeded row keeps its id, uid and token."""
    seeds = [(f"u{i}", "state_at_migration", "eligible", None) for i in range(1, 191)]
    path = _pre_019_db(tmp_path, seeds)

    conn = sqlite3.connect(path)
    before = conn.execute(
        "SELECT event_id, event_uid, event_type, to_state, reason_code "
        "FROM paper_events ORDER BY event_id").fetchall()
    conn.close()
    assert len(before) == 190

    result = m019.run_migration(str(path))
    assert result == {"status": "executed", "rows_preserved": 190}

    conn = sqlite3.connect(path)
    after = conn.execute(
        "SELECT event_id, event_uid, event_type, to_state, reason_code "
        "FROM paper_events ORDER BY event_id").fetchall()
    conn.close()
    assert after == before, "019 must copy rows verbatim, ids included"


def test_the_column_order_is_preserved_so_the_content_fingerprint_does_not_move(tmp_path):
    """`db_fingerprint` hashes a row in PRAGMA column order (see its
    CANONICAL_SERIALIZATION). A rebuild that reorders columns changes every
    per-table hash without changing one value."""
    path = _pre_019_db(tmp_path, [("u1", "state_at_migration", "eligible", None)])
    conn = sqlite3.connect(path)
    before = [r[1] for r in conn.execute("PRAGMA table_info(paper_events)")]
    conn.close()

    m019.run_migration(str(path))

    conn = sqlite3.connect(path)
    after = [r[1] for r in conn.execute("PRAGMA table_info(paper_events)")]
    conn.close()
    assert after == before
    assert tuple(after) == m019._COLUMNS


# ── G2: the triggers come back, and survive a failure ────────────────

def test_both_append_only_triggers_are_restored(tmp_path):
    path = _pre_019_db(tmp_path, [("u1", "state_at_migration", "eligible", None)])
    assert _objects(path, "trigger") == ["paper_events_no_delete", "paper_events_no_update"]
    m019.run_migration(str(path))
    assert _objects(path, "trigger") == ["paper_events_no_delete", "paper_events_no_update"]
    # the named index AND the UNIQUE(event_uid) autoindex — per-event identity
    # is what read-out §2.2b needs, and a rebuild that dropped it would be silent
    assert _objects(path, "index") == [
        "idx_paper_events_paper", "sqlite_autoindex_paper_events_1"]


def test_a_mid_rebuild_failure_leaves_the_table_and_both_triggers_intact(tmp_path):
    """I11 — the forced-exception rehearsal.

    The raise lands after the rows are copied, which is the moment a
    non-transactional rebuild would have the old table dropped and the guard off.
    """
    path = _pre_019_db(tmp_path, [("u1", "state_at_migration", "eligible", None)])

    with pytest.raises(RuntimeError, match="forced mid-rebuild failure"):
        m019.run_migration(str(path), _fail_after_copy=True)

    conn = sqlite3.connect(path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0] == 1
        # the OLD CHECK is still in force: the rebuild did not half-land
        sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='paper_events'").fetchone()[0]
        assert "'full_text_not_obtainable'" in sql
        assert "paper_events_new_019" not in {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master")}
    finally:
        conn.close()
    assert _objects(path, "trigger") == ["paper_events_no_delete", "paper_events_no_update"]


def test_append_only_still_refuses_update_and_delete_after_019(tmp_path):
    path = _pre_019_db(tmp_path, [("u1", "state_at_migration", "eligible", None)])
    m019.run_migration(str(path))
    conn = sqlite3.connect(path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute("UPDATE paper_events SET to_state='abstract_out'")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute("DELETE FROM paper_events")
    finally:
        conn.close()


# ── G2: the reason CHECK, both directions ────────────────────────────

def _insert(conn, **kw):
    cols = dict(event_uid="x", event_type="extraction_failed", occurred_at="2026-01-01",
                recorded_at="2026-01-01", actor_kind="engine", actor_role="system",
                actor_name="t", paper_id=1, to_state="extraction_failed",
                reason_code=None)
    cols.update(kw)
    names = ", ".join(cols)
    marks = ", ".join("?" * len(cols))
    conn.execute(f"INSERT INTO paper_events ({names}) VALUES ({marks})",
                 tuple(cols.values()))


@pytest.mark.parametrize("token", paper_state.FAILURE_STATES)
def test_a_failure_token_with_no_reason_is_refused(tmp_path, token):
    path = _pre_019_db(tmp_path)
    m019.run_migration(str(path))
    conn = sqlite3.connect(path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            _insert(conn, event_uid=f"f-{token}", to_state=token, reason_code=None,
                    event_type="extraction_failed")
    finally:
        conn.close()


@pytest.mark.parametrize("token", ("parsed", "extracted", "audited_ai"))
def test_a_non_failure_token_carrying_a_reason_is_refused(tmp_path, token):
    path = _pre_019_db(tmp_path)
    m019.run_migration(str(path))
    etype = {"parsed": "parsed", "extracted": "extracted", "audited_ai": "audited"}[token]
    conn = sqlite3.connect(path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            _insert(conn, event_uid=f"n-{token}", to_state=token,
                    reason_code="something", event_type=etype)
    finally:
        conn.close()


def test_a_failure_token_with_a_reason_is_accepted(tmp_path):
    path = _pre_019_db(tmp_path)
    m019.run_migration(str(path))
    conn = sqlite3.connect(path)
    try:
        _insert(conn, event_uid="ok", to_state="extraction_failed",
                reason_code="extraction failed after retries")
        conn.commit()
        assert conn.execute(
            "SELECT reason_code FROM paper_events WHERE event_uid='ok'"
        ).fetchone()[0] == "extraction failed after retries"
    finally:
        conn.close()


# ── G2: the axis CHECK ───────────────────────────────────────────────

def test_an_eligibility_event_type_cannot_carry_a_processing_token(tmp_path):
    path = _pre_019_db(tmp_path)
    m019.run_migration(str(path))
    conn = sqlite3.connect(path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            _insert(conn, event_uid="ax1", event_type="screened", to_state="extracted")
    finally:
        conn.close()


def test_a_processing_event_type_cannot_carry_an_eligibility_token(tmp_path):
    path = _pre_019_db(tmp_path)
    m019.run_migration(str(path))
    conn = sqlite3.connect(path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            _insert(conn, event_uid="ax2", event_type="extracted", to_state="eligible")
    finally:
        conn.close()


def test_state_at_migration_may_land_on_either_axis(tmp_path):
    """The seed writes eligibility; a later seed may need a processing fact."""
    path = _pre_019_db(tmp_path)
    m019.run_migration(str(path))
    conn = sqlite3.connect(path)
    try:
        _insert(conn, event_uid="b1", event_type="state_at_migration", to_state="eligible")
        _insert(conn, event_uid="b2", event_type="state_at_migration", to_state="audited_ai")
        conn.commit()
    finally:
        conn.close()


# ── the migration refuses rather than inventing history ──────────────

def test_a_pre_existing_failure_row_with_no_reason_stops_the_migration(tmp_path):
    """R25: a reason is not reconstructable, so 019 will not invent one."""
    path = _pre_019_db(
        tmp_path, [("u1", "not_obtainable", "full_text_not_obtainable", None)])
    with pytest.raises(RuntimeError, match="will not invent one"):
        m019.run_migration(str(path))
    # and it stopped BEFORE touching anything
    conn = sqlite3.connect(path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0] == 1
        sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='paper_events'").fetchone()[0]
        assert "parse_failed" not in sql
    finally:
        conn.close()


def test_the_migration_is_idempotent_by_marker(tmp_path):
    path = _pre_019_db(tmp_path, [("u1", "state_at_migration", "eligible", None)])
    assert m019.run_migration(str(path))["status"] == "executed"
    assert m019.run_migration(str(path))["status"] == "already_applied"


def test_the_autoincrement_high_water_mark_is_not_lowered(tmp_path):
    """A rebuild resets the sequence to max(event_id); if the sequence had run
    ahead, a later insert would REUSE an id the store already issued."""
    path = _pre_019_db(tmp_path, [("u1", "state_at_migration", "eligible", None)])
    conn = sqlite3.connect(path)
    conn.execute("UPDATE sqlite_sequence SET seq=500 WHERE name='paper_events'")
    conn.commit()
    conn.close()

    m019.run_migration(str(path))

    conn = sqlite3.connect(path)
    try:
        assert conn.execute(
            "SELECT seq FROM sqlite_sequence WHERE name='paper_events'"
        ).fetchone()[0] == 500
    finally:
        conn.close()


# ── the fresh database reaches the post-019 shape through the runner ──

def test_a_fresh_review_database_carries_the_two_axis_vocabulary(tmp_path):
    """I10: the runner applies 019 on construction, so fixtures are writable
    before any live write happens."""
    from engine.core.database import ReviewDatabase

    db = ReviewDatabase(str(tmp_path / "rev"))
    try:
        ids = {r[0] for r in db._conn.execute(
            "SELECT migration_id FROM schema_migrations")}
        assert "019_paper_state_axes" in ids
        ddl = db._conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='paper_events'").fetchone()[0]
        for token in paper_state.PROCESSING_STATES:
            assert f"'{token}'" in ddl
        trig = {r[0] for r in db._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' "
            "AND tbl_name='paper_events'")}
        assert trig == {"paper_events_no_update", "paper_events_no_delete"}
    finally:
        db.close()
