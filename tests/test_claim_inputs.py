"""claim_inputs — the writer invariant (R217/D16) and selection's read of it
(10a-C4).

`claim_inputs` is the constrained, indexed authority for one extraction
call's input identity, one row per `extraction_uid`. The writer fills it at
the first claim-bearing event of a call and checks every later event of the
same `extraction_uid` against it; selection reads it instead of the payload.
"""

from __future__ import annotations

import sqlite3

import pytest

from engine.core import events
from engine.core.database import ReviewDatabase
from engine.core.events import (
    ClaimInputMismatch,
    ClaimWithoutInputIdentity,
)
from engine.core.reuse_key import reuse_key
from engine.core.selection import select_for_extraction
from _event_store_fixture import claim_identity, fixture_run, seed_claim

ARM = "local_test_arm"
FIELD_A = "study_design"
FIELD_B = "sample_size"


@pytest.fixture
def db(tmp_path):
    d = ReviewDatabase("ci", data_root=tmp_path)
    d._conn.execute(
        "INSERT INTO papers (id, title, source, created_at, updated_at) "
        "VALUES (1, 't', 's', 'n', 'n')")
    d._conn.commit()
    yield d
    d.close()


def _rows(conn):
    conn.row_factory = sqlite3.Row
    return conn.execute(
        "SELECT extraction_uid, arm, paper_id, reuse_key, parsed_text_sha256, "
        "parsed_text_uid, run_id FROM claim_inputs"
    ).fetchall()


def _write(conn, *, uid, field_name=FIELD_A, arm=ARM, paper_id=1,
          event_type="asserted", sha="5" * 64, key_override=None, run_id=None):
    ident = claim_identity(arm, paper_id, sha=sha, uid=f"puid-{sha[:8]}")
    if key_override is not None:
        ident["reuse_key"] = key_override
    if run_id is None:
        run_id = fixture_run(conn, arm)
    return events.write_field_event(
        conn, event_type=event_type, paper_id=paper_id, field_name=field_name,
        arm=arm, extraction_uid=uid, value="v", source_snippet="v",
        actor_kind="model", actor_role="extractor", actor_name="m",
        payload=ident, run_id=run_id)


# ── T1 ──────────────────────────────────────────────────────────────


def test_T1_first_claim_writes_one_row_second_event_same_uid_stays_one(db):
    conn = db._conn
    uid = events.mint_extraction_uid()
    _write(conn, uid=uid, field_name=FIELD_A)
    rows = _rows(conn)
    assert len(rows) == 1
    assert rows[0]["extraction_uid"] == uid
    assert rows[0]["arm"] == ARM
    assert rows[0]["paper_id"] == 1
    assert rows[0]["reuse_key"] == reuse_key(ARM, 1, "5" * 64)
    assert rows[0]["parsed_text_sha256"] == "5" * 64

    _write(conn, uid=uid, field_name=FIELD_B)  # same call, second field
    assert len(_rows(conn)) == 1


# ── T2 ──────────────────────────────────────────────────────────────


def test_T2_different_sha_on_same_uid_refuses_and_writes_nothing(db):
    conn = db._conn
    uid = events.mint_extraction_uid()
    _write(conn, uid=uid, field_name=FIELD_A, sha="5" * 64)
    before = _rows(conn)
    before_events = conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0]

    with pytest.raises(ClaimInputMismatch) as exc:
        _write(conn, uid=uid, field_name=FIELD_B, sha="6" * 64)

    assert uid in str(exc.value)
    assert "parsed_text_sha256" in str(exc.value)
    assert _rows(conn) == before
    assert conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == before_events


# ── T3 ──────────────────────────────────────────────────────────────


def test_T3_different_reuse_key_on_same_uid_refuses(db):
    conn = db._conn
    uid = events.mint_extraction_uid()
    _write(conn, uid=uid, field_name=FIELD_A)
    before = _rows(conn)

    with pytest.raises(ClaimInputMismatch) as exc:
        _write(conn, uid=uid, field_name=FIELD_B, key_override="a-different-key")

    assert "reuse_key" in str(exc.value)
    assert _rows(conn) == before


# ── T4 ──────────────────────────────────────────────────────────────


def test_T4_different_arm_on_same_uid_refuses(db):
    conn = db._conn
    uid = events.mint_extraction_uid()
    _write(conn, uid=uid, field_name=FIELD_A, arm=ARM)
    before = _rows(conn)

    other_arm = "other_local_arm"
    fixture_run(conn, other_arm)
    with pytest.raises(ClaimInputMismatch) as exc:
        _write(conn, uid=uid, field_name=FIELD_B, arm=other_arm)

    assert "arm" in str(exc.value)
    assert _rows(conn) == before


# ── T5 ──────────────────────────────────────────────────────────────


def test_T5_missing_payload_keys_refuses_as_before_no_claim_inputs_row(db):
    conn = db._conn
    run_id = fixture_run(conn, ARM)
    with pytest.raises(ClaimWithoutInputIdentity):
        events.write_field_event(
            conn, event_type="asserted", paper_id=1, field_name=FIELD_A, arm=ARM,
            extraction_uid=events.mint_extraction_uid(), value="v", source_snippet="v",
            actor_kind="model", actor_role="extractor", actor_name="m",
            payload={}, run_id=run_id)
    assert _rows(conn) == []


# ── T6 ──────────────────────────────────────────────────────────────


def test_T6_citation_located_on_a_pre_manifest_arm_writes_no_claim_inputs_row(db):
    conn = db._conn
    events.register_arm(conn, "premanifest_arm", "model",
                        configuration_marker="not recorded (pre-manifest)")
    claim_id = seed_claim(conn, arm="premanifest_arm", paper_id=1, field_name=FIELD_A,
                          value="5")
    conn.commit()
    events.write_field_event(
        conn, event_type="citation_located", paper_id=1, field_name=FIELD_A,
        arm="premanifest_arm", claim_id=claim_id,
        actor_kind="engine", actor_role="system", actor_name="locator",
        payload={"located": True}, run_id=fixture_run(conn))
    row = conn.execute(
        "SELECT 1 FROM field_events WHERE claim_id = ? AND event_type = 'citation_located'",
        (claim_id,)).fetchone()
    assert row is not None
    assert _rows(conn) == []


# ── Selection: T7–T10 ──────────────────────────────────────────────


@pytest.fixture
def sel_db(tmp_path):
    from _event_store_fixture import seed_eligibility
    from _parsed_text_fixture import write_parsed

    d = ReviewDatabase("ci_sel", data_root=tmp_path)
    for pid in (1, 2):
        d._conn.execute(
            "INSERT INTO papers (id, title, source, status, created_at, updated_at) "
            "VALUES (?, 't', 's', 'AI_AUDIT_COMPLETE', 'n', 'n')", (pid,))
        seed_eligibility(d._conn, pid)
        write_parsed(d, pid, f"paper {pid} text\n")
    d._conn.commit()
    yield d
    d.close()


def _text_sha(conn, pid):
    from engine.core.parsed_text import resolve_parsed_text
    return resolve_parsed_text(conn, pid).sha256


def test_T7_matching_claim_inputs_and_live_claim_is_skipped(sel_db):
    conn = sel_db._conn
    uid = events.mint_extraction_uid()
    sha = _text_sha(conn, 1)
    _write(conn, uid=uid, field_name=FIELD_A, paper_id=1, sha=sha)
    sel = select_for_extraction(conn, arm=ARM)
    assert sel.skipped_asserted == (1,)
    assert 1 not in [pid for pid, _ in sel.to_extract]


def test_T8_superseded_claim_under_the_old_key_does_not_block(sel_db):
    conn = sel_db._conn
    uid = events.mint_extraction_uid()
    sha = _text_sha(conn, 1)
    claim_id = events.make_claim_id(ARM, uid, FIELD_A)
    _write(conn, uid=uid, field_name=FIELD_A, paper_id=1, sha=sha)
    events.write_field_event(
        conn, event_type="superseded", paper_id=1, field_name=FIELD_A, arm=ARM,
        claim_id=claim_id, actor_kind="engine", actor_role="system", actor_name="w",
        against_claims={claim_id}, run_id=fixture_run(conn, ARM))
    sel = select_for_extraction(conn, arm=ARM)
    assert 1 in [pid for pid, _ in sel.to_extract]
    assert sel.skipped_asserted == ()


def test_T9_claim_inputs_row_with_no_live_claim_does_not_block(sel_db):
    """A claim_inputs row can outlive its field_events row only in a
    contrived fixture (the writer always writes both together) — this proves
    the join, not just the payload copy, gates the skip: no LIVE claim means
    no skip, even with a matching claim_inputs row sitting in the table."""
    conn = sel_db._conn
    uid = events.mint_extraction_uid()
    sha = _text_sha(conn, 1)
    key = reuse_key(ARM, 1, sha)
    run_id = fixture_run(conn, ARM)
    conn.execute(
        "INSERT INTO claim_inputs (extraction_uid, arm, paper_id, reuse_key, "
        "parsed_text_sha256, parsed_text_uid, run_id, recorded_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'now')",
        (uid, ARM, 1, key, sha, "puid", run_id))
    conn.commit()
    sel = select_for_extraction(conn, arm=ARM)
    assert 1 in [pid for pid, _ in sel.to_extract]
    assert sel.skipped_asserted == ()


def test_T10_payload_reuse_key_matches_but_no_claim_inputs_row_does_not_block(sel_db):
    """The pre-manifest / migration-seed path (run_id NULL): a live claim
    whose payload carries the right reuse_key but has no claim_inputs row
    (R150 — never written by the writer for such a row) must NOT block —
    proving selection's authority moved off the payload entirely."""
    conn = sel_db._conn
    sha = _text_sha(conn, 1)
    key = reuse_key(ARM, 1, sha)
    events.register_arm(conn, ARM, "model",
                        configuration_marker="not recorded (pre-manifest)")
    seed_claim(conn, arm=ARM, paper_id=1, field_name=FIELD_A, value="5",
              payload={"reuse_key": key, "parsed_text_sha256": sha,
                       "parsed_text_uid": "puid"})
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM claim_inputs").fetchone()[0] == 0

    sel = select_for_extraction(conn, arm=ARM)
    assert 1 in [pid for pid, _ in sel.to_extract]
    assert sel.skipped_asserted == ()
