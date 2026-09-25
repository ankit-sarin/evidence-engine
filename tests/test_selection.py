"""Extraction selection: the corpus predicate with the reuse key (WRITE-PATH-01 9b-2a).

T1–T9 of the 9b-2a brief, with T2/T3 widened and T2b added by its ruling R3
(any live claim-bearing event blocks, not only `asserted`) and T9 replaced by
R4's payload-read pin. Every database here is a scratch `ReviewDatabase` under
`tmp_path`; conftest's live-data fence refuses anything else.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import patch

import pytest

from engine.core import events
from engine.core.database import ReviewDatabase
from engine.core.events import (
    PAYLOAD_PARSED_TEXT_SHA256,
    PAYLOAD_PARSED_TEXT_UID,
    PAYLOAD_REUSE_KEY,
)
from engine.core.parsed_text import (
    NoParsedText,
    ParsedTextMissing,
    ParsedTextModified,
    resolve_parsed_text,
)
from engine.core.reuse_key import reuse_key
from engine.core.selection import SelectionResult, select_for_extraction
from _event_store_fixture import fixture_run, open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

ARM = "local_test_arm"
OTHER = "other_test_arm"
FIELD = "study_design"
REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("sel", data_root=tmp_path)
    for pid in (1, 2, 3):
        rdb._conn.execute(
            "INSERT INTO papers (id, title, source, status, created_at, updated_at) "
            "VALUES (?, 't', 's', 'AI_AUDIT_COMPLETE', 'n', 'n')", (pid,))
        seed_eligibility(rdb._conn, pid)
        write_parsed(rdb, pid, f"paper {pid} text\n")
    rdb._conn.commit()
    yield rdb
    rdb.close()


def _key(conn, pid, arm=ARM):
    return reuse_key(arm, pid, resolve_parsed_text(conn, pid).sha256)


def _claim(conn, pid, *, arm=ARM, key=None, event_type="asserted", payload=None):
    """One claim-bearing event on (pid, FIELD, arm) carrying `key`; its claim id."""
    uid = events.mint_extraction_uid()
    body = dict(payload or {})
    if key is not None:
        body[PAYLOAD_REUSE_KEY] = key
    events.write_field_event(
        conn, event_type=event_type, paper_id=pid, field_name=FIELD, arm=arm,
        extraction_uid=uid, value="v" if event_type == "asserted" else None,
        source_snippet="v" if event_type == "asserted" else None,
        actor_kind="model", actor_role="extractor", actor_name="m",
        payload=body, run_id=fixture_run(conn, arm))
    return events.make_claim_id(arm, uid, FIELD)


def _ids(sel: SelectionResult):
    return [pid for pid, _ in sel.to_extract]


# ── T1 ────────────────────────────────────────────────────────────────
def test_t1_no_field_events_selects_every_eligible_paper(db):
    sel = select_for_extraction(db._conn, arm=ARM)
    assert _ids(sel) == [1, 2, 3]
    assert sel.skipped_asserted == () and sel.skipped_refused == ()
    for pid, ref in sel.to_extract:
        assert ref == resolve_parsed_text(db._conn, pid)


# ── T2 / T2b (R3: any live claim-bearing event blocks) ───────────────
@pytest.mark.parametrize("event_type", ["asserted", "declined"])
def test_t2_live_claim_carrying_the_key_is_skipped(db, event_type):
    _claim(db._conn, 2, key=_key(db._conn, 2), event_type=event_type)
    sel = select_for_extraction(db._conn, arm=ARM)
    assert sel.skipped_asserted == (2,)
    assert _ids(sel) == [1, 3]


def test_t2b_paper_whose_only_live_claim_is_contract_unmet_is_skipped(db):
    _claim(db._conn, 2, key=_key(db._conn, 2), event_type="contract_unmet",
           payload={"violation_codes": ["X"], "attempts": 3})
    sel = select_for_extraction(db._conn, arm=ARM)
    assert sel.skipped_asserted == (2,)
    assert 2 not in _ids(sel)


# ── T3 ────────────────────────────────────────────────────────────────
def test_t3_superseded_claim_never_blocks(db):
    cid = _claim(db._conn, 2, key=_key(db._conn, 2))
    events.write_field_event(
        db._conn, event_type="superseded", paper_id=2, field_name=FIELD, arm=ARM,
        claim_id=cid, actor_kind="engine", actor_role="system", actor_name="w",
        against_claims={cid}, run_id=fixture_run(db._conn, ARM))
    sel = select_for_extraction(db._conn, arm=ARM)
    assert 2 in _ids(sel) and sel.skipped_asserted == ()


# ── T4 ────────────────────────────────────────────────────────────────
def test_t4_key_from_an_earlier_text_version_does_not_block(db):
    old_key = _key(db._conn, 2)
    _claim(db._conn, 2, key=old_key)
    write_parsed(db, 2, "paper 2 text, corrected\n")        # version 2
    assert _key(db._conn, 2) != old_key
    sel = select_for_extraction(db._conn, arm=ARM)
    assert 2 in _ids(sel) and sel.skipped_asserted == ()
    assert dict(sel.to_extract)[2].version == 2


# ── T5 ────────────────────────────────────────────────────────────────
def test_t5_another_arms_claim_does_not_block_this_arm(db):
    _claim(db._conn, 2, arm=OTHER, key=_key(db._conn, 2, arm=OTHER))
    assert 2 in _ids(select_for_extraction(db._conn, arm=ARM))
    assert select_for_extraction(db._conn, arm=OTHER).skipped_asserted == (2,)


# ── T6 ────────────────────────────────────────────────────────────────
def _no_text(db):
    db._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                     "updated_at) VALUES (4, 't', 's', 'PARSED', 'n', 'n')")
    seed_eligibility(db._conn, 4)
    db._conn.commit()
    return 4


def _missing(db):
    resolve_parsed_text(db._conn, 2).path.unlink()
    return 2


def _modified(db):
    resolve_parsed_text(db._conn, 2).path.write_text("edited in place\n")
    return 2


@pytest.mark.parametrize("make, exc", [
    (_no_text, NoParsedText),
    (_missing, ParsedTextMissing),
    (_modified, ParsedTextModified),
])
def test_t6_refused_text_is_reported_with_its_code_and_the_run_continues(db, make, exc,
                                                                        caplog):
    pid = make(db)
    with caplog.at_level("WARNING", logger="engine.core.selection"):
        sel = select_for_extraction(db._conn, arm=ARM)
    assert sel.skipped_refused == ((pid, exc.reason_code),)
    assert pid not in _ids(sel)
    assert set(_ids(sel)) == {1, 2, 3} - {pid}
    assert any(exc.reason_code in r.getMessage() for r in caplog.records)


def test_t6_reason_codes_are_the_ruled_three():
    assert (NoParsedText.reason_code, ParsedTextMissing.reason_code,
            ParsedTextModified.reason_code) == (
        "parsed_text_not_recorded", "parsed_text_missing", "parsed_text_modified")


# ── T7 ────────────────────────────────────────────────────────────────
def test_t7_papers_off_the_eligibility_axis_are_never_considered(db):
    for pid, state in ((5, "full_text_out"), (6, None)):
        db._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                         "updated_at) VALUES (?, 't', 's', 'PARSED', 'n', 'n')", (pid,))
        if state:
            seed_eligibility(db._conn, pid, to_state=state)
        write_parsed(db, pid, "resolvable text\n")
    db._conn.commit()
    sel = select_for_extraction(db._conn, arm=ARM)
    seen = set(_ids(sel)) | set(sel.skipped_asserted) | {p for p, _ in sel.skipped_refused}
    assert seen == {1, 2, 3}


def test_selection_is_idempotent(db):
    _claim(db._conn, 1, key=_key(db._conn, 1))
    _modified(db)
    assert select_for_extraction(db._conn, arm=ARM) == select_for_extraction(db._conn, arm=ARM)


# ── T8 ────────────────────────────────────────────────────────────────
def test_t8_extract_stage_with_nothing_selected_reports_and_runs_nothing(tmp_path):
    import scripts.run_pipeline as rp
    from engine.core.review_spec import load_review_spec
    spec = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")
    rdb = ReviewDatabase("sel_empty", data_root=tmp_path)
    try:
        run_id = open_extraction_run(rdb, spec)
        with patch.object(rp, "run_extraction") as run:
            out = rp._stage_extract(rdb, spec, "sel_empty", run_id=run_id)
        run.assert_not_called()
        assert set(out) == {"extracted", "skipped_asserted", "skipped_refused", "elapsed"}
        assert (out["extracted"], out["skipped_asserted"], out["skipped_refused"]) == (0, 0, 0)
    finally:
        rdb.close()


def test_extract_stage_hands_its_selection_to_the_run(db):
    import scripts.run_pipeline as rp
    from engine.core.review_spec import load_review_spec
    spec = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")
    run_id = open_extraction_run(db, spec)
    with patch.object(rp, "run_extraction", return_value={"extracted": 3}) as run:
        rp._stage_extract(db, spec, "sel", run_id=run_id)
    assert run.call_args.kwargs["run_id"] == run_id
    sel = run.call_args.kwargs["selection"]
    assert sel.arm == spec.extraction_models.arm and _ids(sel) == [1, 2, 3]


# ── T9 (R4) ───────────────────────────────────────────────────────────
_PAYLOAD_READ = re.compile(
    r"""payload\s*(?:\[\s*|\.get\(\s*)["'](reuse_key|parsed_text_sha256|parsed_text_uid)["']""")


def test_t9_the_pin_catches_a_literal_payload_read():
    """A pin that cannot fail pins nothing: the pattern must match the shapes it bans."""
    for s in ('ev.payload.get("reuse_key")', "payload['parsed_text_uid']",
              'e.payload[ "parsed_text_sha256" ]'):
        assert _PAYLOAD_READ.search(s), s
    assert not _PAYLOAD_READ.search("payload.get(PAYLOAD_REUSE_KEY)")


def test_t9_no_module_reads_a_payload_by_a_literal_key_name():
    assert (PAYLOAD_REUSE_KEY, PAYLOAD_PARSED_TEXT_SHA256, PAYLOAD_PARSED_TEXT_UID) == (
        "reuse_key", "parsed_text_sha256", "parsed_text_uid")
    home = REPO / "engine" / "core" / "events.py"
    excluded = {home, REPO / "engine" / "core" / "reuse_key.py"}
    hits = []
    for top in ("engine", "scripts", "analysis"):
        for f in (REPO / top).rglob("*.py"):
            if f in excluded or "migrations" in f.relative_to(REPO).parts:
                continue
            if _PAYLOAD_READ.search(f.read_text()):
                hits.append(f.relative_to(REPO).as_posix())
    assert hits == []
