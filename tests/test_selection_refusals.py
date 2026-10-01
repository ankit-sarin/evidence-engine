"""12c-D21 — a paper refused at selection is recorded, as R122 records one refused in the loop.

`select_for_extraction` stays read-only: it reports `(paper_id, reason_code)` in
`skipped_refused`. Before D21 nothing wrote those, so a paper whose parsed text
was missing, modified or never recorded sat at `no_recorded_state` and counted
in PRISMA's `extraction_in_progress` indefinitely. Now the run writes, for each,
the event the loop writes for a text refused on re-read: one processing-axis
`extraction_failed` event carrying the error's own reason code, the run's
`run_id`, actor engine/system/extractor, stage `extract_pass1`.

Written from `_stage_extract` right after selection — before `--max-papers`
bounds it and before the empty-selection early return — and from
`run_extraction`'s own selection when it is given none. Throwaway databases
only; no model is called.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from engine.agents import extractor as E
from engine.core.database import ReviewDatabase
from engine.core.parsed_text import (NoParsedText, ParsedTextMissing, ParsedTextModified,
                                     resolve_parsed_text)
from engine.core.review_spec import load_review_spec
from engine.core.selection import select_for_extraction
from engine.exporters.prisma import generate_prisma_flow
from _event_store_fixture import open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


@pytest.fixture
def spec():
    return load_review_spec(SPEC_PATH)


def _add(db, pid, *, text=True):
    db._conn.execute(
        "INSERT INTO papers (id, title, source, status, created_at, updated_at) "
        "VALUES (?, 't', 's', 'AI_AUDIT_COMPLETE', 'n', 'n')", (pid,))
    seed_eligibility(db._conn, pid)
    if text:
        write_parsed(db, pid, f"paper {pid} text\n")
    db._conn.commit()


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("refusals", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    for pid in (1, 2, 3):
        _add(rdb, pid)
    yield rdb
    rdb.close()


def _no_text(db):
    _add(db, 4, text=False)
    return 4


def _missing(db, pid=2):
    resolve_parsed_text(db._conn, pid).path.unlink()
    return pid


def _modified(db, pid=2):
    resolve_parsed_text(db._conn, pid).path.write_text("edited in place\n")
    return pid


def _failures(db, pid):
    return [dict(r) for r in db._conn.execute(
        "SELECT * FROM paper_events WHERE paper_id = ? AND event_type = 'extraction_failed'",
        (pid,)).fetchall()]


def _stage(db, spec, run_id, **kw):
    """`_stage_extract` with its downstream stubbed: the run_extraction stub
    extracts nothing, so only the selection-time write is observed."""
    import scripts.run_pipeline as rp
    with patch.object(rp, "run_extraction",
                      lambda *a, **k: {"extracted": 0, "skipped_asserted": 0,
                                       "skipped_refused": len(k["selection"].skipped_refused)}), \
            patch.object(rp, "_distribution_check", lambda *a, **k: None):
        return rp._stage_extract(db, spec, "refusals", run_id=run_id, **kw)


def _assert_one_loop_event(db, pid, reason, run_id):
    rows = _failures(db, pid)
    assert len(rows) == 1, rows
    r = rows[0]
    assert (r["reason_code"], r["run_id"], r["to_state"], r["stage_name"]) == \
        (reason, run_id, "extraction_failed", "extract_pass1")
    assert (r["actor_kind"], r["actor_role"], r["actor_name"]) == ("engine", "system", "extractor")


# (a) per reason
@pytest.mark.parametrize("make, exc", [
    (_no_text, NoParsedText), (_missing, ParsedTextMissing), (_modified, ParsedTextModified),
], ids=["not_recorded", "missing", "modified"])
def test_a_paper_refused_at_selection_gets_one_event_with_its_reason(db, spec, make, exc):
    run_id = open_extraction_run(db, spec)
    pid = make(db)
    _stage(db, spec, run_id)
    _assert_one_loop_event(db, pid, exc.reason_code, run_id)


# (b) nothing left to extract — the early return still records
def test_every_paper_refused_still_records_every_refusal(db, spec):
    run_id = open_extraction_run(db, spec)
    for pid in (1, 2, 3):
        _missing(db, pid)
    out = _stage(db, spec, run_id)
    assert out["extracted"] == 0 and out["skipped_refused"] == 3
    for pid in (1, 2, 3):
        _assert_one_loop_event(db, pid, ParsedTextMissing.reason_code, run_id)


# (c) --max-papers bounds to_extract, never the refusals
def test_a_bounded_run_records_every_refusal(db, spec):
    run_id = open_extraction_run(db, spec)
    _modified(db, 2)
    _stage(db, spec, run_id, max_papers=1)
    _assert_one_loop_event(db, 2, ParsedTextModified.reason_code, run_id)


# (d) run_extraction given no selection selects, and records, for itself
def test_the_selection_less_path_records_its_refusals(db, spec):
    run_id = open_extraction_run(db, spec)
    for pid in (1, 2, 3):
        _missing(db, pid)
    with patch("engine.utils.ollama_preflight.require_preflight"):
        stats = E.run_extraction(db, spec, "refusals", experiment_lock=False,
                                 restart_every=0, run_id=run_id)
    assert stats["skipped_refused"] == 3
    for pid in (1, 2, 3):
        _assert_one_loop_event(db, pid, ParsedTextMissing.reason_code, run_id)


def _real_stage(db, spec, run_id, side_effect):
    """`_stage_extract` with the real `run_extraction`; extraction itself raises."""
    import scripts.run_pipeline as rp
    with patch.object(E, "extract_paper_with_completeness", side_effect=side_effect), \
            patch("engine.utils.ollama_preflight.require_preflight"), \
            patch.object(rp, "_distribution_check", lambda *a, **k: None):
        return rp._stage_extract(db, spec, "refusals", run_id=run_id)


# (e) the selection _stage_extract hands to run_extraction is not recorded twice
def test_no_double_write_across_both_paths(db, spec):
    run_id = open_extraction_run(db, spec)
    _modified(db, 2)
    _real_stage(db, spec, run_id, side_effect=TimeoutError("model"))
    _assert_one_loop_event(db, 2, ParsedTextModified.reason_code, run_id)
    assert len(_failures(db, 1)) == 1 and len(_failures(db, 3)) == 1   # the loop's own


# (f) PRISMA: from extraction_in_progress to failures["extraction_failed"]
def test_prisma_moves_the_refused_paper_to_extraction_failed(db, spec):
    run_id = open_extraction_run(db, spec)
    _modified(db, 2)
    before = generate_prisma_flow(db)
    assert before["extraction_in_progress"] == 3 and before["extraction_failed"] == 0
    _stage(db, spec, run_id)
    after = generate_prisma_flow(db)
    assert after["extraction_in_progress"] == 2 and after["extraction_failed"] == 1
    assert after["failure_reasons"]["extraction_failed"] == {ParsedTextModified.reason_code: 1}


# (g) selection stays read-only; the recording is the helper's
def test_selection_writes_nothing_and_the_helper_writes_the_refusals(db, spec):
    run_id = open_extraction_run(db, spec)
    _missing(db, 2)
    before = db._conn.total_changes
    sel = select_for_extraction(db._conn, arm=spec.extraction_models.arm)
    assert db._conn.total_changes == before, "select_for_extraction wrote"
    assert sel.skipped_refused == ((2, ParsedTextMissing.reason_code),)
    E.record_selection_refusals(db, sel, run_id=run_id)
    _assert_one_loop_event(db, 2, ParsedTextMissing.reason_code, run_id)


# (h) refusals never advance the consecutive-failure abort counter
def test_selection_refusals_do_not_advance_the_abort_counter(db, spec):
    run_id = open_extraction_run(db, spec)
    for pid in (4, 5, 6):
        _add(db, pid)
        _missing(db, pid)
    # Two loop failures (papers 1, 2) then a success (3) would never abort; three
    # selection refusals counted on top would (CONSECUTIVE_FAILURE_ABORT = 3).
    outcomes = iter([TimeoutError("a"), TimeoutError("b"),
                     type("R", (), {"fields": []})()])

    def extract(*a, **k):
        o = next(outcomes)
        if isinstance(o, BaseException):
            raise o
        return o

    _real_stage(db, spec, run_id, side_effect=extract)       # no RunAborted
    for pid in (4, 5, 6):
        _assert_one_loop_event(db, pid, ParsedTextMissing.reason_code, run_id)
