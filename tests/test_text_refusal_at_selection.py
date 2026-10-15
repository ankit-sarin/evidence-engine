"""P1 unit U11 (R612): a missing or changed text file on an already-extracted
paper is reported, not re-failed on every run (INT-i12-69).

Selection read the parsed-text file before it asked whether the arm already
held the paper under its key. So a paper whose recorded text file had gone
missing or been altered was refused whether or not it was already extracted:
every run wrote another `extraction_failed` event, and its processing state
moved from `audited_ai` to `extraction_failed` while its claims stayed live.

Now selection decides "already extracted under this key" first, from the
recorded reference. Such a paper is neither selected nor refused; its file's
condition is reported — in the selection result, the log, the run's telemetry
and the run summary. A paper with no claims under its key is refused as before,
and its refusal event is written once, not once per run.

Scratch databases under `tmp_path`; the model passes are stubbed. Nothing here
reaches a model, a service or the live review.
"""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

import scripts.run_pipeline as rp
from engine.agents import extractor as E
from engine.agents.models import EvidenceSpan, ExtractionResult
from engine.core import events
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.effective import effective_state, live_claim_events
from engine.core.extraction_events import claimed_text_unreadable_entries
from engine.core.parsed_text import resolve_parsed_text
from engine.core.review_spec import load_review_spec
from engine.core.run_telemetry import read_run_events
from engine.core.selection import select_for_extraction
from _event_store_fixture import open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
TEXT = "The paper reports a trial. A clean sentence.\n"
HASH = "d" * 64
DONE, FRESH = 7, 8            # one paper extracted and audited, one never extracted
MISSING, MODIFIED = "parsed_text_missing", "parsed_text_modified"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture(scope="module")
def names():
    return tuple(f["name"] for f in load_codebook(LIVE_CODEBOOK).fields)


def _add(rdb, pid):
    rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                      "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
    seed_eligibility(rdb._conn, pid)
    path = write_parsed(rdb, pid, TEXT)
    rdb._conn.commit()
    return path


@pytest.fixture
def db(tmp_path, spec, names, monkeypatch):
    """A review with paper DONE extracted (20 live claims) and audited under its
    key. Returns (database, DONE's parsed-text path)."""
    rdb = ReviewDatabase("sel11", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    path = _add(rdb, DONE)

    def pass2(prompt, trace, spec_, paper_id, **kw):
        spans = [EvidenceSpan(field_name=n, value="RCT", source_snippet="A clean sentence.",
                              confidence=0.9, tier=1) for n in names]
        return ExtractionResult(paper_id=paper_id, fields=spans, reasoning_trace="t",
                                model="deepseek-r1:32b", codebook_hash="h",
                                extracted_at="2026-10-12T00:00:00Z"), HASH
    monkeypatch.setattr(E, "extract_pass1_reasoning", lambda *a, **k: ("trace", HASH))
    monkeypatch.setattr(E, "extract_pass2_structured", pass2)
    monkeypatch.setattr(E, "_validate_and_retry_snippets",
                        lambda fields, *a, **k: (fields, {}, ()))
    run_id = open_extraction_run(rdb, spec)
    with patch("engine.utils.ollama_preflight.require_preflight"):
        stats = E.run_extraction(rdb, spec, "sel11", experiment_lock=False,
                                 restart_every=0, run_id=run_id)
    assert stats["extracted"] == 1
    events.write_paper_event(rdb._conn, event_type="audited", paper_id=DONE,
                             to_state="audited_ai", from_state="extracted",
                             actor_kind="engine", actor_role="system", actor_name="auditor",
                             run_id=run_id)
    rdb._conn.commit()
    assert effective_state(rdb._conn, DONE).processing == "audited_ai"
    yield rdb, path
    rdb.close()


def _failed_events(rdb, pid):
    return [tuple(r) for r in rdb._conn.execute(
        "SELECT run_id, reason_code FROM paper_events WHERE paper_id = ? "
        "AND event_type = 'extraction_failed' ORDER BY event_id", (pid,))]


def _claims(rdb, spec, pid):
    return sorted(ev.claim_id for ev in live_claim_events(
        rdb._conn, pid, spec.extraction_models.arm))


def _extract_stage(rdb, spec):
    """One run reaching the extract stage through `run_pipeline._stage_extract`."""
    run_id = open_extraction_run(rdb, spec)
    with patch("engine.utils.ollama_preflight.require_preflight"):
        stats = rp._stage_extract(rdb, spec, "sel11", run_id=run_id)
    return run_id, stats


def _reported(rdb, run_id):
    return [e["payload"] for e in read_run_events(Path(rdb.db_path).parent)
            if e["run_id"] == run_id and e["kind"] == "claimed_papers_text_unreadable"]


# ── gate 1 ────────────────────────────────────────────────────────────
def test_a_missing_text_on_an_audited_paper_is_not_re_failed_each_run(db, spec, caplog):
    rdb, path = db
    arm = spec.extraction_models.arm
    claims_before = _claims(rdb, spec, DONE)
    assert len(claims_before) == 20
    stored_path = resolve_parsed_text(rdb._conn, DONE).stored_path
    path.unlink()

    runs = []
    for _ in range(2):
        with caplog.at_level(logging.WARNING, logger="engine.core.selection"):
            sel = select_for_extraction(rdb._conn, arm=arm)
        # neither selected nor refused: it is already extracted under its key
        assert sel.skipped_asserted == (DONE,) and sel.to_extract == ()
        assert sel.skipped_refused == ()
        assert sel.claimed_text_unreadable == ((DONE, MISSING, stored_path),)
        runs.append(_extract_stage(rdb, spec))

    # no refusal event, on either run; state and live claims as they were
    assert _failed_events(rdb, DONE) == []
    assert effective_state(rdb._conn, DONE).processing == "audited_ai"
    assert _claims(rdb, spec, DONE) == claims_before

    # and the missing file is reported: in the log, and in each run's telemetry
    said = [r.getMessage() for r in caplog.records]
    assert any(f"Paper {DONE}" in m and MISSING in m and stored_path in m for m in said), said
    for run_id, stats in runs:
        assert stats["skipped_asserted"] == 1 and stats["skipped_refused"] == 0
        assert _reported(rdb, run_id) == [{
            "run_id": run_id, "arm": arm,
            "papers": [{"paper_id": DONE, "condition": MISSING, "path": stored_path}]}]


# ── gate 2 ────────────────────────────────────────────────────────────
def test_changed_bytes_on_an_audited_paper_are_reported_as_changed(db, spec):
    rdb, path = db
    claims_before = _claims(rdb, spec, DONE)
    stored_path = resolve_parsed_text(rdb._conn, DONE).stored_path
    path.write_text(TEXT + "An added line.\n")

    run_1, _ = _extract_stage(rdb, spec)
    run_2, _ = _extract_stage(rdb, spec)
    assert _failed_events(rdb, DONE) == []
    assert effective_state(rdb._conn, DONE).processing == "audited_ai"
    assert _claims(rdb, spec, DONE) == claims_before
    for run_id in (run_1, run_2):
        (payload,) = _reported(rdb, run_id)
        assert payload["papers"] == [{"paper_id": DONE, "condition": MODIFIED,
                                      "path": stored_path}]


def test_the_run_summary_names_the_paper_when_other_papers_are_extracted(db, spec):
    """With something to extract, the run goes through `run_extraction` and its
    summary carries the entry."""
    rdb, path = db
    stored_path = resolve_parsed_text(rdb._conn, DONE).stored_path
    path.unlink()
    _add(rdb, FRESH)
    run_id, stats = _extract_stage(rdb, spec)
    assert stats["extracted"] == 1 and stats["skipped_asserted"] == 1
    assert stats["claimed_text_unreadable"] == [
        {"paper_id": DONE, "condition": MISSING, "path": stored_path}]
    assert effective_state(rdb._conn, DONE).processing == "audited_ai"


# ── gate 3 ────────────────────────────────────────────────────────────
def test_a_paper_with_no_claims_and_a_missing_file_is_refused_once(db, spec):
    rdb, _ = db
    arm = spec.extraction_models.arm
    fresh_path = _add(rdb, FRESH)
    fresh_path.unlink()

    sel = select_for_extraction(rdb._conn, arm=arm)
    assert sel.skipped_refused == ((FRESH, MISSING),)
    run_1, stats_1 = _extract_stage(rdb, spec)
    run_2, stats_2 = _extract_stage(rdb, spec)
    # refused on both runs, written once
    assert stats_1["skipped_refused"] == stats_2["skipped_refused"] == 1
    assert _failed_events(rdb, FRESH) == [(run_1, MISSING)]
    assert effective_state(rdb._conn, FRESH).processing == "extraction_failed"
    assert sel.claimed_text_unreadable == ()        # it holds no claim: refused, not reported
    # a refusal for a DIFFERENT reason is a new fact and is written
    fresh_path.write_text("Different bytes.\n")
    run_3, _ = _extract_stage(rdb, spec)
    assert _failed_events(rdb, FRESH) == [(run_1, MISSING), (run_3, MODIFIED)]
    # the file restored, the paper is selected again
    fresh_path.write_text(TEXT)
    assert [p for p, _ in select_for_extraction(rdb._conn, arm=arm).to_extract] == [FRESH]


# ── INT-l12-25 (U12 commit 8, R643(f)) ────────────────────────────────
# On an empty selection `run_pipeline._stage_extract` returned before
# `run_extraction`, with three keys of its own, so the run summary — and in it the
# `claimed_text_unreadable` entry this module is about — never reached the stats.
def test_an_empty_selection_returns_the_populated_paths_key_set(db, spec):
    """The key set is the populated path's own, taken from a populated run on the
    same review — never written down here."""
    rdb, path = db
    path.unlink()
    _add(rdb, FRESH)
    _, populated = _extract_stage(rdb, spec)      # FRESH extracted, through run_extraction
    assert populated["extracted"] == 1
    _, empty = _extract_stage(rdb, spec)          # nothing left to extract: the early return
    assert empty["extracted"] == 0
    assert set(empty) == set(populated)


def test_an_empty_selection_reports_the_selections_unreadable_entries(db, spec):
    """F1's case: every paper already extracted, one with its text file missing.
    Gate 1's setup."""
    rdb, path = db
    path.unlink()
    sel = select_for_extraction(rdb._conn, arm=spec.extraction_models.arm)
    assert sel.to_extract == () and sel.claimed_text_unreadable
    _, stats = _extract_stage(rdb, spec)
    assert stats["claimed_text_unreadable"] == claimed_text_unreadable_entries(sel)
