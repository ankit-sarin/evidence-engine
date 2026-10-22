"""P1 unit U16 (R612, R685(e), R687): the AI-audit workflow stage completes on the whole corpus,
not on one paper (INT-i12-73).

`run_pipeline._advance_extraction_workflow` completed AI_AUDIT_COMPLETE_STAGE "once any" corpus
paper was `audited_ai`, so a run whose audit left extracted papers unaudited still completed it
(12u-U16A: 5 of 7). R685(e): the stage completes only when no eligible paper's processing state is
`extracted`; when it does not, the advance names each paper holding it pending. An admitted paper
at `extracted` with no claim to locate (an all-declined record) is audited vacuously, with a
zero-located payload and one log line; a paper whose text the audit refuses (D23) stays
`extracted`, is listed on the audit report with its reason, and holds the stage pending.

Each test drives run_pipeline's own stage functions — `_stage_extract`, `_stage_audit`, then
`_advance_extraction_workflow`, run_pipeline's order — on a scratch review under `tmp_path`, with
the model passes stubbed as tests/test_text_refusal_at_selection.py stubs them and
`semantic_verify` patched. Nothing here reaches a model, a service or the live review.

T4 (a failing advance is not swallowed) is C56 handler 3's (R685(e)(iv)); T5 (a second arm) is
A15's under F15 (R687(b)). Neither is written here.
"""

from __future__ import annotations

import logging
import re
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

import scripts.run_pipeline as rp
from engine.adjudication.workflow import ensure_workflow_table, is_stage_done
from engine.agents import audit_events as AE
from engine.agents import extractor as E
from engine.agents.auditor import AuditVerdict
from engine.agents.models import EvidenceSpan, ExtractionResult
from engine.core import events
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.effective import effective_state, live_claim_events
from engine.core.parsed_text import resolve_parsed_text
from engine.core.review_spec import load_review_spec
from _event_store_fixture import (FIXTURE_CONTEXT_SHA, claim_identity, open_extraction_run,
                                  seed_eligibility, seed_processing)
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
TEXT = "The paper reports a trial. A clean sentence.\n"
SNIP = "A clean sentence."
HASH = "d" * 64
STAGE = "AI_AUDIT_COMPLETE_STAGE"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture(scope="module")
def names():
    return tuple(f["name"] for f in load_codebook(LIVE_CODEBOOK).fields)


@pytest.fixture
def db(tmp_path, names, monkeypatch):
    """A scratch review, the model passes stubbed, the auditor's model call patched."""
    rdb = ReviewDatabase("u16", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    ensure_workflow_table(rdb._conn)

    def pass2(prompt, trace, spec_, paper_id, **kw):
        spans = [EvidenceSpan(field_name=n, value="RCT", source_snippet=SNIP,
                              confidence=0.9, tier=1) for n in names]
        return ExtractionResult(paper_id=paper_id, fields=spans, reasoning_trace="t",
                                model="deepseek-r1:32b", codebook_hash="h",
                                extracted_at="2026-10-22T00:00:00Z"), HASH
    monkeypatch.setattr(E, "extract_pass1_reasoning", lambda *a, **k: ("trace", HASH))
    monkeypatch.setattr(E, "extract_pass2_structured", pass2)
    monkeypatch.setattr(E, "_validate_and_retry_snippets",
                        lambda fields, *a, **k: (fields, {}, ()))
    monkeypatch.setattr(AE, "semantic_verify", lambda *a, **k: AuditVerdict(
        status="flagged", grep_found=False, reasoning="stub"))
    with patch("engine.utils.ollama_preflight.require_preflight"):
        yield rdb
    rdb.close()


def _add(rdb, pid, text=TEXT) -> Path:
    rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                      "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
    seed_eligibility(rdb._conn, pid)
    path = write_parsed(rdb, pid, text)
    rdb._conn.commit()
    return path


def _extract(rdb, spec) -> int:
    run_id = open_extraction_run(rdb, spec)
    rp._stage_extract(rdb, spec, "u16", run_id=run_id)
    return run_id


def _audit(rdb, spec, run_id) -> dict:
    return rp._stage_audit(rdb, "u16", spec, run_id=run_id)


def _listed(obj, pid) -> bool:
    """True when `pid` appears as a list/tuple member, the head of a tuple, or a dict's
    `paper_id`, anywhere inside `obj` — not as a bare count."""
    if isinstance(obj, dict):
        return obj.get("paper_id") == pid or any(_listed(v, pid) for v in obj.values())
    if isinstance(obj, (list, tuple)):
        return any(x == pid or (isinstance(x, (list, tuple)) and x and x[0] == pid)
                   or _listed(x, pid) for x in obj)
    return False


def _lines(caplog, *, logger=None):
    return [r.getMessage() for r in caplog.records if logger is None or r.name == logger]


def _names(line: str, pid: int) -> bool:
    return re.search(rf"\b{pid}\b", line) is not None


# ── T1 (P1A's stage test; fixture per R687(c)) ────────────────────────
def test_ai_audit_stage_completes_only_when_every_extracted_corpus_paper_is_audited(
        db, spec, caplog):
    """Three eligible papers: 201 audited_ai, 202 extracted with live unlocated claims, 203
    extraction_failed. The stage stays pending because 202 is unaudited, and the shortfall names
    202; once 202 is audited it completes."""
    _add(db, 201)
    _add(db, 203).unlink()                          # refused at selection: extraction_failed
    run_1 = _extract(db, spec)
    _audit(db, spec, run_1)                         # 201 audited
    _add(db, 202)
    run_2 = _extract(db, spec)                      # 202 extracted, its claims unlocated
    assert effective_state(db._conn, 201).processing == "audited_ai"
    assert effective_state(db._conn, 202).processing == "extracted"
    assert effective_state(db._conn, 203).processing == "extraction_failed"
    assert len(live_claim_events(db._conn, 202, spec.extraction_models.arm,
                                 event_types=("asserted",))) == len(load_codebook(LIVE_CODEBOOK).fields)

    with caplog.at_level(logging.INFO):
        rp._advance_extraction_workflow(db._conn)
    assert not is_stage_done(db._conn, STAGE)
    said = [m for m in _lines(caplog, logger="pipeline") if _names(m, 202)]
    assert len(said) == 1 and "extracted" in said[0], said

    _audit(db, spec, run_2)                         # 202 audited
    rp._advance_extraction_workflow(db._conn)
    assert is_stage_done(db._conn, STAGE)


# ── T2 / TV2 (the refused-text case, R685(e)(iii)) ────────────────────
def _refused_world(db, spec):
    _add(db, 211)
    p217, p218 = _add(db, 217), _add(db, 218)
    run_id = _extract(db, spec)                     # 211, 217, 218 extracted
    p217.write_text(TEXT + "Changed before audit.\n")
    p218.unlink()
    return run_id


def test_a_text_refused_at_audit_leaves_the_ai_audit_stage_pending_and_named(db, spec, caplog):
    run_id = _refused_world(db, spec)
    with caplog.at_level(logging.INFO):
        _audit(db, spec, run_id)
        rp._advance_extraction_workflow(db._conn)
    said = _lines(caplog)
    assert any(_names(m, 217) and "parsed_text_modified" in m for m in said), said
    assert any(_names(m, 218) and "parsed_text_missing" in m for m in said), said
    assert not is_stage_done(db._conn, STAGE)


def test_tv2_a_paper_refused_under_d23_stays_extracted_is_named_and_holds_the_stage(db, spec):
    run_id = _refused_world(db, spec)
    report = _audit(db, spec, run_id)
    rp._advance_extraction_workflow(db._conn)
    for pid, reason in ((217, "parsed_text_modified"), (218, "parsed_text_missing")):
        assert effective_state(db._conn, pid).processing == "extracted"
        assert [pid, reason] in [list(x) for x in report["skipped_refused"]]
    assert effective_state(db._conn, 211).processing == "audited_ai"
    assert not is_stage_done(db._conn, STAGE)


# ── T3 / TV1 (the all-declined case, R685(e)(ii)) ─────────────────────
def _declined_world(db, spec, names):
    _add(db, 221)
    run_id = _extract(db, spec)                     # 221 extracted (20 asserted claims)
    _add(db, 222)                                   # 222: every live claim declined
    ref = resolve_parsed_text(db._conn, 222)
    arm = spec.extraction_models.arm
    for n in names:
        events.write_field_event(
            db._conn, run_id=run_id, event_type="declined", paper_id=222, field_name=n, arm=arm,
            extraction_uid=events.mint_extraction_uid(), actor_kind="model",
            actor_role="extractor", actor_name="deepseek-r1:32b",
            payload=claim_identity(arm, 222, sha=ref.sha256, uid=ref.parsed_text_uid),
            presented_context_sha256=FIXTURE_CONTEXT_SHA, commit=False)
    seed_processing(db._conn, 222, "extracted", run_id=run_id)
    db._conn.commit()
    assert {e.event_type for e in live_claim_events(db._conn, 222, arm)} == {"declined"}
    return run_id


def _audited_events(db, pid):
    import json
    return [json.loads(r[0] or "{}") for r in db._conn.execute(
        "SELECT payload_json FROM paper_events WHERE paper_id = ? AND to_state = 'audited_ai'",
        (pid,))]


def test_an_extracted_paper_with_nothing_to_audit_is_reported_not_silent(db, spec, names, caplog):
    run_id = _declined_world(db, spec, names)
    with caplog.at_level(logging.INFO):
        _audit(db, spec, run_id)
    said = [m for m in _lines(caplog) if _names(m, 222)]
    assert len(said) == 1, said
    assert effective_state(db._conn, 222).processing == "audited_ai"


def test_tv1_an_all_declined_paper_is_audited_vacuously_and_the_stage_completes(
        db, spec, names, caplog):
    run_id = _declined_world(db, spec, names)
    with caplog.at_level(logging.INFO, logger="engine.agents.audit_events"):
        report = _audit(db, spec, run_id)
    audited = _audited_events(db, 222)
    assert len(audited) == 1, audited
    assert audited[0].get("located") == 0
    assert effective_state(db._conn, 222).processing == "audited_ai"
    assert len([m for m in _lines(caplog, logger="engine.agents.audit_events")
                if _names(m, 222)]) == 1
    assert report["papers_audited"] == 2 or _listed(report, 222)
    rp._advance_extraction_workflow(db._conn)
    assert is_stage_done(db._conn, STAGE)
