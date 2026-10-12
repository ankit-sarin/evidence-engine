"""P1 unit U07 (R612, R619(c)): the write-boundary guard on blank values and
non-value tokens, and stored records counted as extracted.

INT-i12-2 — a Pass-2 value that is empty or whitespace is not a value. With
evidence behind it it used to pass both guard modes and be stored as an
`asserted`, non-sentinel value. It is an offender now, `BLANK_VALUE`, in strict
and legacy mode; after the retry budget the field is `contract_unmet`.
INT-i12-6, N13 — on the legacy path the escape token and the contract-unmet
token are not values the prompt offers; one returned as a value is an offender,
`NON_VALUE_TOKEN_AS_VALUE`, and is never asserted.
INT-l12-20 — a record stored as `extracted` after the retry budget is counted
extracted: in the run's stats, on its progress line, in the log, and in the
counts the distribution check receives.

Scratch databases under `tmp_path`; the model passes are stubbed. Nothing here
reaches a model, a service or the live review.
"""

from __future__ import annotations

import json
import logging
import shutil
import types
from pathlib import Path
from unittest.mock import patch

import pytest

import scripts.run_pipeline as rp
from engine.agents import extractor as E
from engine.agents.models import EvidenceSpan, ExtractionOutput, ExtractionResult
from engine.core import citation_guard as G
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.elicitation import classes as C
from engine.elicitation import pipeline as PL
from engine.utils.progress import ProgressReporter
from _event_store_fixture import open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
PID = 7
TEXT = "The paper reports a trial. A clean sentence.\n"
HASH = "d" * 64
ESCAPE, UNMET = "NO_EVIDENCE_LOCATABLE", "CONTRACT_UNMET"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture(scope="module")
def elicited(spec):
    return spec.model_copy(update={
        "extraction_models": spec.extraction_models.model_copy(update={"elicitation": True})})


@pytest.fixture(scope="module")
def cb():
    return load_codebook(LIVE_CODEBOOK)


@pytest.fixture(scope="module")
def names(cb):
    assert (cb.escape_token, cb.raw["contract_unmet_token"]) == (ESCAPE, UNMET)
    return tuple(f["name"] for f in cb.fields)


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("guard", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                      "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (PID,))
    seed_eligibility(rdb._conn, PID)
    write_parsed(rdb, PID, TEXT)
    rdb._conn.commit()
    yield rdb
    rdb.close()


def _run(db, spec, run_id):
    with patch("engine.utils.ollama_preflight.require_preflight"):
        return E.run_extraction(db, spec, "guard", experiment_lock=False,
                                restart_every=0, run_id=run_id)


def _claims(db) -> dict[str, tuple[str, str | None, dict]]:
    return {r[1]: (r[0], r[2], json.loads(r[3])) for r in db._conn.execute(
        "SELECT event_type, field_name, value, payload_json FROM field_events "
        "WHERE paper_id = ? ORDER BY event_id", (PID,))}


def _response(content):
    return (types.SimpleNamespace(
        message=types.SimpleNamespace(content=content, thinking="reasoning"),
        done_reason="stop", prompt_eval_count=1234, eval_count=99), HASH)


def _good_pass1(cb) -> str:
    out = []
    for f in cb.fields:
        cls = C.field_class(f["name"], cb.raw)
        if cls == C.STATED:
            out.append({"field_name": f["name"], "unit_indices": [1], "value": "v"})
        elif cls == C.INFERABLE:
            out.append({"field_name": f["name"], "unit_indices": [1],
                        "inference": "The trial reported fixes the value.", "value": "v"})
        else:
            out.append({"field_name": f["name"], "value": "v", "reasoning_steps": [
                {"step": "The trial is described.", "unit_indices": [1]}]})
    return json.dumps({"fields": out})


def _legacy(db, spec, names, spans_for, monkeypatch):
    """A legacy run whose Pass 2 answers `spans_for(name) -> (value, snippet)`
    for every field, on every attempt."""
    def pass2(prompt, trace, spec_, paper_id, **kw):
        spans = [EvidenceSpan(field_name=n, value=spans_for(n)[0],
                              source_snippet=spans_for(n)[1], confidence=0.9, tier=1)
                 for n in names if spans_for(n) is not None]
        return ExtractionResult(paper_id=paper_id, fields=spans, reasoning_trace="t",
                                model="deepseek-r1:32b", codebook_hash="h",
                                extracted_at="2026-10-12T00:00:00Z"), HASH
    monkeypatch.setattr(E, "extract_pass1_reasoning", lambda *a, **k: ("trace", HASH))
    monkeypatch.setattr(E, "extract_pass2_structured", pass2)
    monkeypatch.setattr(E, "_validate_and_retry_snippets",
                        lambda fields, *a, **k: (fields, {}, ()))
    return open_extraction_run(db, spec)


# ── INT-i12-2, INT-i12-6 ──────────────────────────────────────────────
def test_a_blank_pass2_value_with_evidence_is_refused_in_both_modes(
        db, spec, elicited, cb, names, monkeypatch):
    blank_field = names[0]
    # the predicate: "" and whitespace with evidence are offenders, in both modes
    for value in ("", "   ", "\n\t "):
        span = [{"field_name": "f", "value": value, "source_snippet": "A clean sentence."}]
        for mode in (G.STRICT, G.LEGACY):
            r = G.check_citations(span, escape_token=ESCAPE, mode=mode,
                                  contract_unmet_token=UNMET)
            assert r.offenders == (("f", G.BLANK_VALUE),), (value, mode)
        # and by validated citation count, as the elicited path supplies it
        r = G.check_citations([{"field_name": "f", "value": value, "source_snippet": ""}],
                              escape_token=ESCAPE, mode=G.STRICT, citation_counts={"f": 2},
                              contract_unmet_token=UNMET)
        assert r.offenders == (("f", G.BLANK_VALUE),)
    # a value with evidence is still fine, and an uncited value keeps its own code
    ok = G.check_citations([{"field_name": "f", "value": "RCT", "source_snippet": "s"}],
                           escape_token=ESCAPE, mode=G.STRICT)
    assert ok.ok
    assert G.check_citations([{"field_name": "f", "value": "RCT", "source_snippet": ""}],
                             escape_token=ESCAPE, mode=G.STRICT).offenders == (
        ("f", G.VALUE_WITHOUT_CITATION),)

    # through the elicited path: Pass 1 evidences every field, Pass 2 returns a
    # blank value for one of them on every attempt
    pass2 = ExtractionOutput(fields=[
        EvidenceSpan(field_name=f["name"], value="  " if f["name"] == blank_field else "v",
                     source_snippet="typed by the model", confidence=0.9, tier=f["tier"])
        for f in cb.fields]).model_dump_json()
    monkeypatch.setattr(PL, "ollama_chat", lambda **kw: _response(_good_pass1(cb)))
    monkeypatch.setattr(E, "ollama_chat", lambda **kw: _response(pass2))
    _run(db, elicited, open_extraction_run(db, elicited))
    claims = _claims(db)
    kind, value, payload = claims[blank_field]
    assert (kind, value) == ("contract_unmet", None)
    # three attempts were refused the same way: three were spent, and the attempt
    # rule (U06) kept the first on the tie (R620(c))
    assert payload["violation_codes"] == [G.BLANK_VALUE]
    assert (payload["attempts"], payload["kept_attempt"]) == (3, 1)
    asserted = {n: c for n, c in claims.items() if c[0] == "asserted"}
    assert len(asserted) == 19 and all((c[1] or "").strip() for c in asserted.values())


def test_on_the_legacy_path_a_non_value_token_is_never_asserted(db, spec, names, monkeypatch):
    escape_field, unmet_field, blank_field = names[0], names[1], names[2]
    # the predicate, legacy mode: neither token is a value the legacy prompt offers
    for token, snippet in ((ESCAPE, ""), (ESCAPE, "A clean sentence."),
                           (UNMET, ""), (UNMET, "A clean sentence."), (ESCAPE.lower(), "")):
        r = G.check_citations([{"field_name": "f", "value": token, "source_snippet": snippet}],
                              escape_token=ESCAPE, mode=G.LEGACY, contract_unmet_token=UNMET)
        assert r.offenders == (("f", G.NON_VALUE_TOKEN_AS_VALUE),), (token, snippet)
    # strict mode is untouched: there the tokens are the engine's own states
    for token in (ESCAPE, UNMET):
        assert G.check_citations([{"field_name": "f", "value": token, "source_snippet": ""}],
                                 escape_token=ESCAPE, mode=G.STRICT,
                                 contract_unmet_token=UNMET).ok

    def spans_for(name):
        return {escape_field: (ESCAPE, ""), unmet_field: (UNMET, "A clean sentence."),
                blank_field: ("", "A clean sentence.")}.get(name, ("RCT", "A clean sentence."))
    run_id = _legacy(db, spec, names, spans_for, monkeypatch)
    _run(db, spec, run_id)
    claims = _claims(db)
    assert claims[escape_field][0] == claims[unmet_field][0] == "contract_unmet"
    assert claims[escape_field][2]["violation_codes"] == [G.NON_VALUE_TOKEN_AS_VALUE]
    assert claims[unmet_field][2]["violation_codes"] == [G.NON_VALUE_TOKEN_AS_VALUE]
    assert (claims[blank_field][0], claims[blank_field][2]["violation_codes"]) == (
        "contract_unmet", [G.BLANK_VALUE])
    values = {c[1] for c in claims.values() if c[0] == "asserted"}
    assert values == {"RCT"} and len(claims) == 20


# ── INT-l12-20 ────────────────────────────────────────────────────────
def test_a_stored_record_with_incomplete_fields_is_counted_extracted(
        db, spec, names, monkeypatch, caplog):
    held = 12
    run_id = _legacy(db, spec, names,
                     lambda n: ("RCT", "A clean sentence.") if n in names[:held] else None,
                     monkeypatch)
    reported: list[str] = []
    real_report = ProgressReporter.report

    def report(self, paper_id, status, elapsed):
        reported.append(status)
        return real_report(self, paper_id, status, elapsed)
    monkeypatch.setattr(ProgressReporter, "report", report)

    with patch("engine.utils.ollama_preflight.require_preflight"), \
         patch.object(rp, "run_post_extraction_check",
                      wraps=rp.run_post_extraction_check) as check, \
         caplog.at_level(logging.INFO, logger="engine.agents.extractor"):
        stats = rp._stage_extract(db, spec, "guard", run_id=run_id)

    # the store has it extracted, with its eight missing fields named
    state, payload = db._conn.execute(
        "SELECT to_state, payload_json FROM paper_events WHERE paper_id = ? "
        "AND event_type = 'extracted'", (PID,)).fetchone()
    assert state == "extracted" and len(json.loads(payload)["incomplete_fields"]) == 8
    # and the run says the same
    assert (stats["extracted"], stats["failed"], stats["stored_incomplete"]) == (1, 0, 1)
    assert stats["total_spans"] == held
    assert reported == ["EXTRACTED"]
    assert check.call_count == 1
    kwargs = check.call_args.kwargs
    assert (kwargs["extracted_count"], kwargs["failed_count"]) == (1, 0)
    said = [r.getMessage() for r in caplog.records]
    assert any("stored as extracted" in m and "8 incomplete" in m for m in said), said
    assert not any("extraction failed" in m for m in said)
    assert any("1 extracted" in m and "0 failed" in m for m in said)


def test_a_paper_that_fails_is_still_counted_failed(db, spec, names, monkeypatch):
    """Companion: a record under the floor is a failure, and is counted as one."""
    run_id = _legacy(db, spec, names,
                     lambda n: ("RCT", "A clean sentence.") if n in names[:3] else None,
                     monkeypatch)
    monkeypatch.setattr(E, "CONSECUTIVE_FAILURE_ABORT", 99)
    reported: list[str] = []
    monkeypatch.setattr(ProgressReporter, "report",
                        lambda self, pid, status, elapsed: reported.append(status))
    with patch("engine.utils.ollama_preflight.require_preflight"), \
         patch.object(rp, "run_post_extraction_check",
                      wraps=rp.run_post_extraction_check) as check:
        stats = rp._stage_extract(db, spec, "guard", run_id=run_id)
    assert (stats["extracted"], stats["failed"], stats["below_floor"]) == (0, 1, 1)
    assert reported == ["FAILED"]
    assert (check.call_args.kwargs["extracted_count"],
            check.call_args.kwargs["failed_count"]) == (0, 1)
