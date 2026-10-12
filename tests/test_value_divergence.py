"""P1 unit U08 (R612(j), D9 Option A; R620(c)): value divergence recorded where a
reader sees it, and what `attempts` means on what is stored.

INT-i12-3, N11 — Pass 2 supplies the stored value; Pass 1 stated a value when it
chose the evidence. When the two differ, the claim's payload carries the Pass-1
value under `pass1_value` — on a record stored after the retry budget as well —
and the run summary counts the divergent claims and names their fields, split
into differences of case or whitespace only and substantive ones. Nothing is
reconciled: the stored value is Pass 2's.
R620(c) — `attempts` on what is stored is the number of attempts spent on the
paper; `kept_attempt` is the attempt whose record was kept.

Scratch databases under `tmp_path`; the model passes are stubbed. Nothing here
reaches a model, a service or the live review.
"""

from __future__ import annotations

import json
import shutil
import types
from pathlib import Path
from unittest.mock import patch

import pytest

from engine.agents import extractor as E
from engine.agents.models import EvidenceSpan, ExtractionOutput, ExtractionResult
from engine.core import events
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.effective import effective_value, load_absence_sentinels
from engine.core.review_spec import load_review_spec
from engine.elicitation import classes as C
from engine.elicitation import pipeline as PL
from _event_store_fixture import open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
PID = 7
TEXT = "The paper reports a trial. A clean sentence.\n"
HASH = "d" * 64
NONE = {"substantive": {"count": 0, "fields": {}},
        "case_or_whitespace": {"count": 0, "fields": {}}}

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
    return tuple(f["name"] for f in cb.fields)


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("diverge", data_root=tmp_path)
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
        return E.run_extraction(db, spec, "diverge", experiment_lock=False,
                                restart_every=0, run_id=run_id)


def _claims(db) -> dict[str, tuple[str, str | None, dict]]:
    return {r[1]: (r[0], r[2], json.loads(r[3])) for r in db._conn.execute(
        "SELECT event_type, field_name, value, payload_json FROM field_events "
        "WHERE paper_id = ? ORDER BY event_id", (PID,))}


def _paper_payload(db) -> dict:
    return json.loads(db._conn.execute(
        "SELECT payload_json FROM paper_events WHERE paper_id = ? AND event_type = 'extracted'",
        (PID,)).fetchone()[0])


def _response(content):
    return (types.SimpleNamespace(
        message=types.SimpleNamespace(content=content, thinking="reasoning"),
        done_reason="stop", prompt_eval_count=1234, eval_count=99), HASH)


def _pass1(cb, values: dict) -> str:
    """A Pass 1 that meets every class contract; a field's value is `values[name]`
    or "v"."""
    out = []
    for f in cb.fields:
        name, cls, v = f["name"], C.field_class(f["name"], cb.raw), values.get(f["name"], "v")
        if cls == C.STATED:
            out.append({"field_name": name, "unit_indices": [1], "value": v})
        elif cls == C.INFERABLE:
            out.append({"field_name": name, "unit_indices": [1],
                        "inference": "The trial reported fixes the value.", "value": v})
        else:
            out.append({"field_name": name, "value": v, "reasoning_steps": [
                {"step": "The trial is described.", "unit_indices": [1]}]})
    return json.dumps({"fields": out})


def _elicited(db, elicited, cb, monkeypatch, *, pass1: dict, pass2: dict, drop=()):
    """One elicited run: Pass 1 states `pass1[name]` (default "v"), Pass 2
    returns `pass2[name]` (default "v") and omits the fields in `drop`."""
    p2 = ExtractionOutput(fields=[
        EvidenceSpan(field_name=f["name"], value=pass2.get(f["name"], "v"),
                     source_snippet="typed by the model", confidence=0.9, tier=f["tier"])
        for f in cb.fields if f["name"] not in drop]).model_dump_json()
    monkeypatch.setattr(PL, "ollama_chat", lambda **kw: _response(_pass1(cb, pass1)))
    monkeypatch.setattr(E, "ollama_chat", lambda **kw: _response(p2))
    return _run(db, elicited, open_extraction_run(db, elicited))


# ── INT-i12-3 ─────────────────────────────────────────────────────────
def test_value_divergence_reaches_the_stored_record(db, elicited, cb, names, monkeypatch):
    field = names[0]
    stats = _elicited(db, elicited, cb, monkeypatch, pass1={field: "A"}, pass2={field: "B"})
    claims = _claims(db)
    kind, value, payload = claims[field]
    assert (kind, value) == ("asserted", "B")          # the stored value is Pass 2's
    assert payload["pass1_value"] == "A"
    # equal values add nothing
    assert [n for n, c in claims.items() if "pass1_value" in c[2]] == [field]
    assert stats["value_divergence"] == {
        "substantive": {"count": 1, "fields": {field: 1}},
        "case_or_whitespace": {"count": 0, "fields": {}}}
    assert events.PAYLOAD_PASS1_VALUE == "pass1_value"


def test_equal_values_record_no_divergence(db, elicited, cb, names, monkeypatch):
    stats = _elicited(db, elicited, cb, monkeypatch, pass1={names[0]: "A"},
                      pass2={names[0]: "  A "})        # exact after strip
    assert not any("pass1_value" in c[2] for c in _claims(db).values())
    assert stats["value_divergence"] == NONE


def test_divergence_is_recorded_on_a_record_stored_after_the_retry_budget(
        db, elicited, cb, names, monkeypatch):
    """N11: on the refusal path the divergence list was never set."""
    field, dropped = names[0], names[-1]
    stats = _elicited(db, elicited, cb, monkeypatch, pass1={field: "A"}, pass2={field: "B"},
                      drop=(dropped,))                 # incomplete on every attempt
    claims = _claims(db)
    assert dropped not in claims and len(claims) == 19
    assert _paper_payload(db)["incomplete_fields"] == [dropped]
    assert claims[field][1] == "B" and claims[field][2]["pass1_value"] == "A"
    assert stats["value_divergence"]["substantive"] == {"count": 1, "fields": {field: 1}}


def test_a_case_or_whitespace_difference_is_recorded_and_counted_apart(
        db, elicited, cb, names, monkeypatch):
    a, b, c = names[0], names[1], names[2]
    stats = _elicited(db, elicited, cb, monkeypatch,
                      pass1={a: "Yes", b: "da  Vinci\tResearch Kit", c: "Canada"},
                      pass2={a: "yes", b: "da Vinci Research Kit", c: "United States"})
    claims = _claims(db)
    assert claims[a][2]["pass1_value"] == "Yes" and claims[a][1] == "yes"
    assert claims[b][2]["pass1_value"] == "da  Vinci\tResearch Kit"
    assert claims[c][2]["pass1_value"] == "Canada"
    assert stats["value_divergence"] == {
        "case_or_whitespace": {"count": 2, "fields": {a: 1, b: 1}},
        "substantive": {"count": 1, "fields": {c: 1}}}


# ── R620(c) ───────────────────────────────────────────────────────────
def _legacy(db, spec, names, spans_for, monkeypatch):
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
    return _run(db, spec, open_extraction_run(db, spec))


def test_attempts_is_the_attempts_spent_and_the_kept_attempt_is_separate(
        db, spec, names, monkeypatch):
    uncited = names[0]
    # every attempt is refused the same way (one uncited value), so the attempt
    # rule keeps attempt 1's record and three attempts are spent
    _legacy(db, spec, names,
            lambda n: ("A real value", "") if n == uncited else ("RCT", "A clean sentence."),
            monkeypatch)
    kind, _, payload = _claims(db)[uncited]
    assert kind == "contract_unmet"
    assert (payload["attempts"], payload["kept_attempt"]) == (3, 1)
    paper = _paper_payload(db)
    assert (paper["attempts"], paper["kept_attempt"]) == (3, 1)
    # the one reader of a claim's `attempts` passes on the attempts spent
    got = effective_value(db._conn, PID, uncited, spec.extraction_models.arm,
                          sentinels=load_absence_sentinels(
                              Path(db.db_path).parent / "extraction_codebook.yaml"))
    assert got.rule_row == 15 and got.provenance["attempts"] == 3
    assert events.PAYLOAD_KEPT_ATTEMPT == "kept_attempt"


def test_a_first_attempt_extraction_spent_one_attempt(db, spec, names, monkeypatch):
    _legacy(db, spec, names, lambda n: ("RCT", "A clean sentence."), monkeypatch)
    paper = _paper_payload(db)
    assert (paper["attempts"], paper["kept_attempt"]) == (1, 1)
