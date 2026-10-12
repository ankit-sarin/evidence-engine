"""P1 unit U09 (R612): a duplicate field on Pass-1 attempt 2 does not discard
attempt 1 (INT-i12-5).

`check_response` refuses a response that names an expected field twice (R118:
"an asked-for field answered twice is not collapsed last-wins"). When that
response was attempt 2, the refusal used to unwind past the acceptance rule:
attempt 1's result and telemetry were dropped and the paper's record was built
from nothing, with a context chain of attempt 2's hash only. Now attempt 2 is an
attempt that produced no usable record, as an unparseable attempt 2 is: attempt
1's result is the elicited result, both calls are in the context chain in order,
and attempt 2's telemetry entry is kept with the duplicated field names and its
raw response. Nothing of attempt 2 is merged in, and no duplicate is collapsed.

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
from engine.agents.models import EvidenceSpan, ExtractionOutput
from engine.core.codebook import load_codebook
from engine.core.completeness import DUPLICATE_FIELD
from engine.core.database import ReviewDatabase
from engine.core.extraction_telemetry import telemetry_path
from engine.core.review_spec import load_review_spec
from engine.elicitation import classes as C
from engine.elicitation import pipeline as PL
from _event_store_fixture import open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
PID = 7
TEXT = "The paper reports a trial. A clean sentence.\n"
H1, H2, H_PASS2 = "1" * 64, "2" * 64, "f" * 64
FLOOR = "held_fields_below_floor"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


@pytest.fixture(scope="module")
def elicited():
    spec = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")
    return spec.model_copy(update={
        "extraction_models": spec.extraction_models.model_copy(update={"elicitation": True})})


@pytest.fixture(scope="module")
def cb():
    return load_codebook(LIVE_CODEBOOK)


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("dup", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                      "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (PID,))
    seed_eligibility(rdb._conn, PID)
    write_parsed(rdb, PID, TEXT)
    rdb._conn.commit()
    yield rdb
    rdb.close()


def _response(content, request_hash):
    return (types.SimpleNamespace(
        message=types.SimpleNamespace(content=content, thinking="reasoning"),
        done_reason="stop", prompt_eval_count=1234, eval_count=99), request_hash)


def _entry(field: dict, cb, good: bool) -> dict:
    name, cls = field["name"], C.field_class(field["name"], cb.raw)
    if not good:
        return {"field_name": name, "unit_indices": [], "value": "uncited"}
    if cls == C.STATED:
        return {"field_name": name, "unit_indices": [1], "value": "v"}
    if cls == C.INFERABLE:
        return {"field_name": name, "unit_indices": [1],
                "inference": "The trial reported fixes the value.", "value": "v"}
    return {"field_name": name, "value": "v", "reasoning_steps": [
        {"step": "The trial is described.", "unit_indices": [1]}]}


def _drive(db, elicited, cb, n_good: int, monkeypatch):
    """One elicited run. Pass-1 attempt 1: the first `n_good` fields meet their
    contracts, the rest assert an uncited value. Attempt 2: every field good, and
    the first field named a second time. Pass 2 answers the fields attempt 1
    evidenced. Returns (stats, attempt-1 response, attempt-2 response, pass-1 calls)."""
    first = json.dumps({"fields": [_entry(f, cb, i < n_good) for i, f in enumerate(cb.fields)]})
    all_good = [_entry(f, cb, True) for f in cb.fields]
    second = json.dumps({"fields": all_good + [dict(all_good[0], value="a second answer")]})
    pass2 = ExtractionOutput(fields=[
        EvidenceSpan(field_name=f["name"], value="v", source_snippet="typed by the model",
                     confidence=0.9, tier=f["tier"]) for f in cb.fields[:n_good]
    ]).model_dump_json()
    calls: list[str] = []

    def p1(**kw):
        calls.append("feedback" if "did not meet the contract" in kw["messages"][-1]["content"]
                     else "first")
        return _response(first, H1) if calls[-1] == "first" else _response(second, H2)
    monkeypatch.setattr(PL, "ollama_chat", p1)
    monkeypatch.setattr(E, "ollama_chat", lambda **kw: _response(pass2, H_PASS2))
    monkeypatch.setattr(E, "CONSECUTIVE_FAILURE_ABORT", 99)
    with patch("engine.utils.ollama_preflight.require_preflight"):
        stats = E.run_extraction(db, elicited, "dup", experiment_lock=False, restart_every=0,
                                 run_id=open_extraction_run(db, elicited))
    return stats, first, second, calls


def _claims(db) -> dict[str, tuple[str, str | None, dict]]:
    return {r[1]: (r[0], r[2], json.loads(r[3])) for r in db._conn.execute(
        "SELECT event_type, field_name, value, payload_json FROM field_events "
        "WHERE paper_id = ? ORDER BY event_id", (PID,))}


def _paper_event(db):
    r = db._conn.execute(
        "SELECT to_state, reason_code, payload_json FROM paper_events WHERE paper_id = ? "
        "AND event_type IN ('extracted', 'extraction_failed') ORDER BY event_id DESC",
        (PID,)).fetchone()
    return r[0], r[1], json.loads(r[2])


def _telemetry(db) -> list[dict]:
    return [json.loads(line) for line in
            telemetry_path(Path(db.db_path).parent).read_text().splitlines()]


def test_a_duplicate_on_pass1_attempt_2_keeps_both_hashes_and_attempt_1(
        db, elicited, cb, monkeypatch):
    names = [f["name"] for f in cb.fields]
    failing, doubled = names[19], names[0]
    stats, first, second, calls = _drive(db, elicited, cb, 19, monkeypatch)

    # one outer attempt: attempt 1, then the feedback attempt that named a field twice
    assert calls == ["first", "feedback"]
    assert stats["extracted"] == 1 and stats["failed"] == 0

    # the stored record is attempt 1's
    claims = _claims(db)
    assert len(claims) == 20
    assert [n for n, c in claims.items() if c[0] == "asserted"] == names[:19]
    kind, value, payload = claims[failing]
    assert (kind, value) == ("contract_unmet", None)
    # attempt 1's own codes: the field is a JUDGMENT field answered with an
    # uncited value and no reasoning steps
    assert C.field_class(failing, cb.raw) == C.JUDGMENT
    assert payload["violation_codes"] == ["VALUE_WITHOUT_CITATION", "STEPS_MISSING"]
    assert DUPLICATE_FIELD not in json.dumps(claims)
    # nothing of attempt 2 was merged in, and the doubled field is attempt 1's single answer
    assert claims[doubled][:2] == ("asserted", "v")
    assert "a second answer" not in json.dumps(claims)

    # both Pass-1 calls are in every claim's context chain, in order, then Pass 2
    assert {tuple(c[2]["context_chain"]) for c in claims.values()} == {(H1, H2, H_PASS2)}
    assert _paper_event(db)[0] == "extracted"

    # telemetry holds both attempts; attempt 2's names the duplicated field and keeps its raw
    rows = _telemetry(db)
    assert [r["outcome"] for r in rows] == ["stored"]
    tel = rows[0]["extra"]
    assert (tel["n_pass1_attempts"], tel["accepted_attempt"]) == (2, 1)
    a1, a2 = tel["attempts"]
    assert (a1["raw_content"], a1["request_hash"], a1["duplicated_fields"]) == (first, H1, [])
    assert (a2["raw_content"], a2["request_hash"], a2["duplicated_fields"]) == (
        second, H2, [doubled])
    assert a2["parse_path"] == "duplicate_field" and a2["feedback_chars"] > 0


def test_with_twelve_good_fields_attempt_1_is_still_kept_and_stored(db, elicited, cb,
                                                                    monkeypatch):
    names = [f["name"] for f in cb.fields]
    stats, _, _, calls = _drive(db, elicited, cb, 12, monkeypatch)
    assert calls == ["first", "feedback"]
    claims = _claims(db)
    assert [n for n, c in claims.items() if c[0] == "asserted"] == names[:12]
    assert sum(1 for c in claims.values() if c[0] == "contract_unmet") == 8
    state, _, payload = _paper_event(db)
    assert state == "extracted" and (payload["asserted"], payload["contract_unmet"]) == (12, 8)
    assert stats["extracted"] == 1


def test_the_floor_applies_to_the_kept_attempt_1_record_as_to_any_record(db, elicited, cb,
                                                                         monkeypatch):
    stats, _, _, calls = _drive(db, elicited, cb, 9, monkeypatch)      # 9 held of 20
    assert calls == ["first", "feedback"]                               # not retried
    state, reason, payload = _paper_event(db)
    assert (state, reason) == ("extraction_failed", FLOOR)
    assert (payload["held"], payload["contract_unmet"]) == (9, 11)
    assert payload["context_chain"] == [H1, H2, H_PASS2]
    assert _claims(db) == {} and stats["failed"] == 1
    assert DUPLICATE_FIELD not in json.dumps(payload)


def test_a_duplicate_on_attempt_1_keeps_its_own_path(db, elicited, cb, monkeypatch):
    """Out of scope and unchanged: attempt 1 naming a field twice is refused as a
    duplicate (R118), retried under the outer budget, and recorded as one."""
    good = [_entry(f, cb, True) for f in cb.fields]
    doubled = json.dumps({"fields": good + [dict(good[0])]})
    monkeypatch.setattr(PL, "ollama_chat", lambda **kw: _response(doubled, H1))
    monkeypatch.setattr(E, "ollama_chat",
                        lambda **kw: pytest.fail("Pass 2 was reached after a duplicate"))
    monkeypatch.setattr(E, "CONSECUTIVE_FAILURE_ABORT", 99)
    with patch("engine.utils.ollama_preflight.require_preflight"):
        E.run_extraction(db, elicited, "dup", experiment_lock=False, restart_every=0,
                         run_id=open_extraction_run(db, elicited))
    assert [r["outcome"] for r in _telemetry(db)] == [
        "incomplete_retry", "incomplete_retry", "incomplete_exhausted"]
    state, reason, payload = _paper_event(db)
    assert (state, reason) == ("extraction_failed", FLOOR)
    assert payload["contract_unmet_fields"] == {cb.fields[0]["name"]: [DUPLICATE_FIELD]}
    assert payload["context_chain"] == [H1]
