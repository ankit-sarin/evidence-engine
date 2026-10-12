"""P1 unit U06 (R612(f), amending R535): the floor, the stored attempt, and the
claim-on-unpinned-arm refusal (A-11, N09, INT-l12-18).

* **The floor.** A field is HELD when it carries an asserted value (a cited
  sentinel is one) or a declined outcome; `contract_unmet` does not count, and
  neither does a field that never came back. A record holding fewer than half of
  the expected fields is not an extraction: the paper is `extraction_failed`
  with reason `held_fields_below_floor`, counts toward the consecutive-failure
  abort, and is selected again. Every record, exhausted or not.
* **At or above the floor** the record is stored `extracted`; the fields that
  never came back are named in `incomplete_fields`, and the run summary and
  PRISMA report them. The summary also counts papers whose held fields are all
  declined.
* **The attempt rule.** Across the retry driver's attempts a later record
  replaces the kept one only if it holds strictly more fields.
* **INT-l12-18.** The event writer refuses a model arm's claim while the arm is
  unpinned.

Scratch databases under `tmp_path`; the model passes are stubbed. Nothing here
reaches a model, a service or the live review.
"""

from __future__ import annotations

import csv
import json
import shutil
import types
from pathlib import Path
from unittest.mock import patch

import pytest

from engine.agents import extractor as E
from engine.agents.models import EvidenceSpan, ExtractionOutput, ExtractionResult
from engine.core import events
from engine.core import extraction_events as X
from engine.core import paper_state as PS
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.extraction_telemetry import telemetry_path
from engine.core.parsed_text import resolve_parsed_text
from engine.core.review_spec import load_review_spec
from engine.core.selection import select_for_extraction
from engine.elicitation import classes as C
from engine.elicitation import pipeline as PL
from engine.exporters.prisma import export_prisma_csv, generate_prisma_flow
from _event_store_fixture import FIXTURE_CONTEXT_SHA, claim_identity, open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
FLOOR = "held_fields_below_floor"
PIDS = (7, 8, 9)
TEXT = "The paper reports a trial. A clean sentence.\n"
HASH = "d" * 64

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture(scope="module")
def elicited(spec):
    return spec.model_copy(update={
        "extraction_models": spec.extraction_models.model_copy(update={"elicitation": True})})


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("floor", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    for pid in PIDS:
        rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                          "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
        seed_eligibility(rdb._conn, pid)
        write_parsed(rdb, pid, TEXT)
    rdb._conn.commit()
    yield rdb
    rdb.close()


@pytest.fixture(scope="module")
def cb():
    return load_codebook(LIVE_CODEBOOK)


@pytest.fixture(scope="module")
def names(cb):
    return tuple(f["name"] for f in cb.fields)


def _run(db, spec, run_id):
    with patch("engine.utils.ollama_preflight.require_preflight"):
        return E.run_extraction(db, spec, "floor", experiment_lock=False,
                                restart_every=0, run_id=run_id)


def _paper_events(db, pid=None):
    sql = ("SELECT paper_id, to_state, reason_code, payload_json FROM paper_events "
           "WHERE event_type IN ('extracted', 'extraction_failed')")
    rows = db._conn.execute(sql + " ORDER BY event_id").fetchall()
    return [(r[0], r[1], r[2], json.loads(r[3])) for r in rows if pid in (None, r[0])]


def _claims(db, pid):
    return [tuple(r) for r in db._conn.execute(
        "SELECT event_type, field_name, value FROM field_events WHERE paper_id = ? "
        "ORDER BY event_id", (pid,))]


def _selected(db, spec):
    return [pid for pid, _ in select_for_extraction(
        db._conn, arm=spec.extraction_models.arm).to_extract]


# ── the predicate (pure) ──────────────────────────────────────────────
def _record(db, spec, run_id, fields, incomplete=(), pid=7):
    return X.ExtractionRecord(
        paper_id=pid, arm=spec.extraction_models.arm, run_id=run_id,
        extraction_uid=events.mint_extraction_uid(),
        parsed_text=resolve_parsed_text(db._conn, pid), model="deepseek-r1:32b",
        model_digest="a" * 64, fields=tuple(fields), incomplete_fields=tuple(incomplete),
        attempts=1, stage_name="extract_pass2",
        presented_context_sha256=HASH, context_chain=(HASH,))


def _mixed(names, *, values=0, declined=0, unmet=0):
    it = iter(names)
    out = [X.FieldOutcome(next(it), X.VALUE, value="v", source_snippet="A clean sentence.",
                          confidence=0.9) for _ in range(values)]
    out += [X.FieldOutcome(next(it), X.DECLINED) for _ in range(declined)]
    out += [X.FieldOutcome(next(it), X.CONTRACT_UNMET, violation_codes=("VALUE_WITHOUT_CITATION",))
            for _ in range(unmet)]
    return out, tuple(it)


def test_the_floor_counts_held_fields_against_the_expected(db, spec, names):
    assert PS.REASON_HELD_FIELDS_BELOW_FLOOR == FLOOR
    assert PS.EXTRACTION_REASONS[FLOOR] == "extraction_failed"
    run_id = open_extraction_run(db, spec)
    assert len(names) == 20

    def rec(**kw):
        fields, rest = _mixed(names, **kw)
        return _record(db, spec, run_id, fields, incomplete=rest)

    # held = asserted values + declined; contract_unmet and missing fields are not held
    assert X.held_count(rec(values=6, declined=3, unmet=5)) == 9
    assert X.expected_count(rec(values=6, declined=3, unmet=5)) == 20
    for kw, under in (({"values": 9, "unmet": 11}, True), ({"values": 10, "unmet": 10}, False),
                      ({"values": 6, "declined": 3, "unmet": 11}, True),
                      ({"values": 6, "declined": 4}, False),        # 10 held, 10 never came back
                      ({"unmet": 20}, True), ({"declined": 20}, False),
                      ({"values": 1}, True)):
        assert X.below_floor(rec(**kw)) is under, kw

    # under the floor the plan is a paper failure and carries no claim
    plan = X.plan_extraction_events(rec(values=6, declined=3, unmet=5), live={}, from_state=None)
    assert plan.field_events == ()
    pe = plan.paper_event
    assert (pe["event_type"], pe["to_state"], pe["reason_code"]) == (
        "extraction_failed", "extraction_failed", FLOOR)
    assert (pe["payload"]["held"], pe["payload"]["expected"]) == (9, 20)
    assert (pe["payload"]["asserted"], pe["payload"]["declined"],
            pe["payload"]["contract_unmet"]) == (6, 3, 5)
    assert len(pe["payload"]["incomplete_fields"]) == 6
    # at the floor it is an extraction
    plan = X.plan_extraction_events(rec(values=10, unmet=4), live={}, from_state=None)
    assert plan.paper_event["to_state"] == "extracted"
    assert len(plan.paper_event["payload"]["incomplete_fields"]) == 6
    # a record with no field at all keeps its own reason
    assert X.plan_extraction_events(rec(), live={}, from_state=None).paper_event[
        "reason_code"] == PS.REASON_NO_FIELDS_RETURNED
    # the abort counts every record under the floor (N09), and none at or above it
    assert X.counts_toward_abort(rec(values=1)) and X.counts_toward_abort(rec(unmet=20))
    assert not X.counts_toward_abort(rec(values=10, unmet=10))


def test_a_record_under_the_floor_is_never_written_as_a_record(db, spec, names):
    run_id = open_extraction_run(db, spec)
    fields, rest = _mixed(names, values=3, unmet=17)
    rec = _record(db, spec, run_id, fields, incomplete=rest)
    with pytest.raises(X.HeldFieldsBelowFloor) as exc:
        X.write_extraction_events(db._conn, rec)
    assert exc.value.record is rec
    assert _paper_events(db) == [] and _claims(db, 7) == []
    # its failure is what the extractor's failure branch records
    outcome = X.outcome_for_exception(exc.value, paper_id=7, arm=rec.arm, run_id=run_id,
                                      stage_name="extract_pass2")
    assert isinstance(outcome, X.PaperFailure) and outcome.reason_code == FLOOR
    X.write_extraction_events(db._conn, outcome)
    assert [(e[1], e[2]) for e in _paper_events(db)] == [("extraction_failed", FLOOR)]
    assert _claims(db, 7) == []


# ── the elicited path: a record that is not exhausted ─────────────────
def _response(content):
    return (types.SimpleNamespace(
        message=types.SimpleNamespace(content=content, thinking="reasoning"),
        done_reason="stop", prompt_eval_count=1234, eval_count=99), HASH)


def _entry(field: dict, cb, kind: str) -> dict:
    """One Pass-1 entry: 'good' meets the field's class contract citing unit 1,
    'bad' asserts a value with nothing cited, 'escape' is the escape token."""
    name, cls = field["name"], C.field_class(field["name"], cb.raw)
    if kind == "escape":
        return {"field_name": name, "unit_indices": [], "value": cb.raw["escape_token"]}
    if kind == "bad":
        return {"field_name": name, "unit_indices": [], "value": "uncited"}
    if cls == C.STATED:
        return {"field_name": name, "unit_indices": [1], "value": "v"}
    if cls == C.INFERABLE:
        return {"field_name": name, "unit_indices": [1],
                "inference": "The trial reported fixes the value.", "value": "v"}
    return {"field_name": name, "reasoning_steps": [{"step": "The trial is described.",
                                                     "unit_indices": [1]}], "value": "v"}


def _elicited_run(db, elicited, cb, kinds, monkeypatch):
    """One elicited run over every paper: Pass 1 answers `kinds[i]` for field i
    on every attempt (or the raw string `kinds` itself); Pass 2 answers every
    field Pass 1 evidenced."""
    if isinstance(kinds, str):
        pass1, good = kinds, []
    else:
        pass1 = json.dumps({"fields": [_entry(f, cb, k) for f, k in zip(cb.fields, kinds)]})
        good = [f for f, k in zip(cb.fields, kinds) if k == "good"]
    pass2 = ExtractionOutput(fields=[
        EvidenceSpan(field_name=f["name"], value="v", source_snippet="typed by the model",
                     confidence=0.9, tier=f["tier"]) for f in good]).model_dump_json()
    calls = {"pass1": 0, "pass2": 0}

    def p1(**kw):
        calls["pass1"] += 1
        return _response(pass1)

    def p2(**kw):
        calls["pass2"] += 1
        return _response(pass2)
    monkeypatch.setattr(PL, "ollama_chat", p1)
    monkeypatch.setattr(E, "ollama_chat", p2)
    run_id = open_extraction_run(db, elicited)
    return run_id, calls


@pytest.mark.parametrize("content", ["", '{"fields": [', "I could not find the paper."],
                         ids=["empty", "truncated", "not-json"])
def test_an_empty_pass1_on_both_attempts_fails_the_paper_counts_and_is_selected_again(
        db, elicited, cb, content, monkeypatch):
    monkeypatch.setattr(E, "CONSECUTIVE_FAILURE_ABORT", 2)
    run_id, calls = _elicited_run(db, elicited, cb, content, monkeypatch)
    with pytest.raises(X.RunAborted) as exc:
        _run(db, elicited, run_id)
    assert FLOOR in str(exc.value)
    # two papers, each two Pass-1 attempts and no Pass 2; the third was not reached
    assert calls == {"pass1": 4, "pass2": 0}
    ev = _paper_events(db)
    assert [(e[0], e[1], e[2]) for e in ev] == [(7, "extraction_failed", FLOOR),
                                                (8, "extraction_failed", FLOOR)]
    assert (ev[0][3]["held"], ev[0][3]["expected"], ev[0][3]["contract_unmet"]) == (0, 20, 20)
    assert db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == 0
    # selected again by the next run
    assert _selected(db, elicited) == [7, 8, 9]


def test_a_record_under_the_floor_that_was_not_exhausted_fails_the_same_way(
        db, elicited, cb, monkeypatch):
    kinds = ["good"] * 9 + ["bad"] * 11                  # 9 held of 20, every field answered
    run_id, calls = _elicited_run(db, elicited, cb, kinds, monkeypatch)
    monkeypatch.setattr(E, "CONSECUTIVE_FAILURE_ABORT", 99)
    stats = _run(db, elicited, run_id)
    assert stats["extracted"] == 0 and stats["failed"] == 3 and stats["below_floor"] == 3
    ev = _paper_events(db)
    assert [(e[0], e[1], e[2]) for e in ev] == [(p, "extraction_failed", FLOOR) for p in PIDS]
    assert (ev[0][3]["held"], ev[0][3]["asserted"], ev[0][3]["contract_unmet"]) == (9, 9, 11)
    assert ev[0][3]["incomplete_fields"] == []           # not exhausted: nothing was missing
    assert db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == 0
    # one outer attempt per paper: the floor is not a retry
    assert calls == {"pass1": 6, "pass2": 3}
    rows = [json.loads(line) for line in
            telemetry_path(Path(db.db_path).parent).read_text().splitlines()]
    assert [r["outcome"] for r in rows] == ["below_floor"] * 3
    assert _selected(db, elicited) == [7, 8, 9]


def test_a_record_at_the_floor_is_stored(db, elicited, cb, monkeypatch):
    kinds = ["good"] * 10 + ["bad"] * 10                 # 10 held of 20
    run_id, _ = _elicited_run(db, elicited, cb, kinds, monkeypatch)
    stats = _run(db, elicited, run_id)
    assert stats["extracted"] == 3 and stats["failed"] == 0 and stats["below_floor"] == 0
    ev = _paper_events(db, 7)
    assert [(e[1], e[2]) for e in ev] == [("extracted", None)]
    assert (ev[0][3]["asserted"], ev[0][3]["contract_unmet"]) == (10, 10)
    kinds_stored = [c[0] for c in _claims(db, 7)]
    assert kinds_stored.count("asserted") == 10 and kinds_stored.count("contract_unmet") == 10
    assert _selected(db, elicited) == []


def test_a_record_of_all_declined_fields_is_stored_and_counted(db, elicited, cb, monkeypatch):
    run_id, calls = _elicited_run(db, elicited, cb, ["escape"] * 20, monkeypatch)
    stats = _run(db, elicited, run_id)
    assert calls["pass2"] == 0
    assert stats["extracted"] == 3 and stats["all_declined"] == 3
    assert stats["stored_incomplete"] == 0 and stats["below_floor"] == 0
    assert [c[0] for c in _claims(db, 7)] == ["declined"] * 20
    assert [(e[1], e[3]["declined"], e[3]["asserted"]) for e in _paper_events(db, 7)] == [
        ("extracted", 20, 0)]


# ── the legacy path: exhausted records, and the attempt rule ──────────
def _legacy_run(db, spec, names, attempts, monkeypatch):
    """One legacy run over every paper. `attempts[i]` is what Pass 2 returns on
    a paper's attempt i+1: a number n (the first n fields, each valued
    "attempt<i+1>"), or an exception to raise. The last entry repeats."""
    seen: dict[int, int] = {}

    def pass2(prompt, trace, spec_, paper_id, **kw):
        n = seen[paper_id] = seen.get(paper_id, 0) + 1
        what = attempts[min(n, len(attempts)) - 1]
        if isinstance(what, BaseException):
            raise what
        spans = [EvidenceSpan(field_name=name, value=f"attempt{n}",
                              source_snippet="A clean sentence.", confidence=0.9, tier=1)
                 for name in names[:what]]
        return ExtractionResult(paper_id=paper_id, fields=spans, reasoning_trace="t",
                                model="deepseek-r1:32b", codebook_hash="h",
                                extracted_at="2026-10-12T00:00:00Z"), HASH
    monkeypatch.setattr(E, "extract_pass1_reasoning", lambda *a, **k: ("trace", HASH))
    monkeypatch.setattr(E, "extract_pass2_structured", pass2)
    monkeypatch.setattr(E, "_validate_and_retry_snippets",
                        lambda fields, *a, **k: (fields, {}, ()))
    return open_extraction_run(db, spec)


def _unparseable():
    try:
        ExtractionOutput.model_validate_json("{")
    except Exception as exc:            # pydantic.ValidationError
        return exc
    raise AssertionError("expected a ValidationError")


def test_a_record_at_or_above_the_floor_is_stored_with_incomplete_fields_and_reported(
        db, spec, names, monkeypatch, tmp_path):
    run_id = _legacy_run(db, spec, names, [12], monkeypatch)          # 12 of 20, three times
    stats = _run(db, spec, run_id)
    missing = list(names[12:])
    ev = _paper_events(db, 7)
    assert [(e[1], e[2]) for e in ev] == [("extracted", None)]
    assert ev[0][3]["incomplete_fields"] == missing and ev[0][3]["asserted"] == 12
    assert len(_claims(db, 7)) == 12
    assert _selected(db, spec) == []

    # the run summary reads incomplete_fields
    assert stats["stored_incomplete"] == 3 and stats["below_floor"] == 0
    assert stats["incomplete_fields"] == {pid: missing for pid in PIDS}
    assert stats["all_declined"] == 0

    # and so does PRISMA
    flow = generate_prisma_flow(db)
    assert flow["extracted_incomplete"] == 3
    assert flow["incomplete_fields"] == {name: 3 for name in missing}
    out = tmp_path / "prisma.csv"
    export_prisma_csv(db, str(out))
    rows = list(csv.reader(out.open()))
    assert ["Extracted with incomplete fields", "3",
            "papers stored with fields not returned after the retry budget"] in rows
    assert ["", "3", f"field not returned: {missing[0]}"] in rows


def test_an_exhausted_record_under_the_floor_fails_and_no_longer_resets_the_abort_count(
        db, spec, names, monkeypatch):
    """N09: an exhausted record holding one field used to store as `extracted`
    and reset the consecutive-failure counter."""
    run_id = _legacy_run(db, spec, names, [1], monkeypatch)           # 1 of 20, three times
    with pytest.raises(X.RunAborted) as exc:
        _run(db, spec, run_id)
    assert FLOOR in str(exc.value) and "3 consecutive" in str(exc.value)
    ev = _paper_events(db)
    assert [(e[0], e[1], e[2]) for e in ev] == [(p, "extraction_failed", FLOOR) for p in PIDS]
    assert (ev[0][3]["held"], ev[0][3]["expected"]) == (1, 20)
    assert ev[0][3]["incomplete_fields"] == list(names[1:])
    assert db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == 0
    assert _selected(db, spec) == [7, 8, 9]


@pytest.mark.parametrize("attempts, kept, held", [
    ([15, 12, 12], 1, 15),          # a later attempt with fewer held fields does not replace
    ([12, 12, 12], 1, 12),          # a tie keeps the earlier
    ([12, 15, 13], 2, 15),          # strictly more held fields replaces
    ([12, 13, 15], 3, 15),
], ids=["earlier-better", "tie", "second-best", "last-best"])
def test_the_attempt_rule_keeps_the_record_that_holds_strictly_more(
        db, spec, names, attempts, kept, held, monkeypatch):
    run_id = _legacy_run(db, spec, names, attempts, monkeypatch)
    _run(db, spec, run_id)
    claims = _claims(db, 7)
    assert {c[2] for c in claims} == {f"attempt{kept}"} and len(claims) == held
    ev = _paper_events(db, 7)
    assert ev[0][1] == "extracted" and ev[0][3]["incomplete_fields"] == list(names[held:])


def test_an_unparseable_last_attempt_does_not_cost_the_record_an_earlier_one_held(
        db, spec, names, monkeypatch):
    """Wave 1 row 9: two 19-field attempts followed by an unparseable third
    used to store nothing."""
    run_id = _legacy_run(db, spec, names, [19, 19, _unparseable()], monkeypatch)
    _run(db, spec, run_id)
    claims = _claims(db, 7)
    assert {c[2] for c in claims} == {"attempt1"} and len(claims) == 19
    assert _paper_events(db, 7)[0][1] == "extracted"


def test_three_unparseable_attempts_are_still_response_unparseable(db, spec, names,
                                                                   monkeypatch):
    monkeypatch.setattr(E, "CONSECUTIVE_FAILURE_ABORT", 99)
    run_id = _legacy_run(db, spec, names, [_unparseable()], monkeypatch)
    _run(db, spec, run_id)
    assert [(e[1], e[2]) for e in _paper_events(db, 7)] == [
        ("extraction_failed", PS.REASON_RESPONSE_UNPARSEABLE)]


# ── INT-l12-18 ────────────────────────────────────────────────────────
def _claim(conn, arm, run_id, **kw):
    return events.write_field_event(
        conn, event_type="asserted", paper_id=7, field_name="study_design", arm=arm,
        value="RCT", source_snippet="RCT", actor_kind="model", actor_role="extractor",
        actor_name="m", payload=claim_identity(arm, 7), run_id=run_id,
        presented_context_sha256=FIXTURE_CONTEXT_SHA, **kw)


def test_a_model_arms_claim_on_an_unpinned_arm_is_refused_by_name(db, spec):
    from engine.core.codebook import load_codebook_beside
    conn, arm = db._conn, spec.extraction_models.arm
    handle = rm.open_run(conn, spec, kind="extraction",
                         stages=[*E.extraction_stages(spec), "audit"],
                         codebook=load_codebook_beside(db.db_path), digest_fn=lambda m: "a" * 64,
                         git=rm.GitState(commit="0" * 40, dirty=False, tag=None), host="fixture")
    assert conn.execute("SELECT configuration_marker FROM arms WHERE arm_name = ?",
                        (arm,)).fetchone()[0] is None
    with pytest.raises(events.ClaimOnUnpinnedArm, match=arm) as exc:
        _claim(conn, arm, handle.run_id)
    assert isinstance(exc.value, events.EventRefused)
    assert conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == 0
    # once the run reaches the extract stage the arm is pinned and the claim lands
    E.verify_extraction_run(conn, spec, handle.run_id)
    _claim(conn, arm, handle.run_id)
    assert conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == 1
