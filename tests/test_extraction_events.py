"""The extraction event mapping (WRITE-PATH-01 9b-2c): records, paper events, F9.

T1–T12 of the 9b-2c brief and the ruling's added test (an exhaustion carrying no
record). T13 lives with the run tests in `test_extraction_run_link.py`. Every
database is a scratch `ReviewDatabase` with a real manifest (the 2(b) helper);
the extractor's model calls are stubbed at the pass functions.
"""

from __future__ import annotations

import json
import re
import shutil
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from engine.agents import extractor as E
from engine.agents.models import EvidenceSpan, ExtractionResult
from engine.core import events
from engine.core import extraction_events as X
from engine.core import paper_state as PS
from engine.core.codebook import load_codebook
from engine.core.completeness import DUPLICATE_FIELD, DuplicateFieldError, IncompleteExtractionError
from engine.core.citation_guard import UncitedValueError
from engine.core.database import ReviewDatabase
from engine.core.effective import (
    classify_field_state, effective_state, effective_value, live_claims,
    load_absence_sentinels,
)
from engine.core.parsed_text import (
    NoParsedText, ParsedTextMissing, ParsedTextModified, resolve_parsed_text,
)
from engine.core.review_spec import load_review_spec
from engine.core.selection import select_for_extraction
from engine.utils.ollama_client import InputTruncated
from _event_store_fixture import open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
PID = 7


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("xev", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                      "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (PID,))
    seed_eligibility(rdb._conn, PID)
    write_parsed(rdb, PID, "The paper reports a trial. A clean sentence.\n")
    rdb._conn.commit()
    yield rdb
    rdb.close()


@pytest.fixture
def run_id(db, spec):
    return open_extraction_run(db, spec)


@pytest.fixture
def cb_path(db):
    return Path(db.db_path).parent / "extraction_codebook.yaml"


@pytest.fixture
def sentinels(cb_path):
    return load_absence_sentinels(cb_path)


@pytest.fixture
def expected(cb_path):
    return tuple(f["name"] for f in load_codebook(cb_path).fields)


def _record(db, spec, run_id, fields, incomplete=(), ref=None, uid=None):
    return X.ExtractionRecord(
        paper_id=PID, arm=spec.extraction_models.arm, run_id=run_id,
        extraction_uid=uid or events.mint_extraction_uid(),
        parsed_text=ref or resolve_parsed_text(db._conn, PID), model="deepseek-r1:32b",
        model_digest="a" * 64, fields=tuple(fields), incomplete_fields=tuple(incomplete),
        attempts=1, stage_name="extract_pass2",
        # R224a: every claim needs a presented context; the fixture chain is one hash.
        presented_context_sha256="d" * 64, context_chain=("d" * 64,))


def _value(name, value="RCT", snippet="A clean sentence."):
    return X.FieldOutcome(name, X.VALUE, value=value, source_snippet=snippet, confidence=0.9)


def _fev(db, **where):
    sql = "SELECT event_type, field_name, payload_json, run_id FROM field_events"
    if where:
        sql += " WHERE " + " AND ".join(f"{k} = ?" for k in where)
    return db._conn.execute(sql + " ORDER BY event_id", tuple(where.values())).fetchall()


def _pev(db):
    return db._conn.execute("SELECT event_type, to_state, reason_code, payload_json, run_id "
                            "FROM paper_events WHERE paper_id = ? AND event_type <> 'screened' "
                            "ORDER BY event_id", (PID,)).fetchall()


#: R224a: a fixture hash for the mocked pass1/pass2 calls below — real
#: extract_paper now unpacks (value, request_hash) from both.
FIXTURE_PASS_HASH = "d" * 64


def _pass2(fields):
    return ExtractionResult(paper_id=PID, fields=fields, reasoning_trace="t",
                            model="deepseek-r1:32b", codebook_hash="h",
                            extracted_at="2026-09-25T00:00:00Z")


def _spans(expected, **override):
    out = []
    for name in expected:
        value, snippet = override.get(name, ("NR", f"Snippet for {name}."))
        out.append(EvidenceSpan(field_name=name, value=value, source_snippet=snippet,
                                confidence=0.9, tier=1))
    return out


def _legacy(db, spec, run_id, spans, *, attempt=3):
    """The real legacy `extract_paper`, its two model passes stubbed."""
    with patch.object(E, "extract_pass1_reasoning",
                      return_value=("trace", FIXTURE_PASS_HASH)), \
         patch.object(E, "extract_pass2_structured",
                      return_value=(_pass2(spans), FIXTURE_PASS_HASH)):
        return E.extract_paper(PID, "The paper reports a trial.", spec, db, attempt=attempt,
                               parsed_text_ref=resolve_parsed_text(db._conn, PID),
                               run_id=run_id)


# ── T1 ────────────────────────────────────────────────────────────────
def test_t1_a_complete_record_asserts_every_field_with_its_input(db, spec, run_id,
                                                                  sentinels):
    names = ("study_design", "sample_size", "country")
    rec = _record(db, spec, run_id, [_value("study_design"), _value("sample_size", "NR", ""),
                                     _value("country", "USA")])
    X.write_extraction_events(db._conn, rec, sentinels=sentinels)
    rows = _fev(db)
    assert [r[0] for r in rows] == ["asserted"] * 3 and [r[1] for r in rows] == list(names)
    ref = resolve_parsed_text(db._conn, PID)
    for (_, name, payload, rid), fo in zip(rows, rec.fields):
        p = json.loads(payload)
        assert p[events.PAYLOAD_PARSED_TEXT_SHA256] == ref.sha256
        assert p[events.PAYLOAD_PARSED_TEXT_UID] == ref.parsed_text_uid
        assert p[events.PAYLOAD_REUSE_KEY].startswith("rk1:")
        assert p["state_at_write"] == classify_field_state(fo.value, [], "asserted",
                                                           sentinels=sentinels)
        assert rid == run_id
    (etype, to_state, reason, payload, rid), = _pev(db)
    assert (etype, to_state, reason, rid) == ("extracted", "extracted", None, run_id)
    p = json.loads(payload)
    assert (p["asserted"], p["contract_unmet"], p["declined"], p["incomplete_fields"]) == \
        (3, 0, 0, [])
    assert effective_state(db._conn, PID).processing == "extracted"


# ── T2 (R139 through R3's carrier, both exception types) ─────────────
def test_t2_an_exhausted_uncited_value_is_contract_unmet_and_the_paper_stores(
        db, spec, run_id, sentinels, expected):
    bad = expected[0]
    with pytest.raises(UncitedValueError) as err:
        _legacy(db, spec, run_id, _spans(expected, **{bad: ("A real value", "")}))
    rec = X.outcome_for_exception(err.value, paper_id=PID, arm=spec.extraction_models.arm,
                                  run_id=run_id, stage_name="extract_pass2")
    assert rec is err.value.record and rec.attempts == 3
    X.write_extraction_events(db._conn, rec, sentinels=sentinels)
    unmet = _fev(db, event_type="contract_unmet")
    assert [r[1] for r in unmet] == [bad]
    p = json.loads(unmet[0][2])
    assert p["violation_codes"] and p["attempts"] == 3
    assert len(_fev(db, event_type="asserted")) == len(expected) - 1
    assert effective_value(db._conn, PID, bad, spec.extraction_models.arm,
                           sentinels=sentinels).rule_row == 15
    assert _pev(db)[0][1] == "extracted"


def test_t2_an_elicited_contract_unmet_and_an_incomplete_carrier(db, spec, run_id,
                                                                 sentinels, expected):
    # The elicited token, through the elicited record builder.
    unmet = X.elicited_record(
        paper_id=PID, arm=spec.extraction_models.arm, run_id=run_id,
        extraction_uid=events.mint_extraction_uid(),
        parsed_text=resolve_parsed_text(db._conn, PID), model="m", model_digest=None,
        expected=("a", "b"), states={"a": "CONTRACT_UNMET", "b": "EVIDENCED_VALUE"},
        spans=[{"field_name": "b", "value": "v", "source_snippet": "s"}],
        violations={"a": ("INDEX_MALFORMED",)}, unmet_token="CONTRACT_UNMET",
        escape_token="NO_EVIDENCE_LOCATABLE", evidenced_token="EVIDENCED_VALUE")
    assert [(f.field_name, f.kind, f.violation_codes) for f in unmet.fields] == \
        [("a", X.CONTRACT_UNMET, ("INDEX_MALFORMED",)), ("b", X.VALUE, ())]
    # IncompleteExtractionError carries its record from the legacy raising site.
    with pytest.raises(IncompleteExtractionError) as err:
        _legacy(db, spec, run_id, _spans(expected[1:]))
    assert err.value.record.incomplete_fields == (expected[0],)
    X.write_extraction_events(db._conn, err.value.record, sentinels=sentinels)
    assert effective_value(db._conn, PID, expected[0], spec.extraction_models.arm,
                           sentinels=sentinels).rule_row == 1


# ── T3 (R140) ─────────────────────────────────────────────────────────
def test_t3_missing_fields_write_nothing_and_are_named_on_the_paper(db, spec, run_id,
                                                                    sentinels):
    rec = _record(db, spec, run_id, [_value("study_design")],
                  incomplete=("country", "sample_size"))
    X.write_extraction_events(db._conn, rec, sentinels=sentinels)
    assert [r[1] for r in _fev(db)] == ["study_design"]
    payload = json.loads(_pev(db)[0][3])
    assert payload["incomplete_fields"] == ["country", "sample_size"]
    for name in ("country", "sample_size"):
        assert effective_value(db._conn, PID, name, spec.extraction_models.arm,
                               sentinels=sentinels).rule_row == 1


# ── T4 (R118 through the budget) ─────────────────────────────────────
def test_t4_a_duplicated_field_is_retried_then_contract_unmet(db, spec, run_id, sentinels,
                                                             expected, tmp_path):
    dup = expected[2]
    spans = _spans(expected) + [EvidenceSpan(field_name=dup, value="other",
                                             source_snippet="x.", confidence=0.9, tier=1)]
    calls = {"n": 0}

    def pass2(*a, **k):
        calls["n"] += 1
        return (_pass2(spans), FIXTURE_PASS_HASH)
    with patch.object(E, "extract_pass1_reasoning",
                      return_value=("trace", FIXTURE_PASS_HASH)), \
         patch.object(E, "extract_pass2_structured", side_effect=pass2):
        with pytest.raises(DuplicateFieldError) as err:
            E.extract_paper_with_completeness(
                PID, "The paper reports a trial.", spec, db, max_attempts=3,
                parsed_text_ref=resolve_parsed_text(db._conn, PID), run_id=run_id)
    assert calls["n"] == 3 and err.value.duplicated == (dup,)
    rec = X.outcome_for_exception(err.value, paper_id=PID, arm=spec.extraction_models.arm,
                                  run_id=run_id, stage_name="extract_pass2")
    X.write_extraction_events(db._conn, rec, sentinels=sentinels)
    on_dup = _fev(db, field_name=dup)
    assert [r[0] for r in on_dup] == ["contract_unmet"]
    assert json.loads(on_dup[0][2])["violation_codes"] == [DUPLICATE_FIELD]
    assert len(live_claims(db._conn, PID, dup, spec.extraction_models.arm)) == 1


# ── T5 (R118) ─────────────────────────────────────────────────────────
def test_t5_an_unexpected_field_is_dropped_and_logged(db, spec, run_id, expected, caplog):
    spans = _spans(expected) + [EvidenceSpan(field_name="Title", value="x",
                                             source_snippet="", confidence=1.0, tier=1)]
    with caplog.at_level("WARNING", logger="engine.core.completeness"):
        result = _legacy(db, spec, run_id, spans)
    assert "Title" not in {f.field_name for f in result.fields}
    assert any("Title" in r.getMessage() and "dropped" in r.getMessage()
               for r in caplog.records)
    rec = X.legacy_record(paper_id=PID, arm="a", run_id=run_id, extraction_uid="u",
                          parsed_text=None, model="m", model_digest=None,
                          expected=expected, spans=spans)
    assert "Title" not in {f.field_name for f in rec.fields} | set(rec.incomplete_fields)


# ── T6 (R120, R130) ───────────────────────────────────────────────────
def test_t6_an_input_that_does_not_fit_is_a_paper_event_and_a_telemetry_row(
        db, spec, run_id, tmp_path):
    exc = InputTruncated(model="deepseek-r1:32b", count=131_072, ceiling=131_072,
                         chars=305_628)
    out = X.outcome_for_exception(exc, paper_id=PID, arm=spec.extraction_models.arm,
                                  run_id=run_id, stage_name="extract_pass1")
    review_dir = Path(db.db_path).parent
    X.write_extraction_events(db._conn, out, review_dir=review_dir)
    assert _fev(db) == []
    (etype, to_state, reason, _, _), = _pev(db)
    assert (etype, to_state, reason) == ("extraction_failed", "input_exceeds_context",
                                         PS.REASON_INPUT_TRUNCATED_AT_CEILING)
    rows = [json.loads(line) for f in review_dir.rglob("*.jsonl")
            for line in f.read_text().splitlines()]
    assert len(rows) == 1
    row = rows[0]
    assert row["outcome"] == X.TELEMETRY_OUTCOME_INPUT_EXCEEDS_CONTEXT
    assert row["pass1_done_reason"] is None and row["pass1_prompt_eval_count"] == 131_072


# ── T7 (R122) ─────────────────────────────────────────────────────────
@pytest.mark.parametrize("exc", [
    NoParsedText(PID),
    ParsedTextMissing("uid", Path("/nowhere")),
    ParsedTextModified("uid", Path("/nowhere"), "a" * 64, "b" * 64),
])
def test_t7_each_a14_refusal_is_extraction_failed_with_its_code(db, spec, run_id, exc):
    out = X.outcome_for_exception(exc, paper_id=PID, arm=spec.extraction_models.arm,
                                  run_id=run_id, stage_name="extract_pass1")
    X.write_extraction_events(db._conn, out)
    assert _fev(db) == []
    assert _pev(db)[0][:3] == ("extraction_failed", "extraction_failed", exc.reason_code)


# ── T8 ────────────────────────────────────────────────────────────────
def test_t8_a_failed_model_call_leaves_the_paper_selectable(db, spec, run_id):
    out = X.outcome_for_exception(TimeoutError("gave up"), paper_id=PID,
                                  arm=spec.extraction_models.arm, run_id=run_id,
                                  stage_name="extract_pass1")
    X.write_extraction_events(db._conn, out)
    assert _pev(db)[0][:3] == ("extraction_failed", "extraction_failed",
                               PS.REASON_MODEL_CALL_FAILED)
    assert _fev(db) == []
    sel = select_for_extraction(db._conn, arm=spec.extraction_models.arm)
    assert [pid for pid, _ in sel.to_extract] == [PID]


# ── T9 ────────────────────────────────────────────────────────────────
def test_t9_a_new_text_version_supersedes_and_the_same_key_refuses(db, spec, run_id,
                                                                  sentinels):
    arm = spec.extraction_models.arm
    X.write_extraction_events(db._conn, _record(db, spec, run_id, [_value("study_design")]),
                              sentinels=sentinels)
    old = live_claims(db._conn, PID, "study_design", arm)
    n_fe, n_pe = len(_fev(db)), len(_pev(db))
    with pytest.raises(X.ReuseKeyAlreadyClaimed):
        X.write_extraction_events(db._conn, _record(db, spec, run_id, [_value("study_design")]),
                                  sentinels=sentinels)
    assert (len(_fev(db)), len(_pev(db))) == (n_fe, n_pe)

    write_parsed(db, PID, "A corrected parse of the paper.\n")         # version 2
    X.write_extraction_events(db._conn, _record(db, spec, run_id, [_value("study_design", "RCT2")]),
                              sentinels=sentinels)
    types = [r[0] for r in _fev(db, field_name="study_design")]
    assert types == ["asserted", "superseded", "asserted"]
    new = live_claims(db._conn, PID, "study_design", arm)
    assert len(new) == 1 and new != old
    assert effective_value(db._conn, PID, "study_design", arm,
                           sentinels=sentinels).value == "RCT2"


# ── T10 (R1) ──────────────────────────────────────────────────────────
def test_t10_the_writer_refuses_an_extractor_claim_without_input_identity(db, spec, run_id):
    arm = spec.extraction_models.arm
    with pytest.raises(events.ClaimWithoutInputIdentity, match="reuse_key"):
        events.write_field_event(
            db._conn, event_type="asserted", paper_id=PID, field_name="f", arm=arm,
            value="v", source_snippet="v", actor_kind="model", actor_role="extractor",
            actor_name="m", run_id=run_id)
    from _event_store_fixture import claim_identity
    uid = events.mint_extraction_uid()
    events.write_field_event(
        db._conn, event_type="asserted", paper_id=PID, field_name="f", arm=arm, value="v",
        source_snippet="v", extraction_uid=uid, actor_kind="model", actor_role="extractor",
        actor_name="m", payload=claim_identity(arm, PID), run_id=run_id,
        presented_context_sha256="d" * 64)  # R224a
    cid = events.make_claim_id(arm, uid, "f")
    events.write_field_event(      # a reviewer event names no input: not refused
        db._conn, event_type="human_corrected", paper_id=PID, field_name="f", arm=arm,
        value="w", actor_kind="human", actor_role="reviewer", actor_name="pi",
        against_claims={cid}, run_id=run_id)
    assert [r[0] for r in _fev(db)] == ["asserted", "human_corrected"]


# ── T11 ───────────────────────────────────────────────────────────────
def test_t11_a_refusal_on_the_last_field_writes_nothing_for_the_paper(db, spec, run_id,
                                                                      sentinels):
    rec = _record(db, spec, run_id, [_value("study_design"), _value("country", "USA"),
                                     _value("sample_size", "40")])
    real, n = X.write_field_event, {"k": 0}

    def flaky(conn, **kw):
        n["k"] += 1
        if n["k"] == 3:
            raise events.ArmNotInRun("refused on the last field")
        return real(conn, **kw)
    with patch.object(X, "write_field_event", side_effect=flaky):
        with pytest.raises(events.ArmNotInRun):
            X.write_extraction_events(db._conn, rec, sentinels=sentinels)
    assert _fev(db) == [] and _pev(db) == []


# ── B10 (R244): the paper-event and SQLite-raised failure points ─────
OTHER = 8


def _paper_rows(db, pid):
    """Every row the writer can leave for `pid`: field events, paper events,
    claim_inputs — the three stores one paper's write spans."""
    return tuple(
        tuple(tuple(r) for r in db._conn.execute(
            f"SELECT * FROM {table} WHERE paper_id = ? ORDER BY 1", (pid,)))
        for table in ("field_events", "paper_events", "claim_inputs"))


def _another_paper_written_whole(db, spec, run_id, sentinels):
    """A second paper, written by the writer before the failing one, so each test
    can show the rollback is scoped to the paper that failed."""
    db._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                     "updated_at) VALUES (?, 't2', 's', 'FT_ELIGIBLE', 'n', 'n')", (OTHER,))
    seed_eligibility(db._conn, OTHER)
    write_parsed(db, OTHER, "The paper reports a trial. A clean sentence.\n")
    db._conn.commit()
    X.write_extraction_events(db._conn, X.ExtractionRecord(
        paper_id=OTHER, arm=spec.extraction_models.arm, run_id=run_id,
        extraction_uid=events.mint_extraction_uid(),
        parsed_text=resolve_parsed_text(db._conn, OTHER), model="deepseek-r1:32b",
        model_digest="a" * 64, fields=(_value("study_design"), _value("country", "USA")),
        incomplete_fields=(), attempts=1, stage_name="extract_pass2",
        presented_context_sha256="d" * 64, context_chain=("d" * 64,)),
        sentinels=sentinels)
    rows = _paper_rows(db, OTHER)
    assert all(rows), "the other paper must hold field events, a paper event and claim_inputs"
    return rows


def _claim_inputs(db, pid):
    return db._conn.execute("SELECT COUNT(*) FROM claim_inputs WHERE paper_id = ?",
                            (pid,)).fetchone()[0]


def test_b10_a_failure_at_the_paper_event_write_writes_nothing_for_the_paper(
        db, spec, run_id, sentinels):
    """T-PE. Every field event and the claim_inputs row are written; the paper
    event, the last write, fails. Nothing for the paper survives — the rollback
    reaches back over the whole savepoint — and the exception propagates."""
    other = _another_paper_written_whole(db, spec, run_id, sentinels)
    before = _paper_rows(db, PID)
    names = ("study_design", "country", "sample_size")
    rec = _record(db, spec, run_id, [_value("study_design"), _value("country", "USA"),
                                     _value("sample_size", "40")])
    real, written = X.write_field_event, []

    def spy(conn, **kw):
        event_id = real(conn, **kw)
        written.append(kw["field_name"])
        return event_id

    def refuse(conn, **kw):
        # Inside the open write: every field event and the claim_inputs row exist.
        assert sorted(written) == sorted(names)
        assert _claim_inputs(db, PID) == 1
        raise RuntimeError("refused at the paper-event write")
    with patch.object(X, "write_field_event", side_effect=spy), \
            patch.object(X, "write_paper_event", side_effect=refuse):
        with pytest.raises(RuntimeError, match="paper-event write"):
            X.write_extraction_events(db._conn, rec, sentinels=sentinels)
    fe, pe, ci = _paper_rows(db, PID)
    assert fe == () and ci == ()
    assert pe == before[1]                  # no paper event added
    assert _paper_rows(db, OTHER) == other
    assert not db._conn.in_transaction


def test_b10_a_sqlite_raised_error_mid_write_writes_nothing_for_the_paper(
        db, spec, run_id, sentinels):
    """T-SQ. SQLite itself refuses the second field event (a TEMP trigger on the
    test connection, RAISE(ABORT) — no engine code touched), after the first
    field event and its claim_inputs row and before the paper event. Nothing for
    the paper survives and the sqlite3 error propagates unwrapped.

    On this path the rollback comes from `write_field_event`'s own handler
    (`conn.rollback()`, which ends the whole transaction), not from the writer's
    savepoint: by the time the writer's `except` runs, no transaction is open."""
    other = _another_paper_written_whole(db, spec, run_id, sentinels)
    before = _paper_rows(db, PID)
    db._conn.execute(
        f"CREATE TEMP TRIGGER b10_abort BEFORE INSERT ON main.field_events "
        f"WHEN NEW.paper_id = {PID} AND "
        f"(SELECT COUNT(*) FROM main.field_events WHERE paper_id = {PID}) >= 1 "
        f"BEGIN SELECT RAISE(ABORT, 'b10: refused by SQLite'); END")
    rec = _record(db, spec, run_id, [_value("study_design"), _value("country", "USA"),
                                     _value("sample_size", "40")])
    real, seen = X.write_field_event, []

    def spy(conn, **kw):
        seen.append(_claim_inputs(db, PID))
        return real(conn, **kw)
    try:
        with patch.object(X, "write_field_event", side_effect=spy):
            with pytest.raises(sqlite3.IntegrityError, match="b10: refused by SQLite"):
                X.write_extraction_events(db._conn, rec, sentinels=sentinels)
    finally:
        db._conn.execute("DROP TRIGGER IF EXISTS temp.b10_abort")
    assert seen == [0, 1]                   # the failure came after a field event and its claim_inputs row
    fe, pe, ci = _paper_rows(db, PID)
    assert fe == () and ci == ()
    assert pe == before[1]
    assert _paper_rows(db, OTHER) == other
    assert not db._conn.in_transaction


def test_an_exhaustion_without_a_record_raises_and_writes_nothing(db, spec, run_id):
    exc = IncompleteExtractionError(paper_id=PID, arm="m", missing=("x",), n_stored=0,
                                    n_expected=1)
    with pytest.raises(X.ExhaustedWithoutRecord):
        X.outcome_for_exception(exc, paper_id=PID, arm=spec.extraction_models.arm,
                                run_id=run_id, stage_name="extract_pass2")
    assert _fev(db) == [] and _pev(db) == []


def test_run_faults_propagate_unmapped(spec):
    from engine.elicitation.classes import CodebookContractError
    with pytest.raises(CodebookContractError):
        X.outcome_for_exception(CodebookContractError("bad codebook"), paper_id=PID,
                                arm="a", run_id=1, stage_name="s")


# ── T12 (F9) ──────────────────────────────────────────────────────────
def test_t12_every_code_the_mapping_emits_is_in_the_closed_set(spec):
    from pydantic import BaseModel, ValidationError
    from engine.utils.ollama_client import CeilingUnavailable, InputDropped, InputOverflow

    class _M(BaseModel):
        x: int
    try:
        _M.model_validate_json("not json")
    except ValidationError as v:
        invalid = v
    cases = [NoParsedText(PID), ParsedTextMissing("u", Path("/p")),
             ParsedTextModified("u", Path("/p"), "a", "b"),
             InputOverflow(model="m", chars=1, estimate_low=2.0, ceiling=1),
             InputTruncated(model="m", count=1, ceiling=1, chars=1),
             InputDropped(model="m", count=1, chars=1, floor=2.0, ceiling=3),
             CeilingUnavailable(model="m", reason="r"), TimeoutError("t"), invalid,
             E.MissingThinkingChannelError("m"), KeyboardInterrupt(), ValueError("v")]
    codes = set()
    for exc in cases:
        out = X.outcome_for_exception(exc, paper_id=PID, arm="a", run_id=1, stage_name="s")
        assert out.reason_code in PS.EXTRACTION_REASON_CODES
        assert out.to_state in PS.PROCESSING_STATES and PS.is_failure(out.to_state)
        codes.add(out.reason_code)
    empty = X.ExtractionRecord(PID, "a", 1, "u", None, "m", None, (), ("x",), 1, "s")
    plan = X.plan_extraction_events(empty, live={}, from_state=None)
    codes.add(plan.paper_event["reason_code"])
    # E-DEADKI: an interrupt never reaches the mapping — it is run-level (C47),
    # pinned end to end by test_run_interrupts.py::test_t6_an_interrupt_mid_paper_
    # keeps_finished_papers_and_writes_none_for_it. Called directly with one, the
    # mapping has no interrupt branch and returns what the remaining code does.
    # REASON_RUN_INTERRUPTED stays in the closed set (R146) with no emitter.
    ki = X.outcome_for_exception(KeyboardInterrupt(), paper_id=PID, arm="a", run_id=1,
                                 stage_name="s")
    assert ki.reason_code == PS.REASON_UNCLASSIFIED_ERROR
    assert codes == PS.EXTRACTION_REASON_CODES - {PS.REASON_RUN_INTERRUPTED}


def test_t12_the_reason_codes_are_spelled_only_in_the_vocabulary():
    """R227 (10a-C6-B): extended from EXTRACTION_REASON_CODES alone to the
    union PROCESSING_REASON_CODES — the parse and acquisition codes must be
    spelled nowhere but paper_state.py either, same pin, same reason."""
    home = REPO / "engine" / "core" / "paper_state.py"
    pattern = re.compile("|".join(rf"[\"']{re.escape(c)}[\"']"
                                  for c in PS.PROCESSING_REASON_CODES))
    hits = [f.relative_to(REPO).as_posix() for f in (REPO / "engine").rglob("*.py")
            if f != home and pattern.search(f.read_text())]
    assert hits == []
    assert pattern.search(home.read_text())      # the pin can fire


def test_t12_processing_reasons_is_exactly_the_union_of_its_three_parts():
    """R227: PROCESSING_REASONS is the union of EXTRACTION_REASONS,
    PARSE_REASONS and ACQUISITION_REASONS — nothing more, nothing less."""
    assert PS.PROCESSING_REASON_CODES == (
        PS.EXTRACTION_REASON_CODES | frozenset(PS.PARSE_REASONS) | frozenset(PS.ACQUISITION_REASONS))
    # No overlap between the three closed sets.
    assert not (PS.EXTRACTION_REASON_CODES & frozenset(PS.PARSE_REASONS))
    assert not (PS.EXTRACTION_REASON_CODES & frozenset(PS.ACQUISITION_REASONS))
    assert not (frozenset(PS.PARSE_REASONS) & frozenset(PS.ACQUISITION_REASONS))
    # Every code maps to the token its own dict/family name says it should.
    assert set(PS.PARSE_REASONS.values()) == {"parse_failed"}
    assert set(PS.ACQUISITION_REASONS.values()) == {"full_text_not_obtainable"}


# ── D24 (12d-D24-R1): a new outcome supersedes the paper's old-text claims ──
V3_TEXT = "A corrected parse of the paper. A clean sentence.\n"


def _two_claims_on_the_first_text(db, spec, run_id, sentinels):
    """`study_design` and `country` claimed on the paper's first text; returns
    their claim ids."""
    arm = spec.extraction_models.arm
    X.write_extraction_events(
        db._conn, _record(db, spec, run_id, [_value("study_design"), _value("country", "USA")]),
        sentinels=sentinels)
    return (live_claims(db._conn, PID, "study_design", arm)[0],
            live_claims(db._conn, PID, "country", arm)[0])


def _reads(db, spec, sentinels, field):
    ev = effective_value(db._conn, PID, field, spec.extraction_models.arm, sentinels=sentinels)
    return ev.state, ev.rule_row, ev.value


def _superseded(db):
    return {r[1] for r in _fev(db, event_type="superseded")}


def test_d24_t1_an_incomplete_field_on_reextraction_is_superseded_and_reads_missing(
        db, spec, run_id, sentinels):
    arm = spec.extraction_models.arm
    _two_claims_on_the_first_text(db, spec, run_id, sentinels)
    write_parsed(db, PID, V3_TEXT)
    X.write_extraction_events(
        db._conn, _record(db, spec, run_id, [_value("study_design", "RCT2")],
                          incomplete=("country",)), sentinels=sentinels)
    assert _superseded(db) == {"study_design", "country"}
    assert [r[0] for r in _fev(db, field_name="country")] == ["asserted", "superseded"]
    assert live_claims(db._conn, PID, "country", arm) == []
    assert _reads(db, spec, sentinels, "country") == ("missing", 1, None)
    assert _reads(db, spec, sentinels, "study_design")[2] == "RCT2"
    new = resolve_parsed_text(db._conn, PID)
    payload = json.loads(_fev(db, field_name="country")[-1][2])
    assert payload["parsed_text_sha256"] == new.sha256
    sel = select_for_extraction(db._conn, arm=arm)
    assert sel.skipped_asserted == (PID,) and sel.to_extract == ()


def test_d24_t2_a_failed_reextraction_supersedes_every_old_text_claim(db, spec, run_id,
                                                                      sentinels):
    arm = spec.extraction_models.arm
    _two_claims_on_the_first_text(db, spec, run_id, sentinels)
    write_parsed(db, PID, V3_TEXT)
    out = X.outcome_for_exception(TimeoutError("gave up"), paper_id=PID, arm=arm,
                                  run_id=run_id, stage_name="extract_pass1")
    X.write_extraction_events(db._conn, out)
    assert _superseded(db) == {"study_design", "country"}
    assert _pev(db)[-1][:3] == ("extraction_failed", "extraction_failed",
                                PS.REASON_MODEL_CALL_FAILED)
    for field in ("study_design", "country"):
        assert _reads(db, spec, sentinels, field) == ("missing", 1, None)
    sel = select_for_extraction(db._conn, arm=arm)
    assert [p for p, _ in sel.to_extract] == [PID] and sel.skipped_asserted == ()


def test_d24_t3_a_selection_refusal_supersedes_every_old_text_claim_in_one_savepoint(
        db, spec, run_id, sentinels):
    arm = spec.extraction_models.arm
    _two_claims_on_the_first_text(db, spec, run_id, sentinels)
    write_parsed(db, PID, V3_TEXT).unlink()
    sel = select_for_extraction(db._conn, arm=arm)
    assert sel.skipped_refused == ((PID, PS.REASON_PARSED_TEXT_MISSING),)
    n_fe, n_pe = len(_fev(db)), len(_pev(db))

    # One savepoint: a failure at the paper event leaves no superseded event.
    with patch.object(X, "write_paper_event", side_effect=events.RunLinkRefused("boom")):
        with pytest.raises(events.RunLinkRefused):
            E.record_selection_refusals(db, sel, run_id=run_id)
    assert (len(_fev(db)), len(_pev(db))) == (n_fe, n_pe)

    assert E.record_selection_refusals(db, sel, run_id=run_id) == 1
    assert _superseded(db) == {"study_design", "country"}
    assert len(_fev(db)) == n_fe + 2 and len(_pev(db)) == n_pe + 1
    assert _pev(db)[-1][:3] == ("extraction_failed", "extraction_failed",
                                PS.REASON_PARSED_TEXT_MISSING)
    for field in ("study_design", "country"):
        assert _reads(db, spec, sentinels, field) == ("missing", 1, None)


def test_d24_t4_a_first_extraction_plans_no_superseded_event(db, spec, run_id, sentinels):
    """Run 7's shape — no earlier claim on the paper: stored and failed alike
    plan exactly the events they planned before D24."""
    arm = spec.extraction_models.arm
    stored = X.plan_extraction_events(
        _record(db, spec, run_id, [_value("study_design")], incomplete=("country",)),
        live={}, from_state=None, sentinels=sentinels)
    assert [fe["event_type"] for fe in stored.field_events] == ["asserted"]
    assert stored.paper_event["to_state"] == "extracted"
    failed = X.plan_extraction_events(
        X.PaperFailure(PID, arm, run_id, PS.REASON_MODEL_CALL_FAILED, "extract_pass1"),
        live={}, from_state=None, sentinels=sentinels)
    assert failed.field_events == () and failed.paper_event["to_state"] == "extraction_failed"
    # Through the writer, with a text on record and nothing live:
    X.write_extraction_events(
        db._conn, X.PaperFailure(PID, arm, run_id, PS.REASON_MODEL_CALL_FAILED,
                                 "extract_pass1"))
    assert _fev(db) == []


@pytest.mark.parametrize("how", ["stored_incomplete", "failed", "refused_at_selection"])
def test_d24_t5_another_arms_claims_on_the_paper_are_untouched(db, spec, run_id, sentinels,
                                                               how):
    from engine.core.effective import PRE_MANIFEST
    from _event_store_fixture import seed_claim
    arm = spec.extraction_models.arm
    events.register_arm(db._conn, "other_arm", "model", configuration_marker=PRE_MANIFEST)
    other = seed_claim(db._conn, arm="other_arm", paper_id=PID, field_name="country",
                       value="Norway", source_snippet="x")
    db._conn.commit()
    _two_claims_on_the_first_text(db, spec, run_id, sentinels)
    path = write_parsed(db, PID, V3_TEXT)
    if how == "stored_incomplete":
        X.write_extraction_events(
            db._conn, _record(db, spec, run_id, [_value("study_design", "RCT2")],
                              incomplete=("country",)), sentinels=sentinels)
    elif how == "failed":
        X.write_extraction_events(db._conn, X.PaperFailure(
            PID, arm, run_id, PS.REASON_MODEL_CALL_FAILED, "extract_pass1"))
    else:
        path.unlink()
        E.record_selection_refusals(db, select_for_extraction(db._conn, arm=arm),
                                    run_id=run_id)
    assert live_claims(db._conn, PID, "country", arm) == []
    assert live_claims(db._conn, PID, "country", "other_arm") == [other]
    assert db._conn.execute("SELECT COUNT(*) FROM field_events WHERE arm = 'other_arm'"
                            ).fetchone()[0] == 1


def test_d24_t6_after_a_partial_reextraction_the_audit_runs_on_the_paper(db, spec, run_id,
                                                                         sentinels):
    from engine.agents import audit_events as AE
    from engine.agents.auditor import AuditVerdict
    _two_claims_on_the_first_text(db, spec, run_id, sentinels)
    write_parsed(db, PID, V3_TEXT)
    X.write_extraction_events(
        db._conn, _record(db, spec, run_id, [_value("study_design", "RCT2")],
                          incomplete=("country",)), sentinels=sentinels)
    with patch.object(AE, "semantic_verify", return_value=AuditVerdict(
            status="flagged", grep_found=False, reasoning="r")):
        rep = AE.audit_run(db._conn, spec, run_id=run_id, arm=spec.extraction_models.arm,
                           review_dir=Path(db.db_path).parent)
    assert rep.skipped_refused == () and rep.papers_audited == 1
    assert effective_state(db._conn, PID).processing == "audited_ai"


def test_d24_t7_a_reviewer_decision_on_a_claim_the_reextraction_retired_needs_rereview(
        db, spec, run_id, sentinels):
    arm = spec.extraction_models.arm
    _, country = _two_claims_on_the_first_text(db, spec, run_id, sentinels)
    events.write_field_event(
        db._conn, event_type="human_corrected", paper_id=PID, field_name="country", arm=arm,
        claim_id=country, value="Canada", actor_kind="human", actor_role="reviewer",
        actor_name="PI", against_claims={country}, sentinels=sentinels, run_id=run_id)
    assert _reads(db, spec, sentinels, "country")[1] == 5
    write_parsed(db, PID, V3_TEXT)
    X.write_extraction_events(
        db._conn, _record(db, spec, run_id, [_value("study_design", "RCT2")],
                          incomplete=("country",)), sentinels=sentinels)
    state, row, value = _reads(db, spec, sentinels, "country")
    assert (row, value) == (3, None) and state.startswith("unresolved")
