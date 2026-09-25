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
        attempts=1, stage_name="extract_pass2")


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
    with patch.object(E, "extract_pass1_reasoning", return_value="trace"), \
         patch.object(E, "extract_pass2_structured", return_value=_pass2(spans)):
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
        return _pass2(spans)
    with patch.object(E, "extract_pass1_reasoning", return_value="trace"), \
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
        actor_name="m", payload=claim_identity(arm, PID), run_id=run_id)
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
    assert codes == PS.EXTRACTION_REASON_CODES


def test_t12_the_reason_codes_are_spelled_only_in_the_vocabulary():
    home = REPO / "engine" / "core" / "paper_state.py"
    pattern = re.compile("|".join(rf"[\"']{re.escape(c)}[\"']"
                                  for c in PS.EXTRACTION_REASON_CODES))
    hits = [f.relative_to(REPO).as_posix() for f in (REPO / "engine").rglob("*.py")
            if f != home and pattern.search(f.read_text())]
    assert hits == []
    assert pattern.search(home.read_text())      # the pin can fire
