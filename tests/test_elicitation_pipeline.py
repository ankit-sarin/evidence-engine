"""ELICIT-DESIGN-01 — the elicited two-pass flow, end to end, with no live model.

Proves the four properties that only show up when the pieces are assembled: the
Pass-1 response is read from the CONTENT channel, the stored snippet is the
engine's materialized unit text rather than whatever Pass 2 typed, the unit map
is persisted per paper per run, and a contract violation stops the paper before
Pass 2 spends a second call.
"""

from __future__ import annotations

import json
import types
from pathlib import Path

import pytest
import yaml

from engine.agents.models import EvidenceSpan, ExtractionOutput
from unittest.mock import patch

from engine.core.citation_guard import (
    STRICT, UncitedValueError, VALUE_WITHOUT_CITATION,
)

PAPER = (
    "The system used a da Vinci Research Kit. "
    "All experiments ran at Vancouver General Hospital. "
    "Five porcine subjects were enrolled in the trial. "
    "The trajectory planner computed paths without operator input."
)

CODEBOOK = {
    "escape_token": "NO_EVIDENCE_LOCATABLE",
    "absence_sentinels": ["NR", "NOT_FOUND"],
    "fields": [
        {"name": "robot_platform", "type": "free_text", "field_class": "stated",
         "definition": "The robot.", "tier": 1},
        {"name": "country", "type": "free_text", "field_class": "inferable",
         "definition": "Where.", "tier": 2},
    ],
}

PASS1_GOOD = json.dumps({"fields": [
    {"field_name": "robot_platform", "unit_indices": [1], "value": "da Vinci Research Kit"},
    {"field_name": "country", "unit_indices": [2],
     "inference": "Vancouver General Hospital is in Vancouver, so the country is Canada.",
     "value": "Canada"},
]})

PASS1_BAD = json.dumps({"fields": [
    {"field_name": "robot_platform", "unit_indices": [], "value": "da Vinci Research Kit"},
    {"field_name": "country", "unit_indices": [2], "value": "Canada"},
]})

PASS2 = ExtractionOutput(fields=[
    EvidenceSpan(field_name="robot_platform", value="da Vinci Research Kit",
                 source_snippet="the authors used a da Vinci system (paraphrased)",
                 confidence=0.9, tier=1),
    EvidenceSpan(field_name="country", value="Canada",
                 source_snippet="somewhere in Canada (paraphrased)",
                 confidence=0.8, tier=2),
]).model_dump_json()


def _response(content, thinking="some reasoning"):
    return types.SimpleNamespace(
        message=types.SimpleNamespace(content=content, thinking=thinking),
        done_reason="stop", prompt_eval_count=1234, eval_count=99,
    )


class _Field:
    def __init__(self, name, tier):
        self.name, self.tier = name, tier


class _Schema:
    def fields_by_tier(self, tier):
        return [_Field(n, t) for n, t in
                (("robot_platform", 1), ("country", 2)) if t == tier]


class _Models:
    elicitation = True
    pass1_think = True
    pass2_think = False


class _Spec:
    extraction_schema = _Schema()
    extraction_models = _Models()

    def extraction_hash(self):
        return "deadbeef"


class _DB:
    def __init__(self, path):
        self.db_path = str(path / "review.db")
        self.stored = None

    def add_extraction_atomic(self, **kw):
        self.stored = kw
        return 1


@pytest.fixture
def review(tmp_path):
    (tmp_path / "extraction_codebook.yaml").write_text(yaml.safe_dump(CODEBOOK))
    return tmp_path


def _run(review, monkeypatch, pass1_content, run_id="run_TEST"):
    import engine.agents.extractor as E
    import engine.elicitation.pipeline as PL

    monkeypatch.setattr(PL, "ollama_chat", lambda **kw: _response(pass1_content))
    monkeypatch.setattr(E, "ollama_chat", lambda **kw: _response(PASS2, thinking=None))
    db = _DB(review)
    return db, PL.extract_paper_elicited(7, PAPER, _Spec(), db, run_id=run_id)


def test_stored_snippet_is_the_engines_materialized_text_not_pass2s(review, monkeypatch):
    db, result = _run(review, monkeypatch, PASS1_GOOD)
    snippets = {s["field_name"]: s["source_snippet"] for s in db.stored["spans"]}
    assert snippets["robot_platform"] == "The system used a da Vinci Research Kit."
    assert snippets["country"] == "All experiments ran at Vancouver General Hospital."
    for s in snippets.values():
        assert "paraphrased" not in s
        assert s in PAPER, "every stored snippet is verbatim paper text"


def test_values_come_from_pass2(review, monkeypatch):
    db, _ = _run(review, monkeypatch, PASS1_GOOD)
    values = {s["field_name"]: s["value"] for s in db.stored["spans"]}
    assert values == {"robot_platform": "da Vinci Research Kit", "country": "Canada"}


def test_reasoning_trace_is_the_materialized_evidence(review, monkeypatch):
    db, _ = _run(review, monkeypatch, PASS1_GOOD)
    trace = db.stored["reasoning_trace"]
    assert "[S1]" in trace and "[S2]" in trace
    assert "Declared inference:" in trace
    assert "The system used a da Vinci Research Kit." in trace


def test_pass1_is_read_from_the_content_channel(review, monkeypatch):
    """Production Pass 1 reads message.thinking and discards content; this
    design reads content, which is where the citations arrive."""
    import engine.agents.extractor as E
    import engine.elicitation.pipeline as PL

    monkeypatch.setattr(PL, "ollama_chat",
                        lambda **kw: _response(PASS1_GOOD, thinking="unrelated musing"))
    monkeypatch.setattr(E, "ollama_chat", lambda **kw: _response(PASS2, thinking=None))
    db = _DB(review)
    PL.extract_paper_elicited(7, PAPER, _Spec(), db, run_id="run_TEST")
    assert db.stored is not None
    assert "unrelated musing" not in db.stored["reasoning_trace"]


def test_unit_map_is_persisted_per_paper_per_run(review, monkeypatch):
    _run(review, monkeypatch, PASS1_GOOD, run_id="run_20260903T000000Z")
    path = review / "elicitation" / "run_20260903T000000Z" / "unit_maps" / "7.json"
    assert path.exists()
    data = json.loads(path.read_text())
    assert data["paper_id"] == 7 and data["n_units"] == 4
    assert data["units"][0] == "The system used a da Vinci Research Kit."
    assert data["version"] and data["min_unit_tokens"] == 3


def test_contract_violation_stops_the_paper_before_pass_2(review, monkeypatch):
    import engine.agents.extractor as E
    import engine.elicitation.pipeline as PL

    pass2_calls = {"n": 0}

    def count_pass2(**kw):
        pass2_calls["n"] += 1
        return _response(PASS2, thinking=None)

    monkeypatch.setattr(PL, "ollama_chat", lambda **kw: _response(PASS1_BAD))
    monkeypatch.setattr(E, "ollama_chat", count_pass2)
    db = _DB(review)
    with pytest.raises(PL.Pass1ContractError) as exc:
        PL.extract_paper_elicited(7, PAPER, _Spec(), db, run_id="run_TEST")
    assert pass2_calls["n"] == 0, "a doomed extraction must not spend a second 32B call"
    assert db.stored is None
    assert "robot_platform" in str(exc.value) and "country" in str(exc.value)


def test_telemetry_carries_the_per_field_citation_record(review, monkeypatch):
    import engine.agents.extractor as E

    _run(review, monkeypatch, PASS1_GOOD)
    tel = E._LAST_PASS1_TELEMETRY["elicitation"]
    assert tel["parse_path"] == "direct" and tel["n_units"] == 4
    assert tel["fields"]["robot_platform"]["indices"] == [1]
    assert tel["fields"]["country"]["class"] == "inferable"
    assert tel["fields"]["country"]["has_inference"] is True
    assert tel["pass1_truncation_tripwire"] is False


def test_elicitation_is_off_by_default(review, monkeypatch):
    """Upgrading the engine changes no existing review's behaviour."""
    from engine.core.review_spec import ExtractionModels

    assert ExtractionModels().elicitation is False


def test_dispatch_follows_the_spec_flag(review, monkeypatch):
    import engine.agents.extractor as E

    seen = {}
    monkeypatch.setattr(
        "engine.elicitation.pipeline.extract_paper_elicited",
        lambda *a, **kw: seen.setdefault("elicited", True),
    )
    monkeypatch.setattr(E, "build_extraction_prompt",
                        lambda *a, **kw: seen.setdefault("legacy", True) or "p")
    E.extract_paper(7, PAPER, _Spec(), _DB(review))
    assert seen == {"elicited": True}


def test_write_guard_also_fires_on_the_elicited_path(review, monkeypatch):
    """Defence in depth: even if a contract passed, an uncited span is refused."""
    import engine.agents.extractor as E
    import engine.elicitation.pipeline as PL

    monkeypatch.setattr(PL, "ollama_chat", lambda **kw: _response(PASS1_GOOD))
    monkeypatch.setattr(E, "ollama_chat", lambda **kw: _response(PASS2, thinking=None))
    monkeypatch.setattr(PL.M, "source_snippet", lambda rec, um: "")
    monkeypatch.setattr(PL, "enforce_citations", _uncited_stub)
    db = _DB(review)
    with pytest.raises(UncitedValueError):
        PL.extract_paper_elicited(7, PAPER, _Spec(), db, run_id="run_TEST")
    assert db.stored is None


def _uncited_stub(spans, **kw):
    from engine.core.citation_guard import enforce_citations
    return enforce_citations(spans, **{**kw, "citation_counts": None})


def test_retry_is_bounded_and_the_paper_is_failed(tmp_path, monkeypatch):
    """A paper that never satisfies the contract is failed, not stored.

    The retry driver shares ONE bounded budget across every pre-write
    refusal — incomplete Pass 2, Pass-1 contract violation, uncited value —
    because they are the same event (the answer cannot be stored) and
    separate budgets would let a paper alternate between them forever.
    """
    import engine.agents.extractor as E

    calls = {"n": 0}

    def always_uncited(*a, **kw):
        calls["n"] += 1
        raise UncitedValueError(paper_id=1, arm=E.MODEL,
                                offenders=(("robot_platform", VALUE_WITHOUT_CITATION),),
                                mode=STRICT, attempt=calls["n"])

    db = _DB(tmp_path)
    (tmp_path / "extraction_codebook.yaml").write_text("fields: []\n")

    class _Schema:
        def fields_by_tier(self, tier):
            return []

    class _Spec:
        extraction_schema = _Schema()

    monkeypatch.setattr(E, "extract_paper", always_uncited)
    with patch.object(E, "record_call"):
        with pytest.raises(UncitedValueError):
            E.extract_paper_with_completeness(
                1, "text", _Spec(), db, max_attempts=3,
            )
    assert calls["n"] == 3, "exactly the bounded budget, no more"
    assert db.stored is None
