"""ELICIT-DESIGN-01/02 — the elicited two-pass flow, end to end, no live model.

Proves the properties that only show up when the pieces are assembled: the
Pass-1 response is read from the CONTENT channel, the stored snippet is the
engine's materialized unit text rather than whatever Pass 2 typed, and the unit
map is persisted per paper per run.

**Updated for ELICIT-DESIGN-02 Ruling 1.** The old
`test_contract_violation_stops_the_paper_before_pass_2` asserted the policy this
task replaced — one failing field dropped the whole paper. Its successors below
assert the new one: the failing FIELD takes the CONTRACT_UNMET terminal state
and stores no value, the fields that met their contracts store normally, and
Pass 2 is skipped only when NO field is left to supply a value for.
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
    "contract_unmet_token": "CONTRACT_UNMET",
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

# One field meets its contract, one does not. Ruling 1's whole point: this is a
# storable paper with one value and one recorded refusal, where ELICIT-DESIGN-01
# would have stored nothing at all.
PASS1_HALF = json.dumps({"fields": [
    {"field_name": "robot_platform", "unit_indices": [1], "value": "da Vinci Research Kit"},
    {"field_name": "country", "unit_indices": [], "value": "Canada"},
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


def test_a_failing_field_is_refused_and_the_rest_of_the_paper_is_stored(
        review, monkeypatch):
    """Ruling 1: the unit of refusal is the field, not the paper.

    ELICIT-DESIGN-01 stored nothing for p604 — a 15/20-clean extraction — over
    two fields. Here `country` is uncited and `robot_platform` is not, and the
    paper stores both: one value, one recorded refusal.
    """
    import engine.agents.extractor as E
    import engine.elicitation.pipeline as PL

    monkeypatch.setattr(PL, "ollama_chat", lambda **kw: _response(PASS1_HALF))
    monkeypatch.setattr(E, "ollama_chat", lambda **kw: _response(PASS2, thinking=None))
    db = _DB(review)
    PL.extract_paper_elicited(7, PAPER, _Spec(), db, run_id="run_TEST")

    spans = {s["field_name"]: s for s in db.stored["spans"]}
    assert len(spans) == 2, "all fields written, or none"
    assert spans["robot_platform"]["value"] == "da Vinci Research Kit"
    assert spans["country"]["value"] == "CONTRACT_UNMET"
    assert spans["country"]["source_snippet"] == ""
    assert spans["country"]["confidence"] == 0.0

    states = {e["field_name"]: e["terminal_state"] for e in db.stored["extracted_data"]}
    assert states == {"robot_platform": "EVIDENCED_VALUE", "country": "CONTRACT_UNMET"}


def test_an_uncited_value_is_never_stored_as_a_value(review, monkeypatch):
    """Ruling 1 moved the unit of refusal. It did not soften the refusal.

    `country` asserted "Canada" with nothing cited. The stored row must not
    contain "Canada" anywhere — not in the value, not in a snippet.
    """
    import engine.agents.extractor as E
    import engine.elicitation.pipeline as PL

    monkeypatch.setattr(PL, "ollama_chat", lambda **kw: _response(PASS1_HALF))
    monkeypatch.setattr(E, "ollama_chat", lambda **kw: _response(PASS2, thinking=None))
    db = _DB(review)
    PL.extract_paper_elicited(7, PAPER, _Spec(), db, run_id="run_TEST")

    country = next(s for s in db.stored["spans"] if s["field_name"] == "country")
    assert "Canada" not in country["value"]
    assert "Canada" not in (country["source_snippet"] or "")


def test_pass_2_is_skipped_when_no_field_survived(review, monkeypatch):
    """A doomed extraction still must not spend a second 32B call.

    ELICIT-DESIGN-01 got this right for the wrong reason — it refused the paper.
    The reason survives the ruling: with zero EVIDENCED_VALUE fields there is no
    value for Pass 2 to supply, so it is not called. The terminal states ARE the
    extraction, and they are stored.
    """
    import engine.agents.extractor as E
    import engine.elicitation.pipeline as PL

    pass2_calls = {"n": 0}
    pass1_calls = {"n": 0}

    def count_pass2(**kw):
        pass2_calls["n"] += 1
        return _response(PASS2, thinking=None)

    def count_pass1(**kw):
        pass1_calls["n"] += 1
        return _response(PASS1_BAD)

    monkeypatch.setattr(PL, "ollama_chat", count_pass1)
    monkeypatch.setattr(E, "ollama_chat", count_pass2)
    db = _DB(review)
    PL.extract_paper_elicited(7, PAPER, _Spec(), db, run_id="run_TEST")

    assert pass2_calls["n"] == 0, "nothing for Pass 2 to answer"
    assert pass1_calls["n"] == 2, "Ruling 4: one retry, with feedback"
    assert {s["value"] for s in db.stored["spans"]} == {"CONTRACT_UNMET"}


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
    refusal — incomplete Pass 2, a missing terminal state, an uncited value —
    because they are the same event (the answer cannot be stored) and
    separate budgets would let a paper alternate between them forever.

    Ruling 1 removed `Pass1ContractError` from that budget: a field that fails
    its contract no longer refuses the paper. The elicited path's own two-attempt
    Pass-1 loop (Ruling 4) is a different loop and is tested separately.
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


def test_the_completeness_predicate_and_the_contracts_share_one_field_set(
        review, monkeypatch):
    """Ruling 1: the redefined guard must derive its field set from the same
    traversal the per-class contracts were built from.

    Two sources would let the guard and the prompt disagree about what a paper
    is — the guard passing a paper the prompt never asked twenty questions of,
    or failing one it did. That disagreement is what Ruling 1 exists to end, so
    it is asserted on the assembled path rather than assumed from the code.
    """
    import engine.agents.extractor as E
    import engine.elicitation.pipeline as PL

    seen: dict[str, tuple] = {}

    real_check = PL.check_response
    real_enforce = PL.enforce_terminal_states

    def spy_check(raw, um, cb, expected):
        seen["contracts"] = tuple(expected)
        return real_check(raw, um, cb, expected)

    def spy_enforce(states, expected, vocab, **kw):
        seen["completeness"] = tuple(expected)
        return real_enforce(states, expected, vocab, **kw)

    monkeypatch.setattr(PL, "check_response", spy_check)
    monkeypatch.setattr(PL, "enforce_terminal_states", spy_enforce)
    monkeypatch.setattr(PL, "ollama_chat", lambda **kw: _response(PASS1_GOOD))
    monkeypatch.setattr(E, "ollama_chat", lambda **kw: _response(PASS2, thinking=None))

    PL.extract_paper_elicited(7, PAPER, _Spec(), _DB(review), run_id="run_TEST")

    assert seen["contracts"] == seen["completeness"] == ("robot_platform", "country")


def test_the_accepted_attempt_and_both_attempts_reach_telemetry(review, monkeypatch):
    """Ruling 4: a retry that regressed is a measurement. Discarding the losing
    attempt would delete the only evidence that the feedback did not land."""
    import engine.agents.extractor as E
    import engine.elicitation.pipeline as PL

    monkeypatch.setattr(PL, "ollama_chat", lambda **kw: _response(PASS1_HALF))
    monkeypatch.setattr(E, "ollama_chat", lambda **kw: _response(PASS2, thinking=None))
    PL.extract_paper_elicited(7, PAPER, _Spec(), _DB(review), run_id="run_TEST")

    tel = E._LAST_PASS1_TELEMETRY["elicitation"]
    assert tel["n_pass1_attempts"] == 2
    assert tel["accepted_attempt"] == 1, "a tie keeps attempt 1"
    assert tel["n_contract_unmet"] == 1 and tel["n_evidenced"] == 1
    assert tel["terminal_states"]["country"] == "CONTRACT_UNMET"
    assert [a["attempt"] for a in tel["attempts"]] == [1, 2]
    assert tel["attempts"][0]["feedback_chars"] == 0
    assert tel["attempts"][1]["feedback_chars"] > 0, "attempt 2 carried feedback"
