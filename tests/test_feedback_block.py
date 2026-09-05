"""ELICIT-DESIGN-02 Ruling 4 — the typed feedback block.

ELICIT-DESIGN-01's F7 measured what an identical retry buys at temperature 0:
nothing, and sometimes less than nothing. Per-attempt failing-field counts were
121 = 6->7->7, 604 = 2->5->5, 498 = 13->13->13. Nine 32B calls bought nothing
over three, and p604's retry cost three fields that its first answer had right.

The diagnosis was that the retry policy was inherited from the completeness
guard, where re-issuing an identical request IS correct because the failure is
response SHAPE. A contract failure is response CONTENT, and an identical request
is the one instrument guaranteed not to change it. This block is what makes
attempt 2 a different request.
"""

from __future__ import annotations

from engine.elicitation import classes as C
from engine.elicitation.contracts import (
    FieldRecord, INDEX_MALFORMED, INFERENCE_MISSING, Pass1Result, STEPS_MISSING,
    VALUE_WITHOUT_CITATION,
)
from engine.elicitation.prompts import (
    FEEDBACK_TRUNCATION_MARKER, build_feedback_block,
)

CB = {
    "escape_token": "NO_EVIDENCE_LOCATABLE",
    "contract_unmet_token": "CONTRACT_UNMET",
    "absence_sentinels": ["NR"],
    "fields": [{"name": "sample_size", "field_class": "stated", "type": "free_text"}],
}


def _result(*recs, n_units=152):
    return Pass1Result(records={r.field_name: r for r in recs},
                       parse_path="direct", n_units=n_units)


UNCITED = FieldRecord("sample_size", "stated", "12 patients", False,
                      violations=(VALUE_WITHOUT_CITATION,))
NO_STEPS = FieldRecord("key_limitation", "judgment", "Small cohort", False,
                       indices=(4,), violations=(STEPS_MISSING,))
NO_INFER = FieldRecord("country", "inferable", "Canada", False, indices=(2,),
                       violations=(INFERENCE_MISSING,))
BAD_INDEX = FieldRecord("study_type", "stated", "Review", False,
                        bad_indices=("S12", 9999), violations=(INDEX_MALFORMED,))
CLEAN = FieldRecord("robot_platform", "stated", "da Vinci", False, indices=(1,))


def test_it_names_the_field_the_code_the_output_and_the_requirement():
    block = build_feedback_block(_result(UNCITED), CB)
    assert "sample_size" in block                       # field
    assert VALUE_WITHOUT_CITATION in block              # code
    assert "12 patients" in block                       # offending output
    assert "cite at least one unit that asserts the value" in block   # requirement


def test_clean_fields_are_not_mentioned():
    block = build_feedback_block(_result(UNCITED, CLEAN), CB)
    assert "robot_platform" not in block
    assert "sample_size" in block


def test_no_failing_fields_means_no_block():
    assert build_feedback_block(_result(CLEAN), CB) == ""


def test_the_requirement_is_class_specific():
    judgment = build_feedback_block(_result(NO_STEPS), CB)
    inferable = build_feedback_block(_result(NO_INFER), CB)
    assert "reason in steps" in judgment
    assert "criteria_application" in judgment
    assert "declare the inference" in inferable
    assert "reason in steps" not in inferable


def test_it_offers_the_escape_token_on_every_uncited_value():
    """F1: the token is not being reached for. The retry is another chance to say so."""
    block = build_feedback_block(_result(UNCITED), CB)
    assert "NO_EVIDENCE_LOCATABLE" in block
    assert "EMPTY citation list" in block


def test_it_teaches_directly_stated_on_an_inferable_failure():
    block = build_feedback_block(_result(NO_INFER), CB)
    assert "DIRECTLY_STATED" in block


def test_unresolved_indices_are_echoed_verbatim():
    """Telemetry keeps `bad_indices` unnormalized so the model sees what it wrote."""
    block = build_feedback_block(_result(BAD_INDEX), CB)
    assert "S12" in block and "9999" in block
    assert "never `[S12]`" in block


def test_the_echo_is_capped_and_the_truncation_is_visible():
    """D5: a silently shortened echo shows the model a doctored artifact of its
    own output and invites it to 'fix' wording it never wrote."""
    long_value = "x" * 5000
    rec = FieldRecord("sample_size", "stated", long_value, False,
                      violations=(VALUE_WITHOUT_CITATION,))
    block = build_feedback_block(_result(rec), CB, cap=200)
    assert FEEDBACK_TRUNCATION_MARKER in block
    assert long_value not in block
    assert len(block) < 2000


def test_a_short_value_is_not_marked():
    block = build_feedback_block(_result(UNCITED), CB, cap=200)
    assert FEEDBACK_TRUNCATION_MARKER not in block


def test_it_is_deterministic():
    res = _result(UNCITED, NO_STEPS, NO_INFER, BAD_INDEX)
    assert build_feedback_block(res, CB) == build_feedback_block(res, CB)


def test_it_demands_a_full_replacement_not_a_patch():
    """Pass 1 elicits all fields in ONE request; a partial answer would arrive
    with nineteen FIELD_MISSING violations."""
    block = build_feedback_block(_result(UNCITED), CB)
    assert "re-emit EVERY field" in block


def test_it_names_no_field_of_its_own():
    """Gate 2 holds on the retry path: requirements come from the class."""
    import inspect

    from engine.elicitation import prompts as P

    body = inspect.getsource(P._requirement) + inspect.getsource(P.build_feedback_block)
    for name in ("sample_size", "key_limitation", "comparison_to_human",
                 "surgical_domain", "country"):
        assert name not in body


def test_the_out_of_range_requirement_states_the_real_range():
    rec = FieldRecord("study_type", "stated", "Review", False,
                      bad_indices=(9999,), violations=("INDEX_OUT_OF_RANGE",))
    block = build_feedback_block(_result(rec, n_units=304), CB)
    assert "1 to 304" in block
