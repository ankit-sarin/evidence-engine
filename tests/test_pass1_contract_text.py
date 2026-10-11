"""P1 unit U04, commit 1 (R612): the Pass-1 prompt text R571(a)/(b) rules, the
sentinel rule's sentinel, the marker sentence, and the JUDGMENT step check.

INT-h12-2 (a) — a field's own instruction to record a sentinel does not set
aside the sentinel rule; said in the STATED and INFERABLE class contracts, next
to their field blocks (R612(e)). (b) — the JUDGMENT contract and its attempt-2
accompaniment state that at least one step must cite a unit.
INT-i12-38 — the sentinel rule names the codebook's canonical sentinel.
INT-i12-41 — the marker sentence says what the checker does.
INT-i12-1 — a step that is not an object, a `criteria_application` that is not
JSON `true`, and a step with no text each fail the field.

Text assertions here are on sentences a ruling put in the prompt; the hash
assertions show they are inside what the pin renders.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from engine.core import effective_config as ec
from engine.core.review_paths import load_spec_for
from engine.elicitation import classes as C
from engine.elicitation import contracts as K
from engine.elicitation import pipeline as PL
from engine.elicitation import prompts as P
from engine.elicitation.units import build_unit_map

CODEBOOK_PATH = Path("data/surgical_autonomy/extraction_codebook.yaml")

pytestmark = pytest.mark.skipif(
    not CODEBOOK_PATH.exists(), reason="review codebook not present"
)

PAPER = " ".join(f"Sentence number {i} carries some words for the unit map."
                 for i in range(1, 40))

#: `prompt_hash('elicitation_pass1')` on the live spec and codebook before this
#: unit (12l_U03/prompt_hash.json).
PASS1_HASH_BEFORE_U04 = "e3edb932f97469241dc6bc2de281c5ba6e5d719ea87f8165c01be5d8dbb67cc7"

T_B = "At least one step must cite a unit."
SENTINEL_RULE_ON_LIVE = (
    "**The sentinel rule.** A sentinel such as NR is a claim about the "
    "paper and must cite the sentence stating that absence. If no such "
    "sentence exists, output NO_EVIDENCE_LOCATABLE.")


def t_a(canonical: str, escape: str) -> str:
    return (f"A field instruction to record {canonical} does not set aside the sentinel "
            f"rule: cite the sentence that states the absence, or output {escape} if no "
            "such sentence exists.")


@pytest.fixture(scope="module")
def cb():
    return yaml.safe_load(CODEBOOK_PATH.read_text())


def _prompt(codebook: dict) -> str:
    names = tuple(f["name"] for f in codebook["fields"])
    return P.build_pass1_prompt(build_unit_map(1, PAPER), codebook, names)


def _sections(prompt: str) -> dict[str, str]:
    """The three class blocks and what follows them, by their own headings."""
    marks = ["### STATED", "### INFERABLE", "### JUDGMENT", "## Escape and absence"]
    at = [prompt.index(m) for m in marks]
    assert at == sorted(at)
    out = {m: prompt[a:b] for m, a, b in zip(marks, at, at[1:] + [len(prompt)])}
    return {"stated": out[marks[0]], "inferable": out[marks[1]],
            "judgment": out[marks[2]], "rest": out[marks[3]]}


@pytest.fixture(scope="module")
def elicited():
    spec = load_spec_for("surgical_autonomy")
    em = spec.extraction_models.model_copy(update={"elicitation": True})
    return spec.model_copy(update={"extraction_models": em})


# ── INT-h12-2 (a), (b) — R571, R612(e) ────────────────────────────────
def test_the_stated_and_judgment_contract_sentences_are_in_the_pass1_prompt(cb, elicited):
    assert (cb["canonical_absence_sentinel"], cb["escape_token"]) == (
        "NR", "NO_EVIDENCE_LOCATABLE")
    sentence = t_a("NR", "NO_EVIDENCE_LOCATABLE")
    prompt = _prompt(cb)
    s = _sections(prompt)

    # (a) in the STATED and the INFERABLE contract, each ahead of its field
    # blocks; not in the JUDGMENT block and not in the shared sentinel rule
    for cls in ("stated", "inferable"):
        assert s[cls].count(sentence) == 1
        assert s[cls].index(sentence) < s[cls].index("*Nothing citable for this field?*")
    assert sentence not in s["judgment"] and sentence not in s["rest"]
    assert prompt.count(sentence) == 2

    # (b) in the JUDGMENT contract, and in its attempt-2 accompaniment
    assert s["judgment"].count(T_B) == 1
    assert s["judgment"].index(T_B) < s["judgment"].index("*Nothing citable for this field?*")
    assert T_B not in s["stated"] and T_B not in s["inferable"] and T_B not in s["rest"]
    for code in (K.VALUE_WITHOUT_CITATION, K.STEPS_MISSING):
        assert T_B in P._requirement(code, C.JUDGMENT, "NO_EVIDENCE_LOCATABLE", 9)
    for cls in (C.STATED, C.INFERABLE):
        assert T_B not in P._requirement(K.VALUE_WITHOUT_CITATION, cls,
                                         "NO_EVIDENCE_LOCATABLE", 9)

    # both are inside the render the pin hashes, so the hash moves
    rendered = PL.sentinel_pass1_prompt(elicited, CODEBOOK_PATH)
    assert rendered.count(sentence) == 2 and rendered.count(T_B) == 1
    assert T_B in PL.sentinel_feedback_block(elicited, CODEBOOK_PATH)
    now = ec.prompt_hash("elicitation_pass1", elicited,
                         ec.stage_config("elicitation_pass1", elicited),
                         codebook_path=CODEBOOK_PATH)
    assert now != PASS1_HASH_BEFORE_U04


def test_the_contract_sentence_is_rendered_from_the_codebook(cb):
    other = {**cb, "canonical_absence_sentinel": "NOT_FOUND", "escape_token": "NOTHING_TO_CITE"}
    prompt = _prompt(other)
    assert prompt.count(t_a("NOT_FOUND", "NOTHING_TO_CITE")) == 2
    assert t_a("NR", "NO_EVIDENCE_LOCATABLE") not in prompt


# ── INT-i12-38 — R589 (row 56), A5 ────────────────────────────────────
def test_the_sentinel_rule_renders_the_canonical_sentinel(cb):
    # A5: on the live codebook the rule is byte-identical to the sentence before
    assert _prompt(cb).count(SENTINEL_RULE_ON_LIVE) == 1
    other = {**cb, "canonical_absence_sentinel": "NOT_FOUND"}
    assert "NOT_FOUND" in cb["absence_sentinels"]
    prompt = _prompt(other)
    assert SENTINEL_RULE_ON_LIVE.replace("such as NR", "such as NOT_FOUND") in prompt
    assert "such as NR " not in prompt


# ── INT-i12-41 — R589 (row 60) ────────────────────────────────────────
def test_the_marker_sentence_says_what_the_checker_does(cb):
    prompt = _prompt(cb)
    assert "the whole response is discarded" not in prompt
    assert ("A marker written into an index list is not valid JSON, and the field it "
            "is written in fails.") in " ".join(prompt.split())
    # what the checker does: the field with the marker fails, the other survives
    small = {"escape_token": "NO_EVIDENCE_LOCATABLE", "contract_unmet_token": "CONTRACT_UNMET",
             "absence_sentinels": ["NR"], "canonical_absence_sentinel": "NR",
             "fields": [{"name": "a", "type": "free_text", "field_class": "stated",
                         "definition": "A."},
                        {"name": "b", "type": "free_text", "field_class": "stated",
                         "definition": "B."}]}
    raw = ('{"fields": [{"field_name": "a", "unit_indices": [S2], "value": "x"}, '
           '{"field_name": "b", "unit_indices": [3], "value": "y"}]}')
    r = K.check_response(raw, build_unit_map(1, PAPER), small, ("a", "b"))
    assert r.parse_path == "recovered_marker_tokens"
    assert K.INDEX_MALFORMED in r.records["a"].fatal and r.records["b"].ok


# ── INT-i12-1 — R587, R612(c) ─────────────────────────────────────────
JUDGMENT_CODEBOOK = {
    "escape_token": "NO_EVIDENCE_LOCATABLE", "contract_unmet_token": "CONTRACT_UNMET",
    "absence_sentinels": ["NR"], "canonical_absence_sentinel": "NR",
    "fields": [{"name": "study_design", "type": "categorical", "field_class": "judgment",
                "definition": "The design."}],
}
GOOD_STEP = {"step": "Five subjects were enrolled.", "unit_indices": [3]}


def _judgment(steps, **extra) -> K.FieldRecord:
    entry = {"field_name": "study_design", "reasoning_steps": steps,
             "value": "Preclinical validation", **extra}
    r = K.check_response(json.dumps({"fields": [entry]}), build_unit_map(1, PAPER),
                         JUDGMENT_CODEBOOK, ("study_design",))
    return r.records["study_design"]


def test_judgment_step_shapes_that_carry_no_basis_fail_the_field():
    assert _judgment([GOOD_STEP]).ok
    assert _judgment([GOOD_STEP, {"step": "The criteria apply.",
                                  "criteria_application": True}]).ok

    # a bare-string step beside a valid one: kept, counted, and without a basis
    rec = _judgment([GOOD_STEP, "It looks preclinical to me."])
    assert not rec.ok and K.STEP_WITHOUT_BASIS in rec.fatal
    assert len(rec.steps) == 2 and rec.steps[1].text == "It looks preclinical to me."
    for other in (7, None, ["nested"], True):
        rec = _judgment([GOOD_STEP, other])
        assert not rec.ok and K.STEP_WITHOUT_BASIS in rec.fatal and len(rec.steps) == 2

    # criteria_application counts only as the JSON boolean true
    for not_true in ("false", "no", 0, "true", 1, "yes", False, None):
        rec = _judgment([GOOD_STEP, {"step": "The criteria apply.",
                                     "criteria_application": not_true}])
        assert not rec.ok and K.STEP_WITHOUT_BASIS in rec.fatal, not_true
        assert rec.steps[1].criteria_application is False

    # a step with indices and no text is not a reasoning step
    for text in ("", "   ", None):
        rec = _judgment([GOOD_STEP, {"step": text, "unit_indices": [4]}])
        assert not rec.ok and K.STEPS_MISSING in rec.fatal, text
    rec = _judgment([GOOD_STEP, {"unit_indices": [4]}])
    assert not rec.ok and K.STEPS_MISSING in rec.fatal
    # and neither is a criteria application with no text
    rec = _judgment([GOOD_STEP, {"criteria_application": True}])
    assert not rec.ok and K.STEPS_MISSING in rec.fatal

    # every failing shape uses the closed vocabulary: no new code, no new feedback text
    assert {K.STEP_WITHOUT_BASIS, K.STEPS_MISSING} <= K.FATAL and len(K.FATAL) == 10


# ── R616(c), commit 4 — the check follows R571(b) ─────────────────────
CRITERIA_STEP = {"step": "The codebook's criteria apply.", "criteria_application": True}


def test_a_judgment_field_needs_a_step_that_cites_a_unit():
    # a field-level citation and only criteria-application steps: not enough
    rec = _judgment([CRITERIA_STEP], unit_indices=[3])
    assert rec.indices == (3,) and not rec.ok
    assert rec.fatal == (K.VALUE_WITHOUT_CITATION,)
    rec = _judgment([CRITERIA_STEP, dict(CRITERIA_STEP)], unit_indices=[3, 4])
    assert not rec.ok and rec.fatal == (K.VALUE_WITHOUT_CITATION,)

    # the same field with one cited step passes, with or without the field-level citation
    assert _judgment([GOOD_STEP, CRITERIA_STEP], unit_indices=[3]).ok
    assert _judgment([GOOD_STEP, CRITERIA_STEP]).ok
    assert _judgment([CRITERIA_STEP, GOOD_STEP], unit_indices=[4]).indices == (4, 3)

    # no steps at all: still STEPS_MISSING, and the citation requirement is unmet too
    rec = _judgment([], unit_indices=[3])
    assert set(rec.fatal) == {K.STEPS_MISSING, K.VALUE_WITHOUT_CITATION}

    # the escape is untouched: no citation, no steps needed
    entry = {"field_name": "study_design", "unit_indices": [], "value": "NO_EVIDENCE_LOCATABLE"}
    r = K.check_response(json.dumps({"fields": [entry]}), build_unit_map(1, PAPER),
                         JUDGMENT_CODEBOOK, ("study_design",))
    assert r.records["study_design"].ok and r.records["study_design"].is_escape

    # the other classes are untouched: a field-level citation meets theirs
    stated = {**JUDGMENT_CODEBOOK, "fields": [{"name": "a", "type": "free_text",
                                               "field_class": "stated", "definition": "A."}]}
    r = K.check_response(json.dumps({"fields": [{"field_name": "a", "unit_indices": [3],
                                                 "value": "x"}]}),
                         build_unit_map(1, PAPER), stated, ("a",))
    assert r.records["a"].ok

    # attempt 2's feedback for the failing field already says what the rule requires
    failing = K.check_response(
        json.dumps({"fields": [{"field_name": "study_design", "unit_indices": [3],
                                "reasoning_steps": [CRITERIA_STEP],
                                "value": "Preclinical validation"}]}),
        build_unit_map(1, PAPER), JUDGMENT_CODEBOOK, ("study_design",))
    feedback = P.build_feedback_block(failing, JUDGMENT_CODEBOOK)
    assert f"✗ {K.VALUE_WITHOUT_CITATION} — " in feedback and T_B in feedback
