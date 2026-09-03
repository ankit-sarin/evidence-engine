"""ELICIT-DESIGN-01 — per-class Pass-1 contracts, tokens, indices and sizing.

Covers required tests 1, 2, 3 and 5. The write-boundary fail-fast (test 4) is in
`tests/test_citation_guard.py`, which exercises it as the mechanism-independent
predicate it is meant to be rather than through the prompt path.
"""

from __future__ import annotations

import json

import pytest

from engine.elicitation import classes as C
from engine.elicitation import contracts as K
from engine.elicitation import materialize as M
from engine.elicitation import sizing as S
from engine.elicitation.units import build_unit_map

CODEBOOK = {
    "escape_token": "NO_EVIDENCE_LOCATABLE",
    "absence_sentinels": ["NR", "NOT_FOUND", "NOT REPORTED"],
    "fields": [
        {"name": "robot_platform", "type": "free_text", "field_class": "stated",
         "definition": "The robot."},
        {"name": "country", "type": "free_text", "field_class": "inferable",
         "definition": "Where the study happened."},
        {"name": "study_design", "type": "categorical", "field_class": "judgment",
         "definition": "The design."},
    ],
}

TEXT = (
    "The system used a da Vinci Research Kit. "
    "All experiments ran at Vancouver General Hospital. "
    "Five porcine subjects were enrolled in the trial. "
    "The trajectory planner computed paths without operator input."
)

ALL_FIELDS = ("robot_platform", "country", "study_design")


@pytest.fixture
def unit_map():
    um = build_unit_map(7, TEXT)
    assert um.n == 4, um.units
    return um


def wrap(*entries) -> str:
    return json.dumps({"fields": list(entries)})


def check(raw, unit_map, fields=ALL_FIELDS):
    return K.check_response(raw, unit_map, CODEBOOK, fields)


# ══ Test 1 — per-class contract parsing ═══════════════════════════════


def test_stated_valid(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [1],
                    "value": "da Vinci Research Kit"}), unit_map, ("robot_platform",))
    rec = r.records["robot_platform"]
    assert rec.ok and rec.indices == (1,) and not rec.violations


def test_stated_without_citation_is_detected(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [],
                    "value": "da Vinci Research Kit"}), unit_map, ("robot_platform",))
    rec = r.records["robot_platform"]
    assert not rec.ok
    assert K.VALUE_WITHOUT_CITATION in rec.fatal


def test_inferable_valid(unit_map):
    r = check(wrap({"field_name": "country", "unit_indices": [2],
                    "inference": "Vancouver General Hospital is in Vancouver, so Canada.",
                    "value": "Canada"}), unit_map, ("country",))
    rec = r.records["country"]
    assert rec.ok and rec.inference and rec.indices == (2,)


def test_inferable_missing_declared_inference_is_detected(unit_map):
    r = check(wrap({"field_name": "country", "unit_indices": [2], "value": "Canada"}),
              unit_map, ("country",))
    rec = r.records["country"]
    assert not rec.ok and K.INFERENCE_MISSING in rec.fatal


def test_inferable_inference_longer_than_three_sentences_is_detected(unit_map):
    long = " ".join(f"Sentence number {i} explains a step." for i in range(1, 6))
    r = check(wrap({"field_name": "country", "unit_indices": [2],
                    "inference": long, "value": "Canada"}), unit_map, ("country",))
    assert K.INFERENCE_MALFORMED in r.records["country"].fatal


def test_judgment_valid_mixed_steps(unit_map):
    r = check(wrap({"field_name": "study_design",
                    "reasoning_steps": [
                        {"step": "Five porcine subjects were enrolled.", "unit_indices": [3]},
                        {"step": "Animal work with a stated n is preclinical validation "
                                 "under the codebook.", "criteria_application": True},
                    ],
                    "value": "Preclinical validation (animal/cadaver)"}),
              unit_map, ("study_design",))
    rec = r.records["study_design"]
    assert rec.ok and len(rec.steps) == 2 and rec.indices == (3,)


def test_judgment_step_with_neither_citation_nor_criteria_marker_is_detected(unit_map):
    r = check(wrap({"field_name": "study_design",
                    "reasoning_steps": [
                        {"step": "It looks preclinical to me."},
                    ],
                    "value": "Preclinical validation (animal/cadaver)"}),
              unit_map, ("study_design",))
    rec = r.records["study_design"]
    assert not rec.ok and K.STEP_WITHOUT_BASIS in rec.fatal


def test_judgment_without_steps_is_detected(unit_map):
    r = check(wrap({"field_name": "study_design", "unit_indices": [3],
                    "value": "Preclinical validation (animal/cadaver)"}),
              unit_map, ("study_design",))
    assert K.STEPS_MISSING in r.records["study_design"].fatal


def test_missing_field_is_detected_not_skipped(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [1], "value": "dVRK"}),
              unit_map)
    assert set(r.failed_fields) == {"country", "study_design"}
    assert K.FIELD_MISSING in r.records["country"].fatal


def test_unknown_field_is_recorded_not_absorbed(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [1], "value": "dVRK"},
                   {"field_name": "Title", "unit_indices": [1], "value": "x"}),
              unit_map, ("robot_platform",))
    assert r.unknown_fields == ("Title",)


def test_unparseable_response_is_not_silently_empty(unit_map):
    r = check("the model apologised instead of answering", unit_map, ("robot_platform",))
    assert r.parse_path == "unparseable" and not r.ok


# ══ Test 2 — escape token vs absence sentinel ═════════════════════════


def test_escape_accepted_with_zero_citations(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [],
                    "value": "NO_EVIDENCE_LOCATABLE"}), unit_map, ("robot_platform",))
    rec = r.records["robot_platform"]
    assert rec.ok and rec.is_escape and rec.indices == ()


def test_escape_rejected_when_it_carries_a_citation(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [1],
                    "value": "NO_EVIDENCE_LOCATABLE"}), unit_map, ("robot_platform",))
    rec = r.records["robot_platform"]
    assert not rec.ok and K.ESCAPE_WITH_CITATION in rec.fatal


def test_absence_sentinel_with_citation_is_accepted(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [4], "value": "NR"}),
              unit_map, ("robot_platform",))
    rec = r.records["robot_platform"]
    assert rec.ok and not rec.is_escape


def test_absence_sentinel_without_citation_is_rejected(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [], "value": "NR"}),
              unit_map, ("robot_platform",))
    rec = r.records["robot_platform"]
    assert not rec.ok and K.VALUE_WITHOUT_CITATION in rec.fatal


def test_escape_token_is_not_an_absence_sentinel():
    assert C.is_escape("NO_EVIDENCE_LOCATABLE", CODEBOOK)
    assert not C.is_absence_sentinel("NO_EVIDENCE_LOCATABLE", CODEBOOK)
    assert C.is_absence_sentinel("NOT_FOUND", CODEBOOK)
    assert not C.is_escape("NOT_FOUND", CODEBOOK)


def test_real_codebook_keeps_not_found_a_sentinel_and_coins_a_distinct_escape():
    """Section 4.1 ruling: absence_sentinels untouched, escape token additive."""
    cb = C.load("data/surgical_autonomy/extraction_codebook.yaml")
    assert C.escape_token(cb) == "NO_EVIDENCE_LOCATABLE"
    assert "NOT_FOUND" in C.absence_sentinels(cb)
    assert C.escape_token(cb).upper() not in C.absence_sentinels(cb)


# ══ Test 3 — index handling ═══════════════════════════════════════════


def test_valid_index_round_trips_to_verbatim_paper_text(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [1], "value": "dVRK"}),
              unit_map, ("robot_platform",))
    cites = M.citations(r.records["robot_platform"], unit_map)
    assert len(cites) == 1
    assert cites[0].text in unit_map.source_stripped
    assert cites[0].text == "The system used a da Vinci Research Kit."


def test_out_of_range_index_fails_the_field_and_is_recorded(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [1, 99],
                    "value": "dVRK"}), unit_map, ("robot_platform",))
    rec = r.records["robot_platform"]
    assert not rec.ok
    assert K.INDEX_OUT_OF_RANGE in rec.fatal
    assert 99 in rec.bad_indices
    assert rec.indices == (1,), "the valid index is kept for the record, not to rescue the field"


def test_malformed_index_fails_the_field(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": ["S1", 2.5, True],
                    "value": "dVRK"}), unit_map, ("robot_platform",))
    rec = r.records["robot_platform"]
    assert K.INDEX_MALFORMED in rec.fatal
    assert set(rec.bad_indices) == {"S1", 2.5, True}


def test_bools_are_malformed_not_index_one(unit_map):
    """bool is a subclass of int; True must never resolve to unit 1."""
    assert unit_map.resolve(True) is None
    assert unit_map.resolve(1) is not None


def test_duplicate_indices_are_advisory_not_fatal(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [1, 1, 1],
                    "value": "dVRK"}), unit_map, ("robot_platform",))
    rec = r.records["robot_platform"]
    assert rec.ok, "citing a unit twice supports the value exactly as well as once"
    assert rec.indices == (1,)
    assert rec.duplicate_indices == (1,)
    assert K.DUPLICATE_INDICES in rec.advisories


def test_value_before_evidence_is_advisory(unit_map):
    r = check(wrap({"field_name": "robot_platform", "value": "dVRK",
                    "unit_indices": [1]}), unit_map, ("robot_platform",))
    rec = r.records["robot_platform"]
    assert rec.ok
    assert K.VALUE_BEFORE_EVIDENCE in rec.advisories


def test_no_index_is_ever_silently_repaired(unit_map):
    """Every bad index appears verbatim in the record; none is clamped."""
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [0, 5, -3],
                    "value": "dVRK"}), unit_map, ("robot_platform",))
    rec = r.records["robot_platform"]
    assert rec.indices == ()
    assert set(rec.bad_indices) == {0, 5, -3}

# ══ Materialization ═══════════════════════════════════════════════════


def test_source_snippet_is_the_first_contiguous_run(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [1, 2, 4],
                    "value": "dVRK"}), unit_map, ("robot_platform",))
    snippet = M.source_snippet(r.records["robot_platform"], unit_map)
    assert snippet.startswith("The system used a da Vinci Research Kit.")
    assert "trajectory planner" not in snippet, "disjoint units must not be stitched"


def test_source_snippet_is_verbatim_contiguous_in_the_paper(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [1, 2],
                    "value": "dVRK"}), unit_map, ("robot_platform",))
    snippet = M.source_snippet(r.records["robot_platform"], unit_map)
    assert snippet in unit_map.source_stripped


def test_escape_field_materializes_no_snippet(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [],
                    "value": "NO_EVIDENCE_LOCATABLE"}), unit_map, ("robot_platform",))
    assert M.source_snippet(r.records["robot_platform"], unit_map) == ""


# ══ Regression — the aborted-smoke serialization failure ══════════════
#
# Measured in the aborted ELICIT-DESIGN-01 smoke: the model returned a
# contract-shaped answer whose index lists held bare [Sn] markers instead of
# integers, because the prompt had lost ELICIT-01's "Use the integer only, not
# the '[S12]' marker" line. Invalid JSON, so all 20 fields surfaced as
# FIELD_MISSING — a serialization slip read as total non-compliance. The prompt
# line is restored; these pin the backstop.


BARE_MARKER_RESPONSE = """```json
{
  "fields": [
    {"field_name": "robot_platform", "unit_indices": [S1], "value": "dVRK"},
    {"field_name": "country", "unit_indices": [S2, S3],
     "inference": "The affiliation fixes the country.", "value": "Canada"},
    {"field_name": "study_design",
     "reasoning_steps": [{"step": "Animals were used.", "unit_indices": [S3]}],
     "value": "Preclinical validation (animal/cadaver)"}
  ]
}
```"""


def test_bare_marker_response_is_a_detected_violation_not_an_unparseable_loss(unit_map):
    r = check(BARE_MARKER_RESPONSE, unit_map)
    assert r.parse_path == "recovered_marker_tokens"
    assert not r.records["robot_platform"].ok
    for name in ALL_FIELDS:
        rec = r.records[name]
        assert K.INDEX_MALFORMED in rec.fatal, name
        assert K.FIELD_MISSING not in rec.fatal, (
            f"{name}: a serialization slip must not read as a missing field"
        )


def test_bare_markers_are_refused_never_translated(unit_map):
    """The backstop repairs the CONTAINER, never the index."""
    r = check(BARE_MARKER_RESPONSE, unit_map)
    rec = r.records["robot_platform"]
    assert rec.indices == (), "S1 must not become index 1"
    assert "S1" in [str(b) for b in rec.bad_indices]
    assert M.source_snippet(rec, unit_map) == ""


def test_a_marker_inside_a_judgment_step_is_caught_too(unit_map):
    r = check(BARE_MARKER_RESPONSE, unit_map)
    rec = r.records["study_design"]
    assert K.INDEX_MALFORMED in rec.fatal
    assert rec.steps and rec.steps[0].indices == ()


def test_recovery_does_not_fire_on_a_valid_response(unit_map):
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [1], "value": "dVRK"}),
              unit_map, ("robot_platform",))
    assert r.parse_path == "direct"


def test_prose_containing_an_s_number_is_not_rewritten(unit_map):
    """The marker pattern is anchored to list context, not to the word 'S12'."""
    r = check(wrap({"field_name": "robot_platform", "unit_indices": [1],
                    "value": "Model S12 arm"}), unit_map, ("robot_platform",))
    assert r.parse_path == "direct"
    assert r.records["robot_platform"].value == "Model S12 arm"


def test_the_prompt_carries_the_integer_only_instruction():
    """The line whose absence caused the aborted smoke."""
    from engine.elicitation import classes as CB
    from engine.elicitation import prompts as P

    cb = CB.load("data/surgical_autonomy/extraction_codebook.yaml")
    names = tuple(f["name"] for f in cb["fields"])
    prompt = P.build_pass1_prompt(build_unit_map(1, TEXT), cb, names)
    assert "Use the\ninteger only" in prompt or "Use the integer only" in prompt
    assert "never `[S12, S13]`" in prompt
    assert "never `[S7]`" in prompt, "the JUDGMENT step contract needs it too"
