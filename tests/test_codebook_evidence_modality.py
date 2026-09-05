"""ELICIT-DESIGN-02 Ruling 3(b) — a field's evidence-modality flag may not
contradict its class contract.

Sits in the pin-test family beside `test_codebook_field_class.py`, and for the
same reason: prompt construction reads the codebook, so a codebook that asks for
one kind of evidence while the class contract asks for another holds the model to
two incompatible standards at once and then records its failure to meet both.

ELICIT-DESIGN-01's F5 is the measurement behind the rule. `key_limitation`
carried `source_quote_required: true` on a JUDGMENT field and returned
STEPS_MISSING on 5 of 6 attempts across all three smoke papers — "the codebook
asks for a quote, and the class contract asks for steps, and the model does the
first". That is a codebook defect wearing a model failure's clothes.

The gate requires the lint to FAIL on the pre-fix codebook and PASS after, so
both directions are asserted here: the shipped codebook is clean, and a
reconstruction of the pre-fix one is caught.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path

import pytest
import yaml

from engine.elicitation import classes as C

CODEBOOK_PATH = Path("data/surgical_autonomy/extraction_codebook.yaml")

pytestmark = pytest.mark.skipif(
    not CODEBOOK_PATH.exists(), reason="review codebook not present"
)


@pytest.fixture(scope="module")
def cb():
    return yaml.safe_load(CODEBOOK_PATH.read_text())


def test_the_shipped_codebook_is_clean(cb):
    assert C.check_evidence_modality(cb) == []


def test_the_pre_fix_codebook_is_caught(cb):
    """The exact defect F5 diagnosed, reconstructed and re-detected.

    Both fields that carried `source_quote_required` before this task were
    JUDGMENT. Restoring the flag on them must fail the lint — otherwise the lint
    would have passed on the codebook it was written to catch.
    """
    pre_fix = copy.deepcopy(cb)
    restored = []
    for f in pre_fix["fields"]:
        if f["name"] in ("key_limitation", "clinical_readiness_assessment"):
            f["source_quote_required"] = True
            restored.append(f["name"])
    assert restored == ["key_limitation", "clinical_readiness_assessment"]

    violations = C.check_evidence_modality(pre_fix)
    assert len(violations) == 2
    assert all("source_quote_required" in v for v in violations)
    assert any("key_limitation" in v for v in violations)
    assert any("clinical_readiness_assessment" in v for v in violations)


def test_the_flag_is_legal_on_a_stated_field(cb):
    """The rule is about contradiction, not about banning the flag.

    A STATED field's value IS asserted by a passage, so "quote it" and "cite the
    unit asserting it" ask for the same evidence. A lint that failed here would
    be enforcing a preference rather than catching an incoherence.
    """
    ok = copy.deepcopy(cb)
    stated = next(f for f in ok["fields"] if f["field_class"] == C.STATED)
    stated["source_quote_required"] = True
    assert C.check_evidence_modality(ok) == []


def test_the_rule_is_derived_from_the_class_not_from_a_field_list():
    """Gate 2 holds for the lint too: no field is named in the rule."""
    import inspect

    body = inspect.getsource(C.check_evidence_modality) + repr(C._MODALITY_COMPATIBLE)
    for name in ("key_limitation", "clinical_readiness_assessment",
                 "surgical_domain", "sample_size"):
        assert name not in body, f"{name} is hand-listed in the lint rule"


def test_the_rewritten_judgment_fields_ask_for_steps(cb):
    """Ruling 3(a) and the A3 follow-on: the instructions were rewritten, not
    merely de-flagged. Removing the flag while the prose still says "quote the
    evidence" would leave the F5 collision intact one layer down."""
    for name in ("key_limitation", "clinical_readiness_assessment"):
        f = next(x for x in cb["fields"] if x["name"] == name)
        assert "source_quote_required" not in f
        assert f["field_class"] == C.JUDGMENT
        instruction = f["instruction"].lower()
        assert "step" in instruction, name
        assert "cite the unit" in instruction, name


def test_no_judgment_instruction_demands_a_quote(cb):
    """The prose-level analogue of the flag-level lint, pinning the A3 audit.

    `check_evidence_modality` reads FLAGS. An instruction that says "Quote the key
    evidence" is the same contradiction in prose, and the A3 audit found exactly
    one such field left after Ruling 3(a) -- `clinical_readiness_assessment`,
    since rewritten. This test is what stops a third one appearing unnoticed.

    Matched on the IMPERATIVE verb only. `key_limitation` legitimately contains
    the noun "quotation" inside a negation -- "the steps are how you got to YOUR
    judgment, not a quotation of theirs" -- and a lint that could not tell a
    demand from its refusal would force the negation out of a sentence whose whole
    job is to make it.
    """
    imperative = re.compile(r"\b(quote|copy|reproduce|transcribe)\b", re.I)
    for f in cb["fields"]:
        if f["field_class"] != C.JUDGMENT:
            continue
        hits = imperative.findall(f.get("instruction", ""))
        assert not hits, f"{f['name']}: instruction demands {hits} on a JUDGMENT field"


def test_the_stated_copy_instruction_is_left_alone(cb):
    """The rule is class-scoped, not a global ban on the word.

    `primary_outcome_value` is STATED and says "Copy the exact numeric reported",
    which is correct for a field whose value the paper asserts verbatim. A lint
    that fired here would be enforcing a style, not catching an incoherence.
    """
    f = next(x for x in cb["fields"] if x["name"] == "primary_outcome_value")
    assert f["field_class"] == C.STATED
    assert "copy" in f["instruction"].lower()
    assert C.check_evidence_modality(cb) == []
