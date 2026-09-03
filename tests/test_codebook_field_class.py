"""ELICIT-DESIGN-01 C1 — the codebook, the mirror and the document must agree.

Prompt construction reads the codebook, so if the codebook drifts the prompts
hold the model to a contract nobody ratified. The assignments are NOT authored
in the codebook: they are reproduced from `analysis/provenance/FIELD_CLASSES.md`
section 2 (`prov-fieldclass-1`), a pre-registration artifact whose criteria were
written and hashed before any field was examined, and from its machine-readable
mirror `analysis/provenance/field_class3.py`. On disagreement the codebook is
the copy that is wrong.
"""


from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest
import yaml

from analysis.provenance.field_class3 import FIELD_CLASS3
from engine.elicitation import classes as C

CODEBOOK_PATH = Path("data/surgical_autonomy/extraction_codebook.yaml")
DOC = Path("analysis/provenance/FIELD_CLASSES.md")

pytestmark = pytest.mark.skipif(
    not CODEBOOK_PATH.exists(), reason="review codebook not present"
)


@pytest.fixture(scope="module")
def cb():
    return yaml.safe_load(CODEBOOK_PATH.read_text())


# ══ C1 — three-way pin ════════════════════════════════════════════════


def test_every_codebook_field_declares_a_class(cb):
    missing = [f["name"] for f in cb["fields"] if not f.get("field_class")]
    assert not missing, f"fields with no field_class: {missing}"


def test_codebook_agrees_with_the_machine_readable_mirror(cb):
    for f in cb["fields"]:
        expected = FIELD_CLASS3[f["name"]][0]
        assert f["field_class"] == expected, (
            f"{f['name']}: codebook says {f['field_class']!r}, field_class3 says "
            f"{expected!r}. The codebook is the copy that is wrong."
        )


def test_codebook_agrees_with_the_pre_registration_document(cb):
    text = DOC.read_text()
    for f in cb["fields"]:
        row = next((ln for ln in text.splitlines()
                    if re.search(rf"\|\s*{re.escape(f['name'])}\s*\|", ln)), None)
        assert row is not None, f"{f['name']} missing from FIELD_CLASSES.md"
        assert f["field_class"].upper() in row, (
            f"{f['name']}: codebook and FIELD_CLASSES.md section 2 disagree"
        )


def test_class_totals_are_the_ratified_nine_six_five(cb):
    counts = {c: len(C.fields_by_class(c, cb)) for c in C.CLASSES}
    assert counts == {C.STATED: 9, C.INFERABLE: 6, C.JUDGMENT: 5}


def test_the_nine_stated_fields_are_the_elicit01_field_set(cb):
    from analysis.eval.elicit01.prompts import stated_fields

    assert [f["name"] for f in C.fields_by_class(C.STATED, cb)] == \
           [f["name"] for f in stated_fields(CODEBOOK_PATH)]


def test_paper_variable_fields_carry_their_censused_class(cb):
    """C2 ruling: run the ratified classes unchanged; the smoke measures the cost."""
    known = C.classes_by_field(cb)
    assert known["sample_size"] == C.STATED
    assert known["surgical_domain"] == C.INFERABLE
