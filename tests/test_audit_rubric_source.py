"""SCHEMA-DERIVE-01 C4 / R5 — one renderer, two readers (T7).

The human audit-review queue renders each field's contract with the same
function that builds the model's prompt block. Before this, it rendered
`spec.extraction_schema.fields[].description`, which is not what the model was
shown: the prompt's field content has always come from the codebook, and the
two texts differ on 19 of 20 fields (Phase 2a). A human adjudicator was judging
an extraction against a rubric the extractor never saw.
"""

from __future__ import annotations

import pytest

from engine.adjudication.audit_adjudicator import _build_audit_reference_content
from engine.agents.extractor import _build_field_block
from engine.core.codebook import load_codebook_for
from engine.core.review_paths import load_spec_for

REVIEW = "surgical_autonomy"


@pytest.fixture(scope="module")
def rendered():
    return _build_audit_reference_content(load_spec_for(REVIEW))


@pytest.fixture(scope="module")
def cb():
    return load_codebook_for(REVIEW)


def test_every_field_block_appears_verbatim(rendered, cb):
    """T7 — byte-for-byte, per field, modulo the two-space indent."""
    missing = []
    for entry in cb.fields:
        block = _build_field_block(entry)
        indented = "\n".join(f"  {ln}" if ln else "" for ln in block.split("\n"))
        if indented not in rendered:
            missing.append(entry["name"])
    assert missing == [], missing


def test_all_twenty_fields_are_present(rendered, cb):
    for name in cb.field_names:
        assert f"**{name}**" in rendered, name


def test_fields_appear_in_prompt_order(rendered, cb):
    """Same traversal as the prompt: tier 1→4, codebook order within."""
    order = [f["name"] for tier in (1, 2, 3, 4) for f in cb.fields_by_tier(tier)]
    positions = [rendered.index(f"**{n}**") for n in order]
    assert positions == sorted(positions)


def test_the_spec_description_is_no_longer_rendered(rendered):
    """The clearest single case: study_type's spec text is not in the sheet.

    Its spec description ended '...like 'prospective study,' 'case series,'
    etc.' with single quotes; the codebook's instruction uses double quotes and
    adds a fallback sentence. If the spec text reappears, the adjudicator has
    drifted back off the model's rubric.
    """
    assert "Look for explicit statements like 'prospective study,'" not in rendered
    assert 'Look for explicit statements like "prospective study,"' in rendered


def test_value_definitions_and_decision_criteria_reach_the_human(rendered):
    """The content the spec description never carried."""
    assert "**Value definitions:**" in rendered
    assert "**Decision criteria:**" in rendered
    assert "Primary research reporting new data" in rendered


def test_the_sheet_says_the_text_is_what_the_model_was_given(rendered):
    assert "EXTRACTION FIELD CONTRACTS" in rendered
    assert "as the extractor was given them" in rendered


def test_no_codebook_and_no_spec_renders_the_states_without_the_contracts():
    """The reference sheet still builds when neither is supplied."""
    out = _build_audit_reference_content()
    assert "AUDIT STATES" in out
    assert "EXTRACTION FIELD CONTRACTS" not in out


def test_an_explicit_codebook_is_used_when_given(cb):
    out = _build_audit_reference_content(spec=None, codebook=cb)
    assert "EXTRACTION FIELD CONTRACTS" in out
    assert "**study_type**" in out
