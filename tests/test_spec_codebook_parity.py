"""CODEBOOK-AUTH-01 Phase 2 C10 — the spec's schema equals the codebook's.

R1 makes the codebook the single field authority and the spec's
`extraction_schema` derived from it. The derivation itself lands in
SCHEMA-DERIVE-01; until it does, this test is what holds the two in step.

It is not a style check. The two files carried `type` independently and
disagreed on nine of twenty fields, one of them substantively —
`sample_size` was `text` to the spec and `numeric` to the codebook — and
nothing in the engine compared them. Which value a consumer saw depended
purely on which file it happened to open.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from engine.core.codebook import VALID_FIELD_TYPES, load_codebook_for
from engine.core.review_paths import load_spec_for

REVIEW = "surgical_autonomy"


@pytest.fixture(scope="module")
def pair():
    return load_spec_for(REVIEW), load_codebook_for(REVIEW)


def _cb_values(entry):
    vv = entry.get("valid_values")
    return [v["value"] for v in vv] if vv else None


def test_same_field_names_in_the_same_order(pair):
    spec, cb = pair
    assert [f.name for f in spec.extraction_schema.fields] == list(cb.field_names)


def test_every_field_agrees_on_type_tier_and_values(pair):
    """One assertion per attribute the two files both carry."""
    spec, cb = pair
    divergences = []
    for f in spec.extraction_schema.fields:
        entry = cb.field(f.name)
        if f.type != entry["type"]:
            divergences.append(f"{f.name}: type spec={f.type!r} codebook={entry['type']!r}")
        if f.tier != entry["tier"]:
            divergences.append(f"{f.name}: tier spec={f.tier} codebook={entry['tier']}")
        if (f.enum_values or None) != _cb_values(entry):
            divergences.append(
                f"{f.name}: values spec={f.enum_values!r} "
                f"codebook={_cb_values(entry)!r}"
            )
    assert divergences == [], "\n".join(divergences)


def test_the_spec_uses_the_codebook_vocabulary(pair):
    spec, _ = pair
    for f in spec.extraction_schema.fields:
        assert f.type in VALID_FIELD_TYPES, f"{f.name}: {f.type!r}"


def test_the_spec_model_refuses_a_type_outside_the_vocabulary():
    """The old spelling is now a validation error, not a silent divergence."""
    import yaml

    from engine.core.review_spec import ReviewSpec

    raw = yaml.safe_load(
        (Path(__file__).resolve().parent.parent
         / "review_specs" / "surgical_autonomy.yaml").read_text())
    raw["extraction_schema"]["fields"][0]["type"] = "text"
    with pytest.raises(Exception) as exc:
        ReviewSpec.model_validate(raw)
    assert "text" in str(exc.value)


def test_the_vocabulary_has_exactly_one_definition():
    """analysis/ aliases the engine's tuple; it does not keep a copy."""
    from analysis.paper1 import judge_loader

    assert judge_loader._VALID_FIELD_TYPES is VALID_FIELD_TYPES


def test_sample_size_is_numeric_in_both(pair):
    """The one substantive disagreement, pinned by name.

    The prompt has always rendered `numeric` — the model was never shown the
    spec's `text` — so this is the codebook's value winning, not a change to
    what the extractor asks for.
    """
    spec, cb = pair
    assert cb.field("sample_size")["type"] == "numeric"
    assert next(f for f in spec.extraction_schema.fields
                if f.name == "sample_size").type == "numeric"
