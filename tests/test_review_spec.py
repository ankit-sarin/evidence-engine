"""Tests for the Review Spec parser and protocol hashing."""

import copy
from pathlib import Path

import pytest
import yaml

from engine.core.review_spec import ReviewSpec, load_review_spec

SPEC_PATH = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy_v1.yaml"


# ── Loading & Validation ─────────────────────────────────────────────


def test_load_surgical_autonomy_spec():
    spec = load_review_spec(SPEC_PATH)
    assert isinstance(spec, ReviewSpec)
    assert "Autonomy" in spec.title
    assert len(spec.pico.outcomes) >= 3
    assert len(spec.extraction_schema.fields) >= 15


def test_spec_has_tier1_fields():
    spec = load_review_spec(SPEC_PATH)
    tier1 = spec.extraction_schema.fields_by_tier(1)
    assert len(tier1) >= 5


def test_spec_search_strategy():
    spec = load_review_spec(SPEC_PATH)
    assert "PubMed" in spec.search_strategy.databases
    assert spec.search_strategy.date_range[0] <= spec.search_strategy.date_range[1]


# ── Protocol Hashing ─────────────────────────────────────────────────


def test_hash_deterministic():
    spec1 = load_review_spec(SPEC_PATH)
    spec2 = load_review_spec(SPEC_PATH)
    assert spec1.screening_hash() == spec2.screening_hash()
    assert spec1.extraction_hash() == spec2.extraction_hash()


def test_screening_hash_changes_on_modification():
    spec = load_review_spec(SPEC_PATH)
    original_hash = spec.screening_hash()

    modified = spec.model_copy(deep=True)
    modified.screening_criteria.inclusion.append("Must involve humans")
    assert modified.screening_hash() != original_hash


def test_extraction_hash_changes_on_modification():
    spec = load_review_spec(SPEC_PATH)
    original_hash = spec.extraction_hash()

    modified = spec.model_copy(deep=True)
    modified.extraction_schema.fields.pop()
    assert modified.extraction_hash() != original_hash


def test_screening_change_does_not_affect_extraction_hash():
    spec = load_review_spec(SPEC_PATH)
    original_extraction_hash = spec.extraction_hash()

    modified = spec.model_copy(deep=True)
    modified.screening_criteria.exclusion.append("Exclude all RCTs")
    assert modified.extraction_hash() == original_extraction_hash


# ── Validation Errors ────────────────────────────────────────────────


def test_malformed_yaml_missing_title():
    raw = yaml.safe_load(SPEC_PATH.read_text())
    del raw["title"]
    with pytest.raises(Exception):
        ReviewSpec.model_validate(raw)


def test_malformed_yaml_invalid_date_range():
    raw = yaml.safe_load(SPEC_PATH.read_text())
    raw["search_strategy"]["date_range"] = [2025, 2010]
    with pytest.raises(Exception):
        ReviewSpec.model_validate(raw)


def test_malformed_yaml_no_tier1_fields():
    raw = yaml.safe_load(SPEC_PATH.read_text())
    for field in raw["extraction_schema"]["fields"]:
        field["tier"] = 2
    with pytest.raises(Exception):
        ReviewSpec.model_validate(raw)


def test_malformed_yaml_invalid_tier():
    raw = yaml.safe_load(SPEC_PATH.read_text())
    raw["extraction_schema"]["fields"][0]["tier"] = 5
    with pytest.raises(Exception):
        ReviewSpec.model_validate(raw)
