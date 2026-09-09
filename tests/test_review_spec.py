"""Tests for the Review Spec parser and protocol hashing."""

import copy
from pathlib import Path

import pytest
import yaml

from engine.core.review_spec import ReviewSpec, ReviewSpecError, load_review_spec

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


# ── M11: User-friendly errors ────────────────────────────────────────


def test_load_missing_file_raises_review_spec_error():
    """M11: Missing file produces ReviewSpecError with clear message."""
    with pytest.raises(ReviewSpecError, match="Review spec not found"):
        load_review_spec("/nonexistent/path/to/spec.yaml")


def test_load_malformed_yaml_raises_review_spec_error(tmp_path):
    """M11: Invalid YAML produces ReviewSpecError with parse details."""
    bad_yaml = tmp_path / "bad.yaml"
    bad_yaml.write_text("title: [\n  broken: yaml:\n")
    with pytest.raises(ReviewSpecError, match="invalid YAML"):
        load_review_spec(bad_yaml)


def test_review_spec_error_is_value_error():
    """M11: ReviewSpecError inherits from ValueError."""
    assert issubclass(ReviewSpecError, ValueError)


# ── SPEC-AUTH-01 T1: strictness — unknown keys are rejected ──────────
#
# The spec is the review's configuration authority. Under the previous
# `extra='ignore'` a misspelled key was dropped with no error and no
# warning, so a review could declare configuration that never took effect
# and read as if it had.


def _write(tmp_path, raw):
    p = tmp_path / "spec.yaml"
    p.write_text(yaml.dump(raw))
    return p


def test_every_spec_model_forbids_extra_keys():
    """Structural pin: no model in review_spec.py may be lenient.

    Iterating the module rather than listing names is deliberate — a model
    added later inherits the guarantee or turns this test red.
    """
    import inspect

    from pydantic import BaseModel

    from engine.core import review_spec as rs

    models = [
        obj for obj in vars(rs).values()
        if inspect.isclass(obj)
        and issubclass(obj, BaseModel)
        and obj.__module__ == rs.__name__
    ]
    assert len(models) >= 17, f"expected the full model set, found {len(models)}"
    lenient = [m.__name__ for m in models if m.model_config.get("extra") != "forbid"]
    assert lenient == [], f"models not forbidding extra keys: {lenient}"


def test_unknown_top_level_key_rejected_and_named(tmp_path):
    raw = yaml.safe_load(SPEC_PATH.read_text())
    raw["reviw_id"] = "typo"
    with pytest.raises(ReviewSpecError) as exc:
        load_review_spec(_write(tmp_path, raw))
    assert "reviw_id" in str(exc.value)
    assert "unknown key" in str(exc.value)


# One case per nested model. `path` is where the bogus key is planted;
# `expected` is the dotted path the error message must name.
NESTED_CASES = [
    (["pico"], "pico.bogus_key"),
    (["search_strategy"], "search_strategy.bogus_key"),
    (["screening_models"], "screening_models.bogus_key"),
    (["ft_screening_models"], "ft_screening_models.bogus_key"),
    (["screening_criteria"], "screening_criteria.bogus_key"),
    (["extraction_schema"], "extraction_schema.bogus_key"),
    (["extraction_schema", "fields", 0], "extraction_schema.fields.0.bogus_key"),
    (["specialty_scope"], "specialty_scope.bogus_key"),
    (["pdf_quality_check"], "pdf_quality_check.bogus_key"),
    (["extraction_models"], "extraction_models.bogus_key"),
    (["pdf_parsing"], "pdf_parsing.bogus_key"),
    (["pdf_parsing", "parse_quality"], "pdf_parsing.parse_quality.bogus_key"),
    (["cloud_models"], "cloud_models.bogus_key"),
    (["cloud_models", "openai"], "cloud_models.openai.bogus_key"),
    (["distribution_monitor"], "distribution_monitor.bogus_key"),
]


@pytest.mark.parametrize("path,expected", NESTED_CASES, ids=[c[1] for c in NESTED_CASES])
def test_unknown_nested_key_rejected_and_named(tmp_path, path, expected):
    raw = yaml.safe_load(SPEC_PATH.read_text())
    node = raw
    for step in path:
        if isinstance(node, dict) and step not in node:
            node[step] = {}
        node = node[step]
    node["bogus_key"] = "x"
    with pytest.raises(ReviewSpecError) as exc:
        load_review_spec(_write(tmp_path, raw))
    assert expected in str(exc.value), str(exc.value)
