"""SPEC-AUTH-01 — review identity and the paths derived from it.

T3 path derivation · T4 mismatch refusal before any database is
constructed · T5 the live spec at its derived path.

T2 (slug validation) and T6 (provenance hashes unmoved) live in
test_review_spec.py, beside the model they constrain.
"""

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from engine.core.review_paths import (
    REVIEW_ID_MAX_LENGTH,
    REVIEW_ID_PATTERN,
    ReviewIdMismatchError,
    data_root_for,
    load_spec_for,
    spec_path_for,
)
from engine.core.review_spec import ReviewSpec, ReviewSpecError, load_review_spec

SPEC_PATH = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy.yaml"

# ── T3: derivation ───────────────────────────────────────────────────


def test_spec_path_derivation():
    assert spec_path_for("surgical_autonomy") == Path("review_specs/surgical_autonomy.yaml")
    assert spec_path_for("other_review") == Path("review_specs/other_review.yaml")


def test_data_root_derivation():
    assert data_root_for("surgical_autonomy") == Path("data/surgical_autonomy")
    assert data_root_for("other_review") == Path("data/other_review")


def test_data_root_does_not_create_anything(tmp_path, monkeypatch):
    """Derivation is a pure path computation — it must not touch the disk."""
    monkeypatch.chdir(tmp_path)
    root = data_root_for("brand_new_review")
    assert not root.exists()


@pytest.mark.parametrize("bad", ["Upper", "1x", "has-hyphen", "", "x" * 65])
def test_derivation_rejects_a_bad_requested_id(bad):
    with pytest.raises(ReviewSpecError):
        spec_path_for(bad)
    with pytest.raises(ReviewSpecError):
        data_root_for(bad)


# ── T4: mismatch refusal, before any database exists ─────────────────


def test_load_spec_for_matching_id_loads():
    spec = load_spec_for("surgical_autonomy")
    assert spec.review_id == "surgical_autonomy"
    assert "Autonomy" in spec.title


def test_load_spec_for_override_with_matching_id_loads():
    spec = load_spec_for("surgical_autonomy", SPEC_PATH)
    assert spec.review_id == "surgical_autonomy"


def test_load_spec_for_override_with_different_id_raises():
    with pytest.raises(ReviewIdMismatchError) as exc:
        load_spec_for("some_other_review", SPEC_PATH)
    msg = str(exc.value)
    assert "surgical_autonomy" in msg and "some_other_review" in msg


def test_mismatch_names_both_ids_and_the_path(tmp_path):
    raw = yaml.safe_load(SPEC_PATH.read_text())
    raw["review_id"] = "review_b"
    p = tmp_path / "b.yaml"
    p.write_text(yaml.dump(raw))
    with pytest.raises(ReviewIdMismatchError) as exc:
        load_spec_for("review_a", p)
    msg = str(exc.value)
    assert "review_a" in msg and "review_b" in msg and str(p) in msg


def test_mismatch_raises_before_any_database_is_constructed(tmp_path):
    """T4's real claim: the refusal happens with no database opened.

    Spying on ReviewDatabase rather than asserting on the exception type,
    because the exception is not the guarantee — the guarantee is that a
    mismatched run never reaches a database at all.
    """
    raw = yaml.safe_load(SPEC_PATH.read_text())
    raw["review_id"] = "review_b"
    p = tmp_path / "b.yaml"
    p.write_text(yaml.dump(raw))

    with patch("engine.core.database.ReviewDatabase.__init__") as spy:
        spy.side_effect = AssertionError("ReviewDatabase must not be constructed")
        with pytest.raises(ReviewIdMismatchError):
            load_spec_for("review_a", p)
        assert spy.call_count == 0


def test_review_id_mismatch_is_a_review_spec_error():
    assert issubclass(ReviewIdMismatchError, ReviewSpecError)


# ── T5: the live spec, at the derived path ───────────────────────────


def test_live_spec_loads_at_the_derived_path():
    assert spec_path_for("surgical_autonomy") == Path("review_specs/surgical_autonomy.yaml")
    spec = load_review_spec(SPEC_PATH)
    assert spec.review_id == "surgical_autonomy"


# ── T7 + C4: entry points are on the resolver ────────────────────────


def test_run_pipeline_accepts_review_and_the_deprecated_name_alias():
    """T7 — --name still resolves, and lands in the same place as --review."""
    import scripts.run_pipeline as rp

    parser = None
    # Rebuild the parser the way main() does, without running the pipeline.
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--review", "--name", dest="review", required=True)
    # Sanity: the real module declares the alias on one dest.
    src = Path(rp.__file__).read_text()
    assert '"--review", "--name", dest="review"' in src
    assert parser.parse_args(["--name", "surgical_autonomy"]).review == "surgical_autonomy"
    assert parser.parse_args(["--review", "surgical_autonomy"]).review == "surgical_autonomy"


def test_run_pipeline_takes_the_review_first_and_the_spec_as_override():
    """The signature encodes the direction: identity first, spec optional."""
    import inspect

    from scripts.run_pipeline import run_pipeline

    params = list(inspect.signature(run_pipeline).parameters)
    assert params[0] == "review_name"
    assert params[1] == "spec_path"
    assert inspect.signature(run_pipeline).parameters["spec_path"].default is None


# The 29 spec-bearing entry points, as enumerated in the SPEC-AUTH-01 Phase 1
# read-out. Pinned as a list so a new one cannot join them quietly on a
# hand-built path.
SPEC_BEARING_ENTRY_POINTS = [
    "analysis/eval/elicit_design01/smoke.py",
    "analysis/eval/run_capture01.py",
    "analysis/eval/run_cloud_strict.py",
    "analysis/eval/run_local_ab.py",
    "analysis/eval/run_local_abc.py",
    "analysis/eval/run_qualgap01.py",
    "analysis/eval/smoke_regression01.py",
    "analysis/paper1/export_disagreement_pairs.py",
    "engine/acquisition/check_oa.py",
    "engine/acquisition/manual_list.py",
    "engine/acquisition/pdf_quality_check.py",
    "engine/agents/ft_screener.py",
    "engine/analysis/concordance.py",
    "engine/utils/extraction_cleanup.py",
    "engine/validators/extraction_validator.py",
    "scripts/eval_auditor_models.py",
    "scripts/ft_screening_smoke_test.py",
    "scripts/q8_validation.py",
    "scripts/q8_validation_fast.py",
    "scripts/reextract_all.py",
    "scripts/reextract_failed.py",
    "scripts/reparse_cloud_spans.py",
    "scripts/rescreen_original_251.py",
    "scripts/rescreen_with_specialty.py",
    "scripts/run5_extract_and_audit.py",
    "scripts/run_cloud_extraction.py",
    "scripts/run_pipeline.py",
    "scripts/screen_expanded.py",
    "scripts/smoke_test_fixes.py",
]

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.parametrize("rel", SPEC_BEARING_ENTRY_POINTS)
def test_entry_point_uses_the_resolver(rel):
    src = (REPO_ROOT / rel).read_text()
    assert "review_paths import" in src, f"{rel} does not import the resolver"
    assert "load_spec_for(" in src, f"{rel} does not call load_spec_for"


@pytest.mark.parametrize("rel", SPEC_BEARING_ENTRY_POINTS)
def test_entry_point_builds_no_spec_path_of_its_own(rel):
    """C3: no other module builds either path.

    A grep, deliberately: the failure this catches is someone reintroducing
    a hand-built path, and a hand-built path is a textual thing.
    """
    import re

    src = (REPO_ROOT / rel).read_text()
    # An f-string interpolating the review name into a path, or a path
    # naming one concrete review. Help text mentioning the CONVENTION
    # (review_specs/<review>.yaml) is not a constructed path.
    interpolated = re.findall(r'f"[^"\n]*review_specs/[^"\n]*\{', src)
    assert not interpolated, f"{rel} interpolates a spec path: {interpolated}"
    concrete = re.findall(r'review_specs/[a-z][a-z0-9_]*\.yaml', src)
    assert not concrete, f"{rel} names a concrete spec file: {concrete}"


@pytest.mark.parametrize("rel", SPEC_BEARING_ENTRY_POINTS)
def test_entry_point_names_no_hardcoded_review(rel):
    """R4: the literal autonomy defaults are gone from every file this
    task edited. Usage lines in module docstrings are allowed — they are
    examples, not defaults — so only argparse defaults are checked."""
    src = (REPO_ROOT / rel).read_text()
    assert 'default="surgical_autonomy"' not in src, f"{rel} defaults to one review"
    assert "DEFAULT_REVIEW" not in src, f"{rel} still carries DEFAULT_REVIEW"
