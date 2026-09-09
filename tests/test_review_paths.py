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
