"""Tests for analysis/paper1/judge_schema.py."""

import pytest
from pydantic import TypeAdapter, ValidationError

from analysis.paper1.judge_schema import (
    DisagreementPair,
    EquivalentPair,
    PairwiseRating,
    Pass1Output,
    pair_disagreement_type,
)


def _equivalent(a=1, b=2):
    return EquivalentPair(slot_a=a, slot_b=b, rating="EQUIVALENT", rationale="same")


def _partial(a=1, b=2, dtype="GRANULARITY"):
    return DisagreementPair(
        slot_a=a, slot_b=b, rating="PARTIAL",
        disagreement_type=dtype, rationale="partial overlap",
    )


# ---------------------------------------------------------------------------
# EquivalentPair
# ---------------------------------------------------------------------------


class TestEquivalentPair:
    def test_valid_minimal(self):
        p = _equivalent()
        assert p.rating == "EQUIVALENT"
        assert not hasattr(p, "disagreement_type")

    def test_rejects_slot_order(self):
        with pytest.raises(ValidationError):
            EquivalentPair(slot_a=2, slot_b=2, rating="EQUIVALENT",
                           rationale="x")

    def test_rejects_rating_partial(self):
        with pytest.raises(ValidationError):
            EquivalentPair(slot_a=1, slot_b=2, rating="PARTIAL",
                           rationale="x")

    def test_extra_disagreement_type_ignored_or_rejected(self):
        # Pydantic default: extra fields are ignored; regardless, the model
        # must not materialize a disagreement_type attribute.
        p = EquivalentPair(slot_a=1, slot_b=2, rating="EQUIVALENT",
                           rationale="x", disagreement_type="GRANULARITY")
        assert not hasattr(p, "disagreement_type")

    def test_empty_rationale_rejected(self):
        with pytest.raises(ValidationError):
            EquivalentPair(slot_a=1, slot_b=2, rating="EQUIVALENT",
                           rationale="")

    def test_long_rationale_rejected(self):
        with pytest.raises(ValidationError):
            EquivalentPair(slot_a=1, slot_b=2, rating="EQUIVALENT",
                           rationale="x" * 501)


# ---------------------------------------------------------------------------
# DisagreementPair
# ---------------------------------------------------------------------------


class TestDisagreementPair:
    def test_valid_partial(self):
        p = _partial()
        assert p.rating == "PARTIAL"
        assert p.disagreement_type == "GRANULARITY"

    def test_valid_divergent(self):
        p = DisagreementPair(
            slot_a=1, slot_b=3, rating="DIVERGENT",
            disagreement_type="CONTRADICTION", rationale="x",
        )
        assert p.rating == "DIVERGENT"

    def test_rejects_equivalent_rating(self):
        with pytest.raises(ValidationError):
            DisagreementPair(slot_a=1, slot_b=2, rating="EQUIVALENT",
                             disagreement_type="GRANULARITY", rationale="x")

    def test_missing_disagreement_type_rejected(self):
        with pytest.raises(ValidationError):
            DisagreementPair(slot_a=1, slot_b=2, rating="PARTIAL",
                             rationale="x")

    def test_invalid_disagreement_type_rejected(self):
        with pytest.raises(ValidationError):
            DisagreementPair(slot_a=1, slot_b=2, rating="PARTIAL",
                             disagreement_type="UNKNOWN", rationale="x")

    def test_slot_a_zero_rejected(self):
        with pytest.raises(ValidationError):
            DisagreementPair(slot_a=0, slot_b=1, rating="PARTIAL",
                             disagreement_type="OMISSION", rationale="x")

    def test_slot_a_greater_than_b_rejected(self):
        with pytest.raises(ValidationError):
            DisagreementPair(slot_a=3, slot_b=2, rating="DIVERGENT",
                             disagreement_type="CONTRADICTION", rationale="x")


# ---------------------------------------------------------------------------
# PairwiseRating discriminated union (via TypeAdapter + Pass1Output)
# ---------------------------------------------------------------------------


class TestPairwiseRatingUnion:
    def test_adapter_routes_to_equivalent(self):
        ta = TypeAdapter(PairwiseRating)
        p = ta.validate_python(
            {"slot_a": 1, "slot_b": 2, "rating": "EQUIVALENT", "rationale": "x"}
        )
        assert isinstance(p, EquivalentPair)

    def test_adapter_routes_to_disagreement(self):
        ta = TypeAdapter(PairwiseRating)
        p = ta.validate_python(
            {"slot_a": 1, "slot_b": 2, "rating": "PARTIAL",
             "disagreement_type": "OMISSION", "rationale": "x"}
        )
        assert isinstance(p, DisagreementPair)

    def test_partial_without_disagreement_type_raises_at_parse(self):
        ta = TypeAdapter(PairwiseRating)
        with pytest.raises(ValidationError):
            ta.validate_python(
                {"slot_a": 1, "slot_b": 2, "rating": "PARTIAL", "rationale": "x"}
            )

    def test_pair_disagreement_type_accessor(self):
        assert pair_disagreement_type(_equivalent()) is None
        assert pair_disagreement_type(_partial()) == "GRANULARITY"


# ---------------------------------------------------------------------------
# Pass1Output
# ---------------------------------------------------------------------------


class TestPass1Output:
    def _valid(self, **overrides):
        defaults = dict(
            pairwise_ratings=[_equivalent()],
            fabrication_risk="low",
            proposed_consensus="some value",
            overall_rationale="fine",
        )
        defaults.update(overrides)
        return Pass1Output(**defaults)

    def test_valid_minimal(self):
        out = self._valid()
        assert out.fabrication_risk == "low"

    def test_empty_pairwise_ratings_rejected(self):
        with pytest.raises(ValidationError):
            self._valid(pairwise_ratings=[])

    def test_invalid_fabrication_risk_rejected(self):
        with pytest.raises(ValidationError):
            self._valid(fabrication_risk="severe")

    def test_null_proposed_consensus_allowed(self):
        out = self._valid(proposed_consensus=None)
        assert out.proposed_consensus is None

    def test_empty_overall_rationale_rejected(self):
        with pytest.raises(ValidationError):
            self._valid(overall_rationale="")

    def test_long_overall_rationale_rejected(self):
        with pytest.raises(ValidationError):
            self._valid(overall_rationale="x" * 1001)

    def test_round_trip_json_both_branches(self):
        out = self._valid(
            pairwise_ratings=[
                _equivalent(),
                _partial(a=1, b=3, dtype="SELECTION"),
            ],
            fabrication_risk="medium",
        )
        blob = out.model_dump_json()
        restored = Pass1Output.model_validate_json(blob)
        assert len(restored.pairwise_ratings) == 2
        assert isinstance(restored.pairwise_ratings[0], EquivalentPair)
        assert isinstance(restored.pairwise_ratings[1], DisagreementPair)
        assert restored == out

    def test_parse_partial_without_type_fails_fast(self):
        bad = (
            '{"pairwise_ratings": [{"slot_a": 1, "slot_b": 2, '
            '"rating": "PARTIAL", "rationale": "x"}], '
            '"fabrication_risk": "low", '
            '"overall_rationale": "x"}'
        )
        with pytest.raises(ValidationError):
            Pass1Output.model_validate_json(bad)

    def test_schema_has_discriminator(self):
        schema = Pass1Output.model_json_schema()
        items = schema["properties"]["pairwise_ratings"]["items"]
        # Pydantic emits discriminator metadata under `discriminator` with
        # `propertyName` = "rating" and a `mapping` table of tag → $ref.
        assert "discriminator" in items, items
        assert items["discriminator"]["propertyName"] == "rating"
        assert set(items["discriminator"]["mapping"].keys()) == {
            "EQUIVALENT", "PARTIAL", "DIVERGENT",
        }
        # The oneOf (or anyOf) union points at both branches.
        oneof = items.get("oneOf") or items.get("anyOf")
        assert oneof is not None
        refs = {entry.get("$ref") for entry in oneof}
        assert any("EquivalentPair" in r for r in refs if r)
        assert any("DisagreementPair" in r for r in refs if r)
