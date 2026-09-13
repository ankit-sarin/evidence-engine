"""The ported unit machinery (ELICIT-DESIGN-01).

ELICIT-DESIGN-01 test 5, the elicitation pipeline's private size estimator, was
removed with `engine/elicitation/sizing.py` in INPUT-FIT-01 Phase 2. The input-fit
guard in `engine/utils/ollama_client.py` is the one authority now; its tests,
including the migrated ones, are in `tests/test_ollama_input_fit.py`.
"""

from __future__ import annotations

import pytest

from engine.elicitation.units import (
    MIN_UNIT_TOKENS, build_unit_map, merge_short, strip_comments,
)


def test_units_exclude_docling_comment_artifacts():
    um = build_unit_map(1, "Alpha beta gamma delta. <!-- image --> Epsilon zeta eta theta.")
    assert um.n == 2
    assert all("<!--" not in u for u in um.units)


def test_short_units_are_merged_never_discarded():
    merged = merge_short(["H.", "One two three four five.", "2 (c)."], MIN_UNIT_TOKENS)
    assert "".join(merged).replace(" ", "") == "H.Onetwothreefourfive.2(c)."
    assert all(len(u.split()) >= MIN_UNIT_TOKENS for u in merged)


def test_bijection_nothing_is_dropped():
    text = "Alpha beta gamma. Delta epsilon zeta. H. Eta theta iota kappa."
    um = build_unit_map(1, text)
    assert " ".join(um.units) == strip_comments(text).strip()


def test_min_unit_tokens_is_the_frozen_elicit01_value():
    """A retuned threshold would make the index space incomparable with the
    measurement that justified it. Re-derivation belongs to the parse-quality gate."""
    assert MIN_UNIT_TOKENS == 3
