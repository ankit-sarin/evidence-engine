"""The ported unit machinery (ELICIT-DESIGN-01).

ELICIT-DESIGN-01 test 5, the elicitation pipeline's private size estimator, was
removed with `engine/elicitation/sizing.py` in INPUT-FIT-01 Phase 2. The input-fit
guard in `engine/utils/ollama_client.py` is the one authority now; its tests,
including the migrated ones, are in `tests/test_ollama_input_fit.py`.
"""

from __future__ import annotations

import pytest

from engine.elicitation.units import MIN_UNIT_TOKENS, build_unit_map, merge_short


def test_units_exclude_docling_comment_artifacts():
    um = build_unit_map(1, "Alpha beta gamma delta. <!-- image --> Epsilon zeta eta theta.")
    assert um.n == 2
    assert all("<!--" not in u for u in um.units)


def test_short_units_are_merged_never_discarded():
    # P1 unit U01: units are (start, end) spans of the text, and a merge joins
    # two spans — start of the first, end of the last — instead of two strings.
    text = "H.  One two three four five.\n2 (c)."
    pieces = ["H.", "One two three four five.", "2 (c)."]
    spans = [(text.index(p), text.index(p) + len(p)) for p in pieces]
    merged = merge_short(spans, text, MIN_UNIT_TOKENS)
    assert merged == [(0, len(text))]
    assert all(len(text[s:e].split()) >= MIN_UNIT_TOKENS for s, e in merged)


def test_bijection_nothing_is_dropped():
    text = "Alpha beta gamma. Delta epsilon zeta. H. Eta theta iota kappa."
    um = build_unit_map(1, text)
    assert " ".join(um.units) == text
    assert [text[s:e] for s, e in um.spans] == list(um.units)


def test_min_unit_tokens_is_the_frozen_elicit01_value():
    """A retuned threshold would make the index space incomparable with the
    measurement that justified it. Re-derivation belongs to the parse-quality gate."""
    assert MIN_UNIT_TOKENS == 3
