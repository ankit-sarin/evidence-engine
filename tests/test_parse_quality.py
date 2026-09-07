"""PARSE-GATE-01 — parse-quality metrics and the absolute PASS/FAIL verdict.

The load-bearing case is p455: a document of exactly the right length whose text
is shattered into single characters. Its fixture and p561's differ by 33
characters out of ~60,000 in the source files, so every test that separates them
is separating them on structure alone.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from engine.parsers.parse_quality import (
    ADDED_METRIC_NAMES,
    CHARS_PER_UNIT,
    EMPTY_TEXT,
    GLYPH_DENSITY,
    METRIC_NAMES,
    PHASE1_METRIC_NAMES,
    REPLACEMENT_CHARS,
    SHORT_UNIT_SHARE,
    Thresholds,
    Verdict,
    assess,
    compute_metrics,
)

REPO = Path(__file__).resolve().parents[1]
FIXTURES = REPO / "tests" / "fixtures" / "parse_quality"
SHATTERED = FIXTURES / "p455_shattered.md"
CLEAN = FIXTURES / "p561_clean.md"

# Data-dependent tests follow the repo's existing convention: skipif on path
# existence (as in test_completeness_guard.py, test_codebook_field_class.py and
# seven others), not a registered marker.
SWEEP = REPO / "data" / "surgical_autonomy" / "eval" / "parse01" / "sweep.jsonl"
PARSED = REPO / "data" / "surgical_autonomy" / "parsed_text"

# Prose with no defect: healthy unit length, no artifacts.
GOOD_PROSE = (
    "The robotic assistant was evaluated in a porcine model over twelve "
    "procedures. Task completion time fell by nineteen percent against the "
    "manual baseline. No adverse events were recorded during the study "
    "period. The authors conclude that further trials in humans are "
    "warranted before clinical adoption. "
) * 6


# ── T1 — the two real fixtures ────────────────────────────────────────

def test_shattered_fixture_fails_on_both_segmentation_criteria():
    v = assess(SHATTERED.read_text())
    assert v.passed is False
    assert set(v.criteria) == {SHORT_UNIT_SHARE, CHARS_PER_UNIT}
    assert v.metrics["short_unit_share_pct"] > 50.0
    assert v.metrics["chars_per_unit"] < 20.0


def test_clean_fixture_passes_with_zero_failures():
    v = assess(CLEAN.read_text())
    assert v.passed is True
    assert v.failures == ()


def test_the_two_fixtures_are_the_same_size_and_opposite_structure():
    """The point of the pair: length cannot tell them apart, structure can."""
    a, b = compute_metrics(SHATTERED.read_text()), compute_metrics(CLEAN.read_text())
    assert abs(a["chars"] - b["chars"]) < 50          # indistinguishable by length
    assert a["short_unit_share_pct"] > 90.0           # ... and worlds apart by structure
    assert b["short_unit_share_pct"] < 10.0


# ── T2 — glyph density, both encodings ────────────────────────────────

def _with_glyphs(n_per_kchar: float) -> str:
    """Prose carrying ~n glyph artifacts per 1,000 characters of the result."""
    body = GOOD_PROSE
    token = "GLYPH<c=3,font=/JGFKKL+TimesNewRoman>"
    # Solve for count against the FINAL length, which the tokens themselves grow.
    n = round(n_per_kchar * (len(body)) / (1000 - n_per_kchar * len(token)))
    return body + " " + " ".join([token] * n)


def test_glyph_density_above_the_limit_fails():
    v = assess(_with_glyphs(6.0))
    assert v.metrics["glyph_density_per_kchar"] > 5.0
    assert GLYPH_DENSITY in v.criteria
    assert v.passed is False


def test_glyph_density_below_the_limit_passes():
    v = assess(_with_glyphs(4.0))
    assert v.metrics["glyph_density_per_kchar"] < 5.0
    assert GLYPH_DENSITY not in v.criteria


def test_both_glyph_encodings_are_counted():
    raw = "GLYPH<c=3,font=/X> and GLYPH&lt;c=4,font=/Y&gt; together."
    assert compute_metrics(raw)["glyph_artifacts"] == 2


def test_glyph_density_is_per_kchar_not_a_raw_count():
    """542 artifacts in 69K chars is worse than 419 in 48K only once normalised."""
    dense = compute_metrics("GLYPH<x>" * 10 + "a" * 100)
    sparse = compute_metrics("GLYPH<x>" * 10 + "a" * 100_000)
    assert dense["glyph_artifacts"] == sparse["glyph_artifacts"] == 10
    assert dense["glyph_density_per_kchar"] > sparse["glyph_density_per_kchar"]


# ── T3 — replacement characters ───────────────────────────────────────

def test_a_single_replacement_character_fails():
    v = assess(GOOD_PROSE + "�")
    assert v.passed is False
    assert REPLACEMENT_CHARS in v.criteria
    assert v.metrics["replacement_chars"] == 1


def test_clean_prose_has_no_replacement_characters():
    assert assess(GOOD_PROSE).passed is True


# ── T4 — threshold overrides ──────────────────────────────────────────

def test_override_from_mapping_changes_the_verdict():
    text = SHATTERED.read_text()
    assert assess(text).passed is False
    lax = Thresholds.from_mapping(
        {"short_unit_share_pct_max": 99.9, "chars_per_unit_min": 1.0})
    assert assess(text, lax).passed is True


def test_missing_keys_fall_back_to_defaults():
    th = Thresholds.from_mapping({"chars_per_unit_min": 3.0})
    assert th.chars_per_unit_min == 3.0
    assert th.short_unit_share_pct_max == Thresholds().short_unit_share_pct_max
    assert th.glyph_density_per_kchar_max == Thresholds().glyph_density_per_kchar_max
    assert th.replacement_chars_max == Thresholds().replacement_chars_max


def test_none_and_empty_mapping_are_the_defaults():
    assert Thresholds.from_mapping(None) == Thresholds()
    assert Thresholds.from_mapping({}) == Thresholds()


def test_unknown_keys_are_ignored_not_rejected():
    """A spec written for a later engine must not break an earlier one."""
    th = Thresholds.from_mapping({"chars_per_unit_min": 9.0, "future_key": 1})
    assert th.chars_per_unit_min == 9.0


def test_override_accepts_an_attribute_object_like_a_spec_model():
    class FakeSpecSection:
        short_unit_share_pct_max = 12.5

    th = Thresholds.from_mapping(FakeSpecSection())
    assert th.short_unit_share_pct_max == 12.5
    assert th.chars_per_unit_min == Thresholds().chars_per_unit_min


def test_a_stricter_threshold_can_fail_a_clean_document():
    strict = Thresholds(chars_per_unit_min=10_000.0)
    assert assess(CLEAN.read_text(), strict).passed is False


# ── T5 — the metric name set ──────────────────────────────────────────

def test_metric_names_are_phase1_plus_exactly_two_additions():
    got = set(compute_metrics(GOOD_PROSE))
    assert got == METRIC_NAMES
    assert got == PHASE1_METRIC_NAMES | ADDED_METRIC_NAMES
    assert ADDED_METRIC_NAMES == {"glyph_density_per_kchar", "is_empty"}
    assert len(PHASE1_METRIC_NAMES) == 17


def test_phase1_names_match_the_sweep_definition_verbatim():
    """Pins the name set against the Phase 1 source, not against a memory of it.

    sweep.py is read as TEXT, never imported: `analysis/` is the study lane and
    must not become an engine dependency (that is why the definitions were
    reproduced rather than shared).
    """
    src = (REPO / "analysis" / "eval" / "parse01" / "sweep.py").read_text()
    body = src.split("def measure_paper")[1].split("def local_ratio_and_scaffold")[0]
    keys = set(__import__("re").findall(r'^\s+"(\w+)":', body, __import__("re").M))
    # paper_id and parsed_file identify a file; compute_metrics takes only text.
    assert keys - {"paper_id", "parsed_file"} == PHASE1_METRIC_NAMES


# ── T6 — determinism ──────────────────────────────────────────────────

def test_assess_is_deterministic_on_repeated_calls():
    text = SHATTERED.read_text()
    a, b = assess(text), assess(text)
    assert a.metrics == b.metrics
    assert a.failures == b.failures
    assert a.passed == b.passed


def test_compute_metrics_does_not_mutate_its_input():
    text = GOOD_PROSE
    before = text
    compute_metrics(text)
    assert text == before


# ── T7 — regression pin against the committed Phase 1 sweep ───────────

PINNED_PIDS = (415, 719, 586, 455, 491, 562, 607, 644, 699)


@pytest.mark.skipif(not SWEEP.exists(), reason="parse01 sweep not present")
@pytest.mark.parametrize("pid", PINNED_PIDS)
def test_metrics_reproduce_the_phase1_sweep_row(pid):
    """Every Phase 1 metric, exactly: integers exact, floats to 2 dp.

    A mismatch here is a definition drift between this module and the instrument
    that produced the PARSE-01 classification -- not a tolerance to widen.
    """
    rows = {
        json.loads(line)["paper_id"]: json.loads(line)
        for line in SWEEP.read_text().splitlines() if line.strip()
    }
    expected = rows[pid]
    src = PARSED / expected["parsed_file"]
    got = compute_metrics(src.read_text(errors="replace"))

    for name in sorted(PHASE1_METRIC_NAMES):
        exp, act = expected[name], got[name]
        if isinstance(exp, bool) or isinstance(exp, int):
            assert act == exp, f"p{pid} {name}: {act!r} != {exp!r}"
        else:
            assert round(float(act), 2) == round(float(exp), 2), \
                f"p{pid} {name}: {act!r} != {exp!r}"


@pytest.mark.skipif(not SWEEP.exists(), reason="parse01 sweep not present")
def test_the_pinned_papers_are_the_nine_parse01_flagged():
    assert set(PINNED_PIDS) == {415, 719, 586, 455, 491, 562, 607, 644, 699}


# ── T8 — empty text ───────────────────────────────────────────────────

def test_empty_string_fails_with_its_own_criterion():
    v = assess("")
    assert v.passed is False
    assert v.criteria == (EMPTY_TEXT,)


def test_whitespace_only_fails_as_empty_not_as_segmentation():
    """Otherwise a blank document reads as a chars-per-unit defect."""
    v = assess("   \n\n \t ")
    assert v.criteria == (EMPTY_TEXT,)
    assert CHARS_PER_UNIT not in v.criteria


def test_empty_text_returns_a_verdict_not_an_exception():
    assert isinstance(assess(""), Verdict)
    assert compute_metrics("")["is_empty"] is True
