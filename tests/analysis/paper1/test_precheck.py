"""Tests for analysis/paper1/precheck.py."""

import dataclasses

import pytest

from analysis.paper1.precheck import (
    FieldType,
    PreCheckFlags,
    STOP_WORDS,
    compute_precheck_flags,
)


# ---------------------------------------------------------------------------
# FLAG 1: span_present
# ---------------------------------------------------------------------------


class TestSpanPresent:
    def _flags(self, span):
        return compute_precheck_flags(
            value="v", span=span, source_text="source", field_type="categorical"
        )

    def test_none_span_is_false(self):
        assert self._flags(None).span_present is False

    def test_empty_string_is_false(self):
        assert self._flags("").span_present is False

    def test_whitespace_only_is_false(self):
        assert self._flags("   \t\n  ").span_present is False

    def test_unicode_whitespace_only_is_false(self):
        # NBSP (\u00a0) and em-space (\u2003) — str.strip() handles both.
        assert self._flags("\u00a0\u2003\u00a0").span_present is False

    def test_short_span_is_false(self):
        assert self._flags("short").span_present is False

    def test_exactly_ten_chars_is_true(self):
        assert self._flags("exactly10!").span_present is True

    def test_long_span_is_true(self):
        assert self._flags("a" * 100).span_present is True


# ---------------------------------------------------------------------------
# FLAG 2: span_in_source — exact substring
# ---------------------------------------------------------------------------


class TestSpanInSourceExact:
    def _flags(self, span, source):
        return compute_precheck_flags(
            value="x", span=span, source_text=source, field_type="categorical"
        )

    def test_verbatim_match(self):
        source = "The patient underwent autonomous suturing of porcine intestine."
        span = "autonomous suturing of porcine intestine"
        f = self._flags(span, source)
        assert f.span_in_source is True
        assert f.span_match_method == "exact_substring"

    def test_whitespace_normalization(self):
        source = "foo bar baz qux"
        span = "foo  bar  baz"  # extra whitespace collapses
        f = self._flags(span, source)
        assert f.span_in_source is True
        assert f.span_match_method == "exact_substring"

    def test_case_insensitive(self):
        source = "The Autonomous Robot Performed Well"
        span = "autonomous robot performed"
        f = self._flags(span, source)
        assert f.span_in_source is True
        assert f.span_match_method == "exact_substring"

    def test_not_in_source_at_all(self):
        source = "completely unrelated content here"
        span = "alpha beta gamma delta"
        f = self._flags(span, source)
        assert f.span_in_source is False
        assert f.span_match_method == "none"

    def test_newlines_and_tabs_normalized(self):
        source = "line one\nline two\tline three"
        span = "line one line two line three"
        f = self._flags(span, source)
        assert f.span_in_source is True
        assert f.span_match_method == "exact_substring"


# ---------------------------------------------------------------------------
# FLAG 2: span_in_source — jaccard fallback
# ---------------------------------------------------------------------------


class TestSpanInSourceJaccard:
    def _flags(self, span, source):
        return compute_precheck_flags(
            value="x", span=span, source_text=source, field_type="categorical"
        )

    def test_reordered_tokens_trigger_jaccard(self):
        # span is not a substring, but some window has identical token set.
        span = "quick brown fox jumps"
        source = "the brown quick jumps fox wisely"
        f = self._flags(span, source)
        assert f.span_in_source is True
        assert f.span_match_method == "jaccard_fallback"

    def test_fifty_percent_tokens_changed_fails(self):
        span = "alpha beta gamma delta"
        source = "alpha foo gamma bar baz qux corge"
        f = self._flags(span, source)
        assert f.span_in_source is False
        assert f.span_match_method == "none"

    def test_span_longer_than_source_fails(self):
        span = "one two three four five six seven eight nine ten"
        source = "short source"
        f = self._flags(span, source)
        assert f.span_in_source is False
        assert f.span_match_method == "none"

    def test_span_with_no_alphabetic_tokens_fails(self):
        span = "123 456 789 000"
        source = "123 456 789 000 present here"
        f = self._flags(span, source)
        # Exact substring would match these digit tokens; the no-alphabetic
        # guard applies only to the jaccard fallback, not to exact hits.
        # So this first asserts the exact-substring happy path, then we
        # verify the jaccard-only path in the next test.
        assert f.span_match_method == "exact_substring"

    def test_span_no_alphabetic_blocks_jaccard_only(self):
        # Reordered purely numeric span — exact-substring fails, jaccard
        # should be blocked by the no-alphabetic guard.
        span = "123 456 789"
        source = "789 456 123 extra tokens here please"
        f = self._flags(span, source)
        assert f.span_in_source is False
        assert f.span_match_method == "none"

    def test_empty_source(self):
        f = self._flags("autonomous suturing", "")
        assert f.span_in_source is False
        assert f.span_match_method == "none"


# ---------------------------------------------------------------------------
# FLAG 3: value_in_span — categorical
# ---------------------------------------------------------------------------


class TestValueInSpanCategorical:
    def _flags(self, value, span):
        return compute_precheck_flags(
            value=value,
            span=span,
            source_text=span,  # span-in-source grounded for convenience
            field_type="categorical",
        )

    def test_match_verbatim(self):
        f = self._flags(
            value="Case Report/Series",
            span="...this is a case report/series of three patients...",
        )
        assert f.value_in_span is True
        assert f.value_match_method == "categorical_exact"

    def test_mismatch(self):
        f = self._flags(
            value="Review", span="original research study with 45 patients"
        )
        assert f.value_in_span is False
        assert f.value_match_method == "categorical_exact"

    def test_case_and_whitespace_normalization(self):
        f = self._flags(
            value="Case  REPORT", span="A case report was published."
        )
        assert f.value_in_span is True
        assert f.value_match_method == "categorical_exact"


# ---------------------------------------------------------------------------
# FLAG 3: value_in_span — numeric
# ---------------------------------------------------------------------------


class TestValueInSpanNumeric:
    def _flags(self, value, span, tol=0.0):
        return compute_precheck_flags(
            value=value,
            span=span,
            source_text=span,
            field_type="numeric",
            numeric_tolerance=tol,
        )

    def test_exact_integer_match(self):
        f = self._flags("45", "n = 45 patients", tol=0.0)
        assert f.value_in_span is True
        assert f.value_match_method == "numeric_tolerance"

    def test_close_miss_zero_tolerance(self):
        f = self._flags("45", "n = 47 patients", tol=0.0)
        assert f.value_in_span is False

    def test_close_miss_within_tolerance(self):
        f = self._flags("45", "n = 47 patients", tol=2.0)
        assert f.value_in_span is True

    def test_unparseable_value(self):
        f = self._flags("not a number", "span with 45 somewhere")
        assert f.value_in_span is False
        assert f.value_match_method == "none"

    def test_no_numbers_in_span(self):
        f = self._flags("45", "no numbers here at all")
        assert f.value_in_span is False
        assert f.value_match_method == "numeric_tolerance"

    def test_comma_stripped_value(self):
        f = self._flags("1,234", "a total of 1234 cases were reviewed")
        assert f.value_in_span is True

    def test_comma_stripped_span(self):
        f = self._flags("1234", "a total of 1,234 cases were reviewed")
        assert f.value_in_span is True

    def test_float_value_with_tolerance(self):
        f = self._flags("3.14", "pi is 3.141 in this context", tol=0.01)
        assert f.value_in_span is True

    def test_integer_semantics_at_zero_tolerance(self):
        # value "45" with span containing "45.0" — both integer-equivalent.
        f = self._flags("45", "the count was 45.0 overall", tol=0.0)
        assert f.value_in_span is True

    def test_integer_semantics_rejects_non_integer_candidate(self):
        # value "45" with tolerance 0 and span containing 45.7 → False.
        f = self._flags("45", "the count was 45.7 overall", tol=0.0)
        assert f.value_in_span is False


# ---------------------------------------------------------------------------
# FLAG 3: value_in_span — free_text
# ---------------------------------------------------------------------------


class TestValueInSpanFreeText:
    def _flags(self, value, span):
        return compute_precheck_flags(
            value=value, span=span, source_text=span, field_type="free_text"
        )

    def test_high_overlap_matches(self):
        f = self._flags(
            value="autonomous suturing of porcine intestine",
            span=(
                "the robot performed autonomous suturing on porcine "
                "intestinal tissue during the procedure"
            ),
        )
        assert f.value_in_span is True
        assert f.value_match_method == "freetext_jaccard"

    def test_no_overlap_fails(self):
        f = self._flags(
            value="suturing",
            span="needle driving and knot tying",
        )
        assert f.value_in_span is False

    def test_stopword_heavy_value_is_not_collapsed(self):
        # "of the and to" is all stop words — but a real surviving token
        # keeps the method in the jaccard path (not collapsed to "none").
        # Value tokens: {"suturing", "porcine"}; span tokens include both →
        # jaccard = 2/4 = 0.5 >= 0.3 → True.
        f = self._flags(
            value="of the suturing of porcine",
            span="autonomous suturing on porcine intestinal tissue",
        )
        assert f.value_in_span is True
        assert f.value_match_method == "freetext_jaccard"

    def test_all_stopwords_value_yields_none_method(self):
        # After filtering, no tokens survive → value_match_method == "none".
        f = self._flags(value="of the and to", span="irrelevant text")
        assert f.value_in_span is False
        assert f.value_match_method == "none"

    def test_short_tokens_dropped(self):
        # Single-char tokens are dropped; if that empties the set, method="none".
        f = self._flags(value="a b c", span="a b c")
        assert f.value_in_span is False
        assert f.value_match_method == "none"


# ---------------------------------------------------------------------------
# Integration / dataclass invariants
# ---------------------------------------------------------------------------


class TestIntegration:
    def test_all_flags_true_clean_case(self):
        span = "the sample size was 45 patients in total"
        source = "Background. " + span + " Methods."
        f = compute_precheck_flags(
            value="45",
            span=span,
            source_text=source,
            field_type="numeric",
            numeric_tolerance=0.0,
        )
        assert f.span_present is True
        assert f.span_in_source is True
        assert f.value_in_span is True
        assert f.span_match_method == "exact_substring"
        assert f.value_match_method == "numeric_tolerance"
        assert f.span_length == len(span)

    def test_all_flags_false_null_value(self):
        f = compute_precheck_flags(
            value=None,
            span="some span text here with enough length",
            source_text="source",
            field_type="categorical",
        )
        assert f.span_present is False
        assert f.span_in_source is False
        assert f.value_in_span is False
        assert f.span_match_method == "none"
        assert f.value_match_method == "none"
        assert f.span_length == 0

    def test_all_flags_false_null_span(self):
        f = compute_precheck_flags(
            value="45",
            span=None,
            source_text="source with 45 here",
            field_type="numeric",
        )
        assert f.span_present is False
        assert f.span_in_source is False
        assert f.value_in_span is False
        assert f.span_match_method == "none"
        assert f.value_match_method == "none"

    def test_empty_value_all_false(self):
        f = compute_precheck_flags(
            value="", span="long-enough span text here", source_text="...",
            field_type="categorical",
        )
        assert f.span_present is False
        assert f.span_in_source is False
        assert f.value_in_span is False
        assert f.span_match_method == "none"
        assert f.value_match_method == "none"

    def test_precheckflags_is_frozen(self):
        f = compute_precheck_flags(
            value="x", span="long enough span", source_text="x long enough span",
            field_type="categorical",
        )
        assert dataclasses.is_dataclass(f)
        with pytest.raises(dataclasses.FrozenInstanceError):
            f.span_present = False  # type: ignore[misc]

    def test_unknown_field_type_raises(self):
        with pytest.raises(ValueError):
            compute_precheck_flags(
                value="x", span="long enough span here", source_text="source",
                field_type="mystery",  # type: ignore[arg-type]
            )

    def test_stop_words_exported(self):
        assert "the" in STOP_WORDS
        assert "suturing" not in STOP_WORDS

    def test_field_type_alias_exported(self):
        # FieldType is a Literal; just confirm import-time availability.
        assert FieldType is not None

    def test_span_length_reports_stripped_count(self):
        span = "   padded span text here   "
        f = compute_precheck_flags(
            value="x", span=span, source_text="padded span text here",
            field_type="categorical",
        )
        assert f.span_length == len(span.strip())
