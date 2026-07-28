"""Write-boundary completeness guard and retry policy (INSTRUMENT-01).

The regression these tests exist for: SPANLOSS-01 found 17 openai extractions
that returned a bare single span object, were salvaged into a valid-looking
one-span extraction, and were stored. Paper 277's exact response shape is
reproduced below and must now be rejected and retried, never stored.
"""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from engine.core.completeness import (
    MAX_COMPLETENESS_ATTEMPTS,
    CompletenessResult,
    IncompleteExtractionError,
    check_completeness,
    enforce_completeness,
    expected_field_names,
)
from engine.core.extraction_telemetry import read_calls, record_call, telemetry_path
from engine.core.review_spec import load_review_spec

SPEC_PATH = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy_v1.yaml"
CODEBOOK = Path(__file__).resolve().parent.parent / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not SPEC_PATH.exists(), reason="Spec not available")


# The literal response that collapsed paper 277 (SPANLOSS-01 §2), byte-for-byte
# in shape: a bare, syntactically complete, unwrapped single span object.
PAPER_277_RESPONSE = {
    "field_name": "study_type",
    "value": "Original Research",
    "source_snippet": (
        "We introduced an open-source surgical embodied intelligence simulator "
        "for an interactive environment to develop reinforcement learning "
        "methods for minimally invasive surgical robots."
    ),
    "confidence": 0.9,
    "tier": 1,
}


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(str(SPEC_PATH))


@pytest.fixture(scope="module")
def expected(spec):
    return expected_field_names(spec, CODEBOOK if CODEBOOK.exists() else None)


def _complete(expected):
    return [{"field_name": n, "value": "NR", "source_snippet": "s",
             "confidence": 0.9, "tier": 1} for n in expected]


# ── expected field set ───────────────────────────────────────────────────


def test_expected_set_is_derived_not_hardcoded(spec, expected):
    n_spec = sum(len(spec.extraction_schema.fields_by_tier(t)) for t in (1, 2, 3, 4))
    assert len(expected) == n_spec
    assert len(set(expected)) == len(expected), "no duplicates"
    assert "study_type" in expected and "clinical_readiness_assessment" in expected


def test_expected_set_matches_the_codebook(spec):
    """Spec and codebook must agree; the guard follows the spec, but a
    divergence would mean the prompt and the codebook disagree."""
    if not CODEBOOK.exists():
        pytest.skip("codebook not available")
    import yaml
    cb = {f["name"] for f in yaml.safe_load(CODEBOOK.read_text())["fields"]}
    assert set(expected_field_names(spec)) == cb


def test_expected_set_is_in_prompt_order(spec, expected):
    """Tier 1 fields first — the same traversal build_extraction_prompt uses."""
    tier1 = [f.name for f in spec.extraction_schema.fields_by_tier(1)]
    assert list(expected[:len(tier1)]) == tier1


# ── check_completeness ───────────────────────────────────────────────────


def test_complete_set_passes(expected):
    r = check_completeness(_complete(expected), expected)
    assert r.complete is True
    assert r.missing == ()
    assert r.n_produced == r.n_expected == len(expected)


def test_missing_fields_are_named(expected):
    spans = [s for s in _complete(expected) if s["field_name"] != "country"]
    r = check_completeness(spans, expected)
    assert r.complete is False
    assert r.missing == ("country",)


def test_single_span_is_incomplete(expected):
    r = check_completeness([PAPER_277_RESPONSE], expected)
    assert r.complete is False
    assert r.n_produced == 1
    assert len(r.missing) == len(expected) - 1


def test_empty_is_incomplete(expected):
    assert check_completeness([], expected).complete is False
    assert check_completeness(None, expected).complete is False


def test_unexpected_and_duplicate_fields_reported_but_not_fatal(expected):
    """'Title' and 'field_1' — the local arm's junk field names — must surface."""
    spans = _complete(expected) + [
        {"field_name": "Title", "value": "x", "source_snippet": "", "confidence": 1.0, "tier": 1},
        {"field_name": "country", "value": "dup", "source_snippet": "", "confidence": 1.0, "tier": 2},
    ]
    r = check_completeness(spans, expected)
    assert r.complete is True
    assert r.unexpected == ("Title",)
    assert r.duplicated == ("country",)


def test_accepts_objects_as_well_as_dicts(expected):
    spans = [SimpleNamespace(field_name=n) for n in expected]
    assert check_completeness(spans, expected).complete is True


# ── enforce_completeness ─────────────────────────────────────────────────


def test_enforce_passes_complete(expected):
    r = enforce_completeness(_complete(expected), expected, paper_id=1, arm="test")
    assert isinstance(r, CompletenessResult) and r.complete


def test_enforce_raises_on_the_paper_277_shape(expected):
    with pytest.raises(IncompleteExtractionError) as ei:
        enforce_completeness([PAPER_277_RESPONSE], expected,
                             paper_id=277, arm="openai_o4_mini_high",
                             salvage="single_span_dict", attempt=2)
    exc = ei.value
    assert exc.paper_id == 277
    assert exc.n_stored == 1
    assert exc.salvage == "single_span_dict"
    assert exc.attempt == 2
    assert "study_type" not in exc.missing      # the one field that survived
    assert "robot_platform" in exc.missing
    assert "1/20 fields" in str(exc) or f"1/{len(expected)} fields" in str(exc)


# ── telemetry ────────────────────────────────────────────────────────────


def test_telemetry_records_a_call(tmp_path):
    record_call(tmp_path, arm="openai_o4_mini_high", paper_id=277, attempt=1,
                outcome="incomplete_retry", model="o4-mini-2025-04-16",
                finish_reason="length", raw_content=json.dumps(PAPER_277_RESPONSE),
                spans_parsed=1, fields_expected=20, missing_fields=("country",),
                salvage="single_span_dict", input_tokens=15920,
                output_tokens=19635, reasoning_tokens=19392)
    rows = read_calls(tmp_path)
    assert len(rows) == 1
    r = rows[0]
    assert r["finish_reason"] == "length"          # the field SPANLOSS-01 lacked
    assert json.loads(r["raw_content"]) == PAPER_277_RESPONSE
    assert r["attempt"] == 1 and r["outcome"] == "incomplete_retry"
    assert r["salvage"] == "single_span_dict"
    assert r["reasoning_tokens"] == 19392
    assert r["missing_fields"] == ["country"]


def test_telemetry_appends_one_line_per_attempt(tmp_path):
    for a in (1, 2, 3):
        record_call(tmp_path, arm="local", paper_id=9, attempt=a, outcome="incomplete_retry")
    assert [r["attempt"] for r in read_calls(tmp_path)] == [1, 2, 3]


def test_telemetry_truncates_a_huge_response_and_says_so(tmp_path):
    from engine.core.extraction_telemetry import RAW_CONTENT_LIMIT
    record_call(tmp_path, arm="a", paper_id=1, attempt=1, outcome="stored",
                raw_content="x" * (RAW_CONTENT_LIMIT + 10))
    r = read_calls(tmp_path)[0]
    assert r["raw_content_truncated"] is True
    assert r["raw_content_chars"] == RAW_CONTENT_LIMIT + 10
    assert len(r["raw_content"]) == RAW_CONTENT_LIMIT


def test_telemetry_never_raises_on_a_bad_path():
    assert record_call("/proc/nonexistent/telemetry", arm="a", paper_id=1,
                       attempt=1, outcome="stored") is None


def test_read_calls_skips_malformed_lines(tmp_path):
    p = telemetry_path(tmp_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text('{"attempt": 1}\nnot json\n{"attempt": 2}\n')
    assert [r["attempt"] for r in read_calls(tmp_path)] == [1, 2]


# ── retry driver (cloud) ─────────────────────────────────────────────────


class _FakeCloud:
    """Minimal stand-in exercising CloudExtractorBase.extract_with_completeness."""

    ARM = "openai_o4_mini_high"
    model_string = "o4-mini-2025-04-16"

    def __init__(self, results, review_dir):
        self._results = list(results)
        self._review_dir = Path(review_dir)
        self._last_salvage = None
        self.calls = 0

    def extract_paper(self, paper_id, parsed_text):
        self.calls += 1
        r = self._results[min(self.calls - 1, len(self._results) - 1)]
        self._last_salvage = r.get("_salvage")
        return r


def _driver(fake, expected):
    from engine.cloud.base import CloudExtractorBase
    fake.expected_fields = expected
    return CloudExtractorBase.extract_with_completeness(fake, 277, "text")


def test_retry_stops_as_soon_as_a_complete_result_arrives(tmp_path, expected):
    incomplete = {"spans": [PAPER_277_RESPONSE], "_salvage": "single_span_dict"}
    complete = {"spans": _complete(expected)}
    fake = _FakeCloud([incomplete, complete], tmp_path)
    out = _driver(fake, expected)
    assert fake.calls == 2
    assert len(out["spans"]) == len(expected)
    rows = read_calls(tmp_path)
    assert [r["outcome"] for r in rows] == ["incomplete_retry", "stored"]
    assert rows[0]["salvage"] == "single_span_dict"


def test_paper_277_regression_exhausts_and_never_stores(tmp_path, expected):
    """The SPANLOSS-01 regression: this response must be retried, then fail."""
    fake = _FakeCloud([{"spans": [PAPER_277_RESPONSE], "_salvage": "single_span_dict"}], tmp_path)
    with pytest.raises(IncompleteExtractionError) as ei:
        _driver(fake, expected)
    assert fake.calls == MAX_COMPLETENESS_ATTEMPTS
    assert ei.value.n_stored == 1
    rows = read_calls(tmp_path)
    assert len(rows) == MAX_COMPLETENESS_ATTEMPTS
    assert rows[-1]["outcome"] == "incomplete_exhausted"
    assert all(r["salvage"] == "single_span_dict" for r in rows)


def test_retry_records_finish_reason_and_raw_content_per_attempt(tmp_path, expected):
    fake = _FakeCloud([{
        "spans": [PAPER_277_RESPONSE],
        "finish_reason": "length",
        "raw_content": json.dumps(PAPER_277_RESPONSE),
        "input_tokens": 15920, "output_tokens": 19635, "reasoning_tokens": 19392,
    }], tmp_path)
    with pytest.raises(IncompleteExtractionError):
        _driver(fake, expected)
    rows = read_calls(tmp_path)
    assert all(r["finish_reason"] == "length" for r in rows)
    assert all(json.loads(r["raw_content"]) == PAPER_277_RESPONSE for r in rows)
    assert all(r["output_tokens"] == 19635 for r in rows)


def test_a_complete_first_attempt_makes_exactly_one_call(tmp_path, expected):
    fake = _FakeCloud([{"spans": _complete(expected)}], tmp_path)
    _driver(fake, expected)
    assert fake.calls == 1
    assert [r["outcome"] for r in read_calls(tmp_path)] == ["stored"]
