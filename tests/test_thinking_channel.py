"""Thinking-channel parse regression (REGRESSION-01).

Ollama >= 0.12 auto-enables thinking for deepseek-r1 and moves it from inline
`<think>` tags in the response content to a separate `message.thinking` field.
The old `parse_thinking_trace` matched only the tags and *silently returned the
whole content* when they were absent — so on 0.21.0 the fallback fired on every
Pass 1 call and Pass 2 was primed with the model's answer instead of its
reasoning. Local anchored rate fell 54.3% → 10.5% on identical papers.

These tests pin both response shapes and, most importantly, assert that the
silent fallback is gone.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from engine.agents.extractor import (
    MissingThinkingChannelError,
    extract_pass1_reasoning,
    parse_thinking_trace,
)
from engine.core.extraction_telemetry import SCHEMA_VERSION, read_calls, record_call
from engine.core.review_spec import ExtractionModels

NATIVE_THINKING = "Let me work through the fields. The paper says n=12 pigs..."
ANSWER_CONTENT = '{"fields": [{"field_name": "study_type", "value": "Original Research"}]}'


# ── parse_thinking_trace ─────────────────────────────────────────────────


def test_native_thinking_field_is_the_primary_source():
    """Ollama >= 0.12 shape: thinking arrives in message.thinking."""
    trace, branch = parse_thinking_trace(ANSWER_CONTENT, NATIVE_THINKING)
    assert trace == NATIVE_THINKING
    assert branch == "native"


def test_legacy_inline_tags_still_parse():
    """Pre-0.12 shape: <think>…</think> inline in content."""
    content = f"<think>{NATIVE_THINKING}</think>\n{ANSWER_CONTENT}"
    trace, branch = parse_thinking_trace(content, None)
    assert trace == NATIVE_THINKING
    assert branch == "legacy-tags"


def test_native_wins_over_legacy_tags_when_both_present():
    content = f"<think>stale tags</think>{ANSWER_CONTENT}"
    trace, branch = parse_thinking_trace(content, NATIVE_THINKING)
    assert trace == NATIVE_THINKING
    assert branch == "native"


def test_multiline_legacy_tags():
    content = "<think>line one\nline two</think>answer"
    trace, branch = parse_thinking_trace(content, None)
    assert trace == "line one\nline two"
    assert branch == "legacy-tags"


# ── the regression: the silent fallback must be gone ─────────────────────


def test_missing_thinking_channel_raises_and_never_substitutes_content():
    """THE regression test.

    Old behaviour: return `content` — i.e. hand Pass 2 the model's answer and
    call it a reasoning trace. New behaviour: raise.
    """
    with pytest.raises(MissingThinkingChannelError) as ei:
        parse_thinking_trace(ANSWER_CONTENT, None)
    msg = str(ei.value)
    assert "Refusing to substitute" in msg
    assert "REGRESSION-01" in msg
    # the offending content is quoted for diagnosis, but never returned as a trace
    assert "study_type" in msg


@pytest.mark.parametrize("thinking", [None, "", "   ", "\n\t"])
def test_blank_thinking_is_treated_as_absent(thinking):
    with pytest.raises(MissingThinkingChannelError):
        parse_thinking_trace(ANSWER_CONTENT, thinking)


def test_empty_content_and_no_thinking_also_raises():
    with pytest.raises(MissingThinkingChannelError):
        parse_thinking_trace("", None)


def test_parse_returns_a_branch_label_for_every_success():
    assert parse_thinking_trace("x", "t")[1] in ("native", "legacy-tags")
    assert parse_thinking_trace("<think>t</think>", None)[1] in ("native", "legacy-tags")


# ── extract_pass1_reasoning: explicit think, native consumption ──────────


def _resp(content, thinking=None, done_reason="stop"):
    return SimpleNamespace(
        message=SimpleNamespace(content=content, thinking=thinking),
        done_reason=done_reason,
    )


def test_pass1_passes_think_explicitly_and_defaults_on():
    """No reliance on the Ollama default — that is what hid the interface change."""
    with patch("engine.agents.extractor.ollama_chat") as chat:
        chat.return_value = _resp(ANSWER_CONTENT, NATIVE_THINKING)
        extract_pass1_reasoning("prompt")
    assert chat.call_args.kwargs["think"] is True


def test_pass1_think_is_overridable():
    with patch("engine.agents.extractor.ollama_chat") as chat:
        chat.return_value = _resp(ANSWER_CONTENT, NATIVE_THINKING)
        with pytest.raises(MissingThinkingChannelError):
            # think=False on a model that then returns no thinking must still
            # refuse to substitute content.
            chat.return_value = _resp(ANSWER_CONTENT, None)
            extract_pass1_reasoning("prompt", think=False)
    assert chat.call_args.kwargs["think"] is False


def test_pass1_returns_the_native_trace_not_the_answer():
    with patch("engine.agents.extractor.ollama_chat") as chat:
        chat.return_value = _resp(ANSWER_CONTENT, NATIVE_THINKING)
        trace = extract_pass1_reasoning("prompt")
    assert trace == NATIVE_THINKING
    assert "field_name" not in trace, "the answer must never leak in as the trace"


def test_pass1_records_branch_telemetry():
    from engine.agents.extractor import _LAST_PASS1_TELEMETRY

    with patch("engine.agents.extractor.ollama_chat") as chat:
        chat.return_value = _resp(ANSWER_CONTENT, NATIVE_THINKING, done_reason="stop")
        extract_pass1_reasoning("prompt")
    assert _LAST_PASS1_TELEMETRY["parse_branch"] == "native"
    assert _LAST_PASS1_TELEMETRY["thinking_present"] is True
    assert _LAST_PASS1_TELEMETRY["thinking_chars"] == len(NATIVE_THINKING)
    assert _LAST_PASS1_TELEMETRY["finish_reason"] == "stop"


# ── think policy is declared in one place ────────────────────────────────


def test_spec_declares_think_policy_per_pass():
    m = ExtractionModels()
    assert m.pass1_think is True, "Pass 1 exists to produce a reasoning trace"
    assert m.pass2_think is False, "Pass 2 emits schema-constrained JSON"


def test_spec_policy_is_reachable_from_a_loaded_spec():
    from pathlib import Path

    from engine.core.review_spec import load_review_spec

    spec_path = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy_v1.yaml"
    if not spec_path.exists():
        pytest.skip("spec not available")
    spec = load_review_spec(str(spec_path))
    assert spec.extraction_models.pass1_think is True
    assert spec.extraction_models.pass2_think is False


# ── telemetry v2 ─────────────────────────────────────────────────────────


def test_telemetry_schema_is_v2_and_carries_the_thinking_fields(tmp_path):
    record_call(tmp_path, arm="deepseek-r1:32b", paper_id=39, attempt=1,
                outcome="stored", thinking_present=True, thinking_chars=1744,
                parse_branch="native")
    row = read_calls(tmp_path)[0]
    assert row["schema"] == SCHEMA_VERSION == "extraction-telemetry-2"
    assert row["thinking_present"] is True
    assert row["thinking_chars"] == 1744
    assert row["parse_branch"] == "native"


def test_telemetry_thinking_fields_default_to_none_for_cloud_arms(tmp_path):
    record_call(tmp_path, arm="openai_o4_mini_high", paper_id=1, attempt=1, outcome="stored")
    row = read_calls(tmp_path)[0]
    assert row["parse_branch"] is None
    assert row["thinking_present"] is None
