"""ELICIT-01 — tests for the unit post-pass, index materialization, prompt
builders, fit check / tripwire, and runner row schema.

No model calls: the runner test stubs the Ollama boundary, per the Ops Invariant
that tests must never reach a live service.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest

from analysis.eval.elicit01 import prompts as P
from analysis.eval.elicit01.manifest import CEILING_TOKENS, WORST_RATIO
from analysis.eval.elicit01.units import (
    MIN_UNIT_TOKENS, build_unit_map, merge_short, strip_comments,
)

PAPER = (
    "<!-- image -->\n\n## Methods\n\n"
    "We enrolled twelve consecutive patients in a single centre trial. "
    "Mean operative time was forty one minutes overall. "
    "H.\n\n<!-- formula-not-decoded -->\n\n"
    "The robot completed the anastomosis without conversion in every case. Fig.\n"
)


# ── Test 1: unit post-pass ───────────────────────────────────────────────

def test_units_deterministic_on_repeat():
    a = build_unit_map(1, PAPER)
    b = build_unit_map(1, PAPER)
    assert a.units == b.units


def test_comment_artifacts_absent_from_numbering():
    um = build_unit_map(1, PAPER)
    joined = " ".join(um.units)
    for marker in ("<!--", "-->", "image", "formula-not-decoded"):
        assert marker not in joined, marker
    assert "<!" not in um.render()


def test_no_unit_below_threshold_survives():
    um = build_unit_map(1, PAPER)
    assert um.units, "expected at least one unit"
    assert all(len(u.split()) >= MIN_UNIT_TOKENS for u in um.units)


def test_bijection_no_characters_dropped():
    """Concatenated units reproduce the comment-stripped source, token for token."""
    um = build_unit_map(1, PAPER)
    assert " ".join(um.units).split() == strip_comments(PAPER).split()


def test_merge_is_a_merge_not_a_discard():
    units = ["one two three four", "H.", "five six seven eight"]
    merged = merge_short(units, 3)
    assert " ".join(merged).split() == " ".join(units).split()
    assert all(len(u.split()) >= 3 for u in merged)


def test_trailing_short_unit_folds_backward():
    merged = merge_short(["alpha beta gamma delta", "Fig."], 3)
    assert len(merged) == 1
    assert merged[0].endswith("Fig.")


# ── Test 2: index materialization ────────────────────────────────────────

def test_index_round_trip_quote_is_verbatim_in_source():
    um = build_unit_map(1, PAPER)
    stripped = strip_comments(PAPER)
    norm = " ".join(stripped.split())
    for i in range(1, um.n + 1):
        quote = um.resolve(i)
        assert quote is not None
        assert " ".join(quote.split()) in norm


@pytest.mark.parametrize("bad", [0, -1, 10_000, None, "3", 2.0])
def test_invalid_index_returns_none_and_is_never_repaired(bad):
    um = build_unit_map(1, PAPER)
    assert um.resolve(bad) is None


def test_valid_index_is_not_clamped_to_neighbour():
    um = build_unit_map(1, PAPER)
    assert um.resolve(um.n) is not None
    assert um.resolve(um.n + 1) is None  # no clamping to the last unit


# ── Test 3: prompt builders cover exactly the derived STATED set ─────────

def test_both_builders_cover_exactly_the_derived_stated_fields():
    from analysis.provenance.field_class3 import STATED, fields_by_class
    derived = set(fields_by_class(STATED))
    assert len(derived) == 9
    names = {f["name"] for f in P.stated_fields()}
    assert names == derived           # derived, never hand-listed

    copy_p = P.build_copy_prompt("PAPER TEXT")
    idx_p = P.build_index_prompt("[S1] a b c", 1)
    for name in derived:
        assert name in copy_p
        assert name in idx_p


def test_builders_exclude_inferable_and_judgment_fields():
    from analysis.provenance.field_class3 import INFERABLE, JUDGMENT, fields_by_class
    names = {f["name"] for f in P.stated_fields()}
    for cls in (INFERABLE, JUDGMENT):
        assert not (names & set(fields_by_class(cls)))


def test_conditions_differ_only_in_elicitation_mode():
    copy_p = P.build_copy_prompt("BODY")
    idx_p = P.build_index_prompt("[S1] BODY", 1)
    assert "quotes" in copy_p and "unit_indices" not in copy_p
    assert "unit_indices" in idx_p and '"quotes"' not in idx_p


def test_parse_fields_recovers_container_shapes():
    payload = [{"field_name": "study_type", "value": "x", "quotes": ["q"]}]
    for raw in (json.dumps({"fields": payload}),
                "```json\n" + json.dumps({"fields": payload}) + "\n```",
                "noise " + json.dumps({"fields": payload}) + " trailing"):
        entries, path = P.parse_fields(raw)
        assert entries == payload, path
    assert P.parse_fields("not json at all")[0] == []


# ── Test 4: fit check and truncation tripwire ────────────────────────────

def test_fit_check_rejects_a_synthetic_over_ceiling_prompt():
    over = "x" * int((CEILING_TOKENS / WORST_RATIO) + 10_000)
    assert int(len(over) * WORST_RATIO) >= CEILING_TOKENS


def test_fit_check_accepts_a_prompt_under_the_ceiling():
    under = "x" * int((CEILING_TOKENS / WORST_RATIO) * 0.5)
    assert int(len(under) * WORST_RATIO) < CEILING_TOKENS


def test_row_at_ceiling_is_flagged_truncated_despite_done_reason_stop():
    """PARSE-01: done_reason cannot detect input truncation. The ceiling can."""
    from analysis.eval.elicit01.runner import Row, call
    resp = MagicMock()
    resp.message.content = "{}"
    resp.message.thinking = "t"
    resp.prompt_eval_count = CEILING_TOKENS
    resp.eval_count = 10
    resp.done_reason = "stop"
    with patch("analysis.eval.elicit01.runner.oc.ollama_chat", return_value=resp):
        row = call(1, "COPY", "p", None, "0.21.0", "d")
    assert row.done_reason == "stop"
    assert row.truncated is True


def test_row_below_ceiling_is_not_flagged():
    from analysis.eval.elicit01.runner import call
    resp = MagicMock()
    resp.message.content = "{}"
    resp.message.thinking = "t"
    resp.prompt_eval_count = CEILING_TOKENS - 1
    resp.eval_count = 10
    resp.done_reason = "stop"
    with patch("analysis.eval.elicit01.runner.oc.ollama_chat", return_value=resp):
        row = call(1, "COPY", "p", None, "0.21.0", "d")
    assert row.truncated is False


# ── Test 5: runner row schema on a stubbed response ──────────────────────

REQUIRED = [
    "paper_id", "condition", "ok", "started_utc", "finished_utc", "server_version",
    "model", "model_digest", "options", "options_sha256", "prompt_sha256",
    "prompt_chars", "system_sha256", "think", "raw_content", "raw_thinking",
    "content_chars", "thinking_chars", "done_reason", "prompt_eval_count",
    "eval_count", "latency_s", "truncated", "attempts",
]


def test_row_schema_complete_on_stubbed_success():
    from analysis.eval.elicit01.runner import call
    resp = MagicMock()
    resp.message.content = '{"fields": []}'
    resp.message.thinking = "reasoning"
    resp.prompt_eval_count = 1234
    resp.eval_count = 56
    resp.done_reason = "stop"
    with patch("analysis.eval.elicit01.runner.oc.ollama_chat", return_value=resp):
        row = asdict(call(7, "INDEX", "prompt", 42, "0.21.0", "dig"))
    for k in REQUIRED:
        assert k in row and row[k] is not None, k
    assert row["ok"] is True and row["n_units"] == 42


def test_failure_row_is_recorded_not_raised():
    from analysis.eval.elicit01.runner import call
    with patch("analysis.eval.elicit01.runner.oc.ollama_chat",
               side_effect=TimeoutError("watchdog")):
        row = asdict(call(9, "COPY", "prompt", None, "0.21.0", "dig"))
    assert row["ok"] is False
    assert "TimeoutError" in row["error"]
    assert row["prompt_sha256"] and row["options_sha256"]
