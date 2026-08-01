"""QUALGAP-01 harness invariants.

The study asks whether the Ollama runtime version explains the Run 6 anchoring
gap, and every answer it can give depends on three properties that are easy to
break silently and impossible to notice from the output:

  1. cell V1 must send **no** `think` key at all — `think=False` would disable
     thinking outright and `think=True` would erase the distinction from V2;
  2. the run must talk to the 0.17.7 server and refuse anything else, because a
     silent fall-back onto the 0.21.0 production port produces a confident answer
     to the wrong question;
  3. the pinned reads must fire on the thresholds the brief fixed in advance.

Offline: no Ollama, no network, no review.db.
"""

from unittest.mock import patch

import httpx
import pytest

from analysis.eval.analyze_qualgap01 import PINNED_MARGIN_PP, _SNIPPET_RE, pinned_read
from analysis.eval.qualgap01 import CELL_V1, CELL_V2, EXPECTED_SERVER_VERSION, PASS1_THINK
from analysis.eval import run_qualgap01 as R


@pytest.fixture(autouse=True)
def _restore_client_module():
    """`bind_runtime` rebinds module globals; never let that leak into the suite."""
    client, restart = R.oc._client, R.oc._restart_ollama_and_retry
    yield
    R.oc._client, R.oc._restart_ollama_and_retry = client, restart


class _Msg:
    content = "answer text"
    thinking = "reasoning text"


class _Resp:
    message = _Msg()
    done_reason = "stop"


# ── cell definitions ─────────────────────────────────────────────────────


def test_v1_omits_the_think_kwarg_entirely():
    """V1 reproduces the Run 6-era call shape: the kwarg did not exist."""
    with patch.object(R.oc, "ollama_chat", return_value=_Resp()) as chat:
        R.pass1("prompt", PASS1_THINK[CELL_V1])
    assert "think" not in chat.call_args.kwargs


def test_v2_passes_think_true():
    with patch.object(R.oc, "ollama_chat", return_value=_Resp()) as chat:
        R.pass1("prompt", PASS1_THINK[CELL_V2])
    assert chat.call_args.kwargs["think"] is True


def test_cells_differ_only_in_the_think_argument():
    """Anything else differing would confound the version question."""
    calls = []
    for cell in (CELL_V1, CELL_V2):
        with patch.object(R.oc, "ollama_chat", return_value=_Resp()) as chat:
            R.pass1("prompt", PASS1_THINK[cell])
        kwargs = dict(chat.call_args.kwargs)
        kwargs.pop("think", None)
        calls.append(kwargs)
    assert calls[0] == calls[1]


def test_pass1_reads_the_native_channel_and_reports_the_branch():
    with patch.object(R.oc, "ollama_chat", return_value=_Resp()):
        trace, branch, content, _ = R.pass1("prompt", None)
    assert (trace, branch, content) == ("reasoning text", "native", "answer text")


# ── runtime binding ──────────────────────────────────────────────────────


def _version_response(version: str):
    return httpx.Response(200, json={"version": version},
                          request=httpx.Request("GET", "http://x/api/version"))


def test_bind_runtime_refuses_a_server_of_the_wrong_version():
    """Landing on the 0.21.0 production port must fail loudly, not silently."""
    with patch.object(R.httpx, "get", return_value=_version_response("0.21.0")):
        with pytest.raises(RuntimeError, match="0.21.0"):
            R.bind_runtime("http://127.0.0.1:11434")


def test_bind_runtime_accepts_the_expected_version_and_disarms_restart():
    with patch.object(R.httpx, "get", return_value=_version_response(EXPECTED_SERVER_VERSION)):
        assert R.bind_runtime("http://127.0.0.1:11435") == EXPECTED_SERVER_VERSION
    # The production systemd service must be unreachable from this run.
    with pytest.raises(RuntimeError, match="disarmed"):
        R.oc._restart_ollama_and_retry(model="m", messages=[])


# ── pinned reads ─────────────────────────────────────────────────────────


def test_within_margin_of_run6_convicts_the_runtime():
    r = pinned_read(v1_pct=56.0, run6_pct=58.2, cond_b_pct=38.8)
    assert r["outcome"] == "RUNTIME_CONVICTED"


def test_at_the_0210_level_kills_the_hypothesis():
    r = pinned_read(v1_pct=39.5, run6_pct=58.2, cond_b_pct=38.8)
    assert r["outcome"] == "HYPOTHESIS_DEAD"


def test_between_the_arms_yields_no_auto_conclusion():
    r = pinned_read(v1_pct=48.0, run6_pct=58.2, cond_b_pct=38.8)
    assert r["outcome"] == "BETWEEN"


def test_arms_closer_than_the_margin_are_reported_indeterminate():
    """If the reference arms nearly coincide, the 5pp lens cannot separate them."""
    r = pinned_read(v1_pct=40.0, run6_pct=41.0, cond_b_pct=39.0)
    assert r["outcome"] == "INDETERMINATE"


def test_margin_is_the_briefs_five_points():
    assert PINNED_MARGIN_PP == 5.0


def test_missing_arm_is_not_computable_rather_than_guessed():
    assert pinned_read(None, 58.2, 38.8)["outcome"] == "NOT_COMPUTABLE"


# ── channel snippet extraction ───────────────────────────────────────────


RUN6_SHAPED_DRAFT = """### Extraction Fields
- **study_type**: Original Research
  **source_snippet**: "TYPE Original Research PUBLISHED 21 October 2025"
  **confidence**: 1.0
- **robot_platform**: Raven II
  "source_snippet": "Hubot was implemented on a Raven II surgical robot"
"""


def test_snippet_regex_reads_both_draft_shapes():
    """Pass-1 drafts are free-form text, not JSON — markdown and JSON keys both occur."""
    found = _SNIPPET_RE.findall(RUN6_SHAPED_DRAFT)
    assert found == [
        "TYPE Original Research PUBLISHED 21 October 2025",
        "Hubot was implemented on a Raven II surgical robot",
    ]


def test_snippet_regex_ignores_a_channel_that_quotes_nothing():
    """The thinking channel narrates rather than quoting; it must yield no snippets."""
    monologue = "Okay, so the paper mentions a Raven II somewhere. Let me check the methods."
    assert _SNIPPET_RE.findall(monologue) == []
