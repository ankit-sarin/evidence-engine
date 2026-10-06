"""12c-C41-R2/R3 — the extraction entry points take their config, never default it.

`extract_pass1_reasoning` and `run_pass1` used to fall back to
`stage_config("<stage>")` with no spec when called without `cfg`, so a caller
holding a spec could silently send the declared defaults instead of the spec's
values. `cfg` is now a required keyword-only argument on both, and on `elicit`,
which passes it through. The parameters after `paper_id` on
`extract_pass2_structured` and `run_pass1` are keyword-only, so removing a
parameter can never shift a positional argument into its neighbour's slot.

Each test patches the model call to fail loudly: on the tree before this change
the calls ran their bodies and reached it, which is what made them red.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from engine.acquisition import pdf_quality_check as QC
from engine.agents import auditor as AU
from engine.agents import extractor as E
from engine.agents.extractor import extract_pass1_reasoning, extract_pass2_structured
from engine.agents.models import EvidenceSpan
from engine.elicitation import pipeline as PL
from engine.parsers import pdf_parser as PP


def _no_model_call(**kw):
    raise AssertionError("a model call was built")


@pytest.fixture(autouse=True)
def _fence_model_calls():
    with patch("engine.agents.extractor.ollama_chat", _no_model_call), \
            patch("engine.elicitation.pipeline.ollama_chat", _no_model_call), \
            patch("engine.agents.auditor.ollama_chat", _no_model_call), \
            patch("engine.parsers.pdf_parser.ollama_chat", _no_model_call), \
            patch("engine.acquisition.pdf_quality_check.ollama_chat", _no_model_call):
        yield


_SPAN = EvidenceSpan(field_name="f", value="v", source_snippet="s", confidence=0.5, tier=1)


@pytest.mark.parametrize("call", [
    lambda: extract_pass1_reasoning("prompt"),
    lambda: PL.run_pass1(None, {}, (), 1),
    lambda: PL.elicit(None, {}, (), 1),
    # 12c-C52: the four remaining spec-dropping sites and their two pass-throughs.
    lambda: E._retry_snippet("f", "v", "text", 1),
    lambda: E._validate_and_retry_snippets([], "text", 1),
    lambda: PP.parse_with_vision("no-such.pdf"),
    lambda: AU.semantic_verify(_SPAN),
    lambda: AU.audit_span({"field_name": "f", "value": "v", "source_snippet": "s"}, "text"),
    lambda: QC._classify_page("img"),
], ids=["extract_pass1_reasoning", "run_pass1", "elicit",
        "_retry_snippet", "_validate_and_retry_snippets", "parse_with_vision",
        "semantic_verify", "audit_span", "_classify_page"])
def test_cfg_is_required(call):
    with pytest.raises(TypeError, match="missing 1 required keyword-only argument: 'cfg'"):
        call()


def test_semantic_verify_takes_nothing_positional_after_the_span():
    """12d-B23-R2: `paper_text` was the second positional parameter until B23
    removed it, which left `field_type` in that slot — a stale caller still
    passing the paper text second would have had it rendered as the request's
    `Field type:`. Everything after `span` is keyword-only, so that call is a
    TypeError before any request is built."""
    from engine.core.effective_config import stage_config
    with pytest.raises(TypeError, match="positional argument"):
        AU.semantic_verify(_SPAN, "the paper text", cfg=stage_config("audit"))


@pytest.mark.parametrize("call", [
    lambda: extract_pass2_structured("prompt", "trace", None, 1, "codebook-hash"),
    lambda: PL.run_pass1(None, {}, (), 1, "feedback"),
], ids=["extract_pass2_structured", "run_pass1"])
def test_parameters_after_paper_id_are_keyword_only(call):
    with pytest.raises(TypeError, match="positional argument"):
        call()
