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

from engine.agents.extractor import extract_pass1_reasoning, extract_pass2_structured
from engine.elicitation import pipeline as PL


def _no_model_call(**kw):
    raise AssertionError("a model call was built")


@pytest.fixture(autouse=True)
def _fence_model_calls():
    with patch("engine.agents.extractor.ollama_chat", _no_model_call), \
            patch("engine.elicitation.pipeline.ollama_chat", _no_model_call):
        yield


@pytest.mark.parametrize("call", [
    lambda: extract_pass1_reasoning("prompt"),
    lambda: PL.run_pass1(None, {}, (), 1),
    lambda: PL.elicit(None, {}, (), 1),
], ids=["extract_pass1_reasoning", "run_pass1", "elicit"])
def test_cfg_is_required(call):
    with pytest.raises(TypeError, match="missing 1 required keyword-only argument: 'cfg'"):
        call()


@pytest.mark.parametrize("call", [
    lambda: extract_pass2_structured("prompt", "trace", None, 1, "codebook-hash"),
    lambda: PL.run_pass1(None, {}, (), 1, "feedback"),
], ids=["extract_pass2_structured", "run_pass1"])
def test_parameters_after_paper_id_are_keyword_only(call):
    with pytest.raises(TypeError, match="positional argument"):
        call()
