"""U38 (INT-s12-1, R694(f)) — the extractor's snippet precheck consults the paper text, and an exhausted
snippet retry keeps the original snippet instead of replacing it with "".

12v_RSA.md §Q6 read the defect: `_has_invalid_snippet` judges the snippet alone, although the paper text
is in scope at its caller (`_validate_and_retry_snippets(fields, paper_text, …)`); each flag costs up to
two `extract_retry_snippet` calls carrying the whole paper; and when both fail, "a valid snippet is
replaced with "", which the legacy write boundary refuses (UncitedValueError, retryable)". §Q7: 29 of 190
corpus texts contain an ellipsis. R694(f) fixes it as S-1 (the precheck consults the paper text, so a
snippet whose ellipsis is verbatim in the text is not flagged) with S-2 folded in (a snippet is never
replaced with "" when retries run out — the original stays, and the write boundary decides). No pin moves.

Written in Phase 1 (12v-U38A) against the unmodified tree, where T1 and T4 failed and the rest passed,
and held in 12v_U38A/phase1/ until the fix commit (R695(b)); T5 rewritten there to R699(b)'s form (a).
Every model call is mocked at the extractor's `ollama_chat`; no database.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from engine.agents import extractor as E
from engine.agents.models import EvidenceSpan
from engine.core.citation_guard import LEGACY, enforce_citations
from engine.core.effective_config import stage_config

PAPER = ("Methods. Twenty pigs were enrolled. The robot completed the anastomosis … and no leak was "
         "observed at 30 cmH2O. Results were recorded... in a blinded log. Two were lost to follow-up. "
         "Complications were minor.")
VERBATIM_UNICODE = "The robot completed the anastomosis … and no leak was observed at 30 cmH2O."
VERBATIM_DOTS = "Results were recorded... in a blinded log."
BRIDGING = "Two were lost to follow-up ... Complications were minor."   # U14's bridging fixture
PLAIN = "Twenty pigs were enrolled."


def _span(snippet, field="sample_size", value="20"):
    return EvidenceSpan(field_name=field, value=value, source_snippet=snippet, confidence=0.9, tier=1)


def _cfg():
    return stage_config("extract_retry_snippet")


def _reply(snippet):
    """The extractor's `ollama_chat` as `_retry_snippet` calls it (return_request_hash=True)."""
    return lambda **kw: (SimpleNamespace(message=SimpleNamespace(content=json.dumps({"source_snippet": snippet}))),
                         "f" * 64)


def test_u38_a_verbatim_snippet_containing_an_ellipsis_is_not_flagged():
    """INT-s12-1, R694(f) S-1, 12v_RSA.md §11 S-1 ("a verbatim quote containing "…" / "..." from a text
    containing it sends no retry call"): a snippet that is an exact substring of the paper text is not
    flagged, for the Unicode ellipsis and for three dots. Fails on the unmodified tree, whose precheck
    reads the snippet alone and retries both."""
    for snippet in (VERBATIM_UNICODE, VERBATIM_DOTS):
        assert snippet in PAPER
        with patch.object(E, "_retry_snippet") as retry:
            retry.return_value = ("unused", "e" * 64)
            out = E._validate_and_retry_snippets([_span(snippet)], PAPER, paper_id=1, cfg=_cfg())
        assert retry.call_count == 0, f"a verbatim quote was sent to the retry: {snippet!r}"
        assert out[0].source_snippet == snippet


def test_u38_a_snippet_bridging_passages_with_an_ellipsis_is_still_flagged():
    """INT-s12-1, R694(f) S-1, 12v_RSA.md §11 S-1 ("a bridging snippet on a text without the ellipsis
    [still] makes the call"): the ellipsis is in the snippet and not in the text at that span, so the
    snippet is flagged and retried. Passes before and after U38."""
    assert BRIDGING not in PAPER
    with patch.object(E, "_retry_snippet") as retry:
        retry.return_value = ("Two were lost to follow-up.", "e" * 64)
        out = E._validate_and_retry_snippets([_span(BRIDGING)], PAPER, paper_id=1, cfg=_cfg())
    assert retry.call_count == 1
    assert out[0].source_snippet == "Two were lost to follow-up."


def test_u38_a_snippet_without_an_ellipsis_is_unchanged():
    """INT-s12-1, R694(f): a snippet with no ellipsis is not flagged and passes through untouched. A
    snippet with no ellipsis that is NOT in the text is not flagged either: the precheck is not a
    locator (the audit stage's locator judges placement). This pins today's behaviour on purpose; U38
    must keep it."""
    for snippet in (PLAIN, "A sentence that appears nowhere in this paper."):
        with patch.object(E, "_retry_snippet") as retry:
            out = E._validate_and_retry_snippets([_span(snippet)], PAPER, paper_id=1, cfg=_cfg())
        assert retry.call_count == 0
        assert out[0].source_snippet == snippet


def test_u38_exhausted_retries_keep_the_original_snippet():
    """INT-s12-1, R694(f) S-2 ("a snippet is never replaced with "" when retries run out — the original
    stays"), 12v_RSA.md §11 S-2: the model call (mocked) returns the same bridging snippet on both
    retries, `_retry_snippet` rejects it each time, and after exhaustion the field still carries the
    original snippet, not "". Fails on the unmodified tree, which writes "" (pinned today by
    tests/test_extractor.py::test_retry_exhausted_nulls_snippet_preserves_value)."""
    with patch.object(E, "ollama_chat", side_effect=_reply(BRIDGING)) as chat:
        out = E._validate_and_retry_snippets([_span(BRIDGING)], PAPER, paper_id=1, cfg=_cfg())
    assert chat.call_count == E.SNIPPET_MAX_RETRIES
    assert out[0].value == "20"
    assert out[0].source_snippet == BRIDGING


def test_u38_the_write_boundary_still_refuses_an_uncited_value():
    """INT-s12-1, R694(f) as amended by R699(b) (form (a)): when the retries are exhausted the original
    snippet stays on the claim; the legacy write boundary (`enforce_citations`, mode=LEGACY), which
    checks that a snippet is present, accepts the claim as cited; and the audit stage's locator judges
    the snippet — not EXACT, and `bridged` (U14) — so an unlocated or bridged quote reaches semantic
    verification instead of the value being refused. The id is the Phase 1 brief's; the assertion is
    R699(b)'s chain (Phase 1 asserted the refusal that only the emptied snippet produced)."""
    from engine.core.locator import EXACT, locate
    with patch.object(E, "ollama_chat", side_effect=_reply(BRIDGING)):
        out = E._validate_and_retry_snippets([_span(BRIDGING)], PAPER, paper_id=1, cfg=_cfg())
    assert out[0].source_snippet == BRIDGING                       # kept on the claim
    spans = [{"field_name": s.field_name, "value": s.value, "source_snippet": s.source_snippet} for s in out]
    enforce_citations(spans, paper_id=1, arm="deepseek-r1:32b", mode=LEGACY,      # accepts: no raise
                      escape_token="NO_EVIDENCE_LOCATABLE", absence_sentinels=frozenset({"NR"}),
                      contract_unmet_token="CONTRACT_UNMET")
    located = locate(PAPER, out[0].source_snippet)
    assert located.kind != EXACT and located.bridged


#: Both arms' per-stage prompt hashes after U37 (12v_U37B/pin_tuples_u37.json; R695(d): prompt hashes,
#: not the whole tuple, so an installed-library upgrade does not fail this test).
PROMPT_HASHES = {
    "elicited": {
        "audit": "d972c46dde944f1381c4f2d5472c6264c3a1053bd23bdd3801bdf0fdc2a5838d",
        "elicitation_pass1": "5939ce6ffc5cfc1960e1fa22ac4bb811a15e7bac4e0420a89d793bdd5919b713",
        "extract_pass2": "b2a44f28c9d9cade12a5782fdfce3618daca1b95a83b152c448fffb88ac710fe",
        "preflight:deepseek-r1:32b": "de95332f63517c2c6a6558c76892369173dd3a55cf71944cabd0850ed44d322a",
        "preflight:gemma3:27b": "de95332f63517c2c6a6558c76892369173dd3a55cf71944cabd0850ed44d322a",
    },
    "non_elicited": {
        "audit": "d972c46dde944f1381c4f2d5472c6264c3a1053bd23bdd3801bdf0fdc2a5838d",
        "extract_pass1": "c0083f30e0d7a4ff3e8036ac6e6feeb0e79319dab7693ec246aee3809d054f34",
        "extract_pass2": "a4f3cb35706fcc1974494f0e746e10a6bc21499a661bc9a49bcbf2d51b6e8e78",
        "extract_retry_snippet": "05a981814969ec38c5785343a403d02f6c5c8ff2724490cae3a602da069b55aa",
        "preflight:deepseek-r1:32b": "de95332f63517c2c6a6558c76892369173dd3a55cf71944cabd0850ed44d322a",
        "preflight:gemma3:27b": "de95332f63517c2c6a6558c76892369173dd3a55cf71944cabd0850ed44d322a",
    },
}


def test_u38_both_pins_are_unchanged():
    """INT-s12-1, R694(f) ("No pin moves"), 12v_RSA.md F14: every stage's prompt hash of both arms,
    resolved as 12l_U04/pin_tuples.py resolves them (digests fixed, no fetch), equals its value after U37.
    Passes before and after U38."""
    from engine.agents.extractor import extraction_stages
    from engine.core import run_manifest as rm
    from engine.core.codebook import load_codebook
    from engine.core.review_paths import load_spec_for
    cb = load_codebook("data/surgical_autonomy/extraction_codebook.yaml")
    spec0 = load_spec_for("surgical_autonomy")
    digests = {"deepseek-r1:32b": "edba8017331d15236e57480eb45406c0d721db77a4cdcf234df500fc2ad3960c",
               "gemma3:27b": "a418f5838eaf7fe2cfe0a3046c8384b68ba43a4435542c942f9db00a5f342203"}
    for label, elicit in (("non_elicited", False), ("elicited", True)):
        s = spec0.model_copy(update={"extraction_models": spec0.extraction_models.model_copy(update={"elicitation": elicit})})
        stages = [*extraction_stages(s), "audit", "preflight"]
        arms = {st: a for st in stages if (a := rm._stage_arm(s, st))}
        res = rm.resolve_run(s, stages, digest_fn=digests.__getitem__, preflight_models=sorted(digests),
                             codebook_path=cb.path, arms_by_stage=arms)
        assert {k: r.prompt_hash for k, r in sorted(res.items())} == PROMPT_HASHES[label], label
