"""12c-E-PIN — a stage's prompt_hash renders every template its runtime call can send.

The invariant (12c-E-PIN-R1): through the same builder the runtime calls, with
only model output and paper text as placeholders. The arm pin carries each
stage's prompt_hash, and the reuse key (arm, paper_id, parsed_text_sha256)
relies on the pin for prompt identity — so a template that no prompt_hash
renders is a template an edit can change while `open_run` accepts the run and
selection skips the papers already extracted under the old text.

Before E-PIN, two elicited templates sat outside every hash: the Pass-2 priming
message (it filled `pass2_messages`' "R" slot) and the attempt-2 feedback block
(the render used `feedback=""`).

**Mutation tests.** Each patches a template at the name the runtime looks it up
by, and asserts the elicited stage's hash moves while every non-elicited stage's
does not. On the tree before E-PIN each of them fails: the render never called
the patched function.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from engine.core import effective_config as ec
from engine.core.codebook import load_codebook
from engine.core.review_paths import load_spec_for
from engine.elicitation import classes as C
from engine.elicitation import contracts as K
from engine.elicitation import materialize as M
from engine.elicitation import pipeline as PL
from engine.elicitation import prompts as P

LIVE_CODEBOOK = "data/surgical_autonomy/extraction_codebook.yaml"

NON_ELICITED = ("extract_pass1", "extract_pass2", "extract_retry_snippet", "audit")
MUTANT = "⟦MUTATION⟧"


@pytest.fixture(scope="module")
def cb():
    return load_codebook(LIVE_CODEBOOK)


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture(scope="module")
def elicited(spec):
    em = spec.extraction_models.model_copy(update={"elicitation": True})
    return spec.model_copy(update={"extraction_models": em})


def _hash(stage, s, cb):
    return ec.prompt_hash(stage, s, ec.stage_config(stage, s), codebook_path=cb.path)


def _text(stage, s, cb):
    return "\n".join(m["content"] for m in
                     ec.render_messages(stage, s, codebook_path=cb.path))


def _non_elicited(spec, cb):
    return {st: _hash(st, spec, cb) for st in NON_ELICITED}


def _assert_moves(stage, elicited, spec, cb, target, attr, wrap):
    before = _hash(stage, elicited, cb)
    others = _non_elicited(spec, cb)
    real = getattr(target, attr)
    with patch.object(target, attr, wrap(real)):
        assert _hash(stage, elicited, cb) != before, f"{attr} is not in {stage}'s prompt_hash"
        assert _non_elicited(spec, cb) == others, f"{attr} moved a non-elicited stage"


def _suffix(real):
    return lambda *a, **kw: real(*a, **kw) + MUTANT


# ── the Pass-2 priming message (extract_pass2, elicited) ─────────────
@pytest.mark.parametrize("target,attr", [
    (PL, "build_pass2_priming_message"),
    (M, "priming_block"),
    (M, "evidence_block"),
], ids=["build_pass2_priming_message", "priming_block", "evidence_block"])
def test_a_priming_template_edit_moves_the_elicited_pass2_hash(elicited, spec, cb, target, attr):
    _assert_moves("extract_pass2", elicited, spec, cb, target, attr, _suffix)


# ── the attempt-2 feedback block (elicitation_pass1) ─────────────────
def test_a_feedback_block_edit_moves_the_elicitation_pass1_hash(elicited, spec, cb):
    _assert_moves("elicitation_pass1", elicited, spec, cb, PL, "build_feedback_block", _suffix)


@pytest.mark.parametrize("code", sorted(K.FATAL))
def test_each_violation_requirement_text_is_hashed(elicited, spec, cb, code):
    def wrap(real):
        return lambda c, cls, *a, **kw: real(c, cls, *a, **kw) + (MUTANT if c == code else "")
    _assert_moves("elicitation_pass1", elicited, spec, cb, P, "_requirement", wrap)


@pytest.mark.parametrize("field_class", C.CLASSES)
def test_each_class_accompaniment_is_hashed(elicited, spec, cb, field_class):
    def wrap(real):
        return lambda c, cls, *a, **kw: real(c, cls, *a, **kw) + (MUTANT if cls == field_class else "")
    _assert_moves("elicitation_pass1", elicited, spec, cb, P, "_requirement", wrap)


def test_the_echo_truncation_marker_is_hashed(elicited, spec, cb):
    before = _hash("elicitation_pass1", elicited, cb)
    others = _non_elicited(spec, cb)
    with patch.object(P, "FEEDBACK_TRUNCATION_MARKER", MUTANT):
        assert _hash("elicitation_pass1", elicited, cb) != before
        assert _non_elicited(spec, cb) == others


# ── coverage: the sentinels reach every branch the runtime can send ──
def test_the_feedback_sentinel_carries_every_fatal_code(elicited, cb):
    text = _text("elicitation_pass1", elicited, cb)
    for code in K.FATAL:
        assert f"✗ {code} — " in text, code
    sent = {c for c in text.split() if c in K.FATAL | K.ADVISORY}
    assert sent == set(K.FATAL), "a new violation code needs a sentinel record"


def test_the_feedback_sentinel_carries_every_class_accompaniment(elicited, cb):
    text = _text("elicitation_pass1", elicited, cb)
    escape = C.escape_token(cb.raw)
    for cls in C.CLASSES:
        assert P._requirement(K.VALUE_WITHOUT_CITATION, cls, escape, 1) in text, cls


def test_the_feedback_sentinel_carries_every_line_template(elicited, cb):
    text = _text("elicitation_pass1", elicited, cb)
    for piece in ("## Your previous response did not meet the contract on",
                  "you returned value:", f"with {K.KEY_INDICES}:",
                  "indices that did not resolve:", f"with {K.KEY_INFERENCE}:",
                  "reasoning step(s)", P.FEEDBACK_TRUNCATION_MARKER):
        assert piece in text, piece


def test_elicitation_pass1_hashes_both_attempts(elicited, cb):
    msgs = ec.render_messages("elicitation_pass1", elicited, codebook_path=cb.path)
    assert [m["role"] for m in msgs] == ["system", "user", "system", "user"]
    assert msgs[3]["content"].startswith(msgs[1]["content"])
    assert len(msgs[3]["content"]) > len(msgs[1]["content"])


def test_the_priming_sentinel_carries_every_reachable_branch(elicited, cb):
    text = _text("extract_pass2", elicited, cb)
    for piece in ("Here is the evidence you cited", '[S1] "', '[S2] "', "(S1, S2)",
                  "criteria application, no textual basis claimed",
                  "Declared inference:", "Pass-1 value:", "Here is your prior analysis"):
        assert piece in text, piece
    for cls in C.CLASSES:
        assert f"  [{cls}]" in text, cls
    # R-1 (12c-E-PIN-B-R1): the escape branch is never sent, so it is not rendered.
    assert "no evidence was locatable" not in text


def test_extract_pass2_hash_differs_between_configurations(elicited, spec, cb):
    assert _hash("extract_pass2", spec, cb) != _hash("extract_pass2", elicited, cb)
    assert "Here is the evidence you cited" not in _text("extract_pass2", spec, cb)
