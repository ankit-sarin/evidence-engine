"""12x-U39A — U39's acceptance tests (Phase 1, scratch; not under tests/).

INT-v12-1 / INT-w12-1 (R707(d)–(e)): for every hashed stage on the local run path, the
message list the runtime sends, captured through a stand-in for `ollama_chat` at the
name the call site looks it up by, equals, byte for byte, the list
`render_messages(stage)` hashes for the same sentinel inputs. Where the render
concatenates variants the runtime sends as separate calls (`elicitation_pass1`'s
attempts, `audit`'s field-type branches), each variant is compared and the variant
count too: the render is split at its system messages and the number of runtime calls
must equal the number of rendered variants.

The stand-in raises `_Sent` (a BaseException, so the call site's own `except
Exception` cannot swallow it) carrying the messages; nothing after the call runs and
no server is reached. Seeded by 12w_V1A/repro/test_v1a_attempt2_structure.py.
"""

from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

from engine.core import effective_config as ec
from engine.core.codebook import load_codebook
from engine.core.review_paths import load_spec_for

LIVE_CODEBOOK = "data/surgical_autonomy/extraction_codebook.yaml"


class _Sent(BaseException):
    def __init__(self, messages):
        super().__init__("sent")
        self.messages = copy.deepcopy(messages)


def _fake(**kw):
    raise _Sent(kw["messages"])


def _send(fn, *a, **kw) -> list[dict]:
    """Call one runtime call site; return the message list it sent."""
    with pytest.raises(_Sent) as e:
        fn(*a, **kw)
    return e.value.messages


def _variants(rendered: list[dict]) -> list[list[dict]]:
    """The render split at each system message: one chunk per variant."""
    out: list[list[dict]] = []
    for m in rendered:
        if m["role"] == "system" or not out:
            out.append([])
        out[-1].append(m)
    return out


def _assert_one(runtime: list[dict], rendered: list[dict]):
    assert [m["role"] for m in runtime] == [m["role"] for m in rendered]
    assert len(runtime) == len(rendered)
    assert ec.canonical_json(runtime) == ec.canonical_json(rendered)
    assert runtime == rendered


def _assert_variants(calls: list[list[dict]], rendered: list[dict]):
    chunks = _variants(rendered)
    assert len(calls) == len(chunks), "variant count: runtime calls vs rendered variants"
    for sent, chunk in zip(calls, chunks):
        _assert_one(sent, chunk)
    _assert_one([m for c in calls for m in c], rendered)


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture(scope="module")
def elicited(spec):
    em = spec.extraction_models.model_copy(update={"elicitation": True})
    return spec.model_copy(update={"extraction_models": em})


@pytest.fixture(scope="module")
def cb_path():
    return load_codebook(LIVE_CODEBOOK).path


# ── abstract screen ──────────────────────────────────────────────────
@pytest.mark.parametrize("role", ["primary", "verifier"])
def test_abstract_screen_runtime_equals_hash(spec, role, monkeypatch):
    from engine.agents import screener as S
    monkeypatch.setattr(S, "ollama_chat", _fake)
    model = spec.screening_models.primary if role == "primary" else spec.screening_models.verification
    sent = _send(S.screen_paper, dict(ec.SENTINEL_PAPER), spec, 1, model=model, role=role)
    _assert_one(sent, ec.render_messages(f"abstract_screen_{role}", spec))


# ── full-text screen ─────────────────────────────────────────────────
@pytest.mark.parametrize("role", ["primary", "verifier"])
def test_ft_screen_runtime_equals_hash(spec, role, monkeypatch):
    from engine.agents import ft_screener as F
    monkeypatch.setattr(F, "ollama_chat", _fake)
    fn = F.ft_screen_paper if role == "primary" else F.ft_verify_paper
    sent = _send(fn, ec.SENTINEL_TEXT, spec)
    _assert_one(sent, ec.render_messages(f"ft_screen_{role}", spec))


# ── non-elicited extraction (the live spec) ──────────────────────────
def test_extract_pass1_runtime_equals_hash(spec, cb_path, monkeypatch):
    from engine.agents import extractor as X
    monkeypatch.setattr(X, "ollama_chat", _fake)
    prompt = X.build_extraction_prompt(ec.SENTINEL_TEXT, spec, cb_path)
    sent = _send(X.extract_pass1_reasoning, prompt, cfg=ec.stage_config("extract_pass1", spec))
    _assert_one(sent, ec.render_messages("extract_pass1", spec, codebook_path=cb_path))


def test_extract_pass2_non_elicited_runtime_equals_hash(spec, cb_path, monkeypatch):
    from engine.agents import extractor as X
    monkeypatch.setattr(X, "ollama_chat", _fake)
    prompt = X.build_extraction_prompt(ec.SENTINEL_TEXT, spec, cb_path)
    sent = _send(X.extract_pass2_structured, prompt, "R", spec, 0, codebook_hash="x",
                 cfg=ec.stage_config("extract_pass2", spec))
    _assert_one(sent, ec.render_messages("extract_pass2", spec, codebook_path=cb_path))


def test_extract_retry_snippet_runtime_equals_hash(spec, cb_path, monkeypatch):
    from engine.agents import extractor as X
    monkeypatch.setattr(X, "ollama_chat", _fake)
    sent = _send(X._retry_snippet, "f", "v", ec.SENTINEL_TEXT, 0,
                 cfg=ec.stage_config("extract_retry_snippet", spec))
    _assert_one(sent, ec.render_messages("extract_retry_snippet", spec, codebook_path=cb_path))


# ── elicited extraction (Run 7's path) ───────────────────────────────
def test_elicitation_pass1_runtime_equals_hash(elicited, cb_path, monkeypatch):
    """The seed's invariant, per attempt and with the attempt count compared."""
    from engine.elicitation import pipeline as PL
    from engine.elicitation.units import build_unit_map
    monkeypatch.setattr(PL, "ollama_chat", _fake)
    codebook, names, path = PL._sentinel_codebook(elicited, cb_path)
    um = build_unit_map(0, ec.SENTINEL_TEXT)
    cfg = ec.stage_config("elicitation_pass1", elicited)
    calls = [_send(PL.run_pass1, um, codebook, names, 0, cfg=cfg)]
    if PL.MAX_PASS1_ATTEMPTS > 1:
        calls.append(_send(PL.run_pass1, um, codebook, names, 0, cfg=cfg,
                           feedback=PL.sentinel_feedback_block(elicited, path)))
    _assert_variants(calls, ec.render_messages("elicitation_pass1", elicited,
                                               codebook_path=cb_path))


def test_extract_pass2_elicited_runtime_equals_hash(elicited, cb_path, monkeypatch):
    from engine.agents import extractor as X
    from engine.elicitation import pipeline as PL
    monkeypatch.setattr(X, "ollama_chat", _fake)
    prompt = X.build_extraction_prompt(ec.SENTINEL_TEXT, elicited, cb_path)
    priming = PL.sentinel_priming_message(elicited, cb_path)
    sent = _send(X.extract_pass2_structured, prompt, priming, elicited, 0, codebook_hash="x",
                 cfg=ec.stage_config("extract_pass2", elicited))
    _assert_one(sent, ec.render_messages("extract_pass2", elicited, codebook_path=cb_path))


# ── audit ────────────────────────────────────────────────────────────
def test_audit_runtime_equals_hash(spec, monkeypatch):
    """Both field-type branches, the variant count compared."""
    from engine.agents import auditor as A
    from engine.agents.models import EvidenceSpan
    monkeypatch.setattr(A, "ollama_chat", _fake)
    cfg = ec.stage_config("audit", spec)
    span = EvidenceSpan(field_name="f", value="v", source_snippet="s", confidence=0.5, tier=1)
    calls = [_send(A.semantic_verify, span, field_type=t, cfg=cfg)
             for t in ("text", "categorical")]
    _assert_variants(calls, ec.render_messages("audit", spec))


# ── vision parse ─────────────────────────────────────────────────────
def test_vision_parse_runtime_equals_hash(spec, tmp_path, monkeypatch):
    """A one-page PDF is made in tmp_path; the page image's base64 is replaced by the
    render's sentinel image at the name `parse_with_vision` looks base64 up by."""
    import fitz
    from engine.parsers import pdf_parser as P
    pdf = tmp_path / "one_page.pdf"
    doc = fitz.open(); doc.new_page(); doc.save(pdf); doc.close()
    monkeypatch.setattr(P, "ollama_chat", _fake)
    monkeypatch.setattr(P, "base64",
                        SimpleNamespace(b64encode=lambda b: ec.SENTINEL_IMAGE.encode()))
    sent = _send(P.parse_with_vision, str(pdf), cfg=ec.stage_config("vision_parse", spec))
    _assert_one(sent, ec.render_messages("vision_parse", spec))


# ── preflight: the duplicated expression, both copies ────────────────
def test_preflight_runtime_equals_hash(spec, monkeypatch):
    from engine.utils import ollama_preflight as OP
    monkeypatch.setattr(OP, "ollama_chat", _fake)
    model = ec.stage_config("extract_pass1", spec).model
    sent = _send(OP.check_model, model, spec=spec)
    _assert_one(sent, ec.render_messages("preflight", spec))
