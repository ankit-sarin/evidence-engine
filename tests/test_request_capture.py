"""The session-7 gate instrument: every model request, captured as sent (R56, R61).

MANIFEST-01 Phase 2a. The capture sits at the engine's last view of a request —
the keyword arguments handed to `engine.utils.ollama_client._client.chat` — and at
each cloud extractor's `client.<...>.create`. It does NOT stub the input-fit
guards: the fake client answers `_client.show` with a trained context and returns
a `prompt_eval_count` the post-call guard accepts, so both guards run exactly as
in production. It answers any number of calls, so a site that calls twice (Pass 1
then Pass 2; the snippet retry) is followed through.

Per site it asserts two things (G1):

1. **Against Phase 1** — the key set sent and every value equal the request
   MANIFEST-01 Phase 1 P1 measured at that site, with `keep_alive=-1` the only
   addition (R66). Values are compared with their TYPE: `0` and `0.0` are
   different literals on the wire, and R66 says nothing sent changes.
2. **Against the resolver** — the key set equals `EffectiveConfig.sent_keys` and
   each value equals the resolver's, so every option originates in the spec or a
   declared default.

No test here reaches a model server (R45): the client is replaced, and any real
HTTP request raises.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from engine.core.effective_config import (
    OLLAMA_STAGES, SENTINEL_IMAGE, cloud_stage_config, sha256_canonical, stage_config,
)
from engine.core.review_paths import load_spec_for
from engine.utils import ollama_client as oc

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


# ── The Phase 1 reference (MANIFEST-01 Phase 1 read-out, P1, measured at f07c9a5) ──
def _schemas():
    from engine.agents.auditor import AuditVerdict
    from engine.agents.ft_screener import FTScreeningDecision, FTVerificationDecision
    from engine.agents.models import ExtractionOutput
    from engine.agents.screener import ScreeningDecision
    from engine.core import eligibility_render as render
    spec = load_spec_for("surgical_autonomy")
    return {
        "screen": ScreeningDecision.model_json_schema(),
        "ft_primary": render.with_reason_code_vocabulary(
            FTScreeningDecision.model_json_schema(), spec.eligibility),
        "ft_verifier": FTVerificationDecision.model_json_schema(),
        "audit": AuditVerdict.model_json_schema(),
        "pass2": ExtractionOutput.model_json_schema(),
    }


def phase1_requests() -> dict[str, dict]:
    """What each site sent before session 7 — model, options, think, format —
    exactly as P1's table records it. `keep_alive` was absent at every site."""
    s = _schemas()
    return {
        "abstract_screen_primary": {"model": "qwen3:8b", "options": {"temperature": 0},
                                    "think": False, "format": s["screen"]},
        "abstract_screen_verifier": {"model": "gemma3:27b", "options": {"temperature": 0},
                                     "think": False, "format": s["screen"]},
        "ft_screen_primary": {"model": "qwen3:32b", "options": {"temperature": 0.0},
                              "think": False, "format": s["ft_primary"]},
        "ft_screen_verifier": {"model": "gemma3:27b", "options": {"temperature": 0.0},
                               "think": False, "format": s["ft_verifier"]},
        "audit": {"model": "gemma3:27b", "options": {"temperature": 0},
                  "think": False, "format": s["audit"]},
        "extract_pass1": {"model": "deepseek-r1:32b", "options": {"temperature": 0},
                          "think": True},
        "extract_pass2": {"model": "deepseek-r1:32b", "options": {"temperature": 0},
                          "think": False, "format": s["pass2"]},
        "extract_retry_snippet": {"model": "deepseek-r1:32b", "options": {"temperature": 0},
                                  "think": False},
        "elicitation_pass1": {"model": "deepseek-r1:32b", "options": {"temperature": 0},
                              "think": True},
        "vision_parse": {"model": "qwen2.5vl:7b",
                         "options": {"temperature": 0, "num_predict": 2048, "num_ctx": 8192}},
        "pdf_quality": {"model": "qwen2.5vl:7b", "options": {"temperature": 0}},
        "preflight": {"model": "deepseek-r1:32b",
                      "options": {"temperature": 0, "num_predict": 4}},
    }


def _typed(obj):
    """A value with the TYPE of every scalar made explicit, so 0 != 0.0."""
    if isinstance(obj, dict):
        return {k: _typed(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_typed(v) for v in obj]
    return (type(obj).__name__, obj)


# ── The capture ─────────────────────────────────────────────────────
class FakeClient:
    """Stands in for `ollama.Client`. Records every `chat(**kwargs)` verbatim."""

    def __init__(self, responder):
        self.calls: list[dict] = []
        self.shows: list[str] = []
        self._responder = responder
        self._client = SimpleNamespace(base_url="http://capture.invalid")

    def show(self, model):
        self.shows.append(model)
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        content, thinking = self._responder(kwargs, len(self.calls))
        chars = oc.message_chars(kwargs.get("messages"))
        return SimpleNamespace(
            message=SimpleNamespace(content=content, thinking=thinking),
            # Inside the guard's accepted band: above chars x RATIO_DROP, below
            # the ceiling — so the post-call guard runs and passes, as in production.
            prompt_eval_count=max(1, int(chars * 0.3)),
            done_reason="stop", eval_count=10,
        )


@pytest.fixture(autouse=True)
def _no_real_http(monkeypatch):
    """R45: any real HTTP request from this module is a failure, not a call."""
    import httpx

    def _refuse(*a, **kw):
        raise AssertionError("a test in the capture module attempted a real HTTP request")
    monkeypatch.setattr(httpx.Client, "send", _refuse)
    monkeypatch.setattr(httpx.AsyncClient, "send", _refuse)
    oc.clear_ceiling_cache()
    yield
    oc.clear_ceiling_cache()


@pytest.fixture
def capture(monkeypatch):
    def install(responder):
        fake = FakeClient(responder)
        monkeypatch.setattr(oc, "_client", fake)
        return fake
    return install


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


def _assert_site(stage, sent, spec, *, model=None):
    """G1's two assertions for one captured request."""
    cfg = stage_config(stage, spec, model=model)
    sent_keys = set(sent) | {"messages"}
    expected_keys = set(cfg.sent_keys)
    assert sent_keys == expected_keys, (stage, sorted(sent_keys ^ expected_keys))
    # Against the resolver: every value it resolved is the value sent.
    for key, value in cfg.kwargs().items():
        if key == "stage":
            continue
        assert _typed(sent[key]) == _typed(value), (stage, key)
    # Against Phase 1: unchanged, keep_alive=-1 the one addition.
    before = phase1_requests()[stage]
    after = {k: v for k, v in sent.items() if k != "messages"}
    assert after.pop("keep_alive") == -1, stage
    assert _typed(after) == _typed(before), stage


def _json(model_cls, **fields):
    return json.dumps(fields)


# ── The eleven call sites (twelve stages) ───────────────────────────
def test_abstract_screen_primary_and_verifier(capture, spec):
    from engine.agents import screener
    fake = capture(lambda kw, n: (json.dumps(
        {"decision": "include", "rationale": "r", "confidence": 0.9}), None))
    paper = {"title": "T", "abstract": "A", "id": 1}
    screener.screen_paper(paper, spec, 1, model=spec.screening_models.primary, role="primary")
    screener.screen_paper(paper, spec, 2, model=spec.screening_models.verification,
                          role="verifier")
    assert len(fake.calls) == 2
    _assert_site("abstract_screen_primary", fake.calls[0], spec)
    _assert_site("abstract_screen_verifier", fake.calls[1], spec)


def test_ft_screen_primary_and_verifier(capture, spec):
    from engine.agents import ft_screener

    def respond(kw, n):
        if n == 1:
            return json.dumps({"decision": "FT_ELIGIBLE", "reason_code": "eligible",
                               "rationale": "r", "confidence": 0.9}), None
        return json.dumps({"decision": "FT_ELIGIBLE", "rationale": "r",
                           "confidence": 0.9}), None
    fake = capture(respond)
    ft_screener.ft_screen_paper("Title: T\n\nAbstract: A", spec,
                                model=spec.ft_screening_models.primary)
    ft_screener.ft_verify_paper("Title: T\n\nAbstract: A", spec,
                                model=spec.ft_screening_models.verifier)
    _assert_site("ft_screen_primary", fake.calls[0], spec)
    _assert_site("ft_screen_verifier", fake.calls[1], spec)


def test_audit(capture, spec):
    from engine.agents import auditor
    fake = capture(lambda kw, n: (json.dumps(
        {"status": "verified", "grep_found": True, "reasoning": "r"}), None))
    auditor.audit_span(
        {"id": 1, "field_name": "study_design", "value": "RCT",
         "source_snippet": "A randomized trial.", "confidence": 0.9},
        "A randomized trial.", field_type="text", field_tier=1,
        cfg=stage_config("audit", spec))
    assert len(fake.calls) == 1
    _assert_site("audit", fake.calls[0], spec)


def test_extraction_pass1_pass2_and_snippet_retry(capture, spec, tmp_path):
    """Driven through `extract_paper`, the production caller, so the stage
    configs it passes are the ones checked — not ones this test built."""
    from engine.agents.extractor import extract_paper
    from engine.agents.models import EvidenceSpan, ExtractionOutput
    from engine.core.codebook import load_codebook
    from engine.core.database import ReviewDatabase
    from engine.search.models import Citation

    db = ReviewDatabase("capture_ext", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    db.add_papers([Citation(title="T", source="pubmed", pmid="C1")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    fields = [EvidenceSpan(field_name=f["name"], value="NR",
                           source_snippet=f"Snippet for {f['name']}.",
                           confidence=0.9, tier=f["tier"]) for f in cb.fields]
    # One bridged snippet, so the retry site fires.
    fields[0] = EvidenceSpan(field_name=fields[0].field_name, value="X",
                             source_snippet="first part ... second part",
                             confidence=0.9, tier=fields[0].tier)

    def respond(kw, n):
        if n == 1:
            return "draft", "The paper reports a trial."
        if n == 2:
            return ExtractionOutput(fields=fields).model_dump_json(), None
        return json.dumps({"source_snippet": "A clean sentence."}), None
    fake = capture(respond)
    # 9b-FLIP: the write is events now — a run and the text's identity. Neither
    # makes a model call, so the three captured calls are unchanged.
    from _event_store_fixture import open_extraction_run
    from _parsed_text_fixture import write_parsed
    from engine.core.parsed_text import resolve_parsed_text
    text = "The paper reports a trial. A clean sentence."
    write_parsed(db, pid, text)
    extract_paper(pid, text, spec, db, run_id=open_extraction_run(db, spec),
                  parsed_text_ref=resolve_parsed_text(db._conn, pid))
    db.close()

    assert len(fake.calls) == 3
    _assert_site("extract_pass1", fake.calls[0], spec)
    _assert_site("extract_pass2", fake.calls[1], spec)
    _assert_site("extract_retry_snippet", fake.calls[2], spec)


def test_elicitation_pass1(capture, spec):
    from engine.elicitation import pipeline as el
    from engine.elicitation.units import build_unit_map
    from engine.core.codebook import load_codebook
    from engine.core.completeness import expected_field_names
    cb = load_codebook(LIVE_CODEBOOK)
    fields = expected_field_names(spec, LIVE_CODEBOOK)
    fake = capture(lambda kw, n: ("{}", "thinking"))
    el.run_pass1(build_unit_map(1, "The paper reports a trial of a robot."), cb.raw,
                 fields, 1, cfg=stage_config("elicitation_pass1", spec))
    assert len(fake.calls) == 1
    _assert_site("elicitation_pass1", fake.calls[0], spec)


def test_vision_parse(capture, spec, tmp_path):
    """Driven through `parse_with_vision` with the config `parse_pdf` builds."""
    import fitz
    from engine.parsers.pdf_parser import parse_with_vision
    pdf = tmp_path / "one_page.pdf"
    doc = fitz.open()
    doc.new_page().insert_text((72, 72), "Scanned page")
    doc.save(str(pdf))
    doc.close()
    fake = capture(lambda kw, n: ("page text", None))
    parse_with_vision(str(pdf), vision_model=spec.pdf_parsing.vision_model,
                      cfg=stage_config("vision_parse", spec))
    assert len(fake.calls) == 1
    _assert_site("vision_parse", fake.calls[0], spec)


def test_pdf_quality(capture, spec):
    from engine.acquisition.pdf_quality_check import _classify_page
    fake = capture(lambda kw, n: (json.dumps(
        {"language": "English", "content_type": "full_manuscript",
         "confidence": 0.9, "note": ""}), None))
    _classify_page(SENTINEL_IMAGE, timeout=5, cfg=stage_config("pdf_quality", spec))
    assert len(fake.calls) == 1
    _assert_site("pdf_quality", fake.calls[0], spec)


def test_preflight(capture, spec):
    from engine.utils.ollama_preflight import check_model
    fake = capture(lambda kw, n: ("OK", None))
    with patch("engine.utils.ollama_preflight._get_model_vram_gb", return_value=0.0):
        result = check_model("deepseek-r1:32b", spec=spec)
    assert result.status == "ok"
    assert len(fake.calls) == 1
    _assert_site("preflight", fake.calls[0], spec, model="deepseek-r1:32b")


def test_every_stage_has_a_capture_test():
    """Adding a stage without a capture site here fails, rather than passing silently."""
    covered = set(phase1_requests())
    assert covered == set(OLLAMA_STAGES)


# ── The two cloud sites (C16: the complete outbound payload) ─────────
PHASE1_CLOUD = {
    "openai": {"model": "o4-mini-2025-04-16", "reasoning_effort": "high",
               "response_format": {"type": "json_object"}},
    "anthropic": {"model": "claude-sonnet-4-6", "max_tokens": 16000,
                  "thinking": {"type": "enabled", "budget_tokens": 10000}},
}
SYSTEM_LITERAL = (
    "You are a systematic review data extractor. "
    "Output valid JSON matching the requested schema. "
    "Be thorough and cite source text for every extracted value."
)


def _cloud_db(tmp_path):
    from engine.core.database import ReviewDatabase
    db = ReviewDatabase("capture_cloud", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    path = db.db_path
    db.close()
    return str(path)


def test_openai_request_as_sent(tmp_path, spec):
    from engine.cloud.openai_extractor import OpenAIExtractor
    seen = {}

    class Completions:
        def create(self, **kw):
            seen.update(kw)
            msg = SimpleNamespace(content=json.dumps({"fields": []}), reasoning_content=None)
            return SimpleNamespace(
                choices=[SimpleNamespace(message=msg, finish_reason="stop")],
                usage=SimpleNamespace(prompt_tokens=1, completion_tokens=1,
                                      completion_tokens_details=None))
    with patch("engine.cloud.openai_extractor.openai.OpenAI") as cls:
        cls.return_value = SimpleNamespace(chat=SimpleNamespace(completions=Completions()))
        ex = OpenAIExtractor(_cloud_db(tmp_path), str(REPO / "review_specs" / "surgical_autonomy.yaml"),
                             api_key="k")
        ex.extract_paper(1, "Paper text.")
        ex.close()
    user = seen.pop("messages")
    assert user[0] == {"role": "system", "content": SYSTEM_LITERAL}
    assert user[1]["role"] == "user" and "Paper text." in user[1]["content"]
    assert _typed(seen) == _typed(PHASE1_CLOUD["openai"])
    cfg = cloud_stage_config(spec, ex.arm_name)
    assert set(seen) | {"messages"} == set(cfg.sent_keys)
    # C16: the recorded hash covers system message, user turn AND parameters.
    assert ex.last_request_hash == sha256_canonical({**seen, "messages": user})


def test_anthropic_request_as_sent(tmp_path, spec):
    from engine.cloud.anthropic_extractor import AnthropicExtractor
    seen = {}

    class Messages:
        def create(self, **kw):
            seen.update(kw)
            return SimpleNamespace(
                content=[SimpleNamespace(type="text", text=json.dumps({"fields": []}))],
                stop_reason="end_turn",
                usage=SimpleNamespace(input_tokens=1, output_tokens=1))
    with patch("engine.cloud.anthropic_extractor.anthropic.Anthropic") as cls:
        cls.return_value = SimpleNamespace(messages=Messages())
        ex = AnthropicExtractor(_cloud_db(tmp_path),
                                str(REPO / "review_specs" / "surgical_autonomy.yaml"),
                                api_key="k")
        ex.extract_paper(1, "Paper text.")
        ex.close()
    system, messages = seen.pop("system"), seen.pop("messages")
    assert system == SYSTEM_LITERAL
    assert messages[0]["role"] == "user" and "Paper text." in messages[0]["content"]
    assert _typed(seen) == _typed(PHASE1_CLOUD["anthropic"])
    assert ex.last_request_hash == sha256_canonical(
        {**seen, "system": system, "messages": messages})


def test_the_fake_client_was_hit_not_a_server(capture, spec):
    """T6: the capture proves the boundary was reached, and nothing past it."""
    from engine.agents import screener
    fake = capture(lambda kw, n: (json.dumps(
        {"decision": "include", "rationale": "r", "confidence": 0.9}), None))
    screener.screen_paper({"title": "T", "abstract": "A"}, spec, 1)
    assert fake.calls and fake.shows == ["qwen3:8b"]
