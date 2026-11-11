"""U29 (C24 for Wave 2 row 28; N47; R800(b)–(f)): send records every call it makes.

Seams: 12af_U40B's (tests/test_u40_cloud_claim_events.py) and 12af_U29A/fake_probe_u29a.py's. A scratch ReviewDatabase under tmp_path
with four papers, each with an eligibility event and one recorded parsed text; the tracked codebook copied beside it; a spec copy whose
`enabled_arms: []` names both cloud arms; `rm.git_state` patched clean; the cloud run opened by `rm.open_run(… cloud_arms=[both] …)` as
`scripts/run_cloud_extraction.run_arm` opens it, its run_id set on the instance as the runner sets it. Each instance is the real transport
class with only `CloudExtractorBase.__init__` run (no SDK client) and its one provider seam, `_create`, replaced: it raises the exception
it is given or returns the response it is given. Responses are the pinned SDKs' own pydantic types (`openai.types.chat.ChatCompletion`,
`anthropic.types.Message`) built from dicts — the SDK's shape, no client, no network. `openai.OpenAI` and `anthropic.Anthropic` are
replaced by recorders, and every test asserts that no client was constructed (case (5) records the constructor's keywords instead).
Both transports are parametrised.

Cases, by R800(h)'s numbering:
  (1) test_a_cloud_send_that_raises_still_records_its_call — the entry's test.
  (2) a truncated and a refused response, each provider's forms, the refusal-text-only form included: error rows with R800(c)'s
      details, each with a digest.
  (3) a complete response: completed, rm.response_digest of its text and thinking, equal for two responses differing only in id / usage.
  (4) after a raise, then a success and a store, every hash in the stored claims' chain has a run_calls row.
  (5) each client is constructed with max_retries=0.
  (6) a recorder fault while a provider exception is in flight surfaces as the run fault, the provider's exception its context.
  (7) outside a run no row is written and the exception passes (preservation: green on the parent).
  (8) an unreadable response is an error row and raises.
"""

from __future__ import annotations

import json
import shutil
from collections import Counter
import sqlite3
from pathlib import Path

import anthropic
import httpx
import openai
import pytest
from anthropic.types import Message
from openai.types.chat import ChatCompletion

from engine.cloud.anthropic_extractor import AnthropicExtractor
from engine.cloud.base import CloudExtractorBase
from engine.cloud.openai_extractor import OpenAIExtractor
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.extraction_events import is_run_fault
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from _event_store_fixture import seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"
CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
REQ = httpx.Request("POST", "https://example.invalid/v1")
PROVIDERS = ["openai", "anthropic"]


def _fields_json():
    cbk = load_codebook(CODEBOOK)
    return json.dumps({"fields": [{"field_name": f["name"], "value": "NR", "source_snippet": "", "confidence": 0.9, "tier": f["tier"]}
                                  for tier in (1, 2, 3, 4) for f in cbk.fields_by_tier(tier)]})


def oa(*, content=None, finish="stop", refusal=None, rid="chatcmpl-1", tokens=100):
    return ChatCompletion(**{"id": rid, "object": "chat.completion", "created": 1700000000, "model": "o4-mini-2025-04-16",
                             "choices": [{"index": 0, "finish_reason": finish,
                                          "message": {"role": "assistant", "content": content, "refusal": refusal}}],
                             "usage": {"prompt_tokens": tokens, "completion_tokens": 50, "total_tokens": tokens + 50}})


def an(*, text="", stop="end_turn", rid="msg_1", tokens=100, thinking="t"):
    return Message(**{"id": rid, "type": "message", "role": "assistant", "model": "claude-sonnet-4-6",
                      "content": [{"type": "thinking", "thinking": thinking, "signature": "sig"}, {"type": "text", "text": text}],
                      "stop_reason": stop, "stop_sequence": None, "usage": {"input_tokens": tokens, "output_tokens": 50}})


def complete(provider, **kw):
    return oa(content=_fields_json(), **kw) if provider == "openai" else an(text=_fields_json(), **kw)


def text_and_thinking(provider, resp):
    """What R800(d) names, read here independently of the engine: OpenAI's message content (no reasoning content on these responses),
    Anthropic's joined text blocks and joined thinking blocks."""
    if provider == "openai":
        return resp.choices[0].message.content, None
    return ("".join(b.text for b in resp.content if b.type == "text"),
            "\n".join(b.thinking for b in resp.content if b.type == "thinking"))


def _transport(cls):
    class T(cls):
        def __init__(self, db_path, spec_path, arm_name):
            CloudExtractorBase.__init__(self, db_path, spec_path, arm_name=arm_name)
            self.next = None

        def _create(self, **payload):
            if isinstance(self.next, BaseException):
                raise self.next
            return self.next
    return T


TRANSPORT = {"openai": _transport(OpenAIExtractor), "anthropic": _transport(AnthropicExtractor)}


@pytest.fixture
def world(tmp_path, monkeypatch):
    """(instances by provider, db_path, paper ids, sdk record, spec path) on a scratch review with both cloud arms registered by open_run."""
    if not CODEBOOK.exists():
        pytest.skip("codebook not available")
    sdk = {"constructed": [], "refuse": True}

    def recorder(name):
        class _R:
            def __init__(self, *a, **k):
                sdk["constructed"].append((name, dict(k)))
                if sdk["refuse"]:
                    raise RuntimeError("no SDK client in this test")
        return _R

    monkeypatch.setattr(openai, "OpenAI", recorder("openai"))
    monkeypatch.setattr(anthropic, "Anthropic", recorder("anthropic"))
    monkeypatch.setattr(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None))
    db = ReviewDatabase("u29", data_root=tmp_path)
    db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in range(1, 5)])
    pids = [r["id"] for r in db.get_papers_by_status("INGESTED")]
    for pid in pids:
        seed_eligibility(db._conn, pid)
        write_parsed(db, pid, f"Paper {pid}.\n\nText.\n", version=1)
    db._conn.commit()
    db_path = Path(db.db_path)
    shutil.copy2(CODEBOOK, db_path.parent / "extraction_codebook.yaml")
    spec0 = load_review_spec(SPEC_PATH)
    arms = {p: next(a.name for a in spec0.arms if a.provider == p) for p in PROVIDERS}
    text = SPEC_PATH.read_text()
    assert text.count("enabled_arms: []") == 1
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arms['openai']}, {arms['anthropic']}]"))
    handle = rm.open_run(db._conn, load_review_spec(spec_path), kind="extraction", stages=(),
                         codebook=load_codebook(db_path.parent / "extraction_codebook.yaml"),
                         cloud_arms=list(arms.values()), payload_description="test")
    db.close()
    inst = {p: TRANSPORT[p](str(db_path), str(spec_path), arms[p]) for p in PROVIDERS}
    for i in inst.values():
        i.run_id = handle.run_id
    yield inst, db_path, pids, sdk, spec_path
    for i in inst.values():
        i.close()


def _rows(db_path):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT request_hash, response_digest, outcome, outcome_detail FROM run_calls ORDER BY call_id").fetchall()
    finally:
        conn.close()


def _no_client(sdk):
    assert sdk["constructed"] == []


@pytest.mark.parametrize("provider", PROVIDERS)
def test_a_cloud_send_that_raises_still_records_its_call(world, provider):
    """(1), the entry's test (C24, Wave 2 row 28; R800(b)): inside an open run, a provider call that raises leaves one run_calls row,
    outcome error, its request hash the paper's chain's last hash, its detail `Class: message`; the same exception reaches the caller."""
    inst, db_path, pids, sdk, _ = world
    i = inst[provider]
    exc = (openai if provider == "openai" else anthropic).APIConnectionError(request=REQ)
    i.next = exc
    text = i.load_parsed_text(pids[0])
    with pytest.raises(type(exc)) as got:
        i.extract_paper(pids[0], text)
    assert got.value is exc
    rows = _rows(db_path)
    assert len(rows) == 1
    request_hash, digest, outcome, detail = rows[0]
    assert outcome == "error" and digest is None
    assert request_hash == i._request_chain[1][-1] == i.last_request_hash
    assert detail == f"{type(exc).__name__}: {exc}"
    _no_client(sdk)


FORMS = [
    ("openai", "truncated", "truncated: finish_reason=length"),
    ("openai", "content_filter", "refused: finish_reason=content_filter"),
    ("openai", "refusal_text_only", "refused: finish_reason=stop; refusal text present"),
    ("anthropic", "truncated", "truncated: stop_reason=max_tokens"),
    ("anthropic", "refusal", "refused: stop_reason=refusal"),
]


@pytest.mark.parametrize("provider,form,detail", FORMS, ids=[f"{p}-{f}" for p, f, _ in FORMS])
def test_a_truncated_or_refused_response_is_an_error_row_with_its_detail_and_a_digest(world, provider, form, detail):
    """(2), R800(c), (d): a truncated or refused response is recorded error with the detail `truncated: <field>=<value>` /
    `refused: <field>=<value>`, the refusal-text-only form ending `; refusal text present`; the refusal text is not stored; the row
    carries rm.response_digest of the response's text and thinking."""
    inst, db_path, pids, sdk, _ = world
    i = inst[provider]
    resp = {("openai", "truncated"): lambda: oa(content='{"fields": [', finish="length"),
            ("openai", "content_filter"): lambda: oa(content=None, finish="content_filter"),
            ("openai", "refusal_text_only"): lambda: oa(content=None, finish="stop", refusal="I can't help with that."),
            ("anthropic", "truncated"): lambda: an(text='{"fields": [', stop="max_tokens"),
            ("anthropic", "refusal"): lambda: an(text="", stop="refusal")}[(provider, form)]()
    i.next = resp
    i.extract_paper(pids[1], i.load_parsed_text(pids[1]))
    rows = _rows(db_path)
    assert len(rows) == 1
    request_hash, digest, outcome, got = rows[0]
    assert outcome == "error" and got == detail
    assert "help with that" not in got
    assert digest == rm.response_digest(*text_and_thinking(provider, resp))
    _no_client(sdk)


@pytest.mark.parametrize("provider", PROVIDERS)
def test_a_complete_response_is_completed_with_a_content_digest(world, provider):
    """(3), R800(c), (d): a complete response is recorded completed, its digest rm.response_digest of its text and thinking; two
    responses identical in content and differing in id and usage have equal digests."""
    inst, db_path, pids, sdk, _ = world
    i = inst[provider]
    a, b = complete(provider, rid="r-1", tokens=100), complete(provider, rid="r-2", tokens=101)
    text = i.load_parsed_text(pids[2])
    i.next = a
    i.extract_paper(pids[2], text)
    i.next = b
    i.extract_paper(pids[2], text)
    rows = _rows(db_path)
    assert [r[2] for r in rows] == ["completed", "completed"] and [r[3] for r in rows] == [None, None]
    assert rows[0][1] == rows[1][1] == rm.response_digest(*text_and_thinking(provider, a))
    _no_client(sdk)


@pytest.mark.parametrize("provider", PROVIDERS)
def test_every_hash_in_a_stored_claims_chain_has_a_run_calls_row(world, provider):
    """(4), R800(b) with U40's chain (R798(d)(ii)): a raise, then a success, then a store; every hash in the stored claims'
    context_chain names a run_calls row."""
    inst, db_path, pids, sdk, _ = world
    i = inst[provider]
    pid = pids[3]
    text = i.load_parsed_text(pid)
    i.next = (openai if provider == "openai" else anthropic).APIConnectionError(request=REQ)
    with pytest.raises(Exception):
        i.extract_paper(pid, text)
    i.next = complete(provider)
    r = i.extract_paper(pid, text)
    i.store_result(paper_id=pid, arm=i.arm_name, model_string=i.model_string, extracted_data=r["extracted_data"],
                   reasoning_trace=r["reasoning_trace"], prompt_text=r["prompt_text"], input_tokens=r["input_tokens"],
                   output_tokens=r["output_tokens"], reasoning_tokens=r["reasoning_tokens"], cost_usd=r["cost_usd"], spans=r["spans"])
    conn = sqlite3.connect(db_path)
    try:
        chains = {tuple(json.loads(p)["context_chain"]) for (p,) in conn.execute(
            "SELECT payload_json FROM field_events WHERE arm = ? AND paper_id = ?", (i.arm_name, pid))}
        recorded = Counter(h for (h,) in conn.execute("SELECT request_hash FROM run_calls"))
    finally:
        conn.close()
    assert len(chains) == 1
    (chain,) = chains
    assert len(chain) == 2
    # The two sends carry the same payload, so the same hash: each occurrence in the chain needs its own row.
    assert all(recorded[h] >= n for h, n in Counter(chain).items()), (Counter(chain), recorded)
    _no_client(sdk)


@pytest.mark.parametrize("provider", PROVIDERS)
def test_each_sdk_client_is_constructed_with_max_retries_zero(world, provider):
    """(5), R800(f), N47: the transport constructs its SDK client with max_retries=0 (the recorder records the keywords and builds
    nothing), so one recorded call is one HTTP send."""
    inst, db_path, pids, sdk, spec_path = world
    sdk["refuse"] = False
    cls = OpenAIExtractor if provider == "openai" else AnthropicExtractor
    ex = cls(str(db_path), str(spec_path), api_key="placeholder-not-a-credential", arm_name=inst[provider].arm_name)
    ex.close()
    assert sdk["constructed"] == [(provider, {"api_key": "placeholder-not-a-credential", "max_retries": 0})]


@pytest.mark.parametrize("provider", PROVIDERS)
def test_a_recorder_fault_during_a_provider_exception_surfaces_as_the_run_fault(world, provider, monkeypatch):
    """(6), R800(b): when the recorder itself raises while a provider exception is in flight, the recorder's sqlite3 error leaves send
    — a run fault — with the provider's exception as its context."""
    inst, db_path, pids, sdk, _ = world
    i = inst[provider]
    provider_exc = (openai if provider == "openai" else anthropic).APIConnectionError(request=REQ)
    fault = sqlite3.OperationalError("database is locked")

    def _failing(*a, **k):
        raise fault
    monkeypatch.setattr(rm, "record_call", _failing)
    i.next = provider_exc
    with pytest.raises(sqlite3.OperationalError) as got:
        i.extract_paper(pids[0], i.load_parsed_text(pids[0]))
    assert got.value is fault and is_run_fault(got.value)
    assert got.value.__context__ is provider_exc
    _no_client(sdk)


@pytest.mark.parametrize("provider", PROVIDERS)
def test_outside_a_run_nothing_is_recorded_and_the_exception_passes(world, provider):
    """(7), R800(b), preservation: with no run on the instance, a raising provider call writes no row and its exception passes."""
    inst, db_path, pids, sdk, _ = world
    i = inst[provider]
    i.run_id = None
    exc = (openai if provider == "openai" else anthropic).APIConnectionError(request=REQ)
    i.next = exc
    with pytest.raises(type(exc)) as got:
        i.extract_paper(pids[0], i.load_parsed_text(pids[0]))
    assert got.value is exc
    assert _rows(db_path) == []
    _no_client(sdk)


@pytest.mark.parametrize("provider", PROVIDERS)
def test_an_unreadable_response_is_an_error_row_and_raises(world, provider):
    """(8), R800(e): a returned object the transport's accessor cannot read is recorded error with its class and message, and send
    raises — it is never recorded completed."""
    inst, db_path, pids, sdk, _ = world
    i = inst[provider]
    i.next = "response"
    with pytest.raises(AttributeError) as got:
        i.send(pids[0], "prompt text")
    rows = _rows(db_path)
    assert len(rows) == 1 and rows[0][2] == "error" and rows[0][1] is None
    assert rows[0][3] == f"AttributeError: {got.value}"
    _no_client(sdk)
