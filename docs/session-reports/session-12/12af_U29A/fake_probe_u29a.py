"""12af-U29A — FAKE probe for U29's Phase A (C24 for Wave 2 row 28; N47; R799(g)), on 12af_U40A/fake_probe_u40a.py's scratch setup.
At HEAD, inside an open cloud run, each transport's provider seam (`_create`) replaced at the instance:
  (1) THE ENTRY'S TEST AT HEAD — a _create that raises before anything is sent (ValueError, the class the Anthropic SDK raises for a
      non-streaming request it will not send) and one that raises after (openai.APITimeoutError / anthropic.APIConnectionError, built on
      an httpx.Request, no client): run_calls rows, the paper's U40 chain, last_request_hash;
  (2) a returned response with a truncated finish reason (openai "length", anthropic "max_tokens") and one with a refusal (openai
      message.refusal with finish_reason "content_filter", anthropic stop_reason "refusal"): the run_calls row each leaves, and the
      finish_reason extract_paper returns;
  (3) a complete response: its run_calls row's response_digest; and what a digest over each SDK's response object would vary on — two
      answers identical in content, differing in id / created / usage, hashed whole (to_json) and by content only.
Seams: a scratch ReviewDatabase under a temp directory (4 papers, each with an eligibility event and one recorded parsed text); the
tracked codebook copied beside it; a spec copy whose `enabled_arms: []` names both cloud arms; rm.git_state patched clean; the cloud run
opened by rm.open_run(… cloud_arms=[both] …) as scripts/run_cloud_extraction.run_arm opens it. Each instance is the real transport class
with only CloudExtractorBase.__init__ run (no SDK client) and `_create` replaced. openai.OpenAI and anthropic.Anthropic are replaced by
refusers that count (expected 0). Response objects are the SDKs' own pydantic types (ChatCompletion, Message), built from dicts — no
client. No server, no provider, no key, no network, no live database.
usage, from the repository root: .venv/bin/python fake_probe_u29a.py <tmp dir>"""
import json, logging, shutil, sqlite3, sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path.cwd())); sys.path.insert(0, str(Path.cwd() / "tests"))
logging.disable(logging.CRITICAL)
import anthropic, httpx, openai
from anthropic.types import Message
from openai.types.chat import ChatCompletion
from engine.cloud.anthropic_extractor import AnthropicExtractor
from engine.cloud.base import CloudExtractorBase
from engine.cloud.openai_extractor import OpenAIExtractor
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.effective_config import sha256_canonical
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from _event_store_fixture import seed_eligibility
from _parsed_text_fixture import write_parsed

TMP = Path(sys.argv[1]); TMP.mkdir(parents=True, exist_ok=True)
SPEC_PATH = Path("review_specs/surgical_autonomy.yaml"); CODEBOOK = Path("data/surgical_autonomy/extraction_codebook.yaml")
CBK = load_codebook(CODEBOOK)
SDK = {"openai.OpenAI": 0, "anthropic.Anthropic": 0}
REQ = httpx.Request("POST", "https://example.invalid/v1")
FIELDS = json.dumps({"fields": [{"field_name": f["name"], "value": "NR", "source_snippet": "", "confidence": 0.9, "tier": f["tier"]}
                                for tier in (1, 2, 3, 4) for f in CBK.fields_by_tier(tier)]})


class _O:
    def __init__(self, *a, **k): SDK["openai.OpenAI"] += 1; raise RuntimeError("probe: no SDK client")
class _A:
    def __init__(self, *a, **k): SDK["anthropic.Anthropic"] += 1; raise RuntimeError("probe: no SDK client")


def oa_resp(*, content=FIELDS, finish="stop", refusal=None, rid="chatcmpl-1", created=1700000000, tokens=100):
    return ChatCompletion(**{"id": rid, "object": "chat.completion", "created": created, "model": "o4-mini-2025-04-16",
                             "choices": [{"index": 0, "finish_reason": finish,
                                          "message": {"role": "assistant", "content": content, "refusal": refusal}}],
                             "usage": {"prompt_tokens": tokens, "completion_tokens": 50, "total_tokens": tokens + 50}})


def an_resp(*, text=FIELDS, stop="end_turn", rid="msg_1", tokens=100):
    return Message(**{"id": rid, "type": "message", "role": "assistant", "model": "claude-sonnet-4-6",
                      "content": [{"type": "thinking", "thinking": "t", "signature": "sig"}, {"type": "text", "text": text}],
                      "stop_reason": stop, "stop_sequence": None, "usage": {"input_tokens": tokens, "output_tokens": 50}})


def transport(cls):
    class T(cls):
        def __init__(self, db_path, spec_path, arm_name):
            CloudExtractorBase.__init__(self, db_path, spec_path, arm_name=arm_name)
            self.next = None
        def _create(self, **payload):
            n = self.next
            if isinstance(n, BaseException): raise n
            return n
    T.__name__ = f"Fake{cls.__name__}"
    return T


def calls(conn, run_id, since):
    return [tuple(r) for r in conn.execute(
        "SELECT call_id, stage, paper_id, substr(request_hash, 1, 12), response_digest, outcome, outcome_detail FROM run_calls "
        "WHERE run_id = ? AND call_id > ? ORDER BY call_id", (run_id, since))]


with patch.object(openai, "OpenAI", _O), patch.object(anthropic, "Anthropic", _A), \
     patch.object(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None)):
    d = TMP / "root"; d.mkdir()
    db = ReviewDatabase("u29aprobe", data_root=d)
    db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in range(1, 5)])
    pids = [r["id"] for r in db.get_papers_by_status("INGESTED")]
    for pid in pids:
        seed_eligibility(db._conn, pid); write_parsed(db, pid, f"Paper {pid}.\n\nText.\n", version=1)
    db._conn.commit()
    root = Path(db.db_path).parent; db_path = str(db.db_path); shutil.copy2(CODEBOOK, root / "extraction_codebook.yaml")
    spec0 = load_review_spec(SPEC_PATH)
    oarm = next(a.name for a in spec0.arms if a.provider == "openai"); aarm = next(a.name for a in spec0.arms if a.provider == "anthropic")
    text = SPEC_PATH.read_text(); assert text.count("enabled_arms: []") == 1
    sp = root / "spec.yaml"; sp.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{oarm}, {aarm}]"))
    spec = load_review_spec(sp); cb = load_codebook(root / "extraction_codebook.yaml")
    run_id = rm.open_run(db._conn, spec, kind="extraction", stages=(), codebook=cb, cloud_arms=[oarm, aarm],
                         payload_description="probe").run_id
    conn = db._conn
    print(f"=== setup: cloud run {run_id}; arms {oarm}, {aarm}; run_stage_configs "
          f"{[tuple(r) for r in conn.execute('SELECT stage, provider, model_name FROM run_stage_configs WHERE run_id = ?', (run_id,))]}")
    O = transport(OpenAIExtractor)(db_path, str(sp), oarm); O.run_id = run_id
    A = transport(AnthropicExtractor)(db_path, str(sp), aarm); A.run_id = run_id
    print(f"  instances: {type(O).__name__} (PROVIDER {O.PROVIDER!r}, stage {O.stage_cfg.stage}), "
          f"{type(A).__name__} (PROVIDER {A.PROVIDER!r}, stage {A.stage_cfg.stage}); no SDK client attribute: "
          f"{not hasattr(O, 'client') and not hasattr(A, 'client')}")
    p1, p2, p3, p4 = pids

    def case(label, inst, pid, nxt):
        last = conn.execute("SELECT COALESCE(MAX(call_id), 0) FROM run_calls").fetchone()[0]
        inst.next = nxt
        text = inst.load_parsed_text(pid)
        try:
            r = inst.extract_paper(pid, text)
            got = f"returned; extract_paper finish_reason={r['finish_reason']!r}; spans {len(r['spans'])}"
        except BaseException as e:   # noqa: BLE001 — the probe reports the class
            got = f"RAISED {type(e).__name__}: {str(e)[:90]}"
        ch = inst._request_chain
        rows = calls(conn, run_id, last)
        print(f"  {label}\n    {got}\n    run_calls rows: {rows}\n    chain: paper {ch[0]}, {len(ch[1])} hash(es) "
              f"{[h[:12] for h in ch[1]]}; last_request_hash {inst.last_request_hash[:12]}")
        return rows

    print("\n=== (1) the entry's test at HEAD — a _create that raises")
    case("openai, raises before transmission (ValueError)", O, p1, ValueError("Streaming is required for operations that may take longer than 10 minutes."))
    case("openai, raises after transmission (openai.APITimeoutError)", O, p1, openai.APITimeoutError(request=REQ))
    case("anthropic, raises after transmission (anthropic.APIConnectionError)", A, p2, anthropic.APIConnectionError(request=REQ))
    print(f"  the papers' chains after the raises: openai paper {p1}: {len(O._request_chain[1])} hashes; anthropic paper {p2}: "
          f"{len(A._request_chain[1])} hash; run_calls rows for the run so far: "
          f"{conn.execute('SELECT COUNT(*) FROM run_calls WHERE run_id = ?', (run_id,)).fetchone()[0]}")

    print("\n=== (2) truncated and refused responses")
    case("openai, finish_reason 'length' (truncated)", O, p3, oa_resp(finish="length", content=FIELDS[:200]))
    case("openai, refusal (message.refusal set, content None, finish_reason 'content_filter')", O, p3,
         oa_resp(finish="content_filter", content=None, refusal="I can't help with that."))
    case("anthropic, stop_reason 'max_tokens' (truncated)", A, p4, an_resp(stop="max_tokens", text=FIELDS[:200]))
    case("anthropic, stop_reason 'refusal'", A, p4, an_resp(stop="refusal", text=""))

    print("\n=== (3) a complete response, and what a digest over it would vary on")
    rows_o = case("openai, complete (finish_reason 'stop')", O, p1, oa_resp())
    rows_a = case("anthropic, complete (stop_reason 'end_turn')", A, p2, an_resp())
    print(f"  response_digest on the two complete rows: {[r[4] for r in rows_o + rows_a]}")
    a1, a2 = oa_resp(rid="chatcmpl-1", created=1700000000, tokens=100), oa_resp(rid="chatcmpl-2", created=1700000099, tokens=101)
    b1, b2 = an_resp(rid="msg_1", tokens=100), an_resp(rid="msg_2", tokens=101)
    print(f"  openai ChatCompletion, two answers identical in content (id, created, usage differ): "
          f"to_json() sha256 equal {sha256_canonical(json.loads(a1.to_json())) == sha256_canonical(json.loads(a2.to_json()))}; "
          f"rm.response_digest(message.content) equal {rm.response_digest(a1.choices[0].message.content) == rm.response_digest(a2.choices[0].message.content)}; "
          f"fields that differ: {sorted(k for k in a1.to_dict() if a1.to_dict()[k] != a2.to_dict()[k])}")
    print(f"  anthropic Message, two answers identical in content (id, usage differ): "
          f"to_json() sha256 equal {sha256_canonical(json.loads(b1.to_json())) == sha256_canonical(json.loads(b2.to_json()))}; "
          f"content blocks' model_dump equal {[c.model_dump() for c in b1.content] == [c.model_dump() for c in b2.content]}; "
          f"fields that differ: {sorted(k for k in b1.to_dict() if b1.to_dict()[k] != b2.to_dict()[k])}")
    print(f"\n=== totals: run_calls rows for the run: {[tuple(r) for r in conn.execute('SELECT outcome, COUNT(*), SUM(response_digest IS NULL) FROM run_calls WHERE run_id = ? GROUP BY outcome', (run_id,))]}")
    O.close(); A.close(); db.close()

print("\n=== the record")
print(f"SDK client constructions (openai.OpenAI / anthropic.Anthropic): {SDK}")
