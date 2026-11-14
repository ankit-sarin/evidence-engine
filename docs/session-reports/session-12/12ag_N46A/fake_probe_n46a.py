"""12ag-N46A — FAKE probe for N46's Phase A read-out, in 12af_U30A/fake_probe_u30a.py's form and seams.
At HEAD, `scripts/run_cloud_extraction.run_arm` itself drives one cloud store per case on a fresh scratch review:
  (A) a complete expected set plus one span named `invented_field` (not in the codebook), cited: a non-sentinel value and a
      snippet found in the paper's text — the brief's E2 case;
  (B) the same, but the extra span uncited: a non-sentinel value with an empty snippet;
  (C) the control: the complete expected set alone.
For each: run_arm's propagation; the run's end_status / end_reason; for paper 1 the rows in cloud_extractions,
cloud_evidence_spans (with whether `invented_field` is among them), field_events (with their distinct field names and whether
any is outside the codebook) and claim_inputs; extracted_data's top-level keys and the field names it carries; the
WARNING-and-above lines logged by engine.core.completeness, engine.core.citation_guard and engine.cloud.openai_extractor
(with the exception's class and message when one is attached); run_calls; whether get_pending_papers still
lists the paper. Then three take-every-row readers of cloud_evidence_spans, driven on (A)'s database, read-only:
analysis/provenance/census.load_spans (spans by field name for paper 1, and field_class('invented_field')),
analysis/paper1/judge_provenance.unjoined_summary's cloud count query (as written there), and
analysis/paper1/spanloss_autopsy.load_extractions (spans_stored for paper 1).
Seams, as 12af_U30A's: a fresh scratch ReviewDatabase per case under a temp directory (two papers, each with an eligibility
event and one recorded parsed text); the tracked codebook beside it; a spec copy whose `enabled_arms: []` names the openai arm;
rm.git_state patched clean; `cli.EXTRACTORS["openai"]` a thin subclass of the real transport passing a placeholder key; the
transport module's `openai.OpenAI` a fake whose create returns a complete response in the SDK's shape (`ChatCompletion`);
`time.sleep` in the transport a no-op recorder; httpx.Client.send and socket.socket.connect refusing recorders. No server, no
provider, no key, no network, no live database.
usage, from the repository root: .venv/bin/python fake_probe_n46a.py <tmp dir>"""
import json, logging, shutil, socket, sqlite3, sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path.cwd())); sys.path.insert(0, str(Path.cwd() / "tests"))
import httpx, openai
from openai.types.chat import ChatCompletion
from engine.cloud import openai_extractor as oe
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from scripts import run_cloud_extraction as cli
from _event_store_fixture import seed_eligibility
from _parsed_text_fixture import write_parsed

TMP = Path(sys.argv[1]); TMP.mkdir(parents=True, exist_ok=True)
SPEC_PATH = Path("review_specs/surgical_autonomy.yaml"); CODEBOOK = Path("data/surgical_autonomy/extraction_codebook.yaml")
CBK = load_codebook(CODEBOOK); NAMES = set(CBK.field_names)
REC = {"constructors": [], "creates": 0, "sleeps": [], "httpx_send": 0, "socket_connect": 0}
BASE = [{"field_name": f["name"], "value": "Original Research" if f["name"] == "study_type" else "NR",
         "source_snippet": "Snippet for study_type." if f["name"] == "study_type" else "", "confidence": 0.9, "tier": f["tier"]}
        for tier in (1, 2, 3, 4) for f in CBK.fields_by_tier(tier)]
EXTRA = {"A": {"field_name": "invented_field", "value": "An invented value", "source_snippet": "Snippet for study_type.",
               "confidence": 0.9, "tier": 1},
         "B": {"field_name": "invented_field", "value": "An invented value", "source_snippet": "", "confidence": 0.9, "tier": 1},
         "C": None}
assert "invented_field" not in NAMES


class Catch(logging.Handler):
    def __init__(self): super().__init__(logging.WARNING); self.lines = []
    def emit(self, r):
        exc = f" [{r.exc_info[0].__name__}: {str(r.exc_info[1])[:160]}]" if r.exc_info else ""
        self.lines.append(f"{r.name} {r.levelname}: {r.getMessage()[:200]}{exc}")


CATCH = Catch()
for name in ("engine.core.completeness", "engine.core.citation_guard", "engine.cloud.openai_extractor"):
    lg = logging.getLogger(name); lg.addHandler(CATCH); lg.setLevel(logging.WARNING)
logging.getLogger().setLevel(logging.CRITICAL)


def make_case(name, content):
    d = TMP / name; d.mkdir()
    db = ReviewDatabase("n46aprobe", data_root=d)
    db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in (1, 2)])
    pids = [r["id"] for r in db.get_papers_by_status("INGESTED")]
    for pid in pids:
        seed_eligibility(db._conn, pid); write_parsed(db, pid, f"Paper {pid}.\n\nSnippet for study_type.\n", version=1)
    db._conn.commit(); db_path = Path(db.db_path); db.close()
    shutil.copy2(CODEBOOK, db_path.parent / "extraction_codebook.yaml")
    arm = next(a.name for a in load_review_spec(SPEC_PATH).arms if a.provider == "openai")
    text = SPEC_PATH.read_text(); assert text.count("enabled_arms: []") == 1
    spec_path = d / "spec.yaml"; spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arm}]"))

    def create(**payload):
        REC["creates"] += 1
        return ChatCompletion(**{"id": "c", "object": "chat.completion", "created": 0, "model": payload["model"],
                                 "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": content}}],
                                 "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})

    class FakeOpenAI:
        def __init__(self, **kw):
            REC["constructors"].append(kw)
            self.chat = type("C", (), {"completions": type("CC", (), {"create": staticmethod(create)})()})()

    class Probe(oe.OpenAIExtractor):
        def __init__(self, *a, **k):
            k["api_key"] = "placeholder-not-a-credential"
            super().__init__(*a, **k)

    return arm, db_path, spec_path, pids, FakeOpenAI, Probe


def state(db_path, arm, pid):
    c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        q = lambda s, *a: c.execute(s, a).fetchone()[0]
        span_names = [r[0] for r in c.execute("SELECT s.field_name FROM cloud_evidence_spans s JOIN cloud_extractions e ON e.id = "
                                              "s.cloud_extraction_id WHERE e.arm = ? AND e.paper_id = ?", (arm, pid))]
        ev_names = sorted({r[0] for r in c.execute("SELECT field_name FROM field_events WHERE arm = ? AND paper_id = ?", (arm, pid))})
        ed = c.execute("SELECT extracted_data FROM cloud_extractions WHERE arm = ? AND paper_id = ?", (arm, pid)).fetchone()
        ed = json.loads(ed[0]) if ed else None
        return {"cloud_extractions": q("SELECT COUNT(*) FROM cloud_extractions WHERE arm = ? AND paper_id = ?", arm, pid),
                "spans": len(span_names), "invented_in_spans": span_names.count("invented_field"),
                "field_events": q("SELECT COUNT(*) FROM field_events WHERE arm = ? AND paper_id = ?", arm, pid),
                "event_names": len(ev_names), "event_names_outside_codebook": [n for n in ev_names if n not in NAMES],
                "claim_inputs": q("SELECT COUNT(*) FROM claim_inputs WHERE arm = ? AND paper_id = ?", arm, pid),
                "extracted_data_keys": sorted(ed) if isinstance(ed, dict) else type(ed).__name__,
                "extracted_data_fields": len(ed["fields"]) if isinstance(ed, dict) and "fields" in ed else None,
                "invented_in_extracted_data": ("invented_field" in [f.get("field_name") for f in ed["fields"]]) if isinstance(ed, dict) and "fields" in ed else None,
                "run_calls": [tuple(r) for r in c.execute("SELECT outcome, outcome_detail, response_digest IS NOT NULL FROM run_calls "
                                                           "WHERE paper_id = ? ORDER BY call_id", (pid,))],
                "manifest": [tuple(r) for r in c.execute("SELECT run_id, end_status, end_reason, ended_at IS NOT NULL FROM run_manifests "
                                                          "WHERE run_kind = 'extraction' ORDER BY run_id")]}
    finally:
        c.close()


def pending(db_path, spec_path, arm):
    with patch.object(oe.openai, "OpenAI", lambda **k: None):
        base = cli.CloudExtractorBase(str(db_path), str(spec_path))
    try:
        return [p["paper_id"] for p in base.get_pending_papers(arm)]
    finally:
        base.close()


def refuse_send(self, *a, **k):
    REC["httpx_send"] += 1; raise RuntimeError("no network")


def refuse_connect(self, *a, **k):
    REC["socket_connect"] += 1; raise RuntimeError("no network")


DBS = {}
with patch.object(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None)), \
     patch.object(httpx.Client, "send", refuse_send), patch.object(socket.socket, "connect", refuse_connect), \
     patch.object(oe.time, "sleep", lambda s: REC["sleeps"].append(s)):
    for key, label in (("A", "one unexpected span, cited"), ("B", "one unexpected span, uncited"), ("C", "control: no unexpected span")):
        content = json.dumps({"fields": BASE + ([EXTRA[key]] if EXTRA[key] else [])})
        arm, db_path, spec_path, pids, FakeOpenAI, Probe = make_case(key, content)
        CATCH.lines.clear()
        with patch.object(oe.openai, "OpenAI", FakeOpenAI), patch.dict(cli.EXTRACTORS, {"openai": Probe}):
            try:
                cli.run_arm(arm, str(db_path), str(spec_path), None, None)
                left = "returned"
            except BaseException as e:   # noqa: BLE001 — the probe reports what left run_arm
                left = f"{type(e).__name__} propagated"
        p0 = pids[0]; st = state(db_path, arm, p0); pend = pending(db_path, spec_path, arm); DBS[key] = (db_path, arm, p0)
        logs = [l for l in CATCH.lines if f"Paper {p0} " in l]
        print(f"=== ({key}) {label}\n  run_arm: {left}\n  manifest (run_id, end_status, end_reason, ended): {st['manifest']}"
              f"\n  paper {p0}: cloud_extractions {st['cloud_extractions']}; cloud_evidence_spans {st['spans']} (invented_field {st['invented_in_spans']});"
              f" field_events {st['field_events']} over {st['event_names']} names, outside the codebook {st['event_names_outside_codebook']};"
              f" claim_inputs {st['claim_inputs']}\n  extracted_data: keys {st['extracted_data_keys']}, fields {st['extracted_data_fields']},"
              f" invented_field in it {st['invented_in_extracted_data']}\n  run_calls (outcome, detail, digest present): {st['run_calls']}"
              f"\n  logged for paper {p0}: {logs if logs else 'nothing'}\n  get_pending_papers: {pend} — paper {p0} pending: {p0 in pend}\n")

print("=== readers driven on (A)'s database (read-only)")
from analysis.provenance import census as pc
from analysis.provenance.field_class import field_class
from analysis.paper1 import spanloss_autopsy as sa
db_path, arm, p0 = DBS["A"]
sp = [s for s in pc.load_spans(db_path).get(p0, []) if s["arm"] == arm]
print(f"  census.load_spans: paper {p0} {arm}: {len(sp)} spans; invented_field among them {sum(s['field_name'] == 'invented_field' for s in sp)};"
      f" field_class('invented_field') = {field_class('invented_field')!r}")
c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
cnt = {(r[0], r[1]): r[2] for r in c.execute("SELECT c.arm, c.paper_id, COUNT(*) FROM cloud_evidence_spans s "
                                              "JOIN cloud_extractions c ON c.id = s.cloud_extraction_id GROUP BY 1,2")}
c.close()
print(f"  judge_provenance.unjoined_summary's cloud count query: ({arm!r}, {p0}) -> {cnt.get((arm, p0))} against {len(NAMES)} codebook fields")
rows = [r for r in sa.load_extractions(db_path) if r["arm"] == arm and r["paper_id"] == p0]
print(f"  spanloss_autopsy.load_extractions: paper {p0}: spans_stored {[r['spans_stored'] for r in rows]}, spans_in_response "
      f"{[r['spans_in_response'] for r in rows]}, verdict {[r['verdict'] for r in rows]}; EXPECTED_SPANS {sa.EXPECTED_SPANS}")
print("=== the record")
print(f"SDK constructions recorded (FakeOpenAI, no client): {len(REC['constructors'])}, keywords {REC['constructors'][:1]}; provider creates "
      f"{REC['creates']}; sleeps {REC['sleeps']}; httpx sends {REC['httpx_send']}; socket connects {REC['socket_connect']}; real "
      f"openai.OpenAI never constructed (patched in every case)")
