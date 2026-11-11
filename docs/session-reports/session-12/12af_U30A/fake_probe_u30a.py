"""12af-U30A — FAKE probe for U30's Phase A (INT-i12-71; R801(h)), on 12af_U40A/fake_probe_u40a.py's scratch setup and
tests/test_u27_cloud_run_faults.py's way of driving `scripts/run_cloud_extraction.run_arm` itself on a scratch database.
At HEAD, a KeyboardInterrupt raised in-process at three seams (no signal is sent to any process):
  (A) between store_result's extraction INSERT and its span INSERTs — THE ENTRY'S TEST AT HEAD (E1, E2);
  (B) inside the provider call (the SDK fake's create raises it);
  (C) inside the event writer's savepoint (on the first `INSERT INTO field_events`).
For each: run_arm's propagation, the run's end_status / end_reason / ended_at, the paper's rows in cloud_extractions,
cloud_evidence_spans, field_events, claim_inputs and run_calls, and whether get_pending_papers still lists it.
Seams: a fresh scratch ReviewDatabase per case under a temp directory (two papers, each with an eligibility event and one recorded parsed
text); the tracked codebook beside it; a spec copy whose `enabled_arms: []` names the openai arm; rm.git_state patched clean;
`cli.EXTRACTORS["openai"]` a thin subclass of the real transport passing a placeholder key (never reading the environment), whose
connection is wrapped so the named INSERT raises KeyboardInterrupt (the wrapper delegates everything else); the transport module's
`openai.OpenAI` replaced by a fake whose create returns a complete response in the SDK's shape (`ChatCompletion`), or raises
KeyboardInterrupt for (B); `time.sleep` in the transport a no-op recorder; httpx.Client.send and socket.socket.connect refusing recorders.
No server, no provider, no key, no network, no live database.
usage, from the repository root: .venv/bin/python fake_probe_u30a.py <tmp dir>"""
import json, logging, shutil, socket, sqlite3, sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path.cwd())); sys.path.insert(0, str(Path.cwd() / "tests"))
logging.disable(logging.CRITICAL)
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
CBK = load_codebook(CODEBOOK)
REC = {"constructors": [], "creates": 0, "sleeps": [], "httpx_send": 0, "socket_connect": 0}
CONTENT = json.dumps({"fields": [{"field_name": f["name"], "value": "Original Research" if f["name"] == "study_type" else "NR",
                                  "source_snippet": "Snippet for study_type." if f["name"] == "study_type" else "",
                                  "confidence": 0.9, "tier": f["tier"]} for tier in (1, 2, 3, 4) for f in CBK.fields_by_tier(tier)]})


class InterruptingConn:
    """Delegates everything to the real connection, but raises KeyboardInterrupt on the first statement starting with `trip`."""

    def __init__(self, real, trip):
        self.real, self.trip, self.tripped = real, trip, False

    def execute(self, sql, *a, **k):
        if self.trip and not self.tripped and sql.lstrip().upper().startswith(self.trip):
            self.tripped = True
            raise KeyboardInterrupt()
        return self.real.execute(sql, *a, **k)

    def __getattr__(self, name):
        return getattr(self.real, name)


def make_case(name, trip, provider_raises):
    d = TMP / name; d.mkdir()
    db = ReviewDatabase("u30aprobe", data_root=d)
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
        if provider_raises:
            raise KeyboardInterrupt()
        return ChatCompletion(**{"id": "c", "object": "chat.completion", "created": 0, "model": payload["model"],
                                 "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": CONTENT}}],
                                 "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})

    class FakeOpenAI:
        def __init__(self, **kw):
            REC["constructors"].append(kw)
            self.chat = type("C", (), {"completions": type("CC", (), {"create": staticmethod(create)})()})()

    class Probe(oe.OpenAIExtractor):
        def __init__(self, *a, **k):
            k["api_key"] = "placeholder-not-a-credential"
            super().__init__(*a, **k)
            if trip:
                self._real_conn = self._conn
                self._conn = InterruptingConn(self._conn, trip)

        def close(self):
            getattr(self, "_real_conn", self._conn).close()

    return arm, db_path, spec_path, pids, FakeOpenAI, Probe


def state(db_path, arm, pid):
    c = sqlite3.connect(db_path)
    try:
        q = lambda s, *a: c.execute(s, a).fetchone()[0]
        return {"cloud_extractions": q("SELECT COUNT(*) FROM cloud_extractions WHERE arm = ? AND paper_id = ?", arm, pid),
                "cloud_evidence_spans": q("SELECT COUNT(*) FROM cloud_evidence_spans s JOIN cloud_extractions e ON e.id = s.cloud_extraction_id "
                                          "WHERE e.arm = ? AND e.paper_id = ?", arm, pid),
                "field_events": q("SELECT COUNT(*) FROM field_events WHERE arm = ? AND paper_id = ?", arm, pid),
                "claim_inputs": q("SELECT COUNT(*) FROM claim_inputs WHERE arm = ? AND paper_id = ?", arm, pid),
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


CASES = [("A", "between store_result's extraction INSERT and its span INSERTs", "INSERT INTO CLOUD_EVIDENCE_SPANS", False),
         ("B", "inside the provider call", None, True),
         ("C", "inside the event writer's savepoint (first INSERT INTO field_events)", "INSERT INTO FIELD_EVENTS", False)]
with patch.object(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None)), \
     patch.object(httpx.Client, "send", refuse_send), patch.object(socket.socket, "connect", refuse_connect), \
     patch.object(oe.time, "sleep", lambda s: REC["sleeps"].append(s)):
    for key, label, trip, prov in CASES:
        arm, db_path, spec_path, pids, FakeOpenAI, Probe = make_case(key, trip, prov)
        with patch.object(oe.openai, "OpenAI", FakeOpenAI), patch.dict(cli.EXTRACTORS, {"openai": Probe}):
            try:
                cli.run_arm(arm, str(db_path), str(spec_path), None, None)
                left = "returned"
            except BaseException as e:   # noqa: BLE001 — the probe reports what left run_arm
                left = f"{type(e).__name__} propagated"
        p0 = pids[0]
        st = state(db_path, arm, p0)
        pend = pending(db_path, spec_path, arm)
        print(f"=== ({key}) KeyboardInterrupt {label}\n  run_arm: {left}\n  manifest (run_id, end_status, end_reason, ended): {st['manifest']}"
              f"\n  paper {p0}: cloud_extractions {st['cloud_extractions']}, cloud_evidence_spans {st['cloud_evidence_spans']}, field_events "
              f"{st['field_events']}, claim_inputs {st['claim_inputs']}\n  run_calls for paper {p0} (outcome, detail, digest present): "
              f"{st['run_calls']}\n  get_pending_papers({arm!r}): {pend} — paper {p0} pending: {p0 in pend}\n")
print("=== the record")
print(f"SDK constructions recorded (FakeOpenAI, no client): {len(REC['constructors'])}, keywords {REC['constructors'][:1]}; provider creates "
      f"{REC['creates']}; sleeps {REC['sleeps']}; httpx sends {REC['httpx_send']}; socket connects {REC['socket_connect']}; real "
      f"openai.OpenAI never constructed (patched in every case)")
