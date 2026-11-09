"""12ae-U27A — FAKE probe for U27 (C56's cloud handlers). For each U27 handler (I2's mapping: triage 8 → openai_extractor.py:179,
9 → anthropic_extractor.py:205, 10 → openai_extractor.py:209, 11 → anthropic_extractor.py:237), drive one exception into it through
scripts/run_cloud_extraction.run_arm — the one entry point — on a scratch ReviewDatabase under a temp directory, and print what the papers,
run_calls, cloud_extractions, the arm's stats and the manifest record.

The seams are tests/test_failed_close_reason.py's cloud fixture (a scratch ReviewDatabase, the live codebook copied beside it — read, never
written — a spec copy whose `enabled_arms: []` names the one arm under test, rm.git_state patched clean) and tests/test_cloud_extraction.py's
SDK patch (`engine.cloud.openai_extractor.openai.OpenAI`, `engine.cloud.anthropic_extractor.anthropic.Anthropic`). Here the SDK classes are
replaced by recording fakes: the constructor's arguments and every create(**payload) call are recorded, and the fake answers a fixed complete
response built from the codebook. No provider is reached: besides the fakes, httpx.Client.send, httpx.AsyncClient.send and socket.connect
are replaced by recorders that refuse, and their counts are printed. The api_key passed is a literal placeholder, so neither
OPENAI_API_KEY nor ANTHROPIC_API_KEY is read (`key = api_key or os.environ.get(...)` short-circuits). time.sleep in both transports is
replaced by a recorder (the retry loop's waits are printed, not slept).

Faults:
  8 / 9   the recorder: engine.core.run_manifest.record_call raises (send() calls it after _create returned, under a run).
  10 / 11 the store: the extractor's connection refuses `INSERT INTO cloud_extractions` (store_result's INSERT; base.py:401 rolls back and
          re-raises into the handler).
  Exceptions: sqlite3.OperationalError("database is locked") (a run_fault_classes() member), KeyError (not one).
Extra cases: the provider raising openai.APIConnectionError / anthropic.APIConnectionError (what the SDK raises for a refused connection),
and a bare ConnectionError (a tuple member) raised by create; the same recorder and store faults outside a run (extractor.run called
directly, run_id None).
usage, from the repository root: .venv/bin/python fake_probe_u27a.py <tmp dir>   (stdout: one block per case, then the call record)"""
import contextlib, io, json, logging, shutil, socket, sqlite3, sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
sys.path.insert(0, str(Path.cwd())); sys.path.insert(0, str(Path.cwd() / "tests"))
logging.disable(logging.CRITICAL)
import httpx, openai, anthropic
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.extraction_events import is_run_fault
from engine.core.review_spec import load_review_spec
from scripts import run_cloud_extraction as cli
from _event_store_fixture import seed_eligibility
from _parsed_text_fixture import write_parsed

TMP = Path(sys.argv[1]); TMP.mkdir(parents=True, exist_ok=True)
SPEC_PATH = Path("review_specs/surgical_autonomy.yaml")
LIVE_CODEBOOK = Path("data/surgical_autonomy/extraction_codebook.yaml")
PLACEHOLDER_KEY = "fake-key-not-a-credential"
CBK = load_codebook(LIVE_CODEBOOK)
FIELDS = [{"field_name": f["name"], "value": "Original Research" if f["name"] == "study_type" else "NR",
           "source_snippet": f"Snippet for {f['name']}.", "confidence": 0.9, "tier": f["tier"]}
          for tier in (1, 2, 3, 4) for f in CBK.fields_by_tier(tier)]
CONTENT = json.dumps({"fields": FIELDS})

RECORD = {"constructors": [], "creates": [], "httpx_send": 0, "socket_connect": 0, "sleeps": []}
PROVIDER_EXC = {"value": None}


class _Completions:
    def create(self, **payload):
        RECORD["creates"].append(("openai", payload.get("model"), len(json.dumps(payload, default=str))))
        if PROVIDER_EXC["value"] is not None: raise PROVIDER_EXC["value"]
        usage = SimpleNamespace(prompt_tokens=100, completion_tokens=50, completion_tokens_details=SimpleNamespace(reasoning_tokens=10))
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=CONTENT, reasoning_content="trace"),
                                                        finish_reason="stop")], usage=usage)


class FakeOpenAI:
    def __init__(self, **kw):
        RECORD["constructors"].append(("openai.OpenAI", sorted(kw), kw.get("api_key") == PLACEHOLDER_KEY))
        self.chat = SimpleNamespace(completions=_Completions())


class _Messages:
    def create(self, **payload):
        RECORD["creates"].append(("anthropic", payload.get("model"), len(json.dumps(payload, default=str))))
        if PROVIDER_EXC["value"] is not None: raise PROVIDER_EXC["value"]
        return SimpleNamespace(content=[SimpleNamespace(type="thinking", thinking="trace"), SimpleNamespace(type="text", text=CONTENT)],
                               usage=SimpleNamespace(input_tokens=100, output_tokens=50), stop_reason="end_turn")


class FakeAnthropic:
    def __init__(self, **kw):
        RECORD["constructors"].append(("anthropic.Anthropic", sorted(kw), kw.get("api_key") == PLACEHOLDER_KEY))
        self.messages = _Messages()


def _refuse_send(self, *a, **k):
    RECORD["httpx_send"] += 1; raise RuntimeError("probe: no network")
def _refuse_connect(self, *a, **k):
    RECORD["socket_connect"] += 1; raise RuntimeError("probe: no network")


class FailingConn:
    """Delegates everything, but refuses store_result's INSERT INTO cloud_extractions (test_parse_gate.py's wrapper form)."""
    def __init__(self, real, exc): self.real, self.exc = real, exc
    def execute(self, sql, *a, **k):
        if "INSERT INTO cloud_extractions" in sql: raise self.exc
        return self.real.execute(sql, *a, **k)
    def __getattr__(self, n): return getattr(self.real, n)


def scratch(provider):
    d = TMP / f"case{len(list(TMP.iterdir()))}"; d.mkdir()
    db = ReviewDatabase("u27probe", data_root=d)
    from engine.search.models import Citation
    db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in (1, 2)])
    pids = [r["id"] for r in db.get_papers_by_status("INGESTED")]
    for pid in pids:
        seed_eligibility(db._conn, pid); write_parsed(db, pid, "Autonomous robotic suturing. " * 30)
    db_path = Path(db.db_path); shutil.copy2(LIVE_CODEBOOK, db_path.parent / "extraction_codebook.yaml"); db.close()
    arm = next(a.name for a in load_review_spec(SPEC_PATH).arms if a.provider == provider)
    text = SPEC_PATH.read_text(); assert text.count("enabled_arms: []") == 1
    spec_path = d / "spec.yaml"; spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arm}]"))
    return db_path, spec_path, arm, pids


def case(label, provider, fault, exc, *, under_run=True):
    db_path, spec_path, arm, pids = scratch(provider)
    n_creates0 = len(RECORD["creates"]); n_sleeps0 = len(RECORD["sleeps"])
    stash = {}
    base_cls = cli.EXTRACTORS[provider]

    class Probe(base_cls):
        def __init__(self, *a, **k):
            k["api_key"] = PLACEHOLDER_KEY
            super().__init__(*a, **k)
            if fault == "store":
                self._real_conn = self._conn; self._conn = FailingConn(self._conn, exc)
        def run(self, **kw):
            stash["stats"] = super().run(**kw); return stash["stats"]
        def close(self):
            getattr(self, "_real_conn", self._conn).close()

    PROVIDER_EXC["value"] = exc if fault == "provider" else None
    real_record_call = rm.record_call
    def failing_record_call(*a, **k): raise exc
    patches = [patch("engine.cloud.openai_extractor.openai.OpenAI", FakeOpenAI),
               patch("engine.cloud.anthropic_extractor.anthropic.Anthropic", FakeAnthropic),
               patch("engine.cloud.openai_extractor.time.sleep", lambda s: RECORD["sleeps"].append(s)),
               patch("engine.cloud.anthropic_extractor.time.sleep", lambda s: RECORD["sleeps"].append(s)),
               patch.object(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None)),
               patch.dict(cli.EXTRACTORS, {provider: Probe})]
    if fault == "recorder": patches.append(patch.object(rm, "record_call", failing_record_call))
    propagated = None
    for p in patches: p.start()
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):   # the ProgressReporter's lines
            if under_run:
                cli.run_arm(arm, str(db_path), str(spec_path), None, None)
            else:
                ex = Probe(str(db_path), str(spec_path), arm_name=arm)
                try: ex.run()
                finally: ex.close()
    except BaseException as e:
        propagated = f"{type(e).__name__}: {e}"[:80]
    finally:
        for p in patches: p.stop()
        PROVIDER_EXC["value"] = None
    conn = sqlite3.connect(db_path)
    ce = conn.execute("SELECT COUNT(*) FROM cloud_extractions").fetchone()[0]
    rc = conn.execute("SELECT COUNT(*) FROM run_calls").fetchone()[0]
    man = conn.execute("SELECT end_status, end_reason FROM run_manifests WHERE run_kind = 'extraction' ORDER BY run_id").fetchall()
    ev = conn.execute("SELECT COUNT(*) FROM paper_events WHERE event_type NOT IN ('screened')").fetchone()[0]
    conn.close()
    sends = len(RECORD["creates"]) - n_creates0; sleeps = RECORD["sleeps"][n_sleeps0:]
    st = stash.get("stats"); st = {k: v for k, v in st.items() if k in ("extracted", "failed")} if st else "-"
    print(f"  {label:44s} run_fault={is_run_fault(exc)!s:5s} | propagated: {propagated or 'no'} | sends {sends} | sleeps {sleeps} | "
          f"stats {st} | cloud_extractions {ce} of {len(pids)} papers | run_calls {rc} | paper events (non-screening) {ev} | "
          f"manifest {[tuple(m) for m in man]}")


httpx.Client.send = _refuse_send; httpx.AsyncClient.send = _refuse_send; socket.socket.connect = _refuse_connect
req = httpx.Request("POST", "https://example.invalid/v1")
for h, provider, fault in (("8 (openai_extractor.py:179, retry loop)", "openai", "recorder"),
                           ("9 (anthropic_extractor.py:205, retry loop)", "anthropic", "recorder"),
                           ("10 (openai_extractor.py:209, store_result)", "openai", "store"),
                           ("11 (anthropic_extractor.py:237, store_result)", "anthropic", "store")):
    print(f"\n=== handler {h} — fault at the {fault}, under a run (run_arm)")
    for name, exc in (("sqlite3.OperationalError", sqlite3.OperationalError("database is locked")), ("KeyError (unclassified)", KeyError("x"))):
        case(name, provider, fault, exc)
    print(f"--- the same, outside a run (extractor.run, run_id None)")
    for name, exc in (("sqlite3.OperationalError", sqlite3.OperationalError("database is locked")), ("KeyError (unclassified)", KeyError("x"))):
        case(name, provider, fault, exc, under_run=False)
for provider, sdk in (("openai", openai), ("anthropic", anthropic)):
    print(f"\n=== the provider raising, {provider} transport (handler {'8' if provider == 'openai' else '9'}), under a run")
    case(f"{sdk.__name__}.APIConnectionError", provider, "provider", sdk.APIConnectionError(request=req))
    case("ConnectionError (bare)", provider, "provider", ConnectionError("[Errno 111] Connection refused"))

print("\n=== the fakes' call record")
print(f"constructors: {len(RECORD['constructors'])}, every one with the placeholder key and no other argument: "
      f"{all(c[2] and c[1] == ['api_key'] for c in RECORD['constructors'])}; kinds: {sorted(set(c[0] for c in RECORD['constructors']))}")
print(f"fake creates (the would-be provider calls, answered in-process): {len(RECORD['creates'])}; "
      f"by provider: { {p: sum(1 for c in RECORD['creates'] if c[0] == p) for p in ('openai', 'anthropic')} }")
print(f"outbound requests attempted: httpx send {RECORD['httpx_send']}, socket connect {RECORD['socket_connect']}")
