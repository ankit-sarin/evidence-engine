"""U27 (C56's cloud half; R792(b), R792(e)): a run fault reaching one of the four cloud handlers leaves `run_arm` as itself.

The handlers, by the I2 mapping of 12ae_U27A/12ae_U27A.md (12f_triage.md's numbering; lines at 0d1d004):
  8  OpenAIExtractor.run, the retry loop's `except Exception as exc:`        (openai_extractor.py:179)
  9  AnthropicExtractor.run, the retry loop's `except Exception as exc:`     (anthropic_extractor.py:205)
  10 OpenAIExtractor.run, around store_result                              (openai_extractor.py:209)
  11 AnthropicExtractor.run, around store_result                           (anthropic_extractor.py:237)

Seams: 12ae_U27A/fake_probe_u27a.py's. A scratch ReviewDatabase under tmp_path with two papers, each with an eligibility event
(`seed_eligibility`) and a recorded parsed text (`write_parsed`); the live codebook copied beside it (read, never written); a spec copy
whose `enabled_arms: []` names the one arm under test; `rm.git_state` patched clean. The real transport is driven through
`scripts.run_cloud_extraction.run_arm`, with `cli.EXTRACTORS` pointing at a thin subclass that passes a placeholder key (so
`os.environ.get(...)` is never reached), captures `run`'s stats and, for the store cases, wraps the connection so that
`INSERT INTO cloud_extractions` raises. The SDK classes are replaced by recording fakes that answer a fixed complete response;
`time.sleep` in both transports is a recorder; `httpx.Client.send`, `httpx.AsyncClient.send` and `socket.socket.connect` are refusing
recorders. No provider, no server and no gitignored fixture is touched.

Routes: at 8-9 the fault comes from the recorder (`engine.core.run_manifest.record_call`, which `send` calls after the provider
answered) or from the provider seam (the fake's create). The recorder exists only under a run (`base.py:141`, `if self.run_id is not
None:`; 12ae_U27A.md F3), which is why every case runs under `run_arm`. At 10-11 the fault comes from the store's INSERT, which
`base.py:401` rolls back and re-raises into the handler.

Run faults (sqlite3.OperationalError at the recorder or the store; a bare ConnectionError from the provider seam) leave `run_arm` as
themselves after exactly one send: no cloud_extractions row (the paper stays pending), and run_arm's manifest closed `failed` with
`rm.failure_reason(exc)`. KeyError and the SDK's own APIConnectionError are not run faults and keep today's outcome (12ae_U27A.md §5):
at 8-9 three sends per paper, waits [2, 4, 2, 4], failed 2, manifest `completed`; at 10-11 failed 2, run_calls 2, manifest `completed`.
"""

from __future__ import annotations

import json
import shutil
import socket
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import anthropic
import httpx
import openai
import pytest

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
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
PLACEHOLDER_KEY = "fake-key-not-a-credential"
_REQ = httpx.Request("POST", "https://example.invalid/v1")

CASES = [
    # (id, provider, route, exception factory)
    ("8-openai-recorder-sqlite3", "openai", "recorder", lambda: sqlite3.OperationalError("database is locked")),
    ("8-openai-provider-ConnectionError", "openai", "provider", lambda: ConnectionError("[Errno 111] Connection refused")),
    ("8-openai-recorder-KeyError", "openai", "recorder", lambda: KeyError("x")),
    ("8-openai-provider-APIConnectionError", "openai", "provider", lambda: openai.APIConnectionError(request=_REQ)),
    ("9-anthropic-recorder-sqlite3", "anthropic", "recorder", lambda: sqlite3.OperationalError("database is locked")),
    ("9-anthropic-provider-ConnectionError", "anthropic", "provider", lambda: ConnectionError("[Errno 111] Connection refused")),
    ("9-anthropic-recorder-KeyError", "anthropic", "recorder", lambda: KeyError("x")),
    ("9-anthropic-provider-APIConnectionError", "anthropic", "provider", lambda: anthropic.APIConnectionError(request=_REQ)),
    ("10-openai-store-sqlite3", "openai", "store", lambda: sqlite3.OperationalError("database is locked")),
    ("10-openai-store-KeyError", "openai", "store", lambda: KeyError("x")),
    ("11-anthropic-store-sqlite3", "anthropic", "store", lambda: sqlite3.OperationalError("database is locked")),
    ("11-anthropic-store-KeyError", "anthropic", "store", lambda: KeyError("x")),
]


def _content() -> str:
    cbk = load_codebook(LIVE_CODEBOOK)
    return json.dumps({"fields": [
        {"field_name": f["name"], "value": "Original Research" if f["name"] == "study_type" else "NR",
         "source_snippet": f"Snippet for {f['name']}.", "confidence": 0.9, "tier": f["tier"]}
        for tier in (1, 2, 3, 4) for f in cbk.fields_by_tier(tier)]})


class _FailingConn:
    """Delegates everything, but refuses store_result's INSERT INTO cloud_extractions."""

    def __init__(self, real, exc):
        self.real, self.exc = real, exc

    def execute(self, sql, *a, **k):
        if "INSERT INTO cloud_extractions" in sql:
            raise self.exc
        return self.real.execute(sql, *a, **k)

    def __getattr__(self, name):
        return getattr(self.real, name)


@pytest.mark.parametrize("case_id,provider,route,make_exc", CASES, ids=[c[0] for c in CASES])
def test_run_faults_leave_the_cloud_handlers_as_themselves(tmp_path, monkeypatch, case_id, provider, route, make_exc):
    if not LIVE_CODEBOOK.exists():
        pytest.skip("codebook not available")
    from scripts import run_cloud_extraction as cli

    exc = make_exc()
    content = _content()
    record = {"constructors": [], "creates": 0, "sleeps": [], "httpx_send": 0, "socket_connect": 0}

    # ── the scratch review ────────────────────────────────────────────
    db = ReviewDatabase("u27", data_root=tmp_path)
    db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in (1, 2)])
    pids = [r["id"] for r in db.get_papers_by_status("INGESTED")]
    for pid in pids:
        seed_eligibility(db._conn, pid)
        write_parsed(db, pid, "Autonomous robotic suturing. " * 30)
    db_path = Path(db.db_path)
    shutil.copy2(LIVE_CODEBOOK, db_path.parent / "extraction_codebook.yaml")
    db.close()
    arm = next(a.name for a in load_review_spec(SPEC_PATH).arms if a.provider == provider)
    text = SPEC_PATH.read_text()
    assert text.count("enabled_arms: []") == 1
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arm}]"))

    # ── the fakes and the recorders ───────────────────────────────────
    def _create(**payload):
        record["creates"] += 1
        if route == "provider":
            raise exc
        if provider == "openai":
            usage = SimpleNamespace(prompt_tokens=100, completion_tokens=50,
                                    completion_tokens_details=SimpleNamespace(reasoning_tokens=10))
            return SimpleNamespace(choices=[SimpleNamespace(
                message=SimpleNamespace(content=content, reasoning_content="trace"), finish_reason="stop")], usage=usage)
        return SimpleNamespace(content=[SimpleNamespace(type="thinking", thinking="trace"),
                                        SimpleNamespace(type="text", text=content)],
                               usage=SimpleNamespace(input_tokens=100, output_tokens=50), stop_reason="end_turn")

    class FakeOpenAI:
        def __init__(self, **kw):
            record["constructors"].append(kw)
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=_create))

    class FakeAnthropic:
        def __init__(self, **kw):
            record["constructors"].append(kw)
            self.messages = SimpleNamespace(create=_create)

    def _refuse_send(self, *a, **k):
        record["httpx_send"] += 1
        raise RuntimeError("no network in this test")

    def _refuse_connect(self, *a, **k):
        record["socket_connect"] += 1
        raise RuntimeError("no network in this test")

    monkeypatch.setattr("engine.cloud.openai_extractor.openai.OpenAI", FakeOpenAI)
    monkeypatch.setattr("engine.cloud.anthropic_extractor.anthropic.Anthropic", FakeAnthropic)
    monkeypatch.setattr("engine.cloud.openai_extractor.time.sleep", lambda s: record["sleeps"].append(s))
    monkeypatch.setattr("engine.cloud.anthropic_extractor.time.sleep", lambda s: record["sleeps"].append(s))
    monkeypatch.setattr(httpx.Client, "send", _refuse_send)
    monkeypatch.setattr(httpx.AsyncClient, "send", _refuse_send)
    monkeypatch.setattr(socket.socket, "connect", _refuse_connect)
    monkeypatch.setattr(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None))
    if route == "recorder":
        def _failing_record_call(*a, **k):
            raise exc
        monkeypatch.setattr(rm, "record_call", _failing_record_call)

    stash = {}

    class Probe(cli.EXTRACTORS[provider]):
        def __init__(self, *a, **k):
            k["api_key"] = PLACEHOLDER_KEY
            super().__init__(*a, **k)
            if route == "store":
                self._real_conn = self._conn
                self._conn = _FailingConn(self._conn, exc)

        def run(self, **kw):
            stash["stats"] = super().run(**kw)
            return stash["stats"]

        def close(self):
            getattr(self, "_real_conn", self._conn).close()

    monkeypatch.setitem(cli.EXTRACTORS, provider, Probe)

    # ── drive run_arm ─────────────────────────────────────────────────
    propagated = None
    try:
        cli.run_arm(arm, str(db_path), str(spec_path), None, None)
    except BaseException as e:  # noqa: BLE001 — the test reads what left the entry point
        propagated = e

    conn = sqlite3.connect(db_path)
    try:
        stored = conn.execute("SELECT COUNT(*) FROM cloud_extractions WHERE arm = ?", (arm,)).fetchone()[0]
        calls = conn.execute("SELECT COUNT(*) FROM run_calls").fetchone()[0]
        end = conn.execute("SELECT end_status, end_reason FROM run_manifests ORDER BY run_id DESC LIMIT 1").fetchone()
    finally:
        conn.close()

    if is_run_fault(exc):
        assert propagated is exc, f"{case_id}: the run fault did not leave run_arm as itself: {propagated!r}"
        assert record["creates"] == 1, f"{case_id}: {record['creates']} sends"
        assert stored == 0, "the paper stays pending"
        assert end == ("failed", rm.failure_reason(exc))
    else:
        assert propagated is None, f"{case_id}: a paper-outcome error propagated: {propagated!r}"
        assert stash["stats"]["failed"] == 2 and stash["stats"]["extracted"] == 0
        assert stored == 0
        assert end == ("completed", None)
        if route in ("recorder", "provider"):
            assert record["creates"] == 6 and record["sleeps"] == [2, 4, 2, 4] and calls == 0
        else:
            assert record["creates"] == 2 and record["sleeps"] == [] and calls == 2

    # ── the fakes' record: no provider, no key, no network ────────────
    assert record["constructors"] and all(kw == {"api_key": PLACEHOLDER_KEY} for kw in record["constructors"])
    assert record["httpx_send"] == 0 and record["socket_connect"] == 0
