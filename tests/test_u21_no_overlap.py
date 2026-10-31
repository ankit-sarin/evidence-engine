"""U21 (B-F14, R761(c), R762): no request is sent while the previous one is alive.

A watchdog timeout now marks a request late and waits on the same future. A response after it is
a completion. A read timeout ends the request, and only then may the recovery restart run, once,
its final send preceded by the digest and environment checks (F7). How the path ended is at the
head of `run_calls.outcome_detail`, from the one closed set in `run_manifest`. Every executor is
shut down in a finally (N24). A slow request with `max_retries=0` no longer restarts (INT-ab12-1).

`oc.time` is replaced so the restart's stabilisation wait does not run; the fakes sleep on their
own `time`.
"""
from __future__ import annotations

import json
import shutil
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import ollama
import pytest

from engine.core import effective_config as ec
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_paths import load_spec_for
from engine.core.run_telemetry import telemetry_path
from engine.utils import ollama_client as oc
from engine.utils import ollama_preflight as pf

REPO = Path(__file__).resolve().parent.parent
PROBE = REPO / "docs/session-reports/session-12/12aa_U20P"
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
A, B = "a" * 64, "b" * 64
MESSAGES = [{"role": "user", "content": "hello"}]
CEILING = 131_072  # gemma3:27b's trained context, the audit model's (12aa-U20B)
WATCHDOG = 0.05


def _answer():
    return SimpleNamespace(message=SimpleNamespace(content="OK", thinking=None),
                           prompt_eval_count=2, done_reason="stop", eval_count=1)


class Fake:
    """Answers `show` and `ps` from committed bodies; `chat` runs one step of `script` per send
    and counts how many sends are in flight at once."""

    def __init__(self, *script):
        self._client = SimpleNamespace(base_url="http://u21.invalid")
        self.script = list(script)
        self.lock = threading.Lock()
        self.now = self.peak = self.sends = 0
        self.release = threading.Event()
        self.events = None

    def show(self, model):
        body = json.loads((PROBE / "show" / f"{model.replace(':', '_')}.response.json").read_text(encoding="utf-8"))
        return ollama.ShowResponse(**body)

    def ps(self):
        template = json.loads((PROBE / "load/P3_ps.json").read_text(encoding="utf-8"))["models"][0]
        name = self.model
        return ollama.ProcessResponse(models=[{**template, "name": name, "model": name, "digest": A, "context_length": CEILING}])

    def chat(self, **kwargs):
        self.model = kwargs["model"]
        if self.events is not None:
            self.events.append("send")
        with self.lock:
            self.sends += 1; self.now += 1; self.peak = max(self.peak, self.now)
            step = self.script.pop(0) if self.script else "answer"
        try:
            if step == "answer":
                return _answer()
            if step == "late":
                time.sleep(0.25)
                return _answer()
            if step == "read_timeout":
                time.sleep(0.15)
                raise httpx.ReadTimeout("read timed out")
            if step == "block":
                self.release.wait(10)
                raise httpx.ReadTimeout("read timed out")
            raise step
        finally:
            with self.lock:
                self.now -= 1


@pytest.fixture(autouse=True)
def no_stabilisation_wait(monkeypatch):
    monkeypatch.setattr(oc, "time", SimpleNamespace(monotonic=time.monotonic, sleep=lambda s: None))
    monkeypatch.delenv(oc.RESTART_OPT_OUT_ENV, raising=False)
    monkeypatch.setattr(oc, "effective_ceiling", lambda model, options=None: CEILING)


@pytest.fixture
def install(monkeypatch):
    fakes = []

    def _install(*script):
        fake = Fake(*script)
        monkeypatch.setattr(oc, "_client", fake)
        fakes.append(fake)
        return fake
    yield _install
    for fake in fakes:
        fake.release.set()


@pytest.fixture
def service():
    with patch("engine.utils.ollama_client.subprocess.run", return_value=MagicMock(returncode=0)) as run:
        yield run


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture
def review(tmp_path):
    db = ReviewDatabase("u21", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    db.close()


class Digests:
    def __init__(self, *values, events=None):
        self.values = list(values); self.events = events

    def __call__(self, model):
        if self.events is not None:
            self.events.append("digest")
        return self.values.pop(0) if len(self.values) > 1 else self.values[0]


def _reading(correctness=None):
    return pf.EnvReading(raw="OLLAMA_NUM_PARALLEL=1", correctness=correctness or {}, performance={})


def _call(spec, wall=WATCHDOG, **kw):
    return oc.ollama_chat(messages=MESSAGES, wall_timeout=wall, max_retries=0, retry_delay=0,
                          **{**ec.stage_config("audit", spec).kwargs(), **kw})


def _run(review, spec, digests):
    db, cb = review
    return rm.open_run(db._conn, spec, kind="screening", stages=("audit",), codebook=cb,
                       preflight_models=(), git=CLEAN, digest_fn=digests).run_id


def _rows(db):
    return db._conn.execute("SELECT outcome, outcome_detail FROM run_calls ORDER BY call_id").fetchall()


# ── the entry's acceptance test ─────────────────────────────────────────────


def test_no_attempt_starts_while_the_previous_request_is_still_running(install, monkeypatch):
    fake = install("read_timeout", "read_timeout", "read_timeout")
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, "1")
    with pytest.raises(TimeoutError):
        oc.ollama_chat(model="m:8b", messages=MESSAGES, wall_timeout=WATCHDOG, max_retries=2, retry_delay=0)
    assert fake.peak == 1
    assert fake.sends == 1


# ── a late response is a completion ─────────────────────────────────────────


def test_a_late_response_is_used_and_recorded_completed_with_lateness(install, review, spec, service):
    fake = install("late")
    db, _ = review
    with rm.active_run(db._conn, _run(review, spec, Digests(A))):
        response = _call(spec)
    assert response.message.content == "OK"
    assert fake.sends == 1
    service.assert_not_called()
    (row,) = _rows(db)
    assert row["outcome"] == "completed"
    assert row["outcome_detail"].startswith("late_completion: ")


def test_a_slow_request_with_no_retries_does_not_restart(install, service):
    """INT-ab12-1: the preflight's and the quality check's `max_retries=0`, past the watchdog."""
    fake = install("late")
    response = oc.ollama_chat(model="m:8b", messages=MESSAGES, wall_timeout=WATCHDOG, max_retries=0)
    assert response.message.content == "OK"
    assert fake.sends == 1
    service.assert_not_called()


# ── the read timeout reaches the restart once, and the final send is re-checked ──


def test_a_read_timed_out_request_reaches_the_restart_once_and_the_final_attempt_is_rechecked(
        install, review, spec, service, monkeypatch):
    fake = install("read_timeout", "answer")
    db, _ = review
    events = []
    digests = Digests(A, events=events)
    run_id = _run(review, spec, digests)
    events.clear()
    fake.events = events
    service.side_effect = lambda *a, **k: (events.append("restart"), MagicMock(returncode=0))[1]
    monkeypatch.setattr(pf, "recheck_ollama_env", lambda: (events.append("env"), _reading())[1])
    with rm.active_run(db._conn, run_id):
        assert _call(spec).message.content == "OK"
    assert events == ["digest", "send", "restart", "digest", "env", "send"]
    service.assert_called_once()
    (row,) = _rows(db)
    assert (row["outcome"], row["outcome_detail"]) == ("completed", "restart_recovered")


def test_a_digest_changed_by_the_restart_refuses_the_final_send(install, review, spec, service, monkeypatch):
    fake = install("read_timeout")
    db, _ = review
    monkeypatch.setattr(pf, "recheck_ollama_env", lambda: _reading())
    run_id = _run(review, spec, Digests(A, A, B))
    with rm.active_run(db._conn, run_id):
        with pytest.raises(rm.ModelDigestChanged):
            _call(spec)
    assert fake.sends == 1
    assert _rows(db)[0]["outcome"] == "error"


def test_an_env_drift_after_the_restart_refuses_the_final_send_and_writes_its_record(install, review, spec, service, monkeypatch):
    fake = install("read_timeout")
    db, _ = review
    drift = {"OLLAMA_NUM_PARALLEL": ("1", "2")}
    monkeypatch.setattr(pf, "recheck_ollama_env", lambda: _reading(drift))
    run_id = _run(review, spec, Digests(A))
    with rm.active_run(db._conn, run_id):
        with pytest.raises(rm.ServerEnvChanged, match="recovery Ollama restart"):
            _call(spec)
    assert fake.sends == 1
    rows = [json.loads(l) for l in telemetry_path(Path(db.db_path).parent).read_text().splitlines()]
    row = [r for r in rows if r["kind"] == "ollama_env_after_restart"][-1]
    assert row["run_id"] == run_id and row["payload"]["verdict"] == "drift"


def test_the_post_restart_path_writes_the_env_record(install, review, spec, service, monkeypatch):
    install("read_timeout", "answer")
    db, _ = review
    monkeypatch.setattr(pf, "recheck_ollama_env", lambda: _reading())
    run_id = _run(review, spec, Digests(A))
    with rm.active_run(db._conn, run_id):
        _call(spec)
    rows = [json.loads(l) for l in telemetry_path(Path(db.db_path).parent).read_text().splitlines()]
    (row,) = [r for r in rows if r["kind"] == "ollama_env_after_restart"]
    assert row["run_id"] == run_id
    assert row["payload"] == {"environment": "OLLAMA_NUM_PARALLEL=1", "papers_done": None, "verdict": "ok",
                              "correctness": {}, "performance": {}}


def test_outside_a_run_the_env_recheck_writes_nothing_and_says_so(install, service, monkeypatch, caplog):
    install("read_timeout", "answer")
    monkeypatch.setattr(pf, "recheck_ollama_env", lambda: pytest.fail("not outside a run"))
    with caplog.at_level("INFO", logger="engine.core.run_manifest"):
        oc.ollama_chat(model="m:8b", messages=MESSAGES, wall_timeout=WATCHDOG, max_retries=0)
    assert "nothing recorded" in caplog.text


# ── the closed set of details ───────────────────────────────────────────────


def test_the_detail_set_is_the_six_endings():
    assert rm.RUN_CALL_ENDING_DETAILS == (
        "restart_refused_opt_out", "restart_refused_foreign_lock", "restart_command_failed",
        "post_restart_call_failed", "restart_recovered", "worker_alive_at_read_timeout")


def _ended(install, review, spec, script):
    fake = install(*script)
    db, _ = review
    run_id = _run(review, spec, Digests(A))
    with rm.active_run(db._conn, run_id):
        with pytest.raises(TimeoutError):
            _call(spec)
    (row,) = _rows(db)
    assert row["outcome"] == "error"
    return fake, row["outcome_detail"]


def test_the_opt_out_ending_is_recorded(install, review, spec, service, monkeypatch):
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, "1")
    _, detail = _ended(install, review, spec, ["read_timeout"])
    assert detail.startswith(rm.DETAIL_RESTART_REFUSED_OPT_OUT + ": TimeoutError: ")
    service.assert_not_called()


def test_the_foreign_lock_ending_is_recorded(install, review, spec, service, monkeypatch):
    monkeypatch.setattr(oc, "foreign_lock_held", lambda: True)
    _, detail = _ended(install, review, spec, ["read_timeout"])
    assert detail.startswith(rm.DETAIL_RESTART_REFUSED_LOCK + ": TimeoutError: ")
    service.assert_not_called()


def test_the_restart_command_ending_is_recorded(install, review, spec, service):
    service.side_effect = OSError("sudo not available")
    _, detail = _ended(install, review, spec, ["read_timeout"])
    assert detail.startswith(rm.DETAIL_RESTART_COMMAND_FAILED + ": TimeoutError: ")


def test_the_post_restart_ending_is_recorded(install, review, spec, service, monkeypatch):
    monkeypatch.setattr(pf, "recheck_ollama_env", lambda: _reading())
    fake, detail = _ended(install, review, spec, ["read_timeout", ollama.ResponseError("runner crashed", 500)])
    assert detail.startswith(rm.DETAIL_POST_RESTART_FAILED + ": TimeoutError: ")
    assert fake.sends == 2
    service.assert_called_once()


def test_the_worker_alive_ending_is_recorded_with_no_restart_and_nothing_sent(install, review, spec, service, monkeypatch):
    monkeypatch.setattr(oc, "_REQUEST_BOUND", 0.2)
    fake, detail = _ended(install, review, spec, ["block"])
    assert detail.startswith(rm.DETAIL_WORKER_ALIVE + ": TimeoutError: ")
    assert fake.sends == 1
    service.assert_not_called()


# ── what stays as it was ────────────────────────────────────────────────────


def test_other_timeout_members_keep_their_retry(install, service):
    """A pin (R736(c)): R762(c)'s D3, green on the parent by construction."""
    fake = install(httpx.ConnectTimeout("connect timed out"), "answer")
    assert oc.ollama_chat(model="m:8b", messages=MESSAGES, max_retries=1, retry_delay=0).message.content == "OK"
    assert fake.sends == 2
    service.assert_not_called()


# ── N24 ─────────────────────────────────────────────────────────────────────


class CountingExecutor(oc.ThreadPoolExecutor):
    made: list = []

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.shut = False
        CountingExecutor.made.append(self)

    def shutdown(self, *a, **k):
        self.shut = True
        super().shutdown(*a, **k)


def test_no_executor_outlives_its_call(install, service, monkeypatch):
    monkeypatch.setattr(oc, "ThreadPoolExecutor", CountingExecutor)
    CountingExecutor.made = []
    install("answer")
    oc.ollama_chat(model="m:8b", messages=MESSAGES, wall_timeout=5.0, max_retries=0)
    install("read_timeout", "answer")
    oc.ollama_chat(model="m:8b", messages=MESSAGES, wall_timeout=WATCHDOG, max_retries=0)
    assert len(CountingExecutor.made) == 3
    assert all(e.shut for e in CountingExecutor.made)
