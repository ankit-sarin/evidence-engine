"""U19 commit 1 (R734(c), R735): the configured Ollama environment is read before a
model-calling run opens and recorded in its manifest body, and re-checked after every
PERFORMED proactive restart, a correctness-tier drift on that read a `RunRefused` that
closes the run 'aborted'. Each post-restart read is one run-telemetry event.

The subprocess boundary is patched in every test: `systemctl show` answers from a
hand-typed `Environment=` line in the form `systemctl show ollama --property=Environment`
printed on this host (12z_U19A.md §3.4). The extract loop is driven as
`test_restart_ollama_guard.py`'s T3 drives it; the manifests are opened as
`test_selection_bound.py` (`_open_run_manifest`, digest and git injected) and
`test_ft_bridge.py` (the FT runner's open, `git=CLEAN`) open theirs.
"""
from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.run_pipeline as rp
from engine.agents import extractor as E
from engine.agents import ft_screener as ft
from engine.core import extraction_events as X
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.core.run_telemetry import read_run_events
from engine.utils import ollama_client as oc
from engine.utils import ollama_preflight as pf
from _event_store_fixture import FIXTURE_DIGEST

REPO = Path(__file__).resolve().parents[1]
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
CLEAN = rm.GitState(commit="0" * 40, dirty=False, tag=None)

RAW = ("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin "
       "OLLAMA_KV_CACHE_TYPE=f16 OLLAMA_MAX_LOADED_MODELS=1 OLLAMA_NUM_PARALLEL=1 "
       "OLLAMA_FLASH_ATTENTION=true OLLAMA_KEEP_ALIVE=-1")
DRIFTED = RAW.replace("OLLAMA_KV_CACHE_TYPE=f16", "OLLAMA_KV_CACHE_TYPE=q8_0")


class Systemctl:
    """The subprocess boundary: answers `systemctl show ollama --property=Environment`
    with `raw` and records each such read in `seq`; refuses anything else."""

    def __init__(self, raw: str, seq: list):
        self.raw, self.seq, self.reads = raw, seq, 0

    def __call__(self, argv, *a, **k):
        assert list(argv[:3]) == ["systemctl", "show", "ollama"], argv
        self.reads += 1
        self.seq.append("env")
        return subprocess.CompletedProcess(argv, 0, stdout=f"Environment={self.raw}\n", stderr="")


@pytest.fixture
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


def _loop(monkeypatch, tmp_path, *, papers=4, cadence=2, restarted=True, raw=RAW):
    """T3's harness: the per-paper loop over `papers` papers at `cadence`, with the
    paper call and the restart replaced and the boundary answering `raw`."""
    seq: list = []
    ctl = Systemctl(raw, seq)
    monkeypatch.setattr(E, "load_codebook_beside", lambda p: SimpleNamespace(semantic_hash="h" * 64))
    monkeypatch.setattr(E, "stage_config", lambda stage, spec: SimpleNamespace(model="fake-model"))
    monkeypatch.setattr(E, "read_parsed_text", lambda ref: "paper text")

    def paper(pid, *a, **k):
        seq.append(("paper", pid))
        return SimpleNamespace(fields=["f"])
    monkeypatch.setattr(E, "extract_paper_with_completeness", paper)
    monkeypatch.setattr(E, "run_extraction_summary", lambda conn, run_id, selection: {
        "stored_incomplete": 0, "all_declined": 0, "below_floor": 0, "incomplete_fields": {},
        "value_divergence": {"substantive": {"count": 0, "fields": {}},
                             "case_or_whitespace": {"count": 0, "fields": {}}}})
    if restarted is not None:
        def restart(**kw):
            seq.append("restart")
            return restarted
        monkeypatch.setattr(E, "restart_ollama", restart)
    monkeypatch.setattr(pf, "require_preflight", lambda *a, **k: None)
    monkeypatch.setattr(pf.subprocess, "run", ctl)
    conn = SimpleNamespace(execute=lambda *a: SimpleNamespace(fetchone=lambda: ("A title",)))
    db = SimpleNamespace(_conn=conn, db_path=str(tmp_path / "review.db"))
    selection = SimpleNamespace(to_extract=[(pid, object()) for pid in range(1, papers + 1)],
                                skipped_asserted=[], skipped_refused=[])
    run = lambda: E._extract_selected(db, SimpleNamespace(extraction_models=SimpleNamespace(arm="an_arm")),
                                      selection, cadence, run_id=7, extractor_digest="d" * 64,
                                      auditor_digest=None)
    return run, seq, ctl


def _events(tmp_path):
    return [e for e in read_run_events(tmp_path) if e["kind"] == "ollama_env_after_restart"]


# (a) — the unit entry's acceptance test, clause 1
def test_the_env_check_runs_again_after_every_restart(monkeypatch, tmp_path):
    run, seq, ctl = _loop(monkeypatch, tmp_path)
    stats = run()
    assert stats["extracted"] == 4
    assert seq == [("paper", 1), ("paper", 2), "restart", "env",
                   ("paper", 3), ("paper", 4), "restart", "env"]


# (b) — clause 2, the loop: a correctness drift on the post-restart read stops it
def test_u19_a_correctness_drift_after_a_restart_raises_server_env_changed(monkeypatch, tmp_path):
    run, seq, _ = _loop(monkeypatch, tmp_path, raw=DRIFTED)
    with pytest.raises(rm.ServerEnvChanged) as exc:
        run()
    assert seq == [("paper", 1), ("paper", 2), "restart", "env"]      # paper 3 never starts
    msg = str(exc.value)
    assert msg.startswith("ServerEnvChanged: ")
    assert "OLLAMA_KV_CACHE_TYPE" in msg and "'q8_0'" in msg and "'f16'" in msg
    assert exc.value.drift == {"OLLAMA_KV_CACHE_TYPE": ("f16", "q8_0")}


# (b) — clause 2, the runner: run_pipeline closes it 'aborted' with reason=str(exc)
def test_u19_run_pipeline_closes_aborted_on_a_server_env_change(spec, tmp_path, monkeypatch):
    """test_u18_run_pipeline_closes_aborted_on_a_digest_change's form."""
    db = ReviewDatabase("u19", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    cb = load_codebook(LIVE_CODEBOOK)
    run_id = rm.open_run(db._conn, spec, kind="extraction", stages=("extract_pass1",),
                         codebook=cb, git=CLEAN, digest_fn=lambda m: FIXTURE_DIGEST).run_id
    refusal = rm.ServerEnvChanged.at({"OLLAMA_KV_CACHE_TYPE": ("f16", "q8_0")}, papers_done=25)

    def extract(*a, **k):
        raise refusal
    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda name: db)
    monkeypatch.setattr(rp, "_open_run_manifest", lambda d, s, i, **k: run_id)
    monkeypatch.setattr(rp, "is_adjudication_complete", lambda conn: True)
    monkeypatch.setattr(rp, "_stage_parse", lambda *a, **k: {})
    monkeypatch.setattr(rp, "_stage_extract", extract)
    with pytest.raises(rm.ServerEnvChanged):
        rp.run_pipeline("u19", skip_to="extract")
    conn = sqlite3.connect(db.db_path)
    status, reason = conn.execute("SELECT end_status, end_reason FROM run_manifests "
                                  "WHERE run_id = ?", (run_id,)).fetchone()
    conn.close()
    assert status == "aborted"
    assert reason == str(refusal)


# (c) — clause 3: the body run_pipeline opens carries the raw string read before open
def test_u19_the_body_opened_by_run_pipeline_carries_the_environment(spec, tmp_path, monkeypatch):
    db = ReviewDatabase("u19", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    real = rm.open_run
    monkeypatch.setattr(rm, "open_run", lambda *a, **k: real(
        *a, digest_fn=lambda m: FIXTURE_DIGEST, git=CLEAN, host="fixture", **k))
    ctl = Systemctl(RAW, [])
    monkeypatch.setattr(pf.subprocess, "run", ctl)
    run_id = rp._open_run_manifest(db, spec, rp.STAGES.index("extract"))
    raw, sha = db._conn.execute("SELECT manifest_json, manifest_sha256 FROM run_manifests "
                                "WHERE run_id = ?", (run_id,)).fetchone()
    body = json.loads(raw)
    assert body["ollama_environment"] == RAW
    assert sha == rm.sha256_canonical(body)                 # manifest_sha256 covers the key
    assert ctl.reads == 1
    db.close()


# (d) — a skip (opt-out) is not a restart: no check, and T3's counter rule holds
def test_u19_a_skipped_restart_triggers_no_env_check(monkeypatch, tmp_path, caplog):
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, "1")
    run, seq, ctl = _loop(monkeypatch, tmp_path, restarted=None)
    assert E.restart_ollama(reason="proactive", papers_done=0) is False   # a skip says so
    caplog.clear()                                           # count the loop's skips only
    with caplog.at_level(logging.WARNING, logger="engine.agents.extractor"):
        stats = run()
    assert stats["extracted"] == 4
    assert ctl.reads == 0 and "env" not in seq
    skips = [r for r in caplog.records if "RESTART SKIPPED" in r.getMessage()]
    assert len(skips) == 2                                   # reset each time, as T3 asserts


# (e) — the keyword absent (None): no key, today's body shape
def test_u19_a_body_opened_without_the_environment_has_no_such_key(spec, tmp_path):
    db = ReviewDatabase("u19", data_root=tmp_path)
    cb = load_codebook(LIVE_CODEBOOK)
    run_id = rm.open_run(db._conn, spec, kind="extraction", stages=("extract_pass1",),
                         codebook=cb, git=CLEAN, digest_fn=lambda m: FIXTURE_DIGEST,
                         ollama_environment=None).run_id
    body = json.loads(db._conn.execute("SELECT manifest_json FROM run_manifests "
                                       "WHERE run_id = ?", (run_id,)).fetchone()[0])
    assert "ollama_environment" not in body
    db.close()


# (f) — every post-restart read, pass or drift, is one telemetry event
def test_u19_each_post_restart_read_writes_one_telemetry_event(monkeypatch, tmp_path):
    run, _, _ = _loop(monkeypatch, tmp_path)
    run()
    events = _events(tmp_path)
    assert len(events) == 2
    assert all(e["run_id"] == 7 for e in events)
    assert all(e["payload"]["environment"] == RAW and e["payload"]["verdict"] == "ok"
               for e in events)


def test_u19_a_drift_read_writes_its_telemetry_event_before_the_refusal(monkeypatch, tmp_path):
    run, _, _ = _loop(monkeypatch, tmp_path, raw=DRIFTED)
    with pytest.raises(rm.ServerEnvChanged):
        run()
    (event,) = _events(tmp_path)
    assert event["payload"]["environment"] == DRIFTED
    assert event["payload"]["verdict"] == "drift"
    assert event["payload"]["correctness"] == {"OLLAMA_KV_CACHE_TYPE": ["f16", "q8_0"]}


# (g) — the class is a RunRefused and so a run fault
def test_u19_server_env_changed_is_a_run_refused_run_fault():
    assert issubclass(rm.ServerEnvChanged, rm.RunRefused)
    assert not issubclass(rm.ServerEnvChanged, RuntimeError)   # S9's except RuntimeError passes it
    assert X.is_run_fault(rm.ServerEnvChanged("refused"))


# (m) — the FT runner's open carries the raw string (test_ft_bridge.py's open: git=CLEAN)
def test_u19_the_body_opened_by_the_ft_runner_carries_the_environment(spec, tmp_path, monkeypatch):
    db = ReviewDatabase("u19ft", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    monkeypatch.setattr(pf.subprocess, "run", Systemctl(RAW, []))
    run_id = ft.open_screening_manifest(db, spec, with_preflight=True, git=CLEAN,
                                        digest_fn=lambda m: FIXTURE_DIGEST)
    body = json.loads(db._conn.execute("SELECT manifest_json FROM run_manifests "
                                       "WHERE run_id = ?", (run_id,)).fetchone()[0])
    assert body["ollama_environment"] == RAW
    db.close()


# (n) — a run that sends nothing to Ollama records none: a pin, green on the parent
def test_u19_a_cloud_shaped_open_records_no_environment(spec, tmp_path):
    """run_cloud_extraction's open is `open_run(kind="extraction", stages=(), cloud_arms=[arm],
    payload_description=…)` with no environment keyword. That path cannot be driven under the
    fence here: it builds a provider client and needs a non-empty `cloud.enabled_arms`, which
    the live spec keeps empty (R71). So the pin is open_run with the cloud runner's keywords
    minus the arm — no keyword, no key."""
    db = ReviewDatabase("u19cl", data_root=tmp_path)
    cb = load_codebook(LIVE_CODEBOOK)
    run_id = rm.open_run(db._conn, spec, kind="extraction", stages=(), codebook=cb,
                         git=CLEAN, digest_fn=lambda m: FIXTURE_DIGEST).run_id
    body = json.loads(db._conn.execute("SELECT manifest_json FROM run_manifests "
                                       "WHERE run_id = ?", (run_id,)).fetchone()[0])
    assert "ollama_environment" not in body
    db.close()
