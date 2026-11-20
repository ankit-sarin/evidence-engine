"""U41 (INT-ai12-1, R819(c)–(g)): the Ollama restart paths are opt-in and off by default.

The recovery restart runs only inside a run whose manifest records `ollama_restarts.on_read_timeout`
true; otherwise a read timeout ends `restart_not_enabled`, the seventh call ending (R819(e)). A call
outside any run never restarts. The proactive cadence defaults to 0, a failed proactive restart resets
the cadence counter (R819(f)), both commands carry `sudo -n`, and the two analysis harnesses continue
past a failed restart (R819(c)). `run_pipeline` asks through `--restart-every` and
`--restart-on-read-timeout`, recorded under one optional body key (R819(d)). `pdf_quality_check`'s
loop stops on three read timeouts in a row or at the first run fault and the process exits non-zero
(R819(g)).

The conftest fence is on throughout: every restart command is caught at its own site's seam
(`engine.utils.ollama_client.subprocess.run`, `engine.agents.extractor.subprocess.run`) or the
restart function is replaced; no model, server, socket, sudo or systemctl is reached.

A run that asks is opened through `_open`, which passes `ollama_restarts` only where `open_run`
takes it, so that on the parent the cases expected green there (4, 10c, 10d) test what they test
today and the red ones fail on their assertion, not on a keyword.
"""
from __future__ import annotations

import argparse
import inspect
import shutil
import sqlite3
import sys
import threading
import time
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest

from engine.agents import extractor as E
from engine.core import effective_config as ec
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_paths import load_spec_for
from engine.utils import ollama_client as oc
from engine.utils import ollama_preflight as pf

import test_extractor as _te          # its DB helper; a module name, so nothing is collected twice
import test_u21_no_overlap as _u21    # its fake client and committed probe bodies

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="c" * 40, dirty=False, tag=None)
A = "a" * 64
MESSAGES = [{"role": "user", "content": "hello"}]
ARGV_N = ["sudo", "-n", "systemctl", "restart", "ollama"]
ASKED = {"proactive_every": 0, "on_read_timeout": True}


# ── fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def quiet(monkeypatch):
    monkeypatch.setattr(oc, "time", SimpleNamespace(monotonic=time.monotonic, sleep=lambda s: None))
    monkeypatch.delenv(oc.RESTART_OPT_OUT_ENV, raising=False)
    monkeypatch.setattr(oc, "effective_ceiling", lambda model, options=None: _u21.CEILING)
    monkeypatch.setattr(pf, "recheck_ollama_env", lambda: _u21._reading())


@pytest.fixture
def install(monkeypatch):
    fakes = []

    def _install(*script):
        fake = _u21.Fake(*script)
        monkeypatch.setattr(oc, "_client", fake)
        fakes.append(fake)
        return fake
    yield _install
    for fake in fakes:
        fake.release.set()


@pytest.fixture
def service():
    """The recovery restart's own seam."""
    with patch("engine.utils.ollama_client.subprocess.run", return_value=MagicMock(returncode=0)) as run:
        yield run


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture
def review(tmp_path):
    db = ReviewDatabase("u41", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    db.close()


def _open(review, spec, restarts=None):
    db, cb = review
    kw = {}
    if restarts is not None and "ollama_restarts" in inspect.signature(rm.open_run).parameters:
        kw["ollama_restarts"] = restarts
    return rm.open_run(db._conn, spec, kind="screening", stages=("audit",), codebook=cb,
                       preflight_models=(), git=CLEAN, digest_fn=_u21.Digests(A), **kw).run_id


def _call(spec):
    return oc.ollama_chat(messages=MESSAGES, wall_timeout=_u21.WATCHDOG, max_retries=0, retry_delay=0,
                          **ec.stage_config("audit", spec).kwargs())


def _rows(db):
    return db._conn.execute("SELECT outcome, outcome_detail FROM run_calls ORDER BY call_id").fetchall()


# ── 1–4, 11: the recovery restart ───────────────────────────────────────────


def test_1_inside_a_run_that_did_not_ask_a_read_timeout_runs_no_command(install, review, spec, service):
    """Case 1. Seam: the fake client at `oc._client` raises `httpx.ReadTimeout`; the restart command at
    `engine.utils.ollama_client.subprocess.run`. A run opened with no `ollama_restarts`: no command, a
    TimeoutError, the run_calls row `error` with its detail beginning `restart_not_enabled`, and a
    message that does not contain "restart"."""
    install("read_timeout")
    db, _ = review
    with rm.active_run(db._conn, _open(review, spec)):
        with pytest.raises(TimeoutError) as caught:
            _call(spec)
    service.assert_not_called()
    (row,) = _rows(db)
    assert row["outcome"] == "error"
    assert row["outcome_detail"].startswith("restart_not_enabled: TimeoutError: ")
    assert "restart" not in str(caught.value)


def test_2_outside_any_run_a_read_timeout_runs_no_command(install, service):
    """Case 2. Seam as case 1, no active run: no command, and the TimeoutError carries
    `restart_not_enabled` as its run-call detail."""
    install("read_timeout")
    with pytest.raises(TimeoutError) as caught:
        oc.ollama_chat(model="m:8b", messages=MESSAGES, wall_timeout=_u21.WATCHDOG, max_retries=0)
    service.assert_not_called()
    assert getattr(caught.value, "run_call_detail", None) == "restart_not_enabled"


def test_3_inside_a_run_that_asked_the_command_runs_once_with_dash_n(install, review, spec, service):
    """Case 3. Seam as case 1. A run whose manifest records `on_read_timeout` true: the command runs
    once, its argv exactly sudo, -n, systemctl, restart, ollama; the final attempt answers."""
    install("read_timeout", "answer")
    db, _ = review
    with rm.active_run(db._conn, _open(review, spec, ASKED)):
        assert _call(spec).message.content == "OK"
    service.assert_called_once()
    assert service.call_args[0][0] == ARGV_N
    (row,) = _rows(db)
    assert (row["outcome"], row["outcome_detail"]) == ("completed", "restart_recovered")


def test_4_the_opt_out_wins_over_a_run_that_asked(install, review, spec, service, monkeypatch):
    """Case 4. Seam as case 1, the opt-out variable set: no command; `restart_refused_opt_out`, as now
    (R819(d): the variable wins over the key)."""
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, "1")
    install("read_timeout")
    db, _ = review
    with rm.active_run(db._conn, _open(review, spec, ASKED)):
        with pytest.raises(TimeoutError):
            _call(spec)
    service.assert_not_called()
    (row,) = _rows(db)
    assert row["outcome_detail"].startswith(rm.DETAIL_RESTART_REFUSED_OPT_OUT + ": TimeoutError: ")


def test_11_a_thread_without_the_runs_context_is_not_enabled(install, review, spec, service):
    """Case 11. Seam as case 1. A run that asked is active on this thread; a call made from a new
    thread, which does not carry the run's context, is not enabled and runs no command."""
    install("read_timeout")
    db, _ = review
    got = {}

    def other():
        try:
            oc.ollama_chat(model="m:8b", messages=MESSAGES, wall_timeout=_u21.WATCHDOG, max_retries=0)
        except BaseException as exc:  # noqa: BLE001 — the thread reports what ended it
            got["exc"] = exc
    with rm.active_run(db._conn, _open(review, spec, ASKED)):
        t = threading.Thread(target=other)
        t.start(); t.join(10)
    service.assert_not_called()
    assert isinstance(got.get("exc"), TimeoutError)
    assert getattr(got["exc"], "run_call_detail", None) == "restart_not_enabled"


# ── 5: the closed set ───────────────────────────────────────────────────────


def test_5_the_call_endings_are_the_six_then_restart_not_enabled():
    """Case 5. The closed set of R761(c)(iv) and R762(c), amended from six to seven by R819(e)."""
    assert rm.RUN_CALL_ENDING_DETAILS == (
        "restart_refused_opt_out", "restart_refused_foreign_lock", "restart_command_failed",
        "post_restart_call_failed", "restart_recovered", "worker_alive_at_read_timeout",
        "restart_not_enabled")


# ── 6: run_pipeline's two flags and the one key ─────────────────────────────


def _pipeline_review(tmp_path):
    from _event_store_fixture import seed_eligibility
    from _parsed_text_fixture import write_parsed
    rdb = ReviewDatabase("u41p", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
                      "VALUES (1, 't', 's', 'FT_ELIGIBLE', 'n', 'n')")
    seed_eligibility(rdb._conn, 1)
    write_parsed(rdb, 1, "One line of text.\n")
    rdb._conn.commit()
    return rdb


def _drive(tmp_path, monkeypatch, spec, **flags):
    """`run_pipeline` from EXTRACT with the real `_open_run_manifest` (digest, git and the open's
    environment read injected) and the real `_stage_extract`; `run_extraction` stubbed to record what
    it receives. Returns (manifest body, run_extraction's keyword arguments)."""
    import json
    import scripts.run_pipeline as rp
    from _event_store_fixture import FIXTURE_DIGEST
    db = _pipeline_review(tmp_path)
    monkeypatch.setattr(pf, "read_ollama_env", lambda: ("", {}))
    real = rm.open_run
    monkeypatch.setattr(rm, "open_run", lambda *a, **k: real(
        *a, digest_fn=lambda m: FIXTURE_DIGEST, git=CLEAN, host="fixture", **k))
    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda name: db)
    monkeypatch.setattr(rp, "is_adjudication_complete", lambda conn: True)
    monkeypatch.setattr(rp, "_distribution_check", lambda *a, **k: None)
    seen = {}

    def fake_run_extraction(db_, spec_, review_name, **kw):
        seen.update(kw)
        raise RuntimeError("stop after extract")
    monkeypatch.setattr(rp, "run_extraction", fake_run_extraction)
    monkeypatch.setattr(db, "close", lambda: None)
    with pytest.raises(RuntimeError, match="stop after extract"):
        rp.run_pipeline("u41p", skip_to="extract", **flags)
    # the fixture's eligibility seed writes a manifest of its own; the pipeline's is the last
    body = json.loads(db._conn.execute(
        "SELECT manifest_json FROM run_manifests ORDER BY run_id DESC LIMIT 1").fetchone()[0])
    assert body["run_kind"] == "extraction"
    db._conn.close()
    return body, seen


def test_6_run_pipeline_with_neither_flag_records_no_key_and_passes_zero(tmp_path, monkeypatch, spec):
    """Case 6, neither flag. Seam: `run_pipeline` with `run_extraction` stubbed; the manifest read back
    from the scratch database."""
    body, seen = _drive(tmp_path, monkeypatch, spec)
    assert "ollama_restarts" not in body
    assert seen.get("restart_every") == 0


def test_6_run_pipeline_with_both_flags_records_the_key_and_passes_the_cadence(tmp_path, monkeypatch, spec):
    """Case 6, both flags (`--restart-every 3 --restart-on-read-timeout`)."""
    body, seen = _drive(tmp_path, monkeypatch, spec, restart_every=3, restart_on_read_timeout=True)
    assert body["ollama_restarts"] == {"proactive_every": 3, "on_read_timeout": True}
    assert seen["restart_every"] == 3


@pytest.mark.parametrize("flags, key", [
    ({"restart_every": 4}, {"proactive_every": 4, "on_read_timeout": False}),
    ({"restart_on_read_timeout": True}, {"proactive_every": 0, "on_read_timeout": True}),
])
def test_6_run_pipeline_with_one_flag_records_the_other_member_off(tmp_path, monkeypatch, spec, flags, key):
    """Case 6, one flag only: the other member at its off value."""
    body, seen = _drive(tmp_path, monkeypatch, spec, **flags)
    assert body["ollama_restarts"] == key
    assert seen["restart_every"] == key["proactive_every"]


def test_6_the_cli_parses_both_flags_and_refuses_a_negative_cadence(monkeypatch):
    """Case 6, the parser: `main()` hands both flags to `run_pipeline`; `--restart-every -1` is refused
    by argparse. Seam: `run_pipeline` replaced; `maybe_background` replaced."""
    import scripts.run_pipeline as rp
    import engine.utils.background as bg
    got = {}
    monkeypatch.setattr(bg, "maybe_background", lambda *a, **k: None)
    monkeypatch.setattr(rp, "run_pipeline", lambda *a, **k: got.update(k))
    monkeypatch.setattr(sys, "argv", ["run_pipeline.py", "--review", "x", "--restart-every", "3",
                                      "--restart-on-read-timeout"])
    assert rp.main() == 0
    assert got["restart_every"] == 3 and got["restart_on_read_timeout"] is True
    monkeypatch.setattr(sys, "argv", ["run_pipeline.py", "--review", "x"])
    assert rp.main() == 0
    assert got["restart_every"] == 0 and got["restart_on_read_timeout"] is False
    monkeypatch.setattr(sys, "argv", ["run_pipeline.py", "--review", "x", "--restart-every", "-1"])
    with pytest.raises(SystemExit) as caught:
        rp.main()
    assert caught.value.code == 2


# ── 7: the defaults ─────────────────────────────────────────────────────────


_REAL_PARSE_ARGS = argparse.ArgumentParser.parse_args


def _parsed_args(monkeypatch, main, argv):
    class _Stop(Exception):
        pass
    seen = {}
    real = _REAL_PARSE_ARGS

    def capture(self, args=None, namespace=None):
        seen["args"] = real(self, args, namespace)
        raise _Stop
    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", capture)
    with pytest.raises(_Stop):
        main(argv)
    return seen["args"]


def test_7_the_proactive_default_is_zero_in_the_engine_and_both_harnesses(monkeypatch):
    """Case 7. `extractor.RESTART_EVERY_N` is 0, and the two harnesses' parsers default
    `--restart-every` to 0. Seam: `argparse.ArgumentParser.parse_args`, wrapped to stop after parsing."""
    from analysis.eval import run_capture01 as cap
    from analysis.eval.elicit01 import runner as eli
    assert E.RESTART_EVERY_N == 0
    assert _parsed_args(monkeypatch, eli.main, []).restart_every == 0
    assert _parsed_args(monkeypatch, cap.main, ["--review", "x"]).restart_every == 0


# ── 8: a failed proactive restart resets the counter ───────────────────────


@patch("engine.agents.extractor.extract_paper")
@patch("engine.utils.ollama_preflight.require_preflight")
@patch("engine.utils.ollama_client.fetch_model_digest", return_value="a" * 64)
def test_8_a_failing_proactive_restart_is_tried_every_restart_every_papers(
        _digest, _preflight, mock_extract, tmp_path, monkeypatch):
    """Case 8. Seam: the proactive restart's command at `engine.agents.extractor.subprocess.run`, failing
    every time; `extract_paper` faked (test_extractor's helper). restart_every 2 over six papers: three
    attempts, after papers 2, 4 and 6, each argv carrying -n. The parent attempts after every paper from
    the second on (I4)."""
    helper = _te.TestRestartOllamaGraceful()
    db, spec_ = helper._setup_db(tmp_path, n_papers=6)
    mock_extract.side_effect = helper._make_fake_extract(spec_)
    import subprocess as sp
    with patch("engine.agents.extractor.subprocess.run",
               side_effect=sp.CalledProcessError(1, ARGV_N)) as cmd:
        stats = E.run_extraction(db, spec_, "test_review", restart_every=2, run_id=helper.run_id)
    assert stats["extracted"] == 6
    assert cmd.call_count == 3, f"attempts: {cmd.call_count}"
    assert all(c[0][0] == ARGV_N for c in cmd.call_args_list)
    db._conn.close()


# ── 9: the two harnesses continue past a failed restart ────────────────────


def test_9_elicit01_continues_when_the_restart_fails(tmp_path, monkeypatch):
    """Case 9, elicit01. Seam: the harness's own `main`, its module functions faked (no server, no
    model): two papers x two conditions, `--restart-every 1`, `restart_ollama` raising RuntimeError.
    Every unit is called."""
    from analysis.eval.elicit01 import runner as eli
    calls = []
    papers = [SimpleNamespace(paper_id=1, length_stratum="s"), SimpleNamespace(paper_id=2, length_stratum="s")]
    monkeypatch.setattr(eli, "preflight_version", lambda: "0.21.0")
    monkeypatch.setattr(eli, "digest", lambda: "d" * 64)
    monkeypatch.setattr(eli, "select_sample", lambda review_dir: papers)
    monkeypatch.setattr(eli, "EXCLUDED", frozenset())
    monkeypatch.setattr(eli, "newest_parsed", lambda d, pid: SimpleNamespace(read_text=lambda: "text"))
    monkeypatch.setattr(eli, "build_unit_map", lambda pid, raw: SimpleNamespace(render=lambda: "u", n=1))
    monkeypatch.setattr(eli, "build_copy_prompt", lambda raw, cb: "copy")
    monkeypatch.setattr(eli, "build_index_prompt", lambda r, n, cb: "index")
    monkeypatch.setattr(eli, "restart_ollama", MagicMock(side_effect=RuntimeError("Failed to restart Ollama")))

    def fake_call(pid, cond, prompt, nu, version, dg):
        calls.append((pid, cond))
        return eli.Row(paper_id=pid, condition=cond, ok=True, started_utc="", finished_utc="")
    monkeypatch.setattr(eli, "call", fake_call)
    rc = eli.main(["--data-root", str(tmp_path), "--restart-every", "1"])
    assert rc == 0
    assert len(calls) == 4
    assert eli.restart_ollama.call_count == 3


def test_9_capture01_continues_when_the_restart_fails(tmp_path, monkeypatch):
    """Case 9, capture01. Seam: the harness's own `main`, its module functions faked (no server, no
    model): three papers with a parsed text each, `--restart-every 1`, `restart_ollama` raising
    RuntimeError. Every paper is captured."""
    from analysis.eval import run_capture01 as cap
    parsed = tmp_path / "x" / "parsed_text"
    parsed.mkdir(parents=True)
    papers = [SimpleNamespace(paper_id=i, length_stratum="s", study_type="t") for i in (1, 2, 3)]
    for p in papers:
        (parsed / f"{p.paper_id}_v1.md").write_text("text")
    captured = []
    monkeypatch.setattr(cap, "preflight_version", lambda: "0.21.0")
    monkeypatch.setattr(cap, "model_digest", lambda: "d" * 64)
    monkeypatch.setattr(cap, "load_spec_for", lambda name, path=None: None)
    monkeypatch.setattr(cap, "select_sample", lambda review_dir: papers)
    monkeypatch.setattr(cap, "restart_ollama", MagicMock(side_effect=RuntimeError("Failed to restart Ollama")))
    monkeypatch.setattr(cap, "capture_one", lambda paper, text, spec, version, digest: (
        captured.append(paper.paper_id),
        SimpleNamespace(ok=True, pass1_content_chars=1, pass1_thinking_chars=1, done_reason="stop",
                        pass1_latency_s=0.0))[1])
    monkeypatch.setattr(cap, "append_result", lambda review_dir, label, r: None)
    monkeypatch.setattr(cap, "channel_check", lambda r: (True, "ok"))
    rc = cap.main(["--review", "x", "--data-root", str(tmp_path), "--restart-every", "1"])
    assert rc == 0
    assert captured == [1, 2, 3]
    assert cap.restart_ollama.call_count == 2


# ── 10: pdf_quality_check's bound ───────────────────────────────────────────

_ANSWER = '{"language": "English", "content_type": "full_manuscript", "confidence": 0.9, "note": ""}'


def _quality_review(tmp_path, ids):
    """A scratch review: a `papers` table with the columns the quality check reads and writes, one PDF
    file on disk per paper (its bytes never read: the render is faked)."""
    d = tmp_path / "qc"
    d.mkdir()
    conn = sqlite3.connect(d / "review.db")
    conn.execute("CREATE TABLE papers (id INTEGER PRIMARY KEY, ee_identifier TEXT, title TEXT, "
                 "pdf_local_path TEXT, pdf_quality_check_status TEXT, status TEXT, pdf_ai_language TEXT, "
                 "pdf_ai_content_type TEXT, pdf_ai_confidence REAL)")
    conn.execute("CREATE TABLE full_text_assets (paper_id INTEGER, pdf_path TEXT)")
    for i, ee in enumerate(ids, 1):
        pdf = d / f"{ee}.pdf"
        pdf.write_bytes(b"%PDF")
        conn.execute("INSERT INTO papers (id, ee_identifier, title, pdf_local_path, status) "
                     "VALUES (?, ?, 't', ?, 'PARSED')", (i, ee, str(pdf)))
    conn.commit(); conn.close()


def _quality_main(tmp_path, monkeypatch, plan):
    """Run `pdf_quality_check.main()` over papers EE-001… with `plan[i]` the outcome of every model call
    for paper i: "ok", "timeout" (TimeoutError), "unrenderable" (the render raises), or an exception
    instance. Seam: `pdf_quality_check.ollama_chat` stubbed, `_render_first_page` faked, `time.sleep`
    patched. Returns (SystemExit or None, model calls by paper)."""
    from engine.acquisition import pdf_quality_check as q
    import engine.core.review_paths as review_paths
    ids = [f"EE-{i:03d}" for i in range(1, len(plan) + 1)]
    by_ee = dict(zip(ids, plan))
    _quality_review(tmp_path, ids)
    calls = []

    def render(path, dpi=150):
        ee = Path(path).stem
        if by_ee[ee] == "unrenderable":
            raise ValueError(f"PDF has 0 pages: {path}")
        return ee

    def chat(messages, **kw):
        ee = messages[0]["images"][0]
        calls.append(ee)
        step = by_ee[ee]
        if step == "ok":
            return SimpleNamespace(message=SimpleNamespace(content=_ANSWER))
        if step == "timeout":
            raise TimeoutError(f"Ollama call timed out ({ee})")
        raise step
    monkeypatch.setattr(q, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(q, "_render_first_page", render)
    monkeypatch.setattr(q, "ollama_chat", chat)
    monkeypatch.setattr(q, "time", SimpleNamespace(sleep=lambda s: None))
    monkeypatch.setattr(review_paths, "load_spec_for", lambda name, path=None: SimpleNamespace(pdf_quality_check=None))
    monkeypatch.setattr(sys, "argv", ["pdf_quality_check", "--review", "qc"])
    try:
        q.main()
    except SystemExit as exc:
        return exc, calls
    return None, calls


def test_10a_three_read_timeouts_in_a_row_stop_the_loop_and_exit_non_zero(tmp_path, monkeypatch):
    """Case 10a. Ten papers, every call a read timeout: the loop stops after the third paper, at most
    six calls (one retry each), and main() exits non-zero naming the three papers."""
    exited, calls = _quality_main(tmp_path, monkeypatch, ["timeout"] * 10)
    assert exited is not None and exited.code not in (0, None)
    assert len(calls) <= 6 and set(calls) == {"EE-001", "EE-002", "EE-003"}
    message = str(exited.code)
    assert all(ee in message for ee in ("EE-001", "EE-002", "EE-003"))


@pytest.mark.parametrize("fault", [ConnectionError("Failed to connect to Ollama"),
                                   rm.RunRefused("run refused")], ids=["refused_connection", "run_refused"])
def test_10b_a_run_fault_on_the_first_paper_stops_the_loop_after_one_call(tmp_path, monkeypatch, fault):
    """Case 10b. A refused connection, and separately a RunRefused, on the first paper: one call (a run
    fault is not retried), the loop stops, non-zero status naming the paper."""
    exited, calls = _quality_main(tmp_path, monkeypatch, [fault, "ok", "ok"])
    assert calls == ["EE-001"]
    assert exited is not None and exited.code not in (0, None)
    assert "EE-001" in str(exited.code)


def test_10c_render_failures_do_not_stop_the_loop(tmp_path, monkeypatch):
    """Case 10c. Three papers whose page cannot be rendered, then two readable ones: no stop, the
    readable ones checked."""
    exited, calls = _quality_main(tmp_path, monkeypatch, ["unrenderable"] * 3 + ["ok", "ok"])
    assert exited is None or exited.code in (0, None)
    assert calls == ["EE-004", "EE-005"]


def test_10d_a_success_resets_the_count(tmp_path, monkeypatch):
    """Case 10d. Timeout, success, timeout, timeout, success: no stop."""
    exited, calls = _quality_main(tmp_path, monkeypatch, ["timeout", "ok", "timeout", "timeout", "ok"])
    assert exited is None or exited.code in (0, None)
    assert calls[-1] == "EE-005"
