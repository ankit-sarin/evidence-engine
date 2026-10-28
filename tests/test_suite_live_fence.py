"""The suite's own rules about the experiment lock and the Ollama server (U12 commit 1).

R625(a)(1), R625(a)(2); rows INT-m12-1 and INT-m12-2. Three rules, all in
`tests/conftest.py`:

  * no test takes the real experiment lock — every test gets its own lock path;
  * a request to the Ollama server from a test without the `ollama` marker is
    refused and fails the test;
  * `ollama`-, `network`- and `integration`-marked tests skip unless
    `EVIDENCE_ENGINE_LIVE_TIERS=1`, and under it they hold the lock
    non-blocking and skip when someone else holds it.

Everything but T1 is an inner pytest run, because the subject is what the
conftest does to a *test* — its outcome, its skip reason, whether its body ran.
The inner run loads this suite's conftest as a plugin, points `OLLAMA_HOST` at a
port nothing listens on and `OLLAMA_EXPERIMENT_LOCK` at a temp file, so no inner
test can reach a server or the real lock even on a tree where the rules are
missing. No inner body makes a model call, and none names the real server's
address.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import textwrap
from pathlib import Path

# Imported here, before any test moves OLLAMA_HOST: the module builds its client
# at import, and a first import inside such a test would bind it to that host.
from engine.utils import ollama_client  # noqa: F401
from engine.utils import ollama_lock as L

REPO_ROOT = Path(__file__).resolve().parent.parent
INNER_TIMEOUT_S = 120  # a hang is a failure, never a wait (T4)


def _unused_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _inner(tmp_path: Path, source: str, *args: str, live_tiers: bool = False,
           lock: Path | None = None) -> subprocess.CompletedProcess:
    """Run `source` as a test file under this suite's conftest, in a child pytest."""
    test_file = tmp_path / "test_inner.py"
    test_file.write_text(textwrap.dedent(source))
    env = {k: v for k, v in os.environ.items()
           if k not in ("EVIDENCE_ENGINE_LIVE_TIERS", "PYTEST_CURRENT_TEST")}
    env["PYTHONPATH"] = os.pathsep.join([str(REPO_ROOT / "tests"), str(REPO_ROOT)])
    env["OLLAMA_HOST"] = f"127.0.0.1:{_unused_port()}"
    env["OLLAMA_EXPERIMENT_LOCK"] = str(lock or tmp_path / "inner_experiment.lock")
    env["INNER_BODY_RAN"] = str(tmp_path / "body_ran")
    if live_tiers:
        env["EVIDENCE_ENGINE_LIVE_TIERS"] = "1"
    target = [str(test_file)] if not args else [f"{test_file}::{a}" for a in args]
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "conftest", "-p", "no:cacheprovider",
         "-c", str(REPO_ROOT / "pyproject.toml"), "--basetemp", str(tmp_path / "inner_tmp"),
         "-rA", "-q", *target],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=INNER_TIMEOUT_S,
    )


def _ran(tmp_path: Path) -> list[str]:
    marker = tmp_path / "body_ran"
    return marker.read_text().split() if marker.exists() else []


_BODY = """
    import os
    def _mark(name):
        with open(os.environ["INNER_BODY_RAN"], "a") as fh:
            fh.write(name + "\\n")
"""


# ── T1 — the lock a test would take is its own ─────────────────────────

def test_an_unmarked_test_resolves_the_lock_under_its_tmp_path(tmp_path):
    assert L.lock_path().is_relative_to(tmp_path)
    assert L.lock_path() != L._DEFAULT_LOCK_PATH


# ── T2 — every route to the server is refused in an unmarked test ──────

_ROUTES = _BODY + """
    import socket, urllib.request
    import httpx, ollama, requests

    def _url():
        from ollama._client import _parse_host
        return _parse_host(os.environ["OLLAMA_HOST"])

    def test_httpx_get():
        _mark("httpx"); httpx.get(_url() + "/api/tags", timeout=2)

    def test_ollama_client():
        _mark("client"); ollama.Client().ps()

    def test_ollama_module_default_client():
        _mark("module"); ollama.ps()

    def test_requests_get():
        _mark("requests"); requests.get(_url() + "/api/tags", timeout=2)

    def test_urllib():
        _mark("urllib"); urllib.request.urlopen(_url() + "/api/tags", timeout=2)

    def test_raw_socket():
        _mark("socket")
        host, port = os.environ["OLLAMA_HOST"].split(":")
        socket.create_connection((host, int(port)), timeout=2)

    def test_a_swallowed_refusal_still_fails():
        _mark("swallowed")
        try:
            httpx.get(_url() + "/api/tags", timeout=2)
        except BaseException:
            pass
"""


def test_an_unmarked_test_is_refused_on_every_route_to_the_ollama_server(tmp_path):
    r = _inner(tmp_path, _ROUTES)
    out = r.stdout + r.stderr
    # six raise through the test; the seventh swallows the refusal, so its call
    # passes and the fence's teardown re-assert turns it into an error
    assert "6 failed, 1 passed, 1 error" in out, out
    assert "ERROR ::test_a_swallowed_refusal_still_fails" in out, out
    assert len(_ran(tmp_path)) == 7
    for route in ("httpx transport", "requests", "socket.connect"):
        assert f"via {route}" in out, (route, out)
    # the refusal names the test, and nothing was a connection attempt
    assert "BLOCKED: ::test_httpx_get sent a request to the Ollama server" in out, out
    assert "ConnectError" not in out and "Connection refused" not in out, out


def test_the_fenced_server_is_OLLAMA_HOST_and_the_addresses_the_engine_names(monkeypatch):
    """The engine reads one host, resolved at import (U18): the fence covers it, both
    loopback spellings of the service's default address, and whatever OLLAMA_HOST says
    now — a test may move the variable after the engine client was built."""
    from conftest import _url_endpoint, ollama_endpoints

    from engine.utils import ollama_client as oc

    monkeypatch.setenv("OLLAMA_HOST", "10.1.2.3:4567")
    assert {("10.1.2.3", 4567), ("loopback", 11434)} <= ollama_endpoints()
    assert _url_endpoint(oc.ollama_host()) in ollama_endpoints()
    monkeypatch.setenv("OLLAMA_HOST", "localhost:4567")
    assert ("loopback", 4567) in ollama_endpoints()


def test_the_fenced_server_includes_the_second_server_on_loopback():
    """R626(c): `run_qualgap01` targets port 11435 by default. Every loopback name
    is the same host to the fence."""
    from conftest import _endpoint, ollama_endpoints

    from analysis.eval.qualgap01 import DEFAULT_HOST

    assert DEFAULT_HOST.endswith(":11435")
    for name in ("127.0.0.1", "localhost", "::1"):
        assert _endpoint(name, 11435) in ollama_endpoints()
    assert _endpoint("127.0.0.1", 11436) not in ollama_endpoints()


# ── T3 — without the opt-in a live-tier test skips, by node id too ─────

_LIVE = _BODY + """
    import pytest
    from engine.utils import ollama_lock as L

    @pytest.mark.ollama
    def test_live_ollama():
        _mark("ollama")
        assert os.environ.get("OLLAMA_EXPERIMENT_LOCK") == str(L.lock_path())
        assert L.self_holds_lock()            # the conftest holds it for the test
        with L.hold_experiment_lock():        # blocking: re-entrant, or this hangs
            assert L.self_holds_lock()

    @pytest.mark.ollama
    def test_live_ollama_is_not_fenced():
        import httpx
        from ollama._client import _parse_host
        _mark("exempt")
        with pytest.raises(httpx.ConnectError):   # reached the (unused) port
            httpx.get(_parse_host(os.environ["OLLAMA_HOST"]) + "/api/tags", timeout=2)

    @pytest.mark.network
    def test_live_network_is_still_fenced_from_ollama():
        import httpx
        from ollama._client import _parse_host
        _mark("network")
        httpx.get(_parse_host(os.environ["OLLAMA_HOST"]) + "/api/tags", timeout=2)

    @pytest.mark.integration
    def test_live_integration():
        _mark("integration")
        assert L.self_holds_lock()
"""


def test_without_the_opt_in_a_live_tier_test_named_by_node_id_is_skipped(tmp_path):
    r = _inner(tmp_path, _LIVE, "test_live_ollama", "test_live_ollama_is_not_fenced",
               "test_live_network_is_still_fenced_from_ollama", "test_live_integration")
    out = r.stdout + r.stderr
    assert "4 skipped" in out and "passed" not in out and "failed" not in out, out
    assert "EVIDENCE_ENGINE_LIVE_TIERS=1" in out, out
    assert _ran(tmp_path) == []


# ── T4 — with the opt-in and a free lock, it runs holding the lock ─────

def test_with_the_opt_in_a_live_tier_test_runs_holding_the_lock(tmp_path):
    lock = tmp_path / "redirected.lock"
    r = _inner(tmp_path, _LIVE, live_tiers=True, lock=lock)
    out = r.stdout + r.stderr
    assert "3 passed" in out and "1 failed" in out and "skipped" not in out, out
    assert sorted(_ran(tmp_path)) == ["exempt", "integration", "network", "ollama"]
    assert "FAILED" in out and "test_live_network_is_still_fenced_from_ollama" in out
    assert "OllamaCallBlocked" in out
    assert lock.exists()                      # the lock taken was the configured one
    assert L.check_experiment_lock() is False


# ── T5 — with the opt-in and the lock held elsewhere, it skips ─────────

_HOLDER = """
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o644)
fcntl.flock(fd, fcntl.LOCK_EX)
print("held", flush=True)
sys.stdin.read()
"""


def test_with_the_opt_in_and_the_lock_held_elsewhere_a_live_tier_test_skips(tmp_path):
    lock = tmp_path / "redirected.lock"
    holder = subprocess.Popen([sys.executable, "-c", _HOLDER, str(lock)],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "held"
        r = _inner(tmp_path, _LIVE, live_tiers=True, lock=lock)
    finally:
        holder.stdin.close()
        holder.wait(timeout=10)
    out = r.stdout + r.stderr
    assert "4 skipped" in out and "passed" not in out and "failed" not in out, out
    assert str(lock) in out and "experiment lock is held" in out, out
    assert _ran(tmp_path) == []
