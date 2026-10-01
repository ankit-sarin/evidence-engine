"""E-EXEC: an interrupted command-line process exits at once.

`rm.exit_process(code)` flushes and `os._exit`s for an interrupt exit code, so
interpreter shutdown never joins `ollama_chat`'s in-flight executor worker; every
other code goes through `sys.exit`. The subprocess tests use a sleeping worker in
place of a model call — no Ollama, no signal sent to any other process.
"""

from __future__ import annotations

import logging
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from engine.core import run_manifest as rm

REPO = Path(__file__).resolve().parent.parent


class _Exited(BaseException):
    def __init__(self, code):
        self.code = code


class _Stream:
    def __init__(self, name, order):
        self.name, self.order = name, order

    def write(self, _s):
        return 0

    def flush(self):
        self.order.append(self.name)


# ── T1: the helper's two paths ────────────────────────────────────────
def test_t1_the_interrupt_codes_come_from_the_signal_constants():
    assert rm.INTERRUPT_EXIT_CODES == {128 + signal.SIGINT, 128 + signal.SIGTERM,
                                       128 + signal.SIGHUP} == {130, 143, 129}


@pytest.mark.parametrize("code", sorted(rm.INTERRUPT_EXIT_CODES))
def test_t1_an_interrupt_code_flushes_then_os_exits(monkeypatch, code):
    order = []
    monkeypatch.setattr(rm, "_flush_logging_handlers", lambda: order.append("logging"))
    monkeypatch.setattr(sys, "stdout", _Stream("stdout", order))
    monkeypatch.setattr(sys, "stderr", _Stream("stderr", order))

    def fake_exit(c):
        order.append(("os._exit", c))
        raise _Exited(c)
    monkeypatch.setattr(rm.os, "_exit", fake_exit)
    with pytest.raises(_Exited):
        rm.exit_process(code)
    assert order == ["logging", "stdout", "stderr", ("os._exit", code)]


@pytest.mark.parametrize("code", [0, 1])
def test_t1_any_other_code_goes_through_sys_exit(monkeypatch, code):
    monkeypatch.setattr(rm.os, "_exit", lambda c: pytest.fail("os._exit on a normal exit"))
    with pytest.raises(SystemExit) as caught:
        rm.exit_process(code)
    assert caught.value.code == code


def test_t1_every_logger_handler_is_flushed(monkeypatch):
    flushed = []

    class _H(logging.Handler):
        def emit(self, record):
            pass

        def flush(self):
            flushed.append(self)
    h = _H()
    lg = logging.getLogger("e_exec_test_named_logger")
    lg.addHandler(h)
    try:
        rm._flush_logging_handlers()
    finally:
        lg.removeHandler(h)
    assert flushed == [h]


# ── T2 / T3: a real process with an in-flight worker ──────────────────
CHILD = textwrap.dedent("""
    import io, logging, signal, sys, time
    sys.path.insert(0, {repo!r})
    from concurrent.futures import ThreadPoolExecutor
    from engine.core import run_manifest as rm

    # A block-buffered stderr handler and an unflushed stdout line: both are
    # lost unless the helper flushes before os._exit.
    buffered = io.TextIOWrapper(io.BufferedWriter(io.FileIO(2, "w", closefd=False)))
    handler = logging.StreamHandler(buffered)
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)

    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(time.sleep, float(sys.argv[2]))   # the in-flight call
    try:
        signal.raise_signal(signal.SIGINT)              # KeyboardInterrupt, main thread
    except KeyboardInterrupt:
        code = 130
    logging.getLogger("child").info("CHILD-LOGGED-BEFORE-EXIT")
    print("CHILD-STDOUT-BEFORE-EXIT")
    if sys.argv[1] == "helper":
        rm.exit_process(code)
    sys.exit(code)
""")


def _run_child(tmp_path, mode, sleep_s):
    script = tmp_path / "child.py"
    script.write_text(CHILD.format(repo=str(REPO)))
    t0 = time.monotonic()
    proc = subprocess.run([sys.executable, str(script), mode, str(sleep_s)],
                          capture_output=True, text=True, timeout=120)
    return proc, time.monotonic() - t0


def test_t2_t3_an_interrupted_process_exits_at_once_and_its_output_survives(tmp_path):
    proc, wall = _run_child(tmp_path, "helper", 30)
    assert proc.returncode == 130
    assert wall < 5, f"held {wall:.1f}s — the worker was joined"
    assert "CHILD-LOGGED-BEFORE-EXIT" in proc.stderr          # T3: logging flushed
    assert "CHILD-STDOUT-BEFORE-EXIT" in proc.stdout          # T3: stdout flushed


def test_t2_control_sys_exit_joins_the_worker(tmp_path):
    """The control, on an 8 s worker rather than 30 s so the gate pays ~8 s:
    the same child ending with sys.exit(130) is held until the worker ends."""
    proc, wall = _run_child(tmp_path, "sys_exit", 8)
    assert proc.returncode == 130
    assert wall >= 7, f"exited in {wall:.1f}s — no join observed, the control proves nothing"
