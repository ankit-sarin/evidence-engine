"""Suite-wide fence: no test may touch a system service.

Root cause this exists for (OPSFIX-01). `test_timeout_logs_warning` patched the
Ollama *client* but not the `subprocess` boundary, so when `ollama_chat`'s
last-resort recovery fired it ran a real `sudo systemctl restart ollama`. A
NOPASSWD sudoers rule for exactly that command let it through, so **every offline
suite run restarted the production Ollama service** — confirmed by journal
timestamp during QUALGAP-01 (14:56:09, 15:49:23) and again by that task's own
acceptance-gate run (2026-08-23 04:58:52). A 15-hour experiment on the same box
would have been destroyed by a `pytest` invocation.

Patching that one test would fix that one test. This fences the whole suite
instead, so the next test to reach a service call fails instead of succeeding
quietly.

Design, pinned:

  * **Fence the boundary, not the caller.** `subprocess.run/Popen/call/
    check_call/check_output` and `os.system` are wrapped once, on the stdlib
    modules, so every caller is covered no matter which module it lives in and
    no matter how it was imported.

  * **Block on the command, allow everything else.** Only argv naming a service
    manager (`systemctl`, `service`, `sudo`, `shutdown`, `reboot`, `init`,
    `telinit`) is refused. Tests that legitimately spawn subprocesses — the
    flock holders in `test_ollama_lock.py` and `test_restart_ollama_guard.py`
    spawn `sys.executable` — pass through untouched.

  * **Raise a `BaseException`, not an `Exception`.** This is the load-bearing
    detail. `_restart_ollama_and_retry` wraps its restart in `except Exception`
    and re-raises as `RuntimeError`, which `ollama_chat` converts to
    `TimeoutError` — the very outcome `test_timeout_logs_warning` asserts. A
    fence raising `Exception` would therefore be swallowed and the test would go
    green while the fence "worked", which is indistinguishable from the bug.
    `ServiceCallBlocked` derives from `BaseException` (the same reason
    `pytest.fail` does) so it propagates through application error handling.

  * **Belt and braces.** Every violation is also recorded and re-asserted at
    teardown, so a test that catches `BaseException` still fails. The teardown
    check stays quiet when the test already failed, so a single violation is
    reported once rather than as a failure plus a teardown error, and
    `@pytest.mark.fence_selftest` exempts the fence's own tests — they assert
    the raise via `pytest.raises`, so for them a recorded violation is the
    expected result rather than an escape.

**No tier is exempt, including the nightly full-suite run.** The exemption was
considered and is not needed: the `ollama`-marked tier loads models over HTTP and
the `network`/`integration` tiers hit APIs and parse PDFs — none of them manage
a service. Nothing in this repository has a legitimate reason to restart Ollama
from inside a test, so the fence is unconditional and `scripts/nightly_tests.sh`
(which runs `pytest tests/` with no marker filter) is covered by it too.
"""

from __future__ import annotations

import os
import subprocess

import pytest

# Service managers and privilege escalation. Matched on the command basename, so
# `/usr/bin/systemctl` and `systemctl` are both caught.
BLOCKED_COMMANDS = frozenset({
    "systemctl", "service", "sudo", "doas", "pkexec",
    "shutdown", "reboot", "halt", "poweroff", "init", "telinit",
})


class ServiceCallBlocked(BaseException):
    """A test tried to invoke a service manager.

    Deliberately a BaseException: application code that catches `Exception`
    around a restart must not be able to swallow this and turn a fenced call
    into a passing test. See the module docstring.
    """


_violations: list[str] = []


def _argv_words(cmd) -> list[str]:
    """Flatten a subprocess command into comparable words.

    Handles both the list form (`["sudo", "systemctl", ...]`) and the string
    form used with `shell=True`, and tolerates `Path` and `bytes` arguments.
    """
    if cmd is None:
        return []
    if isinstance(cmd, (str, bytes, os.PathLike)):
        text = os.fsdecode(cmd)
        return text.replace(";", " ").replace("|", " ").replace("&", " ").split()
    if isinstance(cmd, (list, tuple)):
        words = []
        for part in cmd:
            if isinstance(part, (str, bytes, os.PathLike)):
                words.append(os.fsdecode(part))
            else:
                words.append(str(part))
        return words
    return [str(cmd)]


def _blocked_word(cmd) -> str | None:
    """Return the offending word, or None if the command is allowed."""
    for word in _argv_words(cmd):
        # basename so an absolute path still matches; strip any `env`-style
        # VAR=value prefixes that would otherwise hide the real command.
        if "=" in word and not word.startswith("/"):
            continue
        if os.path.basename(word) in BLOCKED_COMMANDS:
            return word
    return None


def _refuse(cmd, api: str):
    offender = _blocked_word(cmd)
    if offender is None:
        return
    rendered = " ".join(_argv_words(cmd))
    message = (
        f"BLOCKED: test attempted a service-manager call via {api}: {rendered!r} "
        f"(matched {offender!r}).\n"
        f"Tests must never touch a system service — a real `systemctl restart "
        f"ollama` from the suite destroys any experiment running on this box "
        f"(OPSFIX-01).\n"
        f"Fix: patch the subprocess boundary, e.g. "
        f"@patch('engine.utils.ollama_client.subprocess.run')."
    )
    _violations.append(message)
    raise ServiceCallBlocked(message)


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    """Stash each phase's outcome so the fence teardown can stay quiet on failure."""
    outcome = yield
    report = outcome.get_result()
    item.stash.setdefault(_PHASE_FAILED, False)
    if report.failed:
        item.stash[_PHASE_FAILED] = True


_PHASE_FAILED = pytest.StashKey[bool]()


@pytest.fixture(autouse=True, scope="function")
def block_service_calls(monkeypatch, request):
    """Refuse any service-manager invocation for the duration of every test."""
    real = {
        "run": subprocess.run,
        "Popen": subprocess.Popen,
        "call": subprocess.call,
        "check_call": subprocess.check_call,
        "check_output": subprocess.check_output,
    }
    real_system = os.system

    def _wrap(name):
        original = real[name]

        def fenced(*args, **kwargs):
            _refuse(args[0] if args else kwargs.get("args"), f"subprocess.{name}")
            return original(*args, **kwargs)

        return fenced

    for name in real:
        monkeypatch.setattr(subprocess, name, _wrap(name))

    def fenced_system(command):
        _refuse(command, "os.system")
        return real_system(command)

    monkeypatch.setattr(os, "system", fenced_system)

    before = len(_violations)
    yield
    # Safety net for a test that catches BaseException and passes anyway. If the
    # test already failed, the ServiceCallBlocked traceback has said everything
    # this would, so stay quiet rather than double-reporting one violation.
    new = _violations[before:]
    expected = request.node.get_closest_marker("fence_selftest") is not None
    if new and not expected and not request.node.stash.get(_PHASE_FAILED, False):
        pytest.fail("service-manager call was blocked during this test:\n" + "\n".join(new))
