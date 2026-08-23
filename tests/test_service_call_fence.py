"""The suite fence itself must work — kept as a permanent negative test.

OPSFIX-01's acceptance gate was "a deliberate probe hitting systemctl fails
loudly". Rather than probe once and delete it, the probe stays: the fence is
infrastructure that fails *open* if it breaks (a broken fence looks exactly like
a suite with nothing to block), so it needs a test that goes red when it stops
working. None of these tests can execute a service command — the fence refuses
before the real `subprocess.run` is ever reached, which is the property under
test.
"""

from __future__ import annotations

import os
import subprocess

import pytest

from conftest import BLOCKED_COMMANDS, ServiceCallBlocked, _blocked_word


# ── the probe: the exact call that was restarting production ─────────────


@pytest.mark.fence_selftest
def test_the_original_bug_is_now_blocked():
    """`sudo systemctl restart ollama` — what every offline run used to do."""
    with pytest.raises(ServiceCallBlocked, match="systemctl"):
        subprocess.run(["sudo", "systemctl", "restart", "ollama"], check=False)


@pytest.mark.fence_selftest
def test_bare_systemctl_is_blocked_without_sudo():
    with pytest.raises(ServiceCallBlocked):
        subprocess.run(["systemctl", "restart", "ollama"], check=False)


@pytest.mark.fence_selftest
def test_absolute_path_is_blocked():
    """The sudoers rule names /usr/bin/systemctl, so the path form must match too."""
    with pytest.raises(ServiceCallBlocked):
        subprocess.run(["/usr/bin/systemctl", "restart", "ollama"], check=False)


@pytest.mark.fence_selftest
def test_shell_string_form_is_blocked():
    with pytest.raises(ServiceCallBlocked):
        subprocess.run("sudo systemctl restart ollama", shell=True, check=False)


@pytest.mark.fence_selftest
def test_os_system_is_blocked():
    with pytest.raises(ServiceCallBlocked):
        os.system("systemctl restart ollama")


@pytest.mark.parametrize("api", ["call", "check_call", "check_output", "Popen"])
@pytest.mark.fence_selftest
def test_every_subprocess_entry_point_is_fenced(api):
    with pytest.raises(ServiceCallBlocked):
        getattr(subprocess, api)(["systemctl", "restart", "ollama"])


# ── the fence must not break legitimate subprocess use ───────────────────


def test_ordinary_subprocess_calls_still_work():
    """`test_ollama_lock` and `test_restart_ollama_guard` spawn real helpers."""
    out = subprocess.run(["echo", "hello"], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "hello"


def test_spawning_python_is_not_blocked():
    import sys
    out = subprocess.run([sys.executable, "-c", "print(7 * 6)"],
                         capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "42"


def test_a_command_merely_mentioning_a_service_is_not_blocked():
    """Matching is on the command word, not a substring of the whole line."""
    out = subprocess.run(["echo", "restarting the ollama service"],
                         capture_output=True, text=True, check=True)
    assert "ollama" in out.stdout


# ── predicate unit tests ─────────────────────────────────────────────────


def test_blocked_word_ignores_env_style_prefixes():
    assert _blocked_word(["FOO=sudo", "echo", "hi"]) is None


@pytest.mark.parametrize("cmd", sorted(BLOCKED_COMMANDS))
def test_every_declared_command_is_actually_caught(cmd):
    assert _blocked_word([cmd, "whatever"]) == cmd


@pytest.mark.fence_selftest
def test_blocked_exception_is_not_catchable_as_exception():
    """The load-bearing property: application `except Exception` must not eat it.

    `_restart_ollama_and_retry` wraps its restart in `except Exception` and
    re-raises RuntimeError, which `ollama_chat` turns into TimeoutError — the
    exact outcome `test_timeout_logs_warning` asserts. If the fence were an
    ordinary Exception it would be swallowed and the test would pass while the
    service was still being restarted.
    """
    assert issubclass(ServiceCallBlocked, BaseException)
    assert not issubclass(ServiceCallBlocked, Exception)

    with pytest.raises(ServiceCallBlocked):
        try:
            subprocess.run(["systemctl", "restart", "ollama"], check=False)
        except Exception:  # noqa: BLE001 - deliberately mimics the engine's handler
            pytest.fail("the fence was swallowed by `except Exception`")
