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


# ── Live-data fence: no test may open a real review database ─────────
#
# Root cause this exists for (JUDGE-DBGUARD-01). `test_paper_366_grammar_
# prevents_four_element_emission` constructed `ReviewDatabase("surgical_
# autonomy")` with no `data_root`, which resolves to the production corpus
# database, read-write. It is marked `ollama` + `integration`, and
# `scripts/nightly_tests.sh` runs `pytest tests/ -v` with no marker
# expression, so it executed unattended every night at 09:00 UTC. It ran on
# 2026-09-08 and it ran during the PARSE-GATE-06c deviation, where the live
# file's size and mtime moved.
#
# What the open actually does, measured on copies rather than argued
# (JUDGE-DBGUARD-01 Phase 1, runs A/A2/B/C/D):
#
#   * On a database whose schema is already current, `ReviewDatabase.__init__`
#     is a **file no-op** — size and mtime unchanged, `-wal`/`-shm` created for
#     the session and removed at clean close. Measured twice.
#   * On a database missing any schema object, the same constructor **writes**:
#     dropping one table and reopening grew the file 99,770,368 -> 99,774,464
#     and moved its mtime, silently recreating the table.
#
# So the danger is not what an open does today; it is that `__init__` runs
# `executescript(_SCHEMA)`, `ensure_adjudication_table`, fifteen ALTERs, and
# migrations 006-009 unconditionally on every construction. **The live corpus
# database is one engine migration away from the nightly test run applying a
# schema change to production data with no migration step and no operator
# present.** That is not hypothetical: PARSE-GATE-03's run created
# `parse_attempts` on the live database exactly this way.
#
# Design, pinned — deliberately the same shape as the service fence above:
#
#   * **Fence the boundary, not the caller.** `ReviewDatabase.__init__` is
#     wrapped once on the class, so every construction is covered no matter
#     which module it lives in or how the name was imported.
#
#   * **Refuse by resolved ancestry, never by review name.** `DATA_ROOT` is the
#     relative `Path("data")` and resolves against the process CWD, so a string
#     match on "surgical_autonomy" would miss both a CWD change and the next
#     live review. Both sides are `Path.resolve()`d and compared with
#     `is_relative_to`.
#
#   * **The guard never opens a database.** It resolves paths and nothing else,
#     so it cannot itself become the thing it exists to prevent.
#
#   * **Refuse reads too.** A read-only open of the live file is still an open:
#     it creates `-wal`/`-shm` sidecars, and `ReviewDatabase` has no read-only
#     mode to ask for. Tests that need real corpus rows copy the database to
#     `tmp_path` first — see `test_judge_pass2.py::live_review_copy`.
#
#   * **Raise a `BaseException`.** Same reasoning as `ServiceCallBlocked`:
#     application code that catches `Exception` around a database open must not
#     be able to swallow the refusal and go green.
#
# `@pytest.mark.dbguard_selftest` exempts the guard's own tests, which assert
# the raise with `pytest.raises` and would otherwise be failed by the teardown
# re-assert.

from _live_db_guard import (  # noqa: E402
    LiveDatabaseBlocked,
    is_live_data_path,
    refuse as _refuse_live_db,
    resolved_review_db_path,
    violations as _db_violations,
)

__all__ = ["ServiceCallBlocked", "LiveDatabaseBlocked"]


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "dbguard_selftest: exercises the live-data fence and expects it to fire",
    )


# ── Every review directory carries a codebook ────────────────────────
#
# CODEBOOK-AUTH-01 R4 closed the read-side tolerance: the five D1 consumers
# (the auditor's audit call and its LOW_YIELD count, the categorical
# normaliser, the distribution monitor's observation set and cross-arm
# concordance scoring) now propagate a CodebookError instead of quietly
# treating a missing codebook as "this review declares no terminal-state
# tokens" — which is not inert, it makes every terminal state score, audit and
# count as a real extracted value.
#
# In production that condition cannot arise: a review directory always holds a
# codebook. In tests it arose in sixty-five places, because a temp
# ReviewDatabase created the directory tree and nothing put one in it. Writing
# it here models the production invariant once rather than in fifteen
# fixtures. A test that means to exercise a MISSING codebook builds its review
# directory directly instead of through ReviewDatabase.
#
# Only ever inside a test's own tree: the live-DB guard above has already
# refused anything under data/ by the time this runs.

_TEST_CODEBOOK = {
    "version": "1.0",
    "date": "2026-01-01",
    "escape_token": "NO_EVIDENCE_LOCATABLE",
    "contract_unmet_token": "CONTRACT_UNMET",
    "absence_sentinels": ["NR", "N/A", "NA", "NOT_FOUND", "NOT FOUND", "NOT REPORTED"],
    "fields": [
        {"name": "study_type", "type": "categorical", "tier": 1,
         "definition": "The study design.", "instruction": "Classify it.",
         "field_class": "stated", "judge_rubric_family": "categorical",
         "valid_values": [{"value": "RCT", "definition": "Randomised."},
                          {"value": "Cohort", "definition": "Not randomised."}]},
    ],
}


def _ensure_test_codebook(review_dir, review_id: str) -> None:
    from pathlib import Path as _Path

    path = _Path(review_dir) / "extraction_codebook.yaml"
    if path.exists():
        return
    import yaml

    try:
        path.write_text(yaml.safe_dump(dict(_TEST_CODEBOOK, review=review_id)))
    except OSError:  # pragma: no cover - a read-only tmp tree
        pass


@pytest.fixture(autouse=True, scope="function")
def block_live_database(monkeypatch, request):
    """Refuse any ReviewDatabase construction under the real data/ tree."""
    from engine.core import database as db_module

    original_init = db_module.ReviewDatabase.__init__

    def guarded_init(self, review_name, data_root=None, *args, **kwargs):
        candidate = resolved_review_db_path(
            review_name, data_root, db_module.DATA_ROOT
        )
        if is_live_data_path(candidate):
            _refuse_live_db(candidate, request.node.nodeid)
        result = original_init(self, review_name, data_root, *args, **kwargs)
        _ensure_test_codebook(self.db_path.parent, review_name)
        return result

    monkeypatch.setattr(db_module.ReviewDatabase, "__init__", guarded_init)

    before = len(_db_violations)
    yield
    # Safety net for a test that catches BaseException and passes anyway, with
    # the same quiet-on-failure rule as the service fence.
    new = _db_violations[before:]
    expected = request.node.get_closest_marker("dbguard_selftest") is not None
    if new and not expected and not request.node.stash.get(_PHASE_FAILED, False):
        pytest.fail(
            "a live review database was opened during this test:\n"
            + "\n".join(new)
        )
