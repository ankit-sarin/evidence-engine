"""The nightly script runs the standard gate and reports pytest's own status.

U12 commit 5 (R625(a)(1), R635(c); rows A-1N and B-F17 (f)). Before it,
`scripts/nightly_tests.sh` ran pytest with no marker expression — the model tier
included, every night at 09:00 UTC — and ended with an unconditional `exit 0`, so its
status was success whatever the suite did. A dry run checked the new script once; these
tests keep it checked.

Nothing here runs the real suite, writes under `logs/`, reaches Ollama or takes a lock.
The two run cases execute the script itself under a scratch `$HOME`: the script derives
its project directory, virtualenv and log directory from `$HOME`, so a scratch home with
an empty `activate` and a `python` shim first on `PATH` stands in for all three.
"""

from __future__ import annotations

import ast
import os
import re
import stat
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "nightly_tests.sh"
#: The morning digest's failure test on the nightly log (dgx-infra `morning_digest.sh`).
DIGEST_FAILURE = re.compile(r"^FAILED |[0-9]+ failed", re.M)


def _code_lines() -> list[str]:
    return [line.strip() for line in SCRIPT.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")]


def _standard_gate() -> str:
    values = re.findall(r'^STANDARD_GATE="([^"]*)"$', SCRIPT.read_text(), re.M)
    assert len(values) == 1, "the expression is written once, in the script"
    return values[0]


# ── (a) the selection is the standard gate's, written once ───────────


def test_the_nightly_script_selects_the_standard_gate_written_once():
    gate = _standard_gate()
    pytest_lines = [line for line in _code_lines() if "-m pytest" in line]
    assert len(pytest_lines) == 1, pytest_lines
    assert '-m "$STANDARD_GATE"' in pytest_lines[0] and "--durations=0" in pytest_lines[0]
    assert "EVIDENCE_ENGINE_LIVE_TIERS=" not in SCRIPT.read_text()      # never opted in here

    # The standard gate's expression is recorded in one place besides the script:
    # CLAUDE.md's "Running" block. The two must be the same string.
    recorded = re.findall(r'-m "([^"]+)"\s+# standard gate', (REPO / "CLAUDE.md").read_text())
    assert recorded == [gate], recorded

    # And it deselects exactly the tiers the suite calls live (tests/conftest.py).
    conftest = ast.parse((REPO / "tests" / "conftest.py").read_text())
    live = [ast.literal_eval(node.value) for node in conftest.body
            if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "LIVE_TIER_MARKERS"]
    assert len(live) == 1
    assert sorted(gate.split(" and ")) == sorted(f"not {marker}" for marker in live[0])


# ── (b) the script's status is pytest's ──────────────────────────────


def test_the_nightly_script_has_no_unconditional_exit_and_exits_with_pytests_code():
    lines = _code_lines()
    assert "exit 0" not in lines                               # the old last line
    assert lines[-1] == 'exit "$rc"'
    assert sum(1 for line in lines if line.startswith("exit")) == 1
    (pytest_line,) = [line for line in lines if "-m pytest" in line]
    assert pytest_line.endswith("|| rc=$?")                    # a failing suite does not end the script early
    assert lines.index('echo "pytest exit code: $rc"') > lines.index(pytest_line)
    assert "set -euo pipefail" in lines


# ── (c), (d) run the script on a shim ────────────────────────────────

_SHIM = """#!/bin/bash
printf '%s\\n' "$@" > "$SHIM_ARGV"
echo "============================= test session starts =============================="
echo "collecting ... collected 3 items / 1 deselected / 2 selected"
echo "============================= slowest durations ============================="
echo "==== $SHIM_SUMMARY in 0.01s ===="
exit "$SHIM_EXIT"
"""


def _run(tmp_path: Path, exit_code: int, summary: str):
    """The script under a scratch $HOME. Returns (completed process, log text, durations
    path, the shim's argv)."""
    home = tmp_path / "home"
    project = home / "projects" / "evidence-engine"
    (project / ".venv" / "bin").mkdir(parents=True)
    (project / ".venv" / "bin" / "activate").write_text("")          # sourced; sets nothing
    shim_dir = tmp_path / "bin"
    shim_dir.mkdir()
    shim = shim_dir / "python"
    shim.write_text(_SHIM)
    shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
    argv_file = tmp_path / "argv.txt"
    env = {"HOME": str(home), "PATH": f"{shim_dir}:/usr/bin:/bin", "SHIM_ARGV": str(argv_file),
           "SHIM_EXIT": str(exit_code), "SHIM_SUMMARY": summary}
    done = subprocess.run(["/bin/bash", str(SCRIPT)], env=env, cwd=tmp_path,
                          capture_output=True, text=True, timeout=60)
    logs = sorted((project / "logs").glob("nightly_test_*.log"))
    assert len(logs) == 1, (done.returncode, done.stdout, done.stderr)
    durations = project / "logs" / logs[0].name.replace("nightly_test_", "nightly_durations_").replace(".log", ".txt")
    return done, logs[0].read_text(), durations, argv_file.read_text().splitlines()


@pytest.mark.parametrize("exit_code,summary", [
    pytest.param(0, "2 passed, 1 deselected", id="exit_0"),
    pytest.param(3, "1 failed, 1 passed, 1 deselected", id="exit_3"),
])
def test_nightly_script_runs_the_standard_gate_and_propagates_status(tmp_path, exit_code, summary):
    done, log, durations, argv = _run(tmp_path, exit_code, summary)

    # the status is pytest's, on the last log line and as the script's own exit
    assert done.returncode == exit_code, (done.stdout, done.stderr)
    assert log.rstrip("\n").splitlines()[-1] == f"pytest exit code: {exit_code}"
    assert (done.stdout, done.stderr) == ("", "")              # cron mails nothing: all output is in the log

    # the durations file is written beside the log, with the same date stamp
    assert durations.exists() and durations.read_text() == ""  # the shim's durations section is empty

    # pytest was asked for the standard gate, with durations
    assert argv[:3] == ["-m", "pytest", "tests/"]
    assert argv[argv.index("-m", 1) + 1] == _standard_gate()
    assert "--durations=0" in argv

    # what the morning digest's failure grep sees
    hits = [line for line in log.splitlines() if DIGEST_FAILURE.search(line)]
    if exit_code == 0:
        assert hits == []                                      # neither the header nor the exit-code line
    else:
        assert hits == [f"==== {summary} in 0.01s ===="]       # the summary's "1 failed", and nothing else

    # nothing was written outside the scratch home
    assert not (tmp_path / "logs").exists()
    assert os.path.commonpath([durations, tmp_path]) == str(tmp_path)
