"""Every entry point that can call a local model or restart Ollama holds the experiment lock.

U12 commit 2 (R625(b), R629(c); rows A-1, INT-g12-7). The census is static and finds its
own entry points: nothing here lists one. Before this commit twelve entry points reached
`ollama_chat` with no lock, and `run_pipeline` held it for the extract stage only, so the
07:00 health job could run its inference loop against the server an audit was using (N19).

Cloud-only entry points are outside the rule (R625(a)(3)): they reach no sink.
"""

import ast
import textwrap
from pathlib import Path

from _lock_census import SINKS, TREES, census, sinks_defined

REPO = Path(__file__).resolve().parents[1]


def _failures(rows) -> list[str]:
    return [f"{r['entry']}: {label} reachable outside `with hold_experiment_lock()` — {chain}"
            for r in rows for label, chain in sorted(r["unlocked"].items())]


# ── T1 — the acceptance test ─────────────────────────────────────────


def test_every_model_calling_entry_point_holds_the_experiment_lock():
    rows = census(REPO)
    failures = _failures(rows)
    assert not failures, "\n".join(failures)
    # Not vacuous: the sinks are still where the census looks for them, and entry points
    # in each tree do reach one under the lock.
    assert all(sinks_defined(REPO).values()), sinks_defined(REPO)
    locked_trees = {r["entry"].split("/")[0] for r in rows if r["locked"]}
    assert locked_trees == set(TREES), locked_trees


# ── T2 — the census can fail ─────────────────────────────────────────

_CLIENT = "def ollama_chat(**kw):\n    return _restart_ollama_and_retry()\n\ndef _restart_ollama_and_retry():\n    pass\n"
_LOCK = "from contextlib import contextmanager\n\n@contextmanager\ndef hold_experiment_lock():\n    yield\n"
_EXTRACTOR = "def restart_ollama():\n    pass\n"
_AGENT = "from engine.utils.ollama_client import ollama_chat\n\ndef screen(paper):\n    return ollama_chat(model='m')\n"


def _tree(root: Path, scripts: dict[str, str]) -> Path:
    files = {"engine/__init__.py": "", "engine/utils/__init__.py": "", "engine/agents/__init__.py": "",
             "engine/utils/ollama_client.py": _CLIENT, "engine/utils/ollama_lock.py": _LOCK,
             "engine/agents/extractor.py": _EXTRACTOR, "engine/agents/screener.py": _AGENT,
             "analysis/__init__.py": ""}
    files.update({f"scripts/{name}": textwrap.dedent(src) for name, src in scripts.items()})
    for rel, src in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(src)
    return root


_UNLOCKED = """
    from engine.agents.screener import screen

    def main():
        return screen({})

    if __name__ == "__main__":
        main()
"""
_LOCKED = """
    from engine.agents.screener import screen
    from engine.utils.ollama_lock import hold_experiment_lock

    def main():
        with hold_experiment_lock():
            return screen({})

    if __name__ == "__main__":
        main()
"""
_PART = """
    from engine.agents import extractor
    from engine.agents.screener import screen
    from engine.utils import ollama_lock

    def main():
        with ollama_lock.hold_experiment_lock():
            screen({})
        extractor.restart_ollama()

    if __name__ == "__main__":
        main()
"""
_NO_SINK = """
    def main():
        return 0

    if __name__ == "__main__":
        main()
"""
_NOT_AN_ENTRY = """
    from engine.agents.screener import screen

    def helper():
        return screen({})
"""


def test_an_entry_point_with_an_unlocked_path_fails_the_census_and_names_its_chain(tmp_path):
    rows = census(_tree(tmp_path, {"run.py": _UNLOCKED}))
    failures = _failures(rows)
    assert len(failures) == 2, failures      # the call, and the restart it can reach
    assert failures[1].startswith("scripts/run.py: local model call reachable outside")
    assert ("scripts.run::main -> engine.agents.screener::screen -> "
            "engine.utils.ollama_client::ollama_chat") in failures[1]


def test_the_same_entry_point_under_the_wrapper_passes(tmp_path):
    rows = census(_tree(tmp_path, {"run.py": _LOCKED}))
    assert _failures(rows) == []
    assert "[locked]" in rows[0]["locked"]["local model call"]


def test_a_sink_reached_after_the_wrapper_closes_still_fails(tmp_path):
    failures = _failures(census(_tree(tmp_path, {"run.py": _PART})))
    assert len(failures) == 1 and "proactive Ollama restart" in failures[0], failures
    assert "engine.agents.extractor::restart_ollama" in failures[0]


def test_entry_points_are_found_by_their_main_guard_and_only_those(tmp_path):
    rows = census(_tree(tmp_path, {"a.py": _UNLOCKED, "b.py": _NO_SINK, "lib.py": _NOT_AN_ENTRY}))
    assert [r["entry"] for r in rows] == ["scripts/a.py", "scripts/b.py"]
    assert rows[1] == {"entry": "scripts/b.py", "unlocked": {}, "locked": {}}


# ── T3 — the sink list is complete ───────────────────────────────────

_CLIENT_FILE = "engine/utils/ollama_client.py"
_RESTART_FILES = {"engine/utils/ollama_client.py", "engine/agents/extractor.py"}


def _sources():
    for tree in TREES:
        for path in sorted((REPO / tree).rglob("*.py")):
            yield path.relative_to(REPO).as_posix(), ast.parse(path.read_text(encoding="utf-8"))


def _last_name(node) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _client_generation_sites(tree) -> list[int]:
    """`ollama.chat`, `ollama.generate`, `<anything>._client.chat|generate` — called or passed."""
    return [n.lineno for n in ast.walk(tree)
            if isinstance(n, ast.Attribute) and n.attr in ("chat", "generate")
            and _last_name(n.value) in ("ollama", "_client")]


def _restart_command_sites(tree) -> list[int]:
    """An argv list, or a shell string handed to a call, that restarts the service."""
    hits = []
    for n in ast.walk(tree):
        if isinstance(n, (ast.List, ast.Tuple)):
            words = [e.value for e in n.elts if isinstance(e, ast.Constant) and isinstance(e.value, str)]
            if "systemctl" in words and "restart" in words:
                hits.append(n.lineno)
        elif isinstance(n, ast.Call) and _last_name(n.func) in ("run", "Popen", "call", "check_call",
                                                                "check_output", "system", "popen"):
            for a in n.args:
                if isinstance(a, ast.Constant) and isinstance(a.value, str) \
                        and "systemctl" in a.value and "restart" in a.value:
                    hits.append(n.lineno)
    return hits


def test_only_the_engine_client_calls_the_ollama_client():
    """`ollama_chat` is the one route to a local model call, which is what the census assumes."""
    hits = {rel: lines for rel, tree in _sources()
            if rel != _CLIENT_FILE and (lines := _client_generation_sites(tree))}
    assert hits == {}, hits


def test_only_the_two_restart_sites_restart_the_service():
    hits = {rel: _restart_command_sites(tree) for rel, tree in _sources()}
    hits = {rel: lines for rel, lines in hits.items() if lines}
    assert set(hits) == _RESTART_FILES, hits
    assert all(len(lines) == 1 for lines in hits.values()), hits
    assert set(SINKS) >= {("engine.agents.extractor", "restart_ollama"),
                          ("engine.utils.ollama_client", "_restart_ollama_and_retry")}


def test_the_sink_scans_can_fail():
    bad = ast.parse("import ollama, subprocess\nollama.chat(model='m')\noc._client.generate\n"
                    "subprocess.run(['sudo', 'systemctl', 'restart', 'ollama'])\n"
                    "os.system('sudo systemctl restart ollama')\n")
    assert sorted(_client_generation_sites(bad)) == [2, 3]
    assert sorted(_restart_command_sites(bad)) == [4, 5]
