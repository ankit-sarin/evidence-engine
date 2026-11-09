"""U24 (C49): every engine preflight probe is sent the spec's block (R785(b), (d)).

The capture half drives each runner that probes — `run_screening`, `run_verification` and
`run_ft_screening` — with `db=None`: each calls the probe before its first database read, and
stops on `None` after it. The engine client is a fake at `ollama_client._client` (the
`FakeClient` form of `tests/test_request_capture.py`), and `check_ollama_env`, which shells out to
`systemctl show`, is replaced. No server, no database; the suite's fences stay on. Every captured
probe request must carry the options and `keep_alive` that `stage_config("preflight", spec,
model=m)` resolves — the call `resolve_run` makes for the run's `preflight:<model>` row — under a
spec whose `preflight` and `ollama` blocks are not the defaults (on the live spec they would be
equal by coincidence, R491).

The census half walks `engine/` by AST for calls whose callee's last name is `require_preflight`,
`preflight_check` or `check_model` and that pass no `spec=` keyword, and asserts there are none.
Declared completeness (`12ae_U24A/census_u24a.py`'s): direct names and attribute calls
(`pf.check_model(...)`) are seen; a call through a variable bound to the function, a name assembled
at runtime, or a `**kwargs` that happens to carry `spec` is not.
"""
import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from engine.agents.ft_screener import run_ft_screening
from engine.agents.screener import run_screening, run_verification
from engine.core.effective_config import stage_config
from engine.core.review_paths import load_spec_for
from engine.core.review_spec import OllamaRuntime, PreflightConfig
from engine.utils import ollama_client as oc
from engine.utils import ollama_preflight as pf

REPO = Path(__file__).resolve().parents[1]
PROBE_NAMES = {"require_preflight", "preflight_check", "check_model"}


class FakeClient:
    """Stands in for `ollama.Client`; records every `chat(**kwargs)` verbatim."""

    def __init__(self):
        self.calls: list[dict] = []
        self._client = SimpleNamespace(base_url="http://u24-capture.invalid")

    def show(self, model):
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def ps(self):
        return {"models": []}

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        chars = oc.message_chars(kwargs.get("messages"))
        return SimpleNamespace(
            message=SimpleNamespace(content="OK", thinking=None),
            prompt_eval_count=max(1, int(chars * 0.3)), done_reason="stop", eval_count=1,
        )


@pytest.fixture
def non_default_spec():
    live = load_spec_for("surgical_autonomy")
    return live.model_copy(update={
        "preflight": PreflightConfig(num_predict=7, temperature=0.5),
        "ollama": OllamaRuntime(keep_alive="5m"),
    })


RUNNERS = {
    "run_screening": lambda spec: run_screening(None, spec),
    "run_verification": lambda spec: run_verification(None, spec),
    "run_ft_screening": lambda spec: run_ft_screening(None, spec, run_id=0),
}


def test_every_engine_preflight_probe_sends_the_spec_preflight_block(monkeypatch, non_default_spec):
    spec = non_default_spec
    monkeypatch.setattr(pf, "check_ollama_env", lambda: None)
    oc.clear_ceiling_cache()
    mismatches, captured = [], {}
    for name, run in RUNNERS.items():
        fake = FakeClient()
        monkeypatch.setattr(oc, "_client", fake)
        with pytest.raises(AttributeError):   # the runner stops on db=None, after its probe
            run(spec)
        captured[name] = len(fake.calls)
        for sent in fake.calls:
            want = stage_config("preflight", spec, model=sent["model"]).kwargs()
            got = {"options": sent.get("options"), "keep_alive": sent.get("keep_alive")}
            exp = {"options": want["options"], "keep_alive": want["keep_alive"]}
            if got != exp:
                mismatches.append((name, sent["model"], got, exp))
    oc.clear_ceiling_cache()
    assert captured == {"run_screening": 1, "run_verification": 1, "run_ft_screening": 2}
    assert mismatches == []


def _calls_without_spec(root: Path) -> list[str]:
    found = []
    for p in sorted(root.rglob("*.py")):
        tree = ast.parse(p.read_text(encoding="utf-8"))
        for n in ast.walk(tree):
            if not isinstance(n, ast.Call):
                continue
            tail = ast.unparse(n.func).rsplit(".", 1)[-1]
            if tail in PROBE_NAMES and not any(k.arg == "spec" for k in n.keywords):
                found.append(f"{p.relative_to(REPO)}:{n.lineno} {tail}")
    return found


def test_no_engine_probe_call_omits_the_spec():
    assert _calls_without_spec(REPO / "engine") == []
