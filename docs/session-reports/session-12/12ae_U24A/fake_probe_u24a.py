"""12ae-U24A — FAKE probe for U24 (C49): the request each engine preflight site sends, beside the options its run's preflight row
would record. No server, no database: the engine client is a fake at `ollama_client._client` (test_request_capture.py's FakeClient
form), every real HTTP send raises, `check_ollama_env` (which shells out to `systemctl show`) is replaced by a no-op, and each
runner is handed `db=None` — the probe fires before the runner's first database read, and the runner then stops on `None`.
The row's options are computed by `stage_config("preflight", spec, model=m)`, the call `resolve_run` makes for each
`preflight:<model>` row that `open_run` writes (effective_config.py:521–525, run_manifest.py:604–609).
usage, from the repository root: .venv/bin/python fake_probe_u24a.py   (stdout: one block per spec variant)"""
import json, sys
from types import SimpleNamespace
from pathlib import Path
sys.path.insert(0, str(Path.cwd()))
import httpx

def _refuse(*a, **kw):
    raise AssertionError("fake_probe_u24a attempted a real HTTP request")
httpx.Client.send = _refuse; httpx.AsyncClient.send = _refuse

from engine.utils import ollama_client as oc
from engine.utils import ollama_preflight as pf
from engine.core.review_paths import load_spec_for
from engine.core.review_spec import PreflightConfig, OllamaRuntime
from engine.core.effective_config import stage_config
from engine.agents.screener import run_screening, run_verification
from engine.agents.ft_screener import run_ft_screening


class FakeClient:
    def __init__(self):
        self.calls = []; self._client = SimpleNamespace(base_url="http://u24a-probe.invalid")
    def show(self, model):
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})
    def ps(self):
        return {"models": []}
    def chat(self, **kw):
        self.calls.append(kw)
        chars = oc.message_chars(kw.get("messages"))
        return SimpleNamespace(message=SimpleNamespace(content="OK", thinking=None),
                               prompt_eval_count=max(1, int(chars * 0.3)), done_reason="stop", eval_count=1)


pf.check_ollama_env = lambda: None
oc.clear_ceiling_cache()
live = load_spec_for("surgical_autonomy")
variants = {
    "live spec (no preflight or ollama block in its YAML)": live,
    "preflight num_predict=7, temperature=0.5": live.model_copy(update={"preflight": PreflightConfig(num_predict=7, temperature=0.5)}),
    "ollama keep_alive='5m'": live.model_copy(update={"ollama": OllamaRuntime(keep_alive="5m")}),
    "both": live.model_copy(update={"preflight": PreflightConfig(num_predict=7, temperature=0.5), "ollama": OllamaRuntime(keep_alive="5m")}),
}
runners = {
    "run_screening (screener.py:171)": lambda s: run_screening(None, s),
    "run_verification (screener.py:259)": lambda s: run_verification(None, s),
    "run_ft_screening (ft_screener.py:325)": lambda s: run_ft_screening(None, s, run_id=0),
    "control: require_preflight(..., spec=spec), the extractor's form (extractor.py:1149)":
        lambda s: pf.require_preflight([s.extraction_models.primary if hasattr(s.extraction_models, "primary") else stage_config("extract_pass1", s).model], runner_name="control", spec=s),
}
summary = []
for vname, spec in variants.items():
    print(f"\n=== spec variant: {vname}")
    print(f"    spec.preflight = {spec.preflight.model_dump()}; spec.ollama = {spec.ollama.model_dump()}")
    for rname, run in runners.items():
        fake = FakeClient(); oc._client = fake
        try:
            run(spec); outcome = "returned"
        except AttributeError as exc:
            outcome = f"stopped after the probe on db=None ({type(exc).__name__})"
        except Exception as exc:
            outcome = f"raised {type(exc).__name__}: {str(exc)[:120]}"
        print(f"  -- {rname}: {len(fake.calls)} probe request(s); {outcome}")
        for kw in fake.calls:
            m = kw["model"]; row = stage_config("preflight", spec, model=m)
            sent = {"options": kw.get("options"), "keep_alive": kw.get("keep_alive")}
            rec = {"options": dict(row.options), "keep_alive": row.keep_alive}
            eq = sent == rec
            print(f"     model {m}: sent {json.dumps(sent)}; row would record {json.dumps(rec)}; equal: {eq}")
            summary.append((vname, rname.split(" (")[0], m, eq))
print("\n=== summary (variant | site | model | sent == row)")
for r in summary: print(" | ".join(map(str, r)))
non_default = [r for r in summary if r[0] != "live spec (no preflight or ollama block in its YAML)" and not r[1].startswith("control")]
print(f"\nengine sites under a non-default spec: {len(non_default)} captures, {sum(1 for r in non_default if not r[3])} unequal to their row")
print(f"control (spec passed) under every variant: all equal = {all(r[3] for r in summary if r[1].startswith('control'))}")
print(f"live spec, engine sites: all equal = {all(r[3] for r in summary if r[0].startswith('live') and not r[1].startswith('control'))} (R491: the coincidence of defaults)")
