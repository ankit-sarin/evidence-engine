"""12ac-U22A §5 — HEAD's recorders against a fake response carrying eval_count 123, no server. Read-only on the tree:
replaces ollama_client._client, effective_ceiling and subprocess.run in this process only; the opt-out is set and
subprocess.run raises if reached, so no service manager is touched; no network call is made; no run is active, so
run_calls records nothing. The spec is loaded read-only for the two extract stage configs; the telemetry record is
written under the scratch directory given as argv[1]. usage, from the repository root: fake_seam_probe.py <scratch dir>"""
import inspect, json, logging, os, sys
from types import SimpleNamespace
os.environ["EVIDENCE_ENGINE_NO_OLLAMA_RESTART"] = "1"
sys.path.insert(0, os.getcwd())
from engine.utils import ollama_client as oc
def boom(*a, **k): raise SystemExit("subprocess.run reached")
oc.subprocess.run = boom
oc.effective_ceiling = lambda model, options=None: 131072
class Fake:
    def __init__(self, with_eval_count: bool, content: str): self.with_ec, self.content, self.calls = with_eval_count, content, 0
    def chat(self, **kw):
        self.calls += 1
        r = SimpleNamespace(message=SimpleNamespace(content=self.content, thinking="a trace"), prompt_eval_count=400, done_reason="stop")
        if self.with_ec: r.eval_count = 123
        return r
    def show(self, *a, **k): raise SystemExit("show reached")
lines = []
class H(logging.Handler):
    def emit(self, record): lines.append(record.getMessage())
logging.getLogger("engine.utils.ollama_client").addHandler(H()); logging.getLogger("engine.utils.ollama_client").setLevel(logging.INFO)
from engine.core.review_spec import load_review_spec
from engine.core.effective_config import stage_config
from engine.agents import extractor as E
from engine.core import extraction_telemetry as T
spec = load_review_spec("review_specs/surgical_autonomy.yaml")
cfg1, cfg2 = stage_config("extract_pass1", spec), stage_config("extract_pass2", spec)
print("stage configs (read-only):", cfg1.model, sorted(cfg1.sent_keys), "|", cfg2.model, sorted(cfg2.sent_keys))
for label, with_ec in (("with eval_count=123", True), ("without eval_count", False)):
    print(f"\n== {label}")
    oc._client = Fake(with_ec, '{"fields": []}'); lines.clear()
    r = oc.ollama_chat(model="m:8b", messages=[{"role": "user", "content": "x" * 2000}], paper_id=7)
    fit = [l for l in lines if l.startswith("input_fit paper_id=7 ")]
    tel = json.loads(fit[0].split(" ", 2)[2]) if fit else None
    print("ollama_chat returned the response; getattr(response, 'eval_count', None) =", getattr(r, "eval_count", None))
    print("input_fit line keys:", sorted(tel) if tel else fit, "| carries eval_count:", bool(tel) and "eval_count" in tel)
    E._LAST_PASS1_TELEMETRY.clear(); E._LAST_PASS2_TELEMETRY.clear()
    trace = E.extract_pass1_reasoning("p", cfg=cfg1, paper_id=7)
    print("_LAST_PASS1_TELEMETRY keys:", sorted(E._LAST_PASS1_TELEMETRY), "| values:", {k: E._LAST_PASS1_TELEMETRY[k] for k in ("finish_reason", "prompt_eval_count")})
    res = E.extract_pass2_structured("p", trace, spec, 7, cfg=cfg2)
    print("_LAST_PASS2_TELEMETRY keys:", sorted(E._LAST_PASS2_TELEMETRY), "| values:", {k: E._LAST_PASS2_TELEMETRY[k] for k in ("finish_reason", "prompt_eval_count")})
    d = os.path.join(sys.argv[1], "probe_review_" + ("with" if with_ec else "without"))
    T.record_call(d, arm="probe", paper_id=7, attempt=1, outcome="stored", model=cfg2.model,
                  finish_reason=E._LAST_PASS2_TELEMETRY.get("finish_reason"),
                  pass1_done_reason=E._LAST_PASS1_TELEMETRY.get("finish_reason"),
                  pass1_prompt_eval_count=E._LAST_PASS1_TELEMETRY.get("prompt_eval_count"),
                  pass2_prompt_eval_count=E._LAST_PASS2_TELEMETRY.get("prompt_eval_count"))
    row = T.read_calls(d)[0]
    print("telemetry row keys:", sorted(row)); print("row carries eval_count:", "eval_count" in row, "| output_tokens:", row["output_tokens"], "| pass1_prompt_eval_count:", row["pass1_prompt_eval_count"])
    print("fake chat calls:", oc._client.calls, "| raised: nothing")
sig = inspect.signature(T.record_call)
print("\nrecord_call parameters:", sorted(sig.parameters)); print("has eval_count parameter:", "eval_count" in sig.parameters)
try:
    T.record_call(os.path.join(sys.argv[1], "probe_review_sig"), arm="probe", paper_id=7, attempt=1, outcome="stored", eval_count=123)
except TypeError as exc:
    print("record_call(eval_count=123) ->", type(exc).__name__ + ":", exc)
print("subprocess.run reached:", False)
