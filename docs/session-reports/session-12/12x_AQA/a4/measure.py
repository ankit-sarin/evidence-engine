"""12x-AQA A4 — one option's measurements, run from the worktree root (cwd).
audit's manifest prompt_hash as the manifest computes it (resolve_run → prompt_hash("audit",
spec, stage_config("audit", spec)), effective_config.py); the runtime request for three
fields (one per codebook type) captured at semantic_verify with ollama_chat replaced; the
render's variant count. No model call."""
import copy, hashlib, json, sys
sys.path.insert(0, ".")
import engine
from engine.core import effective_config as ec
from engine.core.codebook import load_codebook
from engine.core.review_paths import load_spec_for
from engine.agents import auditor as A
from engine.agents.models import EvidenceSpan
assert engine.__file__.startswith(sys.argv[2]), engine.__file__
class _Sent(BaseException):
    def __init__(self, m): self.messages = copy.deepcopy(m)
def _fake(**kw): raise _Sent(kw["messages"])
A.ollama_chat = _fake
spec = load_spec_for("surgical_autonomy")
cb = load_codebook("data/surgical_autonomy/extraction_codebook.yaml")
cfg = ec.stage_config("audit", spec)
types = {f["name"]: f.get("type", "text") for f in cb.fields}
out = {"audit_prompt_hash": ec.prompt_hash("audit", spec, cfg),
       "rendered_messages": len(ec.render_messages("audit", spec)),
       "rendered_variants": sum(m["role"] == "system" for m in ec.render_messages("audit", spec)),
       "requests": {}}
for name in ("study_type", "robot_platform", "sample_size"):
    span = EvidenceSpan(field_name=name, value="v", source_snippet="s", confidence=0.5, tier=1)
    try:
        A.semantic_verify(span, field_type=types[name], cfg=cfg); raise SystemExit("no call")
    except _Sent as s:
        out["requests"][f"{name} ({types[name]})"] = hashlib.sha256(ec.canonical_json(s.messages).encode()).hexdigest()
json.dump(out, open(sys.argv[1], "w"), indent=1); print(json.dumps(out, indent=1))
