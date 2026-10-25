"""12w-V1A resume Q4 / R706(f)(ii): run_pass1's pass1_prompt_chars and pass1_feedback_chars for the
fixed case (capture_attempts.py's paper and feedback), through a stand-in ollama_chat. Run from a tree's root."""
import json, sys
sys.path.insert(0, ".")
import engine.utils.ollama_client as oc
def _boom(*a, **k): raise AssertionError("a real Ollama call was attempted")
oc.ollama_chat = _boom; oc.fetch_model_digest = _boom
from types import SimpleNamespace
from engine.core.codebook import load_codebook
from engine.core.completeness import expected_field_names
from engine.core.effective_config import stage_config
from engine.core.review_spec import load_review_spec
from engine.elicitation import pipeline as PL
from engine.elicitation.units import build_unit_map
PAPER = ("Title: A synthetic trial of a surgical robot.\n\nAbstract: We report a single-centre "
         "series of twenty cholecystectomies. The robot performed the dissection autonomously. "
         "No conversion to open surgery occurred. Mean operative time was 84 minutes.")
spec0 = load_review_spec(sys.argv[1]); cbp = sys.argv[2]
spec = spec0.model_copy(update={"extraction_models": spec0.extraction_models.model_copy(update={"elicitation": True})})
cb = load_codebook(cbp); names = expected_field_names(spec, cbp)
PL.ollama_chat = lambda **kw: (SimpleNamespace(message=SimpleNamespace(content="{}", thinking="t"),
                                               prompt_eval_count=1, done_reason="stop"), "h")
fb = PL.sentinel_feedback_block(spec, cbp)
out = {}
for label, f in (("attempt_1", ""), ("attempt_2", fb)):
    _, tel = PL.run_pass1(build_unit_map(1, PAPER), cb.raw, names, 1, feedback=f, cfg=stage_config("elicitation_pass1", spec))
    out[label] = {k: tel[k] for k in ("pass1_prompt_chars", "pass1_feedback_chars")}
print(json.dumps(out, sort_keys=True))
