"""12w-V1A Q2: the runtime Pass-1 message lists for one fixed synthetic paper, attempt 1 and
attempt 2 (fixed feedback), captured through a mocked `ollama_chat` (no server, no request).
usage, from a tree's root: capture_attempts.py <spec.yaml> <codebook.yaml> <out.json>"""
import json, sys
sys.path.insert(0, ".")
import engine.utils.ollama_client as oc
def _boom(*a, **k): raise AssertionError("a real Ollama call was attempted")
oc.ollama_chat = _boom
oc.fetch_model_digest = _boom
from types import SimpleNamespace
from engine.core.codebook import load_codebook
from engine.core.completeness import expected_field_names
from engine.core.effective_config import stage_config, sha256_canonical
from engine.core.review_spec import load_review_spec
from engine.elicitation import pipeline as PL
from engine.elicitation.units import build_unit_map

PAPER = ("Title: A synthetic trial of a surgical robot.\n\nAbstract: We report a single-centre "
         "series of twenty cholecystectomies. The robot performed the dissection autonomously. "
         "No conversion to open surgery occurred. Mean operative time was 84 minutes.")
spec_path, cb_path, out_path = sys.argv[1:4]
spec0 = load_review_spec(spec_path)
spec = spec0.model_copy(update={"extraction_models": spec0.extraction_models.model_copy(update={"elicitation": True})})
cb = load_codebook(cb_path)
names = expected_field_names(spec, cb_path)
feedback = PL.sentinel_feedback_block(spec, cb_path)   # fixed: deterministic over the sentinel result
cfg = stage_config("elicitation_pass1", spec)
sent = []
def fake(**kw):
    sent.append(kw)
    return SimpleNamespace(message=SimpleNamespace(content="{}", thinking="t"),
                           prompt_eval_count=1, done_reason="stop"), "fakehash"
PL.ollama_chat = fake
um = build_unit_map(1, PAPER)
for fb in ("", feedback):
    try:
        PL.run_pass1(um, cb.raw, names, 1, feedback=fb, cfg=cfg)
    except Exception as exc:  # the response is a stub; only the request matters
        print("after capture:", type(exc).__name__, file=sys.stderr)
out = {}
for label, kw in zip(("attempt_1", "attempt_2"), sent):
    kw = {k: v for k, v in kw.items() if k not in ("paper_id", "return_request_hash")}
    out[label] = {"n_messages": len(kw["messages"]),
                  "roles": [m["role"] for m in kw["messages"]],
                  "content_chars": [len(m["content"]) for m in kw["messages"]],
                  "messages_sha256": sha256_canonical(kw["messages"]),
                  "request_kwargs_sha256": sha256_canonical(kw),
                  "messages": kw["messages"]}
out["feedback_sha256"] = sha256_canonical(feedback)
out["paper_sha256"] = sha256_canonical(PAPER)
json.dump(out, open(out_path, "w"), indent=1, sort_keys=True)
for label in ("attempt_1", "attempt_2"):
    o = out[label]
    print(label, o["n_messages"], o["roles"], o["content_chars"], "msgs", o["messages_sha256"], "kwargs", o["request_kwargs_sha256"])
