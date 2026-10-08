"""12f startup verify — pins and stage rows by pure recompute. No DB, no fetch.
usage: pins.py <repo_root> <live_spec_path> <live_db_path>"""
import sys, json
root, spec_path, db_path = sys.argv[1:4]
sys.path.insert(0, root)
import engine.utils.ollama_client as oc
def _boom(*a, **k): raise AssertionError("fetch_model_digest fired")
oc.fetch_model_digest = _boom
from engine.core import run_manifest as rm
from engine.core.review_paths import load_spec_for
from engine.core.codebook import load_codebook_beside
from engine.core.effective_config import sha256_canonical
D = {"deepseek-r1:32b": "edba8017331d15236e57480eb45406c0d721db77a4cdcf234df500fc2ad3960c",
     "gemma3:27b": "a418f5838eaf7fe2cfe0a3046c8384b68ba43a4435542c942f9db00a5f342203"}
def dfn(m): return D[m]
spec0 = load_spec_for("surgical_autonomy", spec_path)
cb = load_codebook_beside(db_path)
out = {"engine_file": rm.__file__}
for label, elicit in (("non_elicited", False), ("elicited", True)):
    em = spec0.extraction_models.model_copy(update={"elicitation": elicit})
    spec = spec0.model_copy(update={"extraction_models": em})
    p1 = "elicitation_pass1" if elicit else "extract_pass1"
    stages = [p1, "extract_pass2", "extract_retry_snippet", "audit", "preflight"]
    arms = {s: rm._stage_arm(spec, s) for s in stages if rm._stage_arm(spec, s)}
    res = rm.resolve_run(spec, stages, digest_fn=dfn,
                         preflight_models=["deepseek-r1:32b", "gemma3:27b"],
                         arms_by_stage=arms)
    tup = rm.pin_tuple(spec, "local_deepseek_r1_32b", res, cb.semantic_hash, rm.library_versions())
    out[label] = {"pin": sha256_canonical(tup),
                  "rows": {k: {"model": r.config.model, "prompt_hash": r.prompt_hash,
                               "options_hash": r.options_hash,
                               "format_schema_hash": r.format_schema_hash}
                           for k, r in sorted(res.items())}}
print(json.dumps(out, indent=1, sort_keys=True))
