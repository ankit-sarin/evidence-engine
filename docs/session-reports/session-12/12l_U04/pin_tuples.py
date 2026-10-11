"""12l-U04b — both arm pins as a run pins them, key by key, by pure recompute. No DB write, no fetch.
usage, from the repository root: pin_tuples.py <spec.yaml> <codebook.yaml>   (stdout: JSON)
Stages are the extractor's own for the spec (`extraction_stages`) plus audit and preflight; the
digests are the two recorded at 12f (`~/scratch/12i-scope/pins.py`), so `pin` is comparable with
the 12l-open record wherever the stage set is the same."""
import json, sys
sys.path.insert(0, ".")
import engine.utils.ollama_client as oc
def _boom(*a, **k): raise AssertionError("fetch_model_digest fired")
oc.fetch_model_digest = _boom
from engine.agents.extractor import extraction_stages
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.effective_config import sha256_canonical
from engine.core.review_spec import load_review_spec
D = {"deepseek-r1:32b": "edba8017331d15236e57480eb45406c0d721db77a4cdcf234df500fc2ad3960c",
     "gemma3:27b": "a418f5838eaf7fe2cfe0a3046c8384b68ba43a4435542c942f9db00a5f342203"}
spec0, cb = load_review_spec(sys.argv[1]), load_codebook(sys.argv[2])
out = {"codebook_semantic_hash": cb.semantic_hash, "library_versions": rm.library_versions()}
for label, elicit in (("non_elicited", False), ("elicited", True)):
    spec = spec0.model_copy(update={"extraction_models": spec0.extraction_models.model_copy(update={"elicitation": elicit})})
    stages = [*extraction_stages(spec), "audit", "preflight"]
    arms = {s: a for s in stages if (a := rm._stage_arm(spec, s))}
    res = rm.resolve_run(spec, stages, digest_fn=D.__getitem__, preflight_models=sorted(D),
                         codebook_path=cb.path, arms_by_stage=arms)
    tup = rm.pin_tuple(spec, spec.extraction_models.arm, res, cb.semantic_hash, rm.library_versions())
    out[label] = {"pin": sha256_canonical(tup), "tuple": tup,
                  "prompt_hashes": {k: r.prompt_hash for k, r in sorted(res.items())}}
print(json.dumps(out, indent=1, sort_keys=True))
