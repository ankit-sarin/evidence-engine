"""12w-U17A Q8 simulation. NOT an engine change: in THIS process only, every _SpecModel subclass is
rebuilt with extra='allow', so a scratch spec carrying the candidate family keys loads, and the keys
appear in model_dump (so in spec_hash) but nothing reads them. Then the pins are recomputed exactly as
12l_U04/pin_tuples.py does (same fixed digests, no fetch), and spec_hash (run_manifest.spec_hash) is
computed for the live spec and each scratch spec. usage, from the repo root: simulate_pins.py <spec.yaml>..."""
import json, sys, hashlib
sys.path.insert(0, ".")
import engine.utils.ollama_client as oc
def _boom(*a, **k): raise AssertionError("fetch_model_digest fired")
oc.fetch_model_digest = _boom
import engine.core.review_spec as rs
classes = [c for c in vars(rs).values() if isinstance(c, type) and issubclass(c, rs._SpecModel)]
def depth(c): return len(c.__mro__)
for c in sorted(classes, key=depth, reverse=True) + [rs.ReviewSpec]:
    c.model_config["extra"] = "allow"; c.model_rebuild(force=True)
for c in classes:   # second pass so containers rebuild over rebuilt members
    c.model_rebuild(force=True)
from engine.agents.extractor import extraction_stages
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.effective_config import sha256_canonical
D = {"deepseek-r1:32b": "edba8017331d15236e57480eb45406c0d721db77a4cdcf234df500fc2ad3960c",
     "gemma3:27b": "a418f5838eaf7fe2cfe0a3046c8384b68ba43a4435542c942f9db00a5f342203"}
cb = load_codebook("data/surgical_autonomy/extraction_codebook.yaml")
ref = json.load(open("docs/session-reports/session-12/12v_close/pin_tuples_close.json"))
for path in sys.argv[1:]:
    spec0 = rs.load_review_spec(path)
    row = {"file": path, "file_sha256": hashlib.sha256(open(path, "rb").read()).hexdigest(),
           "spec_hash": rm.spec_hash(spec0), "screening_hash": spec0.screening_hash()}
    for label, elicit in (("non_elicited", False), ("elicited", True)):
        spec = spec0.model_copy(update={"extraction_models": spec0.extraction_models.model_copy(update={"elicitation": elicit})})
        stages = [*extraction_stages(spec), "audit", "preflight"]
        arms = {s: a for s in stages if (a := rm._stage_arm(spec, s))}
        res = rm.resolve_run(spec, stages, digest_fn=D.__getitem__, preflight_models=sorted(D),
                             codebook_path=cb.path, arms_by_stage=arms)
        tup = rm.pin_tuple(spec, spec.extraction_models.arm, res, cb.semantic_hash, rm.library_versions())
        ph = {k: r.prompt_hash for k, r in sorted(res.items())}
        oh = {k: r.options_hash for k, r in sorted(res.items())}
        row[label] = {"pin": sha256_canonical(tup), "pin_equal_close": sha256_canonical(tup) == ref[label]["pin"],
                      "tuple_equal_close": tup == ref[label]["tuple"],
                      "prompt_hashes_equal_close": ph == ref[label]["prompt_hashes"],
                      "options_hashes": oh}
    extras = {k: v for k, v in (spec0.model_extra or {}).items()}
    row["top_level_extras"] = sorted(extras)
    print(json.dumps(row, sort_keys=True))
