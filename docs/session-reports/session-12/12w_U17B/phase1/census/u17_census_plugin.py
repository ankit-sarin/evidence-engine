"""12w-U17B Phase 1, I2 census plugin (scratch; loaded with -p, never under tests/).
Records — never raises — every place R703's checks would bite, per test id:
  LOAD   : a ReviewSpec validated from a dict whose top level has no `model_lineage` key and is not
           the live spec's dict (after the fix the live spec carries the map; anything else needs it);
  OPEN_COVERAGE : an open_run whose declared stages / cloud arms / preflight models name a model
           outside the 2b map (R703(d) re-check);
  OPEN_PAIR : an open_run declaring audit whose extraction-stage model has the audit model's lineage
           (R703(e)); for an audit-only run the extractor is the spec's extraction arm.
Output: one JSON line per record to $U17_CENSUS_OUT."""
import json, os, pathlib
import yaml
MAP = {"qwen3:8b": "qwen", "qwen3:32b": "qwen", "qwen2.5vl:7b": "qwen", "deepseek-r1:32b": "qwen",
       "gemma3:27b": "gemma", "o4-mini-2025-04-16": "openai", "claude-sonnet-4-6": "anthropic"}
OUT = pathlib.Path(os.environ["U17_CENSUS_OUT"])
LIVE = pathlib.Path.home() / "projects/evidence-engine/review_specs/surgical_autonomy.yaml"
LIVE_RAW = yaml.safe_load(LIVE.read_text())
EXTRACTION = ("extract_pass1", "extract_pass2", "extract_retry_snippet", "elicitation_pass1")
def _rec(kind, **kw):
    kw.update(kind=kind, test=os.environ.get("PYTEST_CURRENT_TEST", "<no test>").rsplit(" (", 1)[0])
    with OUT.open("a") as f: f.write(json.dumps(kw, sort_keys=True, default=str) + "\n")

def pytest_configure(config):
    from engine.core import review_spec as rs, run_manifest as rm
    from engine.core import effective_config as ec
    orig_validate = rs.ReviewSpec.model_validate.__func__
    def model_validate(cls, obj, *a, **k):
        res = orig_validate(cls, obj, *a, **k)
        if cls is rs.ReviewSpec and isinstance(obj, dict) and "model_lineage" not in obj and obj != LIVE_RAW:
            diff = sorted(set(obj) ^ set(LIVE_RAW)) + sorted(x for x in set(obj) & set(LIVE_RAW) if obj[x] != LIVE_RAW[x])
            _rec("LOAD", review_id=obj.get("review_id"), top_level_keys_differing_from_live=diff)
        return res
    rs.ReviewSpec.model_validate = classmethod(model_validate)
    orig_open = rm.open_run
    def open_run(conn, spec, *, kind, stages, codebook, cloud_arms=(), preflight_models=(), **kw):
        try:
            named = {}
            for s in stages:
                if s == "preflight": continue
                try: named[s] = ec.stage_config(s, spec).model
                except Exception as e: named[s] = f"<unresolved {type(e).__name__}>"
            for a in cloud_arms:
                try: named[f"cloud:{a}"] = spec.arm(a).model
                except Exception: pass
            for m in preflight_models: named[f"preflight:{m}"] = m
            missing = {s: m for s, m in named.items() if m not in MAP}
            if missing: _rec("OPEN_COVERAGE", run_kind=kind, missing=missing)
            if "audit" in named and named["audit"] in MAP:
                ext = {s: named[s] for s in stages if s in EXTRACTION}
                src = "declared"
                if not ext and getattr(spec.extraction_models, "arm", None):
                    ext = {"<spec arm>": spec.extraction_models.extractor}; src = "spec"
                same = {s: m for s, m in ext.items() if MAP.get(m) == MAP[named["audit"]]}
                if same: _rec("OPEN_PAIR", run_kind=kind, audit=named["audit"], same=same, source=src)
        except Exception as e:
            _rec("PLUGIN_ERROR", error=repr(e))
        return orig_open(conn, spec, kind=kind, stages=stages, codebook=codebook,
                         cloud_arms=cloud_arms, preflight_models=preflight_models, **kw)
    rm.open_run = open_run
