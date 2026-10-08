"""A11 — print the `format` the resolver gives the extract_pass2 stage for the live spec
file, elicitation off and on. Imports the resolver and loads the YAML only; no DB, no model."""
import json, pathlib
from engine.core.review_spec import load_review_spec
from engine.core.effective_config import stage_config
spec = load_review_spec(pathlib.Path("/home/ankitsarin/projects/evidence-engine/review_specs/surgical_autonomy.yaml"))
def describe(label, spec):
    cfg = stage_config("extract_pass2", spec)
    fmt = cfg.kwargs().get("format")
    print(f"--- {label}: elicitation={spec.extraction_models.elicitation} model={cfg.model} sent_keys={sorted(cfg.sent_keys)}")
    print("top-level type:", fmt.get("type"), "| properties:", list(fmt.get("properties", {})), "| required:", fmt.get("required"),
          "| additionalProperties:", fmt.get("additionalProperties", "<absent>"))
    f = fmt["properties"]["fields"]
    print("fields:", {k: v for k, v in f.items() if k != "description"})
    print("minItems anywhere in schema:", "minItems" in json.dumps(fmt), "| format_hash:", cfg.format_hash if hasattr(cfg, "format_hash") else "n/a")
    return json.dumps(fmt, sort_keys=True)
a = describe("live spec file", spec)
em = spec.extraction_models.model_copy(update={"elicitation": True})
b = describe("same spec, elicitation=True", spec.model_copy(update={"extraction_models": em}))
print("identical format on both paths:", a == b)
print(json.dumps(json.loads(a), indent=1))
