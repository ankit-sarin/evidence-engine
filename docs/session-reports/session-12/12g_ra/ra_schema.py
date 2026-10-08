"""ra_schema.py — the Pass 2 format schema the run sent (12g_RA-A.md section 4). The body is
not stored by the engine; it is rebuilt from stage_config("extract_pass2", spec) with the
retained spec, at the run's commit, and its hash checked against the run's stage row.
Writes ra_schema.json (the check and the body)."""
import json
import os
import subprocess
import sys

sys.path.insert(0, os.getcwd())
from ra_common import RUN_ID, db, retained, write

from engine.core.effective_config import sha256_canonical, stage_config
from engine.core.review_spec import load_review_spec

conn = db()
man = conn.execute("SELECT git_commit FROM run_manifests WHERE run_id = ?", (RUN_ID,)).fetchone()
head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
engine_diff = subprocess.run(["git", "diff", "--stat", man["git_commit"], "HEAD", "--", "engine", "scripts"],
                             capture_output=True, text=True).stdout.strip()
spec = load_review_spec(os.path.join(retained(), "spec.yaml"))
out = {"run_git_commit": man["git_commit"], "head_at_analysis": head,
       "engine_and_scripts_changed_since_run_commit": bool(engine_diff), "stages": {}}
for stage in ("extract_pass2", "elicitation_pass1", "audit"):
    row = conn.execute("SELECT model_name, format_schema_hash, options_json, sent_keys_json, keep_alive, prompt_hash "
                       "FROM run_stage_configs WHERE run_id = ? AND stage = ?", (RUN_ID, stage)).fetchone()
    cfg = stage_config(stage, spec)
    rebuilt = cfg.format_schema_hash
    body = cfg.kwargs().get("format")      # the value the call site passes to ollama_chat
    out["stages"][stage] = {
        "stored_format_schema_hash": row["format_schema_hash"], "rebuilt_format_schema_hash": rebuilt,
        "equal": row["format_schema_hash"] == rebuilt,
        "sha256_canonical_of_body": sha256_canonical(body) if body is not None else None,
        "model": row["model_name"], "options": json.loads(row["options_json"]),
        "sent_keys": json.loads(row["sent_keys_json"]), "keep_alive": row["keep_alive"],
        "format_body": body,
    }
p2 = out["stages"]["extract_pass2"]["format_body"]


def walk(node, path=""):
    """Every place the schema constrains an array's length or an object's required keys."""
    found = []
    if isinstance(node, dict):
        if node.get("type") == "array":
            found.append({"path": path or "$", "minItems": node.get("minItems"), "maxItems": node.get("maxItems")})
        if "required" in node:
            found.append({"path": path or "$", "required": node["required"]})
        for k, v in node.items():
            found += walk(v, f"{path}.{k}" if path else k)
    elif isinstance(node, list):
        for i, v in enumerate(node):
            found += walk(v, f"{path}[{i}]")
    return found


out["extract_pass2_constraints"] = walk(p2)
write("ra_schema.json", out)
print(json.dumps({k: v for k, v in out.items() if k != "stages"}, indent=1))
for s, v in out["stages"].items():
    print(s, {k: v[k] for k in ("stored_format_schema_hash", "rebuilt_format_schema_hash", "equal", "sent_keys", "options")})
print(json.dumps(p2)[:900])
