"""12v-RSA Q2 — RENDERED (not stored) attempt-2 feedback blocks for 12r's two attempt-2 cases after
STATED-only feedback (papers 8 and 12), by HEAD's own builders from attempt 1's stored raw response and
the run's stored unit map; every socket connect refused, so no model call or Ollama request is possible.
Each render is checked against the run: rendered feedback length == recorded feedback_chars, and
request_hash(attempt-1 prompt + rendered block) == the stored attempt-2 request hash.
Also prints the Pass-1 prompt's "## Output" section for paper 8 (the instruction attempt 2 repeats).
usage, from the repository root: render_feedback.py <ra_12q_dir> <out_dir>
Writes <out_dir>/feedback_12r_p<id>.rendered.txt and prints a JSON summary."""
import inspect, json, os, socket, sys


def _refuse(*a, **k):
    raise OSError("12v-RSA: network refused")


socket.socket.connect = _refuse
sys.path.insert(0, os.getcwd())
from engine.core.codebook import load_codebook
from engine.core.completeness import expected_field_names
from engine.core.effective_config import stage_config
from engine.core.review_spec import load_review_spec
from engine.core.run_manifest import request_hash
from engine.elicitation import contracts as K
from engine.elicitation import pipeline as PL
from engine.elicitation.prompts import build_feedback_block
from engine.elicitation.units import UnitMap
from engine.utils.ollama_client import ollama_chat

R_DIR, OUT = sys.argv[1], sys.argv[2]
cb_path = os.path.join(R_DIR, "extraction_codebook.yaml")
cb = load_codebook(cb_path).raw
spec = load_review_spec(os.path.join(R_DIR, "spec.yaml"))
names = expected_field_names(spec, cb_path)
cfg = stage_config("elicitation_pass1", spec)
# the request hash covers what ollama_chat hands the client: its own keyword parameters are not sent
# (the 12r_U13_PA/attempts_pa.py construction)
consumed = {n for n, p in inspect.signature(ollama_chat).parameters.items()
            if p.kind is not p.VAR_KEYWORD and n not in ("model", "messages")}
kw = cfg.kwargs(); model = kw.pop("model"); kw = {k: v for k, v in kw.items() if k not in consumed}
rows = {r["paper_id"]: r for r in map(json.loads, open(os.path.join(R_DIR, "telemetry", "extraction_calls.jsonl")))}
summary = {}
for pid in (8, 12):
    e = rows[pid]["extra"]
    j = json.load(open(os.path.join(R_DIR, "elicitation", e["unit_map_dir"], "unit_maps", f"{pid}.json")))
    um = UnitMap(paper_id=j["paper_id"], units=tuple(j["units"]), spans=tuple(map(tuple, j["spans"])),
                 source_raw="", min_unit_tokens=j["min_unit_tokens"], version=j["version"])
    a1, a2 = e["attempts"][0], e["attempts"][1]
    res1 = K.check_response(a1["raw_content"], um, cb, names)
    fb = build_feedback_block(res1, cb)
    p1 = PL.pass1_prompt(um, cb, names)
    h1 = request_hash({"model": model, "messages": PL.pass1_messages(p1), **kw})
    h2 = request_hash({"model": model, "messages": PL.pass1_messages(p1 + fb), **kw})
    with open(os.path.join(OUT, f"feedback_12r_p{pid}.rendered.txt"), "w", encoding="utf-8") as fh:
        fh.write(fb)
    summary[str(pid)] = {"rendered_chars": len(fb), "recorded_feedback_chars": a2["feedback_chars"],
                    "attempt1_hash_equal": h1 == a1["request_hash"], "attempt2_hash_equal": h2 == a2["request_hash"],
                    "attempt2_request_hash": a2["request_hash"], "messages_roles": [m["role"] for m in PL.pass1_messages(p1 + fb)],
                    "feedback_is_suffix_of_user_turn": PL.pass1_messages(p1 + fb)[1]["content"].endswith(fb)}
    if pid == 8:
        i = p1.rfind("## Output"); k = p1.find("## Numbered Paper Text", i)
        summary["pass1_output_section_p8"] = p1[i:k]
print(json.dumps(summary, indent=1, sort_keys=True))
