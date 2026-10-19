"""12r-U13-PA R2/R3/R6/R8 — Pass-1 attempts per paper, 12r (ra_12q run 2) against 12g (ra_12g), read-only.
usage, from the repository root (HEAD's engine is imported; no model call, every socket connect refused):
    attempts_pa.py <ra_12q_dir> <ra_12g_copy_dir> <12r_run.log> <out_dir>
Writes to <out_dir>: attempts_pa.json (the tables), excerpts_pa.txt (the first and last 300 characters
of every raw response the telemetry holds for papers 3, 8, 9, 10), and the full raw responses as
raw_<side>_p<paper>_a<attempt>.txt. stdout: attempts_pa.json.

Per attempt: violation codes (the telemetry's fields[].violations), the raw response's length, how
contracts.parse_container read it, whether the JSON inside it parses whole (json.loads on the text
between the first "{" and the last "}"), the field names present, the codebook's expected names per
class and the difference, the request hash, prompt and feedback lengths, prompt_eval_count. The 12r
side adds each Pass-1 call's done_reason from run.log's `input_fit paper_id=N` lines (three per paper
in call order: attempt 1, attempt 2, Pass 2) and re-renders attempt 2's feedback block with HEAD's
build_feedback_block from attempt 1's re-checked result, hashing attempt 1's prompt + that block
against the stored attempt-2 request hash. 12g's telemetry has no per-attempt raw response (it
predates a0b818e): its field set per attempt is derived from the codes (a field is absent when its
codes include FIELD_MISSING), labelled "derived", and only the accepted attempt's raw
(pass1_raw_content) is read.
R8: per paper, attempt-1 FIELD_MISSING by class, all twelve papers, both sides."""
import inspect, json, os, re, socket, sys
from collections import Counter, defaultdict

def _refuse(*a, **k):
    raise RuntimeError("network refused by attempts_pa.py")
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

R_DIR, G_DIR, RUN_LOG, OUT = sys.argv[1:5]
FOUR = (3, 10, 8, 9)
cb = load_codebook(os.path.join(R_DIR, "extraction_codebook.yaml")).raw
CLASS = {f["name"]: f["field_class"] for f in cb["fields"]}
EXPECTED = defaultdict(list)
for n, c in CLASS.items():
    EXPECTED[c].append(n)


def rows(d):
    return {r["paper_id"]: r for r in map(json.loads, open(os.path.join(d, "telemetry", "extraction_calls.jsonl")))}


def json_whole(raw):
    if not raw or "{" not in raw:
        return {"parses": False, "why": "no '{'"}
    body = raw[raw.index("{"):raw.rindex("}") + 1]
    try:
        json.loads(body); return {"parses": True}
    except json.JSONDecodeError as e:
        return {"parses": False, "why": str(e)}


def names_in(raw):
    entries, path = K.parse_container(raw)
    return [str(e.get(K.KEY_FIELD)) for e in entries if e.get(K.KEY_FIELD)], path


def by_class(present):
    return {c: {"expected": len(EXPECTED[c]), "present": sum(n in present for n in EXPECTED[c]),
                "missing": [n for n in EXPECTED[c] if n not in present]} for c in ("stated", "inferable", "judgment")}


done = defaultdict(list)
for line in open(RUN_LOG):
    m = re.search(r"input_fit paper_id=(\d+) (\{.*\})", line)
    if m:
        j = json.loads(m.group(2))
        if "done_reason" in j:
            done[int(m.group(1))].append({"at": line[:8], "done_reason": j["done_reason"], "prompt_count": j["count"]})

spec = load_review_spec(os.path.join(R_DIR, "spec.yaml"))
names = expected_field_names(spec, os.path.join(R_DIR, "extraction_codebook.yaml"))
kw = stage_config("elicitation_pass1", spec).kwargs()
consumed = {n for n, p in inspect.signature(ollama_chat).parameters.items()
            if p.kind is not p.VAR_KEYWORD and n not in ("model", "messages")}
model = kw.pop("model"); kw = {k: v for k, v in kw.items() if k not in consumed}

out = {"done_reason_by_paper": dict(done), "papers": {}, "r8": {}}
excerpts = []
for side, d in (("12r", R_DIR), ("12g", G_DIR)):
    tel = rows(d)
    for pid in sorted(tel):
        r = tel[pid]; e = r.get("extra") or {}
        fm = Counter(CLASS[n] for n, f in ((e.get("attempts") or [{}])[0].get("fields") or {}).items()
                     if "FIELD_MISSING" in f["violations"])
        out["r8"].setdefault(side, {})[pid] = {"outcome": r["outcome"], **{c: fm.get(c, 0) for c in ("stated", "inferable", "judgment")},
                                               "total": sum(fm.values())}
        if pid not in FOUR:
            continue
        atts = []
        for a in e.get("attempts") or []:
            raw = a.get("raw_content")
            if raw is None and a["attempt"] == e.get("accepted_attempt"):
                raw, src = e.get("pass1_raw_content"), "pass1_raw_content (accepted)"
            else:
                src = "attempts[].raw_content" if raw is not None else None
            rec = {"attempt": a["attempt"], "codes": dict(Counter(c for f in a["fields"].values() for c in f["violations"])),
                   "n_failed": a["n_failed"], "parse_path_recorded": a.get("parse_path"),
                   "prompt_chars": a.get("prompt_chars"), "feedback_chars": a.get("feedback_chars"),
                   "prompt_eval_count": a.get("prompt_eval_count"), "request_hash": a.get("request_hash"),
                   "duplicated_fields": a.get("duplicated_fields"), "raw_source": src}
            if raw is not None:
                present, path = names_in(raw)
                rec.update(raw_chars=len(raw), parse_container_path=path, json_whole=json_whole(raw),
                           names_present=present, field_set="from raw", classes=by_class(set(present)))
                open(os.path.join(OUT, f"raw_{side}_p{pid}_a{a['attempt']}.txt"), "w").write(raw)
                excerpts.append(f"=== {side} paper {pid} attempt {a['attempt']} ({src}; {len(raw)} chars)\n"
                                f"--- first 300\n{raw[:300]}\n--- last 300\n{raw[-300:]}\n")
            else:
                present = [n for n, f in a["fields"].items() if "FIELD_MISSING" not in f["violations"]]
                rec.update(field_set="derived from codes (no raw stored)", classes=by_class(set(present)))
            atts.append(rec)
        p = {"outcome": r["outcome"], "accepted_attempt": e.get("accepted_attempt"), "attempts": atts}
        if side == "12r":
            p["pass1_calls_done_reason"] = done.get(pid)
            j = json.load(open(os.path.join(R_DIR, "elicitation", e["unit_map_dir"], "unit_maps", f"{pid}.json")))
            um = UnitMap(paper_id=j["paper_id"], units=tuple(j["units"]), spans=tuple(map(tuple, j["spans"])),
                         source_raw="", min_unit_tokens=j["min_unit_tokens"], version=j["version"])
            a1 = e["attempts"][0]
            res1 = K.check_response(a1["raw_content"], um, cb, names)
            fb = build_feedback_block(res1, cb)
            p1 = PL.pass1_prompt(um, cb, names)
            h1 = request_hash({"model": model, "messages": PL.pass1_messages(p1), **kw})
            h2 = request_hash({"model": model, "messages": PL.pass1_messages(p1 + fb), **kw})
            p["rerender"] = {"attempt1_hash_equal": h1 == a1["request_hash"],
                             "attempt2_hash_equal": h2 == e["attempts"][1]["request_hash"],
                             "feedback_chars": len(fb), "feedback_chars_recorded": e["attempts"][1]["feedback_chars"]}
            open(os.path.join(OUT, f"feedback_12r_p{pid}.txt"), "w").write(fb)
        out["papers"].setdefault(side, {})[pid] = p

open(os.path.join(OUT, "excerpts_pa.txt"), "w").write("\n".join(excerpts))
text = json.dumps(out, indent=1, sort_keys=True, default=str)
open(os.path.join(OUT, "attempts_pa.json"), "w").write(text + "\n")
print(text)
