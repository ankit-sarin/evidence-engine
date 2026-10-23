"""12v-U37R step 2 — rebuild each case's attempt-2 request with one tree's builders. No model call:
every socket connect is refused. Live review.db is opened mode=ro for parsed-text resolution only.
usage, from the repository root:
    build_requests.py <engine-tree-root> <arm-label> <cases.json> <out_dir>
  <engine-tree-root> is the directory whose `engine/` is imported first: `.` for HEAD (the new template),
  `~/scratch/12v-u37r/tree_old` for `git archive 37bb382 engine` (the old template; engine/ differs from
  HEAD only in engine/elicitation/prompts.py).
For each case of cases.json:
  1. the paper's current parsed text, by the engine's resolver (resolve_parsed_text + read_parsed_text,
     sha256-verified), checked equal to the case's recorded parsed_text_sha256;
  2. the unit map built from that text by this tree's build_unit_map; where the run stored its unit map
     (12r), the built units and spans are checked equal to the stored ones;
  3. the stored attempt-1 raw response re-checked by this tree's check_response against that unit map;
     its failed fields and per-field codes are checked equal to the recorded attempt-1 telemetry (else the
     feedback would not be the one the case is about, and the script refuses);
  4. the feedback block, the attempt-2 prompt and messages, and the request hash as the engine's
     request_hash computes it over the kwargs the client is handed (the construction of 12v_RSA's
     render_feedback.py); where the run stored an attempt-2 request hash, it is reported for comparison.
Writes <out_dir>/<case>/<arm>.json (the full request: model, messages and the stage's kwargs) and prints
a JSON summary."""
import inspect, json, os, socket, sqlite3, sys

ROOT, ARM, CASES, OUT = sys.argv[1:5]
REPO = os.getcwd()


def _refuse(*a, **k):
    raise OSError("12v-U37R build: network refused")


socket.socket.connect = _refuse
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.abspath(ROOT))
import engine
assert os.path.dirname(os.path.abspath(engine.__file__)) == os.path.join(os.path.abspath(ROOT), "engine"), engine.__file__
from engine.core.codebook import load_codebook
from engine.core.completeness import expected_field_names
from engine.core.effective_config import stage_config
from engine.core.parsed_text import read_parsed_text, resolve_parsed_text
from engine.core.review_paths import load_spec_for
from engine.core.run_manifest import request_hash
from engine.elicitation import contracts as K
from engine.elicitation import pipeline as PL
from engine.elicitation.prompts import build_feedback_block
from engine.elicitation.units import UnitMap, build_unit_map
from engine.utils.ollama_client import ollama_chat

LIVE_DB = os.path.join(REPO, "data/surgical_autonomy/review.db")
cb = load_codebook(os.path.join(REPO, "data/surgical_autonomy/extraction_codebook.yaml"))
spec = load_spec_for("surgical_autonomy")
em = spec.extraction_models.model_copy(update={"elicitation": True})
spec = spec.model_copy(update={"extraction_models": em})
names = expected_field_names(spec, cb.path)
cfg = stage_config("elicitation_pass1", spec)
consumed = {n for n, p in inspect.signature(ollama_chat).parameters.items()
            if p.kind is not p.VAR_KEYWORD and n not in ("model", "messages")}
kwargs = cfg.kwargs()
model = kwargs.pop("model")
hash_kw = {k: v for k, v in kwargs.items() if k not in consumed}

conn = sqlite3.connect(f"file:{LIVE_DB}?mode=ro", uri=True)
summary = {"arm": ARM, "engine_root": os.path.abspath(ROOT), "stage_kwargs": {"model": model, **kwargs},
           "hashed_kwargs": sorted(hash_kw), "cases": {}}
for case in json.load(open(CASES))["cases"]:
    cid = case["case"]
    text = read_parsed_text(resolve_parsed_text(conn, case["live_paper_id"]))
    import hashlib
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    assert sha == case["parsed_text_sha256"], (cid, sha)
    um = build_unit_map(case["throwaway_paper_id"], text)
    um_check = None
    if case.get("unit_map_path"):
        j = json.load(open(case["unit_map_path"]))
        um_check = (list(um.units) == list(j["units"]) and [list(s) for s in um.spans] == [list(s) for s in j["spans"]])
        assert um_check, (cid, "built unit map differs from the stored one")
    raw = open(case["attempt1_raw_path"], encoding="utf-8").read()
    res1 = K.check_response(raw, um, cb.raw, names)
    got = {n: sorted(r.violations) for n, r in res1.records.items() if not r.ok}
    want = {n: sorted(v) for n, v in case["attempt1_failed_codes"].items()}
    assert got == want, (cid, "attempt-1 failures do not reproduce", got, want)
    fb = build_feedback_block(res1, cb.raw)
    messages = PL.pass1_messages(PL.pass1_prompt(um, cb.raw, names, fb))
    h = request_hash({"model": model, "messages": messages, **hash_kw})
    os.makedirs(os.path.join(OUT, cid), exist_ok=True)
    req = {"model": model, "messages": messages, **kwargs}
    with open(os.path.join(OUT, cid, f"{ARM}.json"), "w", encoding="utf-8") as fh:
        json.dump({"request_hash": h, "paper_id": case["throwaway_paper_id"], "request": req}, fh, ensure_ascii=False)
    summary["cases"][cid] = {"parsed_text_sha256_ok": True, "unit_map_equals_stored": um_check,
                             "n_units": um.n, "attempt1_failures_reproduce": True,
                             "feedback_chars": len(fb), "recorded_feedback_chars": case.get("recorded_feedback_chars"),
                             "request_hash": h, "stored_attempt2_request_hash": case.get("stored_attempt2_request_hash"),
                             "equals_stored": (h == case["stored_attempt2_request_hash"]) if case.get("stored_attempt2_request_hash") else None,
                             "roles": [m["role"] for m in messages]}
conn.close()
print(json.dumps(summary, indent=1, sort_keys=True))
