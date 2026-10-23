"""12v-U37R step 2a — the six STATED-only-feedback cases of 12v_RSA.md §Q4, from their telemetry. Read-only.
usage, from the repository root: make_cases.py <scratch_dir>   (stdout: cases.json; writes each stored
attempt-1 raw response to <scratch_dir>/attempt1/<case>.txt for build_requests.py)
A case is usable only if its attempt-1 response text is stored, because attempt 2's feedback block echoes
attempt 1's values and indices: `attempts[0].raw_content` (12r), or `extra.pass1_raw_content` when the
accepted attempt is 1 (12g). The others are listed under `excluded` with the reason."""
import hashlib, json, os, sys

SCR = sys.argv[1]
D = "docs/session-reports/session-12"
H = os.path.expanduser("~")
SOURCES = {
    "12g": (f"{H}/scratch/retained/ra_12g/telemetry/extraction_calls.jsonl", None),
    "12r": (f"{H}/scratch/12r-u13/run/ra_12q/telemetry/extraction_calls.jsonl", f"{H}/scratch/12r-u13/run/ra_12q/elicitation"),
    "sep05_0243": ("data/surgical_autonomy/eval/elicit_design01/scratch/smoke_20260905T024348Z/elicit_design01/telemetry/extraction_calls.jsonl", None),
}
census = json.load(open(f"{D}/12v_RSA/census_attempt2.json"))
label = {"12g_ra_12g": "12g", "12r_ra_12q": "12r", "sep05_0243": "sep05_0243"}
six = [c for c in census["cases"] if c["feedback_classes"] == ["stated"]]
assert len(six) == 6, len(six)
idmap = {e["throwaway_paper_id"]: e for e in json.load(open(f"{D}/12g_ra/id_map.json"))}
os.makedirs(os.path.join(SCR, "attempt1"), exist_ok=True)
cases, excluded = [], []
for c in six:
    src = label[c["source"]]; path, umdir = SOURCES[src]
    row = next(json.loads(l) for l in open(path) if json.loads(l)["paper_id"] == c["paper_id"])
    x = row["extra"]; a1, a2 = x["attempts"][0], x["attempts"][1]
    raw = a1.get("raw_content") or (x.get("pass1_raw_content") if x.get("accepted_attempt") == 1 else None)
    cid = f"{src}_p{c['paper_id']}"
    base = {"case": cid, "source": src, "paper_id_in_source": c["paper_id"],
            "historical_attempt2_shape": c["a2_shape"], "historical_accepted": c["accepted_attempt"]}
    if raw is None:
        excluded.append({**base, "reason": "attempt-1 response text not stored (accepted attempt 2; the 2026-09-05 "
                                           "telemetry schema keeps only the accepted attempt's raw), so its feedback "
                                           "block cannot be rebuilt"})
        continue
    if src == "sep05_0243":
        raise SystemExit("unexpected: a September case with attempt-1 raw")
    m = idmap[c["paper_id"]]
    rawp = os.path.join(SCR, "attempt1", f"{cid}.txt")
    open(rawp, "w", encoding="utf-8").write(raw)
    cases.append({**base, "throwaway_paper_id": c["paper_id"], "live_paper_id": m["live_paper_id"],
                  "parsed_text_version": m["version"], "parsed_text_sha256": m["sha256"],
                  "attempt1_raw_path": rawp, "attempt1_raw_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
                  "attempt1_raw_source": "attempts[0].raw_content" if a1.get("raw_content") else "pass1_raw_content (accepted attempt 1)",
                  "attempt1_failed_codes": {n: f["violations"] for n, f in a1["fields"].items() if n in a1["failed_fields"]},
                  "recorded_feedback_chars": a2.get("feedback_chars"),
                  "stored_attempt2_request_hash": a2.get("request_hash"),
                  "unit_map_path": (os.path.join(umdir, x["unit_map_dir"], "unit_maps", f"{c['paper_id']}.json")
                                    if umdir and x.get("unit_map_dir") else None)})
print(json.dumps({"cases": cases, "excluded": excluded}, indent=1, sort_keys=True))
