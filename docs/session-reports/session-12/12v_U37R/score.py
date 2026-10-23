"""12v-U37R step 3b — score every reply with the engine's own check_response. No model call (sockets
refused); live review.db opened mode=ro for parsed-text resolution only.
usage, from the repository root: score.py <cases.json> <send_log.json> <responses_dir>   (stdout: scores JSON)
Each reply is checked against the unit map built from the case's parsed text (the same units in both
arms: engine/elicitation/units.py is identical at 37bb382 and HEAD), with the live codebook's expected
fields. Shapes, from the per-field codes (the rule of 12r_U13_PA/shape_pa.py and 12v_RSA census_attempt2.py):
  stated_only      — every INFERABLE and JUDGMENT field FIELD_MISSING, at least one STATED field present
  full_contract_met — every field present and every field passes its contract
  full_other_failures — every field present, at least one field failing (no FIELD_MISSING)
  partial          — some fields FIELD_MISSING, not stated_only
  unparseable      — check_response raised (the raised error is recorded)
A call that failed is scored `call_failed`. The warm-up is scored but not counted in the totals."""
import collections, hashlib, json, os, socket, sqlite3, sys

def _refuse(*a, **k):
    raise OSError("12v-U37R score: network refused")

socket.socket.connect = _refuse
sys.path.insert(0, os.getcwd())
from engine.core.codebook import load_codebook
from engine.core.completeness import expected_field_names
from engine.core.parsed_text import read_parsed_text, resolve_parsed_text
from engine.core.review_paths import load_spec_for
from engine.elicitation import contracts as K
from engine.elicitation.units import build_unit_map

CASES, LOG, RESP = sys.argv[1:4]
cb = load_codebook("data/surgical_autonomy/extraction_codebook.yaml")
names = expected_field_names(load_spec_for("surgical_autonomy"), cb.path)
cls = {f["name"]: f["field_class"] for f in cb.raw["fields"]}
cases = {c["case"]: c for c in json.load(open(CASES))["cases"]}
conn = sqlite3.connect("file:data/surgical_autonomy/review.db?mode=ro", uri=True)
units = {}
for cid, c in cases.items():
    text = read_parsed_text(resolve_parsed_text(conn, c["live_paper_id"]))
    assert hashlib.sha256(text.encode("utf-8")).hexdigest() == c["parsed_text_sha256"]
    units[cid] = build_unit_map(c["throwaway_paper_id"], text)
conn.close()


def shape(res):
    missing = {n for n, r in res.records.items() if "FIELD_MISSING" in r.violations}
    present = {k: sum(1 for n in names if cls[n] == k and n not in missing) for k in ("stated", "inferable", "judgment")}
    non_stated = {n for n in names if cls[n] != "stated"}
    if not missing:
        s = "full_contract_met" if not res.failed_fields else "full_other_failures"
    elif non_stated <= missing and present["stated"] > 0:
        s = "stated_only"
    else:
        s = "partial"
    return s, present


log = json.load(open(LOG))
rows = []
for call in log["calls"]:
    r = {k: call.get(k) for k in ("seq", "role", "case", "case_no", "arm", "request_hash", "raw_sha256",
                                   "done_reason", "eval_count", "prompt_eval_count", "wall_s")}
    if call.get("error"):
        r.update(shape="call_failed", error=call["error"]); rows.append(r); continue
    raw = open(os.path.join(RESP, f"{call['raw_sha256']}.txt"), encoding="utf-8").read()
    try:
        res = K.check_response(raw, units[call["case"]], cb.raw, names)
    except Exception as exc:
        r.update(shape="unparseable", error=f"{type(exc).__name__}: {exc}"); rows.append(r); continue
    s, present = shape(res)
    codes = collections.Counter(v for rec in res.records.values() for v in rec.violations)
    r.update(shape=s, present_SIJ=[present["stated"], present["inferable"], present["judgment"]],
             n_failed=len(res.failed_fields), failed_fields=list(res.failed_fields), codes=dict(sorted(codes.items())),
             parse_path=res.parse_path)
    rows.append(r)

scored = [r for r in rows if r["role"] == "scored"]
tot = {}
for arm in ("old", "new"):
    a = [r for r in scored if r["arm"] == arm]
    c = collections.Counter(r["shape"] for r in a)
    tot[arm] = {"n": len(a), "shapes": dict(sorted(c.items())), "stated_only": c["stated_only"]}
paired = {}
for r in scored:
    paired.setdefault(r["case"], {"historical_attempt2_shape": cases[r["case"]]["historical_attempt2_shape"]})[r["arm"]] = r["shape"]
hist_so = sum(1 for p in paired.values() if p["historical_attempt2_shape"] == "stated_only")
agree = sum(1 for p in paired.values() if (p.get("old") == "stated_only") == (p["historical_attempt2_shape"] == "stated_only"))
print(json.dumps({"rows": rows, "paired": paired, "totals": tot,
                  "historical_stated_only_among_sent": f"{hist_so} of {len(paired)}",
                  "old_arm_agrees_with_history_on_stated_only": f"{agree} of {len(paired)}"},
                 indent=1, sort_keys=True))
