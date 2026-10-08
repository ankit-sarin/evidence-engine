"""12h RD-4 — units beyond the first contiguous run, on the retained ra_12g review.
Read-only (mode=ro; files opened for reading). usage, from the repository root:
    rd4_runs.py <retained_dir>      (stdout: JSON)

For every live `asserted` event: the accepted Pass-1 attempt's cited indices
(telemetry/extraction_calls.jsonl, extra.fields), their contiguous runs
(engine.elicitation.materialize.contiguous_runs — the engine's own function,
imported), the stored source_snippet, and the unit map on disk.
"""
import glob, json, os, sqlite3, sys
from collections import Counter

sys.path.insert(0, os.getcwd())
from engine.elicitation.materialize import contiguous_runs
assert not any(m == "ollama" or m.startswith("ollama.") for m in sys.modules), "ollama imported"

R = sys.argv[1]
c = sqlite3.connect(f"file:{os.path.join(R, 'review.db')}?mode=ro", uri=True)
tel = {}
for line in open(os.path.join(R, "telemetry", "extraction_calls.jsonl")):
    t = json.loads(line)
    tel.setdefault(t["paper_id"], []).append(t)
maps = {}
for p in glob.glob(os.path.join(R, "elicitation", "*", "unit_maps", "*.json")):
    m = json.load(open(p))
    maps[m["paper_id"]] = m

rows = c.execute("SELECT paper_id, field_name, claim_id, source_snippet, payload_json "
                 "FROM field_events WHERE event_type = 'asserted' ORDER BY event_id").fetchall()
n_runs = Counter(); payload_keys = Counter()
first_equal = 0; multi = 0
units_cited = units_stored = 0
later_resolvable = later_units = 0
tel_rows_per_paper = Counter(len(v) for v in tel.values())
unit_map_keys = Counter()
for m in maps.values():
    unit_map_keys.update(m.keys())
per_claim = []
for pid, field, claim, snippet, pj in rows:
    payload_keys.update(json.loads(pj).keys())
    stored = [t for t in tel[pid] if t.get("outcome") == "stored"] or tel[pid]
    f = stored[-1]["extra"]["fields"][field]
    runs = contiguous_runs(tuple(f["indices"]))
    units = maps[pid]["units"]
    # the unit map is 1-indexed in prompts; resolve the way the engine's file is laid out
    def text(i): return units[i - 1]
    first = " ".join(text(i) for i in runs[0]).strip() if runs else ""
    n_runs[len(runs)] += 1
    first_equal += (first == (snippet or ""))
    cited = sum(len(r) for r in runs)
    units_cited += cited; units_stored += len(runs[0]) if runs else 0
    if len(runs) > 1:
        multi += 1
        for r in runs[1:]:
            for i in r:
                later_units += 1
                later_resolvable += (1 <= i <= len(units))
    per_claim.append({"paper_id": pid, "field_name": field, "n_indices": len(f["indices"]),
                      "n_runs": len(runs), "units_in_first_run": len(runs[0]) if runs else 0,
                      "units_in_later_runs": cited - (len(runs[0]) if runs else 0)})

by_field = {}
for r in per_claim:
    b = by_field.setdefault(r["field_name"], {"claims": 0, "multi_run": 0})
    b["claims"] += 1; b["multi_run"] += r["n_runs"] > 1
print(json.dumps({
    "asserted_events": len(rows),
    "telemetry_rows_per_paper": dict(tel_rows_per_paper),
    "telemetry_record_keys": sorted(next(iter(tel.values()))[0].keys()),
    "unit_map_file_keys": dict(unit_map_keys),
    "claims_by_number_of_runs": {str(k): v for k, v in sorted(n_runs.items())},
    "claims_citing_more_than_one_run": multi,
    "stored_snippet_equals_first_run": first_equal,
    "units_cited_total": units_cited,
    "units_in_stored_first_runs": units_stored,
    "units_in_later_runs_not_on_the_claim": units_cited - units_stored,
    "later_run_units_resolvable_in_unit_map": f"{later_resolvable} of {later_units}",
    "asserted_payload_keys": dict(sorted(payload_keys.items())),
    "multi_run_by_field": dict(sorted(by_field.items())),
}, indent=1, sort_keys=True))
