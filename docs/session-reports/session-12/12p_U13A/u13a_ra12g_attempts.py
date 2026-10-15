"""12p-U13A — the 12g side of R617(a) and R622(b), from ra_12g's telemetry. Read-only (files only).
usage, from the repository root: u13a_ra12g_attempts.py <ra_12g dir> [<codebook.yaml>]   (stdout: JSON)
* rows per paper and their outcomes (a completeness retry or a Pass-1 duplicate leaves >1 row);
* per paper: attempts run, accepted attempt, attempt-1 failed fields;
* R617(a): every attempt-1 JUDGMENT field — its codes as recorded, and, where attempt 1's raw
  response is on disk (only when attempt 1 was accepted: 931e385 kept the accepted attempt's raw
  only), whether the entry carried a field-level citation and whether any step cited a unit, and
  the verdict each check gives (pre-dccd679: no citation in field-level ∪ steps; at HEAD: also no
  step citing a unit). Parsing uses the engine's own contracts.parse_container at HEAD.
* R622(b): duplicate field names in each attempt's recorded fields / raw response."""
import json, os, sys
from collections import Counter
sys.path.insert(0, os.getcwd())
import yaml
from engine.elicitation.contracts import parse_container, KEY_FIELD, KEY_INDICES, KEY_STEPS
d = sys.argv[1]
cb = yaml.safe_load(open(sys.argv[2] if len(sys.argv) > 2 else os.path.join(d, "extraction_codebook.yaml")))
cls = {f["name"]: f.get("field_class") for f in cb["fields"]}
rows = [json.loads(l) for l in open(os.path.join(d, "telemetry", "extraction_calls.jsonl"))]
per_paper, judg, dup = {}, [], []
by_pid = {}
for r in rows: by_pid.setdefault(r["paper_id"], []).append(r)
for pid, rs in sorted(by_pid.items()):
    el = [r for r in rs if (r.get("extra") or {}).get("attempts")]
    per_paper[pid] = {"rows": len(rs), "outcomes": [r["outcome"] for r in rs], "rows_with_attempts": len(el)}
    if not el: continue
    e = el[-1]["extra"]; acc = e["accepted_attempt"]; a1 = e["attempts"][0]
    per_paper[pid].update(n_attempts=len(e["attempts"]), accepted=acc, a1_failed=len(a1["failed_fields"]))
    raw1 = e.get("pass1_raw_content") if acc == 1 else None
    ents = parse_container(raw1)[0] if raw1 else []
    names = [str(x.get(KEY_FIELD)) for x in ents if x.get(KEY_FIELD)]
    dup.append({"paper_id": pid, "attempt_1_raw_on_disk": raw1 is not None,
                "duplicates_in_accepted_raw": sorted({n for n in names if names.count(n) > 1})})
    by_name = {str(x.get(KEY_FIELD)): x for x in ents}
    for name, f in a1["fields"].items():
        if f["class"] != "judgment": continue
        row = {"paper_id": pid, "field": name, "codes": f["violations"], "escape": f["escape"],
               "raw_on_disk": raw1 is not None}
        x = by_name.get(name)
        if x is not None:
            fl = x.get(KEY_INDICES) or []
            steps = x.get(KEY_STEPS) if isinstance(x.get(KEY_STEPS), list) else []
            step_cites = any(isinstance(s, dict) and s.get(KEY_INDICES) for s in steps)
            row.update(field_level_citation=bool(fl), any_step_cites=step_cites)
            if not f["escape"]:
                row["vwc_pre_dccd679"] = not (fl or step_cites)
                row["vwc_at_head"] = not (fl or step_cites) or not step_cites
        judg.append(row)
vwc1 = [j for j in judg if "VALUE_WITHOUT_CITATION" in j["codes"]]
on_disk = [j for j in judg if "vwc_at_head" in j]
print(json.dumps({
    "papers": per_paper,
    "papers_with_more_than_one_row": [p for p, v in per_paper.items() if v["rows"] > 1],
    "attempt1_judgment_fields": len(judg),
    "attempt1_judgment_vwc_recorded": len(vwc1),
    "attempt1_judgment_raw_on_disk": len(on_disk),
    "on_disk_split": {
        "field_level_citation_and_no_step_cites": sum(1 for j in on_disk if j["field_level_citation"] and not j["any_step_cites"]),
        "no_field_level_citation_and_no_step_cites": sum(1 for j in on_disk if not j["field_level_citation"] and not j["any_step_cites"]),
        "vwc_pre_dccd679": sum(1 for j in on_disk if j["vwc_pre_dccd679"]),
        "vwc_at_head": sum(1 for j in on_disk if j["vwc_at_head"]),
        "pass_pre_fail_at_head": sum(1 for j in on_disk if j["vwc_at_head"] and not j["vwc_pre_dccd679"]),
        "pass_pre_fail_at_head_already_failing_other_code": sum(
            1 for j in on_disk if j["vwc_at_head"] and not j["vwc_pre_dccd679"] and j["codes"]),
        "pass_pre_fail_at_head_field_newly_failing": sum(
            1 for j in on_disk if j["vwc_at_head"] and not j["vwc_pre_dccd679"] and not j["codes"])},
    "pass_pre_fail_at_head_rows": [j for j in on_disk if j["vwc_at_head"] and not j["vwc_pre_dccd679"]],
    "vwc_recorded_rows": vwc1, "duplicates": dup}, indent=1, sort_keys=True))
