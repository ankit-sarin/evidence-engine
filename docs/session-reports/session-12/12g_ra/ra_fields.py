"""ra_fields.py — field outcomes per paper and per field, incomplete_fields, Pass-1 attempts,
and what R535's floor would have done (12g_RA-A.md section 4). Writes ra_fields.json."""
import collections
import json

from ra_common import LABEL, LIVE_IDS, RUN_ID, db, telemetry, write

conn = db()
EXPECTED = 20
tel = {t["paper_id"]: t for t in telemetry()}
papers, per_field, violations = [], collections.defaultdict(collections.Counter), collections.Counter()
viol_by_field = collections.defaultdict(collections.Counter)
for pid in range(1, 13):
    ev = conn.execute("SELECT to_state, reason_code, payload_json FROM paper_events WHERE run_id = ? AND paper_id = ? "
                      "AND event_type IN ('extracted', 'extraction_failed') ORDER BY event_id", (RUN_ID, pid)).fetchall()
    assert len(ev) == 1, (pid, len(ev))
    payload = json.loads(ev[0]["payload_json"])
    fe = conn.execute("SELECT event_type, field_name, value, payload_json FROM field_events WHERE run_id = ? AND paper_id = ? "
                      "AND event_type IN ('asserted', 'declined', 'contract_unmet')", (RUN_ID, pid)).fetchall()
    counts = collections.Counter(r["event_type"] for r in fe)
    for r in fe:
        per_field[r["field_name"]][r["event_type"]] += 1
        if r["event_type"] == "contract_unmet":
            for code in json.loads(r["payload_json"]).get("violation_codes", []):
                violations[code] += 1
                viol_by_field[r["field_name"]][code] += 1
    e = (tel.get(pid, {}).get("extra") or {})
    attempts = e.get("attempts") or []
    incomplete = payload.get("incomplete_fields", [])
    row = {
        "paper_id": pid, "live_paper_id": LIVE_IDS[pid - 1], "to_state": ev[0]["to_state"], "reason_code": ev[0]["reason_code"],
        "asserted": counts["asserted"], "declined": counts["declined"], "contract_unmet": counts["contract_unmet"],
        "fields_with_a_terminal_state": len(fe), "incomplete_fields": incomplete, "n_incomplete": len(incomplete),
        "payload_counts_agree": ev[0]["to_state"] != "extracted" or (
            payload["asserted"], payload["declined"], payload["contract_unmet"]) == (
            counts["asserted"], counts["declined"], counts["contract_unmet"]),
        "pass1_attempts": len(attempts), "pass1_accepted_attempt": e.get("accepted_attempt"),
        "pass1_failed_fields_by_attempt": [a["n_failed"] for a in attempts],
        "pass1_failed_field_names_attempt1": attempts[0]["failed_fields"] if attempts else [],
        "completeness_attempts": tel.get(pid, {}).get("attempt"), "telemetry_outcome": tel.get(pid, {}).get("outcome"),
        "missing_fields_telemetry": tel.get(pid, {}).get("missing_fields"),
        # R535: "An exhausted record holding fewer than half of the expected fields"
        "under_R535_floor_by_fields_held": ev[0]["to_state"] == "extracted" and (EXPECTED - len(incomplete)) < EXPECTED / 2,
        "asserted_values_under_half": ev[0]["to_state"] == "extracted" and counts["asserted"] < EXPECTED / 2,
    }
    papers.append(row)
ext = [p for p in papers if p["to_state"] == "extracted"]
two = [p for p in ext if p["pass1_attempts"] == 2]
summary = {
    "label": LABEL, "fields_expected": EXPECTED, "papers": papers, "extracted": len(ext),
    "not_extracted": [{k: p[k] for k in ("paper_id", "live_paper_id", "to_state", "reason_code")} for p in papers if p["to_state"] != "extracted"],
    "totals": {k: sum(p[k] for p in ext) for k in ("asserted", "declined", "contract_unmet", "n_incomplete")},
    "cells": len(ext) * EXPECTED,
    "papers_with_any_incomplete_field": sum(p["n_incomplete"] > 0 for p in ext),
    "papers_under_R535_floor_by_fields_held": sum(p["under_R535_floor_by_fields_held"] for p in ext),
    "papers_with_asserted_values_under_half": [p["live_paper_id"] for p in ext if p["asserted_values_under_half"]],
    "papers_with_any_contract_unmet": sum(p["contract_unmet"] > 0 for p in ext),
    "completeness_retries": sum((p["completeness_attempts"] or 1) > 1 for p in ext),
    "pass1": {
        "papers_with_two_attempts": len(two),
        "attempt2_accepted": sum(p["pass1_accepted_attempt"] == 2 for p in two),
        "attempt2_not_accepted": sum(p["pass1_accepted_attempt"] == 1 for p in two),
        "failed_fields_attempt1_total": sum(p["pass1_failed_fields_by_attempt"][0] for p in ext),
        "failed_fields_attempt2_total_where_run": sum(p["pass1_failed_fields_by_attempt"][1] for p in two),
        "failed_fields_attempt1_total_where_attempt2_run": sum(p["pass1_failed_fields_by_attempt"][0] for p in two),
        "attempt2_strictly_fewer": sum(p["pass1_failed_fields_by_attempt"][1] < p["pass1_failed_fields_by_attempt"][0] for p in two),
        "attempt2_equal": sum(p["pass1_failed_fields_by_attempt"][1] == p["pass1_failed_fields_by_attempt"][0] for p in two),
        "attempt2_more": sum(p["pass1_failed_fields_by_attempt"][1] > p["pass1_failed_fields_by_attempt"][0] for p in two),
    },
    "violation_codes": dict(violations.most_common()),
    "per_field": {f: dict(c) for f, c in sorted(per_field.items())},
    "contract_unmet_by_field": {f: dict(c) for f, c in sorted(viol_by_field.items())},
    "all_payload_counts_agree": all(p["payload_counts_agree"] for p in papers),
}
write("ra_fields.json", summary)
print(json.dumps({k: v for k, v in summary.items() if k not in ("papers", "per_field", "contract_unmet_by_field")}, indent=1, sort_keys=True))
for p in papers:
    print(p["paper_id"], p["live_paper_id"], p["to_state"], "A/D/U", p["asserted"], p["declined"], p["contract_unmet"], "incomplete", p["n_incomplete"], "p1 failed by attempt", p["pass1_failed_fields_by_attempt"], "accepted", p["pass1_accepted_attempt"])
for f, c in sorted(per_field.items()):
    print(f"{f:28s}", dict(c), dict(viol_by_field.get(f, {})))
