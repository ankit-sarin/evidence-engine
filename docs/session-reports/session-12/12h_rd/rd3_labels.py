"""12h RD-3 — the stored label against the derived state, on the retained ra_12g review.
Read-only (mode=ro). usage: rd3_labels.py <review.db>   (stdout: JSON)"""
import json, sqlite3, sys
from collections import Counter

c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
rows = c.execute("SELECT event_id, event_type, claim_id, payload_json FROM field_events "
                 "ORDER BY event_id").fetchall()
by_type = Counter()
has_key = Counter()
located = {}
for _id, et, claim, pj in rows:
    p = json.loads(pj or "{}")
    by_type[(et, json.dumps(p.get("state_at_write")))] += 1
    has_key[(et, "state_at_write" in p)] += 1
    if et == "citation_located":
        located[claim] = located.get(claim, False) or bool(p.get("located"))
asserted = [(claim, json.loads(pj)["state_at_write"]) for _i, et, claim, pj in rows
            if et == "asserted"]
out = {
    "field_events": len(rows),
    "label_by_event_type": {f"{et} | {lab}": n for (et, lab), n in sorted(by_type.items())},
    "payload_has_key_by_event_type": {f"{et} | {k}": n for (et, k), n in sorted(has_key.items())},
    "asserted_events": len(asserted),
    "asserted_with_a_located_citation_event": sum(1 for cl, _ in asserted if located.get(cl)),
    "asserted_label_without_evidence_but_located_later":
        sum(1 for cl, lab in asserted
            if lab == "asserted without locatable evidence" and located.get(cl)),
    "asserted_with_no_citation_event": sum(1 for cl, _ in asserted if cl not in located),
    "asserted_with_citation_event_not_located":
        sum(1 for cl, _ in asserted if cl in located and not located[cl]),
}
print(json.dumps(out, indent=1, sort_keys=True))
