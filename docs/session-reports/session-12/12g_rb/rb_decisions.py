"""rb_decisions.py — decisions per record and per stratum, the E5 records, events and
workflow rows, and an INFORMATIONAL agreement table against live's March decisions
(12g_RB-A.md section 5). Read-only on the retained copy. From the repository root:
    .venv/bin/python docs/session-reports/session-12/12g_rb/rb_decisions.py <retained_dir>
Writes rb_decisions.json and rb_decisions_records.csv beside this file."""
import collections
import csv
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.getcwd())
from engine.agents.screener import build_messages
from engine.core.review_spec import load_review_spec

HERE = os.path.dirname(os.path.abspath(__file__))
RET = sys.argv[1]
AGREEMENT_LABEL = "prompts changed since March; not a performance measure"
conn = sqlite3.connect(f"file:{os.path.join(RET, 'review.db')}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
spec = load_review_spec(os.path.join(RET, "spec.yaml"))
sample = {int(r["throwaway_paper_id"]): r for r in csv.DictReader(open(os.path.join(HERE, "rb_sample.csv")))}


def outcome(a, b):
    if a == "include" and b == "include":
        return "in"
    if a == "exclude" and b == "exclude":
        return "out"
    return "flagged"


STATUS = {"ABSTRACT_SCREENED_IN": "in", "ABSTRACT_SCREENED_OUT": "out", "ABSTRACT_SCREEN_FLAGGED": "flagged"}
rows = []
for p in conn.execute("SELECT * FROM papers ORDER BY id"):
    d = {r["pass_number"]: r for r in conn.execute(
        "SELECT pass_number, decision, rationale, model FROM abstract_screening_decisions WHERE paper_id = ? ORDER BY id", (p["id"],))}
    s = sample[p["id"]]
    msgs = build_messages(dict(p), spec, role="primary")       # the request the screener builds for this record
    user = msgs[-1]["content"]
    now = STATUS[p["status"]]
    assert now == outcome(d[1]["decision"], d[2]["decision"]), p["id"]
    march = outcome(s["march_pass1"], s["march_pass2"])
    rows.append({
        "paper_id": p["id"], "live_paper_id": int(s["live_paper_id"]), "stratum": s["stratum"],
        "abstract_chars": len(p["abstract"] or ""), "abstract_is_null": int(p["abstract"] is None),
        "request_chars": sum(len(m["content"]) for m in msgs),
        "user_message_has_abstract_label": int("Abstract: " in user),
        "pass1": d[1]["decision"], "pass2": d[2]["decision"], "status": p["status"], "outcome": now,
        "march_pass1": s["march_pass1"], "march_pass2": s["march_pass2"], "march_outcome": march,
        "same_outcome_as_march": int(now == march), "model": d[1]["model"],
        "rationale_pass1": d[1]["rationale"], "rationale_pass2": d[2]["rationale"],
    })
with open(os.path.join(HERE, "rb_decisions_records.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

by_stratum = {}
for r in rows:
    e = by_stratum.setdefault(r["stratum"], {"records": 0, "in": 0, "out": 0, "flagged": 0, "same_outcome_as_march": 0,
                                             "march": collections.Counter()})
    e["records"] += 1
    e[r["outcome"]] += 1
    e["same_outcome_as_march"] += r["same_outcome_as_march"]
    e["march"][r["march_outcome"]] += 1
for e in by_stratum.values():
    e["march"] = dict(e["march"])
e5 = [r for r in rows if r["abstract_chars"] < 200]
fallback = None
for r in e5:
    if r["abstract_is_null"]:
        p = conn.execute("SELECT * FROM papers WHERE id = ?", (r["paper_id"],)).fetchone()
        fallback = build_messages(dict(p), spec, role="primary")[-1]["content"]
        break
cross = collections.Counter((r["march_outcome"], r["outcome"]) for r in rows)
summary = {
    "records": len(rows), "decision_rows": conn.execute("SELECT COUNT(*) FROM abstract_screening_decisions").fetchone()[0],
    "status": dict(collections.Counter(r["status"] for r in rows)),
    "pass_pairs": {f"{a}/{b}": n for (a, b), n in collections.Counter((r["pass1"], r["pass2"]) for r in rows).items()},
    "passes_disagreed": sum(r["pass1"] != r["pass2"] for r in rows),
    "models": dict(collections.Counter(r["model"] for r in rows)),
    "paper_events": conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0],
    "field_events": conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0],
    "paper_events_of_the_screening_run": conn.execute("SELECT COUNT(*) FROM paper_events WHERE run_id = 2").fetchone()[0],
    "workflow_state": {r[0]: r[1] for r in conn.execute("SELECT stage_name, status FROM workflow_state ORDER BY id")},
    "arms": [dict(r) for r in conn.execute("SELECT arm_name, configuration_marker, pinned_run_id, pinned_sha256 FROM arms")],
    "parsed_text_refs": conn.execute("SELECT COUNT(*) FROM parsed_text_refs").fetchone()[0],
    "by_stratum": by_stratum,
    "e5_records": [{k: r[k] for k in ("paper_id", "live_paper_id", "abstract_chars", "abstract_is_null", "request_chars",
                                      "user_message_has_abstract_label", "pass1", "pass2", "status", "rationale_pass1", "rationale_pass2")} for r in e5],
    "e5_user_message_for_a_null_abstract": fallback,
    "agreement_label": AGREEMENT_LABEL,
    "agreement_same_outcome": sum(r["same_outcome_as_march"] for r in rows),
    "agreement_cross_tab_march_then_now": {f"{a}->{b}": n for (a, b), n in sorted(cross.items())},
    "changed_records": [{k: r[k] for k in ("paper_id", "live_paper_id", "stratum", "march_pass1", "march_pass2", "pass1", "pass2")} for r in rows if not r["same_outcome_as_march"]],
}
json.dump(summary, open(os.path.join(HERE, "rb_decisions.json"), "w"), indent=1, sort_keys=True)
print(json.dumps({k: v for k, v in summary.items() if k not in ("e5_records",)}, indent=1, sort_keys=True))
for r in summary["e5_records"]:
    print(r["live_paper_id"], r["abstract_chars"], "null" if r["abstract_is_null"] else "", r["pass1"], r["pass2"], "|", r["rationale_pass1"][:230])
