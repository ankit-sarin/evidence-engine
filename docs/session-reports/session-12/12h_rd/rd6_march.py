"""12h RD-6 — the March abstract decision rows and run records on live. Read-only (mode=ro).
usage: rd6_march.py <review.db>   (stdout: JSON)

Pairs each pass-1 row with the pass-2 row that follows it for the same paper (adjacent ids,
the order run_screening / screen_expanded write them) and reports how often the two passes
agreed, and how often their rationale text was identical. Also what the tables can and
cannot show: the columns of the decision table and of review_runs.
"""
import json, sqlite3, sys
from collections import Counter

c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
cols = lambda t: [r[1] for r in c.execute(f"PRAGMA table_info({t})")]
rows = c.execute("SELECT id, paper_id, pass_number, decision, rationale, model, decided_at "
                 "FROM abstract_screening_decisions ORDER BY id").fetchall()
by_pass = Counter((r[2], r[5]) for r in rows)
pairs = []; unpaired = 0
i = 0
while i < len(rows):
    a = rows[i]
    b = rows[i + 1] if i + 1 < len(rows) else None
    if a[2] == 1 and b is not None and b[2] == 2 and b[1] == a[1]:
        pairs.append((a, b)); i += 2
    else:
        unpaired += 1; i += 1
by_month = {}
for a, b in pairs:
    m = a[6][:7]
    d = by_month.setdefault(m, {"pairs": 0, "decisions_agree": 0, "rationale_identical": 0})
    d["pairs"] += 1
    d["decisions_agree"] += a[3] == b[3]
    d["rationale_identical"] += (a[4] or "") == (b[4] or "")
tot = {k: sum(d[k] for d in by_month.values()) for k in ("pairs", "decisions_agree", "rationale_identical")}
combo = Counter((a[3], b[3]) for a, b in pairs)
runs = c.execute("SELECT id, started_at, completed_at, status, length(log) FROM review_runs "
                 "ORDER BY id").fetchall()
rb_rows = None
if len(sys.argv) > 2:   # the 12g front-half rehearsal: what the two passes send at HEAD
    rb = sqlite3.connect(f"file:{sys.argv[2]}?mode=ro", uri=True)
    rb_rows = {
        "stage_rows": [dict(zip(("stage", "model", "options_json", "sent_keys_json", "prompt_hash"), r))
                       for r in rb.execute(
                           "SELECT stage, model_name, options_json, sent_keys_json, prompt_hash "
                           "FROM run_stage_configs WHERE stage LIKE 'abstract_screen%' ORDER BY stage")],
        "abstract_screen_primary_calls": rb.execute(
            "SELECT count(*) FROM run_calls WHERE stage = 'abstract_screen_primary'").fetchone()[0],
        "distinct_request_hashes": rb.execute(
            "SELECT count(DISTINCT request_hash) FROM run_calls "
            "WHERE stage = 'abstract_screen_primary'").fetchone()[0],
        "request_hashes_used_exactly_twice": rb.execute(
            "SELECT count(*) FROM (SELECT request_hash FROM run_calls WHERE stage = "
            "'abstract_screen_primary' GROUP BY request_hash HAVING count(*) = 2)").fetchone()[0],
    }
print(json.dumps({
    "rb_12g_at_HEAD": rb_rows,
    "abstract_screening_decisions_columns": cols("abstract_screening_decisions"),
    "review_runs_columns": cols("review_runs"),
    "rows": len(rows),
    "rows_by_pass_and_model": {f"pass {p} | {m}": n for (p, m), n in sorted(by_pass.items())},
    "papers_with_rows": len({r[1] for r in rows}),
    "first_decided_at": rows[0][6], "last_decided_at": max(r[6] for r in rows),
    "adjacent_pass1_pass2_pairs": len(pairs), "rows_not_in_an_adjacent_pair": unpaired,
    "totals": tot,
    "decisions_disagree": tot["pairs"] - tot["decisions_agree"],
    "pass1_pass2_decision_combinations": {f"{a} / {b}": n for (a, b), n in sorted(combo.items())},
    "by_month": by_month,
    "review_runs": [dict(zip(("id", "started_at", "completed_at", "status", "log_chars"), r))
                    for r in runs],
    "manifest_tables_rows": {t: c.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                             for t in ("run_manifests", "run_stage_configs", "run_calls")},
}, indent=1, sort_keys=True))
