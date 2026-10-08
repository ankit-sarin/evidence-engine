"""rb_calls.py — the front-half rehearsal's calls against its declarations, timing, input fit
and foreign model loads (12g_RB-A.md section 5). Read-only on the retained copy. From the
repository root:  .venv/bin/python docs/session-reports/session-12/12g_rb/rb_calls.py <retained_dir>
Writes rb_calls.json and rb_calls_records.csv beside this file."""
import csv
import json
import os
import re
import sqlite3
import statistics
import sys
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
RET = sys.argv[1]
RUN_ID = 2
BLOBS = {"6150cb382311": "deepseek-r1:32b", "e796792eba26": "gemma3:27b", "a3de86cd1c13": "qwen3:8b"}
conn = sqlite3.connect(f"file:{os.path.join(RET, 'review.db')}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
ts = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))
man = conn.execute("SELECT * FROM run_manifests WHERE run_id = ?", (RUN_ID,)).fetchone()
declared = {r["stage"]: dict(r) for r in conn.execute(
    "SELECT stage, stage_kind, arm_name, model_name FROM run_stage_configs WHERE run_id = ?", (RUN_ID,))}
calls = [dict(r) for r in conn.execute(
    "SELECT call_id, stage, paper_id, outcome, outcome_detail, started_at, ended_at FROM run_calls WHERE run_id = ? ORDER BY call_id", (RUN_ID,))]
by_stage = {}
for c in calls:
    by_stage.setdefault(c["stage"], {}).setdefault(c["outcome"], 0)
    by_stage[c["stage"]][c["outcome"]] += 1
stage_table = [{"stage": s, "model": d["model_name"], "arm": d["arm_name"], "calls": sum(by_stage.get(s, {}).values()),
                "outcomes": by_stage.get(s, {}),
                "note": "" if s in by_stage else "declared, not reached: the run stops at the adjudication gate (RB-A2)"}
               for s, d in sorted(declared.items())]

log = open(os.path.join(RET, "logs", "rb_12g_run.log"), errors="replace").read().splitlines()
day = man["started_at"][:10]
LINE = re.compile(r"^(\d\d:\d\d:\d\d) \[(INFO|ERROR|WARNING)\] engine\.utils\.ollama_client: "
                  r"input_fit (REFUSED |TRUNCATED |DROPPED )?paper_id=(\S+) (\{.*\})\s*$")
fits = [{"t": datetime.fromisoformat(f"{day}T{m.group(1)}+00:00"), "flag": (m.group(3) or "").strip(), **json.loads(m.group(5))}
        for m in (LINE.match(l) for l in log) if m]
assert len(fits) == len(calls), (len(fits), len(calls))
for c, f in zip(calls, fits):
    assert abs((f["t"] - ts(c["ended_at"])).total_seconds()) <= 2, (c, f)
    c.update(chars=f["chars"], count=f.get("count"), ratio=f.get("ratio"), ceiling=f["ceiling"],
             done_reason=f.get("done_reason"), seconds=round((ts(c["ended_at"]) - ts(c["started_at"])).total_seconds(), 2))

# pair screening calls to records: two consecutive calls per record, ascending papers.id
screen = [c for c in calls if c["stage"] == "abstract_screen_primary"]
papers = [r["id"] for r in conn.execute("SELECT id FROM papers ORDER BY id")]
assert len(screen) == 2 * len(papers)
idmap = {m["throwaway_paper_id"]: m for m in json.load(open(os.path.join(HERE, "id_map.json")))}
dec = {}
for r in conn.execute("SELECT paper_id, pass_number, decision, decided_at FROM abstract_screening_decisions ORDER BY id"):
    dec[(r["paper_id"], r["pass_number"])] = r
records, max_lag = [], 0.0
for n, pid in enumerate(papers):
    c1, c2 = screen[2 * n], screen[2 * n + 1]
    for k, c in ((1, c1), (2, c2)):
        lag = (ts(dec[(pid, k)]["decided_at"]) - ts(c["ended_at"])).total_seconds()
        assert 0 <= lag < 1.0, (pid, k, lag)       # the decision row is written right after its call ends
        max_lag = max(max_lag, lag)
    records.append({"paper_id": pid, "live_paper_id": idmap[pid]["live_paper_id"], "abstract_chars": idmap[pid]["abstract_chars"],
                    "request_chars_pass1": c1["chars"], "prompt_tokens_pass1": c1["count"], "tokens_per_char_pass1": c1["ratio"],
                    "seconds_pass1": c1["seconds"], "seconds_pass2": c2["seconds"],
                    "seconds_record": round((ts(c2["ended_at"]) - ts(c1["started_at"])).total_seconds(), 2),
                    "done_reason_pass1": c1["done_reason"], "done_reason_pass2": c2["done_reason"],
                    "prompt_share_of_ceiling": round(c1["count"] / c1["ceiling"], 4), "ceiling": c1["ceiling"]})
with open(os.path.join(HERE, "rb_calls_records.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(records[0].keys()))
    w.writeheader()
    w.writerows(records)

loads = []
for ln in open(os.path.join(HERE, "ollama_journal_loads.txt")).read().splitlines():
    m = re.search(r'time=(\S+) .*msg="starting runner" cmd=".*--model \S*blobs/sha256-([0-9a-f]{12})', ln)
    if m:
        model = BLOBS.get(m.group(2), "UNKNOWN")
        t = ts(m.group(1))
        cause = [c for c in calls if declared[c["stage"]]["model_name"] == model
                 and ts(c["started_at"]) - timedelta(seconds=5) <= t <= ts(c["ended_at"])]
        loads.append({"time": m.group(1), "model": model, "run_call_in_flight": cause[0]["stage"] if cause else None,
                      "foreign": not cause})
reqs = {}
for ln in open(os.path.join(HERE, "ollama_journal_requests.txt")).read().splitlines():
    n, method, path = ln.split()
    reqs[f"{method} {path.strip(chr(34))}"] = int(n)

sec = [c["seconds"] for c in screen]
longest = sorted(records, key=lambda r: -r["abstract_chars"])[:2]
summary = {
    "manifest": {"run_kind": man["run_kind"], "end_status": man["end_status"], "end_reason": man["end_reason"],
                 "started_at": man["started_at"], "ended_at": man["ended_at"], "git_commit": man["git_commit"],
                 "wall_seconds": round((ts(man["ended_at"]) - ts(man["started_at"])).total_seconds(), 1)},
    "declared_stage_rows": len(declared), "stages_with_calls": sorted(by_stage), "stage_table": stage_table,
    "calls_total": len(calls), "calls_under_undeclared_stage": sum(c["stage"] not in declared for c in calls),
    "outcomes": {o: sum(c["outcome"] == o for c in calls) for o in sorted({c["outcome"] for c in calls})},
    "calls_with_null_paper_id": sum(c["paper_id"] is None for c in calls),
    "log": {"UndeclaredCall_or_Override": sum(bool(re.search("UndeclaredCall|UndeclaredOverride", l)) for l in log),
            "Traceback": sum("Traceback" in l for l in log), "STAGE_lines": [l[9:] for l in log if "STAGE:" in l],
            "gate_lines": [l[9:] for l in log if "BLOCKED" in l or "Current stage" in l]},
    "screen_call_seconds": {"n": len(sec), "min": min(sec), "median": statistics.median(sec), "max": max(sec), "sum": round(sum(sec), 1)},
    "record_seconds": {"min": min(r["seconds_record"] for r in records), "median": statistics.median(r["seconds_record"] for r in records),
                       "max": max(r["seconds_record"] for r in records)},
    "max_decision_row_lag_after_call_seconds": round(max_lag, 3),
    "preflight": [{k: c[k] for k in ("stage", "outcome", "seconds", "chars", "count", "done_reason")} for c in calls if c["stage"].startswith("preflight")],
    "tokens_per_char": {"min": min(c["ratio"] for c in screen), "median": statistics.median(c["ratio"] for c in screen), "max": max(c["ratio"] for c in screen)},
    "done_reasons_screen": {k: sum(c["done_reason"] == k for c in screen) for k in sorted({str(c["done_reason"]) for c in screen})},
    "ceiling": screen[0]["ceiling"], "max_prompt_share_of_ceiling": max(r["prompt_share_of_ceiling"] for r in records),
    "longest_abstracts": longest,
    "input_fit_flags": {k: sum(f["flag"] == k for f in fits) for k in sorted({f["flag"] for f in fits})},
    "journal_loads": loads, "foreign_loads": [l for l in loads if l["foreign"]], "journal_requests": reqs,
    "journal_chat_requests_equal_completed_calls": reqs.get("POST /api/chat") == sum(c["outcome"] == "completed" for c in calls),
}
json.dump(summary, open(os.path.join(HERE, "rb_calls.json"), "w"), indent=1, sort_keys=True)
print(json.dumps(summary, indent=1, sort_keys=True))
