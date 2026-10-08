"""ra_tokens.py — estimated vs actual prompt tokens, the headroom proxies, input-fit
outcomes and per-paper timing (12g_RA-A.md section 4). Writes ra_tokens.json and
ra_tokens_calls.csv. eval_count is not recorded by the engine (RA-1); output size is
reported in characters and as done_reason only."""
import csv
import json
import os
import re
import statistics
from datetime import datetime, timezone

from ra_common import HERE, LABEL, LIVE_IDS, RUN_ID, db, retained, telemetry, write

LINE = re.compile(r"^(\d\d:\d\d:\d\d) \[(INFO|ERROR|WARNING)\] engine\.utils\.ollama_client: "
                  r"input_fit (REFUSED |TRUNCATED |DROPPED )?paper_id=(\S+) (\{.*\})\s*$")
conn = db()
man = conn.execute("SELECT started_at, ended_at FROM run_manifests WHERE run_id = ?", (RUN_ID,)).fetchone()
day = man["started_at"][:10]
calls = [dict(r) for r in conn.execute(
    "SELECT call_id, stage, paper_id, outcome, outcome_detail, started_at, ended_at FROM run_calls "
    "WHERE run_id = ? ORDER BY call_id", (RUN_ID,))]
ts = lambda s: datetime.fromisoformat(s)

fits = []
for line in open(os.path.join(retained(), "logs", "ra_12g_run.log"), errors="replace"):
    m = LINE.match(line)
    if m:
        fits.append({"t": datetime.fromisoformat(f"{day}T{m.group(1)}+00:00"), "flag": (m.group(3) or "").strip(),
                     "paper": None if m.group(4) == "unknown" else int(m.group(4)), **json.loads(m.group(5))})
# one input_fit line per call, in call order: pair them in sequence and check each pair
assert len(fits) == len(calls), (len(fits), len(calls))
rows = []
for c, f in zip(calls, fits):
    assert c["paper_id"] == f["paper"], (c, f)
    assert abs((f["t"] - ts(c["ended_at"])).total_seconds()) <= 2, (c, f)
    count = f.get("count")
    rows.append({
        "call_id": c["call_id"], "stage": c["stage"], "paper_id": c["paper_id"],
        "live_paper_id": LIVE_IDS[c["paper_id"] - 1] if c["paper_id"] else "",
        "outcome": c["outcome"], "request_chars": f["chars"], "estimate_low_tokens": f["estimate_low"],
        "prompt_eval_count": count if count is not None else "",
        "tokens_per_char": f.get("ratio", ""),
        "actual_over_estimate": round(count / f["estimate_low"], 4) if count and f["estimate_low"] else "",
        "ceiling": f["ceiling"], "headroom_tokens_after_prompt": f["ceiling"] - count if count is not None else "",
        "prompt_share_of_ceiling": round(count / f["ceiling"], 4) if count is not None else "",
        "done_reason": f.get("done_reason", ""),
        "seconds": round((ts(c["ended_at"]) - ts(c["started_at"])).total_seconds(), 1),
        "started_at": c["started_at"], "ended_at": c["ended_at"],
    })
with open(os.path.join(HERE, "ra_tokens_calls.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)


def dist(v):
    v = [x for x in v if x != ""]
    return {"n": len(v), "min": min(v), "median": statistics.median(v), "max": max(v)} if v else {"n": 0}


by_stage = {}
for stage in sorted({r["stage"] for r in rows}):
    rs = [r for r in rows if r["stage"] == stage and r["outcome"] == "completed"]
    by_stage[stage] = {
        "completed_calls": len(rs), "tokens_per_char": dist([r["tokens_per_char"] for r in rs]),
        "actual_over_estimate_low": dist([r["actual_over_estimate"] for r in rs]),
        "prompt_eval_count": dist([r["prompt_eval_count"] for r in rs]),
        "headroom_tokens_after_prompt": dist([r["headroom_tokens_after_prompt"] for r in rs]),
        "prompt_share_of_ceiling": dist([r["prompt_share_of_ceiling"] for r in rs]),
        "seconds": dist([r["seconds"] for r in rs]),
        "done_reason": {k: sum(r["done_reason"] == k for r in rs) for k in sorted({r["done_reason"] for r in rs})},
    }

tel = {t["paper_id"]: t for t in telemetry()}
progress = {}
for line in open(os.path.join(retained(), "logs", "ra_12g_run.log"), errors="replace"):
    m = re.search(r"Paper EE-(\d+) (EXTRACTED|FAILED) \| (?:(\d+)m)?(\d+)s", line)
    if m:
        progress[int(m.group(1))] = (m.group(2), int(m.group(3) or 0) * 60 + int(m.group(4)))
papers = []
for pid in range(1, 13):
    cs = [r for r in rows if r["paper_id"] == pid and r["stage"] != "audit"]
    t = tel.get(pid, {})
    e = t.get("extra") or {}
    p1 = [r for r in cs if r["stage"] == "elicitation_pass1" and r["outcome"] == "completed"]
    papers.append({
        "paper_id": pid, "live_paper_id": LIVE_IDS[pid - 1], "result": progress[pid][0],
        "wall_seconds": progress[pid][1],
        "extract_first_call_start": cs[0]["started_at"] if cs else None,
        "extract_last_call_end": cs[-1]["ended_at"] if cs else None,
        "model_seconds": round(sum(r["seconds"] for r in cs), 1),
        "pass1_calls": len(p1), "pass1_accepted_attempt": e.get("accepted_attempt"),
        "pass2_calls": sum(r["stage"] == "extract_pass2" for r in cs),
        "max_prompt_eval_count": max([r["prompt_eval_count"] for r in cs if r["prompt_eval_count"] != ""], default=None),
        "min_headroom_tokens": min([r["headroom_tokens_after_prompt"] for r in cs if r["headroom_tokens_after_prompt"] != ""], default=None),
        "pass1_content_chars_accepted": e.get("pass1_content_chars"),
        "pass1_thinking_chars_accepted": e.get("pass1_thinking_chars"),
        "pass2_content_chars": t.get("raw_content_chars"),
        "pass1_done_reason": t.get("pass1_done_reason"), "pass2_done_reason": t.get("finish_reason"),
        "telemetry_outcome": t.get("outcome"),
    })
ok = [p for p in papers if p["result"] == "EXTRACTED"]
audit = [r for r in rows if r["stage"] == "audit"]
summary = {
    "label": LABEL,
    "run": {"started_at": man["started_at"], "ended_at": man["ended_at"],
            "wall_seconds": round((ts(man["ended_at"]) - ts(man["started_at"])).total_seconds(), 1)},
    "calls": len(rows), "input_fit_lines": len(fits),
    "run_calls_by_stage_outcome": {f"{r['stage']}|{r['outcome']}": sum(1 for x in rows if x["stage"] == r["stage"] and x["outcome"] == r["outcome"]) for r in rows},
    "by_stage": by_stage,
    "all_done_reasons": {k: sum(r["done_reason"] == k for r in rows) for k in sorted({str(r["done_reason"]) for r in rows})},
    "refused": [{k: r[k] for k in ("paper_id", "live_paper_id", "stage", "outcome", "request_chars", "estimate_low_tokens", "ceiling")} for r in rows if r["outcome"] != "completed"],
    "papers": papers,
    "extracted_wall_seconds": dist([p["wall_seconds"] for p in ok]),
    "extracted_wall_seconds_mean": round(statistics.mean(p["wall_seconds"] for p in ok), 1),
    "extracted_model_seconds_share_of_wall": round(sum(p["model_seconds"] for p in ok) / sum(p["wall_seconds"] for p in ok), 4),
    "papers_with_two_pass1_attempts": sum(p["pass1_calls"] == 2 for p in ok),
    "audit_calls": len(audit), "audit_stage_seconds_first_call_to_manifest_close":
        round((ts(man["ended_at"]) - ts([r for r in rows if r["stage"].startswith("preflight:gemma")][0]["started_at"])).total_seconds(), 1),
    "min_headroom_tokens_any_call": min(r["headroom_tokens_after_prompt"] for r in rows if r["headroom_tokens_after_prompt"] != ""),
    "max_prompt_share_of_ceiling_any_call": max(r["prompt_share_of_ceiling"] for r in rows if r["prompt_share_of_ceiling"] != ""),
}
write("ra_tokens.json", summary)
print(json.dumps({k: v for k, v in summary.items() if k != "papers"}, indent=1, sort_keys=True))
for p in papers:
    print(p["paper_id"], p["live_paper_id"], p["result"], p["wall_seconds"], "model_s", p["model_seconds"], "p1 calls", p["pass1_calls"], "accepted", p["pass1_accepted_attempt"], "max pec", p["max_prompt_eval_count"], "p1 content/thinking", p["pass1_content_chars_accepted"], p["pass1_thinking_chars_accepted"], "p2 chars", p["pass2_content_chars"], p["pass1_done_reason"], p["pass2_done_reason"])
