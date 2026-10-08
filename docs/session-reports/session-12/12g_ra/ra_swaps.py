"""ra_swaps.py — R556: model loads in the Ollama journal during the run that the rehearsal
did not cause, papers whose wall time spans one, and the Run 7 timing figure twice (all
papers; clean papers only). Reads ollama_journal_loads.txt (saved beside this script from
`journalctl -u ollama` for the run window: the "starting runner", "loading model", "loaded
runners", "runner started", general.name and any unload/evict/expire lines) and
ra_tokens.json. The journal logs loads; it has no unload line — under MAX_LOADED_MODELS=1
a load of another model is the eviction. Writes ra_swaps.json."""
import csv
import json
import os
import re
import statistics
from datetime import datetime, timedelta

from ra_common import HERE, LABEL, RUN_ID, db, write

BLOBS = {"6150cb382311": "deepseek-r1:32b", "e796792eba26": "gemma3:27b", "a3de86cd1c13": "qwen3:8b"}  # model-layer digests, from the Ollama manifests
conn = db()
man = conn.execute("SELECT started_at, ended_at FROM run_manifests WHERE run_id = ?", (RUN_ID,)).fetchone()
stage_models = {r["model_name"] for r in conn.execute("SELECT model_name FROM run_stage_configs WHERE run_id = ?", (RUN_ID,))}
calls = [dict(r) for r in conn.execute("SELECT stage, paper_id, started_at, ended_at FROM run_calls WHERE run_id = ? ORDER BY call_id", (RUN_ID,))]
ts = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))
models_by_stage = {r["stage"]: r["model_name"] for r in conn.execute("SELECT stage, model_name FROM run_stage_configs WHERE run_id = ?", (RUN_ID,))}

lines = open(os.path.join(HERE, "ollama_journal_loads.txt")).read().splitlines()
loads, other = [], []
for ln in lines:
    m = re.search(r'time=(\S+) .*msg="starting runner" cmd=".*--model \S*blobs/sha256-([0-9a-f]{12})', ln)
    if m:
        loads.append({"time": m.group(1), "blob": m.group(2), "model": BLOBS.get(m.group(2), "UNKNOWN")})
    elif re.search(r"unload|evict|expire", ln, re.I):
        other.append(ln)
for ld in loads:
    t = ts(ld["time"])
    # caused by the run: the model is one of the run's, and a run call of that model is in flight within 5 s
    cause = [c for c in calls if models_by_stage.get(c["stage"]) == ld["model"]
             and ts(c["started_at"]) - timedelta(seconds=5) <= t <= ts(c["ended_at"])]
    ld["in_run_stages"] = ld["model"] in stage_models
    ld["run_call_in_flight"] = cause[0]["stage"] if cause else None
    ld["foreign"] = not (ld["in_run_stages"] and cause)
foreign = [ld for ld in loads if ld["foreign"]]

tok = json.load(open(os.path.join(HERE, "ra_tokens.json")))
papers = []
for p in tok["papers"]:
    if p["result"] != "EXTRACTED":
        continue
    a, b = ts(p["extract_first_call_start"]), ts(p["extract_last_call_end"])
    span = [ld for ld in foreign if a <= ts(ld["time"]) <= b]
    papers.append({"paper_id": p["paper_id"], "live_paper_id": p["live_paper_id"], "wall_seconds": p["wall_seconds"],
                   "pass1_calls": p["pass1_calls"], "swap_status": "foreign swap" if span else "clean"})
clean = [p for p in papers if p["swap_status"] == "clean"]

# the corpus the figure is scaled to: 190 eligible papers, of which live 415 is refused before send
cand = list(csv.DictReader(open(os.path.join(HERE, "ra_candidates.csv"))))
n_fit = sum(not int(r["precall_refusal_expected"]) for r in cand)
text_chars = {int(r["paper_id"]): int(r["text_chars"]) for r in cand}


def estimate(ps):
    if not ps:
        return None
    mean = statistics.mean(p["wall_seconds"] for p in ps)
    # least squares of wall seconds on text characters, over the sample; applied to each fitting corpus paper
    xs = [text_chars[p["live_paper_id"]] for p in ps]
    ys = [p["wall_seconds"] for p in ps]
    mx, my = statistics.mean(xs), statistics.mean(ys)
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
    a = my - b * mx
    fitted = sum(a + b * int(r["text_chars"]) for r in cand if not int(r["precall_refusal_expected"]))
    return {"papers": len(ps), "mean_wall_seconds": round(mean, 1),
            "hours_at_sample_mean_x_fitting_papers": round(mean * n_fit / 3600, 1),
            "length_model": {"intercept_s": round(a, 1), "seconds_per_1000_text_chars": round(b * 1000, 3),
                             "hours_over_fitting_papers": round(fitted / 3600, 1)},
            "min_wall_seconds": min(ys), "max_wall_seconds": max(ys)}


summary = {
    "label": LABEL, "window": {"from": man["started_at"], "to": man["ended_at"]},
    "run_stage_models": sorted(stage_models), "loads_in_window": loads, "foreign_loads": foreign,
    "unload_or_evict_lines_in_journal": other,
    "papers": papers, "foreign_swap_papers": [p["live_paper_id"] for p in papers if p["swap_status"] != "clean"],
    "corpus_papers_that_fit": n_fit,
    "run7_extraction_estimate_all_papers": estimate(papers),
    "run7_extraction_estimate_clean_papers_only": estimate(clean),
    "caveat": "Sample chosen for length and text pathologies; 10 of 11 papers ran a second Pass-1 attempt. "
              "The mean-based figure overstates a corpus whose median text is shorter than the sample's.",
    "sample_median_text_chars": statistics.median(text_chars[p["live_paper_id"]] for p in papers),
    "corpus_median_text_chars": statistics.median(int(r["text_chars"]) for r in cand),
}
write("ra_swaps.json", summary)
print(json.dumps({k: v for k, v in summary.items() if k != "papers"}, indent=1, sort_keys=True))
