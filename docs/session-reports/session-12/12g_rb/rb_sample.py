"""12g-RB-A — the front-half rehearsal's sample, the stage set a screen-start run declares,
and its arm pin. Read-only: live review.db mode=ro; no model call, no fetch (the digest
fetch is patched to raise; digests are the three measured at 12g-OPEN plus placeholders
that the pin does not hash). Run from the repository root:
    .venv/bin/python docs/session-reports/session-12/12g_rb/rb_sample.py
Writes rb_sample.csv and rb_summary.json beside this file."""
import csv
import importlib.util
import json
import os
import sqlite3
import statistics
import sys
from datetime import datetime

sys.path.insert(0, os.getcwd())
import engine.utils.ollama_client as oc


def _boom(*a, **k):
    raise AssertionError("fetch_model_digest fired")


oc.fetch_model_digest = _boom
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook_beside
from engine.core.effective_config import sha256_canonical, stage_config
from engine.core.review_paths import load_spec_for

HERE = os.path.dirname(os.path.abspath(__file__))
DB = "data/surgical_autonomy/review.db"
conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row

# ---- what a --skip-to screen run declares (run_pipeline._open_run_manifest, read as code)
spec_m = importlib.util.spec_from_file_location("run_pipeline", "scripts/run_pipeline.py")
rp = importlib.util.module_from_spec(spec_m)
spec_m.loader.exec_module(rp)
spec = load_spec_for("surgical_autonomy", "review_specs/surgical_autonomy.yaml")
throw = spec.model_copy(update={"review_id": "rb_12g"})
start = rp.STAGES.index("screen")
stages = []
for name in rp.STAGES[start:]:
    stages.extend(rp._PIPELINE_STAGE_CONFIGS.get(name, ()))
preflight = sorted({stage_config("abstract_screen_primary", spec).model, stage_config("extract_pass1", spec).model,
                    stage_config("audit", spec).model})
models = {s: stage_config(s, spec).model for s in stages}
DIG = {"deepseek-r1:32b": "edba8017331d15236e57480eb45406c0d721db77a4cdcf234df500fc2ad3960c",
       "gemma3:27b": "a418f5838eaf7fe2cfe0a3046c8384b68ba43a4435542c942f9db00a5f342203"}
cb = load_codebook_beside(DB)


def pin(sp):
    arms = {s: rm._stage_arm(sp, s) for s in stages + ["preflight"] if rm._stage_arm(sp, s)}
    res = rm.resolve_run(sp, stages + ["preflight"], digest_fn=lambda m: DIG.get(m, "0" * 64),
                         preflight_models=preflight, arms_by_stage=arms,
                         codebook_path="data/surgical_autonomy/extraction_codebook.yaml")
    tup = rm.pin_tuple(sp, "local_deepseek_r1_32b", res, cb.semantic_hash, rm.library_versions())
    return sha256_canonical(tup), sorted(res), sorted(k for k, r in res.items() if r.arm_name == "local_deepseek_r1_32b")


pin_live, keys, arm_stages = pin(spec)
pin_throw, _, _ = pin(throw)

# ---- March timing: gaps between consecutive screening decisions (rows written live, not backfilled)
ts = [datetime.fromisoformat(r[0]) for r in conn.execute(
    "SELECT decided_at FROM abstract_screening_decisions ORDER BY decided_at, id")]
gaps = [(b - a).total_seconds() for a, b in zip(ts, ts[1:])]
gaps = [g for g in gaps if 0 < g < 120]

# ---- the sample
papers = {r["id"]: r for r in conn.execute("SELECT id, pmid, doi, title, abstract, authors, journal, year, status FROM papers")}
dec = {}
for r in conn.execute("SELECT paper_id, pass_number, decision, decided_at, id FROM abstract_screening_decisions ORDER BY id"):
    dec.setdefault(r["paper_id"], {})[r["pass_number"]] = r["decision"]      # the latest row of each pass wins
ver = {r["paper_id"]: r["decision"] for r in conn.execute("SELECT paper_id, decision FROM abstract_verification_decisions ORDER BY id")}
pi = {r[0] for r in conn.execute("SELECT id FROM papers WHERE rejected_reason IS NOT NULL")}


def alen(p):
    return len(p["abstract"] or "")


def pick(ids, k):
    """k ids evenly spaced through the ascending id list."""
    ids = sorted(ids)
    if len(ids) <= k:
        return ids
    return [ids[round(i * (len(ids) - 1) / (k - 1))] for i in range(k)] if k > 1 else [ids[len(ids) // 2]]


both = lambda a, b: [i for i, d in dec.items() if d.get(1) == a and d.get(2) == b]
normal = lambda i: 400 <= alen(papers[i]) <= 3000
strata = [
    ("included at abstract, eligible today", [i for i in both("include", "include") if papers[i]["status"] == "AI_AUDIT_COMPLETE" and normal(i)], 10),
    ("included at abstract, excluded at full text", [i for i in both("include", "include") if papers[i]["status"] == "FT_SCREENED_OUT" and normal(i)], 6),
    ("both passes include, excluded later at abstract (verifier or PI)", [i for i in both("include", "include") if papers[i]["status"] == "ABSTRACT_SCREENED_OUT" and normal(i)], 4),
    ("both passes exclude", [i for i in both("exclude", "exclude") if normal(i)], 12),
    ("passes disagreed", [i for i in both("include", "exclude") + both("exclude", "include") if normal(i)], 4),
    ("no abstract (E5)", [i for i in papers if alen(papers[i]) == 0], 2),
    ("abstract under 200 characters (E5)", [i for i in papers if 0 < alen(papers[i]) < 200], 2),
]
chosen, why = [], {}
for name, ids, k in strata:
    for i in pick(ids, k):
        if i not in why:
            chosen.append(i)
            why[i] = name
longest = sorted(papers, key=lambda i: -alen(papers[i]))
for i in longest[:2]:
    if i not in why:
        chosen.append(i)
        why[i] = "longest abstracts"
chosen.sort()
rows = []
for n, i in enumerate(chosen, 1):
    p = papers[i]
    rows.append({"throwaway_paper_id": n, "live_paper_id": i, "stratum": why[i], "abstract_chars": alen(p),
                 "title_chars": len(p["title"] or ""), "has_pmid": int(bool(p["pmid"])), "has_doi": int(bool(p["doi"])),
                 "march_pass1": dec.get(i, {}).get(1, ""), "march_pass2": dec.get(i, {}).get(2, ""),
                 "march_verifier": ver.get(i, ""), "pi_reason_recorded": int(i in pi), "live_status_today": p["status"]})
with open(os.path.join(HERE, "rb_sample.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)
summary = {
    "declared_stages_for_skip_to_screen": stages + ["preflight"], "stage_models": models, "preflight_models": preflight,
    "run_stage_configs_rows_expected": keys, "stages_of_the_arm": arm_stages,
    "run_kind": "extraction" if any(s.startswith(("extract", "elicitation", "audit")) for s in stages) else "screening",
    "pin_live_spec": pin_live, "pin_throwaway_review_id_rb_12g": pin_throw, "pins_equal": pin_live == pin_throw,
    "screening_primary_model": spec.screening_models.primary,
    "abstract_screen_primary_config": {"options": dict(stage_config("abstract_screen_primary", spec).options or {}),
                                       "sent_keys": sorted(stage_config("abstract_screen_primary", spec).sent_keys)},
    "march_seconds_between_decisions": {"n": len(gaps), "median": round(statistics.median(gaps), 2),
                                        "p90": round(sorted(gaps)[int(len(gaps) * .9)], 2), "max": round(max(gaps), 2)},
    "strata_population": {name: len(ids) for name, ids, _ in strata}, "n": len(rows),
    "by_stratum": {name: sum(r["stratum"] == name for r in rows) for name in dict.fromkeys(r["stratum"] for r in rows)},
    "live_abstract_chars": {"empty": sum(alen(p) == 0 for p in papers.values()), "under_200": sum(0 < alen(p) < 200 for p in papers.values()),
                            "median": statistics.median(alen(p) for p in papers.values()), "max": max(alen(p) for p in papers.values())},
    "papers_with_decisions": len(dec),
}
json.dump(summary, open(os.path.join(HERE, "rb_summary.json"), "w"), indent=1, sort_keys=True)
print(json.dumps(summary, indent=1, sort_keys=True))
for r in rows:
    print(r["throwaway_paper_id"], r["live_paper_id"], r["abstract_chars"], r["march_pass1"], r["march_pass2"], r["march_verifier"], r["live_status_today"], "|", r["stratum"])
