"""12g-RA-A — sample candidates and the throwaway pin, read-only.

Live review.db opened mode=ro; parsed texts read through the engine's resolver; the unit
map and the Pass-1 prompt are BUILT (pure functions), never sent. No model call, no fetch
(the digest fetch is patched to raise). Run from the repository root:

    .venv/bin/python <this file> <out_dir>

Writes <out_dir>/ra_candidates.csv (one row per eligible paper) and <out_dir>/ra_summary.json.
"""
import csv
import hashlib
import json
import os
import re
import sqlite3
import statistics
import sys
import time

sys.path.insert(0, os.getcwd())
import engine.utils.ollama_client as oc


def _boom(*a, **k):
    raise AssertionError("fetch_model_digest fired")


oc.fetch_model_digest = _boom
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook_beside
from engine.core.completeness import expected_field_names
from engine.core.effective import eligible_paper_ids
from engine.core.effective_config import sha256_canonical
from engine.core.parsed_text import read_parsed_text, resolve_parsed_text
from engine.core.review_paths import load_spec_for
from engine.elicitation.pipeline import pass1_messages, pass1_prompt
from engine.elicitation.units import build_unit_map
from engine.utils.ollama_client import RATIO_MIN, message_chars

OUT = sys.argv[1]
DB = "data/surgical_autonomy/review.db"
SPEC = "review_specs/surgical_autonomy.yaml"
CEILING = 131072  # deepseek-r1:32b, read from the 11c smoke log's input_fit lines (no /api/show call here)
RATIO_OBS = 0.2419  # highest deepseek ratio in that log (three calls: 0.2354, 0.236, 0.2419)

conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
spec0 = load_spec_for("surgical_autonomy", SPEC)
cb = load_codebook_beside(DB)
field_names = expected_field_names(spec0, "data/surgical_autonomy/extraction_codebook.yaml")

# ---- I3: the pin of a spec that differs in review_id and the elicitation flag only
DIGESTS = {"deepseek-r1:32b": "edba8017331d15236e57480eb45406c0d721db77a4cdcf234df500fc2ad3960c",
           "gemma3:27b": "a418f5838eaf7fe2cfe0a3046c8384b68ba43a4435542c942f9db00a5f342203"}


def pin(spec, codebook_path=None):
    stages = ["elicitation_pass1", "extract_pass2", "extract_retry_snippet", "audit", "preflight"]
    arms = {s: rm._stage_arm(spec, s) for s in stages if rm._stage_arm(spec, s)}
    res = rm.resolve_run(spec, stages, digest_fn=lambda m: DIGESTS[m],
                         preflight_models=["deepseek-r1:32b", "gemma3:27b"], arms_by_stage=arms,
                         **({"codebook_path": codebook_path} if codebook_path else {}))
    tup = rm.pin_tuple(spec, "local_deepseek_r1_32b", res, cb.semantic_hash, rm.library_versions())
    rows = {k: {"model": r.config.model, "prompt_hash": r.prompt_hash, "options_hash": r.options_hash,
                "format_schema_hash": r.format_schema_hash, "options": dict(r.config.options or {}),
                "sent_keys": sorted(r.config.sent_keys)} for k, r in sorted(res.items())}
    return sha256_canonical(tup), rows


em = spec0.extraction_models.model_copy(update={"elicitation": True})
live_elicited = spec0.model_copy(update={"extraction_models": em})
throwaway = live_elicited.model_copy(update={"review_id": "ra_12g"})
pin_live, rows_live = pin(live_elicited)
# the throwaway's codebook is a byte copy of live's, so live's file stands in for it here
pin_throw, rows_throw = pin(throwaway, "data/surgical_autonomy/extraction_codebook.yaml")

# ---- candidates: every eligible paper's current parsed text
multi = {r[0]: r[1] for r in conn.execute(
    "SELECT paper_id, group_concat(parsed_text_version) FROM parsed_text_refs GROUP BY paper_id HAVING count(*) > 1")}
LIG = re.compile("[ﬀ-ﬆ]")
EOL_HYPHEN = re.compile(r"[A-Za-z]-\n[a-z]")
rows = []
t0 = time.time()
for pid in sorted(eligible_paper_ids(conn)):
    ref = resolve_parsed_text(conn, pid)
    text = read_parsed_text(ref)
    t = time.time()
    um = build_unit_map(pid, text)
    build_s = time.time() - t
    prompt = pass1_prompt(um, cb.raw, field_names, "")
    chars = message_chars(pass1_messages(prompt))
    title = conn.execute("SELECT title, pmid, doi FROM papers WHERE id = ?", (pid,)).fetchone()
    rows.append({
        "paper_id": pid, "parsed_text_version": ref.version, "parsed_text_sha256": ref.sha256,
        "text_chars": len(text), "n_units": len(um.units), "pass1_request_chars": chars,
        "pass1_chars_over_text": round(chars / len(text), 4) if text else "",
        "est_tokens_low_x0.19": round(chars * RATIO_MIN),
        "est_tokens_x0.2419": round(chars * RATIO_OBS),
        "share_of_ceiling_x0.2419": round(chars * RATIO_OBS / CEILING, 4),
        "precall_refusal_expected": int(chars * RATIO_MIN >= CEILING),
        "postcall_truncation_expected_x0.2419": int(chars * RATIO_OBS >= CEILING),
        "docling_comments": text.count("<!--"), "eol_hyphens": len(EOL_HYPHEN.findall(text)),
        "soft_hyphens": text.count("­"), "ligatures": len(LIG.findall(text)),
        "multi_version": multi.get(pid, ""), "unit_map_build_s": round(build_s, 2),
        "has_pmid": int(bool(title["pmid"])), "has_doi": int(bool(title["doi"])),
    })
cols = list(rows[0].keys())
with open(os.path.join(OUT, "ra_candidates.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    w.writerows(rows)


def dist(key):
    v = [r[key] for r in rows]
    q = statistics.quantiles(v, n=10)
    return {"min": min(v), "p10": q[0], "median": statistics.median(v), "p90": q[8], "max": max(v)}


summary = {
    "pin_live_spec_elicited": pin_live, "pin_throwaway_review_id_ra_12g_elicited": pin_throw,
    "pins_equal": pin_live == pin_throw, "stage_rows_equal": rows_live == rows_throw,
    "stage_rows": rows_throw, "ceiling_assumed": CEILING, "ratio_min": RATIO_MIN, "ratio_observed": RATIO_OBS,
    "eligible": len(rows), "fields_expected": len(field_names),
    "text_chars": dist("text_chars"), "pass1_request_chars": dist("pass1_request_chars"),
    "pass1_chars_over_text": dist("pass1_chars_over_text"), "n_units": dist("n_units"),
    "precall_refusal_expected_ids": [r["paper_id"] for r in rows if r["precall_refusal_expected"]],
    "postcall_truncation_expected_ids": [r["paper_id"] for r in rows if r["postcall_truncation_expected_x0.2419"] and not r["precall_refusal_expected"]],
    "over_half_ceiling_ids": [r["paper_id"] for r in rows if r["share_of_ceiling_x0.2419"] >= 0.5 and not r["postcall_truncation_expected_x0.2419"]],
    "with_docling_comments": sum(r["docling_comments"] > 0 for r in rows),
    "with_eol_hyphens": sum(r["eol_hyphens"] > 0 for r in rows),
    "with_soft_hyphens": sum(r["soft_hyphens"] > 0 for r in rows),
    "with_ligatures": sum(r["ligatures"] > 0 for r in rows),
    "multi_version_ids": sorted(multi), "no_pmid_and_no_doi": [r["paper_id"] for r in rows if not r["has_pmid"] and not r["has_doi"]],
    "unit_map_build_total_s": round(time.time() - t0, 1),
}
json.dump(summary, open(os.path.join(OUT, "ra_summary.json"), "w"), indent=1, sort_keys=True)
print(json.dumps({k: v for k, v in summary.items() if k != "stage_rows"}, indent=1, sort_keys=True))
print(json.dumps(summary["stage_rows"], sort_keys=True)[:2500])
