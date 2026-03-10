#!/usr/bin/env python3
"""Evaluate candidate auditor models on 5 papers from the current corpus.

Compares Qwen3:32b (current), Llama4:scout, and Gemma3:27b on the same
evidence spans, recording per-field audit state and agreement rates.
"""

import json
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.agents.auditor import audit_span, DEFAULT_AUDITOR_MODEL
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("eval_auditor")

# ── Config ──────────────────────────────────────────────────────────

REVIEW_NAME = "surgical_autonomy"
SPEC_PATH = "review_specs/surgical_autonomy_v1.yaml"

# Papers chosen for diverse audit profiles:
# pid 17: 11v/1c/3f (mostly verified)
# pid  5: 10v/2c/3f (mixed, with contested)
# pid 11:  5v/6c/4f (heavy contested)
# pid 18:  6v/1c/8f (heavy flagged)
# pid 28:  3v/7c/5f (heavy contested+flagged)
PAPER_IDS = [17, 5, 11, 18, 28]

MODELS = ["qwen3:32b", "llama4:scout", "gemma3:27b"]

# Per-model Ollama options (e.g. num_ctx to avoid OOM on large models)
MODEL_OPTIONS: dict[str, dict] = {
    "llama4:scout": {"num_ctx": 4096},
}


def load_spans_and_text(db, paper_id, spec, review_dir):
    """Load all evidence spans and parsed text for a paper."""
    # Get latest extraction
    extraction = db._conn.execute(
        "SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
        (paper_id,),
    ).fetchone()
    if not extraction:
        return None, None, None

    ext_id = extraction["id"]

    # Get all spans (regardless of current audit state)
    spans = db._conn.execute(
        "SELECT * FROM evidence_spans WHERE extraction_id = ?",
        (ext_id,),
    ).fetchall()
    spans = [dict(s) for s in spans]

    # Load parsed text
    parsed_dir = review_dir / "parsed_text"
    md_files = sorted(parsed_dir.glob(f"{paper_id}_v*.md"), reverse=True)
    if not md_files:
        return spans, None, ext_id

    paper_text = md_files[0].read_text()
    return spans, paper_text, ext_id


def run_eval():
    spec = load_review_spec(SPEC_PATH)
    db = ReviewDatabase(REVIEW_NAME)
    review_dir = Path(db.db_path).parent

    # Build field metadata from spec
    field_type_map = {f.name: f.type for f in spec.extraction_schema.fields}
    field_tier_map = {f.name: f.tier for f in spec.extraction_schema.fields}

    # Collect existing Qwen3:32b results from DB
    existing_results: dict[int, dict[str, str]] = {}  # paper_id -> {field_name: status}

    # Results: {model: {paper_id: {field_name: {"status": ..., "reasoning": ...}}}}
    results: dict[str, dict] = {m: {} for m in MODELS}
    errors: dict[str, list] = {m: [] for m in MODELS}
    timings: dict[str, float] = {m: 0.0 for m in MODELS}

    for pid in PAPER_IDS:
        spans, paper_text, ext_id = load_spans_and_text(db, pid, spec, review_dir)
        if not spans or not paper_text:
            logger.warning("Paper %d: missing data, skipping", pid)
            continue

        title = db._conn.execute(
            "SELECT title FROM papers WHERE id = ?", (pid,)
        ).fetchone()["title"]
        logger.info("Paper %d: %s (%d spans)", pid, title[:60], len(spans))

        # Record existing audit state from DB
        existing_results[pid] = {}
        for s in spans:
            existing_results[pid][s["field_name"]] = s["audit_status"]

        # Store existing qwen3:32b results directly from DB
        results["qwen3:32b"][pid] = {}
        for s in spans:
            results["qwen3:32b"][pid][s["field_name"]] = {
                "status": s["audit_status"],
                "reasoning": s.get("audit_rationale", ""),
            }

        # Run new models
        for model_name in MODELS:
            if model_name == "qwen3:32b":
                continue  # Already have from DB

            results[model_name][pid] = {}
            t0 = time.time()

            for s in spans:
                fname = s["field_name"]
                ft = field_type_map.get(fname, "text")
                tier = field_tier_map.get(fname, 1)

                try:
                    opts = MODEL_OPTIONS.get(model_name)
                    status, reasoning = audit_span(
                        s, paper_text, field_type=ft, field_tier=tier,
                        model=model_name, ollama_options=opts,
                    )
                    results[model_name][pid][fname] = {
                        "status": status,
                        "reasoning": reasoning,
                    }
                except Exception as exc:
                    logger.error(
                        "Paper %d, field %s, model %s: ERROR %s",
                        pid, fname, model_name, exc,
                    )
                    errors[model_name].append({
                        "paper_id": pid,
                        "field_name": fname,
                        "error": str(exc),
                    })
                    results[model_name][pid][fname] = {
                        "status": "ERROR",
                        "reasoning": str(exc),
                    }

            elapsed = time.time() - t0
            timings[model_name] += elapsed
            logger.info(
                "  %s: %d spans in %.1fs (%.1fs/span)",
                model_name, len(spans), elapsed, elapsed / len(spans) if spans else 0,
            )

    db.close()

    # ── Analysis ────────────────────────────────────────────────────
    print("\n" + "=" * 100)
    print("AUDITOR MODEL EVALUATION — 5 PAPERS × 3 MODELS")
    print("=" * 100)

    # Comparison table
    print(f"\n{'Paper':>6} | {'Field':<30} | {'Qwen3:32b':<14} | {'Llama4:scout':<14} | {'Gemma3:27b':<14} | {'Disagree?'}")
    print("-" * 100)

    disagreements = []
    total_spans = 0
    agree_counts = {"llama4_qwen3": 0, "gemma3_qwen3": 0, "llama4_gemma3": 0}

    for pid in PAPER_IDS:
        if pid not in results["qwen3:32b"]:
            continue

        fields = sorted(results["qwen3:32b"][pid].keys())
        for fname in fields:
            total_spans += 1
            q = results["qwen3:32b"][pid].get(fname, {}).get("status", "?")
            l = results["llama4:scout"][pid].get(fname, {}).get("status", "?") if pid in results["llama4:scout"] else "?"
            g = results["gemma3:27b"][pid].get(fname, {}).get("status", "?") if pid in results["gemma3:27b"] else "?"

            disagree = not (q == l == g)

            if q == l:
                agree_counts["llama4_qwen3"] += 1
            if q == g:
                agree_counts["gemma3_qwen3"] += 1
            if l == g:
                agree_counts["llama4_gemma3"] += 1

            marker = "  ***" if disagree else ""
            print(f"{pid:>6} | {fname:<30} | {q:<14} | {l:<14} | {g:<14} |{marker}")

            if disagree:
                disagreements.append({
                    "paper_id": pid,
                    "field": fname,
                    "qwen3": q,
                    "llama4": l,
                    "gemma3": g,
                })

        print("-" * 100)

    # Agreement rates
    print(f"\n{'AGREEMENT RATES':=^60}")
    if total_spans > 0:
        print(f"  Llama4:scout vs Qwen3:32b:  {agree_counts['llama4_qwen3']:>3}/{total_spans} = {agree_counts['llama4_qwen3']/total_spans*100:.1f}%")
        print(f"  Gemma3:27b   vs Qwen3:32b:  {agree_counts['gemma3_qwen3']:>3}/{total_spans} = {agree_counts['gemma3_qwen3']/total_spans*100:.1f}%")
        print(f"  Llama4:scout vs Gemma3:27b:  {agree_counts['llama4_gemma3']:>3}/{total_spans} = {agree_counts['llama4_gemma3']/total_spans*100:.1f}%")

    print(f"\n  Total spans evaluated: {total_spans}")
    print(f"  Total disagreements:   {len(disagreements)}")

    # Per-model stats
    print(f"\n{'PER-MODEL AUDIT STATE DISTRIBUTION':=^60}")
    for model_name in MODELS:
        counts = {"verified": 0, "contested": 0, "flagged": 0, "invalid_snippet": 0, "ERROR": 0}
        for pid in PAPER_IDS:
            if pid not in results[model_name]:
                continue
            for fname, data in results[model_name][pid].items():
                s = data["status"]
                counts[s] = counts.get(s, 0) + 1
        print(f"\n  {model_name}:")
        for state, cnt in sorted(counts.items()):
            if cnt > 0:
                print(f"    {state}: {cnt}")

    # Errors
    print(f"\n{'ERRORS':=^60}")
    for model_name in MODELS:
        if errors[model_name]:
            print(f"\n  {model_name}: {len(errors[model_name])} errors")
            for e in errors[model_name][:5]:
                print(f"    Paper {e['paper_id']}, {e['field_name']}: {e['error'][:80]}")
        else:
            print(f"  {model_name}: 0 errors")

    # Timings
    print(f"\n{'TIMING':=^60}")
    for model_name in MODELS:
        if model_name == "qwen3:32b":
            print(f"  {model_name}: (from DB — no timing)")
        else:
            span_count = sum(
                len(results[model_name].get(pid, {}))
                for pid in PAPER_IDS
            )
            per_span = timings[model_name] / span_count if span_count else 0
            print(f"  {model_name}: {timings[model_name]:.1f}s total, {per_span:.1f}s/span")

    # Save raw results
    output_path = Path("data/surgical_autonomy/auditor_eval_results.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump({
            "models": MODELS,
            "paper_ids": PAPER_IDS,
            "results": results,
            "errors": errors,
            "agreement_counts": agree_counts,
            "total_spans": total_spans,
            "disagreements": disagreements,
        }, f, indent=2)
    print(f"\nRaw results saved to {output_path}")


if __name__ == "__main__":
    run_eval()
