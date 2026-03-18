"""q8_0 KV cache validation: re-extract 5 papers and compare against f16 originals."""

import json
import logging
import sys
import time
from pathlib import Path

# Project imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.agents.extractor import (
    build_extraction_prompt,
    extract_pass1_reasoning,
    extract_pass2_structured,
    _validate_and_retry_snippets,
)
from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec, load_review_spec

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────

REVIEW = "surgical_autonomy"
SPEC_PATH = "review_specs/surgical_autonomy_v1.yaml"
# ReviewDatabase takes review_name, not a path
PAPER_IDS = [9, 121, 383, 370, 432]
OUTPUT_PATH = None  # set in main() after db init


def load_original(db: ReviewDatabase, paper_id: int) -> list[dict]:
    """Load the original f16 extraction for a paper."""
    row = db._conn.execute(
        "SELECT extracted_data FROM extractions WHERE paper_id = ?",
        (paper_id,),
    ).fetchone()
    if not row:
        return []
    return json.loads(row[0])


def extract_single(paper_id: int, paper_text: str, spec: ReviewSpec) -> list[dict]:
    """Run two-pass extraction on a paper, return field dicts (no DB write)."""
    prompt = build_extraction_prompt(paper_text, spec)

    t0 = time.time()
    reasoning_trace = extract_pass1_reasoning(prompt)
    t1 = time.time()
    logger.info("Paper %d — Pass 1 reasoning: %.1fs", paper_id, t1 - t0)

    result = extract_pass2_structured(prompt, reasoning_trace, spec, paper_id)
    t2 = time.time()
    logger.info("Paper %d — Pass 2 structured: %.1fs", paper_id, t2 - t1)

    validated = _validate_and_retry_snippets(result.fields, paper_text, paper_id)
    t3 = time.time()
    logger.info("Paper %d — Snippet validation: %.1fs", paper_id, t3 - t2)
    logger.info("Paper %d — Total: %.1fs", paper_id, t3 - t0)

    return [span.model_dump() for span in validated]


def compare_fields(original: list[dict], reextracted: list[dict]) -> list[dict]:
    """Field-by-field comparison of two extraction results."""
    orig_map = {f["field_name"]: f for f in original}
    new_map = {f["field_name"]: f for f in reextracted}

    all_fields = sorted(set(list(orig_map.keys()) + list(new_map.keys())))
    comparisons = []

    for field in all_fields:
        orig = orig_map.get(field)
        new = new_map.get(field)

        if orig is None:
            comparisons.append({
                "field_name": field,
                "verdict": "NEW_IN_Q8",
                "f16_value": None,
                "q8_value": new["value"] if new else None,
            })
            continue

        if new is None:
            comparisons.append({
                "field_name": field,
                "verdict": "MISSING_IN_Q8",
                "f16_value": orig["value"],
                "q8_value": None,
            })
            continue

        ov = str(orig["value"]).strip()
        nv = str(new["value"]).strip()

        if ov == nv:
            verdict = "EXACT_MATCH"
        elif ov.lower() == nv.lower():
            verdict = "CASE_DIFF_ONLY"
        else:
            # Check for semantic equivalence heuristics
            # Normalize whitespace and punctuation for comparison
            ov_norm = " ".join(ov.split()).rstrip(".")
            nv_norm = " ".join(nv.split()).rstrip(".")
            if ov_norm == nv_norm:
                verdict = "WHITESPACE_DIFF"
            elif ov_norm.lower() == nv_norm.lower():
                verdict = "CASE_DIFF_ONLY"
            else:
                verdict = "DISAGREEMENT"

        entry = {
            "field_name": field,
            "verdict": verdict,
            "f16_value": orig["value"],
            "q8_value": new["value"],
        }
        # Only include snippets for disagreements to keep output manageable
        if verdict == "DISAGREEMENT":
            entry["f16_snippet"] = orig.get("source_snippet", "")[:200]
            entry["q8_snippet"] = new.get("source_snippet", "")[:200]

        comparisons.append(entry)

    return comparisons


def main():
    spec = load_review_spec(SPEC_PATH)
    db = ReviewDatabase(REVIEW)
    review_dir = Path(db.db_path).parent
    parsed_dir = review_dir / "parsed_text"
    global OUTPUT_PATH
    OUTPUT_PATH = review_dir / "q8_validation_results.json"

    results = {}
    grand_start = time.time()

    for pid in PAPER_IDS:
        logger.info("=" * 60)
        logger.info("Starting paper %d", pid)

        # Load original f16 extraction
        original = load_original(db, pid)
        if not original:
            logger.warning("Paper %d: no original extraction found — skipping", pid)
            continue

        # Load parsed text
        md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)
        if not md_files:
            logger.warning("Paper %d: no parsed text — skipping", pid)
            continue

        paper_text = md_files[0].read_text()

        # Re-extract with q8_0 settings
        t0 = time.time()
        reextracted = extract_single(pid, paper_text, spec)
        elapsed = time.time() - t0

        # Compare
        comparisons = compare_fields(original, reextracted)

        # Tally
        exact = sum(1 for c in comparisons if c["verdict"] == "EXACT_MATCH")
        case_diff = sum(1 for c in comparisons if c["verdict"] in ("CASE_DIFF_ONLY", "WHITESPACE_DIFF"))
        disagree = sum(1 for c in comparisons if c["verdict"] == "DISAGREEMENT")
        missing = sum(1 for c in comparisons if c["verdict"] in ("MISSING_IN_Q8", "NEW_IN_Q8"))

        results[str(pid)] = {
            "paper_id": pid,
            "elapsed_seconds": round(elapsed, 1),
            "n_original_fields": len(original),
            "n_q8_fields": len(reextracted),
            "exact_matches": exact,
            "trivial_diffs": case_diff,
            "disagreements": disagree,
            "structural_diffs": missing,
            "comparisons": comparisons,
        }

        logger.info(
            "Paper %d: %d exact, %d trivial, %d disagreements in %.1fs",
            pid, exact, case_diff, disagree, elapsed,
        )

    # Save results
    OUTPUT_PATH.write_text(json.dumps(results, indent=2, default=str))
    logger.info("Results saved to %s", OUTPUT_PATH)

    grand_elapsed = time.time() - grand_start
    logger.info("Total validation time: %.1fs (%.1f min)", grand_elapsed, grand_elapsed / 60)

    # Print summary table
    print("\n" + "=" * 70)
    print("q8_0 VALIDATION SUMMARY")
    print("=" * 70)
    print(f"{'Paper':>6} {'Fields':>7} {'Exact':>6} {'Trivial':>8} {'Disagree':>9} {'Time':>8}")
    print("-" * 70)
    total_exact = total_trivial = total_disagree = total_fields = 0
    for pid_str, r in results.items():
        total = r["n_original_fields"]
        print(
            f"{r['paper_id']:>6} {total:>7} {r['exact_matches']:>6} "
            f"{r['trivial_diffs']:>8} {r['disagreements']:>9} {r['elapsed_seconds']:>7.1f}s"
        )
        total_exact += r["exact_matches"]
        total_trivial += r["trivial_diffs"]
        total_disagree += r["disagreements"]
        total_fields += total
    print("-" * 70)
    print(
        f"{'TOTAL':>6} {total_fields:>7} {total_exact:>6} "
        f"{total_trivial:>8} {total_disagree:>9} {grand_elapsed:>7.1f}s"
    )
    if total_fields > 0:
        match_rate = (total_exact + total_trivial) / total_fields * 100
        print(f"\nAgreement rate: {match_rate:.1f}% ({total_exact + total_trivial}/{total_fields})")
        print(f"Exact match rate: {total_exact / total_fields * 100:.1f}% ({total_exact}/{total_fields})")
    else:
        print("\nNo papers were successfully compared.")

    if total_disagree > 0:
        print(f"\n{'='*70}")
        print("DISAGREEMENTS:")
        print("=" * 70)
        for pid_str, r in results.items():
            for c in r["comparisons"]:
                if c["verdict"] == "DISAGREEMENT":
                    print(f"\nPaper {r['paper_id']}, {c['field_name']}:")
                    print(f"  f16: {str(c['f16_value'])[:100]}")
                    print(f"  q8:  {str(c['q8_value'])[:100]}")


if __name__ == "__main__":
    main()
