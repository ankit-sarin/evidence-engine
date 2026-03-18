"""q8_0 KV cache validation — fast version, no snippet retries."""

import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.agents.extractor import (
    build_extraction_prompt,
    extract_pass1_reasoning,
    extract_pass2_structured,
)
from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec, load_review_spec

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

REVIEW = "surgical_autonomy"
SPEC_PATH = "review_specs/surgical_autonomy_v1.yaml"
# Only papers that didn't complete in first run
PAPER_IDS = [int(x) for x in sys.argv[1:]] if len(sys.argv) > 1 else [370, 432]


def load_original(db, paper_id):
    row = db._conn.execute(
        "SELECT extracted_data FROM extractions WHERE paper_id = ?",
        (paper_id,),
    ).fetchone()
    return json.loads(row[0]) if row else []


def extract_single_no_snippetfix(paper_id, paper_text, spec):
    """Two-pass extraction, skip snippet validation."""
    prompt = build_extraction_prompt(paper_text, spec)

    t0 = time.time()
    reasoning_trace = extract_pass1_reasoning(prompt)
    t1 = time.time()
    logger.info("Paper %d — Pass 1: %.1fs", paper_id, t1 - t0)

    result = extract_pass2_structured(prompt, reasoning_trace, spec, paper_id)
    t2 = time.time()
    logger.info("Paper %d — Pass 2: %.1fs, Total: %.1fs", paper_id, t2 - t1, t2 - t0)

    return [span.model_dump() for span in result.fields]


def compare_fields(original, reextracted):
    orig_map = {f["field_name"]: f for f in original}
    new_map = {f["field_name"]: f for f in reextracted}
    all_fields = sorted(set(list(orig_map.keys()) + list(new_map.keys())))
    comparisons = []
    for field in all_fields:
        orig = orig_map.get(field)
        new = new_map.get(field)
        if orig is None:
            comparisons.append({"field_name": field, "verdict": "NEW_IN_Q8", "f16_value": None, "q8_value": new["value"]})
            continue
        if new is None:
            comparisons.append({"field_name": field, "verdict": "MISSING_IN_Q8", "f16_value": orig["value"], "q8_value": None})
            continue
        ov = str(orig["value"]).strip()
        nv = str(new["value"]).strip()
        if ov == nv:
            verdict = "EXACT_MATCH"
        elif ov.lower() == nv.lower():
            verdict = "CASE_DIFF_ONLY"
        elif " ".join(ov.split()).rstrip(".") == " ".join(nv.split()).rstrip("."):
            verdict = "WHITESPACE_DIFF"
        else:
            verdict = "DISAGREEMENT"
        entry = {"field_name": field, "verdict": verdict, "f16_value": orig["value"], "q8_value": new["value"]}
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
    output_path = review_dir / "q8_validation_results.json"

    # Load any existing partial results
    if output_path.exists():
        results = json.loads(output_path.read_text())
    else:
        results = {}

    for pid in PAPER_IDS:
        logger.info("=" * 60)
        logger.info("Starting paper %d", pid)
        original = load_original(db, pid)
        if not original:
            logger.warning("Paper %d: no original — skipping", pid)
            continue
        md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)
        if not md_files:
            logger.warning("Paper %d: no parsed text — skipping", pid)
            continue

        paper_text = md_files[0].read_text()
        t0 = time.time()
        reextracted = extract_single_no_snippetfix(pid, paper_text, spec)
        elapsed = time.time() - t0

        comparisons = compare_fields(original, reextracted)
        exact = sum(1 for c in comparisons if c["verdict"] == "EXACT_MATCH")
        trivial = sum(1 for c in comparisons if c["verdict"] in ("CASE_DIFF_ONLY", "WHITESPACE_DIFF"))
        disagree = sum(1 for c in comparisons if c["verdict"] == "DISAGREEMENT")

        results[str(pid)] = {
            "paper_id": pid,
            "elapsed_seconds": round(elapsed, 1),
            "n_original_fields": len(original),
            "n_q8_fields": len(reextracted),
            "exact_matches": exact,
            "trivial_diffs": trivial,
            "disagreements": disagree,
            "comparisons": comparisons,
        }
        # Save after each paper
        output_path.write_text(json.dumps(results, indent=2, default=str))
        logger.info("Paper %d: %d exact, %d trivial, %d disagree in %.1fs (saved)", pid, exact, trivial, disagree, elapsed)

    # Summary
    print("\n" + "=" * 70)
    print("q8_0 VALIDATION SUMMARY")
    print("=" * 70)
    print(f"{'Paper':>6} {'Fields':>7} {'Exact':>6} {'Trivial':>8} {'Disagree':>9} {'Time':>8}")
    print("-" * 70)
    te = tt = td = tf = 0
    for r in results.values():
        n = r["n_original_fields"]
        print(f"{r['paper_id']:>6} {n:>7} {r['exact_matches']:>6} {r['trivial_diffs']:>8} {r['disagreements']:>9} {r['elapsed_seconds']:>7.1f}s")
        te += r["exact_matches"]; tt += r["trivial_diffs"]; td += r["disagreements"]; tf += n
    print("-" * 70)
    print(f"{'TOTAL':>6} {tf:>7} {te:>6} {tt:>8} {td:>9}")
    if tf: print(f"\nAgreement: {(te+tt)/tf*100:.1f}%  |  Exact: {te/tf*100:.1f}%")


if __name__ == "__main__":
    main()
