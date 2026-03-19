#!/usr/bin/env python3
"""Re-screen original 251 papers with updated criteria (PICO + tightened rules).

Read-only against review.db — writes results to a staging CSV only.

# TODO(retention): Like screen_expanded.py, this writes to flat CSV rather than
# the database. Future screening scripts should write all traces directly to DB
# per the retention policy.
"""

import argparse
import csv
import json
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.agents.screener import screen_paper, ScreeningDecision
from engine.core.review_spec import load_review_spec

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_REVIEW = "surgical_autonomy"


def load_checkpoint(checkpoint_path: Path) -> dict[int, dict]:
    """Load already-screened results from checkpoint."""
    if checkpoint_path.exists():
        return {r["id"]: r for r in json.loads(checkpoint_path.read_text())}
    return {}


def save_checkpoint(checkpoint_path: Path, results: list[dict]) -> None:
    checkpoint_path.write_text(json.dumps(results, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Re-screen original 251 papers with updated criteria")
    parser.add_argument("--review", default=DEFAULT_REVIEW, help=f"Review name (default: {DEFAULT_REVIEW})")
    parser.add_argument("--spec", default=None, help="Path to review spec YAML (default: review_specs/<review>_v1.yaml)")
    args = parser.parse_args()

    if args.review == DEFAULT_REVIEW and "--review" not in " ".join(sys.argv):
        logging.warning("No --review specified, using default 'surgical_autonomy'.")

    review = args.review
    spec_path = args.spec or f"review_specs/{review}_v1.yaml"
    db_path = Path(f"data/{review}/review.db")
    output_dir = Path(f"data/{review}/expanded_search")
    OUTPUT_CSV = output_dir / "rescreen_original_251.csv"
    CHECKPOINT = output_dir / "rescreen_checkpoint.json"

    spec = load_review_spec(spec_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    papers = conn.execute(
        "SELECT id, title, abstract, status FROM papers ORDER BY id"
    ).fetchall()
    conn.close()

    logger.info("Loaded %d papers from review.db", len(papers))

    # Resume from checkpoint
    done = load_checkpoint(CHECKPOINT)
    if done:
        logger.info("Resuming: %d already screened", len(done))

    results = list(done.values())

    pending = [p for p in papers if p["id"] not in done]
    total = len(papers)

    for i, paper in enumerate(pending, len(done) + 1):
        pid = paper["id"]
        paper_dict = {
            "title": paper["title"],
            "abstract": paper["abstract"],
        }

        # Dual-pass screening
        d1 = screen_paper(paper_dict, spec, pass_number=1)
        d2 = screen_paper(paper_dict, spec, pass_number=2)

        if d1.decision == "include" and d2.decision == "include":
            new_decision = "include"
        elif d1.decision == "exclude" and d2.decision == "exclude":
            new_decision = "exclude"
        else:
            new_decision = "flagged"

        old_status = paper["status"]
        old_decision = (
            "include" if old_status in ("SCREENED_IN", "AI_AUDIT_COMPLETE",
                                         "EXTRACTED", "EXTRACT_FAILED",
                                         "PDF_ACQUIRED", "PARSED",
                                         "HUMAN_AUDIT_COMPLETE")
            else "exclude"
        )

        row = {
            "id": pid,
            "title": paper["title"],
            "old_status": old_status,
            "old_decision": old_decision,
            "new_decision": new_decision,
            "pass1_decision": d1.decision,
            "pass1_rationale": d1.rationale,
            "pass1_confidence": d1.confidence,
            "pass2_decision": d2.decision,
            "pass2_rationale": d2.rationale,
            "pass2_confidence": d2.confidence,
        }
        results.append(row)

        flipped = "FLIP" if old_decision != new_decision else ""
        logger.info(
            "[%d/%d] Paper %d: %s → %s %s",
            i, total, pid, old_decision, new_decision, flipped,
        )

        # Checkpoint every 10 papers
        if i % 10 == 0 or i == total:
            save_checkpoint(CHECKPOINT, results)

    # Write final CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    # Summary
    still_in = sum(1 for r in results if r["old_decision"] == "include" and r["new_decision"] == "include")
    still_out = sum(1 for r in results if r["old_decision"] == "exclude" and r["new_decision"] == "exclude")
    in_to_out = [r for r in results if r["old_decision"] == "include" and r["new_decision"] != "include"]
    out_to_in = [r for r in results if r["old_decision"] == "exclude" and r["new_decision"] != "exclude"]
    flagged = [r for r in results if r["new_decision"] == "flagged"]

    print("\n" + "=" * 60)
    print("RESCREEN SUMMARY")
    print("=" * 60)
    print(f"Total papers: {len(results)}")
    print(f"Still included:  {still_in}/96")
    print(f"Still excluded:  {still_out}/155")
    print(f"Flipped IN→OUT:  {len(in_to_out)}")
    print(f"Flipped OUT→IN:  {len(out_to_in)}")
    print(f"Flagged (disagree): {len(flagged)}")

    if in_to_out:
        print(f"\n--- Flipped INCLUDE → EXCLUDE/FLAGGED ({len(in_to_out)}) ---")
        for r in in_to_out:
            print(f"  [{r['new_decision']}] Paper {r['id']}: {r['title'][:100]}")
            print(f"    P1: {r['pass1_decision']} ({r['pass1_confidence']}) — {r['pass1_rationale'][:80]}")
            print(f"    P2: {r['pass2_decision']} ({r['pass2_confidence']}) — {r['pass2_rationale'][:80]}")

    if out_to_in:
        print(f"\n--- Flipped EXCLUDE → INCLUDE/FLAGGED ({len(out_to_in)}) ---")
        for r in out_to_in:
            print(f"  [{r['new_decision']}] Paper {r['id']}: {r['title'][:100]}")
            print(f"    P1: {r['pass1_decision']} ({r['pass1_confidence']}) — {r['pass1_rationale'][:80]}")
            print(f"    P2: {r['pass2_decision']} ({r['pass2_confidence']}) — {r['pass2_rationale'][:80]}")

    if flagged:
        print(f"\n--- Flagged (pass disagreement) ({len(flagged)}) ---")
        for r in flagged:
            print(f"  Paper {r['id']}: {r['title'][:100]}")
            print(f"    Old: {r['old_decision']} | P1: {r['pass1_decision']} ({r['pass1_confidence']}) | P2: {r['pass2_decision']} ({r['pass2_confidence']})")

    print(f"\nResults saved to: {OUTPUT_CSV}")
    # Clean up checkpoint on success
    if CHECKPOINT.exists():
        CHECKPOINT.unlink()


if __name__ == "__main__":
    main()
