#!/usr/bin/env python3
"""Re-extract papers that failed or were skipped during overnight run.

Resets EXTRACT_FAILED → PARSED, then runs two-pass DeepSeek-R1 extraction
with extended timeout. Retries once on failure with even longer timeout.
"""

import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import engine.agents.extractor as extractor
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("reextract")

PAPER_IDS = [15, 17, 54, 142]
TIMEOUT_FIRST = 1200.0   # 20 minutes
TIMEOUT_RETRY = 1500.0   # 25 minutes
DEFAULT_REVIEW = "surgical_autonomy"


def main():
    parser = argparse.ArgumentParser(description="Re-extract papers that failed or were skipped")
    parser.add_argument("--review", default=DEFAULT_REVIEW, help=f"Review name (default: {DEFAULT_REVIEW})")
    parser.add_argument("--spec", default=None, help="Path to review spec YAML (default: review_specs/<review>_v1.yaml)")
    args = parser.parse_args()

    if args.review == DEFAULT_REVIEW and "--review" not in " ".join(sys.argv):
        logging.warning("No --review specified, using default 'surgical_autonomy'.")

    review_name = args.review
    spec_path = args.spec or f"review_specs/{review_name}_v1.yaml"

    spec = load_review_spec(spec_path)
    db = ReviewDatabase(review_name)
    review_dir = Path(db.db_path).parent

    # Step 1: Reset EXTRACT_FAILED → PARSED
    for pid in PAPER_IDS:
        row = db._conn.execute(
            "SELECT status FROM papers WHERE id = ?", (pid,)
        ).fetchone()
        status = row["status"]
        if status == "EXTRACT_FAILED":
            db.update_status(pid, "PARSED")
            logger.info("Paper %d: reset %s → PARSED", pid, status)
        elif status == "PARSED":
            logger.info("Paper %d: already PARSED", pid)
        else:
            logger.warning("Paper %d: unexpected status %s — skipping", pid, status)

    # Step 2: Extract each paper
    results = {}
    for pid in PAPER_IDS:
        row = db._conn.execute(
            "SELECT id, title, status FROM papers WHERE id = ?", (pid,)
        ).fetchone()
        if row["status"] != "PARSED":
            logger.warning("Paper %d not PARSED (%s) — skipping", pid, row["status"])
            results[pid] = {"status": "skipped", "reason": row["status"]}
            continue

        # Load parsed markdown
        parsed_dir = review_dir / "parsed_text"
        md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)
        if not md_files:
            logger.error("Paper %d: no parsed markdown found", pid)
            results[pid] = {"status": "failed", "reason": "no parsed text"}
            continue

        paper_text = md_files[0].read_text()
        title = row["title"][:70]

        # Attempt extraction with first timeout
        success = _try_extract(pid, paper_text, title, spec, db, TIMEOUT_FIRST)

        if not success:
            # Retry with longer timeout
            logger.info("Paper %d: retrying with %ds timeout...", pid, int(TIMEOUT_RETRY))
            # Reset back to PARSED for retry
            db._conn.execute(
                "UPDATE papers SET status = 'PARSED', updated_at = datetime('now') WHERE id = ?",
                (pid,),
            )
            db._conn.commit()
            success = _try_extract(pid, paper_text, title, spec, db, TIMEOUT_RETRY)

        if success:
            results[pid] = success
        else:
            results[pid] = {"status": "failed", "reason": "both attempts failed"}

    # Step 3: Report
    logger.info("=" * 60)
    logger.info("RE-EXTRACTION RESULTS")
    logger.info("=" * 60)
    for pid, res in results.items():
        row = db._conn.execute(
            "SELECT title, status FROM papers WHERE id = ?", (pid,)
        ).fetchone()
        if res["status"] == "success":
            logger.info(
                "  Paper %3d: SUCCESS  trace=%d chars  time=%.1fs  '%s'",
                pid, res["trace_len"], res["elapsed"], row["title"][:60],
            )
        else:
            logger.info(
                "  Paper %3d: FAILED   reason=%s  '%s'",
                pid, res.get("reason", "unknown"), row["title"][:60],
            )

    success_count = sum(1 for r in results.values() if r["status"] == "success")
    logger.info(
        "Summary: %d/%d extracted successfully", success_count, len(PAPER_IDS)
    )

    db.close()


def _try_extract(pid, paper_text, title, spec, db, timeout):
    """Attempt extraction with given timeout. Returns result dict or None."""
    import ollama as _ollama

    # Patch the module-level client with new timeout
    extractor._client = _ollama.Client(timeout=timeout)
    logger.info(
        "Paper %d: extracting (timeout=%ds) '%s'", pid, int(timeout), title
    )

    t0 = time.time()
    try:
        result = extractor.extract_paper(pid, paper_text, spec, db)
        db.update_status(pid, "EXTRACTED")
        elapsed = time.time() - t0
        trace_len = len(result.reasoning_trace) if result.reasoning_trace else 0
        logger.info(
            "Paper %d: extracted in %.1fs — %d fields, trace=%d chars",
            pid, elapsed, len(result.fields), trace_len,
        )
        return {
            "status": "success",
            "elapsed": elapsed,
            "fields": len(result.fields),
            "trace_len": trace_len,
        }
    except Exception as exc:
        elapsed = time.time() - t0
        logger.error(
            "Paper %d: extraction failed after %.1fs — %s", pid, elapsed, exc
        )
        try:
            db.update_status(pid, "EXTRACT_FAILED")
        except Exception:
            pass
        return None


if __name__ == "__main__":
    main()
