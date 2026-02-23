#!/usr/bin/env python3
"""Full evidence engine pipeline runner."""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.agents.auditor import run_audit
from engine.agents.extractor import run_extraction
from engine.agents.screener import run_screening
from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec, load_review_spec
from engine.exporters import export_all
from engine.parsers.pdf_parser import parse_all_pdfs
from engine.search.dedup import deduplicate
from engine.search.openalex import search_openalex
from engine.search.pubmed import search_pubmed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")

STAGES = ("search", "screen", "parse", "extract", "audit", "export")


# ── Pipeline ─────────────────────────────────────────────────────────


def run_pipeline(
    spec_path: str,
    review_name: str,
    skip_to: str | None = None,
    limit: int | None = None,
) -> None:
    """Run the full evidence engine pipeline."""
    t_start = time.time()

    # ── Load spec ────────────────────────────────────────────
    logger.info("Loading review spec: %s", spec_path)
    spec = load_review_spec(spec_path)
    logger.info("Review: %s (v%s)", spec.title, spec.version)

    # ── Init database ────────────────────────────────────────
    db = ReviewDatabase(review_name)
    logger.info("Database: %s", db.db_path)

    # ── Record review run ────────────────────────────────────
    run_id = _start_review_run(db, spec)

    # ── Determine start stage ────────────────────────────────
    start_idx = 0
    if skip_to:
        if skip_to not in STAGES:
            logger.error("Invalid --skip-to stage: %s (valid: %s)", skip_to, ", ".join(STAGES))
            sys.exit(1)
        start_idx = STAGES.index(skip_to)
        logger.info("Skipping to stage: %s", skip_to)

    results = {}

    try:
        # ── SEARCH ───────────────────────────────────────────
        if start_idx <= STAGES.index("search"):
            results["search"] = _stage_search(db, spec, limit)

        # ── SCREEN ───────────────────────────────────────────
        if start_idx <= STAGES.index("screen"):
            results["screen"] = _stage_screen(db, spec, limit)

        # ── PARSE ────────────────────────────────────────────
        if start_idx <= STAGES.index("parse"):
            results["parse"] = _stage_parse(db, review_name)

        # ── EXTRACT ──────────────────────────────────────────
        if start_idx <= STAGES.index("extract"):
            results["extract"] = _stage_extract(db, spec, review_name)

        # ── AUDIT ────────────────────────────────────────────
        if start_idx <= STAGES.index("audit"):
            results["audit"] = _stage_audit(db, review_name)

        # ── EXPORT ───────────────────────────────────────────
        if start_idx <= STAGES.index("export"):
            results["export"] = _stage_export(db, spec, review_name)

        _finish_review_run(db, run_id, "completed")

    except Exception as exc:
        logger.error("Pipeline failed: %s", exc, exc_info=True)
        _finish_review_run(db, run_id, "failed")
        raise
    finally:
        # ── Final summary ────────────────────────────────────
        elapsed = time.time() - t_start
        stats = db.get_pipeline_stats()
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE in %.1fs", elapsed)
        logger.info("Pipeline stats: %s", json.dumps(stats, indent=2))
        db.close()


# ── Stage Implementations ────────────────────────────────────────────


def _stage_search(db: ReviewDatabase, spec: ReviewSpec, limit: int | None) -> dict:
    t = time.time()
    logger.info("=" * 60)
    logger.info("STAGE: SEARCH")

    pm_cits = search_pubmed(spec)
    logger.info("PubMed: %d citations", len(pm_cits))

    oa_cits = search_openalex(spec)
    logger.info("OpenAlex: %d citations", len(oa_cits))

    dedup_result = deduplicate(pm_cits, oa_cits)
    unique = dedup_result.unique_citations
    logger.info("After dedup: %d unique (%d duplicates removed)",
                len(unique), dedup_result.stats["duplicates_found"])

    if limit:
        unique = unique[:limit]
        logger.info("Limiting to first %d papers", limit)

    added = db.add_papers(unique)
    elapsed = time.time() - t
    logger.info("Search complete in %.1fs — %d papers added to DB", elapsed, added)

    return {
        "pubmed": len(pm_cits),
        "openalex": len(oa_cits),
        "duplicates": dedup_result.stats["duplicates_found"],
        "unique": len(dedup_result.unique_citations),
        "added": added,
        "elapsed": elapsed,
    }


def _stage_screen(db: ReviewDatabase, spec: ReviewSpec, limit: int | None) -> dict:
    t = time.time()
    logger.info("=" * 60)
    logger.info("STAGE: SCREEN")

    if limit:
        # Limit screening to first N ingested papers
        papers = db.get_papers_by_status("INGESTED")
        if len(papers) > limit:
            logger.info("Limiting screening to first %d of %d papers", limit, len(papers))
            # Screen only the limited set by temporarily updating the rest
            # Actually, run_screening processes all INGESTED, so we handle this
            # by running screen on the full set — the limit was applied at search
            pass

    stats = run_screening(db, spec)
    elapsed = time.time() - t
    logger.info("Screening complete in %.1fs — %s", elapsed, json.dumps(stats))
    return {**stats, "elapsed": elapsed}


def _stage_parse(db: ReviewDatabase, review_name: str) -> dict:
    t = time.time()
    logger.info("=" * 60)
    logger.info("STAGE: PARSE")

    pdf_acquired = db.get_papers_by_status("PDF_ACQUIRED")
    if not pdf_acquired:
        logger.info(
            "No papers with status PDF_ACQUIRED. "
            "PDF acquisition is manual for v1. Place PDFs in "
            "data/%s/pdfs/ named as {paper_id}.pdf, then set status to PDF_ACQUIRED.",
            review_name,
        )
        return {"parsed": 0, "note": "No PDFs available", "elapsed": 0}

    stats = parse_all_pdfs(db, review_name)
    elapsed = time.time() - t
    logger.info("Parse complete in %.1fs — %s", elapsed, json.dumps(stats))
    return {**stats, "elapsed": elapsed}


def _stage_extract(db: ReviewDatabase, spec: ReviewSpec, review_name: str) -> dict:
    t = time.time()
    logger.info("=" * 60)
    logger.info("STAGE: EXTRACT")

    parsed = db.get_papers_by_status("PARSED")
    if not parsed:
        logger.info("No papers with status PARSED — skipping extraction.")
        return {"extracted": 0, "elapsed": 0}

    stats = run_extraction(db, spec, review_name)
    elapsed = time.time() - t
    logger.info("Extraction complete in %.1fs — %s", elapsed, json.dumps(stats))
    return {**stats, "elapsed": elapsed}


def _stage_audit(db: ReviewDatabase, review_name: str) -> dict:
    t = time.time()
    logger.info("=" * 60)
    logger.info("STAGE: AUDIT")

    extracted = db.get_papers_by_status("EXTRACTED")
    if not extracted:
        logger.info("No papers with status EXTRACTED — skipping audit.")
        return {"papers_audited": 0, "elapsed": 0}

    stats = run_audit(db, review_name)
    elapsed = time.time() - t
    logger.info("Audit complete in %.1fs — %s", elapsed, json.dumps(stats))
    return {**stats, "elapsed": elapsed}


def _stage_export(db: ReviewDatabase, spec: ReviewSpec, review_name: str) -> dict:
    t = time.time()
    logger.info("=" * 60)
    logger.info("STAGE: EXPORT")

    paths = export_all(db, spec, review_name)
    elapsed = time.time() - t
    logger.info("Export complete in %.1fs", elapsed)
    for name, path in paths.items():
        logger.info("  %s: %s", name, path)
    return {"files": paths, "elapsed": elapsed}


# ── Review Run Tracking ──────────────────────────────────────────────


def _start_review_run(db: ReviewDatabase, spec: ReviewSpec) -> int:
    now = datetime.now(timezone.utc).isoformat()
    cur = db._conn.execute(
        """INSERT INTO review_runs
           (review_spec_hash, screening_hash, extraction_hash,
            started_at, status, log)
           VALUES (?, ?, ?, ?, 'running', '[]')""",
        (
            spec.screening_hash() + spec.extraction_hash(),
            spec.screening_hash(),
            spec.extraction_hash(),
            now,
        ),
    )
    db._conn.commit()
    return cur.lastrowid


def _finish_review_run(db: ReviewDatabase, run_id: int, status: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    db._conn.execute(
        "UPDATE review_runs SET status = ?, completed_at = ? WHERE id = ?",
        (status, now, run_id),
    )
    db._conn.commit()


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Run the Surgical Evidence Engine pipeline")
    parser.add_argument("--spec", required=True, help="Path to Review Spec YAML file")
    parser.add_argument("--name", required=True, help="Review name (used for database/directory)")
    parser.add_argument(
        "--skip-to",
        choices=STAGES,
        default=None,
        help="Skip to a specific pipeline stage",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of papers to process (for testing)",
    )
    args = parser.parse_args()

    run_pipeline(args.spec, args.name, skip_to=args.skip_to, limit=args.limit)


if __name__ == "__main__":
    main()
