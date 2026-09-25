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

from engine.adjudication.workflow import (
    complete_stage,
    format_workflow_status,
    get_current_blocker,
    is_adjudication_complete,
    is_audit_review_complete,
)
from engine.agents.audit_events import audit_run
from engine.core.extraction_events import RunAborted
from engine.agents.extractor import run_extraction, verify_extraction_run
from engine.core.codebook import CODEBOOK_FILENAME
from engine.core.run_telemetry import record_run_event
from engine.validators.distribution_monitor import run_post_extraction_check
from engine.core.selection import select_for_extraction
from engine.core.effective import effective_state, eligible_paper_ids
from engine.core.paper_state import COMPLETED_PROCESSING_STATES
from engine.agents.screener import run_screening
from engine.core import run_manifest as rm
from engine.core.database import ReviewDatabase
from engine.core.codebook import load_codebook_for
from engine.core.review_paths import load_spec_for
from engine.core.review_spec import ReviewSpec
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

# Post-screening stages that require adjudication to be complete
_POST_SCREENING_STAGES = {"parse", "extract", "audit", "export"}


# ── Pipeline ─────────────────────────────────────────────────────────


def run_pipeline(
    review_name: str,
    spec_path: str | None = None,
    skip_to: str | None = None,
    limit: int | None = None,
) -> None:
    """Run the full evidence engine pipeline.

    `review_name` is the review's identity: the spec file and the data root
    are both derived from it. `spec_path` is an optional override and must
    carry the same review_id.
    """
    t_start = time.time()

    # ── Load spec ────────────────────────────────────────────
    # Before the database, always: load_spec_for refuses a spec that names a
    # different review, and a refusal after the database is opened is a
    # refusal that has already written a directory tree.
    spec = load_spec_for(review_name, spec_path)
    logger.info("Review: %s — %s (v%s)", spec.review_id, spec.title, spec.version)

    # ── Init database ────────────────────────────────────────
    db = ReviewDatabase(review_name)
    logger.info("Database: %s", db.db_path)

    # ── Print workflow status ────────────────────────────────
    logger.info("\n%s", format_workflow_status(db._conn, review_name=review_name))

    # ── Determine start stage ────────────────────────────────
    start_idx = 0
    if skip_to:
        if skip_to not in STAGES:
            logger.error("Invalid --skip-to stage: %s (valid: %s)", skip_to, ", ".join(STAGES))
            sys.exit(1)
        start_idx = STAGES.index(skip_to)
        logger.info("Skipping to stage: %s", skip_to)

    # ── Open the run manifest (R73) ──────────────────────────
    # Before the first model call, and instead of a review_runs row: the
    # manifest records the resolved configuration of every stage this run can
    # call, and refuses a dirty tree or a pre-manifest arm before anything runs.
    run_id = _open_run_manifest(db, spec, start_idx)
    run_token = rm.activate(db._conn, run_id)

    results = {}

    try:
        # ── SEARCH ───────────────────────────────────────────
        if start_idx <= STAGES.index("search"):
            results["search"] = _stage_search(db, spec, limit)

        # ── SCREEN ───────────────────────────────────────────
        if start_idx <= STAGES.index("screen"):
            results["screen"] = _stage_screen(db, spec, limit)

        # ── ADJUDICATION GATE ─────────────────────────────────
        # Check before any post-screening stage
        target_stage = STAGES[start_idx] if skip_to else "parse"
        if target_stage in _POST_SCREENING_STAGES:
            if not is_adjudication_complete(db._conn):
                blocker = get_current_blocker(db._conn)
                if blocker:
                    logger.error("")
                    logger.error("BLOCKED: Adjudication workflow incomplete.")
                    logger.error("Current stage: %s", blocker["stage_name"])
                    logger.error("Next step: %s", blocker["next_step"])
                    logger.error("")
                    logger.error(
                        "Run 'python -m engine.adjudication.advance_stage "
                        "--review %s --status' for full workflow status.",
                        review_name,
                    )
                    _finish_review_run(db, run_id, "interrupted")
                    return

        # ── PARSE ────────────────────────────────────────────
        if start_idx <= STAGES.index("parse"):
            results["parse"] = _stage_parse(db, review_name)

        # ── EXTRACT ──────────────────────────────────────────
        if start_idx <= STAGES.index("extract"):
            results["extract"] = _stage_extract(db, spec, review_name, run_id=run_id)

        # ── AUDIT ────────────────────────────────────────────
        if start_idx <= STAGES.index("audit"):
            results["audit"] = _stage_audit(db, review_name, spec, run_id=run_id)

            # Auto-advance extraction workflow stages
            try:
                _advance_extraction_workflow(db._conn)
            except Exception:
                pass  # workflow table may not exist

        # ── AUDIT REVIEW GATE ──────────────────────────────
        if start_idx <= STAGES.index("export"):
            if not is_audit_review_complete(db._conn):
                blocker = get_current_blocker(db._conn)
                if blocker and blocker["stage_name"] in (
                    "AUDIT_QUEUE_EXPORTED", "AUDIT_REVIEW_COMPLETE",
                ):
                    logger.error("")
                    logger.error("BLOCKED: Audit review workflow incomplete.")
                    logger.error("Current stage: %s", blocker["stage_name"])
                    logger.error("Next step: %s", blocker["next_step"])
                    logger.error("")
                    logger.error(
                        "Run 'python -m engine.adjudication.advance_stage "
                        "--review %s --status' for full workflow status.",
                        review_name,
                    )
                    _finish_review_run(db, run_id, "interrupted")
                    return

        # ── EXPORT ───────────────────────────────────────────
        if start_idx <= STAGES.index("export"):
            results["export"] = _stage_export(db, spec, review_name)

        _finish_review_run(db, run_id, "completed")

    except RunAborted as exc:
        # 9b-FLIP: the consecutive-failure abort. Every aborted paper's event is
        # written; the manifest records 'failed' (020's closed end_status set),
        # and the reason is here and in the exception (row C26).
        logger.error("RUN ABORTED: %s", exc)
        _finish_review_run(db, run_id, "failed")
        raise
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc, exc_info=True)
        _finish_review_run(db, run_id, "failed")
        raise
    finally:
        rm.deactivate(run_token)
        # ── Final summary ────────────────────────────────────
        elapsed = time.time() - t_start
        stats = db.get_pipeline_stats()
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE in %.1fs", elapsed)
        logger.info("Pipeline stats: %s", json.dumps(stats, indent=2))
        db.close()


# ── Stage completion, from the event store (R112) ───────────────────


def _processing_states(conn) -> dict[int, str]:
    """Each corpus paper's processing-axis token, through the one reader.

    One `effective_state` per eligible paper: the axis derivation is Python in
    `engine.core.effective`, and a second SQL copy of it is the defect A1 names.
    `papers.status` is not read.
    """
    return {pid: effective_state(conn, pid).processing for pid in eligible_paper_ids(conn)}


def _advance_extraction_workflow(conn) -> dict[str, int]:
    """Reader 8 on the reader (9b-FLIP 1/2): EXTRACTION_COMPLETE once any corpus
    paper's processing state is completed (extracted or audited_ai), and
    AI_AUDIT_COMPLETE_STAGE once any is audited_ai. Returns the two counts."""
    states = _processing_states(conn)
    extracted = sum(1 for s in states.values() if s in COMPLETED_PROCESSING_STATES)
    audited = sum(1 for s in states.values() if s == "audited_ai")
    if extracted > 0:
        complete_stage(conn, "EXTRACTION_COMPLETE",
                       metadata=f"{extracted} papers extracted")
    if audited > 0:
        complete_stage(conn, "AI_AUDIT_COMPLETE_STAGE",
                       metadata=f"{audited} papers audited")
    return {"extracted": extracted, "audited": audited}


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


def _stage_extract(db: ReviewDatabase, spec: ReviewSpec, review_name: str, *,
                   run_id: int) -> dict:
    t = time.time()
    logger.info("=" * 60)
    logger.info("STAGE: EXTRACT")

    # R117: the manifest's digest agrees with the arm's pin, or the run refuses
    # here — before anything is selected.
    verify_extraction_run(db._conn, spec, run_id)
    # Selection is the corpus predicate with the reuse key (D9, R96, R119), made
    # once here and handed to the run, not a papers.status gate.
    selection = select_for_extraction(db._conn, arm=spec.extraction_models.arm)
    if not selection.to_extract:
        logger.info("Nothing to extract for arm %s — %d skipped (asserted), "
                    "%d skipped (refused).", selection.arm,
                    len(selection.skipped_asserted), len(selection.skipped_refused))
        stats = {"extracted": 0,
                 "skipped_asserted": len(selection.skipped_asserted),
                 "skipped_refused": len(selection.skipped_refused)}
        stats["distribution_check"] = _distribution_check(
            db, spec, review_name, run_id=run_id, stats=stats)
        return {**stats, "elapsed": time.time() - t}

    stats = run_extraction(db, spec, review_name, selection=selection, run_id=run_id)
    stats["distribution_check"] = _distribution_check(
        db, spec, review_name, run_id=run_id, stats=stats)
    elapsed = time.time() - t
    logger.info("Extraction complete in %.1fs — %s", elapsed, json.dumps(stats))
    return {**stats, "elapsed": elapsed}


def _distribution_check(db: ReviewDatabase, spec: ReviewSpec, review_name: str, *,
                        run_id: int, stats: dict) -> dict:
    """B9 (R167): the distribution-collapse check on the local path.

    Non-strict and non-raising. The population rule is the arm's (>= 10 eligible
    papers holding a live claim for it), and a failed paper in this run is
    recorded, not a veto. A COLLAPSED result is logged and written to run
    telemetry (`engine/core/run_telemetry.py`); it never becomes a paper outcome
    and never aborts the run. A skip writes a row too, so every extract stage
    leaves evidence that the check ran.
    """
    arm = spec.extraction_models.arm
    review_dir = Path(db.db_path).parent
    summary = run_post_extraction_check(
        Path(db.db_path), review_name, arm, review_dir / CODEBOOK_FILENAME,
        extracted_count=stats.get("extracted", 0), failed_count=stats.get("failed", 0),
        strict=False, raise_on_collapse=False, skip_on_failures=False,
        min_population="arm")
    record_run_event(review_dir, run_id=run_id, kind="distribution_check", payload={
        "run_id": run_id, "arm": arm,
        "extracted_count": summary["extracted_count"],
        "failed_count": summary["failed_count"],
        "arm_population": summary["arm_population"], "strict": False,
        "skipped": summary["skipped"], "skip_reason": summary["skip_reason"],
        "ok": summary["ok"], "low_variance": summary["low_variance"],
        "collapsed": summary["collapsed"],
        "collapsed_fields": summary["collapsed_fields"],
        "low_variance_fields": summary["low_variance_fields"],
        "results": summary["results"]})
    if summary["skipped"]:
        logger.info("Distribution check (run %d, arm %s): skipped — %s",
                    run_id, arm, summary["skip_reason"])
    elif summary["collapsed"]:
        logger.error("DISTRIBUTION CHECK (run %d, arm %s): COLLAPSED %s — recorded to "
                     "run telemetry; the run continues (R167)",
                     run_id, arm, ", ".join(summary["collapsed_fields"]))
    return {k: summary[k] for k in ("skipped", "skip_reason", "arm_population",
                                    "ok", "low_variance", "collapsed",
                                    "collapsed_fields", "low_variance_fields")}


def _stage_audit(db: ReviewDatabase, review_name: str, spec: ReviewSpec = None, *,
                 run_id: int) -> dict:
    """The event-side audit (9b-FLIP, R111): locate every live claim of the
    spec's arm, verify the unlocated values cross-family, write `audited_ai`.
    No `papers.status` gate — `audit_run` passes over a paper with nothing left
    to locate (R119's pattern)."""
    from dataclasses import asdict

    from engine.core.effective_config import stage_config
    from engine.utils.ollama_preflight import require_preflight

    t = time.time()
    logger.info("=" * 60)
    logger.info("STAGE: AUDIT")
    require_preflight([stage_config("audit", spec).model], runner_name="Audit", spec=spec)
    report = audit_run(db._conn, spec, run_id=run_id, arm=spec.extraction_models.arm,
                       review_dir=Path(db.db_path).parent)
    stats = asdict(report)
    elapsed = time.time() - t
    logger.info("Audit complete in %.1fs — %s", elapsed, json.dumps(stats, default=list))
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


#: Resolver stages each pipeline stage can call. Preflight rows are added per
#: probed model.
_PIPELINE_STAGE_CONFIGS = {
    "screen": ("abstract_screen_primary", "abstract_screen_verifier"),
    "parse": ("vision_parse",),
    "extract": ("extract_pass1", "extract_pass2", "extract_retry_snippet"),
    "audit": ("audit",),
}


def _open_run_manifest(db: ReviewDatabase, spec: ReviewSpec, start_idx: int) -> int:
    """Write this run's manifest before its first call (S3a, R73).

    `review_runs` is no longer written: it linked to nothing, recorded no
    configuration, and one row has read 'running' since 2026-03-01. It stays as
    read-only telemetry and retires at session 10 (retention ledger).
    """
    from engine.core.codebook import load_codebook_beside
    from engine.core.effective_config import stage_config

    stages: list[str] = []
    preflight: list[str] = []
    for name in STAGES[start_idx:]:
        stages.extend(_PIPELINE_STAGE_CONFIGS.get(name, ()))
    if spec.extraction_models.elicitation and "extract_pass1" in stages:
        stages[stages.index("extract_pass1")] = "elicitation_pass1"
    if any(s.startswith("extract") or s == "elicitation_pass1" for s in stages):
        preflight.append(stage_config("extract_pass1", spec).model)
    if "audit" in stages:
        preflight.append(stage_config("audit", spec).model)
    if preflight:
        stages.append("preflight")

    codebook = load_codebook_beside(db.db_path)
    for finding in codebook.lint_findings:
        logger.warning("Codebook lint: %s", finding)
    kind = "extraction" if any(s.startswith(("extract", "elicitation", "audit"))
                               for s in stages) else "screening"
    handle = rm.open_run(db._conn, spec, kind=kind, stages=stages, codebook=codebook,
                         preflight_models=sorted(set(preflight)))
    logger.info("Run manifest %d (%s) written: %d stage rows", handle.run_id,
                handle.run_uid, len(handle.stages))
    return handle.run_id


def _finish_review_run(db: ReviewDatabase, run_id: int, status: str) -> None:
    """Record the run's end on its manifest, once."""
    rm.close_run(db._conn, run_id, status)


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    from engine.utils.background import maybe_background

    # Extract the review id early for the log path (before argparse strips it)
    review_name = "review"
    for i, arg in enumerate(sys.argv):
        if arg in ("--review", "--name") and i + 1 < len(sys.argv):
            review_name = sys.argv[i + 1]
            break

    maybe_background("pipeline", review_name=review_name)

    parser = argparse.ArgumentParser(description="Run the Surgical Evidence Engine pipeline")
    parser.add_argument(
        "--review", "--name", dest="review", required=True,
        help=("Review id. The review's identity — the spec file "
              "(review_specs/<review>.yaml) and the data root (data/<review>) "
              "both derive from it. --name is a deprecated alias."),
    )
    parser.add_argument(
        "--spec", default=None,
        help=("Override the Review Spec path. Defaults to "
              "review_specs/<review>.yaml; an override must carry the same "
              "review_id."),
    )
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

    if "--name" in sys.argv:
        logger.warning(
            "--name is deprecated and will be removed; use --review %s.", args.review
        )

    run_pipeline(args.review, args.spec, skip_to=args.skip_to, limit=args.limit)


if __name__ == "__main__":
    main()
