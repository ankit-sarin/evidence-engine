#!/usr/bin/env python3
"""Re-screen included papers with updated Review Spec (specialty_scope).

Targets: ABSTRACT_SCREENED_IN (554) + AI_AUDIT_COMPLETE (95) = 649 papers.
Runs dual-pass primary screening (qwen3:8b) + verification (gemma3:27b).
All decisions stored in abstract_screening_decisions with pass_number >= 10
to distinguish from the original screening round.

Status handling:
  - EXCLUDE (both passes agree) → ABSTRACT_SCREENED_OUT (administrative override)
  - INCLUDE (both passes agree) → leave at current status
  - FLAGGED (disagree)          → ABSTRACT_SCREEN_FLAGGED

Supports checkpoint/resume and --background (tmux).
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.agents.screener import ScreeningDecision, screen_paper
from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec, load_review_spec

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("rescreen_specialty")

# ── Config ──────────────────────────────────────────────────────────

DEFAULT_REVIEW = "surgical_autonomy"
TARGET_STATUSES = ("ABSTRACT_SCREENED_IN", "AI_AUDIT_COMPLETE")

# Use pass_number 1/2 (schema CHECK constraint). Distinguish via RESCREEN_TAG in rationale.
RESCREEN_PASS1 = 1
RESCREEN_PASS2 = 2
RESCREEN_TAG = "specialty_rescreen"

# ── Checkpoint ──────────────────────────────────────────────────────


def _ckpt_path(db: ReviewDatabase) -> Path:
    return db.db_path.parent / "specialty_rescreen_checkpoint.json"


def _load_ckpt(path: Path) -> dict:
    if not path.exists():
        return {"screened_ids": [], "stats": {}}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return {"screened_ids": [], "stats": {}}


def _save_ckpt(path: Path, screened_ids: set, stats: dict) -> None:
    path.write_text(json.dumps({
        "screened_ids": sorted(screened_ids),
        "stats": stats,
    }))


# ── Administrative Status Override ──────────────────────────────────


def _force_status(db: ReviewDatabase, paper_id: int, new_status: str) -> None:
    """Direct SQL status update — bypasses state machine.

    Used for administrative re-screening where normal transitions
    don't cover the path (e.g., AI_AUDIT_COMPLETE → ABSTRACT_SCREENED_OUT).
    """
    now = datetime.now(timezone.utc).isoformat()
    db._conn.execute(
        "UPDATE papers SET status = ?, updated_at = ? WHERE id = ?",
        (new_status, now, paper_id),
    )
    db._conn.commit()


# ── Main ────────────────────────────────────────────────────────────


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Re-screen with specialty scope")
    parser.add_argument("--review", default=DEFAULT_REVIEW, help=f"Review name (default: {DEFAULT_REVIEW})")
    parser.add_argument("--spec", default=None, help="Path to review spec YAML (default: review_specs/<review>_v1.yaml)")
    parser.add_argument("--background", action="store_true", help="Run in tmux")
    parser.add_argument("--verify-only", action="store_true", help="Run verification pass only")
    parser.add_argument("--report-only", action="store_true", help="Print report from DB, no screening")
    args = parser.parse_args()

    if args.review == DEFAULT_REVIEW and "--review" not in " ".join(sys.argv):
        logging.warning("No --review specified, using default 'surgical_autonomy'.")

    review_name = args.review
    spec_path = args.spec or f"review_specs/{review_name}_v1.yaml"

    if args.background:
        try:
            from engine.utils.background import maybe_background
            maybe_background("specialty_rescreen", review_name=review_name)
        except ImportError:
            logger.warning("Background mode not available — running in foreground")

    spec = load_review_spec(spec_path)
    db = ReviewDatabase(review_name)

    if args.report_only:
        _print_report(db)
        db.close()
        return

    try:
        if args.verify_only:
            run_verification_pass(db, spec)
        else:
            run_primary_rescreen(db, spec)
            run_verification_pass(db, spec)
    finally:
        _print_report(db)
        db.close()


def run_primary_rescreen(db: ReviewDatabase, spec: ReviewSpec) -> dict:
    """Run dual-pass primary re-screen on all target papers."""
    primary_model = spec.screening_models.primary
    logger.info("Primary model: %s", primary_model)
    logger.info("Specialty scope included: %s", spec.specialty_scope is not None)

    # Collect target papers
    papers = []
    for status in TARGET_STATUSES:
        papers.extend(db.get_papers_by_status(status))

    total = len(papers)
    logger.info("Target papers: %d (%s)", total,
                ", ".join(f"{s}: {len(db.get_papers_by_status(s))}" for s in TARGET_STATUSES))

    # Load checkpoint
    ckpt = _ckpt_path(db)
    ckpt_data = _load_ckpt(ckpt)
    screened_ids = set(ckpt_data["screened_ids"])
    stats = ckpt_data.get("stats", {
        "include": 0, "exclude": 0, "flagged": 0, "total": 0, "errors": 0,
    })

    if screened_ids:
        logger.info("Resuming: %d already screened", len(screened_ids))

    pending = [p for p in papers if p["id"] not in screened_ids]
    logger.info("Pending: %d papers", len(pending))

    t_start = time.time()

    for i, paper in enumerate(pending, 1):
        pid = paper["id"]
        original_status = paper["status"]

        try:
            # Pass 1 (primary, high-recall)
            d1 = screen_paper(paper, spec, pass_number=RESCREEN_PASS1, model=primary_model, role="primary")
            db.add_screening_decision(
                pid, RESCREEN_PASS1, d1.decision,
                f"[{RESCREEN_TAG}] {d1.rationale}", primary_model,
            )

            # Pass 2 (primary, second opinion)
            d2 = screen_paper(paper, spec, pass_number=RESCREEN_PASS2, model=primary_model, role="primary")
            db.add_screening_decision(
                pid, RESCREEN_PASS2, d2.decision,
                f"[{RESCREEN_TAG}] {d2.rationale}", primary_model,
            )

            # Resolve
            if d1.decision == "exclude" and d2.decision == "exclude":
                # Both passes say exclude → ABSTRACT_SCREENED_OUT
                _force_status(db, pid, "ABSTRACT_SCREENED_OUT")
                stats["exclude"] = stats.get("exclude", 0) + 1
                logger.info(
                    "Paper %d EXCLUDED (was %s): %s — %s",
                    pid, original_status, paper["title"][:60], d1.rationale[:80],
                )
            elif d1.decision == "include" and d2.decision == "include":
                # Both include → leave at current status
                stats["include"] = stats.get("include", 0) + 1
            else:
                # Disagreement → flag
                if original_status == "ABSTRACT_SCREENED_IN":
                    db.update_status(pid, "ABSTRACT_SCREEN_FLAGGED")
                elif original_status == "AI_AUDIT_COMPLETE":
                    _force_status(db, pid, "ABSTRACT_SCREEN_FLAGGED")
                stats["flagged"] = stats.get("flagged", 0) + 1
                logger.info(
                    "Paper %d FLAGGED (was %s): %s — P1=%s P2=%s",
                    pid, original_status, paper["title"][:60],
                    d1.decision, d2.decision,
                )

        except Exception as e:
            logger.error("Paper %d ERROR: %s", pid, e)
            stats["errors"] = stats.get("errors", 0) + 1

        stats["total"] = stats.get("total", 0) + 1
        screened_ids.add(pid)

        if i % 10 == 0 or i == len(pending):
            elapsed = time.time() - t_start
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(pending) - i) / rate / 60 if rate > 0 else 0
            _save_ckpt(ckpt, screened_ids, stats)
            logger.info(
                "Primary %d/%d — %d include, %d exclude, %d flagged "
                "(%.1f papers/min, ETA %.0f min)",
                i, len(pending),
                stats.get("include", 0), stats.get("exclude", 0),
                stats.get("flagged", 0), rate * 60, eta,
            )

    # Clean up checkpoint
    if ckpt.exists():
        ckpt.unlink()

    logger.info(
        "Primary re-screen complete: %d include, %d exclude, %d flagged, %d errors",
        stats.get("include", 0), stats.get("exclude", 0),
        stats.get("flagged", 0), stats.get("errors", 0),
    )
    return stats


def run_verification_pass(db: ReviewDatabase, spec: ReviewSpec) -> dict:
    """Run verification on papers that remained ABSTRACT_SCREENED_IN after primary."""
    verification_model = spec.screening_models.verification
    logger.info("Verification model: %s", verification_model)

    papers = db.get_papers_by_status("ABSTRACT_SCREENED_IN")
    logger.info("ABSTRACT_SCREENED_IN papers for verification: %d", len(papers))

    # Also include AI_AUDIT_COMPLETE papers (these were 'include' in primary)
    ai_papers = db.get_papers_by_status("AI_AUDIT_COMPLETE")
    papers.extend(ai_papers)
    logger.info("Total for verification: %d (incl. %d AI_AUDIT_COMPLETE)", len(papers), len(ai_papers))

    # Check which already have a rescreen verification decision
    already_verified = set()
    for p in papers:
        row = db._conn.execute(
            "SELECT id FROM abstract_verification_decisions WHERE paper_id = ? "
            "AND rationale LIKE ?",
            (p["id"], f"%{RESCREEN_TAG}%"),
        ).fetchone()
        if row:
            already_verified.add(p["id"])

    pending = [p for p in papers if p["id"] not in already_verified]
    if already_verified:
        logger.info("Resuming verification: %d already done, %d pending",
                     len(already_verified), len(pending))

    stats = {"confirmed": 0, "flagged": 0, "total": 0, "errors": 0}
    t_start = time.time()

    for i, paper in enumerate(pending, 1):
        pid = paper["id"]
        original_status = paper["status"]

        try:
            decision = screen_paper(
                paper, spec, pass_number=1,
                model=verification_model, role="verifier",
            )
            db.add_verification_decision(
                pid, decision.decision,
                f"[{RESCREEN_TAG}] {decision.rationale}",
                verification_model,
            )

            if decision.decision == "include":
                stats["confirmed"] += 1
            else:
                # Verifier excludes → flag for PI review
                if original_status == "ABSTRACT_SCREENED_IN":
                    db.update_status(pid, "ABSTRACT_SCREEN_FLAGGED")
                elif original_status == "AI_AUDIT_COMPLETE":
                    _force_status(db, pid, "ABSTRACT_SCREEN_FLAGGED")
                stats["flagged"] += 1
                logger.info(
                    "Paper %d FLAGGED by verifier (was %s): %s — %s",
                    pid, original_status, paper["title"][:60],
                    decision.rationale[:80],
                )

        except Exception as e:
            logger.error("Paper %d verification ERROR: %s", pid, e)
            stats["errors"] += 1

        stats["total"] += 1

        if i % 10 == 0 or i == len(pending):
            elapsed = time.time() - t_start
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(pending) - i) / rate / 60 if rate > 0 else 0
            logger.info(
                "Verify %d/%d — %d confirmed, %d flagged (%.1f/min, ETA %.0f min)",
                i, len(pending), stats["confirmed"], stats["flagged"],
                rate * 60, eta,
            )

    logger.info(
        "Verification complete: %d confirmed, %d flagged, %d errors",
        stats["confirmed"], stats["flagged"], stats["errors"],
    )
    return stats


# ── Report ──────────────────────────────────────────────────────────


def _print_report(db: ReviewDatabase):
    """Print post-rescreen report."""
    print("\n" + "=" * 80)
    print("SPECIALTY RE-SCREEN REPORT")
    print("=" * 80)

    # Status breakdown
    print("\n── Status Breakdown ──\n")
    rows = db._conn.execute(
        "SELECT status, COUNT(*) as cnt FROM papers GROUP BY status ORDER BY cnt DESC"
    ).fetchall()
    for r in rows:
        print(f"  {r['status']:<30} {r['cnt']:>6}")

    # Rescreen decisions summary
    print("\n── Rescreen Decisions ──\n")
    rescreen_rows = db._conn.execute(
        "SELECT decision, COUNT(*) as cnt FROM abstract_screening_decisions "
        "WHERE rationale LIKE '%specialty_rescreen%' GROUP BY decision"
    ).fetchall()
    for r in rescreen_rows:
        print(f"  {r['decision']}: {r['cnt']}")

    # Excluded papers
    print("\n── Papers Excluded in Re-Screen ──\n")
    excluded = db._conn.execute(
        """SELECT DISTINCT p.id, p.ee_identifier, p.title, sd.rationale
           FROM papers p
           JOIN abstract_screening_decisions sd ON sd.paper_id = p.id
           WHERE p.status = 'ABSTRACT_SCREENED_OUT'
             AND sd.rationale LIKE '%specialty_rescreen%'
             AND sd.decision = 'exclude'
             AND sd.pass_number = 1
           ORDER BY p.id"""
    ).fetchall()

    # Filter to only papers that were recently moved to SCREENED_OUT
    # (i.e., they had a rescreen exclude AND are now SCREENED_OUT
    #  but were previously in a different status)
    # We identify these by checking if they have rescreen decisions
    if excluded:
        print(f"{'ID':>5}  {'EE':>8}  {'Reason':<60}  Title")
        print("-" * 120)
        for r in excluded:
            reason = r["rationale"].replace("[specialty_rescreen] ", "")[:60]
            ee = r["ee_identifier"] or ""
            print(f"{r['id']:>5}  {ee:>8}  {reason:<60}  {r['title'][:50]}")
        print(f"\nTotal excluded: {len(excluded)}")
    else:
        print("  (none)")

    # Flagged papers
    print("\n── Papers Flagged in Re-Screen ──\n")
    flagged = db._conn.execute(
        """SELECT DISTINCT p.id, p.ee_identifier, p.title
           FROM papers p
           WHERE p.status = 'ABSTRACT_SCREEN_FLAGGED'
           ORDER BY p.id"""
    ).fetchall()
    if flagged:
        print(f"{'ID':>5}  {'EE':>8}  Title")
        print("-" * 80)
        for r in flagged:
            ee = r["ee_identifier"] or ""
            print(f"{r['id']:>5}  {ee:>8}  {r['title'][:65]}")
        print(f"\nTotal flagged: {len(flagged)}")
    else:
        print("  (none)")

    # Rescreen verification summary
    print("\n── Verification Decisions (Rescreen) ──\n")
    vrows = db._conn.execute(
        "SELECT decision, COUNT(*) as cnt FROM abstract_verification_decisions "
        "WHERE rationale LIKE '%specialty_rescreen%' GROUP BY decision"
    ).fetchall()
    for r in vrows:
        print(f"  {r['decision']}: {r['cnt']}")

    print()


if __name__ == "__main__":
    main()
