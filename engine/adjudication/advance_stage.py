#!/usr/bin/env python3
"""CLI to advance review workflow stages (screening + extraction audit).

Usage:
    python -m engine.adjudication.advance_stage \
        --review surgical_autonomy \
        --stage DIAGNOSTIC_SAMPLE_COMPLETE \
        --note "50-paper sample reviewed, 4 FP categories identified"

    # Force-bypass a stage (logs warning to audit trail):
    python -m engine.adjudication.advance_stage \
        --review surgical_autonomy \
        --stage CATEGORIES_CONFIGURED \
        --note "Using default categories" \
        --force

    # Show current workflow status:
    python -m engine.adjudication.advance_stage \
        --review surgical_autonomy \
        --status

Screening stages: SCREENING_COMPLETE, DIAGNOSTIC_SAMPLE_COMPLETE,
    CATEGORIES_CONFIGURED, QUEUE_EXPORTED, ADJUDICATION_COMPLETE

Extraction audit stages: EXTRACTION_COMPLETE, AI_AUDIT_COMPLETE_STAGE,
    AUDIT_QUEUE_EXPORTED, AUDIT_REVIEW_COMPLETE
"""

import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.adjudication.workflow import (
    WORKFLOW_STAGES,
    advance_stage,
    format_workflow_status,
)
from engine.core.database import ReviewDatabase


def main():
    parser = argparse.ArgumentParser(
        description="Advance screening adjudication workflow stages",
    )
    parser.add_argument(
        "--review", required=True,
        help="Review name (e.g., surgical_autonomy)",
    )
    parser.add_argument(
        "--stage", choices=WORKFLOW_STAGES,
        help="Stage to advance to",
    )
    parser.add_argument(
        "--note",
        help="Required documentation note (why this stage is being advanced)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Bypass prerequisite checks (logs warning to audit trail)",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show current workflow status and exit",
    )
    args = parser.parse_args()

    db = ReviewDatabase(args.review)

    try:
        if args.status:
            print(format_workflow_status(db._conn, review_name=args.review))
            return

        if not args.stage:
            parser.error("--stage is required unless --status is used")

        if not args.note:
            parser.error("--note is required (document why this stage is being advanced)")

        result = advance_stage(db._conn, args.stage, args.note, force=args.force)

        print(f"\n{result['message']}")
        if result["status"] == "blocked":
            blocker = result["blocker"]
            print(f"\nBlocker: {blocker['stage_name']}")
            print(f"Next step: {blocker['next_step']}")

        print()
        print(format_workflow_status(db._conn, review_name=args.review))
    finally:
        db.close()


if __name__ == "__main__":
    main()
