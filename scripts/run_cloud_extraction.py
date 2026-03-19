#!/usr/bin/env python3
"""CLI for running cloud extraction arms (OpenAI o3-mini, Anthropic Sonnet 4.5).

Usage:
    python scripts/run_cloud_extraction.py --arm openai --spec review_specs/surgical_autonomy_v1.yaml
    python scripts/run_cloud_extraction.py --arm anthropic --spec review_specs/surgical_autonomy_v1.yaml
    python scripts/run_cloud_extraction.py --arm openai --max-papers 5 --max-cost 10.00
    python scripts/run_cloud_extraction.py --arm both
    python scripts/run_cloud_extraction.py --progress
    python scripts/run_cloud_extraction.py --dry-run --arm openai
"""

import argparse
import logging
import sys

from engine.cloud.anthropic_extractor import AnthropicExtractor
from engine.cloud.base import CloudExtractorBase
from engine.cloud.openai_extractor import OpenAIExtractor
from engine.cloud.schema import init_cloud_tables

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_REVIEW = "surgical_autonomy"


def show_progress(db_path: str, spec_path: str):
    """Show extraction progress for all arms without calling APIs."""
    init_cloud_tables(db_path)
    # Use base class just for progress queries
    base = CloudExtractorBase(db_path, spec_path)
    for arm in ["openai_o4_mini_high", "anthropic_sonnet_4_6"]:
        progress = base.get_progress(arm)
        print(
            f"{arm}: "
            f"{progress['completed']}/{progress['total_papers']} papers "
            f"({progress['remaining']} remaining) — "
            f"${progress['total_cost_usd']:.2f} spent"
        )
    base.close()


def dry_run(db_path: str, spec_path: str, arms: list[str]):
    """Show what would be extracted without calling APIs."""
    init_cloud_tables(db_path)
    base = CloudExtractorBase(db_path, spec_path)

    for arm_name in arms:
        arm_key = {
            "openai": "openai_o4_mini_high",
            "anthropic": "anthropic_sonnet_4_6",
        }[arm_name]

        pending = base.get_pending_papers(arm_key)
        print(f"\n{arm_key}: {len(pending)} papers pending")
        for p in pending:
            print(f"  Paper {p['paper_id']}: {p['title'][:70]}")

    base.close()


def run_arm(
    arm_name: str,
    db_path: str,
    spec_path: str,
    max_papers: int | None,
    max_cost: float | None,
):
    """Run a single extraction arm."""
    if arm_name == "openai":
        extractor = OpenAIExtractor(db_path, spec_path)
    elif arm_name == "anthropic":
        extractor = AnthropicExtractor(db_path, spec_path)
    else:
        print(f"Unknown arm: {arm_name}")
        sys.exit(1)

    try:
        extractor.run(max_papers=max_papers, max_cost_usd=max_cost)
    finally:
        extractor.close()


def main():
    parser = argparse.ArgumentParser(
        description="Run cloud extraction arms for concordance study"
    )
    parser.add_argument(
        "--review",
        default=DEFAULT_REVIEW,
        help=f"Review name (default: {DEFAULT_REVIEW})",
    )
    parser.add_argument(
        "--arm",
        choices=["openai", "anthropic", "both"],
        help="Which extraction arm to run",
    )
    parser.add_argument(
        "--spec",
        default=None,
        help="Path to review spec YAML (default: review_specs/<review>_v1.yaml)",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Path to database (default: data/<review>/review.db)",
    )
    parser.add_argument(
        "--max-papers",
        type=int,
        default=None,
        help="Maximum number of papers to extract",
    )
    parser.add_argument(
        "--max-cost",
        type=float,
        default=None,
        help="Maximum cost ceiling in USD",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Show progress for all arms (no API calls)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be extracted (no API calls)",
    )

    args = parser.parse_args()

    if args.review == DEFAULT_REVIEW and "--review" not in " ".join(sys.argv):
        logging.warning("No --review specified, using default 'surgical_autonomy'.")

    review = args.review
    db_path = args.db or f"data/{review}/review.db"
    spec_path = args.spec or f"review_specs/{review}_v1.yaml"

    from engine.utils.background import maybe_background
    maybe_background("cloud_extraction", review_name=review)

    if args.progress:
        show_progress(db_path, spec_path)
        return

    if not args.arm:
        parser.error("--arm is required unless using --progress")

    arms = ["openai", "anthropic"] if args.arm == "both" else [args.arm]

    if args.dry_run:
        dry_run(db_path, spec_path, arms)
        return

    for arm in arms:
        run_arm(arm, db_path, spec_path, args.max_papers, args.max_cost)


if __name__ == "__main__":
    main()
