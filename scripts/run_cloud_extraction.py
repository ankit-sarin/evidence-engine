#!/usr/bin/env python3
"""CLI for running cloud extraction arms — opt-in per run, declared in the spec (S3g).

An arm is a spec-declared arm name (R64). `--arm` must name a non-empty subset
of the spec's `cloud.enabled_arms`, or the run refuses before any request; with
no `--arm`, nothing runs and nothing leaves the machine. Each run writes its
manifest — enabled arms, payload description, per-arm configuration — before
the first request (S3a), and every request's full payload is hashed into
`run_calls` (C16).

Usage:
    python scripts/run_cloud_extraction.py --review surgical_autonomy --arm <arm_name>
    python scripts/run_cloud_extraction.py --review surgical_autonomy --arm <a> <b> --max-cost 10.00
    python scripts/run_cloud_extraction.py --review surgical_autonomy --progress
    python scripts/run_cloud_extraction.py --review surgical_autonomy --dry-run --arm <arm_name>
"""

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

from engine.core.review_paths import data_root_for, load_spec_for, spec_path_for
from engine.cloud.anthropic_extractor import AnthropicExtractor
from engine.cloud.base import PAYLOAD_DESCRIPTION, CloudExtractorBase
from engine.cloud.openai_extractor import OpenAIExtractor
from engine.cloud.schema import init_cloud_tables
from engine.core.run_manifest import CloudArmNotEnabled

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

#: Provider -> transport. The ARM is the spec's; this only picks the client.
EXTRACTORS = {"openai": OpenAIExtractor, "anthropic": AnthropicExtractor}


def select_cloud_arms(spec, cli_arms) -> list[str]:
    """S3g: the CLI must agree with the spec or the run refuses — before any
    extractor, client or request exists. No `--arm` selects nothing."""
    requested = list(cli_arms or [])
    if not requested:
        return []
    enabled = list(spec.cloud.enabled_arms)
    outside = [a for a in requested if a not in enabled]
    if outside:
        raise CloudArmNotEnabled(
            f"refused: --arm {outside} not in the spec's cloud.enabled_arms {enabled}. "
            "A cloud arm runs only when the spec enables it for this run (S3g, R6); "
            "nothing was sent.")
    return requested


def _cloud_arm_names(spec, db_path: str) -> list[str]:
    """Every cloud arm with a spec declaration or a stored extraction."""
    names = {a.name for a in spec.arms if a.provider in EXTRACTORS}
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        names |= {r[0] for r in conn.execute("SELECT DISTINCT arm FROM cloud_extractions")}
    finally:
        conn.close()
    return sorted(names)


def show_progress(db_path: str, spec_path: str):
    """Show extraction progress for all arms without calling APIs."""
    init_cloud_tables(db_path)
    # Use base class just for progress queries
    base = CloudExtractorBase(db_path, spec_path)
    for arm in _cloud_arm_names(base.spec, db_path):
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
        pending = base.get_pending_papers(arm_name)
        print(f"\n{arm_name}: {len(pending)} papers pending")
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
    """Run one spec-declared cloud arm under its own run manifest."""
    from engine.core.codebook import CODEBOOK_FILENAME, load_codebook
    from engine.core import run_manifest as rm

    from engine.core.review_spec import load_review_spec
    spec = load_review_spec(spec_path)
    provider = spec.arm(arm_name).provider
    extractor = EXTRACTORS[provider](db_path, spec_path, arm_name=arm_name)
    try:
        codebook = load_codebook(Path(db_path).parent / CODEBOOK_FILENAME)
        handle = rm.open_run(
            extractor._conn, spec, kind="extraction", stages=(), codebook=codebook,
            cloud_arms=[arm_name], payload_description=PAYLOAD_DESCRIPTION)
        extractor.run_id = handle.run_id
        status = "failed"
        try:
            extractor.run(max_papers=max_papers, max_cost_usd=max_cost)
            status = "completed"
        finally:
            rm.close_run(extractor._conn, handle.run_id, status)
    finally:
        extractor.close()


def main():
    parser = argparse.ArgumentParser(
        description="Run cloud extraction arms for concordance study"
    )
    parser.add_argument(
        "--review",
        required=True,
        help="Review id. The review's identity — the spec file and the data root both derive from it.",
    )
    parser.add_argument(
        "--arm",
        nargs="+",
        default=None,
        help="Spec-declared cloud arm name(s); must be in cloud.enabled_arms",
    )
    parser.add_argument(
        "--spec",
        default=None,
        help="Override the Review Spec path. Defaults to review_specs/<review>.yaml; an override must carry the same review_id.",
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

    review = args.review
    # Identity gate. load_spec_for refuses a spec naming a different review,
    # and it runs before anything opens a database (SPEC-AUTH-01).
    spec = load_spec_for(review, args.spec)
    db_path = args.db or str(data_root_for(review) / "review.db")
    spec_path = str(args.spec or spec_path_for(review))

    from engine.utils.background import maybe_background
    maybe_background("cloud_extraction", review_name=review)

    if args.progress:
        show_progress(db_path, spec_path)
        return

    try:
        arms = select_cloud_arms(spec, args.arm)
    except CloudArmNotEnabled as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)
    if not arms:
        print("No cloud arm selected; nothing leaves the machine. "
              f"The spec enables {list(spec.cloud.enabled_arms)}.")
        return

    if args.dry_run:
        dry_run(db_path, spec_path, arms)
        return

    for arm in arms:
        run_arm(arm, db_path, spec_path, args.max_papers, args.max_cost)


if __name__ == "__main__":
    main()
