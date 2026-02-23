#!/usr/bin/env python3
"""End-to-end test: search PubMed/OpenAlex + dual-pass screening on real data."""

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.agents.screener import run_screening, MODEL as SCREENER_MODEL
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.search.dedup import deduplicate
from engine.search.openalex import search_openalex
from engine.search.pubmed import search_pubmed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("e2e_test")

SPEC_PATH = PROJECT_ROOT / "review_specs" / "surgical_autonomy_v1.yaml"
LIMIT = 20
LOG_PATH = PROJECT_ROOT / "tests" / "e2e_search_screen_log.md"


def main():
    t_start = time.time()
    now = datetime.now(timezone.utc)

    # ── Load spec ────────────────────────────────────────────
    logger.info("Loading spec: %s", SPEC_PATH)
    spec = load_review_spec(SPEC_PATH)

    # ── Search ───────────────────────────────────────────────
    logger.info("Searching PubMed...")
    pm_cits = search_pubmed(spec)
    logger.info("PubMed: %d citations", len(pm_cits))

    logger.info("Searching OpenAlex...")
    oa_cits = search_openalex(spec)
    logger.info("OpenAlex: %d citations", len(oa_cits))

    # ── Dedup ────────────────────────────────────────────────
    dedup_result = deduplicate(pm_cits, oa_cits)
    unique = dedup_result.unique_citations
    logger.info(
        "Dedup: %d unique (%d duplicates removed)",
        len(unique), dedup_result.stats["duplicates_found"],
    )

    # ── Limit + DB ───────────────────────────────────────────
    test_citations = unique[:LIMIT]
    logger.info("Using first %d papers for screening test", len(test_citations))

    db = ReviewDatabase("e2e_test")
    added = db.add_papers(test_citations)
    logger.info("Added %d papers to test DB", added)

    # ── Screen ───────────────────────────────────────────────
    logger.info("Running dual-pass screening on %d papers...", added)
    screen_stats = run_screening(db, spec)
    t_elapsed = time.time() - t_start

    logger.info("Screening results: %s", json.dumps(screen_stats))

    # ── Collect paper details for log ────────────────────────
    paper_details = []
    all_papers = db._conn.execute("SELECT * FROM papers ORDER BY id").fetchall()
    for p in all_papers:
        pid = p["id"]
        decisions = db._conn.execute(
            "SELECT pass_number, decision, rationale FROM screening_decisions "
            "WHERE paper_id = ? ORDER BY pass_number",
            (pid,),
        ).fetchall()

        d1 = dict(decisions[0]) if len(decisions) > 0 else {}
        d2 = dict(decisions[1]) if len(decisions) > 1 else {}

        paper_details.append({
            "id": pid,
            "title": p["title"],
            "status": p["status"],
            "pass1_decision": d1.get("decision", ""),
            "pass1_rationale": d1.get("rationale", ""),
            "pass2_decision": d2.get("decision", ""),
            "pass2_rationale": d2.get("rationale", ""),
        })

    # ── Write log ────────────────────────────────────────────
    _write_log(
        now=now,
        spec=spec,
        pm_count=len(pm_cits),
        oa_count=len(oa_cits),
        dedup_stats=dedup_result.stats,
        screen_stats=screen_stats,
        paper_details=paper_details,
        elapsed=t_elapsed,
    )

    db.close()
    logger.info("Done in %.1fs. Log written to %s", t_elapsed, LOG_PATH)


def _write_log(
    now, spec, pm_count, oa_count, dedup_stats, screen_stats, paper_details, elapsed
):
    lines = [
        f"# E2E Search + Screen Test Log",
        f"",
        f"**Date:** {now.strftime('%Y-%m-%d %H:%M UTC')}",
        f"**Spec:** {spec.title} v{spec.version}",
        f"**Screening model:** {SCREENER_MODEL}",
        f"**Elapsed:** {elapsed:.1f}s",
        f"",
        f"## Search Stats",
        f"| Source | Count |",
        f"|--------|-------|",
        f"| PubMed | {pm_count} |",
        f"| OpenAlex | {oa_count} |",
        f"| Duplicates removed | {dedup_stats['duplicates_found']} |",
        f"| Unique (post-dedup) | {dedup_stats['unique_total']} |",
        f"| Papers screened | {screen_stats['total']} |",
        f"",
        f"## Screening Stats",
        f"| Outcome | Count |",
        f"|---------|-------|",
        f"| Included | {screen_stats['screened_in']} |",
        f"| Excluded | {screen_stats['screened_out']} |",
        f"| Flagged (disagreement) | {screen_stats['flagged']} |",
        f"",
        f"## Paper Details",
        f"| # | Title | Status | Pass 1 | Pass 2 | P1 Rationale |",
        f"|---|-------|--------|--------|--------|-------------|",
    ]

    for p in paper_details:
        title = p["title"][:60] + ("..." if len(p["title"]) > 60 else "")
        lines.append(
            f"| {p['id']} | {title} | {p['status']} | "
            f"{p['pass1_decision']} | {p['pass2_decision']} | "
            f"{p['pass1_rationale'][:60]} |"
        )

    lines.append("")

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(lines))


if __name__ == "__main__":
    main()
