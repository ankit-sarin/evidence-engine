#!/usr/bin/env python3
"""Screen net-new papers from expanded search without modifying review.db.

Two-phase design:
  Phase 1 — Fetch abstracts from OpenAlex/PubMed, save to abstracts.jsonl
            (one line per paper, crash-safe with per-paper append).
  Phase 2 — Dual-pass Qwen3:8b screening from local abstracts.jsonl,
            writes screening_results.csv with checkpoint.

Usage:
    python scripts/screen_expanded.py              # run both phases
    python scripts/screen_expanded.py --fetch-only # phase 1 only
    python scripts/screen_expanded.py --screen-only # phase 2 only
"""

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path

import requests
from Bio import Entrez, Medline

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.agents.screener import screen_paper
from engine.core.review_spec import load_review_spec

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("screen_expanded")

Entrez.email = "ankit.sarin@ucdavis.edu"
OPENALEX_HEADERS = {"User-Agent": "mailto:ankit.sarin@ucdavis.edu"}

STAGING_DIR = Path("data/surgical_autonomy/expanded_search")
INPUT_CSV = STAGING_DIR / "net_new_papers.csv"
ABSTRACTS_JSONL = STAGING_DIR / "abstracts.jsonl"
OUTPUT_CSV = STAGING_DIR / "screening_results.csv"
SCREENING_PROGRESS = STAGING_DIR / "screening_progress.json"


# ── Phase 1: Abstract fetching ───────────────────────────────────────


def fetch_abstract_openalex(doi: str) -> str | None:
    """Fetch abstract from OpenAlex by DOI."""
    if not doi:
        return None
    try:
        r = requests.get(
            f"https://api.openalex.org/works/doi:{doi}",
            headers=OPENALEX_HEADERS,
            timeout=10,
        )
        if r.status_code != 200:
            return None
        work = r.json()
        inv_idx = work.get("abstract_inverted_index")
        if not inv_idx:
            return None
        from pyalex import invert_abstract
        return invert_abstract(inv_idx)
    except Exception:
        return None


def fetch_abstract_pubmed(pmid: str) -> str | None:
    """Fetch abstract from PubMed by PMID."""
    if not pmid:
        return None
    try:
        time.sleep(0.35)
        handle = Entrez.efetch(
            db="pubmed", id=pmid, rettype="medline", retmode="text"
        )
        records = list(Medline.parse(handle))
        handle.close()
        if records:
            return records[0].get("AB")
    except Exception:
        pass
    return None


def paper_key(row: dict) -> str:
    """Stable unique key for a paper row."""
    return row.get("doi") or row.get("pmid") or row["title"]


def run_fetch_phase():
    """Phase 1: fetch abstracts and append to abstracts.jsonl."""
    # Load input papers
    papers = []
    with open(INPUT_CSV, newline="") as f:
        for row in csv.DictReader(f):
            papers.append(row)
    total = len(papers)

    # Load already-fetched keys
    fetched_keys: set[str] = set()
    if ABSTRACTS_JSONL.exists():
        with open(ABSTRACTS_JSONL) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    fetched_keys.add(rec["key"])
                except (json.JSONDecodeError, KeyError):
                    continue
    logger.info(
        "Phase 1: %d papers, %d already fetched, %d remaining",
        total, len(fetched_keys), total - len(fetched_keys),
    )

    if len(fetched_keys) >= total:
        logger.info("All abstracts already fetched — skipping phase 1")
        return

    t_start = time.time()
    fetched_count = 0

    with open(ABSTRACTS_JSONL, "a") as out:
        for i, row in enumerate(papers, 1):
            key = paper_key(row)
            if key in fetched_keys:
                continue

            doi = row.get("doi") or ""
            pmid = row.get("pmid") or ""

            abstract = fetch_abstract_openalex(doi)
            if abstract is None:
                abstract = fetch_abstract_pubmed(pmid)

            rec = {
                "key": key,
                "doi": doi,
                "pmid": pmid,
                "title": row["title"],
                "year": row.get("year", ""),
                "journal": row.get("journal", ""),
                "source": row.get("source", ""),
                "abstract": abstract,
            }
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out.flush()
            fetched_keys.add(key)
            fetched_count += 1

            if fetched_count % 200 == 0:
                elapsed = time.time() - t_start
                rate = fetched_count / elapsed if elapsed > 0 else 0
                remaining = total - len(fetched_keys)
                eta_min = remaining / rate / 60 if rate > 0 else 0
                has_abs = 0  # quick count from this batch
                logger.info(
                    "Fetched %d/%d (%.1f/s) — %d remaining — ETA %.0f min",
                    len(fetched_keys), total, rate, remaining, eta_min,
                )

    elapsed = time.time() - t_start
    logger.info(
        "Phase 1 complete: fetched %d new abstracts in %.1f min (%d total on disk)",
        fetched_count, elapsed / 60, len(fetched_keys),
    )


# ── Phase 2: Screening ──────────────────────────────────────────────


def run_screen_phase():
    """Phase 2: dual-pass screening from local abstracts.jsonl."""
    if not ABSTRACTS_JSONL.exists():
        logger.error("No abstracts.jsonl found — run phase 1 first")
        sys.exit(1)

    spec = load_review_spec("review_specs/surgical_autonomy_v1.yaml")

    # Load all abstracts from disk
    papers: list[dict] = []
    with open(ABSTRACTS_JSONL) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                papers.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    total = len(papers)
    logger.info("Phase 2: loaded %d papers from %s", total, ABSTRACTS_JSONL)

    # Load screening progress
    completed: dict[str, str] = {}
    if SCREENING_PROGRESS.exists():
        with open(SCREENING_PROGRESS) as f:
            completed = json.load(f)
        logger.info("Resuming from %d already screened", len(completed))

    # Open output CSV
    write_header = not OUTPUT_CSV.exists() or len(completed) == 0
    outfile = open(OUTPUT_CSV, "a" if not write_header else "w", newline="")
    writer = csv.writer(outfile)
    if write_header:
        writer.writerow([
            "title", "doi", "pmid", "year", "journal", "source",
            "has_abstract", "screening_decision",
            "pass1_decision", "pass1_rationale", "pass1_confidence",
            "pass2_decision", "pass2_rationale", "pass2_confidence",
        ])

    stats = {"include": 0, "exclude": 0, "flagged": 0, "errors": 0}
    t_start = time.time()
    screened_this_run = 0

    for paper in papers:
        key = paper["key"]
        if key in completed:
            continue

        paper_dict = {
            "title": paper["title"],
            "abstract": paper.get("abstract"),
        }

        try:
            d1 = screen_paper(paper_dict, spec, pass_number=1)
            d2 = screen_paper(paper_dict, spec, pass_number=2)
        except Exception as exc:
            logger.warning("Error screening '%s': %s", paper["title"][:60], exc)
            stats["errors"] += 1
            completed[key] = "error"
            continue

        if d1.decision == "include" and d2.decision == "include":
            decision = "include"
            stats["include"] += 1
        elif d1.decision == "exclude" and d2.decision == "exclude":
            decision = "exclude"
            stats["exclude"] += 1
        else:
            decision = "flagged"
            stats["flagged"] += 1

        writer.writerow([
            paper["title"], paper.get("doi", ""), paper.get("pmid", ""),
            paper.get("year", ""), paper.get("journal", ""), paper.get("source", ""),
            "yes" if paper.get("abstract") else "no", decision,
            d1.decision, d1.rationale, d1.confidence,
            d2.decision, d2.rationale, d2.confidence,
        ])
        outfile.flush()

        completed[key] = decision
        screened_this_run += 1

        if screened_this_run % 50 == 0:
            with open(SCREENING_PROGRESS, "w") as pf:
                json.dump(completed, pf)

            elapsed = time.time() - t_start
            rate = screened_this_run / elapsed if elapsed > 0 else 0
            remaining = total - len(completed)
            eta_min = remaining / rate / 60 if rate > 0 else 0
            logger.info(
                "Screened %d/%d (%.1f/s) — %d in, %d out, %d flagged, "
                "%d errors — ETA %.0f min",
                len(completed), total, rate, stats["include"],
                stats["exclude"], stats["flagged"], stats["errors"],
                eta_min,
            )

    outfile.close()

    # Final progress save
    with open(SCREENING_PROGRESS, "w") as pf:
        json.dump(completed, pf)

    elapsed = time.time() - t_start
    logger.info(
        "Phase 2 complete in %.1f min: %d include, %d exclude, %d flagged, "
        "%d errors (of %d total)",
        elapsed / 60, stats["include"], stats["exclude"],
        stats["flagged"], stats["errors"], total,
    )


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Screen expanded search results (two-phase)"
    )
    parser.add_argument(
        "--fetch-only", action="store_true",
        help="Run phase 1 only (fetch abstracts)",
    )
    parser.add_argument(
        "--screen-only", action="store_true",
        help="Run phase 2 only (screen from local abstracts)",
    )
    args = parser.parse_args()

    if args.fetch_only:
        run_fetch_phase()
    elif args.screen_only:
        run_screen_phase()
    else:
        run_fetch_phase()
        run_screen_phase()


if __name__ == "__main__":
    main()
