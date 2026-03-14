"""Backfill missing author metadata for expanded-corpus papers.

Queries PubMed (by PMID batch) and OpenAlex (by DOI) for papers that have
authors='[]' in the DB. Updates the authors column with JSON arrays.

Usage:
    PYTHONPATH=. python scripts/backfill_authors.py [--dry-run]
"""

import json
import logging
import sqlite3
import sys
import time

from Bio import Entrez, Medline

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "data/surgical_autonomy/review.db"
Entrez.email = "ankit.sarin@ucdavis.edu"

BATCH_SIZE = 200  # PubMed efetch batch size
RATE_LIMIT = 0.34  # NCBI rate limit


def _efetch_authors(pmids: list[str]) -> dict[str, list[str]]:
    """Fetch author lists from PubMed for a batch of PMIDs.

    Returns {pmid: [author_name, ...]}
    """
    result = {}
    for i in range(0, len(pmids), BATCH_SIZE):
        batch = pmids[i : i + BATCH_SIZE]
        logger.info("  PubMed efetch batch %d-%d of %d", i + 1, i + len(batch), len(pmids))
        handle = Entrez.efetch(
            db="pubmed", id=",".join(batch), rettype="medline", retmode="text"
        )
        records = list(Medline.parse(handle))
        handle.close()
        for rec in records:
            pid = rec.get("PMID", "")
            authors = rec.get("AU", [])
            if pid and authors:
                result[pid] = authors
        if i + BATCH_SIZE < len(pmids):
            time.sleep(RATE_LIMIT)
    return result


def _openalex_author_lookup(doi: str) -> list[str] | None:
    """Look up authors from OpenAlex by DOI."""
    try:
        import pyalex

        pyalex.config.email = "ankit.sarin@ucdavis.edu"
        works = pyalex.Works().filter(doi=doi).get()
        if works:
            work = works[0]
            authors = []
            for authorship in work.get("authorships") or []:
                author = authorship.get("author") or {}
                name = author.get("display_name")
                if name:
                    authors.append(name)
            return authors if authors else None
    except Exception as e:
        logger.debug("OpenAlex lookup failed for DOI %s: %s", doi, e)
    return None


def main():
    dry_run = "--dry-run" in sys.argv

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Find all papers with empty authors that are screened in
    rows = conn.execute(
        """SELECT id, pmid, doi, source, title
           FROM papers
           WHERE (authors IS NULL OR authors = '' OR authors = '[]')
           AND status IN ('ABSTRACT_SCREENED_IN', 'AI_AUDIT_COMPLETE',
                          'PDF_ACQUIRED', 'PARSED', 'FT_ELIGIBLE',
                          'EXTRACTED', 'HUMAN_AUDIT_COMPLETE')
           ORDER BY id"""
    ).fetchall()

    logger.info("Found %d papers with missing authors", len(rows))

    if not rows:
        conn.close()
        return

    # Split by lookup strategy
    pubmed_papers = [(r["id"], r["pmid"]) for r in rows if r["pmid"]]
    doi_only_papers = [(r["id"], r["doi"]) for r in rows if not r["pmid"] and r["doi"]]

    logger.info("  %d have PMIDs (PubMed batch fetch)", len(pubmed_papers))
    logger.info("  %d have DOI only (OpenAlex lookup)", len(doi_only_papers))

    # ── Step 1: PubMed batch fetch ──
    updated = 0
    still_missing = []
    pmid_to_id = {pmid: pid for pid, pmid in pubmed_papers}
    pmids = [pmid for _, pmid in pubmed_papers]

    if pmids:
        logger.info("\n[Step 1] Fetching authors from PubMed for %d papers...", len(pmids))
        pmid_authors = _efetch_authors(pmids)
        logger.info("  Got authors for %d / %d PMIDs", len(pmid_authors), len(pmids))

        for pmid, authors in pmid_authors.items():
            pid = pmid_to_id.get(pmid)
            if pid and authors:
                authors_json = json.dumps(authors)
                if not dry_run:
                    conn.execute(
                        "UPDATE papers SET authors = ?, updated_at = datetime('now') WHERE id = ?",
                        (authors_json, pid),
                    )
                updated += 1

        # PMIDs that PubMed didn't return authors for — try OpenAlex
        for pmid, pid in pmid_to_id.items():
            if pmid not in pmid_authors:
                # Find the DOI for this paper
                doi = conn.execute("SELECT doi FROM papers WHERE id = ?", (pid,)).fetchone()
                if doi and doi["doi"]:
                    doi_only_papers.append((pid, doi["doi"]))
                else:
                    still_missing.append(pid)

    # ── Step 2: OpenAlex DOI lookup ──
    if doi_only_papers:
        logger.info(
            "\n[Step 2] Fetching authors from OpenAlex for %d papers...", len(doi_only_papers)
        )
        oa_found = 0
        for i, (pid, doi) in enumerate(doi_only_papers):
            if (i + 1) % 50 == 0:
                logger.info("  OpenAlex progress: %d / %d", i + 1, len(doi_only_papers))
            authors = _openalex_author_lookup(doi)
            if authors:
                authors_json = json.dumps(authors)
                if not dry_run:
                    conn.execute(
                        "UPDATE papers SET authors = ?, updated_at = datetime('now') WHERE id = ?",
                        (authors_json, pid),
                    )
                updated += 1
                oa_found += 1
            else:
                still_missing.append(pid)
            time.sleep(0.1)  # polite rate limiting
        logger.info("  OpenAlex found authors for %d / %d", oa_found, len(doi_only_papers))

    if not dry_run:
        conn.commit()

    # ── Report ──
    print("\n" + "=" * 60)
    print("AUTHOR BACKFILL REPORT")
    print("=" * 60)
    print(f"  Total papers checked:  {len(rows)}")
    print(f"  Authors backfilled:    {updated}")
    print(f"  Still missing:         {len(still_missing)}")
    if dry_run:
        print("  (DRY RUN — no changes written)")

    if still_missing:
        print(f"\n  STILL MISSING ({len(still_missing)} papers):")
        for pid in still_missing:
            row = conn.execute(
                "SELECT id, ee_identifier, doi, pmid, substr(title,1,70) as t FROM papers WHERE id = ?",
                (pid,),
            ).fetchone()
            print(f"    id={row['id']} ee={row['ee_identifier']} doi={row['doi']} title={row['t']}")

    conn.close()


if __name__ == "__main__":
    main()
