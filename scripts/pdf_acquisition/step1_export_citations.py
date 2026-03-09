"""Step 1: Export citations from review DB for PDF acquisition.

Reads all papers from the SQLite database and exports a CSV with
identifiers needed for downloading PDFs (DOI, PMID, title).
"""

import csv
import sqlite3
import sys
from pathlib import Path

# Resolve paths relative to project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "surgical_autonomy" / "review.db"
OUTPUT_DIR = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdf_acquisition"
OUTPUT_CSV = OUTPUT_DIR / "citations_for_download.csv"

CSV_COLUMNS = ["paper_id", "title", "doi", "pmid", "source", "status"]


def main():
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT id, title, doi, pmid, source, status FROM papers ORDER BY id"
    ).fetchall()
    conn.close()

    total = len(rows)
    if total == 0:
        print("No papers found in the database.")
        print("Run the search pipeline first:")
        print("  python scripts/run_pipeline.py --spec review_specs/surgical_autonomy_v1.yaml --name surgical_autonomy")
        sys.exit(0)

    # Write CSV
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "paper_id": r["id"],
                "title": r["title"],
                "doi": r["doi"] or "",
                "pmid": r["pmid"] or "",
                "source": r["source"],
                "status": r["status"],
            })

    # Compute stats
    has_doi = sum(1 for r in rows if r["doi"])
    has_pmid = sum(1 for r in rows if r["pmid"])
    has_both = sum(1 for r in rows if r["doi"] and r["pmid"])
    has_neither = sum(1 for r in rows if not r["doi"] and not r["pmid"])

    # Status breakdown
    status_counts: dict[str, int] = {}
    for r in rows:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1

    print(f"Exported {total} citations → {OUTPUT_CSV}")
    print()
    print("── Identifier Coverage ─────────────────────")
    print(f"  Total papers:       {total}")
    print(f"  With DOI:           {has_doi}")
    print(f"  With PMID:          {has_pmid}")
    print(f"  With both:          {has_both}")
    print(f"  With neither:       {has_neither}  ← need manual lookup")
    print()
    print("── Status Breakdown ────────────────────────")
    for status, count in sorted(status_counts.items()):
        print(f"  {status:<20s} {count}")


if __name__ == "__main__":
    main()
