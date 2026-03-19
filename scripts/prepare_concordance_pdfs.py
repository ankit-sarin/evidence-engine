import argparse
import json
import logging
import re
import shutil
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DEFAULT_REVIEW = "surgical_autonomy"


def main():
    parser = argparse.ArgumentParser(description="Prepare concordance PDFs with EE-NNN naming")
    parser.add_argument("--review", default=DEFAULT_REVIEW, help=f"Review name (default: {DEFAULT_REVIEW})")
    args = parser.parse_args()

    if args.review == DEFAULT_REVIEW and "--review" not in " ".join(sys.argv):
        logging.warning("No --review specified, using default 'surgical_autonomy'.")

    review = args.review
    base = Path.home() / "projects/evidence-engine/data" / review
    db_path = base / "review.db"
    pdf_dir = base / "pdfs"
    out_dir = base / "concordance_pdfs"

    out_dir.mkdir(exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, authors, year, title FROM papers WHERE status != 'SCREENED_OUT' ORDER BY id"
    ).fetchall()

    print(f"Found {len(rows)} included papers\n")

    manifest = []
    copied = 0
    missing = 0

    for seq, row in enumerate(rows, 1):
        paper_id = row["id"]
        year = row["year"] or "XXXX"

        # Extract first author last name
        try:
            authors = json.loads(row["authors"])
            last_name = authors[0].split()[0].rstrip(",")
        except (json.JSONDecodeError, IndexError, TypeError):
            last_name = "Unknown"

        # Clean last name for filename
        last_name = re.sub(r'[^\w-]', '', last_name)

        ee_id = f"EE-{seq:03d}"
        new_name = f"{ee_id}_{last_name}_{year}.pdf"

        src = pdf_dir / f"{paper_id}.pdf"
        dst = out_dir / new_name

        if src.exists():
            shutil.copy2(src, dst)
            copied += 1
            status = "OK"
        else:
            status = "MISSING"
            missing += 1

        manifest.append(f"{ee_id}|{paper_id}|{last_name}|{year}|{row['title'][:80]}|{status}")

    # Write manifest CSV
    manifest_path = out_dir / "paper_manifest.csv"
    with open(manifest_path, "w") as f:
        f.write("ee_id|db_id|first_author|year|title|pdf_status\n")
        for line in manifest:
            f.write(line + "\n")

    print(f"Copied:  {copied}")
    print(f"Missing: {missing}")
    print(f"Output:  {out_dir}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
