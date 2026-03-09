import sqlite3
import json
import shutil
import re
from pathlib import Path

DB = Path.home() / "projects/evidence-engine/data/surgical_autonomy/review.db"
PDF_DIR = Path.home() / "projects/evidence-engine/data/surgical_autonomy/pdfs"
OUT_DIR = Path.home() / "projects/evidence-engine/data/surgical_autonomy/concordance_pdfs"

OUT_DIR.mkdir(exist_ok=True)

conn = sqlite3.connect(DB)
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
    
    src = PDF_DIR / f"{paper_id}.pdf"
    dst = OUT_DIR / new_name
    
    if src.exists():
        shutil.copy2(src, dst)
        copied += 1
        status = "OK"
    else:
        status = "MISSING"
        missing += 1
    
    manifest.append(f"{ee_id}|{paper_id}|{last_name}|{year}|{row['title'][:80]}|{status}")

# Write manifest CSV
manifest_path = OUT_DIR / "paper_manifest.csv"
with open(manifest_path, "w") as f:
    f.write("ee_id|db_id|first_author|year|title|pdf_status\n")
    for line in manifest:
        f.write(line + "\n")

print(f"Copied:  {copied}")
print(f"Missing: {missing}")
print(f"Output:  {OUT_DIR}")
print(f"Manifest: {manifest_path}")
