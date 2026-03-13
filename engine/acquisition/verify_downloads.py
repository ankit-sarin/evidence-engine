"""Verify, rename, and register manually downloaded PDFs.

Scans the PDF directory for downloaded files, matches them to paper records,
validates PDF integrity, renames to canonical EE-{nnn}_{Author}_{Year}.pdf
format, and updates the database.

CLI:
    python -m engine.acquisition.verify_downloads --review surgical_autonomy
    python -m engine.acquisition.verify_downloads --review surgical_autonomy --dry-run
"""

import argparse
import json
import logging
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.core.database import DATA_ROOT, ReviewDatabase

logger = logging.getLogger(__name__)

PDF_MAGIC = b"%PDF"
MIN_PDF_SIZE = 10 * 1024  # 10KB


# ── Filename Helpers ────────────────────────────────────────────────


def _clean_author_name(name: str) -> str:
    """Clean author name for filesystem safety.

    Strips accents, removes non-alphanumeric chars (except hyphen),
    collapses whitespace. Extracts the last name from common formats:
      "Smith J" or "Smith JA" → "Smith" (LastName + initials)
      "John Smith" → "Smith" (First Last)
    """
    if not name:
        return "Unknown"
    # Decompose unicode, strip combining marks (accents)
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Keep only alphanumeric, hyphen, space
    cleaned = re.sub(r"[^a-zA-Z0-9\- ]", "", ascii_name)
    # Collapse whitespace, strip
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    parts = cleaned.split()
    if not parts:
        return "Unknown"

    # If last word is 1-2 chars (likely initials: "Smith J", "Doe AB"),
    # use the first word (the last name)
    if len(parts) > 1 and len(parts[-1]) <= 2:
        return parts[0]
    # Otherwise use the last word (e.g., "John Smith" → "Smith")
    return parts[-1]


def _first_author_last_name(authors_json: str | None) -> str:
    """Extract first author's last name from JSON array."""
    if not authors_json:
        return "Unknown"
    try:
        authors = json.loads(authors_json) if isinstance(authors_json, str) else authors_json
        if authors and isinstance(authors, list) and authors[0]:
            return _clean_author_name(authors[0])
    except (json.JSONDecodeError, IndexError):
        pass
    return "Unknown"


def canonical_filename(ee_identifier: str, authors_json: str | None,
                       year: int | None) -> str:
    """Build canonical PDF filename: EE-{nnn}_{Author}_{Year}.pdf"""
    ee_part = ee_identifier or "EE-000"
    author_part = _first_author_last_name(authors_json)
    year_part = str(year) if year else "XXXX"
    return f"{ee_part}_{author_part}_{year_part}.pdf"


# ── File Matching ───────────────────────────────────────────────────


def _match_file_to_paper(filename: str, paper_index: dict, ee_index: dict
                         ) -> tuple[int | None, str]:
    """Match a PDF filename to a paper_id.

    Returns (paper_id, match_type) or (None, "unmatched").

    Accepts three patterns:
      - Bare integer: 47.pdf → paper_id 47
      - EE-prefixed: EE-047.pdf → look up by ee_identifier
      - Rich name: EE-047_Lukas_2020.pdf → look up by ee_identifier prefix
    """
    stem = Path(filename).stem  # e.g., "47", "EE-047", "EE-047_Lukas_2020"

    # Pattern 1: Bare integer (e.g., 47.pdf)
    if stem.isdigit():
        pid = int(stem)
        if pid in paper_index:
            return pid, "bare_integer"
        return None, "unmatched"

    # Pattern 2/3: EE-prefixed (e.g., EE-047.pdf or EE-047_Lukas_2020.pdf)
    ee_match = re.match(r"^(EE-\d+)", stem)
    if ee_match:
        ee_id = ee_match.group(1)
        if ee_id in ee_index:
            return ee_index[ee_id], "ee_prefixed"
        return None, "unmatched"

    return None, "unmatched"


# ── Validation ──────────────────────────────────────────────────────


def _validate_pdf(path: Path) -> tuple[bool, str]:
    """Validate a PDF file. Returns (is_valid, reason)."""
    if not path.exists():
        return False, "file not found"

    size = path.stat().st_size
    if size == 0:
        return False, "empty file (0 bytes)"

    # Check header first — catches HTML error pages before size check
    try:
        with open(path, "rb") as f:
            header = f.read(4)
    except OSError as e:
        return False, f"read error: {e}"

    if header != PDF_MAGIC:
        # Common case: HTML error page saved as .pdf
        try:
            with open(path, "rb") as f:
                first_bytes = f.read(200).decode("utf-8", errors="replace")
            if "<html" in first_bytes.lower() or "<!doctype" in first_bytes.lower():
                return False, "HTML error page saved as PDF"
        except Exception:
            pass
        return False, f"not a PDF (header: {header!r})"

    if size < MIN_PDF_SIZE:
        return False, f"too small ({size} bytes, minimum {MIN_PDF_SIZE})"

    return True, "valid"


# ── Main Verify/Import ──────────────────────────────────────────────


def verify_downloads(
    review_name: str,
    pdf_dir: str | Path | None = None,
    dry_run: bool = False,
    data_root: str | Path | None = None,
) -> dict:
    """Verify, rename, and register downloaded PDFs.

    Returns summary dict with counts and details.
    """
    effective_root = Path(data_root) if data_root else DATA_ROOT
    db = ReviewDatabase(review_name, data_root=effective_root)
    conn = db._conn

    if pdf_dir is None:
        pdf_dir = effective_root / review_name / "pdfs"
    else:
        pdf_dir = Path(pdf_dir)

    if not pdf_dir.exists():
        print(f"PDF directory not found: {pdf_dir}")
        db.close()
        return {"error": f"PDF directory not found: {pdf_dir}"}

    # Build lookup indices from DB
    all_papers = conn.execute(
        """SELECT id, ee_identifier, title, authors, year, status,
                  download_status, pdf_local_path
           FROM papers
           WHERE status NOT IN ('ABSTRACT_SCREENED_OUT', 'REJECTED')"""
    ).fetchall()

    paper_index = {}  # paper_id → paper row
    ee_index = {}     # ee_identifier → paper_id
    for p in all_papers:
        paper_index[p["id"]] = dict(p)
        if p["ee_identifier"]:
            ee_index[p["ee_identifier"]] = p["id"]

    # Count how many already have valid PDFs
    already_have_pdf = sum(1 for p in all_papers if p["download_status"] == "success")
    total_included = len(all_papers)

    # Scan PDF directory
    pdf_files = sorted(pdf_dir.glob("*.pdf"))

    matched = []       # (path, paper_id, match_type)
    unmatched = []     # (path, reason)
    invalid = []       # (path, paper_id, reason)
    valid = []         # (path, paper_id, canonical_name)
    renamed = []       # (old_path, new_path, paper_id)
    already_canonical = []  # (path, paper_id)

    for pdf_path in pdf_files:
        pid, match_type = _match_file_to_paper(
            pdf_path.name, paper_index, ee_index
        )

        if pid is None:
            unmatched.append((pdf_path, "no matching paper record"))
            continue

        matched.append((pdf_path, pid, match_type))

        # Validate
        is_valid, reason = _validate_pdf(pdf_path)
        if not is_valid:
            invalid.append((pdf_path, pid, reason))
            continue

        # Build canonical name
        paper = paper_index[pid]
        canon = canonical_filename(
            paper["ee_identifier"], paper["authors"], paper["year"]
        )

        if pdf_path.name == canon:
            already_canonical.append((pdf_path, pid))
            valid.append((pdf_path, pid, canon))
        else:
            canonical_path = pdf_dir / canon
            valid.append((pdf_path, pid, canon))
            renamed.append((pdf_path, canonical_path, pid))

    # ── Apply changes (unless dry-run) ──────────────────────────
    now = datetime.now(timezone.utc).isoformat()

    if not dry_run:
        for old_path, new_path, pid in renamed:
            # Handle collision: if target already exists, skip rename
            if new_path.exists() and new_path != old_path:
                logger.warning(
                    "Paper %d: target %s already exists, keeping %s",
                    pid, new_path.name, old_path.name,
                )
                continue

            old_path.rename(new_path)
            logger.info("Renamed: %s → %s", old_path.name, new_path.name)

        # Update database for all valid files
        for path, pid, canon in valid:
            actual_path = pdf_dir / canon
            conn.execute(
                """UPDATE papers
                   SET download_status = 'success',
                       pdf_local_path = ?,
                       acquisition_date = ?
                   WHERE id = ?""",
                (str(actual_path), now, pid),
            )

            # Also update/insert full_text_assets if not present
            existing = conn.execute(
                "SELECT id FROM full_text_assets WHERE paper_id = ?",
                (pid,),
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE full_text_assets SET pdf_path = ? WHERE paper_id = ?",
                    (str(actual_path), pid),
                )
            else:
                conn.execute(
                    """INSERT INTO full_text_assets (paper_id, pdf_path)
                       VALUES (?, ?)""",
                    (pid, str(actual_path)),
                )

        conn.commit()

    # ── Papers still missing ────────────────────────────────────
    verified_pids = {pid for _, pid, _ in valid}
    already_success_pids = {p["id"] for p in all_papers if p["download_status"] == "success"}
    all_covered = verified_pids | already_success_pids
    still_missing = [p for p in all_papers if p["id"] not in all_covered]

    db.close()

    # ── Report ──────────────────────────────────────────────────
    mode_label = "DRY RUN — " if dry_run else ""

    print(f"\n{'='*60}")
    print(f"{mode_label}PDF VERIFY/IMPORT REPORT")
    print(f"{'='*60}")
    print(f"  PDF directory:  {pdf_dir}")
    print(f"  Files scanned:  {len(pdf_files)}")
    print()
    print(f"  Papers matched and verified:     {len(valid)}")
    print(f"  Papers renamed:                  {len(renamed)}")
    print(f"  Papers already in canonical name: {len(already_canonical)}")
    print(f"  Invalid files (need re-download): {len(invalid)}")
    print(f"  Unmatched files:                 {len(unmatched)}")
    print(f"  Papers still missing:            {len(still_missing)}")
    print(f"  Total acquisition progress:      "
          f"{len(all_covered)} of {total_included} papers have valid PDFs")

    if invalid:
        print(f"\n  INVALID FILES ({len(invalid)}):")
        for path, pid, reason in invalid:
            print(f"    {path.name} (paper_id={pid}): {reason}")

    if unmatched:
        print(f"\n  UNMATCHED FILES ({len(unmatched)}):")
        for path, reason in unmatched:
            print(f"    {path.name}: {reason}")

    if renamed and not dry_run:
        print(f"\n  RENAMED ({len(renamed)}):")
        for old_path, new_path, pid in renamed[:20]:
            print(f"    {old_path.name} → {new_path.name}")
        if len(renamed) > 20:
            print(f"    ... and {len(renamed) - 20} more")

    if dry_run:
        print(f"\n  This was a dry run. No files were renamed and no database changes were made.")
        if renamed:
            print(f"\n  WOULD RENAME ({len(renamed)}):")
            for old_path, new_path, pid in renamed[:20]:
                print(f"    {old_path.name} → {new_path.name}")
            if len(renamed) > 20:
                print(f"    ... and {len(renamed) - 20} more")

    print(f"{'='*60}\n")

    return {
        "files_scanned": len(pdf_files),
        "matched": len(matched),
        "valid": len(valid),
        "renamed": len(renamed),
        "already_canonical": len(already_canonical),
        "invalid": len(invalid),
        "invalid_files": [(str(p.name), pid, reason) for p, pid, reason in invalid],
        "unmatched": len(unmatched),
        "unmatched_files": [str(p.name) for p, _ in unmatched],
        "still_missing": len(still_missing),
        "total_included": total_included,
        "total_with_pdf": len(all_covered),
    }


# ── CLI ─────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Verify, rename, and register downloaded PDFs"
    )
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument("--pdf-dir", help="PDF directory (defaults to review's pdfs/ dir)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen without making changes")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    verify_downloads(args.review, pdf_dir=args.pdf_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
