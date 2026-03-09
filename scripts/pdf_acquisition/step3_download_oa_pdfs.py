"""Step 3: Download open-access PDFs for Priority 1 and 2 papers.

Reads Unpaywall results, downloads PDFs that have a URL, validates
the magic bytes, and logs results.
"""

import csv
import sys
import time
from pathlib import Path

import requests

# ── Paths ─────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RESULTS_CSV = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdf_acquisition" / "unpaywall_results.csv"
LOG_CSV = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdf_acquisition" / "download_log.csv"
PDF_DIR = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdfs"

USER_AGENT = "SurgicalEvidenceEngine/1.0 (mailto:axsarin@health.ucdavis.edu)"
DOWNLOAD_DELAY = 2.0
REQUEST_TIMEOUT = 30
PDF_MAGIC = b"%PDF"


def load_download_targets() -> list[dict]:
    """Load papers that have a pdf_url (all priorities)."""
    with open(RESULTS_CSV, newline="") as f:
        reader = csv.DictReader(f)
        return [r for r in reader if r["pdf_url"]]


def is_valid_pdf(path: Path) -> bool:
    """Check if file starts with %PDF magic bytes."""
    try:
        with open(path, "rb") as f:
            header = f.read(8)
        return header[:4] == PDF_MAGIC
    except OSError:
        return False


def download_pdf(url: str, dest: Path) -> tuple[str, int]:
    """Download a single PDF. Returns (status, file_size_kb)."""
    headers = {"User-Agent": USER_AGENT}
    try:
        resp = requests.get(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        resp.raise_for_status()
    except requests.Timeout:
        return "timeout", 0
    except requests.RequestException as exc:
        return f"failed:{exc}", 0

    dest.write_bytes(resp.content)
    size_kb = len(resp.content) // 1024

    if not is_valid_pdf(dest):
        dest.unlink()
        return "not_pdf", size_kb

    return "success", size_kb


def main():
    if not RESULTS_CSV.exists():
        print(f"ERROR: {RESULTS_CSV} not found. Run step2 first.")
        sys.exit(1)

    PDF_DIR.mkdir(parents=True, exist_ok=True)

    targets = load_download_targets()
    print(f"Download targets: {len(targets)} papers (P1+P2 with PDF URL)")

    # ── Download loop ─────────────────────────────────────────
    log_rows: list[dict] = []
    total = len(targets)

    for i, r in enumerate(targets, 1):
        paper_id = r["paper_id"]
        dest = PDF_DIR / f"{paper_id}.pdf"
        title_short = r["title"][:70]

        # Skip if already downloaded
        if dest.exists() and is_valid_pdf(dest):
            size_kb = dest.stat().st_size // 1024
            print(f"  [{i}/{total}] SKIP (exists) paper {paper_id}: {title_short}...")
            log_rows.append({
                "paper_id": paper_id,
                "title": r["title"],
                "doi": r["doi"],
                "priority": r["priority"],
                "status": "skipped_exists",
                "file_size_kb": size_kb,
                "pdf_url": r["pdf_url"],
            })
            continue

        print(f"  [{i}/{total}] Downloading paper {paper_id}: {title_short}...")
        status, size_kb = download_pdf(r["pdf_url"], dest)

        log_rows.append({
            "paper_id": paper_id,
            "title": r["title"],
            "doi": r["doi"],
            "priority": r["priority"],
            "status": status,
            "file_size_kb": size_kb,
            "pdf_url": r["pdf_url"],
        })

        if status == "success":
            print(f"           ✓ {size_kb} KB")
        else:
            print(f"           ✗ {status}")

        if i < total:
            time.sleep(DOWNLOAD_DELAY)

    # ── Save log ──────────────────────────────────────────────
    log_cols = ["paper_id", "title", "doi", "priority", "status", "file_size_kb", "pdf_url"]
    with open(LOG_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=log_cols)
        writer.writeheader()
        writer.writerows(log_rows)
    print(f"\nDownload log saved → {LOG_CSV}")

    # ── Summary ───────────────────────────────────────────────
    print()
    print("=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)

    for pri in (1, 2, 3):
        pri_rows = [r for r in log_rows if int(r["priority"]) == pri]
        if not pri_rows:
            continue
        success = sum(1 for r in pri_rows if r["status"] in ("success", "skipped_exists"))
        failed = sum(1 for r in pri_rows if r["status"].startswith("failed:"))
        not_pdf = sum(1 for r in pri_rows if r["status"] == "not_pdf")
        timed_out = sum(1 for r in pri_rows if r["status"] == "timeout")

        label = {1: "SCREENED_IN", 2: "validation sample", 3: "remaining excluded"}[pri]
        print(f"\n── Priority {pri} ({label}) — {len(pri_rows)} attempted ──")
        print(f"  Valid PDFs downloaded:  {success}")
        if failed:
            print(f"  Failed downloads:       {failed}")
        if not_pdf:
            print(f"  Not a PDF (paywall?):   {not_pdf}")
        if timed_out:
            print(f"  Timed out:              {timed_out}")

    all_success = sum(1 for r in log_rows if r["status"] in ("success", "skipped_exists"))
    all_failed = sum(1 for r in log_rows if r["status"] not in ("success", "skipped_exists"))
    print(f"\n── Totals ──")
    print(f"  Valid PDFs:   {all_success}/{total}")
    print(f"  Failures:     {all_failed}/{total}")


if __name__ == "__main__":
    main()
