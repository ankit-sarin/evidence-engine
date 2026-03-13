"""Download PDFs for papers with OA PDF URLs.

Validates %PDF magic bytes. Includes MDPI/Cloudflare retry logic
(DOI redirect with Accept: application/pdf, /pdf and /pdfdirect suffixes).
Also tries PMC OA packages and IEEE stamp-page scraping.

Idempotent: skips papers with existing valid PDFs.
2-second delay between downloads.

CLI:
    python -m engine.acquisition.download --review surgical_autonomy
"""

import argparse
import io
import logging
import re
import sys
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.core.database import DATA_ROOT, ReviewDatabase

logger = logging.getLogger(__name__)

USER_AGENT = "SurgicalEvidenceEngine/1.0 (mailto:axsarin@health.ucdavis.edu)"
BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
DOWNLOAD_DELAY = 2.0
REQUEST_TIMEOUT = 30
PDF_MAGIC = b"%PDF"


# ── Helpers ─────────────────────────────────────────────────────


def is_valid_pdf(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(4) == PDF_MAGIC
    except OSError:
        return False


def _is_pdf_bytes(data: bytes) -> bool:
    return len(data) >= 4 and data[:4] == PDF_MAGIC


def _save_if_pdf(data: bytes, dest: Path) -> tuple[str, int]:
    if not _is_pdf_bytes(data):
        return "not_pdf", len(data) // 1024
    dest.write_bytes(data)
    return "success", len(data) // 1024


# ── Strategy: Direct URL download ──────────────────────────────


def _download_direct(url: str, dest: Path) -> tuple[str, int]:
    """Download from a direct PDF URL."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
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


# ── Strategy: DOI redirect with content negotiation ────────────


def _download_via_doi_redirect(doi: str, dest: Path) -> tuple[str, int]:
    """Follow DOI redirect, try Accept: application/pdf, then /pdf suffixes."""
    # Attempt 1: content negotiation
    try:
        resp = requests.get(
            f"https://doi.org/{doi}",
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": USER_AGENT, "Accept": "application/pdf"},
        )
        resp.raise_for_status()
        if _is_pdf_bytes(resp.content):
            return _save_if_pdf(resp.content, dest)
    except requests.RequestException:
        pass

    # Attempt 2: landing page + /pdf or /pdfdirect suffix
    try:
        resp = requests.get(
            f"https://doi.org/{doi}",
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
        resp.raise_for_status()
        landing = resp.url.rstrip("/")

        for suffix in ("/pdf", "/pdfdirect"):
            try:
                resp2 = requests.get(
                    landing + suffix,
                    timeout=REQUEST_TIMEOUT,
                    allow_redirects=True,
                    headers={"User-Agent": USER_AGENT},
                )
                if resp2.status_code == 200 and _is_pdf_bytes(resp2.content):
                    return _save_if_pdf(resp2.content, dest)
            except requests.RequestException:
                continue
    except requests.RequestException:
        pass

    return "doi_redirect_failed", 0


# ── Strategy: PMC OA package ───────────────────────────────────


def _lookup_pmcid(doi: str) -> str | None:
    url = (
        f"https://www.ebi.ac.uk/europepmc/webservices/rest/search"
        f"?query=DOI:{quote(doi, safe='')}&format=json"
    )
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        results = resp.json().get("resultList", {}).get("result", [])
        if results:
            return results[0].get("pmcid") or None
    except requests.RequestException:
        pass
    return None


def _download_via_pmc(doi: str, dest: Path) -> tuple[str, int]:
    pmcid = _lookup_pmcid(doi)
    if not pmcid:
        return "no_pmcid", 0

    oa_url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={pmcid}"
    try:
        resp = requests.get(oa_url, timeout=15)
        resp.raise_for_status()
        match = re.search(r'href="(ftp://[^"]+\.tar\.gz)"', resp.text)
        if not match:
            return "no_oa_package", 0
        pkg_url = match.group(1).replace(
            "ftp://ftp.ncbi.nlm.nih.gov", "https://ftp.ncbi.nlm.nih.gov"
        )
        resp2 = requests.get(pkg_url, timeout=60, headers={"User-Agent": USER_AGENT})
        resp2.raise_for_status()
        tar = tarfile.open(fileobj=io.BytesIO(resp2.content), mode="r:gz")
        pdf_members = [m for m in tar.getnames() if m.endswith(".pdf")]
        if not pdf_members:
            return "no_pdf_in_archive", 0
        content = tar.extractfile(pdf_members[0]).read()
        return _save_if_pdf(content, dest)
    except (requests.RequestException, tarfile.TarError) as exc:
        return f"pmc_error:{exc}", 0


# ── Strategy: IEEE stamp page ──────────────────────────────────


def _download_via_ieee(doi: str, dest: Path) -> tuple[str, int]:
    try:
        resp = requests.get(
            f"https://doi.org/{doi}",
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        return f"ieee_landing_error:{exc}", 0

    m = re.search(r"/document/(\d+)", resp.url)
    if not m:
        return "no_arnumber", 0
    arnumber = m.group(1)

    stamp_url = f"https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber={arnumber}"
    try:
        resp2 = requests.get(
            stamp_url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
            headers={"User-Agent": BROWSER_UA},
        )
        resp2.raise_for_status()
    except requests.RequestException as exc:
        return f"ieee_stamp_error:{exc}", 0

    iframe_match = re.search(
        r'<iframe[^>]+src=["\']([^"\']+ielx[^"\']+\.pdf[^"\']*)["\']',
        resp2.text, re.I,
    )
    if not iframe_match:
        return "no_iframe_pdf", 0

    pdf_url = iframe_match.group(1)
    if not pdf_url.startswith("http"):
        pdf_url = f"https://ieeexplore.ieee.org{pdf_url}"

    try:
        resp3 = requests.get(
            pdf_url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
            headers={"User-Agent": BROWSER_UA, "Referer": stamp_url},
        )
        resp3.raise_for_status()
        return _save_if_pdf(resp3.content, dest)
    except requests.RequestException as exc:
        return f"ieee_pdf_error:{exc}", 0


# ── Strategy: MDPI URL construction ───────────────────────────


def _download_via_mdpi(doi: str, dest: Path) -> tuple[str, int]:
    """Construct MDPI PDF URL from DOI redirect landing page."""
    try:
        resp = requests.get(
            f"https://doi.org/{doi}",
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        return f"mdpi_landing_error:{exc}", 0

    landing = resp.url.rstrip("/")

    # MDPI article URLs look like https://www.mdpi.com/XXXX-XXXX/vol/issue/article
    # PDF is at /pdf suffix
    for suffix in ("/pdf", ""):
        pdf_url = landing + suffix
        if not suffix:
            # Try the explicit /pdf download endpoint
            pdf_url = landing.replace("/htm", "/pdf") if "/htm" in landing else landing
            continue
        try:
            resp2 = requests.get(
                pdf_url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
                headers={"User-Agent": USER_AGENT},
            )
            if resp2.status_code == 200 and _is_pdf_bytes(resp2.content):
                return _save_if_pdf(resp2.content, dest)
        except requests.RequestException:
            continue

    return "mdpi_no_pdf", 0


# ── Main download orchestrator ─────────────────────────────────


def _download_one(paper: dict, dest: Path) -> tuple[str, int, str]:
    """Try all strategies for a single paper. Returns (status, size_kb, strategy)."""
    doi = paper["doi"] or ""
    pdf_url = paper["pdf_url"] or ""

    # Strategy 1: Direct Unpaywall URL
    if pdf_url:
        status, size_kb = _download_direct(pdf_url, dest)
        if status == "success":
            return status, size_kb, "unpaywall"

    if not doi:
        return "no_doi", 0, "none"

    # Strategy 2: PMC OA package
    status, size_kb = _download_via_pmc(doi, dest)
    if status == "success":
        return status, size_kb, "pmc"

    # Strategy 3: IEEE stamp (if applicable)
    if doi.startswith("10.1109"):
        status, size_kb = _download_via_ieee(doi, dest)
        if status == "success":
            return status, size_kb, "ieee"

    # Strategy 4: MDPI URL construction (if applicable)
    if "mdpi" in doi.lower():
        status, size_kb = _download_via_mdpi(doi, dest)
        if status == "success":
            return status, size_kb, "mdpi"

    # Strategy 5: DOI redirect with content negotiation
    status, size_kb = _download_via_doi_redirect(doi, dest)
    return status, size_kb, "doi_redirect" if status == "success" else "none"


def download_papers(review_name: str, *, retry_failed: bool = False) -> dict:
    """Download PDFs for all included papers with PDF URLs or DOIs.

    Returns summary stats dict.
    """
    db = ReviewDatabase(review_name)
    conn = db._conn
    pdf_dir = DATA_ROOT / review_name / "pdfs"
    pdf_dir.mkdir(parents=True, exist_ok=True)

    # Find download targets
    if retry_failed:
        # Retry papers that failed before
        papers = conn.execute(
            """SELECT id, doi, title, pdf_url, download_status, pdf_local_path
               FROM papers
               WHERE status NOT IN ('ABSTRACT_SCREENED_OUT', 'REJECTED')
                 AND download_status = 'failed'
               ORDER BY id"""
        ).fetchall()
    else:
        # Papers with PDF URL or DOI, not yet successfully downloaded
        papers = conn.execute(
            """SELECT id, doi, title, pdf_url, download_status, pdf_local_path
               FROM papers
               WHERE status NOT IN ('ABSTRACT_SCREENED_OUT', 'REJECTED')
                 AND download_status != 'success'
               ORDER BY id"""
        ).fetchall()

    # Filter out papers that already have valid PDFs on disk
    targets = []
    skipped = 0
    for p in papers:
        dest = pdf_dir / f"{p['id']}.pdf"
        if dest.exists() and is_valid_pdf(dest):
            # Already have it — update DB
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                """UPDATE papers SET download_status = 'success',
                   pdf_local_path = ?, acquisition_date = ?
                   WHERE id = ?""",
                (str(dest), now, p["id"]),
            )
            skipped += 1
        else:
            targets.append(dict(p))
    conn.commit()

    total = len(targets)
    print(f"PDF download — {review_name}")
    print(f"  Download targets:   {total}")
    print(f"  Already on disk:    {skipped}")
    print()

    if total == 0:
        print("Nothing to download.")
        db.close()
        return {"downloaded": 0, "skipped": skipped, "failed": 0}

    now = datetime.now(timezone.utc).isoformat()
    downloaded = 0
    failed = 0
    strategy_counts: dict[str, int] = {}

    for i, paper in enumerate(targets, 1):
        pid = paper["id"]
        dest = pdf_dir / f"{pid}.pdf"
        title_short = paper["title"][:60]
        doi_short = (paper["doi"] or "no-doi")[:35]

        print(f"  [{i}/{total}] {doi_short:35s}  {title_short}...", end="", flush=True)

        status, size_kb, strategy = _download_one(paper, dest)

        if status == "success":
            downloaded += 1
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
            conn.execute(
                """UPDATE papers SET download_status = 'success',
                   pdf_local_path = ?, acquisition_date = ?
                   WHERE id = ?""",
                (str(dest), now, pid),
            )
            print(f"  OK ({size_kb} KB) [{strategy}]")
        else:
            failed += 1
            conn.execute(
                """UPDATE papers SET download_status = 'failed',
                   acquisition_date = ?
                   WHERE id = ?""",
                (now, pid),
            )
            print(f"  FAIL ({status})")

        conn.commit()

        if i < total:
            time.sleep(DOWNLOAD_DELAY)

    db.close()

    print(f"\n{'='*60}")
    print("DOWNLOAD SUMMARY")
    print(f"{'='*60}")
    print(f"  Downloaded:    {downloaded}")
    print(f"  Already had:   {skipped}")
    print(f"  Failed:        {failed}")
    print(f"  Total:         {downloaded + skipped + failed}")
    if strategy_counts:
        print(f"\n  Strategy breakdown:")
        for strat, count in sorted(strategy_counts.items(), key=lambda x: -x[1]):
            print(f"    {strat:20s} {count:>4d}")

    return {"downloaded": downloaded, "skipped": skipped, "failed": failed,
            "strategies": strategy_counts}


def main():
    from engine.utils.background import maybe_background

    parser = argparse.ArgumentParser(description="Download PDFs for included papers")
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument("--retry", action="store_true",
                        help="Retry previously failed downloads only")
    parser.add_argument("--background", action="store_true",
                        help="Run in detached tmux session")

    # Extract review name early for background launcher
    known, _ = parser.parse_known_args()
    maybe_background("download", review_name=known.review)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    download_papers(args.review, retry_failed=args.retry)


if __name__ == "__main__":
    main()
