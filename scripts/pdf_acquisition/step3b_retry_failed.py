"""Step 3b: Retry failed PDF downloads using alternative strategies.

Strategy order per paper:
  1. PubMed Central OA package (if Europe PMC has a PMCID for the DOI)
  2. IEEE stamp-page scraping (if DOI contains '10.1109')
  3. DOI redirect with Accept: application/pdf header (catch-all)
"""

import csv
import io
import re
import sys
import tarfile
import time
from pathlib import Path
from urllib.parse import quote

import requests

# ── Paths ─────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_CSV = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdf_acquisition" / "download_log.csv"
PDF_DIR = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdfs"

USER_AGENT = "SurgicalEvidenceEngine/1.0 (mailto:axsarin@health.ucdavis.edu)"
BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
DOWNLOAD_DELAY = 2.0
REQUEST_TIMEOUT = 30
PDF_MAGIC = b"%PDF"


# ── Helpers ───────────────────────────────────────────────────────────


def is_valid_pdf(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(4) == PDF_MAGIC
    except OSError:
        return False


def is_pdf_bytes(data: bytes) -> bool:
    return data[:4] == PDF_MAGIC


def save_if_pdf(data: bytes, dest: Path) -> tuple[str, int]:
    """Write data to dest if it's a valid PDF. Returns (status, size_kb)."""
    if not is_pdf_bytes(data):
        return "not_pdf", len(data) // 1024
    dest.write_bytes(data)
    return "success", len(data) // 1024


# ── Strategy 1: PubMed Central OA Package ────────────────────────────


def lookup_pmcid(doi: str) -> str | None:
    """Look up PMCID via Europe PMC API."""
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


def get_pmc_oa_url(pmcid: str) -> str | None:
    """Get OA package FTP URL from NCBI OA service, convert to HTTPS."""
    url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={pmcid}"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        match = re.search(r'href="(ftp://[^"]+\.tar\.gz)"', resp.text)
        if match:
            return match.group(1).replace(
                "ftp://ftp.ncbi.nlm.nih.gov", "https://ftp.ncbi.nlm.nih.gov"
            )
    except requests.RequestException:
        pass
    return None


def download_via_pmc(doi: str, dest: Path) -> tuple[str, int]:
    """Download PDF via PMC OA tar.gz package."""
    pmcid = lookup_pmcid(doi)
    if not pmcid:
        return "no_pmcid", 0

    pkg_url = get_pmc_oa_url(pmcid)
    if not pkg_url:
        return "no_oa_package", 0

    try:
        resp = requests.get(
            pkg_url,
            timeout=60,
            headers={"User-Agent": USER_AGENT},
        )
        resp.raise_for_status()
        tar = tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:gz")
        pdf_members = [m for m in tar.getnames() if m.endswith(".pdf")]
        if not pdf_members:
            return "no_pdf_in_archive", 0
        content = tar.extractfile(pdf_members[0]).read()
        return save_if_pdf(content, dest)
    except (requests.RequestException, tarfile.TarError) as exc:
        return f"pmc_error:{exc}", 0


# ── Strategy 2: IEEE Stamp Page ───────────────────────────────────────


def download_via_ieee_stamp(doi: str, dest: Path) -> tuple[str, int]:
    """Scrape IEEE stamp page for the iframe PDF URL, then download."""
    # Extract arnumber from the DOI landing page
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

    # Extract arnumber from final URL
    m = re.search(r"/document/(\d+)", resp.url)
    if not m:
        return "no_arnumber", 0
    arnumber = m.group(1)

    # Fetch the stamp page
    stamp_url = f"https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber={arnumber}"
    try:
        resp2 = requests.get(
            stamp_url,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": BROWSER_UA},
        )
        resp2.raise_for_status()
    except requests.RequestException as exc:
        return f"ieee_stamp_error:{exc}", 0

    # Extract iframe src with the ielx PDF URL
    iframe_match = re.search(
        r'<iframe[^>]+src=["\']([^"\']+ielx[^"\']+\.pdf[^"\']*)["\']',
        resp2.text,
        re.I,
    )
    if not iframe_match:
        return "no_iframe_pdf", 0

    pdf_url = iframe_match.group(1)
    if not pdf_url.startswith("http"):
        pdf_url = f"https://ieeexplore.ieee.org{pdf_url}"

    try:
        resp3 = requests.get(
            pdf_url,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers={
                "User-Agent": BROWSER_UA,
                "Referer": stamp_url,
            },
        )
        resp3.raise_for_status()
        return save_if_pdf(resp3.content, dest)
    except requests.RequestException as exc:
        return f"ieee_pdf_error:{exc}", 0


# ── Strategy 3: DOI Redirect with Accept: application/pdf ────────────


def download_via_doi_redirect(doi: str, dest: Path) -> tuple[str, int]:
    """Follow DOI redirect and try to get PDF via content negotiation."""
    try:
        resp = requests.get(
            f"https://doi.org/{doi}",
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/pdf",
            },
        )
        resp.raise_for_status()
        if is_pdf_bytes(resp.content):
            return save_if_pdf(resp.content, dest)
    except requests.RequestException:
        pass

    # Fallback: try the landing page URL + /pdf or /pdfdirect
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
                if resp2.status_code == 200 and is_pdf_bytes(resp2.content):
                    return save_if_pdf(resp2.content, dest)
            except requests.RequestException:
                continue
    except requests.RequestException:
        pass

    return "doi_redirect_failed", 0


# ── Main ──────────────────────────────────────────────────────────────


def main():
    if not LOG_CSV.exists():
        print(f"ERROR: {LOG_CSV} not found. Run step3 first.")
        sys.exit(1)

    # Load existing log
    with open(LOG_CSV, newline="") as f:
        all_log_rows = list(csv.DictReader(f))

    log_by_id = {r["paper_id"]: r for r in all_log_rows}

    # Filter to failed/not_pdf papers
    failed = [
        r for r in all_log_rows
        if r["status"].startswith("failed:") or r["status"] == "not_pdf"
    ]
    print(f"Failed papers to retry: {len(failed)}")

    # Skip already-downloaded
    to_retry = []
    for r in failed:
        dest = PDF_DIR / f"{r['paper_id']}.pdf"
        if dest.exists() and is_valid_pdf(dest):
            print(f"  SKIP (already exists) paper {r['paper_id']}")
            log_by_id[r["paper_id"]]["status"] = "skipped_exists"
            log_by_id[r["paper_id"]]["file_size_kb"] = str(dest.stat().st_size // 1024)
            continue
        to_retry.append(r)

    print(f"Retrying: {to_retry_count} papers" if (to_retry_count := len(to_retry)) else "Nothing to retry.")
    if not to_retry:
        return

    # ── Batch lookup PMCIDs first (saves time vs per-paper) ───
    print(f"\nLooking up PMCIDs via Europe PMC...")
    pmcid_map: dict[str, str] = {}
    for i, r in enumerate(to_retry):
        doi = r["doi"]
        if not doi:
            continue
        pmcid = lookup_pmcid(doi)
        if pmcid:
            pmcid_map[r["paper_id"]] = pmcid
        if (i + 1) % 10 == 0:
            print(f"  Checked {i + 1}/{len(to_retry)} DOIs...")
        time.sleep(0.3)

    ieee_count = sum(1 for r in to_retry if r["doi"].startswith("10.1109"))
    other_count = len(to_retry) - len(pmcid_map) - ieee_count
    # Some IEEE papers might also have PMCIDs, so adjust
    print(f"  PMCIDs found:     {len(pmcid_map)}")
    print(f"  IEEE papers:      {ieee_count}")
    print(f"  DOI-redirect:     will try for remaining")

    # ── Download loop ─────────────────────────────────────────
    recovered = 0
    still_failed = 0
    total = len(to_retry)

    print(f"\n{'='*60}")
    print("RETRYING DOWNLOADS")
    print(f"{'='*60}\n")

    for i, r in enumerate(to_retry, 1):
        pid = r["paper_id"]
        doi = r["doi"]
        dest = PDF_DIR / f"{pid}.pdf"
        title_short = r["title"][:65]

        # Pick strategy
        if pid in pmcid_map:
            strategy = "PMC"
            print(f"  [{i}/{total}] PMC     paper {pid}: {title_short}...")
            status, size_kb = download_via_pmc(doi, dest)
        elif doi.startswith("10.1109"):
            strategy = "IEEE"
            print(f"  [{i}/{total}] IEEE    paper {pid}: {title_short}...")
            status, size_kb = download_via_ieee_stamp(doi, dest)
        else:
            strategy = "DOI"
            print(f"  [{i}/{total}] DOI     paper {pid}: {title_short}...")
            status, size_kb = download_via_doi_redirect(doi, dest)

        # If primary strategy failed, try DOI redirect as fallback (except for DOI strategy)
        if status != "success" and strategy != "DOI":
            print(f"           {strategy} failed ({status}), trying DOI redirect...")
            status, size_kb = download_via_doi_redirect(doi, dest)

        # Update log
        log_by_id[pid]["status"] = status
        log_by_id[pid]["file_size_kb"] = str(size_kb)

        if status == "success":
            recovered += 1
            print(f"           ✓ {size_kb} KB via {strategy}")
        else:
            still_failed += 1
            print(f"           ✗ {status}")

        if i < total:
            time.sleep(DOWNLOAD_DELAY)

    # ── Rewrite log CSV ───────────────────────────────────────
    log_cols = ["paper_id", "title", "doi", "priority", "status", "file_size_kb", "pdf_url"]
    with open(LOG_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=log_cols)
        writer.writeheader()
        for r in all_log_rows:
            writer.writerow({c: r[c] for c in log_cols})
    print(f"\nUpdated download log → {LOG_CSV}")

    # ── Summary ───────────────────────────────────────────────
    # Re-read for accurate counts
    with open(LOG_CSV, newline="") as f:
        final_rows = list(csv.DictReader(f))

    print(f"\n{'='*60}")
    print("RETRY SUMMARY")
    print(f"{'='*60}")
    print(f"  Recovered:     {recovered}/{total}")
    print(f"  Still failed:  {still_failed}/{total}")

    print(f"\n── Overall Download Status (all papers) ──")
    for pri in (1, 2, 3):
        pri_rows = [r for r in final_rows if int(r["priority"]) == pri]
        if not pri_rows:
            continue
        success = sum(1 for r in pri_rows if r["status"] in ("success", "skipped_exists"))
        failed_ct = len(pri_rows) - success
        label = {1: "SCREENED_IN", 2: "validation sample", 3: "remaining excluded"}[pri]
        print(f"  P{pri} ({label}): {success}/{len(pri_rows)} PDFs acquired, {failed_ct} still missing")


if __name__ == "__main__":
    main()
