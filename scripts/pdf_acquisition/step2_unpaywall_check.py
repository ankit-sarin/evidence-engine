"""Step 2: Prioritize papers and check Unpaywall for open-access PDFs.

Creates a priority list (P1=screened-in, P2=validation sample, P3=rest),
then queries Unpaywall for every paper with a DOI.
"""

import csv
import random
import sys
import time
from pathlib import Path
from urllib.parse import quote

import requests

# ── Paths ─────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_CSV = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdf_acquisition" / "citations_for_download.csv"
PRIORITY_CSV = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdf_acquisition" / "download_priority_list.csv"
RESULTS_CSV = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdf_acquisition" / "unpaywall_results.csv"

UNPAYWALL_EMAIL = "axsarin@health.ucdavis.edu"
RATE_LIMIT_SECS = 1.0
RANDOM_SEED = 42
VALIDATION_SAMPLE_SIZE = 30


def load_citations() -> list[dict]:
    with open(INPUT_CSV, newline="") as f:
        return list(csv.DictReader(f))


def assign_priorities(rows: list[dict]) -> list[dict]:
    """Assign priority: 1=SCREENED_IN, 2=random excluded sample, 3=rest."""
    screened_in = [r for r in rows if r["status"] == "SCREENED_IN"]
    screened_out = [r for r in rows if r["status"] != "SCREENED_IN"]

    rng = random.Random(RANDOM_SEED)
    rng.shuffle(screened_out)

    sample = screened_out[:VALIDATION_SAMPLE_SIZE]
    remainder = screened_out[VALIDATION_SAMPLE_SIZE:]

    prioritized = []
    for r in screened_in:
        prioritized.append({**r, "priority": 1})
    for r in sample:
        prioritized.append({**r, "priority": 2})
    for r in remainder:
        prioritized.append({**r, "priority": 3})

    return prioritized


def save_priority_list(rows: list[dict]) -> None:
    cols = ["paper_id", "title", "doi", "pmid", "source", "status", "priority"]
    with open(PRIORITY_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for r in rows:
            writer.writerow({c: r[c] for c in cols})
    print(f"Priority list saved → {PRIORITY_CSV}")


def query_unpaywall(doi: str) -> dict:
    """Query Unpaywall API for a single DOI. Returns oa_status and pdf_url."""
    url = f"https://api.unpaywall.org/v2/{quote(doi, safe='')}?email={UNPAYWALL_EMAIL}"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 404:
            return {"oa_status": "not_found", "pdf_url": ""}
        resp.raise_for_status()
        data = resp.json()
        oa_status = data.get("oa_status", "unknown")
        best_loc = data.get("best_oa_location") or {}
        pdf_url = best_loc.get("url_for_pdf") or ""
        return {"oa_status": oa_status, "pdf_url": pdf_url}
    except requests.RequestException as exc:
        return {"oa_status": f"error:{exc}", "pdf_url": ""}


def check_all_dois(rows: list[dict]) -> list[dict]:
    """Query Unpaywall for every row with a DOI. Returns results list."""
    results = []
    dois_to_check = [(i, r) for i, r in enumerate(rows) if r["doi"]]
    total = len(dois_to_check)

    print(f"\nChecking {total} DOIs against Unpaywall (≈{total} seconds)...")
    print()

    for count, (i, r) in enumerate(dois_to_check, 1):
        if count % 25 == 0 or count == 1 or count == total:
            print(f"  [{count}/{total}] Checking DOI: {r['doi'][:60]}...")

        uw = query_unpaywall(r["doi"])
        results.append({
            "paper_id": r["paper_id"],
            "title": r["title"],
            "doi": r["doi"],
            "oa_status": uw["oa_status"],
            "pdf_url": uw["pdf_url"],
            "screening_status": r["status"],
            "priority": r["priority"],
        })

        if count < total:
            time.sleep(RATE_LIMIT_SECS)

    # Add rows without DOI
    for r in rows:
        if not r["doi"]:
            results.append({
                "paper_id": r["paper_id"],
                "title": r["title"],
                "doi": "",
                "oa_status": "no_doi",
                "pdf_url": "",
                "screening_status": r["status"],
                "priority": r["priority"],
            })

    return results


# ── OA status sort key: open-access types first, closed last ─────────
_OA_ORDER = {"gold": 0, "hybrid": 1, "bronze": 2, "green": 3, "closed": 4}


def _sort_key(r: dict) -> tuple:
    return (
        int(r["priority"]),
        _OA_ORDER.get(r["oa_status"], 5),
        r["paper_id"],
    )


def save_results(results: list[dict]) -> None:
    results.sort(key=_sort_key)
    cols = ["paper_id", "title", "doi", "oa_status", "pdf_url", "screening_status", "priority"]
    with open(RESULTS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(results)
    print(f"\nResults saved → {RESULTS_CSV}")


def print_summary(results: list[dict]) -> None:
    total_checked = sum(1 for r in results if r["doi"])
    no_doi = sum(1 for r in results if not r["doi"])

    print()
    print("=" * 60)
    print("UNPAYWALL RESULTS SUMMARY")
    print("=" * 60)
    print(f"  Total DOIs checked:   {total_checked}")
    print(f"  No DOI (skipped):     {no_doi}")
    print()

    for pri in (1, 2, 3):
        pri_rows = [r for r in results if int(r["priority"]) == pri]
        if not pri_rows:
            continue

        has_pdf = [r for r in pri_rows if r["pdf_url"]]
        oa_no_pdf = [r for r in pri_rows if r["oa_status"] not in ("closed", "no_doi", "not_found") and not r["pdf_url"]]
        closed = [r for r in pri_rows if r["oa_status"] == "closed"]
        not_found = [r for r in pri_rows if r["oa_status"] == "not_found"]
        errors = [r for r in pri_rows if r["oa_status"].startswith("error:")]
        no_doi_pri = [r for r in pri_rows if r["oa_status"] == "no_doi"]

        label = {1: "SCREENED_IN", 2: "validation sample", 3: "remaining excluded"}[pri]
        print(f"── Priority {pri} ({label}) — {len(pri_rows)} papers ──")
        print(f"  OA with PDF URL:      {len(has_pdf)}")
        if oa_no_pdf:
            print(f"  OA without PDF URL:   {len(oa_no_pdf)}")
        print(f"  Closed access:        {len(closed)}")
        if not_found:
            print(f"  DOI not found:        {len(not_found)}")
        if errors:
            print(f"  API errors:           {len(errors)}")
        if no_doi_pri:
            print(f"  No DOI:               {len(no_doi_pri)}")
        print()

    # Action summary
    p1 = [r for r in results if int(r["priority"]) == 1]
    p2 = [r for r in results if int(r["priority"]) == 2]
    p1_auto = sum(1 for r in p1 if r["pdf_url"])
    p2_auto = sum(1 for r in p2 if r["pdf_url"])
    p1_manual = len(p1) - p1_auto
    p2_manual = len(p2) - p2_auto

    print("── Action Summary ──────────────────────────")
    print(f"  P1+P2 auto-downloadable:  {p1_auto + p2_auto}  (P1: {p1_auto}, P2: {p2_auto})")
    print(f"  P1+P2 need manual/other:  {p1_manual + p2_manual}  (P1: {p1_manual}, P2: {p2_manual})")


def main():
    if not INPUT_CSV.exists():
        print(f"ERROR: Input CSV not found at {INPUT_CSV}")
        print("Run step1_export_citations.py first.")
        sys.exit(1)

    rows = load_citations()
    print(f"Loaded {len(rows)} citations from {INPUT_CSV}")

    # Step 1: Assign priorities
    prioritized = assign_priorities(rows)
    save_priority_list(prioritized)

    p1 = sum(1 for r in prioritized if r["priority"] == 1)
    p2 = sum(1 for r in prioritized if r["priority"] == 2)
    p3 = sum(1 for r in prioritized if r["priority"] == 3)
    print(f"  Priority 1 (screened in):         {p1}")
    print(f"  Priority 2 (validation sample):   {p2}")
    print(f"  Priority 3 (remaining excluded):  {p3}")

    # Step 2: Check Unpaywall
    results = check_all_dois(prioritized)
    save_results(results)
    print_summary(results)


if __name__ == "__main__":
    main()
