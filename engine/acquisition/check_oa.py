"""Query Unpaywall API for OA status and best PDF URL.

Idempotent: skips papers already checked (oa_status is not NULL).
Rate-limited to 1 request/second per Unpaywall policy.

CLI:
    python -m engine.acquisition.check_oa --review surgical_autonomy
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec

logger = logging.getLogger(__name__)

UNPAYWALL_BASE = "https://api.unpaywall.org/v2"
RATE_LIMIT_SECONDS = 1.0
REQUEST_TIMEOUT = 15


def _query_unpaywall(doi: str, email: str) -> dict:
    """Query Unpaywall for a single DOI. Returns {oa_status, pdf_url}."""
    url = f"{UNPAYWALL_BASE}/{quote(doi, safe='')}?email={email}"
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
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


def check_oa_status(review_name: str, spec_path: str | None = None) -> dict:
    """Check OA status for all included papers with DOIs.

    Returns summary stats dict.
    """
    db = ReviewDatabase(review_name)  # closed at end of function
    conn = db._conn
    # Note: we use db directly (not context manager) because the function
    # has multiple early returns that need conn access. try/finally at bottom.

    # Get email from spec or default
    email = None
    if spec_path:
        spec = load_review_spec(spec_path)
        email = spec.unpaywall_email
    if not email:
        email = "axsarin@health.ucdavis.edu"

    # Find papers to check: included, have DOI, not already checked
    _TERMINAL = "('ABSTRACT_SCREENED_OUT', 'REJECTED', 'PDF_EXCLUDED', 'FT_SCREENED_OUT')"
    papers = conn.execute(
        f"""SELECT id, doi, title, oa_status
           FROM papers
           WHERE status NOT IN {_TERMINAL}
             AND doi IS NOT NULL AND doi != ''
             AND oa_status IS NULL
           ORDER BY id"""
    ).fetchall()

    # Also count already-checked for reporting
    already_checked = conn.execute(
        f"""SELECT COUNT(*) FROM papers
           WHERE status NOT IN {_TERMINAL}
             AND oa_status IS NOT NULL"""
    ).fetchone()[0]

    no_doi = conn.execute(
        f"""SELECT COUNT(*) FROM papers
           WHERE status NOT IN {_TERMINAL}
             AND (doi IS NULL OR doi = '')"""
    ).fetchone()[0]

    total = len(papers)
    print(f"Unpaywall OA check — {review_name}")
    print(f"  Papers to check:    {total}")
    print(f"  Already checked:    {already_checked}")
    print(f"  No DOI (skipped):   {no_doi}")
    print(f"  Email: {email}")
    print()

    if total == 0:
        print("Nothing to do.")
        db.close()
        return {"checked": 0, "already_done": already_checked, "no_doi": no_doi}

    now = datetime.now(timezone.utc).isoformat()
    stats = {"gold": 0, "hybrid": 0, "bronze": 0, "green": 0,
             "closed": 0, "not_found": 0, "error": 0}

    for i, paper in enumerate(papers, 1):
        doi = paper["doi"]
        title_short = paper["title"][:65]
        print(f"  [{i}/{total}] {doi[:40]:40s}  {title_short}...", end="", flush=True)

        result = _query_unpaywall(doi, email)
        oa = result["oa_status"]
        pdf = result["pdf_url"]

        conn.execute(
            """UPDATE papers SET oa_status = ?, pdf_url = ?, acquisition_date = ?
               WHERE id = ?""",
            (oa, pdf or None, now, paper["id"]),
        )

        # Track stats
        if oa.startswith("error:"):
            stats["error"] += 1
            print(f"  ERROR")
        else:
            stats[oa] = stats.get(oa, 0) + 1
            has_pdf = " [PDF]" if pdf else ""
            print(f"  {oa}{has_pdf}")

        if i < total:
            time.sleep(RATE_LIMIT_SECONDS)

    conn.commit()
    db.close()

    # Also mark papers without DOI
    with ReviewDatabase(review_name) as db2:
        db2._conn.execute(
            f"""UPDATE papers SET oa_status = 'no_doi', acquisition_date = ?
               WHERE status NOT IN {_TERMINAL}
                 AND (doi IS NULL OR doi = '')
                 AND oa_status IS NULL""",
            (now,),
        )
        db2._conn.commit()

    print(f"\n{'='*60}")
    print("OA STATUS SUMMARY")
    print(f"{'='*60}")
    for status, count in sorted(stats.items(), key=lambda x: -x[1]):
        if count > 0:
            print(f"  {status:15s} {count:>5d}")
    print(f"  {'total':15s} {total:>5d}")

    with_pdf = sum(1 for s in ("gold", "hybrid", "bronze", "green")
                   if stats.get(s, 0) > 0)
    # Count actual PDF URLs from DB
    with ReviewDatabase(review_name) as db3:
        pdf_count = db3._conn.execute(
            f"""SELECT COUNT(*) FROM papers
               WHERE status NOT IN {_TERMINAL}
                 AND pdf_url IS NOT NULL AND pdf_url != ''"""
        ).fetchone()[0]

    print(f"\n  Papers with PDF URL: {pdf_count}")
    print(f"  Papers without PDF:  {total + already_checked - pdf_count + no_doi}")

    return {
        "checked": total,
        "already_done": already_checked,
        "no_doi": no_doi,
        "stats": stats,
        "pdf_urls_found": pdf_count,
    }


def main():
    from engine.utils.background import maybe_background

    parser = argparse.ArgumentParser(description="Check OA status via Unpaywall")
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument("--spec", help="Path to review spec YAML (for email)")
    parser.add_argument("--background", action="store_true",
                        help="Run in detached tmux session")

    known, _ = parser.parse_known_args()
    maybe_background("oa_check", review_name=known.review)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    check_oa_status(args.review, args.spec)


if __name__ == "__main__":
    main()
