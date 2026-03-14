"""DEPRECATED — Use pdf_quality_html.py --mode acquisition instead.

This module is superseded by:
    python -m engine.acquisition.pdf_quality_html --review NAME --mode acquisition

The new module provides structured disposition tracking (ACQUIRED / WILL_REATTEMPT /
EXCLUDE), JSON export for import into the DB, and integration with the PDF quality
check pipeline. This file is retained for backward compatibility but will be
removed in a future version.

Original description:
Generate HTML + CSV manual download list for papers that need manual PDF acquisition.
Includes institutional proxy links, PubMed fallback, Google Scholar fallback.
Grouped by publisher, sorted by EE-ID within each group.
Progress checkboxes with localStorage persistence.

CLI (deprecated):
    python -m engine.acquisition.manual_list --review surgical_autonomy
"""

import argparse
import csv
import html as html_mod
import json
import logging
import sys
from pathlib import Path
from urllib.parse import quote_plus

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.core.database import DATA_ROOT, ReviewDatabase
from engine.core.review_spec import load_review_spec

logger = logging.getLogger(__name__)

DEFAULT_PROXY = "https://doi-org.libproxy.ucdavis.edu/{doi}"


# ── Publisher Classification ────────────────────────────────────────


# DOI prefix → publisher label (checked in order)
_DOI_PUBLISHER_RULES = [
    ("10.1109/", "IEEE"),
    ("10.1016/", "Elsevier"),
    ("10.1007/", "Springer/Nature"),
    ("10.1038/", "Springer/Nature"),
    ("10.1002/", "Wiley"),
    ("10.3390/", "MDPI"),
    ("10.1080/", "Taylor & Francis"),
    ("10.1177/", "SAGE"),
    ("10.1126/", "Science/AAAS"),
    ("10.1371/", "PLOS"),
    ("10.3389/", "Frontiers"),
    ("10.1097/", "Wolters Kluwer"),
    ("10.1148/", "RSNA"),
    ("10.21037/", "AME"),
    ("10.1515/", "De Gruyter"),
    ("10.1117/", "SPIE"),
    ("10.5281/", "Zenodo"),
]


def classify_publisher(doi: str | None) -> str:
    """Classify publisher from DOI prefix. Returns publisher name or 'Other'."""
    if not doi:
        return "Unknown (no DOI)"
    doi_lower = doi.lower()
    for prefix, publisher in _DOI_PUBLISHER_RULES:
        if doi_lower.startswith(prefix):
            return publisher
    return "Other"


# ── Link Generation ─────────────────────────────────────────────────


def _make_links(doi: str | None, pmid: str | None, title: str,
                proxy_pattern: str) -> list[tuple[str, str]]:
    """Return list of (url, label) for all available download links.

    Always includes Google Scholar. Adds Direct DOI, PubMed, and
    Institutional Proxy when identifiers are available.
    """
    links = []
    # Google Scholar — most reliable fallback, always available
    links.append((
        f"https://scholar.google.com/scholar?q={quote_plus(title)}",
        "Google Scholar",
    ))
    if doi:
        links.append((f"https://doi.org/{doi}", "Direct DOI"))
    if pmid:
        links.append((f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/", "PubMed"))
    if doi:
        url = proxy_pattern.replace("{doi}", doi)
        links.append((url, "Proxy"))
    return links


def _first_author_last_name(authors_json: str | None) -> str:
    """Extract first author's last name from JSON array."""
    if not authors_json:
        return ""
    try:
        authors = json.loads(authors_json) if isinstance(authors_json, str) else authors_json
        if authors and isinstance(authors, list):
            return authors[0].split()[-1] if authors[0] else ""
    except (json.JSONDecodeError, IndexError):
        pass
    return ""


# ── Main Generator ──────────────────────────────────────────────────


def generate_manual_list(review_name: str, spec_path: str | None = None) -> dict:
    """Generate HTML + CSV for papers needing manual download.

    Returns summary stats.
    """
    db = ReviewDatabase(review_name)
    conn = db._conn

    proxy_pattern = DEFAULT_PROXY
    if spec_path:
        spec = load_review_spec(spec_path)
        if spec.institutional_proxy_pattern:
            proxy_pattern = spec.institutional_proxy_pattern

    out_dir = DATA_ROOT / review_name / "pdf_acquisition"
    out_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir = DATA_ROOT / review_name / "pdfs"

    # Find papers that need manual download:
    # Uses DB as source of truth: download_status and pdf_local_path
    papers = conn.execute(
        """SELECT id, doi, pmid, title, authors, year, ee_identifier,
                  download_status, oa_status, pdf_local_path
           FROM papers
           WHERE status NOT IN ('ABSTRACT_SCREENED_OUT', 'REJECTED')
             AND download_status != 'success'
           ORDER BY ee_identifier"""
    ).fetchall()

    # Filter out papers that have a valid PDF on disk (DB path or bare integer name)
    missing = []
    for p in papers:
        # Check DB-recorded path first (handles renamed files)
        if p["pdf_local_path"]:
            candidate = Path(p["pdf_local_path"])
            if not candidate.is_absolute():
                candidate = pdf_dir / candidate
            if candidate.exists():
                try:
                    with open(candidate, "rb") as f:
                        if f.read(4) == b"%PDF":
                            continue
                except OSError:
                    pass

        # Fall back to bare integer filename
        bare_path = pdf_dir / f"{p['id']}.pdf"
        if bare_path.exists():
            try:
                with open(bare_path, "rb") as f:
                    if f.read(4) == b"%PDF":
                        continue
            except OSError:
                pass

        row = dict(p)
        row["publisher"] = classify_publisher(p["doi"])
        row["first_author"] = _first_author_last_name(p["authors"])
        missing.append(row)

    db.close()

    if not missing:
        print("All papers have PDFs. Nothing to generate.")
        return {"total": 0}

    # Publisher breakdown
    pub_counts: dict[str, int] = {}
    for p in missing:
        pub_counts[p["publisher"]] = pub_counts.get(p["publisher"], 0) + 1

    # Sort by publisher then EE-ID within each group
    pub_order = sorted(pub_counts.keys(), key=lambda k: (-pub_counts[k], k))
    pub_rank = {pub: i for i, pub in enumerate(pub_order)}
    missing.sort(key=lambda p: (pub_rank.get(p["publisher"], 999),
                                 p["ee_identifier"] or "zzz"))

    verify_cmd = f"python -m engine.acquisition.verify_downloads --review {review_name}"

    # ── CSV ─────────────────────────────────────────────────────
    csv_path = out_dir / "manual_downloads_needed.csv"
    csv_cols = ["publisher", "ee_id", "paper_id", "first_author", "year",
                "title", "doi", "pmid", "oa_status",
                "download_status", "google_scholar", "direct_doi", "pubmed", "proxy"]
    with open(csv_path, "w", newline="") as f:
        # Comment header block
        f.write(f"# Manual PDF Downloads — {review_name}\n")
        f.write(f"# Total papers needing download: {len(missing)}\n")
        f.write(f"# File naming: save as {{paper_id}}.pdf (e.g., 47.pdf, 605.pdf)\n")
        f.write(f"# Target directory: {pdf_dir}\n")
        f.write(f"# After downloads, run: {verify_cmd}\n")
        f.write("#\n")

        writer = csv.DictWriter(f, fieldnames=csv_cols)
        writer.writeheader()
        for p in missing:
            links = _make_links(p["doi"], p["pmid"], p["title"], proxy_pattern)
            link_map = {label: url for url, label in links}
            writer.writerow({
                "publisher": p["publisher"],
                "ee_id": p["ee_identifier"] or "",
                "paper_id": p["id"],
                "first_author": p["first_author"],
                "year": p.get("year") or "",
                "title": p["title"],
                "doi": p["doi"] or "",
                "pmid": p["pmid"] or "",
                "oa_status": p["oa_status"] or "",
                "download_status": p["download_status"] or "",
                "google_scholar": link_map.get("Google Scholar", ""),
                "direct_doi": link_map.get("Direct DOI", ""),
                "pubmed": link_map.get("PubMed", ""),
                "proxy": link_map.get("Proxy", ""),
            })
    print(f"CSV saved → {csv_path}")

    # ── HTML ────────────────────────────────────────────────────
    html_path = out_dir / "manual_download_list.html"

    # Build publisher breakdown HTML
    pub_summary_rows = []
    for pub in pub_order:
        cnt = pub_counts[pub]
        pub_summary_rows.append(
            f'<tr><td>{html_mod.escape(pub)}</td><td style="text-align:right;">{cnt}</td></tr>'
        )
    pub_summary_html = "\n".join(pub_summary_rows)

    # Build table rows grouped by publisher
    rows_html = []
    current_publisher = None
    for p in missing:
        # Publisher group header
        if p["publisher"] != current_publisher:
            current_publisher = p["publisher"]
            pub_count = pub_counts[current_publisher]
            rows_html.append(f"""      <tr class="pub-header">
        <td colspan="9"><strong>{html_mod.escape(current_publisher)}</strong> ({pub_count} papers)</td>
      </tr>""")

        links = _make_links(p["doi"], p["pmid"], p["title"], proxy_pattern)
        esc_title = html_mod.escape(p["title"])
        title_trunc = esc_title[:80] + ("..." if len(esc_title) > 80 else "")
        doi_display = html_mod.escape(p["doi"]) if p["doi"] else "<em>none</em>"
        ee_display = html_mod.escape(p["ee_identifier"] or "—")
        author_display = html_mod.escape(p["first_author"]) if p["first_author"] else "—"
        year_display = str(p.get("year") or "—")

        link_parts = []
        for url, label in links:
            link_parts.append(
                f'<a href="{html_mod.escape(url)}" target="_blank">{label}</a>'
            )
        links_cell = " &middot; ".join(link_parts)

        rows_html.append(f"""      <tr>
        <td><input type="checkbox" id="cb-{p['id']}" onchange="updateCount()"></td>
        <td class="save-as">{p['id']}</td>
        <td class="ee">{ee_display}</td>
        <td title="{esc_title}">{title_trunc}</td>
        <td>{author_display}</td>
        <td>{year_display}</td>
        <td class="doi">{doi_display}</td>
        <td class="links">{links_cell}</td>
        <td class="save-as">{p['id']}.pdf</td>
      </tr>""")

    has_doi = sum(1 for p in missing if p["doi"])
    has_pmid = sum(1 for p in missing if not p["doi"] and p["pmid"])
    title_only = len(missing) - has_doi - has_pmid

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Manual PDF Downloads — {html_mod.escape(review_name)}</title>
<style>
  body {{ font-family: 'IBM Plex Sans', system-ui, sans-serif; margin: 2rem; background: #EEF5F4; color: #2C2C2C; }}
  h1 {{ color: #0A5E56; margin-bottom: 0.25rem; }}
  h2 {{ color: #0A5E56; font-size: 1.1rem; margin: 1.5rem 0 0.5rem; }}
  .header {{ background: #fff; border-left: 4px solid #0A5E56; padding: 1rem 1.25rem; margin: 1rem 0 1.5rem; border-radius: 4px; }}
  .header ol {{ margin: 0.5rem 0 0 1.25rem; padding: 0; }}
  .header li {{ margin: 0.3rem 0; }}
  .header code {{ background: #f0f0f0; padding: 0.15rem 0.4rem; border-radius: 3px; font-size: 0.9rem; }}
  .naming-note {{ background: #FFF8F0; border-left: 4px solid #B85D3A; padding: 0.75rem 1rem; margin: 0.75rem 0; border-radius: 4px; font-size: 0.9rem; }}
  .naming-note strong {{ color: #B85D3A; }}
  .pub-summary {{ display: inline-block; background: #fff; border-radius: 6px; padding: 0.5rem 1rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 1rem; }}
  .pub-summary table {{ border-collapse: collapse; }}
  .pub-summary td {{ padding: 0.25rem 0.75rem; border-bottom: 1px solid #eee; font-size: 0.85rem; }}
  .pub-summary tr:last-child td {{ border-bottom: none; font-weight: 600; }}
  .stats {{ font-size: 0.95rem; margin-bottom: 1rem; }}
  .stats span {{ font-weight: 600; color: #0A5E56; }}
  #progress {{ font-weight: 600; color: #B85D3A; }}
  table.main {{ border-collapse: collapse; width: 100%; background: #fff; border-radius: 6px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  table.main th {{ background: #0A5E56; color: #fff; padding: 0.6rem 0.75rem; text-align: left; font-size: 0.85rem; position: sticky; top: 0; z-index: 1; }}
  table.main td {{ padding: 0.5rem 0.75rem; border-bottom: 1px solid #e0e0e0; font-size: 0.85rem; }}
  table.main tr:hover {{ background: #e8f0ef; }}
  .pub-header td {{ background: #D4E8E5; font-size: 0.9rem; padding: 0.5rem 0.75rem; border-bottom: 2px solid #0A5E56; }}
  .doi {{ font-family: monospace; font-size: 0.8rem; color: #666; }}
  .ee {{ font-family: monospace; font-weight: 600; color: #0A5E56; }}
  .save-as {{ font-family: monospace; font-weight: 600; color: #B85D3A; }}
  .links {{ white-space: nowrap; }}
  .links a {{ margin: 0 0.15rem; }}
  a {{ color: #0A5E56; }}
  input[type="checkbox"] {{ transform: scale(1.2); cursor: pointer; }}
  tr:has(input:checked) {{ opacity: 0.5; text-decoration: line-through; }}
</style>
</head>
<body>
<h1>Manual PDF Downloads</h1>
<p style="margin-top:0; color:#666;">Surgical Evidence Engine — {html_mod.escape(review_name)}</p>

<div class="header">
  <strong>Total papers needing download: {len(missing)}</strong>

  <div class="naming-note">
    <strong>File naming convention:</strong> Save each PDF as <code>{{paper_id}}.pdf</code> —
    the <strong>bare integer</strong> shown in the leftmost "ID" column of each row below.
    <br>Examples: <code>47.pdf</code>, <code>605.pdf</code>.
    Do NOT use EE identifiers — the engine will rename files automatically after verification.
  </div>

  <strong>Instructions:</strong>
  <ol>
    <li>Connect to your <strong>institutional VPN</strong> for best results (enables Direct DOI and Proxy links).</li>
    <li>Click any link to open the publisher page — try <strong>Google Scholar</strong> first, then Direct DOI or PubMed.</li>
    <li>Download the PDF and save it as <code>{html_mod.escape(str(pdf_dir))}/{{paper_id}}.pdf</code></li>
    <li>Check the box when done. Progress is tracked locally in your browser.</li>
    <li>When all downloads are complete, run:<br>
        <code>{html_mod.escape(verify_cmd)}</code></li>
  </ol>
</div>

<h2>Publisher Breakdown</h2>
<div class="pub-summary">
  <table>
{pub_summary_html}
    <tr><td>Total</td><td style="text-align:right;">{len(missing)}</td></tr>
  </table>
</div>

<div class="stats">
  Total missing: <span>{len(missing)}</span> &nbsp;|&nbsp;
  With DOI (proxy): <span>{has_doi}</span> &nbsp;|&nbsp;
  PMID only: <span>{has_pmid}</span> &nbsp;|&nbsp;
  Title only: <span>{title_only}</span> &nbsp;|&nbsp;
  Completed: <span id="progress">0/{len(missing)}</span>
</div>

<table class="main">
  <thead>
    <tr>
      <th style="width:2rem;">&#10003;</th>
      <th>ID</th>
      <th>EE-ID</th>
      <th>Title</th>
      <th>Author</th>
      <th>Year</th>
      <th>DOI</th>
      <th>Links</th>
      <th>Save As</th>
    </tr>
  </thead>
  <tbody>
{chr(10).join(rows_html)}
  </tbody>
</table>

<script>
function updateCount() {{
  const boxes = document.querySelectorAll('input[type=checkbox]');
  const done = [...boxes].filter(b => b.checked).length;
  document.getElementById('progress').textContent = done + '/{len(missing)}';
  const state = [...boxes].map(b => b.checked);
  localStorage.setItem('manual_dl_{review_name}', JSON.stringify(state));
}}
(function() {{
  const saved = localStorage.getItem('manual_dl_{review_name}');
  if (saved) {{
    const state = JSON.parse(saved);
    const boxes = document.querySelectorAll('input[type=checkbox]');
    boxes.forEach((b, i) => {{ if (state[i]) b.checked = true; }});
    updateCount();
  }}
}})();
</script>
</body>
</html>"""

    html_path.write_text(page)
    print(f"HTML saved → {html_path}")

    print(f"\n{'='*55}")
    print("MANUAL DOWNLOADS NEEDED")
    print(f"{'='*55}")
    print(f"  Total:                   {len(missing)}")
    print(f"  With DOI (proxy link):   {has_doi}")
    print(f"  PMID only (PubMed):      {has_pmid}")
    print(f"  Title only (Scholar):    {title_only}")
    print()
    print("  Publisher breakdown:")
    for pub in pub_order:
        print(f"    {pub:<25s} {pub_counts[pub]:>4}")
    print()
    print(f"  After downloads, run:")
    print(f"    {verify_cmd}")

    return {"total": len(missing), "has_doi": has_doi,
            "has_pmid": has_pmid, "title_only": title_only,
            "by_publisher": pub_counts}


def main():
    from engine.utils.background import maybe_background

    parser = argparse.ArgumentParser(description="Generate manual download list")
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument("--spec", help="Path to review spec YAML (for proxy pattern)")
    parser.add_argument("--background", action="store_true",
                        help="Run in detached tmux session")

    known, _ = parser.parse_known_args()
    maybe_background("manual_list", review_name=known.review)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    generate_manual_list(args.review, args.spec)


if __name__ == "__main__":
    main()
