"""Generate HTML + CSV manual download list for papers that need manual PDF acquisition.

Includes institutional proxy links, PubMed fallback, Google Scholar fallback.
Sorted by EE-ID. Progress checkboxes with localStorage persistence.

CLI:
    python -m engine.acquisition.manual_list --review surgical_autonomy
"""

import argparse
import csv
import html as html_mod
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
    # - included, not successfully downloaded, not already on disk
    papers = conn.execute(
        """SELECT id, doi, pmid, title, ee_identifier, download_status, oa_status
           FROM papers
           WHERE status NOT IN ('SCREENED_OUT', 'REJECTED')
             AND download_status != 'success'
           ORDER BY ee_identifier"""
    ).fetchall()

    # Filter out papers that actually have PDFs on disk
    missing = []
    for p in papers:
        pdf_path = pdf_dir / f"{p['id']}.pdf"
        if pdf_path.exists():
            try:
                with open(pdf_path, "rb") as f:
                    if f.read(4) == b"%PDF":
                        continue
            except OSError:
                pass
        missing.append(dict(p))

    db.close()

    if not missing:
        print("All papers have PDFs. Nothing to generate.")
        return {"total": 0}

    # ── CSV ─────────────────────────────────────────────────────
    csv_path = out_dir / "manual_downloads_needed.csv"
    csv_cols = ["ee_id", "paper_id", "title", "doi", "pmid", "oa_status",
                "download_status", "google_scholar", "direct_doi", "pubmed", "proxy"]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_cols)
        writer.writeheader()
        for p in missing:
            links = _make_links(p["doi"], p["pmid"], p["title"], proxy_pattern)
            link_map = {label: url for url, label in links}
            writer.writerow({
                "ee_id": p["ee_identifier"] or "",
                "paper_id": p["id"],
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

    rows_html = []
    for p in missing:
        links = _make_links(p["doi"], p["pmid"], p["title"], proxy_pattern)
        esc_title = html_mod.escape(p["title"])
        title_trunc = esc_title[:80] + ("..." if len(esc_title) > 80 else "")
        doi_display = html_mod.escape(p["doi"]) if p["doi"] else "<em>none</em>"
        ee_display = html_mod.escape(p["ee_identifier"] or "—")
        oa_display = html_mod.escape(p["oa_status"] or "—")

        link_parts = []
        for url, label in links:
            link_parts.append(
                f'<a href="{html_mod.escape(url)}" target="_blank">{label}</a>'
            )
        links_cell = " &middot; ".join(link_parts)

        rows_html.append(f"""      <tr>
        <td><input type="checkbox" id="cb-{p['id']}" onchange="updateCount()"></td>
        <td class="ee">{ee_display}</td>
        <td>{p['id']}</td>
        <td title="{esc_title}">{title_trunc}</td>
        <td class="doi">{doi_display}</td>
        <td>{oa_display}</td>
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
<title>Manual PDF Downloads — Surgical Evidence Engine</title>
<style>
  body {{ font-family: 'IBM Plex Sans', system-ui, sans-serif; margin: 2rem; background: #EEF5F4; color: #2C2C2C; }}
  h1 {{ color: #0A5E56; margin-bottom: 0.25rem; }}
  .instructions {{ background: #fff; border-left: 4px solid #0A5E56; padding: 1rem 1.25rem; margin: 1rem 0 1.5rem; border-radius: 4px; }}
  .instructions ol {{ margin: 0.5rem 0 0 1.25rem; padding: 0; }}
  .instructions li {{ margin: 0.3rem 0; }}
  .stats {{ font-size: 0.95rem; margin-bottom: 1rem; }}
  .stats span {{ font-weight: 600; color: #0A5E56; }}
  #progress {{ font-weight: 600; color: #B85D3A; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; border-radius: 6px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  th {{ background: #0A5E56; color: #fff; padding: 0.6rem 0.75rem; text-align: left; font-size: 0.85rem; position: sticky; top: 0; }}
  td {{ padding: 0.5rem 0.75rem; border-bottom: 1px solid #e0e0e0; font-size: 0.85rem; }}
  tr:hover {{ background: #e8f0ef; }}
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
<p style="margin-top:0; color:#666;">Surgical Evidence Engine — Autonomy in Surgical Robotics review</p>

<div class="instructions">
  <strong>Instructions:</strong>
  <ol>
    <li>Connect to your <strong>institutional VPN</strong> for best results (enables Direct DOI and Proxy links).</li>
    <li>Click any link to open the publisher page — try <strong>Google Scholar</strong> first (shows institutional access), then Direct DOI or PubMed.</li>
    <li>Download the PDF and save it as <code>data/{review_name}/pdfs/<strong>{{paper_id}}</strong>.pdf</code></li>
    <li>Check the box when done. Progress is tracked locally in your browser.</li>
  </ol>
</div>

<div class="stats">
  Total missing: <span>{len(missing)}</span> &nbsp;|&nbsp;
  With DOI (proxy): <span>{has_doi}</span> &nbsp;|&nbsp;
  PMID only: <span>{has_pmid}</span> &nbsp;|&nbsp;
  Title only: <span>{title_only}</span> &nbsp;|&nbsp;
  Completed: <span id="progress">0/{len(missing)}</span>
</div>

<table>
  <thead>
    <tr>
      <th style="width:2rem;">&#10003;</th>
      <th>EE-ID</th>
      <th>DB ID</th>
      <th>Title</th>
      <th>DOI</th>
      <th>OA Status</th>
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

    return {"total": len(missing), "has_doi": has_doi,
            "has_pmid": has_pmid, "title_only": title_only}


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
