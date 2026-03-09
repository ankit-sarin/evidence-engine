"""Step 4: Generate manual download list for all papers still missing PDFs.

Merges download_log.csv (failed downloads) with priority_list.csv (papers
that had no Unpaywall URL) to produce a single HTML checklist and CSV.
"""

import csv
import html as html_mod
import sys
from pathlib import Path
from urllib.parse import quote_plus

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_CSV = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdf_acquisition" / "download_log.csv"
PRIORITY_CSV = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdf_acquisition" / "download_priority_list.csv"
PDF_DIR = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdfs"
OUT_DIR = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdf_acquisition"
HTML_OUT = OUT_DIR / "manual_download_list.html"
CSV_OUT = OUT_DIR / "manual_downloads_needed.csv"

PROXY_PREFIX = "https://doi-org.libproxy.ucdavis.edu"


def load_missing_all() -> list[dict]:
    """Return all papers that don't have a valid PDF on disk."""
    # Load priority list for full metadata (doi, pmid)
    with open(PRIORITY_CSV, newline="") as f:
        all_papers = {r["paper_id"]: r for r in csv.DictReader(f)}

    # Load download log for status info
    with open(LOG_CSV, newline="") as f:
        log = {r["paper_id"]: r for r in csv.DictReader(f)}

    missing = []
    for pid, paper in all_papers.items():
        pri = int(paper["priority"])

        # Skip if PDF already on disk
        if (PDF_DIR / f"{pid}.pdf").exists():
            continue

        log_entry = log.get(pid)
        fail_reason = log_entry["status"] if log_entry else "no_oa_url"

        missing.append({
            "paper_id": pid,
            "title": paper["title"],
            "doi": paper["doi"],
            "pmid": paper["pmid"],
            "source": paper["source"],
            "priority": pri,
            "fail_reason": fail_reason,
        })

    # Sort: P1 first, then by paper_id
    missing.sort(key=lambda r: (r["priority"], int(r["paper_id"])))
    return missing


def make_link(paper: dict) -> tuple[str, str]:
    """Return (url, label) for the best download link."""
    doi = paper["doi"]
    pmid = paper["pmid"]
    title = paper["title"]

    if doi:
        url = f"{PROXY_PREFIX}/{doi}"
        return url, "UC Davis Proxy"
    if pmid:
        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
        return url, "PubMed"
    url = f"https://scholar.google.com/scholar?q={quote_plus(title)}"
    return url, "Google Scholar"


def save_csv(papers: list[dict]) -> None:
    cols = ["priority", "paper_id", "title", "doi", "pmid", "source", "link", "link_type", "fail_reason"]
    with open(CSV_OUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for p in papers:
            url, label = make_link(p)
            writer.writerow({
                "priority": p["priority"],
                "paper_id": p["paper_id"],
                "title": p["title"],
                "doi": p["doi"],
                "pmid": p["pmid"],
                "source": p["source"],
                "link": url,
                "link_type": label,
                "fail_reason": p["fail_reason"],
            })
    print(f"CSV saved → {CSV_OUT}")


def save_html(papers: list[dict]) -> None:
    p1 = [p for p in papers if p["priority"] == 1]
    p2 = [p for p in papers if p["priority"] == 2]
    p3 = [p for p in papers if p["priority"] == 3]

    rows_html = []
    for p in papers:
        url, label = make_link(p)
        esc_title = html_mod.escape(p["title"])
        title_trunc = esc_title[:80] + ("..." if len(esc_title) > 80 else "")
        doi_display = html_mod.escape(p["doi"]) if p["doi"] else "<em>none</em>"
        pri_class = {1: "p1", 2: "p2", 3: "p3"}[p["priority"]]

        rows_html.append(f"""      <tr class="{pri_class}">
        <td><input type="checkbox" id="cb-{p['paper_id']}" onchange="updateCount()"></td>
        <td>P{p['priority']}</td>
        <td>{p['paper_id']}</td>
        <td title="{esc_title}">{title_trunc}</td>
        <td class="doi">{doi_display}</td>
        <td>{html_mod.escape(p['source'])}</td>
        <td><a href="{html_mod.escape(url)}" target="_blank">{label}</a></td>
        <td class="save-as">{p['paper_id']}.pdf</td>
      </tr>""")

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
  tr.p1 {{ background: #fff; }}
  tr.p2 {{ background: #f8f8f5; }}
  tr.p3 {{ background: #f4f2ee; }}
  tr:hover {{ background: #e8f0ef; }}
  .doi {{ font-family: monospace; font-size: 0.8rem; color: #666; }}
  .save-as {{ font-family: monospace; font-weight: 600; color: #B85D3A; }}
  a {{ color: #0A5E56; }}
  input[type="checkbox"] {{ transform: scale(1.2); cursor: pointer; }}
  input[type="checkbox"]:checked + td {{ opacity: 0.5; }}
  tr:has(input:checked) {{ opacity: 0.5; text-decoration: line-through; }}
</style>
</head>
<body>
<h1>Manual PDF Downloads</h1>
<p style="margin-top:0; color:#666;">Surgical Evidence Engine — Autonomy in Surgical Robotics review</p>

<div class="instructions">
  <strong>Instructions:</strong>
  <ol>
    <li>Connect to <strong>UC Davis VPN</strong> or log into the library proxy first.</li>
    <li>Click each link to open the publisher page in a new tab.</li>
    <li>Download the PDF and save it as <code>data/surgical_autonomy/pdfs/<strong>{{paper_id}}</strong>.pdf</code></li>
    <li>Check the box when done. Progress is tracked locally in your browser.</li>
  </ol>
</div>

<div class="stats">
  P1 (screened-in, needed for extraction): <span>{len(p1)}</span> papers &nbsp;|&nbsp;
  P2 (validation sample): <span>{len(p2)}</span> papers &nbsp;|&nbsp;
  P3 (remaining excluded): <span>{len(p3)}</span> papers &nbsp;|&nbsp;
  Total: <span>{len(papers)}</span> &nbsp;|&nbsp;
  Completed: <span id="progress">0/{len(papers)}</span>
</div>

<table>
  <thead>
    <tr>
      <th style="width:2rem;">&#10003;</th>
      <th>Pri</th>
      <th>ID</th>
      <th>Title</th>
      <th>DOI</th>
      <th>Source</th>
      <th>Link</th>
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
  document.getElementById('progress').textContent = done + '/{len(papers)}';
  // Persist to localStorage
  const state = [...boxes].map(b => b.checked);
  localStorage.setItem('manual_dl_state', JSON.stringify(state));
}}
// Restore on load
(function() {{
  const saved = localStorage.getItem('manual_dl_state');
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

    HTML_OUT.write_text(page)
    print(f"HTML saved → {HTML_OUT}")


def main():
    if not LOG_CSV.exists() or not PRIORITY_CSV.exists():
        print("ERROR: Run step2 and step3 first.")
        sys.exit(1)

    papers = load_missing_all()

    if not papers:
        print("All papers have PDFs. Nothing to do.")
        return

    save_csv(papers)
    save_html(papers)

    p1 = sum(1 for p in papers if p["priority"] == 1)
    p2 = sum(1 for p in papers if p["priority"] == 2)
    p3 = sum(1 for p in papers if p["priority"] == 3)
    has_doi = sum(1 for p in papers if p["doi"])
    has_pmid_only = sum(1 for p in papers if not p["doi"] and p["pmid"])
    title_only = sum(1 for p in papers if not p["doi"] and not p["pmid"])

    print()
    print("=" * 55)
    print("MANUAL DOWNLOADS NEEDED")
    print("=" * 55)
    print(f"  P1 (screened-in):        {p1}")
    print(f"  P2 (validation sample):  {p2}")
    print(f"  P3 (remaining excluded): {p3}")
    print(f"  Total:                   {p1 + p2 + p3}")
    print()
    print(f"  With DOI (proxy link):   {has_doi}")
    print(f"  PMID only (PubMed):      {has_pmid_only}")
    print(f"  Title only (Scholar):    {title_only}")


if __name__ == "__main__":
    main()
