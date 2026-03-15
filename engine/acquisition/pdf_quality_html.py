"""Generate HTML human-review pages for PDF acquisition and quality check.

Two modes:
  --mode acquisition    Initial download list (before PDFs acquired)
  --mode quality_check  Post-download quality review (after AI classification)

CLI:
    python -m engine.acquisition.pdf_quality_html --review surgical_autonomy --mode acquisition
    python -m engine.acquisition.pdf_quality_html --review surgical_autonomy --mode quality_check
    python -m engine.acquisition.pdf_quality_html --review surgical_autonomy --mode quality_check --output path/to/file.html
"""

import argparse
import html as html_mod
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.core.database import DATA_ROOT
from engine.core.naming import review_artifact_filename, review_artifact_path

logger = logging.getLogger(__name__)


# ── Queries ──────────────────────────────────────────────────────────


def _query_section1(conn: sqlite3.Connection) -> list[dict]:
    """Papers without PDFs that still need disposition.

    Excludes already-finalized papers (PDF_EXCLUDED, HUMAN_CONFIRMED).
    """
    rows = conn.execute("""
        SELECT p.id, p.ee_identifier, p.authors, p.year, p.title, p.doi
        FROM papers p
        LEFT JOIN full_text_assets ft ON ft.paper_id = p.id
        WHERE p.status IN (
            'ABSTRACT_SCREENED_IN', 'AI_AUDIT_COMPLETE', 'PDF_ACQUIRED',
            'PARSED', 'FT_ELIGIBLE', 'EXTRACTED', 'HUMAN_AUDIT_COMPLETE'
        )
          AND COALESCE(ft.pdf_path, p.pdf_local_path) IS NULL
        ORDER BY p.ee_identifier
    """).fetchall()
    return [dict(r) for r in rows]


def _query_section2(conn: sqlite3.Connection) -> list[dict]:
    """Papers with AI quality flags that still need review.

    Excludes already-finalized papers (PDF_EXCLUDED, HUMAN_CONFIRMED).
    """
    rows = conn.execute("""
        SELECT p.id, p.ee_identifier, p.authors, p.year, p.title, p.doi,
               p.pdf_ai_language, p.pdf_ai_content_type, p.pdf_ai_confidence
        FROM papers p
        WHERE p.pdf_quality_check_status = 'AI_CHECKED'
          AND (p.pdf_ai_language != 'English' OR p.pdf_ai_content_type != 'full_manuscript')
          AND p.status != 'PDF_EXCLUDED'
        ORDER BY p.ee_identifier
    """).fetchall()
    return [dict(r) for r in rows]


def _query_section3(conn: sqlite3.Connection) -> list[dict]:
    """Papers that passed AI check — no action needed."""
    rows = conn.execute("""
        SELECT p.id, p.ee_identifier, p.authors, p.year, p.title, p.doi
        FROM papers p
        WHERE p.pdf_quality_check_status IN ('AI_CHECKED', 'HUMAN_CONFIRMED')
          AND p.pdf_ai_language = 'English'
          AND p.pdf_ai_content_type = 'full_manuscript'
          AND p.status != 'PDF_EXCLUDED'
        ORDER BY p.ee_identifier
    """).fetchall()
    return [dict(r) for r in rows]


def _query_finalized(conn: sqlite3.Connection) -> dict:
    """Count already-finalized papers (not shown in sections)."""
    confirmed = conn.execute(
        """SELECT COUNT(*) FROM papers
           WHERE pdf_quality_check_status = 'HUMAN_CONFIRMED'
             AND status != 'PDF_EXCLUDED'
             AND (pdf_ai_language != 'English' OR pdf_ai_content_type != 'full_manuscript')"""
    ).fetchone()[0]
    excluded = conn.execute(
        "SELECT COUNT(*) FROM papers WHERE status = 'PDF_EXCLUDED'"
    ).fetchone()[0]
    return {"confirmed": confirmed, "excluded": excluded}


# ── Helpers ──────────────────────────────────────────────────────────


def _first_author(authors: str | None) -> str:
    if not authors:
        return "—"
    first = authors.split(";")[0].split(",")[0].strip()
    return first[:30] if first else "—"


def _esc(text: str | None) -> str:
    return html_mod.escape(str(text)) if text else "—"


def _doi_link(doi: str | None) -> str:
    if not doi:
        return "—"
    safe = _esc(doi)
    return f'<a href="https://doi.org/{safe}" target="_blank">DOI</a>'


def _scholar_link(title: str | None) -> str:
    if not title:
        return ""
    return f'<a href="https://scholar.google.com/scholar?q={quote_plus(title)}" target="_blank">Scholar</a>'


def _links_cell(title: str | None, doi: str | None) -> str:
    parts = [_scholar_link(title)]
    if doi:
        parts.append(_doi_link(doi))
    return '<td class="links">' + " &middot; ".join(p for p in parts if p) + "</td>"


def _flag_reason(paper: dict) -> str:
    """Human-readable flag reason from AI classification."""
    parts = []
    lang = paper.get("pdf_ai_language", "English")
    ctype = paper.get("pdf_ai_content_type", "full_manuscript")
    if lang != "English":
        parts.append(f"Non-English: {lang}")
    if ctype != "full_manuscript":
        label = ctype.replace("_", " ").title()
        parts.append(f"Content type: {label}")
    return "; ".join(parts) if parts else "—"


# ── Acquisition mode queries ──────────────────────────────────────────


def _query_acquisition_papers(conn: sqlite3.Connection) -> list[dict]:
    """Papers needing PDF acquisition (no PDF on disk, not excluded)."""
    rows = conn.execute("""
        SELECT p.id, p.ee_identifier, p.authors, p.year, p.title, p.doi,
               p.oa_status
        FROM papers p
        LEFT JOIN full_text_assets ft ON ft.paper_id = p.id
        WHERE p.status IN (
            'ABSTRACT_SCREENED_IN', 'AI_AUDIT_COMPLETE', 'PDF_ACQUIRED',
            'PARSED', 'FT_ELIGIBLE', 'EXTRACTED', 'HUMAN_AUDIT_COMPLETE'
        )
          AND COALESCE(ft.pdf_path, p.pdf_local_path) IS NULL
        ORDER BY p.ee_identifier
    """).fetchall()
    return [dict(r) for r in rows]


def _save_as_name(paper: dict) -> str:
    """Generate the save-as filename for a paper."""
    return f"{paper['id']}.pdf"


# ── Shared CSS ───────────────────────────────────────────────────────

_SHARED_CSS = """\
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
  body {{ font-family: 'IBM Plex Sans', system-ui, sans-serif; margin: 0; padding: 2rem; background: #EEF5F4; color: #2C2C2C; }}
  h1 {{ color: #0A5E56; margin-bottom: 0.25rem; }}
  h2 {{ color: #0A5E56; margin-top: 2rem; border-bottom: 2px solid #0A5E56; padding-bottom: 0.3rem; }}
  .subtitle {{ margin-top: 0; color: #666; font-size: 0.9rem; }}
  .section-desc {{ background: #fff; border-left: 4px solid #0A5E56; padding: 0.75rem 1rem; margin: 0.75rem 0 1rem; border-radius: 4px; font-size: 0.9rem; }}
  .summary-bar {{
    position: sticky; top: 0; z-index: 100;
    background: #0A5E56; color: #fff; padding: 0.6rem 1.25rem;
    border-radius: 6px; margin-bottom: 1.5rem;
    display: flex; justify-content: space-between; align-items: center;
    box-shadow: 0 2px 8px rgba(0,0,0,0.15); font-size: 0.9rem;
  }}
  .summary-bar .stat {{ margin: 0 1rem; }}
  .summary-bar .count {{ font-weight: 600; color: #B85D3A; font-size: 1.1rem; }}
  .summary-bar .count-ok {{ font-weight: 600; color: #7ee0a0; font-size: 1.1rem; }}
  .summary-bar .count-warn {{ font-weight: 600; color: #ffd080; font-size: 1.1rem; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 1rem; }}
  thead tr:first-child th:first-child {{ border-top-left-radius: 6px; }}
  thead tr:first-child th:last-child {{ border-top-right-radius: 6px; }}
  tbody tr:last-child td:first-child {{ border-bottom-left-radius: 6px; }}
  tbody tr:last-child td:last-child {{ border-bottom-right-radius: 6px; }}
  th {{ background: #0A5E56; color: #fff; padding: 0.6rem 0.75rem; text-align: left; font-size: 0.85rem; position: sticky; top: 44px; z-index: 10; }}
  td {{ padding: 0.5rem 0.75rem; border-bottom: 1px solid #e0e0e0; font-size: 0.85rem; vertical-align: top; }}
  tr:hover {{ background: #e8f0ef; }}
  tr.incomplete {{ background: #fff0f0 !important; border-left: 3px solid #B85D3A; }}
  tr.muted {{ opacity: 0.5; }}
  tr.muted:hover {{ opacity: 0.75; }}
  .ee {{ font-family: monospace; font-weight: 600; color: #0A5E56; white-space: nowrap; }}
  .doi {{ font-family: monospace; font-size: 0.8rem; color: #666; }}
  .flag {{ color: #B85D3A; font-weight: 600; }}
  .title-cell {{ max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  .links {{ white-space: nowrap; font-size: 0.82rem; }}
  .links a {{ margin: 0 0.15rem; }}
  a {{ color: #0A5E56; }}
  select {{ font-size: 0.82rem; padding: 3px 6px; border-radius: 4px; border: 1px solid #ccc; }}
  .detail-input {{ font-size: 0.82rem; padding: 3px 6px; border-radius: 4px; border: 1px solid #ccc; width: 180px; }}
  .save-as {{ font-family: monospace; font-weight: 600; color: #B85D3A; }}
  .btn {{
    display: inline-block; padding: 0.6rem 1.5rem; border-radius: 6px;
    font-weight: 600; font-size: 0.9rem; cursor: pointer; border: none;
    margin: 0.5rem 0.5rem 0.5rem 0; transition: opacity 0.15s;
  }}
  .btn:hover {{ opacity: 0.85; }}
  .btn-primary {{ background: #0A5E56; color: #fff; }}
  .btn-secondary {{ background: #666; color: #fff; }}
  .btn-accent {{ background: #B85D3A; color: #fff; }}
  .btn:disabled {{ opacity: 0.4; cursor: not-allowed; }}
  .button-bar {{ margin-top: 1.5rem; padding: 1rem 0; border-top: 2px solid #0A5E56; }}
  .validation-msg {{ color: #B85D3A; font-weight: 600; font-size: 0.9rem; margin: 0.5rem 0; display: none; }}"""


# ── Shared JS ────────────────────────────────────────────────────────

_SHARED_JS = """\
const STORAGE_KEY = 'pdf_' + MODE + '_' + REVIEW;
function saveState() {{
  const state = {{}};
  document.querySelectorAll('select.disposition').forEach(sel => {{
    const pid = sel.dataset.pid;
    const detail = document.querySelector('.detail-input[data-pid="' + pid + '"]');
    state[pid] = {{ value: sel.value, detail: detail ? detail.value : '' }};
  }});
  localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}}
function restoreState() {{
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) return;
  try {{
    const state = JSON.parse(raw);
    Object.entries(state).forEach(([pid, s]) => {{
      const sel = document.querySelector('select.disposition[data-pid="' + pid + '"]');
      if (!sel) return;
      if (sel.value) return;
      if (s.value) {{
        sel.value = s.value;
        onDisposition(sel, true);
        const detail = document.querySelector('.detail-input[data-pid="' + pid + '"]');
        if (detail && s.detail) detail.value = s.detail;
      }}
    }});
  }} catch(e) {{}}
}}
function onDisposition(sel, skipSave) {{
  const pid = sel.dataset.pid;
  const detail = document.querySelector('.detail-input[data-pid="' + pid + '"]');
  const row = document.getElementById('row-' + pid);
  const needsDetail = sel.value === 'EXCLUDE_OTHER';
  if (detail) detail.style.display = needsDetail ? 'block' : 'none';
  if (row && sel.value) row.classList.remove('incomplete');
  if (row) {{
    if (sel.value && sel.value !== '') {{ row.classList.add('muted'); }}
    else {{ row.classList.remove('muted'); }}
  }}
  if (!skipSave) saveState();
  updateCounts();
}}
function highlightIncomplete() {{
  let incomplete = [];
  document.querySelectorAll('select.disposition').forEach(sel => {{
    const pid = sel.dataset.pid;
    const row = document.getElementById('row-' + pid);
    if (!sel.value) {{
      incomplete.push(pid);
      if (row) row.classList.add('incomplete');
    }} else {{
      if (row) row.classList.remove('incomplete');
    }}
  }});
  return incomplete;
}}
function buildPapers() {{
  const papers = [];
  document.querySelectorAll('select.disposition').forEach(sel => {{
    const pid = parseInt(sel.dataset.pid);
    const val = sel.value;
    const detail = document.querySelector('.detail-input[data-pid="' + sel.dataset.pid + '"]');
    const detailVal = detail && detail.value.trim() ? detail.value.trim() : null;
    if (!val) {{
      papers.push({{ paper_id: pid, disposition: 'UNSET', exclude_reason: null, exclude_detail: null }});
    }} else if (val.startsWith('EXCLUDE')) {{
      const reason = val.replace('EXCLUDE_', '');
      papers.push({{ paper_id: pid, disposition: val, exclude_reason: reason, exclude_detail: detailVal }});
    }} else {{
      papers.push({{ paper_id: pid, disposition: val, exclude_reason: null, exclude_detail: null }});
    }}
  }});
  return papers;
}}
function exportJSON(isFinal) {{
  const incomplete = highlightIncomplete();
  if (incomplete.length > 0) {{
    const msg = document.getElementById('validationMsg');
    msg.textContent = incomplete.length + ' paper(s) still need a disposition. Highlighted in red above.';
    msg.style.display = 'block';
    const first = document.getElementById('row-' + incomplete[0]);
    if (first) first.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
    if (isFinal) return;
  }} else {{
    document.getElementById('validationMsg').style.display = 'none';
  }}
  const output = {{
    review: REVIEW, exported_at: new Date().toISOString(), mode: MODE,
    complete: isFinal, papers: buildPapers()
  }};
  const blob = new Blob([JSON.stringify(output, null, 2)], {{ type: 'application/json' }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  const stage = MODE === 'acquisition' ? 'pdf_acquisition' : 'pdf_quality';
  a.download = REVIEW + '_' + stage + '_decisions' + (isFinal ? '' : '_draft') + '.json';
  a.click();
  URL.revokeObjectURL(url);
}}"""


# ── Acquisition mode HTML ────────────────────────────────────────────


def generate_acquisition_html(
    review_name: str,
    output_path: str | None = None,
) -> tuple[Path, dict]:
    """Generate the acquisition download list HTML. Returns (path, stats)."""
    db_path = DATA_ROOT / review_name / "review.db"
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    papers = _query_acquisition_papers(conn)
    conn.close()

    total = len(papers)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows_html = []
    for p in papers:
        pid = p["id"]
        oa = _esc(p.get("oa_status") or "—")
        rows_html.append(f"""<tr id="row-{pid}" data-pid="{pid}">
  <td class="ee">{_esc(p['ee_identifier'])}</td>
  <td>{pid}</td>
  <td>{_esc(_first_author(p['authors']))}</td>
  <td>{_esc(str(p['year']) if p['year'] else '—')}</td>
  <td class="title-cell" title="{_esc(p['title'])}">{_esc(p['title'])}</td>
  {_links_cell(p['title'], p['doi'])}
  <td>{oa}</td>
  <td class="save-as">{_save_as_name(p)}</td>
  <td>
    <select class="disposition" data-pid="{pid}" onchange="onDisposition(this)">
      <option value="">— select —</option>
      <option value="ACQUIRED">Acquired</option>
      <option value="PDF_WILL_ATTEMPT">Will reattempt</option>
      <option value="EXCLUDE_NON_ENGLISH">Exclude: Non-English</option>
      <option value="EXCLUDE_NOT_MANUSCRIPT">Exclude: Not manuscript</option>
      <option value="EXCLUDE_INACCESSIBLE">Exclude: Inaccessible</option>
      <option value="EXCLUDE_OTHER">Exclude: Other…</option>
    </select>
    <input type="text" class="detail-input" data-pid="{pid}" placeholder="Detail…"
           style="display:none; margin-top:4px;" />
  </td>
</tr>""")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PDF Acquisition List — Surgical Evidence Engine</title>
<style>
{_SHARED_CSS}
</style>
</head>
<body>

<h1>PDF Acquisition List</h1>
<p class="subtitle">Surgical Evidence Engine — {review_name} &nbsp;|&nbsp; Generated {generated}</p>

<div class="summary-bar" id="summaryBar">
  <div>
    <span class="stat">Acquired: <span class="count-ok" id="countAcquired">0</span></span>
    <span class="stat">Will reattempt: <span style="font-weight:600" id="countReattempt">0</span></span>
    <span class="stat">Excluded: <span class="count" id="countExcluded">0</span></span>
    <span class="stat">Needs disposition: <span class="count-warn" id="countNeeds">{total}</span></span>
    <span class="stat">Total: <span style="font-weight:600">{total}</span></span>
  </div>
</div>

<div class="section-desc">
  Download each PDF and save to <code>data/{review_name}/pdfs/</code> using the filename in the
  <strong>Save As</strong> column. Then mark each paper's status: Acquired, Will Reattempt, or Exclude.
</div>

<table>
<thead><tr>
  <th>EE-ID</th><th>DB ID</th><th>First Author</th><th>Year</th><th>Title</th>
  <th>Links</th><th>OA</th><th>Save As</th><th>Disposition</th>
</tr></thead>
<tbody>
{''.join(rows_html)}
</tbody>
</table>

<div class="button-bar">
  <p class="validation-msg" id="validationMsg"></p>
  <button class="btn btn-secondary" onclick="exportJSON(false)">Save Draft</button>
  <button class="btn btn-accent" id="finalBtn" onclick="exportJSON(true)" disabled>
    Export Final
  </button>
</div>

<script>
const REVIEW = "{review_name}";
const MODE = "acquisition";

{_SHARED_JS}

function updateCounts() {{
  let acquired = 0, reattempt = 0, excluded = 0, needs = 0;
  document.querySelectorAll('select.disposition').forEach(sel => {{
    if (!sel.value) {{ needs++; }}
    else if (sel.value === 'ACQUIRED') {{ acquired++; }}
    else if (sel.value === 'PDF_WILL_ATTEMPT') {{ reattempt++; }}
    else {{ excluded++; }}
  }});
  document.getElementById('countAcquired').textContent = acquired;
  document.getElementById('countReattempt').textContent = reattempt;
  document.getElementById('countExcluded').textContent = excluded;
  document.getElementById('countNeeds').textContent = needs;
  document.getElementById('finalBtn').disabled = (needs > 0);
}}

// Init
restoreState();
updateCounts();
highlightIncomplete();
</script>
</body>
</html>"""

    if output_path:
        out = Path(output_path)
    else:
        out = review_artifact_path(
            DATA_ROOT / review_name, review_name,
            "pdf_acquisition", "queue", "html",
        )

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    logger.info("Wrote acquisition HTML: %s", out)

    return out, {"total": total}


# ── Quality check mode HTML ──────────────────────────────────────────


def generate_quality_html(
    review_name: str,
    mode: str = "quality_check",
    output_path: str | None = None,
) -> tuple[Path, dict]:
    """Generate the quality check HTML review page. Returns (path, stats)."""
    db_path = DATA_ROOT / review_name / "review.db"
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    s1 = _query_section1(conn)
    s2 = _query_section2(conn)
    s3 = _query_section3(conn)
    finalized = _query_finalized(conn)
    conn.close()

    # Total includes visible sections + finalized (already done, not shown)
    total_visible = len(s1) + len(s2) + len(s3)
    total_all = total_visible + finalized["confirmed"] + finalized["excluded"]
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build section 1 rows
    s1_rows = []
    for p in s1:
        pid = p["id"]
        s1_rows.append(f"""<tr id="row-{pid}" data-section="1" data-pid="{pid}">
  <td class="ee">{_esc(p['ee_identifier'])}</td>
  <td>{pid}</td>
  <td>{_esc(_first_author(p['authors']))}</td>
  <td>{_esc(str(p['year']) if p['year'] else '—')}</td>
  <td class="title-cell" title="{_esc(p['title'])}">{_esc(p['title'])}</td>
  {_links_cell(p['title'], p['doi'])}
  <td>
    <select class="disposition" data-pid="{pid}" onchange="onDisposition(this)">
      <option value="">— select —</option>
      <option value="PDF_WILL_ATTEMPT">PDF will be attempted</option>
      <option value="EXCLUDE_NON_ENGLISH">Non-English</option>
      <option value="EXCLUDE_NOT_MANUSCRIPT">Not a manuscript</option>
      <option value="EXCLUDE_INACCESSIBLE">Inaccessible</option>
      <option value="EXCLUDE_OTHER">Other…</option>
    </select>
    <input type="text" class="detail-input" data-pid="{pid}" placeholder="Detail…"
           style="display:none; margin-top:4px;" />
  </td>
</tr>""")

    # Build section 2 rows
    s2_rows = []
    for p in s2:
        pid = p["id"]
        conf = f'{p["pdf_ai_confidence"]:.2f}'
        flag = _flag_reason(p)
        s2_rows.append(f"""<tr id="row-{pid}" data-section="2" data-pid="{pid}">
  <td class="ee">{_esc(p['ee_identifier'])}</td>
  <td>{pid}</td>
  <td>{_esc(_first_author(p['authors']))}</td>
  <td>{_esc(str(p['year']) if p['year'] else '—')}</td>
  <td class="title-cell" title="{_esc(p['title'])}">{_esc(p['title'])}</td>
  {_links_cell(p['title'], p['doi'])}
  <td class="flag">{_esc(flag)}</td>
  <td>{conf}</td>
  <td>
    <select class="disposition" data-pid="{pid}" onchange="onDisposition(this)">
      <option value="">— select —</option>
      <option value="PROCEED">Proceed (AI wrong)</option>
      <option value="EXCLUDE_NON_ENGLISH">Exclude: Non-English</option>
      <option value="EXCLUDE_NOT_MANUSCRIPT">Exclude: Not a manuscript</option>
      <option value="EXCLUDE_OTHER">Exclude: Other…</option>
    </select>
    <input type="text" class="detail-input" data-pid="{pid}" placeholder="Detail…"
           style="display:none; margin-top:4px;" />
  </td>
</tr>""")

    # Build section 3 rows
    s3_rows = []
    for p in s3:
        pid = p["id"]
        s3_rows.append(f"""<tr data-section="3" data-pid="{pid}">
  <td class="ee">{_esc(p['ee_identifier'])}</td>
  <td>{pid}</td>
  <td>{_esc(_first_author(p['authors']))}</td>
  <td>{_esc(str(p['year']) if p['year'] else '—')}</td>
  <td class="title-cell" title="{_esc(p['title'])}">{_esc(p['title'])}</td>
  <td class="doi">{_doi_link(p['doi'])}</td>
</tr>""")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PDF Quality Review — Surgical Evidence Engine</title>
<style>
{_SHARED_CSS}
  .collapsible {{ cursor: pointer; user-select: none; }}
  .collapsible::before {{ content: '\\25B6 '; font-size: 0.8rem; }}
  .collapsible.open::before {{ content: '\\25BC '; }}
  .collapsed-content {{ display: none; }}
  .collapsed-content.open {{ display: block; }}
</style>
</head>
<body>

<h1>PDF Quality Review</h1>
<p class="subtitle">Surgical Evidence Engine — Autonomy in Surgical Robotics &nbsp;|&nbsp; Generated {generated}</p>

<div class="summary-bar" id="summaryBar">
  <div>
    <span class="stat">Ready to proceed: <span class="count-ok" id="countReady">0</span></span>
    <span class="stat">Needs disposition: <span class="count-warn" id="countNeeds">0</span></span>
    <span class="stat">Excluded: <span class="count" id="countExcluded">0</span></span>
    <span class="stat">Will attempt: <span style="font-weight:600" id="countAttempt">0</span></span>
    <span class="stat">Already finalized: <span style="font-weight:600">{finalized['confirmed'] + finalized['excluded']}</span></span>
    <span class="stat">Total: <span style="font-weight:600">{total_all}</span></span>
  </div>
</div>

<!-- ── Section 1: No PDFs ──────────────────────────────────────── -->
<h2>Section 1: Papers Without PDFs ({len(s1)})</h2>
<div class="section-desc">
  These papers have no PDF on disk. Select a disposition for each: mark for future PDF acquisition,
  or exclude with a reason.
</div>

<table id="table1">
<thead><tr>
  <th>EE-ID</th><th>DB ID</th><th>First Author</th><th>Year</th><th>Title</th><th>Links</th><th>Disposition</th>
</tr></thead>
<tbody>
{''.join(s1_rows)}
</tbody>
</table>

<!-- ── Section 2: AI Flagged ───────────────────────────────────── -->
<h2>Section 2: AI Quality Flags ({len(s2)})</h2>
<div class="section-desc">
  These papers were flagged by AI as non-English or non-manuscript. Review each classification
  and either <strong>Proceed</strong> (AI was wrong) or <strong>Exclude</strong> with a reason.
</div>

<table id="table2">
<thead><tr>
  <th>EE-ID</th><th>DB ID</th><th>First Author</th><th>Year</th><th>Title</th><th>Links</th>
  <th>Flag Reason</th><th>Conf</th><th>Disposition</th>
</tr></thead>
<tbody>
{''.join(s2_rows)}
</tbody>
</table>

<!-- ── Section 3: Passed ───────────────────────────────────────── -->
<h2 class="collapsible" onclick="toggleSection3()" id="s3header">
  Section 3: Passed AI Check ({len(s3)} + {finalized['confirmed']} confirmed) — No action needed
</h2>
<div class="collapsed-content" id="s3content">
<table id="table3">
<thead><tr>
  <th>EE-ID</th><th>DB ID</th><th>First Author</th><th>Year</th><th>Title</th><th>DOI</th>
</tr></thead>
<tbody>
{''.join(s3_rows)}
</tbody>
</table>
</div>

<!-- ── Buttons ─────────────────────────────────────────────────── -->
<div class="button-bar">
  <p class="validation-msg" id="validationMsg"></p>
  <button class="btn btn-secondary" onclick="exportJSON(false)">Save Draft</button>
  <button class="btn btn-accent" id="finalBtn" onclick="exportJSON(true)" disabled>
    Export Final
  </button>
</div>

<script>
const REVIEW = "{review_name}";
const MODE = "{mode}";
const S3_IDS = {json.dumps([p['id'] for p in s3])};
const FINALIZED_CONFIRMED = {finalized['confirmed']};
const FINALIZED_EXCLUDED = {finalized['excluded']};

{_SHARED_JS}

function updateCounts() {{
  let ready = S3_IDS.length + FINALIZED_CONFIRMED;
  let needs = 0, excluded = 0, attempt = 0;
  document.querySelectorAll('select.disposition').forEach(sel => {{
    if (!sel.value) {{ needs++; }}
    else if (sel.value === 'PROCEED') {{ ready++; }}
    else if (sel.value === 'PDF_WILL_ATTEMPT') {{ attempt++; }}
    else {{ excluded++; }}
  }});
  document.getElementById('countReady').textContent = ready;
  document.getElementById('countNeeds').textContent = needs;
  document.getElementById('countExcluded').textContent = excluded + FINALIZED_EXCLUDED;
  document.getElementById('countAttempt').textContent = attempt;
  document.getElementById('finalBtn').disabled = (needs > 0);
}}

function toggleSection3() {{
  document.getElementById('s3header').classList.toggle('open');
  document.getElementById('s3content').classList.toggle('open');
}}

restoreState();
updateCounts();
highlightIncomplete();
</script>

</body>
</html>"""

    if output_path:
        out = Path(output_path)
    else:
        out = review_artifact_path(
            DATA_ROOT / review_name, review_name,
            "pdf_quality", "queue", "html",
        )

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    logger.info("Wrote quality review HTML: %s", out)

    stats = {
        "section1": len(s1),
        "section2": len(s2),
        "section3": len(s3),
        "finalized_confirmed": finalized["confirmed"],
        "finalized_excluded": finalized["excluded"],
        "total": total_all,
    }
    return out, stats


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Generate HTML for PDF acquisition or quality check review"
    )
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument(
        "--mode", default="quality_check",
        choices=["acquisition", "quality_check"],
        help="Mode: 'acquisition' (download list) or 'quality_check' (post-download review)",
    )
    parser.add_argument("--output", default=None, help="Output file path")
    args = parser.parse_args()

    if args.mode == "acquisition":
        out, stats = generate_acquisition_html(
            review_name=args.review,
            output_path=args.output,
        )
        print(f"Generated: {out}")
        print(f"  Papers needing acquisition: {stats['total']}")
    else:
        out, stats = generate_quality_html(
            review_name=args.review,
            mode=args.mode,
            output_path=args.output,
        )
        print(f"Generated: {out}")
        print(f"  Section 1 (no PDF):      {stats['section1']}  (needs disposition)")
        print(f"  Section 2 (AI flagged):  {stats['section2']}  (needs disposition)")
        print(f"  Section 3 (passed):      {stats['section3']}")
        print(f"  Already finalized:       {stats['finalized_confirmed']} confirmed + {stats['finalized_excluded']} excluded")
        print(f"  Total:                   {stats['total']}")


if __name__ == "__main__":
    main()
