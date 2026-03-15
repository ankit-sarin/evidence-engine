"""Generate self-contained HTML adjudication page for FT_FLAGGED papers.

Follows the pdf_quality_html.py pattern: Python generates a single HTML file
with embedded data, inline CSS/JS, localStorage draft support, and JSON export.

CLI:
    python -m engine.adjudication.ft_adjudication_html --review surgical_autonomy
    python -m engine.adjudication.ft_adjudication_html --review surgical_autonomy --output path/to/file.html
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


# ── Helpers ──────────────────────────────────────────────────────────


def _esc(text: str | None) -> str:
    return html_mod.escape(str(text)) if text else "\u2014"


def _truncate_authors(authors: str | None) -> str:
    """First 3 authors + 'et al' if more."""
    if not authors:
        return "\u2014"
    parts = [a.strip() for a in authors.split(";") if a.strip()]
    if not parts:
        parts = [a.strip() for a in authors.split(",") if a.strip()]
    if len(parts) > 3:
        return "; ".join(parts[:3]) + " et al"
    return "; ".join(parts) if parts else "\u2014"


def _doi_link(doi: str | None) -> str:
    if not doi:
        return ""
    safe = _esc(doi)
    return f'<a href="https://doi.org/{safe}" target="_blank">DOI</a>'


def _scholar_link(title: str | None) -> str:
    if not title:
        return ""
    return (
        f'<a href="https://scholar.google.com/scholar?q={quote_plus(title)}" '
        f'target="_blank">Scholar</a>'
    )


# ── Data Query ───────────────────────────────────────────────────────


def _query_ft_flagged(conn: sqlite3.Connection) -> list[dict]:
    """Collect FT_FLAGGED papers with dual-model screening rationale."""
    papers = conn.execute(
        "SELECT id, title, authors, year, doi, abstract "
        "FROM papers WHERE status = 'FT_FLAGGED' ORDER BY id"
    ).fetchall()

    results = []
    for p in papers:
        pid = p["id"]

        # Primary screening decision
        ft_row = conn.execute(
            "SELECT model, decision, reason_code, rationale "
            "FROM ft_screening_decisions WHERE paper_id = ? "
            "ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()

        # Verification decision
        vrow = conn.execute(
            "SELECT model, decision, rationale "
            "FROM ft_verification_decisions WHERE paper_id = ? "
            "ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()

        results.append({
            "paper_id": pid,
            "title": p["title"] or "",
            "authors": _truncate_authors(p["authors"]),
            "year": p["year"] or "",
            "doi": p["doi"] or "",
            "abstract": (p["abstract"] or "")[:300],
            "primary_model": ft_row["model"] if ft_row else "",
            "primary_decision": ft_row["decision"] if ft_row else "",
            "primary_reason_code": ft_row["reason_code"] if ft_row else "",
            "primary_rationale": ft_row["rationale"] if ft_row else "",
            "verifier_model": vrow["model"] if vrow else "",
            "verifier_decision": vrow["decision"] if vrow else "",
            "verifier_rationale": vrow["rationale"] if vrow else "",
        })

    return results


# ── HTML Generation ──────────────────────────────────────────────────


def generate_ft_adjudication_html(
    review_name: str,
    output_path: str | None = None,
) -> tuple[Path, dict]:
    """Generate self-contained HTML for FT screening adjudication.

    Returns (output_path, stats_dict).
    """
    db_path = DATA_ROOT / review_name / "review.db"
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    papers = _query_ft_flagged(conn)
    conn.close()

    total = len(papers)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Embed paper data as JSON for JS access
    papers_json = json.dumps(papers, ensure_ascii=False)

    # Build paper cards HTML
    cards_html = []
    for p in papers:
        pid = p["paper_id"]
        links = " &middot; ".join(
            lnk for lnk in [_scholar_link(p["title"]), _doi_link(p["doi"])] if lnk
        )

        cards_html.append(f"""
<div class="card undecided" id="card-{pid}" data-pid="{pid}" data-status="undecided">
  <div class="card-header">
    <span class="paper-id">#{pid}</span>
    <span class="paper-meta">{_esc(p['authors'])} ({_esc(str(p['year']))})</span>
    <span class="card-links">{links}</span>
    <span class="card-badge" id="badge-{pid}">PENDING</span>
  </div>
  <h3 class="card-title">{_esc(p['title'])}</h3>
  <div class="abstract-excerpt">
    <strong>Abstract:</strong> {_esc(p['abstract'])}{'&hellip;' if len(p['abstract']) >= 300 else ''}
  </div>
  <div class="panels">
    <div class="panel panel-primary">
      <div class="panel-header">Primary: {_esc(p['primary_model'])}</div>
      <div class="panel-field"><span class="field-label">Decision:</span> <span class="decision-val">{_esc(p['primary_decision'])}</span></div>
      <div class="panel-field"><span class="field-label">Reason:</span> {_esc(p['primary_reason_code'])}</div>
      <div class="panel-rationale">{_esc(p['primary_rationale'])}</div>
    </div>
    <div class="panel panel-verifier">
      <div class="panel-header">Verifier: {_esc(p['verifier_model'])}</div>
      <div class="panel-field"><span class="field-label">Decision:</span> <span class="decision-val">{_esc(p['verifier_decision'])}</span></div>
      <div class="panel-rationale">{_esc(p['verifier_rationale'])}</div>
    </div>
  </div>
  <div class="decision-row">
    <button class="btn btn-include" onclick="setDecision({pid}, 'INCLUDE')">INCLUDE</button>
    <button class="btn btn-exclude" onclick="setDecision({pid}, 'EXCLUDE')">EXCLUDE</button>
    <input type="text" class="note-input" id="note-{pid}" placeholder="Optional note&hellip;"
           onchange="saveState()" />
  </div>
</div>""")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FT Screening Adjudication \u2014 Surgical Evidence Engine</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Fraunces:wght@600;700&family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root {{
    --forest-teal: #0A5E56;
    --terracotta: #B85D3A;
    --warm-charcoal: #2C2C2C;
    --mist-white: #EEF5F4;
    --mist-teal: #DFEBE9;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    font-family: 'IBM Plex Sans', system-ui, sans-serif;
    margin: 0; padding: 1.5rem 2rem;
    background: var(--mist-white); color: var(--warm-charcoal);
  }}
  h1 {{
    font-family: 'Fraunces', serif; font-weight: 700;
    color: var(--forest-teal); margin-bottom: 0.25rem; font-size: 1.8rem;
  }}
  .subtitle {{ margin-top: 0; color: #666; font-size: 0.9rem; }}

  /* ── Sticky summary bar ────────────────────────────── */
  .summary-bar {{
    position: sticky; top: 0; z-index: 100;
    background: var(--forest-teal); color: #fff;
    padding: 0.6rem 1.25rem; border-radius: 6px; margin-bottom: 1.5rem;
    display: flex; justify-content: space-between; align-items: center;
    box-shadow: 0 2px 8px rgba(0,0,0,0.15); font-size: 0.9rem;
    flex-wrap: wrap; gap: 0.5rem;
  }}
  .summary-bar .stats {{ display: flex; gap: 1.25rem; flex-wrap: wrap; }}
  .summary-bar .stat-label {{ opacity: 0.85; }}
  .summary-bar .count {{ font-weight: 600; font-size: 1.1rem; }}
  .count-pending {{ color: #ffd080; }}
  .count-include {{ color: #7ee0a0; }}
  .count-exclude {{ color: #ff9999; }}
  .summary-bar .filter-bar {{ display: flex; gap: 0.4rem; }}
  .filter-btn {{
    background: rgba(255,255,255,0.15); color: #fff; border: 1px solid rgba(255,255,255,0.3);
    padding: 0.3rem 0.7rem; border-radius: 4px; cursor: pointer;
    font-size: 0.82rem; font-family: 'IBM Plex Sans', sans-serif;
  }}
  .filter-btn:hover {{ background: rgba(255,255,255,0.25); }}
  .filter-btn.active {{ background: rgba(255,255,255,0.35); font-weight: 600; }}

  /* ── Paper cards ────────────────────────────────────── */
  .card {{
    background: #fff; border-radius: 8px; padding: 1.25rem;
    margin-bottom: 1rem; box-shadow: 0 1px 4px rgba(0,0,0,0.08);
    border-left: 4px solid #ccc; transition: opacity 0.2s, border-color 0.2s;
  }}
  .card.undecided {{ border-left-color: #ffd080; }}
  .card.included {{ border-left-color: var(--forest-teal); opacity: 0.7; }}
  .card.excluded {{ border-left-color: var(--terracotta); opacity: 0.7; }}
  .card.included:hover, .card.excluded:hover {{ opacity: 0.9; }}
  .card.hidden {{ display: none; }}
  .card-header {{
    display: flex; align-items: center; gap: 0.75rem;
    margin-bottom: 0.4rem; flex-wrap: wrap;
  }}
  .paper-id {{
    font-family: monospace; font-weight: 600; color: var(--forest-teal);
    font-size: 0.9rem;
  }}
  .paper-meta {{ font-size: 0.85rem; color: #666; }}
  .card-links {{ font-size: 0.82rem; }}
  .card-links a {{ color: var(--forest-teal); }}
  .card-badge {{
    margin-left: auto; font-size: 0.75rem; font-weight: 600;
    padding: 0.2rem 0.6rem; border-radius: 4px; text-transform: uppercase;
  }}
  .card.undecided .card-badge {{ background: #fff3e0; color: #e65100; }}
  .card.included .card-badge {{ background: #e8f5e9; color: #2e7d32; }}
  .card.excluded .card-badge {{ background: #fbe9e7; color: #c62828; }}
  .card-title {{
    font-family: 'Fraunces', serif; font-weight: 600;
    font-size: 1.05rem; margin: 0 0 0.5rem; color: var(--warm-charcoal);
  }}
  .abstract-excerpt {{
    font-size: 0.85rem; color: #555; margin-bottom: 0.75rem;
    line-height: 1.45; padding: 0.5rem; background: var(--mist-teal);
    border-radius: 4px;
  }}

  /* ── Side-by-side panels ────────────────────────────── */
  .panels {{ display: grid; grid-template-columns: 1fr 1fr; gap: 0.75rem; margin-bottom: 0.75rem; }}
  @media (max-width: 800px) {{ .panels {{ grid-template-columns: 1fr; }} }}
  .panel {{
    border: 1px solid #e0e0e0; border-radius: 6px; padding: 0.75rem;
    font-size: 0.85rem;
  }}
  .panel-primary {{ background: #f8fffe; }}
  .panel-verifier {{ background: #fdf8f6; }}
  .panel-header {{
    font-weight: 600; font-size: 0.8rem; text-transform: uppercase;
    letter-spacing: 0.03em; margin-bottom: 0.4rem; padding-bottom: 0.3rem;
    border-bottom: 1px solid #e0e0e0;
  }}
  .panel-primary .panel-header {{ color: var(--forest-teal); }}
  .panel-verifier .panel-header {{ color: var(--terracotta); }}
  .panel-field {{ margin-bottom: 0.3rem; }}
  .field-label {{ font-weight: 500; color: #888; font-size: 0.8rem; }}
  .panel-rationale {{
    font-size: 0.82rem; color: #444; line-height: 1.4;
    max-height: 6em; overflow-y: auto; margin-top: 0.3rem;
  }}

  /* ── Decision row ───────────────────────────────────── */
  .decision-row {{ display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap; }}
  .btn {{
    display: inline-block; padding: 0.5rem 1.4rem; border-radius: 6px;
    font-weight: 600; font-size: 0.85rem; cursor: pointer; border: 2px solid transparent;
    font-family: 'IBM Plex Sans', sans-serif; transition: all 0.15s;
  }}
  .btn:hover {{ opacity: 0.85; }}
  .btn-include {{ background: #e8f5e9; color: #2e7d32; border-color: #a5d6a7; }}
  .btn-include:hover {{ background: #c8e6c9; }}
  .btn-include.active {{ background: var(--forest-teal); color: #fff; border-color: var(--forest-teal); }}
  .btn-exclude {{ background: #fbe9e7; color: #c62828; border-color: #ef9a9a; }}
  .btn-exclude:hover {{ background: #ffccbc; }}
  .btn-exclude.active {{ background: var(--terracotta); color: #fff; border-color: var(--terracotta); }}
  .note-input {{
    flex: 1; min-width: 200px; padding: 0.45rem 0.6rem;
    border: 1px solid #ccc; border-radius: 4px;
    font-size: 0.82rem; font-family: 'IBM Plex Sans', sans-serif;
  }}

  /* ── Button bar ─────────────────────────────────────── */
  .button-bar {{
    margin-top: 1.5rem; padding: 1rem 0; border-top: 2px solid var(--forest-teal);
    display: flex; align-items: center; gap: 0.75rem; flex-wrap: wrap;
  }}
  .btn-action {{
    padding: 0.6rem 1.5rem; border-radius: 6px; font-weight: 600;
    font-size: 0.9rem; cursor: pointer; border: none;
    font-family: 'IBM Plex Sans', sans-serif;
  }}
  .btn-action:hover {{ opacity: 0.85; }}
  .btn-draft {{ background: #666; color: #fff; }}
  .btn-final {{ background: var(--terracotta); color: #fff; }}
  .btn-final:disabled {{ opacity: 0.4; cursor: not-allowed; }}
  .validation-msg {{ color: var(--terracotta); font-weight: 600; font-size: 0.9rem; display: none; }}
</style>
</head>
<body>

<h1>Full-Text Screening Adjudication</h1>
<p class="subtitle">Surgical Evidence Engine \u2014 {review_name} &nbsp;|&nbsp; Generated {generated} &nbsp;|&nbsp; {total} papers</p>

<div class="summary-bar">
  <div class="stats">
    <div><span class="stat-label">Pending:</span> <span class="count count-pending" id="countPending">{total}</span></div>
    <div><span class="stat-label">Included:</span> <span class="count count-include" id="countIncluded">0</span></div>
    <div><span class="stat-label">Excluded:</span> <span class="count count-exclude" id="countExcluded">0</span></div>
    <div><span class="stat-label">Total:</span> <span class="count" style="color:#fff">{total}</span></div>
  </div>
  <div class="filter-bar">
    <button class="filter-btn active" onclick="setFilter('all')">All</button>
    <button class="filter-btn" onclick="setFilter('pending')">Pending</button>
    <button class="filter-btn" onclick="setFilter('included')">Included</button>
    <button class="filter-btn" onclick="setFilter('excluded')">Excluded</button>
  </div>
</div>

{''.join(cards_html)}

<div class="button-bar">
  <span class="validation-msg" id="validationMsg"></span>
  <button class="btn-action btn-draft" onclick="exportJSON(false)">Save Draft</button>
  <button class="btn-action btn-final" id="finalBtn" onclick="exportJSON(true)" disabled>Export Final</button>
</div>

<script>
const REVIEW = "{review_name}";
const STORAGE_KEY = "ft_adjudication_" + REVIEW;
const decisions = {{}};

function setDecision(pid, decision) {{
  decisions[pid] = decision;
  const card = document.getElementById("card-" + pid);
  const badge = document.getElementById("badge-" + pid);
  const incBtn = card.querySelector(".btn-include");
  const excBtn = card.querySelector(".btn-exclude");

  card.className = "card " + (decision === "INCLUDE" ? "included" : "excluded");
  badge.textContent = decision === "INCLUDE" ? "INCLUDED" : "EXCLUDED";
  incBtn.classList.toggle("active", decision === "INCLUDE");
  excBtn.classList.toggle("active", decision === "EXCLUDE");

  saveState();
  updateCounts();
  applyFilter();
}}

function updateCounts() {{
  let pending = 0, included = 0, excluded = 0;
  document.querySelectorAll(".card").forEach(card => {{
    const pid = card.dataset.pid;
    if (!decisions[pid]) pending++;
    else if (decisions[pid] === "INCLUDE") included++;
    else excluded++;
  }});
  document.getElementById("countPending").textContent = pending;
  document.getElementById("countIncluded").textContent = included;
  document.getElementById("countExcluded").textContent = excluded;
  document.getElementById("finalBtn").disabled = (pending > 0);
}}

let currentFilter = "all";
function setFilter(filter) {{
  currentFilter = filter;
  document.querySelectorAll(".filter-btn").forEach(b => b.classList.remove("active"));
  document.querySelector('.filter-btn[onclick*="' + filter + '"]').classList.add("active");
  applyFilter();
}}

function applyFilter() {{
  document.querySelectorAll(".card").forEach(card => {{
    const status = card.dataset.status =
      decisions[card.dataset.pid] === "INCLUDE" ? "included" :
      decisions[card.dataset.pid] === "EXCLUDE" ? "excluded" : "undecided";
    const show = currentFilter === "all" ||
      (currentFilter === "pending" && status === "undecided") ||
      (currentFilter === "included" && status === "included") ||
      (currentFilter === "excluded" && status === "excluded");
    card.classList.toggle("hidden", !show);
  }});
}}

function saveState() {{
  const state = {{}};
  document.querySelectorAll(".card").forEach(card => {{
    const pid = card.dataset.pid;
    const note = document.getElementById("note-" + pid);
    state[pid] = {{ decision: decisions[pid] || null, note: note ? note.value : "" }};
  }});
  localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}}

function restoreState() {{
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) return;
  try {{
    const state = JSON.parse(raw);
    Object.entries(state).forEach(([pid, s]) => {{
      if (s.decision) {{
        decisions[pid] = s.decision;
        setDecision(parseInt(pid), s.decision);
      }}
      if (s.note) {{
        const note = document.getElementById("note-" + pid);
        if (note) note.value = s.note;
      }}
    }});
  }} catch(e) {{}}
}}

function exportJSON(isFinal) {{
  if (isFinal) {{
    let pending = 0;
    document.querySelectorAll(".card").forEach(card => {{
      if (!decisions[card.dataset.pid]) {{
        pending++;
        card.style.outline = "3px solid var(--terracotta)";
        card.style.outlineOffset = "-3px";
      }} else {{
        card.style.outline = "none";
      }}
    }});
    if (pending > 0) {{
      const msg = document.getElementById("validationMsg");
      msg.textContent = pending + " paper(s) still need a decision. Highlighted above.";
      msg.style.display = "block";
      const first = document.querySelector(".card:not(.included):not(.excluded)");
      if (first) first.scrollIntoView({{ behavior: "smooth", block: "center" }});
      return;
    }}
  }}
  document.getElementById("validationMsg").style.display = "none";

  const output = [];
  document.querySelectorAll(".card").forEach(card => {{
    const pid = parseInt(card.dataset.pid);
    const note = document.getElementById("note-" + pid);
    const dec = decisions[pid];
    if (dec) {{
      output.push({{
        paper_id: pid,
        decision: dec === "INCLUDE" ? "FT_ELIGIBLE" : "FT_SCREENED_OUT",
        note: note && note.value.trim() ? note.value.trim() : null
      }});
    }}
  }});

  const blob = new Blob([JSON.stringify(output, null, 2)], {{ type: "application/json" }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = REVIEW + "_ft_adjudication_decisions" + (isFinal ? "" : "_draft") + ".json";
  a.click();
  URL.revokeObjectURL(url);
}}

// Init
restoreState();
updateCounts();
</script>
</body>
</html>"""

    if output_path:
        out = Path(output_path)
    else:
        out = review_artifact_path(
            DATA_ROOT / review_name, review_name,
            "ft_adjudication", "queue", "html",
        )

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    logger.info("Wrote FT adjudication HTML: %s", out)

    return out, {"total": total}


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Generate HTML adjudication page for FT_FLAGGED papers"
    )
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument("--output", default=None, help="Output file path")
    args = parser.parse_args()

    out, stats = generate_ft_adjudication_html(
        review_name=args.review,
        output_path=args.output,
    )
    print(f"Generated: {out}")
    print(f"  FT_FLAGGED papers: {stats['total']}")


if __name__ == "__main__":
    main()
