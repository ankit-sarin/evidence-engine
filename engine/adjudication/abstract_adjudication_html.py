"""Generate self-contained HTML adjudication page for ABSTRACT_SCREEN_FLAGGED papers.

Extends the ft_adjudication_html.py pattern with:
  - Auto-categorized FP badges per card (from adjudication_categories.yaml)
  - Category-based filtering (All / Pending / each FP category)
  - Batch "Exclude all in category [X]" action per category
  - Keyboard shortcuts: I = Include, E = Exclude, ↑/↓ = navigate cards

CLI:
    python -m engine.adjudication.abstract_adjudication_html --review surgical_autonomy
    python -m engine.adjudication.abstract_adjudication_html --review surgical_autonomy --output path/to/file.html
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

from engine.adjudication.categorizer import CategoryConfig, categorize_paper, load_config
from engine.core.database import DATA_ROOT

logger = logging.getLogger(__name__)

# Category badge colors (CSS class suffix → color)
_CATEGORY_COLORS = {
    "ambiguous": ("#FFE0E0", "#b71c1c"),
    "cv_perception": ("#E0F0FF", "#0d47a1"),
    "review_editorial": ("#E8E8E8", "#424242"),
    "hardware_sensing": ("#FFF0E0", "#e65100"),
    "planning_only": ("#E0FFE0", "#1b5e20"),
    "teleoperation_only": ("#F0E0FF", "#4a148c"),
    "rehabilitation_prosthetics": ("#FFE0F0", "#880e4f"),
    "industrial_nonmedical": ("#E0FFFF", "#006064"),
}


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


def _query_abstract_flagged(conn: sqlite3.Connection) -> list[dict]:
    """Collect ABSTRACT_SCREEN_FLAGGED papers with dual-model screening rationale."""
    papers = conn.execute(
        "SELECT id, title, authors, year, doi, abstract "
        "FROM papers WHERE status = 'ABSTRACT_SCREEN_FLAGGED' ORDER BY id"
    ).fetchall()

    results = []
    for p in papers:
        pid = p["id"]

        # Primary screening decisions (pass 1 and 2)
        rows = conn.execute(
            "SELECT pass_number, decision, rationale, model "
            "FROM abstract_screening_decisions WHERE paper_id = ? ORDER BY pass_number",
            (pid,),
        ).fetchall()

        primary_model = ""
        primary_decision = ""
        primary_rationale = ""
        for r in rows:
            if r["pass_number"] == 1:
                primary_decision = r["decision"]
                primary_rationale = r["rationale"] or ""
                primary_model = r["model"] or ""

        # Verification decision
        vrow = conn.execute(
            "SELECT model, decision, rationale "
            "FROM abstract_verification_decisions WHERE paper_id = ? "
            "ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()

        results.append({
            "paper_id": pid,
            "title": p["title"] or "",
            "authors": _truncate_authors(p["authors"]),
            "year": p["year"] or "",
            "doi": p["doi"] or "",
            "abstract": p["abstract"] or "",
            "primary_model": primary_model,
            "primary_decision": primary_decision,
            "primary_rationale": primary_rationale,
            "verifier_model": vrow["model"] if vrow else "",
            "verifier_decision": vrow["decision"] if vrow else "",
            "verifier_rationale": vrow["rationale"] if vrow else "",
        })

    return results


# ── HTML Generation ──────────────────────────────────────────────────


def generate_abstract_adjudication_html(
    review_name: str,
    output_path: str | None = None,
) -> tuple[Path, dict]:
    """Generate self-contained HTML for abstract screening adjudication.

    Returns (output_path, stats_dict).
    """
    db_path = DATA_ROOT / review_name / "review.db"
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    papers = _query_abstract_flagged(conn)
    conn.close()

    # Load category config and categorize papers
    cat_config = load_config(review_name=review_name)
    for p in papers:
        p["category"] = categorize_paper(p["title"], p["abstract"], config=cat_config)

    # Sort: ambiguous first, then by category
    papers.sort(key=lambda p: (0 if p["category"] == "ambiguous" else 1, p["category"], p["paper_id"]))

    total = len(papers)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Count by category
    cat_counts: dict[str, int] = {}
    for p in papers:
        cat = p["category"]
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    # Build category badge CSS
    cat_badge_css = []
    for cat_name, (bg, fg) in _CATEGORY_COLORS.items():
        cat_badge_css.append(f"  .cat-{cat_name} {{ background: {bg}; color: {fg}; }}")
    # Fallback for unknown categories
    cat_badge_css.append("  .cat-unknown { background: #f0f0f0; color: #666; }")

    # Build category filter buttons
    cat_filter_btns = ""
    for cat_name in sorted(cat_counts.keys()):
        cnt = cat_counts[cat_name]
        label = cat_name.replace("_", " ").title()
        cat_filter_btns += (
            f'<button class="filter-btn" '
            f'onclick="setFilter(\'cat_{cat_name}\')">{label} ({cnt})</button>\n'
        )

    # Build batch exclude buttons (one per non-ambiguous category)
    batch_btns_html = ""
    for cat_name in sorted(cat_counts.keys()):
        if cat_name == "ambiguous":
            continue
        cnt = cat_counts[cat_name]
        label = cat_name.replace("_", " ").title()
        batch_btns_html += (
            f'<button class="batch-btn" '
            f'onclick="batchExclude(\'{cat_name}\')">'
            f'Exclude all {label} ({cnt})</button>\n'
        )

    # Build paper cards HTML
    cards_html = []
    for p in papers:
        pid = p["paper_id"]
        cat = p["category"]
        cat_css = f"cat-{cat}" if cat in _CATEGORY_COLORS else "cat-unknown"
        cat_label = cat.replace("_", " ").title()
        links = " &middot; ".join(
            lnk for lnk in [_scholar_link(p["title"]), _doi_link(p["doi"])] if lnk
        )
        abstract_display = p["abstract"][:400]
        abstract_ellipsis = "&hellip;" if len(p["abstract"]) >= 400 else ""

        cards_html.append(f"""
<div class="card undecided" id="card-{pid}" data-pid="{pid}" data-status="undecided" data-category="{_esc(cat)}">
  <div class="card-header">
    <span class="paper-id">#{pid}</span>
    <span class="cat-badge {cat_css}">{_esc(cat_label)}</span>
    <span class="paper-meta">{_esc(p['authors'])} ({_esc(str(p['year']))})</span>
    <span class="card-links">{links}</span>
    <span class="card-badge" id="badge-{pid}">PENDING</span>
  </div>
  <h3 class="card-title">{_esc(p['title'])}</h3>
  <div class="abstract-excerpt">
    <strong>Abstract:</strong> {_esc(abstract_display)}{abstract_ellipsis}
  </div>
  <div class="panels">
    <div class="panel panel-primary">
      <div class="panel-header">Primary: {_esc(p['primary_model'])}</div>
      <div class="panel-field"><span class="field-label">Decision:</span> <span class="decision-val">{_esc(p['primary_decision'])}</span></div>
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

    # Empty state
    if not papers:
        cards_html.append("""
<div class="empty-state">
  <h2>No papers in adjudication queue</h2>
  <p>No papers at ABSTRACT_SCREEN_FLAGGED status found.
     Run abstract screening first, or all flagged papers have already been adjudicated.</p>
</div>""")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Abstract Screening Adjudication \u2014 Surgical Evidence Engine</title>
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
  .summary-bar .filter-bar {{ display: flex; gap: 0.4rem; flex-wrap: wrap; }}
  .filter-btn {{
    background: rgba(255,255,255,0.15); color: #fff; border: 1px solid rgba(255,255,255,0.3);
    padding: 0.3rem 0.7rem; border-radius: 4px; cursor: pointer;
    font-size: 0.82rem; font-family: 'IBM Plex Sans', sans-serif;
  }}
  .filter-btn:hover {{ background: rgba(255,255,255,0.25); }}
  .filter-btn.active {{ background: rgba(255,255,255,0.35); font-weight: 600; }}

  /* ── Batch actions ──────────────────────────────────── */
  .batch-bar {{
    margin-bottom: 1rem; display: flex; gap: 0.4rem; flex-wrap: wrap;
    padding: 0.5rem 0;
  }}
  .batch-btn {{
    background: #fbe9e7; color: #c62828; border: 1px solid #ef9a9a;
    padding: 0.35rem 0.8rem; border-radius: 4px; cursor: pointer;
    font-size: 0.8rem; font-weight: 500; font-family: 'IBM Plex Sans', sans-serif;
  }}
  .batch-btn:hover {{ background: #ffccbc; }}

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
  .card.focused {{ outline: 2px solid var(--forest-teal); outline-offset: -2px; }}
  .card-header {{
    display: flex; align-items: center; gap: 0.75rem;
    margin-bottom: 0.4rem; flex-wrap: wrap;
  }}
  .paper-id {{
    font-family: monospace; font-weight: 600; color: var(--forest-teal);
    font-size: 0.9rem;
  }}
  .cat-badge {{
    font-size: 0.72rem; font-weight: 600; padding: 0.15rem 0.5rem;
    border-radius: 3px; text-transform: uppercase; letter-spacing: 0.03em;
  }}
{chr(10).join(cat_badge_css)}
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
    border-radius: 4px; max-height: 8em; overflow-y: auto;
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

  /* ── Empty state ────────────────────────────────────── */
  .empty-state {{
    text-align: center; padding: 4rem 2rem; color: #888;
  }}
  .empty-state h2 {{ font-family: 'Fraunces', serif; color: var(--forest-teal); }}

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

  /* ── Keyboard hint ──────────────────────────────────── */
  .kbd-hint {{
    font-size: 0.78rem; color: #999; margin-top: 0.5rem;
  }}
  kbd {{
    background: #e0e0e0; border: 1px solid #ccc; border-radius: 3px;
    padding: 0.1rem 0.35rem; font-size: 0.75rem; font-family: monospace;
  }}
</style>
</head>
<body>

<h1>Abstract Screening Adjudication</h1>
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
    {cat_filter_btns}
  </div>
</div>

{'<div class="batch-bar">' + batch_btns_html + '</div>' if batch_btns_html else ''}

{''.join(cards_html)}

<div class="button-bar">
  <span class="validation-msg" id="validationMsg"></span>
  <button class="btn-action btn-draft" onclick="exportJSON(false)">Save Draft</button>
  <button class="btn-action btn-final" id="finalBtn" onclick="exportJSON(true)" disabled>Export Final</button>
</div>
<p class="kbd-hint">Keyboard: <kbd>I</kbd> Include &nbsp; <kbd>E</kbd> Exclude &nbsp; <kbd>\u2191</kbd><kbd>\u2193</kbd> Navigate</p>

<script>
const REVIEW = "{review_name}";
const STORAGE_KEY = "abstract_adjudication_" + REVIEW;
const decisions = {{}};
let focusedIdx = -1;

function getVisibleCards() {{
  return Array.from(document.querySelectorAll(".card:not(.hidden)"));
}}

function setFocus(idx) {{
  const cards = getVisibleCards();
  if (focusedIdx >= 0 && focusedIdx < cards.length) {{
    cards[focusedIdx].classList.remove("focused");
  }}
  focusedIdx = idx;
  if (idx >= 0 && idx < cards.length) {{
    cards[idx].classList.add("focused");
    cards[idx].scrollIntoView({{ behavior: "smooth", block: "center" }});
  }}
}}

function setDecision(pid, decision) {{
  decisions[pid] = decision;
  const card = document.getElementById("card-" + pid);
  const badge = document.getElementById("badge-" + pid);
  const incBtn = card.querySelector(".btn-include");
  const excBtn = card.querySelector(".btn-exclude");

  card.className = card.className.replace(/\\b(undecided|included|excluded|focused)\\b/g, "").trim();
  card.classList.add(decision === "INCLUDE" ? "included" : "excluded");
  if (focusedIdx >= 0) {{
    const visCards = getVisibleCards();
    if (visCards[focusedIdx] === card) card.classList.add("focused");
  }}
  badge.textContent = decision === "INCLUDE" ? "INCLUDED" : "EXCLUDED";
  incBtn.classList.toggle("active", decision === "INCLUDE");
  excBtn.classList.toggle("active", decision === "EXCLUDE");

  saveState();
  updateCounts();
  applyFilter();
}}

function batchExclude(category) {{
  document.querySelectorAll(".card").forEach(card => {{
    if (card.dataset.category === category && !decisions[card.dataset.pid]) {{
      setDecision(parseInt(card.dataset.pid), "EXCLUDE");
    }}
  }});
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
  document.querySelector('.filter-btn[onclick*="\\'" + filter + "\\'"]').classList.add("active");
  applyFilter();
  focusedIdx = -1;
}}

function applyFilter() {{
  document.querySelectorAll(".card").forEach(card => {{
    const pid = card.dataset.pid;
    const status = decisions[pid] === "INCLUDE" ? "included" :
                   decisions[pid] === "EXCLUDE" ? "excluded" : "undecided";
    card.dataset.status = status;
    const cat = card.dataset.category;
    let show = false;
    if (currentFilter === "all") show = true;
    else if (currentFilter === "pending") show = (status === "undecided");
    else if (currentFilter === "included") show = (status === "included");
    else if (currentFilter === "excluded") show = (status === "excluded");
    else if (currentFilter.startsWith("cat_")) show = (cat === currentFilter.slice(4));
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
        decision: dec === "INCLUDE" ? "ABSTRACT_SCREENED_IN" : "ABSTRACT_SCREENED_OUT",
        note: note && note.value.trim() ? note.value.trim() : null
      }});
    }}
  }});

  const blob = new Blob([JSON.stringify(output, null, 2)], {{ type: "application/json" }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = REVIEW + "_abstract_adjudication_decisions" + (isFinal ? "" : "_draft") + ".json";
  a.click();
  URL.revokeObjectURL(url);
}}

/* ── Keyboard shortcuts ──────────────────────────────── */
document.addEventListener("keydown", function(e) {{
  // Skip if user is typing in an input
  if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA") return;

  const cards = getVisibleCards();
  if (!cards.length) return;

  if (e.key === "ArrowDown") {{
    e.preventDefault();
    setFocus(Math.min(focusedIdx + 1, cards.length - 1));
  }} else if (e.key === "ArrowUp") {{
    e.preventDefault();
    setFocus(Math.max(focusedIdx - 1, 0));
  }} else if ((e.key === "i" || e.key === "I") && focusedIdx >= 0 && focusedIdx < cards.length) {{
    e.preventDefault();
    setDecision(parseInt(cards[focusedIdx].dataset.pid), "INCLUDE");
  }} else if ((e.key === "e" || e.key === "E") && focusedIdx >= 0 && focusedIdx < cards.length) {{
    e.preventDefault();
    setDecision(parseInt(cards[focusedIdx].dataset.pid), "EXCLUDE");
  }}
}});

// Init
restoreState();
updateCounts();
</script>
</body>
</html>"""

    if output_path:
        out = Path(output_path)
    else:
        out = DATA_ROOT / review_name / "abstract_adjudication_queue.html"

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    logger.info("Wrote abstract adjudication HTML: %s", out)

    return out, {"total": total, "categories": cat_counts}


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Generate HTML adjudication page for ABSTRACT_SCREEN_FLAGGED papers"
    )
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument("--output", default=None, help="Output file path")
    args = parser.parse_args()

    out, stats = generate_abstract_adjudication_html(
        review_name=args.review,
        output_path=args.output,
    )
    print(f"Generated: {out}")
    print(f"  ABSTRACT_SCREEN_FLAGGED papers: {stats['total']}")
    for cat, cnt in sorted(stats["categories"].items()):
        print(f"    {cat}: {cnt}")


if __name__ == "__main__":
    main()
