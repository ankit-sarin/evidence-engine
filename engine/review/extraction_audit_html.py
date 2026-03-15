"""Generate self-contained HTML for human review of extraction audit results.

Per-span decisions (ACCEPT / REJECT / CORRECT) grouped by paper in collapsible
cards. Follows the ft_adjudication_html.py pattern: inline CSS/JS, localStorage
draft, JSON export.

CLI:
    python -m engine.review.extraction_audit_html --review surgical_autonomy
    python -m engine.review.extraction_audit_html --review surgical_autonomy --output path/to/file.html
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

# Audit states eligible for human review, in display sort order
_AUDIT_STATES = ["flagged", "contested", "invalid_snippet", "low_yield", "verified"]
_AUDIT_STATE_ORDER = {s: i for i, s in enumerate(_AUDIT_STATES)}


# ── Helpers ──────────────────────────────────────────────────────────


def _esc(text: str | None) -> str:
    return html_mod.escape(str(text)) if text else "\u2014"


def _truncate_authors(authors: str | None) -> str:
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


def _query_review_spans(conn: sqlite3.Connection) -> list[dict]:
    """Collect all reviewable spans joined with paper and extraction info.

    Returns spans for papers at AI_AUDIT_COMPLETE with audit_status in
    (contested, flagged, invalid_snippet, low_yield) plus optional
    spot-check verified spans.
    """
    rows = conn.execute("""
        SELECT
            es.id          AS span_id,
            e.paper_id,
            p.title,
            p.authors,
            p.year,
            p.doi,
            p.abstract,
            es.field_name,
            es.value       AS extracted_value,
            es.source_snippet,
            es.confidence,
            es.audit_status,
            es.audit_rationale
        FROM evidence_spans es
        JOIN extractions e ON es.extraction_id = e.id
        JOIN papers p ON e.paper_id = p.id
        WHERE p.status = 'AI_AUDIT_COMPLETE'
          AND es.audit_status IN ('contested', 'flagged', 'invalid_snippet', 'low_yield')
        ORDER BY e.paper_id, es.id
    """).fetchall()
    return [dict(r) for r in rows]


def _group_by_paper(spans: list[dict]) -> list[dict]:
    """Group flat span list into paper-centric structure.

    Returns list of paper dicts, each with a 'spans' list sorted by
    audit_state priority then field_name.
    """
    paper_map: dict[int, dict] = {}
    for s in spans:
        pid = s["paper_id"]
        if pid not in paper_map:
            paper_map[pid] = {
                "paper_id": pid,
                "title": s["title"],
                "authors": _truncate_authors(s["authors"]),
                "year": s["year"],
                "doi": s["doi"],
                "abstract": (s["abstract"] or "")[:300],
                "spans": [],
            }
        paper_map[pid]["spans"].append({
            "span_id": s["span_id"],
            "field_name": s["field_name"],
            "extracted_value": s["extracted_value"],
            "source_snippet": s["source_snippet"] or "",
            "confidence": s["confidence"],
            "audit_status": s["audit_status"],
            "audit_rationale": s["audit_rationale"] or "",
        })

    # Sort spans within each paper by audit state priority then field name
    for p in paper_map.values():
        p["spans"].sort(key=lambda s: (
            _AUDIT_STATE_ORDER.get(s["audit_status"], 99),
            s["field_name"],
        ))

    # Sort papers by paper_id
    return sorted(paper_map.values(), key=lambda p: p["paper_id"])


# ── Badge helpers ────────────────────────────────────────────────────


def _state_badge_class(state: str) -> str:
    return {
        "flagged": "badge-flagged",
        "contested": "badge-contested",
        "invalid_snippet": "badge-invalid",
        "low_yield": "badge-lowyield",
        "verified": "badge-verified",
    }.get(state, "badge-flagged")


# ── HTML Generation ──────────────────────────────────────────────────


def generate_extraction_audit_html(
    review_name: str,
    output_path: str | None = None,
) -> tuple[Path, dict]:
    """Generate self-contained HTML for extraction audit review.

    Returns (output_path, stats_dict).
    """
    db_path = DATA_ROOT / review_name / "review.db"
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    raw_spans = _query_review_spans(conn)
    conn.close()

    papers = _group_by_paper(raw_spans)
    total_spans = sum(len(p["spans"]) for p in papers)
    total_papers = len(papers)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Count by audit state
    state_counts: dict[str, int] = {}
    for s in raw_spans:
        st = s["audit_status"]
        state_counts[st] = state_counts.get(st, 0) + 1

    # Build paper cards
    cards_html = []
    for p in papers:
        pid = p["paper_id"]
        n_spans = len(p["spans"])
        links = " &middot; ".join(
            lnk for lnk in [_scholar_link(p["title"]), _doi_link(p["doi"])] if lnk
        )

        # Paper-level state summary
        paper_states: dict[str, int] = {}
        for sp in p["spans"]:
            st = sp["audit_status"]
            paper_states[st] = paper_states.get(st, 0) + 1
        state_summary = ", ".join(
            f'{c} {s.upper()}' for s, c in sorted(
                paper_states.items(),
                key=lambda x: _AUDIT_STATE_ORDER.get(x[0], 99),
            )
        )

        # Span rows
        span_rows = []
        for sp in p["spans"]:
            sid = sp["span_id"]
            badge_cls = _state_badge_class(sp["audit_status"])
            conf = f'{sp["confidence"]:.2f}' if sp["confidence"] is not None else "\u2014"

            span_rows.append(f"""
      <div class="span-row" id="span-{sid}" data-sid="{sid}" data-pid="{pid}" data-status="undecided">
        <div class="span-header">
          <span class="field-name">{_esc(sp['field_name'])}</span>
          <span class="badge {badge_cls}">{_esc(sp['audit_status'].upper())}</span>
          <span class="conf">conf: {conf}</span>
          <span class="span-badge" id="sbadge-{sid}">PENDING</span>
        </div>
        <div class="span-body">
          <div class="span-field"><span class="sf-label">Extracted:</span>
            <span class="sf-value">{_esc(sp['extracted_value'])}</span>
          </div>
          <div class="span-field"><span class="sf-label">Source snippet:</span>
            <div class="snippet-box">{_esc(sp['source_snippet'])}</div>
          </div>
          <div class="span-field"><span class="sf-label">Audit reasoning:</span>
            <div class="rationale-box">{_esc(sp['audit_rationale'])}</div>
          </div>
        </div>
        <div class="span-decision">
          <button class="sbtn sbtn-accept" onclick="setSpanDecision({sid}, 'ACCEPT')">ACCEPT</button>
          <button class="sbtn sbtn-reject" onclick="setSpanDecision({sid}, 'REJECT')">REJECT</button>
          <button class="sbtn sbtn-correct" onclick="setSpanDecision({sid}, 'CORRECT')">CORRECT</button>
          <input type="text" class="correct-input" id="corr-{sid}"
                 placeholder="Corrected value (required)&hellip;"
                 style="display:none;" onchange="saveState()" />
          <input type="text" class="note-input" id="note-{sid}"
                 placeholder="Note (optional)&hellip;" onchange="saveState()" />
        </div>
      </div>""")

        cards_html.append(f"""
<div class="paper-card" id="pcard-{pid}" data-pid="{pid}">
  <div class="paper-header" onclick="toggleCard({pid})">
    <div class="paper-header-left">
      <span class="paper-id">#{pid}</span>
      <span class="paper-title-text">{_esc(p['title'])}</span>
    </div>
    <div class="paper-header-right">
      <span class="paper-states">{state_summary}</span>
      <span class="paper-progress" id="pprog-{pid}">0/{n_spans} decided</span>
      <span class="toggle-icon" id="toggle-{pid}">\u25B6</span>
    </div>
  </div>
  <div class="paper-meta-row">
    <span>{_esc(p['authors'])} ({_esc(str(p['year']))})</span>
    <span class="card-links">{links}</span>
  </div>
  <div class="paper-body" id="pbody-{pid}" style="display:none;">
    {''.join(span_rows)}
  </div>
</div>""")

    # Empty state
    if not papers:
        cards_html.append("""
<div class="empty-state">
  <h2>No spans in audit queue</h2>
  <p>No papers at AI_AUDIT_COMPLETE status have contested, flagged, or low-yield spans.
     Run the extraction and audit pipeline first.</p>
</div>""")

    # State filter buttons
    state_filter_btns = ""
    for st in ["flagged", "contested", "invalid_snippet", "low_yield"]:
        cnt = state_counts.get(st, 0)
        if cnt > 0:
            label = st.upper().replace("_", " ")
            state_filter_btns += (
                f'<button class="filter-btn" '
                f'onclick="setFilter(\'{st}\')">{label} ({cnt})</button>\n'
            )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Extraction Audit Review \u2014 Surgical Evidence Engine</title>
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

  /* ── Sticky summary bar ──────────────────────────── */
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
  .count-accepted {{ color: #7ee0a0; }}
  .count-rejected {{ color: #ff9999; }}
  .count-corrected {{ color: #99ccff; }}
  .summary-bar .filter-bar {{ display: flex; gap: 0.4rem; flex-wrap: wrap; }}
  .filter-btn {{
    background: rgba(255,255,255,0.15); color: #fff; border: 1px solid rgba(255,255,255,0.3);
    padding: 0.3rem 0.7rem; border-radius: 4px; cursor: pointer;
    font-size: 0.82rem; font-family: 'IBM Plex Sans', sans-serif;
  }}
  .filter-btn:hover {{ background: rgba(255,255,255,0.25); }}
  .filter-btn.active {{ background: rgba(255,255,255,0.35); font-weight: 600; }}

  /* ── Paper cards ─────────────────────────────────── */
  .paper-card {{
    background: #fff; border-radius: 8px; margin-bottom: 0.75rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.08); overflow: hidden;
  }}
  .paper-card.hidden {{ display: none; }}
  .paper-header {{
    display: flex; justify-content: space-between; align-items: center;
    padding: 0.75rem 1rem; cursor: pointer; user-select: none;
    background: var(--mist-teal); border-bottom: 1px solid #d0e0dd;
    gap: 0.5rem;
  }}
  .paper-header:hover {{ background: #d0e5e2; }}
  .paper-header-left {{ display: flex; align-items: center; gap: 0.6rem; min-width: 0; flex: 1; }}
  .paper-header-right {{ display: flex; align-items: center; gap: 0.75rem; flex-shrink: 0; }}
  .paper-id {{ font-family: monospace; font-weight: 600; color: var(--forest-teal); font-size: 0.9rem; flex-shrink: 0; }}
  .paper-title-text {{
    font-family: 'Fraunces', serif; font-weight: 600; font-size: 0.95rem;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }}
  .paper-states {{ font-size: 0.8rem; color: #666; white-space: nowrap; }}
  .paper-progress {{ font-size: 0.8rem; font-weight: 600; color: var(--forest-teal); white-space: nowrap; }}
  .toggle-icon {{ font-size: 0.75rem; color: #888; }}
  .paper-meta-row {{
    padding: 0.3rem 1rem; font-size: 0.82rem; color: #666;
    display: flex; justify-content: space-between; align-items: center;
    border-bottom: 1px solid #eee;
  }}
  .card-links a {{ color: var(--forest-teal); font-size: 0.82rem; }}
  .paper-body {{ padding: 0.5rem 1rem 1rem; }}

  /* ── Span rows ───────────────────────────────────── */
  .span-row {{
    border: 1px solid #e8e8e8; border-radius: 6px; padding: 0.75rem;
    margin-bottom: 0.5rem; transition: border-color 0.2s, opacity 0.2s;
  }}
  .span-row.decided {{ opacity: 0.65; }}
  .span-row.decided:hover {{ opacity: 0.85; }}
  .span-row.decided-accept {{ border-left: 4px solid var(--forest-teal); }}
  .span-row.decided-reject {{ border-left: 4px solid var(--terracotta); }}
  .span-row.decided-correct {{ border-left: 4px solid #3a7ab8; }}
  .span-row.hidden {{ display: none; }}
  .span-header {{
    display: flex; align-items: center; gap: 0.6rem; margin-bottom: 0.4rem; flex-wrap: wrap;
  }}
  .field-name {{ font-weight: 600; font-size: 0.9rem; color: var(--warm-charcoal); font-family: monospace; }}
  .badge {{
    font-size: 0.7rem; font-weight: 600; padding: 0.15rem 0.5rem;
    border-radius: 3px; text-transform: uppercase; letter-spacing: 0.03em;
  }}
  .badge-flagged {{ background: #fff3e0; color: #e65100; }}
  .badge-contested {{ background: #fce4ec; color: #c62828; }}
  .badge-invalid {{ background: #f3e5f5; color: #7b1fa2; }}
  .badge-lowyield {{ background: #e8eaf6; color: #283593; }}
  .badge-verified {{ background: #e8f5e9; color: #2e7d32; }}
  .conf {{ font-size: 0.8rem; color: #888; }}
  .span-badge {{
    margin-left: auto; font-size: 0.7rem; font-weight: 600;
    padding: 0.15rem 0.5rem; border-radius: 3px;
    background: #f5f5f5; color: #999;
  }}
  .span-badge.sb-accept {{ background: #e8f5e9; color: #2e7d32; }}
  .span-badge.sb-reject {{ background: #fbe9e7; color: #c62828; }}
  .span-badge.sb-correct {{ background: #e3f2fd; color: #1565c0; }}

  .span-body {{ margin-bottom: 0.5rem; }}
  .span-field {{ margin-bottom: 0.3rem; font-size: 0.85rem; }}
  .sf-label {{ font-weight: 500; color: #888; font-size: 0.8rem; }}
  .sf-value {{ color: var(--warm-charcoal); }}
  .snippet-box, .rationale-box {{
    font-size: 0.82rem; color: #444; line-height: 1.4;
    max-height: 5em; overflow-y: auto; padding: 0.4rem 0.5rem;
    background: var(--mist-teal); border-radius: 4px; margin-top: 0.15rem;
  }}
  .rationale-box {{ background: #fdf8f6; }}

  /* ── Span decision buttons ───────────────────────── */
  .span-decision {{
    display: flex; align-items: center; gap: 0.4rem; flex-wrap: wrap;
  }}
  .sbtn {{
    padding: 0.35rem 1rem; border-radius: 5px; font-weight: 600;
    font-size: 0.8rem; cursor: pointer; border: 2px solid transparent;
    font-family: 'IBM Plex Sans', sans-serif; transition: all 0.15s;
  }}
  .sbtn:hover {{ opacity: 0.85; }}
  .sbtn-accept {{ background: #e8f5e9; color: #2e7d32; border-color: #a5d6a7; }}
  .sbtn-accept.active {{ background: var(--forest-teal); color: #fff; border-color: var(--forest-teal); }}
  .sbtn-reject {{ background: #fbe9e7; color: #c62828; border-color: #ef9a9a; }}
  .sbtn-reject.active {{ background: var(--terracotta); color: #fff; border-color: var(--terracotta); }}
  .sbtn-correct {{ background: #e3f2fd; color: #1565c0; border-color: #90caf9; }}
  .sbtn-correct.active {{ background: #1565c0; color: #fff; border-color: #1565c0; }}
  .correct-input {{
    padding: 0.35rem 0.5rem; border: 1px solid #90caf9; border-radius: 4px;
    font-size: 0.82rem; font-family: 'IBM Plex Sans', sans-serif;
    min-width: 200px; flex: 1;
  }}
  .correct-input.error {{ border-color: var(--terracotta); background: #fff5f5; }}
  .note-input {{
    padding: 0.35rem 0.5rem; border: 1px solid #ccc; border-radius: 4px;
    font-size: 0.82rem; font-family: 'IBM Plex Sans', sans-serif;
    min-width: 150px; max-width: 250px;
  }}

  /* ── Empty state ─────────────────────────────────── */
  .empty-state {{
    text-align: center; padding: 4rem 2rem; color: #888;
  }}
  .empty-state h2 {{ font-family: 'Fraunces', serif; color: var(--forest-teal); }}

  /* ── Button bar ──────────────────────────────────── */
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
  .validation-msg {{
    color: var(--terracotta); font-weight: 600; font-size: 0.9rem;
    display: none; flex-basis: 100%;
  }}
</style>
</head>
<body>

<h1>Extraction Audit Review</h1>
<p class="subtitle">Surgical Evidence Engine \u2014 {review_name} &nbsp;|&nbsp; Generated {generated}
  &nbsp;|&nbsp; {total_spans} spans across {total_papers} papers</p>

<div class="summary-bar">
  <div class="stats">
    <div><span class="stat-label">Pending:</span> <span class="count count-pending" id="cntPending">{total_spans}</span></div>
    <div><span class="stat-label">Accepted:</span> <span class="count count-accepted" id="cntAccepted">0</span></div>
    <div><span class="stat-label">Rejected:</span> <span class="count count-rejected" id="cntRejected">0</span></div>
    <div><span class="stat-label">Corrected:</span> <span class="count count-corrected" id="cntCorrected">0</span></div>
    <div><span class="stat-label">Total:</span> <span class="count" style="color:#fff">{total_spans}</span></div>
  </div>
  <div class="filter-bar">
    <button class="filter-btn active" onclick="setFilter('all')">All</button>
    <button class="filter-btn" onclick="setFilter('pending')">Pending</button>
    {state_filter_btns}
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
const STORAGE_KEY = "extraction_audit_" + REVIEW;
const decisions = {{}};  // span_id -> {{ decision, corrected_value }}

/* ── Card toggle ──────────────────────────────── */
function toggleCard(pid) {{
  const body = document.getElementById("pbody-" + pid);
  const icon = document.getElementById("toggle-" + pid);
  if (body.style.display === "none") {{
    body.style.display = "block";
    icon.textContent = "\u25BC";
  }} else {{
    body.style.display = "none";
    icon.textContent = "\u25B6";
  }}
}}

/* ── Span decision ────────────────────────────── */
function setSpanDecision(sid, decision) {{
  const row = document.getElementById("span-" + sid);
  const badge = document.getElementById("sbadge-" + sid);
  const corrInput = document.getElementById("corr-" + sid);
  const btns = row.querySelectorAll(".sbtn");

  // Toggle off if clicking the same decision again
  if (decisions[sid] && decisions[sid].decision === decision) {{
    delete decisions[sid];
    row.className = "span-row";
    row.dataset.status = "undecided";
    badge.textContent = "PENDING";
    badge.className = "span-badge";
    btns.forEach(b => b.classList.remove("active"));
    if (corrInput) corrInput.style.display = "none";
    saveState(); updateCounts(); return;
  }}

  decisions[sid] = {{ decision: decision, corrected_value: null }};

  row.className = "span-row decided decided-" + decision.toLowerCase();
  row.dataset.status = decision.toLowerCase();
  badge.textContent = decision === "ACCEPT" ? "ACCEPTED" : decision === "REJECT" ? "REJECTED" : "CORRECTED";
  badge.className = "span-badge sb-" + decision.toLowerCase();

  btns.forEach(b => {{
    const type = b.classList.contains("sbtn-accept") ? "ACCEPT"
               : b.classList.contains("sbtn-reject") ? "REJECT" : "CORRECT";
    b.classList.toggle("active", type === decision);
  }});

  if (corrInput) {{
    corrInput.style.display = decision === "CORRECT" ? "inline-block" : "none";
    if (decision === "CORRECT") corrInput.focus();
  }}

  saveState();
  updateCounts();
}}

/* ── Counts ────────────────────────────────────── */
function updateCounts() {{
  let pending = 0, accepted = 0, rejected = 0, corrected = 0;
  document.querySelectorAll(".span-row").forEach(row => {{
    const sid = row.dataset.sid;
    const d = decisions[sid];
    if (!d) pending++;
    else if (d.decision === "ACCEPT") accepted++;
    else if (d.decision === "REJECT") rejected++;
    else if (d.decision === "CORRECT") corrected++;
  }});
  document.getElementById("cntPending").textContent = pending;
  document.getElementById("cntAccepted").textContent = accepted;
  document.getElementById("cntRejected").textContent = rejected;
  document.getElementById("cntCorrected").textContent = corrected;
  document.getElementById("finalBtn").disabled = (pending > 0);

  // Update per-paper progress
  document.querySelectorAll(".paper-card").forEach(card => {{
    const pid = card.dataset.pid;
    const spans = card.querySelectorAll(".span-row");
    let done = 0;
    spans.forEach(s => {{ if (decisions[s.dataset.sid]) done++; }});
    const el = document.getElementById("pprog-" + pid);
    if (el) el.textContent = done + "/" + spans.length + " decided";
  }});
}}

/* ── Filter ────────────────────────────────────── */
let currentFilter = "all";
function setFilter(filter) {{
  currentFilter = filter;
  document.querySelectorAll(".filter-btn").forEach(b => b.classList.remove("active"));
  document.querySelector('.filter-btn[onclick*="\\'" + filter + "\\'"]').classList.add("active");
  applyFilter();
}}

function applyFilter() {{
  document.querySelectorAll(".paper-card").forEach(card => {{
    const spans = card.querySelectorAll(".span-row");
    let anyVisible = false;
    spans.forEach(row => {{
      const sid = row.dataset.sid;
      const d = decisions[sid];
      const auditState = row.querySelector(".badge") ?
        row.querySelector(".badge").textContent.toLowerCase().replace(" ", "_") : "";
      let show = false;
      if (currentFilter === "all") show = true;
      else if (currentFilter === "pending") show = !d;
      else show = (auditState === currentFilter);
      row.classList.toggle("hidden", !show);
      if (show) anyVisible = true;
    }});
    card.classList.toggle("hidden", !anyVisible);
  }});
}}

/* ── Persistence ───────────────────────────────── */
function saveState() {{
  const state = {{}};
  document.querySelectorAll(".span-row").forEach(row => {{
    const sid = row.dataset.sid;
    const corr = document.getElementById("corr-" + sid);
    const note = document.getElementById("note-" + sid);
    const d = decisions[sid];
    state[sid] = {{
      decision: d ? d.decision : null,
      corrected_value: corr ? corr.value : "",
      note: note ? note.value : "",
    }};
  }});
  localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}}

function restoreState() {{
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) return;
  try {{
    const state = JSON.parse(raw);
    Object.entries(state).forEach(([sid, s]) => {{
      if (s.decision) {{
        setSpanDecision(parseInt(sid), s.decision);
      }}
      if (s.corrected_value) {{
        const corr = document.getElementById("corr-" + sid);
        if (corr) corr.value = s.corrected_value;
      }}
      if (s.note) {{
        const note = document.getElementById("note-" + sid);
        if (note) note.value = s.note;
      }}
    }});
  }} catch(e) {{}}
}}

/* ── Export ─────────────────────────────────────── */
function exportJSON(isFinal) {{
  // Validate CORRECT spans have corrected_value
  let errors = [];
  document.querySelectorAll(".span-row").forEach(row => {{
    const sid = row.dataset.sid;
    const d = decisions[sid];
    if (d && d.decision === "CORRECT") {{
      const corr = document.getElementById("corr-" + sid);
      if (!corr || !corr.value.trim()) {{
        errors.push(sid);
        if (corr) corr.classList.add("error");
      }} else {{
        if (corr) corr.classList.remove("error");
      }}
    }}
  }});

  if (isFinal) {{
    // Check all decided
    let pending = 0;
    document.querySelectorAll(".span-row").forEach(row => {{
      if (!decisions[row.dataset.sid]) {{
        pending++;
        row.style.outline = "3px solid var(--terracotta)";
        row.style.outlineOffset = "-3px";
      }} else {{
        row.style.outline = "none";
      }}
    }});
    if (pending > 0) {{
      const msg = document.getElementById("validationMsg");
      msg.textContent = pending + " span(s) still need a decision.";
      msg.style.display = "block";
      return;
    }}
  }}

  if (errors.length > 0) {{
    const msg = document.getElementById("validationMsg");
    msg.textContent = errors.length + " CORRECT span(s) missing corrected value. Highlighted in red.";
    msg.style.display = "block";
    const first = document.getElementById("span-" + errors[0]);
    if (first) {{
      // Expand parent card
      const card = first.closest(".paper-card");
      if (card) {{
        const pid = card.dataset.pid;
        const body = document.getElementById("pbody-" + pid);
        if (body) body.style.display = "block";
      }}
      first.scrollIntoView({{ behavior: "smooth", block: "center" }});
    }}
    if (isFinal) return;
  }} else {{
    document.getElementById("validationMsg").style.display = "none";
  }}

  const output = [];
  document.querySelectorAll(".span-row").forEach(row => {{
    const sid = parseInt(row.dataset.sid);
    const pid = parseInt(row.dataset.pid);
    const d = decisions[sid];
    if (!d) return;
    const fname = row.querySelector(".field-name") ? row.querySelector(".field-name").textContent : "";
    const corr = document.getElementById("corr-" + sid);
    const note = document.getElementById("note-" + sid);
    output.push({{
      span_id: sid,
      paper_id: pid,
      field_name: fname,
      decision: d.decision,
      corrected_value: (d.decision === "CORRECT" && corr) ? corr.value.trim() : null,
      note: (note && note.value.trim()) ? note.value.trim() : "",
    }});
  }});

  const blob = new Blob([JSON.stringify(output, null, 2)], {{ type: "application/json" }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = REVIEW + "_extraction_audit_decisions" + (isFinal ? "" : "_draft") + ".json";
  a.click();
  URL.revokeObjectURL(url);
}}

/* ── Init ──────────────────────────────────────── */
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
            "extraction_audit", "queue", "html",
        )

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    logger.info("Wrote extraction audit HTML: %s", out)

    return out, {
        "total_spans": total_spans,
        "total_papers": total_papers,
        "by_state": state_counts,
    }


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Generate HTML review page for extraction audit spans"
    )
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument("--output", default=None, help="Output file path")
    args = parser.parse_args()

    out, stats = generate_extraction_audit_html(
        review_name=args.review,
        output_path=args.output,
    )
    print(f"Generated: {out}")
    print(f"  Papers:  {stats['total_papers']}")
    print(f"  Spans:   {stats['total_spans']}")
    for state, count in sorted(stats["by_state"].items()):
        print(f"    {state}: {count}")


if __name__ == "__main__":
    main()
