"""Concordance adjudication — export AMBIGUOUS pairs for human review, import decisions.

Paper 1 analysis infrastructure. Follows the same export→review→import
pattern as the engine's abstract/FT/extraction adjudication modules.

CLI:
    python -m analysis.paper1.adjudication export \\
        --review surgical_autonomy \\
        --arms local,openai_o4_mini_high,anthropic_sonnet_4_6 \\
        --output adjudication_queue.html

    python -m analysis.paper1.adjudication import \\
        --decisions adjudication_decisions.json \\
        --review surgical_autonomy
"""

import argparse
import html as html_mod
import json
import logging
import sqlite3
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


# ── Codebook field type lookup ───────────────────────────────────────


def _load_field_types(codebook_path: Path) -> dict[str, str]:
    """Return {field_name: type} from codebook (categorical, free_text, numeric)."""
    with open(codebook_path) as f:
        cb = yaml.safe_load(f)
    return {fd["name"]: fd["type"] for fd in cb.get("fields", [])}


# ── Source snippet queries ───────────────────────────────────────────


def _get_local_snippet(conn: sqlite3.Connection, paper_id: int, field_name: str) -> str | None:
    """Get source_snippet from evidence_spans for the local arm."""
    row = conn.execute(
        """SELECT es.source_snippet
           FROM evidence_spans es
           JOIN extractions e ON e.id = es.extraction_id
           WHERE e.paper_id = ? AND es.field_name = ?
           LIMIT 1""",
        (paper_id, field_name),
    ).fetchone()
    return row[0] if row else None


def _get_cloud_snippet(conn: sqlite3.Connection, paper_id: int, field_name: str, arm: str) -> str | None:
    """Get source_snippet from cloud_evidence_spans for a cloud arm."""
    row = conn.execute(
        """SELECT cs.source_snippet
           FROM cloud_evidence_spans cs
           JOIN cloud_extractions ce ON ce.id = cs.cloud_extraction_id
           WHERE ce.paper_id = ? AND cs.field_name = ? AND ce.arm = ?
           LIMIT 1""",
        (paper_id, field_name, arm),
    ).fetchone()
    return row[0] if row else None


def _get_human_snippet(conn: sqlite3.Connection, paper_id_str: str, field_name: str, extractor_id: str) -> str | None:
    """Get source_quote from human_extractions."""
    row = conn.execute(
        """SELECT source_quote FROM human_extractions
           WHERE paper_id = ? AND field_name = ? AND extractor_id = ?
           LIMIT 1""",
        (paper_id_str, field_name, extractor_id),
    ).fetchone()
    return row[0] if row else None


def _get_snippet(conn: sqlite3.Connection, paper_id: int, field_name: str, arm: str) -> str | None:
    """Route snippet lookup to the right table based on arm name."""
    if arm == "local":
        return _get_local_snippet(conn, paper_id, field_name)
    if arm.startswith("human_"):
        # human_A, human_B, etc.
        extractor_id = arm.split("_", 1)[1]
        paper_id_str = f"EE-{paper_id:03d}"
        return _get_human_snippet(conn, paper_id_str, field_name, extractor_id)
    return _get_cloud_snippet(conn, paper_id, field_name, arm)


# ── Export AMBIGUOUS pairs ───────────────────────────────────────────


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row[0] > 0


def export_ambiguous_pairs(
    db_path: Path,
    review_name: str,
    arms: list[str],
    codebook_path: Path | None = None,
) -> list[dict]:
    """Collect all AMBIGUOUS-scored field pairs across arm combinations.

    Runs concordance for every pair of arms and returns AMBIGUOUS entries
    with source snippets attached.

    Raises RuntimeError if concordance data cannot be computed (e.g. no
    extractions found for an arm).
    """
    from engine.analysis.concordance import load_arm, align_arms
    from engine.analysis.scoring import score_pair
    from engine.core.review_spec import load_review_spec

    db_str = str(db_path)

    # Load spec for scoring
    spec_path = Path(f"review_specs/{review_name}_v1.yaml")
    spec = load_review_spec(str(spec_path)) if spec_path.exists() else None

    # Load field types from codebook
    field_types: dict[str, str] = {}
    if codebook_path and codebook_path.exists():
        field_types = _load_field_types(codebook_path)

    # Load all arms
    arm_data: dict[str, dict] = {}
    for arm in arms:
        data = load_arm(db_str, arm)
        if not data:
            raise RuntimeError(
                f"No extraction data found for arm '{arm}'. "
                f"Run extraction/concordance for this arm first."
            )
        arm_data[arm] = data
        logger.info("Arm '%s': %d papers loaded", arm, len(data))

    # Collect AMBIGUOUS pairs across all arm combinations
    conn = sqlite3.connect(db_str)
    conn.row_factory = sqlite3.Row
    pairs: list[dict] = []

    try:
        for arm_a, arm_b in combinations(arms, 2):
            aligned, _, _ = align_arms(arm_data[arm_a], arm_data[arm_b])

            for pid, fname, val_a, val_b in aligned:
                fs = score_pair(fname, val_a, val_b, spec)
                if fs.result != "AMBIGUOUS":
                    continue

                pairs.append({
                    "paper_id": pid,
                    "field_name": fname,
                    "arm_a_name": arm_a,
                    "arm_a_value": val_a,
                    "arm_a_snippet": _get_snippet(conn, pid, fname, arm_a),
                    "arm_b_name": arm_b,
                    "arm_b_value": val_b,
                    "arm_b_snippet": _get_snippet(conn, pid, fname, arm_b),
                    "field_type": field_types.get(fname, "unknown"),
                    "score_detail": fs.detail,
                })
    finally:
        conn.close()

    logger.info("Collected %d AMBIGUOUS pairs across %d arm combinations",
                len(pairs), len(list(combinations(arms, 2))))
    return pairs


# ── HTML Generation ──────────────────────────────────────────────────


def _esc(text: str | None) -> str:
    """HTML-escape, returning em-dash for None."""
    if text is None:
        return "<em>—</em>"
    return html_mod.escape(str(text))


def generate_adjudication_html(pairs: list[dict], output_path: Path, review_name: str = "") -> None:
    """Generate a self-contained HTML adjudication interface."""
    # Group by paper_id
    by_paper: dict[int, list[dict]] = {}
    for p in pairs:
        by_paper.setdefault(p["paper_id"], []).append(p)

    # Build cards
    cards_html = []
    for pid in sorted(by_paper.keys()):
        group = by_paper[pid]
        field_cards = []
        for idx, pair in enumerate(group):
            uid = f"p{pid}_{pair['field_name']}_{pair['arm_a_name']}_{pair['arm_b_name']}"
            ft_badge = "categorical" if pair["field_type"] == "categorical" else "free-text"

            snippet_a = ""
            if pair.get("arm_a_snippet"):
                snippet_a = f'<blockquote class="snippet">{_esc(pair["arm_a_snippet"])}</blockquote>'
            snippet_b = ""
            if pair.get("arm_b_snippet"):
                snippet_b = f'<blockquote class="snippet">{_esc(pair["arm_b_snippet"])}</blockquote>'

            field_cards.append(f"""
      <div class="field-card undecided" id="{uid}"
           data-pid="{pid}" data-field="{_esc(pair['field_name'])}"
           data-arm-a="{_esc(pair['arm_a_name'])}" data-arm-b="{_esc(pair['arm_b_name'])}">
        <div class="field-header">
          <span class="field-name">{_esc(pair['field_name'])}</span>
          <span class="type-badge {pair['field_type']}">{ft_badge}</span>
          <span class="arms-label">{_esc(pair['arm_a_name'])} vs {_esc(pair['arm_b_name'])}</span>
        </div>
        <div class="comparison">
          <div class="arm-panel arm-a">
            <div class="arm-label">{_esc(pair['arm_a_name'])}</div>
            <div class="arm-value">{_esc(pair['arm_a_value'])}</div>
            {snippet_a}
          </div>
          <div class="arm-panel arm-b">
            <div class="arm-label">{_esc(pair['arm_b_name'])}</div>
            <div class="arm-value">{_esc(pair['arm_b_value'])}</div>
            {snippet_b}
          </div>
        </div>
        <div class="decision-row">
          <label class="radio-label agree">
            <input type="radio" name="{uid}" value="AGREE"
                   onchange="setDecision('{uid}','AGREE')"> AGREE
          </label>
          <label class="radio-label disagree">
            <input type="radio" name="{uid}" value="DISAGREE"
                   onchange="setDecision('{uid}','DISAGREE')"> DISAGREE
          </label>
        </div>
      </div>""")

        cards_html.append(f"""
    <div class="paper-group">
      <h2 class="paper-header">Paper {pid}</h2>
      {''.join(field_cards)}
    </div>""")

    total = len(pairs)
    n_papers = len(by_paper)

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Concordance Adjudication — {_esc(review_name or 'Evidence Engine')}</title>
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
  .count-agree {{ color: #7ee0a0; }}
  .count-disagree {{ color: #ff9999; }}

  /* ── Paper groups ───────────────────────────────────── */
  .paper-group {{ margin-bottom: 2rem; }}
  .paper-header {{
    font-family: 'Fraunces', serif; font-weight: 600;
    color: var(--forest-teal); font-size: 1.2rem;
    border-bottom: 2px solid var(--forest-teal);
    padding-bottom: 0.3rem; margin-bottom: 0.75rem;
  }}

  /* ── Field cards ────────────────────────────────────── */
  .field-card {{
    background: #fff; border-radius: 8px; padding: 1rem 1.25rem;
    margin-bottom: 0.75rem; box-shadow: 0 1px 4px rgba(0,0,0,0.08);
    border-left: 4px solid #ffd080; transition: opacity 0.2s, border-color 0.2s;
  }}
  .field-card.decided-agree {{ border-left-color: var(--forest-teal); opacity: 0.75; }}
  .field-card.decided-disagree {{ border-left-color: var(--terracotta); opacity: 0.75; }}
  .field-card.decided-agree:hover,
  .field-card.decided-disagree:hover {{ opacity: 0.9; }}

  .field-header {{
    display: flex; align-items: center; gap: 0.6rem; margin-bottom: 0.6rem;
    flex-wrap: wrap;
  }}
  .field-name {{
    font-family: monospace; font-weight: 600; font-size: 0.95rem;
    color: var(--forest-teal);
  }}
  .type-badge {{
    font-size: 0.7rem; font-weight: 600; padding: 0.1rem 0.45rem;
    border-radius: 3px; text-transform: uppercase; letter-spacing: 0.03em;
  }}
  .type-badge.categorical {{ background: #e3f2fd; color: #1565c0; }}
  .type-badge.free_text {{ background: #fff3e0; color: #e65100; }}
  .type-badge.numeric {{ background: #f3e5f5; color: #7b1fa2; }}
  .type-badge.unknown {{ background: #f5f5f5; color: #666; }}
  .arms-label {{ font-size: 0.8rem; color: #888; margin-left: auto; }}

  /* ── Side-by-side comparison ────────────────────────── */
  .comparison {{
    display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;
    margin-bottom: 0.75rem;
  }}
  .arm-panel {{
    background: var(--mist-white); border-radius: 6px; padding: 0.75rem;
  }}
  .arm-label {{
    font-size: 0.75rem; font-weight: 600; text-transform: uppercase;
    color: #888; margin-bottom: 0.3rem; letter-spacing: 0.04em;
  }}
  .arm-value {{
    font-size: 0.9rem; line-height: 1.4; word-break: break-word;
  }}
  .snippet {{
    font-size: 0.8rem; color: #666; font-style: italic;
    border-left: 3px solid var(--mist-teal); margin: 0.5rem 0 0 0;
    padding: 0.4rem 0.6rem; background: rgba(255,255,255,0.5);
    border-radius: 0 4px 4px 0;
  }}

  /* ── Decision radio buttons ─────────────────────────── */
  .decision-row {{
    display: flex; gap: 1rem; padding-top: 0.5rem;
    border-top: 1px solid #eee;
  }}
  .radio-label {{
    display: flex; align-items: center; gap: 0.3rem;
    font-size: 0.85rem; font-weight: 500; cursor: pointer;
    padding: 0.3rem 0.6rem; border-radius: 4px;
  }}
  .radio-label.agree:has(input:checked) {{ background: #e8f5e9; color: #2e7d32; }}
  .radio-label.disagree:has(input:checked) {{ background: #fbe9e7; color: #c62828; }}
  .radio-label input {{ cursor: pointer; }}

  /* ── Button bar ─────────────────────────────────────── */
  .button-bar {{
    position: sticky; bottom: 0;
    background: #fff; padding: 0.75rem 1.25rem; border-radius: 6px;
    box-shadow: 0 -2px 8px rgba(0,0,0,0.1); margin-top: 2rem;
    display: flex; align-items: center; justify-content: flex-end; gap: 0.75rem;
  }}
  .validation-msg {{
    margin-right: auto; font-size: 0.85rem; color: #c62828; font-weight: 500;
  }}
  .btn-action {{
    font-family: 'IBM Plex Sans', sans-serif; font-size: 0.9rem;
    font-weight: 600; padding: 0.5rem 1.25rem; border-radius: 5px;
    cursor: pointer; border: none;
  }}
  .btn-draft {{ background: var(--mist-teal); color: var(--forest-teal); }}
  .btn-draft:hover {{ background: #cde5e1; }}
  .btn-final {{ background: var(--forest-teal); color: #fff; }}
  .btn-final:hover {{ background: #084e47; }}
  .btn-final:disabled {{ opacity: 0.4; cursor: not-allowed; }}

  @media (max-width: 768px) {{
    .comparison {{ grid-template-columns: 1fr; }}
  }}
</style>
</head>
<body>

<h1>Concordance Adjudication</h1>
<p class="subtitle">{n_papers} papers &middot; {total} AMBIGUOUS pairs</p>

<div class="summary-bar">
  <div class="stats">
    <div><span class="stat-label">Pending</span> <span class="count count-pending" id="countPending">{total}</span></div>
    <div><span class="stat-label">Agree</span> <span class="count count-agree" id="countAgree">0</span></div>
    <div><span class="stat-label">Disagree</span> <span class="count count-disagree" id="countDisagree">0</span></div>
  </div>
</div>

{''.join(cards_html)}

<div class="button-bar">
  <span class="validation-msg" id="validationMsg"></span>
  <button class="btn-action btn-draft" onclick="exportJSON(false)">Save Draft</button>
  <button class="btn-action btn-final" id="finalBtn" onclick="exportJSON(true)" disabled>Export Final</button>
</div>

<script>
const TOTAL = {total};
const REVIEW = "{_esc(review_name)}";
const decisions = {{}};

function setDecision(uid, value) {{
  decisions[uid] = value;
  const card = document.getElementById(uid);
  card.classList.remove("undecided", "decided-agree", "decided-disagree");
  card.classList.add(value === "AGREE" ? "decided-agree" : "decided-disagree");
  updateCounts();
}}

function updateCounts() {{
  let agree = 0, disagree = 0;
  for (const v of Object.values(decisions)) {{
    if (v === "AGREE") agree++;
    else if (v === "DISAGREE") disagree++;
  }}
  const pending = TOTAL - agree - disagree;
  document.getElementById("countPending").textContent = pending;
  document.getElementById("countAgree").textContent = agree;
  document.getElementById("countDisagree").textContent = disagree;
  document.getElementById("finalBtn").disabled = (pending > 0);
  document.getElementById("validationMsg").textContent =
    pending > 0 ? pending + " pairs still need a decision" : "";
}}

function exportJSON(isFinal) {{
  const decided = Object.keys(decisions).length;
  if (isFinal && decided < TOTAL) {{
    document.getElementById("validationMsg").textContent =
      "All pairs must be decided before final export.";
    return;
  }}

  const output = [];
  document.querySelectorAll(".field-card").forEach(card => {{
    const uid = card.id;
    const dec = decisions[uid];
    if (dec || !isFinal) {{
      output.push({{
        paper_id: parseInt(card.dataset.pid),
        field_name: card.dataset.field,
        arm_a: card.dataset.armA,
        arm_b: card.dataset.armB,
        decision: dec || null
      }});
    }}
  }});

  const blob = new Blob([JSON.stringify(output, null, 2)], {{ type: "application/json" }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  const suffix = isFinal ? "" : "_draft";
  a.download = REVIEW + "_concordance_adjudication" + suffix + ".json";
  a.click();
  URL.revokeObjectURL(url);
}}
</script>
</body>
</html>"""

    output_path.write_text(page)
    logger.info("Adjudication HTML written to %s (%d pairs, %d papers)",
                output_path, total, n_papers)


# ── Import Decisions ─────────────────────────────────────────────────


_CREATE_TABLE = """\
CREATE TABLE IF NOT EXISTS concordance_adjudications (
    id INTEGER PRIMARY KEY,
    paper_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    arm_a TEXT NOT NULL,
    arm_b TEXT NOT NULL,
    decision TEXT NOT NULL CHECK(decision IN ('AGREE','DISAGREE')),
    adjudicated_at TEXT NOT NULL,
    UNIQUE(paper_id, field_name, arm_a, arm_b)
)"""


def import_adjudication_decisions(json_path: Path, db_path: Path) -> int:
    """Import adjudication decisions from exported JSON.

    Returns the number of rows inserted.
    """
    data = json.loads(json_path.read_text())
    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON array, got {type(data).__name__}")

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(_CREATE_TABLE)

    now = datetime.now(timezone.utc).isoformat()
    inserted = 0

    try:
        for entry in data:
            if entry.get("decision") is None:
                continue  # skip undecided draft entries

            decision = entry["decision"]
            if decision not in ("AGREE", "DISAGREE"):
                raise ValueError(
                    f"Invalid decision '{decision}' for paper {entry.get('paper_id')} "
                    f"/ {entry.get('field_name')}"
                )

            conn.execute(
                "INSERT INTO concordance_adjudications "
                "(paper_id, field_name, arm_a, arm_b, decision, adjudicated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    str(entry["paper_id"]),
                    entry["field_name"],
                    entry["arm_a"],
                    entry["arm_b"],
                    decision,
                    now,
                ),
            )
            inserted += 1

        conn.commit()
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        raise RuntimeError(
            f"Duplicate adjudication detected. Clear existing rows before re-importing. "
            f"Detail: {exc}"
        ) from exc
    finally:
        conn.close()

    return inserted


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Concordance adjudication: export AMBIGUOUS pairs or import decisions"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # Export subcommand
    exp = sub.add_parser("export", help="Export AMBIGUOUS pairs as HTML adjudication interface")
    exp.add_argument("--review", required=True, help="Review name")
    exp.add_argument("--arms", required=True, help="Comma-separated arm names")
    exp.add_argument("--output", type=Path, required=True, help="Output HTML path")
    exp.add_argument("--codebook", type=Path, default=None, help="Codebook YAML path (auto-detected)")

    # Import subcommand
    imp = sub.add_parser("import", help="Import adjudication decisions from JSON")
    imp.add_argument("--decisions", type=Path, required=True, help="JSON decisions file")
    imp.add_argument("--review", required=True, help="Review name")

    args = parser.parse_args()

    # Resolve DB path
    from engine.core.database import ReviewDatabase
    db = ReviewDatabase(args.review)
    db_path = Path(db.db_path)
    data_dir = db_path.parent
    db.close()

    if args.command == "export":
        arms = [a.strip() for a in args.arms.split(",")]
        codebook_path = args.codebook or (data_dir / "extraction_codebook.yaml")
        if not codebook_path.exists():
            logger.error("Codebook not found: %s", codebook_path)
            raise SystemExit(1)

        pairs = export_ambiguous_pairs(db_path, args.review, arms, codebook_path)
        if not pairs:
            logger.info("No AMBIGUOUS pairs found — nothing to adjudicate.")
            return

        generate_adjudication_html(pairs, args.output, review_name=args.review)
        logger.info("Done: %d pairs exported to %s", len(pairs), args.output)

    elif args.command == "import":
        if not args.decisions.exists():
            logger.error("Decisions file not found: %s", args.decisions)
            raise SystemExit(1)

        inserted = import_adjudication_decisions(args.decisions, db_path)
        logger.info("Imported %d adjudication decisions into %s", inserted, db_path)


if __name__ == "__main__":
    main()
