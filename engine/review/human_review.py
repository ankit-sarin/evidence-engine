"""Human review infrastructure: export review queues, import decisions."""

import csv
import json
import logging
from pathlib import Path

from engine.agents.auditor import grep_verify
from engine.core.database import ReviewDatabase

logger = logging.getLogger(__name__)

VALID_DECISIONS = {"ACCEPT", "ACCEPT_CORRECTED", "REJECT_VALUE", "REJECT_PAPER"}

REVIEW_COLUMNS = [
    "paper_id", "ee_identifier", "field_name", "tier", "field_type",
    "extracted_value", "source_snippet", "audit_status", "audit_rationale",
    "confidence",
    "parsed_text_context",
    "reviewer_decision", "corrected_snippet", "reviewer_note",
]


def _find_context(snippet: str, paper_text: str, abstract: str, width: int = 300) -> str:
    """Find ~width chars of parsed text centered on where the snippet appears."""
    if snippet and paper_text:
        from engine.agents.auditor import _normalize
        norm_snippet = _normalize(snippet)
        norm_text = _normalize(paper_text)
        idx = norm_text.find(norm_snippet)
        if idx >= 0:
            # Map back to approximate position in original text
            ratio = idx / max(len(norm_text), 1)
            approx_pos = int(ratio * len(paper_text))
            start = max(0, approx_pos - width // 2)
            end = min(len(paper_text), start + width)
            return paper_text[start:end].replace("\n", " ")

    # Fallback: first 300 chars of abstract
    if abstract:
        return abstract[:width].replace("\n", " ")
    if paper_text:
        return paper_text[:width].replace("\n", " ")
    return ""


def export_review_queue(
    db: ReviewDatabase,
    output_path: str,
    paper_ids: list[int] | None = None,
    field_type_map: dict[str, str] | None = None,
    field_tier_map: dict[str, int] | None = None,
) -> int:
    """Export contested and flagged spans as a CSV for human review.

    Returns the number of rows written.
    """
    field_type_map = field_type_map or {}
    field_tier_map = field_tier_map or {}

    if paper_ids is not None:
        placeholders = ",".join("?" for _ in paper_ids)
        papers = db._conn.execute(
            f"SELECT * FROM papers WHERE id IN ({placeholders}) ORDER BY id",
            paper_ids,
        ).fetchall()
    else:
        papers = db._conn.execute(
            "SELECT * FROM papers WHERE status = 'AI_AUDIT_COMPLETE' ORDER BY id"
        ).fetchall()

    review_dir = Path(db.db_path).parent
    rows_written = 0

    # Build EE-XXX mapping (sequential by paper_id order among included papers)
    all_included = db._conn.execute(
        "SELECT id FROM papers WHERE status NOT IN ('ABSTRACT_SCREENED_OUT', 'REJECTED') ORDER BY id"
    ).fetchall()
    ee_map = {row["id"]: f"EE-{seq:03d}" for seq, row in enumerate(all_included, 1)}

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REVIEW_COLUMNS)
        writer.writeheader()

        for paper in papers:
            pid = paper["id"]

            # Load parsed text
            parsed_dir = review_dir / "parsed_text"
            md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)
            paper_text = md_files[0].read_text() if md_files else ""

            extraction = db._conn.execute(
                "SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
                (pid,),
            ).fetchone()
            if not extraction:
                continue

            spans = db._conn.execute(
                """SELECT * FROM evidence_spans
                   WHERE extraction_id = ?
                   AND audit_status IN ('contested', 'flagged', 'invalid_snippet')
                   ORDER BY field_name""",
                (extraction["id"],),
            ).fetchall()

            # Sort by tier then field_name
            span_list = [dict(s) for s in spans]
            span_list.sort(key=lambda s: (
                field_tier_map.get(s["field_name"], 99),
                s["field_name"],
            ))

            for span in span_list:
                context = _find_context(
                    span["source_snippet"] or "",
                    paper_text,
                    paper["abstract"] or "",
                )
                writer.writerow({
                    "paper_id": pid,
                    "ee_identifier": ee_map.get(pid, f"EE-???"),
                    "field_name": span["field_name"],
                    "tier": field_tier_map.get(span["field_name"], ""),
                    "field_type": field_type_map.get(span["field_name"], "text"),
                    "extracted_value": span["value"],
                    "source_snippet": span["source_snippet"] or "",
                    "audit_status": span["audit_status"],
                    "audit_rationale": span["audit_rationale"] or "",
                    "confidence": span["confidence"],
                    "parsed_text_context": context,
                    "reviewer_decision": "",
                    "corrected_snippet": "",
                    "reviewer_note": "",
                })
                rows_written += 1

    logger.info("Review queue exported: %d spans across %d papers → %s",
                rows_written, len(papers), output_path)
    return rows_written


def import_review_decisions(
    db: ReviewDatabase,
    csv_path: str,
    dry_run: bool = False,
) -> dict:
    """Import human review decisions from a completed CSV or JSON file.

    Supports two formats (detected by file extension):
      - .csv  — CSV with reviewer_decision column (original format)
      - .json — JSON array from extraction_audit_html.py

    JSON schema: [{span_id, paper_id, field_name, decision, corrected_value, note}]
    JSON decision mapping: ACCEPT→ACCEPT, REJECT→REJECT_VALUE, CORRECT→ACCEPT_CORRECTED

    Validates all rows before writing. Returns stats dict.
    If dry_run=True, validates and reports but makes no writes.
    """
    input_path = Path(csv_path)
    if input_path.suffix.lower() == ".json":
        return _import_review_json(db, input_path, dry_run=dry_run)
    return _import_review_csv(db, str(csv_path), dry_run=dry_run)


def _import_review_csv(
    db: ReviewDatabase,
    csv_path: str,
    dry_run: bool = False,
) -> dict:
    """Import human review decisions from CSV (original format)."""
    review_dir = Path(db.db_path).parent

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    errors = []
    actions = []  # (span_id, decision, corrected_snippet, paper_id)

    for i, row in enumerate(rows, 2):  # line 2+ (header is line 1)
        decision = (row.get("reviewer_decision") or "").strip()
        if not decision:
            continue  # skip unreviewed rows

        if decision not in VALID_DECISIONS:
            errors.append(f"Row {i}: invalid decision '{decision}'")
            continue

        pid = int(row["paper_id"])
        fname = row["field_name"]

        # Find the span
        span = db._conn.execute(
            """SELECT es.id FROM evidence_spans es
               JOIN extractions e ON es.extraction_id = e.id
               WHERE e.paper_id = ? AND es.field_name = ?
               ORDER BY es.id DESC LIMIT 1""",
            (pid, fname),
        ).fetchone()

        if not span:
            errors.append(f"Row {i}: no span found for paper {pid}, field {fname}")
            continue

        if decision == "ACCEPT_CORRECTED":
            corrected = (row.get("corrected_snippet") or "").strip()
            if not corrected:
                errors.append(f"Row {i}: ACCEPT_CORRECTED requires corrected_snippet")
                continue

            # Verify corrected snippet exists in paper text
            md_files = sorted(
                (review_dir / "parsed_text").glob(f"{pid}_v*.md"), reverse=True
            )
            if md_files:
                paper_text = md_files[0].read_text()
                from difflib import SequenceMatcher
                from engine.agents.auditor import _normalize
                norm_corrected = _normalize(corrected)
                norm_text = _normalize(paper_text)
                # Check exact or very high fuzzy match (0.95 threshold)
                found = norm_corrected in norm_text
                if not found:
                    words = norm_text.split()
                    snippet_words = norm_corrected.split()
                    wsize = len(snippet_words)
                    for j in range(max(1, len(words) - wsize + 1)):
                        window = " ".join(words[j:j + wsize])
                        if SequenceMatcher(None, norm_corrected, window).ratio() >= 0.95:
                            found = True
                            break
                if not found:
                    errors.append(
                        f"Row {i}: corrected_snippet not found verbatim in paper {pid} "
                        f"(fuzzy < 0.95)"
                    )
                    continue

            actions.append((span["id"], decision, corrected, pid))
        else:
            actions.append((span["id"], decision, None, pid))

    if errors:
        logger.error("Validation errors (%d):", len(errors))
        for e in errors:
            logger.error("  %s", e)
        if not dry_run:
            logger.error("Aborting import — fix errors and retry.")
            return {"errors": errors, "applied": 0}

    if dry_run:
        logger.info("Dry run: %d actions would be applied, %d errors", len(actions), len(errors))
        return {"errors": errors, "would_apply": len(actions)}

    return _apply_audit_decisions(db, actions, errors, source="CSV")


def _import_review_json(
    db: ReviewDatabase,
    input_path: Path,
    dry_run: bool = False,
) -> dict:
    """Import extraction audit decisions from JSON (HTML tool output).

    Expected schema:
      [{span_id, paper_id, field_name, decision, corrected_value, note}]

    Decision mapping:
      ACCEPT  → audit_status='verified'
      REJECT  → value='NR', audit_status='verified'
      CORRECT → source_snippet=corrected_value, audit_status='verified'
    """
    try:
        with open(input_path) as f:
            records = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        msg = f"Cannot read JSON: {e}"
        logger.error(msg)
        return {"errors": [msg], "applied": 0}

    if not isinstance(records, list):
        msg = "JSON must be an array of decision objects."
        logger.error(msg)
        return {"errors": [msg], "applied": 0}

    errors = []
    actions = []  # (span_id, decision, corrected_value, paper_id)

    # Map HTML tool decisions to internal decisions
    decision_map = {
        "ACCEPT": "ACCEPT",
        "REJECT": "REJECT_VALUE",
        "CORRECT": "ACCEPT_CORRECTED",
    }

    for i, rec in enumerate(records):
        span_id = rec.get("span_id")
        paper_id = rec.get("paper_id")
        decision_raw = (rec.get("decision") or "").strip().upper()

        if not span_id:
            errors.append(f"Record {i}: missing span_id")
            continue

        decision = decision_map.get(decision_raw)
        if not decision:
            errors.append(
                f"Record {i} (span {span_id}): invalid decision '{decision_raw}' "
                f"(must be ACCEPT, REJECT, or CORRECT)"
            )
            continue

        # Verify span exists
        span = db._conn.execute(
            "SELECT id FROM evidence_spans WHERE id = ?", (span_id,)
        ).fetchone()
        if not span:
            errors.append(f"Record {i}: span_id {span_id} not found in database")
            continue

        if decision == "ACCEPT_CORRECTED":
            corrected = (rec.get("corrected_value") or "").strip()
            if not corrected:
                errors.append(
                    f"Record {i} (span {span_id}): CORRECT requires corrected_value"
                )
                continue
            actions.append((span_id, decision, corrected, paper_id))
        else:
            actions.append((span_id, decision, None, paper_id))

    if errors:
        logger.error("Validation errors (%d):", len(errors))
        for e in errors:
            logger.error("  %s", e)
        if not dry_run:
            logger.error("Aborting import — fix errors and retry.")
            return {"errors": errors, "applied": 0}

    if dry_run:
        logger.info(
            "Dry run: %d actions would be applied, %d errors",
            len(actions), len(errors),
        )
        return {"errors": errors, "would_apply": len(actions)}

    return _apply_audit_decisions(db, actions, errors, source="JSON")


def _apply_audit_decisions(
    db: ReviewDatabase,
    actions: list[tuple],
    errors: list[str],
    source: str = "import",
) -> dict:
    """Apply validated audit decisions to the database.

    Shared by both CSV and JSON import paths. Each action is a tuple of
    (span_id, decision, corrected_value_or_snippet, paper_id).

    Transitions papers to HUMAN_AUDIT_COMPLETE when all spans are resolved.
    """
    applied = 0
    papers_touched = set()

    for span_id, decision, corrected, pid in actions:
        note = f"Human review: {decision}"
        if pid:
            papers_touched.add(pid)

        if decision == "ACCEPT":
            db._conn.execute(
                """UPDATE evidence_spans
                   SET audit_status = 'verified', audit_rationale = ?, audited_at = ?
                   WHERE id = ?""",
                (note, _now(), span_id),
            )
        elif decision == "ACCEPT_CORRECTED":
            db._conn.execute(
                """UPDATE evidence_spans
                   SET audit_status = 'verified', source_snippet = ?,
                       audit_rationale = ?, audited_at = ?
                   WHERE id = ?""",
                (corrected, "Human review: ACCEPT_CORRECTED", _now(), span_id),
            )
        elif decision == "REJECT_VALUE":
            db._conn.execute(
                """UPDATE evidence_spans
                   SET value = 'NR', audit_status = 'verified',
                       audit_rationale = ?, audited_at = ?
                   WHERE id = ?""",
                (note, _now(), span_id),
            )
        elif decision == "REJECT_PAPER":
            db.reject_paper(pid, "Human review rejection")

        applied += 1

    db._conn.commit()

    # Transition papers where all spans are resolved
    for pid in papers_touched:
        status = db._conn.execute(
            "SELECT status FROM papers WHERE id = ?", (pid,)
        ).fetchone()
        if not status or status["status"] != "AI_AUDIT_COMPLETE":
            continue

        unresolved = db._conn.execute(
            """SELECT COUNT(*) FROM evidence_spans es
               JOIN extractions e ON es.extraction_id = e.id
               WHERE e.paper_id = ?
                 AND es.audit_status IN ('contested', 'flagged', 'invalid_snippet')""",
            (pid,),
        ).fetchone()[0]

        if unresolved == 0:
            db.update_status(pid, "HUMAN_AUDIT_COMPLETE")
            logger.info("Paper %d → HUMAN_AUDIT_COMPLETE (all spans resolved)", pid)

    logger.info(
        "%s import complete: %d decisions applied across %d papers",
        source, applied, len(papers_touched),
    )
    return {"errors": errors, "applied": applied}


def bulk_accept(db: ReviewDatabase, paper_ids: list[int] | None = None) -> dict:
    """Mark all contested and flagged spans as verified without individual review.

    For concordance study use only. Accepts all AI audit outcomes as-is for
    downstream analysis. Do not use for production systematic reviews where
    human adjudication is required.

    Transitions papers to AI_AUDIT_COMPLETE (not HUMAN_AUDIT_COMPLETE — this
    is explicitly not human review).
    """
    if paper_ids is not None:
        placeholders = ",".join("?" for _ in paper_ids)
        papers = db._conn.execute(
            f"SELECT id FROM papers WHERE id IN ({placeholders}) AND status = 'AI_AUDIT_COMPLETE'",
            paper_ids,
        ).fetchall()
    else:
        papers = db._conn.execute(
            "SELECT id FROM papers WHERE status = 'AI_AUDIT_COMPLETE'"
        ).fetchall()

    spans_accepted = 0
    for paper in papers:
        pid = paper["id"]
        extraction = db._conn.execute(
            "SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()
        if not extraction:
            continue

        result = db._conn.execute(
            """UPDATE evidence_spans
               SET audit_status = 'verified',
                   audit_rationale = 'Bulk accept (concordance study)',
                   audited_at = ?
               WHERE extraction_id = ?
               AND audit_status IN ('contested', 'flagged', 'invalid_snippet')""",
            (_now(), extraction["id"]),
        )
        spans_accepted += result.rowcount

    db._conn.commit()

    logger.info("Bulk accept: %d spans across %d papers", spans_accepted, len(papers))
    return {"papers": len(papers), "spans_accepted": spans_accepted}


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
