"""Post-extraction field validation — read-only diagnostic tool.

Checks extracted spans against the Review Spec schema:
  - Unknown field names
  - Invalid categorical values (with closest-match suggestion)
  - Non-numeric sample_size values
"""

import argparse
import difflib
import logging
import sys
from pathlib import Path

from engine.core.database import ReviewDatabase
from engine.core.review_spec import ExtractionField, ReviewSpec, load_review_spec

logger = logging.getLogger(__name__)


# ── Core Validation ──────────────────────────────────────────────────


def _closest_match(value: str, valid: list[str]) -> str | None:
    """Return the closest valid value by sequence similarity, or None."""
    matches = difflib.get_close_matches(value, valid, n=1, cutoff=0.4)
    return matches[0] if matches else None


def validate_extraction(
    spec: ReviewSpec, paper_id: int, db: ReviewDatabase,
) -> list[dict]:
    """Validate a paper's extracted spans against the spec schema.

    Returns a list of issue dicts: {paper_id, field_name, value, issue}.
    Read-only — does not modify the DB.
    """
    # Build lookup from spec
    field_map: dict[str, ExtractionField] = {f.name: f for f in spec.extraction_schema.fields}
    valid_field_names = set(field_map)

    # Fetch spans
    rows = db._conn.execute(
        """SELECT es.field_name, es.value
           FROM evidence_spans es
           JOIN extractions e ON es.extraction_id = e.id
           WHERE e.paper_id = ?""",
        (paper_id,),
    ).fetchall()

    issues: list[dict] = []

    for row in rows:
        fname = row["field_name"]
        value = row["value"]

        # 1. Unknown field name
        if fname not in valid_field_names:
            suggestion = _closest_match(fname, list(valid_field_names))
            msg = f"unknown field name"
            if suggestion:
                msg += f" (did you mean '{suggestion}'?)"
            issues.append({"paper_id": paper_id, "field_name": fname,
                           "value": value, "issue": msg})
            continue

        field_def = field_map[fname]

        # Skip NOT_FOUND values — they're valid for any field
        if value in ("NOT_FOUND", "NR"):
            continue

        # 2. Categorical field — check enum_values
        #    Some fields (validation_setting, surgical_domain) allow semicolon-
        #    separated multi-values; each individual value must be in the enum.
        if field_def.type == "categorical" and field_def.enum_values:
            parts = [v.strip() for v in value.split(";")]
            for part in parts:
                if not part:
                    continue
                if part in field_def.enum_values:
                    continue
                # Check prefix match for autonomy_level shorthand (e.g., "2" → "2 (Task autonomy)")
                prefix_match = any(ev.startswith(part + " ") for ev in field_def.enum_values)
                if prefix_match:
                    continue
                suggestion = _closest_match(part, field_def.enum_values)
                msg = f"invalid categorical value"
                if suggestion:
                    msg += f" (closest: '{suggestion}')"
                issues.append({"paper_id": paper_id, "field_name": fname,
                               "value": value, "issue": msg})
                break  # one issue per field is enough

        # 3. sample_size — must be integer or "NR"/"NOT_FOUND" (already handled above)
        if fname == "sample_size":
            stripped = value.strip()
            try:
                int(stripped)
            except ValueError:
                issues.append({"paper_id": paper_id, "field_name": fname,
                               "value": value,
                               "issue": "non-numeric sample_size (expected integer or NR)"})

    return issues


def validate_all(
    spec: ReviewSpec, db: ReviewDatabase, statuses: tuple[str, ...] = ("EXTRACTED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE"),
) -> list[dict]:
    """Validate all extracted papers. Returns combined issue list."""
    paper_ids: list[int] = []
    for status in statuses:
        rows = db._conn.execute(
            "SELECT id FROM papers WHERE status = ?", (status,),
        ).fetchall()
        paper_ids.extend(r["id"] for r in rows)

    all_issues: list[dict] = []
    for pid in sorted(set(paper_ids)):
        all_issues.extend(validate_extraction(spec, pid, db))

    return all_issues


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Post-extraction field validation (read-only)")
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument("--spec", default="review_specs/surgical_autonomy_v1.yaml",
                        help="Path to review spec YAML")
    args = parser.parse_args()

    spec = load_review_spec(args.spec)
    db = ReviewDatabase(args.review)

    try:
        # Count total spans
        total_spans = db._conn.execute(
            """SELECT COUNT(*) FROM evidence_spans es
               JOIN extractions e ON es.extraction_id = e.id
               JOIN papers p ON e.paper_id = p.id
               WHERE p.status IN ('EXTRACTED', 'AI_AUDIT_COMPLETE', 'HUMAN_AUDIT_COMPLETE')"""
        ).fetchone()[0]

        issues = validate_all(spec, db)

        # Summary
        print(f"\nSpans checked:  {total_spans}")
        print(f"Issues found:   {len(issues)}")

        if issues:
            # Group by issue type
            by_type: dict[str, int] = {}
            for iss in issues:
                key = iss["issue"].split("(")[0].strip()
                by_type[key] = by_type.get(key, 0) + 1

            print("\nIssues by type:")
            for t, count in sorted(by_type.items(), key=lambda x: -x[1]):
                print(f"  {t:45s} {count:>4d}")

            print("\nAll issues:")
            for iss in issues:
                print(f"  paper={iss['paper_id']:>5d}  field={iss['field_name']:35s}  "
                      f"value={iss['value'][:50]:50s}  {iss['issue']}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
