"""Post-extraction field validation — read-only diagnostic tool.

Checks extracted spans against the Review Spec schema:
  - Unknown field names
  - Invalid categorical values (with closest-match suggestion)
  - Non-numeric sample_size values

Also provides prefix normalization for categorical values (Item 87a).
"""

import argparse
import difflib
import hashlib
import logging
import sys
from pathlib import Path

from engine.core.database import ReviewDatabase
from engine.core.review_spec import ExtractionField, ReviewSpec, load_review_spec

logger = logging.getLogger(__name__)


# ── Schema Hash Parity ───────────────────────────────────────────────


def verify_schema_parity(spec: ReviewSpec) -> str:
    """Compute a SHA-256 hash of the extraction prompt for schema versioning.

    Builds the extraction prompt with a dummy paper text and hashes the result.
    If the prompt changes (codebook edits, field additions, template changes),
    the hash changes — making schema drift detectable.

    Returns the hex digest string.
    """
    from engine.agents.extractor import build_extraction_prompt

    prompt = build_extraction_prompt("TEST", spec)
    return hashlib.sha256(prompt.encode()).hexdigest()


# ── Prefix Normalization ─────────────────────────────────────────────


def normalize_prefix(value: str, valid_values: list[str]) -> str:
    """If *value* is an unambiguous case-insensitive prefix of exactly one
    valid value, return the canonical form.  Otherwise return *value* unchanged.

    An exact (case-insensitive) match always wins and counts as unambiguous.
    """
    value_lower = value.lower()

    # Exact match (case-insensitive) — always unambiguous
    for v in valid_values:
        if v.lower() == value_lower:
            return v

    # Prefix match — must be unique
    matches = [v for v in valid_values if v.lower().startswith(value_lower)]
    if len(matches) == 1:
        logger.debug(
            "Prefix normalized: '%s' → '%s'", value, matches[0],
        )
        return matches[0]

    return value


def normalize_categorical_values(
    spec: ReviewSpec, paper_id: int, db: ReviewDatabase,
) -> list[dict]:
    """Normalize categorical span values in-place using prefix matching.

    For each categorical field with enum_values, if the stored value is an
    unambiguous prefix of a valid value, update the DB to the canonical form.

    Returns a list of dicts describing each normalization applied:
    ``{paper_id, field_name, original, canonical}``.
    """
    field_map: dict[str, ExtractionField] = {
        f.name: f for f in spec.extraction_schema.fields
    }

    rows = db._conn.execute(
        """SELECT es.id, es.field_name, es.value
           FROM evidence_spans es
           JOIN extractions e ON es.extraction_id = e.id
           WHERE e.paper_id = ?""",
        (paper_id,),
    ).fetchall()

    changes: list[dict] = []

    for row in rows:
        fname = row["field_name"]
        value = row["value"]

        if fname not in field_map:
            continue

        field_def = field_map[fname]
        if field_def.type != "categorical" or not field_def.enum_values:
            continue

        if value in ("NOT_FOUND", "NR"):
            continue

        # Handle semicolon-separated multi-values
        parts = [v.strip() for v in value.split(";")]
        normalized_parts: list[str] = []
        any_changed = False
        for part in parts:
            if not part:
                normalized_parts.append(part)
                continue
            canonical = normalize_prefix(part, field_def.enum_values)
            if canonical != part:
                any_changed = True
            normalized_parts.append(canonical)

        if any_changed:
            new_value = "; ".join(normalized_parts)
            db._conn.execute(
                "UPDATE evidence_spans SET value = ? WHERE id = ?",
                (new_value, row["id"]),
            )
            changes.append({
                "paper_id": paper_id,
                "field_name": fname,
                "original": value,
                "canonical": new_value,
            })
            logger.debug(
                "paper %d field '%s': '%s' → '%s'",
                paper_id, fname, value, new_value,
            )

    if changes:
        db._conn.commit()

    return changes


# ── Cross-field Bleed Detection ───────────────────────────────────────


def detect_cross_field_bleed(
    spec: ReviewSpec,
    extraction_data: list[dict],
) -> list[dict]:
    """Detect categorical values that belong to a different field's vocabulary.

    For each categorical span, if the value is NOT valid for its own field
    but IS an exact (case-insensitive) match for another categorical field's
    valid_values, flag it as cross-field bleed.

    Args:
        spec: Review spec with extraction_schema.
        extraction_data: List of ``{"field_name": str, "value": str}`` dicts.

    Returns:
        List of ``{field_name, extracted_value, belongs_to_field}`` records.
    """
    field_map: dict[str, ExtractionField] = {
        f.name: f for f in spec.extraction_schema.fields
    }

    # Build reverse lookup: lowered value → list of field names that accept it
    value_to_fields: dict[str, list[str]] = {}
    for f in spec.extraction_schema.fields:
        if f.type != "categorical" or not f.enum_values:
            continue
        for v in f.enum_values:
            value_to_fields.setdefault(v.lower(), []).append(f.name)

    bleeds: list[dict] = []

    for span in extraction_data:
        fname = span["field_name"]
        value = span["value"]

        if fname not in field_map:
            continue

        field_def = field_map[fname]
        if field_def.type != "categorical" or not field_def.enum_values:
            continue

        if value in ("NOT_FOUND", "NR"):
            continue

        own_valid_lower = {v.lower() for v in field_def.enum_values}

        parts = [p.strip() for p in value.split(";")]
        for part in parts:
            if not part:
                continue
            part_lower = part.lower()

            # Already valid for its own field — no bleed
            if part_lower in own_valid_lower:
                continue

            # Check if it belongs to any other field
            owner_fields = value_to_fields.get(part_lower, [])
            other_owners = [f for f in owner_fields if f != fname]
            if other_owners:
                for owner in other_owners:
                    logger.warning(
                        "Cross-field bleed: field '%s' has value '%s' "
                        "which belongs to field '%s'",
                        fname, part, owner,
                    )
                    bleeds.append({
                        "field_name": fname,
                        "extracted_value": part,
                        "belongs_to_field": owner,
                    })

    return bleeds


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
        #    Fields may contain semicolon-separated multi-values; each element
        #    is validated independently.  Only invalid elements are reported.
        if field_def.type == "categorical" and field_def.enum_values:
            parts = [v.strip() for v in value.split(";")]
            invalid_parts: list[str] = []
            for part in parts:
                if not part:
                    continue
                if part in field_def.enum_values:
                    continue
                # Check prefix match for autonomy_level shorthand (e.g., "2" → "2 (Task autonomy)")
                prefix_match = any(ev.startswith(part + " ") for ev in field_def.enum_values)
                if prefix_match:
                    continue
                invalid_parts.append(part)

            for bad in invalid_parts:
                suggestion = _closest_match(bad, field_def.enum_values)
                msg = f"invalid categorical value"
                if suggestion:
                    msg += f" (closest: '{suggestion}')"
                issues.append({"paper_id": paper_id, "field_name": fname,
                               "value": bad, "issue": msg})

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
) -> tuple[list[dict], list[dict]]:
    """Validate all extracted papers.

    Returns (issues, bleeds) where issues is the standard validation list
    and bleeds is the cross-field bleed detection list.
    """
    paper_ids: list[int] = []
    for status in statuses:
        rows = db._conn.execute(
            "SELECT id FROM papers WHERE status = ?", (status,),
        ).fetchall()
        paper_ids.extend(r["id"] for r in rows)

    all_issues: list[dict] = []
    all_bleeds: list[dict] = []
    for pid in sorted(set(paper_ids)):
        all_issues.extend(validate_extraction(spec, pid, db))

        # Cross-field bleed detection (after normalization, before audit)
        rows = db._conn.execute(
            """SELECT es.field_name, es.value
               FROM evidence_spans es
               JOIN extractions e ON es.extraction_id = e.id
               WHERE e.paper_id = ?""",
            (pid,),
        ).fetchall()
        spans = [{"field_name": r["field_name"], "value": r["value"]} for r in rows]
        bleeds = detect_cross_field_bleed(spec, spans)
        for b in bleeds:
            b["paper_id"] = pid
        all_bleeds.extend(bleeds)

    return all_issues, all_bleeds


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

        issues, bleeds = validate_all(spec, db)

        # Summary
        print(f"\nSpans checked:  {total_spans}")
        print(f"Issues found:   {len(issues)}")
        print(f"Cross-field bleeds: {len(bleeds)}")

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

        if bleeds:
            print("\nCross-field bleeds:")
            for b in bleeds:
                print(f"  paper={b['paper_id']:>5d}  field={b['field_name']:35s}  "
                      f"value={b['extracted_value'][:50]:50s}  belongs_to={b['belongs_to_field']}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
