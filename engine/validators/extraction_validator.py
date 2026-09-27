"""Post-extraction field validation — read-only diagnostic tool.

Checks one arm's effective values against the codebook:
  - Invalid categorical values (with closest-match suggestion), element-wise
    over semicolon-separated lists
  - Non-numeric sample_size values
Absence sentinels (the codebook's) and the non-value tokens are skipped.

**It reads the reader** (WRITE-PATH-01 9d-C1, R175): the cells are
`engine.core.effective.iter_grid`'s rows for ONE arm over the corpus — eligible
papers x codebook fields. So what is validated is each cell's effective value
under resolution rule v2.1 — one value per cell, not every claim ever written —
which is the effective-result model working as designed. An empty cell (value
None, state `missing`) is skipped, not flagged.

**The arm** (R174 as amended by 9d-C1) is the spec's extraction arm
(`extraction_models.arm`) unless the CLI's `--arm` names another. An `--arm`
that is not a registered arm is refused, listing the registered arms. The
default is used as named: if it is not registered yet (true on a review whose
extraction arm has not pinned at its first manifest), the validator reports zero
cells for it and never refuses.

**There is no unknown-field-name check.** The grid's fields are the codebook's
by construction, so the check could not fire on a grid row. An unknown field
name is the event writer's to refuse (Step 2 row B15).

Also provides prefix normalization for categorical values (Item 87a).
"""

import argparse
import difflib
import hashlib
import logging
import sqlite3
import sys

from engine.core.codebook import Codebook, load_codebook_beside
from engine.core.effective import eligible_paper_ids, iter_grid, registered_arms
from engine.core.review_paths import data_root_for, load_spec_for
from engine.core.review_spec import ReviewSpec

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


def _non_value_tokens(codebook: Codebook) -> frozenset[str]:
    """The review's non-value tokens (ELICIT-DESIGN-02 D1, site 3).

    A terminal state is not a categorical value. The two read-only checks skip
    the tokens: reporting a terminal state as an "invalid categorical value" is a
    false positive that would grow with every field the engine correctly refused.
    (The in-place rewrite path that also skipped them retired 9c-C5, R160a.)

    Absence sentinels are skipped at each site through the codebook
    (`Codebook.is_absence_sentinel`, R124); there is no hand-list here.
    """
    from engine.elicitation.classes import non_value_tokens

    return non_value_tokens(codebook.raw)


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


def detect_cross_field_bleed(
    codebook,
    extraction_data: list[dict],
    non_value: frozenset[str] = frozenset(),
) -> list[dict]:
    """Detect categorical values that belong to a different field's vocabulary.

    For each categorical span, if the value is NOT valid for its own field
    but IS an exact (case-insensitive) match for another categorical field's
    valid_values, flag it as cross-field bleed.

    Args:
        codebook: the review's Codebook — the field vocabulary authority.
        extraction_data: List of ``{"field_name": str, "value": str}`` dicts.

    Returns:
        List of ``{field_name, extracted_value, belongs_to_field}`` records.
    """
    field_map = {v.name: v for v in codebook.views}

    # Build reverse lookup: lowered value → list of field names that accept it
    value_to_fields: dict[str, list[str]] = {}
    for f in codebook.views:
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

        if codebook.is_absence_sentinel(value):
            continue

        if str(value).strip().upper() in non_value:
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
    codebook: Codebook, paper_id: int, cells: list[dict],
) -> list[dict]:
    """Validate one paper's cells for one arm against the codebook.

    `cells` are ``{"field_name", "value"}`` dicts built from `iter_grid` rows —
    field names are the codebook's by construction. A cell whose value is None
    (nothing recorded for the arm) is skipped, not flagged.

    Returns a list of issue dicts: {paper_id, field_name, value, issue}.
    Read-only.
    """
    field_map = {v.name: v for v in codebook.views}
    non_value = _non_value_tokens(codebook)

    issues: list[dict] = []

    for cell in cells:
        fname = cell["field_name"]
        value = cell["value"]

        if value is None:
            continue

        field_def = field_map[fname]

        # Skip absence sentinels — they're valid for any field (R124: the codebook's)
        if codebook.is_absence_sentinel(value):
            continue

        if str(value).strip().upper() in non_value:
            continue

        # 1. Categorical field — check enum_values
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

        # 2. sample_size — must be integer or an absence sentinel (already handled above)
        if fname == "sample_size":
            stripped = value.strip()
            try:
                int(stripped)
            except ValueError:
                issues.append({"paper_id": paper_id, "field_name": fname,
                               "value": value,
                               "issue": "non-numeric sample_size (expected integer or NR)"})

    return issues


def arm_cells(conn: sqlite3.Connection, codebook: Codebook, arm: str) -> dict[int, list[dict]]:
    """The arm's grid over the corpus, as ``{paper_id: [{"field_name", "value"}, ...]}``.

    Empty for an arm that is not registered: `iter_grid` raises `UnknownArm`
    for one, and the default arm is reported, never refused (R174 as amended
    by 9d-C1).
    """
    if arm not in registered_arms(conn, include_retired=True):
        return {}
    cells: dict[int, list[dict]] = {}
    for paper_id, field_name, _arm, ev in iter_grid(conn, codebook=codebook, arms=(arm,)):
        cells.setdefault(paper_id, []).append({"field_name": field_name, "value": ev.value})
    return cells


def validate_all(
    conn: sqlite3.Connection, codebook: Codebook, *, arm: str,
) -> tuple[list[dict], list[dict]]:
    """Validate every corpus paper's cells for `arm`.

    Returns (issues, bleeds) where issues is the standard validation list
    and bleeds is the cross-field bleed detection list.
    """
    non_value = _non_value_tokens(codebook)
    all_issues: list[dict] = []
    all_bleeds: list[dict] = []
    for pid, cells in sorted(arm_cells(conn, codebook, arm).items()):
        all_issues.extend(validate_extraction(codebook, pid, cells))

        spans = [c for c in cells if c["value"] is not None]
        bleeds = detect_cross_field_bleed(codebook, spans, non_value)
        for b in bleeds:
            b["paper_id"] = pid
        all_bleeds.extend(bleeds)

    return all_issues, all_bleeds


# ── CLI ──────────────────────────────────────────────────────────────


class ArmNotRegistered(ValueError):
    """`--arm` named an arm this review's registry does not hold."""


def select_arm(conn: sqlite3.Connection, spec: ReviewSpec, override: str | None) -> str:
    """The arm to report (R174 as amended by 9d-C1): `override` if given — which
    must be registered — else the spec's extraction arm, used as named."""
    if override is None:
        return spec.extraction_models.arm
    registered = registered_arms(conn, include_retired=True)
    if override not in registered:
        raise ArmNotRegistered(
            f"--arm {override!r} is not a registered arm of this review; "
            f"registered arms are {list(registered)}.")
    return override


def open_read_only(db_path) -> sqlite3.Connection:
    """`mode=ro`, never `immutable=1`: the database is live."""
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Post-extraction field validation (read-only)")
    parser.add_argument("--review", required=True, help="Review id. The review's identity — the spec file and the data root both derive from it.")
    parser.add_argument("--spec", default=None,
                        help="Override the Review Spec path. Defaults to review_specs/<review>.yaml; an override must carry the same review_id.")
    parser.add_argument("--arm", default=None,
                        help="Arm to validate. Defaults to the spec's extraction_models.arm; an override must be a registered arm.")
    args = parser.parse_args(argv)

    spec = load_spec_for(args.review, args.spec)
    db_path = data_root_for(args.review) / "review.db"
    codebook = load_codebook_beside(db_path)
    conn = open_read_only(db_path)

    try:
        try:
            arm = select_arm(conn, spec, args.arm)
        except ArmNotRegistered as exc:
            print(f"refused: {exc}", file=sys.stderr)
            return 2

        cells = arm_cells(conn, codebook, arm)
        populated = sum(1 for cs in cells.values() for c in cs if c["value"] is not None)
        issues, bleeds = validate_all(conn, codebook, arm=arm)

        # Summary
        registered = "" if arm in registered_arms(conn, include_retired=True) \
            else "  (not a registered arm — zero cells)"
        print(f"\nArm:             {arm}{registered}")
        print(f"Corpus papers:   {len(eligible_paper_ids(conn))}")
        print(f"Populated cells: {populated}")
        print(f"Issues found:    {len(issues)}")
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
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
