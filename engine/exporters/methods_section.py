"""Auto-generated PRISMA methods section in Markdown."""

import logging
import os

from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec
from engine.exporters.prisma import generate_prisma_flow

logger = logging.getLogger(__name__)


def _format_model_counts(model_counts: dict[str, int]) -> str:
    """Format {model: count} as 'Model1 (n=X) and Model2 (n=Y)' or just 'Model1'."""
    if not model_counts:
        return "[MODEL NOT SPECIFIED]"
    parts = [f"{model} (n={count})" for model, count in sorted(model_counts.items())]
    if len(parts) == 1:
        return parts[0]
    return " and ".join([", ".join(parts[:-1]), parts[-1]]) if len(parts) > 2 else " and ".join(parts)


def _query_ft_screening_models(db: ReviewDatabase) -> dict[str, int]:
    """Query actual FT screening models and paper counts from ft_screening_decisions."""
    rows = db._conn.execute(
        "SELECT model, COUNT(DISTINCT paper_id) as cnt FROM ft_screening_decisions GROUP BY model"
    ).fetchall()
    return {row["model"]: row["cnt"] for row in rows}


def _query_extraction_models(db: ReviewDatabase) -> dict[str, int]:
    """Query actual extraction models and paper counts from extractions table."""
    rows = db._conn.execute(
        "SELECT model, COUNT(DISTINCT paper_id) as cnt FROM extractions WHERE model IS NOT NULL GROUP BY model"
    ).fetchall()
    return {row["model"]: row["cnt"] for row in rows}


def _query_audit_models(db: ReviewDatabase) -> dict[str, int]:
    """Query actual auditor models from evidence_spans."""
    rows = db._conn.execute(
        "SELECT auditor_model, COUNT(DISTINCT es.extraction_id) as cnt "
        "FROM evidence_spans es WHERE es.auditor_model IS NOT NULL GROUP BY es.auditor_model"
    ).fetchall()
    return {row["auditor_model"]: row["cnt"] for row in rows}


def generate_methods_section(db: ReviewDatabase, spec: ReviewSpec) -> str:
    """Generate a draft PRISMA-style methods paragraph from pipeline data."""
    flow = generate_prisma_flow(db)

    databases = ", ".join(spec.search_strategy.databases)
    start_year, end_year = spec.search_strategy.date_range
    queries = "; ".join(spec.search_strategy.query_terms)

    # Count extraction fields
    n_fields = len(spec.extraction_schema.fields)

    source_parts = []
    for src, cnt in flow["records_by_source"].items():
        source_parts.append(f"{cnt} from {src}")
    source_breakdown = " and ".join(source_parts) if source_parts else "multiple sources"

    screened_in = flow["studies_included"] + flow["full_text_assessed"]
    # More precise: records that passed screening
    records_screened = flow["records_screened"]
    records_excluded = flow["records_excluded"]
    flagged = flow["screen_flagged"]
    included_for_extraction = flow["studies_included"]

    # ── Dynamic model names ──────────────────────────────────────
    # Abstract screening: from spec
    abstract_primary = spec.screening_models.primary or "[MODEL NOT SPECIFIED]"

    # FT screening: query DB for actual models used, fall back to spec
    ft_model_counts = _query_ft_screening_models(db)
    if not ft_model_counts:
        ft_primary = spec.ft_screening_models.primary or "[MODEL NOT SPECIFIED]"
        ft_verifier = spec.ft_screening_models.verifier or "[MODEL NOT SPECIFIED]"
    else:
        ft_primary = None  # will use ft_model_counts formatting

    # Extraction: query DB, fall back to spec hint (no spec field exists)
    extraction_model_counts = _query_extraction_models(db)
    if not extraction_model_counts:
        extraction_model_str = "[MODEL NOT SPECIFIED]"
    elif len(extraction_model_counts) == 1:
        extraction_model_str = next(iter(extraction_model_counts))
    else:
        extraction_model_str = _format_model_counts(extraction_model_counts)

    # Audit: query DB, fall back to spec
    audit_model_counts = _query_audit_models(db)
    if not audit_model_counts:
        audit_model_str = spec.auditor_model or "[MODEL NOT SPECIFIED]"
    elif len(audit_model_counts) == 1:
        audit_model_str = next(iter(audit_model_counts))
    else:
        audit_model_str = _format_model_counts(audit_model_counts)

    # Cloud models from spec
    cloud_parts = []
    if spec.cloud_models:
        if spec.cloud_models.openai:
            cloud_parts.append(f"OpenAI {spec.cloud_models.openai.model}")
        if spec.cloud_models.anthropic:
            cloud_parts.append(f"Anthropic {spec.cloud_models.anthropic.model}")

    # ── Build methods text ───────────────────────────────────────
    methods = (
        f"A systematic search was conducted across {databases} "
        f"covering publications from {start_year} to {end_year} "
        f"using the following queries: {queries}. "
        f"{flow['records_identified']} citations were retrieved "
        f"({source_breakdown})"
    )

    if flow["duplicates_removed"] > 0:
        methods += f" and {flow['duplicates_removed']} duplicates removed"
    methods += ". "

    methods += (
        f"Title-abstract screening was performed using a dual-pass local LLM "
        f"approach ({abstract_primary}, Ollama) with structured output constraints. "
        f"{records_screened} abstracts were screened, with "
        f"{records_screened - records_excluded - flagged} included, "
        f"{records_excluded} excluded, and {flagged} flagged for human review. "
    )

    # FT screening with actual model counts
    if ft_model_counts:
        ft_desc = _format_model_counts(ft_model_counts)
        methods += f"Full-text screening was performed by {ft_desc}. "
    elif ft_primary:
        methods += (
            f"Full-text screening was performed by {ft_primary} "
            f"with verification by {ft_verifier}. "
        )

    methods += (
        f"Data extraction was performed using {extraction_model_str} with a two-pass "
        f"reasoning-then-structured-output approach on {included_for_extraction} "
        f"included studies across {n_fields} predefined fields. "
    )

    if cloud_parts:
        methods += (
            f"Concordance extraction was additionally performed by "
            f"{' and '.join(cloud_parts)}. "
        )

    methods += (
        f"Cross-model verification was performed by {audit_model_str}. "
        f"{flow['spans_verified']} evidence spans were verified and "
        f"{flow['spans_flagged']} flagged for review."
    )

    return methods


def export_methods_md(
    db: ReviewDatabase, spec: ReviewSpec, output_path: str
) -> None:
    """Write the methods section to a Markdown file."""
    methods = generate_methods_section(db, spec)
    tmp_path = output_path + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            f.write("# Methods\n\n")
            f.write(methods)
            f.write("\n")
        os.replace(tmp_path, output_path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

    logger.info("Methods section exported to %s", output_path)
