"""Auto-generated PRISMA methods section in Markdown."""

import logging

from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec
from engine.exporters.prisma import generate_prisma_flow

logger = logging.getLogger(__name__)


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
        f"approach (qwen3:8b, Ollama) with structured output constraints. "
        f"{records_screened} abstracts were screened, with "
        f"{records_screened - records_excluded - flagged} included, "
        f"{records_excluded} excluded, and {flagged} flagged for human review. "
    )

    methods += (
        f"Data extraction was performed using deepseek-r1:32b with a two-pass "
        f"reasoning-then-structured-output approach on {included_for_extraction} "
        f"included studies across {n_fields} predefined fields. "
    )

    methods += (
        f"Cross-model verification was performed by qwen3:32b. "
        f"{flow['spans_verified']} evidence spans were verified and "
        f"{flow['spans_flagged']} flagged for review."
    )

    return methods


def export_methods_md(
    db: ReviewDatabase, spec: ReviewSpec, output_path: str
) -> None:
    """Write the methods section to a Markdown file."""
    methods = generate_methods_section(db, spec)
    with open(output_path, "w") as f:
        f.write("# Methods\n\n")
        f.write(methods)
        f.write("\n")

    logger.info("Methods section exported to %s", output_path)
