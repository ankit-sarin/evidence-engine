"""Auto-generated PRISMA methods section in Markdown."""

import logging
import os

from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec
from engine.exporters.prisma import generate_prisma_flow
from engine.core.codebook import load_codebook_beside
# The local extraction stages as the manifest writer declares them: a private
# name imported rather than copied (9d-C2-R1 clause 4); C3 makes it public.
from engine.core.run_manifest import _LOCAL_EXTRACTION_STAGES

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


def _run_extraction_models(conn, run_id: int | None) -> dict[str, int]:
    """{model_name: papers} for the run's local extraction stages (R177).

    The model is the stage row's `model_name`; papers are the distinct
    `run_calls.paper_id` of those stages. No run → {}.
    """
    if run_id is None:
        return {}
    placeholders = ", ".join("?" * len(_LOCAL_EXTRACTION_STAGES))
    rows = conn.execute(
        "SELECT sc.model_name, COUNT(DISTINCT rc.paper_id) "
        "FROM run_stage_configs sc "
        "LEFT JOIN run_calls rc ON rc.run_id = sc.run_id AND rc.stage = sc.stage "
        f"WHERE sc.run_id = ? AND sc.stage_kind IN ({placeholders}) "
        "GROUP BY sc.model_name",
        (run_id, *_LOCAL_EXTRACTION_STAGES),
    ).fetchall()
    return {model: cnt for model, cnt in rows}


def _run_audit_models(conn, run_id: int | None) -> dict[str, int]:
    """{model_name: papers} for the run's audit stage (R177).

    The model is the audit stage row's `model_name`; papers are the distinct
    papers holding an `audited_ai` paper event written by the run. Not
    `run_calls`: the audit call records no `paper_id` (row C31). No run → {}.
    """
    if run_id is None:
        return {}
    rows = conn.execute(
        "SELECT sc.model_name, COUNT(DISTINCT pe.paper_id) "
        "FROM run_stage_configs sc "
        "LEFT JOIN paper_events pe ON pe.run_id = sc.run_id AND pe.to_state = 'audited_ai' "
        "WHERE sc.run_id = ? AND sc.stage_kind = 'audit' "
        "GROUP BY sc.model_name",
        (run_id,),
    ).fetchall()
    return {model: cnt for model, cnt in rows}


def generate_methods_section(db: ReviewDatabase, spec: ReviewSpec, *, run_id: int | None) -> str:
    """Generate a draft PRISMA-style methods paragraph from pipeline data.

    The extraction and audit models are read from run `run_id`'s manifest
    (`run_stage_configs`, `run_calls`, `audited_ai` paper events), never from
    the spec (row C30). `run_id=None` means no run: both model lines render
    "[MODEL NOT SPECIFIED]". The keyword is required.
    """
    flow = generate_prisma_flow(db)

    databases = ", ".join(spec.search_strategy.databases)
    start_year, end_year = spec.search_strategy.date_range
    queries = "; ".join(spec.search_strategy.query_terms)

    # Count extraction fields
    n_fields = len(load_codebook_beside(db.db_path).fields)

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

    # Extraction and audit: the run's manifest, never the spec (R177, C30).
    extraction_model_counts = _run_extraction_models(db._conn, run_id)
    if not extraction_model_counts:
        extraction_model_str = "[MODEL NOT SPECIFIED]"
    elif len(extraction_model_counts) == 1:
        extraction_model_str = next(iter(extraction_model_counts))
    else:
        extraction_model_str = _format_model_counts(extraction_model_counts)

    audit_model_counts = _run_audit_models(db._conn, run_id)
    if not audit_model_counts:
        audit_model_str = "[MODEL NOT SPECIFIED]"
    elif len(audit_model_counts) == 1:
        audit_model_str = next(iter(audit_model_counts))
    else:
        audit_model_str = _format_model_counts(audit_model_counts)

    # Cloud arms the spec ENABLES (S3g). `cloud_models` was retired with the
    # class-constant arm names (C20); an enabled arm is what a run could send.
    _PROVIDER_LABEL = {"openai": "OpenAI", "anthropic": "Anthropic"}
    cloud_parts = [
        f"{_PROVIDER_LABEL[spec.arm(name).provider]} {spec.arm(name).model}"
        for name in spec.cloud.enabled_arms
    ]

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

    methods += f"Cross-model verification was performed by {audit_model_str}."

    return methods


def export_methods_md(
    db: ReviewDatabase, spec: ReviewSpec, output_path: str, *, run_id: int | None
) -> None:
    """Write the methods section to a Markdown file. `run_id` as for
    `generate_methods_section`."""
    methods = generate_methods_section(db, spec, run_id=run_id)
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
