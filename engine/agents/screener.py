"""Dual-model screening agent using Ollama structured output.

Phase 1 (Primary): Dual-pass screen with a fast model (high recall).
Phase 2 (Verification): Re-screen includes with a larger model (higher precision).
"""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, ValidationError

from engine.utils.ollama_client import ollama_chat

from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec

logger = logging.getLogger(__name__)

# Fallback defaults when no spec.screening_models is available
DEFAULT_PRIMARY_MODEL = "qwen3:8b"
DEFAULT_VERIFICATION_MODEL = "qwen3:32b"

# ── Structured Output Model ──────────────────────────────────────────


class ScreeningDecision(BaseModel):
    """Structured output from the screening agent."""

    decision: Literal["include", "exclude"]
    rationale: str = Field(description="1-2 sentence explanation")
    confidence: float = Field(ge=0.0, le=1.0)


# ── Prompt Builder ───────────────────────────────────────────────────


def _build_prompt(paper: dict, spec: ReviewSpec, *, role: str = "primary") -> str:
    """Build the screening prompt with PICO context and criteria.

    Args:
        role: "primary" for high-recall first pass, "verifier" for strict second pass.
    """
    title = paper.get("title", "")
    abstract = paper.get("abstract") or ""

    outcomes_str = "; ".join(spec.pico.outcomes)
    pico_block = (
        f"Population: {spec.pico.population}\n"
        f"Intervention: {spec.pico.intervention}\n"
        f"Comparator: {spec.pico.comparator}\n"
        f"Outcomes: {outcomes_str}"
    )

    inclusion = "\n".join(f"  - {c}" for c in spec.screening_criteria.inclusion)

    if role == "verifier":
        # Verifier sees full exclusion criteria (strict)
        exclusion = "\n".join(f"  - {c}" for c in spec.screening_criteria.exclusion)
    else:
        # Primary sees simplified exclusion criteria (high recall)
        primary_exclusions = [
            "Systematic reviews, meta-analyses, or scoping reviews",
            "Editorials, commentaries, or letters to the editor",
            "Non-surgical robotics (industrial, rehabilitation, exoskeletons, prosthetics)",
            "Papers with no abstract available",
        ]
        exclusion = "\n".join(f"  - {c}" for c in primary_exclusions)

    specialty_block = ""
    if spec.specialty_scope:
        specialty_block = "\n" + spec.specialty_scope.format_for_prompt() + "\n"

    if abstract:
        paper_text = f"Title: {title}\n\nAbstract: {abstract}"
    else:
        paper_text = (
            f"Title: {title}\n\n"
            "Abstract: [Not available. Per the exclusion criteria, papers with "
            "no abstract or insufficient information to determine eligibility "
            "should be EXCLUDED.]"
        )

    if role == "verifier":
        # Strict verification pass — high precision, catches FPs
        decision_instruction = (
            "You are the VERIFICATION pass. This paper was already included by "
            "a primary screener. Your job is to catch false positives.\n\n"
            "Apply these tests strictly:\n"
            "1. Does the abstract describe a robot that EXECUTES a surgical action "
            "autonomously or semi-autonomously? (Not just analysis/tracking/assessment)\n"
            "2. Is there an autonomous component — not purely teleoperated/master-slave?\n"
            "3. Does it involve a physical surgical task — not just a simulation "
            "framework or pure methodology?\n"
            "4. Is it original research — not a review, editorial, or commentary?\n\n"
            "If ANY test fails, EXCLUDE. Only include papers that clearly pass all tests."
        )
    else:
        # Primary pass — high recall, inclusive
        decision_instruction = (
            "Decide 'include' or 'exclude'. When uncertain and the paper MIGHT "
            "involve autonomous surgical robotics, prefer 'include' — a later "
            "verification pass will catch false positives.\n"
            "EXCLUDE only if the paper CLEARLY does not involve surgical robotics "
            "at all, or CLEARLY has no abstract available."
        )

    return f"""/no_think
Evaluate the following paper for inclusion in a systematic review.

REVIEW FOCUS (PICO):
{pico_block}

INCLUSION CRITERIA:
{inclusion}

EXCLUSION CRITERIA:
{exclusion}
{specialty_block}
PAPER:
{paper_text}

{decision_instruction}

Respond with JSON only: {{"decision": "...", "rationale": "...", "confidence": 0.0-1.0}}"""


# ── Single-Paper Screening ───────────────────────────────────────────


def screen_paper(
    paper: dict,
    spec: ReviewSpec,
    pass_number: int,
    model: str | None = None,
    role: str = "primary",
) -> ScreeningDecision:
    """Screen a single paper against the review spec's criteria.

    Calls Ollama with structured JSON output.
    If model is not specified, uses spec.screening_models.primary.
    role: "primary" for high-recall pass, "verifier" for strict pass.
    """
    if model is None:
        model = spec.screening_models.primary

    user_prompt = _build_prompt(paper, spec, role=role)

    response = ollama_chat(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a systematic review screening agent. Evaluate "
                    "whether the paper involves autonomous or semi-autonomous "
                    "surgical robotics. Follow the criteria and instructions "
                    "in the user message. Respond ONLY with the requested JSON."
                ),
            },
            {"role": "user", "content": user_prompt},
        ],
        format=ScreeningDecision.model_json_schema(),
        options={"temperature": 0},
        think=False,
    )

    raw = response.message.content
    return ScreeningDecision.model_validate_json(raw)


# ── Checkpoint Helpers ───────────────────────────────────────────────


def _checkpoint_path(db: ReviewDatabase, suffix: str = "") -> Path:
    """Return the screening checkpoint file path for this review."""
    name = f"screening_checkpoint{suffix}.json"
    return db.db_path.parent / name


def _load_checkpoint(path: Path) -> set[int]:
    """Load set of already-screened paper IDs from checkpoint file."""
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text())
        return set(data.get("screened_ids", []))
    except (json.JSONDecodeError, KeyError):
        return set()


def _save_checkpoint(path: Path, screened_ids: set[int]) -> None:
    """Persist screened paper IDs to checkpoint file."""
    path.write_text(json.dumps({"screened_ids": sorted(screened_ids)}))


# ── Primary Screening (Dual-Pass) ───────────────────────────────────


def run_screening(db: ReviewDatabase, spec: ReviewSpec) -> dict:
    """Run dual-pass primary screening on all INGESTED papers.

    Supports checkpoint/resume — if interrupted, re-running will skip
    papers that were already screened and committed to the database.

    Returns summary stats dict.
    """
    primary_model = spec.screening_models.primary
    papers = db.get_papers_by_status("INGESTED")
    total = len(papers)

    ckpt_path = _checkpoint_path(db)
    screened_ids = _load_checkpoint(ckpt_path)

    if screened_ids:
        logger.info(
            "Resuming screening: %d already screened, %d INGESTED remaining",
            len(screened_ids), total,
        )

    pending = [p for p in papers if p["id"] not in screened_ids]
    logger.info("Starting dual-pass screening on %d papers (%d pending)", total, len(pending))

    stats = {"screened_in": 0, "screened_out": 0, "flagged": 0, "parse_errors": 0, "total": len(pending)}

    for i, paper in enumerate(pending, 1):
        pid = paper["id"]

        try:
            # Pass 1
            d1 = screen_paper(paper, spec, pass_number=1, model=primary_model)
            db.add_screening_decision(pid, 1, d1.decision, d1.rationale, primary_model)

            # Pass 2
            d2 = screen_paper(paper, spec, pass_number=2, model=primary_model)
            db.add_screening_decision(pid, 2, d2.decision, d2.rationale, primary_model)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning(
                "Paper %d: malformed LLM output — flagging for review: %s",
                pid, str(exc)[:200],
            )
            db.update_status(pid, "ABSTRACT_SCREEN_FLAGGED")
            stats["flagged"] += 1
            stats["parse_errors"] += 1
            screened_ids.add(pid)
            continue

        # Resolve agreement
        if d1.decision == "include" and d2.decision == "include":
            db.update_status(pid, "ABSTRACT_SCREENED_IN")
            stats["screened_in"] += 1
        elif d1.decision == "exclude" and d2.decision == "exclude":
            db.update_status(pid, "ABSTRACT_SCREENED_OUT")
            stats["screened_out"] += 1
        else:
            db.update_status(pid, "ABSTRACT_SCREEN_FLAGGED")
            stats["flagged"] += 1

        screened_ids.add(pid)

        if i % 10 == 0 or i == len(pending):
            _save_checkpoint(ckpt_path, screened_ids)
            logger.info("Screened %d/%d papers (checkpoint saved)", i, len(pending))

    # Clean up checkpoint file on successful completion
    if ckpt_path.exists():
        ckpt_path.unlink()

    logger.info(
        "Screening complete: %d in, %d out, %d flagged",
        stats["screened_in"],
        stats["screened_out"],
        stats["flagged"],
    )
    return stats


# ── Verification Screening ───────────────────────────────────────────


def run_verification(db: ReviewDatabase, spec: ReviewSpec) -> dict:
    """Re-screen ABSTRACT_SCREENED_IN papers with the verification model.

    Consensus logic:
      - Both models include → stays ABSTRACT_SCREENED_IN
      - Primary included, verifier excludes → ABSTRACT_SCREEN_FLAGGED

    Supports checkpoint/resume.
    Returns summary stats dict.
    """
    verification_model = spec.screening_models.verification
    papers = db.get_papers_by_status("ABSTRACT_SCREENED_IN")

    ckpt_path = _checkpoint_path(db, suffix="_verification")
    verified_ids = _load_checkpoint(ckpt_path)

    if verified_ids:
        logger.info(
            "Resuming verification: %d already verified, %d ABSTRACT_SCREENED_IN total",
            len(verified_ids), len(papers),
        )

    pending = [p for p in papers if p["id"] not in verified_ids]
    logger.info(
        "Starting verification on %d ABSTRACT_SCREENED_IN papers (%d pending) with %s",
        len(papers), len(pending), verification_model,
    )

    stats = {"confirmed": 0, "flagged": 0, "parse_errors": 0, "total": len(pending)}

    for i, paper in enumerate(pending, 1):
        pid = paper["id"]

        try:
            decision = screen_paper(paper, spec, pass_number=1, model=verification_model, role="verifier")
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning(
                "Paper %d: malformed verifier output — flagging: %s",
                pid, str(exc)[:200],
            )
            db.update_status(pid, "ABSTRACT_SCREEN_FLAGGED")
            stats["flagged"] += 1
            stats["parse_errors"] += 1
            verified_ids.add(pid)
            continue

        db.add_verification_decision(
            pid, decision.decision, decision.rationale, verification_model,
        )

        if decision.decision == "include":
            # Stays ABSTRACT_SCREENED_IN — no status change needed
            stats["confirmed"] += 1
        else:
            # Verifier disagrees — flag for human review
            db.update_status(pid, "ABSTRACT_SCREEN_FLAGGED")
            stats["flagged"] += 1

        verified_ids.add(pid)

        if i % 10 == 0 or i == len(pending):
            _save_checkpoint(ckpt_path, verified_ids)
            logger.info(
                "Verified %d/%d papers — %d confirmed, %d flagged (checkpoint saved)",
                i, len(pending), stats["confirmed"], stats["flagged"],
            )

    # Clean up checkpoint file on successful completion
    if ckpt_path.exists():
        ckpt_path.unlink()

    logger.info(
        "Verification complete: %d confirmed, %d flagged for human review",
        stats["confirmed"],
        stats["flagged"],
    )

    # Auto-advance workflow: ABSTRACT_SCREENING_COMPLETE
    try:
        from engine.adjudication.workflow import complete_stage
        complete_stage(
            db._conn, "ABSTRACT_SCREENING_COMPLETE",
            metadata=f"{stats['confirmed']} confirmed, {stats['flagged']} flagged",
        )
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            logger.debug("Workflow table not found — skipping stage advance")
        else:
            raise

    return stats
