"""Dual-pass title/abstract screening agent using Ollama structured output."""

import json
import logging
from typing import Literal

import ollama
from pydantic import BaseModel, Field

from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec

logger = logging.getLogger(__name__)

MODEL = "qwen3:8b"

# ── Structured Output Model ──────────────────────────────────────────


class ScreeningDecision(BaseModel):
    """Structured output from the screening agent."""

    decision: Literal["include", "exclude"]
    rationale: str = Field(description="1-2 sentence explanation")
    confidence: float = Field(ge=0.0, le=1.0)


# ── Single-Paper Screening ───────────────────────────────────────────


def screen_paper(
    paper: dict,
    spec: ReviewSpec,
    pass_number: int,
) -> ScreeningDecision:
    """Screen a single paper against the review spec's criteria.

    Calls Ollama with structured JSON output.
    """
    title = paper.get("title", "")
    abstract = paper.get("abstract") or ""

    inclusion = "\n".join(f"  - {c}" for c in spec.screening_criteria.inclusion)
    exclusion = "\n".join(f"  - {c}" for c in spec.screening_criteria.exclusion)

    if abstract:
        paper_text = f"Title: {title}\n\nAbstract: {abstract}"
    else:
        paper_text = (
            f"Title: {title}\n\n"
            "Abstract: [Not available — screen based on title only. "
            "Note lower confidence in your decision.]"
        )

    user_prompt = f"""/no_think
Evaluate the following paper for inclusion in a systematic review.

INCLUSION CRITERIA:
{inclusion}

EXCLUSION CRITERIA:
{exclusion}

PAPER:
{paper_text}

Decide "include" or "exclude". When uncertain, prefer "include" — it is
better to include a borderline paper than to miss a relevant one.

Respond with JSON only: {{"decision": "...", "rationale": "...", "confidence": 0.0-1.0}}"""

    response = ollama.chat(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a systematic review screening agent. Your job is "
                    "to decide whether a paper meets the inclusion criteria for "
                    "this review. Be inclusive when uncertain — it's better to "
                    "include a borderline paper than miss a relevant one. "
                    "Respond ONLY with the requested JSON."
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


# ── Dual-Pass Screening Pipeline ─────────────────────────────────────


def run_screening(db: ReviewDatabase, spec: ReviewSpec) -> dict:
    """Run dual-pass screening on all INGESTED papers.

    Returns summary stats dict.
    """
    papers = db.get_papers_by_status("INGESTED")
    total = len(papers)
    logger.info("Starting dual-pass screening on %d papers", total)

    stats = {"screened_in": 0, "screened_out": 0, "flagged": 0, "total": total}

    for i, paper in enumerate(papers, 1):
        pid = paper["id"]

        # Pass 1
        d1 = screen_paper(paper, spec, pass_number=1)
        db.add_screening_decision(pid, 1, d1.decision, d1.rationale, MODEL)

        # Pass 2
        d2 = screen_paper(paper, spec, pass_number=2)
        db.add_screening_decision(pid, 2, d2.decision, d2.rationale, MODEL)

        # Resolve agreement
        if d1.decision == "include" and d2.decision == "include":
            db.update_status(pid, "SCREENED_IN")
            stats["screened_in"] += 1
        elif d1.decision == "exclude" and d2.decision == "exclude":
            db.update_status(pid, "SCREENED_OUT")
            stats["screened_out"] += 1
        else:
            db.update_status(pid, "SCREEN_FLAGGED")
            stats["flagged"] += 1

        if i % 10 == 0 or i == total:
            logger.info("Screened %d/%d papers", i, total)

    logger.info(
        "Screening complete: %d in, %d out, %d flagged",
        stats["screened_in"],
        stats["screened_out"],
        stats["flagged"],
    )
    return stats
