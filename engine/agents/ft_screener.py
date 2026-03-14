"""Full-text screening agent — dual-model, cross-family.

Primary: Qwen3.5:27b (Alibaba/Qwen) — high-recall screen on parsed full text.
Verifier: Gemma3:27b (Google/DeepMind) — strict verification of primary includes.

Mirrors the abstract screening architecture but operates on parsed PDF text
with specialty scope filtering.
"""

import json
import logging
import re
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from engine.core.constants import FT_MAX_TEXT_CHARS, FT_REASON_CODES
from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec
from engine.utils.ollama_client import ollama_chat

logger = logging.getLogger(__name__)


# ── Structured Output Models ─────────────────────────────────────────


class FTScreeningDecision(BaseModel):
    """Structured output from the full-text primary screener."""

    decision: Literal["FT_ELIGIBLE", "FT_EXCLUDE"]
    reason_code: str = Field(description="One of: eligible, wrong_specialty, no_autonomy_content, wrong_intervention, protocol_only, duplicate_cohort, insufficient_data")
    rationale: str = Field(description="1-3 sentence explanation")
    confidence: float = Field(ge=0.0, le=1.0)


class FTVerificationDecision(BaseModel):
    """Structured output from the full-text verifier."""

    decision: Literal["FT_ELIGIBLE", "FT_FLAGGED"]
    rationale: str = Field(description="1-3 sentence explanation")
    confidence: float = Field(ge=0.0, le=1.0)


# ── Text Truncation ──────────────────────────────────────────────────


# Section header patterns (Markdown headings or uppercase labels)
_SECTION_RE = re.compile(
    r"^(?:#{1,4}\s+)?"
    r"(abstract|introduction|background|methods?|materials?\s+and\s+methods?|"
    r"results?|discussion|conclusion|references|acknowledgements?|"
    r"supplementary|appendix)",
    re.IGNORECASE | re.MULTILINE,
)


def truncate_paper_text(full_text: str, title: str = "",
                        abstract: str = "", max_chars: int = FT_MAX_TEXT_CHARS) -> str:
    """Truncate parsed full text to fit within the token budget.

    Strategy: Always include title + abstract at the top. Then include as much
    of the body as fits, prioritizing Introduction, Methods, and Results.
    Truncate from the end if the text exceeds max_chars.
    """
    header = ""
    if title:
        header += f"Title: {title}\n\n"
    if abstract:
        header += f"Abstract: {abstract}\n\n"

    remaining_budget = max_chars - len(header)
    if remaining_budget <= 0:
        return header[:max_chars]

    if len(full_text) <= remaining_budget:
        return header + full_text

    # Try to find where References/Acknowledgements start and cut there
    ref_match = re.search(
        r"^(?:#{1,4}\s+)?(?:references|bibliography|acknowledgements?)\b",
        full_text,
        re.IGNORECASE | re.MULTILINE,
    )
    if ref_match and ref_match.start() <= remaining_budget:
        body = full_text[:ref_match.start()].rstrip()
    else:
        body = full_text[:remaining_budget]

    # Trim to last complete sentence if possible
    last_period = body.rfind(". ")
    if last_period > len(body) * 0.8:
        body = body[:last_period + 1]

    return header + body


# ── Prompt Builders ──────────────────────────────────────────────────


def build_ft_screening_prompt(paper_text: str, spec: ReviewSpec) -> str:
    """Build the full-text screening prompt with PICO and specialty scope."""
    outcomes_str = "; ".join(spec.pico.outcomes)
    pico_block = (
        f"Population: {spec.pico.population}\n"
        f"Intervention: {spec.pico.intervention}\n"
        f"Comparator: {spec.pico.comparator}\n"
        f"Outcomes: {outcomes_str}"
    )

    inclusion = "\n".join(f"  - {c}" for c in spec.screening_criteria.inclusion)
    exclusion = "\n".join(f"  - {c}" for c in spec.screening_criteria.exclusion)

    specialty_block = ""
    if spec.specialty_scope:
        specialty_block = "\n" + spec.specialty_scope.format_for_prompt() + "\n"

    reason_codes_str = ", ".join(FT_REASON_CODES)

    return f"""/no_think
You are performing FULL-TEXT screening for a systematic review. You have access to
the paper's full text (or a substantial portion). Evaluate whether this paper meets
all eligibility criteria.

REVIEW FOCUS (PICO):
{pico_block}

INCLUSION CRITERIA:
{inclusion}

EXCLUSION CRITERIA:
{exclusion}
{specialty_block}
REASON CODES (use exactly one):
  - eligible: Paper passes all criteria
  - wrong_specialty: Autonomous task is in an excluded surgical specialty
  - no_autonomy_content: Abstract suggested autonomy but full text reveals no autonomous component
  - wrong_intervention: Not surgical robotics (e.g., industrial, rehabilitation)
  - protocol_only: Study protocol without results
  - duplicate_cohort: Overlapping dataset with another included paper
  - insufficient_data: Commentary, letter, or editorial with no extractable data

PAPER FULL TEXT:
{paper_text}

Based on the full text, classify this paper as FT_ELIGIBLE or FT_EXCLUDE.
Respond with JSON only: {{"decision": "FT_ELIGIBLE" or "FT_EXCLUDE", "reason_code": "<one of: {reason_codes_str}>", "rationale": "...", "confidence": 0.0-1.0}}"""


def build_ft_verification_prompt(paper_text: str, spec: ReviewSpec) -> str:
    """Build the full-text verification prompt (strict, FP-catching)."""
    outcomes_str = "; ".join(spec.pico.outcomes)
    pico_block = (
        f"Population: {spec.pico.population}\n"
        f"Intervention: {spec.pico.intervention}\n"
        f"Comparator: {spec.pico.comparator}\n"
        f"Outcomes: {outcomes_str}"
    )

    exclusion = "\n".join(f"  - {c}" for c in spec.screening_criteria.exclusion)

    specialty_block = ""
    if spec.specialty_scope:
        specialty_block = "\n" + spec.specialty_scope.format_for_prompt() + "\n"

    return f"""/no_think
You are the VERIFICATION pass for full-text screening. This paper was already
marked as eligible by a primary screener. Your job is to catch false positives.

REVIEW FOCUS (PICO):
{pico_block}

EXCLUSION CRITERIA:
{exclusion}
{specialty_block}
Apply these tests strictly:
1. Does the full text describe a robot that EXECUTES a surgical action
   autonomously or semi-autonomously? (Not just analysis/tracking/assessment)
2. Is there an autonomous component — not purely teleoperated/master-slave?
3. Does it involve a physical surgical task — not just a simulation
   framework or pure methodology?
4. Is the surgical specialty within scope (not dental, ophthalmic, etc.
   unless the task is a generalizable bench/preclinical autonomy task)?
5. Is it original research with extractable data — not a protocol, review,
   or commentary?

If ANY test fails, mark as FT_FLAGGED. Only mark FT_ELIGIBLE if the paper
clearly passes all tests.

PAPER FULL TEXT:
{paper_text}

Respond with JSON only: {{"decision": "FT_ELIGIBLE" or "FT_FLAGGED", "rationale": "...", "confidence": 0.0-1.0}}"""


# ── Single-Paper Screening ───────────────────────────────────────────


def ft_screen_paper(
    paper_text: str,
    spec: ReviewSpec,
    model: str | None = None,
    think: bool | None = None,
    temperature: float | None = None,
) -> FTScreeningDecision:
    """Screen a single paper's full text. Returns structured decision."""
    if model is None:
        model = spec.ft_screening_models.primary
    if think is None:
        think = spec.ft_screening_models.think
    if temperature is None:
        temperature = spec.ft_screening_models.temperature

    prompt = build_ft_screening_prompt(paper_text, spec)

    response = ollama_chat(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a systematic review full-text screening agent. "
                    "Evaluate eligibility based on the full paper text. "
                    "Respond ONLY with the requested JSON."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        format=FTScreeningDecision.model_json_schema(),
        options={"temperature": temperature},
        think=think,
    )

    return FTScreeningDecision.model_validate_json(response.message.content)


def ft_verify_paper(
    paper_text: str,
    spec: ReviewSpec,
    model: str | None = None,
    think: bool | None = None,
    temperature: float | None = None,
) -> FTVerificationDecision:
    """Verify a single paper's full text (strict, FP-catching). Returns structured decision."""
    if model is None:
        model = spec.ft_screening_models.verifier
    if think is None:
        think = spec.ft_screening_models.think
    if temperature is None:
        temperature = spec.ft_screening_models.temperature

    prompt = build_ft_verification_prompt(paper_text, spec)

    response = ollama_chat(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a systematic review full-text verification agent. "
                    "Your job is to catch false positives. Be strict. "
                    "Respond ONLY with the requested JSON."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        format=FTVerificationDecision.model_json_schema(),
        options={"temperature": temperature},
        think=think,
    )

    return FTVerificationDecision.model_validate_json(response.message.content)


# ── Checkpoint Helpers ───────────────────────────────────────────────


def _checkpoint_path(db: ReviewDatabase, suffix: str = "") -> Path:
    name = f"ft_screening_checkpoint{suffix}.json"
    return db.db_path.parent / name


def _load_checkpoint(path: Path) -> set[int]:
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text())
        return set(data.get("screened_ids", []))
    except (json.JSONDecodeError, KeyError):
        return set()


def _save_checkpoint(path: Path, screened_ids: set[int]) -> None:
    path.write_text(json.dumps({"screened_ids": sorted(screened_ids)}))


# ── Parsed Text Loader ──────────────────────────────────────────────


def _load_parsed_text(db: ReviewDatabase, paper_id: int) -> str | None:
    """Load the latest parsed text for a paper from the parsed_text directory."""
    parsed_dir = db.db_path.parent / "parsed_text"
    if not parsed_dir.exists():
        return None

    # Find latest version: {paper_id}_v{N}.md
    candidates = sorted(parsed_dir.glob(f"{paper_id}_v*.md"), reverse=True)
    if candidates:
        return candidates[0].read_text()

    return None


# ── Pipeline: Primary Screening ─────────────────────────────────────


def run_ft_screening(
    db: ReviewDatabase, spec: ReviewSpec, review_name: str = "",
) -> dict:
    """Run full-text primary screening on all PARSED papers with parsed text.

    Papers at PARSED status (or AI_AUDIT_COMPLETE for the existing corpus)
    that have parsed text available are screened.

    Returns summary stats dict.
    """
    primary_model = spec.ft_screening_models.primary

    # Pre-flight: verify models are loaded and responsive
    from engine.utils.ollama_preflight import require_preflight
    require_preflight(
        [spec.ft_screening_models.primary, spec.ft_screening_models.verifier],
        runner_name="FT screening",
    )

    # Collect eligible papers: PARSED or AI_AUDIT_COMPLETE with parsed text
    papers = db.get_papers_by_status("PARSED") + db.get_papers_by_status("AI_AUDIT_COMPLETE")
    total = len(papers)

    ckpt_path = _checkpoint_path(db)
    screened_ids = _load_checkpoint(ckpt_path)

    if screened_ids:
        logger.info(
            "Resuming FT screening: %d already screened, %d PARSED total",
            len(screened_ids), total,
        )

    pending = [p for p in papers if p["id"] not in screened_ids]
    logger.info(
        "Starting full-text screening on %d papers (%d pending) with %s",
        total, len(pending), primary_model,
    )

    stats = {"ft_eligible": 0, "ft_exclude": 0, "skipped_no_text": 0, "total": len(pending)}

    for i, paper in enumerate(pending, 1):
        pid = paper["id"]

        # Load parsed text
        parsed_text = _load_parsed_text(db, pid)
        if not parsed_text:
            logger.warning("Paper %d has no parsed text — skipping", pid)
            stats["skipped_no_text"] += 1
            screened_ids.add(pid)
            continue

        # Truncate for prompt
        truncated = truncate_paper_text(
            parsed_text,
            title=paper.get("title", ""),
            abstract=paper.get("abstract", ""),
        )

        # Screen
        decision = ft_screen_paper(truncated, spec, model=primary_model)

        # Write decision to DB
        db.add_ft_screening_decision(
            pid, primary_model, decision.decision,
            decision.reason_code, decision.rationale, decision.confidence,
        )

        # Update paper status — skip if already past FT screening
        current_status = paper.get("status", "")
        _PAST_FT = {"FT_ELIGIBLE", "FT_FLAGGED", "EXTRACTED", "EXTRACT_FAILED",
                     "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE", "REJECTED"}

        if current_status in _PAST_FT:
            logger.info(
                "Paper %d already at %s, FT decision recorded without status change",
                pid, current_status,
            )
            if decision.decision == "FT_ELIGIBLE":
                stats["ft_eligible"] += 1
            else:
                stats["ft_exclude"] += 1
        elif decision.decision == "FT_ELIGIBLE":
            db.update_status(pid, "FT_ELIGIBLE")
            stats["ft_eligible"] += 1
        else:
            db.update_status(pid, "FT_SCREENED_OUT")
            stats["ft_exclude"] += 1

        screened_ids.add(pid)

        if i % 10 == 0 or i == len(pending):
            _save_checkpoint(ckpt_path, screened_ids)
            logger.info(
                "FT screened %d/%d — %d eligible, %d excluded (checkpoint saved)",
                i, len(pending), stats["ft_eligible"], stats["ft_exclude"],
            )

    if ckpt_path.exists():
        ckpt_path.unlink()

    logger.info(
        "FT screening complete: %d eligible, %d excluded, %d skipped (no text)",
        stats["ft_eligible"], stats["ft_exclude"], stats["skipped_no_text"],
    )
    return stats


# ── Pipeline: Verification ──────────────────────────────────────────


def run_ft_verification(
    db: ReviewDatabase, spec: ReviewSpec, review_name: str = "",
) -> dict:
    """Re-screen FT_ELIGIBLE papers with the verification model.

    Consensus logic:
      - Verifier confirms → stays FT_ELIGIBLE
      - Verifier flags → FT_FLAGGED (for human adjudication)
    """
    verification_model = spec.ft_screening_models.verifier
    papers = db.get_papers_by_status("FT_ELIGIBLE")

    ckpt_path = _checkpoint_path(db, suffix="_verification")
    verified_ids = _load_checkpoint(ckpt_path)

    if verified_ids:
        logger.info(
            "Resuming FT verification: %d already verified, %d FT_ELIGIBLE total",
            len(verified_ids), len(papers),
        )

    pending = [p for p in papers if p["id"] not in verified_ids]
    logger.info(
        "Starting FT verification on %d papers (%d pending) with %s",
        len(papers), len(pending), verification_model,
    )

    stats = {"confirmed": 0, "flagged": 0, "total": len(pending)}

    for i, paper in enumerate(pending, 1):
        pid = paper["id"]

        parsed_text = _load_parsed_text(db, pid)
        if not parsed_text:
            logger.warning("Paper %d has no parsed text — skipping verification", pid)
            verified_ids.add(pid)
            continue

        truncated = truncate_paper_text(
            parsed_text,
            title=paper.get("title", ""),
            abstract=paper.get("abstract", ""),
        )

        decision = ft_verify_paper(truncated, spec, model=verification_model)

        db.add_ft_verification_decision(
            pid, verification_model, decision.decision,
            decision.rationale, decision.confidence,
        )

        if decision.decision == "FT_ELIGIBLE":
            stats["confirmed"] += 1
        else:
            db.update_status(pid, "FT_FLAGGED")
            stats["flagged"] += 1

        verified_ids.add(pid)

        if i % 10 == 0 or i == len(pending):
            _save_checkpoint(ckpt_path, verified_ids)
            logger.info(
                "FT verified %d/%d — %d confirmed, %d flagged (checkpoint saved)",
                i, len(pending), stats["confirmed"], stats["flagged"],
            )

    if ckpt_path.exists():
        ckpt_path.unlink()

    # Auto-advance workflow
    try:
        from engine.adjudication.workflow import complete_stage
        complete_stage(
            db._conn, "FULL_TEXT_SCREENING_COMPLETE",
            metadata=f"{stats['confirmed']} confirmed, {stats['flagged']} flagged",
        )
    except Exception:
        pass

    logger.info(
        "FT verification complete: %d confirmed, %d flagged",
        stats["confirmed"], stats["flagged"],
    )
    return stats


# ── CLI Entry Point ──────────────────────────────────────────────────


if __name__ == "__main__":
    import argparse
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

    from engine.core.review_spec import load_review_spec

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Full-text screening pipeline")
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument(
        "--spec", default="review_specs/surgical_autonomy_v1.yaml",
        help="Path to review spec YAML",
    )
    parser.add_argument("--screen-only", action="store_true", help="Primary screen only")
    parser.add_argument("--verify-only", action="store_true", help="Verification only")
    parser.add_argument("--background", action="store_true", help="Run in tmux background")
    args = parser.parse_args()

    if args.background:
        from engine.utils.background import maybe_background
        maybe_background("ft_screening", review_name=args.review)

    spec = load_review_spec(args.spec)
    db = ReviewDatabase(args.review)

    try:
        if args.verify_only:
            run_ft_verification(db, spec, review_name=args.review)
        elif args.screen_only:
            run_ft_screening(db, spec, review_name=args.review)
        else:
            run_ft_screening(db, spec, review_name=args.review)
            run_ft_verification(db, spec, review_name=args.review)
    finally:
        db.close()
