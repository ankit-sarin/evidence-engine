"""Full-text screening agent — dual-model, cross-family.

Primary: Qwen3.5:27b (Alibaba/Qwen) — high-recall screen on parsed full text.
Verifier: Gemma3:27b (Google/DeepMind) — strict verification of primary includes.

Mirrors the abstract screening architecture but operates on parsed PDF text
with specialty scope filtering.
"""

import contextlib
import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, ValidationError

from engine.core import run_manifest as rm
from engine.core.constants import FT_MAX_TEXT_CHARS
from engine.core.database import ReviewDatabase
from engine.core.effective import NO_RECORDED_STATE, effective_state, eligible_paper_ids
from engine.core.events import write_paper_event
from engine.core import eligibility_render as render
from engine.core.review_spec import ReviewSpec
from engine.core.effective_config import stage_config
from engine.core.parsed_text import NoParsedText, load_parsed_text
from engine.utils.ollama_client import ollama_chat

logger = logging.getLogger(__name__)


# ── Structured Output Models ─────────────────────────────────────────


class FTScreeningDecision(BaseModel):
    """Structured output from the full-text primary screener."""

    decision: Literal["FT_ELIGIBLE", "FT_EXCLUDE"]
    reason_code: str = Field(description="A reason code from the review's eligibility vocabulary")
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

    elig = spec.eligibility
    inclusion = render.inclusion_block(elig, "ft_primary")
    exclusion = render.exclusion_block(elig, "ft_primary")
    specialty_block = render.specialty_prompt_block(elig, "ft_primary")
    reason_code_block = render.reason_code_prompt_block(elig)
    reason_codes_str = ", ".join(elig.reason_codes())

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
{reason_code_block}

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

    elig = spec.eligibility
    exclusion = render.exclusion_block(elig, "ft_verifier")
    specialty_block = render.specialty_prompt_block(elig, "ft_verifier")
    decision_instruction = render.decision_instruction(elig, "ft_verifier")

    return f"""/no_think
You are the VERIFICATION pass for full-text screening. This paper was already
marked as eligible by a primary screener. Your job is to catch false positives.

REVIEW FOCUS (PICO):
{pico_block}

EXCLUSION CRITERIA:
{exclusion}
{specialty_block}
{decision_instruction}

PAPER FULL TEXT:
{paper_text}

Respond with JSON only: {{"decision": "FT_ELIGIBLE" or "FT_FLAGGED", "rationale": "...", "confidence": 0.0-1.0}}"""


# ── Single-Paper Screening ───────────────────────────────────────────


def build_ft_messages(paper_text: str, spec: ReviewSpec, which: str = "primary") -> list[dict]:
    """The message list the FT primary (`which="primary"`) or verifier sends.

    One builder for the call and for the resolver's prompt hash (R60).
    """
    if which == "primary":
        return render.messages("ft_primary", build_ft_screening_prompt(paper_text, spec),
                               review_title=spec.title)
    return render.messages("ft_verifier", build_ft_verification_prompt(paper_text, spec),
                           review_title=spec.title)


def _ft_config(stage: str, spec: ReviewSpec):
    """The resolver's config for an FT stage, unchanged (R223a, R-c).

    No caller override: `model`, `think` and `temperature` used to be accepted
    here and applied through the config's option merge and its private replace
    helper — the latter bypassing `UndeclaredOverride`. Production passed only `model=`, the
    spec's own value, whose one effect was to relabel its source `caller`.
    Every FT value comes from `ft_screening_models`.
    """
    return stage_config(stage, spec)


def ft_screen_paper(paper_text: str, spec: ReviewSpec) -> tuple[FTScreeningDecision, str]:
    """Screen a single paper's full text. Returns the structured decision and
    the request hash of the one call that decided it — the value
    `run_calls.request_hash` records for that call (R261)."""
    cfg = _ft_config("ft_screen_primary", spec)
    response, request_hash = ollama_chat(
        messages=build_ft_messages(paper_text, spec, "primary"),
        return_request_hash=True, **cfg.kwargs())
    return FTScreeningDecision.model_validate_json(response.message.content), request_hash


def ft_verify_paper(paper_text: str, spec: ReviewSpec) -> tuple[FTVerificationDecision, str]:
    """Verify a single paper's full text (strict, FP-catching). Returns the
    structured decision and its deciding call's request hash (R261)."""
    cfg = _ft_config("ft_screen_verifier", spec)
    response, request_hash = ollama_chat(
        messages=build_ft_messages(paper_text, spec, "verifier"),
        return_request_hash=True, **cfg.kwargs())
    return FTVerificationDecision.model_validate_json(response.message.content), request_hash


# ── Eligibility events (the bridge, R258–R261) ─────────────────────


@contextlib.contextmanager
def _paper_transaction(db: ReviewDatabase):
    """One decided paper, one transaction (R260): everything written inside is
    committed together, and any exception — a refused transition included —
    rolls all of it back, leaving nothing written for the paper."""
    try:
        yield
        db._conn.commit()
    except BaseException:
        db._conn.rollback()
        raise


def _write_eligibility_event(db: ReviewDatabase, spec: ReviewSpec, *, run_id: int,
                             paper_id: int, stage: str, event_type: str,
                             to_state: str, request_hash: str) -> int:
    """The model's eligibility event, inside the caller's transaction (R259).

    Actor is the stage's resolved model with the digest this run recorded for
    the stage; `presented_context_sha256` is the deciding call's request hash.
    `from_state` is the paper's current eligibility-axis state, or None when that
    axis has none; `prior_event_id` stays None (R-E1)."""
    current = effective_state(db._conn, paper_id).eligibility
    return write_paper_event(
        db._conn, event_type=event_type, paper_id=paper_id, to_state=to_state,
        from_state=None if current == NO_RECORDED_STATE else current,
        actor_kind="model", actor_role="reviewer",
        actor_name=_ft_config(stage, spec).model,
        actor_digest=rm.stage_digest(db._conn, run_id, stage),
        run_id=run_id, presented_context_sha256=request_hash,
        stage_name=stage, commit=False)


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
    """The paper's current parsed text through the one resolver (S3e), or None.

    None means "no parsed text is recorded", as it meant "no file" before. A
    recorded text that is missing or modified raises (R95) — it is never
    screened as though it were the text that was recorded.
    """
    try:
        return load_parsed_text(db._conn, paper_id)
    except NoParsedText:
        return None


# ── Pipeline: Primary Screening ─────────────────────────────────────


def run_ft_screening(
    db: ReviewDatabase, spec: ReviewSpec, review_name: str = "", *, run_id: int,
) -> dict:
    """Run full-text primary screening on all PARSED papers with parsed text.

    Only PARSED papers are selected (R-e, session 11). The second pickup that
    retrofitted FT screening onto the already-extracted corpus (de7e6a5) read
    a retired status token (D18) and is gone: that corpus has its FT
    decisions, and no paper can enter a retired token now.

    Returns summary stats dict.
    """
    primary_model = spec.ft_screening_models.primary

    # Pre-flight: verify models are loaded and responsive
    from engine.utils.ollama_preflight import require_preflight
    require_preflight(
        [spec.ft_screening_models.primary, spec.ft_screening_models.verifier],
        runner_name="FT screening",
    )

    papers = db.get_papers_by_status("PARSED")
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

    stats = {"ft_eligible": 0, "ft_exclude": 0, "skipped_no_text": 0, "parse_errors": 0, "total": len(pending)}

    for i, paper in enumerate(pending, 1):
        pid = paper["id"]

        # Load parsed text
        parsed_text = _load_parsed_text(db, pid)
        if not parsed_text:
            logger.warning("Paper %d has no parsed text — marking FT_FLAGGED", pid)
            # No decision row. Nothing was screened, so there is no decision to
            # record and no reason code that would be true of it; the status
            # alone parks the paper for a human.
            db.update_status(pid, "FT_FLAGGED")
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
        try:
            decision, request_hash = ft_screen_paper(truncated, spec)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning(
                "Paper %d: malformed FT screening output — flagging: %s",
                pid, str(exc)[:200],
            )
            db.update_status(pid, "FT_FLAGGED")
            stats["parse_errors"] += 1
            screened_ids.add(pid)
            continue

        # Decision row → status → (exclude) event, one transaction (R260). An
        # include writes no event: 'eligible' is the verifier's to write (R-S1).
        with _paper_transaction(db):
            db.add_ft_screening_decision(
                pid, primary_model, decision.decision,
                decision.reason_code, decision.rationale, decision.confidence,
                reason_codes=spec.eligibility.reason_codes(), commit=False,
            )
            if decision.decision == "FT_ELIGIBLE":
                db.update_status(pid, "FT_ELIGIBLE")
            else:
                db.update_status(pid, "FT_SCREENED_OUT")
                _write_eligibility_event(
                    db, spec, run_id=run_id, paper_id=pid, stage="ft_screen_primary",
                    event_type="screened", to_state="full_text_out",
                    request_hash=request_hash)
        if decision.decision == "FT_ELIGIBLE":
            stats["ft_eligible"] += 1
        else:
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
    db: ReviewDatabase, spec: ReviewSpec, review_name: str = "", *, run_id: int,
) -> dict:
    """Re-screen FT_ELIGIBLE papers with the verification model.

    Consensus logic:
      - Verifier confirms → stays FT_ELIGIBLE, and the paper gets its
        `verified` → `eligible` event
      - Verifier flags → FT_FLAGGED (for human adjudication)

    Selection (R-V1): a paper at FT_ELIGIBLE with no live eligible event AND no
    verification decision row — the papers the verifier has never decided. That
    is what makes a repeat run idempotent; the checkpoint is only a within-run
    resume aid.
    """
    verification_model = spec.ft_screening_models.verifier
    eligible = set(eligible_paper_ids(db._conn))
    decided = {r[0] for r in db._conn.execute(
        "SELECT DISTINCT paper_id FROM ft_verification_decisions")}
    papers = [p for p in db.get_papers_by_status("FT_ELIGIBLE")
              if p["id"] not in eligible and p["id"] not in decided]

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

    stats = {"confirmed": 0, "flagged": 0, "parse_errors": 0, "total": len(pending)}
    decisions_written = 0

    for i, paper in enumerate(pending, 1):
        pid = paper["id"]

        parsed_text = _load_parsed_text(db, pid)
        if not parsed_text:
            logger.warning("Paper %d has no parsed text — marking FT_FLAGGED", pid)
            db.update_status(pid, "FT_FLAGGED")
            stats["flagged"] += 1
            verified_ids.add(pid)
            continue

        truncated = truncate_paper_text(
            parsed_text,
            title=paper.get("title", ""),
            abstract=paper.get("abstract", ""),
        )

        try:
            decision, request_hash = ft_verify_paper(truncated, spec)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning(
                "Paper %d: malformed verifier output — flagging: %s",
                pid, str(exc)[:200],
            )
            db.update_status(pid, "FT_FLAGGED")
            stats["flagged"] += 1
            stats["parse_errors"] += 1
            verified_ids.add(pid)
            continue

        # Confirm: decision row → event. Flag: decision row → status. One
        # transaction either way (R260).
        with _paper_transaction(db):
            db.add_ft_verification_decision(
                pid, verification_model, decision.decision,
                decision.rationale, decision.confidence, commit=False,
            )
            if decision.decision == "FT_ELIGIBLE":
                _write_eligibility_event(
                    db, spec, run_id=run_id, paper_id=pid, stage="ft_screen_verifier",
                    event_type="verified", to_state="eligible",
                    request_hash=request_hash)
            else:
                db.update_status(pid, "FT_FLAGGED")
        decisions_written += 1
        if decision.decision == "FT_ELIGIBLE":
            stats["confirmed"] += 1
        else:
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

    # Auto-advance workflow — only when this run decided a paper (R-W1): a run
    # that decided nothing has no completion to record, and must not overwrite
    # the stage's existing metadata with zero counts.
    if decisions_written:
        _complete_ft_stage(db, stats)

    logger.info(
        "FT verification complete: %d confirmed, %d flagged",
        stats["confirmed"], stats["flagged"],
    )
    return stats


def _complete_ft_stage(db: ReviewDatabase, stats: dict) -> None:
    try:
        from engine.adjudication.workflow import complete_stage
        complete_stage(
            db._conn, "FULL_TEXT_SCREENING_COMPLETE",
            metadata=f"{stats['confirmed']} confirmed, {stats['flagged']} flagged",
        )
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            logger.debug("Workflow table not found — skipping stage advance")
        else:
            raise


# ── One invocation, one screening manifest (R258) ──────────────────


def open_screening_manifest(db: ReviewDatabase, spec: ReviewSpec, *,
                            with_preflight: bool, git=None, digest_fn=None) -> int:
    """Write this invocation's `screening` manifest before its first call.

    Both FT stages are always declared; `preflight` with both FT models is
    declared whenever the primary screen runs (it is the one that preflights).
    `git` / `digest_fn` default to the live tree and `/api/tags`."""
    from engine.core.codebook import load_codebook_beside

    stages = ["ft_screen_primary", "ft_screen_verifier"]
    preflight: list[str] = []
    if with_preflight:
        stages.append("preflight")
        preflight = sorted({spec.ft_screening_models.primary,
                            spec.ft_screening_models.verifier})
    handle = rm.open_run(db._conn, spec, kind="screening", stages=stages,
                         codebook=load_codebook_beside(db.db_path),
                         preflight_models=preflight, git=git, digest_fn=digest_fn)
    logger.info("Run manifest %d (%s) written: %d stage rows", handle.run_id,
                handle.run_uid, len(handle.stages))
    return handle.run_id


def run_ft_invocation(db: ReviewDatabase, spec: ReviewSpec, *, review_name: str = "",
                      screen_only: bool = False, verify_only: bool = False,
                      git=None, digest_fn=None) -> int:
    """The CLI's body: open the manifest, run inside it, close it. Returns the
    run id. A resumed run is simply a new invocation, so a new manifest.
    Closes `completed`, or `failed` on any exception, which re-raises; on an
    interrupt (KeyboardInterrupt, or `rm.RunInterrupted` from SIGTERM/SIGHUP)
    closes `interrupted` with `rm.interrupt_reason(exc)` and re-raises (C47).
    The interrupt path rolls back any open transaction before it asks
    `rm.is_closed`, so a close the interrupt caught between its UPDATE and its
    commit is undone rather than mistaken for a close: a rolled-back close is
    not a close (12a-C47-R2)."""
    run_id = open_screening_manifest(db, spec, with_preflight=not verify_only,
                                     git=git, digest_fn=digest_fn)
    token = rm.activate(db._conn, run_id)
    try:
        if verify_only:
            run_ft_verification(db, spec, review_name=review_name, run_id=run_id)
        elif screen_only:
            run_ft_screening(db, spec, review_name=review_name, run_id=run_id)
        else:
            run_ft_screening(db, spec, review_name=review_name, run_id=run_id)
            run_ft_verification(db, spec, review_name=review_name, run_id=run_id)
        rm.close_run(db._conn, run_id, "completed")
    except Exception:
        logger.error("FT screening run %d failed", run_id, exc_info=True)
        rm.close_run(db._conn, run_id, "failed")
        raise
    except (KeyboardInterrupt, rm.RunInterrupted) as exc:
        # C47: as run_pipeline — roll back, then check: a committed close
        # stands, while an uncommitted write or a close caught before its
        # commit is rolled back and the run closes 'interrupted' (12a-C47-R2).
        if db._conn.in_transaction:
            db._conn.rollback()
        if not rm.is_closed(db._conn, run_id):
            rm.close_run(db._conn, run_id, "interrupted",
                         reason=rm.interrupt_reason(exc))
        exc.run_id = run_id
        raise
    finally:
        rm.deactivate(token)
    return run_id


# ── CLI Entry Point ──────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    """The FT screening command line. `argv` None reads sys.argv. Returns the
    exit code: 0, or 128 + signum after an interrupt closed the run (C47)."""
    import argparse
    import signal
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

    from engine.core.review_paths import load_spec_for

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Full-text screening pipeline")
    parser.add_argument("--review", required=True, help="Review id. The review's identity — the spec file and the data root both derive from it.")
    parser.add_argument(
        "--spec", default=None,
        help="Override the Review Spec path. Defaults to review_specs/<review>.yaml; an override must carry the same review_id.",
    )
    parser.add_argument("--screen-only", action="store_true", help="Primary screen only")
    parser.add_argument("--verify-only", action="store_true", help="Verification only")
    parser.add_argument("--background", action="store_true", help="Run in tmux background")
    args = parser.parse_args(argv)

    if args.background:
        from engine.utils.background import maybe_background
        maybe_background("ft_screening", review_name=args.review)

    spec = load_spec_for(args.review, args.spec)
    db = ReviewDatabase(args.review)

    try:
        with rm.interrupt_signals():
            run_ft_invocation(db, spec, review_name=args.review,
                              screen_only=args.screen_only, verify_only=args.verify_only)
    except (KeyboardInterrupt, rm.RunInterrupted) as exc:
        # C47: one line, no traceback, the conventional 128 + signum exit.
        signum = rm.interrupt_signum(exc)
        run_id = getattr(exc, "run_id", None)
        logger.error("Interrupted by %s — %s.", signal.Signals(signum).name,
                     f"run_id {run_id} is closed" if run_id is not None
                     else "no run manifest was open")
        return 128 + signum
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
