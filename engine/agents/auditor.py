"""Cross-model audit agent using Qwen3:32b to verify extractions."""

import logging
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Literal

import ollama
from pydantic import BaseModel

from engine.agents.models import EvidenceSpan
from engine.core.database import ReviewDatabase

logger = logging.getLogger(__name__)

MODEL = "qwen3:32b"


# ── Audit Output Model ──────────────────────────────────────────────


class AuditVerdict(BaseModel):
    """Structured output from the audit agent."""

    status: Literal["verified", "flagged"]
    grep_found: bool
    reasoning: str


# ── Grep Verification ────────────────────────────────────────────────

_WS_RE = re.compile(r"\s+")


def _normalize(text: str) -> str:
    """Lowercase and collapse whitespace."""
    return _WS_RE.sub(" ", text.lower()).strip()


def grep_verify(source_snippet: str, paper_text: str) -> bool:
    """Check if source_snippet exists in paper_text (exact or fuzzy).

    1. Exact substring match on normalized text.
    2. Sliding window fuzzy match (SequenceMatcher > 0.85).
    """
    if not source_snippet or not paper_text:
        return False

    norm_snippet = _normalize(source_snippet)
    norm_text = _normalize(paper_text)

    # Exact substring match
    if norm_snippet in norm_text:
        return True

    # Sliding window fuzzy match
    snippet_len = len(norm_snippet)
    if snippet_len == 0:
        return False

    # Use word-level windows for efficiency
    text_words = norm_text.split()
    snippet_words = norm_snippet.split()
    window_size = len(snippet_words)

    if window_size == 0:
        return False

    for i in range(max(1, len(text_words) - window_size + 1)):
        window = " ".join(text_words[i : i + window_size])
        ratio = SequenceMatcher(None, norm_snippet, window).ratio()
        if ratio > 0.85:
            return True

    return False


# ── Semantic Verification ────────────────────────────────────────────


def semantic_verify(span: EvidenceSpan, paper_text: str) -> AuditVerdict:
    """Use Qwen3:32b to verify if extracted value matches the source snippet."""
    prompt = f"""/no_think
You are an audit agent verifying data extraction from a scientific paper.

Field: {span.field_name}
Extracted value: {span.value}
Source snippet from paper: {span.source_snippet}

Does the extracted value accurately represent what the source snippet states?
Consider:
- Is the value factually supported by the snippet?
- Is there any misinterpretation or hallucination?
- Is the value a reasonable extraction for this field?

Respond with JSON: {{"status": "verified" or "flagged", "grep_found": true, "reasoning": "..."}}"""

    response = ollama.chat(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an audit agent verifying data extractions from "
                    "scientific papers. Be strict: flag anything that is not "
                    "clearly supported by the source snippet. Respond ONLY with JSON."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        format=AuditVerdict.model_json_schema(),
        options={"temperature": 0},
        think=False,
    )

    raw = response.message.content or ""
    return AuditVerdict.model_validate_json(raw)


# ── Single Span Audit ────────────────────────────────────────────────


def audit_span(span_data: dict, paper_text: str) -> AuditVerdict:
    """Audit a single evidence span: grep check, then semantic check."""
    source_snippet = span_data.get("source_snippet", "")
    value = span_data.get("value", "")

    # Skip NOT_FOUND fields — nothing to audit
    if value == "NOT_FOUND":
        return AuditVerdict(
            status="verified",
            grep_found=False,
            reasoning="Field was NOT_FOUND — no extraction to audit.",
        )

    # Step 1: grep check
    grep_found = grep_verify(source_snippet, paper_text)

    if not grep_found:
        return AuditVerdict(
            status="flagged",
            grep_found=False,
            reasoning="Source snippet not found in paper text.",
        )

    # Step 2: semantic check via LLM
    span = EvidenceSpan(
        field_name=span_data["field_name"],
        value=value,
        source_snippet=source_snippet,
        confidence=span_data.get("confidence", 0.5),
        tier=span_data.get("tier", 1),
    )
    verdict = semantic_verify(span, paper_text)
    # Ensure grep_found is set correctly from our check
    verdict.grep_found = True
    return verdict


# ── Batch Audit Pipeline ─────────────────────────────────────────────


def run_audit(db: ReviewDatabase, review_name: str) -> dict:
    """Audit all EXTRACTED papers. Returns stats dict."""
    papers = db.get_papers_by_status("EXTRACTED")
    total = len(papers)
    logger.info("Starting audit on %d papers", total)

    stats = {
        "papers_audited": 0,
        "spans_verified": 0,
        "spans_flagged": 0,
        "grep_failures": 0,
    }
    review_dir = Path(db.db_path).parent

    for i, paper in enumerate(papers, 1):
        pid = paper["id"]

        # Load parsed text
        parsed_dir = review_dir / "parsed_text"
        md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)
        if not md_files:
            logger.warning("Paper %d: no parsed text found — skipping audit", pid)
            continue

        paper_text = md_files[0].read_text()

        # Get the latest extraction for this paper
        extraction = db._conn.execute(
            "SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()
        if not extraction:
            logger.warning("Paper %d: no extraction found — skipping audit", pid)
            continue

        ext_id = extraction["id"]

        # Get all pending evidence spans
        spans = db._conn.execute(
            "SELECT * FROM evidence_spans WHERE extraction_id = ? AND audit_status = 'pending'",
            (ext_id,),
        ).fetchall()

        for span_row in spans:
            span_data = dict(span_row)
            verdict = audit_span(span_data, paper_text)

            db.update_audit(
                span_id=span_data["id"],
                status=verdict.status,
                model=MODEL,
                rationale=verdict.reasoning,
            )

            if verdict.status == "verified":
                stats["spans_verified"] += 1
            else:
                stats["spans_flagged"] += 1

            if not verdict.grep_found:
                stats["grep_failures"] += 1

        db.update_status(pid, "AUDITED")
        stats["papers_audited"] += 1

        if i % 10 == 0 or i == total:
            logger.info(
                "Audited %d/%d papers — %d verified, %d flagged",
                i, total, stats["spans_verified"], stats["spans_flagged"],
            )

    logger.info(
        "Audit complete: %d papers, %d verified, %d flagged (%d grep failures)",
        stats["papers_audited"],
        stats["spans_verified"],
        stats["spans_flagged"],
        stats["grep_failures"],
    )
    return stats
