"""Cross-model audit agent using Qwen3:32b to verify extractions."""

import logging
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Literal, Optional

import ollama
from pydantic import BaseModel

from engine.agents.models import EvidenceSpan
from engine.core.constants import INVALID_SNIPPET_RE
from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec

logger = logging.getLogger(__name__)

MODEL = "qwen3:32b"

# Fields at these tiers skip grep and go straight to semantic verification
SEMANTIC_ONLY_TIERS = {4}


# ── Audit Output Model ──────────────────────────────────────────────


class AuditVerdict(BaseModel):
    """Structured output from the audit agent."""

    status: Literal["verified", "flagged"]
    grep_found: bool
    reasoning: str


# ── Text Normalization ──────────────────────────────────────────────

_WS_RE = re.compile(r"\s+")
_PUNCT_GLUED_RE = re.compile(r"(?<=\w)\.(?=\w)")
_SMART_QUOTES = str.maketrans({
    "\u2018": "'", "\u2019": "'",   # single curly quotes
    "\u201c": '"', "\u201d": '"',   # double curly quotes
    "\u2013": "-", "\u2014": "-",   # en/em dash
})


def _normalize(text: str) -> str:
    """Normalize text for comparison: lowercase, collapse whitespace,
    fix glued punctuation (Table.I → Table I), straighten quotes."""
    text = text.translate(_SMART_QUOTES)
    text = unicodedata.normalize("NFKC", text)
    text = _PUNCT_GLUED_RE.sub(" ", text)
    return _WS_RE.sub(" ", text.lower()).strip()


# ── Grep Verification ────────────────────────────────────────────────


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


def semantic_verify(
    span: EvidenceSpan, paper_text: str, field_type: str = "text"
) -> AuditVerdict:
    """Use Qwen3:32b to verify if extracted value matches the source snippet.

    For categorical fields, the prompt asks whether the source text supports
    the classification rather than whether it contains the exact phrase.
    """
    if field_type == "categorical":
        verification_question = (
            f"Does the source snippet provide sufficient evidence to classify "
            f"this paper as '{span.value}' for the field '{span.field_name}'?\n"
            f"Note: This is a categorical/classification field. The exact phrase "
            f"'{span.value}' does NOT need to appear verbatim in the text. "
            f"The question is whether the described content reasonably supports "
            f"this classification."
        )
    else:
        verification_question = (
            f"Does the extracted value accurately represent what the source snippet states?\n"
            f"Consider:\n"
            f"- Is the value factually supported by the snippet?\n"
            f"- Is there any misinterpretation or hallucination?\n"
            f"- Is the value a reasonable extraction for this field?"
        )

    prompt = f"""/no_think
You are an audit agent verifying data extraction from a scientific paper.

Field: {span.field_name}
Field type: {field_type}
Extracted value: {span.value}
Source snippet from paper: {span.source_snippet}

{verification_question}

Respond with JSON: {{"status": "verified" or "flagged", "grep_found": true, "reasoning": "..."}}"""

    response = ollama.chat(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an audit agent verifying data extractions from "
                    "scientific papers. For free-text fields, be strict: flag anything "
                    "not clearly supported by the source snippet. For categorical fields, "
                    "verify that the source text reasonably supports the chosen category — "
                    "the category label does not need to appear verbatim. "
                    "Respond ONLY with JSON."
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


def audit_span(
    span_data: dict, paper_text: str, field_type: str = "text",
    field_tier: int = 1,
) -> tuple[str, str]:
    """Audit a single evidence span. Returns (audit_status, reasoning).

    4-state outcome:
    - 'invalid_snippet' — snippet contains ellipsis bridging
    - 'verified' — grep pass AND semantic pass
    - 'contested' — grep fail AND semantic pass (or Tier 4 semantic pass)
    - 'flagged' — semantic fail
    """
    source_snippet = span_data.get("source_snippet", "")
    value = span_data.get("value", "")

    # Values that indicate the field is absent/not reported — auto-verify
    _ABSENCE_VALUES = {"NOT_FOUND", "Not discussed", "NR", "No comparison reported"}
    if value in _ABSENCE_VALUES:
        return "verified", f"Field value '{value}' indicates absence — no extraction to audit."

    # Fix A: Invalid snippet detection (before any other logic)
    if source_snippet and source_snippet.strip():
        if INVALID_SNIPPET_RE.search(source_snippet):
            return "invalid_snippet", "Source snippet contains ellipsis bridging — marked invalid."

    # Empty snippet on a non-absence value
    if not source_snippet or not source_snippet.strip():
        return "flagged", "Extracted value present but no source snippet provided."

    # Fix C: Tier 4 semantic-only routing — skip grep entirely
    is_semantic_only = field_tier in SEMANTIC_ONLY_TIERS

    # Compute grep result
    if is_semantic_only:
        grep_pass = True  # not evaluated, treat as pass for routing
    else:
        grep_pass = grep_verify(source_snippet, paper_text)

    # Compute semantic result
    span = EvidenceSpan(
        field_name=span_data["field_name"],
        value=value,
        source_snippet=source_snippet,
        confidence=span_data.get("confidence", 0.5),
        tier=field_tier,
    )
    verdict = semantic_verify(span, paper_text, field_type=field_type)
    semantic_pass = verdict.status == "verified"

    # Fix D: 4-state outcome
    if grep_pass and semantic_pass:
        status = "verified"
        reasoning = verdict.reasoning
    elif not grep_pass and semantic_pass:
        status = "contested"
        reasoning = f"Grep failed but semantic verified. {verdict.reasoning}"
    else:
        # grep_pass or not, semantic failed → flagged
        status = "flagged"
        reasoning = verdict.reasoning

    return status, reasoning


# ── Batch Audit Pipeline ─────────────────────────────────────────────


def run_audit(
    db: ReviewDatabase, review_name: str, spec: Optional[ReviewSpec] = None
) -> dict:
    """Audit all EXTRACTED papers. Returns stats dict.

    If spec is provided, field types and tiers from the extraction schema
    are used to route categorical fields and Tier 4 fields appropriately.
    """
    # Build field_name → (type, tier) lookup from spec
    field_type_map: dict[str, str] = {}
    field_tier_map: dict[str, int] = {}
    if spec:
        for field in spec.extraction_schema.fields:
            field_type_map[field.name] = field.type
            field_tier_map[field.name] = field.tier

    papers = db.get_papers_by_status("EXTRACTED")
    total = len(papers)
    logger.info("Starting audit on %d papers", total)

    stats = {
        "papers_audited": 0,
        "spans_verified": 0,
        "spans_contested": 0,
        "spans_flagged": 0,
        "spans_invalid_snippet": 0,
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
            fname = span_data.get("field_name", "")
            ft = field_type_map.get(fname, "text")
            tier = field_tier_map.get(fname, 1)

            status, reasoning = audit_span(
                span_data, paper_text, field_type=ft, field_tier=tier,
            )

            db.update_audit(
                span_id=span_data["id"],
                status=status,
                model=MODEL,
                rationale=reasoning,
            )

            stats[f"spans_{status}"] = stats.get(f"spans_{status}", 0) + 1
            if status in ("flagged", "contested", "invalid_snippet"):
                stats["grep_failures"] += 1

        # Fix E: Assert no pending spans remain before transitioning
        pending = db._conn.execute(
            "SELECT COUNT(*) FROM evidence_spans WHERE extraction_id = ? AND audit_status = 'pending'",
            (ext_id,),
        ).fetchone()[0]

        if pending > 0:
            logger.warning(
                "Paper %d: %d spans still pending after audit — NOT transitioning",
                pid, pending,
            )
        else:
            db.update_status(pid, "AI_AUDIT_COMPLETE")
            stats["papers_audited"] += 1

        if i % 10 == 0 or i == total:
            logger.info(
                "Audited %d/%d papers — %d verified, %d contested, %d flagged, %d invalid",
                i, total,
                stats["spans_verified"], stats["spans_contested"],
                stats["spans_flagged"], stats["spans_invalid_snippet"],
            )

    logger.info(
        "Audit complete: %d papers, %d verified, %d contested, %d flagged, "
        "%d invalid_snippet (%d grep failures)",
        stats["papers_audited"],
        stats["spans_verified"], stats["spans_contested"],
        stats["spans_flagged"], stats["spans_invalid_snippet"],
        stats["grep_failures"],
    )
    return stats
