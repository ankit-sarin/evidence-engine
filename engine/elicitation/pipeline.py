"""The elicited two-pass extraction: Pass 1 cites, the engine materializes,
Pass 2 is primed.

Mirrors `engine.agents.extractor.extract_paper` — same write boundary, same
completeness guard, same atomic store — and differs in what the two passes are
asked for.

  Pass 1  numbered units in, per-class contracts, citations out. Read from the
          **content** channel. Production's Pass 1 reads `message.thinking` and
          discards `message.content`; PRIME-01 measured that discarded draft at
          37.9–42.9% verbatim richness against 0.4% for thinking, so the channel
          production threw away is the channel this design uses. `think` stays
          on: the reasoning still happens, it is simply no longer what Pass 2 is
          primed with.

  Pass 2  the same schema-constrained call as production, primed with
          materialized evidence instead of a free-form trace, and still carrying
          the full paper text (ELICIT-DESIGN-01 C6-Q4).

**The stored snippet is the engine's, not the model's.** Pass 2 emits a
`source_snippet` and it is overwritten with the Pass-1 materialized unit text.
That is the design: evidence is resolved from the unit map, verbatim by
construction, and never retyped by a model. Pass 2 supplies the value, the
confidence and the tier.

  ⚠ **Consequence for measurement, stated here because it is easy to misread
  later.** A materialized quote is ANCHORED by construction, so an anchoring rate
  computed over these spans measures the materializer, not the model — exactly
  the caution ELICIT-01's report raised about reporting "100% ANCHORED". Anchored
  rates from this path are NOT comparable to Run 6's 58.3% or to the ~39–43%
  corrected baseline. The meaningful measures here are citation validity, the
  contract-violation counts, and judge-scored supportedness.

**Value divergence is recorded, not reconciled.** Pass 2 may return a value that
differs from the one Pass 1 stated while citing the same units. The stored value
is Pass 2's (it is the schema-constrained pass) and the divergence is counted in
telemetry, because a silent reconciliation would hide the one signal that says
priming is not landing.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from engine.agents.extractor import (
    MODEL, _find_codebook_path, build_extraction_prompt, extract_pass2_structured,
    _LAST_PASS1_TELEMETRY, _LAST_PASS2_TELEMETRY,
)
from engine.core.citation_guard import STRICT, enforce_citations
from engine.core.completeness import enforce_completeness, expected_field_names
from engine.elicitation import classes as C
from engine.elicitation import materialize as M
from engine.elicitation import sizing as S
from engine.elicitation.contracts import Pass1Result, check_response
from engine.elicitation.prompts import (
    SYSTEM_PASS1, build_pass1_prompt, build_pass2_priming_message,
    prompt_field_order,
)
from engine.elicitation.units import UnitMap, build_unit_map
from engine.utils.ollama_client import ollama_chat

logger = logging.getLogger(__name__)

PASS1_LABEL = "pass1_elicitation"
PASS2_LABEL = "pass2_primed"


class Pass1ContractError(RuntimeError):
    """A Pass-1 response violated one or more per-class evidence contracts.

    Raised before Pass 2 runs. Two reasons for failing here rather than carrying
    the damage forward: acceptance gate 3 requires a violating response to be
    detected rather than absorbed, and a doomed extraction should not spend a
    second 32B call to arrive at a write the guard will refuse anyway.
    """

    def __init__(self, paper_id: int, result: Pass1Result, attempt: int | None = None):
        self.paper_id = paper_id
        self.result = result
        self.attempt = attempt
        detail = "; ".join(
            f"{n}={list(result.records[n].fatal)}" for n in result.failed_fields[:8]
        )
        if len(result.failed_fields) > 8:
            detail += "…"
        super().__init__(
            f"Paper {paper_id}: {len(result.failed_fields)} field(s) violated their "
            f"Pass-1 evidence contract [parse={result.parse_path}"
            + (f", attempt {attempt}" if attempt else "")
            + f"]. {detail}"
        )


def _codebook_path(db_path: str | Path | None, review_dir: Path | None) -> Path:
    if review_dir is not None:
        p = review_dir / "extraction_codebook.yaml"
        if p.exists():
            return p
    return _find_codebook_path()


def persist_unit_map(unit_map: UnitMap, review_dir: Path, run_id: str) -> Path:
    """Write the paper's unit map for this run, so every cited index is auditable.

    Per paper per run, the shape ELICIT-01 persisted. Kept on the filesystem
    rather than in `review.db`: `evidence_spans` has no column for a citation
    set and no migration is in scope (ELICIT-DESIGN-01 C5).
    """
    out = review_dir / "elicitation" / run_id / "unit_maps"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{unit_map.paper_id}.json"
    path.write_text(json.dumps(unit_map.to_json()))
    return path


def run_pass1(unit_map: UnitMap, codebook: dict, field_names: tuple[str, ...],
              paper_id: int, think: bool = True) -> tuple[Pass1Result, dict]:
    """Elicit citations. Returns (checked result, call telemetry)."""
    prompt = build_pass1_prompt(unit_map, codebook, field_names)
    est = S.enforce_fit(prompt, label=PASS1_LABEL, paper_id=paper_id)

    response = ollama_chat(
        model=MODEL, paper_id=paper_id,
        messages=[{"role": "system", "content": SYSTEM_PASS1},
                  {"role": "user", "content": prompt}],
        options={"temperature": 0}, think=think,
    )
    raw = response.message.content or ""
    thinking = getattr(response.message, "thinking", None) or ""
    pec = getattr(response, "prompt_eval_count", None)

    result = check_response(raw, unit_map, codebook, field_names)
    telemetry = {
        "pass1_prompt_chars": len(prompt),
        "pass1_estimated_tokens": est,
        "pass1_prompt_eval_count": pec,
        "pass1_truncation_tripwire": S.truncation_tripwire(pec),
        "pass1_content_chars": len(raw),
        "pass1_thinking_chars": len(thinking),
        "pass1_done_reason": getattr(response, "done_reason", None),
        "pass1_raw_content": raw,
        **result.telemetry(),
    }
    if telemetry["pass1_truncation_tripwire"]:
        logger.error(
            "TRIPWIRE paper %d: Pass-1 prompt_eval_count %s is at the enforced "
            "ceiling %d — the input was truncated and done_reason cannot say so.",
            paper_id, pec, S.CEILING_TOKENS,
        )
    return result, telemetry


def extract_paper_elicited(
    paper_id: int,
    paper_text: str,
    spec,
    db,
    run_id: str,
    model_digest: str | None = None,
    auditor_model_digest: str | None = None,
    attempt: int | None = None,
):
    """Full elicited two-pass extraction for one paper, storing the result.

    Raises before any INSERT on: a projected context overflow
    (`PromptTooLargeError`), a Pass-1 contract violation (`Pass1ContractError`),
    an incomplete Pass-2 result (`IncompleteExtractionError`) or an uncited value
    at the write boundary (`UncitedValueError`). An exhausted paper leaves
    nothing behind.
    """
    from engine.agents.models import EvidenceSpan, ExtractionResult

    review_dir = Path(db.db_path).parent
    cb_path = _codebook_path(db.db_path, review_dir)
    codebook = C.load(cb_path)
    field_names = expected_field_names(spec, cb_path)

    models = getattr(spec, "extraction_models", None)
    pass1_think = getattr(models, "pass1_think", True)
    pass2_think = getattr(models, "pass2_think", False)

    unit_map = build_unit_map(paper_id, paper_text)
    persist_unit_map(unit_map, review_dir, run_id)

    p1, p1_tel = run_pass1(unit_map, codebook, field_names, paper_id, think=pass1_think)
    _LAST_PASS1_TELEMETRY.clear()
    _LAST_PASS1_TELEMETRY.update(
        thinking_present=bool(p1_tel["pass1_thinking_chars"]),
        thinking_chars=p1_tel["pass1_thinking_chars"],
        parse_branch=f"elicitation:{p1.parse_path}",
        finish_reason=p1_tel["pass1_done_reason"],
        elicitation=p1_tel,
    )
    if not p1.ok:
        raise Pass1ContractError(paper_id, p1, attempt=attempt)

    order = prompt_field_order(codebook, field_names)
    priming = M.priming_block(p1.records, unit_map, order)

    pass2_prompt = build_extraction_prompt(paper_text, spec, cb_path)
    priming_msg = build_pass2_priming_message(priming)
    S.enforce_fit(pass2_prompt + priming_msg, label=PASS2_LABEL, paper_id=paper_id)

    result = extract_pass2_structured(
        pass2_prompt, priming_msg, spec, paper_id, think=pass2_think,
    )

    # The engine's evidence replaces the model's. See the module docstring.
    divergent: list[str] = []
    spans: list[EvidenceSpan] = []
    for span in result.fields:
        rec = p1.records.get(span.field_name)
        if rec is None:
            spans.append(span)          # unexpected field: completeness will speak
            continue
        if rec.value and span.value.strip() != rec.value:
            divergent.append(span.field_name)
        spans.append(EvidenceSpan(
            field_name=span.field_name,
            value=span.value,
            source_snippet=M.source_snippet(rec, unit_map),
            confidence=span.confidence,
            tier=span.tier,
        ))

    citation_counts = {n: len(r.indices) for n, r in p1.records.items()}
    span_dicts = [
        {"field_name": s.field_name, "value": s.value,
         "source_snippet": s.source_snippet, "confidence": s.confidence}
        for s in spans
    ]

    enforce_completeness(span_dicts, field_names, paper_id=paper_id, arm=MODEL)
    enforce_citations(
        span_dicts, paper_id=paper_id, arm=MODEL, mode=STRICT,
        escape_token=C.escape_token(codebook),
        absence_sentinels=C.absence_sentinels(codebook),
        citation_counts=citation_counts, attempt=attempt,
    )

    _LAST_PASS2_TELEMETRY.setdefault("model", MODEL)
    _LAST_PASS2_TELEMETRY["elicitation_run_id"] = run_id
    _LAST_PASS2_TELEMETRY["value_divergence"] = divergent
    _LAST_PASS2_TELEMETRY["n_value_divergence"] = len(divergent)

    stored = ExtractionResult(
        paper_id=paper_id, fields=spans,
        reasoning_trace=priming,          # the materialized evidence IS the trace
        model=MODEL,
        extraction_schema_hash=result.extraction_schema_hash,
        extracted_at=datetime.now(timezone.utc),
    )
    db.add_extraction_atomic(
        paper_id=paper_id,
        schema_hash=stored.extraction_schema_hash,
        extracted_data=[s.model_dump() for s in spans],
        reasoning_trace=priming,
        model=MODEL,
        spans=span_dicts,
        model_digest=model_digest,
        auditor_model_digest=auditor_model_digest,
    )
    return stored
