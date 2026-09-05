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
from engine.core.completeness import (
    enforce_completeness, enforce_terminal_states, expected_field_names,
)
from engine.elicitation import classes as C
from engine.elicitation import materialize as M
from engine.elicitation import sizing as S
from engine.elicitation import terminal as T
from engine.elicitation.contracts import Pass1Result, check_response
from engine.elicitation.prompts import (
    SYSTEM_PASS1, build_feedback_block, build_pass1_prompt,
    build_pass2_priming_message, prompt_field_order,
)
from engine.elicitation.units import UnitMap, build_unit_map
from engine.utils.ollama_client import ollama_chat

logger = logging.getLogger(__name__)

PASS1_LABEL = "pass1_elicitation"
PASS2_LABEL = "pass2_primed"


MAX_PASS1_ATTEMPTS = 2          # Ruling 4


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
              paper_id: int, think: bool = True,
              feedback: str = "") -> tuple[Pass1Result, dict]:
    """Elicit citations. Returns (checked result, call telemetry).

    `feedback` is appended to the prompt STRING, not sent as a separate message,
    so `enforce_fit` counts it: a retry that overflowed the context while the
    guard measured only the base prompt would be a silent truncation of the
    correction itself.
    """
    prompt = build_pass1_prompt(unit_map, codebook, field_names) + feedback
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
        "pass1_feedback_chars": len(feedback),
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


def elicit(unit_map: UnitMap, codebook: dict, field_names: tuple[str, ...],
           paper_id: int, think: bool = True
           ) -> tuple[Pass1Result, int, list[dict]]:
    """Ruling 4's bounded, feedback-carrying Pass-1 loop.

    Two attempts at most. Attempt 2 runs only if attempt 1 left a field failing,
    and it carries the typed feedback block, so the second request differs from
    the first in exactly the way F7 says the old one did not. Acceptance is
    `terminal.accept_attempt`'s strict-inequality rule.

    Returns (accepted result, accepted attempt number, per-attempt telemetry).
    Both attempts' telemetry is returned whichever one wins: a retry that
    regressed is a measurement, and discarding the losing attempt would delete
    the only evidence that the feedback did not land.
    """
    first, tel_first = run_pass1(unit_map, codebook, field_names, paper_id, think=think)
    tels = [tel_first]
    second = None

    if first.failed_fields and MAX_PASS1_ATTEMPTS > 1:
        feedback = build_feedback_block(first, codebook)
        second, tel_second = run_pass1(
            unit_map, codebook, field_names, paper_id, think=think, feedback=feedback,
        )
        tels.append(tel_second)

    accepted, n = T.accept_attempt(first, second, codebook)
    if second is not None:
        logger.info(
            "Paper %d: Pass-1 attempt 1 failed %d field(s), attempt 2 failed %d — "
            "accepted attempt %d.",
            paper_id, len(first.failed_fields), len(second.failed_fields), n,
        )
    return accepted, n, tels


def _token_span(field_name: str, token: str, tier: int):
    """A terminal-state span: the state occupies the value column, nothing else.

    Empty snippet and zero confidence are not decoration. They are what makes
    the row self-describing to a reader who has never heard of the token: there
    is no evidence here and the engine is not claiming any (D6).
    """
    from engine.agents.models import EvidenceSpan

    return EvidenceSpan(field_name=field_name, value=token,
                        source_snippet="", confidence=0.0, tier=tier)


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

    Under Ruling 1 a Pass-1 contract failure no longer refuses the PAPER. Each
    failing field takes the CONTRACT_UNMET terminal state and stores no value;
    the fields that met their contracts are stored normally. What still raises
    before any INSERT: a projected context overflow (`PromptTooLargeError`), a
    missing or illegal terminal state (`TerminalStateError`), an incomplete
    Pass-2 result (`IncompleteExtractionError`) and an uncited value at the write
    boundary (`UncitedValueError`). All twenty states are written in one
    transaction or none are.
    """
    from engine.agents.models import EvidenceSpan, ExtractionResult

    review_dir = Path(db.db_path).parent
    cb_path = _codebook_path(db.db_path, review_dir)
    codebook = C.load(cb_path)
    field_names = expected_field_names(spec, cb_path)
    tiers = {f["name"]: int(f.get("tier", 1)) for f in codebook["fields"]}

    models = getattr(spec, "extraction_models", None)
    pass1_think = getattr(models, "pass1_think", True)
    pass2_think = getattr(models, "pass2_think", False)

    unit_map = build_unit_map(paper_id, paper_text)
    persist_unit_map(unit_map, review_dir, run_id)

    p1, accepted_attempt, pass1_tels = elicit(
        unit_map, codebook, field_names, paper_id, think=pass1_think,
    )
    p1_tel = pass1_tels[accepted_attempt - 1]
    states = T.terminal_states(p1, codebook)
    n_unmet = T.n_contract_unmet(states, codebook)
    n_evidenced = T.n_evidenced(states)

    _LAST_PASS1_TELEMETRY.clear()
    _LAST_PASS1_TELEMETRY.update(
        thinking_present=bool(p1_tel["pass1_thinking_chars"]),
        thinking_chars=p1_tel["pass1_thinking_chars"],
        parse_branch=f"elicitation:{p1.parse_path}",
        finish_reason=p1_tel["pass1_done_reason"],
        elicitation={
            **p1_tel,
            "accepted_attempt": accepted_attempt,
            "n_pass1_attempts": len(pass1_tels),
            "terminal_states": states,
            "n_contract_unmet": n_unmet,
            "n_evidenced": n_evidenced,
            "attempts": [
                {
                    "attempt": i,
                    "parse_path": tel["parse_path"],
                    "failed_fields": tel["failed_fields"],
                    "n_failed": len(tel["failed_fields"]),
                    "feedback_chars": tel.get("pass1_feedback_chars", 0),
                    "prompt_chars": tel["pass1_prompt_chars"],
                    "prompt_eval_count": tel["pass1_prompt_eval_count"],
                    "fields": tel["fields"],
                }
                for i, tel in enumerate(pass1_tels, start=1)
            ],
        },
    )

    escape_tok = C.escape_token(codebook)
    unmet_tok = C.contract_unmet_token(codebook)

    order = prompt_field_order(codebook, field_names)
    evidenced = {n for n, s in states.items() if s == C.EVIDENCED_VALUE}
    priming = M.priming_block(
        {n: r for n, r in p1.records.items() if n in evidenced}, unit_map,
        tuple(n for n in order if n in evidenced),
    )

    # Pass 2 supplies values, and only EVIDENCED_VALUE fields take one. A paper
    # with none of them has nothing for a 32B call to answer, so it does not
    # make one -- the terminal states are already complete without it.
    divergent: list[str] = []
    pass2_values: dict[str, EvidenceSpan] = {}
    schema_hash = spec.extraction_hash()
    if n_evidenced:
        pass2_prompt = build_extraction_prompt(paper_text, spec, cb_path)
        priming_msg = build_pass2_priming_message(priming)
        S.enforce_fit(pass2_prompt + priming_msg, label=PASS2_LABEL, paper_id=paper_id)
        result = extract_pass2_structured(
            pass2_prompt, priming_msg, spec, paper_id, think=pass2_think,
        )
        schema_hash = result.extraction_schema_hash
        for span in result.fields:
            pass2_values[span.field_name] = span
    else:
        logger.warning(
            "Paper %d: no field met its evidence contract — %d CONTRACT_UNMET, "
            "%d escape. Pass 2 skipped; the terminal states are the extraction.",
            paper_id, n_unmet, len(states) - n_unmet,
        )

    # ── Build one span per field, in prompt order, from its terminal state ──
    spans: list[EvidenceSpan] = []
    citation_counts: dict[str, int] = {}
    for name in order:
        state = states.get(name)
        if state == unmet_tok:
            spans.append(_token_span(name, unmet_tok, tiers.get(name, 1)))
            citation_counts[name] = 0
        elif state == escape_tok:
            spans.append(_token_span(name, escape_tok, tiers.get(name, 1)))
            citation_counts[name] = 0
        elif state == C.EVIDENCED_VALUE:
            span = pass2_values.get(name)
            if span is None:
                continue          # Pass 2 dropped it; completeness will speak
            rec = p1.records[name]
            if rec.value and span.value.strip() != rec.value:
                divergent.append(name)
            spans.append(EvidenceSpan(
                field_name=name,
                value=span.value,
                source_snippet=M.source_snippet(rec, unit_map),
                confidence=span.confidence,
                tier=span.tier,
            ))
            citation_counts[name] = len(rec.indices)

    span_dicts = [
        {"field_name": s.field_name, "value": s.value,
         "source_snippet": s.source_snippet, "confidence": s.confidence}
        for s in spans
    ]

    enforce_terminal_states(
        states, field_names, T.state_vocabulary(codebook),
        paper_id=paper_id, arm=MODEL, attempt=attempt,
    )
    enforce_completeness(span_dicts, field_names, paper_id=paper_id, arm=MODEL,
                         attempt=attempt)
    enforce_citations(
        span_dicts, paper_id=paper_id, arm=MODEL, mode=STRICT,
        escape_token=escape_tok,
        absence_sentinels=C.absence_sentinels(codebook),
        citation_counts=citation_counts, contract_unmet_token=unmet_tok,
        attempt=attempt,
    )

    _LAST_PASS2_TELEMETRY.setdefault("model", MODEL)
    _LAST_PASS2_TELEMETRY["elicitation_run_id"] = run_id
    _LAST_PASS2_TELEMETRY["value_divergence"] = divergent
    _LAST_PASS2_TELEMETRY["n_value_divergence"] = len(divergent)
    _LAST_PASS2_TELEMETRY["n_contract_unmet"] = n_unmet
    _LAST_PASS2_TELEMETRY["accepted_pass1_attempt"] = accepted_attempt

    # D6: the terminal state rides on ALL twenty entries, not only the unmet
    # ones. A reader must be able to tell "evidenced" from "not asked" without
    # inferring it from the absence of a marker. The list SHAPE is unchanged --
    # `auditor.count_populated_fields` and `trace_exporter._build_tier_map` both
    # branch on `isinstance(..., list)` and key-access their fields, so an extra
    # key rides along and a wrapper dict would silently break LOW_YIELD's
    # denominator.
    extracted_data = [
        {**s.model_dump(), "terminal_state": states.get(s.field_name)}
        for s in spans
    ]

    stored = ExtractionResult(
        paper_id=paper_id, fields=spans,
        reasoning_trace=priming,          # the materialized evidence IS the trace
        model=MODEL,
        extraction_schema_hash=schema_hash,
        extracted_at=datetime.now(timezone.utc),
    )
    db.add_extraction_atomic(
        paper_id=paper_id,
        schema_hash=schema_hash,
        extracted_data=extracted_data,
        reasoning_trace=priming,
        model=MODEL,
        spans=span_dicts,
        model_digest=model_digest,
        auditor_model_digest=auditor_model_digest,
    )
    return stored
