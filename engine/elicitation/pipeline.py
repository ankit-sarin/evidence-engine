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
    _with_think, build_extraction_prompt, extract_pass2_structured,
    _LAST_PASS1_TELEMETRY, _LAST_PASS2_TELEMETRY,
)
from engine.core.effective_config import EffectiveConfig, stage_config
from engine.core.citation_guard import (
    STRICT, UncitedValueError, check_citations, enforce_citations,
)
from engine.core.completeness import (
    DuplicateFieldError, IncompleteExtractionError, enforce_completeness,
    enforce_terminal_states, expected_field_names,
)
from engine.core.events import mint_extraction_uid
from engine.core.extraction_events import elicited_record, write_extraction_events
from engine.core.codebook import CODEBOOK_FILENAME, load_codebook
from engine.elicitation import classes as C
from engine.elicitation import materialize as M
from engine.elicitation import terminal as T
from engine.elicitation.contracts import Pass1Result, check_response
from engine.elicitation.prompts import (
    SYSTEM_PASS1, build_feedback_block, build_pass1_prompt,
    build_pass2_priming_message, prompt_field_order,
)
from engine.elicitation.units import UnitMap, build_unit_map
from engine.utils.ollama_client import ollama_chat

logger = logging.getLogger(__name__)


MAX_PASS1_ATTEMPTS = 2          # Ruling 4


def _codebook_path(review_dir: Path) -> Path:
    """Derived from the review directory, never searched (CODEBOOK-AUTH-01).

    `review_dir` is `data/<review_id>`, so this is `codebook_path_for` with the
    id already resolved to a directory. The glob that stood here returned the
    first codebook on the box when the join missed.
    """
    return review_dir / CODEBOOK_FILENAME


def persist_unit_map(unit_map: UnitMap, review_dir: Path, unit_map_dir_name: str) -> Path:
    """Write the paper's unit map for this run, so every cited index is auditable.

    Per paper per run, the shape ELICIT-01 persisted. Kept on the filesystem
    rather than in `review.db`: `evidence_spans` has no column for a citation
    set and no migration is in scope (ELICIT-DESIGN-01 C5).
    """
    out = review_dir / "elicitation" / unit_map_dir_name / "unit_maps"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{unit_map.paper_id}.json"
    path.write_text(json.dumps(unit_map.to_json()))
    return path


def pass1_messages(prompt: str) -> list[dict]:
    """Pass 1's message list — one builder for the call and the prompt hash (R60)."""
    return [{"role": "system", "content": SYSTEM_PASS1},
            {"role": "user", "content": prompt}]


def sentinel_pass1_prompt(spec, codebook_path=None) -> str:
    """The Pass-1 prompt over the resolver's sentinel text, for the prompt hash."""
    from engine.core.codebook import load_codebook_for
    from engine.core.effective_config import SENTINEL_TEXT
    cb = load_codebook(codebook_path) if codebook_path else load_codebook_for(spec.review_id)
    cb_path = codebook_path or cb.path
    return build_pass1_prompt(build_unit_map(0, SENTINEL_TEXT), cb.raw,
                              expected_field_names(spec, cb_path))


def run_pass1(unit_map: UnitMap, codebook: dict, field_names: tuple[str, ...],
              paper_id: int, think: bool | None = None,
              feedback: str = "", *, cfg: EffectiveConfig | None = None,
              ) -> tuple[Pass1Result, dict]:
    """Elicit citations. Returns (checked result, call telemetry).

    `feedback` is appended to the prompt STRING, not sent as a separate message,
    so the input-fit guard in `ollama_chat` measures it with the rest of the
    request: a correction that would overflow the context is refused or reported
    like any other input (INPUT-FIT-01), never truncated silently.
    """
    prompt = build_pass1_prompt(unit_map, codebook, field_names) + feedback
    cfg = _with_think(cfg or stage_config("elicitation_pass1"), think)

    response = ollama_chat(paper_id=paper_id, messages=pass1_messages(prompt), **cfg.kwargs())
    raw = response.message.content or ""
    thinking = getattr(response.message, "thinking", None) or ""
    pec = getattr(response, "prompt_eval_count", None)

    result = check_response(raw, unit_map, codebook, field_names)
    telemetry = {
        "pass1_prompt_chars": len(prompt),
        "pass1_feedback_chars": len(feedback),
        "pass1_prompt_eval_count": pec,
        "pass1_content_chars": len(raw),
        "pass1_thinking_chars": len(thinking),
        "pass1_done_reason": getattr(response, "done_reason", None),
        "pass1_raw_content": raw,
        **result.telemetry(),
    }
    return result, telemetry


def elicit(unit_map: UnitMap, codebook: dict, field_names: tuple[str, ...],
           paper_id: int, think: bool | None = None, *,
           cfg: EffectiveConfig | None = None,
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
    first, tel_first = run_pass1(unit_map, codebook, field_names, paper_id, think=think, cfg=cfg)
    tels = [tel_first]
    second = None

    if first.failed_fields and MAX_PASS1_ATTEMPTS > 1:
        feedback = build_feedback_block(first, codebook)
        second, tel_second = run_pass1(
            unit_map, codebook, field_names, paper_id, think=think, feedback=feedback,
            cfg=cfg,
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
    unit_map_dir_name: str,
    model_digest: str | None = None,
    auditor_model_digest: str | None = None,
    attempt: int | None = None,
    run_id: int | None = None,
    parsed_text_ref=None,
):
    """Full elicited two-pass extraction for one paper, storing the result.

    `run_id` and `parsed_text_ref` go only into the record a refusal carries
    (9b-2c R3), so an exhausted budget can store per field; nothing else reads
    them until the flip.

    Under Ruling 1 a Pass-1 contract failure no longer refuses the PAPER. Each
    failing field takes the CONTRACT_UNMET terminal state and stores no value;
    the fields that met their contracts are stored normally. What still raises
    before any INSERT: an input that does not fit the model's context
    (`InputOverflow` / `InputTruncated` / `InputDropped`, from the input-fit guard
    in `ollama_chat`), a missing or illegal terminal state (`TerminalStateError`), an incomplete
    Pass-2 result (`IncompleteExtractionError`) and an uncited value at the write
    boundary (`UncitedValueError`). All twenty states are written in one
    transaction or none are.
    """
    from engine.agents.models import EvidenceSpan, ExtractionResult

    review_dir = Path(db.db_path).parent
    cb_path = _codebook_path(review_dir)
    _cb = load_codebook(cb_path)
    codebook = _cb.raw
    codebook_hash = _cb.semantic_hash
    field_names = expected_field_names(spec, cb_path)
    tiers = {f["name"]: int(f.get("tier", 1)) for f in codebook["fields"]}

    # Model, options and the per-pass think policy come from the one resolver.
    cfg_p1 = stage_config("elicitation_pass1", spec)
    cfg_p2 = stage_config("extract_pass2", spec)
    model_name = cfg_p1.model

    unit_map = build_unit_map(paper_id, paper_text)
    persist_unit_map(unit_map, review_dir, unit_map_dir_name)

    escape_tok = C.escape_token(codebook)
    unmet_tok = C.contract_unmet_token(codebook)

    def _record(states, spans=(), violations=None, offenders=(), duplicated=()):
        """9b-2c R3: this attempt's record as built so far, for a refusal."""
        return elicited_record(
            paper_id=paper_id, arm=getattr(getattr(spec, "extraction_models", None),
                                           "arm", None) or model_name,
            run_id=run_id, extraction_uid=mint_extraction_uid(),
            parsed_text=parsed_text_ref, model=model_name, model_digest=model_digest,
            expected=field_names, states=states, spans=spans,
            violations=violations or {}, unmet_token=unmet_tok, escape_token=escape_tok,
            evidenced_token=C.EVIDENCED_VALUE, offenders=offenders,
            duplicated=duplicated, attempts=attempt or 1)

    try:
        p1, accepted_attempt, pass1_tels = elicit(
            unit_map, codebook, field_names, paper_id, cfg=cfg_p1,
        )
    except DuplicateFieldError as exc:
        # R118: a field answered twice in Pass 1. Nothing else is built yet.
        exc.arm = model_name
        exc.record = _record({}, duplicated=exc.duplicated)
        raise
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
        prompt_eval_count=p1_tel["pass1_prompt_eval_count"],
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
    schema_hash = codebook_hash
    if n_evidenced:
        pass2_prompt = build_extraction_prompt(paper_text, spec, cb_path)
        priming_msg = build_pass2_priming_message(priming)
        result = extract_pass2_structured(
            pass2_prompt, priming_msg, spec, paper_id, cfg=cfg_p2,
            codebook_hash=codebook_hash,
        )
        schema_hash = result.codebook_hash
        names = [s.field_name for s in result.fields]
        duplicated = tuple(sorted({n for n in names if names.count(n) > 1
                                   and n in field_names}))
        if duplicated:
            # R118: Pass 2 answered a field twice; neither copy is chosen.
            raise DuplicateFieldError(
                paper_id=paper_id, arm=model_name, duplicated=duplicated,
                n_expected=len(field_names), attempt=attempt,
                record=_record(states, duplicated=duplicated))
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

    violations = {n: (r.fatal or r.violations) for n, r in p1.records.items()}
    try:
        enforce_terminal_states(
            states, field_names, T.state_vocabulary(codebook),
            paper_id=paper_id, arm=model_name, attempt=attempt,
        )
        enforce_completeness(span_dicts, field_names, paper_id=paper_id, arm=model_name,
                             attempt=attempt)
        enforce_citations(
            span_dicts, paper_id=paper_id, arm=model_name, mode=STRICT,
            escape_token=escape_tok,
            absence_sentinels=C.absence_sentinels(codebook),
            citation_counts=citation_counts, contract_unmet_token=unmet_tok,
            attempt=attempt,
        )
    except (IncompleteExtractionError, UncitedValueError) as exc:
        # 9b-2c R3: the refusal carries this attempt's record.
        exc.record = _record(
            states, span_dicts, violations,
            offenders=check_citations(
                span_dicts, escape_token=escape_tok,
                absence_sentinels=C.absence_sentinels(codebook), mode=STRICT,
                citation_counts=citation_counts, contract_unmet_token=unmet_tok,
            ).offenders)
        raise

    _LAST_PASS2_TELEMETRY.setdefault("model", model_name)
    _LAST_PASS2_TELEMETRY["elicitation_run_id"] = unit_map_dir_name
    _LAST_PASS2_TELEMETRY["value_divergence"] = divergent
    _LAST_PASS2_TELEMETRY["n_value_divergence"] = len(divergent)
    _LAST_PASS2_TELEMETRY["n_contract_unmet"] = n_unmet
    _LAST_PASS2_TELEMETRY["accepted_pass1_attempt"] = accepted_attempt

    stored = ExtractionResult(
        paper_id=paper_id, fields=spans,
        reasoning_trace=priming,          # the materialized evidence IS the trace
        model=model_name,
        codebook_hash=schema_hash,
        extracted_at=datetime.now(timezone.utc),
    )
    # 9b-FLIP (R111): every field's terminal state becomes its event — a value
    # asserted, CONTRACT_UNMET as contract_unmet with its class-contract
    # violations, the escape token as declined — in one transaction with the
    # `extracted` paper event. Nothing is written to the legacy extraction tables.
    write_extraction_events(
        db._conn,
        _record(states, span_dicts, violations),
        sentinels=frozenset(_cb.absence_sentinels), review_dir=review_dir)
    return stored
