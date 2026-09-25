"""The event-side auditor (WRITE-PATH-01 9b-2d; R17, F8, F10).

Built and tested here; `run_pipeline`'s audit stage is wired to it at the flip,
when `run_audit`'s reads of `evidence_spans` and its `update_audit` / LOW_YIELD
writes retire. Until then nothing in the run path calls this module.

**Per eligible paper, for one arm:**

1. Resolve and read the paper's parsed text (R95). A refused text (A14) skips the
   paper with its reason and writes nothing — the extractor wrote the paper
   event that says why (R122).
2. Every live `asserted` claim without a `citation_located` event is located by
   the one locator (`engine.core.locator`). The test is identical for a value and
   an absence sentinel (R17).
3. Cross-family semantic verification (`auditor.semantic_verify`, the `audit`
   stage) runs only on a claim that was NOT located, is not an absence sentinel
   or non-value token, and has a snippet to show the model. A claim with no
   snippet gets verdict `flagged` without a call (9b-2d R4). Verdicts go to
   run-linked telemetry (`engine.core.audit_telemetry`), not to an event.
   The model calls happen before the paper's transaction opens: nothing is held
   across inference.
4. The located events and, once every live asserted claim of the arm carries
   one, the `audited_ai` paper event are written in ONE savepoint with one
   commit — a refusal anywhere writes nothing for the paper.

`low_yield` is computed on read (R136), never stored.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from pydantic import ValidationError

from engine.agents.auditor import semantic_verify
from engine.agents.models import EvidenceSpan
from engine.core import run_manifest as rm
from engine.core.codebook import CODEBOOK_FILENAME, Codebook, load_codebook
from engine.core.audit_telemetry import record_verdict
from engine.core.effective import (
    NO_RECORDED_STATE, effective_state, effective_value, eligible_paper_ids,
    live_claim_events,
)
from engine.core.effective_config import stage_config
from engine.core.events import write_field_event, write_paper_event
from engine.core.locator import FUZZY_THRESHOLD, LOCATOR_VERSION, locate, locate_payload
from engine.core.parsed_text import ParsedTextError, read_parsed_text, resolve_parsed_text

logger = logging.getLogger(__name__)

LOCATOR_ACTOR = f"locator@{LOCATOR_VERSION}"
VERDICT_FLAGGED = "flagged"
NO_SNIPPET_RATIONALE = "no snippet supplied"


@dataclass(frozen=True)
class AuditReport:
    papers_audited: int
    located: int
    not_located: int
    verified: int
    flagged: int
    skipped_refused: tuple[tuple[int, str], ...]
    low_yield: tuple[int, ...]


def _non_value_tokens(codebook: Codebook) -> frozenset[str]:
    from engine.elicitation.classes import non_value_tokens_for
    return non_value_tokens_for(codebook.path)


def _is_value(value, sentinels: frozenset[str], tokens: frozenset[str]) -> bool:
    if value is None:
        return False
    v = str(value).strip().upper()
    return bool(v) and v not in sentinels and v not in tokens


def low_yield(conn, paper_id: int, arm: str, *, codebook: Codebook, threshold: int) -> bool:
    """R136 on the reader: fewer than `threshold` codebook fields whose effective
    value is a value — not None, not one of the codebook's absence sentinels, not
    a non-value token. Every other declared value ("Not assessable") counts."""
    sentinels = codebook.absence_sentinel_set
    tokens = _non_value_tokens(codebook)
    declared = frozenset(codebook.absence_sentinels)
    populated = sum(
        1 for f in codebook.fields
        if _is_value(effective_value(conn, paper_id, f["name"], arm,
                                     sentinels=declared).value, sentinels, tokens))
    return populated < threshold


def audit_run(conn, spec, *, run_id: int, arm: str, review_dir: str | Path) -> AuditReport:
    """Locate, verify and record the audit of every eligible paper for `arm`."""
    auditor_digest = rm.stage_digest(conn, run_id, "audit")
    if auditor_digest is None and not conn.execute(
            "SELECT 1 FROM run_stage_configs WHERE run_id = ? AND stage = 'audit'",
            (run_id,)).fetchone():
        raise rm.StageNotInRun(
            f"audit refused: run {run_id} has no 'audit' stage — open the run with the "
            "audit stage declared before auditing.")
    cfg = stage_config("audit", spec)
    codebook = load_codebook(Path(review_dir) / CODEBOOK_FILENAME)
    sentinels = codebook.absence_sentinel_set
    tokens = _non_value_tokens(codebook)
    types = {f["name"]: f.get("type", "text") for f in codebook.fields}
    tiers = {f["name"]: int(f.get("tier", 1)) for f in codebook.fields}
    threshold = getattr(spec, "low_yield_threshold", 4)

    audited, n_loc, n_not, n_ver, n_flag = 0, 0, 0, 0, 0
    refused: list[tuple[int, str]] = []
    low: list[int] = []
    for pid in eligible_paper_ids(conn):
        claims = live_claim_events(conn, pid, arm, event_types=("asserted",))
        if not claims:
            continue
        done = {ev.claim_id for ev in live_claim_events(
            conn, pid, arm, event_types=("citation_located",))}
        todo = [c for c in claims if c.claim_id not in done]
        if not todo:
            continue
        try:
            ref = resolve_parsed_text(conn, pid)
            text = read_parsed_text(ref)
        except ParsedTextError as exc:
            logger.warning("Paper %d: audit skipped — %s (%s)", pid, exc.reason_code, exc)
            refused.append((pid, exc.reason_code))
            continue

        # Locate and verify first; the transaction below holds no model call.
        results, verdicts = [], []
        for c in todo:
            res = locate(text, c.source_snippet)
            results.append((c, res))
            if res.located or not _is_value(c.value, sentinels, tokens):
                continue
            if not res.snippet_supplied:
                verdicts.append((c, VERDICT_FLAGGED, NO_SNIPPET_RATIONALE))
                continue
            span = EvidenceSpan(field_name=c.field_name, value=str(c.value),
                                source_snippet=c.source_snippet,
                                confidence=c.payload.get("confidence", 0.5),
                                tier=tiers.get(c.field_name, 1))
            try:
                v = semantic_verify(span, text, field_type=types.get(c.field_name, "text"),
                                    cfg=cfg)
                verdicts.append((c, v.status, v.reasoning))
            except ValidationError as exc:
                verdicts.append((c, VERDICT_FLAGGED,
                                 f"auditor returned unparseable output: {str(exc)[:100]}"))

        processing = effective_state(conn, pid).processing
        conn.execute("SAVEPOINT audit_paper")
        try:
            for c, res in results:
                write_field_event(
                    conn, event_type="citation_located", paper_id=pid,
                    field_name=c.field_name, arm=arm, claim_id=c.claim_id,
                    actor_kind="engine", actor_role="system", actor_name=LOCATOR_ACTOR,
                    payload=locate_payload(res, threshold=FUZZY_THRESHOLD,
                                           parsed_text_sha256=ref.sha256,
                                           parsed_text_uid=ref.parsed_text_uid),
                    run_id=run_id, commit=False)
            located = sum(1 for _, r in results if r.located)
            write_paper_event(
                conn, event_type="audited", paper_id=pid, to_state="audited_ai",
                from_state=None if processing == NO_RECORDED_STATE else processing,
                actor_kind="engine", actor_role="system", actor_name="auditor",
                actor_digest=auditor_digest, run_id=run_id, stage_name="audit",
                payload={"arm": arm, "located": located,
                         "not_located": len(results) - located,
                         "locator_version": LOCATOR_VERSION,
                         "parsed_text_sha256": ref.sha256}, commit=False)
            conn.execute("RELEASE audit_paper")
            if conn.in_transaction:
                conn.commit()
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK TO audit_paper")
                conn.execute("RELEASE audit_paper")
            raise

        for c, verdict, why in verdicts:
            record_verdict(review_dir, run_id=run_id, paper_id=pid, claim_id=c.claim_id,
                           field_name=c.field_name, arm=arm, auditor_model=cfg.model,
                           auditor_digest=auditor_digest, verdict=verdict, rationale=why)
        audited += 1
        n_loc += located
        n_not += len(results) - located
        n_ver += sum(1 for _, v, _ in verdicts if v == "verified")
        n_flag += sum(1 for _, v, _ in verdicts if v != "verified")
        if low_yield(conn, pid, arm, codebook=codebook, threshold=threshold):
            low.append(pid)

    return AuditReport(audited, n_loc, n_not, n_ver, n_flag, tuple(refused), tuple(low))
