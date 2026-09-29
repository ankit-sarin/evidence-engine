"""Which papers an extraction run takes (S5a, D9; R96, R119, R217; WRITE-PATH-01 9b-2a).

Selection used to read `papers.status` — `FT_ELIGIBLE` + `PARSED` at the
extractor, `PARSED` at `run_pipeline`'s gate — and skip a paper that held an
`extractions` row under the current codebook hash. On live every corpus paper is
`AI_AUDIT_COMPLETE`, so both sites selected nothing (D9). Now:

* **Candidates** are the corpus: `effective.eligible_paper_ids`, the eligibility
  axis of `paper_events`. `papers.status` is not read.
* **Input identity** is the paper's current parsed text, resolved and then read
  once so that all three refusals surface here, not mid-run (R122, 9b-2a R2).
  A refused paper is reported with its `reason_code` and the run continues.
* **Skip** a paper only when this arm already holds a LIVE claim-bearing event
  (asserted, declined or contract_unmet — 9b-2a R3) whose `claim_inputs` row
  (R217, D16 — the constrained, indexed authority the writer fills, not the
  unindexed payload) carries the reuse key of this arm, paper and text (R96).
  A superseded claim never blocks, and a claim made under a different text
  version carries a different key. A live claim whose extraction_uid has no
  `claim_inputs` row (reachable only from a migration seed, R150) cannot
  match and never blocks — the row is the authority, not the payload copy it
  is derived from.

This is the only caller of `reuse_key` in the engine. It opens no connection
of its own, and two calls on the same database return equal results.

The payload keys (`PAYLOAD_REUSE_KEY` et al.) are still written by the writer
(R217) — provenance, and the input the writer checks new events against —
but this module no longer reads them; retiring them from the payload
entirely is a later, separate ruling (session 12).
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass

from engine.core.effective import eligible_paper_ids, live_claim_events
from engine.core.parsed_text import (
    ParsedTextError,
    ParsedTextRef,
    read_parsed_bytes,
    resolve_parsed_text,
)
from engine.core.reuse_key import reuse_key

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SelectionResult:
    arm: str
    to_extract: tuple[tuple[int, ParsedTextRef], ...]
    skipped_asserted: tuple[int, ...]
    skipped_refused: tuple[tuple[int, str], ...]   # (paper_id, reason_code)


def _live_reuse_keys(conn: sqlite3.Connection, paper_id: int, arm: str) -> set[str]:
    """R217: the reuse keys `claim_inputs` carries for every LIVE claim-bearing
    event of (paper_id, arm) — one join back from each live claim_id to the
    extraction_uid it names, never a second copy of what "live" means
    (`live_claim_events` is that one predicate). A live claim whose
    extraction_uid has no `claim_inputs` row (a migration seed, R150) is
    simply absent from the join and can never contribute a key."""
    events = live_claim_events(conn, paper_id, arm)
    if not events:
        return set()
    claim_ids = sorted({ev.claim_id for ev in events})
    placeholders = ", ".join("?" * len(claim_ids))
    rows = conn.execute(
        f"SELECT DISTINCT ci.reuse_key FROM field_events fe "
        f"JOIN claim_inputs ci ON fe.extraction_uid = ci.extraction_uid "
        f"WHERE fe.claim_id IN ({placeholders})", claim_ids,
    ).fetchall()
    return {r[0] for r in rows}


def select_for_extraction(conn: sqlite3.Connection, *, arm: str) -> SelectionResult:
    """The papers `arm` should extract now, and why each other corpus paper is not."""
    to_extract, skipped_asserted, skipped_refused = [], [], []
    for paper_id in eligible_paper_ids(conn):
        try:
            ref = resolve_parsed_text(conn, paper_id)
            read_parsed_bytes(ref)
        except ParsedTextError as exc:
            logger.warning("Paper %d: not selected — %s (%s)",
                           paper_id, exc.reason_code, exc)
            skipped_refused.append((paper_id, exc.reason_code))
            continue
        key = reuse_key(arm, paper_id, ref.sha256)
        if key in _live_reuse_keys(conn, paper_id, arm):
            skipped_asserted.append(paper_id)
        else:
            to_extract.append((paper_id, ref))
    return SelectionResult(arm, tuple(to_extract), tuple(skipped_asserted),
                           tuple(skipped_refused))
