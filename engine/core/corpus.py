"""Corpus membership — the single authority on "is this paper in the review".

Before CORPUS-PRED-01 this question was answered by an inline SQL status literal
copied into three modules (`analysis/eval/schema_eval2.py`, and twice in
`engine/cloud/base.py`). The three agreed only by accident: `FT_ELIGIBLE`,
`EXTRACTED` and `HUMAN_AUDIT_COMPLETE` all have zero rows in the current review,
so a divergence between the copies could not show up in any count. The first
paper to reach one of those statuses would have been counted differently by
different modules with no signal at all.

**The derivation rule, in words.** A paper is a corpus member once full-text
screening has admitted it and it has not since left by a failure or exclusion
branch. That is `FT_ELIGIBLE` (admitted, extraction not yet run) plus the three
downstream *success* states it can reach: `EXTRACTED`, `AI_AUDIT_COMPLETE`,
`HUMAN_AUDIT_COMPLETE`.

**Why this is a named constant and not computed from the state machine.**
`engine.core.database.ALLOWED_TRANSITIONS` is a transition graph with no marking
of which states are successes. Its forward closure from `FT_ELIGIBLE` is nine
statuses, not four — it drags in `EXTRACT_FAILED`, `FT_FLAGGED`, `FT_SCREENED_OUT`
and `REJECTED`, and reaches backward to `PARSED` through the `EXTRACT_FAILED`
repair edge. Deriving the four from the graph would therefore require a
hand-written success/failure classification, which is the very thing this module
exists to stop anyone writing twice. So the set is declared once, here, and
`tests/test_corpus_authority.py` pins `STATUSES` whole: adding any status to the
lifecycle turns the suite red until someone rules on its corpus membership.

Status definition of record: `engine.core.database.STATUSES` and
`engine.core.database.ALLOWED_TRANSITIONS`.
"""

from __future__ import annotations

# Order matches the inline literal this constant replaced, so the generated SQL is
# textually comparable with the pre-CORPUS-PRED-01 queries.
CORPUS_STATUSES: tuple[str, ...] = (
    "FT_ELIGIBLE",
    "EXTRACTED",
    "AI_AUDIT_COMPLETE",
    "HUMAN_AUDIT_COMPLETE",
)


def is_corpus_member(status: str | None) -> bool:
    """True if `status` places a paper in the review corpus.

    `None` (no such paper) is not a member, so a missing id fails closed.
    """
    return status in CORPUS_STATUSES


def corpus_status_sql(column: str = "status") -> tuple[str, tuple[str, ...]]:
    """Return an ``<column> IN (?, ?, ...)`` fragment and its bind parameters.

    The SQL view and `is_corpus_member` are two views of `CORPUS_STATUSES`, never
    two lists. Callers splice the fragment into a WHERE clause and pass the
    parameters positionally, in the order the fragment appears in the statement.
    """
    placeholders = ", ".join("?" * len(CORPUS_STATUSES))
    return f"{column} IN ({placeholders})", CORPUS_STATUSES
