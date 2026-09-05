"""Write-boundary fail-fast: a value may not be stored without evidence.

Sits beside `engine/core/completeness.py`, and for the same reason. The
completeness guard exists because "did we get any spans" let 21 collapsed Run 6
extractions through. This guard exists because "did we get a span" says nothing
about whether that span carries evidence: ELICIT-01 counted 44 INDEX and 55 COPY
`VALUE_WITHOUT_CITATION` cases across 38 papers and 9 fields — a value asserted
with nothing cited behind it — and the report notes they were *concealed* inside
the provenance ladder, where an empty snippet is recorded as MISSING_SNIPPET or,
when the value happens to be an absence sentinel, as ABSENCE_DECLARED. Reading
the ladder alone attributes uncited assertion to absence.

**Mechanism-independent by design** (section 4.6(c)). The predicate reads spans, not
prompts: a value, a snippet, and optionally a count of validated citations. It
does not know how the evidence was elicited, so a future arm that cites some
other way is governed by the same rule. Nothing about it lives in prompt code.

**Two modes, and the asymmetry is deliberate.**

  `strict` — the elicitation path. Every non-escape value requires at least one
  validated citation. Absence sentinels included: "the paper does not report X"
  is a claim about the paper's text and is evidenced like any other value
  (ELICIT-DESIGN-01 section 4.1 ruling). A sentinel with no citation fails and
  enters bounded retry. That is intended.

  `legacy` — the pre-elicitation prompt, which explicitly instructs the model to
  emit an empty `source_snippet` for an absence value. Failing those would punish
  the model for obeying the prompt it was given, so sentinels are exempt here.
  Every other value still requires a non-empty snippet. The exemption is a
  property of that prompt, and it disappears with it.

Both modes refuse to store a positive claim with nothing behind it. They differ
only on what an absence value owes, because the two prompts asked for different
things.

**The contract-unmet token** (ELICIT-DESIGN-02 Ruling 1) joins the escape token
as the second NON-VALUE token, and is governed identically here: it owes no
citation, and a citation alongside it is a violation. The asymmetry is worth
naming — an escape with a citation is the MODEL contradicting itself, while a
contract-unmet with a citation is the ENGINE contradicting itself, because the
engine writes that token and writes it only where it refused to store evidence.
Hence a distinct code: the two failures have different culprits and the log
should not blur them.

Note what did NOT change. This guard's strictness is untouched: an uncited value
is still refused. Ruling 1 moved where the refusal LANDS — the offending field
becomes CONTRACT_UNMET rather than the whole paper being dropped — and by the
time spans reach here that substitution has already happened upstream. A span
still carrying an uncited value at this boundary is a pipeline defect, and this
guard is what catches it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

STRICT = "strict"
LEGACY = "legacy"
MODES = (STRICT, LEGACY)

VALUE_WITHOUT_CITATION = "VALUE_WITHOUT_CITATION"
ESCAPE_WITH_CITATION = "ESCAPE_WITH_CITATION"
CONTRACT_UNMET_WITH_CITATION = "CONTRACT_UNMET_WITH_CITATION"


class UncitedValueError(RuntimeError):
    """An extraction carries a value with no evidence behind it.

    Raised BEFORE any INSERT, exactly like `IncompleteExtractionError`, so an
    extraction that cannot satisfy the contract leaves nothing behind. There is
    no "store what we got" path: a partially-evidenced extraction is what the
    guard exists to prevent, not a degraded success to record.
    """

    def __init__(self, paper_id: int, arm: str, offenders: tuple[tuple[str, str], ...],
                 mode: str, attempt: int | None = None):
        self.paper_id = paper_id
        self.arm = arm
        self.offenders = offenders
        self.mode = mode
        self.attempt = attempt
        shown = ", ".join(f"{f} ({why})" for f, why in offenders[:8])
        if len(offenders) > 8:
            shown += "…"
        super().__init__(
            f"Paper {paper_id} ({arm}): {len(offenders)} field(s) carry a value with "
            f"no evidence [mode={mode}"
            + (f", attempt {attempt}" if attempt else "")
            + f"]. {shown}"
        )


@dataclass(frozen=True)
class CitationResult:
    ok: bool
    n_checked: int
    offenders: tuple[tuple[str, str], ...] = ()
    n_escape: int = 0
    n_sentinel: int = 0
    n_contract_unmet: int = 0


def _text(span, key: str) -> str:
    v = span.get(key) if isinstance(span, dict) else getattr(span, key, None)
    return str(v or "").strip()


def check_citations(
    spans,
    *,
    escape_token: str | None,
    absence_sentinels: frozenset[str] = frozenset(),
    mode: str = STRICT,
    citation_counts: dict[str, int] | None = None,
    contract_unmet_token: str | None = None,
) -> CitationResult:
    """Check every span carries evidence proportionate to its value.

    `citation_counts` maps field name -> number of VALIDATED citations. When
    supplied (the elicitation path) it is authoritative, because a span's single
    stored snippet is a narrowing of the citation set, not the set itself. When
    absent (any other arm) a non-empty `source_snippet` stands in for it.

    `escape_token=None` means the codebook declares none — a pre-elicitation
    review. No value can then be an escape, which is correct: the token did not
    exist when those extractions were prompted for.
    """
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}")
    escape_u = escape_token.strip().upper() if escape_token else None
    unmet_u = contract_unmet_token.strip().upper() if contract_unmet_token else None
    offenders: list[tuple[str, str]] = []
    n_escape = n_sentinel = n_unmet = 0
    n = 0

    for span in spans or []:
        name = _text(span, "field_name")
        if not name:
            continue
        n += 1
        value_u = _text(span, "value").upper()
        snippet = _text(span, "source_snippet")
        cited = citation_counts.get(name, 0) if citation_counts is not None else None
        has_evidence = bool(snippet) if cited is None else cited > 0

        if escape_u is not None and value_u == escape_u:
            n_escape += 1
            # The escape token is a statement about the search, not the paper.
            # Evidence alongside it contradicts the token it accompanies.
            if has_evidence:
                offenders.append((name, ESCAPE_WITH_CITATION))
            continue

        if unmet_u is not None and value_u == unmet_u:
            n_unmet += 1
            # The engine writes this token only where it refused to store
            # evidence, so evidence alongside it is the engine contradicting
            # itself -- see the module docstring on why the code differs.
            if has_evidence:
                offenders.append((name, CONTRACT_UNMET_WITH_CITATION))
            continue

        is_sentinel = bool(value_u) and value_u in absence_sentinels
        if is_sentinel:
            n_sentinel += 1
            if mode == LEGACY:
                continue

        if not has_evidence:
            offenders.append((name, VALUE_WITHOUT_CITATION))

    return CitationResult(
        ok=not offenders, n_checked=n, offenders=tuple(offenders),
        n_escape=n_escape, n_sentinel=n_sentinel, n_contract_unmet=n_unmet,
    )


def enforce_citations(
    spans,
    *,
    paper_id: int,
    arm: str,
    escape_token: str | None,
    absence_sentinels: frozenset[str] = frozenset(),
    mode: str = STRICT,
    citation_counts: dict[str, int] | None = None,
    contract_unmet_token: str | None = None,
    attempt: int | None = None,
) -> CitationResult:
    """Raise `UncitedValueError` unless every value carries evidence.

    Call immediately before any write, next to `enforce_completeness`.
    """
    result = check_citations(
        spans, escape_token=escape_token, absence_sentinels=absence_sentinels,
        mode=mode, citation_counts=citation_counts,
        contract_unmet_token=contract_unmet_token,
    )
    if not result.ok:
        raise UncitedValueError(
            paper_id=paper_id, arm=arm, offenders=result.offenders,
            mode=mode, attempt=attempt,
        )
    return result
