"""Build-time prompt sizing against the enforced context ceiling.

Scope: THIS pipeline's prompts only. The cross-arm input-fit guard (design
section 4.6(a)) covering every arm remains a separate queued task; this module is
deliberately narrow so that task is free to supersede it.

**The ceiling is 131,072 tokens** -- `n_ctx_train` for deepseek-r1:32b, the value
the runtime clamps to (PARSE-01, M3). The configured values are the wrong numbers
to trust: `default_num_ctx=262144` and `OLLAMA_CONTEXT_LENGTH` both misreport 2x
high, and a guard built on either would pass a prompt that truncates.

**Estimator composition** (ELICIT-DESIGN-01 C4 ruling -- recorded here in full so
the future cross-arm guard inherits the reasoning rather than the number):

  chars -> tokens uses `WORST_RATIO = 0.4288`, the WORST observed tokens/char
  ratio (CAPTURE-01's token-dense p719), never the median. A central estimate is
  the wrong instrument for a safety check: it under-predicts exactly the
  token-dense text most likely to truncate.

  That estimate is then multiplied by `INDEX_MARKER_INFLATION = 1.141`, the
  inflation ELICIT-01 MEASURED between INDEX and COPY `prompt_eval_count` on the
  same papers (report section 5.6). `[Sn]` markers tokenize worse than they read.

  These two do NOT compose cleanly, and the over-count is accepted knowingly. A
  character count of the *rendered numbered* prompt already carries part of the
  marker cost -- ELICIT-01's manifest predicted 1.03-1.05x from characters alone
  against the 1.141x that was measured, so the residual correction is really
  1.141/1.04 ~= 1.10x. Applying the measured 1.141x literally therefore
  OVER-predicts by roughly 4%. That is the safe direction for a hard-fail guard,
  and it keeps the number in the code identical to the number in the report
  rather than substituting a derived one.

**`done_reason` cannot detect input truncation** -- it reports `stop` either way
(PARSE-01, M3). The only reliable post-hoc signal is
`prompt_eval_count == ceiling`, which `truncation_tripwire()` checks. A guard
that passes at build time and a tripwire that fires after the call are answering
different questions; both are needed.
"""

from __future__ import annotations

CEILING_TOKENS = 131_072          # n_ctx_train, the enforced clamp (PARSE-01 / M3)
WORST_RATIO = 0.4288              # worst observed tokens/char (CAPTURE-01 p719)
INDEX_MARKER_INFLATION = 1.141    # measured INDEX/COPY prompt_eval_count (ELICIT-01 5.6)


class PromptTooLargeError(RuntimeError):
    """A prompt is projected to exceed the enforced context ceiling.

    Raised BEFORE the call. The alternative is a silent truncation that
    `done_reason` will report as a clean stop, which is how CAPTURE-01 lost
    papers without noticing.
    """

    def __init__(self, *, label: str, paper_id: int | None, chars: int,
                 estimated_tokens: int, ceiling: int):
        self.label = label
        self.paper_id = paper_id
        self.chars = chars
        self.estimated_tokens = estimated_tokens
        self.ceiling = ceiling
        super().__init__(
            f"{label}"
            + (f" (paper {paper_id})" if paper_id is not None else "")
            + f": projected {estimated_tokens:,} tokens from {chars:,} chars "
            f"exceeds the enforced ceiling of {ceiling:,}. Estimator = "
            f"{WORST_RATIO} tokens/char (worst observed) x {INDEX_MARKER_INFLATION} "
            f"(measured [Sn] marker inflation)."
        )


def estimate_tokens(prompt: str) -> int:
    """Conservative token projection for a marker-bearing prompt."""
    return int(len(prompt) * WORST_RATIO * INDEX_MARKER_INFLATION)


def headroom(prompt: str, ceiling: int = CEILING_TOKENS) -> int:
    return ceiling - estimate_tokens(prompt)


def fits(prompt: str, ceiling: int = CEILING_TOKENS) -> bool:
    return estimate_tokens(prompt) < ceiling


def enforce_fit(prompt: str, *, label: str, paper_id: int | None = None,
                ceiling: int = CEILING_TOKENS) -> int:
    """Hard-fail on projected overflow. Returns the estimate when it fits."""
    est = estimate_tokens(prompt)
    if est >= ceiling:
        raise PromptTooLargeError(
            label=label, paper_id=paper_id, chars=len(prompt),
            estimated_tokens=est, ceiling=ceiling,
        )
    return est


def truncation_tripwire(prompt_eval_count: int | None,
                        ceiling: int = CEILING_TOKENS) -> bool:
    """True when a completed call's input sat exactly at the ceiling.

    `>=` rather than `==` because a runtime that clamped differently would still
    be reporting a truncation, and the tripwire's job is to notice, not to
    adjudicate.
    """
    return prompt_eval_count is not None and prompt_eval_count >= ceiling
