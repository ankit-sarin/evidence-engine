# Paper State Machine

## Paper Lifecycle Statuses

Defined in `engine/core/database.py` as the `STATUSES` tuple:

```python
STATUSES = (
    "INGESTED",
    "SCREENED_IN",
    "SCREENED_OUT",
    "SCREEN_FLAGGED",
    "PDF_ACQUIRED",
    "PARSED",
    "EXTRACT_FAILED",
    "EXTRACTED",
    "AI_AUDIT_COMPLETE",
    "HUMAN_AUDIT_COMPLETE",
    "REJECTED",
)
```

## Status Descriptions

| Status | Description | Set By |
|--------|-------------|--------|
| `INGESTED` | Paper added to DB from search results | `ReviewDatabase.add_papers()` |
| `SCREENED_IN` | Dual-pass screening: both passes agree to include, or adjudicator includes | `screener.run_screening()`, `screening_adjudicator.import_adjudication_decisions()` |
| `SCREENED_OUT` | Dual-pass screening: both passes agree to exclude, or adjudicator excludes | `screener.run_screening()`, `screening_adjudicator.import_adjudication_decisions()` |
| `SCREEN_FLAGGED` | Screening passes disagree, or verifier excludes a primary include | `screener.run_screening()`, `screener.run_verification()` |
| `PDF_ACQUIRED` | Full-text PDF obtained and registered in `full_text_assets` | `advance_to_pdf_acquired.py` |
| `PARSED` | PDF converted to Markdown (Docling or Qwen2.5-VL) | `pdf_parser.parse_pdf()` |
| `EXTRACT_FAILED` | Extraction threw an exception (timeout, parse error) | `extractor.run_extraction()` |
| `EXTRACTED` | Two-pass extraction completed, evidence spans stored | `extractor.run_extraction()` |
| `AI_AUDIT_COMPLETE` | All evidence spans audited by AI (no pending remain) | `auditor.run_audit()` |
| `HUMAN_AUDIT_COMPLETE` | Human reviewer resolved all contested/flagged spans | `audit_adjudicator.import_audit_review_decisions()`, `human_review.import_review_decisions()` |
| `REJECTED` | Paper excluded from final corpus (with reason) | `ReviewDatabase.reject_paper()` |

## Allowed Transitions

Defined in `engine/core/database.py` as `ALLOWED_TRANSITIONS`:

```
INGESTED ──────────> SCREENED_IN
                  \─> SCREENED_OUT
                  \─> SCREEN_FLAGGED

SCREENED_IN ───────> PDF_ACQUIRED
                  \─> SCREEN_FLAGGED

SCREEN_FLAGGED ───> SCREENED_IN
                 \─> SCREENED_OUT

PDF_ACQUIRED ─────> PARSED

PARSED ───────────> EXTRACTED
                 \─> EXTRACT_FAILED

EXTRACT_FAILED ──> PARSED          (retry)
                \─> EXTRACTED       (retry succeeds)

EXTRACTED ────────> AI_AUDIT_COMPLETE

AI_AUDIT_COMPLETE > HUMAN_AUDIT_COMPLETE
                 \─> REJECTED

HUMAN_AUDIT_COMPLETE ─> REJECTED

SCREENED_OUT ─────> (terminal, no transitions)
REJECTED ─────────> (terminal, no transitions)
```

## Terminal States

- `SCREENED_OUT` — Paper excluded during screening. No forward transitions.
- `REJECTED` — Paper removed from corpus by human reviewer. No forward transitions. Rejection reason stored in `papers.rejected_reason`.

## Status Order (for min_status_gate)

Used by exporters to filter papers by minimum completion level:

```python
_STATUS_ORDER = {
    "PARSED": 0,
    "SCREENED_OUT": 1,
    "EXTRACTED": 2,
    "AI_AUDIT_COMPLETE": 3,
    "HUMAN_AUDIT_COMPLETE": 4,
}
```

`ReviewDatabase.min_status_gate(paper_id, min_status)` returns `True` if the paper's current status meets or exceeds the given level.

---

## Evidence Span Audit States

Each evidence span (in `evidence_spans` table) has an `audit_status`:

```
pending ──────────> verified        (grep pass + semantic pass)
                \─> contested       (grep fail + semantic pass)
                \─> flagged         (semantic fail)
                \─> invalid_snippet (ellipsis detected, no LLM call)
```

| State | Meaning | Determined By |
|-------|---------|---------------|
| `pending` | Not yet audited | Default on creation |
| `verified` | Source snippet found in paper AND value semantically correct | `auditor.audit_span()` — grep pass + semantic pass |
| `contested` | Source snippet NOT found verbatim, but value semantically supported | `auditor.audit_span()` — grep fail + semantic pass |
| `flagged` | Value not supported by evidence | `auditor.audit_span()` — semantic fail |
| `invalid_snippet` | Snippet contains ellipsis bridging (`...`) — model abbreviated the quote | `auditor.audit_span()` — regex match on `INVALID_SNIPPET_RE`, no LLM call |

### Audit Logic Details

1. **Invalid snippet check** (regex): If `source_snippet` matches `INVALID_SNIPPET_RE` (3+ dots, `[...]`, Unicode ellipsis), status = `invalid_snippet`. No further checks.
2. **Tier 4 exception**: Judgment fields (tier 4) skip grep, go straight to semantic verification.
3. **Grep verify**: Normalized substring match OR sliding-window fuzzy match (SequenceMatcher > 0.85).
4. **Semantic verify**: LLM (gemma3:27b) checks if extracted value is supported by the source snippet. Categorical fields use a specialized prompt (category label need not appear verbatim in text).

### Human Resolution of Audit States

After AI audit, human reviewers resolve `contested`, `flagged`, and `invalid_snippet` spans:

- `accept_as_is` → all spans marked `verified`
- Per-field correction → span value overwritten, status → `verified`
- `reject_paper` → paper status → `REJECTED`

When all spans for a paper are resolved (no contested/flagged/invalid_snippet remaining), paper transitions to `HUMAN_AUDIT_COMPLETE`.

---

## Administrative Overrides

These methods intentionally bypass the state machine for maintenance operations:

### `ReviewDatabase.reset_for_reaudit()`
- Resets all evidence spans: `audit_status` → `pending`, clears auditor fields
- Resets papers: `AI_AUDIT_COMPLETE` / `HUMAN_AUDIT_COMPLETE` → `EXTRACTED`
- Use case: auditor logic changes, prompt refinements
- Atomic transaction

### `ReviewDatabase.reset_for_reextraction()`
- Four-phase atomic transaction:
  1. Collapse audit states → `EXTRACTED`
  2. DELETE all evidence spans for affected papers
  3. DELETE all extraction records for affected papers
  4. `EXTRACTED` → `PARSED`
- Use case: extractor logic changes, schema updates
- `SCREENED_OUT` and `REJECTED` papers unaffected

### `ReviewDatabase.reject_paper(paper_id, reason)`
- Validates transition is allowed from current status
- Atomic transaction (BEGIN/COMMIT/ROLLBACK)
- Records rejection reason in `papers.rejected_reason`

---

*Generated 2026-03-12 from commit `d65d614`*
