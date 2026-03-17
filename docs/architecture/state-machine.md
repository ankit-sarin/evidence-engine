# Paper State Machine

## Paper Lifecycle Statuses

Defined in `engine/core/database.py` as the `STATUSES` tuple:

```python
STATUSES = (
    "INGESTED",
    "ABSTRACT_SCREENED_IN",
    "ABSTRACT_SCREENED_OUT",
    "ABSTRACT_SCREEN_FLAGGED",
    "PDF_ACQUIRED",
    "PDF_EXCLUDED",
    "PARSED",
    "FT_ELIGIBLE",
    "FT_SCREENED_OUT",
    "FT_FLAGGED",
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
| `ABSTRACT_SCREENED_IN` | Dual-pass abstract screening: both passes include, or adjudicator includes | `screener.run_screening()`, `screening_adjudicator` |
| `ABSTRACT_SCREENED_OUT` | Dual-pass abstract screening: both passes exclude, or adjudicator excludes | `screener.run_screening()`, `screening_adjudicator` |
| `ABSTRACT_SCREEN_FLAGGED` | Abstract screening passes disagree, or verifier excludes a primary include | `screener.run_screening()`, `screener.run_verification()` |
| `PDF_ACQUIRED` | Full-text PDF obtained and registered in `full_text_assets` | `advance_to_pdf_acquired.py` |
| `PDF_EXCLUDED` | PDF excluded at quality check (non-English, non-manuscript, inaccessible) — terminal | `pdf_quality_import.import_dispositions()` |
| `PARSED` | PDF converted to Markdown (Docling, PyMuPDF, or Qwen2.5-VL) | `pdf_parser.parse_pdf()` |
| `FT_ELIGIBLE` | Full-text screening confirms eligibility for extraction | `ft_screener.run_ft_screening()` |
| `FT_SCREENED_OUT` | Full-text screening excludes paper (with reason code) | `ft_screener.run_ft_screening()`, `ft_screening_adjudicator` |
| `FT_FLAGGED` | Full-text primary/verifier disagree, needs human review | `ft_screener.run_ft_verification()` |
| `EXTRACT_FAILED` | Extraction threw an exception (timeout, parse error) | `extractor.run_extraction()` |
| `EXTRACTED` | Two-pass extraction completed, evidence spans stored | `extractor.run_extraction()` |
| `AI_AUDIT_COMPLETE` | All evidence spans audited by AI (no pending remain) | `auditor.run_audit()` |
| `HUMAN_AUDIT_COMPLETE` | Human reviewer resolved all contested/flagged spans | `audit_adjudicator`, `human_review` |
| `REJECTED` | Paper removed from corpus by human reviewer (with reason) | `ReviewDatabase.reject_paper()` |

## Allowed Transitions

Defined in `engine/core/database.py` as `ALLOWED_TRANSITIONS`:

```
INGESTED ──────────> ABSTRACT_SCREENED_IN
                  \─> ABSTRACT_SCREENED_OUT
                  \─> ABSTRACT_SCREEN_FLAGGED

ABSTRACT_SCREENED_IN ───> PDF_ACQUIRED
                       \─> ABSTRACT_SCREEN_FLAGGED

ABSTRACT_SCREEN_FLAGGED ──> ABSTRACT_SCREENED_IN
                         \─> ABSTRACT_SCREENED_OUT

PDF_ACQUIRED ─────> PARSED
                 \─> PDF_EXCLUDED

PDF_EXCLUDED ─────> (terminal, no transitions)

PARSED ───────────> FT_ELIGIBLE
                 \─> FT_SCREENED_OUT
                 \─> FT_FLAGGED
                 \─> EXTRACTED          (skip path)
                 \─> EXTRACT_FAILED     (skip path)

FT_ELIGIBLE ─────> EXTRACTED
                \─> EXTRACT_FAILED
                \─> FT_FLAGGED

FT_FLAGGED ──────> FT_ELIGIBLE
                \─> FT_SCREENED_OUT

EXTRACT_FAILED ──> PARSED              (retry via --retry-failed)
                \─> FT_ELIGIBLE         (retry)
                \─> EXTRACTED           (retry succeeds)

EXTRACTED ────────> AI_AUDIT_COMPLETE

AI_AUDIT_COMPLETE > HUMAN_AUDIT_COMPLETE
                 \─> REJECTED

HUMAN_AUDIT_COMPLETE ─> REJECTED

ABSTRACT_SCREENED_OUT ─> (terminal, no transitions)
FT_SCREENED_OUT ──────> (terminal, no transitions)
REJECTED ─────────────> (terminal, no transitions)
```

## Terminal States

| State | What makes it terminal |
|-------|----------------------|
| `ABSTRACT_SCREENED_OUT` | Paper excluded during abstract screening. No forward transitions. |
| `PDF_EXCLUDED` | Paper excluded at PDF quality check (non-English, non-manuscript, inaccessible). Reason in `papers.pdf_exclusion_reason`. |
| `FT_SCREENED_OUT` | Paper excluded during full-text screening (with reason code). |
| `REJECTED` | Paper removed from corpus by human reviewer. Reason in `papers.rejected_reason`. |

## Flagged States and Resolution Paths

| Flagged State | Resolution Path | Resolved By |
|---------------|----------------|-------------|
| `ABSTRACT_SCREEN_FLAGGED` | → `ABSTRACT_SCREENED_IN` or `ABSTRACT_SCREENED_OUT` | `screening_adjudicator.import_adjudication_decisions()` |
| `FT_FLAGGED` | → `FT_ELIGIBLE` or `FT_SCREENED_OUT` | `ft_screening_adjudicator.import_ft_adjudication_decisions()` |

## Data Retention Policy

All fetched paper data (metadata, abstract, screening traces, verification traces) is retained permanently regardless of screening outcome. `ABSTRACT_SCREENED_OUT` and `FT_SCREENED_OUT` are labels, not deletions. The database is the single source of truth for all papers ever evaluated. This ensures full PRISMA reporting and audit trail.

## Status Order (for min_status_gate)

Used by exporters to filter papers by minimum completion level:

```python
_STATUS_ORDER = {
    "PARSED": 0,
    "ABSTRACT_SCREENED_OUT": 1,
    "EXTRACTED": 2,
    "AI_AUDIT_COMPLETE": 3,
    "HUMAN_AUDIT_COMPLETE": 4,
}
```

`ReviewDatabase.min_status_gate(paper_id, min_status)` returns `True` if the paper's current status meets or exceeds the given level.

## EXTRACT_FAILED Retry Path

Papers at `EXTRACT_FAILED` can be retried via `--retry-failed`:
- `EXTRACT_FAILED` → `FT_ELIGIBLE` (reset) → re-attempt extraction
- `EXTRACT_FAILED` → `PARSED` (reset) → re-attempt extraction
- On success: `EXTRACT_FAILED` → `EXTRACTED` (direct)

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
| `invalid_snippet` | Snippet contains ellipsis bridging — model abbreviated the quote | `auditor.audit_span()` — regex match on `INVALID_SNIPPET_RE`, no LLM call |

### Audit Logic Details

1. **Absence value auto-verify**: Fields with absence sentinels (`NOT_FOUND`, `NR`, `Not discussed`, `No comparison reported`, `Not assessable`) are automatically verified without LLM calls.
2. **Invalid snippet check** (regex): If `source_snippet` matches `INVALID_SNIPPET_RE` (3+ dots, `[...]`, Unicode ellipsis), status = `invalid_snippet`. No further checks.
3. **Tier 4 exception**: Judgment fields (tier 4) skip grep entirely, go straight to semantic verification.
4. **Grep verify**: Normalized substring match OR sliding-window fuzzy match (SequenceMatcher > 0.85). Text normalization includes: lowercase, collapse whitespace, fix glued punctuation (e.g., `Table.I` → `Table I`), straighten smart quotes.
5. **Semantic verify**: gemma3:27b checks if extracted value is supported by the source snippet. Categorical fields use a specialized prompt (category label need not appear verbatim in text).

### LOW_YIELD Detection (Post-Audit)

After all spans are audited, `check_low_yield()` counts non-null, non-absence extracted fields. Papers with fewer than `low_yield_threshold` (configurable in Review Spec, default 4) populated fields are flagged with `low_yield=1` on the extraction record. LOW_YIELD papers are prioritized in the audit review queue — all spans exported for PI review (not just problem spans).

### Human Resolution of Audit States

After AI audit, human reviewers resolve spans via per-span decisions in the audit review workbook:

- **ACCEPT** → span `audit_status` → `verified`, `auditor_model` = `human_review`
- **CORRECT** → span value overwritten with `corrected_value`, original preserved in `audit_adjudication` table, status → `verified`
- **REJECT** → span marked verified, recorded in `audit_adjudication` table with `human_decision='reject_paper'`

Export scope: one row per problematic span (contested/flagged/invalid_snippet). LOW_YIELD papers export all spans. Spot-check papers (10% of all-verified) export all spans.

**Two-pass import validation**: First pass scans all rows for blank decisions, invalid values, or CORRECT without `corrected_value`. Rejects entire import on any error — zero DB changes on failure. Second pass executes DB updates.

Legacy per-paper format (`accept_as_is`/`reject_paper` columns) auto-detected and supported via `_import_legacy_format()`.

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
- `ABSTRACT_SCREENED_OUT` and `REJECTED` papers unaffected

### `ReviewDatabase.reject_paper(paper_id, reason)`
- Validates transition is allowed from current status
- Atomic transaction (BEGIN/COMMIT/ROLLBACK)
- Records rejection reason in `papers.rejected_reason`

### `engine/utils/extraction_cleanup.cleanup_stale_extractions()`
- Remove extractions from a previous schema version (by `extraction_schema_hash`)
- Delete associated evidence spans (cascade)
- Reset affected papers to `PARSED` — only `EXTRACTED` and `AI_AUDIT_COMPLETE` papers; `HUMAN_AUDIT_COMPLETE` papers are protected
- Dry-run default; requires `--confirm` to execute
- Dedup mode (no schema hash): keeps only the latest extraction per paper

**CLI:** `python -m engine.utils.extraction_cleanup --review NAME [--spec YAML] [--confirm]`

---

*Generated 2026-03-17 from commit d0bf07c*
