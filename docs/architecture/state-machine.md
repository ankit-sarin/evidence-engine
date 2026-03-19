# Paper State Machine

## Paper Lifecycle Statuses

Defined in `engine/core/database.py` as the `STATUSES` tuple:

```
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

**Total:** 15 statuses, 4 terminal.

## State Transition Map

Defined in `engine/core/database.py` as `ALLOWED_TRANSITIONS`:

```
INGESTED
├──▶ ABSTRACT_SCREENED_IN
├──▶ ABSTRACT_SCREENED_OUT          ← terminal
└──▶ ABSTRACT_SCREEN_FLAGGED

ABSTRACT_SCREENED_IN
├──▶ PDF_ACQUIRED
└──▶ ABSTRACT_SCREEN_FLAGGED

ABSTRACT_SCREEN_FLAGGED
├──▶ ABSTRACT_SCREENED_IN           ← after adjudication (INCLUDE)
└──▶ ABSTRACT_SCREENED_OUT          ← after adjudication (EXCLUDE)

PDF_ACQUIRED
├──▶ PARSED
└──▶ PDF_EXCLUDED                   ← terminal (quality check)

PDF_EXCLUDED                        ← terminal, no outgoing transitions

PARSED
├──▶ FT_ELIGIBLE
├──▶ FT_SCREENED_OUT                ← terminal
├──▶ FT_FLAGGED
├──▶ EXTRACTED                      ← skip path (reviews without FT screening)
└──▶ EXTRACT_FAILED                 ← skip path

FT_ELIGIBLE
├──▶ EXTRACTED
├──▶ EXTRACT_FAILED
└──▶ FT_FLAGGED

FT_FLAGGED
├──▶ FT_ELIGIBLE                    ← after adjudication
└──▶ FT_SCREENED_OUT                ← terminal

EXTRACT_FAILED
├──▶ PARSED                         ← retry path
├──▶ FT_ELIGIBLE                    ← retry path
└──▶ EXTRACTED                      ← retry success

EXTRACTED
└──▶ AI_AUDIT_COMPLETE

AI_AUDIT_COMPLETE
├──▶ HUMAN_AUDIT_COMPLETE
└──▶ REJECTED                       ← terminal

HUMAN_AUDIT_COMPLETE
└──▶ REJECTED                       ← terminal

ABSTRACT_SCREENED_OUT               ← terminal
FT_SCREENED_OUT                     ← terminal
REJECTED                            ← terminal
```

## Terminal States

| Status | Meaning |
|--------|---------|
| `ABSTRACT_SCREENED_OUT` | Excluded at abstract screening (dual-model agreement or adjudication) |
| `PDF_EXCLUDED` | Excluded at PDF quality check (non-English, not manuscript, inaccessible, other) |
| `FT_SCREENED_OUT` | Excluded at full-text screening (dual-model agreement or adjudication) |
| `REJECTED` | Excluded at human audit review |

**Data retention:** All paper data is retained permanently regardless of terminal status. SCREENED_OUT is a label, not a deletion.

**Transaction safety:** `update_status()` uses `BEGIN IMMEDIATE` to acquire a write lock before reading the current status, preventing TOCTOU races in concurrent access.

## Status Gate Ordering

Used by `min_status_gate()` to filter exports by minimum quality level:

| Order | Status | Use Case |
|-------|--------|----------|
| 0 | PARSED | Raw parsed papers |
| 1 | ABSTRACT_SCREENED_OUT | All screened |
| 2 | EXTRACTED | Raw extraction data |
| 3 | AI_AUDIT_COMPLETE | AI-verified extractions |
| 4 | HUMAN_AUDIT_COMPLETE | Human-verified extractions |

`min_status_gate()` logs WARNING for missing paper IDs (defensive guard against stale references).

## Evidence Span Audit States

Defined in `evidence_spans.audit_status` CHECK constraint:

```
pending → verified        grep pass + semantic pass (or absence value auto-verify)
pending → contested       grep fail + semantic pass (snippet likely paraphrased)
pending → flagged         semantic fail (value not supported by source)
pending → invalid_snippet ellipsis bridging detected (INVALID_SNIPPET_RE match)
```

**Tier-aware routing** (`engine/agents/auditor.py`):
- Tiers 1–3: grep verify first, then semantic verify
- Tier 4 (judgment fields): semantic-only (skip grep, set `SEMANTIC_ONLY_TIERS = {4}`)

**Absence values** auto-verify without grep or semantic check:
`{"NOT_FOUND", "Not discussed", "NR", "No comparison reported", "Not assessable"}`

## Administrative Overrides

### `admin_reset_status(paper_id, target_status, reason)`

Bypasses state machine. Records full audit trail in `admin_resets` table: paper_id, from_status, to_status, reason, reset_at (UTC timestamp). Used by re-screening scripts to force papers back to earlier states (e.g., ABSTRACT_SCREENED_IN → ABSTRACT_SCREENED_OUT).

### `reset_for_reaudit()`

Atomic reset of all audit state:
1. All evidence spans → `audit_status = 'pending'`
2. Papers at AI_AUDIT_COMPLETE or HUMAN_AUDIT_COMPLETE → EXTRACTED

Returns `{papers_reset, spans_reset}`.

### `reset_for_reextraction()`

Atomic four-phase reset:
1. Audited papers → EXTRACTED (collapse status)
2. Delete all evidence spans
3. Delete all extractions
4. EXTRACTED → PARSED (ready for re-extraction)

Returns `{papers_reset, spans_deleted, extractions_deleted}`.

### `cleanup_stale_extractions()` (`engine/utils/extraction_cleanup.py`)

Schema-hash-based stale data removal:
- Deletes extractions where `extraction_schema_hash != current_hash`
- Cascade deletes associated evidence spans
- Resets EXTRACTED/AI_AUDIT_COMPLETE papers → PARSED
- **Protected:** HUMAN_AUDIT_COMPLETE papers are never reset
- **Dry-run default:** Requires `--confirm` flag for execution
- Auto-backs up DB before destructive operations

### `reject_paper(paper_id, reason)`

Atomic: sets status to REJECTED with `rejected_reason` recorded in papers table.

## Database Tables

### Core Tables

| Table | Key Columns | Purpose |
|-------|-------------|---------|
| `papers` | id, pmid, doi, title, abstract, authors, journal, year, source, status, rejected_reason, ee_identifier, oa_status, pdf_url, download_status, pdf_local_path, pdf_exclusion_reason, pdf_exclusion_detail, pdf_quality_check_status, pdf_ai_language, pdf_ai_content_type, pdf_ai_confidence, pdf_content_hash | Paper metadata + lifecycle state |
| `abstract_screening_decisions` | paper_id, pass_number (1 or 2), decision, rationale, model | Dual-pass abstract screening trace |
| `abstract_verification_decisions` | paper_id, decision, rationale, model | Verifier decisions |
| `ft_screening_decisions` | paper_id, model, decision, reason_code, rationale, confidence | FT primary decisions |
| `ft_verification_decisions` | paper_id, model, decision, rationale, confidence | FT verifier decisions |
| `full_text_assets` | paper_id, pdf_path, pdf_hash, parsed_text_path, parsed_text_version, parser_used | PDF → Markdown tracking |
| `extractions` | paper_id, extraction_schema_hash, extracted_data, reasoning_trace, model, model_digest, auditor_model_digest, low_yield | Extraction results + provenance |
| `evidence_spans` | extraction_id, field_name, value, source_snippet, confidence, audit_status, auditor_model, audit_rationale | Per-field evidence with audit trail |
| `review_runs` | review_spec_hash, screening_hash, extraction_hash, status, log | Pipeline run tracking |

### Adjudication Tables (`engine/adjudication/schema.py`)

| Table | Key Columns | Purpose |
|-------|-------------|---------|
| `abstract_screening_adjudication` | paper_id, adjudication_decision, adjudication_source, adjudication_reason, adjudication_category | Human abstract screening decisions |
| `ft_screening_adjudication` | paper_id, reason_code, adjudication_decision, adjudication_reason | Human FT screening decisions |
| `audit_adjudication` | span_id, paper_id, field_name, original_value, human_decision, override_value, reviewer_notes | Per-span human audit decisions |
| `workflow_state` | stage_name, status (pending/complete/bypassed), completed_at, metadata | 12-stage workflow enforcement |

### Cloud Tables (`engine/cloud/schema.py`)

| Table | Key Columns | Purpose |
|-------|-------------|---------|
| `cloud_extractions` | paper_id, arm, model_string, extracted_data, reasoning_trace, cost_usd, extraction_schema_hash | Cloud extraction results |
| `cloud_evidence_spans` | cloud_extraction_id, field_name, value, source_snippet, confidence, tier, notes | Cloud per-field evidence |

### Administrative Tables

| Table | Key Columns | Purpose |
|-------|-------------|---------|
| `admin_resets` | paper_id, from_status, to_status, reason, reset_at | Audit trail for `admin_reset_status()` bypasses |

### Analysis Tables

| Table | Key Columns | Purpose |
|-------|-------------|---------|
| `human_extractions` | paper_id (EE-NNN format), extractor_id (A/B/C/D), field_name, value, source_quote | Human extractor workbook values |

*Generated 2026-03-19 from commit e124b20*
