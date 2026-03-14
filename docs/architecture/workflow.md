# Workflow Stage Enforcement

The engine enforces a 12-stage sequential workflow via the `workflow_state` table. Each stage must be completed (or bypassed) before the next can begin. This prevents skipping human review gates.

Defined in `engine/adjudication/workflow.py`.

## Workflow Stages

```
── Abstract Screening Adjudication ──
  [1] ABSTRACT_SCREENING_COMPLETE
  [2] ABSTRACT_DIAGNOSTIC_COMPLETE
  [3] ABSTRACT_CATEGORIES_CONFIGURED
  [4] ABSTRACT_QUEUE_EXPORTED
  [5] ABSTRACT_ADJUDICATION_COMPLETE
── PDF Acquisition ──
  [6] PDF_ACQUISITION
── Full-Text Screening ──
  [7] FULL_TEXT_SCREENING_COMPLETE
  [8] FULL_TEXT_ADJUDICATION_COMPLETE
── Extraction Audit ──
  [9] EXTRACTION_COMPLETE
 [10] AI_AUDIT_COMPLETE_STAGE
 [11] AUDIT_QUEUE_EXPORTED
 [12] AUDIT_REVIEW_COMPLETE
```

## Stage Details

### Stage 1: ABSTRACT_SCREENING_COMPLETE

- **Trigger:** Auto — `screener.run_verification()` calls `complete_stage()` after verification succeeds
- **Prerequisite:** None (first stage)
- **What it means:** Primary dual-pass abstract screening + verification pass complete

### Stage 2: ABSTRACT_DIAGNOSTIC_COMPLETE

- **Trigger:** Manual — human confirms 50-paper FP analysis
- **Prerequisite:** ABSTRACT_SCREENING_COMPLETE
- **What it means:** Human has reviewed a diagnostic sample of flagged papers and identified FP patterns

**CLI:**
```bash
python -m engine.adjudication.advance_stage --review surgical_autonomy \
    --stage ABSTRACT_DIAGNOSTIC_COMPLETE \
    --note "50-paper sample reviewed, 8 FP categories identified"
```

### Stage 3: ABSTRACT_CATEGORIES_CONFIGURED

- **Trigger:** Auto — `export_adjudication_queue()` auto-sets when `CategoryConfig` loads successfully
- **Prerequisite:** ABSTRACT_DIAGNOSTIC_COMPLETE
- **What it means:** `adjudication_categories.yaml` exists and validates (8 default FP categories)

### Stage 4: ABSTRACT_QUEUE_EXPORTED

- **Trigger:** Auto — `export_adjudication_queue()` auto-sets after successful Excel export
- **Prerequisite:** ABSTRACT_CATEGORIES_CONFIGURED
- **What it means:** Flagged papers exported to Excel for human review

### Stage 5: ABSTRACT_ADJUDICATION_COMPLETE

- **Trigger:** Auto — `import_adjudication_decisions()` auto-sets when zero unresolved papers remain
- **Prerequisite:** ABSTRACT_QUEUE_EXPORTED
- **What it means:** All flagged papers resolved (INCLUDE or EXCLUDE)

### Stage 6: PDF_ACQUISITION

- **Trigger:** Manual — advance after all PDFs acquired (OA check + download + manual)
- **Prerequisite:** ABSTRACT_ADJUDICATION_COMPLETE
- **What it means:** All included papers have full-text PDFs

**CLI:**
```bash
# Run acquisition pipeline
python -m engine.acquisition.check_oa --review surgical_autonomy --spec ...
python -m engine.acquisition.download --review surgical_autonomy --background
python -m engine.acquisition.pdf_quality_html --review surgical_autonomy --mode acquisition
python -m engine.acquisition.verify_downloads --review surgical_autonomy [--dry-run]

# PDF quality check (after downloads)
python -m engine.acquisition.pdf_quality_check --review surgical_autonomy --spec ...
python -m engine.acquisition.pdf_quality_html --review surgical_autonomy --mode quality_check
python -m engine.acquisition.pdf_quality_import --review surgical_autonomy --input dispositions.json

# Advance when done
python -m engine.adjudication.advance_stage --review surgical_autonomy \
    --stage PDF_ACQUISITION --note "237 auto + 411 manual downloads complete"
```

### Stage 7: FULL_TEXT_SCREENING_COMPLETE

- **Trigger:** Auto — `ft_screener.run_ft_verification()` calls `complete_stage()` after FT verification succeeds
- **Prerequisite:** PDF_ACQUISITION
- **What it means:** Full-text primary screen + verification complete for all parsed papers

### Stage 8: FULL_TEXT_ADJUDICATION_COMPLETE

- **Trigger:** Auto — `import_ft_adjudication_decisions()` auto-sets when zero unresolved FT_FLAGGED papers remain
- **Prerequisite:** FULL_TEXT_SCREENING_COMPLETE
- **What it means:** All FT_FLAGGED papers resolved by human reviewer

### Stage 9: EXTRACTION_COMPLETE

- **Trigger:** Auto — `run_pipeline.py` checks all included papers reach `EXTRACTED` status
- **Prerequisite:** FULL_TEXT_ADJUDICATION_COMPLETE
- **What it means:** Two-pass extraction completed for all papers

### Stage 10: AI_AUDIT_COMPLETE_STAGE

- **Trigger:** Auto — `run_pipeline.py` checks audit run finishes (all papers audited)
- **Prerequisite:** EXTRACTION_COMPLETE
- **What it means:** AI grep + semantic verification complete for all evidence spans; LOW_YIELD papers flagged

### Stage 11: AUDIT_QUEUE_EXPORTED

- **Trigger:** Auto — `export_audit_review_queue()` auto-sets after successful export
- **Prerequisite:** AI_AUDIT_COMPLETE_STAGE
- **What it means:** Per-span rows (contested/flagged/invalid_snippet) + LOW_YIELD papers (all spans) + spot-check sample (all spans) exported to self-documenting Excel workbook with ACCEPT/REJECT/CORRECT dropdowns

### Stage 12: AUDIT_REVIEW_COMPLETE

- **Trigger:** Auto — `import_audit_review_decisions()` auto-sets when all span decisions processed (two-pass validated import)
- **Prerequisite:** AUDIT_QUEUE_EXPORTED
- **What it means:** All evidence spans resolved by human reviewer

## Workflow State Table Schema

```sql
CREATE TABLE IF NOT EXISTS workflow_state (
    id              INTEGER PRIMARY KEY,
    stage_name      TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'complete', 'bypassed')),
    completed_at    TEXT,
    metadata        TEXT
);
```

Seeded with all 12 stages as `pending` on first access.

## Stage States

- `pending` — Not yet reached
- `complete` — Legitimately completed (auto or manual)
- `bypassed` — Force-overridden by operator (logged to audit trail)

## The `--force` Flag

```bash
python -m engine.adjudication.advance_stage --review surgical_autonomy \
    --stage ABSTRACT_DIAGNOSTIC_COMPLETE --note "skipping" --force
```

When `--force` is used:
1. Prerequisite checks are skipped
2. Stage is marked `bypassed` (not `complete`)
3. A warning is logged: `"AUDIT: Stage X bypassed by operator at <timestamp>. Note: <note>"`
4. `bypassed` counts as "done" for downstream prerequisite checks
5. Display shows `[!]` instead of `[✓]` in workflow status

## CLI: Workflow Status

```bash
python -m engine.adjudication.advance_stage --review surgical_autonomy --status
```

Output example:
```
Review Workflow — surgical_autonomy
  ── Abstract Screening Adjudication ──
  [✓] ABSTRACT_SCREENING_COMPLETE (2025-09-15 03:42)
  [✓] ABSTRACT_DIAGNOSTIC_COMPLETE (2025-09-16 18:30)
  [✓] ABSTRACT_CATEGORIES_CONFIGURED (2025-09-16 18:35)
  [✓] ABSTRACT_QUEUE_EXPORTED (2025-09-16 18:35)
  [✓] ABSTRACT_ADJUDICATION_COMPLETE (2025-09-17 22:10)
  ── PDF Acquisition ──
  [!] PDF_ACQUISITION (2025-09-18 04:00) — BYPASSED
  ── Full-Text Screening ──
  [ ] FULL_TEXT_SCREENING_COMPLETE — Run full-text screening...
  [ ] FULL_TEXT_ADJUDICATION_COMPLETE
  ── Extraction Audit ──
  [✓] EXTRACTION_COMPLETE (2025-09-20 11:30)
  [✓] AI_AUDIT_COMPLETE_STAGE (2025-09-21 06:15)
  [ ] AUDIT_QUEUE_EXPORTED — Export the audit review queue...
  [ ] AUDIT_REVIEW_COMPLETE
```

## Grouping Constants

```python
SCREENING_STAGES   = WORKFLOW_STAGES[:5]   # stages 1-5
ACQUISITION_STAGES = WORKFLOW_STAGES[5:6]  # stage 6
FULL_TEXT_STAGES   = WORKFLOW_STAGES[6:8]  # stages 7-8
EXTRACTION_STAGES  = WORKFLOW_STAGES[8:]   # stages 9-12
```

---

*Generated 2026-03-14 from commit `b24f9e7`*
