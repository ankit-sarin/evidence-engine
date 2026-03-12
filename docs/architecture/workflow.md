# Workflow Stage Enforcement

The engine enforces a 10-stage sequential workflow via the `workflow_state` table. Each stage must be completed (or bypassed) before the next can begin. This prevents skipping human review gates.

Defined in `engine/adjudication/workflow.py`.

## Workflow Stages

```
── Screening Adjudication ──
  [1] SCREENING_COMPLETE
  [2] DIAGNOSTIC_SAMPLE_COMPLETE
  [3] CATEGORIES_CONFIGURED
  [4] QUEUE_EXPORTED
  [5] ADJUDICATION_COMPLETE
── PDF Acquisition ──
  [6] PDF_ACQUISITION
── Extraction Audit ──
  [7] EXTRACTION_COMPLETE
  [8] AI_AUDIT_COMPLETE_STAGE
  [9] AUDIT_QUEUE_EXPORTED
 [10] AUDIT_REVIEW_COMPLETE
```

## Stage Details

### Stage 1: SCREENING_COMPLETE

- **Trigger:** Auto — `screener.run_verification()` calls `complete_stage()` after verification succeeds
- **Prerequisite:** None (first stage)
- **What it means:** Primary dual-pass screening + verification pass complete

### Stage 2: DIAGNOSTIC_SAMPLE_COMPLETE

- **Trigger:** Manual — human confirms 50-paper FP analysis
- **Prerequisite:** SCREENING_COMPLETE
- **What it means:** Human has reviewed a diagnostic sample of flagged papers and identified FP patterns

**CLI:**
```bash
python -m engine.adjudication.advance_stage --review surgical_autonomy \
    --stage DIAGNOSTIC_SAMPLE_COMPLETE \
    --note "50-paper sample reviewed, 8 FP categories identified"
```

### Stage 3: CATEGORIES_CONFIGURED

- **Trigger:** Auto — `export_adjudication_queue()` auto-sets when `CategoryConfig` loads successfully
- **Prerequisite:** DIAGNOSTIC_SAMPLE_COMPLETE
- **What it means:** `adjudication_categories.yaml` exists and validates (8 default FP categories)

### Stage 4: QUEUE_EXPORTED

- **Trigger:** Auto — `export_adjudication_queue()` auto-sets after successful Excel export
- **Prerequisite:** CATEGORIES_CONFIGURED
- **What it means:** Flagged papers exported to Excel for human review

### Stage 5: ADJUDICATION_COMPLETE

- **Trigger:** Auto — `import_adjudication_decisions()` auto-sets when zero unresolved papers remain
- **Prerequisite:** QUEUE_EXPORTED
- **What it means:** All flagged papers resolved (INCLUDE or EXCLUDE)

### Stage 6: PDF_ACQUISITION

- **Trigger:** Manual — advance after all PDFs acquired (OA check + download + manual)
- **Prerequisite:** ADJUDICATION_COMPLETE
- **What it means:** All included papers have full-text PDFs

**CLI:**
```bash
# Run acquisition pipeline
python -m engine.acquisition.check_oa --review surgical_autonomy --spec ...
python -m engine.acquisition.download --review surgical_autonomy --background
python -m engine.acquisition.manual_list --review surgical_autonomy --spec ...

# Advance when done
python -m engine.adjudication.advance_stage --review surgical_autonomy \
    --stage PDF_ACQUISITION --note "237 auto + 411 manual downloads complete"
```

### Stage 7: EXTRACTION_COMPLETE

- **Trigger:** Auto — `run_pipeline.py` checks all included papers reach `EXTRACTED` status
- **Prerequisite:** PDF_ACQUISITION
- **What it means:** Two-pass extraction completed for all papers

### Stage 8: AI_AUDIT_COMPLETE_STAGE

- **Trigger:** Auto — `run_pipeline.py` checks audit run finishes (all papers audited)
- **Prerequisite:** EXTRACTION_COMPLETE
- **What it means:** AI grep + semantic verification complete for all evidence spans

### Stage 9: AUDIT_QUEUE_EXPORTED

- **Trigger:** Auto — `export_audit_review_queue()` auto-sets after successful export
- **Prerequisite:** AI_AUDIT_COMPLETE_STAGE
- **What it means:** Contested/flagged spans + spot-check sample exported to Excel

### Stage 10: AUDIT_REVIEW_COMPLETE

- **Trigger:** Auto — `import_audit_review_decisions()` auto-sets when zero unresolved spans remain
- **Prerequisite:** AUDIT_QUEUE_EXPORTED
- **What it means:** All evidence spans resolved by human reviewer

## Workflow State Table Schema

```sql
CREATE TABLE workflow_state (
    id              INTEGER PRIMARY KEY,
    stage_name      TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'complete', 'bypassed')),
    completed_at    TEXT,
    metadata        TEXT
);
```

Seeded with all 10 stages as `pending` on first access.

## Stage States

- `pending` — Not yet reached
- `complete` — Legitimately completed (auto or manual)
- `bypassed` — Force-overridden by operator (logged to audit trail)

## The `--force` Flag

```bash
python -m engine.adjudication.advance_stage --review surgical_autonomy \
    --stage DIAGNOSTIC_SAMPLE_COMPLETE --note "skipping" --force
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
  ── Screening Adjudication ──
  [✓] SCREENING_COMPLETE (2025-09-15 03:42)
  [✓] DIAGNOSTIC_SAMPLE_COMPLETE (2025-09-16 18:30)
  [✓] CATEGORIES_CONFIGURED (2025-09-16 18:35)
  [✓] QUEUE_EXPORTED (2025-09-16 18:35)
  [✓] ADJUDICATION_COMPLETE (2025-09-17 22:10)
  ── PDF Acquisition ──
  [!] PDF_ACQUISITION (2025-09-18 04:00) — BYPASSED
  ── Extraction Audit ──
  [✓] EXTRACTION_COMPLETE (2025-09-20 11:30)
  [✓] AI_AUDIT_COMPLETE_STAGE (2025-09-21 06:15)
  [ ] AUDIT_QUEUE_EXPORTED — Export the audit review queue...
  [ ] AUDIT_REVIEW_COMPLETE
```

## Grouping Constants

```python
SCREENING_STAGES  = WORKFLOW_STAGES[:5]   # stages 1-5
ACQUISITION_STAGES = WORKFLOW_STAGES[5:6]  # stage 6
EXTRACTION_STAGES = WORKFLOW_STAGES[6:]    # stages 7-10
```

---

*Generated 2026-03-12 from commit `d65d614`*
