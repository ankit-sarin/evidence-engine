# Workflow Stage Enforcement

The engine enforces a 12-stage sequential workflow via the `workflow_state` table. Each stage must be completed (or bypassed) before the next can begin. This prevents skipping human review gates.

Defined in `engine/adjudication/workflow.py`.

## Workflow Stages

| # | Stage Name | Trigger | Type |
|---|-----------|---------|------|
| 1 | `ABSTRACT_SCREENING_COMPLETE` | `screener.run_verification()` auto-completes | Auto |
| 2 | `ABSTRACT_DIAGNOSTIC_COMPLETE` | Human confirms 50-paper FP analysis | Manual |
| 3 | `ABSTRACT_CATEGORIES_CONFIGURED` | `export_adjudication_queue()` detects `adjudication_categories.yaml` | Auto |
| 4 | `ABSTRACT_QUEUE_EXPORTED` | `export_adjudication_queue()` succeeds | Auto |
| 5 | `ABSTRACT_ADJUDICATION_COMPLETE` | `import_adjudication_decisions()` resolves all ABSTRACT_SCREEN_FLAGGED | Auto |
| 6 | `PDF_ACQUISITION` | Human confirms all PDFs acquired and quality-checked | Manual |
| 7 | `FULL_TEXT_SCREENING_COMPLETE` | `ft_screener.run_ft_verification()` auto-completes | Auto |
| 8 | `FULL_TEXT_ADJUDICATION_COMPLETE` | `import_ft_adjudication_decisions()` resolves all FT_FLAGGED | Auto |
| 9 | `EXTRACTION_COMPLETE` | `run_pipeline.py` auto-completes after extraction stage | Auto |
| 10 | `AI_AUDIT_COMPLETE_STAGE` | `run_pipeline.py` auto-completes after audit stage | Auto |
| 11 | `AUDIT_QUEUE_EXPORTED` | `export_audit_review_queue()` succeeds | Auto |
| 12 | `AUDIT_REVIEW_COMPLETE` | `import_audit_review_decisions()` validates and imports | Auto |

## Stage Groupings

| Group | Stages | Purpose |
|-------|--------|---------|
| `SCREENING_STAGES` | 1–5 | Abstract screening pipeline + human adjudication |
| `ACQUISITION_STAGES` | 6 | PDF acquisition + quality check |
| `FULL_TEXT_STAGES` | 7–8 | Full-text screening + human adjudication |
| `EXTRACTION_STAGES` | 9–12 | Extraction, audit, human review |

## Workflow State Table

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

## State Values

| Value | Meaning | Display |
|-------|---------|---------|
| `pending` | Not yet completed | `[ ]` |
| `complete` | Completed normally | `[✓]` |
| `bypassed` | Force-advanced with `--force` | `[!]` |

## Key Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `ensure_workflow_table(conn)` | | Creates table + seeds all 12 stages (only catches "no such table" errors; all other DB errors re-raise) |
| `get_workflow_status(conn)` | → list[dict] | Returns all stages with status/completed_at/metadata |
| `complete_stage(conn, stage_name, metadata)` | | Marks stage as `complete` with timestamp |
| `bypass_stage(conn, stage_name, metadata)` | | Force override, marks `bypassed` with audit warning |
| `is_stage_done(conn, stage_name)` | → bool | True if `complete` or `bypassed` |
| `reset_stage(conn, stage_name)` | | Reverts to `pending` |
| `get_current_blocker(conn)` | → dict or None | Returns first incomplete stage + next_step guidance |
| `can_advance_to(conn, stage_name)` | → bool | Validates all prerequisites are done |
| `advance_stage(conn, stage_name, note, *, force)` | → dict | Main advancement with gating (force=True bypasses) |
| `format_workflow_status(conn, review_name)` | → str | Human-readable multi-line status display |

## The `--force` Flag

Used via `engine/adjudication/advance_stage.py` CLI:

```bash
python -m engine.adjudication.advance_stage \
  --review surgical_autonomy \
  --stage PDF_ACQUISITION \
  --note "All PDFs confirmed acquired" \
  --force
```

Behavior:
1. Skips prerequisite checks (`can_advance_to()` not called)
2. Marks stage as `bypassed` (not `complete`)
3. Logs warning to audit trail in metadata
4. Display shows `[!]` instead of `[✓]`

## Gate Functions

Each adjudication module provides a gate-check function that returns the count of unresolved items:

| Gate | Function | Counts |
|------|----------|--------|
| Abstract adjudication | `check_adjudication_gate(db)` | Unresolved ABSTRACT_SCREEN_FLAGGED papers |
| FT adjudication | `check_ft_adjudication_gate(db)` | Unresolved FT_FLAGGED papers |
| Audit review | `check_audit_review_gate(db)` | AI_AUDIT_COMPLETE papers with contested/flagged/invalid_snippet spans |

When gate count reaches 0, the corresponding workflow stage auto-completes on import. Auto-advance is blocked if unprocessed papers remain — the stage stays pending and a WARNING is logged with the remaining count.

## CLI Status Display

```bash
python -m engine.adjudication.advance_stage --review surgical_autonomy --status
```

Outputs a formatted list of all 12 stages with their current state, completion timestamps, and next-step guidance for the first incomplete stage.

*Generated 2026-03-19 from commit e124b20*
