# Surgical Evidence Engine — Architecture Reference

> Comprehensive architecture documentation for the local systematic review engine.
> Generated 2026-03-10.

---

## Overview

The Surgical Evidence Engine is a fully local systematic review pipeline running on a DGX Spark. It accepts a Review Spec (YAML), searches PubMed + OpenAlex, screens with dual-model AI, extracts structured evidence with reasoning traces, audits via cross-model verification, and exports publication-ready evidence tables. A 9-stage human-in-the-loop workflow enforces quality gates between screening and export.

**Core principles:**
- No data leaves the machine (all local Ollama inference)
- Cross-family model diversity at every pipeline stage
- Full provenance: every decision is recorded with model, rationale, and timestamp
- Human gates: both screening adjudication and audit review require human sign-off

---

## Table of Contents

1. [Pipeline Data Flow](#1-pipeline-data-flow)
2. [Module Dependency Map](#2-module-dependency-map)
3. [State Machines](#3-state-machines)
4. [Model Roster](#4-model-roster)
5. [Engine Module Inventory](#5-engine-module-inventory)

---

## 1. Pipeline Data Flow

The pipeline runs 8 logical stages, with two human-review gates enforced by a 9-stage workflow state machine.

```
Search → Screen → [Screening Adjudication Gate] → Parse → Extract → Audit → [Audit Review Gate] → Export
```

### Stages

| # | Stage | Module(s) | Model(s) | Output |
|---|-------|-----------|----------|--------|
| 1 | **Search** | `search/pubmed.py`, `search/openalex.py`, `search/dedup.py` | — | Deduplicated citations in DB |
| 2 | **Screen** | `agents/screener.py` | qwen3:8b (primary), gemma3:27b (verifier) | SCREENED_IN / SCREENED_OUT / SCREEN_FLAGGED |
| 3 | **Screening Gate** | `adjudication/workflow.py`, `adjudication/screening_adjudicator.py` | — | 5 stages: diagnostic sample → categories → export → import → complete |
| 4 | **Parse** | `parsers/pdf_parser.py` | Docling (digital), qwen2.5-vl:7b (scanned) | Markdown per paper |
| 5 | **Extract** | `agents/extractor.py` | deepseek-r1:32b | Evidence spans with reasoning traces |
| 5b | **Cloud Extract** | `cloud/openai_extractor.py`, `cloud/anthropic_extractor.py` | o4-mini, sonnet-4.6 | Concordance arms (parallel) |
| 6 | **Audit** | `agents/auditor.py` | gemma3:27b | verified / contested / flagged / invalid_snippet per span |
| 7 | **Audit Gate** | `adjudication/audit_adjudicator.py` | — | 4 stages: extraction complete → audit complete → export → import |
| 8 | **Export** | `exporters/*` | — | PRISMA CSV, evidence CSV/Excel/DOCX, methods MD |

### Two Export Paths

| Path | min_status | Use Case |
|------|-----------|----------|
| **AI output** | `AI_AUDIT_COMPLETE` | Concordance analysis, preliminary results |
| **Production** | `HUMAN_AUDIT_COMPLETE` | Journal submission, final evidence tables |

### Full diagram: [`pipeline_flow.mermaid`](pipeline_flow.mermaid)

---

## 2. Module Dependency Map

The engine is organized into 9 layers:

| Layer | Role | Key Modules |
|-------|------|-------------|
| **core** | Foundation | `review_spec.py` (YAML spec), `database.py` (SQLite state machine), `constants.py` (shared patterns) |
| **search** | Literature retrieval | `pubmed.py`, `openalex.py`, `dedup.py` |
| **agents** | Local LLM pipeline | `screener.py`, `extractor.py`, `auditor.py` |
| **parsers** | PDF → text | `pdf_parser.py` (Docling + Qwen2.5-VL routing) |
| **cloud** | API concordance arms | `base.py`, `openai_extractor.py`, `anthropic_extractor.py` |
| **adjudication** | Human-in-the-loop | `workflow.py` (9 stages), `screening_adjudicator.py`, `audit_adjudicator.py`, `categorizer.py` |
| **exporters** | Output generation | `prisma.py`, `evidence_table.py`, `docx_export.py`, `methods_section.py`, `trace_exporter.py` |
| **review** | Span-level review | `human_review.py` |
| **utils** | Infrastructure | `background.py` (tmux launcher) |

**Dependency direction**: scripts → agents/adjudication/exporters → core/search. The `core` and `search` layers have no upward dependencies.

### Full diagram: [`module_dependencies.mermaid`](module_dependencies.mermaid)

---

## 3. State Machines

### Paper Lifecycle (11 states)

```
INGESTED
  ├─→ SCREENED_IN ──→ PDF_ACQUIRED ──→ PARSED ──→ EXTRACTED ──→ AI_AUDIT_COMPLETE ──→ HUMAN_AUDIT_COMPLETE
  ├─→ SCREENED_OUT (terminal)                        │                    │                     │
  └─→ SCREEN_FLAGGED ──→ SCREENED_IN / SCREENED_OUT  └→ EXTRACT_FAILED   ├→ REJECTED           └→ REJECTED
                                                          └→ retry        (terminal)            (terminal)
```

### Evidence Span Audit States (5 states)

```
pending → verified      (grep ✓ + semantic ✓)
pending → contested     (grep ✗ + semantic ✓)
pending → flagged       (grep ✗ + semantic ✗)
pending → invalid_snippet (ellipsis in source_snippet)

contested/flagged/invalid_snippet → verified  (human accept/override)
```

### Workflow Stages (9 sequential gates)

```
── Screening Adjudication ──
  1. SCREENING_COMPLETE          (auto: screening finishes)
  2. DIAGNOSTIC_SAMPLE_COMPLETE  (manual: human confirms FP analysis)
  3. CATEGORIES_CONFIGURED       (auto: YAML validates)
  4. QUEUE_EXPORTED              (auto: export succeeds)
  5. ADJUDICATION_COMPLETE       (auto: import with 0 unresolved)
    ↓ [pipeline blocks parse/extract/audit until here]
── Extraction Audit ──
  6. EXTRACTION_COMPLETE         (auto: all papers extracted)
  7. AI_AUDIT_COMPLETE_STAGE     (auto: audit run finishes)
  8. AUDIT_QUEUE_EXPORTED        (auto: export succeeds)
  9. AUDIT_REVIEW_COMPLETE       (auto: import with 0 unresolved)
    ↓ [pipeline blocks export until here]
```

Each stage can be: `pending` | `complete` | `bypassed` (force override with audit trail).

### Full diagrams: [`state_machines.mermaid`](state_machines.mermaid)

---

## 4. Model Roster

### Local Models (Ollama)

| Model | Role | VRAM | Configured In |
|-------|------|------|---------------|
| qwen3:8b | Screener — Primary (high recall) | ~5 GB | `review_spec.screening_models.primary` |
| gemma3:27b | Screener — Verifier (strict) + Auditor | ~17 GB | `review_spec.screening_models.verification`, `auditor.DEFAULT_AUDITOR_MODEL` |
| deepseek-r1:32b | Extractor (two-pass) | ~20 GB | `extractor.py` hardcoded |
| qwen2.5-vl:7b | PDF Parser (scanned) | ~5 GB | `pdf_parser.py` hardcoded |

### Cloud Models (API)

| Model | Role | Cost | Configured In |
|-------|------|------|---------------|
| o4-mini-2025-04-16 | Cloud Extractor (OpenAI) | $1.10/$4.40 per M tokens | `cloud/openai_extractor.py` |
| claude-sonnet-4-6 | Cloud Extractor (Anthropic) | $3.00/$15.00 per M tokens | `cloud/anthropic_extractor.py` |

### Cross-Family Diversity

Every pipeline stage uses models from **different families** to catch different error patterns:

- **Screening**: Qwen (primary) vs Google/Gemma (verifier)
- **Extraction → Audit**: DeepSeek (extractor) vs Google/Gemma (auditor)
- **Concordance**: Local (DeepSeek) vs OpenAI vs Anthropic — three independent arms

### Full details: [`model_roster.md`](model_roster.md)

---

## 5. Engine Module Inventory

33 Python modules across 9 layers, plus scripts and tests.

### Quick Reference

| Layer | Modules | Key Files |
|-------|---------|-----------|
| core | 3 | `database.py` (state machine), `review_spec.py` (YAML spec), `constants.py` |
| search | 4 | `pubmed.py`, `openalex.py`, `dedup.py`, `models.py` |
| agents | 4 | `screener.py`, `extractor.py`, `auditor.py`, `models.py` |
| parsers | 2 | `pdf_parser.py`, `models.py` |
| cloud | 5 | `base.py`, `openai_extractor.py`, `anthropic_extractor.py`, `schema.py` |
| adjudication | 7 | `workflow.py`, `screening_adjudicator.py`, `audit_adjudicator.py`, `categorizer.py`, `schema.py`, `advance_stage.py` |
| exporters | 6 | `evidence_table.py`, `docx_export.py`, `prisma.py`, `methods_section.py`, `trace_exporter.py` |
| review | 1 | `human_review.py` |
| utils | 1 | `background.py` |

### Full inventory: [`engine_inventory.md`](engine_inventory.md)

---

## Database Schema

One SQLite database per review (`data/{review}/review.db`), WAL mode.

### Core Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `papers` | Paper metadata + lifecycle status | id, pmid, doi, title, abstract, authors, status |
| `screening_decisions` | Per-pass screening results | paper_id, pass_number, decision, rationale, model |
| `verification_decisions` | Post-screening verification | paper_id, decision, rationale, model |
| `extractions` | Per-paper extraction results | paper_id, extraction_schema_hash, extracted_data (JSON), model |
| `evidence_spans` | Per-field extracted values | extraction_id, field_name, value, source_snippet, confidence, audit_status |
| `review_runs` | Pipeline execution history | review_spec_hash, status, started_at, completed_at |

### Adjudication Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `screening_adjudication` | Screening dispute resolution | paper_id, adjudication_decision, adjudication_source, category |
| `audit_adjudication` | Extraction audit overrides | span_id, paper_id, original_value, human_decision, override_value |
| `workflow_state` | 9-stage workflow progression | stage_name, status (pending/complete/bypassed), metadata |

### Cloud Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `cloud_extractions` | Per-arm cloud extraction results | paper_id, arm, model_string, cost_usd, tokens |
| `cloud_evidence_spans` | Cloud-extracted field values | cloud_extraction_id, field_name, value, confidence |

---

## Test Coverage

266 tests total (252 offline, 14 requiring network/Ollama):

| Test File | Count | Coverage |
|-----------|-------|---------|
| test_database.py | 27 | Tables, lifecycle, transitions, reject, min_status |
| test_adjudication.py | 37 | Categorizer, screening export/import, gate checks |
| test_auditor.py | 26 | Grep verify, semantic verify, full audit |
| test_workflow.py | 22 | 9-stage enforcement, blockers, format |
| test_cloud_extraction.py | 18 | Cloud tables, span parsing, CLI |
| test_extractor.py | 17 | Prompt, thinking trace, two-pass, ellipsis retry |
| test_dedup.py | 15 | DOI/PMID/fuzzy match, merge, stats |
| test_audit_adjudication.py | 15 | Audit export/import, spot-check, reject cascade |
| test_review_spec.py | 11 | YAML loading, hashing, validation |
| test_screener.py | 11 | Dual-pass, verification logic |
| test_trace_exporter.py | 11 | Per-paper traces, quality report |
| test_pdf_parser.py | 9 | Hash, routing, Docling integration |
| test_exporters.py | 8 | PRISMA, CSV, Excel, DOCX, methods |
| test_background.py | 7 | Tmux background mode |
| test_openalex.py | 7 | OpenAlex + abstract reconstruction |
| test_human_review.py | 6 | Review queue export/import |
| test_pubmed.py | 5 | Live PubMed queries |
