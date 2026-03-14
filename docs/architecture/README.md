# Surgical Evidence Engine — Architecture Overview

The Surgical Evidence Engine is a local, end-to-end systematic review pipeline that accepts a Review Spec (YAML), searches PubMed and OpenAlex, screens papers with cross-family dual-model LLM agents at both abstract and full-text stages, acquires PDFs via a 5-strategy cascade, runs AI-based PDF quality classification (language + content type) with human disposition review, parses full text with Docling/Qwen2.5-VL, applies full-text eligibility screening with structured reason codes, extracts structured evidence with two-pass DeepSeek-R1, audits spans with cross-model verification (gemma3:27b) including LOW_YIELD detection, enforces human adjudication gates at screening, full-text screening, and extraction stages, and exports publication-ready evidence tables. An optional cloud concordance arm (OpenAI o4-mini + Anthropic Sonnet 4.6) provides multi-model extraction for validation. All inference runs locally on a DGX Spark (Blackwell GB10) via Ollama; no patient data leaves the machine.

## High-Level Pipeline Flow

```
Review Spec YAML
       |
       v
  +---------+     +----------+     +-------------+     +-----------+
  | SEARCH  | --> | ABSTRACT | --> | ABSTRACT    | --> | ACQUIRE   |
  | PubMed  |     | SCREEN   |     | ADJUDICATE  |     | OA Check  |
  | OpenAlex|     | Primary  |     | Human FP    |     | Download  |
  | Dedup   |     | Verifier |     | Review      |     | QC Check  |
  +---------+     +----------+     +-------------+     +-----------+
                                                              |
       +------------------------------------------------------+
       |
       v
  +---------+     +----------+     +-------------+
  | PARSE   | --> | FT       | --> | FT          |
  | Docling |     | SCREEN   |     | ADJUDICATE  |
  | Qwen-VL |     | Primary  |     | Human       |
  +---------+     | Verifier |     | Review      |
                  +----------+     +-------------+
                                         |
       +---------------------------------+
       |
       v
  +-----------+     +---------+     +----------+     +--------+
  | EXTRACT   | --> | AUDIT   | --> | HUMAN    | --> | EXPORT |
  | DeepSeek  |     | Grep +  |     | REVIEW   |     | PRISMA |
  | R1 2-pass |     | Semantic|     | (Excel)  |     | CSV    |
  +-----------+     | LOW_    |     +----------+     | Excel  |
                    | YIELD   |                      | DOCX   |
                    +---------+                      | Traces |
                                                     +--------+
```

## Companion Documents

| Document | Description |
|----------|-------------|
| [pipeline.md](pipeline.md) | Stage-by-stage data flow, triggers, database transitions, CLI commands, and artifacts |
| [models.md](models.md) | Model roster, agent stack, cross-family verification principle, VRAM and context settings |
| [state-machine.md](state-machine.md) | Paper lifecycle states, allowed transitions, evidence span audit states, administrative overrides |
| [workflow.md](workflow.md) | 12-stage workflow enforcement, auto vs manual triggers, prerequisite checks, `--force` audit trail |
| [modules.md](modules.md) | Complete module inventory — every Python file under `engine/` and `scripts/` with purpose, key functions, and dependencies |

## Technology Stack

- **Runtime:** Python 3.12, SQLite (WAL mode), Ollama 0.17.7 (localhost:11434)
- **LLM Agents:** qwen3:8b, qwen3.5:27b, gemma3:27b, deepseek-r1:32b, qwen2.5vl:7b
- **Cloud APIs:** OpenAI (o4-mini), Anthropic (Sonnet 4.6)
- **PDF Processing:** Docling, PyMuPDF (fitz)
- **Search APIs:** Biopython Entrez (PubMed), pyalex (OpenAlex), Unpaywall
- **Data Validation:** Pydantic v2
- **Export:** openpyxl, python-docx
- **Background Jobs:** tmux auto-detach via `engine/utils/background.py`

## Codebase Statistics

- **101 source files** (Python + Shell)
- **24,432 lines** of code
- **377+ offline tests** passing (10 network/ollama deselected)

---

*Generated 2026-03-14 from commit `66563cb`*
