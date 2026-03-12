# Surgical Evidence Engine — Architecture Overview

The Surgical Evidence Engine is a local, end-to-end systematic review pipeline that accepts a Review Spec (YAML), searches PubMed and OpenAlex, screens papers with cross-family dual-model LLM agents, acquires PDFs via a 5-strategy cascade, parses full text with Docling/Qwen2.5-VL, extracts structured evidence with two-pass DeepSeek-R1, audits spans with cross-model verification (gemma3:27b), enforces human adjudication gates at both screening and extraction stages, and exports publication-ready evidence tables. An optional cloud concordance arm (OpenAI o4-mini + Anthropic Sonnet 4.6) provides multi-model extraction for validation. All inference runs locally on a DGX Spark (Blackwell GB10) via Ollama; no patient data leaves the machine.

## High-Level Pipeline Flow

```
Review Spec YAML
       |
       v
  +---------+     +----------+     +-------------+     +-----------+
  | SEARCH  | --> | SCREEN   | --> | ADJUDICATE  | --> | ACQUIRE   |
  | PubMed  |     | Primary  |     | Human FP    |     | OA Check  |
  | OpenAlex|     | Verifier |     | Review      |     | Download  |
  | Dedup   |     | (dual)   |     | (Excel)     |     | Manual    |
  +---------+     +----------+     +-------------+     +-----------+
                                                              |
       +------------------------------------------------------+
       |
       v
  +---------+     +-----------+     +---------+     +----------+     +--------+
  | PARSE   | --> | EXTRACT   | --> | AUDIT   | --> | HUMAN    | --> | EXPORT |
  | Docling |     | DeepSeek  |     | Grep +  |     | REVIEW   |     | PRISMA |
  | Qwen-VL |     | R1 2-pass |     | Semantic|     | (Excel)  |     | CSV    |
  +---------+     +-----------+     +---------+     +----------+     | Excel  |
                                                                     | DOCX   |
                                                                     | Traces |
                                                                     +--------+
```

## Companion Documents

| Document | Description |
|----------|-------------|
| [pipeline.md](pipeline.md) | Stage-by-stage data flow, triggers, database transitions, CLI commands, and artifacts |
| [models.md](models.md) | Model roster, agent stack, cross-family verification principle, VRAM and context settings |
| [state-machine.md](state-machine.md) | Paper lifecycle states, allowed transitions, evidence span audit states, administrative overrides |
| [workflow.md](workflow.md) | 10-stage workflow enforcement, auto vs manual triggers, prerequisite checks, `--force` audit trail |
| [modules.md](modules.md) | Complete module inventory — every Python file under `engine/` and `scripts/` with purpose, key functions, and dependencies |

## Technology Stack

- **Runtime:** Python 3.12, SQLite (WAL mode), Ollama (localhost:11434)
- **LLM Agents:** qwen3:8b, gemma3:27b, deepseek-r1:32b, qwen2.5vl:7b
- **Cloud APIs:** OpenAI (o4-mini), Anthropic (Sonnet 4.6)
- **PDF Processing:** Docling, PyMuPDF (fitz)
- **Search APIs:** Biopython Entrez (PubMed), pyalex (OpenAlex), Unpaywall
- **Data Validation:** Pydantic v2
- **Export:** openpyxl, python-docx
- **Background Jobs:** tmux auto-detach via `engine/utils/background.py`

## Codebase Statistics

- **82 source files** (Python + Shell)
- **16,459 lines** of code
- **256 offline tests** passing (10 network/ollama deselected)

---

*Generated 2026-03-12 from commit `d65d614`*
