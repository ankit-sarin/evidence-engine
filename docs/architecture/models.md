# Model Roster and Agent Stack

## Agent Architecture

| Agent | Model | Family | Ollama Tag | Role | VRAM (approx) | Context |
|-------|-------|--------|------------|------|---------------|---------|
| S-abs — Primary | Qwen3 8B | Qwen (Alibaba) | `qwen3:8b` | High-recall abstract screen | ~5 GB | default |
| S-abs — Verifier | Gemma3 27B | Gemma (Google) | `gemma3:27b` | Strict abstract verification | ~17 GB | default |
| S-ft — Primary | Qwen3.5 27B | Qwen (Alibaba) | `qwen3.5:27b` | Full-text primary screen | ~17 GB | 256K native, think=False |
| S-ft — Verifier | Gemma3 27B | Gemma (Google) | `gemma3:27b` | Full-text verification | ~17 GB | default |
| PDF Parser (scanned) | Qwen2.5-VL 7B | Qwen (Alibaba) | `qwen2.5vl:7b` | Vision OCR for scanned PDFs | ~5 GB | default |
| PDF Quality Checker | Qwen2.5-VL 7B | Qwen (Alibaba) | `qwen2.5vl:7b` | First-page AI classification (language + content type) | ~5 GB | default |
| Extractor | DeepSeek-R1 32B | DeepSeek | `deepseek-r1:32b` | Two-pass structured extraction | ~20 GB | default |
| Auditor | Gemma3 27B | Gemma (Google) | `gemma3:27b` | Cross-model verification + LOW_YIELD detection | ~17 GB | default (ollama_options pass-through) |
| Cloud — OpenAI | o4-mini-2025-04-16 | GPT (OpenAI) | N/A (API) | Concordance extraction arm | N/A | reasoning_effort=high |
| Cloud — Anthropic | claude-sonnet-4-6 | Claude (Anthropic) | N/A (API) | Concordance extraction arm | N/A | extended thinking (budget=10000) |

## Model Selection Rationale

### Cross-Family Verification Principle

The engine deliberately uses models from different families at each verification boundary. This is based on empirical observation:

- **Same-family verification (e.g., Qwen screening + Qwen verification) produced zero signal** — the verifier agreed with every primary decision, catching no false positives.
- **Cross-family verification (Qwen primary + Gemma verifier) caught 100% of identified FPs** — different model architectures make different errors, so a second model from a different family acts as an effective check.
- **416/416 human-verifier concordance** — in the original 251-paper corpus, the Gemma verifier's decisions matched human reviewer judgment perfectly.

This principle is applied at four points:

1. **Abstract Screening:** Qwen3:8b (primary) → Gemma3:27b (verifier)
2. **Full-Text Screening:** Qwen3.5:27b (primary) → Gemma3:27b (verifier)
3. **Extraction → Audit:** DeepSeek-R1:32b (extractor) → Gemma3:27b (auditor)
4. **Cloud concordance:** o4-mini (OpenAI) + Sonnet 4.6 (Anthropic) — independent extraction for cross-model agreement analysis

### Per-Agent Model Selection

**S-abs — Primary (qwen3:8b):**
Chosen for speed and high recall. Simplified exclusion criteria to maximize sensitivity. At 8B parameters, fast enough for 10K+ paper screening runs. Specialty scope (included/excluded surgical specialties) injected into prompt when configured. Configured via `spec.screening_models.primary`.

**S-abs — Verifier (gemma3:27b):**
Chosen for precision and cross-family diversity. Sees full strict exclusion criteria plus 4 FP-catching tests. At 27B, slower but more capable at nuanced exclusion decisions. Configured via `spec.screening_models.verification`. Default in code is `qwen3:32b` but overridden to `gemma3:27b` in the YAML spec.

**S-ft — Primary (qwen3.5:27b):**
Chosen for the full-text screening stage where longer context and deeper reasoning are needed. 256K native context window (text truncated to 32K chars / ~8K tokens via section-aware truncation). Returns structured reason codes (`wrong_specialty`, `no_autonomy_content`, `wrong_intervention`, `protocol_only`, `duplicate_cohort`, `insufficient_data`, `eligible`). Configured via `spec.ft_screening_models.primary`. Think mode disabled for speed (~27s/paper).

**S-ft — Verifier (gemma3:27b):**
Cross-family verification of full-text primary includes. Returns `FT_ELIGIBLE` or `FT_FLAGGED` for human review. Configured via `spec.ft_screening_models.verifier`.

**PDF Parser — PyMuPDF fallback:**
When Docling fails (hyperlink validation errors, malformed PDF structure), PyMuPDF raw text extraction (`fitz.Page.get_text("text")`) is used as a structural fallback. Records `parser_used="pymupdf"`. This is distinct from the scanned-PDF path — PyMuPDF handles digital PDFs that Docling can't process.

**PDF Parser — Scanned (qwen2.5vl:7b):**
Vision-language model for OCR of scanned PDFs. Each page rendered to PNG via PyMuPDF, sent as base64-encoded image. Routing heuristic: < 100 extracted chars/page = scanned. Also activated if both Docling and PyMuPDF return sparse output (< 100 chars).

**PDF Quality Checker (qwen2.5vl:7b):**
Same vision model used for post-download PDF quality classification. Renders page 0 to PNG at configurable DPI (default 150), classifies language (English, Chinese, German, etc.) and content type (full_manuscript, abstract_only, trial_registration, editorial_erratum, conference_poster, other). Configured via Review Spec `pdf_quality_check` section (model, DPI, timeout). Results drive the human disposition workflow (PROCEED / EXCLUDE).

**Extractor (deepseek-r1:32b):**
Selected for reasoning capability. Two-pass design:
- Pass 1: Free reasoning in `<think>` tags — lets the model work through evidence for each field
- Pass 2: Structured JSON with grammar constraint — reasoning trace provided as context
- Timeout: 900s (15 min) per API call, 2 retries
- Known issue: outputs `confidence: -1` for NOT_FOUND fields; clamped to 0.0 via Pydantic validator

**Auditor (gemma3:27b):**
Cross-family from the extractor (DeepSeek → Gemma). Performs grep verification (normalized text matching) then semantic LLM verification. Supports `ollama_options` pass-through for per-model settings (e.g., `num_ctx` for memory-constrained models). Post-audit: runs `check_low_yield()` to flag papers with fewer than `low_yield_threshold` (default 4) populated fields. Configured via `spec.auditor_model` or defaults to `gemma3:27b`.

### Cloud Concordance Arms

**OpenAI o4-mini (o4-mini-2025-04-16):**
- `reasoning_effort=high` for thorough extraction
- Cost: $1.10/M input + $4.40/M output tokens
- Response format: `json_object`
- 3-attempt retry with exponential backoff

**Anthropic Sonnet 4.6 (claude-sonnet-4-6):**
- Extended thinking enabled: `budget_tokens=10000`, `max_tokens=16000`
- Cost: $3.00/M input + $15.00/M output tokens
- Response parsing: strips markdown JSON fences (` ```json ``` `)
- 3-attempt retry with exponential backoff

Both arms use the same extraction prompt (built by `engine/agents/extractor.build_extraction_prompt()`). Results stored in separate `cloud_extractions` and `cloud_evidence_spans` tables for concordance analysis.

## Temperature Settings

All local models run at **temperature 0** (deterministic output). Set explicitly in every `ollama.chat()` call via `options={"temperature": 0}`.

## Pre-Flight Health Checks

All batch runners (FT screening, extraction, audit) run an Ollama pre-flight check before starting. `engine/utils/ollama_preflight.py` sends a minimal completion to each required model, verifies it loads and responds within the timeout, and reports VRAM usage against the 100 GB budget. If any model fails, the batch aborts with a clear error — no silent failures into broken runs.

**CLI:** `python -m engine.utils.ollama_preflight --models qwen3.5:27b gemma3:27b`

## Inference Infrastructure

- **Host:** DGX Spark (Blackwell GB10, sm_121)
- **Server:** Ollama 0.17.7 at `localhost:11434`
- **Cloud APIs:** OpenAI and Anthropic via env vars `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`

---

*Generated 2026-03-14 from commit `b24f9e7`*
