# Evidence Engine Architectural & Systems Assessment

This assessment reviews the Evidence Engine codebase across four critical architectural pillars: Local Compute Concurrency, State Machine & Database Integrity, Elicitation & Prompt Pipelines, and Analysis & Exporters. Following the evaluation, a fresh-pass re-evaluation is conducted to identify edge cases, latent risks, and engineering trade-offs.

## 1. Local Compute Concurrency & Workstation Execution

### Architecture Overview

The engine runs local inference on large models (principally deepseek-r1:32b, qwen3:8b, and gemma3:27b) via Ollama. Concurrency control and process management rely on a hybrid strategy: POSIX file locking (fcntl.flock) for experiment exclusion, proactive service recycling to remediate CUDA memory fragmentation, and phase-batched subprocess workers.

### Key Findings & Bottlenecks

- **Global Client State Mutation in Multi-Port Execution:** In analysis/eval/run_qualgap01.py, runtime version switching binds the shared client by mutating engine.utils.ollama_client.\_client at runtime (oc.\_client = ollama.Client(host=host, timeout=oc.\_httpx_timeout)). Because ollama_client initializes \_client at module import time, any concurrent imports, background tasks, or scripts executed in the same Python process will silently inherit this rebound socket. This design creates a severe trap where requests intended for the production server at port 11434 can route to ephemeral test instances at port 11435.

- **Flock Exclusivity vs. Service Restarts (sudo systemctl restart ollama):** The locking mechanism in engine/utils/ollama_lock.py (hold_experiment_lock) correctly protects against concurrent invocations across scripts and blocks foreign crons. However, restart_ollama() executes a system-level service restart. When multiple background jobs or child processes run under the same user, if child workers bypass the lock check or rely on EVIDENCE_ENGINE_NO_OLLAMA_RESTART=1, any unplanned timeout on a worker process without the opt-out environment variable can tear down the active Ollama daemon beneath sibling tasks.

- **Model Swapping Overhead in Screen2f:** In analysis/eval/run_screen2f.py, execution is phase-batched (R1 ruling): all primary passes run on qwen3:8b, followed by verifier passes on gemma3:27b. This batching prevents thrashing the GPU VRAM between sequential papers. However, screen2f_worker.py establishes an external process boundary per arm per phase using subprocess.run with sys.executable -P. While -P successfully protects sys.path from contaminating worktrees (e.g., separating 83defc5 from HEAD), the process boundary requires re-importing heavy Python dependencies and re-probing Ollama /api/version on every phase change.

## 2. State Machine & Database Integrity

### Architecture Overview

The system tracks paper states across systematic review phases (acquisition, PDF quality parsing, abstract screening, full-text screening, extraction, and adjudication) using SQLite. State management utilizes a two-axis model (019_paper_state_axes.py) separating screening status from extraction progression, supported by an append-only event store (016_event_store.py) and schema migrations (engine/migrations/).

### Key Findings & Bottlenecks

- **Concurrency and Connection Isolation:** Throughout the evaluation scripts (such as analysis/eval/elicit01/manifest.py, analysis/eval/parse01/sweep.py, and analysis/paper1/adjudication.py), read access to SQLite employs URI connection parameters: file:{db_path}?mode=ro or file:{db_path}?immutable=1. This protects against accidental lock escalation and write corruption during analytical passes. However, immutable=1 ignores WAL (-wal) and shared-memory (-shm) sidecars. If an ingestion or screening job is concurrently writing via WAL, an immutable=1 reader will read stale database pages or uncommitted pages that violate read-consistency invariants.

- **Audit Log vs. Relational Table Drift:** The migration from raw table updates to an append-only event store (016_event_store.py, 017_seed_event_store.py) ensures PRISMA auditability. However, state checks in engine/core/paper_state.py and engine/core/completeness.py still perform direct updates on relational state tables. If an unexpected process crash occurs mid-pipeline, the event store record may not commit in the same transaction as the evidence_spans or extractions tables unless wrapped in an explicit BEGIN IMMEDIATE transaction block.

- **Foreign Key Enforcement on Migrations:** SQLite does not enforce foreign keys by default unless PRAGMA foreign_keys = ON; is executed on every newly opened connection. While migration 015_drop_prerename_adjudication_indices.py cleans up legacy indices, orphaned foreign keys during paper removals or re-extractions can persist silently unless the SQLite connection pool enforces this pragma globally.

## 3. Elicitation & Prompt Pipelines

### Architecture Overview

The engine features a dual-pass extraction design. Pass 1 elicits free-form reasoning or segmented citations, while Pass 2 extracts structured JSON matching the codebook. Investigations across SCHEMA-EVAL-01, SCHEMA-EVAL-02, and ELICIT-01 systematically evaluated unconstrained output (Condition A), array-based Pydantic schemas (Condition B), required-slot JSON schemas (Condition C), and sentence-level indexing (Condition INDEX).

### Key Findings & Bottlenecks

- **Context Length Clamping & Token Inflation:** analysis/eval/elicit01/manifest.py correctly notes that deepseek-r1:32b enforces a strict context ceiling at n_ctx_train = 131,072 tokens, regardless of whether OLLAMA_CONTEXT_LENGTH is set higher. The heuristic estimator uses a worst-case character-to-token ratio of \$0.4288\$ based on font-glyph-polluted paper p719. However, as revealed in analysis/eval/elicit01/analyze.py, the sentence unit indexing format (\[S1\], \[S2\], etc.) causes substantial token inflation. The markers split tokenization boundaries unpredictably, inflating prompt token counts beyond character-based estimates and reducing output generation headroom.

- **Array Schema vs. Required Slots (The SPANLOSS-01 Vulnerability):** Under Condition B (format=ExtractionOutput.model_json_schema()), the schema wraps spans in {"fields": \[...\]}. In JSON Schema standard implementations, array properties without an enforced minItems allow empty or single-element arrays to pass schema validation. This allowed single-span collapses where the model extracted one field and completed the response. Condition C (required_slot_schema in schema_eval2.py) fixes this structurally by converting fields into named object properties with required: \[...\] and additionalProperties: false.

- **Sentence Segmentation Artifacts in units.py:** analysis/eval/elicit01/units.py uses pysbd (Python Sentence Boundary Disambiguation) with a custom merge step (MIN_UNIT_TOKENS = 3) and strips Docling comment tags (\<!-- image --\>). The bijection contract guarantees that no content is dropped. However, regex stripping of comments replacing them with single spaces alters character offsets. When mapping materialized index quotes back to raw text offsets in PaperIndex, character-level span locators will drift if the locator references the original raw markdown rather than source_stripped.

## 4. Analysis, Provenance & Exporters

### Architecture Overview

The verification engine includes an automated provenance classifier (analysis/provenance/classifier.py) that categorizes evidence spans across five taxonomy tiers: ANCHORED, DRIFTED, UNTRACEABLE_NO_BASIS, ABSENCE_DECLARED, and MISSING_SNIPPET. Systematic review outputs feed directly into PRISMA flow generation (exporters/prisma.py) and adjudication workbooks.

### Key Findings & Bottlenecks

- **Exact Match vs. Semantic Drift in Adjudication:** In analysis/eval/adjud01_pairs.py, value agreement between conditions B and C initially showed an \$11.4\\\$ disagreement rate, triggering an automatic rejection threshold (\$\>10\\\$). Qualitative adjudication (adjud01_labels.json) demonstrated that over \$80\\\$ of these disagreements were purely syntactic variations (SAME_FACT) caused by differing delimiters, abbreviations, or formatting rather than conflicting facts. Relying on strict string normalization (\_norm()) without semantic or numeric canonicalization penalizes valid extractions.

- **Absence Sentinel Classification Logic:** analysis/provenance/classifier.py distinguishes between ABSENCE_DECLARED (e.g., "NOT_FOUND", "NR") and MISSING_SNIPPET. When a model returns a valid extracted value but leaves source_snippet empty, it is classified as MISSING_SNIPPET. If the model populates a generic absence phrase into source_snippet (e.g., "The authors do not report sample size"), the classifier can miscategorize the span as ANCHORED if that phrase happens to be a paraphrase rather than a verbatim quote.

- **PRISMA Accounting Across Deduplication and Screening:** exporters/prisma.py computes inclusion and exclusion counts across database status flags. The gate logic in engine/core/corpus.py strictly separates corpus members from non-members (such as FT_SCREENED_OUT). However, because CARRIED_NON_CORPUS papers (e.g., 547, 629, 799) were kept in evaluation samples for longitudinal consistency with CAPTURE-01, running exporters over mixed evaluation databases risks skewing PRISMA stage tallies unless explicitly filtered by review cohort.

# Fresh-Pass Re-Evaluation & Gap Analysis

A second-pass inspection of the codebase uncovers the following critical gaps, subtle edge cases, and architectural trade-offs:

### 1. Concurrency: The Unbounded Lock Wait Risk in screen2f

In analysis/eval/run_screen2f.py, the runner enforces a quiet window (06:30–09:30 UTC) to avoid interfering with scheduled health checks and daily cron jobs. If a worker pauses, wait_for_resume() polls every 60 seconds until in_quiet_window is false, no pytest process is running, and no foreign model is in VRAM.

- **The Gap:** The check for active pytest processes inspects /proc/\*/cmdline. If a containerized test, isolated runner, or differently named test runner executes, it will go undetected. Conversely, an interactive developer debugging a single test file across the 09:30 UTC boundary will block the entire screening pipeline until max_resume_wait_min (240 minutes) expires, causing an unexpected Abort.

- **Recommendation:** Replace process inspection with explicit task-level coordination flags or a dedicated Redis/POSIX semaphore specifically scoped to testing harnesses.

### 2. State Machine: Transaction Safety During Multi-Step Transitions

In engine/adjudication/advance_stage.py and engine/core/paper_state.py, advancing papers from abstract screening to full-text retrieval requires updating the paper state, recording an audit event, and creating placeholder records.

- **The Gap:** SQLite connection pooling without WAL checkpoint management can lead to database contention under high batch loads (database is locked error). Furthermore, if scripts like scripts/advance_to_pdf_acquired.py run while background workers write telemetry, SQLite's default busy timeout (typically 5 seconds) may be exceeded.

- **Recommendation:** Ensure all SQLite connections set PRAGMA busy_timeout = 30000; (30 seconds) and explicitly set PRAGMA journal_mode = WAL; during engine bootstrap.

### 3. Elicitation: Tokenizer Inconsistency in Unit Map Generation

In analysis/eval/elicit01/units.py, sentence units are created using whitespace token splitting (len(u.split()) \< min_tokens) to filter out micro-units.

- **The Gap:** Whitespace splitting does not reflect BPE (Byte Pair Encoding) tokenization used by DeepSeek or Qwen. A 3-word unit containing complex mathematical notation, chemical formulas, or hyphenated medical terminology (e.g., "\[S14\] 4-hydroxy-2-nonenal-adducted protein-bound...") can expand into 15+ subword tokens, while a 5-word common phrase may only consume 5 tokens. Relying on whitespace splits for sizing guarantees distorts token density budgets.

- **Recommendation:** Integrate a fast local tokenizer (e.g., tiktoken or Hugging Face tokenizers) directly into units.py to bound unit size by actual model tokens rather than naive whitespace splits.

### 4. Provenance Classification: Lexical Normalization vs. OCR Hyphenation

analysis/provenance/classifier.py and analysis/provenance/normalize.py normalize whitespace, lowercase text, and strip punctuation to classify whether a snippet is ANCHORED.

- **The Gap:** PDF text extraction (via Docling/PyMuPDF) frequently introduces soft hyphens, line-break hyphenations (e.g., "sur- gical"), and ligature collapses ("fi" \$\rightarrow\$ "ﬁ"). If the extracted markdown retains line-break hyphens and the model outputs the de-hyphenated word (or vice versa), classify_span downgrades a verbatim citation to DRIFTED or UNTRACEABLE_NO_BASIS.

- **Recommendation:** Add a hyphenation-aware de-wrapping normalization step in analysis/provenance/normalize.py before executing the exact window and substring searches against PaperIndex.

# Targeted Implementation Plan

| **Component** | **Issue Identified** | **Remediation Action** |
|----|----|----|
| **Concurrency** | Shared \_client singleton mutation in qualgap01 | Refactor ollama_client.py to instantiate explicit client instances per worker/runner rather than mutating module-level global state. |
| **Database** | Stale reads under ?immutable=1 URI mode | Restrict immutable=1 exclusively to frozen, read-only evaluation snapshots; enforce mode=ro with standard WAL tracking for active review databases. |
| **Pipeline** | Prompt token explosion via index notation (\[S1\]) | Optimize marker design (e.g., shorthand numeric delimiters §1, §2) and measure token inflation directly with the model's tokenizer during pre-flight. |
| **Provenance** | Strict string mismatch on semantically identical extractions | Incorporate field-type-specific equivalence rules: numeric range tolerance for numerical fields and synonym normalization for categorical fields. |
