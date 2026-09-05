Read primer.md for current project state before starting work.

# Surgical Evidence Engine (Project 4)

## Location
~/projects/evidence-engine

## Deployment
- Port: 7864
- URL: evidence.digitalsurgeon.dev
- Service: evidence-engine (systemd, when ready)

## Purpose
Local systematic review engine on DGX Spark. Accepts Review Specs (YAML), runs search/screening/extraction/audit pipeline, exports publication-ready evidence tables. No data leaves the machine.

## Project Structure
```
evidence-engine/
├── CLAUDE.md                   # Static architecture (this file)
├── primer.md                   # Working state (maintained by Claude Code, gitignored)
├── pyproject.toml              # pytest config (markers: network, ollama, integration, fence_selftest)
├── requirements.txt
├── review_specs/               # Review spec YAML files
├── engine/
│   ├── core/                   # Pydantic models, YAML loader, SQLite state machine
│   ├── search/                 # PubMed, OpenAlex, DOI/PMID/fuzzy dedup
│   ├── agents/                 # Screener, FT screener, extractor, auditor
│   ├── cloud/                  # Cloud extraction arms (OpenAI, Anthropic) + schema
│   ├── analysis/               # Concordance analysis (scoring, metrics, normalization, reports)
│   ├── parsers/                # Three-tier PDF parser (Docling → PyMuPDF → Qwen2.5-VL)
│   ├── acquisition/            # Unpaywall, download cascade, PDF quality check, verify
│   ├── migrations/             # DB schema migrations
│   ├── adjudication/           # Workflow stages, screening/FT/audit adjudication
│   ├── utils/                  # tmux background, extraction cleanup, ollama preflight
│   ├── validators/             # Extraction validator + distribution collapse monitor
│   ├── elicitation/            # Per-class evidence elicitation (units, classes, contracts,
│   │                           #   prompts, materialize, sizing, pipeline)
│   └── exporters/              # PRISMA, evidence tables, DOCX, methods, traces
├── analysis/
│   ├── paper1/                 # Human workbook import, consensus derivation, adjudication
│   ├── provenance/             # Frozen v1.1 evidence-provenance taxonomy + classifier
│   └── eval/                   # Response-contract, runtime and priming evaluations
├── scripts/                    # Pipeline runners, batch scripts, monitors
├── tests/                      # ~1,636 offline + 15 network/ollama/integration
│   └── conftest.py             # Suite-wide service-call fence (see Ops Invariants)
└── data/                       # gitignored — per-review databases, PDFs, exports,
                                #   eval stores, telemetry
```

## Agent Architecture
| Agent | Model | Role |
|-------|-------|------|
| Abstract Screener — Primary | qwen3:8b | High-recall abstract screen (simplified exclusion criteria) |
| Abstract Screener — Verifier | gemma3:27b | Strict verification of primary includes (full exclusion criteria) |
| FT Screener — Primary | qwen3.5:27b | Full-text screen with specialty scope (/no_think, ~27s/paper) |
| FT Screener — Verifier | gemma3:27b | Strict FT verification, 5-test FP catcher (~20s/paper) |
| PDF Parser | Docling → PyMuPDF → Qwen2.5-VL:7b | Three-tier: digital → structural fallback → scanned vision |
| Extractor | deepseek-r1:32b | Two-pass structured extraction with reasoning trace |
| Auditor | gemma3:27b | Cross-model verification + LOW_YIELD detection |
| Cloud Extractor (OpenAI) | o4-mini-2025-04-16 | Concordance arm — reasoning_effort=high |
| Cloud Extractor (Anthropic) | claude-sonnet-4-6 | Concordance arm — extended thinking |

## Data Architecture
- SQLite: One database per review (state machine, provenance)
- ChromaDB: Vector embeddings per review (disposable, rebuildable)
- File system: Immutable PDF + parsed Markdown store

### Database Tables (beyond core papers/extractions/evidence_spans)
| Table | Purpose |
|-------|---------|
| cloud_extractions | Parallel to `extractions` — tracks arm, model, cost, reasoning traces |
| cloud_evidence_spans | Parallel to `evidence_spans` — cloud-arm field values |
| human_extractions | Human extractor workbook values (paper_id as "EE-NNN", extractor_id A/B/C/D) |
| judge_runs | Paper 1 LLM-as-judge runs (Pass 1 pairwise rating + Pass 2 fabrication verification). PK `run_id TEXT`. Stores `judge_model_digest` (canonical SHA-256 from Ollama `/api/tags`) and `codebook_sha256`. Migration 007. |
| judge_ratings | Per-triple Pass 1 output. One row per (run_id, paper_id, field_name). Stores `pass1_fabrication_risk`, arm permutation, seed, prompt hash, raw response. Migration 007. |
| judge_pair_ratings | C(N,2) rows per `judge_ratings` row — Level 1 (EQUIVALENT / PARTIAL / DIVERGENT) and Level 2 (GRANULARITY / SELECTION / FABRICATION / …) per arm pair. Migration 007. |
| fabrication_verifications | Pass 2 per-arm verdicts. UNIQUE (judge_run_id, paper_id, field_name, arm_name). verdict ∈ {SUPPORTED, PARTIALLY_SUPPORTED, UNSUPPORTED}. CHECK: UNSUPPORTED requires non-empty reasoning + fabrication_hypothesis. CASCADE FK to judge_runs. Migration 008. |
| judge_run_audit | Post-hoc corrections / annotations on judge_runs (open-vocabulary `event_type`, NOT NULL `rationale`, CASCADE FK). First user: the `backfill_judge_model_digest` event (commit 8fefa66). Migration 009. |

## Paper Lifecycle
INGESTED → ABSTRACT_SCREENED_IN / ABSTRACT_SCREENED_OUT / ABSTRACT_SCREEN_FLAGGED → PDF_ACQUIRED → PDF_EXCLUDED (terminal) or PARSED → FT_ELIGIBLE / FT_SCREENED_OUT / FT_FLAGGED → EXTRACTED / EXTRACT_FAILED → AI_AUDIT_COMPLETE → HUMAN_AUDIT_COMPLETE → REJECTED
(PARSED can skip FT screening directly to EXTRACTED for reviews without FT screening)
(PDF_EXCLUDED is terminal — papers excluded at quality check do not advance)
(Papers at AI_AUDIT_COMPLETE entering FT screening: decisions recorded but status not changed)

## Pipeline Stages
1. **SEARCH** — PubMed + OpenAlex → deduplicate → add to DB
2. **ABSTRACT SCREEN** — Dual-model: primary (qwen3:8b, high-recall) → verifier (gemma3:27b, strict + 4 FP tests)
3. **ACQUIRE** — Unpaywall OA check → 5-strategy cascade download → manual list for remainder
4. **PARSE** — Docling (digital) → PyMuPDF fallback (Docling errors) → Qwen2.5-VL (scanned) → Markdown
5. **FT SCREEN** — Dual-model full-text: primary (qwen3.5:27b) → verifier (gemma3:27b, 5-test FP catcher). Specialty scope filtering. Text truncation to 32K chars.
6. **EXTRACT** — Pass 1: DeepSeek-R1 reasoning → Pass 2: structured JSON
7. **CLOUD EXTRACT** — Parallel concordance arms: OpenAI o4-mini + Anthropic Sonnet 4.6. Same codebook prompt, independent parsing
8. **DISTRIBUTION CHECK** — Post-extraction quality gate: detect categorical field collapse across any arm
9. **AUDIT** — Grep verify + semantic verify via gemma3:27b + LOW_YIELD detection (configurable threshold)
10. **CONCORDANCE** — Multi-arm agreement analysis: scoring, normalization, kappa + percent agreement with 95% CI
11. **ADJUDICATION GATE** — 12-stage workflow: 5 abstract + 1 acquisition + 2 FT + 4 extraction audit (human review required)
12. **EXPORT** — PRISMA CSV, evidence CSV/Excel/DOCX, methods section (min_status filtering)

## Inference
- Local models via Ollama at localhost:11434. Temperature 0 for all agents.
- Cloud models via OpenAI and Anthropic APIs (env vars OPENAI_API_KEY, ANTHROPIC_API_KEY).

## Key Patterns
- Review Spec (YAML) defines the entire review contract
- Protocol hashing: SHA-256 of screening/extraction sections for staleness detection
- Role-aware screening: primary sees simplified exclusions (high recall), verifier sees full strict criteria (high precision). Cross-family diversity (Qwen vs Gemma)
- Two-pass extraction: free reasoning trace → grammar-constrained structured output
- Evidence spans: source_snippet fields for traceability
- Grep + semantic audit: check snippet exists in paper, then verify value matches
- Per-review isolation: each review gets its own SQLite DB and directory tree
- 12-stage workflow enforcement with human gates between phases
- Abstract retention policy: all paper data retained permanently — SCREENED_OUT is a label, not a deletion
- LOW_YIELD detection: post-audit quality gate, configurable threshold, PRISMA-reported
- PDF acquisition: 5-strategy cascade, %PDF validation, publisher grouping, --background tmux support
- PDF verify/import: filename matching, canonical rename to `EE-{nnn}_{Author}_{Year}.pdf`, DB update
- DB-driven PDF path resolution: `full_text_assets.pdf_path` → `papers.pdf_local_path` → glob fallback
- Audit adjudication: per-span ACCEPT/REJECT/CORRECT, spot-check sampling, two-pass import validation
- min_status parameter on exporters: AI_AUDIT_COMPLETE (raw AI) vs HUMAN_AUDIT_COMPLETE (human-verified)
- ollama_options pass-through: per-model Ollama settings (e.g., num_ctx)
- PRISMA reconciliation: validates terminal + in-progress = total, no double-counting
- Three-tier PDF parsing: Docling → PyMuPDF fallback (hyperlink/structure errors) → Qwen2.5-VL:7b (scanned). Sparse threshold <100 chars after both text parsers
- Self-documenting review workbooks: shared builder with DataValidation dropdowns, conditional formatting, Instructions sheet. Used by all 3 adjudication exporters
- PDF quality check: AI classification (vision model) + HTML disposition + JSON import. PDF_EXCLUDED is terminal
- Extraction validator: schema-driven field name + categorical value check. Read-only diagnostic
- Extraction cleanup: schema-hash-based stale data removal. Dry-run default. Pre-flight warning in extractor
- Ollama pre-flight: model health check + VRAM budget validation. Wired into FT screener, extractor, auditor
- FT screening: dual-model cross-family, specialty scope, /no_think, 32K truncation, checkpoint/resume, 7 reason codes. Status-aware for papers at any lifecycle stage
- Pass-1 think policy is declared per pass in the Review Spec (`extraction_models.pass1_think` / `.pass2_think`) and passed explicitly on every call — never left to a version-dependent Ollama default (REGRESSION-01)

## Ops Invariants — Ollama service safety

Two independent code paths can run `sudo systemctl restart ollama`, and a
`NOPASSWD` sudoers rule for exactly that command means both really fire. Both are
now gated, and the suite is fenced. Do not add a third ungated path.

- **Experiment flock** (`engine/utils/ollama_lock.py`, OPS-GUARD-01): `flock(2)` on
  `~/.ollama_experiment.lock`, never an existence check, so a dead holder releases
  automatically. Long runs wrap themselves in `hold_experiment_lock()`.
- **Both restart paths gate on `foreign_lock_held()`**, deliberately narrower than
  `check_experiment_lock()` — a process holding the lock for its own run must still
  be able to restart, but restarting under *someone else's* experiment destroys it.
  - `extractor.restart_ollama()` — proactive CUDA-defrag restart (`RESTART_EVERY_N`).
  - `ollama_client._restart_ollama_and_retry()` — last-resort recovery after the
    wall-clock watchdog exhausts retries (OPSFIX-01).
- **Opt-out:** `EVIDENCE_ENGINE_NO_OLLAMA_RESTART` disarms the recovery branch for
  harnesses pointed at a different server. Refusal raises `RuntimeError`, which
  `ollama_chat` surfaces as `TimeoutError` — never a silent success.
- **Suite fence** (`tests/conftest.py`, OPSFIX-01): an autouse fixture wraps
  `subprocess.run/Popen/call/check_call/check_output` and `os.system` and refuses any
  argv naming a service manager or privilege escalation, matched on command basename.
  `ServiceCallBlocked` derives from **`BaseException`** — load-bearing, because
  application code catches `Exception` around restarts and would otherwise swallow it.
  No tier is exempt, including the nightly full-suite run. Tests that exercise the
  fence carry `@pytest.mark.fence_selftest`.
  - Tests must never reach a live service. Patch the boundary:
    `@patch("engine.utils.ollama_client.subprocess.run")`, or
    `@patch("engine.utils.ollama_preflight.require_preflight")` for anything calling
    `run_extraction` (preflight shells out to `systemctl show` *and* loads a 20 GB model).

## Cloud Extraction Architecture
- `CloudExtractorBase` (engine/cloud/base.py): shared logic — pending paper query, codebook-driven prompt building, response JSON parsing (8+ alternate keys + raw content recovery), progress tracking, cost calculation, distribution monitor integration
- `OpenAIExtractor`: o4-mini-2025-04-16, reasoning_effort=high. Per-paper cost tracking (input/output/reasoning tokens)
- `AnthropicExtractor`: claude-sonnet-4-6, extended thinking (10K token budget). Streaming response with thinking block capture
- `store_extraction()` rejects 0-span results with ValueError — prevents silent data loss
- Cloud schema (engine/cloud/schema.py): creates cloud_extractions + cloud_evidence_spans tables
- Cost rates: OpenAI $1.10/$4.40 per 1M tokens (in/out); Anthropic $3.00/$15.00 per 1M tokens (in/out)

## Concordance Analysis Architecture
- Multi-arm alignment: load extractions from local, openai_o4_mini_high, anthropic_sonnet_4_6, human_A/B/C/D arms → align by paper_id
- Field-pair scoring (engine/analysis/scoring.py): MATCH/MISMATCH/AMBIGUOUS with fuzzy text matching for free-text fields
- Normalization (engine/analysis/normalize.py): canonical categorical prefix matching, multi-value fields, numeric handling
- Metrics (engine/analysis/metrics.py): Cohen's kappa, percent agreement, field summary statistics with 95% CI
- Reports (engine/analysis/report.py): terminal, CSV, and HTML concordance report generators
- Distribution collapse detection (engine/validators/distribution_monitor.py): post-extraction quality gate, flags COLLAPSED/LOW_VARIANCE categorical fields, minimum 10 papers, runs automatically at end of all extraction pipelines

## Paper 1 Analysis (analysis/paper1/)
- Human workbook import (human_import.py): parse v2 extraction workbooks (.xlsx), validate against codebook, import to human_extractions table
- Consensus derivation (consensus.py): identify ~30 shared papers across human extractors, derive majority-vote gold standard
- Adjudication (adjudication.py): export AMBIGUOUS concordance pairs for human review (HTML/JSON), import decisions
- LLM-as-judge pipeline (judge.py + judge_prompts.py + judge_schema.py + judge_storage.py + judge_loader.py + judge_cli.py)
  - Pass 1: pairwise rating of arm outputs per triple. Gemma3:27b judges each pair (EQUIVALENT / PARTIAL / DIVERGENT) and assigns fabrication risk (low / medium / high). Deterministic per-triple seed = SHA-256(paper_id, field_name, run_id) first 4 bytes mod 2^31.
  - Pass 2: per-arm fabrication verification on medium+high risk triples. Grammar-tightened Pydantic schema enforces `arm_verdicts` cardinality (exactly 3) and `arm_slot: Literal[1, 2, 3]` at generation time via Ollama's `format` parameter. Slot uniqueness remains a post-validator check (grammar cannot express `uniqueItems`). Seed = SHA-256(..., "p2") — distinct from Pass 1.
  - Pass 2 orchestrators: `pass2_smoke.py` (24-triple calibration), `pass2_full.py` (all medium+high risk triples, checkpoint every 100), `pass2_retry_single.py` (diagnostic single-triple retry with raw-capture-before-validation).
  - Reports: `pass1_inspection.py` (Pass 1 cross-tabs), `pass2_branchB_report.py` (descriptive preliminary report — 11 sections, uninterpreted, PI decision gate before audit sampling).
- PI audit v1 — single-run (pi_audit_sampler.py + pi_audit_unblind.py)
  - Balanced-within-stratum sampler: 40 UNSUPPORTED / 40 PARTIALLY_SUPPORTED / 20 SUPPORTED arm-rows; per-arm allocation 13/13/14 (40-row strata) and 7/7/6 (20-row stratum). Deterministic master_seed + SHA-256 per-cell seeds.
  - Fully blinded output: two xlsx workbooks (blinded adjudication + separate unblinding key — hidden sheets would break the blind). Row order randomized (master_seed + 1). Forbidden-string scanner guards against leakage in all columns except `source_text`.
  - Source-text windowing, 5 strategies: `full_text` (paper ≤ 32,767 chars), `pass2_window` (paper > 20K tokens — reproduce Pass 2's window), `arm_span_window` (±500 tokens around arm evidence span), `absence_fallback_head` (absence sentinel — no span), `missing_span_fallback_head` (arm snippet not locatable — degraded context). Strategy surfaced in the Adjudication sheet.
  - `pi_audit_unblind.py`: joins the completed blinded adjudication against the key on `row_id`, fail-fast integrity gate, computes judge-reliability metrics with the PI adjudication as gold standard — confusion matrix (4×3, PI×Gemma), per-class precision (conditional + strict), fabrication-flag PPV, weighted Cohen's kappa (bootstrap CI), per-arm agreement, and `source_window_strategy` correlation. Pure-Python stats (Wilson interval, weighted kappa, seeded bootstrap). Writes branded results xlsx + JSON sidecar; no DB access.
- PI audit v2 — verdict-transition + intra-rater (pi_audit_sampler_v2.py)
  - Audits the *delta* between two Pass 2 runs (old `…20260421T174729Z` vs codebook-aware `…codebook_v2_20260604T042317Z`), joined at arm-row level over the 1,211 shared triples. 200 rows, 4 strata: all 97 UNSUPPORTED→SUPPORTED, 60 seeded-random of 100 UNSUPPORTED→PARTIALLY, all 27 SUPPORTED→UNSUPPORTED, and 16 intra-rater overlap re-drawn from the v1 audit (ordinal-first, dedup vs strata 1–3, carrying the original PI adjudication into the key).
  - Arm values sourced from the disagreement-pairs CSV (exactly what Pass 2 saw — empty cell → `NOT REPORTED` absence claim). Source windowing: full text under a 30,000-char safe cap, else a ±8,000-char window; `source_truncated` is an explicit visible flag (never silent — the v1 lesson). Window anchor precedence: verbatim arm_value → this arm's span → co-field arm span → head; spans position only, never shown as evidence. Neutral `»…«` locator marks a verbatim arm_value occurrence as a finding aid only.
  - Same two-file blinded design as v1; blinded sheet shows only field_name + codebook definition, arm_value, source_text, 4-state adjudication, notes. Structural header allow-list + forbidden-string scan guarantee no arm/verdict/stratum/reasoning/span leakage. Provenance (both run_ids, master_seed, per-stratum SHA-256 seeds, input SHA-256s) in the Metadata sheet.

## Per-Class Evidence Elicitation (engine/elicitation/) — ELICIT-DESIGN-01

The Run 7 extraction design. **Off by default** (`extraction_models.elicitation`, ReviewSpec);
`extract_paper()` dispatches to it when set, so upgrading the engine changes no existing review.

**Mechanism.** Pass 1 sees the paper as numbered sentence units and cites `[Sn]` indices; it never
copies text. The engine materializes verbatim text from the persisted unit map and primes Pass 2
with it. One citation mechanism for all three field classes — the classes differ in what must
ACCOMPANY the citations, not in how evidence is cited.

| class | Pass-1 contract |
|---|---|
| STATED (9) | cite ≥1 unit, then the value |
| INFERABLE (6) | cite ≥1 unit, then a declared inference (1–3 sentences), then the value |
| JUDGMENT (5) | stepwise reasoning where EVERY step cites ≥1 unit or is marked criteria application, then the value |

Any class may instead return the escape token with zero citations.

**Two value tokens, and the distinction is load-bearing.** `escape_token`
(`NO_EVIDENCE_LOCATABLE`) = "no evidence locatable for this field in this paper" — a statement
about the extractor's search, zero citations *by definition*, and a citation alongside it is
itself a violation. `absence_sentinels` (six, incl. `NOT_FOUND`) = "the paper does not report
this" — a claim about the body text, therefore a VALUE, therefore it REQUIRES a citation. A
sentinel with no citation fails the write and enters bounded retry. Intended, not an edge case.

**The codebook is the sole source of field classes** (`field_class` per field), reproduced from
`analysis/provenance/FIELD_CLASSES.md` §2 (`prov-fieldclass-1`) and mirrored in `field_class3.py`.
A three-way pin test asserts codebook == module == document; on disagreement **the codebook is the
copy that is wrong**. Prompt construction hand-lists no field — grep-provable, pinned by test.

**Modules.** `units.py` (ELICIT-01 index space, ported; `MIN_UNIT_TOKENS=3` is a study artifact
adopted provisionally — re-derivation belongs to the parse-quality-gate task) · `classes.py` ·
`contracts.py` (parse-time contract enforcement; closed violation vocabulary, FATAL vs ADVISORY) ·
`materialize.py` · `prompts.py` · `sizing.py` · `pipeline.py`.

**Invariants that must not be quietly relaxed:**
- **No silent repair.** An out-of-range or malformed index is recorded verbatim and FAILS the
  field; it is never dropped so the surviving indices can carry the field.
- **The stored snippet is the engine's, not the model's.** Pass 2's `source_snippet` is
  overwritten with materialized unit text. ⚠ A materialized quote is ANCHORED by construction, so
  **anchored rates from this path are NOT comparable to Run 6's 58.3% or the ~39–43% corrected
  baseline**. Citation validity, contract-violation counts and judge-scored supportedness are the
  measures that carry information.
- **First contiguous run only.** The span carries the first run of consecutive cited units, never
  a join of disjoint ones — joining would manufacture a quote appearing nowhere in the paper.
  Full citation set lives in `record_call(extra=…)` and the per-run unit-map file (no migration).
- **Index lists take bare integers.** The prompt must keep "Use the integer only, not the `[S12]`
  marker" for both field-level and step-level `unit_indices`; dropping it cost a whole smoke run.
  `parse_container`'s `recovered_marker_tokens` branch is the backstop and turns a regression into
  `INDEX_MALFORMED`, never into a valid index.
- **Sizing:** `WORST_RATIO=0.4288` × `INDEX_MARKER_INFLATION=1.141` against the 131,072 ceiling.
  Measured over-prediction is **~2×**, not the ~4% an early commit message claimed. Hard-fail
  before the call; `prompt_eval_count == ceiling` is the only post-hoc truncation tripwire.

## Write-Boundary Fail-Fast (engine/core/citation_guard.py)

Sits beside `completeness.py`. Completeness answers "did we get every field"; this answers "does
each field carry evidence". Mechanism-independent — it reads spans, not prompts.

- **`strict`** (elicited path): every value needs ≥1 validated citation, **sentinels included**.
  Only the two NON-VALUE TOKENS are exempt, and they must carry no evidence at all:
  `escape_token` (`NO_EVIDENCE_LOCATABLE`) and `contract_unmet_token` (`CONTRACT_UNMET`).
  Evidence alongside either is itself a violation, with distinct codes — `ESCAPE_WITH_CITATION`
  is the MODEL contradicting itself, `CONTRACT_UNMET_WITH_CITATION` is the ENGINE contradicting
  itself, and the log must not blur two failures with different culprits.
- **`legacy`** (pre-elicitation prompt): sentinels exempt, because that prompt explicitly
  instructs an empty `source_snippet` for an absence value. The exemption is a property of that
  prompt and disappears with it. Every other value still needs a quote.

Raises `UncitedValueError` before any INSERT. **Two loops, and they answer different questions.**

**Inner — the Pass-1 elicitation loop** (ELICIT-DESIGN-02 Ruling 4, inside
`extract_paper_elicited`): at most **2 attempts**. Attempt 2 re-issues the full elicitation with
an appended typed feedback block naming, per failing field, the violation code, the offending
output (echo capped at 200 chars with a visible `…[truncated]` marker) and what its class
contract requires. Acceptance is **strict inequality on CONTRACT_UNMET counts** — attempt 2
replaces attempt 1 only if strictly fewer fields are unmet; a tie keeps attempt 1. No per-field
mixing across attempts: a composite is an extraction no single response produced, whose evidence
set is internally inconsistent. Both attempts reach telemetry whichever wins, because a retry
that regressed is a measurement. F7 is why the identical-retry policy was wrong here: the failure
is response **content** at temperature 0, not response **shape**.

**Outer — the completeness retry driver** (`extract_paper_with_completeness`, untouched by that
ruling): **one** bounded budget across every pre-write refusal — `IncompleteExtractionError`,
`TerminalStateError` (a subclass of it) and `UncitedValueError` — because separate budgets would
let a paper alternate between them indefinitely. Telemetry outcomes: `contract_retry` /
`contract_exhausted`.

**`Pass1ContractError` no longer exists.** It expressed the paper-level contract refusal that
Ruling 1 replaced with per-field terminal states; an exception nothing can raise is a claim about
the code that is not true, so it was deleted rather than left in the tuple.

## Human-in-the-Loop Review Standard

All human review uses HTML → JSON → import round-trip.

File naming: `{review}_{stage}_{queue|decisions}.{html|json}`
Stages: abstract_adjudication, ft_adjudication, pdf_acquisition, pdf_quality, extraction_audit

Generators:
- engine/adjudication/abstract_adjudication_html.py
- engine/adjudication/ft_adjudication_html.py
- engine/acquisition/pdf_quality_html.py (mode=acquisition | quality_check)
- engine/review/extraction_audit_html.py

Importers auto-detect .json vs .xlsx. Default --file auto-discovers
from naming convention. xlsx retained with --format xlsx for archival.

## Extraction Quality Investigation (analysis/eval/)

A chain of evaluations diagnosing why local extraction quality fell after Run 6.
Each writes a JSONL store under gitignored `data/{review}/eval/{study}/` and a
report under `docs/session-reports/`. **Runners deliberately avoid
`extract_paper()` because it stores** — no eval writes to `review.db`.

| study | question | outcome |
|---|---|---|
| SCHEMA-EVAL-01 | constrained vs unconstrained decoding | found the REGRESSION-01 defect incidentally |
| REGRESSION-01 | Pass 1 returned the answer, not the reasoning | fixed (`9190e41`); anchoring 10.5% → ~38% |
| SCHEMA-EVAL-02 | A/B/C response contract, n=40 | RETAIN_B by the pinned rule; **19.4pp still missing vs Run 6** |
| ADJUD-01 | was the deciding clause meant to catch wording variance? | adjudicated |
| QUALGAP-01 | is the 0.17.7 → 0.21.0 runtime change the cause? | **no** — `HYPOTHESIS_DEAD`, +4.3pp pooled / 0.0pp paired |
| PRIME-01 | how quote-rich is each Pass-1 channel? | drafts ~38–43%, thinking 0.4%; Run 6 draft→anchored rho +0.576 |

**The standing finding.** Run 6 primed Pass 2 from the **content** channel (a
first-draft answer dense with verbatim quotes) because the pre-fix parser's
whole-content fallback was active. Post-fix runs prime from the **thinking**
channel, which quotes the paper almost never. Run 6's 58.3% anchoring was an
artifact of that bug, not a level the pipeline earned. Ollama 0.17.7 already used
the native thinking channel, so the interface never moved at the upgrade — the
account in REGRESSION-01 and SCHEMA-EVAL-02 is wrong about *when*.

**Provenance measure.** The frozen v1.1 ladder (`analysis/provenance/`) classifies
each span ANCHORED / STITCHED / DRIFTED / UNTRACEABLE_* / ABSENCE_*. PRIME-01 adds
a coarser verbatim-8-word-window rate for measuring whole channels; it is pinned by
test against QUALGAP-01's published figures and must not be "tidied".

**Known data gap:** eval runners store `raw_content` = the **Pass-2** response and
`think_chars` = an integer length. Pass-1 text is captured only by QUALGAP-01
(`pass1_content` + `pass1_trace`). No 0.21.0 Pass-1 draft text exists on disk.

## Running
```bash
# Full pipeline
python scripts/run_pipeline.py --spec review_specs/surgical_autonomy_v1.yaml --name surgical_autonomy

# Expanded search screening
python scripts/screen_expanded.py                # all phases
python scripts/screen_expanded.py --screen-only  # primary dual-pass only
python scripts/screen_expanded.py --verify-only  # verification pass only

# PDF acquisition
python -m engine.acquisition.check_oa --review surgical_autonomy --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.acquisition.download --review surgical_autonomy [--retry] [--background]
python -m engine.acquisition.verify_downloads --review surgical_autonomy [--dry-run]
python -m engine.acquisition.pdf_quality_check --review surgical_autonomy --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.acquisition.pdf_quality_import --review surgical_autonomy --input dispositions.json

# Full-text screening
python -m engine.agents.ft_screener --review surgical_autonomy --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.agents.ft_screener ... --screen-only
python -m engine.agents.ft_screener ... --verify-only

# Per-class elicitation smoke (ELICIT-DESIGN-01; writes only its own gitignored scratch DB)
PYTHONPATH=. python -m analysis.eval.elicit_design01.smoke --review surgical_autonomy

# Extraction cleanup (schema transition)
python -m engine.utils.extraction_cleanup --review surgical_autonomy          # dry-run
python -m engine.utils.extraction_cleanup --review surgical_autonomy --confirm # execute

# Post-extraction validation
python -m engine.validators.extraction_validator --review surgical_autonomy

# Ollama pre-flight
python -m engine.utils.ollama_preflight --models qwen3.5:27b gemma3:27b deepseek-r1:32b

# Cloud extraction
PYTHONPATH=. python scripts/run_cloud_extraction.py --arm both --max-cost 25.00
PYTHONPATH=. python scripts/run_cloud_extraction.py --progress

# Distribution monitor
python -m engine.validators.distribution_monitor --review surgical_autonomy --arm local
python -m engine.validators.distribution_monitor --review surgical_autonomy --arm anthropic_sonnet_4_6

# Cloud span backfill (for extractions missing span rows)
PYTHONPATH=. python scripts/backfill_cloud_spans.py --review surgical_autonomy [--dry-run]

# q8 KV cache validation
PYTHONPATH=. python scripts/q8_validation.py
PYTHONPATH=. python scripts/q8_validation_fast.py

# Workflow status
python -m engine.adjudication.advance_stage --review surgical_autonomy --status

# LLM-as-judge (Pass 1 / Pass 2 — Paper 1 concordance validation)
PYTHONPATH=. python -m analysis.paper1.judge_cli --review surgical_autonomy --input AI_TRIPLES \
    --pairs-csv data/surgical_autonomy/exports/disagreement_pairs_3arm.csv \
    --codebook data/surgical_autonomy/extraction_codebook.yaml
PYTHONPATH=. python -m analysis.paper1.pass2_full --review surgical_autonomy \
    --pass1-run-id <pass1_run_id> \
    --pairs-csv data/surgical_autonomy/exports/disagreement_pairs_3arm.csv \
    --codebook data/surgical_autonomy/extraction_codebook.yaml
PYTHONPATH=. python -m analysis.paper1.pass2_retry_single --review surgical_autonomy \
    --run-id <run_id> --paper-id <pid> --field-name <field> \
    --pairs-csv ... --codebook ...
PYTHONPATH=. python -m analysis.paper1.pass2_branchB_report --review surgical_autonomy \
    --run-id <run_id> --pairs-csv ... --codebook ... \
    --run-log analysis/paper1/logs/<log> --out-dir artifacts/paper1

# PI audit sampler (balanced, fully blinded, n=100)
PYTHONPATH=. python -m analysis.paper1.pi_audit_sampler --review surgical_autonomy \
    --out-dir artifacts/paper1/pi_audit
# Regeneration with provenance metadata:
PYTHONPATH=. python -m analysis.paper1.pi_audit_sampler --review surgical_autonomy \
    --supersedes <prior-filename> --regeneration-reason "<why>"

# PI audit unblinding + judge-reliability scoring (post-adjudication)
PYTHONPATH=. python -m analysis.paper1.pi_audit_unblind \
    --completed artifacts/paper1/pi_audit/<workbook>_COMPLETED.xlsx \
    --key       artifacts/paper1/pi_audit/pi_audit_key_<ts>.xlsx \
    --out-dir   artifacts/paper1/pi_audit

# PI audit v2 sampler (verdict-transition + intra-rater, n=200, fully blinded)
PYTHONPATH=. python -m analysis.paper1.pi_audit_sampler_v2 --review surgical_autonomy \
    --codebook data/surgical_autonomy/extraction_codebook.yaml \
    --out-dir artifacts/paper1/pi_audit_v2

# Extraction-quality evaluations (analysis/eval/) — all write to gitignored eval stores
PYTHONPATH=. python -m analysis.eval.run_local_abc --review surgical_autonomy [--resume]
PYTHONPATH=. python -m analysis.eval.analyze_schema_eval2 --review surgical_autonomy

# QUALGAP-01 runtime A/B (needs a second Ollama on :11435; --probe does pre-flight only)
PYTHONPATH=. python -m analysis.eval.run_qualgap01 --review surgical_autonomy --probe
PYTHONPATH=. python -m analysis.eval.run_qualgap01 --review surgical_autonomy [--resume]
PYTHONPATH=. python -m analysis.eval.analyze_qualgap01 --review surgical_autonomy

# PRIME-01 Pass-1 channel quote-richness (offline, zero model calls; ~15 min)
PYTHONPATH=. python -m analysis.eval.analyze_prime01 --review surgical_autonomy

# Disarm the last-resort Ollama restart for a harness on another server
EVIDENCE_ENGINE_NO_OLLAMA_RESTART=1 PYTHONPATH=. python -m <harness>

# Test suite
python -m pytest tests/ -v                                        # all tests (nightly)
python -m pytest tests/ -v -m "not network and not ollama"        # offline only
python -m pytest tests/ -v -m "not network and not ollama and not integration"  # standard gate (~3m25s)
```

The standard gate is the third form. All tiers run under the `tests/conftest.py`
service-call fence — see Ops Invariants. Verify a suite run touched nothing:

```bash
systemctl show ollama --property=ExecMainStartTimestamp --property=NRestarts
journalctl -u ollama --since "<window start>" | grep -E "Started|Stopping|Stopped"
```

## Architecture Docs
See `docs/architecture/` — 6-file code-audited reference (README, pipeline, models, state-machine, workflow, modules).
