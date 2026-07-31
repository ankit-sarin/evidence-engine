# Session state — 2026-07-29 → 07-30

Machine: DGX (spark-59e4).
**evidence-engine** — working tree clean, HEAD **`f5e28d8`**.
**inference-determinism** — working tree clean, HEAD **`dc39b75`**.
No tmux sessions running. Experiment flock `~/.ollama_experiment.lock` **not held**.
Suite at exit: **1456 passed, 15 deselected** (`-m "not network and not ollama and not integration"`).

---

## 1. Completed tasks

### evidence-engine — commits from `9b0da41` forward (oldest first)

| commit | task | what it did |
|---|---|---|
| **`9b0da41`** | **FIELDCLASS-01** | Three-way field classification (STATED / INFERABLE / JUDGMENT) replacing the binary extractive/interpretive axis, + census recount on the same 11,017 spans. `FIELD_CLASSES.md` criteria hashed before assignment (`3bcffc57…`). |
| **`97179b4`** | **JUDGE-RESTATE-01** | Restated Pass 2 judge verdicts against the frozen v1.1 taxonomy. Headline 34.0% → **12.1%**; 164 judged arm-rows found to have no stored span, 160 of them SUPPORTED. |
| **`db93ce6`** | **SPANLOSS-01** | Autopsy of the single-span collapse. Verdict **EXTRACTION defect on all 21**, zero storage loss; 17 openai + 2 local papers; ~9% stochastic, no clustering. |
| **`a90bbfe`** | **INSTRUMENT-01** (a) | Telemetry: `finish_reason` / `stop_reason` / `done_reason` captured verbatim per provider, plus pre-parse raw content. |
| **`ee6a82b`** | **INSTRUMENT-01** (b) | Write-boundary completeness guard on all three arms, codebook-derived field set, bounded 3-attempt identical-request retry, salvage demoted to a parse aid. |
| **`c8fcb03`** | **SCHEMA-EVAL-01** | A/B schema harness. Local verdict later **voided** (broken baseline). Cloud findings stand: OpenAI strict works 5/5; Anthropic forced tool ⊕ extended thinking is a hard API incompatibility. |
| **`9190e41`** | **REGRESSION-01** | **Thinking-channel fix.** Native `message.thinking` first, legacy tags as a logged branch, silent whole-content fallback removed (raises). `think=` explicit per Review Spec. Telemetry → schema **-2**. |
| **`8486f89`** | **REGRESSION-01** (smoke) | Post-fix smoke, 3 papers: anchored 0/60 → 21/60; p466 reproduced the Run 6 quote verbatim. |
| **`f5e28d8`** | **SCHEMA-EVAL-02** | A/B/C local response-contract harness, n=40 × 3 = **120 extractions, 0 errors, 16.1 h**. |

### inference-determinism

| commit | task | what it did |
|---|---|---|
| **`dc39b75`** | **DETFIX-01** | Fixed the mirrored parse (same defect, named source commit `9190e41`), `think=` explicit at all 8 call sites + source-level test, `CONTAMINATED.md` marking three defect sets. Verification probe PASS. |

---

## 2. Open decisions

### (a) Local response contract — **RETAIN_B by rule; adoption of C pending adjudication**

The pinned decision rule returned **RETAIN_B** (current production array schema). C was rejected
on a **single clause**: C-vs-B value disagreement **11.4% > 10%**. C was otherwise the best
condition on every measure.

| condition | anchored | no-basis | guard pass | would-retry | median latency |
|---|---:|---:|---:|---:|---:|
| A unconstrained | 35.3% | 28.6% | 80.0% | 8 | 432 s |
| B array schema (production) | 37.8% | 27.8% | 85.0% | 6 | 456 s |
| **C required slots** | **38.6%** | **23.4%** | **100.0%** | **0** | 441 s |

**The adjudication needed:** the 11.4% decomposes to **free-text 25.2% / categorical 2.2%** —
B and C agree on 97.8% of categorical values, measured by exact normalized string match. A-vs-B
is already 9.2% for two conditions differing only by whether a schema is attached, so the 10%
threshold sits inside the natural wording-variance band.

> **Question for the architect: was the disagreement clause intended to catch substantive value
> change, or any string difference?** If substantive → C wins on every measure the study weighed,
> and C is the only contract that can express cardinality (B's array schema has no `minItems`,
> which is why it could not have prevented SPANLOSS-01). If any-string → B stands as ruled.

The pre-registered rule was **not** overridden. Nothing was cut over.

### (b) 19.4 pp anchored gap vs Run 6 — **diagnostic pending, gates Run 7**

Condition B (= production contract) anchors **38.8%** vs Run 6's **58.2%** on the same 37 papers.
Paired: **25/37 papers worse by >3pp**, median **−15.0 pp**; several collapse outright
(549 75%→0%, 522 70%→0%, 470 60%→0%).

Ruled out with evidence: the missing `_validate_and_retry_snippets` (fires only on ellipsis
snippets, 8.3% of spans); the response contract (all three conditions inside a 3.3 pp band);
model weights (blob unchanged since 2026-01-19, before Run 6); sample composition (paired, same
papers, same parsed text).

Remaining hypothesis — **untested**: something in the Ollama **0.17.7 → 0.21.0** runtime change
*beyond* the thinking-channel interface, plausibly that `think=True` yields different pass-1
reasoning than inline `<think>` did, so pass 2 is primed differently even though it is now
primed correctly.

> **Recommendation on the record: raise this as its own task before Run 7 is scheduled.** The
> contract choice is worth ~3 pp; this gap is worth ~19 pp. Running Run 7 at 38% anchored when
> Run 6 achieved 58% would produce a corpus measurably worse than the one already in hand.

---

## 3. Key state

| item | value |
|---|---|
| Ollama | **0.21.0** (installed 2026-04-19, replacing 0.17.7 — the regression boundary) |
| `think=` | **explicit on every call**, declared per pass in the Review Spec (`extraction_models.pass1_think=True`, `pass2_think=False`); never inherits a version default |
| Thinking parse | native `message.thinking` → `legacy-tags` → **raise**. No silent fallback. `parse_branch` in telemetry |
| Telemetry schema | **`extraction-telemetry-2`** — adds `thinking_present`, `thinking_chars`, `parse_branch` |
| Completeness guard | live on all three arm write paths; codebook-derived field set; 3-attempt identical-request retry; **no partial writes** |
| Production local contract | **still B** (`format=ExtractionOutput.model_json_schema()`) — unchanged, no cutover |
| Provenance taxonomy | **frozen at v1.1** (`prov-def-1.1`, absence patterns `prov-absence-1`) |
| Field classification | `prov-fieldclass-1`, three-way, **PROPOSED — architect ratification required** |
| Production extractions | **none since Run 6** (2026-03-15..18, on Ollama 0.17.7). Everything since is eval-store only |
| Run 7 | **gated** on fix-phase completion — contract adjudication (2a) + restoration diagnostic (2b) |
| Cloud arms | OpenAI strict schema validated but **not cut over**; Anthropic forced tool ⊕ thinking incompatible |
| Determinism study | all D0-S2 runs marked contaminated; Arm S + Arm P attempt 3 code-ready, **deliberately unscheduled** until the local contract settles |

### Known-open items not yet tasked

- **`NOT_FOUND` escape values** — lands on whichever contract wins. C's higher empty-snippet
  count (110 vs 88) is the required-slot trap in visible form.
- **Re-extraction of the 17 collapsed openai papers** — values were never produced; nothing is
  recoverable from logs. 320 pairs-CSV cells are empty and were scored MISMATCH, inflating
  24.9% of local-vs-openai and 34.0% of openai-vs-sonnet disagreements.
- **Two schema-violating spans** in `evidence_spans` (`Title` p415, `field_1` p719).
- **`~/scripts/` unversioned** — three cron-driven load-bearing scripts, no history or backup.
- **`ReviewDatabase(path)` footgun** — a wrong argument silently `mkdir`s a plausible-looking
  empty review tree instead of failing (stray `data/data/…` exists from this).

---

## 4. Report pointers

**Scratchpad files are not committed** — no `scratchpad/` exists in either repo and none is
tracked in git. All reports live under the session scratchpad, and they are **split across two
directories** because the session restarted mid-SCHEMA-EVAL-02.

Base A = `/tmp/claude-1000/-home-ankitsarin-projects-evidence-engine/711c6549-f6df-4332-98a8-16c5167ba108/scratchpad/`
Base B = `/tmp/claude-1000/-home-ankitsarin-projects-evidence-engine/02b601c9-af5f-4448-83d3-0d7844830555/scratchpad/`

### The eight tasks listed in §1

| report | base |
|---|---|
| `FIELDCLASS-01_report.md` | A |
| `JUDGE-RESTATE-01_report.md` | A |
| `SPANLOSS-01_report.md` | A |
| `INSTRUMENT-01_report.md` | A |
| `SCHEMA-EVAL-01_report.md` | A |
| `REGRESSION-01_report.md` | A |
| **`SCHEMA-EVAL-02_report.md`** | **B** |
| `DETFIX-01_report.md` | A |

### Earlier reports from the same session (context for the above)

All in base A: `DIAG-UNANCHOR-01_report.md`, `DIAG-OPTSET-01_report.md`,
`DIAG-VISION-01_report.md`, `TAXONOMY-CENSUS-01_report.md`, `TAXONOMY-CENSUS-02_report.md`,
`TAXONOMY-CENSUS-03_report.md`, `OPS-GUARD-01_report.md`, plus
`FIELD_CLASSES.criteria-only.md` (the pre-registration artifact, SHA-256 `3bcffc57…`).

**Caution:** `/tmp` scratchpads are not durable across a reboot. If these fifteen reports matter
beyond this session, copy both directories somewhere permanent — they are the only narrative
record of the diagnostic chain; the commits carry the code but not the reasoning.

### Durable artifacts in the repos (survive independently)

- `analysis/provenance/DEFINITIONS.md` — taxonomy pre-registration (v1.1)
- `analysis/provenance/FIELD_CLASSES.md` — field classification criteria + assignments
- `~/projects/inference-determinism/CONTAMINATED.md` — contamination markers
- Gitignored under `data/surgical_autonomy/`: `eval/schema_eval2/` (120 raw extractions +
  `analysis_summary.json`), `eval/schema_eval/`, `analysis/provenance/` census outputs,
  `telemetry/extraction_calls.jsonl`
