# DISCOVERY-01 Part B — execution and screening-seam facts (D2-1 … D2-16)

**Date:** 2026-09-20. **Task type:** read-only diagnostic. No engine code changed, no fix, no
model call, no write to `review.db`.
**Harness session:** `ba16c4af-2c55-4156-81b6-aad32200662d`.
**HEAD at write:** `7e09de3` (Part A's docs-only commit, pushed; tree clean, level with origin).
**Part B measurement window:** 05:00–05:40 UTC.

**Trust statement.** Every payload in D2-1 was produced by running the real code path with a
stub transport substituted for `ollama.Client.chat`; nothing contacted the Ollama service, and
the input-fit guard's ceiling probe was stubbed so no `/api/tags` or `systemctl` call was made
either. Every count is from a `mode=ro` connection (never `immutable=1`) or from the committed
AST inventory. Where a measurement contradicts a v49 §3.1 row or a stated expectation, **both
values are given and neither is resolved.**

**Tags.** **MEASURED** = produced here by running code or a query. **READ** = quoted from a file
at this HEAD. **INFERRED** = a reading, a pairing or a cause.

**Anchors are quoted content**, never line numbers.

---

## Opening table

| item | one-line answer | EXPECTED |
|---|---|---|
| **D2-1** | 12 call sites captured. Both abstract calls send exactly `{"temperature": 0}`, `think=False`, a JSON schema, no seed, no `num_ctx`. **Extraction sends `temperature: 0` and `deepseek-r1:32b` from module constants; `spec.extraction_models.extractor` and `.temperature` reach no call.** No resolved configuration is persisted per run anywhere. | **confirmed** for the two abstract calls; **R5 confirmed** for extraction |
| **D2-2** | The live spec declares **no `extraction_models` block at all**, so `elicitation` is the pydantic default `False` → the next run executes the **legacy two-pass** path. `cloud_models` is `None`, which **does not disable the cloud arms** — the `--arm` CLI flag does, and both extractors fall back to hardcoded models and prices. | — |
| **D2-3** | `run_pipeline` connects 6 stages (search, screen, parse, extract, audit, export). **FT screening, all four acquisition steps, all three adjudication round-trips, cloud extraction, the distribution monitor and concordance are not connected.** The extraction precheck queries `PARSED` only — **FT_ELIGIBLE is ignored (R5 confirmed)**. **No entry point qualifies as "supported" for the 12-stage pipeline.** | **R5 confirmed** |
| **D2-4** | `CORPUS_STATUSES` = `FT_ELIGIBLE, EXTRACTED, AI_AUDIT_COMPLETE, HUMAN_AUDIT_COMPLETE`. **`EXTRACT_FAILED` is outside the corpus.** | **R5 confirmed** |
| **D2-5** | **42 glob-based resolver sites in 37 modules**, plus 3 DB-driven — not seven. All **6 engine sites are lexical** (`sorted(..., reverse=True)`); correct numeric ordering exists only under `analysis/`. Fixture: `_v9` beats `_v10` (**R3 confirmed**). **NULL `parsed_text_path`: 350 rows / 350 papers — not 16; corpus papers resolving to NULL: 0.** | **v49 CONTRADICTED twice** (7 → 42; 16 → 350/0) |
| **D2-6** | Both target states **exist** in the live schema (two tables; `absence_pattern`; `absence_pattern_version`; the widened CHECK admitting `ABSENCE_CLAIM`, with 150 rows using it). **No applied-receipt anywhere**: no migration table, `PRAGMA user_version = 0`, and `_run_migrations()` never runs the numbered modules. | **I3 confirmed** |
| **D2-7** | **Two copies, still (I4 confirmed)**: `screener.py::run_screening` and `scripts/screen_expanded.py::run_screen_phase`, logically identical. `screener.py` is on the supported path. **11 code sites** would change to route the verifier onto primary OUT, **plus two frozen rendered surfaces** (R2, and the `abstract_verifier` stage policy). | **I4 confirmed** |
| **D2-8** | Format block requires `decision`, `rationale`, `confidence` and **nothing else**. **No column anywhere in the six screening tables can hold a criterion id or a character offset.** **`confidence` is required of the model at the abstract stage and then discarded — neither abstract table has the column.** `reason_code` exists only at FT. | **v49 §3.1 PARTLY CONTRADICTED** — "no reason_code" confirmed; "confidence" is *not* stored |
| **D2-9** | **No screening row references `review_runs`.** No `run_id` column, no FK, in any of the six tables. `review_runs` has 10 columns, all hashes and timing; `log` is `'[]'` in all 6 rows. | — |
| **D2-10** | **Six** `analysis/eval/run_*.py`, not seven. One is live product-adjacent harness code (`run_screen2f`), four are frozen experiment code with committed reports, one (`run_local_ab`) is superseded by `run_local_abc`. **Nothing is an exact duplicate; nothing is safe to delete without a ruling.** | **v49 CONTRADICTED** (7 → 6) |
| **D2-11** | `records_identified` is `SELECT source, COUNT(*) FROM papers GROUP BY source` — **reconstructed from the surviving corpus**. `duplicates_removed` is the **literal `0`**, commented "tracked externally by dedup module". The dedup result is never persisted, and `add_papers` performs a **second, uncounted** dedup by PMID. | — |
| **D2-12** | 12 stages. **2 have no auto-marker at all** (manual only); **7 evaluate their declared condition or a close proxy**; **3 mark complete with no condition evaluated** — `AUDIT_QUEUE_EXPORTED`, `FULL_TEXT_ADJUDICATION_COMPLETE` and **`AUDIT_REVIEW_COMPLETE`**. **All three `check_*_gate` predicates have zero non-test callers.** | **C1 confirmed, and two siblings found** |
| **D2-13** | **YES.** `build_disagreement_rows` writes a triple only `if not any_disagree: continue`; the CSV is the judge's entire universe (`load_ai_triples_csv`). CSV 2,267 rows; Pass 1 attempted **2,266**. The census covers **3,802** cells — **1,535 cells (40.4%) the judge never saw**, excluded because `score_pair` called all three pairs MATCH. The 63 kappas rest on `metrics.py` **and** `scoring.py`; FIELDCLASS-01's 12.1% / 33.7% do **not**. | **C2 confirmed by the code path** |
| **D2-14** | Reuse key is `(paper_id, codebook_hash)` **and nothing else**. Fixture: parsed text changed by one character → `skipped: 1`, prior extraction reused, `extract_paper` never called. A whole new `_v2` parse → also skipped. | **v49 §4.5 confirmed** |
| **D2-15** | 3 adjudication tables + 5 mutated columns + `workflow_state` + the HTML/JSON/xlsx files. **`audit_adjudication` and `abstract_screening_adjudication` are write-only — nothing in the codebase reads them**; `ft_screening_adjudication` is read once, for a PRISMA COUNT. **YES, two readers can disagree today** — three demonstrated pairs. | — |
| **D2-16** | **7** `DEFAULT_REVIEW` constants (v49's number is exactly right), but **9** review-id constants and **35** literal sites in **24 files**. The two that matter are `engine/analysis/normalize.py::_FALLBACK_REVIEW_ID` (D1-9) and **a second engine site the expectation did not name: `engine/analysis/report.py::_get_tier_map`**. Both are unreachable by a resolver-supplied id. | **expectation CONTRADICTED as warned** — 7 is right for `DEFAULT_REVIEW` and is the wrong denominator |

---

## D2-1 — Effective configuration at every Ollama call site

**Method** — MEASURED. `engine.utils.ollama_client._client` replaced by a capture stub;
`_check_input_fits` and `_check_input_was_read` stubbed to no-ops so the ceiling probe made no
service call. Each site driven through its real function with minimal real inputs.

| # | site | model | temp | seed | num_ctx | num_predict | think | format | keep_alive |
|---|---|---|---|---|---|---|---|---|---|
| 1 | abstract primary (`screener.screen_paper`) | `qwen3:8b` | `0` | absent | absent | absent | `False` | JSON schema | absent |
| 2 | abstract verifier (`screener.screen_paper`, role=verifier) | `gemma3:27b` | `0` | absent | absent | absent | `False` | JSON schema | absent |
| 3 | FT primary (`ft_screener.ft_screen_paper`) | `qwen3:32b` | `0.0` | absent | absent | absent | `False` | JSON schema | absent |
| 4 | FT verifier (`ft_screener.ft_verify_paper`) | `gemma3:27b` | `0.0` | absent | absent | absent | `False` | JSON schema | absent |
| 5 | extractor pass 1 | `deepseek-r1:32b` | `0` | absent | absent | absent | `True` | **absent** | absent |
| 6 | extractor pass 2 | `deepseek-r1:32b` | `0` | absent | absent | absent | `False` | JSON schema | absent |
| 6b | extractor `_retry_snippet` | `deepseek-r1:32b` | `0` | absent | absent | absent | `False` | **absent** | absent |
| 7 | auditor `semantic_verify` | `gemma3:27b` | `0` | absent | absent | absent | `False` | JSON schema | absent |
| 7b | same, caller passes `ollama_options={"num_ctx": 32768}` | `gemma3:27b` | `0` | absent | **32768** | absent | `False` | JSON schema | absent |
| 8 | vision fallback (`pdf_parser.parse_with_vision`) | `qwen2.5vl:7b` | `0` | absent | **8192** | **2048** | **absent** | **absent** | absent |
| 9 | PDF quality check (`pdf_quality_check._classify_page`) | `qwen2.5vl:7b` | `0` | absent | absent | absent | **absent** | **absent** | absent |
| 10 | elicitation Pass 1 (`elicitation.pipeline.run_pass1`) | `deepseek-r1:32b` | `0` | absent | absent | absent | `True` | **absent** | absent |
| 11 | judge Pass 1 (`judge.run_pass1`) | `gemma3:27b` | `0.0` | **3019640777** | **8192** | absent | `False` | JSON schema | absent |
| 12 | judge Pass 2 (`judge.run_pass2`) | `gemma3:27b` | `0.0` | **2536383396** | **24576** | absent | `False` | JSON schema | absent |

**`keep_alive` is never sent, at any site.** MEASURED.

### Where each option comes from

| site | model from | temperature from | think from | other |
|---|---|---|---|---|
| 1, 2 | `spec.screening_models.primary` / `.verification` | **literal `0` in `screener.py`** | **literal `False`** | `format` = `ScreeningDecision.model_json_schema()`, a literal |
| 3, 4 | `spec.ft_screening_models.primary` / `.verifier` | **`spec.ft_screening_models.temperature`** | **`spec.ft_screening_models.think`** | FT primary's schema is widened by `render.with_reason_code_vocabulary(..., spec.eligibility)` |
| 5, 6, 6b | **module constant `extractor.MODEL = "deepseek-r1:32b"`** | **literal `0`, three sites** | `spec.extraction_models.pass1_think` / `.pass2_think` (the only spec values that reach extraction) | — |
| 7 | caller arg, else module constant `DEFAULT_AUDITOR_MODEL = "gemma3:27b"` | literal `0`, **merged under** the caller's `ollama_options` | literal `False` | `options={**{"temperature": 0}, **(ollama_options or {})}` — the only merge point in the engine |
| 8 | `spec.pdf_parsing.vision_model` when `parse_pdf` resolves it, else `_VISION_MODEL` | literal `0` | not sent | `num_predict`/`num_ctx`/`wall_timeout` from `spec.pdf_parsing.*` with module-constant fallbacks |
| 9 | `spec.pdf_quality_check.ai_model` at the caller; signature default `"qwen2.5vl:7b"` | literal `0` | not sent | `max_retries=0` literal; `wall_timeout` = `cfg.timeout` |
| 10 | **module constant `elicitation.pipeline.MODEL`** | literal `0` | caller arg | — |
| 11, 12 | signature default `judge.DEFAULT_MODEL = "gemma3:27b"` | `DEFAULT_TEMPERATURE = 0.0` | literal `False` | **Pass 1 `num_ctx` = `DEFAULT_NUM_CTX` (8192); Pass 2's is a *signature* default 24576** — two different constants for the same nominal setting |

**R5 CONFIRMED, exactly.** READ, `engine/agents/extractor.py`:

```python
MODEL = "deepseek-r1:32b"
```

and, at all three call sites, `options={"temperature": 0}`. **`ExtractionModels.extractor` and
`ExtractionModels.temperature` are declared in the spec model and reach no Ollama call.** The
external review's "a structured extraction call using deepseek-r1:32b at temperature zero despite
different requested settings" is the behaviour at HEAD.

**Expectation for the two abstract screening calls (v49 §3.1): CONFIRMED.**
`{"temperature": 0}`, `think` false, `format` a schema, no seed, no `num_ctx` — measured exactly.

### Where the resolved configuration is persisted per run

**MEASURED, read-only.** `review_runs` has ten columns:
`id, review_spec_hash, screening_hash, extraction_hash, started_at, completed_at, status, log,
codebook_hash, codebook_sha256`. **No model name, no options, no digest.** `log` is `'[]'` in all
six rows, so the stage stats `run_pipeline` builds are not persisted either.

What *is* persisted, elsewhere and per artifact rather than per run:

| where | what |
|---|---|
| `extractions.model`, `.model_digest`, `.auditor_model_digest`, `.codebook_hash`, `.codebook_sha256` | the extraction model name and digest |
| `evidence_spans.auditor_model` | the auditor model name (all 3,760 rows: `gemma3:27b`) |
| `cloud_extractions.model_string`, `.prompt_text`, token counts, `.cost_usd` | the cloud arm's model and its literal prompt |
| `abstract_screening_decisions.model`, `abstract_verification_decisions.model`, `ft_screening_decisions.model`, `ft_verification_decisions.model` | the screening model name |
| `judge_runs.judge_model_name`, `.judge_model_digest`, `.codebook_sha256`, `.run_config_json` | the only place a **run-level config blob** exists |
| `provenance_census_runs.*` | the census's own parameters |

**No temperature, seed, `num_ctx`, `think` or `format` value is recorded anywhere for any
non-judge run.** INFERRED consequence: the configuration a completed run used is not
reconstructible from the database; it is only inferable from the code at the commit that ran.

---

## D2-2 — Elicitation and cloud flags

**The live spec declares no `extraction_models` block.** MEASURED — the YAML's top-level keys are
`review_id, title, version, authors, date, prospero_id, pico, search_strategy, unpaywall_email,
institutional_proxy_pattern, screening_models, ft_screening_models, low_yield_threshold,
pdf_quality_check, eligibility`. The loaded value is therefore entirely the pydantic default:

```
extraction_models  declared-in-YAML=False  value={'extractor': 'deepseek-r1:32b',
  'pass1_think': True, 'pass2_think': False, 'temperature': 0.0, 'elicitation': False}
```

READ, `engine/core/review_spec.py`:

```python
    elicitation: bool = Field(
        default=False,
        description=(
            "Pass 1 elicits sentence-unit citations under per-class contracts and "
            "Pass 2 is primed with the materialized evidence (ELICIT-DESIGN-01). "
            "Defaults OFF: the design is smoke-gated and Run 7 flips it "
            "deliberately, so no existing review changes behaviour by upgrading."
        ),
    )
```

The branch it selects, READ, `engine/agents/extractor.py::extract_paper`:

```python
    if getattr(getattr(spec, "extraction_models", None), "elicitation", False):
        from engine.elicitation.pipeline import extract_paper_elicited

        return extract_paper_elicited(
```

**The next run would execute the legacy two-pass path**, not elicitation — the `if` is false.
`pass1_think=True` / `pass2_think=False` are the only spec values that reach it.

**Cloud.** `cloud_models` is `None` (`Optional[CloudModels]`, undeclared). **That does not disable
anything.** `scripts/run_cloud_extraction.py` selects arms from the `--arm {openai,anthropic,both}`
CLI flag, and each extractor falls back to hardcoded values when the spec block is absent — READ,
`engine/cloud/anthropic_extractor.py`: `# Sonnet 4.6 pricing defaults (March 2026) — used when
spec has no cloud_models`. The only consumer of `spec.cloud_models` outside the two extractors is
`engine/exporters/methods_section.py`, which names the arms in the methods text.

What each enabled arm would send off-box, per paper — READ:

| arm | model | payload |
|---|---|---|
| `openai_o4_mini_high` | `o4-mini-2025-04-16`, `reasoning_effort=high` | a system message plus the codebook-driven extraction prompt as the user message; the prompt embeds the **paper's full parsed text** |
| `anthropic_sonnet_4_6` | `claude-sonnet-4-6`, `max_tokens=16000`, extended thinking | the same prompt, streamed |

`cloud_extractions.prompt_text` stores the literal prompt that was sent, so what left the box is
recoverable after the fact.

---

## D2-3 — The runner

**Stages `scripts/run_pipeline.py` connects, in order** — READ:

```python
STAGES = ("search", "screen", "parse", "extract", "audit", "export")
```

with two gates interleaved: an **adjudication gate** before `parse`/`extract`/`audit`/`export`
(`is_adjudication_complete`) and an **audit review gate** before `export`
(`is_audit_review_complete`).

| # | stage | what it calls | selector |
|---|---|---|---|
| 1 | search | `search_pubmed` → `search_openalex` → `deduplicate` → `db.add_papers` | — |
| 2 | screen | `engine.agents.screener.run_screening` | `INGESTED` |
| — | *adjudication gate* | `is_adjudication_complete(db._conn)` | `workflow_state` |
| 3 | parse | `parse_all_pdfs` | `PDF_ACQUIRED` |
| 4 | extract | `run_extraction` | **`PARSED`** |
| 5 | audit | `run_audit` | `EXTRACTED` |
| — | *audit review gate* | `is_audit_review_complete(db._conn)` | `workflow_state` |
| 6 | export | `export_all` | `min_status="AI_AUDIT_COMPLETE"` (the `export_all` default) |

**Stages that exist in `engine/` and are NOT connected** — MEASURED from the runner's import list
and the inventory's entry points:

* **Full-text screening, entirely.** `engine/agents/ft_screener.py` is not imported by
  `run_pipeline`. Both FT stages (5 of the documented 12) run only from that module's own CLI.
* **All four acquisition steps**: `check_oa`, `download`, `verify_downloads`,
  `pdf_quality_check` / `pdf_quality_import`. `_stage_parse` says so in its own log line —
  *"PDF acquisition is manual for v1."*
* **All three adjudication round-trips**: abstract (`screening_adjudicator`), full-text
  (`ft_screening_adjudicator`), audit (`audit_adjudicator`). The runner *reads* their workflow
  stages and blocks on them; it never runs them.
* **Cloud extraction** (`scripts/run_cloud_extraction.py`), the **distribution monitor**
  (documented as "runs automatically at end of all extraction pipelines" — not from here), and
  **concordance** (`engine/analysis/concordance.py`'s own `main`).

**Resume prerequisites.** `--skip-to <stage>` sets `start_idx = STAGES.index(skip_to)`; every
stage at or after that index runs. Resuming at or before `export` re-evaluates the audit gate;
resuming at `parse` or later re-evaluates the adjudication gate. Within a stage, resume is
per-runner: `run_screening` and `run_ft_screening` keep JSONL checkpoints beside the DB;
`run_extraction` resumes through the `(paper_id, codebook_hash)` skip (D2-14); `parse_pdf`
short-circuits on an unchanged `pdf_hash`.

**The extraction precheck ignores FT_ELIGIBLE — R5 CONFIRMED.** READ:

```python
    parsed = db.get_papers_by_status("PARSED")
    if not parsed:
        logger.info("No papers with status PARSED — skipping extraction.")
```

INFERRED, and material: `FT_ELIGIBLE` is the status a paper reaches *after* full-text screening
admits it, and it is the first member of `CORPUS_STATUSES` (D2-4). A review that runs FT screening
therefore has its corpus sitting in a status the runner's extract stage cannot see. **MEASURED:
the live review has 0 papers at `FT_ELIGIBLE` today, so this has not yet bitten.**

**The one supported entry point: none qualifies.** `scripts/run_pipeline.py` is the only
end-to-end runner and it connects **6 of the 12 documented stages**, omits full-text screening
altogether, and cannot advance a paper that FT screening admitted. The live review's own history
shows the gap: 366 `ft_screening_decisions` rows exist, and `run_pipeline` cannot have written
one. INFERRED: the supported unit today is the **stage CLI**, not a pipeline.

---

## D2-4 — Corpus predicate

READ, `engine/core/corpus.py`:

```python
CORPUS_STATUSES: tuple[str, ...] = (
    "FT_ELIGIBLE",
    "EXTRACTED",
    "AI_AUDIT_COMPLETE",
    "HUMAN_AUDIT_COMPLETE",
)
```

```python
def is_corpus_member(status: str | None) -> bool:
    """True if `status` places a paper in the review corpus.

    `None` (no such paper) is not a member, so a missing id fails closed.
    """
    return status in CORPUS_STATUSES
```

**Include set:** those four. **Exclude set:** everything else — the module declares a positive
list, not an exclusion list, so every other lifecycle status is outside by default.

**`EXTRACT_FAILED` is outside the corpus. R5 CONFIRMED.** The module's own docstring explains why
the set is declared rather than derived: the forward closure from `FT_ELIGIBLE` in
`ALLOWED_TRANSITIONS` is nine statuses and "drags in `EXTRACT_FAILED`, `FT_FLAGGED`,
`FT_SCREENED_OUT` and `REJECTED`". `tests/test_corpus_authority.py` pins `STATUSES` whole.

---

## D2-5 — Parsed-text resolvers

**MEASURED, by AST-adjacent census over `engine/`, `scripts/`, `analysis/` (no tests): 42
glob-based resolver sites in 37 modules, plus 3 DB-driven.** **v49's "seven" is contradicted; both
values are recorded and neither is resolved.** One of the 42 is a docstring line
(`analysis/eval/analyze_capture01.py`, describing the convention rather than executing it).

| area | sites | modules | ordering |
|---|---:|---:|---|
| `engine/` | 6 | 5 | **all lexical** |
| `scripts/` | 8 | 7 | all lexical |
| `analysis/` | 28 | 25 | 19 numeric, 9 lexical |

**The 6 engine sites** — READ:

| module | function | rule |
|---|---|---|
| `engine/agents/extractor.py` | `run_extraction` | `sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)` |
| `engine/agents/auditor.py` | `run_audit` | same |
| `engine/agents/ft_screener.py` | `_load_parsed_text` | same |
| `engine/cloud/base.py` | paper-text load | same |
| `engine/review/human_review.py` | ×2 (CSV import; snippet verification) | same |
| — | — | — |

**The one on the supported path** is ambiguous and that is itself the answer: **two** engine
resolvers sit under `run_pipeline` — `extractor.py::run_extraction` (stage 4) and
`auditor.py::run_audit` (stage 5) — and a third, `engine/cloud/base.py`, serves the cloud arms.
All three use the same lexical rule, so they agree with each other and disagree with
`analysis/provenance/census.py`.

**The version-ordering rule, quoted** — READ, `engine/agents/extractor.py`:

```python
        md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)
```

against the only correct one, READ, `analysis/provenance/census.py`:

```python
def parsed_text_path(review_dir: Path, paper_id: int) -> Path | None:
    files = sorted(
        (review_dir / "parsed_text").glob(f"{paper_id}_v*.md"),
        key=lambda p: int(p.stem.rsplit("_v", 1)[1]),
    )
    return files[-1] if files else None
```

**Fixture — MEASURED.** A temp directory holding `7_v1.md`, `7_v2.md`, `7_v9.md`, `7_v10.md`:

```
  LEXICAL  sorted(..., reverse=True)[0]  -> 7_v9.md  => version 9
  NUMERIC  sorted(..., key=int)[-1]      -> 7_v10.md => version 10
```

**R3 CONFIRMED**: lexical filename sorting selects parsed version 9 over version 10.

**Latency of the defect — MEASURED, read-only.** `MAX(parsed_text_version)` in the live review is
**3** (449 rows at v1, 341 at v2, 4 at v3). For all 190 corpus papers the lexical and numeric
resolvers **agree today** (0 divergences), and every corpus paper has at least one
`parsed_text/{id}_v*.md` on disk. The defect arms at the first `_v10`.

**NULL `parsed_text_path` — v49 CONTRADICTED, and the shape of the contradiction matters.**

| measure | value |
|---|---:|
| `full_text_assets` rows total | 794 |
| rows with `parsed_text_path IS NULL` | **350** |
| distinct papers with such a row | **350** |
| …of which are corpus-status papers | 169 |
| **corpus papers for which the DB-driven resolver returns NULL** | **0** |
| corpus papers whose stored path does not exist on disk | **0** |

Tested against all four plausible selection rules (no non-NULL row at all; no row at all; newest
`parsed_text_version`; highest `id`, which is what
`ft_screening_adjudicator` actually uses — `ORDER BY id DESC LIMIT 1`). **Every one returns 0.**
v49 says 16; the measurement says 350 NULL *rows* and 0 NULL *resolutions*. Both values are
recorded; neither is resolved here.

---

## D2-6 — Migrations 010 and 011

**Target states, READ from the migration modules:**

* **010** — two tables, `provenance_census_runs` (run header) and `provenance_classifications`
  (one row per classified span), with a CASCADE FK and a closed `taxonomy_class` enum.
  *"Idempotent (IF NOT EXISTS). Executes in a single transaction."*
* **011** — three changes: `provenance_classifications.absence_pattern` (new nullable TEXT);
  `provenance_census_runs.absence_pattern_version` (new TEXT, `''` for pre-v1.1 runs); and the
  `taxonomy_class` CHECK *"widened to admit `'ABSENCE_CLAIM'`"* via create-copy-drop-rename.

**Live schema, MEASURED read-only:**

| target | present? |
|---|---|
| 010 — `provenance_census_runs` | **yes** |
| 010 — `provenance_classifications` | **yes** |
| 011 — `provenance_classifications.absence_pattern` | **yes** |
| 011 — `provenance_census_runs.absence_pattern_version` | **yes** |
| 011 — `taxonomy_class` CHECK admits `ABSENCE_CLAIM` | **yes** |

And the widened enum is in use: `ABSENCE_CLAIM` carries **150** of the 22,034 classification rows,
alongside `ANCHORED` 12,531, `DRIFTED` 4,526, `UNTRACEABLE_NO_BASIS` 1,925, `STITCHED` 1,458,
`UNTRACEABLE_PARTIAL` 618, `ABSENCE_DECLARED` 422, `MISSING_SNIPPET` 402, `UNCLASSIFIABLE_SHORT` 2.

**I3 CONFIRMED — there is no applied-receipt.** MEASURED: no table in `sqlite_master` whose name
contains `migration` or `schema_version`; `PRAGMA user_version` is **0**. And READ,
`engine/core/database.py::_run_migrations`, which is the only migration runner
`ReviewDatabase.__init__` invokes:

```python
    def _run_migrations(self) -> None:
        """Apply schema migrations, skipping those already applied."""
        # Ensure adjudication table exists
        from engine.adjudication.schema import ensure_adjudication_table
        ensure_adjudication_table(self._conn)

        for sql in _SIMPLE_MIGRATIONS:
```

It iterates `_SIMPLE_MIGRATIONS` and two inline rebuilds; **it never imports or runs the numbered
migration modules.** INFERRED: 010 and 011 were hand-run, which is what the primer's
MIGRATION-WIRE-01 note records. The consequence is that **target state and applied state are
established only by inspecting the schema** — as done here — and a fresh `ReviewDatabase` on a new
review would not have either migration.

---

## D2-7 — Routing rule

**Where it lives — I4 CONFIRMED, still two copies.**

READ, `engine/agents/screener.py::run_screening`:

```python
        # Resolve agreement
        if d1.decision == "include" and d2.decision == "include":
            db.update_status(pid, "ABSTRACT_SCREENED_IN")
            stats["screened_in"] += 1
        elif d1.decision == "exclude" and d2.decision == "exclude":
            db.update_status(pid, "ABSTRACT_SCREENED_OUT")
            stats["screened_out"] += 1
        else:
            db.update_status(pid, "ABSTRACT_SCREEN_FLAGGED")
            stats["flagged"] += 1
```

READ, `scripts/screen_expanded.py::run_screen_phase`:

```python
        if d1.decision == "include" and d2.decision == "include":
            decision = "include"
            stats["include"] += 1
        elif d1.decision == "exclude" and d2.decision == "exclude":
            decision = "exclude"
            stats["exclude"] += 1
```

Logically identical; they differ only in their sink — the first writes `papers.status`, the second
a CSV under a staging directory. **R7's description holds at HEAD.**

**Which is on the supported path:** `screener.py::run_screening`, via
`scripts/run_pipeline.py::_stage_screen`. `screen_expanded.py` is a standalone CLI that
`CLAUDE.md` documents; it is live but not part of any pipeline.

**Sites that would change to route the verifier onto primary OUT instead of primary IN — 11 code
sites plus two frozen surfaces.** MEASURED.

| # | site | what encodes "IN" |
|---|---|---|
| 1 | `screener.py::run_verification` selector | `papers = db.get_papers_by_status("ABSTRACT_SCREENED_IN")` |
| 2 | `screener.py::run_verification` outcome branch | `if decision.decision == "include": # Stays ABSTRACT_SCREENED_IN` … `else: ABSTRACT_SCREEN_FLAGGED` |
| 3 | `screener.py::run_verification` docstring | *"Both models include → stays ABSTRACT_SCREENED_IN / Primary included, verifier excludes → ABSTRACT_SCREEN_FLAGGED"* |
| 4 | `screen_expanded.py::run_verify_phase` selector | `if row["screening_decision"] == "include": includes.append(row)` |
| 5 | `screen_expanded.py::run_verify_phase` outcome branch | `if decision.decision == "include": final = "include"` `else: final = "flagged"` |
| 6 | `scripts/rescreen_with_specialty.py::run_verification` selector | `papers = db.get_papers_by_status("ABSTRACT_SCREENED_IN")` |
| 7 | `analysis/eval/screen2f.py::verifier_targets` | `is_in = primary_results[p["id"]]["outcome"] == IN` |
| 8 | `analysis/eval/screen2f.py::verifier_targets` docstring | *"Primary IN papers are the pipeline's pattern."* |
| 9 | `analysis/eval/screen2f.py::final_outcome` | `if primary != IN: return primary` |
| 10 | `analysis/eval/score_screen2f.py::check_call_pattern` | `if p["outcome"] == lib.IN and v is None:` → *"primary IN with no verifier result"* |
| 11 | `analysis/eval/score_screen2f.py::check_call_pattern` | `if p["outcome"] != lib.IN:` → *"verifier called on primary {outcome}"* |

**Plus two frozen surfaces, and this is the load-bearing part.** The verifier's own prompt is
built for the job of confirming an include: `_build_prompt(..., role="verifier")` selects
`stage = "abstract_verifier"`, which selects that stage's inclusion block, its **full strict
exclusion list**, its specialty block and its `decision_instruction`. Reversing the routing
changes what the verifier is asked, so it moves **R2** (the frozen abstract-verifier request,
one of the fourteen pinned SHA-256 surfaces in `tests/test_eligibility.py`) and the
`stage_policies["abstract_verifier"]` entry in the live spec's `eligibility` block. Those are
**DO NOT TOUCH** under this brief; they are named here as scope, not edited.

---

## D2-8 — Abstract-stage output schema and storage

**The format block of the frozen abstract primary request** — READ, reproduced from
`ScreeningDecision.model_json_schema()` and byte-identical to the `"format"` object at the head of
`docs/session-reports/screen-auth-2g/render/R1.after.txt`:

```json
{
  "description": "Structured output from the screening agent.",
  "properties": {
    "decision":   {"enum": ["include", "exclude"], "title": "Decision", "type": "string"},
    "rationale":  {"description": "1-2 sentence explanation", "title": "Rationale", "type": "string"},
    "confidence": {"maximum": 1.0, "minimum": 0.0, "title": "Confidence", "type": "number"}
  },
  "required": ["decision", "rationale", "confidence"],
  "title": "ScreeningDecision",
  "type": "object"
}
```

Three properties, all three required. **Nothing in the schema can carry a criterion id or a
character offset.** The prompt's closing line matches it —
`Respond with JSON only: {"decision": "...", "rationale": "...", "confidence": 0.0-1.0}`.

**The six screening tables** — MEASURED, read-only:

| table | columns | rows |
|---|---|---:|
| `abstract_screening_decisions` | `id, paper_id, pass_number, decision, rationale, model, decided_at` | 21,374 |
| `abstract_verification_decisions` | `id, paper_id, decision, rationale, model, decided_at` | 1,422 |
| `abstract_screening_adjudication` | `id, paper_id, external_key, title, adjudication_decision, adjudication_source, adjudication_reason, adjudication_category, adjudication_timestamp, created_at` | 0 |
| `ft_screening_decisions` | `id, paper_id, model, decision, **reason_code**, rationale, **confidence**, decided_at` | 366 |
| `ft_verification_decisions` | `id, paper_id, model, decision, rationale, **confidence**, decided_at` | 182 |
| `ft_screening_adjudication` | `id, paper_id, title, **reason_code**, primary_rationale, verifier_rationale, adjudication_decision, adjudication_reason, adjudication_timestamp, created_at` | 36 |

**v49 §3.1 expectation — PARTLY CONTRADICTED.** "No `reason_code` at abstract stage" is
**confirmed**: `reason_code` appears only in the two FT tables. "decision / rationale /
**confidence** only" is **contradicted**: the abstract tables store `decision`, `rationale`,
`model` (and `pass_number` on the primary table) and have **no `confidence` column at all**. Both
readings are recorded; neither is resolved.

**Confidence is required of the model and then discarded.** READ, the two writers:

```python
            """INSERT INTO abstract_screening_decisions
               (paper_id, pass_number, decision, rationale, model, decided_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
```

```python
            """INSERT INTO abstract_verification_decisions
               (paper_id, decision, rationale, model, decided_at)
               VALUES (?, ?, ?, ?, ?)""",
```

`ScreeningDecision.confidence` is a required, range-validated field; neither writer passes it.
INFERRED: the value is produced by the model on every one of the 21,374 + 1,422 calls and is not
retained anywhere.

### Could any column hold a criterion id, a character-offset span, or a verifier judgment on an OUT?

| thing | answer |
|---|---|
| **criterion id** | **No column anywhere in the six tables.** `ft_screening_decisions.reason_code` is a twelve-code *reason* vocabulary, not a criterion identifier, and it does not exist at the abstract stage. `abstract_screening_adjudication.adjudication_category` is a human-assigned FP category, written only by the adjudication importer (0 rows). |
| **character-offset span** | **No column of any type, in any of the six tables.** There is no integer offset column, no span table for screening, and no free-text column whose writer stores one. Storing one today would require a migration. |
| **verifier judgment on an OUT** | **Structurally possible, procedurally never written.** `abstract_verification_decisions` is keyed only on `paper_id` and would accept a row for any paper, but the only writer is `run_verification`, whose selector is `get_papers_by_status("ABSTRACT_SCREENED_IN")`. **MEASURED: 1,422 verification rows against 21,374 screening decisions** — the table holds only papers primary admitted. |

INFERRED, for the screening redesign: an evidence-backed exclusion (criterion id + verified
quote) needs **both** a schema change to `ScreeningDecision` — which moves the frozen R1 and R2
surfaces — **and** new columns or a new table. Neither exists today.

---

## D2-9 — `review_runs` linkage

**No screening row references a `review_runs` row today.** MEASURED, from the full schema:
none of `abstract_screening_decisions`, `abstract_verification_decisions`,
`abstract_screening_adjudication`, `ft_screening_decisions`, `ft_verification_decisions` or
`ft_screening_adjudication` has a `run_id`, `review_run_id` or any foreign key to `review_runs`.
The same is true of `extractions`, `cloud_extractions`, `evidence_spans` and
`cloud_evidence_spans`.

**`review_runs` columns** — MEASURED:

```
id, review_spec_hash, screening_hash, extraction_hash, started_at, completed_at,
status, log, codebook_hash, codebook_sha256
```

Six rows, all from 2026-02-28 → 2026-03-06. `codebook_hash` and `codebook_sha256` are **NULL in
all six** (they postdate those runs). `log` is `'[]'` in all six, so the per-stage stats
`run_pipeline` assembles — including `duplicates` (D2-11) — are not persisted.

INFERRED: the only table in the database that links a result to a run is `judge_ratings`/
`fabrication_verifications`, via `judge_runs.run_id`, and `provenance_classifications` via
`census_run_id`. The two mechanisms that *do* carry run identity are both from the analysis lane,
not the engine.

---

## D2-10 — Harness census

**Six** `analysis/eval/run_*.py`, not seven. **v49's count is contradicted; both are recorded.**
(If the intended set included `analysis/eval/elicit01/runner.py`, which is a runner under a
different name, the count is seven — that is a reading, not a measurement, and it is not resolved
here.)

| harness | classification | results committed where | last-run evidence |
|---|---|---|---|
| `run_screen2f.py` | **live harness code** — the 2f/2g screening arms; still being edited | `docs/session-reports/screen-auth-2f-smoke/` (`8a7dc4f`, 2026-09-13) and `screen-auth-2g/` (`1ec513c`/`4e2a66c`, 2026-09-19/20) | last touched `9a69fbe`, 2026-09-19; run outputs committed, byte-reproducible by `score_screen2f` with no flags |
| `run_qualgap01.py` | **frozen experiment code** — QUALGAP-01 runtime A/B | `docs/session-reports/QUALGAP-01_report.md`; store `data/surgical_autonomy/eval/qualgap01/` (gitignored) | `runtime_v12.jsonl` 1.6 MB, 2026-08-01 14:55; `analysis_summary.json` 2026-08-01 16:42 |
| `run_local_abc.py` | **frozen experiment code** — SCHEMA-EVAL-02 (A/B/C response contract, n=40) | `docs/session-reports/SCHEMA-EVAL-02_report.md`; store `eval/schema_eval2/` | `local_abc.jsonl` 1.4 MB, 2026-07-30 13:25 |
| `run_local_ab.py` | **superseded** — the A/B predecessor of `run_local_abc`; SCHEMA-EVAL-01 | `docs/session-reports/SCHEMA-EVAL-01_report.md`; store `eval/schema_eval/` | `local_ab_20260728T200410Z.jsonl`, 2026-07-28 20:04 — **still read at HEAD** by `analysis/eval/analyze_capture01.py`, which names that exact file |
| `run_cloud_strict.py` | **frozen experiment code** — the cloud arm of SCHEMA-EVAL-01 | `docs/session-reports/SCHEMA-EVAL-01_report.md`; store `eval/schema_eval/` | `cloud_strict_20260728T175143Z.jsonl` and `…175532Z.jsonl`, 2026-07-28 |
| `run_capture01.py` | **frozen experiment code** — CAPTURE-01, the 0.21.0 Pass-1 draft capture | `docs/session-reports/CAPTURE-01_report.md`; store `eval/capture01/` | `capture01.jsonl` 314 KB, 2026-08-30 07:59 |

**No harness is an obsolete duplicate of another, and none is safe to delete.** `run_local_ab` is
the closest candidate — `run_local_abc` supersedes its design — but its **output file is named as
a literal in live analysis code** (`analyze_capture01.py`), so deleting the runner would orphan a
reader, and deleting the store would break it. Five of the six last ran between 2026-07-28 and
2026-08-30; only `run_screen2f` is current. **Classification only; nothing deleted, per scope.**

Note for the record: four of the six were last *edited* in one commit, `fa6c738` (2026-09-10,
*"every spec-bearing entry point resolves the review"*), i.e. by the generalisation lane rather
than by their own study — so edit recency is not evidence of use.

---

## D2-11 — Dedup accounting

**Where the exporter's duplicate count comes from: nowhere.** READ,
`engine/exporters/prisma.py`:

```python
        "duplicates_removed": 0,  # tracked externally by dedup module
```

It is a **literal zero with a comment**, and the comment's claim is not implemented — nothing
reads a dedup count back. The committed `data/surgical_autonomy/exports/prisma_flow.csv`
(2026-03-08) renders it:

```
Records identified,251,
,245,From openalex
,6,From pubmed
Duplicates removed,0,
Records screened,251,
```

**Identification accounting is reconstructed from the final corpus, not from search/dedup
events.** READ:

```python
    source_counts = {}
    for row in conn.execute(
        "SELECT source, COUNT(*) as cnt FROM papers GROUP BY source"
    ).fetchall():
        source_counts[row["source"]] = row["cnt"]

    total_identified = sum(source_counts.values())
```

`papers` holds **survivors of deduplication**, so `records_identified` is by construction a
post-dedup number presented in the PRISMA box that is defined as pre-dedup. The committed CSV's
251 = 251 (identified = screened) with 0 duplicates is the arithmetic signature of that.

**The count does exist, briefly, and is thrown away.** READ,
`scripts/run_pipeline.py::_stage_search`:

```python
    dedup_result = deduplicate(pm_cits, oa_cits)
    unique = dedup_result.unique_citations
    logger.info("After dedup: %d unique (%d duplicates removed)",
                len(unique), dedup_result.stats["duplicates_found"])
```

`dedup_result.stats["duplicates_found"]` reaches a log line and the stage's return dict; the
return dict reaches `results`, which is never written — `review_runs.log` is `'[]'` in all six
rows (D2-9). `deduplicate` has **no caller outside `run_pipeline` and its tests**, and
`DedupResult` is not persisted by anything.

**A second, uncounted dedup exists.** READ, `engine/core/database.py::add_papers`:
*"Bulk insert citations, skip duplicates by pmid. Returns count added."* Papers dropped here are
counted in neither `duplicates_found` nor `duplicates_removed`; only the difference between
`len(unique)` and the returned `added` would reveal them, and that difference is not recorded.

MEASURED, for scale: the live `papers` table holds **10,039** rows, so a PRISMA export run today
would report `Records identified 10,039` and `Duplicates removed 0`.

---

## D2-12 — Stage-completion predicate census

All twelve stages `engine/adjudication/workflow.py` declares. The **declared condition** column
quotes the module's own docstring; the **marks it complete** column names the code that calls
`complete_stage` on the supported path; the **evaluates?** column says what that code checks.

| # | stage | declared condition (quoted) | marks it complete | evaluates? |
|---|---|---|---|---|
| 1 | `ABSTRACT_SCREENING_COMPLETE` | *"auto: set when abstract screening finishes"* | `screener.py::run_verification`, after the loop | **proxy** — fires when the verification loop ends, inside a `try` that swallows a missing table. No count is checked; "finishes" is taken literally. |
| 2 | `ABSTRACT_DIAGNOSTIC_COMPLETE` | *"manual: human confirms 50-paper FP analysis"* | **none** | **NONE — manual only**, via `advance_stage --stage …`. Correct by design; the 50-paper sample is not verifiable in code. |
| 3 | `ABSTRACT_CATEGORIES_CONFIGURED` | *"auto: adjudication_categories.yaml exists & validates"* | `screening_adjudicator.py::export_adjudication_queue` | **yes** — `if category_config and category_config.categories:` |
| 4 | `ABSTRACT_QUEUE_EXPORTED` | *"auto: export_adjudication_queue succeeds"* | `screening_adjudicator.py::export_adjudication_queue`, after the write | **yes, by position** — reached only after the export returns and an early `return` guards the empty case. |
| 5 | `ABSTRACT_ADJUDICATION_COMPLETE` | *"auto: import with zero unresolved papers"* | `screening_adjudicator.py`, import path | **YES, EXPLICITLY** — see below |
| 6 | `PDF_ACQUISITION` | *"manual: advance after all PDFs acquired"* | **none** | **NONE — manual only.** |
| 7 | `FULL_TEXT_SCREENING_COMPLETE` | *"auto: full-text screening finishes"* | `ft_screener.py::run_ft_verification`, after the loop | **proxy** — same shape as #1. |
| 8 | `FULL_TEXT_ADJUDICATION_COMPLETE` | *"auto: full-text adjudication import"* | `ft_screening_adjudicator.py`, import path | **NO CONDITION** — unconditional after the commit. The declared condition is only "import", so the code matches the words; there is no remaining-flagged check as at #5. |
| 9 | `EXTRACTION_COMPLETE` | *"auto: all included papers reach EXTRACTED status"* | `run_pipeline.py`, after `_stage_audit` | **DIFFERENT** — `if extracted > 0`, not *all*. A single extracted paper completes the stage. |
| 10 | `AI_AUDIT_COMPLETE_STAGE` | *"auto: audit run finishes (all papers audited)"* | `run_pipeline.py`, after `_stage_audit` | **DIFFERENT** — `if audited > 0`, not *all*. |
| 11 | `AUDIT_QUEUE_EXPORTED` | *"auto: export_audit_review_queue succeeds"* | `audit_adjudicator.py::export_audit_review_queue` | **NO CONDITION** — unconditional at the end of the function; unlike #4 there is no early return on an empty queue, so an export of zero papers completes it. |
| 12 | `AUDIT_REVIEW_COMPLETE` | *"auto: import with zero unresolved spans"* | `audit_adjudicator.py::import_audit_review_decisions` | **NO CONDITION — C1 CONFIRMED.** |

### The #5 / #12 contrast — one declared condition, implemented once

The two stages declare the *same* condition in the same words. Stage 5 implements it — READ,
`engine/adjudication/screening_adjudicator.py`:

```python
    # Auto-advance workflow — but only if all flagged papers have been adjudicated
    remaining_flagged = review_db._conn.execute(
        "SELECT COUNT(*) FROM papers WHERE status = 'ABSTRACT_SCREEN_FLAGGED'"
    ).fetchone()[0]
    if remaining_flagged > 0:
        logger.warning(
            "Workflow NOT advanced: %d papers still at ABSTRACT_SCREEN_FLAGGED. "
            "Process all flagged papers before completing this stage.",
            remaining_flagged,
        )
    else:
        complete_stage(
```

Stage 12 does not — READ, `engine/adjudication/audit_adjudicator.py::import_audit_review_decisions`,
at function-body indent, outside every conditional:

```python
    # Auto-advance workflow: AUDIT_REVIEW_COMPLETE
    complete_stage(
        review_db._conn, "AUDIT_REVIEW_COMPLETE",
```

**And the legacy importer in the same file DOES gate it** — READ,
`audit_adjudicator.py::_import_legacy_format`:

```python
    if stats["missing"] == 0:
        complete_stage(
            review_db._conn, "AUDIT_REVIEW_COMPLETE",
```

INFERRED: the per-span importer that replaced the legacy format dropped the gate. The
pre-replacement code is the one that behaves as declared.

### Predicates that exist and have no production caller

MEASURED, AST call index over `engine/`, `scripts/`, `analysis/`, `tests/`:

| predicate | non-test callers | total callers |
|---|---:|---:|
| `screening_adjudicator.py::check_adjudication_gate` | **0** | 2 (both tests) |
| `ft_screening_adjudicator.py::check_ft_adjudication_gate` | **0** | 1 (test) |
| `audit_adjudicator.py::check_audit_review_gate` | **0** | 2 (both tests) |

**C1 is confirmed, and it has two siblings.** All three return a count of papers with unresolved
work at their stage; all three are the predicate the corresponding stage's declared condition
names; **none is called by any production code.** What the runner reads instead is
`workflow.py::is_stage_done`, whose non-test callers are `advance_stage`, `can_advance_to`,
`is_adjudication_complete` and `is_audit_review_complete` — i.e. the *stage flag*, never the
underlying count.

INFERRED, and this is the whole shape of the finding: the project wrote the right predicate three
times and wired it zero times.

---

## D2-13 — Downstream consumers of scoring output

### The path, from `scoring.py` outward

```
engine/analysis/scoring.py::score_pair
   │
   ├─► engine/analysis/concordance.py::run_concordance
   │        └─► engine/analysis/metrics.py::field_summary ─► cohens_kappa   [D1-6]
   │        └─► _save_report ─► concordance CSV/HTML (module's own argparse main)
   │
   ├─► analysis/paper1/adjudication.py::export_ambiguous_pairs
   │        └─► the AMBIGUOUS-pair adjudication HTML/JSON
   │
   └─► analysis/paper1/export_disagreement_pairs.py::build_disagreement_rows
            ├─► field_summary ─► cohens_kappa ─► the 63 kappas             [D1-6]
            └─► disagreement_pairs_3arm.{csv,html,xlsx}
                     └─► analysis/paper1/judge_loader.py::load_ai_triples_csv
                              └─► judge Pass 1 ─► judge_ratings, judge_pair_ratings
                                       └─► judge Pass 2 ─► fabrication_verifications
                                                └─► pi_audit_sampler (v1, from Pass 2)
                                                └─► pi_audit_sampler_v2 (arm values from the CSV)
```

No exporter under `engine/exporters/` calls `score_pair`; `evidence_table.*`, `prisma_flow.csv`
and `methods_section.md` are unaffected.

### Committed artifacts that depend on it

| artifact | on disk | commit / provenance | what depends on scoring.py |
|---|---|---|---|
| `data/surgical_autonomy/exports/disagreement_pairs_3arm.csv` | 797 KB, 2026-03-20 | gitignored (`data/`) | **the row set itself** — which triples exist — plus the three `*_score` columns |
| `…_3arm.xlsx` (sheets `Free Text`, `Categorical`, `Summary`, `All`) | 584 KB, 2026-03-20 | gitignored | same, plus **63 kappa/CI triples** in `Summary` |
| `…_3arm.html` | 1.7 MB, 2026-03-20 | gitignored | same, plus the rendered *"Kappa by Arm Pair"* table |
| `judge_ratings` (2,276) · `judge_pair_ratings` (6,828) | live DB | — | **denominator only** — every rated triple came from the CSV |
| `fabrication_verifications` (7,422 across 5 runs) | live DB | — | same |
| `artifacts/paper1/pi_audit/pi_audit_workbook_2026-04-22T19-05-36Z.xlsx` + its key | tracked | committed | sampled from Pass 2, so inherits the denominator |
| `artifacts/paper1/pi_audit_v2/…2026-06-05T02-29-33Z.xlsx` + key | untracked, on disk | — | **arm values read directly from the CSV** |
| `artifacts/paper1/pass2_preliminary_report_2026-04-22T16-26-33Z.md` | tracked | committed | reports over the Pass 2 population |

### Which v49 §3.3 / §10 figures rest on it

* **The 63 kappas — YES, doubly.** They are `field_summary(...).kappa`, so they carry D1-6's
  defective estimator, and their MATCH/MISMATCH inputs are `score_pair`'s, so they carry D1-5.
  Verified arithmetically: `robot_platform / local_vs_o4mini` publishes `pct_agreement 0.6961`
  and `kappa 0.2817`, and `1 − 1/(2 × 0.6961) = 0.28172`.
* **Judge-derived figures — YES, through the denominator.** Every judge rate is computed over the
  2,266 triples the CSV admitted, not over the field × paper grid.
* **The 12.1% and 33.7% I can locate at HEAD — NO.** The only committed figures carrying those
  numbers are in `docs/session-reports/FIELDCLASS-01_report.md` §3.3, a per-arm × field-class
  table of **DRIFTED** rates from the provenance census (`openai / judgment` 12.1%;
  `anthropic / inferable` 33.7%). **MEASURED: the census reads `evidence_spans` and
  `cloud_evidence_spans` directly — all 11,017 spans, 3,802 distinct (paper, field) cells — and
  never touches `scoring.py`.** Those two rates are therefore independent of this defect.
  **The ruling describes them as "judge health 12.1%; restatement 33.7% → 12.1%", which does not
  match the FIELDCLASS-01 table's meaning. Both readings are recorded; I cannot resolve which
  v49 §3.3/§10 rows the ruling means, because v49 is not in the repository.**

### Direct answer: did the Run 6 judge passes see only pairs the scorer called disagreements?

**YES.** The code path, READ, `analysis/paper1/export_disagreement_pairs.py::build_disagreement_rows`:

```python
            for arm_a, arm_b in ARM_PAIRS:
                fs = score_pair(fname, values[arm_a], values[arm_b], spec)
                pk = _pair_key(arm_a, arm_b)
                pair_scores[pk] = fs
                scores_by_pair_field[pk][fname].append(fs)
                if fs.result != "MATCH":
                    any_disagree = True

            if not any_disagree:
                continue
```

and, READ, `analysis/paper1/judge_loader.py::load_ai_triples_csv`:

```python
    """Read a 3-arm disagreement CSV and produce list[JudgeInput].

    Rows are skipped (with WARNING logs, not exceptions) when:
      - field_name is not in the codebook,
      - paper text is missing on disk.
    """
```

One `JudgeInput` per CSV row and no other source. **MEASURED, and the numbers close:**

| measure | value |
|---|---:|
| `disagreement_pairs_3arm.csv` data rows | **2,267** |
| distinct (paper_id, field_name) in the CSV | 2,267 |
| distinct papers × fields in the CSV | 189 × 21 |
| `judge_runs` Pass 1 full run `n_triples_attempted` | **2,266** |
| `judge_ratings` distinct (paper_id, field_name) | **2,266** |
| census distinct (paper, field) cells | **3,802** |
| **census cells the judge never saw** | **1,535 (40.4%)** |
| CSV cells absent from the census | **0** |

The one-row gap between 2,267 and 2,266 is the loader's documented skip path. The 1,535 excluded
cells are exactly those where `score_pair` returned MATCH on all three arm pairs — which, given
D1-5, includes every cell where the three arms differed only by a substring or a negation prefix.

---

## D2-14 — Extraction reuse key

**The idempotence lookup** — READ, `engine/agents/extractor.py::run_extraction`:

```python
        # Check staleness: skip if already extracted with current schema hash
        existing = db._conn.execute(
            "SELECT id FROM extractions WHERE paper_id = ? AND codebook_hash = ?",
            (pid, schema_hash),
        ).fetchone()
        if existing:
            logger.info("Paper %d: already extracted with current schema — skipping", pid)
```

**Inputs in the key:** `paper_id` and `codebook_hash`. **Nothing else.**

| candidate input | in the key? |
|---|---|
| parsed-text identity (hash, path, mtime, version) | **no** — and the text is not even loaded until *after* the check |
| parser version (`full_text_assets.parser_used`) | **no** |
| prompt hash | **no** — no prompt hash is computed on this path |
| model digest | **no** — `extractions.model_digest` is *written*, never read back for reuse |
| inference options (temperature / think / num_ctx) | **no** |
| `pdf_hash` | **no** — used by `parse_pdf` for a different short-circuit, not here |

**Fixture — MEASURED.** Temp `ReviewDatabase`; one paper at `PARSED`; `extract_paper` replaced by
a counter that stores a complete extraction stamped with the codebook's `semantic_hash`;
`require_preflight` stubbed. **No model call.**

```
--- run 1: fresh ---
   stats: {'extracted': 1, 'skipped': 0, 'failed': 0}   extract_paper calls: 1

--- change the parsed text by ONE character, reset status to PARSED, re-run ---
   parsed text now: The trial enrolled 51 patients in a randomised design.
   stats: {'extracted': 0, 'skipped': 1, 'failed': 0}   extract_paper calls (cumulative): 1

--- add a NEW parsed version v2 with different text, re-run ---
   stats: {'extracted': 0, 'skipped': 1, 'failed': 0}   extract_paper calls (cumulative): 1

extract_paper was called with: [{'paper_id': 1, 'text_sha_prefix': 'The trial enrolled 50 patients in a rand'}]
```

**The prior extraction is reused. v49 §4.5 CONFIRMED.** Reused after a one-character edit, and
reused again after an entirely new `_v2` parse landed. `extract_paper` ran once, against the
original text, and the stored extraction continues to be presented as this paper's result.

INFERRED, and it pairs with D2-5: a re-parse that changes the text produces a new
`parsed_text/{id}_v{N}.md` and a new `full_text_assets` row, and **changes nothing about which
extraction the pipeline considers current.** The two defects compose — the resolver may pick the
wrong version, and the extractor will not re-run against any version.

---

## D2-15 — Human-decision stores and effective-value readers

### Stores — where a human decision is recorded

| # | store | kind | rows / state | written by |
|---|---|---|---|---|
| 1 | `abstract_screening_adjudication` | table | **0** | `screening_adjudicator` import |
| 2 | `ft_screening_adjudication` | table | **36** | `ft_screening_adjudicator` import |
| 3 | `audit_adjudication` | table | **0** | `audit_adjudicator` import (xlsx path only) |
| 4 | `papers.status` | mutated column | the effective screening/lifecycle outcome | every adjudication importer, `bulk_accept`, `reject_paper` |
| 5 | `papers.rejected_reason` | mutated column | — | `reject_paper` |
| 6 | `evidence_spans.value` | mutated column | — | audit import: **CORRECT** overwrites it (xlsx), **REJECT** sets `'NR'` (JSON) |
| 7 | `evidence_spans.audit_status` / `.auditor_model` / `.audit_rationale` / `.audited_at` | mutated columns | 2,159 verified · 1,152 flagged · 449 contested; **`auditor_model` is `gemma3:27b` on all 3,760** | audit import (all decisions → `'verified'`, `'human_review'`) |
| 8 | `evidence_spans.source_snippet` | mutated column | — | JSON `ACCEPT_CORRECTED` |
| 9 | `papers.pdf_quality_check_status` / `.pdf_exclusion_reason` / `.pdf_exclusion_detail` | mutated columns | 31 `PDF_EXCLUDED` | `pdf_quality_import` |
| 10 | `workflow_state` | table | 12 rows; 7 complete, 5 pending | `complete_stage`, `bypass_stage`, `advance_stage` (manual) |
| 11 | `judge_run_audit` | table | 1 | post-hoc annotation of a judge run |
| 12 | the HTML → JSON → import files | filesystem | `{review}_{stage}_{queue\|decisions}.{html\|json}`, plus xlsx workbooks | the five generators named in `CLAUDE.md` |
| 13 | the PI audit workbooks | filesystem | v1 (tracked) + v2 (untracked) and their keys | `pi_audit_sampler`, `pi_audit_sampler_v2` |

**🔴 Three of these are write-only.** MEASURED: searching `engine/`, `scripts/` and `analysis/`
for a read of the adjudication tables returns **exactly one hit** —
`engine/exporters/prisma.py`, a `SELECT COUNT(*) FROM ft_screening_adjudication`. **Nothing in the
codebase ever reads `audit_adjudication` or `abstract_screening_adjudication`.** The human
decision survives only as the side effect the importer also applied to the span or the status; the
adjudication row itself is an append-only log nothing consults.

### Readers — how each resolves an "effective" value

| reader | resolution rule (quoted, one line) | human decisions override? | abstentions override? |
|---|---|---|---|
| `evidence_table.py::_build_evidence_rows` | `"SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"`, then all spans of that extraction | **only via the span mutation**; `audit_adjudication` is never read | no — a stored non-value token would be printed verbatim |
| `concordance.py::load_arm` (`local`) | `FROM evidence_spans es JOIN extractions e ON e.id = es.extraction_id ORDER BY e.paper_id, es.field_name` — **no run selection at all** | **no** — `audit_status` is not read either | **yes, and backwards**: `if str(row["value"] or "").strip().upper() in non_value: continue` drops the token and leaves the other run's value (D1-4) |
| `concordance.py::load_arm` (cloud) | same, plus `WHERE ce.arm = ?` | no | same |
| `metrics.py::field_summary` | consumes `load_arm`'s output via `score_pair` | no | inherits |
| `judge_loader.py::load_ai_triples_csv` | one `JudgeInput` per CSV row; *"Rows are skipped … when field_name is not in the codebook, paper text is missing on disk"* | no | inherits the CSV's |
| `audit_adjudicator.py::_collect_papers_for_review` | `"SELECT id, low_yield FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"` | reads `audit_status`, so yes | n/a |
| `audit_adjudicator.py::import_audit_review_decisions` | same `ORDER BY id DESC LIMIT 1`, then `WHERE extraction_id = ? AND field_name = ?` | writes them | n/a |
| `human_review.py::_import_review_csv` | `"… JOIN extractions e ON es.extraction_id = e.id WHERE e.paper_id = ? AND es.field_name = ? ORDER BY es.id DESC LIMIT 1"` — **newest SPAN across all extractions** | writes them | n/a |
| `human_review.py::_import_review_json` | `"SELECT id FROM evidence_spans WHERE id = ?"` — **binds by `span_id`** | writes them | n/a |
| `corpus.py::is_corpus_member` | `return status in CORPUS_STATUSES` | yes — status is what adjudication mutates | n/a |
| `run_pipeline.py` gates | `is_adjudication_complete(db._conn)` / `is_audit_review_complete(db._conn)` — both `is_stage_done(conn, …)` on `workflow_state` | **no** — reads the stage flag, not the work (D2-12) | n/a |
| `prisma.py::_build_flow` | `"SELECT status, COUNT(*) FROM papers GROUP BY status"` (+ one FT adjudication COUNT) | yes, through status | n/a |
| `provenance/census.py` | every row of `evidence_spans` and `cloud_evidence_spans`, unfiltered | no | no — classifies them as `ABSENCE_*` |
| `ft_screening_adjudicator.py` export | `"SELECT parsed_text_path FROM full_text_assets WHERE paper_id = ? ORDER BY id DESC LIMIT 1"` for context; decisions from the two FT tables | n/a | n/a |
| `pi_audit_sampler_v2.py` | arm values read from the disagreement-pairs CSV, *"exactly what Pass 2 saw"* | no | empty cell → `NOT REPORTED` |

### Can two readers return different answers for the same paper/field today?

**YES — three demonstrated pairs, all structural, all at HEAD.**

1. **`evidence_table` vs `concordance.load_arm`, on a re-extracted paper.** The exporter reads
   the **latest** extraction; `load_arm` folds **every** extraction into one dict. With more than
   one extraction per paper they return different values, and `load_arm` additionally drops a
   newer abstention in favour of an older claim (D1-4). *Currently latent:* MEASURED, the live
   review has exactly one extraction per paper.
2. **`human_review._import_review_csv` vs `audit_adjudicator.import_audit_review_decisions`, on
   the same workbook subject.** The first selects the newest **span** (`ORDER BY es.id DESC`)
   across all of a paper's extractions; the second selects the newest **extraction**
   (`ORDER BY id DESC LIMIT 1`) and then that extraction's span. Those are different rows whenever
   a newer extraction produced fewer spans, or an older extraction was appended to. The JSON path
   binds by `span_id` and agrees with neither.
3. **Any reader vs the adjudication log.** `audit_adjudication.override_value` and
   `.human_decision` record what the PI decided; **no reader consults them**. A CORRECT whose
   span was subsequently re-extracted leaves the decision in the log and the old value in every
   export. This is not latent — it is the current design, and it is why D1-1 and D1-2 are
   invisible to every downstream check.

**Inventory and quotes only, as scoped. No design is proposed.**

---

## D2-16 — Review-name literals and defaults at HEAD

**From the committed inventory** (`docs/inventory/entry_points.json`, `--check` = *in sync*),
MEASURED:

| measure | value |
|---|---:|
| review ids on disk | **1** (`surgical_autonomy`) |
| `default_review_named_constants` | **7** |
| `review_id_constants` (all kinds) | **9** |
| `literal_review_id_sites_in_code` | **35** |
| files carrying either | **24** |

**The expectation is confirmed in its number and wrong in its denominator — exactly as the ruling
warned.** There really are **seven** `DEFAULT_REVIEW` constants. But there are **nine** review-id
constants, and the two that are not called `DEFAULT_REVIEW` are the two that matter.

### The nine constants

| # | site | name | supported path? | can a resolver-supplied id reach it? |
|---|---|---|---|---|
| 1–7 | `scripts/advance_to_pdf_acquired.py`, `backfill_authors.py`, `backfill_cloud_spans.py`, `monitor_extraction.py`, `parse_expanded_corpus.py`, `prepare_concordance_pdfs.py`, `retry_parse_6.py` | `DEFAULT_REVIEW` | maintenance CLIs, not `run_pipeline` | **YES** — each is only an argparse default: `--review", default=DEFAULT_REVIEW` |
| 8 | `analysis/eval/run_screen2f.py` | `REVIEW` | harness | no — but it is a harness constant, which is its purpose |
| 9 | **`engine/analysis/normalize.py`** | **`_FALLBACK_REVIEW_ID`** | **YES — every `score_pair` call** | **NO — this is D1-9** |

### The engine sites, which the seven do not cover

**MEASURED: three literal `"surgical_autonomy"` sites live under `engine/`.**

**(a) `engine/analysis/normalize.py` — D1-9.** READ:

```python
#: GENERALIZE B2, still open. Concordance normalisation falls back to ONE
#: review's enum set when no spec is passed, which resolves another review's
#: categorical values against the wrong vocabulary — silently, with no
#: exception, just mismatches. SPEC-AUTH-01 only stops this module building
#: the path itself; the hardcoded review is PATH-AUTH-01's to remove.
_FALLBACK_REVIEW_ID = "surgical_autonomy"
```

**(b) `engine/analysis/report.py::_get_tier_map` — a SECOND engine site the expectation did not
name.** READ:

```python
        try:
            # GENERALIZE B3, still open: the tier map falls back to ONE
            # review's codebook. The hardcoded review is PATH-AUTH-01's.
            from engine.core.codebook import load_codebook_for
            cb = load_codebook_for("surgical_autonomy")
            _TIER_CACHE = {f["name"]: f["tier"] for f in cb.fields}
        except Exception:
            _TIER_CACHE = {}
```

On the concordance report path, cached in a module global, and **guarded by a bare `except` that
converts any failure into an empty map** — so every field silently reports tier 0 rather than
raising. No resolver-supplied id can reach it: the function takes no argument.

**(c) `engine/migrations/003_backfill_expanded_screening.py` — a module-level path literal.** READ:

```python
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "surgical_autonomy"
EXPANDED_DIR = DATA_DIR / "expanded_search"
```

Evaluated at import, with **no `--review` flag at all**. Migrations 004 and 005 by contrast take
`--review", default="surgical_autonomy"`, which a caller can override.

### Why "seven" did not prevent D1-9

INFERRED, and it is the general lesson rather than a new defect: the seven are argparse defaults —
**every one of them is overridable by the caller**, so as a class they are the *benign* kind. The
dangerous kind is a constant no caller can reach, and there are **three** of those, all under
`engine/`, none named `DEFAULT_REVIEW`, and two of them annotated in-code as still open
(GENERALIZE B2 and B3). **A count of `DEFAULT_REVIEW` constants is the wrong instrument for this
question, which is exactly what the ruling anticipated.**

---

## Assumption-ledger verdicts

### The two deferred from Part A

| item | verdict | evidence |
|---|---|---|
| **I3** — migrations 010 and 011 have no applied-receipt anywhere in the repo or the DB | **CONFIRMED** | No `sqlite_master` table whose name contains `migration` or `schema_version`; `PRAGMA user_version = 0`; `ReviewDatabase._run_migrations` iterates `_SIMPLE_MIGRATIONS` plus two inline rebuilds and never imports a numbered migration module. **Both target states are nevertheless present in the live schema** (D2-6), so the migrations were applied by hand and left no trace. |
| **I4** — `screener.py` and `scripts/screen_expanded.py` still carry two copies of the routing rule | **CONFIRMED** | Both quoted in D2-7; the include/include → IN, exclude/exclude → OUT, else FLAGGED branch appears once in each, logically identical, differing only in sink (DB status vs CSV column). **R7's description of the pipeline rule holds at HEAD.** |

### Carried forward from Part A, unchanged

| item | verdict |
|---|---|
| **I1** — the 13 file anchors exist at those paths | **CONFIRMED** (zero misses; inventory in sync) |
| **I2** — the 2g P3 kappas came from `score_screen2f.py`, outside the review's archive scope | **CONFIRMED** |
| **I5** — a consistent read snapshot via `mode=ro` in one read transaction | **CONFIRMED** |
| **I6** — sklearn importable in `.venv` | **CONFIRMED** (1.8.0) |
| **I7** — the cron jobs do not write `review.db` | **CONFIRMED for this session, by postcondition** |

### Expectations stated in the brief and the ruling

| expectation | verdict |
|---|---|
| D2-1, the two abstract calls: `{"temperature": 0}`, think false, format schema, no seed, no `num_ctx` | **CONFIRMED, exactly** |
| D2-1 / R5, extraction sends temperature 0 regardless of the requested value | **CONFIRMED** — and the model name is a module constant too |
| D2-3 / R5, the runner does not connect all stages; the extraction precheck ignores `FT_ELIGIBLE` | **CONFIRMED** |
| D2-4 / R5, `EXTRACT_FAILED` is outside the corpus | **CONFIRMED** |
| D2-5 / v49, seven parsed-text resolvers | **CONTRADICTED** — 42 glob sites in 37 modules, plus 3 DB-driven |
| D2-5 / v49, 16 papers with NULL `parsed_text_path` | **CONTRADICTED** — 350 NULL rows / 350 papers; **0** corpus papers resolve to NULL |
| D2-8 / v49 §3.1, decision / rationale / confidence only, no `reason_code` at abstract stage | **PARTLY CONTRADICTED** — no `reason_code` ✅; **`confidence` is not stored at all** ❌ |
| D2-10 / v49, seven `analysis/eval/run_*.py` | **CONTRADICTED** — six |
| D2-12 / C1, `AUDIT_REVIEW_COMPLETE` is unconditional | **CONFIRMED**, plus two more unconditional stages and three unwired gate predicates |
| D2-13 / C2, the judge saw only scorer-declared disagreements | **CONFIRMED by the code path**, with the numbers closing (2,267 → 2,266) |
| D2-14 / v49 §4.5, keyed on `codebook_hash` only, so the prior extraction is reused | **CONFIRMED** |
| D2-16 / v49 §4.1, seven `DEFAULT_REVIEW` constants and the literal-name sites remain | **CONTRADICTED as the ruling warned** — 7 is right for `DEFAULT_REVIEW`, but there are 9 review-id constants, 35 literal sites, and **three engine-side literals no resolver can reach** |

**Four v49 §3.1/§4.1 rows are contradicted by measurement (D2-5 twice, D2-8, D2-10, D2-16). Per
the contradiction rule, both values are recorded above and none is resolved here.**

---

## Startup values at close

| # | check | at open | at close | verdict |
|---|---|---|---|---|
| E1 | HEAD / tree / origin | `4e2a66c`, clean, level | **`7e09de3`** (Part A's docs-only commit, pushed), clean, level (`0 0`) | moved only by the permitted commit |
| E2 | standard gate, five chunks | 2,329 passed / 17 deselected (504/594/377/438/416; 0/0/10/6/1) | **identical**, re-run before the Part A commit | unchanged |
| E3 | `review.db` file | 101,978,112 B @ 2026-09-11 02:00:52.636956943 UTC | **identical** | unchanged |
| E4 | `tests/test_eligibility.py` | 91 passed, 14 pinned hashes | unmodified; not re-run after Part A (no file it covers changed) | unchanged |
| E5 | ten row counts + 24 tables | all exact | no write was issued to `review.db` at any point in Part B | unchanged |

**Fingerprint of record:** `docs/session-reports/discovery-01/review_db_fingerprint_20260920T044845Z.json`
— schema `1d6af8b9…3a18d`, overall `f376562e…39e00`, `-wal` 0 bytes at read.

**Nothing under `engine/`, `scripts/`, `analysis/`, `tests/` or `review_specs/` was modified in
Part A or Part B.** The only writes this session made to the repository are the two docs-only
commits and the fingerprint JSON.

---

## What this read-out does not claim

* No fix, no design, no repair. D2-15 is an inventory; D2-12's three unwired predicates are named,
  not wired.
* **v49 is not in the repository**, so every "v49 says" above is taken from the brief and the
  ruling as quoted, not read at source. Where a measurement contradicts one, both are recorded and
  neither is resolved — including D2-13's 12.1% / 33.7%, where I can name the committed table that
  carries those two numbers but cannot confirm it is the one v49 §3.3/§10 means.
* D2-1's site list is the **Ollama** call sites. The two cloud arms were read, not driven — their
  payloads are described from source, not captured, because driving them would mean an off-box API
  call.
* The DOCX exporter was not re-driven in Part B; Part A recorded it as unobserved.
* `analysis/eval/elicit01/runner.py` is a runner not matching the `run_*.py` glob; whether v49's
  "seven" intended to include it is a reading and is left open in D2-10.
