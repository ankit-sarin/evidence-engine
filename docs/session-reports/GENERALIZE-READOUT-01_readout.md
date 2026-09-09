# GENERALIZE-READOUT-01 — what is pinned to the autonomy review

Read-only inventory, 2026-09-09. Repository read at HEAD `633e56b`, tree clean.

## INFERRED verdicts

| | claim | verdict |
|---|---|---|
| **I1** | spec loader at `engine/config/review_spec.py`; live spec under `data/` or `configs/` | **HALF FALSE** — loader is `engine/core/review_spec.py:417` (`load_review_spec`); the live spec is `review_specs/surgical_autonomy_v1.yaml`, a tracked top-level directory, neither of the guessed paths. Typed Pydantic schema, as assumed. |
| **I2** | one module loads the codebook *and* builds prompts; codebook carries the structural keys | **TRUE** — `engine/agents/extractor.py:44` `_load_codebook` + `:75` `_build_field_block` + `build_extraction_prompt` in the same module; `engine/elicitation/classes.py:59` imports both. Codebook top level: `version, review, date, absence_sentinels, escape_token, contract_unmet_token, fields`. |
| **I3** | `data/surgical_autonomy/` is derived from a spec field | **FALSE** — `ReviewSpec` has **no review-name field at all**. The directory comes from `ReviewDatabase.__init__(review_name)` (`engine/core/database.py:301-302`), fed by `--review` / `--name` on the CLI. Nothing checks that `--review X` matches the `--spec` you pass beside it. |
| **I4** | some prompt content is review-specific and lives in code, not the codebook | **TRUE** — `engine/agents/screener.py:64-69` (four hardcoded primary exclusions), `:88-101` (four hardcoded verifier tests naming autonomy), `engine/agents/ft_screener.py:138-139, 180-185`. |

---

## S1 — Review Spec schema (`engine/core/review_spec.py`)

| field | line | type | req. | default | consumers |
|---|---:|---|---|---|---|
| `title` | 334 | str | ✅ | — | screening/ft/audit adjudicators, `docx_export.py:35`, `run_pipeline.py` |
| `version` | 335 | str | ✅ | — | same four |
| `authors` | 336 | list[str] | ✅ | — | **NONE — dead field** |
| `date` | 337 | date | ✅ | — | `docx_export.py:38` only |
| `prospero_id` | 338 | str? | ○ | None | **NONE — dead field** |
| `pico` | 339 | PICO | ✅ | — | `screener.py`, `ft_screener.py`, both screening adjudicators (26 refs) |
| `search_strategy` | 340 | SearchStrategy | ✅ | — | `search/pubmed.py`, `search/openalex.py`, `exporters/methods_section.py` |
| `screening_models` | 341 | ScreeningModels | ○ | factory (`qwen3:8b` / `qwen3:32b`) | `screener.py`, `methods_section.py`, 2 scripts |
| `ft_screening_models` | 342 | FTScreeningModels | ○ | factory (`qwen3.5:27b` / `gemma3:27b`) | `ft_screener.py`, `methods_section.py` |
| `extraction_models` | 343 | ExtractionModels | ○ | factory (`deepseek-r1:32b`, pass1_think ✅, pass2_think ❌, `elicitation` ❌) | `extractor.py:481,497`, `elicitation/pipeline.py:222`, 4 eval runners |
| `screening_criteria` | 344 | ScreeningCriteria | ✅ | — | `screener.py:59,61`, `ft_screener.py`, adjudicators |
| `extraction_schema` | 345 | ExtractionSchema | ✅ | — | 12 modules — `extractor.py`, `auditor.py`, `completeness.py`, `evidence_table.py`, `docx_export.py`, `methods_section.py`, `extraction_validator.py`, `analysis/report.py`, `audit_adjudicator.py` |
| `specialty_scope` | 346 | SpecialtyScope? | ○ | None | `screener.py`, `ft_screener.py`, both adjudicators (29 refs) |
| `low_yield_threshold` | 350 | int ≥1 | ○ | 4 | `auditor.py` only |
| `auditor_model` | 358 | str? | ○ | None (→`gemma3:27b`) | `auditor.py`, `methods_section.py` |
| `unpaywall_email` | 362 | str? | ○ | None | `acquisition/check_oa.py` |
| `institutional_proxy_pattern` | 366 | str? | ○ | None | `acquisition/manual_list.py` |
| `pdf_quality_check` | 376 | PDFQualityCheck | ○ | factory | `acquisition/pdf_quality_check.py` |
| `cloud_models` | 380 | CloudModels? | ○ | None | `exporters/methods_section.py` **only** |
| `pdf_parsing` | 384 | PDFParsing | ○ | factory | `parsers/pdf_parser.py:557-568` |
| `distribution_monitor` | 388 | DistributionMonitorConfig | ○ | factory | **NONE — dead field** |

**Live spec sets 16 of 21.** Unset (falling to defaults): `auditor_model`, `cloud_models`, `distribution_monitor`, `extraction_models`, `pdf_parsing`. Loads clean; 20 extraction fields; `specialty_scope` set; `cloud_models` None; `elicitation` False.

**Defined but never read — three:** `authors` (336), `prospero_id` (338), and — the substantive one — **`distribution_monitor` (388)**. `engine/validators/distribution_monitor.py:167-169, 331-333` takes its thresholds from module-level `DEFAULT_*` constants and never reads `spec.distribution_monitor`. **A review that tunes that section in YAML changes nothing, silently.**

**Consumer reading an undeclared key: none.** But the inverse hazard is live: `ReviewSpec.model_config == {}`, so Pydantic's default `extra='ignore'` applies — the live spec was validated with a bogus top-level key added and it **passed with no error and no warning**. A typo'd or renamed spec key is silently dropped.

---

## S2 — Codebook schema (`data/surgical_autonomy/extraction_codebook.yaml`)

Tracked, despite `.gitignore:2` excluding `data/` wholesale — it is the **single** force-added file under `data/` (`git ls-files data/` returns exactly one path).

**Structural keys (review-independent):** `version`, `review: 'surgical_autonomy'`, `date`, `absence_sentinels` (6: NR, N/A, NA, NOT_FOUND, NOT FOUND, NOT REPORTED), `escape_token: NO_EVIDENCE_LOCATABLE`, `contract_unmet_token: CONTRACT_UNMET`.
**Per-field structural keys:** `name`, `field_class`, `judge_rubric_family`, `tier`, `type`, `definition`, `instruction`, `valid_values[{value, definition}]`, optional `source_quote_required`.
**Review-specific:** the 20 field entries and their `valid_values` vocabularies.

**Is there a validator?** No load-time one. `_load_codebook` (`extractor.py:44-47`) is a bare `yaml.safe_load` behind an `lru_cache`. Validation is **lazy and per-accessor**, in `engine/elicitation/classes.py`:

| accessor | line | on malformed input |
|---|---:|---|
| `escape_token()` | 90 | **raises** `CodebookContractError` |
| `contract_unmet_token()` | 105 | **raises** — "a pipeline that can refuse a field must be able to name the refusal" |
| `classes_by_field()` | 149 | **raises**, listing every bad field: *"codebook fields with missing or unknown `field_class`"* |
| `field_class()` | 174 | **raises** for an unknown name |
| `non_value_tokens_for(path)` | 189 | **swallows everything** → `frozenset()`. Deliberate: legacy reviews predate both tokens, and the five read-side consumers must not die for them. Read-side only. |
| `check_evidence_modality()` | 235 | **lint** — returns a list of strings, never raises |

**When does it run?** Only when something calls an accessor — i.e. **on the elicited extraction path**. `check_evidence_modality` has **zero production call sites**: its only callers are `tests/test_codebook_evidence_modality.py`. A codebook with `source_quote_required` on a JUDGMENT field ships unflagged outside the suite.

**A malformed `field_class` therefore fails loudly at prompt-build time and is invisible everywhere else** — including the entire legacy (non-elicited) path, which never calls `classes_by_field` at all.

---

## S3 — Review-specific literals

**AXIS** = topic / specialty / deployment · **SOURCE** = spec-driven / codebook-driven / hardcoded · **REACH** = what breaks.

### Blocking (engine/, hardcoded)

| id | site | axis | source | reach |
|---|---|---|---|---|
| **B1** | `engine/agents/extractor.py:57-58` — `for p in Path("data").glob("*/extraction_codebook.yaml"): return p` | deployment | hardcoded | **The whole extraction + elicitation path.** With two reviews on disk this returns the **first glob hit**, arbitrarily. A second review silently extracts against the autonomy codebook. Reached whenever `codebook_path`/`review_dir` is not passed. |
| **B2** | `engine/analysis/normalize.py:44` — `return load_review_spec("review_specs/surgical_autonomy_v1.yaml")` | topic | hardcoded | Concordance normalisation resolves categorical values against the **wrong** enum set. Silent — no exception, just mismatches. |
| **B3** | `engine/analysis/report.py:25` — same hardcoded literal | topic | hardcoded | Concordance reports render autonomy field names. |
| **B4** | `engine/core/constants.py:10-18` `FT_REASON_CODES` incl. `no_autonomy_content` | topic | hardcoded | FT screening. The Pydantic reason-code enum (`ft_screener.py:34`) and the adjudicator's reason map (`ft_screening_adjudicator.py:23-24`) are two more hand-copies of the same list. |
| **B5** | `engine/agents/screener.py:64-69` primary exclusions; `:88-101` the four verifier tests | topic | hardcoded | Abstract screening. The primary pass **never sees `spec.screening_criteria.exclusion`** (line 61 is the verifier-only branch), so a new review's exclusions do not reach the high-recall pass at all. |
| **B6** | `engine/agents/ft_screener.py:138-139, 180-185` | topic | hardcoded | FT screening prompt asserts "autonomous or semi-autonomous", "not purely teleoperated". |
| **B7** | `engine/adjudication/categorizer.py:185-226` — exclusion-category vocabulary (`autonomous execution`, `robot control`, `semi-autonomous`, `autonomous vehicle`, `drone`, `warehouse`) | topic | hardcoded | Abstract adjudication categorisation. |
| **B8** | `engine/exporters/trace_exporter.py:430-435` `SHARED_FIELDS` — 15 autonomy field names | topic | hardcoded | Disagreement-pair export and its template comments. |
| **B9** | `engine/adjudication/screening_adjudicator.py:27` `EXPANDED_SEARCH_DIR = Path("data/surgical_autonomy/expanded_search")` | deployment | hardcoded | Expanded-search adjudication reads another review's directory. |
| **B10** | `engine/adjudication/screening_adjudicator.py:246, 488`; `ft_screening_adjudicator.py:155-156` | topic | hardcoded | Adjudication HTML instructions to the human reviewer. |
| **B11** | `engine/migrations/003_backfill_expanded_screening.py:19` `DATA_DIR = …/"surgical_autonomy"` | deployment | hardcoded | Migration 003 only. |
| **B12** | `engine/acquisition/pdf_quality_html.py:581` — "Autonomy in Surgical Robotics" in the page subtitle | topic | hardcoded | Cosmetic; appears on every review's quality-check page. |

### Degrading defaults (engine/, hardcoded but overridable)

| id | site | axis | source | reach |
|---|---|---|---|---|
| **D1** | `engine/validators/extraction_validator.py:391`, `engine/agents/ft_screener.py:574` — `--spec` **defaults to** `review_specs/surgical_autonomy_v1.yaml` | deployment | hardcoded default | Runs against the wrong spec if the flag is omitted. |
| **D2** | `engine/migrations/004_pdf_quality_check.py:77`, `005_model_digest.py:66` — `--review` defaults to `surgical_autonomy` | deployment | hardcoded default | Migration hits the wrong DB if the flag is omitted. |
| **D3** | `engine/validators/extraction_validator.py:319`, `engine/analysis/normalize.py:24,79` — prefix-match comments/logic worked example `"2" → "2 (Task autonomy)"` | topic | generic code, autonomy example | Logic is generic; the comment is not. |

### Corpus-derived instrument constants (generic code, autonomy-derived numbers)

| id | site | axis | source | reach |
|---|---|---|---|---|
| **C1** | `engine/parsers/parse_quality.py:22` — thresholds calibrated over "the 190-paper corpus"; docstring marks them **PROVISIONAL** | topic | hardcoded | Parse gate. Absolute (not corpus-relative) by design, so it *runs* on any review — but its pass/fail line was fitted to these PDFs. |
| **C2** | `engine/parsers/font_audit.py:14` — "190 corpus PDFs … 11 carrying the signature" | topic | comment only | None. |
| **C3** | `engine/elicitation/sizing.py:43-44` — `WORST_RATIO = 0.4288` (CAPTURE-01 p719), `INDEX_MARKER_INFLATION = 1.141` (ELICIT-01 §5.6) | topic | hardcoded | Elicited Pass-1 prompt sizing. A corpus with denser tokenisation could exceed the ceiling the estimator was fitted to. |
| **C4** | `engine/elicitation/units.py:53` — `MIN_UNIT_TOKENS = 3`, "study artifact, adopted provisionally" | topic | hardcoded | Unit segmentation. |

### Analysis / test coupling (outside the pipeline)

| id | site | axis | source | reach |
|---|---|---|---|---|
| **A1** | `analysis/provenance/field_class3.py:41-141` `FIELD_CLASS3` — all 20 autonomy fields with per-field justifications; `PAPER_VARIABLE = {"sample_size", "surgical_domain"}` (:39) | topic | hardcoded | Provenance census + the three-way pin. |
| **A2** | `tests/test_codebook_field_class.py:26` `CODEBOOK_PATH = Path("data/surgical_autonomy/extraction_codebook.yaml")`; `:66-68` asserts totals **exactly `{stated: 9, inferable: 6, judgment: 5}`** | topic | hardcoded | Suite. **Bound by path, so adding a review keeps it green** (it is `skipif`-guarded on that file existing, line 29); it turns red only if the autonomy codebook is *replaced* or its classes edited. |
| **A3** | `analysis/eval/schema_eval2.py:52,60` `CARRIED = (39, 386, …, 799)`, `CARRIED_NON_CORPUS = {547, 629, 799}`; mirrored at `tests/_corpus_fixture.py:42-43` and pinned by `tests/test_corpus_authority.py:184-188` | topic | hardcoded | Eval + suite. Ten literal autonomy paper IDs. |
| **A4** | 12 test files reference `data/surgical_autonomy` by path; only 5 carry a `skipif` guard (`test_codebook_field_class`, `test_codebook_evidence_modality`, `test_elicitation_codebook`, `test_elicitation_prompts_ruling2`, `test_prompt_no_hardcoded_fields`). The unguarded 7 include `test_codebook_prompt.py`, `test_elicitation_contracts.py`, `test_distribution_monitor.py`, `test_consensus.py`, `test_human_import.py`, `test_adjudication_pairs.py`, `tests/analysis/paper1/test_judge_pass2.py` | topic | hardcoded | Suite. Fixtures assume autonomy field names and 20-field counts (`test_atomic_terminal_write.py:61,71,94`). |

---

## S4 — Initialization recipe, as observed

**There is no scaffold.** `ReviewDatabase.__init__` (`engine/core/database.py:301-309`) creates everything on first construction:

```python
root = (data_root or DATA_ROOT) / review_name     # DATA_ROOT = Path("data"), line 20
root.mkdir(parents=True, exist_ok=True)
(root / "pdfs").mkdir(exist_ok=True)
(root / "parsed_text").mkdir(exist_ok=True)
(root / "vector_store").mkdir(exist_ok=True)
self.db_path = root / "review.db"
```

then `executescript(_SCHEMA)` plus migrations 006-009 (`:363-396`), unconditionally, on **every** construction — the reason the test fence exists. Migrations **010 and 011 are not wired** (MIGRATION-WIRE-01; they are hand-run).

**Observed sequence for a second review today:**

```
1. write review_specs/<new>_v1.yaml
2. python scripts/run_pipeline.py --spec review_specs/<new>_v1.yaml --name <new>
     → run_pipeline.py:65 ReviewDatabase(<new>) creates data/<new>/{pdfs,parsed_text,vector_store}/ + review.db
     → STAGES = ("search","screen","parse","extract","audit","export")   [run_pipeline.py:41]
3. search   — OK (spec-driven)
4. screen   — RUNS, but the primary pass uses B5's four hardcoded exclusions and B5/B7's autonomy prompts
5. parse    — OK (spec-driven via pdf_parsing)
6. extract  — FIRST HARD FAILURE MODE
```

**Where it first fails: B1.** Nothing creates `data/<new>/extraction_codebook.yaml`, and `_find_codebook_path` (`extractor.py:50-59`) falls through to `Path("data").glob("*/extraction_codebook.yaml")` and **returns the first hit**. If you hand-write a new codebook, the glob picks arbitrarily between two; if you don't, you get autonomy's. Either way it does **not** raise — `FileNotFoundError` fires only when *no* codebook exists anywhere. **The failure is silent and produces plausible-looking output.**

Ahead of that, stage 3 is blocked by the adjudication gate (`run_pipeline.py:97-108`), which is generic.

---

## S5 — Stage-by-stage

| # | stage | verdict | rows |
|---:|---|---|---|
| 1 | SEARCH | **spec-driven** | — |
| 2 | ABSTRACT SCREEN | **hardcoded dependency** | B5, B7 — primary pass never reads `screening_criteria.exclusion` |
| 3 | ACQUIRE | **spec-driven** | — |
| 4 | PARSE | **spec-driven**, corpus-fitted thresholds | C1 |
| 5 | FT SCREEN | **hardcoded dependency** | B4, B6 |
| 6 | EXTRACT | **hardcoded dependency (blocking)** | B1; C3/C4 on the elicited path |
| 7 | CLOUD EXTRACT | **configuration** — see below | — |
| 8 | DISTRIBUTION CHECK | **generic code, dead spec section** | S1 `distribution_monitor` |
| 9 | AUDIT | **spec-driven** (`low_yield_threshold`, `auditor_model`) | — |
| 10 | CONCORDANCE | **hardcoded dependency (blocking)** | B2, B3 |
| 11 | ADJUDICATION GATE | **generic**, review-specific *text* | B7, B9, B10 |
| 12 | EXPORT | **mostly spec-driven** | B8 (trace exporter), B12 |
| 13 | PROVENANCE CENSUS | **hardcoded** | A1 |
| 14 | PI AUDIT / JUDGE | **hardcoded** | A3, A4 |

**Cloud arms are optional by configuration, not by editing code.** `cloud_models` is `Optional[CloudModels] = None` (`review_spec.py:380`) and the live spec omits it; cloud extraction is not in `STAGES` at all — it runs only via `scripts/run_cloud_extraction.py`. Not running that script is the whole opt-out. Its only spec consumer is `methods_section.py`.

---

## S6 — Corpus authority and CARRIED

**`engine/core/corpus.py` is fully GENERIC.** It declares `CORPUS_STATUSES = ("FT_ELIGIBLE", "EXTRACTED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE")` (`:36-41`) with `is_corpus_member()` (`:44`) and `corpus_status_sql()` (`:52`). Lifecycle statuses only — no field, topic, specialty or path. Reusable unchanged. The docstring records why it is a declared constant rather than derived from `ALLOWED_TRANSITIONS`, and `tests/test_corpus_authority.py` pins the tuple whole.

**The CARRIED declaration is REVIEW-BOUND and lives elsewhere:** `analysis/eval/schema_eval2.py:52` `CARRIED = (39, 386, 466, 498, 547, 629, 691, 694, 708, 799)` and `:60` `CARRIED_NON_CORPUS = frozenset({547, 629, 799})` — ten literal autonomy paper IDs, mirrored in `tests/_corpus_fixture.py:42-43` and asserted equal in `tests/test_corpus_authority.py:186`. It is a SCHEMA-EVAL-02 sample declaration, not a corpus predicate. **Do not confuse the two: `corpus.py` carries; `CARRIED` does not.**

---

## S7 — Lane recommendation

**Not "two files and a run" — a GENERALIZE lane of 12 blocking items (B1–B12), of which B1, B2, B3, B5 change results silently rather than failing**; D1–D3 are one-line default changes, C1/C3/C4 are re-derivations to schedule, and A1–A4 are analysis/suite work that can wait because they bind by path and stay green when a review is *added* rather than replaced.

---

## Gates

```
review.db START  size=99770368  mtime=2026-09-09 19:48:46.784974707 +0000
review.db END    size=99770368  mtime=2026-09-09 19:48:46.784974707 +0000   (identical)

git status --porcelain : (empty)
HEAD                   : 633e56b
```

No file written in the repo; no commit; no DB connection opened (S1–S7 answered from source and from two pure-function imports — `load_review_spec` and `yaml.safe_load` on the codebook); no model call. Nothing needed execution, so no section is marked "requires execution".
