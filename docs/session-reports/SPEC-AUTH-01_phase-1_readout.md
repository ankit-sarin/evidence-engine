# SPEC-AUTH-01 phase-1 — recovered read-out

**Provenance header.** This file is a verbatim recovery of one Claude Code
assistant message. Nothing below the header has been edited, summarised or
reformatted.

- **Source transcript:** `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl`
- **Session id:** `4500483d-106b-4e9c-989a-a3314c775cb2`
  - Note: the unified plan v46 attributes this session's work to 2885f251; the work was done in 4500483d, its continuation.
- **Source message line:** L159 (NDJSON record, `type: assistant`)
- **Brief at transcript line:** L6, 2026-09-09T23:35:05.168Z
- **Message timestamp:** 2026-09-09T23:39:01.272Z
- **HEAD stated in read-out:** HEAD not stated in read-out
- **Architect acknowledgement:** L169, 2026-09-09T23:42:12.390Z — "RULINGS on SPEC-AUTH-01 Phase 1 read-out — acknowledged; proceed to Phase 2 under the revised contract below. C4 as written is withdrawn."
  *(user-authored; architect turn, not CC output)*
- **Trust:** Transcript-derived. Values in this report that trace to a tool call in the source transcript at the stated HEAD may be cited as MEASURED. Classifications, counts arrived at by reading, and judgements are INFERRED until re-measured.
- **Recovered by:** READOUT-ARCHIVE-01, 2026-09-11, recovering session `410e9ecc-67a0-44c6-9523-ffad8f528ca3`.
  Compiled from transcript `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl` by session 410e9ecc… (READOUT-ARCHIVE-01) on 2026-09-11.
  *(files written 2026-09-12 00:49 UTC; the session opened 2026-09-11 23:29 UTC and crossed midnight)*

---

Phase 1 complete. No files edited; all measurement was in-memory or read-only. `review.db` untouched (`99770368 B @ 2026-09-09 19:48:46.784974707 UTC` — unchanged).

---

# SPEC-AUTH-01 — PHASE 1 READ-OUT

## R1 — Live spec under a forbid-everywhere copy of the models

Method: imported `engine.core.review_spec`, collected all 16 `BaseModel` subclasses defined in that module, set `model_config["extra"]="forbid"` on each, `model_rebuild(force=True)`, then `ReviewSpec.model_validate(yaml.safe_load(...))`. No file written.

Models forced to forbid (16, all in-module, none nested from elsewhere):
`CloudModelConfig · CloudModels · DistributionMonitorConfig · ExtractionField · ExtractionModels · ExtractionSchema · FTScreeningModels · PDFParsing · PDFQualityCheck · PICO · ParseQuality · ReviewSpec · ScreeningCriteria · ScreeningModels · SearchStrategy · SpecialtyScope`

| spec file | top-level keys | result under `extra='forbid'` everywhere |
|---|---|---|
| `review_specs/surgical_autonomy_v1.yaml` (live, tracked) | 16 | **VALIDATES CLEAN — zero rejected keys** |
| `review_specs/surgical_autonomy_v1_original.yaml` (untracked, gitignored) | 9 | **VALIDATES CLEAN — zero rejected keys** |

Live spec's 16 keys: `authors, date, extraction_schema, ft_screening_models, institutional_proxy_pattern, low_yield_threshold, pdf_quality_check, pico, prospero_id, screening_criteria, screening_models, search_strategy, specialty_scope, title, unpaywall_email, version`.

**I1 HOLDS.** `extra='ignore'` was hiding nothing — no typo, no dead configuration. The 5 undeclared fields of the 21 (`extraction_models`, `auditor_model`, `cloud_models`, `pdf_parsing`, `distribution_monitor`) are absent, not misspelled.

## R2 — Provenance hashing (I2)

`engine/core/review_spec.py:391-402`:

```python
    def screening_hash(self) -> str:
        """SHA-256 of the screening criteria section (canonical JSON)."""
        return _canonical_hash(self.screening_criteria.model_dump())

    def extraction_hash(self) -> str:
        """SHA-256 of the extraction schema section (canonical JSON)."""
        return _canonical_hash(self.extraction_schema.model_dump())
```

`engine/core/review_spec.py:407-410`:

```python
def _canonical_hash(data: dict) -> str:
    """Deterministic SHA-256 hash of a dict via sorted-key JSON."""
    blob = json.dumps(data, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()
```

The only other spec-derived provenance value, `scripts/run_pipeline.py:318-330`:

```python
def _start_review_run(db: ReviewDatabase, spec: ReviewSpec) -> int:
    now = datetime.now(timezone.utc).isoformat()
    cur = db._conn.execute(
        """INSERT INTO review_runs
           (review_spec_hash, screening_hash, extraction_hash,
            started_at, status, log)
           VALUES (?, ?, ?, ?, 'running', '[]')""",
        (
            spec.screening_hash() + spec.extraction_hash(),
            ...
```

`review_spec_hash` is the **string concatenation of the two section hashes** — not a hash of the spec file, the raw YAML, or the `ReviewSpec` object. Grep for `sha256|md5` across `engine/ scripts/ analysis/` intersected with `spec` returns exactly one production hit, `_canonical_hash` itself; the other hits are `codebook_sha256` (a different artifact).

**I2 HOLDS — a top-level `review_id` cannot change any stored provenance value.** Baselines pinned for T6:

```
extraction_hash  d311eb20d1f8c9ea47ef8038a18924198348efce49037292723b2c408b9a6790
screening_hash   0d97b9d61161eeca6c81dd82f895bfb8c6f933b8e8ea23f79056a69f0cf98b90
review_spec_hash 0d97b9d6…f98b90 ‖ d311eb20…9a6790  (concatenation, 128 hex chars)
```

## R3 — Entry points taking a spec and/or a review name

**🔴 I3 IS FALSIFIED.** Not "run_pipeline.py plus a few scripts". Scan of every non-test `.py` for `add_argument("--spec"|"--review"|"--name")`:

| population | count | construct `ReviewDatabase` |
|---|---|---|
| takes `--spec` (with or without a name) | **29** | 15 |
| takes a review name only, **no** `--spec` | **45** | 20 |
| **total** | **74** | **35** |

### The 29 spec-bearing entry points

`db@` / `spec@` are first-occurrence line numbers; **order** is the flow in the entry point.

| file | `--spec` default | name flag | name default | ReviewDatabase | order |
|---|---|---|---|---|---|
| `scripts/run_pipeline.py` | REQUIRED | `--name` | REQUIRED | :65 | spec first |
| `engine/agents/ft_screener.py` | `review_specs/surgical_autonomy_v1.yaml` | `--review` | REQUIRED | :587 | spec first |
| `engine/validators/extraction_validator.py` | `review_specs/surgical_autonomy_v1.yaml` | `--review` | REQUIRED | :396 | spec first |
| `engine/utils/extraction_cleanup.py` | none (auto-discovers) | `--review` | REQUIRED | :253 | spec first |
| `engine/acquisition/check_oa.py` | none | `--review` | REQUIRED | :55 | **DB FIRST** (spec :63) |
| `engine/acquisition/manual_list.py` | none | `--review` | REQUIRED | :124 | **DB FIRST** (spec :129) |
| `engine/acquisition/pdf_quality_check.py` | none | `--review` | REQUIRED | — | spec only |
| `engine/analysis/concordance.py` | none | `--review` | REQUIRED | — | spec only |
| `scripts/run5_extract_and_audit.py` | none → `review_specs/{review}_v1.yaml` | `--review` | `DEFAULT_REVIEW` | :105 | **DB FIRST** (spec :106) |
| `scripts/eval_auditor_models.py` | none → `{review}_v1.yaml` | `--review` | `DEFAULT_REVIEW` | :91 | spec first |
| `scripts/ft_screening_smoke_test.py` | none | `--review` | `DEFAULT_REVIEW` | :59 | spec first |
| `scripts/q8_validation.py` | none → `{review}_v1.yaml` | `--review` | `DEFAULT_REVIEW` | :145 | spec first |
| `scripts/q8_validation_fast.py` | none → `{review}_v1.yaml` | `--review` | `DEFAULT_REVIEW` | :100 | spec first |
| `scripts/reextract_all.py` | none → `{review}_v1.yaml` | `--review` | `DEFAULT_REVIEW` | :48 | spec first |
| `scripts/reextract_failed.py` | none → `{review}_v1.yaml` | `--review` | `DEFAULT_REVIEW` | :47 | spec first |
| `scripts/rescreen_with_specialty.py` | none → `{review}_v1.yaml` | `--review` | `DEFAULT_REVIEW` | :115 | spec first |
| `scripts/rescreen_original_251.py` | none → `{review}_v1.yaml` | `--review` | `DEFAULT_REVIEW` | — | spec only |
| `scripts/screen_expanded.py` | none → `{review}_v1.yaml` | `--review` | `DEFAULT_REVIEW` | — | spec only |
| `scripts/smoke_test_fixes.py` | none → `{review}_v1.yaml` | `--review` | `DEFAULT_REVIEW` | — | spec only |
| `scripts/reparse_cloud_spans.py` | none → `{review}_v1.yaml` | `--review` | `DEFAULT_REVIEW` | — | neither (delegates) |
| `scripts/run_cloud_extraction.py` | none → `{review}_v1.yaml` | `--review` | `DEFAULT_REVIEW` | — | neither (delegates) |
| `analysis/eval/run_capture01.py` | `…surgical_autonomy_v1.yaml` | `--review` | REQUIRED | — | spec only |
| `analysis/eval/run_cloud_strict.py` | `…surgical_autonomy_v1.yaml` | `--review` | REQUIRED | — | spec only |
| `analysis/eval/run_local_ab.py` | `…surgical_autonomy_v1.yaml` | `--review` | REQUIRED | — | spec only |
| `analysis/eval/run_local_abc.py` | `…surgical_autonomy_v1.yaml` | `--review` | REQUIRED | — | spec only |
| `analysis/eval/run_qualgap01.py` | `…surgical_autonomy_v1.yaml` | `--review` | REQUIRED | — | spec only |
| `analysis/eval/smoke_regression01.py` | `…surgical_autonomy_v1.yaml` | `--review` | REQUIRED | — | spec only |
| `analysis/eval/elicit_design01/smoke.py` | `…surgical_autonomy_v1.yaml` | `--review` | `"surgical_autonomy"` | :110 (scratch `data_root`) | **DB FIRST** (spec :187) |
| `analysis/paper1/export_disagreement_pairs.py` | none → `{review}_v1.yaml` | `--review` | `"surgical_autonomy"` | — | spec only |

Four of these construct `ReviewDatabase` **before** the spec is loaded (`check_oa:55`, `manual_list:124`, `run5_extract_and_audit:105`, `elicit_design01/smoke:110` — the last against a scratch root, so harmless). C4's "before `ReviewDatabase` is constructed" requires reordering those three live ones, not just adding a check.

**Six spec-default sites name the autonomy spec literally**, not two as recorded: `ft_screener`, `extraction_validator`, and the five `analysis/eval/run_*` + `smoke_regression01` + `elicit_design01/smoke` runners. Nineteen more derive `review_specs/{review}_v1.yaml` from the name — i.e. today the *name* is authoritative and the spec is derived, exactly inverted from C4.

**Flag divergence:** `run_pipeline.py` alone uses `--name`; the other 28 use `--review`.

**11 files hard-default the name to `surgical_autonomy`** via a module-level `DEFAULT_REVIEW` constant (`rescreen_with_specialty`, `backfill_cloud_spans`, `monitor_extraction`, `q8_validation_fast`, `run5_extract_and_audit`, `retry_parse_6`, `backfill_authors`, `reextract_failed`, `reparse_cloud_spans`, `run_cloud_extraction`, + `eval_auditor_models`/`q8_validation`/`reextract_all`/`ft_screening_smoke_test`/`rescreen_original_251`/`screen_expanded`/`smoke_test_fixes` which import or redefine it).

### The 45 name-only entry points — C4 cannot bind them as written

20 of them construct `ReviewDatabase` with **no spec in scope at all**, so there is nothing to derive a name from or check against:

`analysis/paper1/{adjudication, consensus, human_import, judge_cli, judge_codebook_smoke, pass1_inspection, pass2_branchB_report, pass2_full, pass2_retry_single, pass2_smoke, pi_audit_sampler, pi_audit_sampler_v2, runner_smoke_phase2a}` · `engine/acquisition/{download, verify_downloads}` · `engine/adjudication/advance_stage` · `engine/parsers/pdf_parser` · `engine/validators/distribution_monitor` · `scripts/{parse_expanded_corpus, retry_parse_6}`

Note `analysis/paper1/adjudication.py:125` reconstructs the spec path *from the review name* (`review_specs/{review_name}_v1.yaml`, guarded by `.exists()`), which is the inverse dependency C4 exists to remove.

**Ruling needed:** does C4 bind (a) only the 15 spec-bearing files that construct `ReviewDatabase`, (b) all 29 spec-bearing files, or (c) all 74 — which means adding `--spec` to 45 entry points, several of them Paper-1 analysis tools with no spec dependency today. My recommendation is **(a) plus the 14 spec-bearing non-DB files getting the equality check only**, i.e. all 29, with the 45 name-only files deferred to PATH-AUTH-01 as a named debt. I have not acted on this.

## R4 — Other readers of spec YAML (I4)

**Production code: I4 HOLDS.** `grep yaml.safe_load|yaml.load` over `engine/ scripts/ analysis/` returns 13 sites; the only one reading a review spec is `review_spec.py:426` inside `load_review_spec`. The other 12 read the **extraction codebook** (`extractor.py:47`, `distribution_monitor.py:55`, `completeness.py:135`, `categorizer.py:45`, `judge_loader.py:99/151`, `human_import.py:137`, `consensus.py:54`, `adjudication.py:37`, `pi_audit_sampler_v2.py:217`, `judge_codebook_smoke.py:223`, `adjud01_pairs.py:52`) — a different file, untouched here.

**Tests: six raw-load sites, all derived from the live file, none from scratch.**

| site | what it does | effect of a required `review_id` |
|---|---|---|
| `tests/test_review_spec.py:78, 85, 92, 100` | `yaml.safe_load(SPEC_PATH)` → mutate one key → expect `ReviewSpec.model_validate` to raise | none — `review_id` inherited from the live file |
| `tests/test_cloud_extraction.py:1012→1022, 1039→1049` | load live spec → add `cloud_models` → `yaml.dump` to `tmp_path` → `load_review_spec` | none — inherited |

`ReviewSpec(...)` is **never** constructed positionally or from a hand-built dict anywhere in the repo (grep: one hit, the class definition). No fixture will break on a new required field.

## Findings not covered by R1–R4

1. **`review_specs/surgical_autonomy_v1_original.yaml`** — 4,445 B, untracked and gitignored (`.gitignore:8`), loaded by nothing (zero references in `.py`/`.md`). It validates clean under forbid today but becomes **invalid** the moment `review_id` is required. Inert, but it is a spec file on disk that would no longer load. C2 says "no other change to the live spec" and is silent on this one. **Decision needed:** leave it broken (my recommendation — it is an untracked historical artifact), or add `review_id` to it too.

2. **`DistributionMonitorConfig` is declared-and-dead and `authors`/`prospero_id` are declared-and-dead** (C5) — confirmed, and additionally `distribution_monitor`, `extraction_models`, `auditor_model`, `cloud_models` and `pdf_parsing` are all **absent from the live spec**, so every one of them runs on Pydantic defaults today. That is not a defect (defaults are pinned against the engine by test for `ParseQuality`), but it means the live spec exercises 16 of 22 fields after C2, not 17 of 22.

3. **C3 has a ready seam:** `ReviewDatabase.__init__(review_name, data_root=None)` (`database.py:301-302`) already accepts `data_root`, and `DATA_ROOT` is module-level. A resolver returning `DATA_ROOT / spec.review_id` needs no constructor change, as C3 requires.

---

**Nothing implemented. Awaiting acknowledgement of this read-out, plus rulings on (i) C4's binding scope across the 29 vs 74 entry points, (ii) the three live DB-before-spec reorderings, and (iii) the untracked `_original` spec.**
