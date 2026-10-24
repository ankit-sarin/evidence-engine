# 12w-U17B Phase 1 — U17's record before the fix (R703)

HEAD at the reads: `e5e23b755d25d713ca23d4162d08424cb78180d7` (item 0: R703 as Block 101), parent
`a05d0e0`. Read-only on engine/, tests/ and the live spec (sha256 `917c24de…4e5f`, unchanged).

## 2a — Findings 6 and 7 of 12w_U17A.md §6, quoted (R703(a))

> 6. **`model_copy` bypasses spec validation.** 50 `model_copy(update=` sites in tests/ and
>    `pin_tuples.py`'s `spec0.model_copy(update={"extraction_models": …})`; the reproducer builds its
>    same-family spec that way.
> 7. **The per-role form declares the extractor's family twice.** `extraction_models.extractor_family`
>    and `arms[local_deepseek_r1_32b].family` (`scratch_spec/per_role_vendor.diff`), which can disagree
>    as `extractor` and `arm.model` could before `_arms_consistent`.

## 2b — I1: every model the resolved live spec names (MEASURED)

Command: `stage_config(stage, spec).model` for every `OLLAMA_STAGES` entry but `preflight` (whose model
is the caller's), plus `spec.arms`, on `load_review_spec("review_specs/surgical_autonomy.yaml")`.

| model (exact string) | named by | source | lineage (R703(b)) |
|---|---|---|---|
| `qwen3:8b` | `screening_models.primary` | spec | qwen |
| `gemma3:27b` | `screening_models.verification`, `ft_screening_models.verifier`, `audit.model` | spec, spec, **default factory** (`AuditModels`; no `audit:` block, `auditor_model` None) | gemma |
| `qwen3:32b` | `ft_screening_models.primary` | spec | qwen |
| `deepseek-r1:32b` | `extraction_models.extractor`, `arms[local_deepseek_r1_32b].model` | spec | qwen |
| `qwen2.5vl:7b` | `pdf_parsing.vision_model`, `pdf_quality_check.ai_model` | **default factory** (`PDFParsing`), spec | qwen |
| `o4-mini-2025-04-16` | `arms[openai_o4_mini_2025_04_16_high].model` (provider openai) | spec | openai |
| `claude-sonnet-4-6` | `arms[anthropic_claude_sonnet_4_6].model` (provider anthropic) | spec | anthropic |

No other model is named: `preflight` probes models the run's other stages name; the overridden
declared defaults (`ScreeningModels.verification` `qwen3:32b`, `FTScreeningModels.primary`
`qwen3.5:27b`) are not resolved on the live spec. Callers can still name a model outside the spec at
call time (`stage_config(..., model=)` in `screener.py:123` and `ollama_preflight.py:68`;
`semantic_verify(model=)`), which inside a run `UndeclaredCall` refuses. **I1 holds**: every lineage is
qwen, gemma, openai or anthropic, as the brief lists.

## 2c — I2: the test sites the fix will break (MEASURED)

Command: the census plugin `census/u17_census_plugin.py`, loaded with `-p` (never under tests/), over
the standard gate's selection with `--basetemp` under `~/scratch/12w-u17b/` (R703(j)). It records —
never raises — every `ReviewSpec.model_validate` of a dict without `model_lineage` that is not the live
spec's dict, every `open_run` whose declared stages, cloud arms or preflight models name a model
outside 2b's map, and every same-lineage extraction→audit pair. Run: `3358 passed, 17 deselected, 1
xfailed, 2 warnings in 887.30s`, exit 0 (`census/census_pytest_tail.txt`); 18 records
(`census/census_summary.txt`): 15 LOAD, 3 OPEN_COVERAGE, 0 OPEN_PAIR, 0 plugin errors.

The 15 LOAD records are dicts read from the live YAML and edited in one or two keys (`cloud`, `arms`,
`review_id`, `eligibility`); after the fix they carry the live map. They break only where the edit
introduces a model outside it (read from each test's `mutate`). **Sites the fix breaks: 5**

| test id | where it breaks | why |
|---|---|---|
| `tests/test_cloud_extraction.py::TestSpecDrivenConfig::test_openai_reads_from_spec` | spec load | `a["model"] = "o4-mini-custom"` (line 1065) has no `model_lineage` entry |
| `tests/test_cloud_extraction.py::TestSpecDrivenConfig::test_anthropic_reads_from_spec` | spec load | `a["model"] = "claude-custom-model"` (line 1079) has no entry |
| `tests/test_exporters.py::test_methods_counts_papers_not_calls` | open_run coverage | `RUN_EXTRACTOR = "fixture-extractor:x"`, `RUN_AUDITOR = "fixture-auditor:y"` (lines 462–463) via `model_copy` |
| `tests/test_exporters.py::test_methods_uses_db_audit_model` | open_run coverage | the same fixture |
| `tests/test_exporters.py::test_methods_uses_db_extraction_model` | open_run coverage | the same fixture |

The other 13 LOAD records (`test_no_default_price`, `test_no_default_arm`,
`test_codebook_review_at_open.py::test_run_cloud_extractions_run_arm_refuses_before_any_row_or_send`, three
`test_failed_close_reason.py` cloud tests, two `test_review_paths.py` mismatch tests, four
`test_review_spec.py::test_valid_review_id_accepted[…]`, `test_screen2f_harness.py::test_arm_c_spec_adds_exactly_four_stage_lines`)
change no model; an entry naming no model is permitted (R703(d)), so `test_no_default_arm`'s removed
arm leaves a harmless entry. No test pins the manifest's stage-row shape: grep over tests/ for
`["stages"]` on a manifest body finds pin-tuple stage keys only (`test_run_manifest.py:213`,
`test_pin_records.py:171`, `test_arm_pinned_at_extract.py:219`), which R703 does not change.
**I2 holds** (5 ≤ 30). Not seen by the census: a test that would break on a manifest-body key it asserts
absent; none found by grep.

## 2d — I3 and I4 (READ)

**I3 — does NOT hold as the brief states it.** `open_run` reads `/api/tags` through `resolve_run`'s
default `digest_fn`, `fetch_model_digest` (`engine/core/effective_config.py`: "`digest_fn` defaults to
`fetch_model_digest` (`/api/tags`; R57)"). The response carries `details.family` per model, but
`fetch_model_digest` returns only the digest and discards the entry
(`engine/utils/ollama_client.py`):

```python
    matches = [m for m in models if isinstance(m, dict) and m.get("name") == model_name]
    …
    digest = matches[0].get("digest")
```

So recording the server family without a new request needs a change to `engine/utils/ollama_client.py`
(keep or hand out `matches[0]["details"]["family"]` from the read it already makes), a file that is not
in the brief's Touched list. Without that change, the only ways to record it are a second `/api/tags`
request (which R703(c) forbids: "no new request") or `null` for every model (the brief's I3 fallback),
under which T3 as written cannot pass. 15 test files patch or name `fetch_model_digest`
(`grep -rl fetch_model_digest tests | wc -l`), so its signature is load-bearing for any change made there.

**I4 — holds.** `pin_tuple` (`engine/core/run_manifest.py`) stores, per arm stage:

```python
    stages = {
        key: {"model": r.config.model, "model_digest": r.model_digest,
              "options_hash": r.options_hash,
              "format_schema_hash": r.format_schema_hash,
              "prompt_hash": r.prompt_hash}
        for key, r in sorted(resolved.items()) if r.arm_name == arm_name
    }
```

and the pin is written as `arms.configuration_json = canonical_json(tup)` by `bind_extraction_arm`
(R569(b)); the extraction stages are the only arm-bearing local stages (`_stage_arm`), so the pin names
the model of every extraction stage the arm was pinned with.

## 2e — T1–T10 on HEAD (MEASURED)

`phase1/test_u17_cross_lineage.py` (13 ids: T5 parametrised four ways), run with an explicit path and
`--basetemp` under `~/scratch/12w-u17b/`: **`13 failed in 7.11s`** (`phase1_head_run.txt`). How each
fails on HEAD:

| test | failure on HEAD |
|---|---|
| T1, T2 | `AttributeError: module 'engine.core.run_manifest' has no attribute 'SameLineageVerifier'` (the open itself would succeed: 12w_U17A's reproducer) |
| T3 | the open succeeds with the patched `/api/tags`; `KeyError: 'lineage'` in the stage row |
| T4, T5 ×4 | `KeyError: 'model_lineage'` (the live YAML has no map to edit) |
| T6 | `AttributeError: … no attribute 'ModelLineageMissing'` |
| T7, T8 | the audit-only open succeeds; `KeyError: 'lineage_check'` |
| T9 | `AttributeError: 'ReviewSpec' object has no attribute 'model_lineage'` |
| T10 | `AttributeError: 'ReviewSpec' object has no attribute 'model_lineage'` |

T9's reference `screening_hash` is measured on HEAD (`screening_hash_head.txt`:
`e8fa9719b1028ac16f9af72996bc807c8ad8b1071d2420fc5d98ed826f043596`).

## Stop

Per the brief's CONTRADICTION HANDLING ("Any finding that contradicts an assumption … STOP at that
step"), the fix (step 3) is not started: I3 is false as stated, and its own fallback (record null)
contradicts T3 and R703(c)'s "recorded beside each declared lineage". The decision is the ruling's:
(i) add `engine/utils/ollama_client.py` to U17's files so the existing `/api/tags` read hands out the
family; or (ii) record `null` for every model's server family and amend T3.
