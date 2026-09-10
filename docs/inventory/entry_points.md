# Entry-point and authority-reader inventory

**GENERATED — DO NOT EDIT.** Regenerate with `python -m engine.tools.inventory --write`.
Generated at commit `caaac9372dbb79a1dc169572c81a5224d27e08b4` by `engine/tools/inventory.py`, AST only — no scanned module is imported and no database is opened.
A drift test at the standard gate fails if this file's JSON twin stops matching the tree.

## Summary

| count | value |
|---|---:|
| files scanned | 197 |
| entry points | 98 |
| entry points with spec flag | 29 |
| entry points with review name flag | 74 |
| entry points name only | 45 |
| entry points constructing reviewdatabase | 37 |
| name only constructing reviewdatabase | 20 |
| files calling resolver | 34 |
| files calling load review spec directly | 8 |
| raw yaml load sites | 3 |
| files with raw yaml loads | 3 |
| review id constants | 8 |
| literal review id sites in code | 34 |
| path construction sites in code | 62 |
| db before spec scopes | 0 |
| fstring spec path sites | 0 |
| default review named constants | 7 |
| unparsed sites | 0 |

Review ids on disk: `surgical_autonomy`

`data/` subdirectories NOT counted as reviews (no `review.db`):

| directory | why |
|---|---|
| `backups` | no review.db |
| `my_review` | no review.db |
| `myreview` | no review.db |
| `review` | no review.db |
| `test_review` | no review.db |

## Reconciliation against the hand-built inventories

Baselines are the figures measured by hand in GENERALIZE-READOUT-01 and SPEC-AUTH-01 Phase 1. A difference is not automatically a defect — SPEC-AUTH-01 changed several of these deliberately — but every one is named here rather than left for a reader to notice.

| figure | hand-built | measured now | |
|---|---:|---:|---|
| argparse entry points naming a review | 74 | 74 | matches |
| of those, spec-bearing | 29 | 29 | matches |
| of those, name-only | 45 | 45 | matches |
| entry points constructing ReviewDatabase | 35 | 37 | the hand scan enumerated files by argparse FLAG, so it could not see an entry point that constructs a database without a --review/--name flag; the tool finds those through the __main__ guard instead |
| name-only, constructing ReviewDatabase | 20 | 20 | matches |
| raw yaml load sites | 13 | 3 | differs — unexplained, investigate |
| f-string spec-path builders | 19 | 0 | SPEC-AUTH-01 moved every one of these onto the resolver; a non-zero value here means a hand-built spec path has come back |
| DEFAULT_REVIEW constants | 7 | 7 | matches |

Two figures deliberately have no baseline row. **entry points** (98) counts anything with argparse flags or a `__main__` guard, which is a wider net than the hand scan's review-naming CLIs. And the hand-built note that 12 of the 13 raw YAML loads are codebook readers is a semantic judgement about what a file MEANS; this tool reports the call site and its target expression and makes no such claim.

## Entry points

| file | flags | spec access | ReviewDatabase | order |
|---|---|---|---|---|
| `analysis/eval/adjud01_pairs.py` | `--review` required; `--data-root`='data'; `--score` | — | — | — |
| `analysis/eval/analyze_capture01.py` | `--review` required; `--data-root`='data'; `--label`=dynamic: LABEL; `--out`=None | — | — | — |
| `analysis/eval/analyze_prime01.py` | `--review` required; `--data-root`='data' | — | — | — |
| `analysis/eval/analyze_qualgap01.py` | `--review` required; `--data-root`='data' | — | — | — |
| `analysis/eval/analyze_schema_eval.py` | `--review` required; `--data-root`='data' | — | — | — |
| `analysis/eval/analyze_schema_eval2.py` | `--review` required; `--data-root`='data' | — | — | — |
| `analysis/eval/elicit01/analyze.py` | `--review`='surgical_autonomy'; `--data-root`='data'; `--out`='analysis_summary.json' | — | — | — |
| `analysis/eval/elicit01/manifest.py` | — | — | — | — |
| `analysis/eval/elicit01/runner.py` | `--review`='surgical_autonomy'; `--data-root`='data'; `--smoke`=0; `--resume`; `--restart-every`=dynamic: RESTART_EVERY_N | — | — | — |
| `analysis/eval/elicit_design01/smoke.py` | `--review` required; `--data-root`='data'; `--spec`=None; `--papers`=dynamic: ','.join((str(p) for p in SMOKE_PAPERS)) | load_spec_for | 110 | — |
| `analysis/eval/parse01/flag.py` | — | — | — | — |
| `analysis/eval/parse01/sweep.py` | `--review` required; `--data-root`='data' | — | — | — |
| `analysis/eval/run_capture01.py` | `--review` required; `--data-root`='data'; `--spec`=None; `--label`=dynamic: LABEL; `--smoke`=0; `--resume`; `--restart-every`=dynamic: RESTART_EVERY_N | load_spec_for | — | — |
| `analysis/eval/run_cloud_strict.py` | `--review` required; `--data-root`='data'; `--spec`=None; `--n-papers`=5; `--arms`='openai,anthropic' | load_spec_for | — | — |
| `analysis/eval/run_local_ab.py` | `--review` required; `--data-root`='data'; `--spec`=None; `--n-papers`=10 | load_spec_for | — | — |
| `analysis/eval/run_local_abc.py` | `--review` required; `--data-root`='data'; `--spec`=None; `--label`='local_abc'; `--resume` | load_spec_for | — | — |
| `analysis/eval/run_qualgap01.py` | `--review` required; `--data-root`='data'; `--spec`=None; `--host`=dynamic: DEFAULT_HOST; `--label`='runtime_v12'; `--probe`; `--cells`=dynamic: list(CELLS); `--resume` | load_spec_for | — | — |
| `analysis/eval/smoke_regression01.py` | `--review` required; `--data-root`='data'; `--spec`=None | load_spec_for | — | — |
| `analysis/paper1/adjudication.py` | `--review` required; `--arms` required; `--output` required; `--codebook`=None; `--decisions` required; `--review` required | load_review_spec, spec_path_for | 596 | — |
| `analysis/paper1/consensus.py` | `--review` required; `--codebook`=None; `--dry-run`; `--review` required | — | 427 | — |
| `analysis/paper1/export_disagreement_pairs.py` | `--review` required; `--spec`=None | load_review_spec, load_spec_for, spec_path_for | — | — |
| `analysis/paper1/human_import.py` | `--workbook` required; `--review` required; `--codebook`=None; `--dry-run` | — | 302 | — |
| `analysis/paper1/judge_cli.py` | `--review` required; `--input` required; `--pairs-csv`; `--codebook` required; `--pass`=1; `--limit`=0; `--dry-run`; `--model`=dynamic: DEFAULT_MODEL; `--run-note`=None; `--data-root`=None | — | 191, 192 | — |
| `analysis/paper1/judge_codebook_smoke.py` | `--review` required; `--pairs-csv` required; `--codebook` required; `--audit-dir`=dynamic: Path('artifacts/paper1/pi_audit'); `--completed`=None; `--key`=None; `--out-dir`=dynamic: Path('analysis/paper1/reports'); `--log-dir`=dynamic: Path('analysis/paper1/logs'); `--model`=dynamic: DEFAULT_MODEL; `--run-id`=dynamic: RECORDED_RUN_ID; `--limit`=None; `--data-root`=None; `--builder`='production'; `--assert-v2`=None; `--background` | — | 1186, 1187 | — |
| `analysis/paper1/judge_provenance.py` | `--review` required; `--data-root`='data'; `--judge-run-id`=None; `--census-run-id`=None; `--legacy-csv`=None; `--out-dir`=None | — | — | — |
| `analysis/paper1/pass1_inspection.py` | `--review` required; `--run-id` required; `--pairs-csv` required; `--codebook` required; `--out-dir`=dynamic: Path('analysis/paper1/reports'); `--data-root`=None | — | 554, 555 | — |
| `analysis/paper1/pass2_branchB_report.py` | `--review` required; `--run-id` required; `--pairs-csv` required; `--codebook` required; `--run-log` required; `--out-dir`=dynamic: Path('artifacts/paper1'); `--data-root`=None | — | 541, 542 | — |
| `analysis/paper1/pass2_full.py` | `--review` required; `--pass1-run-id` required; `--pairs-csv` required; `--codebook` required; `--out-dir`=dynamic: Path('analysis/paper1/reports'); `--log-dir`=dynamic: Path('analysis/paper1/logs'); `--model`=dynamic: DEFAULT_MODEL; `--run-tag`='full'; `--limit`=None; `--seed-run-id`=None; `--resume-run-id`=None; `--restart-every`=0; `--dry-run`; `--data-root`=None | — | 662, 663 | — |
| `analysis/paper1/pass2_retry_single.py` | `--review` required; `--run-id` required; `--paper-id` required; `--field-name` required; `--pairs-csv` required; `--codebook` required; `--model`=dynamic: DEFAULT_MODEL; `--log-dir`=dynamic: Path('analysis/paper1/logs'); `--data-root`=None | — | 137, 139 | — |
| `analysis/paper1/pass2_smoke.py` | `--review` required; `--pass1-run-id` required; `--pairs-csv` required; `--codebook` required; `--out-dir`=dynamic: Path('analysis/paper1/reports'); `--log-dir`=dynamic: Path('analysis/paper1/logs'); `--model`=dynamic: DEFAULT_MODEL; `--dry-run`; `--data-root`=None; `--run-tag`='' | — | 797, 798 | — |
| `analysis/paper1/pi_audit_sampler.py` | `--review` required; `--out-dir`=dynamic: Path('artifacts/paper1/pi_audit'); `--data-root`=None; `--supersedes`=None; `--regeneration-reason`=None | — | 1340, 1341 | — |
| `analysis/paper1/pi_audit_sampler_v2.py` | `--review` required; `--codebook` required; `--pairs-csv`=None; `--out-dir`=dynamic: Path('artifacts/paper1/pi_audit_v2'); `--data-root`=None | — | 1205, 1206 | — |
| `analysis/paper1/pi_audit_unblind.py` | `--completed`=None; `--key`=None; `--audit-dir`=dynamic: Path('artifacts/paper1/pi_audit'); `--out-dir`=None | — | — | — |
| `analysis/paper1/runner_smoke_phase2a.py` | `--review` required; `--pass1-run-id` required; `--pairs-csv` required; `--codebook` required; `--prior-run-id` required; `--limit`=3; `--model`=dynamic: DEFAULT_MODEL | — | 87 | — |
| `analysis/paper1/spanloss_autopsy.py` | `--review` required; `--data-root`='data'; `--out-dir`=None | — | — | — |
| `analysis/provenance/census.py` | `--review` required; `--data-root`='data'; `--out-dir`=None; `--threshold`=dynamic: C.THRESHOLD_PRIMARY; `--strict-variant`; `--legacy`; `--workers`=10; `--no-persist`; `--notes`='' | — | — | — |
| `analysis/provenance/recount.py` | `--review` required; `--data-root`='data'; `--run-id`=None; `--out-dir`=None | — | — | — |
| `engine/acquisition/check_oa.py` | `--review` required; `--spec`=None; `--background` | load_spec_for | 62, 139, 160 | check_oa_status:spec_first |
| `engine/acquisition/download.py` | `--review` required; `--retry`; `--background` | — | 343 | — |
| `engine/acquisition/manual_list.py` | `--review` required; `--spec`=None; `--background` | load_spec_for | 128 | generate_manual_list:spec_first |
| `engine/acquisition/pdf_quality_check.py` | `--review` required; `--spec`=None; `--dry-run`; `--limit`=None | load_spec_for | — | — |
| `engine/acquisition/pdf_quality_html.py` | `--review` required; `--mode`='quality_check'; `--output`=None | — | — | — |
| `engine/acquisition/pdf_quality_import.py` | `--review` required; `--input` required; `--dry-run` | — | — | — |
| `engine/acquisition/verify_downloads.py` | `--review` required; `--pdf-dir`; `--dry-run` | — | 171 | — |
| `engine/adjudication/abstract_adjudication_html.py` | `--review` required; `--output`=None | — | — | — |
| `engine/adjudication/advance_stage.py` | `--review` required; `--stage`; `--note`; `--force`; `--status` | — | 73 | — |
| `engine/adjudication/ft_adjudication_html.py` | `--review` required; `--output`=None | — | — | — |
| `engine/agents/ft_screener.py` | `--review` required; `--spec`=None; `--screen-only`; `--verify-only`; `--background` | load_spec_for | 587 | <module>:spec_first |
| `engine/analysis/concordance.py` | `--review` required; `--arms` required; `--spec`=None | data_root_for, load_review_spec, load_spec_for, spec_path_for | — | — |
| `engine/migrations/002_screening_rename.py` | — | — | — | — |
| `engine/migrations/003_backfill_expanded_screening.py` | — | — | — | — |
| `engine/migrations/004_pdf_quality_check.py` | `--review`='surgical_autonomy' | — | — | — |
| `engine/migrations/005_model_digest.py` | `--review`='surgical_autonomy' | — | — | — |
| `engine/migrations/006_not_null_confidence_tier.py` | — | — | — | — |
| `engine/migrations/007_add_judge_tables.py` | — | — | — | — |
| `engine/migrations/008_add_fabrication_verifications.py` | — | — | — | — |
| `engine/migrations/009_add_backfill_audit_log.py` | — | — | — | — |
| `engine/migrations/010_add_provenance_classifications.py` | — | — | — | — |
| `engine/migrations/011_add_absence_claim_class.py` | — | — | — | — |
| `engine/migrations/012_codebook_provenance.py` | — | — | — | — |
| `engine/parsers/pdf_parser.py` | `--verify-hashes`; `--review` required | — | 1168 | — |
| `engine/review/extraction_audit_html.py` | `--review` required; `--output`=None | — | — | — |
| `engine/tools/inventory.py` | `--write`; `--check` | — | — | — |
| `engine/utils/extraction_cleanup.py` | `--review` required; `--keep-schema`; `--spec`=None; `--confirm` | load_spec_for | 238 | — |
| `engine/utils/ollama_preflight.py` | `--models` required; `--timeout`=30 | — | — | — |
| `engine/validators/distribution_monitor.py` | `--review` required; `--arm` required; `--codebook`=None; `--strict` | — | 441 | — |
| `engine/validators/extraction_validator.py` | `--review` required; `--spec`=None | load_spec_for | 397 | main:spec_first |
| `scripts/_pass2_delta.py` | — | — | — | — |
| `scripts/_pass2_eyeball.py` | — | — | — | — |
| `scripts/_pass2_stability.py` | — | — | 62 | — |
| `scripts/advance_to_pdf_acquired.py` | `--review`=dynamic: DEFAULT_REVIEW | — | — | — |
| `scripts/backfill_authors.py` | `--review`=dynamic: DEFAULT_REVIEW; `--dry-run` | — | — | — |
| `scripts/backfill_cloud_spans.py` | `--review`=dynamic: DEFAULT_REVIEW; `--confirm`; `--db`=None | — | — | — |
| `scripts/eval_auditor_models.py` | `--review` required; `--spec`=None | load_spec_for, spec_path_for | 87 | run_eval:spec_first |
| `scripts/ft_screening_smoke_test.py` | `--review` required; `--spec`=None | load_spec_for, spec_path_for | 55 | main:spec_first |
| `scripts/monitor_extraction.py` | `--review`=dynamic: DEFAULT_REVIEW | — | — | — |
| `scripts/parse_expanded_corpus.py` | `--review`=dynamic: DEFAULT_REVIEW | — | 40 | — |
| `scripts/pdf_acquisition/step1_export_citations.py` | — | — | — | — |
| `scripts/pdf_acquisition/step2_unpaywall_check.py` | — | — | — | — |
| `scripts/pdf_acquisition/step3_download_oa_pdfs.py` | — | — | — | — |
| `scripts/pdf_acquisition/step3b_retry_failed.py` | — | — | — | — |
| `scripts/pdf_acquisition/step4_manual_download_list.py` | — | — | — | — |
| `scripts/prepare_concordance_pdfs.py` | `--review`=dynamic: DEFAULT_REVIEW | — | — | — |
| `scripts/q8_validation.py` | `--review` required; `--spec`=None | load_spec_for, spec_path_for | 142 | main:spec_first |
| `scripts/q8_validation_fast.py` | `--review` required; `--spec`=None; `paper_ids`=dynamic: [370, 432] | load_spec_for, spec_path_for | 97 | main:spec_first |
| `scripts/reextract_all.py` | `--review` required; `--spec`=None | load_spec_for, spec_path_for | 44 | main:spec_first |
| `scripts/reextract_failed.py` | `--review` required; `--spec`=None | load_spec_for, spec_path_for | 43 | main:spec_first |
| `scripts/reparse_cloud_spans.py` | `--review` required; `--spec`=None | load_spec_for, spec_path_for | — | — |
| `scripts/rescreen_original_251.py` | `--review` required; `--spec`=None | load_spec_for, spec_path_for | — | — |
| `scripts/rescreen_with_specialty.py` | `--review` required; `--spec`=None; `--background`; `--verify-only`; `--report-only` | load_spec_for, spec_path_for | 112 | main:spec_first |
| `scripts/retry_parse_6.py` | `--review`=dynamic: DEFAULT_REVIEW | — | 113 | — |
| `scripts/run5_extract_and_audit.py` | `--review` required; `--spec`=None; `--retry-failed`; `--paper-ids`; `--restart-every`=25 | load_spec_for, spec_path_for | 105 | main:spec_first |
| `scripts/run_cloud_extraction.py` | `--review` required; `--arm`; `--spec`=None; `--db`=None; `--max-papers`=None; `--max-cost`=None; `--progress`; `--dry-run` | data_root_for, load_spec_for, spec_path_for | — | — |
| `scripts/run_pipeline.py` | `--review/--name` required; `--spec`=None; `--skip-to`=None; `--limit`=None | load_spec_for | 74 | run_pipeline:spec_first |
| `scripts/screen_expanded.py` | `--review` required; `--spec`=None; `--fetch-only`; `--screen-only`; `--verify-only`; `--fresh` | load_review_spec, load_spec_for, spec_path_for | — | — |
| `scripts/smoke_test_fixes.py` | `--review` required; `--spec`=None | load_spec_for, spec_path_for | — | — |
| `scripts/test_e2e_search_screen.py` | — | load_review_spec | 63 | main:spec_first |
| `scripts/test_extraction_validation.py` | — | load_review_spec | — | — |

## Raw YAML loads (outside the spec loader)

| file | line | call | target |
|---|---:|---|---|
| `engine/adjudication/categorizer.py` | 45 | `safe_load` | `f` |
| `engine/core/codebook.py` | 407 | `safe_load` | `text` |
| `engine/core/review_spec.py` | 496 | `safe_load` | `f` |

## Path construction in code

Strings carrying `review_specs`, `data/` or `.yaml` outside docstrings, argparse prose, and messages passed to an exception or a logger. This is the class a filename grep cannot see: an f-string that builds the path from a variable.

| file | line | kind | text |
|---|---:|---|---|
| `analysis/eval/adjud01_pairs.py` | 162 | literal | `extraction_codebook.yaml` |
| `analysis/eval/analyze_capture01.py` | 185 | literal | `data/{review}/parsed_text/{pid}_v*.md, newest version` |
| `analysis/eval/elicit01/analyze.py` | 67 | literal | `extraction_codebook.yaml` |
| `analysis/eval/elicit01/manifest.py` | 104 | literal | `data/surgical_autonomy` |
| `analysis/eval/elicit01/manifest.py` | 105 | literal | `extraction_codebook.yaml` |
| `analysis/eval/elicit01/runner.py` | 148 | literal | `extraction_codebook.yaml` |
| `analysis/eval/elicit_design01/smoke.py` | 112 | literal | `extraction_codebook.yaml` |
| `analysis/eval/elicit_design01/smoke.py` | 113 | literal | `extraction_codebook.yaml` |
| `analysis/eval/parse01/flag.py` | 63 | literal | `data/surgical_autonomy/eval/parse01/sweep.jsonl` |
| `analysis/eval/run_cloud_strict.py` | 176 | literal | `extraction_codebook.yaml` |
| `analysis/eval/run_local_ab.py` | 162 | literal | `extraction_codebook.yaml` |
| `analysis/eval/run_local_abc.py` | 198 | literal | `extraction_codebook.yaml` |
| `analysis/eval/run_qualgap01.py` | 282 | literal | `extraction_codebook.yaml` |
| `analysis/eval/smoke_regression01.py` | 53 | literal | `extraction_codebook.yaml` |
| `analysis/paper1/adjudication.py` | 603 | literal | `extraction_codebook.yaml` |
| `analysis/paper1/consensus.py` | 433 | literal | `extraction_codebook.yaml` |
| `analysis/paper1/human_import.py` | 307 | literal | `extraction_codebook.yaml` |
| `analysis/paper1/pi_audit_sampler_v2.py` | 124 | literal | `data/surgical_autonomy/exports/disagreement_pairs_3arm.csv` |
| `engine/acquisition/pdf_quality_html.py` | 378 | f-string | `f'''<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width, initial-scale=1">\n<title>PDF Acqui...` |
| `engine/adjudication/categorizer.py` | 19 | literal | `adjudication_categories.yaml` |
| `engine/adjudication/screening_adjudicator.py` | 27 | literal | `data/surgical_autonomy/expanded_search` |
| `engine/adjudication/workflow.py` | 68 | literal | `Create or update adjudication_categories.yaml for this review.
  Location: data/<review>/adjudication_categories.yaml
  Generate a starter template with: gen...` |
| `engine/agents/auditor.py` | 272 | literal | `extraction_codebook.yaml` |
| `engine/agents/extractor.py` | 651 | literal | `extraction_codebook.yaml` |
| `engine/analysis/concordance.py` | 78 | literal | `extraction_codebook.yaml` |
| `engine/cloud/base.py` | 421 | literal | `extraction_codebook.yaml` |
| `engine/core/codebook.py` | 36 | literal | `extraction_codebook.yaml` |
| `engine/core/review_paths.py` | 28 | literal | `review_specs` |
| `engine/core/review_paths.py` | 59 | f-string | `f'{review_id}.yaml'` |
| `engine/tools/inventory.py` | 34 | literal | `review_specs` |
| `engine/tools/inventory.py` | 47 | literal | `.yaml` |
| `engine/tools/inventory.py` | 47 | literal | `data/` |
| `engine/tools/inventory.py` | 47 | literal | `review_specs` |
| `engine/tools/inventory.py` | 67 | literal | `*.yaml` |
| `engine/tools/inventory.py` | 431 | literal | `review_specs` |
| `engine/tools/inventory.py` | 498 | literal | ``data/` subdirectories NOT counted as reviews (no `review.db`):` |
| `engine/tools/inventory.py` | 533 | literal | `Strings carrying `review_specs`, `data/` or `.yaml` outside docstrings, argparse prose, and messages passed to an exception or a logger. This is the class a ...` |
| `engine/tools/inventory.py` | 682 | literal | `data/ subdirectories changed: ` |
| `engine/validators/distribution_monitor.py` | 446 | literal | `extraction_codebook.yaml` |
| `engine/validators/extraction_validator.py` | 64 | literal | `extraction_codebook.yaml` |
| `scripts/_pass2_delta.py` | 13 | literal | `data/surgical_autonomy/review.db` |
| `scripts/_pass2_delta.py` | 16 | literal | `data/surgical_autonomy/exports/disagreement_pairs_3arm.csv` |
| `scripts/_pass2_eyeball.py` | 15 | literal | `data/surgical_autonomy/review.db` |
| `scripts/_pass2_eyeball.py` | 17 | literal | `data/surgical_autonomy/exports/disagreement_pairs_3arm.csv` |
| `scripts/_pass2_stability.py` | 25 | literal | `data/surgical_autonomy/review.db` |
| `scripts/_pass2_stability.py` | 27 | literal | `data/surgical_autonomy/exports/disagreement_pairs_3arm.csv` |
| `scripts/backfill_authors.py` | 98 | f-string | `f'data/{args.review}/review.db'` |
| `scripts/backfill_cloud_spans.py` | 42 | f-string | `f'data/{args.review}/review.db'` |
| `scripts/eval_auditor_models.py` | 269 | f-string | `f'data/{review_name}/auditor_eval_results.json'` |
| `scripts/monitor_extraction.py` | 51 | f-string | `f'data/{args.review}/extract_log.txt'` |
| `scripts/monitor_extraction.py` | 52 | f-string | `f'data/{args.review}/review.db'` |
| `scripts/pdf_acquisition/step4_manual_download_list.py` | 121 | f-string | `f"""<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width, initial-scale=1">\n<title>Manual PD...` |
| `scripts/reparse_cloud_spans.py` | 33 | f-string | `f'data/{review}/review.db'` |
| `scripts/rescreen_original_251.py` | 52 | f-string | `f'data/{review}/review.db'` |
| `scripts/rescreen_original_251.py` | 53 | f-string | `f'data/{review}/expanded_search'` |
| `scripts/run5_extract_and_audit.py` | 149 | literal | `extraction_codebook.yaml` |
| `scripts/screen_expanded.py` | 510 | f-string | `f'data/{review}/expanded_search'` |
| `scripts/smoke_test_fixes.py` | 45 | f-string | `f'data/{review}'` |
| `scripts/test_e2e_search_screen.py` | 29 | literal | `review_specs` |
| `scripts/test_e2e_search_screen.py` | 29 | literal | `surgical_autonomy.yaml` |
| `scripts/test_extraction_validation.py` | 33 | literal | `review_specs` |
| `scripts/test_extraction_validation.py` | 33 | literal | `surgical_autonomy.yaml` |

## Literal review ids in code

| file | line | value |
|---|---:|---|
| `analysis/eval/elicit01/analyze.py` | 54 | `surgical_autonomy` |
| `analysis/eval/elicit01/prompts.py` | 54 | `surgical_autonomy` |
| `analysis/eval/elicit01/runner.py` | 139 | `surgical_autonomy` |
| `analysis/paper1/judge_codebook_smoke.py` | 1167 | `surgical_autonomy` |
| `engine/analysis/normalize.py` | 48 | `surgical_autonomy` |
| `engine/analysis/report.py` | 28 | `surgical_autonomy` |
| `engine/migrations/003_backfill_expanded_screening.py` | 19 | `surgical_autonomy` |
| `engine/migrations/004_pdf_quality_check.py` | 77 | `surgical_autonomy` |
| `engine/migrations/005_model_digest.py` | 66 | `surgical_autonomy` |
| `scripts/_pass2_stability.py` | 29 | `surgical_autonomy` |
| `scripts/_pass2_stability.py` | 62 | `surgical_autonomy` |
| `scripts/advance_to_pdf_acquired.py` | 19 | `surgical_autonomy` |
| `scripts/backfill_authors.py` | 23 | `surgical_autonomy` |
| `scripts/backfill_cloud_spans.py` | 29 | `surgical_autonomy` |
| `scripts/monitor_extraction.py` | 14 | `surgical_autonomy` |
| `scripts/parse_expanded_corpus.py` | 28 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step1_export_citations.py` | 14 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step1_export_citations.py` | 15 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step2_unpaywall_check.py` | 18 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step2_unpaywall_check.py` | 19 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step2_unpaywall_check.py` | 20 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step3_download_oa_pdfs.py` | 16 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step3_download_oa_pdfs.py` | 17 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step3_download_oa_pdfs.py` | 18 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step3b_retry_failed.py` | 22 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step3b_retry_failed.py` | 23 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step4_manual_download_list.py` | 14 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step4_manual_download_list.py` | 15 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step4_manual_download_list.py` | 16 | `surgical_autonomy` |
| `scripts/pdf_acquisition/step4_manual_download_list.py` | 17 | `surgical_autonomy` |
| `scripts/prepare_concordance_pdfs.py` | 12 | `surgical_autonomy` |
| `scripts/retry_parse_6.py` | 22 | `surgical_autonomy` |
| `scripts/test_extraction_validation.py` | 34 | `surgical_autonomy` |
| `scripts/test_extraction_validation.py` | 35 | `surgical_autonomy` |

## Module constants holding a review id

| file | line | name | value |
|---|---:|---|---|
| `engine/analysis/normalize.py` | 48 | `_FALLBACK_REVIEW_ID` | `surgical_autonomy` |
| `scripts/advance_to_pdf_acquired.py` | 19 | `DEFAULT_REVIEW` | `surgical_autonomy` |
| `scripts/backfill_authors.py` | 23 | `DEFAULT_REVIEW` | `surgical_autonomy` |
| `scripts/backfill_cloud_spans.py` | 29 | `DEFAULT_REVIEW` | `surgical_autonomy` |
| `scripts/monitor_extraction.py` | 14 | `DEFAULT_REVIEW` | `surgical_autonomy` |
| `scripts/parse_expanded_corpus.py` | 28 | `DEFAULT_REVIEW` | `surgical_autonomy` |
| `scripts/prepare_concordance_pdfs.py` | 12 | `DEFAULT_REVIEW` | `surgical_autonomy` |
| `scripts/retry_parse_6.py` | 22 | `DEFAULT_REVIEW` | `surgical_autonomy` |

## UNPARSED

None. Every scanned file parsed, and every argparse flag name was a string literal.

