| lot | PROPOSED content | units (N / C) | lines | Run 7 share of lines (top + lazy) | 12F not recorded | readers (est.) |
| --- | --- | ---: | ---: | --- | ---: | ---: |
| L01 | Extraction write path: claims as events, guards, selection, locator, telemetry | 9 (8 / 1) | 1,772 | 100% (1,772 top + 0 lazy) | 4 | 1 |
| L02 | Elicitation (the Run 7 extraction design) | 8 (0 / 8) | 2,088 | 100% (288 top + 1,800 lazy) | 4 | 1 |
| L03 | Audit and the distribution gate | 4 (4 / 0) | 1,057 | 100% (1,057 top + 0 lazy) | 1 | 1 |
| L04 | Resolver and run manifest | 2 (1 / 1) | 1,300 | 100% (1,300 top + 0 lazy) | 0 | 1 |
| L05 | Spec, codebook, review identity, eligibility rendering | 5 (4 / 1) | 1,940 | 100% (1,940 top + 0 lazy) | 3 | 1 |
| L06 | Readers: the one reader, the parsed-text resolver, naming | 3 (1 / 2) | 864 | 100% (810 top + 54 lazy) | 1 | 1 |
| L07 | Parser support: font audit, parse-quality verdict, markers, models | 4 (4 / 0) | 1,181 | 100% (1,181 top + 0 lazy) | 1 | 1 |
| L08 | Workflow, adjudication schema and the two entry importers | 5 (3 / 2) | 1,023 | 49% (501 top + 0 lazy) | 2 | 1 |
| L09 | Human screening adjudication: adjudicators, HTML generators, categorizer | 5 (3 / 2) | 3,152 | 100% (1,911 top + 1,241 lazy) | 2 | 2 |
| L10 | Front half remainder: PubMed client, search models, acquisition except download.py | 9 (0 / 9) | 2,644 | 55% (199 top + 1,247 lazy) | 5 | 1 |
| L11 | Local-model utilities and run support: preflight, tmux background, progress | 3 (2 / 1) | 465 | 100% (0 top + 465 lazy) | 2 | 1 |
| L12 | Cloud arms (R71: no cloud extraction yet) | 4 (3 / 1) | 1,105 | 100% (0 top + 1,105 lazy) | 0 | 1 |
| L13 | Export remainder: DOCX and the shared review workbook | 2 (0 / 2) | 568 | 100% (194 top + 374 lazy) | 1 | 1 |
| L14 | Concordance: engine/analysis | 6 (6 / 0) | 1,431 | 0% (0 top + 0 lazy) | 4 | 1 |
| L15 | Numbered migrations and the migrations CLI (applied text is frozen by receipt) | 23 (0 / 23) | 4,604 | 99% (0 top + 4,547 lazy) | 21 | 1 |
| L16 | Developer tools and the read-only validator | 3 (3 / 0) | 1,105 | 0% (0 top + 0 lazy) | 3 | 1 |
| L17 | Scripts off the Run 7 import graph | 23 (23 / 0) | 4,991 | 0% (0 top + 0 lazy) | 10 | 2 |
| L18 | Empty package markers (0 lines each; nothing to read) | 6 (5 / 1) | 0 | — | 6 | 0 |
| all | | 124 | 31,290 | | | 19 |

- **L01** — `engine/core/audit_telemetry.py` (49), `engine/core/citation_guard.py` (210), `engine/core/events.py` (491), `engine/core/extraction_events.py` (529), `engine/core/extraction_telemetry.py` (148), `engine/core/locator.py` (102), `engine/core/reuse_key.py` (55), `engine/core/run_telemetry.py` (69), `engine/core/selection.py` (119)
- **L02** — `engine/elicitation/__init__.py` (10), `engine/elicitation/classes.py` (278), `engine/elicitation/contracts.py` (458), `engine/elicitation/materialize.py` (116), `engine/elicitation/pipeline.py` (612), `engine/elicitation/prompts.py` (378), `engine/elicitation/terminal.py` (104), `engine/elicitation/units.py` (132)
- **L03** — `engine/agents/audit_events.py` (250), `engine/agents/auditor.py` (259), `engine/validators/__init__.py` (1), `engine/validators/distribution_monitor.py` (547)
- **L04** — `engine/core/effective_config.py` (534), `engine/core/run_manifest.py` (766)
- **L05** — `engine/core/codebook.py` (482), `engine/core/constants.py` (10), `engine/core/eligibility_render.py` (424), `engine/core/review_paths.py` (92), `engine/core/review_spec.py` (932)
- **L06** — `engine/core/effective.py` (599), `engine/core/naming.py` (54), `engine/core/parsed_text.py` (211)
- **L07** — `engine/parsers/font_audit.py` (719), `engine/parsers/markers.py` (41), `engine/parsers/models.py` (57), `engine/parsers/parse_quality.py` (364)
- **L08** — `engine/adjudication/__init__.py` (43), `engine/adjudication/import_extraction_entry.py` (333), `engine/adjudication/import_screening_entry.py` (189), `engine/adjudication/schema.py` (78), `engine/adjudication/workflow.py` (380)
- **L09** — `engine/adjudication/abstract_adjudication_html.py` (708), `engine/adjudication/categorizer.py` (259), `engine/adjudication/ft_adjudication_html.py` (533), `engine/adjudication/ft_screening_adjudicator.py` (753), `engine/adjudication/screening_adjudicator.py` (899)
- **L10** — `engine/acquisition/__init__.py` (29), `engine/acquisition/check_oa.py` (197), `engine/acquisition/manual_list.py` (448), `engine/acquisition/pdf_quality_check.py` (326), `engine/acquisition/pdf_quality_html.py` (750), `engine/acquisition/pdf_quality_import.py` (316), `engine/acquisition/verify_downloads.py` (379), `engine/search/models.py` (19), `engine/search/pubmed.py` (180)
- **L11** — `engine/utils/background.py` (66), `engine/utils/ollama_preflight.py` (303), `engine/utils/progress.py` (96)
- **L12** — `engine/cloud/anthropic_extractor.py` (260), `engine/cloud/base.py` (517), `engine/cloud/openai_extractor.py` (232), `engine/cloud/schema.py` (96)
- **L13** — `engine/exporters/docx_export.py` (194), `engine/exporters/review_workbook.py` (374)
- **L14** — `engine/analysis/__init__.py` (0), `engine/analysis/concordance.py` (399), `engine/analysis/metrics.py` (233), `engine/analysis/normalize.py` (181), `engine/analysis/report.py` (359), `engine/analysis/scoring.py` (259)
- **L15** — `engine/migrations/002_screening_rename.py` (207), `engine/migrations/003_backfill_expanded_screening.py` (411), `engine/migrations/004_pdf_quality_check.py` (95), `engine/migrations/005_model_digest.py` (83), `engine/migrations/006_not_null_confidence_tier.py` (116), `engine/migrations/007_add_judge_tables.py` (187), `engine/migrations/008_add_fabrication_verifications.py` (143), `engine/migrations/009_add_backfill_audit_log.py` (180), `engine/migrations/010_add_provenance_classifications.py` (168), `engine/migrations/011_add_absence_claim_class.py` (205), `engine/migrations/012_codebook_provenance.py` (98), `engine/migrations/013_drop_schema_hash_not_null.py` (148), `engine/migrations/014_cloud_tables.py` (60), `engine/migrations/015_drop_prerename_adjudication_indices.py` (78), `engine/migrations/016_event_store.py` (263), `engine/migrations/017_seed_event_store.py` (205), `engine/migrations/018_cloud_shape_and_audit_adjudication.py` (231), `engine/migrations/019_paper_state_axes.py` (304), `engine/migrations/020_run_manifest.py` (496), `engine/migrations/021_parsed_text_sha256.py` (279), `engine/migrations/022_run_kinds_and_audit_tables.py` (590), `engine/migrations/__init__.py` (0), `engine/migrations/__main__.py` (57)
- **L16** — `engine/tools/__init__.py` (1), `engine/tools/inventory.py` (757), `engine/validators/extraction_validator.py` (347)
- **L17** — `scripts/_pass2_delta.py` (287), `scripts/_pass2_eyeball.py` (142), `scripts/_pass2_stability.py` (102), `scripts/backfill_authors.py` (209), `scripts/backfill_cloud_spans.py` (120), `scripts/ft_screening_smoke_test.py` (254), `scripts/parse_expanded_corpus.py` (95), `scripts/pdf_acquisition/step1_export_citations.py` (82), `scripts/pdf_acquisition/step2_unpaywall_check.py` (225), `scripts/pdf_acquisition/step3_download_oa_pdfs.py` (165), `scripts/pdf_acquisition/step3b_retry_failed.py` (373), `scripts/pdf_acquisition/step4_manual_download_list.py` (254), `scripts/prepare_concordance_pdfs.py` (87), `scripts/q8_validation.py` (251), `scripts/q8_validation_fast.py` (162), `scripts/reparse_cloud_spans.py` (119), `scripts/rescreen_original_251.py` (181), `scripts/rescreen_with_specialty.py` (407), `scripts/run_cloud_extraction.py` (211), `scripts/screen_expanded.py` (540), `scripts/smoke_test_fixes.py` (200), `scripts/test_e2e_search_screen.py` (164), `scripts/test_extraction_validation.py` (361)
- **L18** — `engine/__init__.py` (0), `engine/agents/__init__.py` (0), `engine/core/__init__.py` (0), `engine/parsers/__init__.py` (0), `engine/search/__init__.py` (0), `engine/utils/__init__.py` (0)
