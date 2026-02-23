# End-to-End Test Notes

## Test Coverage

| Stage | Test Method | Status |
|-------|-------------|--------|
| Search (PubMed + OpenAlex) | Live queries in `test_pubmed.py`, `test_openalex.py` | Automated |
| Deduplication | Unit tests in `test_dedup.py` | Automated |
| Screening (dual-pass) | Live Ollama in `test_screener.py` + `scripts/test_e2e_search_screen.py` | Automated |
| PDF Parsing | Integration tests in `test_pdf_parser.py` | Automated |
| Extraction (two-pass) | Mocked in `test_extractor.py` | Automated (mock) |
| Audit (cross-model) | Mocked in `test_auditor.py` | Automated (mock) |
| Export (all formats) | Unit tests in `test_exporters.py` | Automated |

## Running the Full Pipeline

```bash
python scripts/run_pipeline.py --spec review_specs/surgical_autonomy_v1.yaml --name surgical_autonomy
```

The pipeline will stop at the PARSE stage because PDF acquisition is manual in v1.
To proceed past PARSE, place PDFs in `data/surgical_autonomy/pdfs/` named as `{paper_id}.pdf`.

## Running the Search + Screen E2E Test

```bash
python scripts/test_e2e_search_screen.py
```

This runs a live test against PubMed, OpenAlex, and Ollama (qwen3:8b) on the first 20 papers.
Results are logged to `tests/e2e_search_screen_log.md`.

## Full E2E Requirements

- Full end-to-end test requires PDFs placed in `data/{review_name}/pdfs/`
- Search + screening test can run immediately (requires network + Ollama)
- Extraction + audit + export are tested via unit tests with mocked Ollama calls
