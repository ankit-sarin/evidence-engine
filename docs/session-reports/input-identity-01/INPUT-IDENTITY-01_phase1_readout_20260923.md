# INPUT-IDENTITY-01 — Phase 1 read-out (session 8, S3d S3e)

**Date:** 2026-09-23 · **HEAD at measurement:** `b512e319a3b234d13f3f7b5f73e4405278e3a30f`
(Part 0) · **Database:** `data/surgical_autonomy/review.db`, opened `mode=ro` only, never
`immutable=1` · **Fingerprint:** `--compare` against
`docs/session-reports/manifest-01/review_db_fingerprint_20260923T162059Z.json` exit 0, 34
tables, overall `bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40`, `-wal` 0 B.
No file under `engine/`, `tests/` or `scripts/` was modified. No extractor, screener or
pipeline entry point was invoked.

Code facts are cited by quoted content anchor, never by line number. Every count below was
reached by query or script unless marked *by eye*.

## The stop, and R90 (provisional)

Phase 1 stopped at P4 under the brief's I3: *"none of the 350 is eligible on the eligibility
axis … Eligible count ≠ 0 → STOP."* Measured: the NULL column is
`full_text_assets.parsed_text_path` (the `papers` table has no such column), and **169 of the
350 papers are `eligible`**. P3 had already found a second failed premise (I4): `parsed_text_refs`
has **no hash column**.

**R90 (provisional until the closeout) — PI, on architect recommendation, 2026-09-23:** resume
Phase 1 under corrected premises, commit this read-out, stop. I3 is replaced by J1 (below): the
NULL rows are per-*asset*, not per-*paper*, so the 169 is not a corpus defect unless some of
those papers have no parsed row. I4 stands as PARTLY. P1's expected outcome was an architect
error: the next local run extracts 0 papers, not 190. D7 stands and R19 stays in force. The
placement of the R86 addendum after the table is accepted.

| id | inferred premise | result | measured |
|---|---|---|---|
| J1 | each of the 169 eligible papers with a NULL row also has ≥1 non-NULL `parsed_text_path` row | **TRUE** | 169 / 169; 0 lacking |
| J2 | SHA-256 over the 194 referenced files is read-only and fast; failure is loud | **TRUE** | 194 hashed, 0 unreadable; `Path.read_bytes()` raises on failure and the script records any failure in `unreadable` |
| J3 | the claude-config ledger row is committed by `/wrap`, not now | **TRUE** | see P7 |

---

## P1 — The reuse path (D7, I1)

**Expected:** the skip predicate is present and evaluated before the text loads; 0 of 190 rows
have `codebook_hash` NOT NULL; the next local run re-extracts all 190.
**Measured:** the predicate and its position are as expected, and 0 / 190 is as expected.
**The next local run extracts 0 papers, not 190 (corrected under R90).**

The skip, in `engine/agents/extractor.py::_run_extraction_unlocked`, sits inside the per-paper
loop and before the parsed-text load:

```python
        # Check staleness: skip if already extracted with current schema hash
        existing = db._conn.execute(
            "SELECT id FROM extractions WHERE paper_id = ? AND codebook_hash = ?",
            (pid, schema_hash),
        ).fetchone()
        if existing:
            ...
            continue

        # Load parsed Markdown
        parsed_dir = review_dir / "parsed_text"
        md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)
```

`schema_hash` is `load_codebook_beside(db.db_path).semantic_hash`. On live, `extractions` has
190 rows over 190 papers and `SELECT count(*) FROM extractions WHERE codebook_hash IS NOT NULL`
= **0**. So `codebook_hash = ?` can never be true: **D7 stands.**

**But the loop is never reached for a corpus paper.** The papers it iterates are selected
earlier in the same function by legacy status:

```python
    ft_papers = db.get_papers_by_status("FT_ELIGIBLE")
    parsed_papers = db.get_papers_by_status("PARSED")
    papers = ft_papers + parsed_papers
```

On live, all 190 corpus papers are `papers.status = 'AI_AUDIT_COMPLETE'`, and **no paper at all**
is at `FT_ELIGIBLE` or `PARSED`. `scripts/run_pipeline.py::_stage_extract` returns before
calling `run_extraction`:

```python
    parsed = db.get_papers_by_status("PARSED")
    if not parsed:
        logger.info("No papers with status PARSED — skipping extraction.")
        return {"extracted": 0, "elapsed": 0}
```

**Plainly: a local run today, through `run_pipeline` or `run_extraction`, extracts 0 papers.**
All 190 come back into scope only if something first forces them to `PARSED`. Two in-tree
paths do that, and both delete first: `scripts/reextract_all.py` and
`engine.utils.extraction_cleanup --confirm` (P8). **R19 therefore stays in force.** It is the
only control between the operator and those paths.

**Tests that pin the current behaviour** (B5: rewritten in Phase 2, never deleted):
- `tests/test_extractor.py::test_staleness_skip` — pre-inserts an extraction stamped with the
  current codebook hash on a `PARSED` paper, then asserts `stats["skipped"] == 1` and
  `stats["extracted"] == 0`. This pins both the `(paper_id, codebook_hash)` key and the
  status-based pickup.
- `tests/test_extractor.py::test_run_extraction_no_parsed_text` — pins the glob resolver's
  empty-result branch on the same path.
- The pre-flight stale warning and the pickup are pinned separately; see P8(c).

## P2 — The resolver sites (D1, I2)

**Expected:** six engine sites, all lexical. **Measured:** six engine sites in five modules, all
lexical. The inventory (`docs/inventory/entry_points.md`, `.json`) **does not list parsed-text
resolvers**. It carries only an incidental mention (`analysis/eval/analyze_capture01.py`, a
literal string describing the convention). The enumeration below is therefore grep, checked
against DISCOVERY-01 D2-5 (`docs/session-reports/DISCOVERY-01_part-B_readout.md`, "The 6 engine
sites"), and the two agree. *INVENTORY-02 candidate:* teach `engine/tools/inventory.py` to census
parsed-text resolvers, so this count stops depending on grep.

### Engine sites (in scope for S3e)

| path | function | quoted glob | quoted sort | order | caller(s) |
|---|---|---|---|---|---|
| `engine/agents/extractor.py` | `_run_extraction_unlocked` | `parsed_dir.glob(f"{pid}_v*.md")` | `sorted(..., reverse=True)` → `md_files[0]` | lexical | `run_extraction` ← `scripts/run_pipeline.py::_stage_extract`, `scripts/reextract_all.py`, `scripts/run5_extract_and_audit.py` |
| `engine/agents/auditor.py` | `run_audit` | `parsed_dir.glob(f"{pid}_v*.md")` | `sorted(..., reverse=True)` | lexical | `scripts/run_pipeline.py`, `scripts/reextract_all.py`, `scripts/run5_extract_and_audit.py` |
| `engine/agents/ft_screener.py` | `_load_parsed_text` | `parsed_dir.glob(f"{paper_id}_v*.md")` | `sorted(..., reverse=True)` → `candidates[0]` | lexical | `run_ft_screening`, `run_ft_verification` (same module) |
| `engine/cloud/base.py` | `CloudExtractorBase.load_parsed_text` | `parsed_dir.glob(f"{paper_id}_v*.md")` | `sorted(..., reverse=True)` | lexical | `OpenAIExtractor.run`, `AnthropicExtractor.run` |
| `engine/review/human_review.py` | `export_review_queue` | `parsed_dir.glob(f"{pid}_v*.md")` | `sorted(..., reverse=True)` | lexical | no in-tree caller outside `tests/test_human_review.py` |
| `engine/review/human_review.py` | `_import_review_csv` (ACCEPT_CORRECTED snippet check) | `(review_dir / "parsed_text").glob(f"{pid}_v*.md")` | `sorted(..., reverse=True)` | lexical | `import_review_decisions` ← `engine/adjudication/audit_adjudicator.py` |

### `scripts/` sites — **not in scope for S3e unless a Phase 2 ruling says so**

| path | function | order |
|---|---|---|
| `scripts/_pass2_stability.py` | `paper_text` | lexical, `reverse=True` |
| `scripts/eval_auditor_models.py` | `load_spans_and_text` | lexical |
| `scripts/ft_screening_smoke_test.py` | `main` (×2) | lexical |
| `scripts/q8_validation.py` | `main` | lexical |
| `scripts/q8_validation_fast.py` | `main` | lexical |
| `scripts/reextract_failed.py` | `main` | lexical |
| `scripts/smoke_test_fixes.py` | `main` | lexical |

8 sites in 7 modules, matching D2-5. The `analysis/` resolvers are frozen with their studies
(S3e) and are not re-listed here. D2-5 counted 28 sites in 25 modules there, 19 numeric and 9
lexical.

**On disk.** Highest parsed-text version for any corpus paper: **3** (expected ≤ 3). Highest in
the whole `parsed_text/` directory: **3**, over 446 files. Corpus papers for which lexical and
numeric ordering choose different files: **0** (expected 0). D1 remains LATENT.

## P3 — `parsed_text_refs` (D8, I4, I8)

**Expected:** a content-hash column; 190 papers / 194 rows; coverage exact; one writer (017);
20/20 sample hashes match. **Measured:** **no hash column**; 190 / 194; coverage exact; one
writer. The sample check is not performable, and P3' replaces it.

`PRAGMA table_info(parsed_text_refs)`: `parsed_text_uid TEXT`, `paper_id INTEGER`,
`parsed_text_path TEXT`, `parsed_text_version INTEGER`, `source_full_text_assets_id INTEGER`,
`recorded_at TEXT`. The DDL in `engine/migrations/016_event_store.py` has
`UNIQUE (paper_id, parsed_text_version, parsed_text_path)` and nothing hashed.

- **Rows:** 194 over 190 papers: 186 papers × 1 row, 4 papers × 2 rows.
- **Hashing in the seed:** `017_seed_event_store.py` defines
  `def _sha256(path: Path) -> str: return hashlib.sha256(path.read_bytes()).hexdigest()` and
  applies it only to the spec (`"file_sha256": _sha256(spec_path)`) for `review_identities`.
  Parsed texts are inserted with `(mint_extraction_uid(), paper_id, path, version, fta_id, now)`,
  with no hash. **`parsed_text_hash` does not exist anywhere on this database today.**
- **Coverage:** on-disk `parsed_text/{id}_v*.md` for corpus papers without a row: **∅**. Rows
  whose file is not on disk: **∅**. Rows whose path does not exist: **0**.
- **Writers:** `INSERT OR IGNORE INTO parsed_text_refs` appears only in
  `engine/migrations/017_seed_event_store.py`. 016 creates the table. No module in `engine/`,
  `scripts/` or `analysis/` writes it (**I8 TRUE**), so **D8 stands**.

*Consequence recorded for Phase 2 (R90):* the S3e resolver cannot "return the hash" from
`parsed_text_refs` without a schema change or an on-the-fly hash. This read-out does not decide
which.

### P3' — Hash baseline

`docs/session-reports/input-identity-01/parsed_text_hashes_20260923T214241Z.json`:
**194 entries, 0 unreadable**, sorted by `(paper_id, parsed_text_version)`. Each entry holds
`parsed_text_uid`, `paper_id`, `parsed_text_path`, `parsed_text_version`, `size_bytes`, and
`sha256` over the raw file bytes as on disk. 194 distinct hashes.
SHA-256 of the JSON file: **`67754a477be575d285654b44cbf025d3ae17db1567b04970c0b6409bab6cb2e7`**.

**The four two-version papers are at v2 and v3, not v1 and v2** (the brief expected v1/v2). All
four pairs differ in hash, as expected. In every case v3 is much smaller than v2:

| paper | v2 bytes | v2 sha256 (prefix) | v3 bytes | v3 sha256 (prefix) |
|---:|---:|---|---:|---|
| 455 | 60,537 | `1e459a150055` | 52,503 | `71d3448949a0` |
| 586 | 69,223 | `dd8935f5f746` | 40,548 | `29eb250acb1d` |
| 699 | 48,568 | `a83f443fb1e6` | 27,069 | `5313b9e20116` |
| 719 | 279,426 | `4cbc3557ff5e` | 28,867 | `37bfcdf80946` |

Every engine resolver picks v3 for these four, because v3 wins under both lexical and numeric
ordering. No explanation for the size drop is claimed here. Their `full_text_assets` rows name
parsers `docling_ocr` (3 rows) and `docling_sanitized` (1 row) at version 3. The pairing of
parser to paper was not measured.

## P4 — The 350 NULL rows (D4; I3 replaced by J1)

**Expected (brief):** on `papers.parsed_text_path`, 350, none eligible. **Measured:** on
`full_text_assets.parsed_text_path`; 350 rows over 350 papers; 169 eligible, 181
`no_recorded_state`. **Expected (R90, J1):** each of the 169 has a non-NULL row. **Measured: 169
/ 169.**

**P4'(a).** Of the 169 eligible papers with a NULL row, those with ≥1 non-NULL
`parsed_text_path` row: **169**. Lacking: **none**. DISCOVERY-01's "0 NULL resolutions" holds.

**P4'(b) — what is on the 350 NULL rows** (distribution over all 350):

| column | value on all 350 NULL rows | on the 444 non-NULL rows |
|---|---|---|
| `pdf_path` | non-NULL, all under `data/surgical_autonomy/pdfs` | non-NULL |
| `pdf_hash` | **NULL** (350) | non-NULL (444) |
| `parsed_text_version` | **1** (350) | 1 (99), 2 (341), 3 (4) |
| `parser_used` | **`docling`** (350) | `docling` 434, `pymupdf` 6, `docling_ocr` 3, `docling_sanitized` 1 |
| `parsed_at` | **NULL** (350) | non-NULL (444) |
| `papers.pdf_local_path` | non-NULL (350) | — |
| `papers.status` | `AI_AUDIT_COMPLETE` 169 · `FT_SCREENED_OUT` 166 · `PDF_EXCLUDED` 15 | — |
| processing axis (`effective_state`) | `no_recorded_state` (350) | — |

`full_text_assets` has no status, attempt or error column; its columns are `id, paper_id,
pdf_path, pdf_hash, parsed_text_path, parsed_text_version, parser_used, parsed_at`. For the 335
papers that also have a parsed row (338 NULL→parsed pairs, because 3 papers have two parsed
rows), the NULL row's `id` is **lower** than the parsed row's in 338 / 338 pairs, and its
`pdf_path` is **equal** in 338 / 338. NULL-row ids span 100–455. On disk, **no `_v1.md` file
exists for any of the 350**. 332 of them have only `_v2`, 3 have `_v2` and `_v3`, and 15 have no
parsed file at all.

**P4'(c) — every writer of `full_text_assets`**, quoted:
- `engine/parsers/pdf_parser.py` (the atomic write after `md_filename = f"{paper_id}_v{version}.md"`):
  `INSERT INTO full_text_assets (paper_id, pdf_path, pdf_hash, parsed_text_path,
  parsed_text_version, parser_used, parsed_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, which always
  writes a path and `parsed_at`.
- `engine/acquisition/verify_downloads.py`: `"UPDATE full_text_assets SET pdf_path = ? WHERE
  paper_id = ?"` when a row exists, else `INSERT INTO full_text_assets (paper_id, pdf_path)`.
  That leaves `parser_used` NULL, because the column has no default.
- `scripts/advance_to_pdf_acquired.py`: `INSERT INTO full_text_assets (paper_id, pdf_path,
  pdf_hash)`, which sets `pdf_hash`.
- `scripts/retry_parse_6.py`, the only UPDATE of `parsed_text_path`:
  `UPDATE full_text_assets SET pdf_hash = ?, parsed_text_path = ?, parsed_text_version = ?, …`,
  which sets it non-NULL.
- DDL (`engine/core/database.py`, identical on live): `parsed_text_version INTEGER NOT NULL
  DEFAULT 1`; `parser_used TEXT` with no default.

**No in-tree writer produces the measured NULL-row shape** (`parser_used = 'docling'` with
`parsed_text_path`, `pdf_hash` and `parsed_at` all NULL). The history of the rows is not
reconstructed here, as the brief constrains.

**P4'(d) — rows per paper across the whole table** (794 rows over 455 papers):
1 row: 119 papers; 2 rows: 333; 3 rows: 3.
Corpus (190): 166 papers with 2 rows = 1 NULL + 1 parsed (as expected); 3 with 3 rows = 1 NULL
+ 2 parsed; 20 with 1 parsed row; 1 with 2 parsed rows. Every corpus paper has ≥1 asset row.
Non-corpus (265): 166 with 1 NULL + 1 parsed; 84 with 1 parsed row; **15 with a single NULL row
and nothing else, all `PDF_EXCLUDED`**.

**P4'(e) — accounting note draft (for Phase 2 placement):**
> The 350 NULL `parsed_text_path` values are 350 `full_text_assets` rows, one per paper. Each has
> `parsed_text_version` 1, `parser_used` `docling`, and no `pdf_hash` or `parsed_at`. 335 of
> them sit beside a later parsed row for the same PDF, and the other 15 are `PDF_EXCLUDED`
> papers with no parsed text. None of the 190 corpus papers resolves to NULL, and no in-tree
> writer produces this row shape. v49's "16" has **no verified explanation**. The nearest
> measured quantity is the 15 `PDF_EXCLUDED` papers whose only asset row is NULL, and that is
> not 16.

## P5 — Key components already at hand (I6)

S3d key: `(paper_id, parsed_text_hash, codebook_hash, prompt_hash, model_digest, options_hash)`.
**Expected:** four from 020, `parsed_text_hash` absent, `paper_id` trivial. **Measured:** as
expected, except that `codebook_hash` is on `run_manifests`, not `run_stage_configs`. I6 is
therefore PARTLY TRUE.

| component | where it comes from at run time today | name |
|---|---|---|
| `paper_id` | trivially present on every event and call row | `field_events.paper_id`, `run_calls.paper_id` |
| `parsed_text_hash` | **nothing** | no column, table or function hashes a parsed text (P3) |
| `codebook_hash` | `run_manifests` (per run) | `run_manifests.codebook_hash` (= `codebook.semantic_hash`) and `.codebook_sha256` |
| `prompt_hash` | `run_stage_configs` (per run × stage) | `prompt_hash`, from `engine/core/effective_config.py::prompt_hash`: "hash of the rendered system + user messages + format, per stage", rendered around `SENTINEL_PAPER` / `SENTINEL_TEXT`, so it identifies the **template** and not the paper |
| `model_digest` | `run_stage_configs` | `model_digest` (`/api/tags`, `fetch_model_digest`); a CHECK requires 64 chars for `provider = 'ollama'` |
| `options_hash` | `run_stage_configs` | `options_hash`, from `EffectiveConfig.options_hash`: docstring "Every request kwarg except model, messages and format (S3d's key)" |

Also present: `run_stage_configs.format_schema_hash`, and `run_calls.request_hash`, which is per
`(run_id, stage, paper_id)` and hashes the request kwargs as sent. Those include the messages
and therefore the paper text, but conflated with the template and options. **Fact for the
ruling:** a local extraction runs as up to four stages (`extract_pass1`, `extract_pass2`,
`extract_retry_snippet`, and `elicitation_pass1` when elicitation is on), each with its own
`model_digest` / `options_hash` / `prompt_hash` row. The single-valued components of the S3d
tuple are therefore a set over stages at run time.

## P6 — Where the reuse decision can live (I7, M2)

**Facts.**
- `field_events` columns: `event_id, event_uid, event_type, occurred_at, recorded_at,
  actor_kind, actor_role, actor_name, actor_digest, run_id, run_marker, prior_event_id,
  presented_context_sha256, reason, payload_json, claim_id, extraction_uid, paper_id,
  field_name, arm, value, source_snippet`. Indexes: `(paper_id, field_name, arm, event_id)`,
  `(claim_id)`, `(extraction_uid)`, `UNIQUE(event_uid)`.
- `paper_events` has the same common columns plus `paper_id, to_state, from_state,
  reason_code, stage_name`. Indexes: `(paper_id, event_id)`, `UNIQUE(event_uid)`.
- Writer: `engine/core/events.py`. `write_field_event(conn, *, event_type, paper_id,
  field_name, arm, claim_id=None, extraction_uid=None, value=None, …,
  presented_context_sha256=None, reason=None, payload=None, …)` requires a `run_id` (R68) and
  stores `payload_json` as free JSON. It also copies `presented_context_sha256` into the payload
  (`payload.setdefault("presented_context_sha256", presented_context_sha256)`).
- **I7 TRUE:** no event column records `parsed_text_hash` or any input-identity tuple. M2 still
  holds: the extractor writes the legacy tables, and live has 0 `field_events`.
- **What can be answered today.** "Has arm A asserted paper P" is answered by the
  `(paper_id, field_name, arm, event_id)` index. "Under key K" is only partly answerable, by
  joining `field_events.run_id` → `run_stage_configs` for model digest, options and prompt
  hashes and → `run_manifests` for the codebook hash. That join has two gaps. It cannot supply
  `parsed_text_hash`, which exists nowhere. And it cannot say which stage's row belongs to a
  given claim, because events carry `run_id` but not a stage.

**`presented_context_sha256`.** It could physically hold one hash with no schema change. Its
defined meaning (S2 design, `S2_phase1_readout_addendum3_20260921.md` rows 5 and 8) is what a
**human reviewer was shown** when deciding, and `engine/core/effective.py` surfaces it as
reviewer provenance: `"presented_context_sha256": governing.payload.get(…)` and
`endorsed.payload.get(…)`. Using it for an extractor's input identity would misuse it in three
ways:
1. A model claim's input hash would be indistinguishable from a reviewer's context hash in the
   reader's provenance.
2. One column cannot carry the six-component tuple, only one hash.
3. Rule rows 5, 7 and 8 read it from the reviewer event, so the reader would start
   reporting extractor inputs as reviewer context.

**Three options, with schema cost** (facts only, no recommendation):

| option | what it is | schema change |
|---|---|---|
| (a) | a key column (or `parsed_text_hash` plus a stage column) on `field_events` | `ALTER TABLE ADD COLUMN`, or a rebuild, of an append-only trigger-guarded table carrying R77's CHECK. The triggers guard UPDATE/DELETE, so an ADD COLUMN is not blocked by them, but an index on the new column is also a DDL change. Migration **021** |
| (b) | a small table keyed by K (or its hash), referencing the event or the `extraction_uid` | new table (+ index, + append-only trigger if it is to match the store). Migration **021** |
| (c) | a manifest-level record, e.g. a per-(run, paper) input table, or a `parsed_text_hash` beside `run_calls` | new table or new column on `run_calls`. Migration **021** |

Keeping the hash only in `payload_json` needs no migration. Querying it would then rely on
`json_extract` without an index, or on an expression index, which is itself DDL.

**Any Phase 2 that writes (a), (b) or (c) to live is a live-write session**: migration 021,
under R85/R86 (07:00–10:35 UTC embargo, exclusivity check, verified pre-write backup,
fingerprint before and after).

## P7 — Closeout facts (this brief)

- `--compare` exit **0** at open, after Part 0, and at the close of Phase 1, measured
  immediately before this commit: 34 tables, overall `bb39ba81…d16c40` unchanged, `-wal` 0 B.
- Open modes: `mode=ro` URI connections (`db_fingerprint`, and ad hoc `sqlite3.connect(
  "file:…?mode=ro", uri=True)`); never `immutable=1`, never writable. The two restore points
  were fingerprinted (mode=ro) at open. The four older backups were not opened.
- Commits: `b512e319a3b234d13f3f7b5f73e4405278e3a30f` (Part 0); this read-out plus the JSON
  artifact (hash in the closeout report).
- Gate: 2,714 / 17 (581/708/433/575/417; deselects 0/0/10/6/1) at open, after Part 0, and on
  this tree before commit.
- **claude-config (J3).** `PROJECT_LEDGER.md` shows one uncommitted appended row for
  `b512e31`. `~/claude-config/hooks/post-commit-ledger.sh` only appends:
  `echo "| ${TIMESTAMP} | … |" >> "$LEDGER"` with `LEDGER="$HOME/.claude/PROJECT_LEDGER.md"`.
  It never commits. The commit target is `/wrap` step 4b (`commands/wrap.md`, "The ledger —
  always, regardless of step 4's verdict"): `git -C ~/claude-config commit -m "ledger: rows
  through <last timestamp>"`. Left uncommitted, as ruled. HEAD is unchanged at `18d4a2b`.

## P8 — Selection and deletion facts (D9, D10)

**(a) Pickup.** Quoted in P1: `_run_extraction_unlocked` selects
`db.get_papers_by_status("FT_ELIGIBLE")` + `db.get_papers_by_status("PARSED")`, and
`scripts/run_pipeline.py::_stage_extract` returns early on
`if not parsed: logger.info("No papers with status PARSED — skipping extraction.")`.
Both read legacy `papers.status`, which `effective_state` deliberately never reads.

**(b) Deletion, and the predicate that points at it.**
- `scripts/reextract_all.py`, "Step 2: Reset status to PARSED and clean old data":
  `"DELETE FROM evidence_spans WHERE extraction_id = ?"`, `"DELETE FROM extractions WHERE
  paper_id = ?"`, then `"UPDATE papers SET status = 'PARSED', updated_at = datetime('now')
  WHERE id = ?"` ("bypass state machine — administrative override"). After that it calls
  `run_extraction`.
- `engine/utils/extraction_cleanup.py::cleanup_stale_extractions` (reached through `main` when
  `--confirm` sets `dry_run = not args.confirm` to False). It selects
  `WHERE (e.codebook_hash IS NULL OR e.codebook_hash != ?)`, then runs
  `f"DELETE FROM evidence_spans WHERE extraction_id IN ({placeholders})"` and
  `f"DELETE FROM extractions WHERE id IN ({placeholders})"`, and resets papers in
  `_RESETTABLE_STATUSES = {"EXTRACTED", "AI_AUDIT_COMPLETE"}` to PARSED. **On live today that
  selection matches all 190 extractions**, because every `codebook_hash` is NULL. The command
  would delete all 190 extractions and their spans and reset all 190 corpus papers to PARSED,
  which is exactly the state in which `run_extraction` picks them up.
- The pre-flight predicate `engine/utils/extraction_cleanup.py::check_stale_extractions`:
  `SELECT COUNT(DISTINCT paper_id) FROM extractions WHERE (codebook_hash IS NULL OR
  codebook_hash != ?)`. It counts **190** today. `_run_extraction_unlocked` logs it as
  `"Found %d papers with stale schema extractions. Run python -m engine.utils.extraction_cleanup
  --review %s to clean up before re-extracting."`. The warning names the cleanup module but not
  `--confirm`.

**(c) Tests that pin each behaviour** (B5: rewritten in Phase 2, never deleted):
- Status pickup and codebook key: `tests/test_extractor.py::test_staleness_skip`.
- Stale-count predicate: `tests/test_extraction_cleanup.py::TestStaleExtractionCheck::test_check_stale_returns_count`,
  `::test_check_stale_zero_when_all_current`, and
  `tests/test_codebook_staleness.py::test_the_count_predicate_agrees_with_the_row_predicate`
  (this one pins count == row selection, i.e. the NULL-inclusive predicate).
- Pre-flight warning: `tests/test_extraction_cleanup.py::TestExtractionRunnerWarning::test_warns_when_stale_exist`,
  `::test_silent_when_no_stale`. `tests/test_extractor.py` also patches
  `engine.utils.extraction_cleanup.check_stale_extractions` to 0 in five tests (the
  `@patch(..., return_value=0)` decorators), so those depend on the pre-flight existing.
- Cleanup deletion and reset: `tests/test_extraction_cleanup.py` classes `TestDryRun`,
  `TestSchemaCleanup` (`test_removes_non_matching_schema_only`, `test_spans_cascade_deleted`),
  `TestStatusReset` (3 tests), `TestDedup`, `TestAtomicDelete` (2), `TestAdminResetAuditTrail`.
- `scripts/reextract_all.py`: **no behavioural test**. It appears only in
  `tests/test_review_paths.py`'s module list, which pins spec-path resolution, not deletion.

**(d) Documented operator procedures that instruct these paths.**
- Project `CLAUDE.md`, "## Running": `python -m engine.utils.extraction_cleanup --review
  surgical_autonomy --confirm # execute`, under "# Extraction cleanup (schema transition)".
  **This instructs the destructive path.**
- `docs/architecture/modules.md` describes `reextract_all.py` as "Full re-extraction (wipe +
  redo)" in its scripts table, and `docs/architecture/state-machine.md` documents
  `cleanup_stale_extractions()`. Both are descriptive, not procedures.
- The extractor's own pre-flight warning (quoted above) directs the operator to the cleanup
  module.
- `docs/session-reports/effective-result-01/S2_phase1_readout.md` already lists all four DELETE
  statements ("`scripts/reextract_all.py` is the re-extraction path, and it *deletes then
  re-inserts*"). No plan row carries them.

## Findings in passing (inventory-row candidates)

| ID | Problem | Evidence anchor | On disk |
|---|---|---|---|
| D9 | Extraction pickup is decided by legacy `papers.status ∈ {FT_ELIGIBLE, PARSED}` before any reuse key is consulted (`_run_extraction_unlocked`: `get_papers_by_status("FT_ELIGIBLE")` + `("PARSED")`; `_stage_extract` early return). S3d's key cannot take effect while that selection stands in front of it, and `papers.status` is a reader the S2 design retired | this read-out P1, P8(a) | ARMED (0 corpus papers selectable today; all 190 at `AI_AUDIT_COMPLETE`) |
| D10 | Two paths delete extraction history against S3d ("superseded … never deleted") and R25: `scripts/reextract_all.py` and `extraction_cleanup --confirm`. The latter's NULL-inclusive predicate matches all 190 live extractions today, `CLAUDE.md` "## Running" instructs it, and the extractor's pre-flight warning points the operator to it | this read-out P8(b)(d); `S2_phase1_readout.md` delete list | ARMED (R19 is the only control) |
| D11 | Four corpus papers (455, 586, 699, 719) resolve to a v3 parse between 13% and 90% smaller than their v2 (719: 279,426 → 28,867 B). Whether v3 is the intended input is unrecorded | P3' table | unmeasured beyond size and hash |
| D12 | 350 `full_text_assets` rows carry a shape no in-tree writer produces (`parser_used='docling'` with path, `pdf_hash` and `parsed_at` all NULL); readers that pick "a row" rather than "the parsed row" can land on them | P4'(b)(c) | LATENT (0 corpus NULL resolutions under the four rules D2-5 tested) |
| I-inv | `docs/inventory/` does not census parsed-text resolvers; the D1 count depends on grep | P2 | structural (INVENTORY-02 candidate) |
