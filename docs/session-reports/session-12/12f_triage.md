# 12f — verification read and triage table

**Status: COMPLETE.** Read-only. No change under `engine/`, `tests/`, `scripts/`, `analysis/`,
`review_specs/` or `data/`; no model call; no network call; no live write. Ends at a STOP for the
PI's ruling on this list (R509). No inventory row was opened.

| | |
| --- | --- |
| Reference HEAD | `297667c17979a8aeba1d02330740b24f8f28e885` (12f item 0c) |
| Input A | `outside/assessment_A_architectural.md` — sha256 `2ef65a1a90c4ba68f58ed58559a370f954738b5ecfdbfe2b269425ac294a8bb4`, 14,089 bytes |
| Input B | `outside/assessment_B_17_findings.md` — sha256 `95ea43c2bdf7189de0694ba0eaafb8b30f5cfd5bf7832d3522795684d8f18e37`, 36,733 bytes |
| Rows in this table | 62 |
| Live fingerprint | IDENTICAL to the migration-02 record at open, after each push, and at close |

## How this read-out is laid out

- **This file** — the triage table (one row per item), the summaries the brief asks for, the
  PI-decision list, and the E4 classification of existing rows.
- **`12f_triage_rows.md`** — every verified item in full: the quoted code or the reproduction, the
  overlap reasoning, the minimum and full fix, and the acceptance test.
- **`12f_C56-A.md`** — the C56 census, one entry per handler (95 handlers).
- **`12f_repro/`** — every reproducer with its recorded output, the two startup-verify scripts
  (`pins.py`, `dbchecks.py`) and the rules the readers worked under (`READER_RULES.md`).

**How the read was done.** Both originals were read in full first. The findings were then read
at HEAD by eight parallel read-only readers (five for B, two for A, two halves of the C56
census), each under `12f_repro/READER_RULES.md`. Reproductions ran on synthetic databases and
stubs under `~/scratch/12f/` only. The lead reran five reproducers independently (F02, F04, F08,
F15, F16 — outputs identical to the recorded ones) and re-read the deciding lines for F01, F06,
F07, F11, F12, the `_retry_snippet` handler, the swallowed `_advance_extraction_workflow` and the
digest host literal. The remaining rows rest on the readers' quoted evidence in
`12f_triage_rows.md` and were not independently re-derived.

**IDs.** The assessments' own IDs clash with names already in the plan (`A7` and `A16` are
inventory rows; `F15` and `F16` are forks). Outside items are therefore prefixed here: `B-F01` …
`B-F17`, `A-1` … `A-11`. `INT-…` rows are adjacent defects found during the read (E5).

**Classes** are proposals for the PI's ruling. Where a reader hedged between two classes the
table gives one and the note says what the other reading is.

## The triage table

Columns: ID · source · present at HEAD · evidence level · overlap result · proposed class ·
size · PI decision · what is wrong and the minimum loud-failure fix. Owning state is **pre-tag
(R507, R512) for every row** unless the note says otherwise. Full fix and acceptance test for
each row are in `12f_triage_rows.md`.

### E1 — Assessment B (17 findings)

| ID | Source | Present | Evidence level | Overlap | Class | Size | PI | Finding at HEAD → minimum loud-failure fix |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| B-F01 | outside B | yes (version-overwrite sub-claim: partial — R99's `FileExistsError` guard exists) | reproduced | none given | 1 | S | yes | `parse_pdf` commits the asset, ref and attempt rows, then renames the temp file. A failed rename leaves committed rows with no file; the handler deletes the only copy; a non-forced retry short-circuits as success and `parse_all_pdfs` then sets `PARSED`. Reads are loud (`ParsedTextMissing`). → Publish the file before the commit; make the same-hash short-circuit check the file exists. |
| B-F02 | outside B | yes (all three parts) | reproduced | C12 confirmed (F02 is broader); I16/R222 related but a different defect (I16's closure holds); C37 family | 1 | M (full: L) | yes | A refused open still writes: `review_runs` 0→1 in a shape that contradicts receipts 012/013, journal mode `delete`→`wal`, directory recreated, fingerprint changed. A populated database with zero receipts is taken as fresh and all 18 migrations recorded `executed`. The refused object keeps a usable connection. Docstrings say the refusal comes "before any write". → Check receipts before any constructor-side write; refuse a populated database with no receipts. |
| B-F03 | outside B | yes (all four parts) | reproduced | none given | 1 | M | yes | `_refuse_if_open` releases its lock before `os.replace`. An idle holder of a DELETE-journal database is not refused and `restore` completes over it; a connection opened after the probe loses committed and later writes silently when its `-wal`/`-shm` are unlinked. No production caller — operator-run only. CLAUDE.md's "refuses an open target by exclusive lock" overstates. → Hold the exclusion across staging, replace and cleanup; or reduce the stated guarantee to what the code does. |
| B-F04 | outside B | yes (all five sub-claims) | reproduced | none given | 1 | M | yes | Same title + two distinct DOIs → 1 unique / 1 duplicate; two identical PubMed entries → 2 unique / 0 duplicates; fuzzy "part 1"/"part 2" with different DOI and PMID merge; indexes are not refreshed after a merge. → Never merge by title when both records carry different identifiers; record the pair for review. |
| B-F05 | outside B | yes | reproduced (full HEAD schema) | none given | 1 | M | yes | `add_papers` checks PMID only; the same DOI-only citation added twice gives two rows (also twice within one call). The entry importers are not affected (they refuse a non-empty review). → Refuse or match on normalised DOI at ingestion. |
| B-F06 | outside B | yes (five sub-claims; the "eligible-and-failed papers vanish" consequence is partial — they keep their own PRISMA rows) | reproduced (a–d); code order (e) | H1 confirmed; H7 confirmed (duplicates H1 on the literal 0); H2 confirmed as family but its row text is stale; C36 confirmed, narrower | 1 | L (min: S) | yes | B's fixture (10 PubMed + 8 OpenAlex, 3 duplicates) reports 15 identified, OpenAlex 5, duplicates 0. Exclusion reasons are counted per decision row, so every automatically excluded report counts twice and PI-adjudicated exclusions never carry the PI's reason; `validate_prisma_counts` still returns valid. `studies_included` = `audited_ai` (live: 0 of 190 eligible). H1/H7 are owned by senior (S8) in the plan; R512 names F06 pre-tag. → Mark identification and duplicate counts "not recorded" instead of printing 0; count one reason per report. |
| B-F07 | outside B | yes (all six triggers) | reproduced | E-METHODS confirmed as the owning row (B is broader); C36 confirmed (= trigger vi); C30 **rejected** as a regression | 1 | M | yes | An enabled cloud arm renders "Concordance extraction was additionally performed by …" with no call made (not reachable on live while R71 keeps `enabled_arms` empty); a zero-call stage names its model with the count hidden; an export-only run prints `[MODEL NOT SPECIFIED]`; the FT verifier drops out of the sentence when decision rows exist. → Render a visible placeholder for any sentence the run's recorded calls do not support. |
| B-F08 | outside B | partial | conditional | none given | 1 (latent) | S | no | The wrapper's control flow is as B says (a generator that raises yields a silent partial result), but the installed pyalex 0.20 paginator is a class that keeps its cursor: one failure re-requests the page, three raise. `pyalex` is unpinned and `search_openalex` never reads `meta["count"]`. By present behaviour this is Class 3; it is Class 1 only if an unpinned reinstall changes the paginator. → Pin `pyalex`; refuse a result shorter than the advertised count. |
| B-F09 | outside B | yes (trigger not established) | code order | none given (H4, closed, is adjacent) | 2 | M | no | `export_all` has no shared snapshot, runs the grid twice inside one workbook, publishes files one by one, and every writer uses `output_path + ".tmp"` (a failed export can unlink a concurrent export's temp file). One caller, behind the audit-review gate. A late exporter failure leaves old and new files mixed with no marker — by that consequence a reader could call this Class 1. → Write to a staging directory and publish only when every artifact succeeds. |
| B-F10 | outside B | yes (all five sub-claims) | code order (nothing run — I22) | D20 and I22 **rejected** as the same defect (same stage only); no row covers F10 | 1 | M | yes | Every strategy reads `.content` whole and writes to the final path; the 100 MB ceiling checks only the `Content-Length` header; the tar member is read whole and `pdf_members[0]` taken; acceptance is a 4-byte `%PDF` prefix. Worse than B says: the resume check marks a partial final-path file `download_status='success'` on the next run. The acquisition cut-over is junior in the plan (D20); R512 names F10 pre-tag. → Download to a temp file and rename; refuse a package with more than one PDF to the manual list. |
| B-F11 | outside B | yes | reproduced (the truncation function); code order (the guard) | D5 confirmed — its "no input-fit guard" half is now untrue as written (the guard runs, but only sees the already-cut text); S3f did not land for FT screening | 1 | M (L if re-screen) | yes | `truncate_paper_text` is a prefix cut with no omission marker; the docstring's section prioritisation is not implemented; primary and verifier see the same cut. Also: the references shortcut matches any line starting `references…`, and cut an over-length body to 50 characters in the reproducer. → Tell the model text was omitted and send a truncated-input exclusion to a human. |
| B-F12 | outside B | yes | code order | none given | 2 | S | yes | `_stage_screen` logs "Limiting screening to first N", executes `pass`, then calls unbounded `run_screening`. `--limit` is unvalidated and not in the manifest. A smoke run screens the whole pending corpus. → Refuse `--limit` on a screen start until it is a real bound. |
| B-F13 | outside B | yes (four sub-claims; the verification half is latent — E9) | reproduced (synthetic DB, stubbed model); code order (checkpoint) | S4 confirmed as the planned home, broader; E9 confirmed related, not the same | 1 | S (min) / L (S4d) | yes | Pass 1, pass 2 and status are three commits; a failure after pass 1 leaves an orphan row and the rerun adds a second with nothing distinguishing them; a second identical `run_verification` doubled the rows. PRISMA's reason query counts those rows. → One transaction per paper. |
| B-F14 | outside B | yes (server-side effect not established) | reproduced (real `ollama_chat`, fake client) | E-EXEC confirmed **partial**, as the brief expected | 2 | M | no | After a watchdog timeout the worker thread keeps running; a new executor is made per attempt; 3 requests were still running when `TimeoutError` was raised; process exit waited for the blocked call. One `run_calls` row stands for up to four requests sent. Whether closing the client cancels generation is a Phase A measurement. → Do not start another attempt while the previous one is still running. |
| B-F15 | outside B | yes for a leading `=` in xlsx and for all CSV; narrower than B for other prefixes | reproduced | none given | 1 | S | no | openpyxl 3.1.5 stores `=1+1` as a formula cell; no writer neutralises it — evidence table, the shared review-workbook builder, the PI-audit workbooks. The cell the PI adjudicates is `arm_value`, so a model value starting `=` reaches a human gate as a formula. Reach is narrow; a reader could call this Class 2. → Force text type on untrusted cells. |
| B-F16 | outside B | yes (live not exposed today) | reproduced | none given | 1 (latent) | S (guard) / M (versioned scheme) | yes | B's exact pair collides at HEAD and `--compare` prints IDENTICAL; a TEXT cell can also impersonate an INTEGER or NULL cell. **Live scan (`mode=ro`): 36 tables, 384 columns, 590,185 text cells — none contains U+001F or U+001E.** The reader proposed Class 3; this table proposes 1 because the instrument that can be silently wrong is the integrity check itself. → Refuse to fingerprint a database whose text contains a separator. |
| B-F17 | outside B | yes (seven sub-claims; the SDK import is narrower than B states) | code order (a–f); reproduced (g) | C18 **rejected** (a closed subset); I20 **rejected**; I3 / NIGHTLY-LOCK-01 confirmed (same cron line, narrower) | 2 ((g) is 2; the rest 3) | S / M | yes | 10 of 15 requirements unpinned (incl. `pydantic`); `requests`, `httpx`, PyMuPDF, `fontTools`, `pandas` imported but undeclared; `pytest` declared nowhere; the nightly runs every tier and ends `exit 0`, so cron never sees a red run; creating a **fresh** review database imports both provider SDKs through migration 014 and fails without them. → Declare the imports; make the nightly's exit code its result. |

### E2 — Assessment A

| ID | Source | Present | Evidence level | Overlap | Class | Size | PI | Finding at HEAD → minimum loud-failure fix |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A-1 | outside A | partial | code order | — | 2 | S (min) / M (policy) | yes | A's child-worker scenario is **not present** (children of a holder are refused by the lock). The real opening is the reverse: `foreign_lock_held()` passes when nobody holds the lock, and only `run_extraction` and the eval harnesses hold it — screeners, auditor, vision parse, the judge passes and the nightly's model tier can be restarted beneath. `pass2_full` restarts on a cadence and takes no lock. The env opt-out protects the process that sets it, not a victim. → Have every model-calling entry point hold the lock. |
| A-1N | architect-derived | yes | code order | I3 / NIGHTLY-LOCK-01 **confirmed** — the same defect | 2 | S | no | The nightly neither takes nor checks the experiment lock and runs the model tier; the suite fence stops it restarting Ollama but not loading models. Existing row; nothing new. → As I3. |
| A-2 | outside A (generalised) | partial | reproduced + code order | C39, D25 (the FKs this is the precondition for) | 1 (latent) | S (guard) / M (one connection factory) | no | Foreign keys are inert on a connection without the pragma (reproduced). Every connection that reaches the event, manifest and audit writers has it ON today; nine other writer sites, the runner and all migrations do not. The event store's append-only guarantee rests on triggers and CHECKs, not FKs; referential refusals (unknown arm, `run_calls` stage) are FK-only. → 023 asserts `PRAGMA foreign_keys` is ON before relying on a new key. |
| A-3 | outside A | yes | reproduced + code order | I5 (its guard test covers four other files) | 3 | S | yes | Six `immutable=1` opens, all in `analysis/eval/`, all aimed at the live `review.db`; none in `engine/` or `scripts/`. Breaks CLAUDE.md's "never `immutable=1`". Engine results are not affected. → `mode=ro` at the six sites, or retire the scripts. |
| A-4a | outside A | yes (the rebind); no (the consequence) | code order | — | 3 | S | no | `run_qualgap01.bind_runtime` assigns `oc._client` and never restores it — in a one-shot CLI nothing imports and that opens no manifest. → None needed; note only. |
| A-4b | architect-derived | yes (structure); conditional (a wrong manifest) | reproduced, no network | none | 1 (latent) | S (refusal) / M (one host) | no | The digest read is a bare `httpx.get` to the literal `http://localhost:11434`; chat calls go through `ollama.Client`, whose host follows `OLLAMA_HOST`. With `OLLAMA_HOST` set (or `_client` rebound), a manifest and arm pin record one server's digests while calls go to another, with no error. `OLLAMA_HOST` is unset in this shell, the profile files and the crontab. → Refuse to open a run when the client's host is not the digest host. |
| A-5 | architect-derived | yes | code order | none | 3 | S | yes | Three engine imports of `analysis.provenance.segment.sentences` (`elicitation/units.py`, `elicitation/contracts.py`, `parsers/parse_quality.py` — the last on the unconditional parse path). The elicited path does **not** use `analysis/eval/elicit01/units.py`; it has its own port. Documented in the module docstrings. → None; a layering decision. |
| A-6 | outside A | **no** | not present at HEAD | C50 and C56 **rejected** as the same defect (C50 is present and adjacent) | — | — | no | `paper_state.py` and `completeness.py` contain no SQL; `advance_stage.py` touches `workflow_state` only; nothing writes `extractions` or `evidence_spans`. The brief's expectation holds. The read found INT-g6-2 beside it. |
| A-7 | outside A (narrowed) | partial | conditional | C37 confirmed (why a second process's status read is a writer) | 2 | S (min); the full fix is C37's | yes | No contention inside one run: every writer shares one connection, telemetry is a file, gate-check writes bracket the extract loop rather than run during it. A lock error caused by a **second process** lands as `extraction_failed` / `unclassified_error` (a paper failure), or is swallowed in `_advance_extraction_workflow`. Timeout is 5 s everywhere. Not established: any process that holds the write lock that long. → Treat `database is locked` as a run fault (see C56). |
| A-8 | outside A | — | n/a | — | — | — | — | Token inflation from `[Sn]` markers against the input-fit estimate — **n/a — measured in 12g**. |
| A-9 | outside A; architect-derived for locator-1 | — | n/a | — | — | — | — | Hyphenation / soft-hyphen / ligature misses — **n/a — measured in 12g**. |
| A-10 | outside A (conditional) | partial | reproduced | none | 1 | M | yes | A's offset drift **cannot occur** — no offset-based locator exists on any engine path. The underlying mismatch is real: materialised quotes are built from comment-stripped text and located against raw text, so a citation touching a Docling comment is not an exact substring (first 60 parsed files: 110 single units, 1,665 adjacent pairs, 4.5%). Such a claim is recorded `located = false` and goes to semantic verification. Contradicts "ANCHORED by construction" in CLAUDE.md. Elicited path only. → Locate against the same text the quote was built from. |
| A-11 | outside A (conditional) | partial | reproduced + code order | none | 1 | S (floor) / M (format change) | yes | Pass 2 sends the array schema with no `minItems` on both paths. A collapsed response is refused and retried three times — then stored as `extracted` with what it has (R140), analysis-ready, the missing fields only in `payload.incomplete_fields`, which nothing reads. The driver's log line and `completeness.py`'s docstring say no partial extraction is stored. → Make the stated guarantee and the behaviour agree. |

### E3 — C56 (existing row; this session's census is its Phase A)

| ID | Source | Present | Evidence level | Overlap | Class | Size | PI | Finding at HEAD → minimum loud-failure fix |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C56 | existing row | yes | code order | C50 adjacent (re-raises); E-CALL adjacent; A-7 lands here | 1 | M (one predicate, about eight sites; the two cloud sites are behind R71) | yes — two questions below | 95 broad handlers, 0 bare: **15 convert**, 30 propagate, 26 benign, 24 off the run path. There is no shared run-fault predicate; `UndeclaredOverride` and `sqlite3.Error` are on no re-raise list. No `ollama_chat` call site lacks a stage (11 sites, all `**cfg.kwargs()`). → One owned run-fault predicate, re-raised ahead of each converting handler. |

The fifteen converting handlers (each has an entry in `12f_C56-A.md`):

| # | Handler | What a run fault becomes | Proposed class |
| --- | --- | --- | --- |
| 1 | `extractor.py::_retry_snippet` — `except Exception: return (None, None)` | The field is stored with an empty snippet; no log, event or count. On the committed spec's extract path (legacy path, `elicitation: false`). The row's "empty or prior snippet" is always empty at HEAD | 1 |
| 2 | `extractor.py::_extract_selected` via `outcome_for_exception` | A sqlite3 error, `UndeclaredOverride` or any `RunRefused` member becomes `extraction_failed` / `unclassified_error`; an intermittent fault resets the three-in-a-row abort counter and the run closes `completed` | 1 |
| 3 | `run_pipeline.py` — `except Exception: pass` around `_advance_extraction_workflow` | Swallowed, unlogged. Since C58 the audit-review gate still stops, so this can no longer let EXPORT run; the comment's reason cannot occur | 1 (conservative) or 3 |
| 4–5 | `pdf_parser.py::parse_pdf` — vision on the scanned route, and on the digital-sparse route | `ParseFailed(parse_vision_exhausted)` → a `parse_failed` paper event | 1 |
| 6 | `parse_pdf` — the gate-driven re-route (`_record_error(...); break`) | No exception and no event; the least-bad gate-failed attempt is accepted and the paper can end terminal `PDF_EXCLUDED` / `PARSE_QUALITY`. The worst of the set | 1 |
| 7 | `pdf_parser.py::parse_all_pdfs` — per-paper `except Exception` | `parse_failed` / unclassified, for store sqlite3 errors, `UnknownStage`, a failed `update_status` after a committed parse | 1 |
| 8–9 | `OpenAIExtractor.run` / `AnthropicExtractor.run` — the retry loop | A `record_call` sqlite3 error after the provider answered is re-sent twice, then counted as a failed paper; the manifest closes `completed` | 1 |
| 10–11 | The same two classes — around `store_result` | A database fault becomes a failed-paper count; the paid result is discarded | 2, leaning 1 |
| 12–13 | `pdf_parser.py::_commit_attempts` — outer and inner | The `parse_attempts` ledger rows are lost with a log line | 3 |
| 14–15 | `ollama_preflight.py` — two handlers around `ollama.ps()` | VRAM reads as 0.0, so the budget refusal cannot fire | 3 |

Also from the census: `check_model` still wraps the recorder's sqlite3 error, `TimeoutError`,
client errors and the input-fit family as `RuntimeError("fix model availability")` — the run
stops, with the wrong message (Class 3). The audit loop, both screener loops and the elicited
path have no broad per-paper handler.

### E5 — Internal-12f rows (adjacent defects found during the read; not chased)

| ID | Source | Present | Evidence level | Class | Size | PI | Finding → minimum loud-failure fix |
| --- | --- | --- | --- | --- | --- | --- | --- |
| INT-g2-1 | internal-12f | yes | reproduced | 1 | S | yes | OpenAlex retrieval is silently capped at 10,000 works by pyalex's `n_max` default (advertised 25,000 → 10,000 yielded, no error). Whether live's stored search hit the cap was not established. → Pass `n_max=None`, or refuse when the advertised count exceeds what was retrieved. |
| INT-g3-1 | internal-12f | yes | reproduced (builder); code order (evidence table) | 2 | S | no | The engine's workbook writers raise `IllegalCharacterError` on control characters that the paper1 writers already strip. Loud. → Rides on B-F15's fix. |
| INT-g6-1 | internal-12f | yes | reproduced | 1 | M | no | Unit-map units are not always verbatim, independent of comments: pysbd rewrites characters, and an ellipsis is dropped from a joined run (first 60 files: 143 units, 1,911 adjacent pairs not exact substrings of even the stripped text). Same contradiction of "ANCHORED by construction" as A-10. → With A-10. |
| INT-g6-2 | internal-12f | yes | code order | 1 | S | no | Two engine paths set `papers.status = 'PDF_EXCLUDED'` by direct UPDATE, bypassing `update_status`: `pdf_quality_import.import_dispositions` validates only that the paper exists (any status → terminal exclusion, no event, no manifest), and `pdf_parser.parse_all`. C51's "one sanctioned bypass" sentence is inaccurate. → Route both through `update_status`. |

Adjacent observations the readers recorded **inside** rows rather than as rows (each is in
`12f_triage_rows.md` under the row named): B-F11 — the `references…` line shortcut and an
abstract at or over the budget yielding a "full text" with no body; B-F10 — a malformed
`Content-Length` or a non-regular tar member aborting the batch; B-F01 — `pipeline.md` step 7
names the wrong exception; B-F15 — CLAUDE.md's "used by all 3 adjudication exporters" (two
callers at HEAD); B-F17 — three packages declared and imported nowhere; C56 half B — the
importers' `except Exception` lets an interrupt skip the rollback and `close_run`, and
`parse_pdf`'s `except BaseException` commits ledger rows on an interrupt; A-11 — nothing reads
`incomplete_fields`. Row-text corrections owed, none made here: H2 (its query left `prisma.py`
at `2153193`), D5 (the guard half), C51, C56 ("empty or prior" is always empty).

## E4 — existing open pre-tag rows

Classified from each row's text in `docs/plan/ENGINE_REFACTOR_PLAN.md` (Step 2 inventory, and the
12e closure's "Next" for the housekeeping items). **Evidence level for every row here: "existing
row, not re-read"** — no code was read for these in 12f unless the "overlap" column names an
E1–E3 item, in which case that item's row carries the evidence. Source for every row: existing
row. Owning state: pre-tag (R507) unless the note says otherwise. "Min. fix" is the minimum
loud-failure fix; where the row is already loud or is hygiene it reads "n/a".

| ID | What the row says (short) | Overlap with E1–E3 | Proposed class — reason | Min. loud-failure fix | Full fix | Acceptance test | Size | PI flag |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| I20 | Gate wall time; one test leaves `ollama.ps` unpatched, so each gate run makes one `GET /api/ps` to the live server | F17 (nightly/test boundaries), partial | 3 — test hygiene; nothing stored is affected | n/a | Patch `engine.utils.ollama_preflight.ollama.ps` in `test_raises_on_failure`, as its sibling does; re-cut the band | The gate makes zero requests to the live server (Ollama journal quiet across a gate run) | S | no |
| C50 | `events.write_field_event(commit=False)` rolls back the caller's whole transaction on error; `write_paper_event` has no handler | A6; C56 census (b) | 2 — the error is re-raised, so it is loud; no current caller is harmed. Becomes Class 1 only for a caller that catches and continues (none found in the row) | n/a (already raises) | Use a SAVEPOINT in the helper when `commit=False`, or document and test that a helper error ends the enclosing transaction | A `commit=False` helper error leaves the caller's earlier writes in the open transaction (or a test pins the documented whole-transaction rollback) | S | no |
| E-UNITMAP | The elicitation unit-map directory is keyed by process UTC time, not `run_id`; its only link back is a telemetry field | — | 1 — the unit map is where a claim's full citation set lives (the span carries the first contiguous run only); a link by wall-clock time can leave that provenance unattributable to its run | Write the `run_id` (and paper id) into the unit-map path or a sidecar at creation; refuse to write a map outside an active run | Key the directory by run uid; record the path against the call (`run_calls` extra) | From a claim, the unit map of the run that produced it is found by id alone, with two runs started in the same second | S | no |
| E-ROOT | `SPEC_ROOT` resolves against the working directory while `git_state` uses `REPO_ROOT` | observed in 12f Step D: a pure recompute run from another cwd failed loudly with `CodebookError: Codebook not found at data/surgical_autonomy/extraction_codebook.yaml` | 2 — a run from another directory fails loudly (measured for the codebook path); the manifest hashes the spec it actually loaded, so a wrong-tree spec is recorded as what it is | n/a (loud) | Resolve spec, codebook and data roots from one root | A run launched from a foreign cwd either works identically or refuses naming the root | S | no |
| E-METHODS (+B23 design D, +R364) | `methods_section`'s fixed prose describes the two-pass method for an elicited run; its `n` counts `COUNT(DISTINCT rc.paper_id)` over all outcomes; the auditor's object is misdescribed | F07 (same module; see F07 for whether B's triggers are the same defect) | 1 — generated methods text is a statement of the review's provenance; it is silently wrong for the Run 7 design | Emit a refusal or a visible placeholder where the run's recorded stages do not match the fixed prose | Generate each sentence from the run's stage rows and counts; fold into F07's full fix | An elicited run's methods text names the elicited contract, and every `n` matches the count its sentence claims | M | no (ride with F07's ruling) |
| E-TEST | No test opens a manifest with `elicitation_pass1`, verifies the digest against that pin and writes through the real event writer | — | 3 — test hygiene (a missing test on the Run 7 path; no behaviour is known wrong) | n/a | Add the test, model call mocked | The test fails if the elicited stage row, pin or writer is disconnected | S | no |
| C48 | FT decision calls record `run_calls.paper_id` NULL; the event still links to its call by request hash | — | 3 — telemetry; the link exists by another key | n/a | Pass `paper_id` from `ft_screen_paper` / `ft_verify_paper` | FT `run_calls` rows carry `paper_id` | S | no |
| C49 (widened, R491) | Three `require_preflight` call sites pass no `spec=`; equal on the live spec, which sets no `preflight` or `ollama` block | — | 2 — latent function defect: a spec that sets those blocks would probe with options its run did not declare; nothing differs today | n/a | Pass `spec=` at the three sites | With a spec that sets a `preflight` block, the probe's options equal the declared row at all three sites | S | no |
| B18 | The human workbook importer maps `NR` (the canonical absence sentinel, R132) to None — a human "not reported" becomes a blank | — | 1 — a stored human claim would be wrong (an absence claim erased) | Refuse an import whose cell is `NR` until the importer preserves it | Preserve `NR` as a value that requires a citation (R206) | A workbook cell `NR` is stored as the sentinel and distinguishable from a blank | M (Phase A first, as ruled) | no |
| B19 | A human extractor claim is refused without parsed-text input identity; `is_assigned` returns only for `model`, so every human cell reads as out of scope | — | 2 — the write is refused loudly; nothing is stored wrong. (The reader half — human cells read "out of scope" — would be Class 1 once such claims exist) | n/a (refuses) | R206's design: input identity scoped to model actors; human claims carry `pdf_hash`; the assignment table admits human arms | A human claim is written and read back as assigned | L (design; assignment table) | **yes** — is the human-arm load (B18/B19) still pre-tag under R507, given Run 7 needs no human arm? |
| C38 | Five postconditions of migration 022 are pinned by no test | — | 3 — test hygiene | n/a | Add the five pins | Each postcondition fails a test when mutated | S | no |
| B12 | Test copies of the status→eligibility map disagree (one on purpose) | — | 3 — test hygiene | n/a | One fixture map, the deliberate difference named | One source of truth in `tests/` | S | no |
| A16 | `check_schema_parity`'s arm test is the name `"local"`; the live local arm `local_deepseek_r1_32b` takes the cloud branch. Plus the frozen `corpus.py` docstring's stale count | — | 2 — a read-only diagnostic reads the wrong table for the live arm (unusable, nothing stored); the docstring half is Class 3 | Route by `registered_arms` kind, not by name; refuse an unknown arm | Same; correct the docstring by a note (the file is frozen, R35) | The validator reads the local table for a local arm of any name | S | no |
| C56 | Broad handlers convert a run fault into a paper outcome | E3 (this session's census) | see the C56 summary row | | | | | |
| HK-RETIRE | Retirements: `scripts/advance_to_pdf_acquired.py`, I19's six scripts | — | 3 — hygiene (I19's retirement removes parametrized test ids, so the gate count moves) | n/a | Remove, with the retention ledger entry | Inventory in sync; id delta explained | S | no |
| I21 | Seven nested `review.db` paths under `data/` | — | 3 — retention at the freeze | n/a | Move or remove per the retention ledger | No nested `review.db` under `data/` | S | no |
| HK-ROOTDB | Root-level `evidence_engine.db`, 0 B | — | 3 | n/a | Remove | Absent | S | no |
| HK-12C | The 12c notes | — | 3 — docs | n/a | As noted at 12c | — | S | no |
| HK-ARCH | Architecture docs still describe the retired auditor | — | 3 — docs (a stated guarantee to check against behaviour, per the discipline) | n/a | Rewrite the affected pages | Docs match `audit_events` | S | no |
| HK-WITHMODEL | `with_model`'s docstring (12e_C54-A F6) | — | 3 — wording | n/a | Correct the docstring | — | S | no |
| R501 | The audit-review stop message names the first pending stage (on live "Current stage: PDF_ACQUISITION"), the wrong next step at Run 7's end | — | 3 — wording, as ruled (R501); note it is wording at a human gate | n/a | Name the audit review and its next step | The stop at the audit-review gate names the audit review | S | no |
| C39 (023) | `audit_verdicts.arm` has no FK to `arms` | A2 (FK enforcement census) | 1 — a provenance constraint is absent | n/a until 023 | 023 adds the FK | A verdict naming an unregistered arm is refused | L (migration) | no |
| E-CALL (023) | An interrupted in-flight model call writes no `run_calls` row; the outcome CHECK admits no `interrupted` token | C56 census (a) | 1 — a call that was sent leaves no record | n/a until 023 | 023 admits `'interrupted'`; `ollama_chat` records it | An interrupted call has a `run_calls` row | L (migration) | no |
| D22 + writer (023) | The audit stage skips a paper whose parsed text is refused with a log line only; the paper stays `extracted` and analysis-ready (ruled C3, `audit_not_possible`) | — | 1 — a paper audit could not run on reads as analysis-ready | n/a until 023 | 023's new state and its writer | An audit-refused paper carries `audit_not_possible` with a reason and is not analysis-ready | L (migration) | no |
| D23 refusal writer (023) | Code closed; the refusal writer lands with D22 | — | 1 — same family as D22 | n/a until 023 | With D22 | As D22, per reason code | L (migration) | no |
| D25 (023) | `claim_inputs.parsed_text_uid` has no FK to `parsed_text_refs` | A2 | 1 — a provenance constraint is absent | n/a until 023 | 023's `claim_inputs` rebuild | A claim input naming an unknown text uid is refused | L (migration) | no |
| D26(b) (023) | `claim_inputs.extraction_uid` is not NOT NULL (the writer already refuses, D26a) | — | 1 — the schema still admits the row the writer refuses | n/a (writer refuses) | 023's rebuild | A NULL uid is refused by the table | L (migration) | no |
| D14 (024) | 250 non-corpus parsed files have no reference; a re-parse of one would refuse. Count unmeasured since INPUT-IDENTITY-01 | — | 2 — no corpus paper affected; the path refuses | n/a (refuses) | 024 seeds the missing refs after measuring | Every parsed file has a ref | L (migration) | no |

## Summaries (F2)

**Rows by proposed class × evidence level** (62 rows; `A-6`, `A-8`, `A-9` carry no class).

| Class | reproduced | code order | conditional | existing row, not re-read | total |
| --- | --- | --- | --- | --- | --- |
| 1 — integrity | 17 | 3 | 1 | 9 | **30** |
| 2 — function | 2 | 5 | 1 | 6 | **14** |
| 3 — wording, telemetry, docs, test hygiene | 1 | 2 | 0 | 12 | **15** |
| no class | — | — | — | — | 3 (`A-6` not present; `A-8`, `A-9` measured in 12g) |

- Class 1: reproduced — B-F01, F02, F03, F04, F05, F06, F07, F11, F13, F15, F16; A-2, A-4b,
  A-10, A-11; INT-g2-1, INT-g6-1. Code order — B-F10, INT-g6-2, C56. Conditional — B-F08.
  Existing — E-UNITMAP, E-METHODS, B18, C39, E-CALL, D22, D23, D25, D26(b).
- Class 2: reproduced — B-F14, INT-g3-1. Code order — B-F09, B-F12, B-F17, A-1, A-1N.
  Conditional — A-7. Existing — C50, E-ROOT, C49, B19, A16, D14.
- Class 3: reproduced — A-3. Code order — A-4a, A-5. Existing — I20, E-TEST, C48, C38, B12,
  HK-RETIRE, I21, HK-ROOTDB, HK-12C, HK-ARCH, HK-WITHMODEL, R501.

Where a row mixes levels the count takes the level of its main claim: B-F06 and B-F11 are
counted reproduced, B-F17 code order (its SDK-import part is reproduced), A-4b reproduced (the
structure; the wrong-manifest consequence is conditional).

**Outside findings by presence.**

| | present | partial | not present | not read (12g) |
| --- | --- | --- | --- | --- |
| Assessment B (17) | 16 | 1 (B-F08) | 0 | 0 |
| Assessment A, its own claims (10) | 2 (A-3, A-4a) | 5 (A-1, A-2, A-7, A-10, A-11) | 1 (A-6) | 2 (A-8, A-9) — one of A-9's two halves is architect-derived |
| Architect-derived (3) | 3 (A-1N, A-4b, A-5) | 0 | 0 | 0 |

Assessment B by evidence level: reproduced 12 (F01–F07, F11, F13–F16), code order 4 (F09, F10,
F12, F17), conditional 1 (F08). B's own "reproduced" claims all reproduce at HEAD with the
figures B gives.

**Overlaps.** Confirmed (16 pairings): C12, I16/R222 (related, different defect), C37 (twice: F02, A-7),
H1, H7, H2 (family; row text stale), C36 (twice: F06, F07), E-METHODS, D5, S4, E9, E-EXEC
(partial, as expected), I3 / NIGHTLY-LOCK-01 (twice: F17, A-1N). Rejected (7 pairings): C30 (not a
regression), D20 and I22 for F10, C18 and I20 for F17, C50 and C56 for A-6.

**R510 — pre-tag rows in / out.** In (candidates; none opened as inventory rows — the PI rules
the list first): **30** — 17 from B, 7 of A's own (A-1, A-2, A-3, A-4a, A-7, A-10, A-11), 2
architect-derived that no row covers (A-4b, A-5), 4 internal-12f. A-1N is existing row I3. Out:
**none**. Existing rows classified: 27 (E4) + C56. By class across all 62: 30 / 14 / 15, 3
without a class.

## PI-decision rows

Each is stated so it can be answered in one line.

1. **B-F06** — Is a study "included" when it is eligible (live: 190) or only at `audited_ai`
   (live: 0)?
2. **B-F11** — Live's full-text decisions were made on truncated input (366 — D5's count,
   carried from the plan, not re-measured): engine fix only, or re-screen those papers?
3. **B-F13** — R512 pulls F13 pre-tag; R183/R323 keep the screeners' cut-over (S4, E9) in
   junior. Is the pre-tag obligation the minimum fix only (one transaction per paper, a
   verified-paper filter), with run and configuration identity staying in S4?
4. **B-F10** — The same collision for acquisition (its cut-over is junior, D20; I22 forbids
   running any acquisition tool): minimum fix pre-tag, or the whole of F10? And for a PMC
   package with more than one PDF: refuse to the manual list, or select by package metadata?
5. **B-F01** — After publishing the file before the commit, should an orphan file stay a manual
   R99 stop, or be adopted automatically when its bytes match a fresh parse?
6. **B-F02** — Should a database with user rows and zero receipts refuse an ordinary open,
   reversing R222's "no receipts = fresh"?
7. **B-F03** — Should restore take a maintenance lock every opener shares, or stay an operator
   procedure with the code's stated guarantee reduced to match?
8. **B-F04** — Same title, different DOI or PMID: keep both, or queue the pair for a person?
9. **B-F05** — A search stage run against a non-empty review: refuse (as the entry importers
   do), or append with identity matching?
10. **B-F07** — Should the methods text describe the run that invoked the export, or every run
    that produced an exported value?
11. **B-F12** — Make `--limit` a real, manifest-recorded screening bound, or retire it?
12. **B-F16** — Version the fingerprint scheme now (which makes every committed record
    non-comparable and supersedes the record of reference), or ship only the separator guard?
13. **B-F17** — Should the nightly run the standard gate only, with the model and network tiers
    moved to a separate lock-holding job?
14. **A-1** — Is the experiment lock mandatory for every model-calling entry point, or does it
    stay extraction-only with non-holders accepted as restartable?
15. **A-3** — Fix the six eval readers to `mode=ro`, or retire those scripts under R31?
16. **A-5** — Keep the documented exception (the engine imports `analysis.provenance.segment`),
    or move the segmenter into `engine/`?
17. **A-7 / C56** — Is `database is locked` during a run a run fault (stop, paper untouched), or
    a paper failure as now?
18. **C56** — Are `TimeoutError`, client errors and `CeilingUnavailable` run faults or paper
    outcomes? On the extract path they are paper outcomes today, bounded by the abort counter.
19. **C56** — Does the row cover store `sqlite3` errors (handlers 10–13 and most of what
    reaches handler 7), or only the fault classes its text names?
20. **A-10** — Locate elicited citations against the comment-stripped text (a new locator
    version), or make a comment break a citation run?
21. **A-11** — Under R140, should a record missing most of its fields still store as
    `extracted`, or fail below a completeness floor?
22. **INT-g2-1** — Was live's OpenAlex query above 10,000 hits when it ran (does the stored
    corpus need checking against the cap)?
23. **B19** — Is the human-arm load (B18/B19) still pre-tag under R507, given Run 7 needs no
    human arm?

## Differences between the originals and the brief's notes

- **C3's premise.** The brief said "add to the core assumption discipline"; the plan had no
  section of that name. The PI chose the placement (a new subsection after "Rules that hold
  across every session") before item 0c was committed.
- **F08.** The brief asked whether it is "reproduced" or "conditional": conditional. B was right
  to hold its incidence at medium.
- **F14 / E-EXEC.** Partial, as the brief expected.
- **F16.** The inferred 0 for the live separator scan is now measured: 0.
- **Overlap pairings rejected (I4):** C30 for F07; D20 and I22 for F10; C18 and I20 for F17;
  C50 and C56 for A-6.
- **A5's example.** The brief suggested `analysis/eval/elicit01/units.py` might be on the
  elicited path. It is not; the engine has its own port. What the engine does import from
  `analysis/` is the segmenter, `analysis.provenance.segment`.
- **A6.** The brief expected the three files "absent or no longer writing". They are present
  and contain no SQL (`paper_state.py`, `completeness.py`) or touch `workflow_state` only.
- **A1.** A's scenario (children bypassing the lock) is not present; the exposure is to
  processes that never take the lock.
- **A10.** A's stated failure (offset drift) cannot occur; the mismatch behind it is real.
- **Where B understates:** F01 (the paper is later marked `PARSED`), F02 (journal mode flips;
  the recreated table contradicts its receipts), F03 (a never-used connection is invisible in
  WAL mode too), F06 (double counting is the rule, not an edge), F10 (the partial file is
  accepted as success on the next run).
- **Where B overstates:** F01's version-overwrite sub-claim (R99's guard exists), F06's "eligible
  failures disappear" (they keep their own PRISMA rows), F15's "formula markers" (only a leading
  `=` in openpyxl), F17's SDK import (only on first construction of a database, through
  migration 014).
- **Where A is wrong about HEAD:** a "connection pool" and "background workers writing
  telemetry" do not exist; `analysis/paper1/adjudication.py` uses neither `mode=ro` nor
  `immutable=1`; the engine does not send the required-slot schema A credits as the fix.
- **ID clashes.** `A7`, `A16`, `F15` and `F16` already name other things in the plan; hence
  the prefixes here.
- **Carried, not measured here:** D5's 366 (v49 FT-INPUT-01); D14's 250 (INPUT-IDENTITY-01).

---

*Addendum 2026-10-09 (session 12i close; R582(2)) — the reader count and roles, as the 12f
record supports them. No line above is edited.*

"How the read was done" says the findings were read "by eight parallel read-only readers (five
for B, two for A, two halves of the C56 census)". The parenthesis adds up to nine. What the
committed record shows, read at the 12i close (`12f_triage_rows.md`, `12f_C56-A.md`, and the
scratch directory `~/scratch/12f/`):

- **Eight reader tags exist:** `g1`, `g2`, `g3`, `g4`, `g5`, `g6`, `c56a` and `c56b` — six
  finding readers and the two halves of the C56 census. This agrees with "eight".
- **Scratch paths in `12f_triage_rows.md` tie five tags to Assessment B's findings and one to
  Assessment A's:** `g1` — F01, F02, F03; `g2` — F04, F05; `g3` — F06, F15; `g4` — F11, F13;
  `g5` — F16, F17; `g6` — A2, A3 (and `~/scratch/12f/g6/` holds working directories named
  `a2`, `a3`, `a4`, `a10`, `a11`). The adjacent rows carry the same tags (`INT-g2-1`,
  `INT-g3-1`, `INT-g6-1`, `INT-g6-2`).
- **Not recorded:** which reader read the sections that name no scratch path (F07, F08, F09,
  F10, F12, F14; A1, A1-NIGHTLY, A4, A5, A6, A7, A10, A11). The record therefore supports
  5 (B) + 1 (A) + 2 (C56) = 8 by tag, and does not show a second reader for A; "two for A" is
  the part of the sentence the record does not support. It is left as written.
