# Evidence Engine repository assessment: problems and suggested fixes

**Assessment date:** October 7, 2026 (America/Los_Angeles)  
**Source:** `repomix-output-ankit-sarin-evidence-engine.md`  
**Source SHA-256:** `7507f4d9f89bf68831b142a6a90e2bf802468c264d64c422c4cb9d9819764e97`

## Overall assessment

The repository has a substantial foundation for a research evidence engine: strict review specifications, append-only event history, explicit claim and input identities, hash-verified parsed text, arm configuration pinning, migration receipts, provenance-aware readers, and safeguards against tests changing live data or restarting shared services. Preserve these mechanisms.

The most consequential remaining problems concern **durable publication of parsed files, safe database opening/restoration, literature identity, and truthful reporting**. Several protections are individually sound but do not cover the complete operation. For example, an atomic file rename does not make the combined database-and-file write atomic, and a migration refusal does not prevent writes that happened earlier in the constructor.

**Recommendation:** fix the P1 findings before treating a new review's generated PRISMA counts, methods section, or exported evidence package as publication-ready. Continue development and supervised exploratory runs; a wholesale rewrite is not justified by this assessment.

There are **17 findings: 9 P1 and 8 P2**. No P0 emergency is established from this snapshot. Severity describes consequence under the stated trigger, not evidence that the live surgical-autonomy corpus is already affected.

## Scope and verification

- Reconstructed 762 text files from the supplied Repomix export. Original source files and the packed attachment were not modified.
- Parsed all 375 reconstructed Python files with Python's AST parser: **no syntax errors**. Counted 2,563 test functions in `tests/`; this is an inventory count, not a pass count.
- Reviewed active search, deduplication, database construction/migrations, acquisition, parsing publication, screening, local model calls, provenance readers, exports, and related tests. The engine contains approximately 35,018 Python source lines; this was a targeted assessment, not an exhaustive semantic review of every line or historical experiment.
- Ran isolated, local reproductions using synthetic citations, temporary SQLite databases, and an extracted pagination function. Confirmed identity, migration-refusal, restore-probe, and fingerprint problems described below.
- **The repository's pytest suite was not run.** Pytest, Ollama, PyAlex, Biopython, and cloud SDKs were unavailable in this environment. A fresh `ReviewDatabase` smoke check stopped at migration 014 because importing `engine.cloud.schema` eagerly imported the absent Anthropic SDK. This is an environment limitation, not evidence that a correctly provisioned production installation fails.
- No live corpus, model server, cloud API, or external literature search was accessed. Binary artifacts and files omitted by Repomix were not available. Reported code locations refer to reconstructed source-file line numbers, not packed-file line numbers.

### Priority definitions

| Priority | Meaning |
|---|---|
| P1 | Can change scientific results, misreport provenance, or compromise durable state under a concrete trigger; fix before dependable production or publication use. |
| P2 | Reliability, security boundary, reproducibility, or operational problem; schedule after the immediate correctness repairs. |

### Findings at a glance

| ID | Priority | Problem | Evidence level |
|---|---|---|---|
| F01 | P1 | Parse references commit before their files are published | Confirmed by code ordering |
| F02 | P1 | Database construction writes before a pending-migration refusal | Reproduced |
| F03 | P1 | Restore checks do not guarantee exclusive ownership through replacement | Probe limitation reproduced; race confirmed by ordering |
| F04 | P1 | Title matching can merge records with conflicting identifiers | Reproduced |
| F05 | P1 | DOI-only ingestion is not idempotent | Reproduced |
| F06 | P1 | PRISMA lacks raw search identity and counts decision rows as reasons | Confirmed by code |
| F07 | P1 | Methods text can claim operations that did not happen | Confirmed by code |
| F08 | P1 | OpenAlex retry can silently finish after a failed generator | Conditional path reproduced |
| F09 | P2 | Exported files and workbook sheets need one shared snapshot | Confirmed by code; concurrency trigger |
| F10 | P2 | Download/archive limits are not enforced on actual bytes | Confirmed by code |
| F11 | P1 | Full-text exclusions can be decided from an unmarked prefix | Confirmed by code; scientific risk depends on omitted content |
| F12 | P2 | Screening ignores the requested paper limit | Confirmed by code |
| F13 | P2 | Screening attempts, status, and checkpoints are not one durable unit | Confirmed by code |
| F14 | P2 | Model watchdog abandons running threads rather than cancelling work | Confirmed by code; timeout trigger |
| F15 | P2 | Untrusted export strings can become spreadsheet formulas | Library behavior reproduced; export path confirmed |
| F16 | P2 | Database fingerprint serialization is ambiguous | Reproduced |
| F17 | P2 | Environment and routine test execution are insufficiently reproducible | Confirmed configuration gaps |

## Detailed findings

### F01 — Parsed text can have a committed reference but no final file

**Priority:** P1. **Confidence:** high.  
**Location:** `engine/parsers/pdf_parser.py:941–984` (`parse_pdf`).

**Problem and trigger.** The parser writes a temporary Markdown file, inserts the asset/reference/attempt rows, commits the database, and then renames the temporary file to its final name. A crash between commit and rename leaves a valid-looking reference to a nonexistent file. If rename raises an ordinary exception, the handler deletes the temporary file and calls rollback, but the database commit has already happened. Rollback cannot remove those committed rows.

**Consequence.** Later reads raise `ParsedTextMissing`; the stored parse history and attempt ledger imply a completed artifact that was never published. The temporary file may have been the only recoverable copy. Existing `test_atomic_write_no_temp_on_db_failure` tests an insertion failure, not this post-commit failure.

**Suggested fix.** Publish an immutable, uniquely named file before committing its reference. Flush and fsync the file, atomically rename it, fsync the directory where required, then commit the asset/reference rows. A crash can then leave an orphan file, which is recoverable, instead of a reference to absent content. Alternatively, introduce an explicit pending/published state and a startup recovery journal. Serialize version allocation and never overwrite an existing version.

**Acceptance test.** Inject failure before commit, immediately after commit, and at rename; simulate process termination. Every committed reference must resolve to its recorded bytes after restart. Any orphan must be identifiable and safely recoverable. Retrying must not overwrite earlier versions.

### F02 — The migration refusal happens after constructor-side writes

**Priority:** P1. **Confidence:** high.  
**Locations:** `engine/core/database.py:374–453`; `engine/migrations/runner.py:255–295`.

**Problem and trigger.** `ReviewDatabase.__init__` creates directories, enables WAL, executes `_SCHEMA`, and runs inline schema maintenance before the numbered runner checks pending migrations. The pending-migration guard therefore does not make an ordinary open a write-free refusal. The runner also regards a database with no receipts as fresh even if it already contains substantial historical data.

**Reproduction.** In a synthetic database with receipts through 021 and 022 pending, remove `review_runs`, then construct `ReviewDatabase`. Construction raises `PendingMigrations`, but `review_runs` has already been recreated: **before = 0 tables; after = 1**.

**Consequence.** An inspection command can change a live-style database before refusing to open it. A populated, unregistered legacy database can enter the fresh initialization route and receive schema changes without explicit adoption.

**Suggested fix.** Separate `create_new`, `open_existing`, `open_readonly`, and `migrate`. Inspect an existing database and receipts before any schema/WAL mutation. Detect freshness by absence of user schema/data, not absence of receipts alone. Require explicit adoption for legacy databases. Move inline schema changes into tracked migrations. Close every connection when construction fails; the current `_run_migrations` finally block reopens a connection even after runner failure.

**Acceptance test.** Fingerprint and inspect journal mode before/after a refused open of a populated fixture. Content and schema must be unchanged. A populated database with missing or empty receipts must refuse ordinary opening and require explicit adoption.

### F03 — Restore's open-database probe is not a complete exclusion mechanism

**Priority:** P1. **Confidence:** high.  
**Location:** `engine/utils/db_backup.py:162–272`.

**Problem and trigger.** `_refuse_if_open` obtains a SQLite exclusive locking mode, commits, and closes its probe connection. `restore` then prepares a replacement and calls `os.replace`; there is no held exclusion covering that interval. Another process can open the target after the probe. In addition, the probe does not detect every idle connection: an idle open connection to a default DELETE-journal database was allowed in an isolated check. The existing test specifically covers an initialized WAL holder.

**Consequence.** Restore can replace a database while another process still owns the old file or a newly opened target. Deleting target WAL/SHM sidecars after replacement compounds that risk. The statement that any open connection is refused is stronger than the implementation guarantees.

**Suggested fix.** Make restore a maintenance operation with an application-level lock held throughout staging, replacement, cleanup, and final verification. Require every database opener to participate in the same locking protocol and quiesce nonparticipating tools. Do not rely on a one-time SQLite probe as proof that no process has an open file descriptor. Retain the staged fingerprint checks.

**Acceptance test.** Cover DELETE-journal and WAL targets, idle and active holders, and a second process opening after the probe but before replacement. Restore must refuse or the competing opener must remain blocked for the entire operation. No WAL belonging to a permitted active connection may be removed.

### F04 — Deduplication can collapse genuinely different reports

**Priority:** P1. **Confidence:** high.  
**Location:** `engine/search/dedup.py:35–116,150–180`.

**Problem and trigger.** After failing DOI/PMID matching, `_exact_match` falls back to normalized title without checking whether both records have different known identifiers. Fuzzy matching likewise returns the first title above 0.9 without identifier conflict checks. PubMed input records are appended without deduplicating within that source. Identifier indexes are not refreshed when merges add missing identifiers.

**Reproduction.** Two citations with the same title and distinct nonempty DOIs produce **one unique citation and one duplicate**. Two identical PubMed entries produce **two unique citations and zero duplicates**.

**Consequence.** Different reports can disappear before screening, while duplicates survive in other paths. PubMed-first merging also discards the secondary source's identity/raw record from the returned canonical citation.

**Suggested fix.** Use one identity/merge routine for both sources. Treat conflicting DOI or PMID values as an ambiguity requiring review rather than permission to merge by title. Use title/year/author similarity for candidate generation. Normalize identifiers consistently; refresh all indexes after a merge. Persist source-record membership and the reason for each merge, keeping raw records accessible.

**Acceptance test.** Include identical titles with different DOIs, corrections or companion publications, near-identical titles, within-source duplicates, and identifiers learned from a secondary record. Input ordering must not change the confirmed canonical identities.

### F05 — Rerunning search duplicates records without PMIDs

**Priority:** P1. **Confidence:** high.  
**Location:** `engine/core/database.py:146–160,456–495`; `scripts/run_pipeline.py:266–296`.

**Problem and trigger.** `papers` has a unique PMID; `add_papers` checks only PMID. There is no persisted DOI/canonical identity check for citations without a PMID. Deduplication occurs within the current search batch, not against existing database records.

**Reproduction.** Calling the actual `add_papers` method twice with the same DOI-only citation against a temporary base-schema database creates **two rows**. The fixture deliberately bypassed numbered migrations because optional SDKs were unavailable.

**Consequence.** Restarted or updated searches can duplicate OpenAlex-only papers, screening work, extraction work, and counts.

**Suggested fix.** Use F04's canonical identity service for ingestion against the existing corpus. After reviewing existing duplicates, enforce appropriate normalized identifier constraints and persist source records separately. Do not make title globally unique. Preserve deliberate multiple reports of one study rather than confusing study identity with report identity.

**Acceptance test.** Import the same batch twice: the second import adds no canonical reports. Test DOI normalization, missing PMIDs, duplicate records across runs, concurrent import, and metadata enrichment without losing provenance.

### F06 — PRISMA counts do not represent the complete identification history

**Priority:** P1. **Confidence:** high.  
**Location:** `engine/exporters/prisma.py:61–103,146–171`; `scripts/run_pipeline.py:266–297`.

**Problem.** Identification counts are computed from already-deduplicated `papers` rows. `duplicates_removed` is hardcoded to zero. `_stage_search` returns raw and duplicate totals but does not persist a search ledger consumed by PRISMA. Thus a PubMed/OpenAlex duplicate retained as PubMed disappears from OpenAlex's identification total. Exclusion reasons count screening decision rows rather than one resolved exclusion per report; two excluding passes can contribute two reason counts for one excluded report. Finally, `studies_included` is defined as papers reaching `audited_ai`, while eligible papers with extraction failures are outside that box.

**Consequence.** Reconciliation can balance the internal processing partition while still giving incorrect search-flow numbers. Audit completion is an engineering state; whether it defines scientific study inclusion must be an explicit review rule rather than an implicit exporter choice.

**Suggested fix.** Add an append-only search ledger containing database, query, retrieval time, source record identifier, raw count, retained/duplicate disposition, and canonical report mapping. Build identification and duplicate counts from that ledger. Produce one resolved abstract exclusion reason per report. Distinguish eligible reports/studies from extraction/audit completion, and explicitly document any study-level grouping.

**Acceptance test.** A fixture with 10 PubMed records, 8 OpenAlex records, and 3 confirmed cross-source duplicates reports 18 identified and 3 removed before screening. One report excluded by two passes contributes one exclusion. An eligible study with an extraction failure remains visible as eligible and failed, rather than silently disappearing from scientific inclusion.

### F07 — Generated methods can describe planned rather than performed work

**Priority:** P1. **Confidence:** high.  
**Location:** `engine/exporters/methods_section.py:27–183`.

**Problem and trigger.** Cloud wording is driven by `spec.cloud.enabled_arms` and says extraction “was additionally performed” even if no cloud call succeeded. Abstract screening models come from the current spec. Full-text models aggregate all historical decision rows, while local extraction/audit models come from the single requested run. Left-joined stage declarations can produce a model entry with zero papers, and the one-model branch hides the count. An export-only run therefore may have no local extraction history even though the evidence table contains results from earlier runs. The narrative's study count comes from `studies_included`, not actual extraction attempts or successful claims.

**Consequence.** A manuscript methods draft can claim an unexecuted cloud arm, omit the actual historical extractor, or pair current configuration with historical results.

**Suggested fix.** Generate methods from the provenance of the actual exported result set. Resolve contributing runs, successful calls, producing events, and screened/adjudicated report sets. Distinguish configured, attempted, successful, failed, and reused work. Use placeholders where history is absent. Record a report manifest tying every methods claim to its supporting run IDs and counts.

**Acceptance test.** Enable a cloud arm without running it: methods must not claim it performed extraction. Export previously produced results from an export-only run: report their actual producing models. A zero-call stage must not appear as completed work; failed attempts and reused results must be described accurately.

### F08 — Retrying an exhausted generator can turn a failed search into apparent success

**Priority:** P1. **Confidence:** high for the control-flow defect; medium for its incidence with the installed PyAlex version.  
**Location:** `engine/search/openalex.py:60–88` (`_paginate_with_retry`).

**Problem and trigger.** The wrapper calls `next` again on the same paginator after an exception. If that paginator is a Python generator and the exception escapes it, the generator closes. The next retry raises `StopIteration`, which the wrapper interprets as normal completion.

**Reproduction.** Executed the actual wrapper function extracted with AST against a generator that yields page 1 and then raises a transient page-2 error. The wrapper returns only **page 1 without propagating the failure**. This reproduction does not establish how PyAlex's unavailable installed paginator behaves.

**Consequence.** Under that iterator behavior, a partial literature retrieval is accepted as complete.

**Suggested fix.** Retry an explicit request for the same cursor/page rather than `next` on a failed iterator. Persist progress and advertised total counts where available; reject incomplete retrieval. Pin the actual client version and verify its retry behavior with fault injection.

**Acceptance test.** After success on page 1, fail page 2 once: page 2 must be fetched again and the entire search must complete, or the run must fail explicitly. Never return a successful partial result.

### F09 — Individually atomic exports do not form one coherent evidence package

**Priority:** P2. **Confidence:** high.  
**Locations:** `engine/exporters/__init__.py:19–70`; `engine/exporters/evidence_table.py:78–132,172–254`.

**Problem and trigger.** `export_all` writes PRISMA, CSV, workbook, DOCX, and methods sequentially without a shared read transaction. Evidence values and processing states also come from separate queries; the workbook recalculates its Field States sheet after building its Evidence Table. A writer can commit between reads. If a later exporter fails, earlier files have already replaced their predecessors. Predictable `.tmp` filenames add collisions between concurrent exports to the same destination.

**Consequence.** One delivered package or workbook can mix different database states; a failed export can leave a mixture of old and new files.

**Suggested fix.** Read all exports from one explicit SQLite snapshot, materialize a shared result grid once, and generate into a unique staging directory. Publish a versioned package with a manifest and atomic completion marker/pointer only after every artifact succeeds. Include run and arm identifiers, codebook hash, snapshot identity, and file hashes.

**Acceptance test.** Commit a concurrent correction between exporter queries: all outputs must consistently represent the chosen snapshot. Fail the last exporter: the previously published package remains intact. Concurrent exports must not share temporary files.

### F10 — Acquisition does not enforce its size limits on actual transfer/decompression

**Priority:** P2. **Confidence:** high.  
**Location:** `engine/acquisition/download.py:59–97,168–203` and the other download strategies.

**Problem and trigger.** Direct downloads load `resp.content` into memory and write directly to the final path. The PMC route sets `stream=True` but later reads all `.content`; its 100 MB ceiling only checks the `Content-Length` header. Missing or understated headers bypass that limit. The extracted PDF member is read fully without a member-size or decompressed-byte ceiling. The first PDF member is selected without confirming whether it is the primary article or a supplement.

**Consequence.** Large or malformed responses can exhaust resources, interrupts can leave partial final-path files, and a supplement can be mistaken for the main report. A `%PDF` prefix alone is not proof that a complete usable document was acquired.

**Suggested fix.** Stream to unique temporary files and enforce actual-byte caps, time budgets, and archive-member/decompression limits. Close responses and archives with context managers. Validate document readability before atomic publication. Select the primary report using explicit package metadata or route ambiguous multiple-PDF packages to review.

**Acceptance test.** Exercise a chunked response without Content-Length, an understated header, a small compressed archive with a large member, an interrupted transfer, and a package whose supplement precedes the article. Resource limits must hold and no partial final file may be accepted.

### F11 — Prefix truncation is not evidence that an eligibility feature is absent

**Priority:** P1. **Confidence:** high for the implementation; consequence depends on the document.  
**Location:** `engine/agents/ft_screener.py:67–105,365–390,478–515`.

**Problem and trigger.** The full-text screener and verifier receive the same character-bounded prefix after title/abstract insertion. The truncation function neither marks omitted text nor guarantees coverage of Methods, Results, appendices, or relevant late sections. Its docstring claims section prioritization, but implementation takes a prefix and optionally cuts at references. The model transport's input-fit checks cannot detect information discarded before the request was built.

**Consequence.** A relevant finding located beyond the cutoff can be treated as absent by both models. Agreement between models on the same incomplete text does not resolve the coverage problem.

**Suggested fix.** Return structured coverage metadata with the text: complete/truncated, original length, covered sections, and omitted ranges. Explicitly inform the model of omissions. For criteria requiring absence evidence, prohibit definitive exclusion from incomplete coverage; route to section retrieval, chunked review, or human adjudication. Reuse the parsed-text identity and record which sections supported the decision.

**Acceptance test.** Put the only qualifying evidence after the cutoff. The outcome must be escalation or a correctly retrieved decision, never an unqualified exclusion based on missing prefix evidence. Test long abstracts and nonstandard section headings as well.

### F12 — The screening stage ignores `limit`

**Priority:** P2. **Confidence:** high.  
**Location:** `scripts/run_pipeline.py:300–318`.

**Problem and trigger.** `_stage_screen` detects more INGESTED papers than the requested limit, logs that screening is limited, then executes `pass` and calls `run_screening` on all INGESTED papers. The search-stage slice does not bound a preexisting database or `--skip-to screen` run.

**Consequence.** A small smoke run can unexpectedly screen the entire pending corpus and consume substantial model time.

**Suggested fix.** Select a deterministic bounded paper-ID set, register the intended selection where appropriate, and pass it into the screening runner. Avoid temporary status changes to represent out-of-scope papers. Clarify the separate semantics of `limit` and extraction's `max_papers`.

**Acceptance test.** Start with 100 INGESTED papers and limit 2 while skipping to screening. Exactly two papers receive model calls and decisions; the other 98 remain untouched.

### F13 — Screening persistence is incomplete across retries and restarts

**Priority:** P2. **Confidence:** high.  
**Locations:** `engine/agents/screener.py:140–153,192–231,263–319`; `engine/core/database.py:567–600`.

**Problem and trigger.** Pass 1, pass 2, and the resolved status are separately committed. An exception or termination after pass 1 leaves a partial attempt; rerun adds another pass 1 rather than distinguishing or resuming the attempt. Checkpoints are non-atomic JSON files containing only paper IDs. Verification deletes its checkpoint after completion and selects all included papers on the next invocation, without filtering previously verified papers by persisted input/configuration identity.

**Consequence.** Decision history can contain indistinguishable partial/repeated passes, model work repeats, and naive reason aggregations overcount. Configuration changes are not represented in the checkpoint identity.

**Suggested fix.** Persist explicit screening attempts with run/configuration/input identity and completion state. Preserve each raw model call, but resolve status only from one identified complete attempt. Resume from that database record rather than a JSON authority. If JSON remains a convenience cache, write it atomically and validate its identity.

**Acceptance test.** Interrupt after each pass and before status update, then restart. A complete, identifiable attempt governs status; no duplicate unlabelled passes appear. Reinvoking verification with unchanged input/configuration makes no unnecessary calls, while a declared changed configuration creates a new attempt.

### F14 — Watchdog timeout does not stop the underlying Ollama call

**Priority:** P2. **Confidence:** high.  
**Location:** `engine/utils/ollama_client.py:575–646,721–735`.

**Problem and trigger.** After `future.result` times out, `shutdown(wait=False, cancel_futures=True)` does not cancel a task already running in a thread. The code creates another executor for the retry. The abandoned call can remain active; repeated timeouts can accumulate active work and threads. Thread-pool worker shutdown can also delay process exit until blocked work finishes.

**Consequence.** A timeout intended to restore control may create overlapping requests, GPU contention, and unpredictable shutdown. Existing experiment locks and restart guards are valuable but do not cancel already dispatched requests.

**Suggested fix.** Use a cancellable transport with explicit deadlines and client closure, or a carefully isolated worker process when reliable local termination is required. If cancelling the client does not cancel server-side generation, separately track outstanding work and bound retries. Do not start another attempt merely because the caller stopped waiting.

**Acceptance test.** Mock a client call that blocks beyond the deadline. Verify bounded worker count, no unintended simultaneous retries, timely CLI exit, and telemetry accounting for abandoned attempts. Test with service restart disabled as well.

### F15 — Text exported to spreadsheets can be interpreted as a formula

**Priority:** P2. **Confidence:** high.  
**Location:** `engine/exporters/evidence_table.py:150–161,193–213,233–241` and other workbook builders accepting external strings.

**Problem and trigger.** Titles, snippets, rationales, and model-produced values are passed directly to CSV writers and `openpyxl` cells. A string beginning with `=` becomes an Excel formula when appended to a cell. In an isolated library check, appending `=1+1` produced cell `data_type = 'f'`. CSV quoting does not itself force spreadsheet applications to interpret content as text.

**Consequence.** Externally supplied content can execute spreadsheet formulas or be transformed on opening. This is relevant to both untrusted source text and model-generated strings; it does not establish an exploit in the present corpus.

**Suggested fix.** Centralize text-cell handling: explicitly serialize untrusted workbook strings as text. For spreadsheet-oriented CSV exports, neutralize formula-leading prefixes and preserve exact raw values in an explicitly non-spreadsheet representation or accompanying provenance record. Keep numeric data numeric.

**Acceptance test.** Export strings beginning with formula markers, whitespace/control prefixes, and ordinary minus-signed numeric values. Untrusted workbook cells must remain text. Confirm spreadsheet-safe CSV behavior without corrupting intended numbers or raw provenance.

### F16 — The integrity fingerprint does not encode text boundaries unambiguously

**Priority:** P2. **Confidence:** high.  
**Location:** `engine/tools/db_fingerprint.py:40–57,89–106,262–280`.

**Problem.** Cells use a type prefix and are concatenated with U+001F; rows use U+001E. Text containing these separators is not escaped or length-prefixed. Therefore different cell boundaries can serialize to identical bytes before hashing.

**Reproduction.** Two otherwise identical SQLite databases with the same two TEXT columns and one row:

```python
# Database A
('a\x1fT:b', 'c')
# Database B
('a', 'b\x1fT:c')
```

Both produce the **same overall fingerprint**, despite different stored text. This is a serialization collision, not a cryptographic SHA-256 collision. Equal row counts and equal schemas do not detect it.

**Consequence.** The standing integrity check and backup verification can miss some changes to text. The exact control-character trigger may be uncommon, but arbitrary document/model text should not invalidate an integrity guarantee.

**Suggested fix.** Introduce a versioned canonical format using typed, length-prefixed cells and explicit row boundaries, or a canonical structured encoding. Store the fingerprint scheme version with every record. Keep the old scheme available for historical comparison; do not silently reinterpret existing baselines.

**Acceptance test.** The two rows above must hash differently. Cover NULL, integers, reals, blobs, Unicode, separators, empty strings, and multirow boundary ambiguity. Backups must still compare equal under the new scheme.

### F17 — Installation and test schedules need explicit reproducibility boundaries

**Priority:** P2. **Confidence:** high.  
**Locations:** `requirements.txt`; `pyproject.toml`; `scripts/nightly_tests.sh:5–27`; `engine/cloud/__init__.py:1–5`.

**Problem.** Several important packages are pinned, but most are unbounded. The requirements comment identifies a validated Docling/Pydantic combination while Pydantic and Docling's transitive dependency versions are not locked. Direct imports of Requests, HTTPX, and PyMuPDF are not declared explicitly. `pytest` is not declared as a development dependency. `pyproject.toml` contains pytest settings but no package/dependency/Python-version metadata. The nightly script runs every test without marker selection, mixing offline checks with network/model/heavy integration tests. Importing the cloud schema eagerly imports both provider SDKs, coupling database schema initialization to optional execution backends.

**Consequence.** A fresh machine may differ from the validated environment; an optional provider can block otherwise local setup; nightly results depend on live services and installed models. Service/live-data fences reduce harm, but do not make the schedule deterministic.

**Suggested fix.** Declare direct runtime and development dependencies, supported Python version, and a reproducible lock or validated constraints file. Make provider imports lazy or isolate provider extras from schema definitions. Default automated checks to offline tests; schedule network, Ollama, and heavy integrations separately with explicit prerequisites and the existing fences enabled. Add a fresh-environment smoke check and a minimal second-review fixture.

**Acceptance test.** Install from the declared environment on a clean machine and run offline tests without a model server or cloud credentials. Local database creation must not require importing an unused provider. Run each integration group separately and report its environment/model versions.

## Suggested repair sequence

### 1. Establish durable state and trustworthy identity

Address **F01, F02, and F03** first using temporary database copies and failure injection. Then implement the shared canonical identity service for **F04/F05**. Inventory existing duplicate candidates before enforcing new uniqueness constraints; never delete or merge historical rows solely on fuzzy similarity.

### 2. Repair scientific reporting and screening coverage

Build the search ledger and resolve PRISMA semantics (**F06**), then drive methods from actual producing history (**F07**). Correct pagination retry behavior (**F08**) and protect absence-based full-text decisions against incomplete coverage (**F11**).

Dependencies: PRISMA identity totals depend on the canonical record/source mapping; methods accuracy depends on reliable attempt/run/result linkage. Preserve retrospective limitations as explicit “not recorded” facts rather than reconstructing exact historical search counts from incomplete data.

### 3. Make repeated and concurrent operations predictable

Introduce snapshot-based export packages (**F09**), bounded acquisition (**F10**), bounded screening (**F12**), and database-backed screening attempts (**F13**). Replace abandoned-thread retries (**F14**). Add spreadsheet text handling (**F15**) and versioned fingerprints (**F16**). Finish the environment/test split (**F17**).

### Completion criteria

- A committed parsed-text reference always resolves to its recorded bytes after a crash/restart.
- A refused database open does not change content or schema; restore has exclusive maintenance ownership throughout.
- Repeated imports add no duplicate canonical reports and preserve all source records.
- PRISMA identification and methods claims derive from persisted facts, with unresolved historical gaps visibly marked.
- Truncated screening input cannot support an unqualified absence-based exclusion.
- Export packages represent one declared snapshot and become visible only when complete.
- All listed regressions pass in the declared environment; selected integration checks pass on a nonproduction review before rollout.

## Reproduction record

| Check | Observed result | Interpretation |
|---|---|---|
| Python AST parse | 375 files parsed; zero syntax errors | Syntax only; does not establish runtime correctness. |
| Same title, different DOIs | 2 inputs → 1 unique citation | Conflicting identifiers do not prevent title-based merging. |
| Duplicate PubMed input | 2 inputs → 2 unique citations | PubMed seeding does not deduplicate that source. |
| DOI-only repeated ingestion | 2 calls → 2 paper rows | Persistent ingestion is not idempotent without PMID. |
| Pending-migration constructor refusal | `PendingMigrations`; absent `review_runs` recreated | Refusal occurs after schema mutation. |
| Idle open DELETE-journal target | Restore probe allowed | Probe does not detect every open connection. |
| Different text cell boundaries | Identical overall fingerprints | Canonical text serialization is ambiguous. |
| Failed page-2 generator | Page 1 returned; no failure propagated | Same-generator retry can mask incomplete retrieval. |
| Spreadsheet string `=1+1` | Cell type `f` | Unsanitized string assignment creates a formula. |

All database reproductions used temporary synthetic data. No finding here should be read as proof that a particular live result has already been corrupted. Existing research outputs should be checked against these specific triggers, especially search reruns, title-based merges, parsing interruptions, and export-only methods generation.
