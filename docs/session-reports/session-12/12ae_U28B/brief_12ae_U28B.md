CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R794, PI ruling 2026-11-09 UTC; not on disk)
================================================================================

R794 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r794.txt: sha256 2f7c7e5594a2a72874fa2250135a962cb358747e399ef50f197f16c1c03bd598,
            15953 bytes, 173 lines, one trailing newline.

===== BEGIN r794.txt =====
RULING [evidence-engine 12ae-R794] — 12ae-U28A accepted; the committed brief equal to the
  architect's copy; U28's design: the unit splits — U28 lands the cloud gate's flags (D), the
  no-values summary (B, at the summary, the per-field contract kept) and the cloud write boundary's
  citation guard (C); the event-store write for cloud claims (A1) is its own unit, U40, with its
  own Phase A; A2 and A3 refused; N44 read against the recorded contract; docs lines authored;
  notes on B9 and C28; readings; the build opens as one brief
STATUS: PI ruling, 2026-11-09 UTC, session 12ae, on architect recommendation, on the 12ae-U28A
  final report (item 0 c4e13f6; read-out 679133a, docs/session-reports/session-12/12ae_U28A/
  12ae_U28A.md, sha256 dfbd5ec73cdd28211b94849a8ec1c65ef25b4f585b8a19cb6eb98c5df8655aee, 253
  lines). Transcribe at item 0 of the 12ae-U28B brief below, by transcribe_ruling.py, in the block
  after R793's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the read-out and the report):
  MEASURED: "12ae_U28A/brief_12ae_U28A.md is cced5fcbe477a348714c3f119cb35f0c8d5c7a818b73053706cd378281af21da,
    31,239 B, 393 lines" (the architect's copy: equal, cmp byte for byte); "stored:
    {'cloud_extractions': 12, 'cloud_evidence_spans': 240, 'field_events': 0}";
    "base.run_distribution_check({'extracted': 12, 'failed': 0}): returned: skipped=False ok=11
    collapsed=0 study_type=[('OK', 0)]"; "the same twelve values as claim events on a registered
    model arm … RAISED DistributionCollapseError"; "arm_population: 0"; "check_distribution: 11
    fields; statuses ['OK']; total_non_null all 0: True"; "store_result: INSERTED
    (cloud_extractions id 13); read back surgical_domain ('Colorectal', '')"; "('openai_o4_mini_2025_04_16_high',
    'model', 'pinned', 2)"; "SDK client constructions (openai.OpenAI / anthropic.Anthropic): 0";
    "Plan +102 / −2; the "−" lines are C56's row and the header line".
  READ (the read-out, §1–§9): "_cell_events is f"SELECT … FROM field_events WHERE paper_id = ? AND
    field_name = ? AND arm = ? …" (effective.py:204–206)"; "The store writes "INSERT INTO
    cloud_extractions … (base.py:364) and "INSERT INTO cloud_evidence_spans … (base.py:383), and
    nothing else"; "Every other argument takes its default: strict=False, raise_on_collapse=True,
    skip_on_failures=True, min_population="run""; the local call "strict=False,
    raise_on_collapse=False, skip_on_failures=False, min_population="arm"" (run_pipeline.py:425–428);
    the skip rules at :418, :427–434, :436, :445, :453; "tests/test_distribution_monitor.py:313
    test_an_arm_with_no_values_is_all_zero_and_OK … "The zeros are still the contract — for a
    REGISTERED arm that holds nothing""; ":293 test_all_nr_excluded"; "engine/agents/extractor.py:684–687
    calls enforce_citations(span_dicts, paper_id=paper_id, arm=cfg1.model, mode=LEGACY, …)";
    "check_citations … :208–209 if not has_evidence: offenders.append((name,
    VALUE_WITHOUT_CITATION)), with sentinels exempt in LEGACY (:202–206)"; "a None value with a
    snippet → BLANK_VALUE under check_citations (citation_guard.py:171–174)"; test_empty_string_normalized_to_null's
    docstring "Empty string '' gets normalized to null with 'empty_string_to_null' annotation";
    "test_null_converted_to_nr asserts nr_span["source_snippet"] == """; "the transports' store
    handlers (10–11) catch UncitedValueError as a paper outcome. It is not a tuple member";
    "write_extraction_events' exact form for a cloud write … was not read line by line"; "what
    concordance, the exports, the judge loader and the audit do with a cloud arm's claims was not
    traced"; F1–F7; finding 1 ("C49 at plan :280 … keeps its leading "LATENT — owner session 12
    (R278)" and appends"); CLAUDE.md:119, :382, :392 and pipeline.md:248–249, :305, :307 as quoted.
  INFERRED: none.

R794(a) — The 12ae-U28A final report is accepted as printed: item 0 c4e13f6 (Block 191 by
  transcribe_ruling.py; C56 CLOSED by an appended note in C49's actual form — CC read the record
  over the brief's description, R732(a), and the architect's description was wrong; the dated units
  line; the section-L header cell; brief_12ae_U28A.md), the read-out 679133a with census_u28a.py/.txt
  and fake_probe_u28a.py/.txt (no SDK client constructed, the journal empty), ledgers 0c98069 and
  7e2e0d2 with trailers. No stop. The committed brief file is byte-equal to the architect's copy,
  the tenth match of ten. I2 and I3 held: for a registered cloud arm the gate reads field_events
  and store_result writes only the two legacy tables; E1–E4 held.
R794(b) — The unit splits (F1, F2). The entry's claim (1) — the gate sees what the cloud path wrote
  — is not a two-file change: its fix is the cloud path writing claim events (A1, below), and the
  read-out could not trace what the grid's consumers then do with a cloud arm's claims, nor the
  event writer's form for a write with a run_id and no activation. Building that blind is the
  thing this programme does not do. So U28, as built here, is claims (2) and (3) and U07 F1 (the
  fourth claim riding on B9's R620(a) note), in the entry's two files, and A1 is its own unit:
  U40 — "The cloud path writes claim events" (rows B9 and C28 for Wave 2 row 25; files
  engine/cloud/base.py and whatever U40's Phase A names; depends on U28; pin effect not
  affecting), two-phase, its Phase A tracing the writer's form and the grid's consumers
  (concordance, the exports, the judge loader, the audit; is_assigned for a model-kind arm, F6)
  before any design. Order: U28, U40, U29 — the cloud work stays together and U29's send/record_call
  ground is untouched by both. P1's fix units 38 (U01–U39, U40), done 30. The entry's named test
  test_cloud_gate_sees_what_store_result_wrote is U40's; U28's tests are named in (f).
R794(c) — Refused: A2 (the reader given a route to the cloud tables) and A3 (a private route for
  the gate) — both against R12 and R30 and against migration 016's "nothing is imported from
  them"; the direct-table paths were removed on purpose and do not come back for one caller. A1
  is the design of record for claim (1), to be built as U40 after its Phase A; the two legacy
  tables stay as the cloud artefact store (cost, tokens, reasoning trace, raw payload) until a
  later housekeeping decides otherwise under 016's note — not U28's or U40's to retire.
R794(d) — D, the cloud gate's flags (U07 F1, R620(a)): run_distribution_check passes
  min_population="arm" and skip_on_failures=False, keeping raise_on_collapse=True (the cloud
  contract: a collapse stops the arm's run) and strict=False. The cloud gate then skips by the
  same population rule as the local stage — fewer than 10 eligible papers holding a live claim
  for the arm — and a failed paper is recorded, not a veto. Until U40 lands, a cloud arm holds no
  claims, so the gate reports skipped with the population reason instead of eleven OK rows over a
  store it never read: the honest telemetry, and the first half of N44's symptom on the cloud
  path gone by construction.
R794(e) — B, N44 against the recorded contract (F3): the per-field contract stands —
  check_distribution returns OK with total_non_null 0 for a registered arm that holds nothing
  (test_an_arm_with_no_values_is_all_zero_and_OK, the B5 rewrite under A12, is not rewritten;
  test_all_nr_excluded is not touched). N44 is read at the summary: run_post_extraction_check,
  after check_distribution, when every result has total_non_null 0, reports the arm skipped with
  the reason "arm holds no non-null value for any categorical field", records the results in the
  summary as today, and does not call assert_no_collapse (there is nothing to assert; a
  "collapse" cannot be read from zero values); the CLI (main) prints that same sentence after the
  report when it holds, exit code unchanged (the CLI is a report, not a gate). A summary that says
  skipped=False, ok=11 over an arm with no values is the lie N44 names; a field that says OK with
  zero values is the contract A12 set. Both hold.
R794(f) — C, the cloud write boundary: store_result calls enforce_citations in LEGACY mode before
  its INSERTs, with the arguments the local legacy path passes at extractor.py:684–687 adapted to
  the cloud arm (arm=self.arm_name; the codebook's absence sentinels and tokens from
  self._codebook as the local call takes them from its spec/codebook — the build brief quotes
  both call forms and matches them; a parameter the cloud path cannot supply is a STOP), so a
  non-sentinel value without a snippet and an empty value are refused (UncitedValueError;
  VALUE_WITHOUT_CITATION / BLANK_VALUE) and nothing is inserted; the sentinel keeps its "" (R132,
  exempt in LEGACY). The "" fill at base.py:282–283 stays as a shape repair feeding the refusal.
  UncitedValueError is not a tuple member, so handlers 10–11 count the paper failed (U27 stands).
  Fail-first (R790(e)'s form): tests/test_u28_cloud_gate_and_boundary.py, in fake_probe_u28a.py's
  seams (a scratch ReviewDatabase, the live codebook copied beside it, the arm registered by
  open_run(… cloud_arms=[arm] …) with git_state patched clean, a CloudExtractorBase with no SDK
  client), three tests: (i) test_a_cloud_run_with_no_claims_is_reported_skipped_not_ok — twelve
  store_result writes with one identical categorical value, then run_distribution_check({'extracted':
  12, 'failed': 0}) and ({'extracted': 12, 'failed': 1}) → skipped True with the population reason
  in both, not eleven OK (red on the parent: skipped=False ok=11; the failed-1 case red with
  "partial run"); (ii) test_an_arm_holding_no_non_null_values_is_reported_skipped —
  run_post_extraction_check on the registered empty arm with min_population="run",
  extracted_count=12, raise_on_collapse=True → skipped True with (e)'s reason and no raise (red
  on the parent: skipped=False), beside a preservation assertion that check_distribution itself
  still returns OK with zeros (green on both); (iii) test_store_result_refuses_an_uncited_or_empty_value
  — a non-sentinel value with "" → UncitedValueError and no cloud_extractions row; a "" value with
  a snippet → the same; the sentinel with "" → inserted (red, red, green on the parent). Recorded
  in b0_parent_acceptance.txt; green in the gate. Callers: tests/test_cloud_extraction.py's
  test_empty_string_normalized_to_null is superseded by (f) — its subject (an empty value stored
  as NULL with an annotation) contradicts R132 and the local boundary — and is rewritten as
  test_an_empty_value_is_refused_at_the_write_boundary on its own drive (the mocked transport,
  the "" study_type), asserting the paper counted failed and no cloud_extractions row for it;
  test_null_converted_to_nr stands (the sentinel path). The one-word docstring fix of R793(d)
  rides this commit. store_result's empty-string normalization branch, if C leaves it unreachable,
  is quoted in the report and left in place (a housekeeping item, not U28's edit).
R794(g) — Docs made false by (d)–(f), authored here, applied in the engine commit verbatim, each
  line located by its text (a line not found as quoted is a stop): CLAUDE.md "Cloud: skipped
  under 10 extracted papers or on a partial run; COLLAPSED raises" becomes "Cloud: skipped by the
  same population rule (U28; a failed paper is recorded, not a veto), or when the arm holds no
  non-null value for any categorical field; COLLAPSED raises"; CLAUDE.md "(`CloudExtractorBase.run_distribution_check`,
  which raises on COLLAPSED; skipped when that run extracted fewer than 10 papers or any failed)"
  becomes "(`CloudExtractorBase.run_distribution_check`, which raises on COLLAPSED; skipped by the
  local stage's population rule since U28, or when the arm holds no non-null value for any
  categorical field)"; CLAUDE.md "`store_result()` rejects 0-span results with ValueError —
  prevents silent data loss." becomes "`store_result()` rejects 0-span results with ValueError
  and, since U28, an uncited non-sentinel value or an empty value with `UncitedValueError`
  (`enforce_citations`, LEGACY mode, as the local write boundary) — prevents silent data loss.";
  docs/architecture/pipeline.md "6. `store_result()` — atomic insert to cloud_extractions +
  cloud_evidence_spans. Rejects 0-span results" becomes "6. `store_result()` — atomic insert to
  cloud_extractions + cloud_evidence_spans. Rejects 0-span results and, through `enforce_citations`
  (LEGACY), uncited or empty values"; pipeline.md "`min_population="run"` (cloud default) when the
  run extracted fewer than 10 papers, `"arm"` (local) when fewer than 10 eligible papers hold a
  live claim for the arm; with `skip_on_failures=True` (cloud default) also when any paper in the
  run failed" becomes "`min_population="run"` when the run extracted fewer than 10 papers, `"arm"`
  (both callers since U28) when fewer than 10 eligible papers hold a live claim for the arm; with
  `skip_on_failures=True` (no caller since U28) also when any paper in the run failed; and, after
  the check, when the arm holds no non-null value for any categorical field (U28)". Any other
  CLAUDE.md or docs/architecture line the build finds false is quoted in the report and left
  unedited (the next ruling's). docs/inventory: --write, a stamp-only diff reverted (R793(e)), a
  recorded-entry change regenerated under R786(b).
R794(h) — Rows and notes, at 12ae-U28B's item 0: on B9, a dated note — U28 lands D and B (R794(d),
  (e)); Wave 2 row 25 (the gate's store) is U40's (R794(b)); U07 F1's "closes in U28" is read as
  D; the two CLAUDE.md lines of F4 are rewritten in U28's engine commit under (g). On C28, a dated
  note — Wave 2 row 27 is U28's C (R794(f)); Wave 2 row 25 is U40's; F5: scripts/backfill_cloud_spans.py:76
  and scripts/reparse_cloud_spans.py:75 also INSERT into cloud_evidence_spans, which migration 016
  calls read-only — recorded, not U28's; the max_papers incidental keeps its owner (R598(b)). The
  dated units line: U40 entered (R794(b)); fix units 38, done 30. No row opened. Neither row
  closes at U28: both wait for U40.
R794(i) — Readings, no row: finding 1 (the architect described C49's CLOSED form from the
  R785(f)/R787(b) wording, not from the cell; the record's form was followed — a form is quoted
  from the record before it is named in a brief); F4 ((g) edits the two lines); F5 ((h)); F6 (a
  cloud arm registers as kind model, as the local arms do; under A1 its unclaimed cells read
  missing — the local semantics, not a defect; U40's Phase A states it); finding 7 (an item-0
  check derived from U27A's by a script that crashed on a non-ASCII character left the earlier
  probe assertions in place, reported FAIL, and the chain committed nothing — the chain did its
  job; the instrument is written for its brief, not derived by transformation); E1–E4 held. §9's
  limits stand and are U40's Phase A's questions.
R794(j) — The build opens as one brief, 12ae-U28B (R785(h)'s form): the read-out quoted every
  site, every coupled test and the probe's seams at 679133a, the engine tree is unchanged since
  077cb0f, and the change is two keyword arguments at one call, one summary rule and one CLI line
  in the monitor, one guard call in store_result, five docs lines, one test rewritten, one
  docstring word, and one new test file. The brief re-reads the sites, the two enforce_citations
  call forms and the coupled tests at HEAD before the first edit and stops on any difference from
  the read-out. No model call, no provider call, no API key read, no live write; the standard gate
  by the wrapper against 837–1021 s. This ruling states no standing rule: the id list, the glosses
  file and STANDING_RULES.md are untouched at item 0, asserted by content.
===== END r794.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ae-U28B] — Land U28 as R794(d), (e), (f) and (g) rule it: the cloud
  gate's flags (D), the no-values summary and CLI line (B), the cloud write boundary's
  citation guard (C), the five docs lines, one test rewritten, the R793(d) docstring word,
  the fail-first acceptance tests; one engine commit with its gate, evidence in its own
  commit. Item 0: R794 as the next block; the B9 and C28 notes; the dated units line (U40
  entered); this brief committed as pasted; no standing rule.
PURPOSE: Make the cloud arm's distribution gate skip honestly (by the arm's live-claim
  population, a failed paper recorded, not a veto) instead of reporting OK over a store it
  never read; make the monitor's summary and CLI say when an arm holds no values; refuse an
  uncited or empty value at the cloud write boundary as the local boundary does. Claim (1)
  — the cloud path writing claim events — is U40's, not this unit's. Behind R71 on live.
  Code task. No live write. No model call. No provider call. No API key read.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  Touched: engine/cloud/base.py (run_distribution_check's two keyword arguments; the
  enforce_citations call in store_result and its import); engine/validators/
  distribution_monitor.py (the no-values summary rule in run_post_extraction_check; the
  CLI line in main); CLAUDE.md (three lines, R794(g)); docs/architecture/pipeline.md (two
  lines, R794(g)); tests/test_cloud_extraction.py (one test rewritten; the R793(d)
  docstring word); tests/test_u28_cloud_gate_and_boundary.py (new); docs/inventory/
  entry_points.{json,md} only if a recorded entry changes (R794(g)); docs/plan/
  ENGINE_REFACTOR_PLAN.md (Block 192; the B9 and C28 notes; the dated units line);
  docs/session-reports/session-12/12ae_open/r794.txt; a new directory docs/session-reports/
  session-12/12ae_U28B/ (brief_12ae_U28B.md at item 0; b0_parent_acceptance.txt,
  gate_u28b/ and the pin recompute at step 3); ~/scratch/12ae-u28b/; the claude-config
  ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): any other file under engine/ (effective.py,
  the reader, the event writer, the transports included), tests/, scripts/ (run_pipeline.py's
  local call included), analysis/; review_specs/ (R71: enabled_arms stays empty on disk; the
  test's spec copy lives under its temp directory); the codebook; engine/core/
  extraction_events.py; check_distribution's per-field contract (R794(e): the zero-value
  branch at distribution_monitor.py:215–227 is not edited); test_an_arm_with_no_values_is_all_zero_and_OK
  and test_all_nr_excluded (not one byte); the tools; the id list, the glosses file and
  STANDING_RULES.md (not one byte — R794 states no rule); review.db (mode=ro instruments
  only); any .env or API key (no SDK client is constructed outside the existing mocked
  tests; a test that would construct a real client is a STOP); the working tree inside
  07:00–10:35 UTC (R86); any scratch directory other than ~/scratch/12ae-u28b/.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ae-U28A report; R794): HEAD 679133ab40982904717c0817de937add74d1ee73, parent
    c4e13f61f758552761d006dd4e7dadb2eb93ebcb, clean, level; the engine tree equal to
    077cb0f's; plan ends "…ends at R793.", last block 191; STANDING_RULES.md 87 / 8 / 95,
    glosses 43, the id list's last A line "A R784(g)"; claude-config at 7e2e0d2 or an
    accepted descendant (R663(b)); gate reference U27's (12ae_U27B/gate_u27b/: 3,537
    passed / 1 xfailed / 17 deselected on 3,555 / 3,538; its collected_ids.txt and
    durations_12ae_u27b.txt — the durations file's name read from the directory, quoted);
    band 837–1021 s (R783(b)); pins 12ae_U27B/pin_tuples_u27b.json. Architect's hash of
    r794.txt: Section 1.
  READ (12ae_U28A/12ae_U28A.md at 679133a: §2's sites, §3's FAKE summaries, §4's coupled
    tests, §5's seams, §7's options — not repeated; step 2a re-reads each at HEAD).
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 077cb0f. I2. The sites read at
    HEAD as the read-out quotes them: base.py:501–517 (the call and its defaults), :279–283
    (the "" fill), :291–298 (the null branch), :336–358 (store_result's checks), :362–403
    (the INSERTs); distribution_monitor.py:418–453 (the skip rules), :215–227 (the
    zero-value branch), :498–499 (assert_no_collapse), :536–543 (main's calls);
    extractor.py:684–687 (the local enforce_citations call) and citation_guard.py:217–
    (enforce_citations' signature: paper_id, arm, escape_token, absence_sentinels, mode,
    citation_counts, contract_unmet_token, attempt); tests/test_cloud_extraction.py:588–630
    (test_empty_string_normalized_to_null) and the TestStoreResultCrashProtection docstring
    at :800 as 077cb0f left it. I3. The three tokens the local call passes (escape,
    sentinels, unmet) are obtainable on the cloud path from self._codebook (or from the
    same source the local path reads them from, quoted) without a spec or run the
    CloudExtractorBase does not have — if one is not, STOP with the finding (R794(f)).
  INFERRED — expectation: E1. On the parent, the new file's red cases are exactly: (i)'s two
    (extracted 12 / failed 0 → skipped=False ok=11; failed 1 → skipped with "partial run",
    not the population reason), (ii)'s skipped assertion, (iii)'s two refusals; its green
    cases: (ii)'s per-field preservation and (iii)'s sentinel insertion (N_red and N_green
    read from the run, not assumed). E2. The gate collects 3,555 + N and selects 3,538 + N,
    only-gate exactly the new ids (the rewritten test keeps its module and changes its
    name, so one id leaves and one arrives: only-ref 1, only-gate N + 1 — read and matched);
    no other test moves (§4: test_distribution_monitor.py's 56, test_distribution_check_wiring.py's
    3 and test_write_boundary_guard.py's 4 green by code order; test_null_converted_to_nr
    green). E3. inventory --write is a stamp-only diff, reverted; no entry moves.

CONTRACT (steps, in order; evidence under ~/scratch/12ae-u28b/):

  STEP 0 — Pre-check (read-only). date -u (outside 07:00–10:35 UTC; the gate must exit
    before 06:30 UTC or launch after 10:35). HEAD and parent proved by git cat-file -e
    <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. Plan: last line "…ends at
    R793.", last block 191, no Block 192. Rules files: the three counts above.
    claude-config: 7e2e0d2 or an accepted descendant, clean, level. I1. Fingerprint
    --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12ae-U27A's item 0
    — a block, notes, a dated line, the brief file — ~/scratch/12ae-u27a/check_item0_u27a.py
    and its chain, quoted before composing).
    1a. 12ae_open/r794.txt from Section 1; hash straight after the write: EXPECTED
        2f7c7e5594a2a72874fa2250135a962cb358747e399ef50f197f16c1c03bd598, 15953, 173.
        Mismatch = STOP.
    1b. block192.json in block191.json's key set: tag R794; item "item 0 of the
        12ae-U28B brief"; beside = "the B9 and C28 notes, the dated units line and the
        brief file it orders" (composed; quoted); made_date 2026-11-09; session 12ae;
        report = the 12ae-U28A final report (item 0 c4e13f6; read-out 679133a),
        final-report form; sentence model, followed "12ae-U28B brief"; prev_ends R793.
        Render to stdout, compare with Block 191's order, then --append ("−" EXPECTED 0 for
        the append, read at 1e).
    1c. Notes (R794(h)), each appended at the end of its row's status cell in that row's
        note form, quoted before composing: (i) B9, "*Note 2026-11-09 (12ae-U28B item 0,
        R794(d), (e), (b)):*" — U28 lands D (the cloud gate takes min_population="arm",
        skip_on_failures=False, raise_on_collapse kept) and B (the no-values summary); Wave
        2 row 25 (the gate's store) is U40's, the cloud path writing claim events, two-phase,
        after U28; U07 F1's "closes in U28" is read as D; CLAUDE.md:119 and :392 rewritten
        in U28's engine commit (R794(g)). (ii) C28, "*Note 2026-11-09 (12ae-U28B item 0,
        R794(f), (b), (h)):*" — Wave 2 row 27 is U28's C (enforce_citations, LEGACY, in
        store_result); Wave 2 row 25 is U40's; scripts/backfill_cloud_spans.py:76 and
        scripts/reparse_cloud_spans.py:75 also INSERT into cloud_evidence_spans (F5),
        recorded, not U28's; the max_papers incidental keeps its owner (R598(b)). The dated
        units line after 12ae-U28A's, in its form: U40 entered by R794(b) ("The cloud path
        writes claim events"; B9, C28; after U28, before U29); fix units 38, done 30
        (U01–U27, U37, U38, U39); rows 284 (94 / 56 / 134, 0 unclassed) unchanged; rows out
        0. Two rows replaced, one line inserted; the "−" count read and matched to two.
    1d. The brief file (R784(g)(v)): 12ae_U28B/brief_12ae_U28B.md from this whole pasted
        block, "CLAUDE CODE" through the last HANDBACK line, one trailing newline, written
        once; sha256, bytes, lines straight after the write, reported.
    1e. The item-0 check in 12ae-U28A's content form, written for this brief (R794(i)):
        --check-plan byte-identical; the plan's numstat read ("−" matched to the two rows);
        every earlier "ends at" line kept; last line "…ends at R794." once; the plan equals
        HEAD's plus the two rows replaced, the line inserted and Block 192 appended, nothing
        else (computed); each row's last note begins with its 1c tag; r794.txt equal to 1a;
        the three rules files unchanged and unstaged; the brief file hashed; the staged set
        exactly the plan, r794.txt and the brief file. Evidence by redirect; quote the
        check.
    1f. <check> && <commit>, one line; message from the staged set (R778(d)), docs scope.
        Fingerprint --compare before the push; push; level; --compare after (IDENTICAL).
        Ledger row with trailers (foreign pending rows named if any); push; level. Record
        hashes and parents.

  STEP 2 — The build (one engine commit).
    2a. Re-read at HEAD and quote: every I2 site; the summary dict's keys as
        run_post_extraction_check builds them (:398–453, the "skipped" / "skip_reason" form
        of the existing skips, to be matched); main's report and exit (:536–543, and how
        it reports a skip today, if it does); the local enforce_citations call (:684–687)
        and where escape, sentinels and unmet are read from (the lines, quoted — I3) against
        what CloudExtractorBase holds (self._codebook's attributes, quoted); check_citations'
        LEGACY exemptions (:202–206) and the BLANK_VALUE branch (:171–174); the five docs
        lines of R794(g) found by their text; test_empty_string_normalized_to_null whole;
        the TestStoreResultCrashProtection docstring; fake_probe_u28a.py's seams (the
        scratch database, the codebook copy, open_run's registration with git_state
        patched, the CloudExtractorBase without a client, the span builder). Any difference
        from the read-out = STOP.
    2b. Write tests/test_u28_cloud_gate_and_boundary.py, fail-first (R794(f)): the three
        tests as R794(f) names and parametrizes them, in the probe's seams; no server, no
        provider, no key, no gitignored fixture; the conftest fence on; each docstring
        states its claim number and its seam. Run on the parent: EXPECTED E1 — the red and
        green ids listed; output to b0_parent_acceptance.txt. A refusal or skipped case
        green on the parent = STOP.
    2c. Edit, R794(d)–(g) exactly, and nothing else:
        D — base.py:510–517: min_population="arm", skip_on_failures=False added to the
          call; no other argument changes (raise_on_collapse stays the default True,
          strict the default False).
        B — distribution_monitor.py, in run_post_extraction_check after check_distribution
          and before assert_no_collapse: if every result's total_non_null is 0, the summary
          takes skipped True and skip_reason "arm holds no non-null value for any
          categorical field" (the existing skip entries' key form, quoted at 2a), the
          results recorded as today, and assert_no_collapse is not called; the log line in
          the existing skip-log form. In main, after print_distribution_report: when every
          result's total_non_null is 0, print "arm {arm} holds no non-null value for any
          categorical field; nothing was checked" (to stderr if the report prints to
          stderr, else stdout — match the report's stream, quoted); exit code unchanged.
          check_distribution not edited.
        C — base.py store_result, after enforce_completeness and before the INSERTs:
          enforce_citations(spans-as-dicts, paper_id=paper_id, arm=arm, mode=LEGACY,
          escape_token=…, absence_sentinels=…, contract_unmet_token=…) with the three
          tokens from the source 2a established (I3), the span dicts in the shape
          check_citations reads (the local call's span_dicts form, quoted); the import of
          enforce_citations and LEGACY from engine.core.citation_guard. The "" fill at
          :282–283 untouched. If store_result's empty-string normalization branch becomes
          unreachable, quote it in the report; do not remove it.
        Docs — the five lines of R794(g), each located by its text; a line not found as
          quoted = STOP (the docs step, not the engine edit).
        Tests — tests/test_cloud_extraction.py: test_empty_string_normalized_to_null
          becomes test_an_empty_value_is_refused_at_the_write_boundary on its own drive,
          asserting stats["failed"] 1 (or the count the drive yields, read) and no
          cloud_extractions row for the paper, the "empty_string_to_null" assertions
          removed; its docstring rewritten to its new subject with "(R794(f))"; the
          TestStoreResultCrashProtection docstring's first word becomes "Paper-outcome"
          (R793(d)): """Paper-outcome store_result failures are caught per-paper; run
          continues.""".
        git diff --stat EXPECTED: base.py, distribution_monitor.py, CLAUDE.md,
        pipeline.md, test_cloud_extraction.py, the new test — six files; plus the two
        inventory files only if a recorded entry changed (E3: --write, git diff --stat, a
        stamp-only diff reverted and quoted as such; an entry change quoted whole and kept
        under R786(b)). The "−" lines of every file quoted and matched to the edits above.
    2d. Pins: 12l_U04/pin_tuples.py with both arguments, byte-identical to
        12ae_U27B/pin_tuples_u27b.json; the audit prompt_hash unchanged. A moved value =
        STOP.
    2e. Selected runs green: the new file; tests/test_cloud_extraction.py whole (the
        fixture present by ls — a reading); tests/test_distribution_monitor.py,
        tests/test_distribution_check_wiring.py, tests/test_write_boundary_guard.py,
        tests/test_readers_on_the_reader.py, tests/test_non_value_tokens_downstream.py and
        tests/test_u27_cloud_run_faults.py whole. Commit the engine change (fix scope; the
        message names B9/C28/U28, D, B and C by the ruling's letters, the docs lines, the
        rewritten test, and the inventory if regenerated), from the staged set. Do not push
        before the gate.
    2f. The standard gate by launch_gate.sh: evidence_dir ~/scratch/12ae-u28b/gate_u28b/,
        session name, ref_ids 12ae_U27B/gate_u27b/collected_ids.txt, ref_durations the
        durations file in 12ae_U27B/gate_u27b/ (name quoted), no selection. Ollama reads
        ≥5 s before and after (R660(b)). EXPECTED: 3,537 + N passed / 1 xfailed / 17
        deselected on 3,555 + N / 3,538 + N (E2; N read; the renamed test's id pair read
        and matched), only-ref exactly the renamed test's old id, only-gate the N new ids
        plus its new id; wall inside 837–1021 s; no phase >10 s slower; no model load in
        the window (R696(b)); the nineteen files. A red: class (c) per R746(c)/R772(b),
        re-run once; any other red = STOP, the engine commit standing. Concurrent jobs
        recorded (R783(b)).
    2g. Fingerprint --compare; push; level; --compare after (IDENTICAL). Ledger row; push;
        level.

  STEP 3 — Evidence commit (R731(c)). 12ae_U28B/: b0_parent_acceptance.txt, gate_u28b/
    (the nineteen files), pin_tuples_u28b.json; the staged set exactly those, every sha256
    equal to its scratch source; chained check && commit; fingerprint IDENTICAL before and
    after the push; level; ledger. Record hashes and parents.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1, I2 and I3 held.
  G2. Item 0 one commit: r794.txt at the architect's hash; Block 192 by the tool; the two
      notes and the dated line in the record's form, the plan equal to HEAD's plus exactly
      those edits and the block; the three rules files untouched by content; the brief
      file hashed; one chain; pushed, level, fingerprint IDENTICAL; ledger.
  G3. E1's red cases red on the parent and green in the gate, its green cases green on
      both; the diff exactly six files plus the inventory under R786(b)'s form; the
      zero-value branch, the two protected monitor tests, effective.py and scripts/
      untouched; pins and audit prompt_hash unchanged; the gate at the E2 counts inside the
      band (or outside, reported with its jobs), no load, nineteen files; engine commit
      pushed, level, fingerprint IDENTICAL; ledger.
  G4. The evidence commit as step 3 states; pushed, level, fingerprint IDENTICAL; ledger.

OUT OF SCOPE: U40 (the cloud path writing claim events; its Phase A is the next brief after
  U28 lands); U29; the reader; retiring or further writing the legacy cloud tables
  (R794(c)); the two scripts of F5; store_result's unreachable normalization branch (quoted,
  left); check_distribution's per-field contract; R71; the tuple; any other edit under
  engine/, tests/, scripts/, analysis/, docs/architecture/ or CLAUDE.md; the tools items;
  the 2026-11-09 job reads (the next open's); /wrap.

CONSTRAINTS (task-only): no model call, no provider call, no API key read, no restart, no
  live write; no edit in the working tree between 07:00 and 10:35 UTC; a count arrived at
  by eye is INFERRED, the computed value given with its command; where this brief
  describes a tool or record, the record governs and a difference is a finding (R732(a));
  a claim is named by its number (1–3) or its option letter (D, B, C) throughout.

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — metadata, render comparison, the
  two rows' last notes, the dated line, the brief file's hash, the check verbatim, chain,
  hashes, fingerprint, ledger; (2) the build — 2a's quotes (I3's token sources first), the
  tests as written, b0_parent_acceptance.txt verbatim, the edits as a diff per file, the
  diff --stat with every "−" line matched, the five docs lines before and after, the
  inventory diff (stamp-only reverted, or regenerated), the pin recompute, the selected
  runs, the engine commit hash and parent; (3) the gate — launch line, timestamps, wall vs
  837–1021, the summary line verbatim, collected/selected, the id comparison (the renamed
  pair named), durations >10 s or none, basetemp, journal window, Ollama reads, concurrent
  jobs, the nineteen files; (4) the evidence commit; (5) contradictions and findings,
  numbered, each with its quoted line; (6) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 or I2 false, a 1a mismatch, or a check
  failure or crash at 1e = STOP before the item-0 commit. I3 false, a red case of E1 green
  on the parent, a docs line not found as quoted, a diff outside the six files (inventory
  excepted under R786(b)'s form), a test that would construct a real SDK client, or a
  moved pin = STOP before the engine commit. A non-(c) red in the gate = STOP, the engine
  commit standing and reported. An expectation mismatch (E1–E3) is reported and the work
  continues. A spec premise that does not survive contact with disk is a finding, not an
  obstacle.

HANDBACK: Stop and report. Do not start U40 or U29 without explicit sign-off. Await the
  next brief or ruling in this session. STOP-12AE-U28B
