CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R793, PI ruling 2026-11-09 UTC; not on disk)
================================================================================

R793 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r793.txt: sha256 c87d908c511383a8e531e88fe8c2c3a3328a1dcb6d1aa65e343fd4c630b10436,
            7505 bytes, 87 lines, one trailing newline.

===== BEGIN r793.txt =====
RULING [evidence-engine 12ae-R793] — 12ae-U27B accepted: U27 landed, C56's three units passed and
  the row CLOSES at the next item 0; the series gains 1059.11 s as a confounded reading with its
  job recorded (R677(f)); readings, three the architect's; the docstring fix rides U28's engine
  commit; the section-L header corrected; U28's Phase A opens
STATUS: PI ruling, 2026-11-09 UTC, session 12ae, on architect recommendation, on the 12ae-U27B
  final report (item 0 76cfd4e; engine 077cb0fed89dee1891b9d989bbd1ce7de43b62ce; evidence
  1f0b36456e76bc0c8ad160ec04704a70c26321c4). Transcribe at item 0 of the 12ae-U28A brief below,
  by transcribe_ruling.py, in the block after R792's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the report):
  MEASURED: "12ae_U27B/brief_12ae_U27B.md is 7a9b05457b5454418138c9028dc4208754745adc2db41d7ae92462de699c3ba3,
    28,645 B, 363 lines" (the architect's copy: equal); "Plan +139 / −1; the "−" line is exactly
    the lead"; "6 failed, 6 passed in 10.79s"; "Each red fails at "…: the run fault did not
    leave run_arm as itself: None""; "the two transports +5 / −0 each, test_cloud_extraction.py
    +2 / −2, and the new test +220"; "--write changed only the generated-at stamp (e89d602 →
    76cfd4e) in both files; no recorded entry changed"; "pin_tuples_u27b.json is aea19354…f24f,
    cmp byte-identical to pin_tuples_u26b.json"; "105 passed, 0 skipped"; "Wall 1059.11 s,
    outside 837–1021 s"; "=== 3537 passed, 17 deselected, 1 xfailed, 2 warnings in 1057.26s
    (0:17:37) ===="; "collected 3555 items / 17 deselected / 3538 selected"; "only-gate 12 (all
    U27), only-ref 0"; "slower by more than 10 s: 0"; "-- No entries --"; "/api/ps is
    {"models": []} both times"; "pid 1962934, timeout 1500 /home/ankitsarin/projects/surgical-cv/
    .venv/bin/python -m pytest tests/ -q -p no:cacheprovider, started 19:20:54"; "pytest-577 …
    last written at 19:35:59"; "all nineteen present"; "21 files, all added"; "Ledger c4d00e2 …
    76161e1 … 959d9e0".
  READ (CC's): findings 1–6; "I reverted it, --check reports "inventory in sync", and the
    inventory is not in the commit"; """a paper-outcome store_result failures are caught
    per-paper; run continues."""; "Ledger commit 76161e1 carries three rows from the
    surgical-cv-fw1-b session alongside mine"; "Every earlier section-L table is opened per
    ruling, e.g. "L (12l additions, R613)."".
  INFERRED: none.

R793(a) — The 12ae-U27B final report is accepted as reported: item 0 76cfd4e (Block 190; the
  INT-ae12-2 row and the lead sentence; the brief file byte-equal to the architect's copy, the
  ninth match of nine), the engine commit 077cb0f (four files: two imports, four re-raise pairs,
  the pinning test's exception and docstring; base.py, scripts/ and the inventory untouched),
  the gate 3,537 / 1 / 17 on 3,555 / 3,538, only-gate the 12 U27 ids, no load, no phase slower
  by more than 10 s, the nineteen files, green at the first run; the evidence commit 1f0b364;
  ledgers c4d00e2, 76161e1 and 959d9e0 with trailers. Both pins and the audit prompt_hash
  byte-identical. U27 is landed; P1's fix units 37, done 30 (U01–U27, U37, U38, U39). The
  acceptance test tests/test_u27_cloud_run_faults.py::test_run_faults_leave_the_cloud_handlers_as_themselves
  (6 run-fault cases red on the parent 76cfd4e in 12ae_U27B/b0_parent_acceptance.txt, green in
  12ae_U27B/gate_u27b/; 6 preservation cases green on both) is C56's cloud-half acceptance.
R793(b) — C56 CLOSES at the next item 0, by R514 and R542: its three units have passed — U02
  (the predicate with handlers 1–2, its commit and acceptance test as the row's own notes
  record them), U26 (69e9db8; tests/test_u26_parse_run_faults.py, R791(a)) and U27 (077cb0f;
  (a)'s test). The CLOSED note in C49's form (R785(f), R787(b)): the row's stated fix delivered
  — one owned run-fault predicate re-raised ahead of every converting handler except 3 (P7,
  R542) — and the three acceptance tests passing, each with its b0 and gate paths; the three
  commits by full hash, read from the record (U02's from the row's notes), not recalled. The
  row is counted until P1 closes (R514, R607(d)).
R793(c) — The series on this host state gains 1059.11 s as a confounded reading, with its
  confounder recorded: the surgical-cv full suite (pid 1962934, `timeout 1500 … python -m pytest
  tests/ -q -p no:cacheprovider`, started 19:20:54 UTC from another Claude session's shell,
  overlapping about 12½ of the gate's 17½ minutes). R677(f): "confounded readings included with
  their confounders recorded … a confounded reading enters the mean"; R783(b): reported with its
  concurrent jobs, not adapted to. CC did not re-run and was right not to: every id passed, no
  phase was slower than the reference by more than 10 s, and the band is a reporting tripwire,
  not a gate. The close's recut (the last four standard gates) will carry it: the wrap ruling
  states the arithmetic and names the confounder beside the value. Thirty-six.
R793(d) — Finding 2 (the docstring "a paper-outcome store_result failures" — the architect's
  wording, applied as ordered): the one-word fix, the line becoming """Paper-outcome
  store_result failures are caught per-paper; run continues.""", rides U28's engine commit as
  a docstring-only class-(c) line in tests/test_cloud_extraction.py, quoted in that brief; no
  commit of its own, since every engine-side commit carries a gate. A docstring is read as a
  sentence before it is ordered.
R793(e) — Readings, no row: finding 3 (inventory --write restamps generated_at_commit when no
  entry changed, so R792(f)'s "empty diff" cannot be observed through --write; CC's revert of
  the stamp-only diff was right — the inventory is regenerated only when a recorded entry
  changes, and a stamp-only diff is reverted; a tools item beside R791(c) finding 4, for the
  tools change: --write writes nothing when no entry changes); finding 4 (ledger 76161e1 carries
  three rows of the surgical-cv-fw1-b session that were pending in the shared working tree —
  the ledger is one file shared by every session on the box, its rows are true and its message
  is true; pushed, no amend (R680(b)); a brief's ledger step stages the file whole and the
  report names any foreign rows, as this one did; no rule); finding 5 (the brief placed
  INT-ae12-2 in R785's table and amended its lead where the record's convention is one table
  per ruling — the architect's, not checked against the earlier section-L tables; the row stays
  where it is, and the header cell "On disk (`12ae_U24A.md`, composed under R785(f))" becomes
  "On disk (`12ae_U24A.md` under R785(f); `12ae_U27A.md` under R792(g))" at the next item 0, so
  the header describes both rows); finding 6 (E1–E3 held). Finding 1 is (c).
R793(f) — U28's Phase A opens: 12l_P1A.md §3's U28, "The cloud collapse gate reads what the
  cloud path wrote; cloud citation guard" (B9, C28; Wave 2 rows 25 (L12-C1) and 27 (L12-C3);
  N44; R599), read-only, in 12ae_U27A.md's form, its entry quoted first. Files:
  engine/cloud/base.py, engine/validators/distribution_monitor.py. The cloud path is behind R71
  and unreachable on Run 7; no provider is called, no key read; the probe stores through
  store_result into a scratch database and drives the monitor against it. No engine, test,
  script, spec or codebook edit; no model call; no live write. This ruling states no standing
  rule.
===== END r793.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ae-U28A] — U28's Phase A read-out (B9, C28: the cloud run's
  distribution-collapse gate against the store the cloud path writes; N44; the cloud
  write boundary's missing-snippet fill). Item 0: R793 as the next block; C56 CLOSED;
  the dated units line; the section-L header cell; this brief committed as pasted; no
  standing rule. Read-only on engine/ and tests/; STOP before any edit.
PURPOSE: Put on disk, before the design, what the cloud gate reads and what store_result
  writes, whether the two meet at all for a registered cloud arm, what the monitor
  reports for an arm holding no claims (N44), what the write boundary does with a
  non-sentinel value whose snippet is empty, which tests pin each behaviour, and whether
  the entry's acceptance test can be written at HEAD and fails. The entry bundles three
  claims in two files; Phase A says whether they are one unit or three, and what the
  gate's store should be (the event store the reader reads, or the legacy tables the
  cloud path writes) — the ruling decides. Mixed: one docs commit (item 0), one docs
  commit (the read-out). No engine, test, script, spec or codebook change. No live
  write. No model call. No provider call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 191; C56's CLOSED note; the dated
  units line; the section-L header cell); docs/session-reports/session-12/12ae_open/
  r793.txt; a new directory docs/session-reports/session-12/12ae_U28A/
  (brief_12ae_U28A.md at item 0; the read-out, census and probe scripts with outputs at
  step 2); ~/scratch/12ae-u28a/; the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/ (R71: enabled_arms stays empty), the codebook, docs/architecture/,
  CLAUDE.md, docs/inventory/, the tools; the id list, the glosses file and
  STANDING_RULES.md (not one byte — R793 states no rule); review.db (mode=ro instruments
  only; the probe uses a scratch database); any .env or API key (never read — the probe
  constructs no SDK client: it calls store_result and the monitor directly, or drives a
  transport with the fakes of fake_probe_u27a.py); any scratch directory other than
  ~/scratch/12ae-u28a/; the working tree inside 07:00–10:35 UTC (R86).

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ae-U27B report; R793): HEAD 1f0b36456e76bc0c8ad160ec04704a70c26321c4, parent
    077cb0fed89dee1891b9d989bbd1ce7de43b62ce, clean, level; the engine tree equal to
    077cb0f's; plan ends "…ends at R792.", last block 190; STANDING_RULES.md 87 / 8 / 95,
    glosses 43, the id list's last A line "A R784(g)"; claude-config at 959d9e0 or an
    accepted descendant (R663(b)). Architect's hash of r793.txt: Section 1.
  READ (the clone at 1f0b364): R1. 12l_P1A.md §3's U28 entry, whole (:422–430): "### U28
    — The cloud collapse gate reads what the cloud path wrote; cloud citation guard /
    Rows: B9, C28 / Pin effect: not affecting / Files: engine/cloud/base.py,
    engine/validators/distribution_monitor.py / Depends on: — / Acceptance test
    (synthetic; discharges R599): test_cloud_gate_sees_what_store_result_wrote — Twelve
    papers stored through store_result with one identical categorical value: the gate
    raises; an arm holding zero claims is reported skipped, not OK (N44). store_result
    refuses a non-sentinel value with an empty snippet and inserts nothing. Fails on the
    unmodified tree. / Note: Cloud; not reachable on live while R71 holds." R2.
    12j_review_w2.md rows 25 (:164, L12-C1: "The cloud run's collapse gate reads a store
    the cloud path never writes"; sites "CloudExtractorBase.run_distribution_check,
    store_result; called at the end of OpenAIExtractor.run / AnthropicExtractor.run";
    "reproduced"; "extends C28 and B9") and 27 (:166, L12-C3: "No citation check at the
    cloud write boundary; a missing snippet is filled with """; sites
    "parse_response_to_spans (if isinstance(span, dict) and "source_snippet" not in span:
    span["source_snippet"] = "", comment "so spans aren't dropped") and store_result
    (checks if not spans and enforce_completeness only)"; "code order"; "extends C28").
    R3. 12l_P1A.md:3393, N44: "engine/validators/distribution_monitor.py::check_distribution
    — an arm with zero values returns status OK on every field with skipped False (the
    monitor side of this row; also affects any CLI call with a registered arm that holds
    no claims) | 1 (conditional, cloud) | P1 | inside U28". R4. Anchors at HEAD, quoted
    beside the line at 2b: base.py:501–517 run_distribution_check (calls
    run_post_extraction_check(db_path=…, review_name=…, arm=self.arm_name,
    codebook_path=…, extracted_count=…, failed_count=…)); openai_extractor.py:233 and
    anthropic_extractor.py:261 "monitor = self.run_distribution_check(stats)";
    distribution_monitor.py:92 _query_values (docstring: "Routing is now the arms
    registry (R12) and there is no name test left. An arm not in the registry raises
    UnknownArm"), :127 _query_all_fields, :185 check_distribution, :351 arm_population
    ("Through the reader's own definitions (eligible_paper_ids, live_claim_events)"),
    :367 run_post_extraction_check (its skip rules :398–453), :507 main;
    scripts/run_pipeline.py:425 (the local caller); base.py:279–300 (the "" fill and the
    null→sentinel branch), :336–358 (store_result's 0-span refusal and
    enforce_completeness), :362–403 (the INSERTs into cloud_extractions /
    cloud_evidence_spans). R5. 12ae_U27A/12ae_U27A.md is the form (the mapping first
    where a numbering must be reconciled; here §0–§9 with the three claims of R1 kept
    apart). R6. Rows B9 and C28 with every dated note, quoted at 2a — including C28's
    "migration 016's docstring lists [cloud_extractions / cloud_evidence_spans] as
    read-only legacy", INT-j12-33 (opt-in and the manifest enforced by one caller) and
    B9's skip-row note (10b-SMOKE C2, run_events.jsonl distribution_check skipped: true,
    arm_population 5).
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 077cb0f. I2. A registered
    cloud arm's values are read by the reader (engine/core/effective.py's iter_grid /
    live_claim_events) from one store, and store_result writes to another, or the same
    — which, is established by quoted code and a FAKE before any option is written; if
    the reader's route for a cloud arm cannot be established from the code, STOP with the
    finding. I3. store_result and the monitor can be driven against a scratch
    ReviewDatabase with the cloud arm registered (the registry's form for a cloud arm —
    open_run's cloud_arms=[…], or the arms table — read and quoted) without any SDK
    client; if registering the arm requires a path the probe cannot take read-only, STOP.
  INFERRED — expectation: E1. The reader reads claim events (the event store) for every
    registered arm and store_result writes only the two legacy tables, so the gate never
    sees a cloud write — the probe shows 12 identical stored values and a gate that reports
    OK or skipped (R2 row 25: "reproduced"). E2. check_distribution returns OK with skipped
    False for an arm holding zero values (N44), and run_post_extraction_check's under-10
    rule masks it on the cloud path when stats["extracted"] < 10. E3. A span with a
    non-sentinel value and source_snippet "" passes store_result today (only if not spans
    and enforce_completeness check), and is inserted with the empty snippet. E4. The
    pinning tests are among tests/test_distribution_monitor.py,
    tests/test_distribution_check_wiring.py, tests/test_cloud_extraction.py (the
    distribution-gate and store tests), tests/test_write_boundary_guard.py,
    tests/test_readers_on_the_reader.py and tests/test_non_value_tokens_downstream.py.

CONTEXT: U27 landed at 077cb0f (R793); C56 closes at this item 0. U28 is next in 12l_P1A.md
  §3's order; it shares base.py with U29 (U29's ground is send/record_call, U28's is
  store_result and the gate — different functions, read and stated). The cloud path is
  unreachable on live (R71) and on Run 7. Phase A of the two-phase form; the design ruling
  follows; one build brief if Phase A is complete (R785(h)'s form).

CONTRACT (steps, in order; evidence under ~/scratch/12ae-u28a/):

  STEP 0 — Pre-check (read-only). date -u (outside 07:00–10:35 UTC). HEAD and parent
    proved by git cat-file -e <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree.
    Plan: last line "…ends at R792.", last block 190, no Block 191. Rules files: the three
    counts above. claude-config: 959d9e0 or an accepted descendant, clean, level. I1.
    Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12ae-U25A's item
    0 — a block, a CLOSED note, a dated line, the brief file — ~/scratch/12ae-u25a/
    check_item0_u25a.py and its chain, quoted before composing).
    1a. 12ae_open/r793.txt from Section 1; hash straight after the write: EXPECTED
        c87d908c511383a8e531e88fe8c2c3a3328a1dcb6d1aa65e343fd4c630b10436, 7505, 87.
        Mismatch = STOP.
    1b. block191.json in block190.json's key set: tag R793; item "item 0 of the
        12ae-U28A brief"; beside = "C56's CLOSED note, the dated units line, the
        section-L header cell and the brief file it orders" (composed; quoted); made_date
        2026-11-09; session 12ae; report = the 12ae-U27B final report (item 0 76cfd4e;
        engine 077cb0f; evidence 1f0b364), final-report form; sentence model, followed
        "12ae-U28A brief"; prev_ends R792. Render to stdout, compare with Block 190's
        order, then --append ("−" EXPECTED 0 for the append, read at 1e).
    1c. C56 CLOSED (R793(b)), in C49's CLOSED form (plan :280, quoted before composing):
        the status cell's leading "OPEN — defect candidate (R-SCOPE-1), before the tag"
        becomes "**CLOSED** on U27 (077cb0fed89dee1891b9d989bbd1ce7de43b62ce), by R514 and
        R542: …" with the stated fix (one owned run-fault predicate re-raised ahead of
        every converting handler except 3, which stays Class 3 in P7) and the three
        acceptance tests, each with its b0 and gate paths — U02's test and commit read
        from the row's own notes and the U02 record (quote the note you take them from;
        a hash not found in the record is a STOP, not a recollection), U26's
        (tests/test_u26_parse_run_faults.py::test_run_faults_leave_the_parse_handlers_as_themselves,
        69e9db8, 12ae_U26B/b0_parent_acceptance.txt, 12ae_U26B/gate_u26b/), U27's
        (tests/test_u27_cloud_run_faults.py::test_run_faults_leave_the_cloud_handlers_as_themselves,
        077cb0f, 12ae_U27B/b0_parent_acceptance.txt, 12ae_U27B/gate_u27b/); "counted
        until P1 closes (R514, R607(d))"; every existing note of the row kept. The dated
        units line after 12ae-U27A's, in its form: U27 landed at 077cb0f; C56 CLOSED on
        it and counted until P1 closes (R514, R607(d), R793(b)); fix units 37, done 30
        (U01–U27, U37, U38, U39); rows 284 (94 / 56 / 134, 0 unclassed) unchanged; rows
        out 0. The section-L header cell (R793(e), finding 5): "On disk (`12ae_U24A.md`,
        composed under R785(f))" becomes "On disk (`12ae_U24A.md` under R785(f);
        `12ae_U27A.md` under R792(g))". Two rows replaced (C56; the header line), one
        line inserted; the "−" count read and matched to the two.
    1d. The brief file (R784(g)(v)): 12ae_U28A/brief_12ae_U28A.md from this whole pasted
        block, "CLAUDE CODE" through the last HANDBACK line, one trailing newline, written
        once; sha256, bytes, lines straight after the write, reported.
    1e. The item-0 check in 12ae-U25A's content form, adapted: --check-plan
        byte-identical; the plan's numstat read ("−" matched to the two lines); every
        earlier "ends at" line kept; last line "…ends at R793." once; the plan equals
        HEAD's plus the two lines replaced, the line inserted and Block 191 appended,
        nothing else (computed); the C56 cell begins "**CLOSED** on U27"; r793.txt equal
        to 1a; the three rules files unchanged and unstaged; the brief file hashed; the
        staged set exactly the plan, r793.txt and the brief file. Evidence by redirect;
        quote the check.
    1f. <check> && <commit>, one line; message from the staged set (R778(d)), docs scope.
        Fingerprint --compare before the push; push; level; --compare after (IDENTICAL).
        Ledger row with trailers (foreign pending rows, if any, named in the report —
        R793(e)); push; level. Record hashes and parents.

  STEP 2 — The read-out (read-only; no server; no provider; no live database; a scratch
    ReviewDatabase for the probe).
    2a. §1: the U28 entry with its neighbours (R1); rows B9 and C28 whole with every dated
        note (R6), the Wave 2 rows 25 and 27 (R2) and N44 (R3) quoted; R599's block; R71's
        block; migration 016's docstring on the two cloud tables (quoted from the
        migration file); INT-j12-33's row; every CLAUDE.md and docs/architecture line on
        the distribution monitor, the cloud store and the cloud citation check (the
        read-out of 12ae_U27A §1 2d quoted pipeline.md:248 and CLAUDE.md:382 — re-read,
        and add the monitor's lines); §1 2e: the questions the inputs raise that Phase A
        does not answer — among them whether the gate's store should be the event store
        (the cloud path writing claim events, as the local path does) or the legacy tables
        (the reader given a cloud route), and whether the three claims of R1 are one unit.
    2b. §2 Sites, three claims kept apart: (i) THE GATE — run_distribution_check and its
        two callers; run_post_extraction_check's skip rules in order (:398–453, each
        quoted with its threshold and what it reads: extracted_count, failed_count,
        arm_population, the codebook); check_distribution; _query_all_fields and the
        reader's route for a registered arm (engine/core/effective.py: iter_grid,
        live_claim_events, registered_arms — the tables each reads, quoted; I2);
        arm_population. (ii) THE STORE — store_result's INSERTs (the tables and columns),
        and whether anything writes a claim event for a cloud arm (grep for the event
        writer under engine/cloud/ — 12ae_U27A §3 found none; re-read). (iii) THE
        BOUNDARY — parse_response_to_spans :279–300 (the "" fill, the null→sentinel branch
        and its own "" clear), the Pydantic span model (source_snippet's type and
        default), enforce_completeness's checks, store_result's checks; the local path's
        citation check at its write boundary (SnippetNotExact's raiser; audit_span — path
        and line) for the contrast. Each site: path:line, the clause and body quoted.
    2c. §3 Mechanism (CODE ORDER unless FAKE): (i) for a registered cloud arm, the table
        the gate's values come from and the table store_result fills — the same or not
        (I2); what the gate reports after twelve store_result writes with one identical
        categorical value (FAKE: scratch database, the arm registered as I3 establishes,
        twelve papers stored through store_result, run_post_extraction_check called as
        base.py:510 calls it with extracted_count 12 — the summary dict quoted; then with
        extracted_count 5 for the under-10 rule); (ii) N44 (FAKE: check_distribution for
        the registered arm holding zero claims — the per-field status and skipped flag
        quoted; and run_post_extraction_check's summary for the same); (iii) the boundary
        (FAKE: a span dict with a non-sentinel value and no source_snippet through
        parse_response_to_spans then store_result — inserted or refused; the row read
        back; the same with the sentinel value). How run_pipeline.py:425 calls the monitor
        for the local arm (arguments quoted) against base.py:510's call, and what the
        local path's write of claim events means for the gate there (the contrast that
        defines U28's gap). The U27 re-raise at the store handlers (077cb0f) and the gate:
        does a gate raise (DistributionCollapseError) pass through run_arm's close as
        failed today (test_distribution_collapse_not_caught — quote it).
    2d. §4 Census: tests coupled to run_distribution_check, run_post_extraction_check,
        check_distribution, _query_values/_query_all_fields, arm_population,
        parse_response_to_spans and store_result — census_u28a.py in census_u27a.py's form
        (declared completeness as stated there); the pinning tests found and quoted (E4),
        each with what each option of §7 would do to it; the fixtures they use (the
        gitignored database of B14, or scratch); how many run on this box.
    2e. §5 The acceptance test against HEAD (FAKE, fake_probe_u28a.py → .txt): the entry's
        three claims as three assertions at HEAD — (1) twelve identical stored values and
        the gate does not raise (EXPECTED, E1); (2) the zero-claim arm reported OK with
        skipped False (EXPECTED, E2); (3) a non-sentinel value with an empty snippet
        inserted (EXPECTED, E3) — each red or green at HEAD stated; whether one test or
        three is the honest shape; what seams it needs (the arm registration, the
        codebook beside the scratch database, the Pydantic model).
    2f. §6 Cases: the gate with 12 / 9 / 0 stored papers; the arm registered / not
        registered (UnknownArm); the sentinel value with "" vs a non-sentinel value with
        ""; the local arm for contrast; strict and non-strict; the CLI (main) with a
        registered arm holding no claims (N44's second clause).
    2g. §7 Options for the ruling, per claim: GATE — A1: store_result also writes claim
        events for the cloud arm (the event writer's form, the reader unchanged; what
        becomes of the two legacy tables — kept as the cloud artefact store, or retired
        under migration 016's note); A2: the reader given a route to cloud_extractions /
        cloud_evidence_spans for a cloud arm (the registry's routing, R12; what else reads
        through the reader and would start seeing cloud rows — concordance, the audit, the
        exports); A3: the gate on the cloud path reads cloud_extractions directly (a
        private route — the thing READERS-01 removed; stated so the ruling can refuse it).
        N44 — B: check_distribution reports skipped (or a distinct status) for an arm with
        zero values, and run_post_extraction_check's summary says so; what the local path's
        tests assert today. BOUNDARY — C: store_result (or enforce_completeness) refuses a
        non-sentinel value whose snippet is empty, inserting nothing (the sentinel keeps
        its "" by R132); the "" fill at :282–283 then feeds a refusal rather than a stored
        row — whether the fill stays. For each: sites, test rewrites, the acceptance
        test's shape, what it does not fix, and whether it belongs in U28 or in another
        row (INT-j12-33, C24/U29, the audit). Classified sites. §8 findings; §9 limits.
    2h. Commit the read-out (12ae_U28A/12ae_U28A.md), census_u28a.py/.txt,
        fake_probe_u28a.py/.txt: the staged set exactly those, nothing under engine/,
        tests/ or data/ (asserted); fingerprint IDENTICAL before and after the push;
        level; the ledger row. Record hashes.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 held.
  G2. Item 0 one commit: r793.txt at the architect's hash; Block 191 by the tool; the
      CLOSED cell, the dated line and the header cell in the record's form, the plan
      equal to HEAD's plus exactly those edits and the block; the three rules files
      untouched by content; the brief file hashed; one chain; pushed, level, fingerprint
      IDENTICAL; ledger.
  G3. I2 established by quoted code and the FAKE before any option is written; I3 shown
      by the quoted registration and the probe's record; the read-out in R5's form with
      every section present and the three claims kept apart; every code claim beside a
      quoted line with path and line read at HEAD; CODE ORDER / FAKE labelled; the census
      and probe instruments and outputs committed with their declared completeness; no
      SDK client constructed (asserted in the probe's output); the Ollama journal for the
      probe window empty; no file outside 12ae_U28A/ in the commit; pushed, level,
      fingerprint IDENTICAL; ledger.

OUT OF SCOPE: U28's design and build; the docstring fix of R793(d) (U28B's engine
  commit); U29 (send/record_call; C24); the tuple; R71 (enabled_arms stays empty); any
  edit under engine/, tests/, scripts/, analysis/, docs/architecture/ or CLAUDE.md; the
  tools items; the 2026-11-09 job reads (the next open's); /wrap.

CONSTRAINTS (task-only): no model call, no provider call, no API key read, no restart, no
  live write; the probe's output records that no SDK client was constructed; a count
  arrived at by eye is INFERRED, the computed value given with its command; where this
  brief describes a tool or record, the record governs and a difference is a finding
  (R732(a)); a claim of the entry is named by its number (1–3) throughout.

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — metadata, render comparison, the
  C56 cell's opening before and after, the dated line, the header cell before and after,
  the brief file's hash, the check verbatim, chain, hashes, fingerprint, ledger; (2) the
  read-out's §0–§9 in full as committed, I2's answer first, the probe's summary dicts
  quoted; (3) contradictions and findings, numbered, each with its quoted line; (4) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 false, a 1a mismatch, a hash for U02 not
  found in the record, or a check failure or crash at 1e = STOP before the item-0 commit.
  I2 or I3 not establishable = STOP before the probe, with the finding. An expectation
  mismatch (E1–E4) is reported and the read-out continues. A spec premise that does not
  survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not start U28's design or build without explicit
  sign-off. Await the next brief or ruling in this session. STOP-12AE-U28A
