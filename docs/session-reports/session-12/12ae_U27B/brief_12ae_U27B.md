CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R792, PI ruling 2026-11-09 UTC; not on disk)
================================================================================

R792 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r792.txt: sha256 d4c263838979181a54a926d2743a328c716258a8cf0ebdd3f030315977553762,
            11130 bytes, 126 lines, one trailing newline.

===== BEGIN r792.txt =====
RULING [evidence-engine 12ae-R792] — 12ae-U27A accepted; the committed brief equal to the
  architect's copy; U27's design: option A at the four cloud handlers, the pinning test moved to
  a paper outcome, the acceptance test through run_arm with fakes and no provider; B not taken
  (nothing to carve out at the pinned SDKs); C not taken (the cloud close stays `failed`, as the
  local path's); U27 lands before U29; INT-ae12-2 opened; readings; the build opens as one brief
STATUS: PI ruling, 2026-11-09 UTC, session 12ae, on architect recommendation, on the 12ae-U27A
  final report (item 0 dd1b0b5; read-out 0d1d004, docs/session-reports/session-12/12ae_U27A/
  12ae_U27A.md, sha256 3376c91fd7bf1a7125caca62f02f344c69b56af87faee12913f172d989c49353, 338
  lines). Transcribe at item 0 of the 12ae-U27B brief below, by transcribe_ruling.py, in the block
  after R791's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the read-out and the report):
  MEASURED: "12ae_U27A/brief_12ae_U27A.md is 00101c9e456dffcac8a8f25d3c697397a3e715269a8bd0184b98013919700138,
    24,125 B, 315 lines" (the architect's copy: equal, cmp byte for byte); "constructors: 20,
    every one with the placeholder key and no other argument: True"; "outbound requests
    attempted: httpx send 0, socket connect 0"; "The Ollama journal for the probe window
    (2026-11-09 19:05:00–19:05:14 UTC) reads -- No entries --"; "8 / 9, recorder
    sqlite3.OperationalError | 6 (3 per paper) | [2, 4, 2, 4] | failed 2 | 0 | 0 | 0 | completed";
    "10 / 11, store sqlite3.OperationalError | 2 | — | failed 2 | 0 (both answers discarded) | 2
    (completed) | 0 | completed"; "issubclass(openai.APIConnectionError, run_fault_classes()) is
    False (and so for anthropic)"; "all 36 of tests/test_cloud_extraction.py's ids report
    PASSED, none skipped"; "Plan +78 / −1; the "−" line is exactly C56's row".
  READ (the read-out, §1–§9): the I2 mapping, "8 → #25 → openai_extractor.py:179", "9 → #27 →
    anthropic_extractor.py:205", "10 → #26 → openai_extractor.py:209", "11 → #28 →
    anthropic_extractor.py:237", each "except Exception as exc:"; "Every handler maps, one to
    one"; "self.client = openai.OpenAI(api_key=key)" (:36) and the anthropic twin (:37);
    "Passing api_key short-circuits os.environ.get, so no key is read"; base.py:141 "if
    self.run_id is not None:"; the close "except BaseException as exc: … status, reason =
    "failed", rm.failure_reason(exc) … raise … finally: rm.close_run(…)" (run_cloud_extraction.py
    :112–127); "There is no aborted close on the cloud path"; "No test patches a run-fault
    side_effect into a cloud seam"; the pinning test "tests/test_cloud_extraction.py:840 …
    raise sqlite3.IntegrityError("simulated constraint violation") … assert stats["failed"] == 1
    / assert stats["extracted"] == 1"; "TestStoreResultCrashProtection has only the OpenAI case";
    "neither transport is in scripts/run_pipeline.py's static closure"; §2e's four questions; F1
    ("C56's row carries only Wave 2 row 24"); F4 ("U29 shares U27's two files"); F5
    ("12f_C56-A.md:97 … predates R625(a)(8)"); F6 ("modules.md:274 … ARM= … retired (C20)").
  INFERRED: none.

R792(a) — The 12ae-U27A final report is accepted as printed: item 0 dd1b0b5 (Block 189 by
  transcribe_ruling.py; the C56 note; the dated units line; brief_12ae_U27A.md), the read-out
  0d1d004 with census_u27a.py/.txt and fake_probe_u27a.py/.txt (the fakes' call record inside:
  twenty constructions with the placeholder key, zero outbound requests, the journal empty),
  ledgers c8b8df1 and 0eddaf6 with trailers. No stop. The committed brief file is byte-equal to
  the architect's copy, the eighth match of eight. I2 and I3 held; E1–E3 held.
R792(b) — U27's design: option A. At the four handlers of the mapping — openai_extractor.py:179
  (8) and anthropic_extractor.py:205 (9), before the `if attempt < 2:` re-send decision;
  openai_extractor.py:209 (10) and anthropic_extractor.py:237 (11), before the failed-paper
  count — `if is_run_fault(exc): raise`, U02's form, `is_run_fault` imported from
  engine.core.extraction_events in each transport. No other engine line changes; base.py is not
  edited (its :308 and :401 are outside U27 by the mapping; :401 already re-raises into 10–11).
  The run closes through run_arm's existing close, `failed` with `rm.failure_reason(exc)`
  (R625(a)(8)), which the read-out quotes; this is not C47's family. The tuple is not touched
  (U02's contract).
R792(c) — Not taken: B (a connection-error carve-out at 8–9). At the pinned SDKs (openai 2.24.0,
  anthropic 0.84.0) every transport exception leaves the client as APIConnectionError or
  APITimeoutError, neither a tuple member, so a provider outage stays with the retry loop under
  A exactly as today; a carve-out would guard a case the SDKs do not produce and would keep a
  rule broader than the tuple's in two files. Question 1 of §2e is answered by the record: a
  provider outage is not a run fault by the tuple's definition, and this ruling does not make it
  one. Not taken: C (an `aborted` close for a run fault at the entry point). run_pipeline closes
  `failed` for a store fault; C would make the cloud path's close differ from the local path's
  for the same sqlite3.Error, and it edits scripts/run_cloud_extraction.py, outside U27's file
  list. Question 4 of §2e: `failed`, with the reason, as today.
R792(d) — Order (F4; question 2 of §2e): U27 lands before U29, 12l_P1A.md §3's order. A recorder
  fault at 8–9 is record_call itself raising after a paid send; U29's fix (a send that raises
  still records its call) cannot write a row through a failing recorder, so the unrecorded paid
  call under a recorder fault is irreducible and U29 does not change it. Under A that run closes
  `failed` with the reason instead of re-sending twice and reading `completed`, which is the
  honest record. Neither unit changes the other's handlers.
R792(e) — Fail-first and callers: the acceptance test
  tests/test_u27_cloud_run_faults.py::test_run_faults_leave_the_cloud_handlers_as_themselves,
  in fake_probe_u27a.py's seams (a scratch ReviewDatabase with two seeded papers and parsed
  texts, the live codebook copied beside it and never written, a spec copy with the one arm
  enabled, rm.git_state patched clean, cli.EXTRACTORS pointing at a thin subclass passing a
  placeholder key, the SDK classes replaced by recording fakes, time.sleep a recorder, httpx
  send and socket.connect refusing recorders), driven through scripts.run_cloud_extraction.run_arm,
  parametrized: at 8 and 9, {sqlite3.OperationalError at the recorder, a bare ConnectionError
  from the provider seam, KeyError, the SDK's APIConnectionError}; at 10 and 11,
  {sqlite3.OperationalError at the store, KeyError} — twelve cases. Tuple-member cases (six):
  the exception leaves run_arm as itself (identity); exactly one send; no cloud_extractions row
  for the arm (the paper stays pending); run_arm's manifest row closed `failed` with
  rm.failure_reason(exc). KeyError and APIConnectionError cases (six): today's outcome as §5
  records it (8–9: three sends per paper, waits [2, 4, 2, 4], failed 2, manifest `completed`;
  10–11: failed 2, run_calls 2 `completed`, manifest `completed`). The test asserts the fakes'
  record: every construction with the placeholder key, zero outbound requests. It opens no
  gitignored fixture (B14's skip does not apply to it). Red on the parent at the six
  tuple-member cases, green at the six others, recorded in b0_parent_acceptance.txt; green in
  the gate. The pinning test tests/test_cloud_extraction.py::TestStoreResultCrashProtection::
  test_openai_store_failure_continues_run moves its injected exception from
  sqlite3.IntegrityError to ValueError (the 0-span refusal, store_result's own paper outcome,
  base.py:342–358), its subject kept — a per-paper store failure that is a paper outcome
  continues the run — class (c); its class docstring gains "a paper-outcome" before
  "store_result failures" so it reads true. No Anthropic twin is added (F8): the acceptance
  test's KeyError cases at 10 and 11 prove the conversion kept on both transports.
R792(f) — Docs: no CLAUDE.md or docs/architecture line is made false by (b) — the lines the
  read-out quotes (pipeline.md:439, models.md:101–102, :126, CLAUDE.md:381–382) describe
  provider errors and the 0-span refusal, which A leaves as they are. None is edited.
  docs/inventory: regenerated in the engine commit only if `inventory --write` changes a file
  (R786(b)'s form, the drift read from the diff, not from --check — R791(c) finding 4); an empty
  diff is the expected case, since no entry point moves.
R792(g) — Rows and notes, at 12ae-U27B's item 0: INT-ae12-2 opened in section L (12ae
  additions), Class 3, P7, from §2e question 3 and 12f_C56-A.md #25: the cloud retry loops
  re-send on the non-retryable provider errors (BadRequestError, NotFoundError,
  PermissionDeniedError) as they do on a transient one — three paid attempts against a request
  that cannot succeed; AuthenticationError alone stops the loop (openai_extractor.py:157,
  anthropic_extractor.py:179). Behind R71; no stored value wrong. The id INT-ae12-2 is first
  used here (R790(f)'s HK-ARCH note named it only as not opened). No note on C56 (nothing has
  landed since R791(a)'s note; its close is the next item 0's, after U27's gate, when U02, U26
  and U27 have passed — R542). No note on HK-ARCH for F6: the 2026-10-15 (12o-U12-C6 resume)
  note already records modules.md's ARM= entries as stale. No dated units line (nothing landed).
R792(h) — Readings, no row: F1 (R6's premise was the architect's — the Wave 2 notes 25, 27 and
  28 sit on B9, C28 and C24, not C56; the overlap stands where it is; a row's content is quoted
  from the row, not recalled); F3 (the recorder route exists only under a run — so the
  acceptance test drives 8–9 through run_arm, and the bare ConnectionError case reaches them
  from the provider seam); F5 (12f_C56-A.md:97 predates R625(a)(8); the record governs,
  R732(a)); F6 (recorded already, (g)); F8 ((e)); E1–E3 held. The read-out's §9 limits stand
  as recorded; the stage-key question at 12f_C56-A.md:717 stays open and is not U27's.
R792(i) — The build opens as one brief, 12ae-U27B (R785(h)'s form): the read-out quoted every
  site, every coupled test and the acceptance test's seams at 0d1d004, the engine tree is
  unchanged since 69e9db8, and the change is two imports, four re-raise pairs, one test's
  exception and docstring, and one new test file. The brief re-reads the four clauses, the
  pinning test, the send, the store's re-raise and run_arm's close at HEAD before the first edit
  and stops on any difference from the read-out. No model call, no provider call, no API key
  read, no live write; the standard gate by the wrapper against 837–1021 s. This ruling states
  no standing rule: the id list, the glosses file and STANDING_RULES.md are untouched at item 0,
  asserted by content.
===== END r792.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ae-U27B] — Land U27 (C56's cloud half, handlers 8–11 by the
  12ae_U27A.md mapping) as R792(b) and (e) rule it: the fail-first acceptance test, the
  four re-raise pairs with their imports, the pinning test moved to a paper outcome, one
  engine commit with its gate, evidence in its own commit. Item 0: R792 as the next block;
  the INT-ae12-2 row; this brief committed as pasted; no standing rule.
PURPOSE: Let a run fault leave the two cloud transports' retry loops and store_result
  handlers as itself, so that a database fault after a paid provider answer closes the run
  `failed` with its reason instead of being re-sent twice or discarded while the manifest
  reads `completed`. Behind R71 on live; the fix is for the day an arm is enabled. Code
  task. No live write. No model call. No provider call. No API key read.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  Touched: engine/cloud/openai_extractor.py and engine/cloud/anthropic_extractor.py (one
  import and two re-raise pairs each); tests/test_cloud_extraction.py (one test's injected
  exception and its class docstring); tests/test_u27_cloud_run_faults.py (new);
  docs/inventory/entry_points.{json,md} only if `inventory --write` changes them
  (R792(f)); docs/plan/ENGINE_REFACTOR_PLAN.md (Block 190; the INT-ae12-2 row);
  docs/session-reports/session-12/12ae_open/r792.txt; a new directory
  docs/session-reports/session-12/12ae_U27B/ (brief_12ae_U27B.md at item 0;
  b0_parent_acceptance.txt, gate_u27b/ and the pin recompute at step 3);
  ~/scratch/12ae-u27b/; the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): any other file under engine/ (base.py
  included), tests/, scripts/ (run_cloud_extraction.py included — R792(c)), analysis/;
  review_specs/ (R71: enabled_arms stays empty on disk; the test's spec copy lives under
  the test's temp directory); the codebook; engine/core/extraction_events.py's tuple;
  the tools; the id list, the glosses file and STANDING_RULES.md (not one byte — R792
  states no rule); review.db (mode=ro instruments only); any .env or API key (never read:
  the fakes replace the SDK classes and the subclass passes a placeholder key; a test
  that would construct a real client is a STOP); the working tree inside 07:00–10:35 UTC
  (R86); any scratch directory other than ~/scratch/12ae-u27b/.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ae-U27A report; R792): HEAD 0d1d004e69872e3521c3b57c5fbe12868482cb13, parent
    dd1b0b508cbf909c1635bf6cb434754e11ab51e6, clean, level; the engine tree equal to
    69e9db8's; plan ends "…ends at R791.", last block 189; STANDING_RULES.md 87 / 8 / 95,
    glosses 43, the id list's last A line "A R784(g)"; claude-config at 0eddaf6 or an
    accepted descendant (R663(b)); gate reference U26's (12ae_U26B/gate_u26b/: 3,525
    passed / 1 xfailed / 17 deselected on 3,543 / 3,526; its collected_ids.txt and
    durations_12ae_u26b.txt); band 837–1021 s (R783(b)); pins 12ae_U26B/pin_tuples_u26b.json.
    Architect's hash of r792.txt: Section 1.
  READ (12ae_U27A/12ae_U27A.md at 0d1d004: the I2 mapping, §2's clauses, §3's send, store
    and close, §4's pinning test and §5's probe seams — not repeated; step 2a re-reads each
    at HEAD).
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 69e9db8. I2. The four clauses
    read at HEAD as the mapping quotes them at openai_extractor.py:179 and :209,
    anthropic_extractor.py:205 and :237, each `except Exception as exc:`; extractor.py:438–440
    shows U02's form; base.py:141 `if self.run_id is not None:`; base.py:401's re-raise;
    run_cloud_extraction.py:112–127's close as quoted; the pinning test's raise at
    tests/test_cloud_extraction.py:840 as quoted.
  INFERRED — expectation: E1. Six of the twelve parametrized cases are red on the parent
    (every tuple-member case), the six others green; E2. The new ids are 12 (or the file's
    count, read); the gate collects 3,543 + N and selects 3,526 + N, only-gate exactly
    those; the moved pinning test keeps its id and stays green; no other test moves (the
    read-out's §4); E3. `inventory --write` changes nothing (no entry point moves); if it
    does, the drift is regenerated under R786(b)'s form and quoted.

CONTRACT (steps, in order; evidence under ~/scratch/12ae-u27b/):

  STEP 0 — Pre-check (read-only). date -u (outside 07:00–10:35 UTC; the gate must exit
    before 06:30 UTC or launch after 10:35). HEAD and parent proved by git cat-file -e
    <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. Plan: last line "…ends at
    R791.", last block 189, no Block 190. Rules files: the three counts above.
    claude-config: 0eddaf6 or an accepted descendant, clean, level. I1. Fingerprint
    --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12ae-U24B's item 0
    — a block, one row, the brief file — ~/scratch/12ae-u27a/check_item0_u27a.py and its
    chain, quoted before composing).
    1a. 12ae_open/r792.txt from Section 1; hash straight after the write: EXPECTED
        d4c263838979181a54a926d2743a328c716258a8cf0ebdd3f030315977553762, 11130, 126.
        Mismatch = STOP.
    1b. block190.json in block189.json's key set: tag R792; item "item 0 of the
        12ae-U27B brief"; beside = "the row INT-ae12-2 and the brief file it orders"
        (composed; quoted); made_date 2026-11-09; session 12ae; report = the 12ae-U27A
        final report (item 0 dd1b0b5; read-out 0d1d004), final-report form; sentence
        model, followed "12ae-U27B brief"; prev_ends R791. Render to stdout, compare with
        Block 189's order, then --append ("−" EXPECTED 0 for the append, read at 1e).
    1c. The row (R792(g)): one line appended to section L's table ("L (12ae additions,
        R785)", after the INT-ae12-1 row, in that row's column form): id INT-ae12-2; text
        composed from R792(g)'s words with the quoted anchors — the retry loops at
        openai_extractor.py:162–179 and anthropic_extractor.py:184–205 re-send on
        BadRequestError, NotFoundError and PermissionDeniedError as on a transient error
        (three paid attempts against a request that cannot succeed); only
        AuthenticationError stops the loop (:157, :179); behind R71, no stored value wrong;
        class 3; P7; R792(g); source "`12ae_U27A.md` §1 2e question 3 and
        `12f_C56-A.md` #25". The section's lead paragraph gains one sentence naming the
        second row and its date, in its form. One line inserted, one line replaced (the
        lead); the "−" count read and matched to the lead. No note on C56 or HK-ARCH; no
        dated units line (R792(g)).
    1d. The brief file (R784(g)(v)): 12ae_U27B/brief_12ae_U27B.md from this whole pasted
        block, "CLAUDE CODE" through the last HANDBACK line, one trailing newline, written
        once; sha256, bytes, lines straight after the write, reported.
    1e. The item-0 check in 12ae-U27A's content form, adapted: --check-plan
        byte-identical; the plan's numstat read ("−" matched to the lead paragraph); every
        earlier "ends at" line kept; last line "…ends at R792." once; the plan equals
        HEAD's plus the row inserted, the lead replaced and Block 190 appended, nothing
        else (computed); r792.txt equal to 1a; the three rules files unchanged and
        unstaged; the brief file hashed; the staged set exactly the plan, r792.txt and the
        brief file. Evidence by redirect; quote the check.
    1f. <check> && <commit>, one line; message from the staged set (R778(d)), docs scope.
        Fingerprint --compare before the push; push; level; --compare after (IDENTICAL).
        Ledger row with trailers; push; level. Record hashes and parents.

  STEP 2 — The build (one engine commit).
    2a. Re-read at HEAD and quote: the four clauses with their bodies (I2, by the mapping
        table's lines); the two constructors (openai_extractor.py:31–36,
        anthropic_extractor.py:32–37) and the two `_create` call sites; base.py:134–149
        (send) and :362–403 (store_result, its re-raise at :401); extractor.py:438–440
        (U02's form) and the import it uses; run_cloud_extraction.py:97–127 (run_arm, the
        EXTRACTORS dict at :39, the close); rm.failure_reason's docstring at
        run_manifest.py:884–; the pinning test whole (tests/test_cloud_extraction.py:800–850,
        the class docstring and the raise); fake_probe_u27a.py's seams (the scratch
        database and seeding, the spec copy, the subclass, the fakes, the recorder patch,
        the connection wrap, the sleep and network recorders). Any difference from the
        read-out = STOP.
    2b. Write tests/test_u27_cloud_run_faults.py, fail-first (R792(e)):
        test_run_faults_leave_the_cloud_handlers_as_themselves, parametrized as R792(e)
        lists — twelve cases, each id naming the handler, the route and the exception —
        in the probe's seams, driven through run_arm; no server, no provider, no key, no
        gitignored fixture; the conftest fence on. Tuple-member cases: identity on the
        propagated exception; sends == 1; no cloud_extractions row for the arm; run_arm's
        manifest row `failed` with rm.failure_reason(exc). KeyError and APIConnectionError
        cases: the §5 outcome asserted per handler. The fakes' record asserted at the end
        of each case (placeholder key only; httpx send 0; socket connect 0). The docstring
        states the seams and why 8–9 are driven under a run (F3). Run on the parent:
        EXPECTED E1 — 6 red at the identity assertion, 6 green; output to
        b0_parent_acceptance.txt. A tuple-member case green on the parent = STOP.
    2c. Edit the two transports, R792(b) exactly: `from engine.core.extraction_events import
        is_run_fault` in each (placed as extractor.py places its import, quoted);
        `if is_run_fault(exc): raise` as the first two lines of the handler body at :179
        and :209 (openai) and at :205 and :237 (anthropic). No other engine line changes;
        base.py untouched. Edit tests/test_cloud_extraction.py, R792(e) exactly: at :840
        `raise sqlite3.IntegrityError("simulated constraint violation")` becomes
        `raise ValueError("simulated 0-span refusal")`; the class docstring at :800 gains
        "a paper-outcome" before "store_result failures" (the two "−" lines of that file,
        and nothing else; if the `sqlite3` import is then unused in that file it stays —
        no other line changes). git diff --stat EXPECTED: the two transports (+5 / −0
        each), test_cloud_extraction.py (+2 / −2), the new test — four files; plus the two
        inventory files only under R786(b)'s form (E3: `inventory --write`, then git diff
        --stat; an empty diff is the expected case and is quoted as such; a non-empty diff
        is quoted whole, EXPECTED line moves only, no entry-point change).
    2d. Pins: 12l_U04/pin_tuples.py with both arguments, byte-identical to
        12ae_U26B/pin_tuples_u26b.json; the audit prompt_hash unchanged. A moved value =
        STOP.
    2e. Selected runs green: the new file; tests/test_cloud_extraction.py whole (36 ids,
        the fixture present by ls — a reading); tests/test_failed_close_reason.py,
        tests/test_codebook_review_at_open.py, tests/test_request_capture.py and
        tests/test_run_manifest.py whole (the census's cloud-coupled files). Commit the
        engine change (fix scope; the message names C56/U27, the four handlers by the
        mapping, the pinning test's move, and the inventory if regenerated), from the
        staged set. Do not push before the gate.
    2f. The standard gate by launch_gate.sh: evidence_dir ~/scratch/12ae-u27b/gate_u27b/,
        session name, ref_ids 12ae_U26B/gate_u26b/collected_ids.txt, ref_durations
        12ae_U26B/gate_u26b/durations_12ae_u26b.txt, no selection. Ollama reads ≥5 s
        before and after (R660(b)). EXPECTED: 3,525 + N passed / 1 xfailed / 17 deselected
        on 3,543 + N / 3,526 + N (E2; N read from the new file's collection), only-gate the
        N new ids, only-ref 0; wall inside 837–1021 s; no phase >10 s slower; no model
        load in the window (R696(b)); the nineteen files. A red: class (c) per
        R746(c)/R772(b), re-run once; any other red = STOP, the engine commit standing.
    2g. Fingerprint --compare; push; level; --compare after (IDENTICAL). Ledger row; push;
        level.

  STEP 3 — Evidence commit (R731(c)). 12ae_U27B/: b0_parent_acceptance.txt, gate_u27b/
    (the nineteen files), pin_tuples_u27b.json; the staged set exactly those, every sha256
    equal to its scratch source; chained check && commit; fingerprint IDENTICAL before and
    after the push; level; ledger. Record hashes and parents.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 and I2 held.
  G2. Item 0 one commit: r792.txt at the architect's hash; Block 190 by the tool; the row
      and the lead sentence in the record's form, the plan equal to HEAD's plus exactly
      those edits and the block; the three rules files untouched by content; the brief
      file hashed; one chain; pushed, level, fingerprint IDENTICAL; ledger.
  G3. The six tuple-member cases red on the parent and green in the gate; the six others
      green on both; the diff exactly four files plus the inventory under R786(b)'s form;
      base.py and scripts/ untouched; pins and audit prompt_hash unchanged; the gate at the
      E2 counts inside the band, no load, nineteen files; the fakes' record in the test
      output showing zero outbound requests; engine commit pushed, level, fingerprint
      IDENTICAL; ledger.
  G4. The evidence commit as step 3 states; pushed, level, fingerprint IDENTICAL; ledger.

OUT OF SCOPE: C56's CLOSED note (it closes at the next item 0, after this gate, when U02,
  U26 and U27 have passed — R542, R792(g)); U28 and U29 (Wave 2 rows 25, 27, 28; R792(d)
  orders U27 first); the tuple; the cloud close's status (R792(c)); INT-ae12-2's fix
  (P7); the stage-key question at 12f_C56-A.md:717; any other edit under engine/, tests/,
  scripts/, analysis/, docs/architecture/ or CLAUDE.md; the tools items; the 2026-11-09
  job reads (the next open's); /wrap.

CONSTRAINTS (task-only): no model call, no provider call, no API key read, no restart, no
  live write; no edit in the working tree between 07:00 and 10:35 UTC; a count arrived at
  by eye is INFERRED, the computed value given with its command; where this brief
  describes a tool or record, the record governs and a difference is a finding (R732(a));
  a handler is named only by the I2 mapping.

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — metadata, render comparison, the
  row and the lead before and after, the brief file's hash, the check verbatim, chain,
  hashes, fingerprint, ledger; (2) the build — 2a's quotes, the test as written,
  b0_parent_acceptance.txt verbatim, the four insertions and the two test lines as a
  diff, the diff --stat, the inventory diff (empty or regenerated), the pin recompute, the
  selected runs, the engine commit hash and parent; (3) the gate — launch line,
  timestamps, wall vs 837–1021, the summary line verbatim, collected/selected, the id
  comparison, durations >10 s or none, basetemp, journal window, Ollama reads, concurrent
  jobs, the nineteen files; (4) the evidence commit; (5) contradictions and findings,
  numbered, each with its quoted line; (6) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 or I2 false, a 1a mismatch, or a check
  failure or crash at 1e = STOP before the item-0 commit. A tuple-member case green on the
  parent, a diff outside the four files (inventory excepted under R786(b)'s form), a test
  that would construct a real SDK client, or a moved pin = STOP before the engine commit.
  A non-(c) red in the gate = STOP, the engine commit standing and reported. An
  expectation mismatch (E1–E3) is reported and the work continues. A spec premise that
  does not survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not start U28 or close C56 without explicit sign-off. Await
  the next brief or ruling in this session. STOP-12AE-U27B
