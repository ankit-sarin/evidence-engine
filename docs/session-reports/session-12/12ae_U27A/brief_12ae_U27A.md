CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R791, PI ruling 2026-11-09 UTC; not on disk)
================================================================================

R791 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r791.txt: sha256 680f76c58b6c561632a02aa056e39d279961fba99302ae0e028447f3fb03cfc6,
            5401 bytes, 64 lines, one trailing newline.

===== BEGIN r791.txt =====
RULING [evidence-engine 12ae-R791] — 12ae-U26B accepted: U26 landed, C56's parse half delivered
  (the row stays open for U27); the series gains 945.63 s; readings, two the architect's; U27's
  Phase A opens
STATUS: PI ruling, 2026-11-09 UTC, session 12ae, on architect recommendation, on the 12ae-U26B
  final report (item 0 e89d602; engine 69e9db81aa7bfba8c9cb119bcbb6e02206cbe2c7; evidence
  5895e8c9e86c978c81fc59d5525d62f09125d3f8). Transcribe at item 0 of the 12ae-U27A brief below,
  by transcribe_ruling.py, in the block after R790's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the report):
  MEASURED: "12ae_U26B/brief_12ae_U26B.md is be04f404ea7dea5cbbb26d736f028ee6a92079aa38a5410947e6c79cceaf7286,
    23,242 B, 306 lines" (the architect's copy: equal); "Plan +107 / −2; the "−" lines are
    exactly C56 and HK-ARCH"; "24 cases: six handlers × {sqlite3.OperationalError,
    ConnectionError, UndeclaredCall, KeyError}"; "18 failed, 6 passed in 18.26s"; "Each red fails
    at "handler N: X did not leave parse_all_pdfs as itself: None""; "pdf_parser.py 15 / 2, the
    new test 199 / 0, entry_points.json 4 / 4 and entry_points.md 2 / 2"; "pdf_parser.py's "−"
    lines are exactly the two clauses"; "pin_tuples_u26b.json … cmp byte-identical to
    12ae_U25B/pin_tuples_u25b.json"; "211 passed, 6 deselected"; "69e9db81aa7bfba8c9cb119bcbb6e02206cbe2c7,
    parent e89d602"; "Wall 945.63 s"; "==== 3525 passed, 17 deselected, 1 xfailed, 2 warnings in
    943.58s (0:15:43) ===="; "collected 3543 items / 17 deselected / 3526 selected"; "only-gate 24
    (exactly the U26 ids), only-ref 0"; "-- No entries --"; "surgical-cv uvicorn (pid 1017068,
    since 02:15)"; "all nineteen present"; "5895e8c9e86c978c81fc59d5525d62f09125d3f8, parent
    69e9db8"; "21 files, all added"; "Ledger cbab7fe … 99a8cb2 … 51ff0e8".
  READ (CC's): findings 1–5; ":520 and :525 read except Exception:. … U02's form needs a name,
    so both clauses became except Exception as exc:"; "the test raises RuntimeError("insert
    refused") at the INSERT, which handler 12 converts, and the parametrized exception at the
    rollback, which is handler 13's own trigger"; "--check printed one entry's flags drift (1267,
    1269). --write also moved 1273 → 1286 in the same entry's lock line".
  INFERRED: none.

R791(a) — The 12ae-U26B final report is accepted as reported: item 0 e89d602 (Block 188; the C56
  and L07-C4 notes in one row; the HK-ARCH note; the brief file byte-equal to the architect's
  copy, the seventh match of seven), the engine commit 69e9db8 (four files; the import, six
  re-raise pairs, the two clause names; the inventory regenerated under R786(b)'s form, line
  moves only), the gate 3,525 / 1 / 17 on 3,543 / 3,526 at 945.63 s inside 837–1021 s, only-gate
  the 24 U26 ids, no load, the nineteen files, green at the first run; the evidence commit
  5895e8c; ledgers cbab7fe, 99a8cb2 and 51ff0e8 with trailers. Both pins and the audit
  prompt_hash byte-identical. U26 is landed; P1's fix units 37, done 29. The acceptance test
  tests/test_u26_parse_run_faults.py::test_run_faults_leave_the_parse_handlers_as_themselves
  (18 run-fault cases red on the parent e89d602 in 12ae_U26B/b0_parent_acceptance.txt, green in
  12ae_U26B/gate_u26b/; 6 KeyError cases green on both) is C56's parse-half acceptance. C56
  stays OPEN until U27's handlers 8–11 land (R542); its note at the next item 0 records U26's
  landing and this test, not a close.
R791(b) — The series on this host state gains 945.63 s: thirty-five. The band stays 837–1021 s
  (R783(b)).
R791(c) — Readings, no row: finding 1 (handlers 12–13's clauses gained `as exc` — U02's form
  needs the name; the brief's "No other line changes" and its binding clause written for :901
  were the architect's, and CC read the ruling's form over the brief's wording, as R732(a)
  directs; the two "−" lines are those clauses and nothing else); finding 2 (handler 13's seam:
  a non-fault at the INSERT and the parametrized exception at the rollback, so 13 is reached as
  itself — the test's docstring states it; accepted); finding 3 (every case through
  parse_all_pdfs with an identity assertion, which also shows 4–5's fault is not wrapped —
  stronger than the ruling asked; accepted); finding 4 (inventory --check reports one entry's
  flags drift where --write moves three lines — a recorded limit of the inventory tool, joining
  the tools list for the next tools change beside R752(c), R765(d), R766(d), R784(d)); finding 5
  (E1–E3 held). The regenerated inventory's generated-at stamp names the parent, the tool's
  form (R787(d)).
R791(d) — U27's Phase A opens: 12l_P1A.md §3's U27, "Run-fault predicate at the cloud handlers
  (P1's tail)" (C56, handlers 8–11 in 12f_triage.md's numbering: the two transports' retry loops
  and their `store_result` handlers; R542, R612(g)), read-only, in 12ae_U26A.md's form (the I2
  mapping first), its entry quoted first. The cloud path is behind R71 (`enabled_arms` empty on
  live) and unreachable on Run 7; no provider is called — the fakes stand in for the SDK clients,
  and no API key is read or needed. No engine, test, script, spec or codebook edit; no model
  call; no live write. C56 closes at the item 0 after U27's gate, when U02, U26 and U27 have
  passed (the entry's note). This ruling states no standing rule.
===== END r791.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ae-U27A] — U27's Phase A read-out (C56's cloud handlers 8–11:
  the OpenAI and Anthropic transports' retry loops and their store_result handlers).
  Item 0: R791 as the next block; the C56 note (U26 landed, row open); the dated units
  line; this brief committed as pasted; no standing rule. Read-only on engine/ and
  tests/; STOP before any edit.
PURPOSE: Put on disk, before the design, what each cloud handler catches and converts
  today, which of U02's run-fault classes can reach it and by what route (the recorder,
  the store, the event writer — not the provider's own errors), what the paper, the
  manifest and the arm's stats record when one does, which tests pin the converting
  behaviour, and whether the acceptance test can be written at HEAD and fails with no
  provider called. Mixed: one docs commit (item 0), one docs commit (the read-out). No
  engine, test, script, spec or codebook change. No live write. No model call. No
  provider call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 189; C56's note; the dated units
  line); docs/session-reports/session-12/12ae_open/r791.txt; a new directory
  docs/session-reports/session-12/12ae_U27A/ (brief_12ae_U27A.md at item 0; the read-out,
  census and probe scripts with outputs at step 2); ~/scratch/12ae-u27a/; the
  claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/ (R71: enabled_arms stays empty), the codebook, docs/architecture/,
  CLAUDE.md, docs/inventory/, the tools; the id list, the glosses file and
  STANDING_RULES.md (not one byte — R791 states no rule); review.db (mode=ro instruments
  only; the probe uses a scratch database); any .env or API key (never read, never
  needed: the fakes replace the SDK clients; a probe that would reach a provider is a
  STOP); the network (no provider request leaves the box — assert by the fake's call
  record, not by trust); any scratch directory other than ~/scratch/12ae-u27a/; the
  working tree inside 07:00–10:35 UTC (R86).

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ae-U26B report; R791): HEAD 5895e8c9e86c978c81fc59d5525d62f09125d3f8, parent
    69e9db81aa7bfba8c9cb119bcbb6e02206cbe2c7, clean, level; the engine tree equal to
    69e9db8's; plan ends "…ends at R790.", last block 188; STANDING_RULES.md 87 / 8 / 95,
    glosses 43, the id list's last A line "A R784(g)"; claude-config at 51ff0e8 or an
    accepted descendant (R663(b)). Architect's hash of r791.txt: Section 1.
  READ (the clone at 5895e8c): R1. 12l_P1A.md §3's U27 entry, whole: "### U27 — Run-fault
    predicate at the cloud handlers (P1's tail) / Rows: C56 / Pin effect: not affecting /
    Files: engine/cloud/openai_extractor.py, engine/cloud/anthropic_extractor.py / Depends
    on: U02 / Acceptance test (synthetic; discharges R542):
    test_run_faults_leave_the_cloud_handlers_as_themselves — The predicate's classes raised
    in either transport's retry loop or around store_result propagate and the paper stays
    pending. Fails on the unmodified tree. / Note: Handlers 8–11, Class 1 by R542. Not
    reachable on live while R71 holds. The C56 row closes when U02, U26 and U27 pass."
    R2. 12f_triage.md's converting-handler table rows 8–9 ("OpenAIExtractor.run /
    AnthropicExtractor.run — the retry loop | A record_call sqlite3 error after the
    provider answered is re-sent twice, then counted as a failed paper; the manifest
    closes completed | 1") and 10–11 ("The same two classes — around store_result | A
    database fault becomes a failed-paper count; the paid result is discarded | 2,
    leaning 1"); 12f_C56-A.md's census rows for them (the I2 mapping, as 12ae_U26A.md built
    it). R3. grep -n "except Exception" engine/cloud/ at 5895e8c: openai_extractor.py:179
    and :209; anthropic_extractor.py:205 and :237; base.py:308 and :401 (anchors only
    beside the quoted line at 2b; base.py's two are classified at 2b, not assumed U27's).
    R4. The run-fault tuple at extraction_events.py:468–474 as 12ae_U26A.md quotes it;
    U02's form at extractor.py:438–440 and U26's at pdf_parser.py (69e9db8). R5.
    12ae_U26A/12ae_U26A.md is the form (§0–§9, the I2 mapping first). R6. C56's row
    carries the Wave 2 notes on rows 25, 27 and 28 (L12-C1, -C3, -C4: the cloud path's
    collapse gate, citation check and run_calls recording) — adjacent, P1 U28/U29, not
    U27's; quoted at 2a as overlap, changed by nothing here.
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 69e9db8. I2. The handler
    numbering is reconciled on disk before any handler is named: each of the triage
    table's handlers 8–11 mapped to its 12f_C56-A.md census row and to its except clause
    at HEAD by quoted line, in a table — a handler that cannot be mapped is a STOP. I3.
    The two SDK clients can be replaced by fakes at the seam the extractors construct
    them (read the constructor and the call site; quote), so no provider is reachable
    from the probe — if the construction cannot be intercepted without a key, the probe
    is not run and that is a STOP with the finding.
  INFERRED — expectation: E1. The census finds the tests pinning the converting behaviour
    (tests/test_cloud_extraction.py's module fixture and skips, B14's row, among them);
    E2. A fake client that answers and a fault injected at record_call or store_result
    shows, at HEAD, the paper counted failed and the manifest closing completed
    (rows 8–9: re-sent twice first); E3. Neither transport is reachable from run_pipeline
    while enabled_arms is empty (R71) — the entry point is scripts/run_cloud_extraction.py
    only, quoted.

CONTEXT: U26 landed at 69e9db8 (R791). U27 is C56's cloud half, the last of the row's
  three units (U02, U26, U27). The cloud path is unreachable on live (R71) and on Run 7;
  the handlers are Class 1 because, once an arm is enabled, a store fault after a paid
  provider answer discards the answer and the run reads as completed. Phase A of the
  two-phase form; the design ruling follows; one build brief if Phase A is complete
  (R785(h)'s form).

CONTRACT (steps, in order; evidence under ~/scratch/12ae-u27a/):

  STEP 0 — Pre-check (read-only). date -u (outside 07:00–10:35 UTC). HEAD and parent
    proved by git cat-file -e <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree.
    Plan: last line "…ends at R790.", last block 188, no Block 189. Rules files: the three
    counts above. claude-config: 51ff0e8 or an accepted descendant, clean, level. I1.
    Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12ae-U26A's item
    0 — a block, a note, a dated line, the brief file — ~/scratch/12ae-u26a/check_item0_u26a.py
    and its chain, quoted before composing).
    1a. 12ae_open/r791.txt from Section 1; hash straight after the write: EXPECTED
        680f76c58b6c561632a02aa056e39d279961fba99302ae0e028447f3fb03cfc6, 5401, 64.
        Mismatch = STOP.
    1b. block189.json in block188.json's key set: tag R791; item "item 0 of the
        12ae-U27A brief"; beside = "the C56 note, the dated units line and the brief file
        it orders" (composed; quoted); made_date 2026-11-09; session 12ae; report = the
        12ae-U26B final report (item 0 e89d602; engine 69e9db8; evidence 5895e8c),
        final-report form; sentence model, followed "12ae-U27A brief"; prev_ends R790.
        Render to stdout, compare with Block 188's order, then --append ("−" EXPECTED 0
        for the append, read at 1e).
    1c. C56's note (R791(a)) at the end of its status cell, "*Note 2026-11-09 (12ae-U27A
        item 0, R791(a)):*" — U26 landed at 69e9db8; handlers 4–7 and 12–13 re-raise a run
        fault in U02's form; the acceptance test named with its b0 and gate paths; the row
        stays OPEN for U27 (R542); the tools-list reading of R791(c) finding 4 is not a
        note here (it is the tools change's). The dated units line after 12ae-U26A's, in
        its form: U26 landed at 69e9db8; C56's parse half delivered, the row open for U27;
        fix units 37, done 29 (U01–U26, U37, U38, U39); rows 284 (94 / 56 / 134, 0
        unclassed) unchanged; rows out 0. One row replaced, one line inserted; the "−"
        count read and matched to the one row.
    1d. The brief file (R784(g)(v)): 12ae_U27A/brief_12ae_U27A.md from this whole pasted
        block, "CLAUDE CODE" through the last HANDBACK line, one trailing newline, written
        once; sha256, bytes, lines straight after the write, reported.
    1e. The item-0 check in 12ae-U26A's content form, adapted: --check-plan
        byte-identical; the plan's numstat read ("−" matched to the one row); every
        earlier "ends at" line kept; last line "…ends at R791." once; the plan equals
        HEAD's plus the row replaced, the line inserted and Block 189 appended, nothing
        else (computed); r791.txt equal to 1a; the three rules files unchanged and
        unstaged; the brief file hashed; the staged set exactly the plan, r791.txt and the
        brief file. Evidence by redirect; quote the check.
    1f. <check> && <commit>, one line; message from the staged set (R778(d)), docs scope.
        Fingerprint --compare before the push; push; level; --compare after (IDENTICAL).
        Ledger row with trailers; push; level. Record hashes and parents.

  STEP 2 — The read-out (read-only; no server; no provider; no live database; a scratch
    ReviewDatabase for the probe; the fake clients' call records committed as proof that
    nothing left the box).
    2a. §1: the U27 entry with its neighbours (R1); row C56 whole with every dated note
        (quote the notes naming handlers 8–11, U27, R542, R612(g), and the Wave 2 rows 25,
        27, 28 as overlap, R6); 12f_triage.md's rows 8–9 and 10–11 and its C56 row;
        12f_C56-A.md's census rows for them (I2's mapping table, built here); the tuple at
        HEAD and U02's and U26's forms, quoted; B14's row (test_cloud_extraction.py's
        gitignored database fixture) and R71's block (enabled_arms empty); every CLAUDE.md
        and docs/architecture line on the cloud path's error handling; §1 2e: the
        questions the inputs raise that Phase A does not answer.
    2b. §2 Sites: every broad except under engine/cloud/ (R3), path:line, the clause and
        body quoted, classified: a U27 handler (8–11 by the mapping), a handler in U27's
        files the triage table did not number (reported, not changed), or outside U27
        (base.py's, if not mapped). For each U27 handler: what it catches, what it does
        (re-sends? counts a failed paper? logs? discards the provider's answer?), what it
        records (run_calls? a paper event? the arm's stats?), and whether it re-raises
        anything today. The SDK client's construction and the call site (I3), quoted.
    2c. §3 Mechanism (CODE ORDER unless FAKE): for each U27 handler, the route by which a
        tuple member can reach it — record_call's recorder (sqlite3.Error), the store
        (store_result's INSERTs into cloud_extractions / cloud_evidence_spans, C28), the
        event writer if the cloud path writes events (EventRefused), UndeclaredCall /
        UndeclaredOverride if the cloud path goes through check_declared_call (read it —
        it may not), a connection error from the provider (ConnectionError /
        httpx.Connect* are tuple members: state whether a provider outage is therefore a
        run fault by the tuple's definition, and what that means for the retry loop —
        this is the design question Phase A surfaces, not answers); what the paper, the
        manifest (end_status), the arm's stats and the distribution gate (B9/U28 overlap)
        record under each; the "re-sent twice" of rows 8–9: quote the loop and count the
        sends a fake observes under a recorder fault. FAKE: a fake client answering a
        fixed response, the fault injected at the recorder or the store; the fake's call
        record committed.
    2d. §4 Census: tests coupled to the two extractors, base.py's shared methods, the
        retry loop and store_result — census_u27a.py in census_u26a.py's form (declared
        completeness as stated there), classes of R722(a), R723(c), R724(a), R725(b); the
        pinning tests found and quoted (E1), each with what a re-raise would do to it;
        B14's skips: how many of test_cloud_extraction.py's ids run on this box (the
        gitignored fixture present or absent, by ls, not opened) — a reading.
    2e. §5 The acceptance test against HEAD (FAKE, fake_probe_u27a.py → .txt): for each
        U27 handler, a fake raising one tuple member and KeyError; at HEAD both converted
        (EXPECTED per row: re-sent then failed-paper for 8–9; failed-paper, answer
        discarded for 10–11); the test the entry names can be written iff each handler is
        reached by a fake with no provider and the conversion observed in the record. The
        probe asserts the fake's outbound call count and that no network client was
        constructed with a real key (the fake records the constructor arguments).
    2f. §6 Cases: each tuple member at each handler; KeyError; a provider ConnectionError
        (the tuple question of 2c); under an active run and outside one; with the
        gitignored fixture present and absent.
    2g. §7 Options for the ruling: A — U02's form at the four handlers, `if
        is_run_fault(exc): raise` first (for 8–9, before the re-send decision; for 10–11,
        before the failed-paper count), the run closing through the entry point's
        existing close (read scripts/run_cloud_extraction.py's close and quote it — if it
        has none, say so: that is C47's family and a finding); B — A, with provider
        connection errors carved out of the re-raise at 8–9 so the retry loop keeps
        retrying a transient outage (state how, without touching the tuple — U02's
        contract); C — A plus closing the manifest 'aborted' at the entry point if it does
        not today. For each: sites, test rewrites, the acceptance test's shape, what it
        does not fix. Classified sites (3g). §8 findings; §9 limits.
    2h. Commit the read-out (12ae_U27A/12ae_U27A.md), census_u27a.py/.txt,
        fake_probe_u27a.py/.txt (the fake's call record inside): the staged set exactly
        those, nothing under engine/, tests/ or data/ (asserted); fingerprint IDENTICAL
        before and after the push; level; the ledger row. Record hashes.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 held.
  G2. Item 0 one commit: r791.txt at the architect's hash; Block 189 by the tool; the C56
      note and the dated line in the record's form, the plan equal to HEAD's plus exactly
      those edits and the block; the three rules files untouched by content; the brief
      file hashed; one chain; pushed, level, fingerprint IDENTICAL; ledger.
  G3. I2's mapping table present before any handler is named; I3 shown by the quoted
      constructor and the fake's record; the read-out in R5's form with every section
      present; every code claim beside a quoted line with path and line read at HEAD;
      CODE ORDER / FAKE labelled; the census and probe instruments and outputs committed
      with their declared completeness; the fake's call record showing zero outbound
      requests and no real key; the Ollama journal for the probe window empty; no file
      outside 12ae_U27A/ in the commit; pushed, level, fingerprint IDENTICAL; ledger.

OUT OF SCOPE: U27's design and build; U28 and U29 (the cloud collapse gate, citation
  check and run_calls recording — Wave 2 rows 25, 27, 28); C28's store_result INSERTs;
  the tuple (U02's contract); R71 (enabled_arms stays empty); any edit under engine/,
  tests/, scripts/, analysis/, docs/architecture/ or CLAUDE.md; the tools items; the
  2026-11-09 job reads (the next open's); /wrap.

CONSTRAINTS (task-only): no model call, no provider call, no restart, no live write, no
  API key read; the probe's fake records every constructor and call and the record is
  committed; a count arrived at by eye is INFERRED, the computed value given with its
  command; where this brief describes a tool or record, the record governs and a
  difference is a finding (R732(a)); a handler is named only through I2's mapping.

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — metadata, render comparison, the
  row and the dated line before and after, the brief file's hash, the check verbatim,
  chain, hashes, fingerprint, ledger; (2) the read-out's §0–§9 in full as committed, the
  mapping table first, the fake's call record quoted; (3) contradictions and findings,
  numbered, each with its quoted line; (4) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 false, a 1a mismatch, or a check failure or
  crash at 1e = STOP before the item-0 commit. A handler that I2 cannot map, or a client
  construction I3 cannot intercept without a key = STOP before the probe. An expectation
  mismatch (E1–E3) is reported and the read-out continues. A spec premise that does not
  survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not start U27's design or build without explicit
  sign-off. Await the next brief or ruling in this session. STOP-12AE-U27A
