CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R820, PI ruling 2026-11-20 UTC; not on disk)
================================================================================

R820 is transcribed at item 0 of the brief below, by
docs/session-reports/tools/transcribe_ruling.py, as the next block of the decision log
(Block 218). The lines between the two marker lines, exclusive of the markers, are the
byte content of r820.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's
independent hash:
  r820.txt: sha256 1b1ee4a45d81b756278ebc1b64a39eaf07158601f2f62b3d1cff3c784fbd76ab,
            6577 bytes, 82 lines, one trailing newline.

===== BEGIN r820.txt =====
RULING [evidence-engine 12ai-R820] — 12ai-U41B accepted; U41 landed; the two foreign-lock tests
  classed coupled, on the PI's word; INT-ai12-1 closes at the next item 0; the gate's 963.33 s
  enters the series; readings on the findings; U30's build with N46 next; no standing rule
STATUS: PI ruling, 2026-11-20 UTC, session 12ai, on architect recommendation, on the 12ai-U41B
  final report (item 0 ee20dbab3b7ec37c31b9ca44a3ac8afea47131de; engine
  339370dd92a766ff45ad7abc44e7ec2faab1362b; evidence
  03abfaf5b16b577d0a089338c787a988db7e6fa5). Transcribe at item 0 of the 12ai-U30B brief
  below, by transcribe_ruling.py, in the block after R819's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12ai-U41B final report, quoted; wrapped lines joined): "Commit:
    ee20dbab3b7ec37c31b9ca44a3ac8afea47131de (parent 483969d), +839 −0 over 3 files."; "Ledger:
    ef77495 (parent a84a03e)."; "r819.txt: b962a519…03e6, 10,574 B, 130 lines. Equal to the
    architect's hash."; "Brief file:
    f981a4516f61ed8529a06e189c480c58eab4c542c8405b922c6090bbb8fb8849, 41,998 B, 559 lines."
    (the architect's copy: the same); "339370dd92a766ff45ad7abc44e7ec2faab1362b (parent
    ee20dbab3b7ec37c31b9ca44a3ac8afea47131de)"; "On the parent, 17 are red and 3 green (cases
    4, 10c, 10d), which is E1."; "After the edits all 20 pass."; "pin_tuples_u41b.json is
    byte-identical to 12ah_close/pin_tuples_close.json"; "Live spec_hash: 9b18f36c…58fb
    before and after."; "equal to the parent's; there is no ollama_restarts key"; "exactly
    two restart sites, each with -n"; "3593 passed, 17 deselected, 1 xfailed, 2 warnings in
    961.45s"; "wall 963.3 s, inside 897–1095"; "collected 3,611 = 3,591 + 20"; "selected
    3,594 = 3,574 + 20"; "NRestarts=0, unit start unchanged since 2026-11-16: no restart and
    no model load"; "Commit: 03abfaf5b16b577d0a089338c787a988db7e6fa5 (parent 339370d), 27
    files, +13,436 −0"; "Ledgers: 124d44c for the engine commit and b967485 for the evidence
    commit."; "Rows 289 (94 / 59 / 136, 0 unclassed)".
  READ (CC's): findings 1–8; I2–I7 as the report gives them.
  READ (the architect's clone at 03abfaf): the three hashes in STATUS in a straight chain on
    483969d; 12ai_U41B/brief_12ai_U41B.md and 12ai_open/r819.txt equal to the architect's
    copies by cmp; the plan's last line "The decision log in this file ends at R819.";
    12ai_U41B/gate_u41b/gate_wall.txt "wall: 963.330798171 s"; the engine commit's diff of
    the eight files, read whole against R819(c)–(g); in
    tests/test_restart_recovery_guard.py and tests/test_u21_no_overlap.py, the two
    foreign-lock tests' assertions unchanged, a run that asked added; text that still
    states the old behaviour at analysis/eval/run_qualgap01.py:21,
    docs/architecture/models.md:62 and :66, modules.md:146, pipeline.md:223 and :231.
  INFERRED: none.

R820(a) — The 12ai-U41B final report is accepted as printed. Item 0 ee20dba (Block 217; rows
  INT-ai12-1 and INT-ai12-2; U41 entered; r819.txt and the brief file equal to the
  architect's copies), ledger ef77495. The engine commit 339370d, as R819(c)–(g) and (j)
  rule it, read by the architect clause by clause: the default 0; sudo -n at both sites;
  the cadence counter reset outside its try; restart_not_enabled the seventh ending; the
  recovery order opt-out, not enabled, foreign lock, command; ollama_restarts written only
  when asked; pdf_quality_check stopped by a run fault at once and by three read timeouts
  in a row, exiting non-zero. Ledger 124d44c. The evidence commit 03abfaf, ledger b967485.
  I1–I7 held; E1–E6 held. U41 has landed: P1's fix units 39, done 34.
R820(b) — The gate's 963.33 s is the forty-fifth member of the series: a standard gate
  launched as a unit, no hold, on a tree with twenty more tests. The band stays 897–1095 s
  until the closeout cuts the next.
R820(c) — Finding 2, the PI's word: "finding 2 agree". The two foreign-lock tests that went
  red are coupled tests, not regressions, and their rewrite stands: R819(e) puts the
  not-enabled ending before the lock, so the lock is reached only from a run that asked;
  each test's assertion is unchanged. The brief's stop list named "any test of the
  foreign-lock stand-down" as a regression, against the order the ruling beside it set:
  the architect's error, of the kind R816(c) records. CC went on by its own judgement past
  a stated stop and said so first in its report; accepted here, no rule.
R820(d) — Readings, no row. Finding 1 is the architect's: case 1 forbade the word the edit
  step required; CC met both by wording, and the three endings in which no restart was
  attempted no longer say "+ restart". Finding 5: a docstring, a log line, a gate list and
  a comment brought to the fact beyond the named edits, accepted. Finding 7: the old recount
  script's expectation no longer holds, as its rows moved; the closeout writes the next
  one. Finding 8: CC stashed the tree while a selected run was going, discarded that run
  and ran the whole set again; its own slip, without effect. Two readings from the
  architect's read of the diff: ollama_client's table of endings without a restart is
  keyed by string literals, not by run_manifest's names; run_pipeline opens its manifest
  through two branches so as not to pass an empty value.
R820(e) — INT-ai12-1 closes at the item 0 that transcribes this ruling, in the record's
  CLOSED form, on 339370d. With it, one dated note on the row: text that states the old
  restart behaviour remains at analysis/eval/run_qualgap01.py:21 and, in
  docs/architecture/, at models.md:62 and :66, modules.md:146 and pipeline.md:223 and
  :231; the architecture lines are HK-ARCH's (R819(j)) and the first goes with them. The
  count's effect is the recount's to state.
R820(f) — The hash owed to the workbench is 339370dd92a766ff45ad7abc44e7ec2faab1362b; the PI
  sends it. The removal of the sudoers rule is the workbench's.
R820(g) — The count so far in 12ai: no stop, no pause; eight architect's errors (R819(a)'s
  six; finding 1 and the stop list of (c)), one reaching a commit.
R820(h) — This ruling states no standing rule: the id list, the glosses file and
  STANDING_RULES.md are untouched at the item 0 that transcribes it, asserted by content.
  Next: U30's build with N46 (the brief below), from 12af_U30A/12af_U30A.md,
  12ag_N46A/12ag_N46A.md, R805(d) and R807(b); INT-j12-51 closed at the item 0 after it
  lands (R807(c)); the clone deletion brief, before the close; the report to the workbench.
===== END r820.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ai-U30B] — Land U30 with N46 as R805(d)(1)–(4) and R807(b) rule
  them: an interrupted cloud run rolls back and closes interrupted with its signal's
  reason; store_result rolls back on any exit that did not commit; run_arm rolls back
  before its close and closes only a run not already closed; send records a provider
  call an interrupt ends; the cloud entry point handles SIGTERM and SIGHUP as the other
  mains do; the cloud store drops a field the codebook did not ask for before any
  write. The fail-first acceptance file; the coupled tests rewritten; the text lines;
  one engine commit with its gate, evidence in its own commit. Item 0: R820 as the next
  block; INT-ai12-1 CLOSED with its note; the dated units line; the dated notes on
  INT-i12-71; this brief committed as pasted; no standing rule.
PURPOSE: Make a stopped cloud run leave nothing half-written and say that it was
  stopped, and make the cloud store hold the field names its claims hold. Behind R71 on
  live. Code task. No live write. No model call. No provider call. No network request.
  No API key read. No signal sent to any process.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  This brief authorizes one transient systemd-run --user unit, gate-12ai_u30b, launched
  by docs/session-reports/tools/launch_gate.sh at step 2h, and nothing else of systemd.
  Touched: scripts/run_cloud_extraction.py (run_arm's handlers and close; main's
  signals and exit; the comment of F3; nothing else); engine/cloud/base.py (send's
  handling of an interrupt; store_result's rollback and its one dropped-fields line;
  nothing else); tests/test_u30_cloud_interrupt.py (new); the coupled tests the census
  and the selected runs find; the text lines of step 2d; docs/inventory/ (regenerated
  if a recorded entry moves, R786(b)); docs/plan/ENGINE_REFACTOR_PLAN.md (Block 218;
  INT-ai12-1's close and note; the dated units line; INT-i12-71's notes);
  12ai_open/r820.txt; a new directory 12ai_U30B/; ~/scratch/12ai-u30b/; the
  claude-config ledger; git push on both repos; one standard gate.
  STOP / DO NOT TOUCH (beyond the global list): any other file under engine/ —
  run_manifest.py (close_run, record_call, interrupt_signals, interrupt_reason,
  is_closed and exit_process are called, not changed), extraction_events.py and its
  savepoint, completeness.py (drop_unexpected is called, not changed), the two
  transports, the migrations; in base.py, send's recording of a raised Exception and of
  a returned response (R800), extract_with_completeness, store_result's checks and
  their order apart from the one line, its INSERTs, its writer call and its commit,
  get_pending_papers, request_payload; extracted_data, which stays the response as
  received (R807(b)); scripts/run_pipeline.py and engine/agents/ft_screener.py (read as
  models, not edited); analysis/; review_specs/ (R71); the codebook; the tools; the
  installed SDK packages; the id list, the glosses file and STANDING_RULES.md (not one
  byte); review.db (mode=ro instruments only; no new query on live); any .env or API
  key; every scratch directory other than ~/scratch/12ai-u30b/. No test or step sends
  a signal to a process, constructs a real SDK client or opens a socket: one that
  would is a STOP.

PATHS: a name of the form 12xx_<name>/ is a directory of docs/session-reports/session-12/;
  a tool is named by its full path.

FORM: 12af_U29B/brief_12af_U29B.md (sha256
  cb9ae249537d5d652e3d8df50b30494a01f60914b4c19bf01f47fae35c1b50a9) is this brief's form
  for a build, read as 12ai_U41B/brief_12ai_U41B.md read it (the gate launched as a
  unit; the evidence beside it). Read the form's Section 2 whole before step 0. Where a
  form and this brief disagree, this brief governs; where either disagrees with a tool
  or the record, the tool and the record govern and the difference is a finding
  (R732(a)). The design is named by its ruling clause throughout: R805(d)(1)–(5) and
  R807(b), both on disk in the decision log and in 12ag_open/r805.txt and r807.txt;
  read both clauses whole before step 2.

TIMING: issued at 20:56 UTC on Friday 2026-11-20. Valid outside the day's window
  (R812(a)): the next opens at 09:00 UTC on Saturday 2026-11-21. No edit in the working
  tree inside a day's window. The gate must exit before a window opens: launch by 30
  minutes before it or after 10:35 UTC. If the clock reaches that bound before the
  engine commit is gated, hold with the commit unpushed and resume in this brief after
  10:35 UTC (R789(a)); say so in the report. If the clock at step 0 is inside a window
  or within 15 minutes before one opens, STOP at once and write nothing.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ai-U41B final report, as R820 quotes it): HEAD
    03abfaf5b16b577d0a089338c787a988db7e6fa5, parent
    339370dd92a766ff45ad7abc44e7ec2faab1362b, clean, level; claude-config at b967485 or
    an accepted descendant (R663(b)); gate reference 12ai_U41B/gate_u41b/ (3,593 passed
    / 1 xfailed / 17 deselected on 3,611 / 3,594; its collected_ids.txt and
    durations_12ai_u41b.txt); band 897–1095 s (R816(b)); pins
    12ai_U41B/pin_tuples_u41b.json; rows 289; fix units 39.
  READ (the architect's clone at 03abfaf): the plan's sha256
    c305ca581de72f8bd6d7fb72b6c4a99dd3007e5468251c7d211cd9bbc854a4a3, 3,425,999 B, 20,490
    lines, ends "…ends at R819.", last block 217, 174 "ends at" lines, six-cell INT-
    rows 215; row INT-i12-71 at plan :600 and row INT-ai12-1 at :977; git diff --stat
    977c499 HEAD on engine/cloud, scripts/run_cloud_extraction.py,
    engine/core/completeness.py and engine/core/extraction_events.py is empty, so the
    two read-outs' lines in those files stand; engine/core/run_manifest.py moved at
    339370d, so their lines in that file do not; scripts/run_cloud_extraction.py's
    run_arm, whose comment reads "Any exit but a return closes the run 'failed', an
    interrupt included, with the exception's class and message (R625(a)(8))."
  READ (12af_U30A/12af_U30A.md and 12ag_N46A/12ag_N46A.md at their committed bytes — the
    first's Q1's local runner on an interrupt, Q2's table of thirteen points, Q3's
    pending read, Q4's signals, Q6, §4's census, §5's probe, §7 items 6–9 and F1–F6;
    the second's §2, §5's probe, §6 "U30's seam", §7 items 1, 3 and 4 and F3 — not
    repeated; step 2a re-reads each site at HEAD). From them, the anchors: base.py send
    :164 with its handlers :194 and :202; store_result :393, the completeness call
    :422, "for span in spans:" :461, the commit :500, the rollback :507–508;
    get_pending_papers :215–234; run_cloud_extraction.py :121–129, :189–190, :211,
    :214–215; run_pipeline.py's interrupt branch and main as the first read-out quotes
    them; completeness.py:212–215 and :220; extractor.py:672; extraction_events.py
    :711–724; tests/test_failed_close_reason.py:146–152.
  INFERRED — BLOCKING (read at 2a before any edit; false = STOP):
    I1. The engine tree is unchanged since 339370d (git diff --stat on engine tests
        scripts analysis review_specs, empty).
    I2. rm.interrupt_signals, rm.RunInterrupted, rm.interrupt_reason, rm.is_closed and
        the exit the other mains use exist at HEAD with the behaviour the first
        read-out quotes (each quoted at its present line).
    I3. On an interrupt, a rollback on the cloud connection before the close undoes
        the store's open rows and no committed row (the connection's transaction
        state at each of Q2's rows 4, 7, 8, 9 and 12, shown by the parent probe).
    I4. drop_unexpected, called as the local path calls it, returns the spans whose
        field the codebook asked for and changes nothing else about them (its body
        and extractor.py's call, quoted).
    I5. No reader on a run or export path changes its output when the legacy spans
        table stops receiving a field outside the codebook (the second read-out's §3
        rows 1–4, re-read at HEAD).
  INFERRED — expectation (report a mismatch, continue):
    E1. On the parent, the acceptance file's cases 1–6, 9 and 10 are red and cases 7
        and 8 are green; N_red and N_green read from the run.
    E2. Coupled tests (c):
        tests/test_failed_close_reason.py::test_a_cloud_arm_interrupted_closes_failed_naming_the_interrupt,
        superseded by R805(d)(1); any test that pins an uncited field outside the
        codebook costing the paper, superseded by R807(b); the census and the selected
        runs decide, and a file not named here is a (c), not a stop.
    E3. The gate collects 3,611 + N and selects 3,594 + N, N the new file's ids plus
        any id a rewrite renames (each rename an only-ref / only-gate pair).
    E4. The inventory's recorded entries for run_cloud_extraction.py and base.py move:
        an entry change, regenerated and kept (R786(b)); a stamp-only diff is
        reverted (R793(e)).
    E5. Pins, the audit prompt_hash and each transport's request payload do not move.
    E6. A provider call's run_calls row, once committed, survives every rollback this
        unit adds.

CONTRACT (steps, in order; evidence under ~/scratch/12ai-u30b/):

  STEP 0 — Pre-check (read-only). The form's step 0 with this brief's values: last block
    217, no Block 218; I1; the three rules files at the 12ai-OPEN report's hashes;
    claude-config; fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; the check written for
    this brief, R794(i)). Models, each found in the log and quoted before its
    counterpart is composed: ee20dba for the block's metadata keys and the dated units
    line; the most recent item 0 that closed an INT- row after its unit landed, for
    the CLOSED form and what it does to the row and the counts; ffc6c38 for a dated
    note on a row.
    1a. 12ai_open/r820.txt from Section 1, one write, hashed straight after (R738(b))
        against Section 1's hash. Mismatch = STOP (R739(b)).
    1b. block218.json in block217.json's key set: tag R820; transcribed = the commit's
        UTC date (R775(a)); item "item 0 of the 12ai-U30B brief"; made_by PI; made_date
        2026-11-20; session 12ai; report as R820's STATUS names it, the 12ai-U41B final
        report with its three hashes, composed by CC in the form of the record's
        nearest block whose report carries an item-0, an engine and an evidence hash
        (found, quoted, named in the report); sentence model, followed "12ai-U30B
        brief"; prev_ends R819; "beside" composed by CC from the models' words for
        what this commit carries. Render to stdout, compare the element order with
        the model's, then --append.
    1c. INT-ai12-1 (R820(e)): closed in the model's CLOSED form on
        339370dd92a766ff45ad7abc44e7ec2faab1362b, and one dated note in the model's
        note form, tagged with the commit's UTC date and "(12ai-U30B item 0,
        R820(e))", its text between the markers:
        ----- BEGIN note on INT-ai12-1 -----
        U41 landed at `339370d`: both restart paths are off by default and run only for a run that asks (R819(c)–(g)). Text that still states the old behaviour: `analysis/eval/run_qualgap01.py:21`; `docs/architecture/models.md:62` and `:66`, `modules.md:146`, `pipeline.md:223` and `:231`. The architecture lines are HK-ARCH's (R819(j)); the first goes with them.
        ----- END note on INT-ai12-1 -----
    1d. The dated units line, in ee20dba's line's form, tagged "(12ai-U30B item 0,
        R820(a))": U41 landed at 339370d; P1's fix units 39, done 34 (U01–U29,
        U37–U41); the row counts computed from the plan after 1c by the method the
        model's line used, not from this line.
    1e. INT-i12-71 (R805(d)): dated notes in ffc6c38's note form, tagged "(12ai-U30B
        item 0, R805(d))", the text between the markers; class and package unchanged;
        the row closes at the item 0 after U30 lands.
        ----- BEGIN note on INT-i12-71 -----
        U30's design is ruled (R805(d), from `12af_U30A/12af_U30A.md` §7): (1) an interrupted cloud run closes `interrupted` with its signal's reason, the local runners' form; (2) `store_result` rolls back on any exit that did not commit, and `run_arm` rolls back an open transaction before its close and closes only a run not already closed; (3) `send` records a provider call that an interrupt ends, as an error naming the interrupt, and re-raises, amending R800(b) for interrupts; (4) the cloud entry point handles SIGTERM and SIGHUP as the other mains do. (5) The row's first half is closed as answered: no activation is owed, because since U40 a cloud claim is written through `legacy_record`, which iterates the codebook's expected names only. N46 is built with it (R807(b)).
        ----- END note on INT-i12-71 -----
    1f. The brief file (R784(g)(v)): 12ai_U30B/brief_12ai_U30B.md from this whole pasted
        block, "CLAUDE CODE" through the STOP-12AI-U30B line, one trailing newline,
        written once; sha256, bytes and lines straight after the write, reported.
    1g. The item-0 check, as ee20dba's, with: --check-plan byte-identical for Block
        218; the plan's numstat read, its "−" matched to the rows 1c and 1e replace
        and to nothing else; every earlier "ends at" line kept (174 before); the last
        line "…ends at R820." once; the plan equal to HEAD's plus those rows replaced,
        the dated line and Block 218, nothing else (computed); r820.txt equal to 1a's;
        the three rules files unchanged and unstaged (R777(b)); the staged set
        exactly the plan, r820.txt and the brief file. Quote the check.
    1h. <check> && <commit>, one line; the message from the staged set, docs scope.
        Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers (R801(f)); push; level.

  STEP 2 — The build (one engine commit).
    2a. Re-read at HEAD and quote, before any edit: send whole; store_result whole;
        run_arm and main whole; the local runner's interrupt branch and its main's
        signal handling and exit (the model for R805(d)(1) and (4)); the event
        writer's savepoint handler; close_run, record_call, is_closed,
        interrupt_reason and interrupt_signals at their present lines (I2);
        drop_unexpected and the local path's call (I4); the readers of I5; every line
        in CLAUDE.md and docs/architecture/pipeline.md that states how a cloud run
        closes, what send records on an interrupt, or what happens to a field outside
        the codebook on the cloud path (found by text, listed). A difference from the
        read-outs that changes an edit below, or I2 or I4 false, = STOP.
    2b. Census (census_u30a.py's form, run before the edit; committed with the
        evidence): every test that reaches run_arm, the cloud main, send or
        store_result, with what it asserts about the close, the rollback, run_calls
        or the spans table; every test that pins a KeyboardInterrupt on the cloud
        path; every test that stores a field outside the codebook through the cloud
        store.
    2c. Write tests/test_u30_cloud_interrupt.py, fail-first, in the two probes' manner
        (run_arm on a scratch review; the real transport with a placeholder key and a
        fake SDK class; the interrupt raised in-process at a seam; no signal sent; no
        client constructed and no socket opened, asserted); each docstring states its
        case and its seam. The cases:
         1. A KeyboardInterrupt between the extraction INSERT and the span INSERTs: no
            cloud_extractions, span, event or claim_inputs row for the paper; the
            paper still pending; the run closed interrupted, reason interrupt:SIGINT;
            the paper's committed run_calls row still there.
         2. The same inside the event writer's savepoint: the same four things.
         3. A KeyboardInterrupt inside the provider call: one run_calls row for it,
            error, its detail naming the interrupt in R800(b)'s form; the paper
            pending; the run closed interrupted.
         4. rm.RunInterrupted for SIGTERM, and for SIGHUP, raised at case 1's seam:
            closed interrupted with interrupt:SIGTERM and interrupt:SIGHUP; nothing
            of the paper left.
         5. The cloud main: inside its call of run_arm the two signals' handlers are
            the ones interrupt_signals installs (read by signal.getsignal from a
            patched run_arm, no signal sent), and a RunInterrupted from run_arm ends
            main as the local main ends, with the same exit value for the signal.
         6. An interrupt between close_run's UPDATE and its commit: the manifest is
            not left open; it reads interrupted with its reason.
         7. An Exception from the store: the run closes failed with "<Class>:
            <message>", as now, and no row of the paper is committed by the close.
         8. A run that returns: completed, no reason, as now.
         9. A response of the codebook's fields and one field outside it, cited: the
            spans table holds exactly the codebook's names; the claims are the
            codebook's and no other; extracted_data still carries the name; the
            "dropped (R118)" line is logged.
        10. The same with the outside field uncited: the paper stores.
        Run on the parent: EXPECTED E1; the red and green ids listed; output to
        b0_parent_acceptance.txt, with the connection's in_transaction read at each
        seam (I3). A red case of E1 green on the parent = STOP.
    2d. Edit, R805(d)(1)–(4) and R807(b) exactly, and nothing else:
        — run_cloud_extraction.py, run_arm: an interrupt (KeyboardInterrupt or
          rm.RunInterrupted) is handled before the failed branch, in the local
          runner's form: roll back if a transaction is open, then close interrupted
          with rm.interrupt_reason only if the run is not already closed, then
          re-raise. On every other exit, an open transaction is rolled back before
          the close, and the close is made only for a run not already closed
          (R805(d)(2)). extractor.close() stays last. The comment that cites
          R625(a)(8) for an interrupt is corrected to the fact (F3).
        — run_cloud_extraction.py, main: SIGTERM and SIGHUP handled as
          scripts/run_pipeline.py's main handles them, quoted at 2a: the same
          context manager, the same exit value, the same way of ending the process
          (R805(d)(4)).
        — base.py, store_result: a rollback on any exit that did not commit, an
          interrupt included (R805(d)(2)); and, after the completeness check and
          before the citation check, the spans reduced by drop_unexpected to the
          fields the codebook asked for, as extractor.py's call does it (R807(b)).
          extracted_data is not filtered.
        — base.py, send: a provider call that an interrupt ends is recorded as
          error, its detail naming the interrupt in the form send already uses for
          a raised call, and the interrupt re-raised (R805(d)(3)). This covers the
          provider call only: an interrupt that lands in send's own reading or
          recording of a returned response is not recorded a second time. One
          record per send still holds on every path inside a run; none outside one.
        — Text: the lines 2a listed, each to the fact, before and after quoted; a
          line not found as listed = STOP for the text step only.
        — Tests: each (c) test rewritten; the superseded assertion of E2 becomes the
          fact R805(d)(1) states, its docstring naming R805(d); every other (c) test
          keeps its assertion. A test whose assertion cannot be kept and is not
          superseded by R805(d) or R807(b): STOP with the test quoted.
        git diff --stat EXPECTED: the two files named, the new test file, the (c)
        test files, the text files of 2a, and docs/inventory/ under E4. The "−"
        lines of the two files quoted and matched to the edits above.
    2e. Pins: 12l_U04/pin_tuples.py with both arguments, byte-identical to
        12ai_U41B/pin_tuples_u41b.json; the audit prompt_hash unchanged. A moved
        value = STOP.
    2f. Selected runs green: the new file; tests/test_failed_close_reason.py,
        tests/test_u29_cloud_call_record.py, tests/test_u40_cloud_claim_events.py,
        tests/test_u27_cloud_run_faults.py, tests/test_u28_cloud_gate_and_boundary.py,
        tests/test_cloud_extraction.py, tests/test_codebook_review_at_open.py,
        tests/test_corpus_authority.py, tests/test_request_capture.py and
        tests/test_run_manifest.py whole; every other file the 2b census lists. A
        red is classified before anything else: (c) coupled → rewritten under 2d's
        rule; (r) a regression — a test of send's record for a raised Exception or a
        returned response, of the claims a cloud store writes, of the local
        runners' interrupt handling, or a pin — = STOP. A red that follows only from
        the order or the fact a ruling clause sets is a (c): if a red fits both
        descriptions, STOP and report it with the clause; do not class it alone.
        Commit the engine change (fix scope; the message names U30, INT-i12-71, N46
        and INT-j12-51, R805(d) and R807(b), the interrupted close, the two
        rollbacks, the recorded interrupted call, the signals, the dropped fields,
        the text lines, the rewritten tests and the inventory if regenerated), from
        the staged set. Do not push before the gate.
    2g. No tree change is made while a test run is going: nothing is stashed, checked
        out or edited until the run has ended.
    2h. The standard gate, launched as the unit gate-12ai_u30b by
        docs/session-reports/tools/launch_gate.sh as 12ai-U41B's step 2h launched its
        own: evidence_dir ~/scratch/12ai-u30b/gate_u30b/, name 12ai_u30b, ref_ids
        12ai_U41B/gate_u41b/collected_ids.txt, ref_durations
        12ai_U41B/gate_u41b/durations_12ai_u41b.txt, no selection; never a CC
        background task. EXPECTED E3; 1 xfailed; 17 deselected; wall inside
        897–1095 s, outside it reported with the concurrent jobs, not adapted to; no
        phase >10 s slower; no model load in the gate's journal window (R696(b));
        the unit's start line; the nineteen files. A red: class (c) per R746(c) and
        R772(b), fixed under 2d's rule and the gate re-run once; any other red =
        STOP, the engine commit standing.
    2i. Fingerprint --compare; push; level; --compare after (IDENTICAL). Ledger row;
        push; level.

  STEP 3 — Evidence commit (R731(c)). 12ai_U30B/: b0_parent_acceptance.txt, the 2b
    census script and output, gate_u30b/ (the nineteen files) with the launcher's
    stdout and the hold snapshot beside it, pin_tuples_u30b.json; the staged set
    exactly those, every sha256 equal to its scratch source; chained check && commit;
    fingerprint IDENTICAL before and after the push; level; ledger.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1–I5 held, each beside its quoted lines.
  G2. Item 0 one commit that passed 1g in one chain: r820.txt at the architect's hash;
      Block 218 by the tool; INT-ai12-1 closed with its note; the dated line;
      INT-i12-71's notes; the rules files untouched; the brief file hashed; pushed,
      level, fingerprint IDENTICAL; ledger.
  G3. E1's red cases red on the parent and green in the gate, its green cases green on
      both; the engine diff confined to the two files and to the edits named;
      run_manifest.py, extraction_events.py, completeness.py, the transports and the
      local runners untouched; U29's and U40's acceptance files green unedited in
      their assertions, or each change named as superseded by R805(d)(3); pins, the
      audit prompt_hash and both request payloads unchanged; no signal sent, no SDK
      client constructed and no socket opened in any test; the gate at E3's counts,
      no load, nineteen files; pushed, level, fingerprint IDENTICAL; ledger.
  G4. The evidence commit as step 3 states.

OUT OF SCOPE: activating the cloud run (R805(d)(5)); a schema change or a new outcome
  token (023's); filtering extracted_data or refusing the response (R807(b), not
  taken); whether live holds a row with a field outside the codebook, the two repair
  scripts' insert path and the three analysis readers (R807(d)); a second signal
  inside an interrupt handler's close; INT-ae12-2's retry budget and a run that closes
  completed with every paper refused; A15's per-arm axis; U31; closing INT-i12-71 and
  INT-j12-51 (the next item 0, R807(c)); INT-ai12-2; R71; the clone deletion; the
  tools items; /wrap.

CONSTRAINTS (task-only): no model call, no provider call, no network request, no API key
  read, no signal sent, no live write, no new query on live; no edit in the working
  tree inside a day's window. A count arrived at by eye is INFERRED; a computed value
  is reported with its command. An SDK's behaviour is stated from its installed
  source with the file and line.

FINAL REPORT FORMAT: the form's sections (0)–(6) with this brief's contents: (0) the
  pre-check; (1) item 0 — block218.json with its models quoted, INT-ai12-1's row as
  closed with its note, the dated line, INT-i12-71's notes as written, r820.txt's
  hash, the brief file's sha256, bytes and lines, the check, hashes, numstat,
  fingerprint, ledger; (2) the build — I2–I5 each with its quoted lines; the census;
  the new tests as written; b0_parent_acceptance.txt verbatim; the two files' diffs
  whole; each (c) test before and after, the superseded one marked; the text lines
  before and after; the inventory diff; the pin recompute; the selected runs; the
  engine commit's full hash and parent; (3) the gate; (4) the evidence commit; (5)
  contradictions and findings, numbered, each with its quoted line; (6) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, a 1a mismatch, or a check failure or crash at
  1g = STOP before the item-0 commit. I2 or I4 false, a difference from the read-outs
  at 2a that changes an edit, a red case of E1 green on the parent, an (r) red, a red
  that fits both (c) and (r), a (c) test whose assertion cannot be kept and is not
  superseded, a diff in an engine file outside base.py, a test that would send a
  signal, construct a real SDK client or open a socket, or a moved pin = STOP before
  the engine commit. I3 or I5 false is reported with the lines; I3 false = STOP, I5
  false is a finding and the build continues. A non-(c) red in the gate = STOP, the
  engine commit standing and reported. An expectation mismatch (E1–E6) is reported
  and the work continues. A spec premise that does not survive contact with disk is
  a finding, not an obstacle.

HANDBACK: Stop and report. Do not close INT-i12-71 or INT-j12-51 and do not start U31 or
  the clone deletion. Await the next brief or ruling in this session. STOP-12AI-U30B
