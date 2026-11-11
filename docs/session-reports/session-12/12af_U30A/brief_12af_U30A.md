CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R801, PI ruling 2026-11-11 UTC; not on disk)
================================================================================

R801 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r801.txt: sha256 3332bb4efec21b55b45c0ade1b6ce158deae2d6239ab3542a8389f1feae8411c,
            7845 bytes, 92 lines, one trailing newline.

===== BEGIN r801.txt =====
RULING [evidence-engine 12af-R801] — U29 landed and accepted; C24 closes on it; the series gains
  979.95 s; the PI's instruction on read-outs, decisions and briefs recorded; no clause of
  STANDING_RULES.md; U30's Phase A opens, the last work of the night
STATUS: PI ruling, 2026-11-11 UTC, session 12af, on architect recommendation, on the 12af-U29B
  final report (item 0 95ce15f0fe8df50618880b08d901a5c97496b9e0; engine commit
  977c499f5d399d64b15edf58cbe0fa32151eb255; evidence 2b68cd4c659e5402c9fd4efc3c3d2541d7cc0d80),
  and on the PI's words of 2026-11-11 05:15–05:23 UTC quoted in (g) and (h). Transcribe at item 0
  of the 12af-U30A brief below, by transcribe_ruling.py, in the block after R800's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12af-U29B final report, quoted): "95ce15f (parent 1e3b74d)"; "977c499 (parent
    95ce15f)"; "2b68cd4 (parent 977c499)"; ledgers "c061dcf", "d69edf8", "7661913"; "r800.txt:
    aaa70ee3…d583, 11,412 B, 132 lines. Equal to your hash."; "brief_12af_U29B.md:
    cb9ae249537d5d652e3d8df50b30494a01f60914b4c19bf01f47fae35c1b50a9, 32,114 B, 410 lines" (the
    architect's copy: equal, cmp byte for byte against the clone); "Plan numstat +145/−2; the "−"
    lines are exactly C24 and INT-ae12-2."; "The plan is now 3d697748…ec9a, 3,133,528 B, 18,349
    lines."; "b0 on the parent: 17 red, 2 green."; "Pins: byte-identical to pin_tuples_u40b.json
    (aea19354…)."; "Selected runs: 16 files, 288 passed."; "Launch: 04:27:25Z. No hold: the
    snapshot before showed uvicorn only."; "Wall: 979.95 s, inside 876–1069."; "3573 passed, 17
    deselected, 1 xfailed, 2 warnings in 977.75s"; "collected 3,591 and selected 3,574";
    "only-gate is the 19 new ids; only-ref is empty"; "2b68cd4, 24 files".
  READ (CC's): findings 1–8; "They run outside a run, and the accessor runs only inside one, so
    they were not edited."; "a transient connection error or 5xx gets three sends per paper (the
    transports' loop, fixed 2 s / 4 s waits) where it used to get up to nine with the SDK's
    backoff"; "The surgical-cv uvicorn restarted under a new pid at 04:28:43"; "Ledger d69edf8
    says "No foreign pending rows." but carried two surgical-cv-fw2 rows, 14dcfb2 (04:15:52Z) and
    40a0fb3 (04:19:42Z)".
  READ (the architect's clone at 2b68cd4): the three full hashes in STATUS; the plan's sha256
    3d697748583b6ba42de3306ed588a34f5ed91e70b01b6b057fb6dac8bd2dec9a, 18,349 lines; the engine
    diff 95ce15f..977c499 whole (base.py's send, the two transports' read_response and
    constructor lines); gate_wall.txt "wall: 979.947490304 s".
  INFERRED: none.

R801(a) — The 12af-U29B final report is accepted as printed: item 0 95ce15f (Block 198; the C24
  and INT-ae12-2 notes; the brief file byte-equal to the architect's copy; the rules files
  untouched), the engine commit 977c499 (send's one row per call inside a run; read_response on
  each transport; max_retries=0 at both constructions; the three guards and the fallback gone;
  the new file's nineteen ids; the coupled tests rewritten, one assertion superseded by R800(h)
  and marked; two docs lines; the inventory regenerated for moved entries), the gate 3,573 / 1 /
  17 on 3,591 / 3,574 at 979.95 s inside 876–1069 s, green at its first run, only-gate the
  nineteen new ids, no load, the nineteen files; the evidence commit 2b68cd4; ledgers c061dcf,
  d69edf8 and 7661913. Pins and the audit prompt_hash unchanged. I1–I5 held. U29 is landed: P1's
  fix units 38, done 33 (U01–U29, U37, U38, U39, U40). No stop, no pause.
R801(b) — The series gains 979.95 s: forty. A standard member; the restart of another lane's
  uvicorn service inside its window (04:28:43) is carried beside the value as a reading. The
  band in force stays 876–1069 s.
R801(c) — C24 is CLOSED on U29 at 12af-U30A's item 0, in C49's form, by R514, for the Wave 2
  finding it was reopened for (row 28), with N47: inside a run a cloud send that raises leaves
  its error row, a truncated or refused response is not recorded completed, a returned response
  carries its digest, and one recorded call is one HTTP send. The acceptance test named is
  tests/test_u29_cloud_call_record.py::test_a_cloud_send_that_raises_still_records_its_call, red
  on the parent in 12af_U29B/b0_parent_acceptance.txt and green in 12af_U29B/gate_u29b/, with the
  file's other cases beside it. The note names the superseded assertion (U27's zero calls on the
  provider route, R800(h)) and what stays 023's: a token of its own for a truncated or refused
  output on both paths (R800(c)). Counted until P1 closes (R607(d)).
R801(d) — The dated units line at the same item 0, after the 12af-U29A line and in its form: U29
  landed at 977c499; C24 CLOSED on it and counted until P1 closes; P1's fix units 38, done 33;
  the row count measured on the plan as that item 0 leaves it, by the method the 12af-U29A line
  names (EXPECTED 287, 94 / 57 / 136, 0 unclassed — an expectation); rows in 0, rows out 0.
R801(e) — A dated note on INT-ae12-2 at the same item 0: the retry budget, measured (the report's
  finding 3) — with the SDK's retries off a transient connection error or 5xx gets three sends a
  paper at fixed 2 s and 4 s waits, where the SDK's own backoff used to add up to six more; no
  stored value depends on it; the loops' budget is this row's to set; class and package
  unchanged.
R801(f) — Readings, no row: finding 1 (R800(h) named the two request-capture tests as coupled;
  they run outside a run and were rightly left alone — the architect's, the fifth error of the
  session, caught by CC reading the code over the brief; none has stopped a brief); finding 2
  (the accessor reads the SDK types' optional fields with a default so that U40's fakes, which
  lack them, pass — harmless and test-shaped; it goes when those fakes are given the SDK's
  types, in the next build whose files include the transports, else P7); finding 4 ((b));
  finding 5 (ledger d69edf8's message says no foreign rows were pending and it carried two,
  14dcfb2 and 40a0fb3; the rows are true; nothing is amended; the next ledger commit's message
  names both as carried by d69edf8); finding 6 (three more coupled fakes, class (c), found by
  the census); finding 7; finding 8.
R801(g) — The PI's instruction to the architect, in his words: "Always do a read out before
  asking for decisions so your understanding is clear . Then ask me for decisions in plain
  english with tradeoffs (if you are not high on confidence). Wait for my ruling before writing
  the brief. make this a rule in your next plan", and, on the case where nothing is open: "when
  a report leaves no open decision (everything is high or moderate high confidence), the
  read-out and the next brief go in the same reply." It binds the architect and enters the
  unified plan's workflow section as its rule 40 at the next plan version. It is not a clause
  of STANDING_RULES.md and changes nothing Claude Code does: the id list, the glosses file and
  STANDING_RULES.md are untouched at item 0, asserted by content. R798's and R800's designs
  were issued with their briefs in one block and ruled by the PI's paste; they stand. From this
  ruling on, a design clause that rests on a decision below moderate-high confidence is written
  only after the PI's word on it.
R801(h) — The PI's ruling on what follows, in his words: "A. Run U30’s read-out now then close
  for the night". U30's Phase A opens: "An interrupted cloud run rolls back and closes
  interrupted" (INT-i12-71), read-only, in 12af_U29A.md's form, its entry quoted first. No
  engine, test, script, spec or codebook edit; no model call; no provider call; no live write.
  Its design decision is put to the PI after its read-out, under (g); no build brief is written
  before his word.
===== END r801.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12af-U30A] — U30's Phase A read-out ("An interrupted cloud run rolls
  back and closes interrupted"; INT-i12-71). Item 0: R801 as the next block; C24 CLOSED;
  the note on INT-ae12-2; the dated units line; this brief committed as pasted; no standing
  rule. Read-only on engine/, scripts/ and tests/; STOP before any edit.
PURPOSE: Put on disk, before the design, what an interrupt does to a cloud run at each
  point it can land: what survives in the database, what later commits it, how the run is
  closed and with which status, and whether the paper is still pending afterwards. Mixed:
  one docs commit (item 0), one docs commit (the read-out). No engine, test, script, spec
  or codebook change. No live write. No model call. No provider call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 199; two rows' notes; one dated line);
  docs/session-reports/session-12/12af_open/r801.txt; a new directory
  docs/session-reports/session-12/12af_U30A/ (brief_12af_U30A.md at item 0; the read-out,
  census and probe with outputs at step 2); ~/scratch/12af-u30a/; the claude-config ledger;
  git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/ (R71), the codebook, docs/architecture/, CLAUDE.md, docs/inventory/, the
  tools; the id list, the glosses file and STANDING_RULES.md (not one byte); review.db
  (mode=ro instruments only; no new query on live); any .env or API key; no signal is sent
  to any process this probe did not start, and none to a process outside its own scratch
  run; any scratch directory other than ~/scratch/12af-u30a/; the working tree inside
  07:00–10:35 UTC (R86).

TIMING: outside 07:00–10:35 UTC. If the clock reaches 06:45 UTC before a commit, hold with
  the work on disk uncommitted and resume in this brief after 10:35 UTC (R789(a)); say so.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12af-U29B report; R801): HEAD 2b68cd4c659e5402c9fd4efc3c3d2541d7cc0d80, parent
    977c499f5d399d64b15edf58cbe0fa32151eb255, clean, level; plan sha256
    3d697748583b6ba42de3306ed588a34f5ed91e70b01b6b057fb6dac8bd2dec9a, 18,349 lines, ends
    "…ends at R800.", last block 198; claude-config at 7661913 or an accepted descendant
    (R663(b)); the three rules files as 12af-OPEN's 0c read them.
  READ (the architect's clone at 2b68cd4; re-read on the box, not trusted):
    R1. U30's entry, 12l_P1A/12l_P1A.md:439–446: rows INT-i12-71; pin effect not affecting;
        files "`scripts/run_cloud_extraction.py`, `engine/cloud/base.py`"; acceptance test
        "`test_an_interrupted_cloud_run_rolls_back_and_closes_interrupted` — With
        `store_result` interrupted between its inserts, no row for that paper survives, the
        run closes `interrupted` with a reason, and the paper is still pending. Fails on
        the unmodified tree."; its note, "Half (b) is worse than the row states: by code
        order an interrupt between the extraction insert and its commit commits a partial
        extraction that is then treated as done."
    R2. Row INT-i12-71 (plan :600), two halves: "the cloud run never calls `rm.activate`,
        so R225's field-name check does not apply there; and its `finally:
        close_run(status)` closes `failed` on an interrupt with no rollback first (L04)";
        Class 1 (latent), P1.
    R3. scripts/run_cloud_extraction.py run_arm, :119–127: "except BaseException as exc:"
        with the comment "Any exit but a return closes the run 'failed', an interrupt
        included, with the exception's class and message (R625(a)(8)).", then "finally:" /
        "rm.close_run(extractor._conn, handle.run_id, status, reason=reason)".
    R4. engine/cloud/base.py: store_result's handler is ":507 except Exception:" / ":508
        self._conn.rollback()" — an interrupt is not an Exception; send's two handlers are
        "except Exception" (U29, R800(b): "KeyboardInterrupt is not this unit's (U30)").
        Since U40 the event writer runs inside store_result's transaction and commits it;
        since U29 rm.record_call commits the connection at every send.
    R5. R798(e) recorded, for the row's first half: "the cloud run is not activated
        (enforce_completeness already refuses unknown and missing names)".
    R6. 12af_U29A/12af_U29A.md is the form; census_u29a.py and fake_probe_u29a.py are the
        instruments' forms; 12af-U29A's item 0 (ffc6c38) is the model for a CLOSED note, a
        row note and a dated line in one item 0. The last dated line is "*Dated 2026-11-11
        (12af-U29A item 0, R799):*" at plan :932. Six-cell INT- rows: 213.
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 977c499 (git diff --stat
    977c499 HEAD on engine tests scripts analysis review_specs empty).
  INFERRED — expectation (report a mismatch, continue):
    E1. At HEAD a KeyboardInterrupt raised between store_result's two INSERTs is not rolled
        back by store_result, and run_arm's close_run then commits what was left: the
        paper has an extraction row with no span rows and is no longer pending.
    E2. The run closes "failed", not "interrupted".
    E3. The row count after item 0 is 287 (94 / 57 / 136, 0 unclassed), rows in 0, out 0.
    E4. Nothing is predicted about Q2–Q6 below: the architect has not read close_run, the
        run status tokens, R625's block or the local pipeline's interrupt handling.

CONTRACT (steps, in order; evidence under ~/scratch/12af-u30a/):

  STEP 0 — Pre-check (read-only). date -u. HEAD and parent proved by git cat-file -e
    <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. Plan: sha256, lines, last
    line, last block 198, no Block 199. Rules files: the three sha256s. claude-config:
    7661913 or an accepted descendant, level; a pending foreign row carried and named
    (R654(d)). I1. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12af-U29A's item 0,
    ffc6c38, less its new row; ~/scratch/12af-u29a/check_item0_u29a.py and block198.json
    quoted before composing; the check written for this brief, R794(i)).
    1a. docs/session-reports/session-12/12af_open/r801.txt from Section 1, one write,
        hashed straight after (R738(b)): EXPECTED 3332bb4efec21b55b45c0ade1b6ce158deae2d6239ab3542a8389f1feae8411c,
        7845, 92. Mismatch = STOP (R739(b)).
    1b. block199.json in block198.json's key set: tag R801; transcribed = the commit's UTC
        date (R775(a)); item "item 0 of the 12af-U30A brief"; beside = "the C24 closing
        note, the note on INT-ae12-2, the dated units line and the brief file it orders"
        (composed; quote it); made_by PI; made_date 2026-11-11; session 12af; report = the
        12af-U29B final report with its item-0, engine and evidence hashes, in Block 197's
        form (quote that header first); sentence model, followed "12af-U30A brief";
        prev_ends R800. Render to stdout, compare the element order with Block 197's, then
        --append.
    1c. The rows, each note appended at the end of its row's status cell in that row's
        note form, quoted before composing, tagged "*Note <the commit's UTC date>
        (12af-U30A item 0, R801(<clause>)):*": C24, R801(c) — **CLOSED** on U29 in C49's
        form, with the full engine hash, by R514, what was delivered, the acceptance test
        with its red and green evidence paths, the superseded assertion, what stays 023's,
        counted until P1 closes. INT-ae12-2, R801(e) — the measured retry budget, in CC's
        words, nothing added; class and package unchanged. Then the dated units line,
        R801(d), after the 12af-U29A line, its row count MEASURED by the method that line
        names (E3). Two rows replaced, one line inserted; the "−" count read and matched.
    1d. The brief file (R784(g)(v)): 12af_U30A/brief_12af_U30A.md from this whole pasted
        block, "CLAUDE CODE" through the STOP-12AF-U30A line, one trailing newline, written
        once; sha256, bytes and lines straight after the write, reported.
    1e. The item-0 check: --check-plan byte-identical; the plan's numstat read; every
        earlier "ends at" line kept; the last line "…ends at R801." once; the plan equals
        HEAD's plus the two rows replaced, the line inserted and Block 199 appended,
        nothing else (computed); each replaced row's last note begins with its 1c tag; the
        six-cell INT- row count 213 before and after; r801.txt equal to 1a; the three rules
        files unchanged and unstaged; the brief file hashed; block199.json's transcribed
        value equal to the commit-time UTC date; the staged set exactly the plan, r801.txt
        and the brief file. Evidence by a redirect inside the check; quote the check.
    1f. <check> && <commit>, one line; the message from the staged set (R778(d)), docs
        scope. Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers; its message names every foreign row it
        carries, counted from the diff and not from a fixed sentence, and also names
        14dcfb2 and 40a0fb3 as carried by d69edf8 (R801(f)); push; level. Record hashes and
        parents.

  STEP 2 — The read-out (read-only; no server; no provider; no live database). Every code
    claim beside a quoted line with its path and line at HEAD, labelled CODE ORDER or FAKE.
    2a. §1 Inputs quoted: U30's entry (R1); row INT-i12-71 whole with every dated note
        (R2); the 12i_review_w1.md adjacent item 6 and its reader passage (L04); R625's
        block as far as it rules how a cloud run closes, clause (a)(8) whole; R225 on the
        field-name check; R798(e) and R800(b) on what was left to this unit.
    2b. Q1 — THE STATUS. The run status tokens the schema allows, quoted from the migration
        that states them; close_run whole — what it writes, what it commits, what it does
        with a transaction left open on the connection it is given; what R625(a)(8) ruled
        and why; how the local pipeline closes a run on an interrupt (the status, the
        reason, the code path, quoted) and what "interrupted" means there. State plainly
        whether the entry's "closes `interrupted`" contradicts R625(a)(8), and what each
        would record.
    2c. Q2 — WHERE AN INTERRUPT CAN LAND. A table, one row per point: before the first
        send; inside the provider call; after the call returns and before its run_calls
        row; inside record_call; between a paper's sends; before store_result's first
        INSERT; between the extraction INSERT and the span INSERTs; among the span
        INSERTs; inside the event writer's savepoint; after the writer's commit and before
        store_result returns; in the transports' loops and sleeps; in run_arm after the
        run returns. For each: which handler it passes (quoted), what is left uncommitted
        on the connection, which later statement commits it (close_run, a later
        record_call, the connection's close), and what the database holds afterwards.
    2d. Q3 — WHAT "STILL PENDING" READS. get_pending_papers and get_progress, quoted: what
        makes a paper done; which of 2c's end states leaves a paper done without its
        spans, with legacy rows and no claims, with claims and no legacy rows, or with a
        chain hash and no run_calls row. Each such state named, with whether it is
        reachable by code order.
    2e. Q4 — WHICH INTERRUPTS. KeyboardInterrupt, SystemExit and a SIGTERM with no handler:
        which of them Python raises as an exception here and which ends the process with
        no handler run (the signal handlers this entry point installs, if any, quoted);
        what a tmux kill-session or a closed terminal sends; what the experiment lock and
        the connection do in each case.
    2f. Q5 — THE ROW'S FIRST HALF. What rm.activate would add to a cloud run that
        enforce_completeness does not already give (R5, R225), from quoted code; whether
        anything is still owed for that half, or whether it can be closed with this unit
        or noted as answered.
    2g. Q6 — WHAT A ROLLBACK MUST COVER. If store_result rolled back on any exit: what
        that rollback would and would not undo (rows already committed by a record_call
        earlier in the same paper; the kept reference and chain on the instance); what
        the transports' handlers and run_arm would have to do, if anything; whether U29's
        send should record an interrupted call and with which existing outcome token.
    2h. §4 Census: census_u30a.py in census_u29a.py's form (AST; declared completeness
        stated): every commit and rollback on the cloud connection with its caller; every
        handler on the cloud path by the class it catches; every close_run call and the
        statuses passed; tests coupled to run_arm's close, to store_result's rollback and
        to get_pending_papers, with their fixtures.
    2i. §5 The probe (FAKE, fake_probe_u30a.py → .txt, on fake_probe_u40a.py's scratch
        setup, an open cloud run driven through run_arm's own try/finally shape or run_arm
        itself if it can be pointed at the scratch database without touching the live
        root): the entry's test at HEAD — a KeyboardInterrupt raised between store_result's
        INSERTs (E1: the rows that survive, the run's status and reason, whether the paper
        is pending); the same raised inside the provider call and inside the event write;
        each end state's rows in cloud_extractions, cloud_evidence_spans, field_events,
        claim_inputs and run_calls quoted. The interrupt is raised in-process at a seam;
        no signal is sent. The output records that no SDK client was constructed and
        appends the Ollama journal for its window.
    2j. §7 Options for the ruling, each with what it records and what it costs: the
        status on an interrupt (keep R625(a)(8)'s "failed", or "interrupted" as the entry
        states, with what each means for a reader of the run table and for resume); where
        the rollback sits; whether an interrupted provider call is recorded; every file
        touched; the tests that move; the acceptance test's shape and seams; what U30 does
        not fix and whose it is. §8 findings; §9 what could not be established read-only.
    2k. Commit the read-out (12af_U30A/12af_U30A.md), census_u30a.py/.txt and
        fake_probe_u30a.py/.txt: the staged set exactly those, nothing under engine/,
        tests/ or data/ (asserted); fingerprint IDENTICAL before and after the push; level;
        the ledger row, its message as 1f states. Record hashes.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 held.
  G2. Item 0 one commit that passed 1e in one chain: r801.txt at the architect's hash;
      Block 199 by the tool; the two notes and the dated line in the record's forms with
      the row count measured; the rules files untouched by content; the brief file hashed;
      pushed, level, fingerprint IDENTICAL; ledger.
  G3. Q1–Q6 each answered from quoted code before §7 is written; 2c's table complete; the
      census and probe committed with their declared completeness; no SDK client
      constructed; no signal sent; the journal window empty; no file outside 12af_U30A/ in
      the commit; pushed, level, fingerprint IDENTICAL; ledger.

OUT OF SCOPE: U30's design and build; any schema change; activating the cloud run; U31 and
  later units; any edit under engine/, tests/, scripts/, analysis/, docs/architecture/ or
  CLAUDE.md; the tools items; the 2026-11-11 job reads; /wrap.

CONSTRAINTS (task-only): no model call, no provider call, no API key read, no network
  request, no restart, no live write, no new query on live, no signal to any process. A
  count arrived at by eye is INFERRED; a computed value is reported with its command. Where
  this brief describes a tool, a record or a line of code, the record and the code govern
  and a difference is a finding (R732(a)). The read-out states options and what each
  costs; it recommends nothing as decided — the decision is the PI's after it (R801(g)).

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — block199.json, the render
  comparison, the two notes, the dated line with its measured count, r801.txt's hash, the
  brief file's sha256, bytes and lines, the check verbatim, chain, hashes, numstat, the
  plan's new sha256, bytes and lines, fingerprint, the ledger message; (2) the answers to
  Q1–Q6 in plain sentences first, 2c's table whole, then the read-out's §7–§9 as
  committed and the probe's outputs quoted; (3) contradictions and findings, numbered,
  each with its quoted line; (4) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 false, a 1a mismatch, a row or model line not
  found as R6 quotes it, or a check failure or crash at 1e = STOP before the item-0 commit.
  An expectation mismatch (E1–E3) is reported and the work continues; a measured row count
  other than 287 is written as measured and reported. A spec premise that does not survive
  contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not start U30's design or build. Await the next brief or
  ruling in this session. STOP-12AF-U30A
