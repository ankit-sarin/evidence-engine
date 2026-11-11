CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R802, PI ruling 2026-11-11 UTC; not on disk)
================================================================================

R802 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r802.txt: sha256 cabdefe4ff69f155c7ede38eaa84549a383837e50329e8d0ae6b5b1394686c81,
            5696 bytes, 69 lines, one trailing newline.

===== BEGIN r802.txt =====
RULING [evidence-engine 12af-R802] — U30A accepted; U30's design is not ruled — its five
  decisions are the PI's at session 12ag; session 12af closes formally on U29 with U30's read-out
  committed; no closeout gate; no standing rule
STATUS: PI ruling, 2026-11-11 UTC, session 12af, on architect recommendation, on the 12af-U30A
  final report (item 0 faa672914666cc8c21dfb45687d0abe92e9212d4; read-out
  dacc67c1de93c74ef28f761e5af0b86089a0755b), and on the PI's words of 05:45 UTC quoted in (d).
  Transcribe at item 0 of the 12af-CLOSE brief below, by transcribe_ruling.py, in the block after
  R801's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12af-U30A final report, quoted): "faa6729 (parent 2b68cd4)"; "dacc67c (parent
    faa6729)"; ledgers "702d158", "6c5b4b9"; "r801.txt: 3332bb4e…411c, 7,845 B, 92 lines. Equal to
    your hash."; "brief_12af_U30A.md:
    fd161193679f686a034675a92db9aafa7a4d070240a2a3c9e3f9e3708b662b9f, 26,450 B, 340 lines" (the
    architect's copy: equal, cmp byte for byte against the clone); "check_item0_u30a.py → RESULT
    PASS"; "Plan numstat +107/−2; the "−" lines are the two rows."; "Six-cell INT- rows: 213
    before and after."; "The plan is now bd06ac88…59e5, 3,149,415 B, 18,454 lines."; "The measured
    count is 287, as E3 expected."; "E1 and E2 held."
  READ (CC's, the report): "a cloud interrupt is rolled back nowhere. run_arm's finally: close_run
    then commits whatever the interrupt left open"; "The run status tokens are completed, failed,
    interrupted and aborted."; "R625(a)(8) rules only the failed-close reason form."; "SIGTERM and
    SIGHUP have no handler on the cloud entry point, so the process ends with no finally run.";
    "enforce_completeness refuses missing names but only logs unknown ones, which still reach the
    legacy table (N46)"; the probe, "(A) between extraction INSERT and spans: run 'failed' /
    'KeyboardInterrupt'; cloud_extractions 1, spans 0, claims 0; pending False" and "(C) inside
    the writer's savepoint: 'failed'; cloud_extractions 1, spans 20, field_events 0; pending
    False"; findings 1–8.
  READ (the architect's clone at dacc67c): the two full hashes in STATUS; the plan's sha256
    bd06ac8805d953e88e8bce322ef1684566632238e0b89a33257812d9b21859e5, 18,454 lines;
    12af_U30A/12af_U30A.md's sha256
    5514b6510f04ed2161990de3e8a1cc64a8c8e3def2fc33067976c29812d3b9b4.
  INFERRED: none.

R802(a) — The 12af-U30A final report and its read-out are accepted as committed: item 0 faa6729
  (Block 199; C24 CLOSED on U29; the note on INT-ae12-2; the dated line at the measured 287 (94 /
  57 / 136), fix units 38, done 33; the brief file byte-equal to the architect's copy; the rules
  files untouched), the read-out dacc67c with its census and probe, ledgers 702d158 and 6c5b4b9.
  No stop. E1 and E2 held: an interrupt between the store's INSERTs leaves a paper done without
  its spans, and the run closes failed.
R802(b) — U30's design is not ruled. The read-out's §7 puts five decisions, and under R801(g)
  each is the PI's after a plain-English read-out, at session 12ag: the status an interrupted
  cloud run closes with (failed as found, or interrupted in the local runners' form); where the
  rollback sits (run_arm, store_result, or both); whether an interrupted provider call is
  recorded; whether the cloud entry point handles SIGTERM and SIGHUP; and whether INT-i12-71's
  first half is closed as answered. No build brief is written before his word. A dated note on
  INT-i12-71 at 12af-CLOSE's item 0 records the read-out (12af_U30A/12af_U30A.md, dacc67c), the
  two committed end states its probe shows (a paper done without its spans; a paper done with
  legacy rows and no claims), that SIGTERM and SIGHUP leave the manifest open, and that the
  design waits for the PI; class and package unchanged.
R802(c) — Readings, no row: finding 4 (run_arm's comment cites R625(a)(8) for closing failed on
  an interrupt, and that clause rules only the reason's form — corrected with U30's build);
  finding 5 (R798(e) said enforce_completeness already refuses unknown names; it logs them and
  they still reach the legacy table, which is N46, open with its owner — the architect's, the
  sixth error of the session, a clause on code no read-out had quoted for that case; no claim
  carries an unknown name, because legacy_record limits claims to the expected fields, so U40's
  claims are unaffected; the legacy table's unknown names are put with U30's decisions);
  finding 6 (the one gap left in U29's record, an interrupted provider call, is decision three
  of (b)); finding 7 (two ledger messages carry the same correction; both true); finding 8.
R802(d) — The PI's ruling, in his words: "Formal close with exit and archive of CC session
  followed by new unified plan and continuation prompt". Session 12af closes on U29 (977c499),
  with U30's Phase A committed and its design open. The closeout is written in 12ae-CLOSE's form.
  No closeout gate: the engine tree is unchanged since U29B's gate, which is the gate of record.
  The closeout drafts the lane line and does not apply it; the wrap ruling follows its report.
R802(e) — The band figure for 12ag is computed at the closeout from the last four standard gates
  on this host state, each read from its own file, and is set by the wrap ruling; the band in
  force until then stays 876–1069 s. The series stands at forty (R801(b)).
R802(f) — This ruling states no standing rule: the id list, the glosses file and
  STANDING_RULES.md are untouched at item 0, asserted by content. Whether R797(d)'s hold becomes
  a standing rule is the wrap ruling's, on the PI's word.
===== END r802.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12af-CLOSE] — Close session 12af on U29, in 12ae-CLOSE's form read
  from the record: item 0 (R802 as the next block; the INT-i12-71 note; this brief
  committed as pasted; no standing rule), then the closeout commit (the closure paragraph,
  the "Expected values at session-12ag open" block, 12af_close/'s measurements, the
  expected nightly file, the band computation, the recount, R510, retention, the lane line
  drafted and not applied). Docs only. No engine edit. No live write. No model call. No
  closeout gate (R802(d)).
PURPOSE: Leave the record in the state the next session opens on: every fact of 12af in the
  plan's closeout block with its evidence file beside it, the expected values for 12ag's
  open stated from U29's gate, and the band figure computed for the wrap ruling.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 200; the INT-i12-71 note; then the
  closeout block and the 12ag expected-values block); docs/session-reports/session-12/
  12af_open/r802.txt; a new directory docs/session-reports/session-12/12af_close/ (the
  counterpart files); docs/session-reports/session-12/12af_CLOSE/brief_12af_CLOSE.md (at
  item 0); ~/scratch/12af-close/ (the lane line draft and scratch); the claude-config
  ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/, the codebook, docs/architecture/, CLAUDE.md, docs/inventory/, the tools;
  the id list, the glosses file and STANDING_RULES.md (not one byte — R802 states no
  rule); 12ae_close/ and every earlier close directory (read, never rewritten); the dated
  units lines already in the plan; the lane line in the primers (drafted to scratch,
  applied only by the wrap ruling); review.db (mode=ro instruments only); the working tree
  inside 07:00–10:35 UTC (R86).

FORM (R784(g)(iv)): docs/session-reports/session-12/12ae_CLOSE/brief_12ae_CLOSE.md,
  Section 2, is this brief's form — its steps 0, 1 and 2 (2a–2h), its acceptance gates, its
  constraints, its final report format and its contradiction handling, read whole before
  anything is composed, with the 12ae closeout it produced (840db24: the closure and the
  "Expected values at session-12af open" block; 12ae_close/'s 24 files plus the files
  12af-OPEN added there) as the worked example. This brief states only what differs. Where
  the two differ, this one governs; where that brief names a 12ae source, the 12af source
  listed below takes its place; where it names a 12ad form, the 12ae counterpart is the
  form. A step of that brief with no 12af source is reported as not done, with its reason.

TIMING: outside 07:00–10:35 UTC; the clock is read before each commit. If it reaches 06:45
  UTC before a commit, hold with the work on disk uncommitted and resume in this brief
  after 10:35 UTC (R789(a)); say so in the report. If the brief resumes after 10:35 UTC,
  the 2026-11-11 health job and nightly have run: read them as 12ae-CLOSE's 2b read
  2026-11-09's, the nightly against the gate of the engine tree it ran on (U29B's, by the
  reflog method), and say so in the jobs paragraph.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12af-U30A report; R802): HEAD dacc67c1de93c74ef28f761e5af0b86089a0755b, parent
    faa672914666cc8c21dfb45687d0abe92e9212d4, clean, level; plan sha256
    bd06ac8805d953e88e8bce322ef1684566632238e0b89a33257812d9b21859e5, 18,454 lines, ends
    "…ends at R801.", last block 199; the three rules files as 12af-OPEN's 0c read them;
    claude-config at 6c5b4b9 or an accepted descendant (R663(b)); the session opened on
    840db24 (the 12ae closeout commit); U29's gate 12af_U29B/gate_u29b/: 3,573 / 1 / 17 on
    3,591 / 3,574, 979.95 s; pins 12af_U29B/pin_tuples_u29b.json; six-cell INT- rows 213;
    the dated line of 12af-U30A's item 0, rows 287 (94 / 57 / 136), fix units 38, done 33.
    Architect's hash of r802.txt: Section 1.
  READ (the record; quoted before each element is composed): R1. The 12ae closeout block
    and the 12af expected-values block in the plan — the form, element by element. R2.
    12ae_close/'s files — the counterpart names and forms (band_computation.txt,
    recount_12ae_close.py, expected_nightly_20261110.md, lane_line_12ae.txt,
    retention_close.txt and the rest). R3. The rulings 12ae_close/r796.txt and
    12af_open/r797.txt–r802.txt — the stops, the readings, the rows and the architect's
    errors, listed from them. R4. The session's commit list from git log 840db24..HEAD,
    each hash read, its ledger commit found in ~/claude-config's log by its row.
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 977c499. I2. No scheduled job
    has run since 12af-OPEN read the 2026-11-10 pair (the logs and the lock, read); if one
    has, it is read under TIMING's last sentence.
  INFERRED — expectation (report a mismatch, continue with the measured value):
    E1. The recount reads 287 (94 / 57 / 136, 0 unclassed); the dated lines' fix units 38,
        done 33; R510 for 12af in 1 (INT-af12-1), out 0; six-cell INT- rows 212 at 840db24
        and 213 at the item-0 commit.
    E2. The band computation, from the last four standard gates on this host state — U28B
        963.43, 12af-OPEN 966.10, U40B 965.92, U29B 979.95, each read from its
        gate_wall.txt — gives a mean of 968.85 s and, rounded inward as band_computation.txt
        of 12ae rounds, 872–1065 s. The series has forty members.
    E3. git diff --stat 840db24 HEAD on engine tests scripts analysis review_specs lists
        the files of the two engine commits (53eabfd, 977c499) and nothing else; from
        977c499 it is empty.
    E4. Retention 23 MATCH / 0 MISMATCH by file sha256, none opened, no new entry.
    E5. The fingerprint IDENTICAL; dbchecks_close.txt and pin_tuples_close.json
        byte-identical to 12ae_close/'s, the pins also to 12af_U29B/pin_tuples_u29b.json.

CONTRACT (the form's steps, with these 12af terms; evidence under ~/scratch/12af-close/):

  STEP 0 — Pre-check, as the form's: HEAD and parent above proved by git cat-file -e
    <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. Plan: sha256, lines, last
    line, last block 199, no Block 200. Rules files: the three sha256s. claude-config:
    6c5b4b9 or an accepted descendant, level; a pending foreign row carried and named. I1,
    I2. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (model 12af-U30A's item 0, faa6729, less its dated line;
    ~/scratch/12af-u30a/check_item0_u30a.py and block199.json quoted before composing; the
    check written for this brief, R794(i)).
    1a. docs/session-reports/session-12/12af_open/r802.txt from Section 1, one write,
        hashed straight after (R738(b)): EXPECTED cabdefe4ff69f155c7ede38eaa84549a383837e50329e8d0ae6b5b1394686c81,
        5696, 69. Mismatch = STOP (R739(b)).
    1b. block200.json in block199.json's key set: tag R802; transcribed = the commit's UTC
        date (R775(a)); item "item 0 of the 12af-CLOSE brief"; beside = "the INT-i12-71
        note and the brief file it orders" (composed; quote it); made_by PI; made_date
        2026-11-11; session 12af; report = the 12af-U30A final report with its item-0 and
        read-out hashes, in Block 198's form (quote that header first); sentence model,
        followed "12af-CLOSE brief"; prev_ends R801. Render to stdout, compare the element
        order with Block 198's, then --append.
    1c. One note, appended at the end of INT-i12-71's status cell in that row's note form,
        quoted before composing, tagged "*Note <the commit's UTC date> (12af-CLOSE item 0,
        R802(b)):*", carrying the facts R802(b) gives, in CC's words, nothing added. One
        row replaced; the "−" count read and matched to one. No dated units line: no unit
        has landed since 12af-U30A's.
    1d. The brief file (R784(g)(v)): 12af_CLOSE/brief_12af_CLOSE.md from this whole pasted
        block, "CLAUDE CODE" through the STOP-12AF-CLOSE line, one trailing newline, written
        once; sha256, bytes and lines straight after the write, reported.
    1e. The item-0 check, as the form's 1e with these terms: last line "…ends at R802."
        once; the plan equals HEAD's plus the one row replaced and Block 200 appended,
        nothing else (computed); the row's last note begins with its 1c tag; six-cell INT-
        rows 213 before and after; r802.txt equal to 1a; the three rules files unchanged
        and unstaged; the brief file hashed; block200.json's transcribed value equal to the
        commit-time UTC date; the staged set exactly the plan, r802.txt and the brief file.
    1f. <check> && <commit>, one line; then fingerprint, push, level and the ledger row as
        the form's 1f; the ledger message built from the diff (foreign rows counted and
        named).

  STEP 2 — The closeout commit, every element in the form's order, each with its file under
    12af_close/ in 12ae_close/'s counterpart name; a name with no 12af source is listed as
    not written, with the reason.
    2a. Measurements as the form's 2a, dated names carrying the read's UTC date. Differences:
        the temp root's figure is the root's own du -sb, as 12ae_close/'s file explains
        (R796(d) finding 2); the diff files are diff_engine_since_840db24.txt,
        diff_since_977c499.txt and diff_since_f4a5586.txt (E3); collected_ids_12af_close.txt
        is a copy of 12af_U29B/gate_u29b/collected_ids.txt and durations_12af_u29b.txt a
        copy of its durations file, each cmp equal; dbchecks, pins and the codebook and
        spec hashes compared with 12ae_close/'s (E5).
    2b. The jobs: I2. With no new job, the jobs paragraph says the 2026-11-10 pair was read
        at 12af-OPEN (its files in 12ae_close/) and that the 2026-11-11 pair is governed by
        2e's file and read by 12ag-OPEN.
    2c. The recount: recount_12af_close.py in recount_12ae_close.py's chained form,
        extended to read "L (12af additions, R799)" as that script reads the 12ae table, its
        own expectation stated (R800(a)); run in a scratch clone on the plan at the item-0
        commit; the chain reproduces 12ae's committed JSON
        (0b9f41f9dca0cf64bdb19d3135b81a32071106e0b956961a83fe901e88a886be) and through it
        the chain back; output recount_12af_close.json; both sha256s recorded. E1; a row
        count other than 287, or R510 other than 1 / 0, = STOP before the closeout commit,
        both values reported. r510_12af.txt in r510_12ae.txt's form.
    2d. band_computation.txt in 12ae's form: the four inputs of E2, each with its evidence
        path, its gate_wall.txt value and the concurrent jobs from its snapshots, with the
        readings carried beside two of them (pytest-614 inside 12af-OPEN's window, R797(b);
        the uvicorn restart inside U29B's, R801(b)); mean, ×0.9, ×1.1, the inward rounding,
        the figure; the series of forty listed — 12ae's thirty-seven plus 966.10, 965.92
        and 979.95, each read from its file; "a figure, to be set by the wrap ruling; the
        band in force stays 876–1069 s".
    2e. expected_nightly_20261111.md in expected_nightly_20261110.md's form with U29B's
        counts, each read from 12af_U29B/gate_u29b/: collected 3591 / 17 deselected / 3574
        selected; 3573 passed, 17 deselected, 1 xfailed, 2 warnings; exit code 0; ids equal
        to collected_ids_12af_close.txt; governs 2026-11-11 and each later date while the
        engine tree is unchanged.
    2f. The lane line, ~/scratch/12af-close/lane_line_12af.txt, in lane_line_12ae.txt's
        form: its five "−" lines are ~/claude-config/primer.md's lines 41–45 as they stand,
        taken from primer.md itself; its five "+" lines carry 12af's sources — HEAD the
        closeout commit (filled after 2h's commit, so the file is written last, is not in
        the commit, and is pasted whole in the report), the plan's sha256, bytes and lines,
        the decision log ending R802 (R803 at 12ag-OPEN's item 0), and the session's
        summary: two fix units landed (U40 at 53eabfd, U29 at 977c499) and U30's read-out
        committed with its design open; rows closed B9, C28, C24, opened INT-af12-1; 287
        rows; fix units 38, done 33; no stop, no pause; three gates and the band figure;
        pins unchanged; no live write; the ledger readings; the PI's instruction of
        R801(g); the next steps — 12ag-OPEN, then U30's decisions put to the PI, then its
        build. Not applied.
    2g. The closeout block and "Expected values at session-12ag open", placed after the
        12af expected-values block's last line and before the decision-log section (the
        position read and matched as 12ae's was), in the 12ae block's element order with
        12af's content: the closure paragraph (opened on 840db24; the CC session's id and
        times read from the record); the briefs in order — 12af-OPEN, U40A, U40B, U29A,
        U29B, U30A, this closeout — each with its item-0 ruling and its committed brief
        file's sha256 against the architect's (six of six so far, from the rulings);
        the commits in order from git log 840db24..HEAD with their ledger commits; the
        diff lists; the rulings R796–R802 one line each (R796 made at the 12ae wrap and
        transcribed at 12af-OPEN); the notes recorded (A15, A16, INT-i12-73, INT-ae12-2
        twice, INT-i12-71) and the rows opened (INT-af12-1) and closed (B9 and C28 on U40;
        C24 on U29); the stops and pauses (none, read from the rulings) and the
        architect's errors as the rulings count them (six); the tools paragraph (the six
        items carried unchanged; the recount script's own expectation; ledger messages now
        built from the diff); one short paragraph each for U40 and for U29 (design,
        acceptance test, what was not taken) in 12ae's unit paragraphs' form; a paragraph
        for U30's Phase A (its read-out, the two committed end states, the five decisions
        open and the PI's, R802(b)); "Carried forward" — U30's five decisions; the legacy
        table's unknown names (N46, R802(c)); the optional-field defaults in the two
        accessors (R801(f)); whether a truncated response that a repair completes may be
        stored (U31, R800(c)); a token of its own for a truncated or refused output on
        both paths and the local output-length stop (023, R800(c)); the retry budget
        (INT-ae12-2, R801(e)); INT-af12-1; A15's per-arm axis; base.py's getattr on
        model_string (P7); the stage-key question of 12f_C56-A.md:717; the six tools
        items; the cross-lane readings for dgx-infra (the launch hold of R797(d), a job
        starting inside a gate window, a service restart inside one, the shared ledger);
        the PI's instruction of R801(g); whether R797(d)'s hold becomes a standing rule
        (the wrap ruling's); the gates table (12af-OPEN with its hold, U40B, U29B); the
        band paragraph; the jobs paragraph (2b); the recount paragraph; retention; ledger
        (foreign rows carried; d69edf8's message and its correction); counterpart files;
        the host; live; "Session 12af closed. Next: session 12ag, fresh."; then the 12ag
        expected-values block in the 12af block's form with U29's gate, the band figure,
        the nightlies per 2e's file, the health jobs, the temp root, collection 3,591 and
        inventory --check in sync, the diff lists, the fingerprint and dbchecks, its HEAD
        bullet written "its full hash is in the lane line and the closeout report" (R796(d)
        finding 7). Every figure from a file in 12af_close/ or an evidence directory, named
        beside it; nothing from this brief's text where a file exists.
    2h. The closeout check and commit as the form's 2h, with these terms: the last "ends
        at" line "…ends at R802." once; the staged set exactly the plan and 12af_close/;
        the rules files, 12ae_close/ and every earlier close directory unchanged; the
        message in 840db24's form. Then 2f's lane line completed and pasted whole in the
        report.

ACCEPTANCE GATES: the form's G1–G3 with these terms — I1 held and I2 read; item 0 one
  commit that passed 1e in one chain with r802.txt at the architect's hash and Block 200 by
  the tool; the closeout commit with every element of the 12ae block present in order with
  its 12af source, every counterpart file written or listed as not written with its reason,
  the recount at E1 or stopped, the band computed from the files, the expected nightly
  file, E3–E5 held or reported, the check passed in one chain; both pushed, level,
  fingerprint IDENTICAL; ledgers; the lane line pasted whole, not applied.

OUT OF SCOPE: the wrap ruling (R803: the band set, the lane line approved and applied, the
  stops counted, R797(d)'s hold — after this report); U30's design and build; any engine,
  test, script, docs/architecture, CLAUDE.md or tools edit; the tools items; /wrap, /exit
  and the archive (the PI's, after the wrap ruling).

CONSTRAINTS (task-only): no model call, no provider call, no live write; a count arrived at
  by eye is INFERRED, the computed value given with its command; every figure in the block
  beside its file; where this brief or its form describes a tool or record, the record
  governs and a difference is a finding (R732(a)).

FINAL REPORT FORMAT: the form's — (0) pre-check table; (1) item 0; (2) the closeout: the
  measurements with their values, the recount output and chain, band_computation.txt
  verbatim, the expected nightly file verbatim, the closeout block and the 12ag block as
  committed (verbatim), the check verbatim, chain, hashes, fingerprint, ledgers, the lane
  line verbatim with its sha256, bytes and lines; (3) every place this brief's FORM clause
  left a choice, and the choice made; (4) contradictions and findings, numbered, each with
  its quoted line; (5) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 false, a 1a mismatch, or a check failure or
  crash at 1e = STOP before the item-0 commit. A recount other than E1's row count or R510,
  or a check failure at 2h = STOP before the closeout commit, the work on disk reported. An
  expectation mismatch (E2–E5) is reported and the closeout continues with the measured
  value in the block. A spec premise that does not survive contact with disk is a finding,
  not an obstacle.

HANDBACK: Stop and report. Do not apply the lane line, do not run /wrap, do not start 12ag
  or U30. Await the wrap ruling in this session. STOP-12AF-CLOSE
