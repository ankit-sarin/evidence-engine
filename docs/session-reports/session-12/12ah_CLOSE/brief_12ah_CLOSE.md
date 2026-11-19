CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R815, PI ruling 2026-11-19 UTC; not on disk)
================================================================================

R815 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the next
block of the decision log. The lines between the two marker lines, exclusive of the
markers, are the byte content of the named file. Write the file from those lines
exactly: same characters, same line breaks, one trailing newline. Never retype a
ruling. The architect's independent hash:
  r815.txt: sha256 9dd2450111211f12f52ff962e253a5dee5b0f12ded253178b8f66a485e0b21c0,
            4562 bytes, 60 lines, one trailing newline.

===== BEGIN r815.txt =====
RULING [evidence-engine 12ah-R815] — 12ah-OLRA-REC-R1 accepted: the interim read-out's
  twenty-seven files are in the record, scanned and verified by their own lists; readings;
  no closeout gate; no standing rule; the session closes by the brief below
STATUS: PI ruling, 2026-11-19 UTC, session 12ah, on architect recommendation, on the
  12ah-OLRA-REC-R1 final report (item 0 6dc36b312879bdd16c682093d86a9e2b3c331c86).
  Transcribe at item 0 of the 12ah-CLOSE brief below, by transcribe_ruling.py, in the block
  after R814's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12ah-OLRA-REC-R1 final report, quoted): item 0
    "6dc36b312879bdd16c682093d86a9e2b3c331c86", parent "c412033"; ledger
    "b0bb0bad95c64d0c70c90eb2db01bfb5c12dc4f0", parent "d46c74d"; "Result: 0 stop-class hits
    on all six files."; "Reading-class hits: 12, all abs_path_outside"; "the control flagged
    all 17 classes and the clean fixture gave nothing; the 21 top-level files gave 0
    stop-class hits and 235 reading-class hits"; "r813.txt: cc073a2e…10a9, 6,957 B, 87
    lines."; "r814.txt: d8223ede…ddf1, 4,587 B, 58 lines."; "Both equal the architect's
    hashes."; "Plan: +167 / −0"; "now
    ec03dc26cf44a022233afd534dbef064176abc13054fd93c7cf85c4325f2366e, 3,328,199 B, 19,766
    lines"; "SHA256SUMS 18 of 18, SHA256SUMS_add1 5 of 5, SHA256SUMS_close 1 of 1";
    "brief_12ah_OLRA_REC.md: 86a9e5f06d8ddee28cf5b94229559dbbf84779ce124a7b9234e0f14dc1017141,
    22,449 B, 315 lines, equal to the architect's copy."; "brief_12ah_OLRA_REC_R1.md:
    ef71492456f49dea253433be3248504286abe2c6e08cda810f49aefba7c56fab, 19,414 B, 258 lines.";
    "It confirmed 36 staged paths: the plan plus 35 new files, with exactly the six work/
    paths SHA256SUMS names."; "12ah_OLRA.md" "f054eedb…d3c09790"; "12ah_OLRA_add1.md"
    "568b179d…62e34ef1".
  READ (CC's): findings 1–3; what each of work/'s six files holds; that rec_scan.py is the
    file the first run used.
  READ (the architect's clone at 6dc36b3): the full hash in STATUS and its parent
    c4120330c7c8102d89d6c8298363e2624c7aca24; both brief files under 12ah_OLRA/ equal to the
    architect's copies by cmp (the resume's sha256, bytes and lines as the report gives
    them); the plan's sha256, bytes and lines as the report gives them, last line "The
    decision log in this file ends at R814."; the three rules files at the hashes they
    had after 12ah-OPEN's item 0; 12ah_OLRA/ holding thirty-three files.
  INFERRED: none.

R815(a) — The 12ah-OLRA-REC-R1 final report is accepted as printed: item 0 6dc36b3 (Blocks
  211 and 212; r813.txt and r814.txt equal to the architect's hashes; twenty-seven copies,
  each equal to its source; every line of the three committed lists a match; four evidence
  files; both brief files equal to the architect's copies), ledger b0bb0ba. R0 and R1 held.
  With the first run's report, 12ah-OLRA-REC's gates G1–G3 are met.
R815(b) — The scan: no stop-class hit in any of the twenty-seven files, on a control that
  flagged every class. The reading-class hits (paths, quoted sudoers lines, the
  all-interfaces address, one no-reply address) are what R813(d) and R814(b) accepted for
  publication. R814's expectation item held as CC read the six files.
R815(c) — The interim read-out is in the record: docs/session-reports/session-12/12ah_OLRA/
  12ah_OLRA.md and 12ah_OLRA_add1.md, with their briefs, their evidence and the three lists.
  The restart change's design ruling quotes them from there. Nothing of that design is
  ruled in session 12ah.
R815(d) — Readings, no row. Finding 1: the scratch helper's "files no list names" line looks
  at top-level names only; its line-by-line check is sound; the helper is not a tool of the
  repository and nothing rests on that line now. Finding 2: the resume file's hash is the
  architect's copy's, compared here. The scan helper's sha256 is first printed in this
  report; the committed file is its record.
R815(e) — No closeout gate: nothing under engine, tests, scripts, analysis, review_specs or
  the tools has changed since the 12ah-OPEN gate ran on the same tree, and the three commits
  since are docs. The 12ah-OPEN gate (12ah_open/gate_12ah_open/, 978.22 s) is the session's
  gate of record and the reference for 12ai's.
R815(f) — This ruling states no standing rule: the id list, the glosses file and
  STANDING_RULES.md are untouched at the item 0 that transcribes it, asserted by content.
  Session 12ah closes by the brief below and a wrap ruling after its report (R814(e)).
===== END r815.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ah-CLOSE] — Close session 12ah, in 12ag-CLOSE's form read from
  the record: item 0 (R815 as the next block; this brief committed as pasted; no standing
  rule), then the closeout commit (the closure paragraph, the "Expected values at
  session-12ai open" block, 12ah_close/'s measurements, any job run since the 2026-11-18
  pair, the band computation, the recount, R510, retention, the lane line drafted and not
  applied). Docs only. No engine edit. No live write. No model call. No closeout gate
  (R815(e)).
PURPOSE: Leave the record in the state the next session opens on: every fact of 12ah in
  the plan's closeout block with its evidence file beside it, the expected values for
  12ai's open stated from the 12ah-OPEN gate and the new window, and the band figure
  computed for the wrap ruling.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 213; then the closeout block and the
  12ai expected-values block); docs/session-reports/session-12/12ah_open/r815.txt; a new
  directory docs/session-reports/session-12/12ah_close/ (the counterpart files); a new
  directory docs/session-reports/session-12/12ah_CLOSE/ (brief_12ah_CLOSE.md, at item 0);
  ~/scratch/12ah-close/ (the lane line draft and scratch); the claude-config ledger; git
  push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/, the codebook, docs/architecture/, CLAUDE.md, docs/inventory/, the tools
  (no gate and no unit is launched); the id list, the glosses file and STANDING_RULES.md
  (not one byte, R815(f)); 12ag_close/, 12ah_open/'s existing files, 12ah_OLRA/ and every
  earlier close or open directory (read, never rewritten); the dated units lines already
  in the plan; the lane line in the primers (drafted to scratch, applied only by the wrap
  ruling); review.db (mode=ro instruments only); ollama.service and the health job
  (read only).

FORM (R784(g)(iv)): docs/session-reports/session-12/12ag_CLOSE/brief_12ag_CLOSE.md
  (sha256 1218ef13735534f177b1df7759c88d8e94db2afc43b0e875dff58fc68df8a1cb, 339 lines),
  Section 2, is this brief's form, with the forms it names in turn (12af_CLOSE/
  brief_12af_CLOSE.md, then 12ae_CLOSE/brief_12ae_CLOSE.md) — steps 0, 1 and 2 (2a–2h),
  the acceptance gates, constraints, final report format and contradiction handling,
  read whole before anything is composed, with the 12ag closeout it produced
  (35a914b2bdd75147af104e5abfaea388eae3a057: the closure and the "Expected values at
  session-12ah open" block; 12ag_close/'s twenty-three files, plus those 12ah-OPEN added
  there) as the worked example. This brief states only what differs. Where the forms and
  this brief differ, this one governs; where a form names a 12ag source, the 12ah source
  listed below takes its place. A step of the form with no 12ah source is reported as
  not done, with its reason. The item-0 model for one block is 12ag-GATEUNIT-B's item 0
  less its rules regeneration; a single block's wording is Block 201's.

TIMING: issued at 06:30 UTC on Thursday 2026-11-19. Valid outside the day's window
  (R812(a)); today's opens at 09:00 UTC. The clock is read before each commit. If it
  reaches 08:45 UTC before a commit, hold with the work on disk uncommitted and resume in
  this brief after 10:35 UTC; say so. Wherever a form says "07:00–10:35 UTC", "06:45
  UTC" or "before 07:00 UTC", read the day's window and its opening (R812(b)).

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ah-OLRA-REC-R1 final report; R815): HEAD
    6dc36b312879bdd16c682093d86a9e2b3c331c86, parent
    c4120330c7c8102d89d6c8298363e2624c7aca24, clean, level; claude-config at
    b0bb0bad95c64d0c70c90eb2db01bfb5c12dc4f0 or an accepted descendant (R663(b)). From
    the 12ah-OPEN final report (R813): the gate 12ah_open/gate_12ah_open/, 3,573 / 1 / 17
    on 3,591 / 3,574, 978.22 s, launched as a unit, no hold.
  READ (the architect's clone at 6dc36b3, 2026-11-19 06:20 UTC; re-read on the box):
    R1. The plan: sha256 ec03dc26cf44a022233afd534dbef064176abc13054fd93c7cf85c4325f2366e,
        3328199 bytes, 19766 lines; last line "The decision log in this file ends at
        R814."; last block 212; 169 lines begin "The decision log in this file ends at
        R". "Expected values at session-12ah open" starts at plan line 5678 and "##
        Decision log" is at 5717.
    R2. Rules files: STANDING_RULES.md
        d56dbdaeafb3659527cb77e1e81744eb01dadbe871fd96944104bb9e01b7e3ff; the id list
        a28da0611e1a4d62cbc8185ff89df22d2df7a2a37340da0eb164dec678e75257; the glosses
        7f7374b25f7efd2c1df97cfbe3fb9b828c7fec5bce4e948d751cec08a4fa72ed.
    R3. The session's commits, git log 35a914b..HEAD, three: 34ff8a7, c412033, 6dc36b3.
        None is outside docs/plan and docs/session-reports/session-12. The session
        opened on 35a914b2bdd75147af104e5abfaea388eae3a057, the 12ag closeout. git diff
        --stat 35a914b HEAD on engine tests scripts analysis review_specs and
        docs/session-reports/tools is empty.
    R4. The session's rulings: 12ag_close/r811.txt (made at the 12ag wrap, transcribed
        at 12ah-OPEN), 12ah_open/r812.txt, r813.txt and r814.txt on disk, R815 in
        Section 1. One ruling stated a standing rule: R812(a).
    R5. Six-cell "| INT-" rows in the plan: 213. No row was opened or closed and no fix
        unit landed in 12ah; the last dated units line is 12af's.
    R6. 12af_close/recount_12af_close.py's committed JSON is
        3657ce2f219c9fc130d5248295e99071855245d87dc2b7ab6d80e9b5c16d15a5.
    R7. gate_wall.txt reads "wall: 978.222568504 s" in 12ah_open/gate_12ah_open/, "wall:
        982.433030153 s" in 12ag_GATEUNIT_B/gate_12ag_unit/ and "wall: 1042.650800830 s"
        in 12ag_open/gate_12ag_open/; 12af_U29B/gate_u29b/gate_wall.txt holds U29B's.
        12ah_open/gate_12ah_open/ holds collected_ids.txt and durations_12ah_open.txt.
    R8. 12ag_close/ now holds 42 files: the closeout's twenty-three, r811.txt,
        lane_line_12ag.txt and the 2026-11-15 to 2026-11-18 job reads. It holds
        lane_line_12ag.txt (the lane line's form) and host_state_close.txt.
    R9. 12ah_open/between_sessions_read.txt holds the readings the closure cites for the
        host: the health job's schedule and script (dgx-infra commit b5af93d), the
        snapshot timer, and ollama.service's start, MainPID, RestartUSec and keep-alive.
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. The engine tree is unchanged since 977c499.
    I2. transcribe_ruling.py --append only appends.
  INFERRED — expectation (report a mismatch, continue with the measured value):
    E1. The recount by 12af_close/recount_12af_close.py on the plan at the item-0 commit
        reads 287 (94 / 57 / 136, 0 unclassed), the dated lines' fix units 38, done 33,
        its JSON byte-equal to the committed one; R510 for 12ah is in 0, out 0, the
        six-cell INT- rows 213 at 35a914b and at the item-0 commit.
    E2. The band computation, from the last four standard gates on this host state —
        U29B 979.95, 12ag-OPEN 1042.65 (confounded, R805(b)), the 12ag unit gate 982.43
        and the 12ah-OPEN gate 978.22, each read from its gate_wall.txt — gives a mean
        of 995.81 s and, rounded inward, 897–1095 s. The series has forty-three members.
    E3. git diff --stat on engine tests scripts analysis review_specs is empty from
        35a914b and from 977c499; from f4a5586 it is 96 files, +6,609 / −345.
    E4. Retention 23 MATCH / 0 MISMATCH by file sha256, none opened, no new entry.
    E5. The fingerprint IDENTICAL; dbchecks_close.txt, pin_tuples_close.json and the
        codebook and spec hashes byte-identical to 12ag_close/'s.
    E6. No nightly later than 2026-11-18's has run when 2b reads. A 2026-11-19 health
        job, if it has run, is a light check with no model load.
    E7. The host's four band values equal 12ag_close/host_state_close.txt's. If one
        differs, the band paragraph says so and the figure is computed and marked.

CONTRACT (the form's steps, with these 12ah terms; evidence under ~/scratch/12ah-close/):

  STEP 0 — Pre-check, as the form's: HEAD and parent proved by git cat-file -e
    <hash>^{commit}; clean; fetch; 0 0; one worktree. R1, R2, R5, R8. claude-config: the
    MEASURED commit or an accepted descendant, level; a pending foreign row carried and
    named. I1. No gate-* unit loaded. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (the model quoted before composing; the check written for this brief,
    R794(i)).
    1a. 12ah_open/r815.txt from Section 1, one write, hashed straight after against
        Section 1's hash. A mismatch = STOP (R738(b), R739(b)).
    1b. block213.json (R815): made_by PI; made_date 2026-11-19; session 12ah; report as
        its STATUS names it, "the 12ah-OLRA-REC-R1 final report (item 0 <full hash>)",
        the hash in backticks; report_short the same with the short hash; item "item 0
        of the 12ah-CLOSE brief"; sentence model, followed "12ah-CLOSE brief"; "beside"
        "the brief file it orders", in the model's words, quoted; transcribed = the
        commit's UTC date; prev_ends R814. Render, compare the element order with the
        model's, then --append.
    1c. No row note and no dated units line: no unit has landed, and R805(d) and R807(c)
        place their notes at and after U30's build.
    1d. The brief file (R784(g)(v)): 12ah_CLOSE/brief_12ah_CLOSE.md from this whole
        pasted block, "CLAUDE CODE" through the STOP-12AH-CLOSE line, one trailing
        newline, written once; sha256, bytes and lines straight after the write,
        reported.
    1e. The item-0 check, as the form's 1e with these terms: the last line "…ends at
        R815." once; the plan equals HEAD's plus Block 213 appended, nothing else (the
        "−" count read and matched to zero); six-cell INT- rows 213 before and after;
        r815.txt equal to 1a; the three rules files at R2's hashes and unstaged; the
        staged set exactly the plan, r815.txt and the brief file.
    1f. <check> && <commit>, one line; then fingerprint, push, level and the ledger row
        as the form's 1f; the ledger message built from the diff.

  STEP 2 — The closeout commit, every element in the form's order, each with its file
    under 12ah_close/ in 12ag_close/'s counterpart name; a name with no 12ah source is
    listed as not written, with the reason.
    2a. Measurements as the form's 2a, dated names carrying the read's UTC date.
        Differences: the diff files are diff_engine_since_35a914b.txt,
        diff_since_977c499.txt and diff_since_f4a5586.txt (E3); no tools diff file is
        written, the tools being unchanged (R3), and the closure says so;
        collected_ids_12ah_close.txt is a copy of 12ah_open/gate_12ah_open/
        collected_ids.txt and durations_12ah_open.txt a copy of that gate's durations
        file, each cmp equal to its source, the ids also compared with
        12ag_close/collected_ids_12ag_close.txt; dbchecks, pins and the codebook and
        spec hashes compared with 12ag_close/'s (E5); the host state with E7, and in it,
        as readings, ollama.service's start time, MainPID, NRestarts, RestartUSec and
        OLLAMA_KEEP_ALIVE (no other environment value), the health job's crontab line
        and its script's sha256, the user manager's state and the absence of any gate-*
        unit.
    2b. The jobs (E6): every health job and nightly run after the 2026-11-18 pair and
        before this read, read as 12ah-OPEN's step 1 read 2026-11-15 to 11-18, their
        files into 12ah_close/ in those names, a light check's file saying which form
        ran. Expected: none, or the 2026-11-19 light check alone. A nightly found is
        read against 12af_close/expected_nightly_20261111.md with its HEAD by the
        reflog method; a nightly mismatch = STOP before the closeout commit, both
        values reported. The jobs paragraph says the 2026-11-15 to 11-18 pairs were
        read at 12ah-OPEN (their files in 12ag_close/), which file governs the later
        nightlies, and that from 2026-11-16 the health job is a light check on Monday
        to Saturday and the full sweep on Sunday (R813(b)).
    2c. The recount (E1): no row was opened in 12ah, so no new recount script is written
        (R678(f)); recount_12af_close.py is run in a scratch clone on the plan at the
        item-0 commit, with the arguments its docstring names. r510_12ah.txt in
        r510_12ag.txt's form (the open's copy in ~/scratch/12ah-open/ is a reading; this
        one is computed at the item-0 commit). A row count other than 287, dated lines
        other than 38 / 33, or R510 other than 0 / 0 = STOP before the closeout commit,
        both values reported.
    2d. band_computation.txt in 12ag's form: the four inputs of E2, each with its
        evidence path, its gate_wall.txt value, how it was launched (tmux for the first
        two, a user unit for the last two) and the concurrent jobs from its snapshots;
        the confounder of 12ag-OPEN's as R805(b) records it; mean, ×0.9, ×1.1, the
        inward rounding, the figure; the series of forty-three listed — 12ag's forty-two
        plus 978.22, read from its file; "a figure, to be set by the wrap ruling; the
        band in force stays 894–1092 s".
    2e. No expected-nightly file is written: the engine tree is unchanged since 977c499,
        so 12af_close/expected_nightly_20261111.md governs each later nightly (R749(b)).
        Say so where the form's 2e stands.
    2f. The lane line, ~/scratch/12ah-close/lane_line_12ah.txt, in
        12ag_close/lane_line_12ag.txt's form: its five "−" lines are
        ~/claude-config/primer.md's lines 41–45 as they stand, taken from primer.md
        itself; its five "+" lines carry 12ah's sources — HEAD the closeout commit
        (filled after 2h's commit, so the file is written last, is not in the commit,
        and is pasted whole in the report), the plan's sha256, bytes and lines, the
        decision log ending R815 (R816 at 12ai-OPEN's item 0), and the session's
        summary: no fix unit landed; the no-live-write window ruled by UTC day
        (R812(a)), its basis measured (R813(b)); one standing rule; the interim
        read-out 12ah-OLRA committed with its evidence (6dc36b3) after a credential
        scan; the restart change's design not yet ruled; 287 rows; fix units 38, done
        33; one stop, no pause; one gate and the band figure; pins unchanged; no live
        write; the next steps — 12ai-OPEN, then the restart change's design ruling and
        its build, then U30's build with N46, then U31–U36. Each "+" line read whole
        after writing, every word separated from its neighbour and its code marks
        paired. Not applied.
    2g. The closeout block and "Expected values at session-12ai open", placed after the
        12ah expected-values block's last line and before the decision-log section (the
        position read and matched, R1), in the 12ag block's element order with 12ah's
        content: the closure paragraph (opened on 35a914b; this CC session's id and
        times read from the record; the interim CC session c72bc042 before the open,
        read-only, counted with this session, R813(f)); the briefs in order — the
        interim session's four under 12ah_OLRA/, 12ah-OPEN, 12ah-OLRA-REC and its resume
        R1, this closeout — each with the ruling at its item 0 or carried in it and its
        committed file's sha256 as the rulings record its match with the architect's
        copy (the interim briefs' as their own lists record them); the commits in order
        from git log 35a914b..HEAD with their ledger commits; the diff lists; the
        rulings R811–R815 one line each (R811 made at the 12ag wrap and transcribed at
        12ah-OPEN); the rows (none opened, none closed); the stops, listed from the
        rulings (one: 12ah-OLRA-REC at its step 2c, R814(a), the architect's; the first
        paste of 12ah-OPEN stopped on its own TIMING and is not a stop of the session,
        R813(a)), the pauses (none), and the architect's errors as the rulings count
        them (three, R814(c)); the tools paragraph (no tools change; the standing-rules
        file regenerated once; the tools items carried from 12ag unchanged, and with
        them two more, R813(e): the open check's health section cannot read a
        light-check day, and the open check has no key for the Ollama service start);
        one paragraph for the window (R812 as ruled, what it amends, its verification
        from the health job's schedule and script, R813(b); CLAUDE.md's line stale and
        its correction owed, R812(c)); one for the host changes made in the dgx-infra
        lane, from R9's file (the Ollama restart, the keep-alive, the restart delay,
        the health job's two modes), each a reading; one for the interim read-out (the
        session, the twenty-seven files, the scan and its control, the lists verified,
        the two files from other lanes, R813(d), R814(b), R815(b)–(c)), saying that the
        restart change's design is not ruled in 12ah and that nothing in this closure
        states it; "Carried forward" — the restart change's design ruling from
        12ah_OLRA/12ah_OLRA.md and its addendum, then its build as one engine-commit
        brief; U30's build with N46 and the notes R805(d) and R807(c) order; then
        U31–U36; the items the 12ag closure carried that 12ah did not touch, read from
        that closure and listed; the cross-lane readings for dgx-infra (the eighteen
        NV_ERR_NO_MEMORY lines of the 2026-11-15 sweep; other lanes' model loads
        outside the health windows; other lanes' commits in claude-config), R813(e);
        the gates table (the 12ah-OPEN gate); the band paragraph; the jobs paragraph
        (2b); the recount paragraph; retention; ledger; counterpart files; the host;
        live; "Session 12ah closed. Next: session 12ai, fresh."; then the 12ai
        expected-values block in the 12ah block's form, with these differences: the
        references are collected_ids_12ah_close.txt and durations_12ah_open.txt; the
        band figure of 2d; the nightlies per 12af_close/expected_nightly_20261111.md
        from 2026-11-19 on, with whatever 2b read; the health jobs as readings in their
        two forms, the count of NV_ERR_NO_MEMORY lines this boot as 2a reads it;
        Ollama's service start, MainPID and keep-alive as readings from 2a; "The
        decision log in this file ends at R815" and R816, the wrap ruling, at
        12ai-OPEN's item 0; the rules files' three hashes (R2); standing rules stated
        in 12ah: R812(a); the open's timing line reading "outside the day's window
        (R812(a))" where the 12ah block says "outside 07:00–10:35 UTC"; the four 12ah
        scratch directories (12ah-open/, 12ah-olra/, 12ah-olra-rec/, 12ah-close/); 12ai's
        first items — 12ai-OPEN, then the restart change's design ruling and build, then
        U30's build. Every figure from a file in 12ah_close/ or an evidence directory,
        named beside it; nothing from this brief's text where a file exists.
    2h. The closeout check and commit as the form's 2h, with these terms: the last "ends
        at" line "…ends at R815." once; the staged set exactly the plan and 12ah_close/;
        the rules files, 12ag_close/ and every earlier close directory unchanged; the
        message in 35a914b's form. Then 2f's lane line completed and pasted whole in
        the report.

ACCEPTANCE GATES: the form's G1–G3 with these terms — I1 held; item 0 one commit that
  passed 1e in one chain with r815.txt at its hash and Block 213 by the tool; the
  closeout commit with every element of the 12ag block present in order with its 12ah
  source, every counterpart file written or listed as not written with its reason, the
  recount at E1 or stopped, any nightly found matched or stopped, the band computed from
  the files, E3–E7 held or reported, the check passed in one chain; both pushed, level,
  fingerprint IDENTICAL; ledgers; the lane line pasted whole, not applied.

OUT OF SCOPE: the wrap ruling (R816: the band set, the lane line approved and applied,
  the stops counted — after this report); the restart change's design or build; U30's
  build; any engine, test, script, docs/architecture, CLAUDE.md or tools edit; any gate
  or unit; the tools items; /wrap, /exit and the archive (after the wrap ruling).

CONSTRAINTS (task-only): no model call, no provider call, no live write; a count arrived
  at by eye is INFERRED, the computed value given with its command; every figure in the
  block beside its file; where this brief or its form describes a tool or record, the
  record governs and a difference is a finding (R732(a)).

FINAL REPORT FORMAT: the form's — (0) pre-check table; (1) item 0; (2) the closeout: the
  measurements with their values, any job read, the recount output, band_computation.txt
  verbatim, the check verbatim, chain, hashes, fingerprint, ledgers, the plan's sha256,
  bytes and lines, the two blocks' line ranges in the plan (the blocks themselves need
  not be pasted, being committed), the lane line verbatim with its sha256, bytes and
  lines and its fifth "+" line quoted whole; (3) every place this brief's FORM clause
  left a choice, and the choice made; (4) contradictions and findings, numbered, each
  with its quoted line; (5) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 or I2 false, a 1a mismatch, or a check
  failure or crash at 1e = STOP before the item-0 commit. A recount or R510 off E1, a
  nightly mismatch, or a check failure at 2h = STOP before the closeout commit, the work
  on disk reported. An expectation mismatch (E2–E7) is reported and the closeout
  continues with the measured value in the block. A spec premise that does not survive
  contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not apply the lane line, do not run /wrap, do not start
  12ai, the restart change or U30. Await the wrap ruling in this session.
  STOP-12AH-CLOSE
