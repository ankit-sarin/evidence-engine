CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R808, PI ruling 2026-11-14 UTC; not on disk)
================================================================================

R808 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the next
block of the decision log. The lines between the two marker lines, exclusive of the
markers, are the byte content of r808.txt. Write the file from those lines exactly:
same characters, same line breaks, one trailing newline. Never retype a ruling. The
architect's independent hash:
  r808.txt: sha256 801dfd05bf83d6fd6400a8195594a2b734229a7e173f726f441f852037c275c7,
            4737 bytes, 58 lines, one trailing newline.

===== BEGIN r808.txt =====
RULING [evidence-engine 12ag-R808] — 12ag-GATEUNIT-A accepted; the gate launches as a
  systemd-run --user unit: the design on the PI's word; the evidence set stays nineteen; one
  standing rule on the launch line's record; readings; the tools change is built and validated
STATUS: PI ruling, 2026-11-14 UTC, session 12ag, on architect recommendation, on the
  12ag-GATEUNIT-A final report (item 0 12c11080e8e83685b20817f66868083ee21e6782; read-out
  e5055c67ec33fd5cf62025200e4d56dc42f068b9). Transcribe at item 0 of the 12ag-GATEUNIT-B brief
  below, by transcribe_ruling.py, in the block after R807's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12ag-GATEUNIT-A final report, quoted): "Commit: 12c11080e8e83685b20817f66868083ee21e6782
    (parent 19b1f01)"; "Ledger: 55bdba19f0c94865059972be410d805d48f7b01b (parent a4d4296)";
    "Commit: e5055c67ec33fd5cf62025200e4d56dc42f068b9 (parent 12c1108)"; "Ledger
    e9d4e4c2ff5f1372662edba468758bcdc89abc54"; "Plan now:
    a61e2e974d53a15be4f81a78fa2c03647716bb3fa1351fca7012cffca63d9377, 3,235,878 B, 19,043 lines.";
    "brief_12ag_GATEUNIT_A.md is 33ab527def2c53c21866ed20b6545feabe59446e1a4d77147b6dcbe43a009ac7,
    20,507 B, 281 lines" (the architect's copy: the same sha256, bytes and lines); "tmux appears
    only in launch_gate.sh:8 (the refusal), :36 (the launch line) and :38 (its eval), plus the
    comment at run_gate.sh:2."; "A caller waits on gate_rc.txt. That does not change with the
    launch."; "Linger=yes. systemd 255."; "DefaultOOMPolicy=stop, DefaultLimitNOFILESoft=1024
    (this shell has 524288)"; "the unit has LANG en_US.utf8 (this session C.UTF-8), no .venv on
    PATH, no VIRTUAL_ENV, and neither provider key".
  READ (CC's): findings 1–9; the census's classes, CC's judgement; what could not be established
    without launching a unit.
  READ (the architect's clone at e5055c6): 12ag_GATEUNIT_A/12ag_GATEUNIT_A.md §8 option (A), "The
    launch line: `systemd-run --user --unit=<unit> --collect [--property=…] bash $E/run_gate.sh
    $E <name> $REF $REFD $*`, held in `LINE`, written to `launch_line.txt` and run (R738(c)
    unchanged)"; run_gate.sh:10 ".venv/bin/python -m pytest"; the plan's sha256 as the report
    gives it, last line "The decision log in this file ends at R807.".
  INFERRED: none.

R808(a) — The 12ag-GATEUNIT-A final report is accepted as printed: item 0 12c1108 (Blocks 204
  and 205; the rules files untouched; the brief file equal to the architect's copy), ledger
  55bdba1; the read-out e5055c6 with its census and captured reads, ledger e9d4e4c. No unit was
  launched and no script run. CC's two slips (finding 9) are readings.
R808(b) — The design, the PI's ruling on the five recommendations put to him, in his words:
  "Agree with your recommendations". (1) launch_gate.sh launches the gate as a transient
  systemd-run --user unit in place of tmux, the read-out's option (A); there is one launch
  form. (2) The unit has no memory cap, and the launch record says so. (3) The launch line
  gives the unit the launching shell's open-files limit and its LANG; no provider key and no
  other variable is passed. (4) The change is validated by a dry run in 12aa-SETUP's form and
  then by one standard gate under the hold; that gate's wall time is a reading, and how it
  enters the series is ruled after it is read. (5) The standing rule of (d).
R808(c) — The evidence set stays the nineteen files of R738(d). The global rule's status
  report and Linger confirmation are Claude Code's at each launch: the launcher prints them
  after the launch and the report carries them. A twentieth file is not added; it would change
  a standing rule and has not been put to the PI.
R808(d) — Rule from here, restating what R731(b) records: every gate's evidence directory
  records the complete launch line as it was run, the launcher's own command included,
  whatever launches it. From this change the launcher is systemd-run --user. R738(c)'s form
  is unchanged: one variable holds the line, is written to launch_line.txt and is then run.
R808(e) — Readings, no row: the user manager reads degraded on two failed units, neither this
  lane's, and nothing is cleared (the cross-lane item for dgx-infra); gates launched from a
  Claude Code session have carried both provider keys in their environment, read by no test,
  and a unit carries neither; the cron nightlies' 984–986 s beside the tmux gates' 963–980 s.
  R804(b)'s exception was used once and is spent.
R808(f) — This ruling states one standing rule, (d). The id list gains "A R808(d)" and
  STANDING_RULES.md is regenerated at the item 0 that transcribes this ruling (R766(b)).
  R731(b) stays in the list; R808(d) is read with it.
===== END r808.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ag-GATEUNIT-B] — the tools change: launch_gate.sh launches a gate
  as a transient systemd-run --user unit. Item 0: R808 as the next block; "A R808(d)" on
  the list and STANDING_RULES.md regenerated; this brief committed as pasted. Then a probe
  unit, the edit, a dry run, the tools commit, one standard gate under the unit, and the
  evidence commit.
PURPOSE: Make the gate's launch conform to the global long-running-jobs rule, and prove
  the new launch on one test file and then on the whole suite before any unit's build
  depends on it. Code task on the tools only. No engine, test, script, spec or codebook
  change. No live write. No model call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  This ruling authorizes transient systemd-run --user units, launched by Claude Code, for
  the probe (step 2), the dry run (step 4) and the gate (step 6), and nothing else of
  systemd.
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 206 appended by the tool, nothing
  else); the three rules files (one id line, one gloss, the regeneration);
  docs/session-reports/session-12/12ag_open/r808.txt;
  docs/session-reports/tools/launch_gate.sh, run_gate.sh (its line-2 comment only) and
  README.md; a new directory docs/session-reports/session-12/12ag_GATEUNIT_B/;
  ~/scratch/12ag-gateunit-b/; the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/, the codebook, docs/architecture/, CLAUDE.md, docs/inventory/; the other
  tools; review.db (mode=ro instruments only); no systemctl reset-failed, enable, disable
  or set-environment, no loginctl enable-linger, no system unit, no edit under
  ~/.config/systemd/ (the degraded state is read, never cleared); no provider key's value
  is printed or written anywhere — key names only; crontab; other lanes' units, processes
  and files; the working tree inside 07:00–10:35 UTC (R86).

FORM: the item-0 model is 12ag-N46A's item 0, dcf623e, single-block (its check, its block
  files and its regeneration command are in ~/scratch/12ag-n46a/; a single block's wording
  as Block 201's). The validation's form is 12aa-SETUP's: 12aa_SETUP/dry_run/ and
  2d_check.txt. The gate's form is 12ag-OPEN's step 3e and its evidence
  12ag_open/gate_12ag_open/. Quote each model before composing its counterpart.

TIMING: issued at 21:00 UTC on 2026-11-14. Valid outside 07:00–10:35 UTC. The gate must
  exit before 07:00 UTC: if step 6 cannot launch by 06:30 UTC, the hold included, STOP
  before it and report. If the clock reaches 06:45 UTC before a commit, hold uncommitted
  and resume after 10:35 UTC (R789(a)).

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ag-GATEUNIT-A final report; R808): evidence-engine HEAD
    e5055c67ec33fd5cf62025200e4d56dc42f068b9, parent
    12c11080e8e83685b20817f66868083ee21e6782; claude-config at
    e9d4e4c2ff5f1372662edba468758bcdc89abc54 or an accepted descendant (R663(b)); on this
    box Linger=yes, systemd 255, the launching shell's open-files limit 524288 and its
    LANG C.UTF-8, a unit's default soft limit 1024.
  READ (the architect's clone at e5055c6, 2026-11-14 20:59 UTC; re-read on the box):
    R1. The plan: sha256 a61e2e974d53a15be4f81a78fa2c03647716bb3fa1351fca7012cffca63d9377,
        3235878 bytes, 19043 lines; last line "The decision log in this file ends at
        R807."; last block 205; 162 lines begin "The decision log in this file ends at R".
    R2. Rules files: STANDING_RULES.md 9c037e86…1a51; the id list 5c301ffb…a91e (it holds
        "A R731(b)", "A R738(c)" and "A R738(d)"); the glosses 24b6fce3…abc8.
    R3. The tools at HEAD: launch_gate.sh
        8a7500b9e8f457bd7678d067bab36b6e118f2cb6d2819ee6ff00f115c09dc741 (39 lines),
        run_gate.sh 06c823c5775c188928cf80b6c30056d4dc1d307aa249fcb1b392e4bf27cfb889 (50
        lines), README.md 29c127145323097265224018ab4b210debfdf5c6f5f9ab11b2a8ff9bf38aebae.
        launch_gate.sh :8 is the tmux refusal, :36 sets LINE to the tmux line, :37 writes
        launch_line.txt, :38 is "eval "$LINE"", :39 "cat "$E/launch_line.txt"". README :11
        says run_gate.sh is called "inside tmux".
    R4. 12ag_GATEUNIT_A/12ag_GATEUNIT_A.md §8 option (A) is the design's source; its §6
        census is the list of what the present launch touches. A grep of tests, engine
        and scripts for "launch_gate" or "run_gate" finds nothing.
    R5. The gate's references are those of 12ag-OPEN: 12af_close/collected_ids_12af_close.txt
        and 12af_close/durations_12af_u29b.txt; the counts are the 12ag block's.
    R6. Six-cell "| INT-" rows in the plan: 213.
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. The engine tree is unchanged since 977c499.
    I2. transcribe_ruling.py --append only appends.
    I3. A transient user service starts and runs on this box while the user manager reads
        degraded (step 2 measures it; false = STOP before any edit).
  INFERRED — expectation (report a mismatch, continue): none. Whether the suite needs more
    than 1,024 open files, and the unit gate's wall time, are to be measured (R707(a)).

CONTRACT (steps, in order; evidence under ~/scratch/12ag-gateunit-b/):

  STEP 0 — Pre-check (read-only). date -u. HEAD and parent proved by git cat-file -e
    <hash>^{commit}; clean; fetch; 0 0; one worktree. R1, R2, R3, R6. claude-config: the
    MEASURED commit or an accepted descendant, level; a pending foreign row carried and
    named. I1. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; the check written for
    this brief). r808.txt into 12ag_open/ from Section 1, one write, hashed straight
    after against Section 1's hash (mismatch = STOP). block206.json: R808, made
    2026-11-14, session 12ag, report the 12ag-GATEUNIT-A final report with its item-0 and
    read-out hashes, followed "12ag-GATEUNIT-B brief", prev_ends R807, beside naming the
    standing-rules line, the regeneration and the brief file. "A R808(d)" appended to the
    id list; one gloss, composed by CC in the file's form from the clause's words and
    quoted; STANDING_RULES.md regenerated by dcf623e's command. The brief file:
    12ag_GATEUNIT_B/brief_12ag_GATEUNIT_B.md from this whole pasted block, "CLAUDE CODE"
    through the STOP-12AG-GATEUNIT-B line, one trailing newline, written once, hashed
    straight after, reported. The check: as dcf623e's, with R766(b)'s classification of
    every changed line of STANDING_RULES.md (any other changed line = STOP); the last
    line "…ends at R808." once; INT- rows 213 before and after; the staged set exactly
    the plan, the three rules files, r808.txt and the brief file. <check> && <commit>,
    one line; fingerprint before and after the push; the ledger row, built from the diff.

  STEP 2 — The probe unit (I3; before any edit; nothing of the repository runs in it).
    One transient unit, named gate-probe-12ag, launched by systemd-run --user --collect
    with the two settings step 3's line will carry (the open-files limit and LANG, each
    taken from this shell at that moment), its command a short shell that writes to a
    scratch file: its own soft and hard open-files limits, its cgroup, its oom_score_adj,
    its LANG and PATH, and the NAMES only of its environment variables. Report: the
    systemd-run line, systemctl --user status for the unit, the Linger line, the scratch
    file, and the unit's start and stop lines from the user journal. A launch that
    fails, a unit that does not run, or a limit or LANG that is not what the line set =
    STOP before step 3, the output reported.

  STEP 3 — The edit (three files; the contract, not the code).
    C1. launch_gate.sh launches run_gate.sh as a transient unit: systemd-run --user, the
        unit named gate-<second argument>, --collect, the open-files limit set to the
        launching shell's own soft limit and LANG set to the launching shell's own, both
        read at launch and neither a literal; no memory property; no other variable
        passed. The command the unit runs is the present inner command, unchanged, so
        gate_launch.txt's second line still equals the launch line's inner command.
    C2. R738(c) holds as now: one variable holds the whole line, launch_line.txt gets the
        timestamp sentence and that variable's value, and that variable is then run. The
        timestamp sentence also says the unit has no memory cap (R808(b)(2)).
    C3. The refusal: where the script now refuses an existing tmux session of the name,
        it refuses when a user unit of the gate's name is loaded in any state, with a
        REFUSED line and exit 2, nothing launched. The existing-gate refusal is unchanged.
    C4. After the launch it prints, in this order: launch_line.txt; systemctl --user
        status for the unit; the Linger line for this user. A systemd-run that fails
        ends the script non-zero with its message, and no gate_rc.txt is written.
    C5. Everything else in launch_gate.sh is byte-identical: the before-gate reads, the
        six-second wait, the files written and their names. The set stays nineteen files
        (R808(c)). No tmux call remains.
    C6. run_gate.sh: its line-2 comment says what launches it; every other byte equal.
    C7. README.md: the two scripts' rows state the unit launch; one dated note in the
        file's form (this change, R808, what was validated and where). Nothing else.

  STEP 4 — Validation (TESTS REQUIRED; 12aa-SETUP's form; evidence
    ~/scratch/12ag-gateunit-b/dry_run/ and its check's output).
    4a. Fail-first: the check's launch-form assertion (4c: the line begins with
        systemd-run --user and names the unit), run on the committed
        12ag_open/gate_12ag_open/launch_line.txt, must FAIL: that file holds a tmux
        line. An assertion that cannot fail is reported UNPROVEN.
    4b. The dry run: the edited launcher on tests/test_u20_modelfile_num_ctx.py, name
        unit_dry (so the unit is gate-unit_dry), references "-" "-", its
        basetemp under scratch as the script provides. Wait on gate_rc.txt, one wait.
    4c. The check asserts, each from the files and the captured output: the launch
        line's inner command equals gate_launch.txt's second line; the selection is in
        the line; the nineteen files by name; the basetemp under scratch; the launch
        line begins with systemd-run --user and names the unit; it carries the
        open-files limit and LANG this shell had and no memory property; the printed
        status names the unit; Linger=yes; gate_rc.txt holds the dry run's exit code
        and that code is 0; no tmux session of the name exists.
    4d. Negative controls, each must refuse with exit 2 and launch nothing: a second
        launch into the same evidence directory; a launch whose unit name is already
        loaded (hold one open with a second probe unit of that name, then let it end).
    4e. git diff of the three files, whole, in the report; C5 and C6 asserted by the
        check from the diff (no changed line outside the launch block, the refusal, the
        prints and the comment — computed, the hunks listed).
    A failed assertion or control = STOP before step 5 with the tree's edit left in
    place uncommitted and reported; nothing is reverted without a ruling.

  STEP 5 — The tools commit. <check> && <commit>, one line: the staged set exactly
    launch_gate.sh, run_gate.sh and README.md; its numstat read into the evidence. A
    commit of its own, tools scope, the message from the staged set. Fingerprint before
    and after the push; push; level; the ledger row.

  STEP 6 — The standard gate under the unit (unattended; a later step depends on it).
    Its home: the transient unit gate-12ag_unit, no memory cap, launched by the edited
    launch_gate.sh from the repository root with evidence_dir
    ~/scratch/12ag-gateunit-b/gate_12ag_unit/, name 12ag_unit, R5's two references, no
    selection. The hold is the standing rule's (R805(f)). One wait on gate_rc.txt.
    Report at launch what C4 printed. EXPECTED (acceptance): the 12ag block's counts line
    for line, exit 0, ids 0 / 0, the nineteen files by name against
    12ag_open/gate_12ag_open/'s, no model load in the gate's journal window, the unit's
    start and stop lines in the user journal. Wall time: to be measured; reported beside
    the band 872–1065 s, the series of forty-one and the concurrent jobs, matched to
    nothing, and never a stop. Durations against the reference, any phase slower by
    more than 10 s named. A red, a model load in the window, a unit that fails or is
    killed, or a missing gate_rc.txt at the wait's bound = STOP before step 7, the
    unit's status and journal reported; the tools commit stands and is not reverted
    without a ruling.

  STEP 7 — The evidence commit (R731(c)). Into 12ag_GATEUNIT_B/: the probe's files, the
    dry run's directory and its check with output, the negative controls' output, and
    gate_12ag_unit/'s nineteen files; every copy sha256-equal to its source; the check
    asserts the staged set and reads its numstat; <check> && <commit>; fingerprint
    before and after the push; the ledger row. Record hashes and parents.

ACCEPTANCE GATES:
  G1. Step 0 matched; item 0 committed in one chain; the rules regeneration within
      R766(b)'s three kinds.
  G2. The probe unit ran with the limit and LANG its line set (I3 true).
  G3. The dry run's check passed every assertion of 4c; 4a failed as it must; both
      controls of 4d refused.
  G4. The tools commit holds three files and no changed line outside C1–C7.
  G5. The unit gate: the 12ag block's counts, ids 0 / 0, nineteen files, no load in its
      window; its wall time reported, not matched.
  G6. The evidence commit: every copy equal to its source; pushed, level, fingerprint
      IDENTICAL.

OUT OF SCOPE: a tmux window following the unit's log; a memory cap; a twentieth evidence
  file; session_open_check.py and every other tool; the other tools items the plan
  carries; clearing the user manager's failed units; any ruling on the band or the
  series; U30's build; /wrap.

CONSTRAINTS (task-only): no live write, no model call. Any pytest run outside the
  standard gate passes --basetemp under ~/scratch/12ag-gateunit-b/. Nothing deleted
  outside that directory. A unit to be stopped is named by its unit name, never by a
  pattern, and only a unit this brief launched. A count arrived at by eye is INFERRED.
  Where this brief describes a tool or a record, the tool and the record govern and a
  difference is a finding (R732(a)).

FINAL REPORT FORMAT: (0) the pre-check table and the clock; (1) item 0 — r808.txt's
  hash, the block file, the gloss, the regeneration's numstat and classification, the
  brief file's sha256, bytes and lines, hashes and parents, the plan's new sha256, bytes
  and lines; (2) the probe — its line, status, Linger, the scratch file, the journal
  lines; (3) the edit — the three diffs whole; (4) validation — 4a's FAIL, the dry run's
  launch line and printed status, the check verbatim with its output, both controls;
  (5) the tools commit and its ledger row; (6) the gate — the launch line, what C4
  printed, the summary line, ids, the nineteen files, wall time beside the band and the
  series, durations, concurrent jobs, the hold taken or not needed, the unit's journal
  lines, the journal window; (7) the evidence commit; (8) findings, numbered, each with
  its quoted line, including anything in R1–R6 the box contradicts; (9) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 or I2 false, r808.txt off its hash, a
  regeneration line outside R766(b)'s kinds, or a check failure or crash = STOP before
  the commit it guards. I3 false = STOP before any edit. A step-4 failure = STOP before
  step 5. A step-6 stop condition = STOP before step 7. A spec premise that does not
  survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not start U30's build. Await the next brief or ruling in
  this session. STOP-12AG-GATEUNIT-B
