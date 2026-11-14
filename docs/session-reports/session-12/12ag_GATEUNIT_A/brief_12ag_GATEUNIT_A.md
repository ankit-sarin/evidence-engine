CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R807, PI ruling 2026-11-14 UTC; not on disk)
================================================================================

R806 and R807 are transcribed at item 0 of the brief below, by transcribe_ruling.py, as
the next two blocks of the decision log. R806 is taken from the committed resume file
(step 1a). The lines between the two marker lines below, exclusive of the markers, are
the byte content of r807.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's hash:
  r807.txt: sha256 8d3b5d8ede3aad12a2924a0a16c53950ed2f7cfc70279fa2ebe825d126bf2970,
            4711 bytes, 59 lines, one trailing newline.

===== BEGIN r807.txt =====
RULING [evidence-engine 12ag-R807] — 12ag-N46A accepted; N46's design: the cloud store drops an
  unexpected field before any write; INT-j12-51 closes on U30's build with N46 noted as the same
  finding; readings; no standing rule; the gate-launch tools change opens with its Phase A
STATUS: PI ruling, 2026-11-14 UTC, session 12ag, on architect recommendation, on the 12ag-N46A-R1
  final report (item 0 dcf623e18d9134e054185f61f3ca3b97c68153d7; read-out
  19b1f011f9f039f5ff1a6cfaee5dc9abe4c99700). Transcribe at item 0 of the 12ag-GATEUNIT-A brief
  below, by transcribe_ruling.py, in the block after R806's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12ag-N46A-R1 final report, quoted): "Commit: dcf623e18d9134e054185f61f3ca3b97c68153d7
    (parent 60a6331)"; "Ledger: 46057aabff24377b74eb5cf463f77ef66678b10b (parent 98fedf6)";
    "Commit: 19b1f011f9f039f5ff1a6cfaee5dc9abe4c99700 (parent dcf623e)"; "Ledger
    a4d4296832f9aae8741c9747cdc040dd6de2bfab (parent 46057aa)"; "The new plan is
    4013412bbbe754bd8f908ab20dbb4879c2d6b230f4ef8177d4dcf5550fb027a2, 3,222,530 B, 18,921 lines.";
    "3 header lines, 2 new sections, 18 "Named by" lines that only append ids, 0 other lines";
    "brief_12ag_N46A.md is 7b9c3cc75cda9e3190299a1d573e5cc48d78478c8ebf13aa39c7294efc75ceaf,
    21,419 B, 288 lines" and "brief_12ag_N46A_R1.md is
    ef67895d845f61576f6a26936795f16e07816fc6d22fce860a630af4dce2e449, 6,473 B, 91 lines" (the
    architect's copies: the same sha256, bytes and lines, both); "E1 held: the cloud store never
    calls drop_unexpected"; "the fix would go in store_result, after enforce_completeness (:422)
    and before enforce_citations (:433), which is the local path's order. Dropping there changes
    no request byte, pin or claim hash. No existing test moves."
  READ (CC's): findings 1–6; the reader census's classes, CC's judgement; probe cases (A)–(C),
    FAKE.
  READ (the architect's clone at 19b1f01): 12ag_N46A/12ag_N46A.md §7, "(D) Drop before any write,
    as R118's docstring states." and "`extracted_data` keeps the response verbatim, the name in
    it."; the plan's sha256 as the report gives it, last line "The decision log in this file ends
    at R805."; row INT-j12-51 at plan :685.
  INFERRED: none.

R807(a) — The 12ag-N46A-R1 final report is accepted as printed: item 0 dcf623e (Blocks 202 and
  203; "A R805(c)" and "A R805(f)" on the list, the file regenerated, every changed line of
  R766(b)'s three kinds; both brief files equal to the architect's copies), ledger 46057aa; the
  read-out 19b1f01 with its census and probe, ledger a4d4296. CC's finding 6, a check that
  failed on its own bug and was fixed before the commit, is a reading (R763(c)).
R807(b) — N46's design, the PI's ruling, in his words: "Drop it before any write". From
  12ag_N46A/12ag_N46A.md §7 option (D): store_result drops the spans whose field the codebook
  did not ask for, by the function the local path uses, after the completeness check and before
  the citation check, so the legacy spans table holds the names the claims hold and an uncited
  invented field no longer costs the paper. extracted_data stays the response as received.
  Acceptance in the read-out's §7 item 3 shape, both cases, each failing on the unmodified
  tree. Not taken: filtering extracted_data (D′); refusing the response (X). It is built in
  U30's build brief, with R805(d)'s design.
R807(c) — The PI's ruling, in his words: "Close INT-j12-51 on U30's build, with N46 noted as
  the same finding". The notes are written at the item 0 after U30 lands, in the record's
  CLOSED form; the row's class and package are read from the row there, and the count's
  effect is the recount's to state.
R807(d) — Readings, no row: finding 2 (U28 never carried N46; "with U28" in the P1 list and in
  the architect's earlier wording was not what U28's entry or read-out say); findings 3 and 4
  (the uncited case, and a run that closes completed with every paper refused at the store, a
  reading beside INT-ae12-2); finding 5 (the brief's R5 named engine/cloud/schema.py, which
  selects no row). Left open by N46 and not U30's: whether live holds such a row; the two
  repair scripts' insert path from extracted_data; the three analysis readers that count every
  row.
R807(e) — The tools change of R805(g) opens with its Phase A, read-only: the gate wrapper's
  launch as a systemd-run --user unit, and nothing else. The other tools items carried in the
  plan are not part of this change. No unit is launched in Phase A.
R807(f) — This ruling states no standing rule: the id list, the glosses file and
  STANDING_RULES.md are untouched at the next item 0, asserted by content.
===== END r807.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ag-GATEUNIT-A] — Phase A read-out for one tools change: the gate
  wrapper launching a gate as a systemd-run --user unit. Item 0: R806 and R807 as the
  next two blocks; this brief committed as pasted; no standing rule. Read-only on the
  tools, engine/, scripts/ and tests/; no unit is launched; STOP before any edit.
PURPOSE: Put on disk, before the wrapper is changed, what the two gate scripts do line by
  line, what the global long-running-jobs rule requires of a launch, what this box gives a
  user unit, and what else depends on the present tmux launch. Mixed: one docs commit
  (item 0), one docs commit (the read-out). No tools, engine, test or script change. No
  gate. No live write. No model call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Blocks 204 and 205 appended by the tool,
  nothing else); docs/session-reports/session-12/12ag_open/ (r806.txt, r807.txt); a new
  directory docs/session-reports/session-12/12ag_GATEUNIT_A/ (brief_12ag_GATEUNIT_A.md at
  item 0; the read-out and its captured reads at step 2); ~/scratch/12ag-gateunit-a/; the
  claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): docs/session-reports/tools/ (read, never
  edited, and neither gate script is run); engine/, tests/, scripts/, analysis/,
  review_specs/, the codebook, docs/architecture/, CLAUDE.md, docs/inventory/; the id
  list, the glosses file and STANDING_RULES.md (not one byte — R807(f)); review.db
  (mode=ro instruments only); no systemd-run, no systemctl start, stop, enable, disable or
  set-environment, no loginctl enable-linger — only the show, status, list and cat forms;
  no tmux session created or killed; crontab read with -l only; other lanes' files read,
  never edited; any scratch directory other than ~/scratch/12ag-gateunit-a/; the working
  tree inside 07:00–10:35 UTC (R86).

FORM: 12af_U30A/brief_12af_U30A.md is the form of the steps and 12ag_N46A/12ag_N46A.md
  the form of the read-out. The item-0 model is 12ag-N46A's item 0, dcf623e, less its
  standing-rules step: its check, its two block files and the pattern commit it quoted
  (d488573) are in ~/scratch/12ag-n46a/; quote each before composing its counterpart.

TIMING: issued at 20:30 UTC on 2026-11-14. Valid outside 07:00–10:35 UTC. If the clock
  reaches 06:45 UTC before a commit, hold with the work on disk uncommitted and resume in
  this brief after 10:35 UTC (R789(a)); say so.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ag-N46A-R1 final report; R807): evidence-engine HEAD
    19b1f011f9f039f5ff1a6cfaee5dc9abe4c99700, parent
    dcf623e18d9134e054185f61f3ca3b97c68153d7; claude-config at
    a4d4296832f9aae8741c9747cdc040dd6de2bfab or an accepted descendant (R663(b)).
  READ (the architect's clone at 19b1f01, 2026-11-14 20:30 UTC; re-read on the box):
    R1. The plan: sha256 4013412bbbe754bd8f908ab20dbb4879c2d6b230f4ef8177d4dcf5550fb027a2,
        3222530 bytes, 18921 lines; last line "The decision log in this file ends at
        R805."; last block 203; 160 lines begin "The decision log in this file ends at R".
    R2. Rules files after dcf623e: STANDING_RULES.md
        9c037e869c02667ecc772c399e74d948229f72aa26e663ffbf4674c113fa1a51; the id list
        5c301ffbb7a4e3e7697006590a863f4df36f03386e6be96262acbbb27ad9a91e; the glosses
        24b6fce3739a0d592c5338e8f8a88d8f9bc325a38484216229d067e46ca8abc8.
    R3. R806's text is on disk only inside 12ag_N46A/brief_12ag_N46A_R1.md, between its
        "===== BEGIN r806.txt =====" and "===== END r806.txt =====" lines; that content
        hashes to ded7382da2d4f880f5b4f94c38813a1daebe00b22e10c058fba0fc8e485c9599, 3227
        bytes, 41 lines (R807's own hash is in Section 1).
    R4. docs/session-reports/tools/launch_gate.sh is 39 lines and run_gate.sh 50. In
        launch_gate.sh: "tmux has-session -t "$SESSION" 2>/dev/null && { echo "REFUSED:
        tmux session $SESSION exists"; exit 2; }" (:8) and "LINE="tmux new-session -d -s
        $SESSION \"bash $E/run_gate.sh $E $SESSION $REF $REFD $*\""" (:36), written to
        launch_line.txt and then run by eval. run_gate.sh's header says "run inside tmux
        by launch_gate.sh"; it names its durations file "durations_$SESSION.txt" and,
        by the tools README, writes gate_rc.txt last.
    R5. The tools README: launch_gate.sh was validated at 12aa-SETUP by a "Dry run on
        `tests/test_u20_modelfile_num_ctx.py`: the nineteen files, and the launch line's
        inner command equal to `gate_launch.txt`'s line 2" (evidence 12aa_SETUP/dry_run/,
        2d_check.txt). session_open_check.py's cc_sessions section counts a process whose
        arguments carry "run_gate.sh" as this lane's.
    R6. The global rule, as the 12ag-OPEN stop report quoted it from claude-config
        9f78b9a: "it runs as a transient systemd-run --user unit. The brief names the
        unit and any memory cap. At launch Claude Code reports systemctl --user status
        <unit> and confirms Linger=yes. The run logs to the journal and to its own
        folder." and "A tmux window may follow a unit's log."
    R7. Six-cell "| INT-" rows in the plan: 213.
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. The engine tree is unchanged since 977c499 (git diff --stat on engine tests
        scripts analysis review_specs empty).
    I2. transcribe_ruling.py --append only appends (the "−" count read from the numstat).
  INFERRED — expectation: none. The architect has read the two scripts' tmux lines and
    the README only, and predicts nothing about this box's user manager, its linger
    state, its environment, or what a unit would change in a gate's wall time: all to be
    measured or, where Phase A cannot measure it, named as such.

CONTRACT (steps, in order; evidence under ~/scratch/12ag-gateunit-a/):

  STEP 0 — Pre-check (read-only). date -u. HEAD and parent proved by git cat-file -e
    <hash>^{commit}; clean; fetch; 0 0; one worktree. R1, R2, R3, R7. claude-config: the
    MEASURED commit or an accepted descendant, level; a pending foreign row carried and
    named (R654(d)). I1. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model dcf623e less its
    standing-rules step; the check written for this brief, R794(i)).
    1a. 12ag_open/r806.txt: the lines between R3's two marker lines, exclusive, extracted
        by a command, one write, hashed straight after against R3's hash. r807.txt
        beside it, from Section 1, one write, hashed straight after against Section 1's
        hash. A mismatch on either = STOP (R738(b), R739(b)).
    1b. block204.json (R806) and block205.json (R807) in block203.json's key set and the
        pattern's two-block wording (R806(c)): each "beside" names the other block first,
        then "the brief file it orders"; R806 followed by the 12ag-N46A-R1 resume, its
        report the 12ag-N46A stop report as its STATUS names it; R807 followed by the
        12ag-GATEUNIT-A brief, its report the 12ag-N46A-R1 final report with its item-0
        and read-out hashes; both made 2026-11-14, session 12ag; transcribed = the
        commit's UTC date; prev_ends R805 then R806. Render both, compare the element
        order with Blocks 202 and 203, then --append in order.
    1c. The brief file (R784(g)(v)): 12ag_GATEUNIT_A/brief_12ag_GATEUNIT_A.md from this
        whole pasted block, "CLAUDE CODE" through the STOP-12AG-GATEUNIT-A line, one
        trailing newline, written once; sha256, bytes and lines straight after, reported.
    1d. The item-0 check: --check-plan byte-identical; the plan's numstat read ("−"
        matched to zero); every earlier "ends at" line kept; the last line "The decision
        log in this file ends at R807." once; both ruling files equal to 1a; the three
        rules files equal to R2's hashes and none staged; six-cell INT- rows 213 before
        and after; the staged set exactly the plan, r806.txt, r807.txt and the brief
        file. Every value from arguments or the files checked (R742(b)); evidence by a
        redirect inside the check; quote the check.
    1e. <check> && <commit>, one line; the message from the staged set, docs scope.
        Fingerprint --compare before the push; push; level; --compare after (IDENTICAL).
        The ledger row with trailers, its message built from the diff; push; level.
        Record hashes and parents.

  STEP 2 — The read-out (read-only). Every claim beside a quoted line with its path and
    line, or beside the command that produced it and its output saved under scratch.
    2a. §1 Inputs, quoted: the global CLAUDE.md's long-running-jobs rule whole, with the
        commit that carries it; R804(b); R805(g); R731(b), R732(d), R738(c) and R738(d)
        on the launch line and the nineteen files; R746(e)(iv); the tools README's rows
        for the two scripts.
    2b. Q1 — THE TWO SCRIPTS. launch_gate.sh and run_gate.sh whole, in code order: what
        each line does; which of the nineteen files each writes and when; every place
        tmux, the session name or a terminal is assumed; how the exit code reaches
        gate_rc.txt; what a caller waits on.
    2c. Q2 — WHAT THE RULE ASKS OF A LAUNCH, item by item from its text: the unit's
        name; a memory cap; the status report; Linger; the journal and "its own folder".
        For each, what the present launch records and what it does not.
    2d. Q3 — THIS BOX, by show, status, list and cat forms only: the user manager's
        state; loginctl show-user for this user (Linger and its other fields); systemctl
        --user show-environment beside this session's env, the differences listed (PATH,
        HOME, LANG, TMPDIR, XDG_RUNTIME_DIR, anything the venv or pytest reads); user
        units and timers present; how the 07:00 health job and the 09:00 nightly are
        launched and what environment each gets, quoted from their own definitions;
        whether any lane on this box already launches a job by systemd-run --user, with
        the line quoted. systemd's version.
    2e. Q4 — WHAT A GATE NEEDS FROM ITS ENVIRONMENT, from run_gate.sh and the repository's
        test configuration: the working directory, the interpreter, the shared pytest
        temp root, the experiment lock if any test takes it, stdout and stderr, anything
        read from the terminal.
    2f. Q5 — WHAT ELSE DEPENDS ON THE PRESENT LAUNCH. A census by script of every place
        in docs/session-reports/tools/, the standing rules and the 12ag expected-values
        block that names tmux, the session name, launch_line.txt's form, gate_launch.txt's
        second line, or "run_gate.sh" as a process pattern; for each, whether a unit
        launch would break it, leave it true, or leave it stale. The classes are CC's
        judgement and are marked so.
    2g. Q6 — WALL TIME. What differs between a process started under the user manager and
        one started from tmux that could move a gate's wall time on this box, each
        stated from a quoted setting or named as not established. Beside it, as
        readings, the three nightlies' wall times from 12af_close/'s nightly files and
        how the nightly is launched. Nothing is run to measure it.
    2h. Options for the ruling, each with what it records and what it costs, nothing
        decided: at least a unit launch replacing tmux in launch_gate.sh; a mode switch
        on launch_gate.sh; a second launcher beside it. For each: the launch line's
        form and what launch_line.txt would hold; the unit's name; a memory cap or
        none, with what a cap would do to a gate that crossed it; what the evidence set
        gains or loses against the nineteen of record; what the caller waits on; the
        Phase B validation in 12aa-SETUP's form (the dry run named in R5) and whether a
        standard gate belongs in it; what the first unit-launched gate means for the
        band series.
    2i. Findings, numbered; then what could not be established without launching a unit.
    2j. The read-out commit: 12ag_GATEUNIT_A/12ag_GATEUNIT_A.md with the census script
        and the captured reads; the check asserts the staged set and reads its numstat;
        <check> && <commit>; fingerprint before and after the push; the ledger row.
        Record hashes and parents.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 verified.
  G2. Item 0: one commit that passed 1d in one chain; both ruling files equal to their
      hashes; the rules files untouched; pushed, level, fingerprint IDENTICAL; the
      ledger row.
  G3. The read-out committed: both scripts read whole; the rule's items each answered;
      the box's reads captured with their commands; 2f's census classed; 2h's options
      written with nothing decided; no unit launched and no script run.

OUT OF SCOPE: any edit to a tool; launching any unit or gate; the other tools items the
  plan carries (R807(e)); the cross-lane item for dgx-infra; U30's build; /wrap.

CONSTRAINTS (task-only): no live write, no model call. Nothing deleted outside
  ~/scratch/12ag-gateunit-a/. A count arrived at by eye is INFERRED; a computed value is
  reported with its command. Where this brief describes a tool or a record, the tool and
  the record govern and a difference is a finding (R732(a)). A tempting fix is a finding,
  not an edit.

FINAL REPORT FORMAT: (0) the pre-check table and the clock; (1) item 0 — the two ruling
  files' hashes, both block files, the brief file's sha256, bytes and lines, the check
  verbatim, hashes and parents, the plan's new sha256, bytes and lines, the ledger
  commit; (2) the read-out's path and commit; Q1's table of the nineteen files by writer;
  Q2's item table; Q3's reads, Linger first; Q5's census as a table; Q6's list; the
  options by name with their costs; (3) findings, numbered, each with its quoted line,
  including anything in R1–R7 the box contradicts; (4) what could not be established;
  (5) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 or I2 false, a ruling file off its hash,
  or a check failure or crash = STOP before the commit it guards. Anything else the box
  contradicts is a finding and the run continues. A spec premise that does not survive
  contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not edit a tool or launch a unit. Await the next brief or
  ruling in this session. STOP-12AG-GATEUNIT-A
