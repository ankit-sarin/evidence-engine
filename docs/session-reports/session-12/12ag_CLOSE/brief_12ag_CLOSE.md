CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R810, PI ruling 2026-11-14 UTC; not on disk)
================================================================================

R809 and R810 are transcribed at item 0 of the brief below, by transcribe_ruling.py, as
the next two blocks of the decision log. R809 is taken from the committed resume file
(step 1a). The lines between the two marker lines below, exclusive of the markers, are
the byte content of r810.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's hash:
  r810.txt: sha256 6749ae7c1bca5afed3c5aee7c12b72ab56942913295dc3efb4c545b8fce40342,
            4162 bytes, 52 lines, one trailing newline.

===== BEGIN r810.txt =====
RULING [evidence-engine 12ag-R810] — 12ag-GATEUNIT-B accepted: the gate launches as a user unit;
  982.43 s joins the series as a standard member with its launch form recorded; readings; the
  PI closes session 12ag; no closeout gate; no standing rule
STATUS: PI ruling, 2026-11-14 UTC, session 12ag, on architect recommendation, on the
  12ag-GATEUNIT-B-R1 final report (tools commit b3d353417b9a2dcf004b532acceca6b7d29dd689; evidence
  3e3c33486120ad40a48fb9e1ffe3a4a7eba1d4ba). Transcribe at item 0 of the 12ag-CLOSE brief below,
  by transcribe_ruling.py, in the block after R809's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12ag-GATEUNIT-B-R1 final report, quoted): "The unit wrote "nofile soft 500000
    hard 500000" and "LANG=C.UTF-8", both as set."; "4c, the check: every assertion PASS."; "rc
    1, as it must"; "REFUSED: user unit gate-unit_hold.service is loaded"; "Commit:
    b3d353417b9a2dcf004b532acceca6b7d29dd689 (parent b7e5fb1)"; "Ledger:
    8e5f4fe676840e7ddd00924ce3c23ce3902bfa66."; "The snapshot at 21:28:08 showed surgical-cv's
    uvicorn only."; "summary "3573 passed, 17 deselected, 1 xfailed, 2 warnings in 980.31s"";
    "ids 3574 against 3574, 0 / 0"; "Wall time: 982.43 s, inside 872–1065 s."; ""Started
    gate-12ag_unit.service" at 21:28:21.998696Z"; "shows "-- No entries --""; "Commit:
    3e3c33486120ad40a48fb9e1ffe3a4a7eba1d4ba (parent b3d3534), 74 new files, +13,426."; "Ledger:
    5bb0961ba232ac61de2ba314127a7bf3720fb5ae (parent 9b6b7d6)."; "brief_12ag_GATEUNIT_B_R1.md is
    603220349ec30b59ab607b653562307ac327cb68c42cf02578df11697b723cda, 6,726 B, 96 lines" (the
    architect's copy: the same sha256, bytes and lines).
  READ (CC's): findings 1–6; the edit as described; "It was not exercised, since no systemd-run
    was made to fail."
  READ (the architect's clone at 3e3c334): 12ag_GATEUNIT_B/gate_12ag_unit/gate_wall.txt "wall:
    982.433030153 s"; git diff --stat 977c499 HEAD on engine tests scripts analysis review_specs,
    empty; the plan's last line "The decision log in this file ends at R808.".
  INFERRED: none.

R810(a) — The 12ag-GATEUNIT-B-R1 final report is accepted as printed: the second probe with the
  limit and LANG its line set; the tools commit b3d3534 (launch_gate.sh, run_gate.sh's comment,
  the README; the dry run's check passed, the fail-first assertion failed on the tmux line,
  both controls refused), ledger 8e5f4fe; the standard gate under gate-12ag_unit at the 12ag
  block's counts, ids 0 / 0, nineteen files, no load in its window, its start line in the user
  journal; the evidence commit 3e3c334, ledger 5bb0961; the resume file equal to the
  architect's copy. The gate launches as a transient user unit from here (R808(b), R809(b)).
R810(b) — The PI's ruling on the series, in his words: "As a standard member, with its launch
  form recorded". 982.43 s is the forty-second member, standard, the first launched as a unit.
  The band in force stays 872–1065 s until the wrap ruling sets 12ah's; the closeout computes
  the figure from the last four standard gates on this host state.
R810(c) — Readings, no row: finding 2 (the unit's journal "Consumed" line; its memory figure is
  not a measure of the suite); finding 3 (one reading does not separate the launch's effect
  from the day's); finding 4 (claude-config 9b6b7d6, another lane's skills commit, accepted
  under R663(b)); finding 5 (the launcher's failed-launch path is read from the code and was
  not exercised — carried with the tools items).
R810(d) — The PI's ruling, in his words: "Wrap with close of CC session exit and archive then
  continuation". Session 12ag closes on the tools change (b3d3534), with U30's design ruled
  (R805(d), R807(b)) and its build not started. The closeout is written in 12af-CLOSE's form.
  No closeout gate: the engine tree is unchanged since 977c499 and the unit gate is the gate
  of record. The closeout drafts the lane line and does not apply it; the wrap ruling follows
  its report.
R810(e) — This ruling states no standing rule: the id list, the glosses file and
  STANDING_RULES.md are untouched at item 0, asserted by content.
===== END r810.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ag-CLOSE] — Close session 12ag on the gate-launch tools change, in
  12af-CLOSE's form read from the record: item 0 (R809 and R810 as the next two blocks;
  this brief committed as pasted; no standing rule), then the closeout commit (the closure
  paragraph, the "Expected values at session-12ah open" block, 12ag_close/'s measurements,
  the 2026-11-14 job reads, the band computation, the recount, R510, retention, the lane
  line drafted and not applied). Docs only. No engine edit. No live write. No model call.
  No closeout gate (R810(d)).
PURPOSE: Leave the record in the state the next session opens on: every fact of 12ag in the
  plan's closeout block with its evidence file beside it, the expected values for 12ah's
  open stated from the unit gate, and the band figure computed for the wrap ruling.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Blocks 207 and 208; then the closeout block
  and the 12ah expected-values block); docs/session-reports/session-12/12ag_open/
  (r809.txt, r810.txt); a new directory docs/session-reports/session-12/12ag_close/ (the
  counterpart files); docs/session-reports/session-12/12ag_CLOSE/brief_12ag_CLOSE.md (at
  item 0); ~/scratch/12ag-close/ (the lane line draft and scratch); the claude-config
  ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/, the codebook, docs/architecture/, CLAUDE.md, docs/inventory/, the tools
  (no gate and no unit is launched); the id list, the glosses file and STANDING_RULES.md
  (not one byte — R810(e)); 12af_close/ and every earlier close or open directory (read,
  never rewritten); the dated units lines already in the plan; the lane line in the
  primers (drafted to scratch, applied only by the wrap ruling); review.db (mode=ro
  instruments only); the working tree inside 07:00–10:35 UTC (R86).

FORM (R784(g)(iv)): docs/session-reports/session-12/12af_CLOSE/brief_12af_CLOSE.md (sha256
  e8c2b13f8e3375ec8bef4180e5f954a5e466ad45d7e81cbf3da2d88d52605a47, 332 lines), Section 2,
  is this brief's form, with the form it names in turn (12ae_CLOSE/brief_12ae_CLOSE.md) —
  its steps 0, 1 and 2 (2a–2h), its acceptance gates, constraints, final report format and
  contradiction handling, read whole before anything is composed, with the 12af closeout it
  produced (7ddabca: the closure and the "Expected values at session-12ag open" block;
  12af_close/'s twenty files plus those 12ag-OPEN added there) as the worked example. This
  brief states only what differs. Where the two differ, this one governs; where that brief
  names a 12af source, the 12ag source listed below takes its place. A step of the form
  with no 12ag source is reported as not done, with its reason. The item-0 model for two
  blocks is 12ag-GATEUNIT-A's item 0, 12c1108.

TIMING: issued at 21:55 UTC on 2026-11-14. Outside 07:00–10:35 UTC; the clock is read
  before each commit. If it reaches 06:45 UTC before a commit, hold with the work on disk
  uncommitted and resume in this brief after 10:35 UTC (R789(a)); say so.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ag-GATEUNIT-B-R1 final report; R810): HEAD
    3e3c33486120ad40a48fb9e1ffe3a4a7eba1d4ba, parent
    b3d353417b9a2dcf004b532acceca6b7d29dd689, clean, level; claude-config at
    5bb0961ba232ac61de2ba314127a7bf3720fb5ae or an accepted descendant (R663(b)); the unit
    gate 12ag_GATEUNIT_B/gate_12ag_unit/: 3,573 / 1 / 17 on 3,591 / 3,574, 982.43 s.
  READ (the architect's clone at 3e3c334, 2026-11-14 21:50 UTC; re-read on the box):
    R1. The plan: sha256 3dbe145a0413afec97e2b3f747b442c4b7e80e36951cd90d99d770b4800cdabd,
        3243845 bytes, 19112 lines; last line "The decision log in this file ends at
        R808."; last block 206; 163 lines begin "The decision log in this file ends at R".
        "Expected values at session-12ag open" starts at plan line 5480 and "## Decision
        log" is at 5516.
    R2. Rules files: STANDING_RULES.md
        95329e5e2384afa01d68f7f42080f27592b55cb41fb7faef24413f12a566d3f4; the id list
        f3fc106cfa1514a890cd4bac8d95927acf2034f49cb0197285b690c0d2bbda95; the glosses
        11579e9e7a0d3d2d0e97fef2f837c4013e78bf05045747bd1c6fda5768a7a60c.
    R3. R809's text is on disk only inside 12ag_GATEUNIT_B/brief_12ag_GATEUNIT_B_R1.md,
        between its "===== BEGIN r809.txt =====" and "===== END r809.txt =====" lines;
        that content hashes to
        337d6875e66ade252bd13fe5b853c5a86bba4102dba4a87e270f61de32c170d2, 3503 bytes, 45
        lines (R810's own hash is in Section 1).
    R4. The session's commits, git log 54266b7..HEAD, nine: 16cb307, 60a6331, dcf623e,
        19b1f01, 12c1108, e5055c6, b7e5fb1, b3d3534, 3e3c334. One is outside docs/plan and
        docs/session-reports/session-12: b3d3534, the tools commit. The session opened on
        54266b756f92a77cf9f8cc107310fa6346d0bca0, five WR5 commits past the 12af closeout.
    R5. The session's rulings: 12af_close/r803.txt and 12ag_open/r804.txt to r808.txt on
        disk, R809 as R3 places it, R810 in Section 1. Three rulings stated standing
        rules: R805(c), R805(f) and R808(d).
    R6. Six-cell "| INT-" rows in the plan: 213. No row was opened or closed and no fix
        unit landed in 12ag; the last dated units line is 12af's.
    R7. 12af_close/recount_12af_close.py's committed JSON is
        3657ce2f219c9fc130d5248295e99071855245d87dc2b7ab6d80e9b5c16d15a5.
    R8. gate_12ag_unit/gate_wall.txt reads "wall: 982.433030153 s";
        12ag_open/gate_12ag_open/gate_wall.txt reads "wall: 1042.650800830 s".
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. The engine tree is unchanged since 977c499.
    I2. transcribe_ruling.py --append only appends.
  INFERRED — expectation (report a mismatch, continue with the measured value):
    E1. The recount by 12af_close/recount_12af_close.py on the plan at the item-0 commit
        reads 287 (94 / 57 / 136, 0 unclassed), the dated lines' fix units 38, done 33,
        its JSON byte-equal to the committed one; R510 for 12ag is in 0, out 0, the
        six-cell INT- rows 213 at 54266b7 and at the item-0 commit.
    E2. The band computation, from the last four standard gates on this host state — U40B
        965.92, U29B 979.95, 12ag-OPEN 1042.65 (confounded, R805(b)) and the unit gate
        982.43, each read from its gate_wall.txt — gives a mean of 992.74 s and, rounded
        inward, 894–1092 s. The series has forty-two members.
    E3. git diff --stat on engine tests scripts analysis review_specs is empty from
        54266b7 and from 977c499; from f4a5586 it is 96 files, +6,609 / −345.
    E4. Retention 23 MATCH / 0 MISMATCH by file sha256, none opened, no new entry.
    E5. The fingerprint IDENTICAL; dbchecks_close.txt, pin_tuples_close.json and the
        codebook and spec hashes byte-identical to 12af_close/'s.
    E6. The 2026-11-14 health job and nightly have run and no later pair has; the
        nightly matches 12af_close/expected_nightly_20261111.md line for line on a HEAD
        whose engine tree equals 977c499's.
    E7. The host's four band values equal 12af_close/host_state_close.txt's. If one
        differs, the band paragraph says so and the figure is computed and marked.

CONTRACT (the form's steps, with these 12ag terms; evidence under ~/scratch/12ag-close/):

  STEP 0 — Pre-check, as the form's: HEAD and parent proved by git cat-file -e
    <hash>^{commit}; clean; fetch; 0 0; one worktree. R1, R2, R3, R6. claude-config: the
    MEASURED commit or an accepted descendant, level; a pending foreign row carried and
    named. I1. No gate-* unit loaded. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (model 12c1108; its check and block files quoted before composing; the
    check written for this brief, R794(i)).
    1a. 12ag_open/r809.txt: the lines between R3's two marker lines, exclusive, extracted
        by a command, one write, hashed straight after against R3's hash. r810.txt beside
        it, from Section 1, one write, hashed straight after against Section 1's hash. A
        mismatch on either = STOP (R738(b), R739(b)).
    1b. block207.json (R809) and block208.json (R810) in the pattern's two-block wording
        (R806(c)): each "beside" names the other block first, then "the brief file it
        orders"; R809 followed by the 12ag-GATEUNIT-B-R1 resume, its report the
        12ag-GATEUNIT-B stop report as its STATUS names it; R810 followed by the
        12ag-CLOSE brief, its report the 12ag-GATEUNIT-B-R1 final report with its tools
        and evidence hashes; both made 2026-11-14, session 12ag; transcribed = the
        commit's UTC date; prev_ends R808 then R809. Render both, compare the element
        order with Blocks 204 and 205, then --append in order.
    1c. No row note and no dated units line: R805(d) and R807(c) place their notes at and
        after U30's build, and no unit has landed.
    1d. The brief file (R784(g)(v)): 12ag_CLOSE/brief_12ag_CLOSE.md from this whole pasted
        block, "CLAUDE CODE" through the STOP-12AG-CLOSE line, one trailing newline,
        written once; sha256, bytes and lines straight after the write, reported.
    1e. The item-0 check, as the form's 1e with these terms: the last line "…ends at
        R810." once; the plan equals HEAD's plus Blocks 207 and 208 appended, nothing
        else (the "−" count read and matched to zero); six-cell INT- rows 213 before and
        after; both ruling files equal to 1a; the three rules files at R2's hashes and
        unstaged; the staged set exactly the plan, r809.txt, r810.txt and the brief file.
    1f. <check> && <commit>, one line; then fingerprint, push, level and the ledger row
        as the form's 1f; the ledger message built from the diff.

  STEP 2 — The closeout commit, every element in the form's order, each with its file
    under 12ag_close/ in 12af_close/'s counterpart name; a name with no 12ag source is
    listed as not written, with the reason.
    2a. Measurements as the form's 2a, dated names carrying the read's UTC date.
        Differences: the diff files are diff_engine_since_54266b7.txt,
        diff_since_977c499.txt and diff_since_f4a5586.txt (E3), and beside them
        diff_tools_since_54266b7.txt, the stat of docs/session-reports/tools/ (the three
        files of b3d3534); collected_ids_12ag_close.txt is a copy of
        gate_12ag_unit/collected_ids.txt and durations_12ag_unit.txt a copy of that
        gate's durations file, each cmp equal to its source, the ids also compared with
        12af_close/collected_ids_12af_close.txt; dbchecks, pins and the codebook and spec
        hashes compared with 12af_close/'s (E5); the host state with E7; the user
        manager's state and the absence of any gate-* unit, as readings.
    2b. The jobs (E6): the 2026-11-14 health job and nightly, read as 12ag-OPEN's step 1
        read 2026-11-11 to 11-13, their files into 12ag_close/ in those names; the
        nightly against 12af_close/expected_nightly_20261111.md, the HEAD it ran on by
        the reflog method. A nightly mismatch = STOP before the closeout commit, both
        values reported. The jobs paragraph says the 2026-11-11 to 11-13 pairs were read
        at 12ag-OPEN (their files in 12af_close/) and which file governs 2026-11-15 on.
    2c. The recount (E1): no row was opened in 12ag, so no new recount script is written
        (R678(f): one script per ruling that opens rows); recount_12af_close.py is run in
        a scratch clone on the plan at the item-0 commit, with the arguments its
        docstring names. r510_12ag.txt in r510_12af.txt's form. A row count other than
        287, dated lines other than 38 / 33, or R510 other than 0 / 0 = STOP before the
        closeout commit, both values reported.
    2d. band_computation.txt in 12af's form: the four inputs of E2, each with its
        evidence path, its gate_wall.txt value, how it was launched (tmux for three, a
        user unit for the fourth, R810(b)) and the concurrent jobs from its snapshots;
        the confounder of 12ag-OPEN's as R805(b) records it; mean, ×0.9, ×1.1, the inward
        rounding, the figure; the series of forty-two listed — 12af's forty plus 1042.65
        and 982.43, each read from its file; "a figure, to be set by the wrap ruling;
        the band in force stays 872–1065 s".
    2e. No expected-nightly file is written: the engine tree is unchanged since 977c499,
        so 12af_close/expected_nightly_20261111.md governs each later nightly (R749(b)).
        Say so where the form's 2e stands.
    2f. The lane line, ~/scratch/12ag-close/lane_line_12ag.txt, in lane_line_12af.txt's
        form: its five "−" lines are ~/claude-config/primer.md's lines 41–45 as they
        stand, taken from primer.md itself; its five "+" lines carry 12ag's sources —
        HEAD the closeout commit (filled after 2h's commit, so the file is written last,
        is not in the commit, and is pasted whole in the report), the plan's sha256,
        bytes and lines, the decision log ending R810 (R811 at 12ah-OPEN's item 0), and
        the session's summary: no fix unit landed; U30's design ruled on the PI's five
        decisions (R805(d)) with N46 folded in (R807(b)) and INT-j12-51 to close with it
        (R807(c)); three standing rules (R805(c), R805(f), R808(d)); the gate now
        launches as a user unit (b3d3534); 287 rows; fix units 38, done 33; three stops,
        no pause; two gates and the band figure; pins unchanged; no live write; the next
        steps — 12ah-OPEN, then U30's build, then U31–U36. Each "+" line read whole
        after writing, its emphasis marks paired. Not applied.
    2g. The closeout block and "Expected values at session-12ah open", placed after the
        12ag expected-values block's last line and before the decision-log section (the
        position read and matched, R1), in the 12af block's element order with 12ag's
        content: the closure paragraph (opened on 54266b7; the CC session's id and times
        read from the record); the briefs in order — 12ag-OPEN and its resume R1,
        12ag-N46A and its resume R1, 12ag-GATEUNIT-A, 12ag-GATEUNIT-B and its resume R1,
        this closeout — each with the ruling at its item 0 or carried in it and its
        committed file's sha256 as the rulings record its match with the architect's
        copy; the commits in order from git log 54266b7..HEAD with their ledger commits;
        the diff lists; the rulings R803–R810 one line each (R803 made at the 12af wrap
        and transcribed at 12ag-OPEN); the rows (none opened, none closed; INT-j12-51 and
        N46 found to be one finding, R807(c)); the stops, listed from the rulings (three:
        12ag-OPEN before its gate, R804(a); 12ag-N46A before its item 0, R806(a);
        12ag-GATEUNIT-B at its probe, R809(a); each the architect's), the pauses (none),
        and the architect's errors as the rulings count them (five, R809(a)); the tools
        paragraph (the launcher's change and its validation; the standing-rules file
        regenerated twice; the tools items carried from 12af unchanged, and with
        them the launcher's unexercised failed-launch path, R810(c)); one paragraph for
        the gate-launch change (design, validation, what was not taken: a memory cap, a
        twentieth evidence file, a tmux path); one for U30's design as ruled and not
        built (R805(d)); one for N46 (the read-out, the design R807(b), what it leaves
        open, R807(d)); one for the R667(e) read and its narrowing (R805(c)); "Carried
        forward" — U30's build with N46 and the notes R805(d) and R807(c) order; the
        items the 12af closure carried that 12ag did not touch, read from that closure
        and listed; the cross-lane readings for dgx-infra (a suite starting inside a gate
        window after both snapshots, R805(b); the degraded user manager, R808(e); other
        lanes' commits in claude-config); the provider keys reading (R808(e)); the gates
        table (12ag-OPEN, tmux, confounded; the unit gate); the band paragraph; the jobs
        paragraph (2b); the recount paragraph; retention; ledger; counterpart files; the
        host; live; "Session 12ag closed. Next: session 12ah, fresh."; then the 12ah
        expected-values block in the 12ag block's form, with these differences: the gate
        is launched by launch_gate.sh as the unit gate-<name>, its launch line and
        printed status as R808 and R809 rule them, its journal start line (R809(c)); the
        hold is cited as the standing rule R805(f), not restated; the references are
        collected_ids_12ag_close.txt and durations_12ag_unit.txt; the band figure of 2d;
        the nightlies per 12af_close/expected_nightly_20261111.md; "The decision log in
        this file ends at R810" and R811, the wrap ruling, at 12ah-OPEN's item 0; the
        rules files' three hashes; the five 12ag scratch directories; 12ah's first items
        — 12ah-OPEN, then U30's build. Every figure from a file in 12ag_close/ or an
        evidence directory, named beside it; nothing from this brief's text where a file
        exists.
    2h. The closeout check and commit as the form's 2h, with these terms: the last "ends
        at" line "…ends at R810." once; the staged set exactly the plan and 12ag_close/;
        the rules files, 12af_close/ and every earlier close directory unchanged; the
        message in 7ddabca's form. Then 2f's lane line completed and pasted whole in the
        report.

ACCEPTANCE GATES: the form's G1–G3 with these terms — I1 held; item 0 one commit that
  passed 1e in one chain with both ruling files at their hashes and Blocks 207 and 208 by
  the tool; the closeout commit with every element of the 12af block present in order
  with its 12ag source, every counterpart file written or listed as not written with its
  reason, the recount at E1 or stopped, the 2026-11-14 nightly matched or stopped, the
  band computed from the files, E3–E7 held or reported, the check passed in one chain;
  both pushed, level, fingerprint IDENTICAL; ledgers; the lane line pasted whole, not
  applied.

OUT OF SCOPE: the wrap ruling (R811: the band set, the lane line approved and applied,
  the stops counted — after this report); U30's build; any engine, test, script,
  docs/architecture, CLAUDE.md or tools edit; any gate or unit; the tools items; /wrap,
  /exit and the archive (after the wrap ruling).

CONSTRAINTS (task-only): no model call, no provider call, no live write; a count arrived
  at by eye is INFERRED, the computed value given with its command; every figure in the
  block beside its file; where this brief or its form describes a tool or record, the
  record governs and a difference is a finding (R732(a)).

FINAL REPORT FORMAT: the form's — (0) pre-check table; (1) item 0; (2) the closeout: the
  measurements with their values, the 2026-11-14 job reads, the recount output,
  band_computation.txt verbatim, the check verbatim, chain, hashes, fingerprint, ledgers,
  the plan's sha256, bytes and lines, the two blocks' line ranges in the plan (the blocks
  themselves need not be pasted, being committed), the lane line verbatim with its
  sha256, bytes and lines and its fifth "+" line quoted whole; (3) every place this
  brief's FORM clause left a choice, and the choice made; (4) contradictions and
  findings, numbered, each with its quoted line; (5) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 or I2 false, a 1a mismatch, or a check
  failure or crash at 1e = STOP before the item-0 commit. A recount or R510 off E1, a
  nightly mismatch, or a check failure at 2h = STOP before the closeout commit, the work
  on disk reported. An expectation mismatch (E2–E5, E7) is reported and the closeout
  continues with the measured value in the block. A spec premise that does not survive
  contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not apply the lane line, do not run /wrap, do not start
  12ah or U30. Await the wrap ruling in this session. STOP-12AG-CLOSE
