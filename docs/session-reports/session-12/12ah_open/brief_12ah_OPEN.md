CLAUDE CODE

================================================================================
SECTION 1 — RULINGS TO TRANSCRIBE (R811, PI ruling 2026-11-14 UTC; R812, PI ruling
2026-11-16 UTC; neither on disk)
================================================================================

R811 and R812 are transcribed at item 0 of the brief below, by transcribe_ruling.py, as
the next two blocks of the decision log, R811 first. For each file, the lines between
its two marker lines, exclusive of the markers, are the byte content of the named file.
Write each file from those lines exactly: same characters, same line breaks, one
trailing newline. Never retype a ruling. The architect's independent hashes:
  r811.txt: sha256 c94df6893a93b891d59b9f798fb4227aa34cf6f05557935cedea7910805fbb03,
            8430 bytes, 100 lines, one trailing newline.
  r812.txt: sha256 dcc9e6e1af2df929eeae74dc2c57a1614d01b8b8f56941b82ad41c46f75958f1,
            3606 bytes, 45 lines, one trailing newline.

===== BEGIN r811.txt =====
RULING [evidence-engine 12ag-R811] — wrap ruling: closeout accepted; the band set 894–1092 s;
  three stops this session, each the architect's, and no pause; readings on the closeout's
  findings and choices; no standing rule; lane line approved on one condition and applied;
  close now
STATUS: PI ruling, 2026-11-14 UTC, session 12ag, on architect recommendation, on the 12ag-CLOSE
  closeout report (item 0 e329cedc280e8136cc825e56dc59c54fd9329968; closeout
  35a914b2bdd75147af104e5abfaea388eae3a057). Transcribe as item 0 of 12ah-OPEN by
  transcribe_ruling.py, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the closeout report, quoted): "Commit: e329cedc280e8136cc825e56dc59c54fd9329968
    (parent 3e3c334). Ledger a9853b6."; "Commit: 35a914b2bdd75147af104e5abfaea388eae3a057 (parent
    e329ced)"; "Ledger 5f13593aa04587e84b03363e9802b9d14fe5cbe0."; "r809.txt is 337d6875…70d2
    (3,503 B, 45 lines) and r810.txt is 6749ae7c…0342 (4,162 B, 52 lines). Both equal your
    hashes."; "brief_12ag_CLOSE.md is
    1218ef13735534f177b1df7759c88d8e94db2afc43b0e875dff58fc68df8a1cb, 25,657 B, 339 lines" (the
    architect's copy: the same sha256, bytes and lines); "The plan is +119 / −0"; "Plan now:
    22eafc7ded18daa7a1f3b8bb69d206aa749c2592c4f4356ab90f17501cf52adc, 3,287,665 B, 19,432 lines.";
    "(+201 / −0)"; "12ag_close/, 23 files"; "23 MATCH / 0 MISMATCH; the 12v probe scratch 10/10
    and 53/53"; ""ALL NIGHTLIES MATCH""; "now {287, 94, 57, 136, 0}, fix_units_dated_lines {38,
    done 33}, and expected_holds True"; "rows in 0 []; rows out 0 []"; "mean 992.74 s; x0.9
    893.464; x1.1 1092.012"; "rounded inward: 894–1092 s"; ""RESULT: PASS""; the lane line
    "sha256 a774632983299dbb5f78f13caa5a3d7b07222af3fdb159a1a5d00b70ad0ec306, 7,574 B, 10 lines;
    not applied"; "Its five "−" lines equal primer.md lines 41–45."
  READ (CC's): choices 1–6 and findings 1–5; "acb83b21, opened 05:24 UTC".
  READ (the architect's clone at 35a914b): the two full hashes in STATUS; the plan's sha256 as
    the report gives it, 19,432 lines, last line "The decision log in this file ends at R810.";
    the closure at plan :5516–:5676 and "Expected values at session-12ah open" from :5678;
    12ag_close/'s twenty-three files listed; band_computation.txt "mean 992.74 s; x0.9 893.464;
    x1.1 1092.012" and "rounded inward: 894–1092 s".
  INFERRED — BLOCKING (verified before R811(f) is carried out; false = STOP, nothing applied):
    the lane-line file's fifth "+" line reads whole. The report's copy of it runs words together
    where a name in code marks meets its neighbour (after "num_predict", and before and after
    the expected-nightly file's name, twice), which is either the report's rendering or the
    file's own text; the file decides.

R811(a) — The 12ag-CLOSE closeout is accepted as committed: item 0 e329ced (Blocks 207 and 208;
  the brief file equal to the architect's copy; the rules files untouched), the closeout
  35a914b (the closure and the 12ah expected-values block, +201 / −0 at the matched position;
  12ag_close/'s twenty-three files; the 2026-11-14 job reads; the recount; the band
  computation), ledgers a9853b6 and 5f13593. E1–E7 held. Eight committed brief and resume
  files this session, eight equal to the architect's copies.
R811(b) — The band for 12ah is 894–1092 s: ±10% of 992.74 s, the mean of the last four standard
  gates on this host state (965.92, 979.95, 1042.65, 982.43), rounded inward. The third is a
  confounded member carried under R677(f) with its confounder named (R805(b)); the standing
  rule answers the closeout's finding 1, as R796(b) applied it. The fourth is the first gate
  launched as a unit (R810(b)). The series stands at forty-two. A 12ah gate outside the band
  is reported with its concurrent jobs, not adapted to. The band is wider and higher than
  12ag's because of the confounded member; no rule changes for that.
R811(c) — The stops, counted from the rulings: three, each the architect's — 12ag-OPEN before
  its gate (R804(a)), 12ag-N46A before its item 0 (R806(a)), 12ag-GATEUNIT-B at its probe
  (R809(a)). Pauses: none. CC's stops: none. The architect's errors this session, five, each
  caught by CC reading the record or the box over the brief (R732(a)), none reaching a commit:
  an expectation about the global rule written without its text (R804(a)); "clean" where the
  form expects other lanes' rows (R804(a)); a regeneration condition narrower than R766(b)
  (R806(a)); one "followed" wording given to two blocks (R806(a)); the unit's open-files
  limit set above what the manager holds (R809(a)). Twice the fact was in a file the
  architect had already read. The session's measured point for the estimate: one CC session
  (the open at 05:24 UTC on 2026-11-14 to this wrap), no fix unit landed; U30's design and
  N46's ruled; one read-out for N46; one tools change from Phase A to a validated gate; three
  standing rules; three stops, no pause.
R811(d) — Readings, no row. The closeout's choices 1–6 are accepted as made. Its findings: 2
  (the temp root's figure by the root's own du -sb); 3 (the 2026-11-14 nightly's 1039.56 s
  by pytest's own time, on an unchanged engine tree — a reading for the cross-lane item); 4
  (NV_ERR_NO_MEMORY thirty-seven this boot, the six new lines inside health windows); 5.
R811(e) — Carried to 12ah and the plan, from the closure's "Carried forward": U30's build with
  N46, one brief, from 12af_U30A.md, 12ag_N46A.md and R805(d) and R807(b), with the notes
  R805(d) and R807(c) order; then U31–U36; what N46 leaves open (R807(d)); the tools items,
  the launcher's unexercised failed-launch path among them; the cross-lane readings for
  dgx-infra; the 2026-11-15 and later jobs against 12af_close/expected_nightly_20261111.md.
  This ruling states no standing rule: the id list, the glosses file and STANDING_RULES.md
  are untouched at 12ah-OPEN's item 0, asserted by content.
R811(f) — The lane line (sha256 a774632983299dbb5f78f13caa5a3d7b07222af3fdb159a1a5d00b70ad0ec306,
  7,574 bytes, 10 lines, ~/scratch/12ag-close/lane_line_12ag.txt) is approved on the condition
  above. First verify the file's sha256 and quote its fifth "+" line whole from the file, its
  code marks shown. If that line reads whole — every word separated from its neighbour, its
  "next:" clause running from 12ah-OPEN through U30's build with N46 to the freeze, the tag
  and Run 7 with no words missing — apply it: replace lines 41–45 of ~/claude-config/primer.md
  (which must equal its five "−" lines, taken from primer.md itself) with its five "+" lines,
  applied from the file, not from a paste; commit primer.md with any uncommitted hook row
  ("ledger" form, trailers, the row's exact timestamp, foreign rows counted from the diff and
  named by hash); push; level. If the line is broken in the file itself, STOP before anything
  is applied and report the line as it stands; the architect then rules its text.
R811(g) — 12ah-OPEN checks claude-config at the commit the wrap leaves (the lane-line commit, or
  a child carrying only hook rows, reported), level and clean. Its item 0 writes this ruling
  with transcribe_ruling.py as the next block (Block 209) and copies lane_line_12ag.txt into
  docs/session-reports/session-12/12ag_close/, with r811.txt beside it; no standing rule, the
  three rules files untouched by content. It runs session_open_check.py and the steps R747(b)
  keeps explicit, against the "Expected values at session-12ah open" block, the recount by
  12af_close/recount_12af_close.py (287; 38 done 33; R510 0 / 0); reads the 2026-11-15 and
  every later job run before it against 12af_close/expected_nightly_20261111.md; the gate by
  the wrapper as a unit, against 894–1092 s, its references
  12ag_close/collected_ids_12ag_close.txt and durations_12ag_unit.txt. Its brief is committed
  at its item 0 as brief_12ah_OPEN.md (R784(g)(v)), in 12ag_open/brief_12ag_OPEN.md's form.
  Then U30's build.
R811(h) — Session 12ag closes now. After R811(f) has landed, run /wrap; this ruling authorizes
  /wrap's own commits and pushes and nothing else. The project primer and the global primer
  record the session as the lane line states it. Report the lane-line commit's hash and
  parent, the fifth "+" line as quoted from the file, the wrap's verdict, and the archive
  command to be run after /exit.
===== END r811.txt =====

===== BEGIN r812.txt =====
RULING [evidence-engine 12ah-R812] — the no-live-write window from session 12ah: 09:00–10:35 UTC
  Monday to Saturday and 07:00–10:35 UTC on Sunday, by UTC day; the gate hold's clock follows
  it; one standing rule; amends R86 and R749(b) in timing only
STATUS: PI ruling, 2026-11-16 UTC, session 12ah, before its open, on architect recommendation, on
  the PI's report of his host changes of 2026-11-16 UTC (the dgx-infra lane; no Claude Code
  report). Transcribe at item 0 of the 12ah-OPEN brief below, by transcribe_ruling.py, in the
  block after R811's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED: none. No Claude Code report of this lane has read the health job's new schedule.
  READ (the architect's clone at 35a914b, docs/plan/STANDING_RULES.md): R86 "The no-live-write
    window is 07:00–10:35 UTC"; R749(b) "A brief is valid at any time outside 07:00–10:35 UTC
    (R86), and a gate must exit before 07:00 UTC."; R805(f) "bounded at sixty minutes and by
    the R86 clock". CLAUDE.md, one line beginning "- **No live write to review.db between
    07:00 and 10:35 UTC**".
  INFERRED — BLOCKING (the PI's report; verified at 12ah-OPEN's step 0 from the job's own
    schedule and script on the box, before item 0; false = STOP, nothing transcribed): the
    health job loads models on one day of the week only, and that day is the day (a) gives the
    longer window. The day and the hour as the box states them are readings, quoted.
  INFERRED — expectation (reported if false): on its other six days the health job sends
    read-only requests only and takes no lock; on its sweep day it stands down when the
    experiment lock is held.

R812(a) — Rule from here, a standing rule, the PI's ruling in his words: "From session 12ah, the
  no-live-write window is 09:00–10:35 UTC Monday to Saturday (the 09:00 nightly suite and the
  10:30 snapshot timer) and 07:00–10:35 UTC on Sunday (the full health sweep). A brief is
  valid outside the day's window, and a gate exits before the day's window opens. This amends
  R86 and R749(b) in timing only." With two sentences the architect recommended and the PI
  accepted ("yes with the two sentences"): the days are UTC days; and the clock that bounds
  the gate hold of R805(f), its "R86 clock", is the opening of the day's window.
R812(b) — What does not change: the jobs an open or a closeout reads and the file that governs
  each (R749(b)); the stop on a model load in a gate's window (R696(b)); the hold's one wait,
  by pid, and its sixty minutes (R805(f)). From session 12ah, "07:00–10:35 UTC" and "before
  07:00 UTC" in an earlier clause, a committed brief or an expected-values block are read as
  the day's window and its opening; none of them is edited (R102). A margin a brief sets
  before the window is set before the day's opening.
R812(c) — Readings, no row. The window follows this ruling, not the health job: if the job's
  schedule changes again, the window is ruled again. CLAUDE.md's no-live-write line states
  the old window; (a) governs from here, and the line's correction is put to the PI as quoted
  text with the next brief that edits CLAUDE.md, not at 12ah-OPEN.
R812(d) — This ruling states one standing rule, (a). The id list gains "A R812(a)" and
  STANDING_RULES.md is regenerated at the item 0 that transcribes this ruling (R766(b)). R86,
  R749(b) and R805(f) stay in the list; R812(a) is read with them. R811(e) and R811(g) say
  the three rules files are untouched at 12ah-OPEN's item 0; that is read as R811's own
  effect, and the files change there by this ruling alone.
===== END r812.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ah-OPEN] — session 12ah open: pre-check (the 12ag close and wrap
  claims against disk; the between-session readings: one interim read-only CC session,
  its scratch directory, the PI's host changes, the health job's schedule), the
  scheduled-job reads for every pair run since the 2026-11-14 pair, item 0 (R811 and R812
  as the next two blocks by transcribe_ruling.py; the 12ag lane-line copy; "A R812(a)" on
  the list and STANDING_RULES.md regenerated; this brief committed as pasted),
  session_open_check.py plus the explicit steps, the gate as a unit against 894–1092 s,
  the evidence in a commit of its own.
PURPOSE: Open refactor session 12ah on origin main and leave a verified base for the
  briefs that follow. Mixed: two docs commits, git, one standard pytest gate. No engine,
  test, script, spec, codebook or tools edit. No live write. No model call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  This brief authorizes one transient systemd-run --user unit, gate-12ah_open, launched
  by launch_gate.sh at step 3e, and nothing else of systemd.
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Blocks 209 and 210 appended by the tool,
  nothing else); the three rules files (one id line, one gloss, the regeneration);
  docs/session-reports/session-12/12ag_close/ (r811.txt and lane_line_12ag.txt at item 0;
  the job-read files at step 4); a new directory docs/session-reports/session-12/
  12ah_open/ (r812.txt and brief_12ah_OPEN.md at item 0; gate_12ah_open/ and
  between_sessions_read.txt at step 4); ~/scratch/12ah-open/ (new); the claude-config
  ledger (one commit per evidence-engine commit); git push on both repos; one standard
  gate.
  STOP / DO NOT TOUCH (beyond the global list): the form's list, read with 12ag for 12af
  and 12ah for 12ag (so: every file already in 12ag_close/ and every earlier close or
  open directory; ~/scratch/12ag-*/ read and copied from, never edited); CLAUDE.md;
  ~/scratch/12ah-olra/ (read and hashed; not edited, not copied and not committed here);
  the health job, its script, its schedule and every file of dgx-infra (read, never
  edited; crontab -l and systemctl cat, show and list-timers are reads); ollama.service
  (read by systemctl show; no restart, no stop, no pull, no model call).

FORM (R784(g)(iv), R811(g)): docs/session-reports/session-12/12ag_open/brief_12ag_OPEN.md
  (sha256 6cb11d640e5eacbcfaafaab176b8a44b285f7c77efdf41e7567f51365b6df573, 422 lines) is
  this brief's form. It is itself written on 12af_open/brief_12af_OPEN.md (sha256
  d6f66cff023f65eb1e59944a06c9c51e3cf72490ccb2237818e07307b6ea6d5f, 430 lines), which
  holds the steps' full text. Verify both hashes and read both Section 2s whole before
  step 0. Each step below is the step of the same number, 12af's text read through 12ag's
  deltas, with this brief's values in place of theirs; this brief states only what
  differs. Quote the step before running its counterpart. The record of the 12ag open
  (item 0 16cb307164c9203c695c3a4f842cf6d294c771ad, evidence
  60a63314118dc9f9ea41a009e5b5197676922b4c, the resume 12ag_open/brief_12ag_OPEN_R1.md,
  ~/scratch/12ag-open/) is the model for each file this open writes. Where a form and
  this brief disagree, this brief governs; where either disagrees with a tool or the
  record, the tool and the record govern and the difference is a finding (R732(a)). The
  deltas:
  D1. HEAD is the closeout commit and nothing sits on it. 12ag's D1 and its R9 fall away.
  D2. Two earlier CC sessions are read at 0f (V3). The later one was read-only and left
      files in scratch only (V4).
  D3. More than one job pair is expected (0a). Each date gets its own files. The health
      job's form changed between the pairs (V6, E3). The open check's nightly and health
      sections take the latest date; every earlier date is step 1's explicit read
      (R784(d)).
  D4. The hold is the standing rule R805(f), read with R812(a) for its clock, and is not
      restated. 12ag's D4, its E7 and its tmux launch fall away: the gate is launched as
      a unit by the tool (R808, R810).
  D5. 12ag's step 3g is not run. One read-only step is added, 0h, with its file in the
      evidence commit.
  D6. Item 0 transcribes two rulings and regenerates the rules file (2b, 2c2). Its models
      are named in step 2.
  D7. I1 expects other lanes' commits and rows (R804(a)): "clean" there means clean but
      for other lanes' uncommitted hook rows (R654(d)).

TIMING: not before 10:35 UTC on Monday 2026-11-16. At issue the architect's clock read
  00:52 UTC on 2026-11-16. Valid outside the day's window (R812(a), in Section 1):
  09:00–10:35 UTC Monday to Saturday and 07:00–10:35 UTC on Sunday, by UTC day. If the
  clock at step 0 is before 10:35 UTC on 2026-11-16, inside a day's window, or within 30
  minutes before one opens, STOP at once, write nothing and say so. The gate must exit
  before the next opening of a day's window: if step 3e cannot launch by 30 minutes
  before that opening, the hold included, STOP before 3e and report; item 0 stands. (Run
  on 2026-11-16 after 10:35 UTC, that opening is 09:00 UTC on Tuesday 2026-11-17 and the
  launch bound is 08:30 UTC.) If the clock reaches 15 minutes before an opening with a
  commit pending, hold with the work on disk uncommitted and resume after 10:35 UTC, as
  12ag_GATEUNIT_B/brief_12ag_GATEUNIT_B.md's TIMING carries R789(a). The pairs are read
  from the logs, never from this paragraph. Blocks 209 and 210's transcribed date is the
  item-0 commit's UTC date, read at commit time beside the day boundary (R775(a), (d));
  the made dates are the rulings' own: R811 2026-11-14, R812 2026-11-16.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ag-CLOSE closeout report and the wrap, as R811 quotes them in Section 1):
    the closeout is 35a914b, its parent item 0 e329ced; the plan 22eafc7d…2adc, 3,287,665
    B, 19,432 lines; 287 rows (94 / 57 / 136, 0 unclassed), fix units 38, done 33; R510
    in 0, out 0; the band 894–1092 s; the lane line's sha256, bytes and lines (V2).
  READ (the architect's clone of origin at 35a914b2bdd75147af104e5abfaea388eae3a057,
    2026-11-16 00:50 UTC; to be re-read on the box, not trusted):
    R1. docs/plan/ENGINE_REFACTOR_PLAN.md: sha256
        22eafc7ded18daa7a1f3b8bb69d206aa749c2592c4f4356ab90f17501cf52adc, 3287665 bytes,
        19432 lines; last line "The decision log in this file ends at R810."; last block
        208; no "**Block 209.**"; 165 lines begin "The decision log in this file ends at
        R".
    R2. "Expected values at session-12ah open" starts at plan line 5678 and ends before
        "## Decision log" (5717). It is the expected-values source for this open; read it
        whole at 0c. Its "outside 07:00–10:35 UTC" is read as R812(b) reads it. This
        brief repeats a value from it only where the value gates a stop.
    R3. Rules files, unchanged since 12ag-GATEUNIT-B's item 0: STANDING_RULES.md
        95329e5e2384afa01d68f7f42080f27592b55cb41fb7faef24413f12a566d3f4 (header "Tier A
        90, Tier B 8, 98 of 1694 clauses"); standing_rules_ids.txt
        f3fc106cfa1514a890cd4bac8d95927acf2034f49cb0197285b690c0d2bbda95 (last A line
        "A R808(d)"); standing_rules_glosses.json
        11579e9e7a0d3d2d0e97fef2f837c4013e78bf05045747bd1c6fda5768a7a60c (46 entries);
        12aa_SETUP/rules_glosses.json
        2a4e785c7edb0eb9552a9a1c753dae396592ace990171f1a2c4e5f07153e9dfe (57 entries);
        tools/extract_rules.py
        fc95e62a246a4571295a99180f61b93dd10fc11a528f98d4320dcb8bb82bd7f2.
    R4. 12ag_close/ holds 23 files. The job-read names of record are its four for
        2026-11-14 (health_job_, health_journal_, nightly_, nightly_ids_ with the job's
        date) and nvrm_this_boot_<read date>_read.txt. None is named r811.txt or
        lane_line_12ag.txt. 12ag_open/gate_12ag_open/ and 12ag_GATEUNIT_B/gate_12ag_unit/
        hold 19 files each. No 12ah_open/ exists.
    R5. Six-cell "| INT-" rows in the plan, by 12ag_close/r510_12ag.txt's method line:
        213 at 35a914b.
    R6. Block 201's header reads "in its own commit with the lane-line copy and the brief
        file it orders" and names its report "the 12af-CLOSE closeout report (item 0
        `<full hash>`; closeout `<full hash>`)". Block 207's reads "in its own commit
        with Block 208 and the brief file it orders", and Block 208's names Block 207 the
        same way.
    R7. 12af_close/recount_12af_close.py (sha256
        a86247439038b1610b989e1ed08f6744d5322ec98c55858bda7fbc9f68f03a47) gives its usage
        in its docstring; it writes its JSON beside itself. Its committed JSON is
        3657ce2f219c9fc130d5248295e99071855245d87dc2b7ab6d80e9b5c16d15a5.
    R8. On engine tests scripts analysis review_specs: git diff --stat 977c499 HEAD is
        empty; from f4a5586, 96 files, +6,609 / −345.
    R9. The tools at HEAD: launch_gate.sh
        a1d771d88d684504193c1d4d52950988dccebc2b2a06ef13df38e1d1fb2ad2c9; run_gate.sh
        be60c31fff5edc3997b00211e517a93d31f2f0f6b47b3ca44f2e3fb1a24f9327;
        transcribe_ruling.py
        18f69e20f143c48215f80285fa81369d8d67c4c453a69d8e8002e40366457151;
        session_open_check.py
        49acf8aad11dfcf4195bfbc05dae7a0836c17e22c3d132f04580fe685ac8b5e4.
    R10. 12af_close/expected_nightly_20261111.md is
        4c5927b71ca0533490f230e20806929e5d5d9b672a3673b67f348a9caed80e35;
        12ag_close/collected_ids_12ag_close.txt holds 3,574 lines
        (d63633b8071e6cc6ac003bbf58873e781830216afb2df1a18821221363322b0c);
        12ag_close/durations_12ag_unit.txt is
        38a63a0c02614f4ebbafe08cf36580002a3351564e003f4e43b34fd7adf208e4. `git ls-files`
        lists 30 files named brief_*.md.
    R11. CLAUDE.md holds one line beginning "- **No live write to review.db between 07:00
        and 10:35 UTC**" (at :252). It is a reading under R812(c) and is not edited.
  READINGS FROM OUTSIDE THE CLONE (unified plan v105, R811, and the PI's report of
    2026-11-16 UTC; INFERRED by source; each is the value an item below is checked
    against):
    V1. claude-config after the 12ag wrap: a2bbd81ffed41ccd6c6702dcdba2aa148724f0ca,
        parent 03d1bbefe103c684b53db40905ee0f574938da30. At the interim session's close:
        1ae06141c32f3694d2693258406de38d5ab71eba, past the wrap commit by other lanes'
        commits (a primer section 18, ledger rows), with primer.md lines 41–45 byte-equal
        and this lane's 673 ledger rows intact when read.
    V2. ~/scratch/12ag-close/lane_line_12ag.txt: sha256
        a774632983299dbb5f78f13caa5a3d7b07222af3fdb159a1a5d00b70ad0ec306, 7,574 B, 10
        lines.
    V3. CC sessions: acb83b21-b22a-402f-a4ad-1b12c11dc4ed (12ag; opened 2026-11-14 05:24
        UTC) and c72bc042-ed30-45b4-924f-2b0cddc440ea (the interim session, task
        12ah-OLRA, read-only; 2026-11-15 to 2026-11-16 UTC; reported wrapped
        CLEAN-TO-EXIT with no commit in either repository, exited and archived). Each
        one's first and last entry and its archive state are read from the record, never
        from this line.
    V4. ~/scratch/12ah-olra/ holds the interim read-out (12ah_OLRA.md and its addendum
        12ah_OLRA_add1.md), its brief, its three rulings (R1, R2, CLOSE) and three lists
        whose lines have the form name, bytes, sha256:
          SHA256SUMS        85850dccb00d6baeec7cb8201eb51ff09358439f2fcd87fd655cb58b624f7e5f
                            (18 entries)
          SHA256SUMS_add1   9cd077bc85069836344948dca79efe7880f3a8b010f619ae5978420cc2d20baa
          SHA256SUMS_close  22868d549d55c9ed9c0f96041d317689e5d35185c92fd952c54390252b5958c2
    V5. The last jobs read before this open: the 2026-11-14 pair, at 12ag-CLOSE.
    V6. Host changes by the PI, in the dgx-infra lane: ollama.service restarted
        2026-11-16 00:06:51 UTC (MainPID 3633749) with OLLAMA_KEEP_ALIVE=30m and
        RestartSec=10; the 07:00 UTC health job is a light check (GETs only, no lock)
        Monday to Saturday and the full 20-model sweep on Sunday, standing down when the
        experiment lock is held. The 12ah block's Ollama service start and its health-job
        form (twenty loads a day) are superseded by these as readings; a difference from
        the block on either is not a stop.
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. claude-config HEAD is the commit the 12ag wrap left, or a descendant acceptable
        under R663(b) and R811(g): level, clean but for other lanes' uncommitted hook
        rows, no row of this lane missing, and primer.md lines 41–45 equal to the lane
        line's five "+" lines.
    I2. Both CC sessions of V3 are exited with no running process, and nothing of this
        lane survives either.
    I3. Every nightly since the 12ag closeout ran on a HEAD whose engine tree (R8's five
        paths) equals 977c499's (reflog method with the lock line, R779(c)).
    I4. transcribe_ruling.py --append only appends: the plan's "−" count is read from the
        numstat and matched to zero, never assumed (R766(c)).
    I5. R812's blocking item: the health job loads models on one day of the week only,
        and that day is the day R812(a) gives the longer window. Read at 0h from the
        job's own schedule and script, quoted.
    I6. evidence-engine HEAD is the 12ag closeout commit: the interim session committed
        nothing.
  INFERRED — expectation (report a mismatch, continue):
    E1. ~/scratch/12ag-open/ still holds expected_12ag.json, block201.json and the item-0
        and step-4 checks, and the scratch of the regeneration model (step 2) still holds
        its block file and its command. If one is gone, the committed evidence and the
        tools' docstrings govern.
    E2. The recount's JSON is byte-equal to the committed one.
    E3. The 2026-11-15 health job loaded every model it lists with no FAIL. A health job
        on a day R812(a) gives the shorter window loaded no model. No new
        NV_ERR_NO_MEMORY line falls outside a health-job window.
    E4. The gate lands inside the band beside surgical-cv's uvicorn only, with no hold.
    E5. The host's four band values equal 12ag_close/host_state_close.txt's. If one
        differs the band series restarts: report it before the gate and do not match.
        The Ollama restart of V6 is not one of the four.
    E6. The interim CC session is archived, and every file the three lists of V4 name is
        in ~/scratch/12ah-olra/ at the bytes and sha256 its line states.
    E7. ollama.service reads as V6 states it, and the five digests equal the 12ah
        block's.

CONTRACT (steps, in order; evidence under ~/scratch/12ah-open/):

  STEP 0 — Pre-check (read-only). The form's 0a–0g, with:
    0a. Every 07:00 health run and 09:00 nightly after the 2026-11-14 pair (V5).
        EXPECTED on 2026-11-16 after 10:35 UTC: the 2026-11-15 and 2026-11-16 pairs; a
        later day's pair only past 10:35 UTC that day. A nightly that should exist and
        cannot be found = STOP. A health run that is not where the earlier ones were is
        a finding, not a stop: it is read where 0h's script puts it, and the Ollama
        journal's window for its hour is its record.
    0b. HEAD 35a914b2bdd75147af104e5abfaea388eae3a057 and its parent
        e329cedc280e8136cc825e56dc59c54fd9329968, each proved by cat-file -e; clean;
        fetch; 0 0; one worktree (I6). git diff --stat 977c499 HEAD on R8's paths empty.
        A commit on the closeout, or any other mismatch = STOP.
    0c. R1, R2, R3, R4, R9, R10 and R11. The directory listings are read and printed,
        not asserted; a name step 1, 2 or 4 will write that already exists = STOP
        (nothing is overwritten).
    0d. claude-config (I1, V1): the form's reads, with git log a2bbd81..HEAD, every
        commit named by hash and lane and classed: this lane's or another's ledger rows
        only; another lane's committed change under R663(b); anything else. V1's interim
        hash is proved by cat-file -e and looked for in that chain; its absence is a
        finding. This lane's ledger-row count is printed beside V1's figure, as a
        reading.
    0e. The lane line (V2) against ~/claude-config/primer.md 41–45, as the form's.
    0f. Leftovers (I2, V3): both sessions, each with its first and last entry and
        archive state from the record. No user unit named gate-* is loaded. A running
        process of either session, a loaded gate-* unit, or a survivor of this lane =
        STOP, reported, not killed. Other lanes' are readings.
    0g. The five 12ag scratch directories, ~/scratch/12ag-open/'s and 12ag-close/'s
        listings (E1), the temp root, and the project primer's 12ag block.
    0h. The between-session readings (read-only), written with each command and its
        timestamp to ~/scratch/12ah-open/between_sessions_read.txt:
        (i) The health job (I5). Its schedule as the box holds it (the crontab entry or
            entries, or the timer and its unit), quoted whole. The script each entry
            runs: its path, its sha256, and the lines that decide between the light
            check and the sweep, that read or take the experiment lock, and that name
            its log, quoted. From those lines: the day or days on which it loads
            models, the hour, the box's time zone, what the light check sends, and
            what the sweep does when the lock is held. I5 is false if models are loaded
            on any day other than Sunday by UTC, or if a sweep can start outside
            07:00–10:35 UTC. I5 false, or a schedule or script that cannot be read =
            STOP before step 1, nothing committed.
        (ii) The snapshot timer: dgx-snapshot-user's schedule line and its last and
            next trigger. A reading.
        (iii) ollama.service (E7): ExecMainStartTimestamp, MainPID, NRestarts and
            RestartUSec by systemctl show -p; the value of OLLAMA_KEEP_ALIVE and no
            other environment value; /api/version and /api/ps.
        (iv) ~/scratch/12ah-olra/ (V4, E6): its listing with bytes; each list's own
            sha256 against V4; then every line of each list against the named file's
            bytes and sha256, line by line, by a helper that reads the list (not
            sha256sum -c), the counts of lines, matches and mismatches printed, and
            any file in the directory that no list names.

  STEP 1 — The scheduled-job reads (read-only). The form's 1a–1b for each date 0a found,
    earliest first, written under ~/scratch/12ah-open/ in R4's names; one
    nvrm_this_boot_<read date>_read.txt for the whole open. Models: 12ag_close/'s
    2026-11-14 files; quote each header first.
    1a. Health (readings): as the form's, the NV_ERR_NO_MEMORY count against the 12ah
        block's figure, and model loads outside the health windows counted from the 12ag
        session's last entry as read at 0f, with the Ollama restart of V6 named where
        the journal shows it. For a date on which the light check ran: the requests in
        the window by method and path, the loads (E3), and the log's lines as they
        stand. The file keeps the name health_job_<date>.txt, and its header says which
        form ran.
    1b. Nightly (governed): line for line against 12af_close/expected_nightly_20261111.md
        (R10), that date in the log and durations names; ids as a set against
        12ag_close/collected_ids_12ag_close.txt, 0 / 0; the HEAD each ran on, named, and
        its engine tree compared with 977c499's (I3).
    A nightly mismatch on any date, or I3 false for any date, = STOP before step 2, both
    values and the date reported. Do not adapt.

  STEP 2 — Item 0 (one evidence-engine commit, one ledger commit; the check written for
    this brief, R794(i)). The form's 2a–2f, with the step added at 2c2. Three models,
    each quoted before its counterpart is composed. For the files and the elements of a
    wrap ruling's block: the 12ag open's item 0, 16cb307. For two rulings in one commit:
    no block is named; find the record's most recent commit that transcribed more than
    one block, quote its element order and clause form, and compose from it (R732(a)).
    For the regeneration: the record's most recent item 0 that regenerated
    STANDING_RULES.md, found in the log and named in the report (to be measured; the
    architect's reading of the log is b7e5fb1011632b7ff082c4aca7a5d6ad9a166424), with
    its command, both glosses files in it.
    2a. docs/session-reports/session-12/12ag_close/r811.txt and docs/session-reports/
        session-12/12ah_open/r812.txt from Section 1, each one write, each hashed
        straight after (R738(b)) against Section 1's hash. A mismatch on either = STOP.
    2b. ~/scratch/12ah-open/block209.json (R811) and block210.json (R812) in
        block201.json's key set (E1). Both: transcribed = the commit's UTC date; item
        "item 0 of the 12ah-OPEN brief"; made_by PI; sentence model, followed "12ah-OPEN
        brief" (each ruling reaches disk through this paste; Block 201 is the record's
        precedent for a wrap ruling). R811: made_date 2026-11-14; session 12ag; report
        as its STATUS names it, "the 12ag-CLOSE closeout report (item 0 <full hash>;
        closeout <full hash>)", the hashes in backticks in Block 201's form (R6);
        report_short the same with short hashes; prev_ends R810. R812: made_date
        2026-11-16; session 12ah; report as its STATUS names it, "the PI's report of his
        host changes of 2026-11-16 UTC (the dgx-infra lane; no Claude Code report)";
        report_short the same; prev_ends R811. Each block's "beside" is composed by CC
        from the models, not written here: the other block named as the pair's pattern
        names it; Block 209's also the lane-line copy, in Block 201's words; Block
        210's also the standing-rules line and the regeneration, in the words of the
        regeneration model's block; the brief file where the models place it. Quote the
        models and the two composed values in the report. Render both blocks to stdout,
        compare the element order with the models', then --append in order.
    2c. cp ~/scratch/12ag-close/lane_line_12ag.txt into 12ag_close/; sha256 equal to
        0e's.
    2c2. The standing rule (R812(d)): append "A R812(a)" to the id list in its Tier A
        form; add one gloss to docs/plan/standing_rules_glosses.json, composed by CC in
        that file's form from the clause's own words and quoted in the report;
        regenerate STANDING_RULES.md by the model's command. The header's counts before
        and after and the diff's numstat are to be measured (R707(a)) and reported.
    2d. docs/session-reports/session-12/12ah_open/brief_12ah_OPEN.md from this whole
        pasted block, "CLAUDE CODE" through the STOP-12AH-OPEN line, one trailing
        newline, written once; sha256, bytes and lines straight after the write,
        reported.
    2e. The item-0 check, as the form's, with: the last line "The decision log in this
        file ends at R812." once; the "ends at" lines counted before and after, every
        earlier one kept; the plan's line count the prior count plus N; r811.txt and
        r812.txt equal to 2a's; the id list equal to HEAD's plus the one line and
        nothing else; the glosses file equal to HEAD's plus the one key and nothing
        else; STANDING_RULES.md holding a section for R812(a) whose clause text is
        found in r812.txt (asserted by content, R777(b)); every changed line of
        STANDING_RULES.md classified by R766(b)'s three kinds, the classification
        written by the check into its evidence file, any other changed line = STOP;
        six-cell INT- rows 213 before and after; the staged set exactly the plan, the
        three rules files, r811.txt, lane_line_12ag.txt, r812.txt and
        brief_12ah_OPEN.md.
    2f. As the form's. The ledger message is built from the diff (R801(f)).

  STEP 3 — The session-open check, the explicit steps and the gate.
    3a. ~/scratch/12ah-open/expected_12ah.json in expected_12ag.json's shape, every
        section: repos at the 2f commits and their parents; the post-item-0 plan values
        and the three rules files as 2c2 left them; every reference file 12ag_close/'s,
        as the 12ah block names them; nightly and health at the latest date read in
        step 1 (D3); the Ollama service start as read at 0h (V6); cc_sessions with both
        ids of V3 and one window from the earlier session's first entry to the later
        one's last, as read at 0f. Every value from the 12ah block, a 12ag_close/ file
        or this run's own measurements. Quote the file.
    3b. As the form's. One exception: if the check's health section cannot read a
        light-check day, that section's failure alone is a finding for the tools items,
        reported with its output, and the run continues on step 1a's read. Any other
        failed section stops as the form's.
    3c. Recount (R7), in a scratch clone under ~/scratch/12ah-open/, with the arguments
        its docstring names, the last the plan at the 2f commit. EXPECTED: the 12ah
        block's pre-tag bullet (rows 287, 94 / 57 / 136, 0; 38 with 33 done;
        expected_holds true); E2. Rows other than 287, or dated lines other than 38 /
        33, = STOP before the gate.
    3d. R510 at this open: the six-cell INT- rows at 35a914b and at the 2f commit by
        R5's method. EXPECTED 213 and 213, in 0, out 0, no class or package move;
        written to ~/scratch/12ah-open/r510_12ah.txt. Another value = STOP before the
        gate.
    3e. The gate, as the 12ah block's gate bullets state it, by launch_gate.sh from the
        repository root: evidence_dir ~/scratch/12ah-open/gate_12ah_open/, name
        12ah_open (its home is the transient user unit gate-12ah_open, no memory cap;
        never a CC background task), ref_ids 12ag_close/collected_ids_12ag_close.txt,
        ref_durations 12ag_close/durations_12ag_unit.txt, no selection. The model is
        step 6 of 12ag_GATEUNIT_B/brief_12ag_GATEUNIT_B.md and its record,
        12ag_GATEUNIT_B/gate_12ag_unit/ with gate_launcher_stdout.txt and
        hold_snapshot.txt beside it. One wait on gate_rc.txt, its pid reported. Report
        at launch what the launcher prints. EXPECTED: the 12ah block's counts line for
        line; exit 0; ids 0 / 0; the unit's start line in the user journal, and a stop
        line reported if one appears; no model load in the gate's journal window; wall
        time inside 894–1092 s — outside it, reported with the concurrent jobs named,
        not adapted to and not re-run (R811(b)); the nineteen files by name against
        gate_12ag_unit/'s; durations against the reference, any phase slower by more
        than 10 s named. A red, a model load in the window, a unit that fails or is
        killed, or no gate_rc.txt at the wait's bound = STOP before 3f, the unit's
        status and journal reported.
    3f. As the form's, with the 12ah block's collection figure and R8's diffs against
        12ag_close/'s three diff files, compared by content.

  STEP 4 — The evidence commit (R731(c); model 60a6331). cp the gate's files into
    docs/session-reports/session-12/12ah_open/gate_12ah_open/, the launcher's stdout and
    the hold snapshot beside it under the model's names, step 1's files into 12ag_close/
    under their step-1 names, and between_sessions_read.txt into 12ah_open/; every
    sha256 equal after; scratch untouched. The check, the chain, the pushes, the
    fingerprint compares and the ledger row as the form's step 4.

ACCEPTANCE GATES (each step's EXPECTED values are its gate; in summary):
  G1. Step 0 matched or stopped; every full hash proved; I1, I2, I5 and I6 verified; 0h's
      file written.
  G2. Step 1: each nightly a line-for-line match on a tree equal to 977c499's, ids 0 / 0,
      its window empty; each health job reported as readings with its form named; the
      files written per date.
  G3. Step 2: one item-0 commit that passed 2e in one chain, two blocks, one id line, one
      gloss, the regeneration within R766(b)'s kinds; pushed, level, fingerprint
      IDENTICAL; the ledger row with trailers.
  G4. Step 3: the open check clean, or clean but for 3b's one exception; the recount at
      287 with 38 / 33; R510 213 / 213; the gate at the 12ah block's counts, inside the
      band or reported outside it, the hold reported as taken or not needed; 3f matched.
  G5. Step 4: every copy sha256-equal to its source; pushed, level, fingerprint
      IDENTICAL; its ledger row.

OUT OF SCOPE: U30's build and every later unit. The Ollama restart change: its design
  ruling, its build, and any edit to CLAUDE.md, review_spec.py or docs/architecture/.
  Copying or committing anything from ~/scratch/12ah-olra/ (the next brief's item 0).
  The tools items. Any file of dgx-infra, its ollama_lane_models.txt included. Any edit
  to the expected nightly file, either primer, a recount script, or a committed read-out
  or brief. Killing or cleaning any process, unit, tmux session, worktree or temp-root
  entry. Any model call or Ollama restart. /wrap.

CONSTRAINTS (task-only): the form's, with ~/scratch/12ah-open/ as the one place a helper
  may delete from (R658(c)).

FINAL REPORT FORMAT: the form's sections (0)–(8), without 12ag's section for 3g, and
  with: in (0) the claude-config chain since a2bbd81, each commit with its hash, lane
  and class; the two CC sessions' first and last entries and archive states; and 0h in
  full — the health job's schedule and script lines as quoted, the days and the hour
  read from them, the snapshot timer, ollama.service's values, and for
  ~/scratch/12ah-olra/ the listing, the three lists' hashes and the line-by-line
  counts; in (1) one row per date, each naming the health form that ran; in (2) both
  blocks' metadata as composed with the models quoted, the gloss, the regeneration's
  command, header before and after, numstat and classification, and the sha256, bytes
  and lines of r811.txt, r812.txt and the brief file; in (4) what the launcher printed,
  the unit's journal start line, and the hold — the snapshot before it, the job and pid
  waited on, the wait's start and end, or "not needed" with the snapshot. Then
  findings, numbered, each with its quoted line, including anything in R1–R11, V1–V6
  or the FORM section the box contradicts. Then STOP.

CONTRADICTION HANDLING: the form's, with these read for its values: a step-0 mismatch, a
  hash cat-file -e does not prove, I1, I2, I5 or I6 false, or a nightly mismatch or I3
  false under step 1 = STOP before any commit. A changed line outside R766(b)'s kinds,
  or a check that fails or crashes (R763(c)) = STOP before the item-0 commit. The clock
  at the launch bound before 3e launches (a hold included) = STOP before the gate; item
  0 stands. An expectation mismatch (E1–E7), every health reading and every difference
  from V1–V6 that no blocking item names are reported and the run continues. A spec
  premise that does not survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not start the next brief (the commit of the 12ah-OLRA
  read-out), the restart change or U30. Await the next brief or ruling in this session.
  STOP-12AH-OPEN
