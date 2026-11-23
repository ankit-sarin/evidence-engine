CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R825, PI ruling 2026-11-20 UTC; not on disk)
================================================================================

R825 is transcribed at item 0 of the brief below, by
docs/session-reports/tools/transcribe_ruling.py, as the next block of the decision log
(Block 223). The lines between the two marker lines, exclusive of the markers, are the
byte content of r825.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's
independent hash:
  r825.txt: sha256 f08a80d6bec8f7ab6948ac7791a123bc72d3834385480972499c23fe03801417,
            8687 bytes, 105 lines, one trailing newline.

===== BEGIN r825.txt =====
RULING [evidence-engine 12ai-R825] — wrap ruling: closeout accepted; the band set 881–1076 s;
  one stop this session, the architect's, and no pause; fifteen architect's errors; readings
  on the closeout's findings and choices; no standing rule; lane line approved on one
  condition and applied; close now
STATUS: PI ruling, 2026-11-20 UTC, session 12ai, on architect recommendation, on the PI's word
  "wrap" (R824(c)) and the 12ai-CLOSE closeout report (item 0
  45980ced45fc58a36107d16ad4c6ea9627946ae2; closeout
  dccc546155da11777916522882691e6aea9af817). Transcribe as item 0 of 12aj-OPEN by
  transcribe_ruling.py, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the closeout report, quoted; wrapped lines joined): "Commit:
    45980ced45fc58a36107d16ad4c6ea9627946ae2 (parent 366de1e). Plan +73 / −0."; "Ledger:
    6d31de6."; "r824.txt: dd3beb97…3b2d, 4,761 B, 62 lines. Equal to the architect's hash.";
    "Brief file: a1b8218a3d80f60e81cb7845bafe56c632886974db7d02917bf1bc5b2c468a5b, 30,739 B,
    420 lines." (the architect's copy: the same); "Closeout commit:
    dccc546155da11777916522882691e6aea9af817 (parent 45980ce)"; "Ledger:
    606f87038d9e843678acbb4f622b5ba9cb5a9ebd (parent 6d31de6), no foreign rows."; "plan
    numstat: +185 / -0"; "Plan now: a06d30f3…cc74, 3,510,630 B, 21,090 lines."; "Retention
    (E4 holds): 23 MATCH / 0 MISMATCH; probe 10 / 10 and 53 / 53"; "3593 passed, 17
    deselected, 1 xfailed, 2 warnings, exit 0 last"; "now {rows 289, class_1 94, class_2 59,
    class_3 136, without_class 0}, dated {units 39, done 35}, expected_holds true"; "R510:
    213 → 215; in 2"; "mean 978.655 s; x0.9 880.789; x1.1 1076.521"; "rounded inward:
    881–1076 s"; "NVRM 57 lines, 55 of them NV_ERR_NO_MEMORY"; the lane line
    "85bd8ddc602b5f1668a2071f5c488d4bc9843687c3357a47cd3193af3ec33667, 8,474 B, 10 lines";
    "Its five "−" lines equal primer.md lines 41–45 as they stand."
  READ (CC's): the five choices of section (3) and findings 1–4; "The fifth line has one
    unpaired ** at its end; the form's own fifth line does too, so I kept it."
  READ (the architect's clone at dccc546): the two full hashes in STATUS in a straight chain
    on 366de1e; 12ai_CLOSE/brief_12ai_CLOSE.md and 12ai_open/r824.txt equal to the
    architect's copies by cmp; the plan's sha256
    a06d30f373ed3220cebffa7b14bc1ba2ce9f955603d2e92b9cab3e7ac6a1cc74, 3510630 bytes, 21090
    lines, last line "The decision log in this file ends at R824."; "Expected values at
    session-12aj open" at plan :6045–:6083, read whole; the closure at :5900–:6043, its
    first and last lines and its "Carried forward" heading read, the rest not read line by
    line; 12ai_close/'s twenty-six files listed; band_computation.txt "mean 978.655 s; x0.9
    880.789; x1.1 1076.521" and "rounded inward: 881–1076 s"; git diff --stat 7fad72a HEAD
    on engine tests scripts analysis review_specs, empty.
  INFERRED — BLOCKING (verified before R825(f) is carried out; false = STOP, nothing applied):
    the lane-line file's fifth "+" line, read from the file, is the line the report quotes,
    word for word. The report's quotation reads whole; the file decides.

R825(a) — The 12ai-CLOSE closeout is accepted as committed: item 0 45980ce (Block 222; the
  brief file equal to the architect's copy; the rules files untouched), the closeout dccc546
  (the closure and the 12aj expected-values block, +185 / −0 at the matched position;
  12ai_close/'s twenty-six files; the new recount script and its recount; the band
  computation; the new expected-nightly file), ledgers 6d31de6 and 606f870. I1 and I2 held.
  E1–E7 held. The 2026-11-20 nightly matched the gate of the tree it ran on. Nine committed
  brief and resume files this session, nine equal to the architect's copies; the interim
  session's brief was not saved.
R825(b) — The band for 12aj is 881–1076 s: ±10% of 978.655 s, the mean of the last four standard
  gates on this host state (978.22, 995.53, 963.33, 977.54), rounded inward. All four were
  launched as units; none is a confounded member. The four ran different test counts (3,574,
  3,574, 3,594, 3,608 selected), a reading beside the figure. The series stands at forty-six.
  A 12aj gate outside the band is reported with its concurrent jobs, not adapted to.
R825(c) — The stops, counted from the rulings: one, the architect's — 12ai-CLNA at its step 2j
  (R822(a)). Pauses: none. CC's stops: none. The architect's errors this session, fifteen:
  R824(g)'s fourteen and the closeout's finding 1 (the brief called a net change of
  thirty-four ids "the thirty-four new ids"; the record shows thirty-seven and three). The
  closure's "fourteen" stands as written at its time; this clause governs. They are of two
  kinds: a statement about a file or a code path that no one had read it for, and a
  mechanism's behaviour taken from its name. The session's measured point for the estimate:
  one CC session (the open at 14:57 UTC on 2026-11-19 to this wrap) and one interim read-only
  session before it; two fix units landed, each with one gate; no standing rule; one
  publication read-out and one removal on a measured list; one stop, no pause.
R825(d) — Readings, no row. The closeout's five choices are accepted as made. Findings 2 and 3
  are CC's own slips, each corrected before the commit (a rounding comparison; a header line
  counted). Finding 4: the kept clones number five, as R824(e) allows.
R825(e) — Carried to 12aj and the plan, from the closure's "Carried forward": U31's Phase A,
  then U32–U36; INT-ai12-2 (Class 2, P4); the read-out R819(k) orders before the freeze; the
  stale restart text, HK-ARCH's (R820(e)); R807(d)'s three items; the tools items; the five
  kept clones; the cross-lane readings for dgx-infra and what is owed between lanes
  (R824(f)); the 2026-11-21 and later nightlies against
  12ai_close/expected_nightly_20261120.md. This ruling states no standing rule: the id list,
  the glosses file and STANDING_RULES.md are untouched at 12aj-OPEN's item 0, asserted by
  content.
R825(f) — The lane line (sha256 85bd8ddc602b5f1668a2071f5c488d4bc9843687c3357a47cd3193af3ec33667,
  8,474 bytes, 10 lines, ~/scratch/12ai-close/lane_line_12ai.txt) is approved on the condition
  above, its fifth line's closing mark kept as the form has it. First verify the file's
  sha256 and quote its fifth "+" line whole from the file, its code marks shown. If it is the
  line the closeout report quotes — its "next:" clause running from 12aj-OPEN through U31's
  Phase A and U32–U36 to the freeze, the tag and Run 7 with no words missing — apply it:
  replace lines 41–45 of ~/claude-config/primer.md (which must equal its five "−" lines,
  taken from primer.md itself) with its five "+" lines, applied from the file, not from a
  paste; commit primer.md with any uncommitted hook row ("ledger" form, trailers, the row's
  exact timestamp, foreign rows counted from the diff and named by hash); push; level. If the
  line differs in the file, STOP before anything is applied and report the line as it
  stands; the architect then rules its text.
R825(g) — 12aj-OPEN checks claude-config at the commit the wrap leaves (the lane-line commit, or
  a child carrying only hook rows, reported), level and clean but for other lanes' hook rows.
  Its item 0 writes this ruling with transcribe_ruling.py as the next block (Block 223) and
  copies lane_line_12ai.txt into docs/session-reports/session-12/12ai_close/, with r825.txt
  beside it; no standing rule, the three rules files untouched by content. It runs
  session_open_check.py and the steps R747(b) keeps explicit, a light-check day's health read
  among them, against the "Expected values at session-12aj open" block, the recount by
  12ai_close/recount_12ai_close.py (289; 39 done 35); reads the 2026-11-21 and every later
  job run before it against 12ai_close/expected_nightly_20261120.md; the gate by the wrapper
  as a unit, against 881–1076 s, its references 12ai_close/collected_ids_12ai_close.txt and
  durations_12ai_u30b.txt. Its brief is committed at its item 0 as brief_12aj_OPEN.md
  (R784(g)(v)), in 12ai_open/brief_12ai_OPEN.md's form. Then U31's Phase A.
R825(h) — Session 12ai closes now. After R825(f) has landed, run /wrap; this ruling authorizes
  /wrap's own commits and pushes and nothing else. The project primer and the global primer
  record the session as the lane line states it. Lane status: closed clean; next, 12aj-OPEN.
  Report the lane-line commit's hash and parent, the fifth "+" line as quoted from the file,
  the wrap's verdict, and the archive command to be run after /exit.
===== END r825.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12aj-OPEN] — session 12aj open: pre-check (the 12ai close and wrap
  claims against disk; the between-session readings: the health job's schedule, the
  service, the kept and removed scratch clones, the lane-model list), the scheduled-job
  reads for every pair run since the 2026-11-20 pair (a Sunday sweep among them), item 0
  (R825 as the next block by transcribe_ruling.py; the 12ai lane-line copy; no standing
  rule; this brief committed as pasted), session_open_check.py plus the explicit steps,
  the gate as a unit against 881–1076 s, the evidence in a commit of its own.
PURPOSE: Open refactor session 12aj on origin main and leave a verified base for U31's
  Phase A. Mixed: two docs commits, git, one standard pytest gate. No engine, test,
  script, spec, codebook, rules-file or tools edit. No live write. No model call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
  A fresh CC session.
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  This brief authorizes one transient systemd-run --user unit, gate-12aj_open, launched
  by docs/session-reports/tools/launch_gate.sh at step 3e, and nothing else of systemd.
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 223 appended by the tool, nothing
  else); docs/session-reports/session-12/12ai_close/ (r825.txt and lane_line_12ai.txt at
  item 0; the job-read files at step 4); a new directory docs/session-reports/session-12/
  12aj_open/ (brief_12aj_OPEN.md at item 0; gate_12aj_open/, its two side files and
  between_sessions_read.txt at step 4); ~/scratch/12aj-open/ (new); the claude-config
  ledger (one commit per evidence-engine commit); git push on both repos; one standard
  gate.
  STOP / DO NOT TOUCH (beyond the global list): the form's list, read with 12ai for 12ah
  and 12aj for 12ai (so: every file already in 12ai_close/ and every earlier close or
  open directory; ~/scratch/12ai-*/ read and copied from, never edited); the three rules
  files; CLAUDE.md; the five kept clones of V7 (named and listed from outside; not
  entered, and no git command run inside one, R822(e)) and every other entry of
  ~/scratch (nothing deleted, moved or cleaned; none entered except where a step names
  it); every home folder of another lane (not read, and not named in this brief or in
  any file this open commits); the health job, its script, its schedule and every file
  of dgx-infra (read, never edited); ollama.service (read by systemctl show; no restart,
  no stop, no pull, no model call).

PATHS: in this brief a name of the form 12xx_<name>/ is a directory of
  docs/session-reports/session-12/, and a tool is named by its full path under
  docs/session-reports/tools/. ~/scratch names are written whole.

FORM (R784(g)(iv), R825(g)): docs/session-reports/session-12/12ai_open/brief_12ai_OPEN.md
  (sha256 f7e8501a259e7b4f9d3cb7e06e0236216707ee65ab9f3d9e9430de111d929302, 557 lines) is
  this brief's form. It is written on 12ah_open/brief_12ah_OPEN.md (sha256
  3ed2db46ae90ece0d63f4081670bfa49a02c4994149b65ea25111daded860bd8, 603 lines), that on
  12ag_open/brief_12ag_OPEN.md (sha256
  6cb11d640e5eacbcfaafaab176b8a44b285f7c77efdf41e7567f51365b6df573, 422 lines), and that
  on 12af_open/brief_12af_OPEN.md (sha256
  d6f66cff023f65eb1e59944a06c9c51e3cf72490ccb2237818e07307b6ea6d5f, 430 lines), which
  holds the steps' full text. Verify the four hashes and read the four Section 2s whole
  before step 0. Each step below is the step of the same number, 12af's text read through
  12ag's deltas, then 12ah's, then 12ai's, with this brief's values in place of theirs;
  this brief states only what differs. Quote the step before running its counterpart. The
  record of the 12ai open (item 0 7ff68fe697ee94562c540158a43bc81f77523e75, evidence
  35fc9ec694f07c05db787f3a504611f7fdbb8b67, ~/scratch/12ai-open/) is the model for each
  file this open writes. Where a form and this brief disagree, this brief governs; where
  either disagrees with a tool or the record, the tool and the record govern and the
  difference is a finding (R732(a)). The deltas from 12ai's brief:
  D1. One ruling, R825, and no standing rule, as 12ai's D1. The three rules files are
      asserted untouched (2e).
  D2. One earlier CC session is read at 0f (V3). The architect knows of no CC session of
      this lane between the 12ai wrap and this open. 12ai's D2, its V4, V6 and E6, and
      its 0h(iv) on ~/scratch/12ai-scri/ fall away; that directory is one of the nine
      of V7 and is listed at 0g like the others.
  D3. Job pairs: every pair after the 2026-11-20 pair. Each date gets its own files.
      2026-11-21 is a Saturday (a light check is expected) and 2026-11-22 a Sunday: the
      first full sweep run by the light/full script (R13). Its log is read as it
      stands; no line of it is predicted here.
  D4. 0h keeps 12ai's (i), (ii), (iii), (v) and (vi), each with this brief's values;
      (iv) is replaced by a read of the kept and removed clones from outside, and (vii)
      is added, a read of dgx-infra's lane-model list.
  D5. The job-read files and r825.txt go to 12ai_close/, as 12ai's went to 12ah_close/.
  D6. I1 as 12ai's D6: "clean" for claude-config means clean but for other lanes'
      uncommitted hook rows (R654(d), R825(g)).
  D7. The engine tree changed in 12ai. Every comparison 12ai's brief made against
      977c499's tree is made here against 7fad72a's (R8), and the nightly's expected
      file is 12ai_close/expected_nightly_20261120.md (R10).
  D8. Task-only, for this open (3b): a failure of the open check's health section alone
      does not stop the run, on a light-check day (the known tools item, R813(e)) or on
      the sweep day (the tool has not read a sweep written by the light/full script).
      Health is a reading at every step of the form; step 1a's read stands in its place.

TIMING: valid from issue. At issue the architect's clock read 03:38 UTC on Monday
  2026-11-23, before that day's window. Valid outside the day's window (R812(a)):
  09:00–10:35 UTC Monday to Saturday and 07:00–10:35 UTC on Sunday, by UTC day. If the
  clock at step 0 is inside a day's window, or within 30 minutes before one opens, STOP
  at once, write nothing and say so. The gate must exit before the next opening of a
  day's window: if step 3e cannot launch by 30 minutes before that opening, the hold
  included, STOP before 3e and report; item 0 stands. (Run on 2026-11-23 before 08:30
  UTC, that opening is 09:00 UTC that day and the launch bound is 08:30 UTC. Run after
  10:35 UTC on 2026-11-23, the opening is 09:00 UTC on Tuesday 2026-11-24 and the bound
  08:30 UTC that day.) If the clock reaches 15 minutes before an opening with a commit
  pending, hold with the work on disk uncommitted and resume after 10:35 UTC, as the
  form's TIMING. The pairs are read from the logs, never from this paragraph. Block
  223's transcribed date is the item-0 commit's UTC date, read at commit time beside the
  day boundary (R775(a), (d)); its made date is the ruling's own, 2026-11-20.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ai-CLOSE closeout report and the wrap, as R825 quotes them in Section 1):
    the closeout is dccc546, its parent item 0 45980ce; the plan a06d30f3…cc74, 3,510,630
    B, 21,090 lines; 289 rows (94 / 59 / 136, 0 unclassed), fix units 39, done 35; R510
    213 → 215, in 2; the band 881–1076 s; the lane line's sha256, bytes and lines (V2).
  READ (the architect's clone of origin at dccc546155da11777916522882691e6aea9af817,
    2026-11-23 03:38 UTC; to be re-read on the box, not trusted):
    R1. docs/plan/ENGINE_REFACTOR_PLAN.md: sha256
        a06d30f373ed3220cebffa7b14bc1ba2ce9f955603d2e92b9cab3e7ac6a1cc74, 3510630 bytes,
        21090 lines; last line "The decision log in this file ends at R824."; last block
        222 (its header at :21019); no "**Block 223.**"; 179 lines begin "The decision
        log in this file ends at R".
    R2. "Expected values at session-12aj open" starts at plan line 6045 and its list of
        first items ends at 6083. It is the expected-values source for this open; read
        it whole at 0c. This brief repeats a value from it only where the value gates a
        stop.
    R3. Rules files: docs/plan/STANDING_RULES.md
        d56dbdaeafb3659527cb77e1e81744eb01dadbe871fd96944104bb9e01b7e3ff;
        docs/plan/standing_rules_ids.txt
        a28da0611e1a4d62cbc8185ff89df22d2df7a2a37340da0eb164dec678e75257;
        docs/plan/standing_rules_glosses.json
        7f7374b25f7efd2c1df97cfbe3fb9b828c7fec5bce4e948d751cec08a4fa72ed (47 entries).
    R4. 12ai_close/ holds 26 files. None is named r825.txt or lane_line_12ai.txt. Its
        job-read names are the 2026-11-20 four (health_job_, health_journal_, nightly_,
        nightly_ids_), nvrm_this_boot_20261120.txt and nvrm_this_boot_20261120_read.txt;
        it holds none for a later date. The names of record for several dates are
        12ag_close/'s for 2026-11-15 to 2026-11-18. 12ai_open/gate_12ai_open/ holds 19
        files, with gate_launcher_stdout.txt and hold_snapshot.txt beside it. No
        12aj_open/ exists.
    R5. Six-cell "| INT-" rows in the plan: 12ai_close/r510_12ai.txt reads "b13b5eb:
        213; 45980ce: 215; rows in 2 ['INT-ai12-1', 'INT-ai12-2']; rows out 0 []", its
        method line "lines starting '| INT-' with six ' | '-separated cells". The
        architect's count by that method at dccc546: 215.
    R6. Block 214's header (R816, one wrap ruling with its lane-line copy) reads "in its
        own commit with the lane-line copy and the brief file it orders" and names its
        report "the 12ah-CLOSE closeout report (item 0 `<full hash>`; closeout `<full
        hash>`)". R825's STATUS names more than a report: "on the PI's word "wrap"
        (R824(c)) and the 12ai-CLOSE closeout report (item 0 …; closeout …)". No block
        header line in the plan (lines beginning "**Block <n>.**") holds the words
        "PI's word", by grep: that clause has no model in a header (2b).
    R7. 12ai_close/recount_12ai_close.py (sha256
        6f8595520de10e9df3988f3fbad30a248bb89d1ba5a74bcf6684b35401d5d3a0) gives its usage
        in its docstring: ten plan arguments, "the first nine files are `git show
        <commit>:…` of the plan", the tenth docs/plan/ENGINE_REFACTOR_PLAN.md; "Writes
        recount_12ai_close.json beside itself and prints it." Its committed JSON is
        7f1de39c4b6e1f09c60e904db1c66336d9a5efdcca34f92fc5d86b775f8f0f00.
    R8. On engine tests scripts analysis review_specs: git diff --stat 7fad72a HEAD is
        empty; from b13b5eb and from 977c499, "18 files changed, 1282 insertions(+), 121
        deletions(-)"; from f4a5586, "102 files changed, 7861 insertions(+), 436
        deletions(-)". 12ai_close/ holds diff_engine_since_b13b5eb.txt,
        diff_since_977c499.txt and diff_since_f4a5586.txt.
    R9. The tools at HEAD, each under docs/session-reports/tools/: launch_gate.sh
        a1d771d88d684504193c1d4d52950988dccebc2b2a06ef13df38e1d1fb2ad2c9; run_gate.sh
        be60c31fff5edc3997b00211e517a93d31f2f0f6b47b3ca44f2e3fb1a24f9327;
        transcribe_ruling.py
        18f69e20f143c48215f80285fa81369d8d67c4c453a69d8e8002e40366457151;
        session_open_check.py
        49acf8aad11dfcf4195bfbc05dae7a0836c17e22c3d132f04580fe685ac8b5e4. Each equals
        the value 12ai's brief gave at its R9.
    R10. 12ai_close/expected_nightly_20261120.md is
        715e1ca068ebcf32acd298d7c7ddf83696393f54a1b311ed54d260a37788ccc4;
        12ai_close/collected_ids_12ai_close.txt holds 3,608 lines
        (4d0c25d559afe6568371039adc697412bb3b3b434c8613e8f3bc768fe5b2fa96);
        12ai_close/durations_12ai_u30b.txt is
        7dc3913381648feae656dd01514de53426fe154220c350647ed10de3b25f2519. git ls-files
        lists 47 files named brief_*.md.
    R11. session_open_check.py's docstring: "usage, from the repository root:
        session_open_check.py <expected.json>" and "a section the file omits is not run
        and is printed as SKIPPED". It names fourteen sections. Its health section
        compares the file's "models" value with a count of the log's test lines that
        hold " OK " ("health last {v['models']} models OK"). No file named
        expected_12*.json is committed; one committed expected-values config exists,
        12aa_SETUP/open_expected_validation.json (R817(g)), and no value of this open
        comes from it.
    R12. 12ai_close/host_state_close.txt:74–75: the health job's crontab line "0 7 * * *
        /bin/bash /home/ankitsarin/scripts/ollama_health_check.sh >> /home/ankitsarin/
        logs/ollama_health_cron.log 2>&1" and its script's sha256
        42c11f18ba8d3411b79196cc7c39c893ed67331669fc9cc7fa1ae058cd7de50b. Line 73:
        "ollama.service ExecMainStartTimestamp=Mon 2026-11-16 00:06:51 UTC;
        MainPID=3633749; NRestarts=0; RestartUSec=10s; OLLAMA_KEEP_ALIVE=30m".
    R13. The health script's lines as 12ai_open/between_sessions_read.txt quotes them:
        "if [[ "$(date -u +%u)" == 7 ]]; then" / "MODE=full;  MODE_WHY="Sunday (UTC) full
        sweep"", and for the sweep "MODELS=$(ollama list 2>/dev/null | tail -n +2 | awk
        '{print $1}')": the sweep tests every model installed that day, so its count is
        the day's and twenty is the 2026-11-15 figure, not a constant. The light check
        "FAILs on any model listed in ollama_lane_models.txt that is missing from
        /api/tags". 12ag_close/health_job_20261115.txt's header: "the dated log carries
        no 'Mode:' line; the light/full script landed later". No sweep written by the
        light/full script is in the record.
    R14. 12ai_CLNB/to_remove.tsv holds 54 lines: one header and 53 rows, the first
        column ("top") each removed clone's path.
  READINGS FROM OUTSIDE THE CLONE (unified plan v107, R825, and the PI's note of
    2026-11-22; INFERRED by source; each is the value an item below is checked against):
    V1. claude-config after the 12ai wrap: 42fabff7b6b2db8d330adad4322a3845cb11c335 (the
        12ai lane-line commit), parent 606f87038d9e843678acbb4f622b5ba9cb5a9ebd.
    V2. ~/scratch/12ai-close/lane_line_12ai.txt: sha256
        85bd8ddc602b5f1668a2071f5c488d4bc9843687c3357a47cd3193af3ec33667, 8,474 B, 10
        lines. Since the wrap applied it, its five "+" lines equal
        ~/claude-config/primer.md 41–45. The wrap report's quotation of the fifth line
        showed words joined where code marks stand; CC stated the file's line matched
        word for word. A display reading: 0e shows the marks.
    V3. CC session 6cc698ba-95fa-44b1-adff-b232a32f62cc (12ai; opened 2026-11-19 14:57
        UTC; wrapped CLEAN-TO-EXIT, exited, archived). Its first and last entry and its
        archive state are read from the record, never from this line. The interim
        session bf3224f8 was read at the 12ai open and is not read again.
    V4. The last jobs read before this open: the 2026-11-20 pair, at the 12ai closeout.
    V5. ollama.service as R12's line 73 gives it; Ollama 0.21.0; nothing resident at
        the closeout.
    V6. The wrap's audit reported an uncommitted path in ~/projects/dgx-infra and a
        stale-session warning, neither this lane's. Readings if seen; not looked for.
    V7. ~/scratch: the nine 12ai directories (12ai-open/, 12ai-scri/, 12ai-scri-reca/,
        12ai-scri-rec/, 12ai-u41b/, 12ai-u30b/, 12ai-clna/, 12ai-clnb/, 12ai-close/)
        beside those present at the 12ai open and retained/; the five kept clones
        12ai-open/recount, 12ai-u41b/clone, 12ai-u30b/clone, 12ai-clna/clone and
        12ai-close/recount_clone; the fifty-three paths of R14 absent.
    V8. The PI's note: the workbench has not yet reported the merge sha for adding
        deepseek-r1:32b to the Evidence Engine block of dgx-infra's
        scripts/ollama_lane_models.txt. Whether the line is there is not known to the
        architect; 0h(vii) reads it.
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. claude-config HEAD is V1's commit, or a descendant acceptable under R663(b) and
        R825(g): level, clean but for other lanes' uncommitted hook rows, no row of this
        lane missing, and primer.md lines 41–45 equal to the lane line's five "+" lines.
    I2. The CC session of V3 is exited with no running process, nothing of this lane
        survives it, and no CC session of this lane has a record entry between that
        session's last entry and this open's first. A session found in that interval is
        reported with its id, first and last entry, and = STOP before step 1.
    I3. Every nightly since the 12ai closeout ran on a HEAD whose engine tree (R8's five
        paths) equals 7fad72a's (reflog method with the lock line, R779(c)).
    I4. transcribe_ruling.py --append only appends: the plan's "−" count is read from the
        numstat and matched to zero, never assumed (R766(c)).
    I5. The health job still loads models on Sunday by UTC only, from 07:00 UTC, as
        R813(b) measured: read at 0h from its schedule and script. False if models are
        loaded on any other day, or a sweep can start outside 07:00–10:35 UTC. A changed
        script hash or crontab line with I5 still true is a finding, not a stop.
    I6. evidence-engine HEAD is the 12ai closeout commit, clean and level, and no ref of
        the repository moved between the closeout's push and this open.
  INFERRED — expectation (report a mismatch, continue):
    E1. ~/scratch/12ai-open/ still holds expected_12ai.json, block214.json and the
        item-0 and step-4 checks. If one is gone, the committed evidence and the tools'
        docstrings govern, and what was missing is named in the report.
    E2. The recount's JSON is byte-equal to the committed one (R7).
    E3. The 2026-11-21 health job ran as a light check: three GET requests, no load,
        "Mode: light" in its log. The 2026-11-22 job ran the full sweep, held no FAIL
        and did not stand down on the lock. New NV_ERR_NO_MEMORY lines, if any, fall
        inside the sweep's window (55 this boot before them, the last 2026-11-15
        07:03:19 UTC; the 2026-11-15 sweep added eighteen). A light check that reports a
        lane model missing is quoted whole.
    E4. The gate lands inside the band beside surgical-cv's uvicorn only, with no hold.
    E5. The host's four band values (kernel, Python, libsqlite3-0, NVIDIA driver) equal
        12ai_close/host_state_close.txt's. If one differs the band series restarts:
        report it before the gate and do not match.
    E6. The five kept clones of V7 are present and the fifty-three paths of R14 absent.
    E7. ollama.service reads as V5 states it, and the five digests equal the 12aj
        block's.
    E8. No file of this open's steps is read from a clone of evidence-engine under
        ~/scratch other than the one this open makes at 3c.

CONTRACT (steps, in order; evidence under ~/scratch/12aj-open/):

  STEP 0 — Pre-check (read-only). The form's 0a–0h, with:
    0a. Every 07:00 health run and 09:00 nightly after the 2026-11-20 pair (V4).
        EXPECTED on 2026-11-23 before that day's window: the 2026-11-21 and 2026-11-22
        pairs; a later day's pair only past 10:35 UTC that day. A nightly that should
        exist and cannot be found = STOP. A health run that is not where the earlier
        ones were is a finding.
    0b. HEAD dccc546155da11777916522882691e6aea9af817 and its parent
        45980ced45fc58a36107d16ad4c6ea9627946ae2, each proved by cat-file -e; clean;
        fetch; 0 0; one worktree (I6). git diff --stat 7fad72a HEAD on R8's paths empty.
        Before this step's own fetch, 0h(v) is read. A commit on the closeout, or any
        other mismatch = STOP.
    0c. R1–R4, R7, R9–R12 and R14. The directory listings are read and printed, not
        asserted; a name step 1, 2 or 4 will write that already exists = STOP (nothing
        is overwritten).
    0d. claude-config (I1, V1): the form's reads, with both hashes of V1 proved by
        cat-file -e and git log 42fabff..HEAD, every commit named by hash and lane and
        classed: this lane's or another's ledger rows only; another lane's committed
        change under R663(b); anything else. This lane's ledger-row count is printed, as
        a reading.
    0e. The lane line (V2): the file's sha256, bytes and lines, and its five "+" lines
        against ~/claude-config/primer.md 41–45, taken from primer.md itself. Then
        primer.md line 45 printed whole with its code marks visible.
    0f. Leftovers (I2, V3): the session, with its first and last entry and archive
        state from the record; the record searched for any session of this lane after
        its last entry. No user unit named gate-* is loaded. A running process of that
        session, a loaded gate-* unit, a session I2 does not allow, or a survivor of
        this lane = STOP, reported, not killed. Other lanes' are readings.
    0g. ~/scratch's top-level listing, printed (V7; an entry V7 does not account for is
        a finding); ~/scratch/12ai-open/'s and 12ai-close/'s listings (E1); the temp
        root; the project primer's 12ai block.
    0h. The between-session readings (read-only), written with each command and its
        timestamp to ~/scratch/12aj-open/between_sessions_read.txt, in the form of
        12ai_open/between_sessions_read.txt:
        (i) The health job (I5): as 12ai's 0h(i), against R12's lines.
        (ii) The snapshot timer, as the form's. A reading.
        (iii) ollama.service (E7), as the form's.
        (iv) The clones (E6, V7), from outside only: for each of the five kept clones,
            that its path exists and is a directory, by a test and stat, nothing
            listed beneath it and no git command with it as working directory or -C
            target; for each of R14's fifty-three paths, read from the committed
            to_remove.tsv, that it does not exist. Counts printed: kept present, removed
            absent, and any path that reads otherwise, named.
        (v) The refs (I6), read before 0b's fetch: in ~/projects/evidence-engine,
            .git/FETCH_HEAD's mtime and content; the reflogs of HEAD, main and
            origin/main with dates, showing no entry after the closeout's push of
            2026-11-20; origin/main equal to HEAD. A later entry, or a ref not at
            dccc546 = STOP.
        (vi) How this open's config is supplied (R11): quote session_open_check.py's
            usage line; state whether ~/scratch/12ai-open/expected_12ai.json exists,
            with its sha256. This open builds its own file at 3a and passes it by
            path; the 12ai file is read as a shape model only, and no step of this
            open takes a value from it or from 12aa_SETUP/open_expected_validation.json.
        (vii) The lane-model list (V8), a reading, no stop: the path and sha256 of
            dgx-infra's scripts/ollama_lane_models.txt; the lines of its Evidence
            Engine block, quoted whole; whether deepseek-r1:32b is among them; the last
            commit of dgx-infra that touched the file (full hash, commit date, subject)
            and whether the working copy of that file differs from it. Nothing is
            concluded here about what the workbench owes; the lines are for the PI.

  STEP 1 — The scheduled-job reads (read-only). The form's 1a–1b for each date 0a found,
    earliest first, written under ~/scratch/12aj-open/ in R4's names; one
    nvrm_this_boot_<read date>_read.txt for the whole open. Models: 12ag_close/'s
    2026-11-18 files for a light-check day and its 2026-11-15 files for a sweep day;
    quote each header first.
    1a. Health (readings): as the form's, the NV_ERR_NO_MEMORY count against 55, and
        model loads outside the health windows counted from the 12ai session's last
        entry as read at 0f. The file's header says which form ran (E3). For the sweep
        day also: the log's own mode line if it has one, quoted; the number of models
        tested, beside twenty; whether the log shows the lock consulted and its state;
        each new NV_ERR_NO_MEMORY line with the load beside it.
    1b. Nightly (governed): line for line against 12ai_close/expected_nightly_20261120.md
        (R10), that date in the log and durations names; ids as a set against
        12ai_close/collected_ids_12ai_close.txt, 0 / 0; the HEAD each ran on, named, and
        its engine tree compared with 7fad72a's (I3).
    A nightly mismatch on any date, or I3 false for any date, = STOP before step 2, both
    values and the date reported. Do not adapt.

  STEP 2 — Item 0 (one evidence-engine commit, one ledger commit; the check written for
    this brief, R794(i)). The form's 2a–2f as 12ai ran them. One model for all of it,
    quoted before each counterpart is composed: the 12ai open's item 0,
    7ff68fe697ee94562c540158a43bc81f77523e75 (Block 214: one wrap ruling, its lane-line
    copy, the brief file, the check, the ledger message).
    2a. 12ai_close/r825.txt from Section 1, one write, hashed straight after (R738(b))
        against Section 1's hash. A mismatch = STOP.
    2b. ~/scratch/12aj-open/block223.json in block214.json's key set (E1): transcribed =
        the commit's UTC date; item "item 0 of the 12aj-OPEN brief"; made_by PI;
        made_date 2026-11-20; session 12ai; report as R825's STATUS names it, whole —
        the PI's word "wrap" (R824(c)) and the 12ai-CLOSE closeout report (item 0 <full
        hash>; closeout <full hash>) — the hashes in backticks in Block 214's form
        (R6); report_short the same with short hashes; sentence model, followed
        "12aj-OPEN brief"; prev_ends R824; "beside" in Block 214's words (R6), read from
        block214.json or from Block 214's header, not from this line. Render the block
        to stdout and compare its header's element order with Block 214's. The report
        clause is longer than any model's (R6): if the tool refuses it, or the rendered
        header does not read as one sentence, STOP before --append and report the
        render. Otherwise --append.
    2c. cp ~/scratch/12ai-close/lane_line_12ai.txt into 12ai_close/; sha256 equal to
        0e's.
    2d. 12aj_open/brief_12aj_OPEN.md from this whole pasted block, "CLAUDE CODE" through
        the STOP-12AJ-OPEN line, one trailing newline, written once; sha256, bytes and
        lines straight after the write, reported.
    2e. The item-0 check, as the form's, with: the last line "The decision log in this
        file ends at R825." once; the "ends at" lines counted before (R1) and after,
        every earlier one kept; the plan's line count the prior count plus N, N read
        from the numstat; r825.txt equal to 2a's; the three rules files of R3 byte-equal
        to HEAD's (R777(b), R825(e)); six-cell INT- rows 215 before and after; the
        staged set exactly the plan, r825.txt, lane_line_12ai.txt and
        brief_12aj_OPEN.md.
    2f. As the form's. The ledger message is built from the diff (R801(f)).

  STEP 3 — The session-open check, the explicit steps and the gate.
    3a. ~/scratch/12aj-open/expected_12aj.json in expected_12ai.json's shape (E1,
        0h(vi)), every section the tool names: repos at the 2f commits and their
        parents; the post-item-0 plan values and the three rules files at R3's hashes;
        every reference file 12ai_close/'s, as the 12aj block names them; nightly and
        health at the latest date read in step 1 (D3), the health section's "models"
        the number of models that date's log tested, as read at 1a; the Ollama service
        start as read at 0h; cc_sessions with the id of V3 and its window as read at
        0f. Every value from the 12aj block, a 12ai_close/ file or this run's own
        measurements. Quote the file.
    3b. As the form's, with D8: a failure of the health section alone is reported with
        its output, named as the light-day tools item or as a sweep-day finding for the
        tools items, and the run continues on step 1a's read. Any other failed section
        stops as the form's.
    3c. Recount (R7), in a scratch clone made by this step under ~/scratch/12aj-open/,
        with the ten arguments its docstring names, the tenth the plan at the 2f
        commit. EXPECTED: rows 289, 94 / 59 / 136, 0; 39 with 35 done; expected_holds
        true; E2. Rows other than 289, or dated lines other than 39 / 35, = STOP before
        the gate.
    3d. R510 at this open: the six-cell INT- rows at dccc546 and at the 2f commit by
        R5's method. EXPECTED 215 and 215, in 0, out 0, no class or package move;
        written to ~/scratch/12aj-open/r510_12aj_open.txt. Another value = STOP before
        the gate.
    3e. The gate, as the 12aj block's gate bullets state it, by
        docs/session-reports/tools/launch_gate.sh from the repository root: evidence_dir
        ~/scratch/12aj-open/gate_12aj_open/, name 12aj_open (its home is the transient
        user unit gate-12aj_open, no memory cap; never a CC background task), ref_ids
        12ai_close/collected_ids_12ai_close.txt, ref_durations
        12ai_close/durations_12ai_u30b.txt, no selection. The model is the 12ai open's
        step 3e and its record, 12ai_open/gate_12ai_open/ with gate_launcher_stdout.txt
        and hold_snapshot.txt beside it. One wait on gate_rc.txt, its pid reported.
        Report at launch what the launcher prints. EXPECTED: the 12aj block's counts
        line for line (3,607 passed / 1 xfailed / 17 deselected, collected 3,625,
        selected 3,608); exit 0; ids 0 / 0; the unit's start line in the user journal;
        no model load in the gate's journal window; wall time inside 881–1076 s —
        outside it, reported with the concurrent jobs named, not adapted to and not
        re-run (R825(b)); the nineteen files by name against gate_12ai_open/'s;
        durations against the reference, any phase slower by more than 10 s named. A
        red, a model load in the window, a unit that fails or is killed, or no
        gate_rc.txt at the wait's bound = STOP before 3f, the unit's status and journal
        reported.
    3f. As the form's, with the 12aj block's collection figure (3,625), every other
        count of the form's 3f taken from the 12ai open's record and not from an older
        brief, and R8's diffs against 12ai_close/'s three diff files, compared by
        content.

  STEP 4 — The evidence commit (R731(c); model 35fc9ec694f07c05db787f3a504611f7fdbb8b67).
    cp the gate's files into 12aj_open/gate_12aj_open/, the launcher's stdout and the
    hold snapshot beside it under the model's names, step 1's files into 12ai_close/
    under their step-1 names, and between_sessions_read.txt into 12aj_open/; every
    sha256 equal after; scratch untouched. The check, the chain, the pushes, the
    fingerprint compares and the ledger row as the form's step 4.

ACCEPTANCE GATES (each step's EXPECTED values are its gate; in summary):
  G1. Step 0 matched or stopped; every full hash proved; I1, I2, I5 and I6 verified; 0h's
      file written; no git command run inside a kept clone.
  G2. Step 1: each nightly a line-for-line match on a tree equal to 7fad72a's, ids 0 / 0,
      its window empty; each health job reported as readings with its form named; the
      files written per date.
  G3. Step 2: one item-0 commit that passed 2e in one chain, one block, the rules files
      untouched; pushed, level, fingerprint IDENTICAL; the ledger row with trailers.
  G4. Step 3: the open check clean, or clean but for 3b's one exception; the recount at
      289 with 39 / 35; R510 215 / 215; the gate at the 12aj block's counts, inside the
      band or reported outside it, the hold reported as taken or not needed; 3f matched.
  G5. Step 4: every copy sha256-equal to its source; pushed, level, fingerprint
      IDENTICAL; its ledger row.

OUT OF SCOPE: U31's Phase A and every later unit. Any ruling on a finding of this open:
  it follows this open's report. Deleting, moving or cleaning any clone, record, log or
  database under ~/scratch, the kept clones, the test databases and the pytest temp root
  included. The tools items, the health-section ones among them. The stale restart text
  in docs/architecture/ and analysis/ (the HK-ARCH line's). Any file of dgx-infra, and
  any message to another lane. Any edit to the expected nightly file, either primer, the
  retention ledger, a recount script, or a committed read-out or brief. Killing or
  cleaning any process, unit, tmux session or worktree. Any model call or Ollama
  restart. /wrap.

CONSTRAINTS (task-only): the form's, with ~/scratch/12aj-open/ as the one place a helper
  may write to or delete from (R658(c)). D8 is this open's only and expires with it.

FINAL REPORT FORMAT: the form's sections (0)–(8), with: in (0) the claude-config chain
  since 42fabff, each commit with its hash, lane and class; the CC session's first and
  last entry and archive state, and the result of the search for a later session;
  primer.md line 45 with its marks; ~/scratch's top-level listing; and 0h in full — the
  health job's schedule and script hash beside R12's, the snapshot timer,
  ollama.service's values, the clone counts of (iv), the FETCH_HEAD and reflog lines of
  (v) quoted, the config reading of (vi), and (vii)'s lines quoted whole with the commit
  that last touched the file; in (1) one row per date, each naming the health form that
  ran, the sweep's model count and its new NV_ERR_NO_MEMORY lines; in (2) Block 223's
  metadata as composed with the model quoted, the rendered header, and the sha256, bytes
  and lines of r825.txt and the brief file; in (3) the open check's output with the
  health section's result named under D8; in (4) what the launcher printed, the unit's
  journal start line, and the hold — the snapshot before it, the job and pid waited on,
  the wait's start and end, or "not needed" with the snapshot. Then one list: every path
  under ~/scratch that a step of this open read, with the step, and for each whether it
  lies inside a clone of evidence-engine (E8). Then findings, numbered, each with its
  quoted line, including anything in R1–R14, V1–V8 or the FORM section the box
  contradicts. Then STOP.

CONTRADICTION HANDLING: the form's, with these read for its values: a step-0 mismatch, a
  hash cat-file -e does not prove, I1, I2, I5 or I6 false, or a nightly mismatch or I3
  false under step 1 = STOP before any commit. A 2a mismatch, a render the tool refuses
  or that does not read (2b), a changed byte in a rules file, or a check that fails or
  crashes (R763(c)) = STOP before the item-0 commit. The clock at the launch bound
  before 3e launches (a hold included) = STOP before the gate; item 0 stands. An
  expectation mismatch (E1–E8), every health reading, 0h(vii)'s reading and every
  difference from V1–V8 that no blocking item names are reported and the run continues.
  A spec premise that does not survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not start U31's Phase A. Await the next brief or ruling in
  this session.
  STOP-12AJ-OPEN
