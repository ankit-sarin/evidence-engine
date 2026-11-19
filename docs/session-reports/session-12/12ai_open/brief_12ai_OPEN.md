CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R816, PI ruling 2026-11-19 UTC; not on disk)
================================================================================

R816 is transcribed at item 0 of the brief below, by
docs/session-reports/tools/transcribe_ruling.py, as the next block of the decision log
(Block 214). The lines between the two marker lines, exclusive of the markers, are the
byte content of r816.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's
independent hash:
  r816.txt: sha256 15c8f6f252c2a12198e4e819bd86e365663326862f30c9c5d5860fe4879d2f9e,
            8523 bytes, 105 lines, one trailing newline.

===== BEGIN r816.txt =====
RULING [evidence-engine 12ah-R816] — wrap ruling: closeout accepted; the band set 897–1095 s;
  one stop this session, the architect's, and no pause; readings on the closeout's findings
  and choices; no standing rule; lane line approved on one condition and applied; close now
STATUS: PI ruling, 2026-11-19 UTC, session 12ah, on architect recommendation, on the 12ah-CLOSE
  closeout report (item 0 9fed14881a114107f65090f17aa77a432dc8dc80; closeout
  b13b5eb23c86c5da9df566d9e59a9764c7831335). Transcribe as item 0 of 12ai-OPEN by
  transcribe_ruling.py, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the closeout report, quoted; its table's wrapped cells joined): item 0
    "9fed14881a114107f65090f17aa77a432dc8dc80", parent "6dc36b3", ledger
    "43473fc32768e6e62703d829a04f6b37ed3a8644 (parent b0bb0ba)"; closeout
    "b13b5eb23c86c5da9df566d9e59a9764c7831335", parent "9fed148", ledger
    "5a261b8a895582ba8cbfc9e94af3f796fbaf1602 (parent 43473fc)"; "r815.txt: 9dd24501…21c0,
    4,562 B, 60 lines, equal to the architect's hash."; "Plan: +71 / −0"; "brief_12ah_CLOSE.md,
    f860358d1b8f80876430d6cf57e946b32faea91cc0af57059fb763a70d5aaa6a, 28,059 B, 377 lines"
    (the architect's copy: the same sha256, bytes and lines); "Measurements (12ah_close/, 17
    files)"; "Retention: 23 MATCH / 0 MISMATCH; probe scratch 10/10 and 53/53"; "Jobs: none
    ran after the 2026-11-18 pair."; "Recount: 287 (94 / 57 / 136, 0 unclassed), dated lines
    38 with 33 done, expected_holds true"; "R510: 213 at 35a914b and at 9fed148, in 0, out
    0"; "mean 995.81 s; x0.9 896.231; x1.1 1095.394"; "rounded inward: 897–1095 s";
    "(+170 / −0)"; "Plan now:
    7722074c3760b4c7025cba659773d55a05886c9b00d50d7785bedd2a29b6b3ad, 3,366,202 B, 20,007
    lines."; "55 NV_ERR_NO_MEMORY lines this boot"; the lane line
    "5319f267abcf984bc2f44394052f0a327cd095c5fb136ac9eb8851646d29fa0b, 7,979 B, 10 lines";
    "Its five "−" lines equal primer.md 41–45."
  READ (CC's): the five choices of section (3) and findings 1–4; "This session's start is
    stated as 2026-11-19 04:39 UTC".
  READ (the architect's clone at b13b5eb): the two full hashes in STATUS, each with its
    parent; 12ah_CLOSE/brief_12ah_CLOSE.md equal to the architect's copy by cmp; the plan's
    sha256, bytes and lines as the report gives them, last line "The decision log in this
    file ends at R815."; "Expected values at session-12ai open" at plan :5846–:5885, read
    whole; the closure's range as the report gives it, :5717–:5844; 12ah_close/'s seventeen
    files listed; band_computation.txt "mean 995.81 s; x0.9 896.231; x1.1 1095.394" and
    "rounded inward: 897–1095 s".
  INFERRED — BLOCKING (verified before R816(f) is carried out; false = STOP, nothing applied):
    the lane-line file's fifth "+" line, read from the file, is the line the report quotes,
    word for word. The report's quotation reads whole; the file decides.

R816(a) — The 12ah-CLOSE closeout is accepted as committed: item 0 9fed148 (Block 213; the
  brief file equal to the architect's copy; the rules files untouched), the closeout b13b5eb
  (the closure and the 12ai expected-values block, +170 / −0 at the matched position;
  12ah_close/'s seventeen files; the recount; the band computation), ledgers 43473fc and
  5a261b8. I1 and I2 held. E1–E7 held. Five committed brief and resume files this session,
  five equal to the architect's copies; the interim session's four are recorded by their own
  lists.
R816(b) — The band for 12ai is 897–1095 s: ±10% of 995.81 s, the mean of the last four standard
  gates on this host state (979.95, 1042.65, 982.43, 978.22), rounded inward. The second is
  the confounded member carried under R677(f) with its confounder named (R805(b)); the last
  two were launched as units. The series stands at forty-three. A 12ai gate outside the band
  is reported with its concurrent jobs, not adapted to.
R816(c) — The stops, counted from the rulings: one, the architect's — 12ah-OLRA-REC at its step
  2c (R814(a)). Pauses: none. CC's stops: none. The first paste of 12ah-OPEN stopped on its
  own TIMING, nothing written, and is not counted (R813(a)). The architect's errors this
  session, three, none reaching a commit (R814(c)): tool paths written short; a premise passed
  to the PI unchecked; a gate that contradicted the ruling beside it. The second and third
  are one failure seen twice: a statement about a file that no one had read the file for. The
  session's measured point for the estimate: one CC session (the open at 04:39 UTC on
  2026-11-19 to this wrap) and one interim read-only session before it; no fix unit landed;
  one standing rule; the window's basis measured; the interim read-out committed after a
  scan; one stop, no pause.
R816(d) — Readings, no row. The closeout's five choices are accepted as made, the two lines it
  added to the 12ai block among them (the window rule; a light-check day read by the explicit
  step). Its findings 1 and 2 are CC's own slips, each refused by its own guard before
  anything was written; finding 3 (the interim session's start taken from the 12ah-OPEN
  record) is a reading.
R816(e) — Carried to 12ai and the plan, from the closure's "Carried forward": the restart
  change's design ruling, quoting 12ah_OLRA/12ah_OLRA.md and its addendum, then its build as
  one engine-commit brief and its merge sha reported to the workbench; U30's build with N46,
  one brief, with the notes R805(d) and R807(c) order; then U31–U36; the tools items, the two
  of R813(e) among them; the correction of CLAUDE.md's no-live-write line (R812(c)); the
  cross-lane readings for dgx-infra; the 2026-11-19 and later jobs against
  12af_close/expected_nightly_20261111.md. This ruling states no standing rule: the id list,
  the glosses file and STANDING_RULES.md are untouched at 12ai-OPEN's item 0, asserted by
  content.
R816(f) — The lane line (sha256 5319f267abcf984bc2f44394052f0a327cd095c5fb136ac9eb8851646d29fa0b,
  7,979 bytes, 10 lines, ~/scratch/12ah-close/lane_line_12ah.txt) is approved on the condition
  above. First verify the file's sha256 and quote its fifth "+" line whole from the file, its
  code marks shown. If it is the line the closeout report quotes — its "next:" clause running
  from 12ai-OPEN through the restart change's design ruling and build and U30's build with
  N46 to the freeze, the tag and Run 7 with no words missing — apply it: replace lines 41–45
  of ~/claude-config/primer.md (which must equal its five "−" lines, taken from primer.md
  itself) with its five "+" lines, applied from the file, not from a paste; commit primer.md
  with any uncommitted hook row ("ledger" form, trailers, the row's exact timestamp, foreign
  rows counted from the diff and named by hash); push; level. If the line differs in the
  file, STOP before anything is applied and report the line as it stands; the architect then
  rules its text.
R816(g) — 12ai-OPEN checks claude-config at the commit the wrap leaves (the lane-line commit, or
  a child carrying only hook rows, reported), level and clean but for other lanes' hook rows.
  Its item 0 writes this ruling with transcribe_ruling.py as the next block (Block 214) and
  copies lane_line_12ah.txt into docs/session-reports/session-12/12ah_close/, with r816.txt
  beside it; no standing rule, the three rules files untouched by content. It runs
  session_open_check.py and the steps R747(b) keeps explicit, a light-check day's health read
  among them, against the "Expected values at session-12ai open" block, the recount by
  12af_close/recount_12af_close.py (287; 38 done 33; R510 0 / 0); reads the 2026-11-19 and
  every later job run before it against 12af_close/expected_nightly_20261111.md; the gate by
  the wrapper as a unit, against 897–1095 s, its references
  12ah_close/collected_ids_12ah_close.txt and durations_12ah_open.txt. Its brief is committed
  at its item 0 as brief_12ai_OPEN.md (R784(g)(v)), in 12ah_open/brief_12ah_OPEN.md's form.
  Then the restart change's design ruling.
R816(h) — Session 12ah closes now. After R816(f) has landed, run /wrap; this ruling authorizes
  /wrap's own commits and pushes and nothing else. The project primer and the global primer
  record the session as the lane line states it. Lane status: closed clean; next, 12ai-OPEN.
  Report the lane-line commit's hash and parent, the fifth "+" line as quoted from the file,
  the wrap's verdict, and the archive command to be run after /exit.
===== END r816.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ai-OPEN] — session 12ai open: pre-check (the 12ah close and wrap
  claims against disk; the between-session readings: one interim read-only CC session,
  12ai-SCRI, and its scratch directory; the health job's schedule; the service), the
  scheduled-job reads for every pair run since the 2026-11-18 pair, item 0 (R816 as the
  next block by transcribe_ruling.py; the 12ah lane-line copy; no standing rule; this
  brief committed as pasted), session_open_check.py plus the explicit steps, the gate as
  a unit against 897–1095 s, the evidence in a commit of its own.
PURPOSE: Open refactor session 12ai on origin main and leave a verified base for the
  briefs that follow. Mixed: two docs commits, git, one standard pytest gate. No engine,
  test, script, spec, codebook, rules-file or tools edit. No live write. No model call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  This brief authorizes one transient systemd-run --user unit, gate-12ai_open, launched
  by docs/session-reports/tools/launch_gate.sh at step 3e, and nothing else of systemd.
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 214 appended by the tool, nothing
  else); docs/session-reports/session-12/12ah_close/ (r816.txt and lane_line_12ah.txt at
  item 0; the job-read files at step 4); a new directory docs/session-reports/session-12/
  12ai_open/ (brief_12ai_OPEN.md at item 0; gate_12ai_open/, its two side files and
  between_sessions_read.txt at step 4); ~/scratch/12ai-open/ (new); the claude-config
  ledger (one commit per evidence-engine commit); git push on both repos; one standard
  gate.
  STOP / DO NOT TOUCH (beyond the global list): the form's list, read with 12ah for 12ag
  and 12ai for 12ah (so: every file already in 12ah_close/ and every earlier close or
  open directory; ~/scratch/12ah-*/ read and copied from, never edited); the three rules
  files; CLAUDE.md; ~/scratch/12ai-scri/ (read and hashed in place; nothing written into
  it or under it, no helper, no output file, no bytecode cache; not copied and not
  committed here); every clone of evidence-engine under ~/scratch and every other entry
  there (nothing deleted, moved or cleaned; none entered except where a step names it);
  ~/ebr-scratch, ~/phi-scratch, ~/fd2-4-scratch and ~/fd2-d2-scratch (other lanes';
  not read); the health job, its script, its schedule and every file of dgx-infra (read,
  never edited); ollama.service (read by systemctl show; no restart, no stop, no pull,
  no model call).

PATHS: in this brief a name of the form 12xx_<name>/ is a directory of
  docs/session-reports/session-12/, and a tool is named by its full path under
  docs/session-reports/tools/. ~/scratch names are written whole.

FORM (R784(g)(iv), R816(g)): docs/session-reports/session-12/12ah_open/brief_12ah_OPEN.md
  (sha256 3ed2db46ae90ece0d63f4081670bfa49a02c4994149b65ea25111daded860bd8, 603 lines) is
  this brief's form. It is written on 12ag_open/brief_12ag_OPEN.md (sha256
  6cb11d640e5eacbcfaafaab176b8a44b285f7c77efdf41e7567f51365b6df573, 422 lines), itself
  written on 12af_open/brief_12af_OPEN.md (sha256
  d6f66cff023f65eb1e59944a06c9c51e3cf72490ccb2237818e07307b6ea6d5f, 430 lines), which
  holds the steps' full text. Verify the three hashes and read the three Section 2s whole
  before step 0. Each step below is the step of the same number, 12af's text read through
  12ag's deltas and then 12ah's, with this brief's values in place of theirs; this brief
  states only what differs. Quote the step before running its counterpart. The record of
  the 12ah open (item 0 34ff8a7a20e90868f8622a58cf1e31a81f05a47d, evidence
  c4120330c7c8102d89d6c8298363e2624c7aca24, ~/scratch/12ah-open/) is the model for each
  file this open writes. Where a form and this brief disagree, this brief governs; where
  either disagrees with a tool or the record, the tool and the record govern and the
  difference is a finding (R732(a)). The deltas from 12ah's brief:
  D1. One ruling, R816, and no standing rule. 12ah's Section 1 for two rulings, its D6,
      its step 2c2 and its regeneration model fall away. The three rules files are
      asserted untouched (2e).
  D2. Two earlier CC sessions are read at 0f (V3): 12ah's, and one interim read-only
      session, task 12ai-SCRI, which left files in scratch only (V4). 12ah's I5 (R812's
      blocking item) was verified at that open (R813(b)); here the health job is read
      again and compared with that measurement (I5 below).
  D3. Job pairs: every pair after the 2026-11-18 pair. Each date gets its own files. A
      light-check day's health read is step 1a's explicit read; the open check's health
      section cannot read one (R813(e), a known tools item, not a new finding).
  D4. 0h's four reads are kept, with (iv) pointed at ~/scratch/12ai-scri/, and two are
      added: (v) the interim session's fetch, (vi) how this open's config is supplied.
  D5. The job-read files and r816.txt go to 12ah_close/, as 12ah's went to 12ag_close/.
  D6. I1 as 12ah's D7: "clean" for claude-config means clean but for other lanes'
      uncommitted hook rows (R654(d), R816(g)).

TIMING: valid from issue. At issue the architect's clock read 14:43 UTC on Thursday
  2026-11-19, after that day's window. Valid outside the day's window (R812(a)):
  09:00–10:35 UTC Monday to Saturday and 07:00–10:35 UTC on Sunday, by UTC day. If the
  clock at step 0 is inside a day's window, or within 30 minutes before one opens, STOP
  at once, write nothing and say so. The gate must exit before the next opening of a
  day's window: if step 3e cannot launch by 30 minutes before that opening, the hold
  included, STOP before 3e and report; item 0 stands. (Run on 2026-11-19 after 14:43
  UTC, that opening is 09:00 UTC on Friday 2026-11-20 and the launch bound is 08:30 UTC.)
  If the clock reaches 15 minutes before an opening with a commit pending, hold with the
  work on disk uncommitted and resume after 10:35 UTC, as the form's TIMING. The pairs
  are read from the logs, never from this paragraph. Block 214's transcribed date is the
  item-0 commit's UTC date, read at commit time beside the day boundary (R775(a), (d));
  its made date is the ruling's own, 2026-11-19.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ah-CLOSE closeout report and the wrap, as R816 quotes them in Section 1):
    the closeout is b13b5eb, its parent item 0 9fed148; the plan 7722074c…b3ad, 3,366,202
    B, 20,007 lines; 287 rows (94 / 57 / 136, 0 unclassed), fix units 38, done 33; R510
    in 0, out 0; the band 897–1095 s; the lane line's sha256, bytes and lines (V2).
  READ (the architect's clone of origin at b13b5eb23c86c5da9df566d9e59a9764c7831335,
    2026-11-19 14:43 UTC; to be re-read on the box, not trusted):
    R1. docs/plan/ENGINE_REFACTOR_PLAN.md: sha256
        7722074c3760b4c7025cba659773d55a05886c9b00d50d7785bedd2a29b6b3ad, 3366202 bytes,
        20007 lines; last line "The decision log in this file ends at R815."; last block
        213 (its header at :19938); no "**Block 214.**"; 170 lines begin "The decision
        log in this file ends at R".
    R2. "Expected values at session-12ai open" starts at plan line 5846 and its list of
        first items ends at 5885. It is the expected-values source for this open; read
        it whole at 0c. This brief repeats a value from it only where the value gates a
        stop.
    R3. Rules files: docs/plan/STANDING_RULES.md
        d56dbdaeafb3659527cb77e1e81744eb01dadbe871fd96944104bb9e01b7e3ff;
        docs/plan/standing_rules_ids.txt
        a28da0611e1a4d62cbc8185ff89df22d2df7a2a37340da0eb164dec678e75257;
        docs/plan/standing_rules_glosses.json
        7f7374b25f7efd2c1df97cfbe3fb9b828c7fec5bce4e948d751cec08a4fa72ed (47 entries).
    R4. 12ah_close/ holds 17 files. None is named r816.txt or lane_line_12ah.txt, and
        none has a health_job_, health_journal_, nightly_ or nightly_ids_ name. It holds
        nvrm_this_boot_20261119.txt; the name step 1 writes is
        nvrm_this_boot_<read date>_read.txt, which it does not hold. The job-read names
        of record are 12ag_close/'s sixteen for 2026-11-15 to 2026-11-18 and its
        nvrm_this_boot_20261119_read.txt. 12ah_open/gate_12ah_open/ holds 19 files, with
        gate_launcher_stdout.txt and hold_snapshot.txt beside it. No 12ai_open/ exists.
    R5. Six-cell "| INT-" rows in the plan, by 12ah_close/r510_12ah.txt's method line
        ("lines starting '| INT-' with six ' | '-separated cells"): "35a914b: 213;
        9fed148: 213".
    R6. Block 201's header reads "in its own commit with the lane-line copy and the brief
        file it orders" and names its report "the 12af-CLOSE closeout report (item 0
        `<full hash>`; closeout `<full hash>`)". Block 213's header (R815, one ruling,
        no lane line) reads "in its own commit with the brief file it orders".
    R7. 12af_close/recount_12af_close.py (sha256
        a86247439038b1610b989e1ed08f6744d5322ec98c55858bda7fbc9f68f03a47) gives its usage
        in its docstring. Its committed JSON is
        3657ce2f219c9fc130d5248295e99071855245d87dc2b7ab6d80e9b5c16d15a5.
    R8. On engine tests scripts analysis review_specs: git diff --stat 977c499 HEAD is
        empty; from f4a5586, "96 files changed, 6609 insertions(+), 345 deletions(-)".
    R9. The tools at HEAD, each under docs/session-reports/tools/: launch_gate.sh
        a1d771d88d684504193c1d4d52950988dccebc2b2a06ef13df38e1d1fb2ad2c9; run_gate.sh
        be60c31fff5edc3997b00211e517a93d31f2f0f6b47b3ca44f2e3fb1a24f9327;
        transcribe_ruling.py
        18f69e20f143c48215f80285fa81369d8d67c4c453a69d8e8002e40366457151;
        session_open_check.py
        49acf8aad11dfcf4195bfbc05dae7a0836c17e22c3d132f04580fe685ac8b5e4. None changed
        since the 12ah open.
    R10. 12af_close/expected_nightly_20261111.md is
        4c5927b71ca0533490f230e20806929e5d5d9b672a3673b67f348a9caed80e35;
        12ah_close/collected_ids_12ah_close.txt holds 3,574 lines
        (d63633b8071e6cc6ac003bbf58873e781830216afb2df1a18821221363322b0c);
        12ah_close/durations_12ah_open.txt is
        243c1080b4d6db7db3eb52128a5cef3c2b09b4680e836b139e07809529622798. git ls-files
        lists 38 files named brief_*.md.
    R11. session_open_check.py's docstring: "usage, from the repository root:
        session_open_check.py <expected.json>" and "Every expected value comes from the
        file (R742(b)); a section the file omits is not run and is printed as SKIPPED".
        Its repos section runs "git fetch -q" in each repository. No file named
        expected_12*.json is committed.
    R12. 12ah_close/host_state_close.txt:74–75: the health job's crontab line "0 7 * * *
        /bin/bash /home/ankitsarin/scripts/ollama_health_check.sh >> /home/ankitsarin/
        logs/ollama_health_cron.log 2>&1" and its script's sha256
        42c11f18ba8d3411b79196cc7c39c893ed67331669fc9cc7fa1ae058cd7de50b.
  READINGS FROM OUTSIDE THE CLONE (unified plan v106, R816, and the PI's addendum of
    2026-11-19 on the interim session; INFERRED by source; the architect has not read the
    interim read-out; each is the value an item below is checked against):
    V1. claude-config after the 12ah wrap: 60ae7bf7f9bc2726f30f5b08e06fe088cec9c749,
        parent 5a261b8a895582ba8cbfc9e94af3f796fbaf1602; reported unchanged at the
        interim session's close.
    V2. ~/scratch/12ah-close/lane_line_12ah.txt: sha256
        5319f267abcf984bc2f44394052f0a327cd095c5fb136ac9eb8851646d29fa0b, 7,979 B, 10
        lines. Since the wrap applied it, its five "+" lines equal
        ~/claude-config/primer.md 41–45.
    V3. CC sessions: 65ffbac0-e521-445e-9694-f9e4eb97a2d3 (12ah; first entries
        2026-11-16 04:43–04:44 UTC, the stopped first paste; the work from 2026-11-19
        04:39 UTC; wrapped CLEAN-TO-EXIT, exited, archived) and
        bf3224f8-1d94-4728-9a0a-13db7c9ffb8d (the interim session, task 12ai-SCRI,
        read-only; 2026-11-19 14:13:52 to 14:37:51 UTC; reported wrapped CLEAN-TO-EXIT
        with no commit and no push in either repository, exited and archived). Each
        one's first and last entry and its archive state are read from the record, never
        from this line.
    V4. ~/scratch/12ai-scri/ holds the interim read-out 12ai_SCRI.md (sha256
        7e3bdda1dc59c1c3a3a2d53d70af378d6fa52a6364e660217389b0a9518301ad), a list
        SHA256SUMS reported as covering 96 files, and subdirectories, patches/ and
        untracked/ among them. The list's own sha256 and its line form are not known to
        the architect; they are read at 0h.
    V5. The last jobs read before this open: the 2026-11-18 pair, at 12ah-OPEN. The
        12ah closeout read at 2026-11-19 06:28 UTC, before that day's jobs.
    V6. During the interim session CC ran one git fetch in ~/projects/evidence-engine
        that its brief forbade; reported as FETCH_HEAD only, no ref changed.
    V7. ollama.service: started 2026-11-16 00:06:51 UTC, MainPID 3633749, NRestarts 0,
        OLLAMA_KEEP_ALIVE=30m (the 12ai block's readings).
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. claude-config HEAD is V1's commit, or a descendant acceptable under R663(b) and
        R816(g): level, clean but for other lanes' uncommitted hook rows, no row of this
        lane missing, and primer.md lines 41–45 equal to the lane line's five "+" lines.
    I2. Both CC sessions of V3 are exited with no running process, and nothing of this
        lane survives either.
    I3. Every nightly since the 12ah closeout ran on a HEAD whose engine tree (R8's five
        paths) equals 977c499's (reflog method with the lock line, R779(c)).
    I4. transcribe_ruling.py --append only appends: the plan's "−" count is read from the
        numstat and matched to zero, never assumed (R766(c)).
    I5. The health job still loads models on Sunday by UTC only, from 07:00 UTC, as
        R813(b) measured: read at 0h from its schedule and script. False if models are
        loaded on any other day, or a sweep can start outside 07:00–10:35 UTC. A changed
        script hash or crontab line with I5 still true is a finding, not a stop.
    I6. evidence-engine HEAD is the 12ah closeout commit, clean and level, and no ref of
        the repository moved between the wrap and this open: the interim session
        committed nothing and its fetch changed no ref (V6).
  INFERRED — expectation (report a mismatch, continue):
    E1. ~/scratch/12ah-open/ still holds expected_12ah.json, block209.json and the
        item-0 and step-4 checks, and ~/scratch/12ag-open/ still holds block201.json.
        If one is gone, the committed evidence and the tools' docstrings govern, and
        what was missing is named in the report.
    E2. The recount's JSON is byte-equal to the committed one.
    E3. Each health job on a Monday-to-Saturday date ran as a light check: three GET
        requests, no load, "Mode: light" in its log. A Sunday date ran the full sweep
        with no FAIL. No new NV_ERR_NO_MEMORY line falls outside a sweep's window (55
        this boot before them, the last 2026-11-15 07:03:19 UTC).
    E4. The gate lands inside the band beside surgical-cv's uvicorn only, with no hold.
    E5. The host's four band values equal 12ah_close/host_state_close.txt's. If one
        differs the band series restarts: report it before the gate and do not match.
    E6. The interim CC session is archived; every line of ~/scratch/12ai-scri/SHA256SUMS
        matches its file; the list has 96 lines; 12ai_SCRI.md hashes to V4's value; no
        file under the directory is unlisted except the list itself.
    E7. ollama.service reads as V7 states it, and the five digests equal the 12ai
        block's.
    E8. No file of this open's steps is read from a clone of evidence-engine under
        ~/scratch other than the one this open makes at 3c.

CONTRACT (steps, in order; evidence under ~/scratch/12ai-open/):

  STEP 0 — Pre-check (read-only). The form's 0a–0h, with:
    0a. Every 07:00 health run and 09:00 nightly after the 2026-11-18 pair (V5).
        EXPECTED on 2026-11-19 after 14:43 UTC: the 2026-11-19 pair; a later day's pair
        only past 10:35 UTC that day. A nightly that should exist and cannot be found =
        STOP. A health run that is not where the earlier ones were is a finding.
    0b. HEAD b13b5eb23c86c5da9df566d9e59a9764c7831335 and its parent
        9fed14881a114107f65090f17aa77a432dc8dc80, each proved by cat-file -e; clean;
        fetch; 0 0; one worktree (I6). git diff --stat 977c499 HEAD on R8's paths empty.
        Before this step's own fetch, 0h(v) is read. A commit on the closeout, or any
        other mismatch = STOP.
    0c. R1–R4, R7 and R9–R12. The directory listings are read and printed, not
        asserted; a name step 1, 2 or 4 will write that already exists = STOP (nothing
        is overwritten).
    0d. claude-config (I1, V1): the form's reads, with git log 60ae7bf..HEAD, every
        commit named by hash and lane and classed: this lane's or another's ledger rows
        only; another lane's committed change under R663(b); anything else. This lane's
        ledger-row count is printed, as a reading.
    0e. The lane line (V2): the file's sha256, bytes and lines, and its five "+" lines
        against ~/claude-config/primer.md 41–45, taken from primer.md itself.
    0f. Leftovers (I2, V3): both sessions, each with its first and last entry and
        archive state from the record. No user unit named gate-* is loaded. A running
        process of either session, a loaded gate-* unit, or a survivor of this lane =
        STOP, reported, not killed. Other lanes' are readings.
    0g. ~/scratch's top-level listing, printed (the 12ai block expects the four 12ah
        directories, those present at the 12ah open and retained/; 12ai-scri/ is the
        one addition this brief expects; any other new entry is a finding);
        ~/scratch/12ah-open/'s and 12ah-close/'s listings (E1); the temp root; the
        project primer's 12ah block.
    0h. The between-session readings (read-only), written with each command and its
        timestamp to ~/scratch/12ai-open/between_sessions_read.txt, in the form of
        12ah_open/between_sessions_read.txt:
        (i) The health job (I5): its schedule as the box holds it, quoted whole, against
            R12's line; the script's path and sha256 against R12's; the lines that
            decide between the light check and the sweep and that read the experiment
            lock, quoted. I5 false, or a schedule or script that cannot be read = STOP
            before step 1, nothing committed.
        (ii) The snapshot timer, as the form's. A reading.
        (iii) ollama.service (E7), as the form's.
        (iv) ~/scratch/12ai-scri/ (V4, E6): its listing with bytes, recursive, every
            subdirectory entered; SHA256SUMS's own sha256, bytes and line count, and
            its line form quoted from its first three lines; then every line against
            the named file, by a helper kept under ~/scratch/12ai-open/helpers/ that
            reads the list and walks subdirectories (not sha256sum -c; nothing written
            under 12ai-scri/), the counts of lines, matches and mismatches printed;
            12ai_SCRI.md's sha256 against V4's; every file under the directory that
            the list does not name. The directory's own mtime before and after this
            step, as a check that nothing was written.
        (v) The interim session's fetch (V6, I6), read before 0b's fetch: in
            ~/projects/evidence-engine, .git/FETCH_HEAD's mtime and content; the
            reflogs of HEAD, main and origin/main with dates, showing no entry between
            the 12ah wrap and this open; origin/main equal to HEAD. An entry in that
            interval, or a ref not at b13b5eb = STOP.
        (vi) How this open's config is supplied (R11): quote session_open_check.py's
            usage line; state whether ~/scratch/12ah-open/expected_12ah.json exists,
            with its sha256, and whether any committed file holds an expected-values
            config. This open builds its own file at 3a and passes it by path; the
            12ah file is read as a shape model only, and no step of this open takes a
            value from it.

  STEP 1 — The scheduled-job reads (read-only). The form's 1a–1b for each date 0a found,
    earliest first, written under ~/scratch/12ai-open/ in R4's names; one
    nvrm_this_boot_<read date>_read.txt for the whole open. Models: 12ag_close/'s
    2026-11-18 files for a light-check day and its 2026-11-15 files for a sweep day;
    quote each header first.
    1a. Health (readings): as the form's, the NV_ERR_NO_MEMORY count against 55, and
        model loads outside the health windows counted from the 12ah session's last
        entry as read at 0f. The file's header says which form ran (E3).
    1b. Nightly (governed): line for line against 12af_close/expected_nightly_20261111.md
        (R10), that date in the log and durations names; ids as a set against
        12ah_close/collected_ids_12ah_close.txt, 0 / 0; the HEAD each ran on, named, and
        its engine tree compared with 977c499's (I3).
    A nightly mismatch on any date, or I3 false for any date, = STOP before step 2, both
    values and the date reported. Do not adapt.

  STEP 2 — Item 0 (one evidence-engine commit, one ledger commit; the check written for
    this brief, R794(i)). The form's 2a–2f without 2c2. Two models, each quoted before
    its counterpart is composed: for a single wrap ruling with its lane-line copy, the
    12ag open's item 0, 16cb307164c9203c695c3a4f842cf6d294c771ad (Block 201); for the
    files, the check and the ledger message, the 12ah open's item 0, 34ff8a7.
    2a. 12ah_close/r816.txt from Section 1, one write, hashed straight after (R738(b))
        against Section 1's hash. A mismatch = STOP.
    2b. ~/scratch/12ai-open/block214.json in block201.json's key set (E1): transcribed =
        the commit's UTC date; item "item 0 of the 12ai-OPEN brief"; made_by PI;
        made_date 2026-11-19; session 12ah; report as its STATUS names it, "the
        12ah-CLOSE closeout report (item 0 <full hash>; closeout <full hash>)", the
        hashes in backticks in Block 201's form (R6); report_short the same with short
        hashes; sentence model, followed "12ai-OPEN brief"; prev_ends R815; "beside" in
        Block 201's words (R6), read from block201.json or from Block 201's header, not
        from this line. Render the block to stdout, compare its header's element order
        with Block 201's and Block 209's, then --append.
    2c. cp ~/scratch/12ah-close/lane_line_12ah.txt into 12ah_close/; sha256 equal to
        0e's.
    2d. 12ai_open/brief_12ai_OPEN.md from this whole pasted block, "CLAUDE CODE" through
        the STOP-12AI-OPEN line, one trailing newline, written once; sha256, bytes and
        lines straight after the write, reported.
    2e. The item-0 check, as the form's, with: the last line "The decision log in this
        file ends at R816." once; the "ends at" lines counted before (R1) and after,
        every earlier one kept; the plan's line count the prior count plus N, N read
        from the numstat; r816.txt equal to 2a's; the three rules files of R3 byte-equal
        to HEAD's (R777(b), R816(e)); six-cell INT- rows 213 before and after; the
        staged set exactly the plan, r816.txt, lane_line_12ah.txt and
        brief_12ai_OPEN.md.
    2f. As the form's. The ledger message is built from the diff (R801(f)).

  STEP 3 — The session-open check, the explicit steps and the gate.
    3a. ~/scratch/12ai-open/expected_12ai.json in expected_12ah.json's shape (E1,
        0h(vi)), every section the tool names: repos at the 2f commits and their
        parents; the post-item-0 plan values and the three rules files at R3's hashes;
        every reference file 12ah_close/'s, as the 12ai block names them; nightly and
        health at the latest date read in step 1 (D3); the Ollama service start as
        read at 0h; cc_sessions with both ids of V3 and one window from the earlier
        session's first entry of 2026-11-19 to the later one's last, as read at 0f.
        Every value from the 12ai block, a 12ah_close/ file or this run's own
        measurements. Quote the file.
    3b. As the form's, its one exception kept: on a light-check day the health
        section's failure is the known tools item (R813(e)), reported with its output,
        and the run continues on step 1a's read. Any other failed section stops as the
        form's.
    3c. Recount (R7), in a scratch clone made by this step under ~/scratch/12ai-open/,
        with the arguments its docstring names, the last the plan at the 2f commit.
        EXPECTED: rows 287, 94 / 57 / 136, 0; 38 with 33 done; expected_holds true; E2.
        Rows other than 287, or dated lines other than 38 / 33, = STOP before the gate.
    3d. R510 at this open: the six-cell INT- rows at b13b5eb and at the 2f commit by
        R5's method. EXPECTED 213 and 213, in 0, out 0, no class or package move;
        written to ~/scratch/12ai-open/r510_12ai_open.txt. Another value = STOP before
        the gate.
    3e. The gate, as the 12ai block's gate bullets state it, by
        docs/session-reports/tools/launch_gate.sh from the repository root: evidence_dir
        ~/scratch/12ai-open/gate_12ai_open/, name 12ai_open (its home is the transient
        user unit gate-12ai_open, no memory cap; never a CC background task), ref_ids
        12ah_close/collected_ids_12ah_close.txt, ref_durations
        12ah_close/durations_12ah_open.txt, no selection. The model is the 12ah open's
        step 3e and its record, 12ah_open/gate_12ah_open/ with gate_launcher_stdout.txt
        and hold_snapshot.txt beside it. One wait on gate_rc.txt, its pid reported.
        Report at launch what the launcher prints. EXPECTED: the 12ai block's counts
        line for line (3,573 passed / 1 xfailed / 17 deselected, collected 3,591,
        selected 3,574); exit 0; ids 0 / 0; the unit's start line in the user journal;
        no model load in the gate's journal window; wall time inside 897–1095 s —
        outside it, reported with the concurrent jobs named, not adapted to and not
        re-run (R816(b)); the nineteen files by name against gate_12ah_open/'s;
        durations against the reference, any phase slower by more than 10 s named. A
        red, a model load in the window, a unit that fails or is killed, or no
        gate_rc.txt at the wait's bound = STOP before 3f, the unit's status and journal
        reported.
    3f. As the form's, with the 12ai block's collection figure and R8's diffs against
        12ah_close/'s three diff files, compared by content.

  STEP 4 — The evidence commit (R731(c); model c4120330c7c8102d89d6c8298363e2624c7aca24).
    cp the gate's files into 12ai_open/gate_12ai_open/, the launcher's stdout and the
    hold snapshot beside it under the model's names, step 1's files into 12ah_close/
    under their step-1 names, and between_sessions_read.txt into 12ai_open/; every
    sha256 equal after; scratch untouched. The check, the chain, the pushes, the
    fingerprint compares and the ledger row as the form's step 4.

ACCEPTANCE GATES (each step's EXPECTED values are its gate; in summary):
  G1. Step 0 matched or stopped; every full hash proved; I1, I2, I5 and I6 verified; 0h's
      file written; ~/scratch/12ai-scri/'s mtime unchanged by this open.
  G2. Step 1: each nightly a line-for-line match on a tree equal to 977c499's, ids 0 / 0,
      its window empty; each health job reported as readings with its form named; the
      files written per date.
  G3. Step 2: one item-0 commit that passed 2e in one chain, one block, the rules files
      untouched; pushed, level, fingerprint IDENTICAL; the ledger row with trailers.
  G4. Step 3: the open check clean, or clean but for 3b's one exception; the recount at
      287 with 38 / 33; R510 213 / 213; the gate at the 12ai block's counts, inside the
      band or reported outside it, the hold reported as taken or not needed; 3f matched.
  G5. Step 4: every copy sha256-equal to its source; pushed, level, fingerprint
      IDENTICAL; its ledger row.

OUT OF SCOPE: Copying, committing or scanning anything from ~/scratch/12ai-scri/ (the
  next brief). Deleting, moving or cleaning any clone, record, log or database under
  ~/scratch, the test databases and the pytest temp root included (a later brief of its
  own, on a removal list the PI approves in words). Any ruling on the interim session:
  it follows this open's report. The Ollama restart change: its design ruling, its
  build, and any edit to CLAUDE.md, review_spec.py or docs/architecture/. U30's build
  and every later unit. The tools items. Any file of dgx-infra. Any edit to the
  expected nightly file, either primer, the retention ledger, a recount script, or a
  committed read-out or brief. Killing or cleaning any process, unit, tmux session or
  worktree. Any model call or Ollama restart. /wrap.

CONSTRAINTS (task-only): the form's, with ~/scratch/12ai-open/ as the one place a helper
  may write to or delete from (R658(c)).

FINAL REPORT FORMAT: the form's sections (0)–(8), with: in (0) the claude-config chain
  since 60ae7bf, each commit with its hash, lane and class; the two CC sessions' first
  and last entries and archive states; ~/scratch's top-level listing; and 0h in full —
  the health job's schedule and script hash beside R12's, the snapshot timer,
  ollama.service's values, for ~/scratch/12ai-scri/ the recursive listing's counts, the
  list's hash, line form and line-by-line counts, 12ai_SCRI.md's hash, unlisted files
  and the mtime before and after, the fetch reading with FETCH_HEAD and the reflog
  lines quoted, and the config reading of (vi); in (1) one row per date, each naming the
  health form that ran; in (2) Block 214's metadata as composed with the models quoted,
  and the sha256, bytes and lines of r816.txt and the brief file; in (4) what the
  launcher printed, the unit's journal start line, and the hold — the snapshot before
  it, the job and pid waited on, the wait's start and end, or "not needed" with the
  snapshot. Then one list: every path under ~/scratch that a step of this open read,
  with the step, and for each whether it lies inside a clone of evidence-engine (E8).
  Then findings, numbered, each with its quoted line, including anything in R1–R12,
  V1–V7 or the FORM section the box contradicts. Then STOP.

CONTRADICTION HANDLING: the form's, with these read for its values: a step-0 mismatch, a
  hash cat-file -e does not prove, I1, I2, I5 or I6 false, or a nightly mismatch or I3
  false under step 1 = STOP before any commit. A changed byte in a rules file, or a
  check that fails or crashes (R763(c)) = STOP before the item-0 commit. The clock at
  the launch bound before 3e launches (a hold included) = STOP before the gate; item 0
  stands. An expectation mismatch (E1–E8), every health reading and every difference
  from V1–V7 that no blocking item names are reported and the run continues. A spec
  premise that does not survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not start the next brief (the ruling on the interim
  session and the commit of its read-out), the restart change or U30. Await the next
  brief or ruling in this session.
  STOP-12AI-OPEN
