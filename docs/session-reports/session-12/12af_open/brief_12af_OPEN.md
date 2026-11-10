CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R796, PI ruling 2026-11-09 UTC; not on disk)
================================================================================

R796 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r796.txt: sha256 7805916b462f9efb073e983ed397a62232d2fc8db90d96bc913c87e435d9b2f1,
            10200 bytes, 111 lines, one trailing newline.

===== BEGIN r796.txt =====
RULING [evidence-engine 12ae-R796] — wrap ruling: closeout accepted; the band set 876–1069 s; one
  stop and one pause this session, both counted; readings on the closeout's eleven findings; no
  standing rule; lane line approved and applied; close now
STATUS: PI ruling, 2026-11-09 UTC, session 12ae, on architect recommendation, on the 12ae-CLOSE
  closeout block (840db24454f08f2486d488ba3e7501fdb73d2dfb) and final report. Transcribe as item 0
  of 12af-OPEN by transcribe_ruling.py, not now.

ASSUMPTIONS IN THIS RULING (quoted from the closeout block and report):
  MEASURED: "item 0 is df3e721 and the closeout is 840db24"; "12ae_CLOSE/brief_12ae_CLOSE.md is
    05170e3e1d61c4af62bf33f996aaff904dfbe985e5325a065520ad32009d5367, 32,218 B, 395 lines" (the
    architect's copy: equal, cmp byte for byte — the twelfth match of twelve); "plan sha256
    ae6159342c216594ae88a6ec7af71f7be05d0c803fccd60638aaac27fd374ba7 3036042 bytes 17693 lines";
    "rows 286 (94/56/136/0); P1 62 (25/25/12); P6 12; P7 121"; "fix_units 35; literal 37/19; dated
    lines 38/31; R510 in 2 (INT-ae12-1, INT-ae12-2), out 0"; "the chain reproduced 12ad's JSON
    3c733cd1…ce21"; "mean 972.5374999999999 s; x0.9 875.2837499999999; x1.1 1069.79125 / rounded
    inward: 876–1069 s"; "the series has thirty-seven members (37): True"; "diff_engine_since_6cb4ab6.txt:
    18 files, 854 insertions, 37 deletions"; "diff_since_24ce26b.txt: empty"; "23 MATCH / 0
    MISMATCH"; "fingerprint_close.txt: IDENTICAL"; "dbchecks_close.txt … cmp equal to 12ad's;
    pin_tuples_close.json cmp equal to 12ad's and to pin_tuples_u28b.json"; the 2026-11-09 health
    job "20/20 loads OK, 0 FAIL … NV_ERR_NO_MEMORY in its window: 0"; the 2026-11-09 nightly "ran
    on 4a4c850 … Its engine tree equals 1f6bc16's, so it was compared with U24's gate of record …
    3495 passed, 17 deselected, 1 xfailed, 2 warnings in 922.59s … Ids: 3,496, 0/0"; "NV_ERR_NO_MEMORY:
    31 this boot, the last at 2026-11-08T07:03:51"; "pytest-586, -587, -588 and pytest-current;
    412,381,306 B"; the lane line "sha256 4ef90bb2882523f16ed51bbe5b56c1fc2f2148fcebef5912bb9096d533df21f0,
    7,205 B, 10 lines … Its five "->" lines are 12ad's five "+>" lines with the sign changed. Those
    equal primer.md lines 41–45 today"; "Ledger: fff9296 … carries my row (23:07:50Z) and the two
    pending foreign rows (surgical-cv-fw1h-b 9f5ab96, 88f0cc2)"; "Ledger: 2aa4eac … row 23:17:30Z.
    No foreign rows"; "RESULT: PASS" twice.
  READ (CC's): findings 1–11; "the closeout's "full hash is in the closeout block". A commit cannot
    contain its own hash"; "du counts each inode once across its arguments"; "No expected-nightly
    file covered the 2026-11-09 nightly's tree"; "The carry-forward error covers five lines … plan
    :920–:928"; "2930dff carries both 94ab7a8 and 1f6bc16".
  INFERRED: none.

R796(a) — The 12ae-CLOSE closeout block is accepted as printed: item 0 df3e721 (Block 193 by
  transcribe_ruling.py; the B9 and C28 landing notes with C28's housekeeping note; the dated units
  line with the measured count 286; the brief file byte-equal to the architect's copy; the three
  rules files untouched by content), the closeout 840db24 (the plan's closure and 12af blocks, +162
  / −0, inserted at the matched anchor; the 24 counterpart files; the 2026-11-09 jobs read; the
  recount and its chain; the band computation; the expected nightly file), ledgers fff9296 and
  2aa4eac with trailers. Every expectation held (E1–E5; I1; I2 read). Twelve committed briefs this
  session, twelve byte-equal to the architect's copies: R784(g)(v)'s check has not yet found a
  paste difference.
R796(b) — The band for 12af is 876–1069 s: ±10% of 972.5375 s, the mean of the last four standard
  gates on this host state (921.98, 945.63, 1059.11, 963.43), rounded inward as R683(c) rounded,
  each described as band_computation.txt describes it, the third a confounded member carried under
  R677(f) with its confounder named (the surgical-cv full suite beside an ffmpeg encode at launch,
  R793(c)). The series stands at the thirty-seven of R795(b); U24's red run (931.16 s) is not a
  member. A 12af gate outside the band is reported with its concurrent jobs, not adapted to. The
  band is wider and higher than 12ae's because of the confounded member; the 12af closeout's recut
  from four unconfounded gates is expected to bring it back, and no rule changes for that.
R796(c) — The stops, counted by the PI from the rulings: one stop, the architect's — 12ae-U24B
  stopped before its first edit on the inventory question (R786(a); R786(b) allowed the
  regeneration and the build resumed as 12ae-U24B-R1); one pause, the clock's — 12ae-U25B's item 0
  reached the R86 window and resumed in the same brief (R789(a)). CC's stops: none. The architect's
  other errors this session (the open brief's length, R784(g); the expected block's health-section
  premise, R784(d); the inventory counters, R787; the ten yaml sites and the R124/R131 citation,
  R788(e); the fourteen except lines, R790; the "no other line changes" clause, R791(c); the Wave 2
  notes' rows, R792(h); C49's CLOSED form, R794(i); the inventory stamp expectation, R795(c); the
  branch's placement, R795(c); the five dated lines' row count, R795(g)) were each caught by a
  check reading its arguments or by CC reading the record over the brief (R732(a)), and none
  stopped a brief or reached a commit wrong. The session's measured point for the estimate: one CC
  session (the open at 03:xx UTC on 2026-11-09 to the wrap at 23:xx), five fix units from Phase A
  to gate, three rows closed, two opened, one unit entered, one stop, one pause.
R796(d) — Readings, no row: finding 1 (two foreign hook rows pending in the shared ledger at step 0,
  carried by fff9296 and named — the third shared-ledger reading, R793(e), R795(c); no rule);
  finding 2 (du's inode de-duplication across arguments made the combined line read 0; the root's
  own du -sb is the figure and the file says why — a reading on the measurement form, carried to
  12af-CLOSE's temp-root line); finding 3 (the 2026-11-09 nightly ran on 4a4c850, U24's engine tree,
  between two engine commits of the same session, so no expected-nightly file covered it; CC read
  it against U24's gate of record and it matched line for line — the right reading under R749(b);
  a session whose engine tree changes mid-day leaves that day's nightly to the closeout's reflog
  method, as here); finding 4 (the five dated lines at plan :920–:928 stand as written, named by
  the closure and the new dated line); finding 5 (band_computation.txt's raw floats stand as
  committed; the figure and the closure's values are exact; no reformat); finding 6 (the closeout
  check failed its dry run on two unprefixed file names and passed after the fix — the check did
  its job); finding 7 (the expected block's HEAD bullet, inherited from 12ad's form, says the
  closeout's full hash is "in the closeout block": it is in the lane line and the report; 12af-CLOSE
  writes that bullet as "its full hash is in the lane line and the closeout report", the form
  corrected forward, the 12ae block not rewritten); finding 8 (the tags read lists five models
  where 12ad's listed three — the two added since are readings, the five digests named in the
  block, R767(c)); finding 9 (2930dff carried two of this lane's rows, 94ab7a8's and 1f6bc16's; the
  commit list says so); finding 10 (E1–E5 held); finding 11 ((a)).
R796(e) — Carried to 12af and the plan, from R795(i) and this closeout: the six tools items
  (R752(c), R765(d), R766(d), R784(d), R791(c) finding 4, R793(e) finding 3) for one tools change;
  U40 next after 12af-OPEN, two-phase, with 12ae_U28A.md §9's questions (the event writer's form
  for a write with a run_id and no activation; the grid's consumers — concordance, the exports, the
  judge loader, the audit; is_assigned for a model-kind arm; whether any reader filters cloud
  arms); the housekeeping at U40's build or P7 (anthropic_extractor.py:108–113); INT-ae12-1 and
  INT-ae12-2 (P7); the stage-key question at 12f_C56-A.md:717 (not U40's); the shared-ledger
  readings (three; a rule is not proposed); the 2026-11-10 and 11-11 jobs against
  12ae_close/expected_nightly_20261110.md. This ruling states no standing rule: the id list, the
  glosses file and STANDING_RULES.md are untouched at 12af-OPEN's item 0, asserted by content.
R796(f) — The lane line (sha256 4ef90bb2882523f16ed51bbe5b56c1fc2f2148fcebef5912bb9096d533df21f0,
  7,205 bytes, 10 lines, ~/scratch/12ae-close/lane_line_12ae.txt) is approved as pasted and applied
  now: verify the file's sha256, replace lines 41–45 of ~/claude-config/primer.md (which must equal
  its five "−" lines, taken from primer.md itself) with its five "+" lines, applied from the file,
  not from a paste; commit primer.md with any uncommitted hook row ("ledger" form, trailers, the
  row's exact timestamp, foreign rows named by hash); push; level.
R796(g) — 12af-OPEN checks claude-config at the commit the wrap leaves (the lane-line commit, or a
  child carrying only hook rows, reported), level and clean. Its item 0 writes this ruling with
  transcribe_ruling.py as the next block (Block 194) and copies lane_line_12ae.txt into
  docs/session-reports/session-12/12ae_close/; no standing rule, the three rules files untouched by
  content. It runs session_open_check.py and the steps R747(b) keeps explicit, against the
  "Expected values at session-12af open" block, the recount by 12ae_close/recount_12ae_close.py
  (286; 38 done 31; R510 2 / 0); reads the 2026-11-10 and, if run, 2026-11-11 jobs against
  12ae_close/expected_nightly_20261110.md (R795(e)); the gate by the wrapper, against 876–1069 s,
  its reference 12ae_close/collected_ids_12ae_close.txt and durations_12ae_u28b.txt. Its brief is
  committed at its item 0 as brief_12af_OPEN.md (R784(g)(v)). Then U40's Phase A.
R796(h) — Session 12ae closes now. After R796(f) has landed, run /wrap. Report the lane-line
  commit's hash and parent, the wrap's verdict, and the archive command as run with its result.
===== END r796.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12af-OPEN] — session 12af open: pre-check (the 12ae close and wrap
  claims against disk; leftovers of CC session 53a21c5b), the scheduled-job reads for every
  pair run since the 12ae closeout, item 0 (R796 as the next block by transcribe_ruling.py;
  the 12ae lane-line copy; this brief committed as pasted; no standing rule),
  session_open_check.py plus the explicit steps, the gate by the wrapper against 876–1069 s,
  the gate evidence and the job-read files in a commit of their own.
PURPOSE: Open refactor session 12af on the 12ae closeout commit and leave a verified base
  for U40's Phase A. Mixed: two docs commits, git, one standard pytest gate. No engine,
  test, script, spec, codebook or tools edit. No live write. No model call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 194 appended by the tool, nothing
  else); docs/session-reports/session-12/12ae_close/ (r796.txt and lane_line_12ae.txt at
  item 0; the job-read files at step 4); a new directory docs/session-reports/session-12/
  12af_open/ (brief_12af_OPEN.md at item 0; gate_12af_open/ at step 4); ~/scratch/12af-open/
  (new); the claude-config ledger (one commit per evidence-engine commit); git push on both
  repos; one standard gate.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/, the codebook, docs/inventory/, docs/architecture/, CLAUDE.md, the tools
  (run, never edited; the six tools items are not worked here); the id list, the glosses
  file and STANDING_RULES.md (not one byte — R796(e)); every file already in 12ae_close/
  and every earlier close or open directory (R102); the recount scripts; review.db (mode=ro
  instruments only); crontab, systemd, earlyoom, the Ollama service; any process or tmux
  session found at 0f (read, never signalled); the pytest temp root (read, never emptied);
  the retained probe scratch (hashed, never opened); ~/scratch/12ae-*/ (read and copied
  from, never edited); both primers (read); the working tree inside 07:00–10:35 UTC (R86).

FORM (R784(g)(iv)): no 12ae-OPEN brief is on disk (R784(g): it is not reconstructed). The
  form of an open is that open's record: its item 0 ca9d348 and evidence commit 95dad8d,
  R784's account of them (12ae_open/r784.txt), and ~/scratch/12ae-open/ where present (E1).
  Quote each model before composing its counterpart. The deltas from that open:
  D1. No standing-rules step: R796 states no rule. The three rules files are asserted
      unchanged by content (R777(b)), not regenerated.
  D2. Item 0 carries this brief as 12af_open/brief_12af_OPEN.md (R784(g)(v); model: step 1d
      of 12ae_CLOSE/brief_12ae_CLOSE.md).
  D3. r796.txt goes to 12ae_close/, where r783.txt went in 12ad_close/. The 12ae closure
      says so: "`r783.txt` (its counterpart is R796, not yet ruled)". The lane-line copy and
      this open's job-read files go there too, as 12ae-OPEN's went to 12ad_close/.
  D4. The open check's health section names the latest run only; an earlier run is step
      1's explicit read (R784(d)).

TIMING: valid at any time outside 07:00–10:35 UTC (R86, R749(b)). At issue the architect's
  clock read 22:44 UTC on 2026-11-10: the 2026-11-10 pair has run; the 2026-11-11 pair is
  this open's only if the clock is past 10:35 UTC on 2026-11-11. The pairs are read from
  the logs, never from this line. The gate must exit before 07:00 UTC: if step 3e cannot
  launch by 06:30 UTC, STOP before it and report. Block 194's transcribed date is the
  item-0 commit's UTC date, read at commit time beside the day boundary (R775(a), (d)); its
  made date is R796's own, 2026-11-09.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ae-CLOSE closeout, as R796 quotes it in Section 1): the closeout is 840db24,
    its parent item 0 df3e721; plan sha256 ae6159342c216594ae88a6ec7af71f7be05d0c803fccd60638aaac27fd374ba7,
    3036042 bytes, 17693 lines; rows 286 (94/56/136/0); the lane line sha256
    4ef90bb2882523f16ed51bbe5b56c1fc2f2148fcebef5912bb9096d533df21f0, 7,205 B, 10 lines;
    NV_ERR_NO_MEMORY 31 this boot, the last 2026-11-08T07:03:51; the band 876–1069 s.
  READ (the architect's clone of origin at 840db24454f08f2486d488ba3e7501fdb73d2dfb,
    2026-11-10; to be re-read on the box, not trusted):
    R1. The plan there hashes to the value above; its last line is "The decision log in this
        file ends at R795."; its last block is 193; it holds no "**Block 194.**".
    R2. "Expected values at session-12af open" starts at plan line 5339 and ends before
        "## Decision log" (5374). It is the expected-values source for this open; read it
        whole at 0c. This brief repeats a value from it only where the value gates a stop.
    R3. Rules files: STANDING_RULES.md 0be5aa940e887699ecfe61fae76eff958b4fcd9d7e874d4a117f0fa376028d5f
        (header "Tier A 87, Tier B 8, 95 of 1524 clauses"); standing_rules_ids.txt
        0657ba83ca1dd54871ed7634a866cc9de3c5286038b46d81f8911fa6dd2bcd62 (last A line
        "A R784(g)"); standing_rules_glosses.json d0dd6dc6f5445b6eec38ee3de8fb1e16d08f016c0e563edc9de7264597f68ac0
        (43 entries).
    R4. 12ae_close/ holds 24 files, none named r796.txt, lane_line_12ae.txt or dated
        20261110 / 20261111. 12ae_open/gate_12ae_open/ holds 19 files.
    R5. Six-cell "| INT-" rows in the plan, by 12ae_close/r510_12ae.txt's method line: 212
        at 840db24 and at df3e721.
    R6. Block 181's header reads "with the lane-line copy and the standing-rules
        regeneration it orders" and names "the 12ad-CLOSE closeout block (`<full hash>`)";
        Block 176's reads "with the lane-line copy it orders"; Block 193's ends its list
        "and the brief file it orders".
    R7. 12ae_close/recount_12ae_close.py's docstring gives its usage: seven `git show
        <commit>:…` plan files, then the plan — eight arguments; it writes its JSON beside
        itself. Its committed JSON is 0b9f41f9dca0cf64bdb19d3135b81a32071106e0b956961a83fe901e88a886be.
    R8. On engine tests scripts analysis review_specs: git diff --stat 24ce26b HEAD is
        empty; from 6cb4ab6, 18 files, +854 / −37; from f4a5586, 92 files, +5,692 / −312.
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. (unified plan v102, from the wrap report) claude-config HEAD is
        34e422a6476d9601f57f61b83f6b1fc64030f9f2, parent 2aa4eac1b54a69d80e7b26587f6ff4df25114b67,
        clean and level, or a descendant acceptable under R663(b) and R796(g); primer.md
        lines 41–45 equal the lane line's five "+" lines.
    I2. (same source) CC session 53a21c5b-0879-47ef-9a86-9ba2b7e623a4 ran 2026-11-09 02:57
        to 23:24 UTC and is archived; nothing of this lane survives it. The archive time is
        read from the record.
    I3. Every nightly since the closeout ran on 840db24 (reflog method with the lock line,
        R779(c); form: 12ae_close/nightly_20261109.txt).
    I4. transcribe_ruling.py --append only appends: the plan's "−" count is 0, read from
        the numstat and matched, never assumed (R766(c)).
  INFERRED — expectation (report a mismatch, continue):
    E1. ~/scratch/12ae-open/ still holds expected_12ae.json, block181.json and the item-0
        and step-4 checks; ~/scratch/12ae-close/ holds block193.json. If one is gone, the
        committed evidence and the tool's docstring govern.
    E2. The recount's JSON is byte-equal to the committed one (item 0 moves no row and no
        dated line).
    E3. Health jobs 20/20, no FAIL; NV_ERR_NO_MEMORY at or above 31, new lines inside
        health-job windows.
    E4. The gate lands inside the band beside surgical-cv's uvicorn only.
    E5. The host's four band values equal 12ae_close/host_state_close.txt's. If one
        differs the band series restarts: report it before the gate and do not match.

CONTRACT (steps, in order; evidence under ~/scratch/12af-open/):

  STEP 0 — Pre-check (read-only).
    0a. date -u beside the day boundary; state the UTC date the item-0 commit will carry.
        List every 07:00 health run and 09:00 nightly since 2026-11-09 23:24 UTC, each
        identified from its log (the health log is undated: by mtime and tail, as
        12ae_close/health_job_20261109.txt's header does). EXPECTED: the 2026-11-10 pair;
        the 2026-11-11 pair only past 10:35 UTC that day. A run that should exist and
        cannot be found = STOP.
    0b. evidence-engine: HEAD 840db24454f08f2486d488ba3e7501fdb73d2dfb, parent
        df3e7214cc421a627add3271bbd049da2fafb757, each proved by git cat-file -e
        <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. git diff --stat 24ce26b
        HEAD on R8's paths empty. Mismatch = STOP.
    0c. Plan: sha256, bytes, lines, last line, last block, no Block 194 (R1). Read the 12af
        block whole (R2) and report any clause this brief does not carry or contradicts.
        The three rules files' sha256, header counts, last A line and gloss count (R3).
        12ae_close/ and 12ae_open/gate_12ae_open/ listings (R4). Mismatch = STOP.
    0d. claude-config (I1): cat-file -e on both hashes; HEAD, parent, status, ahead/behind;
        git log 2aa4eac..HEAD with authors and trailers. A descendant is accepted when each
        commit is attributed, removes no row of this lane and leaves primer.md 41–45
        intact (R663(b)); foreign uncommitted rows are recorded for 2f (R654(d)). Anything
        else = STOP.
    0e. Lane line: ~/scratch/12ae-close/lane_line_12ae.txt sha256, bytes, lines (the
        MEASURED values); its five "+" lines equal ~/claude-config/primer.md 41–45; none
        of its five "−" lines is there. Mismatch = STOP.
    0f. Leftovers (I2): this user's processes and tmux sessions started in 53a21c5b's
        window, each with its origin, this session's own tree excluded (R680(c)). A
        survivor of this lane = STOP, reported, not killed. Other lanes' are readings.
    0g. Readings: the twelve 12ae scratch directories and ~/scratch/12ae-open/'s and
        12ae-close/'s listings (E1); the temp root's entries and bytes (R647(c)); the
        project primer's 12ae block lines naming the lane line and claude-config.
    (Fingerprint, probe scratch, retention, stage/, pins, dbchecks, codebook and spec
    hashes and the Ollama reads run inside step 3's script and are not duplicated here.)

  STEP 1 — The scheduled-job reads (read-only; R747(b)'s explicit part). For each date 0a
    found, earliest first, written under ~/scratch/12af-open/ in 12ae_close/'s 2026-11-09
    names with the job's date (health_job_, health_journal_, nightly_, nightly_ids_), plus
    one nvrm_this_boot_<read date>_read.txt in 12ad_close/nvrm_this_boot_20261109_read.txt's
    form. Quote each model file's header before writing its counterpart.
    1a. Health job (readings, no stop): loads OK of total, any FAIL with its model; non-200s
        in the Ollama journal 06:59–07:12; NV_ERR_NO_MEMORY lines this boot, count and
        timestamps against 31, each new line with the load beside it; NRestarts; earlyoom
        kill lines this boot (R684(d)); boot id and service start (a change is a dated
        fact, reported before the gate); residency now; any model load since 2026-11-09
        23:24 UTC outside the health windows, named with its origin.
    1b. Nightly (governed): line for line against 12ae_close/expected_nightly_20261110.md
        (sha256 4fe1f18a8e0c89fbf019b341e7204beb3d37acee472b1b53473e373547c7f6af) — the
        collection line, the summary line, the exit-code line last, the durations file, the
        journal 09:00–09:25 empty of loads, the lock mtime; its ids as a set (R655(b))
        against 12ae_close/collected_ids_12ae_close.txt (3,545), 0 / 0; the HEAD it ran on
        (I3).
    A nightly mismatch on any date, or a nightly HEAD other than 840db24, = STOP before
    step 2, both values and the date reported. Do not adapt.

  STEP 2 — Item 0 (one evidence-engine commit, one ledger commit; model ca9d348 less its
    standing-rules step, plus D2; the check written for this brief, R794(i)).
    2a. docs/session-reports/session-12/12ae_close/r796.txt from Section 1, one write,
        hashed straight after (R738(b)): EXPECTED 7805916b462f9efb073e983ed397a62232d2fc8db90d96bc913c87e435d9b2f1,
        10200, 111. Mismatch = STOP; the file is not edited to match (R739(b)).
    2b. ~/scratch/12af-open/block194.json in block181.json's key set (E1): tag R796;
        transcribed = the commit's UTC date; item "item 0 of the 12af-OPEN brief"; beside =
        "the lane-line copy and the brief file it orders" (composed for this item from
        R6's wordings; quote it); made_by PI; made_date 2026-11-09; session 12ae; report =
        as R796's STATUS line names it, "the 12ae-CLOSE closeout block (<full hash>) and
        final report", the hash in backticks as Block 181 writes its own; report_short the
        same with the short hash; sentence model, followed "12af-OPEN brief"; prev_ends
        R795. Render to stdout first and compare the element order with Block 181's (a
        difference in form is a finding); then --append.
    2c. cp the lane-line file into 12ae_close/; sha256 equal to 0e's.
    2d. The brief file (D2): docs/session-reports/session-12/12af_open/brief_12af_OPEN.md
        from this whole pasted block, "CLAUDE CODE" through the STOP-12AF-OPEN line, one
        trailing newline, written once; sha256, bytes and lines straight after the write,
        reported.
    2e. The item-0 check: --check-plan byte-identical, exit 0; the plan's numstat read ("+"
        N measured, "−" matched to 0 — I4); every earlier "ends at" line kept (count before
        and after); the last line "The decision log in this file ends at R796." once; line
        count 17693 + N; r796.txt equal to 2a; the lane-line copy equal to its source; the
        three rules files' sha256 equal to 0c's and none staged; the brief file hashed;
        block194.json's transcribed value equal to the commit-time UTC date; the staged set
        exactly the plan, r796.txt, lane_line_12ae.txt and brief_12af_OPEN.md; the plan's
        new sha256, bytes and lines read into the evidence. Every value from arguments or
        the files checked (R742(b)); no phrase searched in any text (R777(b)); evidence by a
        redirect inside the check. Quote the check.
    2f. <check> && <commit>, one line, no pipe (R716(e), R720(c)); the message composed
        from the final staged set (R778(d)), docs scope, naming Block 194, the lane-line
        copy and the brief file. A check failure or crash = STOP before the commit (R705(d),
        R763(c)). Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). Then the ledger row in the "ledger" form with trailers (R680(b)), the
        row's exact timestamp, foreign pending rows named by hash (R654(d)); push; level.
        If another lane has committed this lane's row, record it and do not re-commit
        (R709(c)). Record hashes and parents.

  STEP 3 — The session-open check, the explicit steps and the gate.
    3a. ~/scratch/12af-open/expected_12af.json in expected_12ae.json's shape (E1; else the
        script's own section readers), every one of the script's fourteen sections present:
        repos (evidence-engine at the 2f commit, parent 840db24; claude-config at the 2f
        ledger commit and its parent; worktrees 1 each; no status key); plan (the post-item-0
        values, last line "…ends at R796."); fingerprint, hashes, pins, dbchecks, retention,
        probe_scratch and stage with 12ae_close/'s files as references, as the 12af block
        names them; nightly (date 2026-11-10, the expected file of 1b, that date's log and
        durations paths, the lock); health (the latest run's date, D4; models 20); ollama
        (0.21.0, the five digests in full from 12ae_close/ollama_reads.txt, NRestarts 0);
        temp_root; cc_sessions (ids ["53a21c5b"], window from I2 with the archive time as
        read). Every value from the 12af block, a 12ae_close/ file or this run's own
        measurements; none invented. Quote the file.
    3b. Run session_open_check.py on it from the repository root, output by redirect.
        EXPECTED no SKIPPED, no MISMATCH, exit 0; each "READING foreign:" line attributed.
        A SKIPPED or a MISMATCH = STOP (item 0 stands and is reported, not reverted).
    3c. Recount (R7): in a scratch clone under ~/scratch/12af-open/ (R673(b)), with the
        eight arguments its docstring names, the eighth the plan at the 2f commit. EXPECTED
        now = rows 286, class 94 / 56 / 136, without_class 0; P1 62 (25 / 25 / 12); P6 12;
        P7 121; fix units 35 (script) / 37, 19 (literal) / 38, done 31 (dated lines)
        (R714(c)); r510_session_12ae rows in 2, out 0; expected_holds true; E2. The script's
        sha256 against 9946640e9e1a25f1a039de8ad193ce470277b4828babe72856c8eb61ce18d0b8.
        Rows other than 286, or dated lines other than 38 / 31, = STOP before the gate.
    3d. R510 at this open: the six-cell INT- rows at 840db24 and at the 2f commit by R5's
        method. EXPECTED 212 and 212, in 0, out 0, no class or package move; written to
        ~/scratch/12af-open/r510_12af.txt. Another value = STOP before the gate.
    3e. The gate, as the 12af block's gate bullets state it, by launch_gate.sh from the
        repository root: evidence_dir ~/scratch/12af-open/gate_12af_open/, a session name in
        12ae-OPEN's form, ref_ids 12ae_close/collected_ids_12ae_close.txt, ref_durations
        12ae_close/durations_12ae_u28b.txt, no selection. Before it: the host snapshot (E5)
        and the Ollama reads at least 5 s ahead, timestamped (R660(b)). One waiting process
        on gate_rc.txt (R704(e)(ii)). EXPECTED "collected 3562 items / 17 deselected / 3545
        selected"; 3,544 passed, 1 xfailed, 17 deselected, 2 warnings (R641(c)); exit 0;
        ids 0 / 0; wall time inside 876–1069 s — outside it, reported with the concurrent
        jobs named (R720(e)), not adapted to and not re-run (R796(b)); durations against the
        reference, any phase slower by more than 10 s named; basetemp read straight after
        (R660(c)); the nineteen files present by name against 12ae_open/gate_12ae_open/'s
        (R738(d)). A model load in the gate's journal window = STOP; a read-only request is
        a reading (R696(b)). Any red = STOP (the tree is unchanged, so no census miss is
        expected, R746(c)). A wrapper fault is reported as the wrapper's and = STOP before
        step 4; the gate's own result is still reported.
    3f. After the gate: Ollama reads, host snapshot, temp root. Then no-marker collection
        (3,562), tests/test_eligibility.py (91), inventory --check (in sync), and R8's three
        diff stats against 12ae_close/'s diff files.

  STEP 4 — The evidence commit (R731(c); model 95dad8d). cp the gate's files into
    docs/session-reports/session-12/12af_open/gate_12af_open/ and step 1's files into
    12ae_close/ under their step-1 names; every sha256 equal after; scratch untouched. The
    pre-commit check: the staged set exactly the copied files, nothing else, its numstat
    read into the evidence (R772(c)); <check> && <commit>, one line; docs scope; the message
    from the staged set. Fingerprint --compare before and after the push (IDENTICAL); push;
    level. The ledger row as at 2f. Record hashes and parents.

ACCEPTANCE GATES (each step's EXPECTED values are its gate; in summary):
  G1. Step 0 matched or stopped; every full hash proved by cat-file -e; I1 and I2 verified.
  G2. Step 1: each nightly a line-for-line match on 840db24 with ids 0 / 0 and an empty
      window; each health job reported as readings; the files written per date.
  G3. Step 2: one item-0 commit that passed 2e in one chain; pushed, level, fingerprint
      IDENTICAL; the ledger row with trailers.
  G4. Step 3: the open check clean; the recount at 286 with 38 / 31; R510 212 / 212; the
      gate at 3e's EXPECTED values, inside the band or reported outside it; 3f matched.
  G5. Step 4: every copy sha256-equal to its source; pushed, level, fingerprint IDENTICAL;
      its ledger row.

OUT OF SCOPE: U40's Phase A and every later unit. The six tools items. Any edit to the
  rules files, the expected nightly file, either primer, a recount script or a committed
  read-out. Killing or cleaning any process, tmux session, worktree or temp-root entry.
  Any model call or Ollama restart. /wrap.

CONSTRAINTS (task-only): no live write, no model call, no provider call. Nothing deleted
  outside ~/scratch/12af-open/ (R658(c)). A count arrived at by eye is INFERRED; a computed
  value is reported with its command. Where this brief describes a tool or a record, the
  tool and the record govern and a difference is a finding (R732(a)). A tempting fix found
  during the reads is a finding, not an edit. A reboot or host change mid-brief is a dated
  fact of the report; 3e does not start until it is reported.

FINAL REPORT FORMAT: (0) the pre-check table, EXPECTED against READ, the clock, each hash's
  cat-file result, 0d's log, 0f's list with origins, the archive time as read, the jobs
  found by date; (1) one row per health job and per nightly, the files named; (2) item 0 —
  block194.json, the render comparison, r796.txt's hash, the brief file's sha256, bytes and
  lines, the check verbatim with its evidence, the chain, hashes and parents, numstat, the
  plan's new sha256, bytes and lines, fingerprint, the ledger commit; (3) expected_12af.json
  and the script's output verbatim, the recount's JSON fields, R510; (4) the gate — the two
  launch lines, wall time against the band, the summary line, ids, durations, journal
  attribution, concurrent jobs, the nineteen files, 3f; (5) the evidence commit — the file
  table, check, hashes, fingerprint, ledger; (6) host readings and temp root before and
  after; (7) contradictions and findings, numbered, each with its quoted line, including
  anything in R1–R8 or the FORM section the box contradicts; (8) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, a hash cat-file -e does not prove, I1 or I2
  false, a survivor of this lane, or a nightly mismatch or HEAD under step 1 = STOP before
  any commit. A 2a mismatch, a form difference at 2b that the tool refuses, or a check
  failure or crash at 2e = STOP before the item-0 commit. A SKIPPED or MISMATCH at 3b, or
  a 3c or 3d value off its EXPECTED = STOP before the gate; item 0 stands. A model load in
  the gate window, a red, or a wrapper fault = STOP before step 4. The clock at 06:30 UTC
  before 3e launches = STOP. An expectation mismatch (E1–E5) and every health reading are
  reported and the run continues. A spec premise that does not survive contact with disk
  is a finding, not an obstacle.

HANDBACK: Stop and report. Do not start U40's Phase A. Await the next brief or ruling in
  this session. STOP-12AF-OPEN
