CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R824, PI ruling 2026-11-20 UTC; not on disk)
================================================================================

R824 is transcribed at item 0 of the brief below, by
docs/session-reports/tools/transcribe_ruling.py, as the next block of the decision log
(Block 222). The lines between the two marker lines, exclusive of the markers, are the
byte content of r824.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's
independent hash:
  r824.txt: sha256 dd3beb97cb6748eb7eb98e4f4dff04de00093501633638e8775aacec2f5f3b2d,
            4761 bytes, 62 lines, one trailing newline.

===== BEGIN r824.txt =====
RULING [evidence-engine 12ai-R824] — 12ai-CLNB accepted: the fifty-three clones removed; the
  PI's word to wrap; session 12ai closes by a closeout and a wrap ruling of its own; no
  closeout gate; the kept clones left as they are; readings on the findings; no standing rule
STATUS: PI ruling, 2026-11-20 UTC, session 12ai, on architect recommendation, on the 12ai-CLNB
  final report (item 0 3a65641ee9c16cf1e50e40eb9560adf375cecc3e; evidence
  366de1e7b60e37d804fab9c8161a3d41be2e1b41). Transcribe at item 0 of the 12ai-CLOSE brief
  below, by transcribe_ruling.py, in the block after R823's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12ai-CLNB final report, quoted; wrapped lines joined): "Commit:
    3a65641ee9c16cf1e50e40eb9560adf375cecc3e (parent b204649)."; "Ledger: a9ae0c2."; "r823.txt:
    66956e9b…28e0, 4,870 B, 64 lines. Equal to the architect's hash."; "Brief file:
    619a8f3975857ce7ddbf579b304f3afb84408b841a4deca490997b013369dda6, 20,546 B, 303 lines."
    (the architect's copy: the same); "53 of 53"; "53 removed, 0 skipped; every one of the 7
    checks passed for every row."; "First removal 22:49:17Z (12aa-close/clone), last
    22:49:22Z (12z-open/clone)."; "The four KEEP clones and the two fixture repositories read
    exactly as at 2a."; "a rise of 5,646,884,864 B, against the link-count-1 sum of
    4,933,649,532 B"; "git fsck --connectivity-only exits 0"; "497 "dangling" lines and
    nothing missing or broken"; "MATCH retention MATCH / MISMATCH: [23, 0]"; "RESULT: PASS (0
    mismatches)"; "Commit: 366de1e7b60e37d804fab9c8161a3d41be2e1b41 (parent 3a65641), 11
    files, +1,190 −0"; "Ledger: 78bd0eb."
  READ (CC's): findings 1–3; the cause of the space freed beyond the list's figure, "not
    measured".
  READ (the architect's clone at 366de1e): the two hashes in STATUS in a straight chain on
    b204649; 12ai_CLNB/brief_12ai_CLNB.md and 12ai_open/r823.txt equal to the architect's
    copies by cmp; the plan's last line "The decision log in this file ends at R823.";
    12ai_CLNB/removal_log.tsv, fifty-three rows; 12ai_CLNB/3a_main_repo.txt, 497 lines
    beginning "dangling" and no line holding "missing" or "broken"; git diff --stat
    7fad72a HEAD on engine tests scripts analysis review_specs, empty.
  INFERRED: none that a clause below rests on.

R824(a) — The 12ai-CLNB final report is accepted as printed. Item 0 3a65641 (Block 221;
  r823.txt and the brief file equal to the architect's copies), ledger a9ae0c2. The
  fifty-three clones of R823(b) are removed, each by its own top directory after its seven
  checks, none skipped; the four kept clones, the two fixture repositories and every
  parent entry's other files read as before; the main repository whole; the retention,
  probe and stage rows MATCH; the fingerprint IDENTICAL. The evidence commit 366de1e,
  ledger 78bd0eb. I1 and I2 held; E1–E4 held. 5,646,884,864 bytes were freed by df's
  reading.
R824(b) — Readings, no row. Finding 2 is the architect's: the brief ordered the repository
  check after the removal and no count of dangling objects before it, so the 497 has
  nothing to be set against; a removal of directories outside .git makes none, which is
  reasoning and not a measurement. Finding 1: the space freed beyond the list's lower
  bound is unexplained by measurement. Finding 3: the fixture's leftovers stay in this
  session's scratch directory.
R824(c) — The PI's word: "wrap". Session 12ai closes: a closeout brief (below), its report
  read and accepted, then a wrap ruling of its own. Nothing else is started in 12ai. U31
  opens the next session with its Phase A.
R824(d) — No closeout gate. The engine tree at HEAD is 7fad72a's, gated at 12ai-U30B
  (R821(b)); every later commit is under docs/. The band for the next session is computed
  at the closeout from the last four standard gates and set by the wrap ruling.
R824(e) — The four clones this session made, and the one the closeout makes for its recount,
  are left as they are. They are not ruled for removal; a later session lists them if the
  PI asks.
R824(f) — What this lane owed the workbench is the PI's to send: the restart change's hash
  (R820(f)) and the removal's block as the report prints it. Still owed by the workbench:
  the merge sha for deepseek-r1:32b in dgx-infra's scripts/ollama_lane_models.txt.
R824(g) — The count for 12ai, to be confirmed by the wrap ruling: one stop, the architect's
  (12ai-CLNA at its step 2j, R822(a)); no pause; fourteen architect's errors (R823(f)'s
  thirteen; finding 2), one reaching a commit.
R824(h) — This ruling states no standing rule: the id list, the glosses file and
  STANDING_RULES.md are untouched at the item 0 that transcribes it, asserted by content.
===== END r824.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ai-CLOSE] — Close session 12ai, in 12ah-CLOSE's form read from
  the record: item 0 (R824 as the next block; this brief committed as pasted; no
  standing rule), then the closeout commit (the closure paragraph, the "Expected values
  at session-12aj open" block, 12ai_close/'s measurements, every job run since the
  2026-11-19 pair, the band computation, a new recount script and its recount, R510,
  retention, a new expected-nightly file, the lane line drafted and not applied). Docs
  only. No engine edit. No live write. No model call. No closeout gate (R824(d)).
PURPOSE: Leave the record in the state the next session opens on: every fact of 12ai in
  the plan's closeout block with its evidence file beside it, the expected values for
  12aj's open stated from the 12ai-U30B gate, and the band figure computed for the wrap
  ruling.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 222; then the closeout block and the
  12aj expected-values block); 12ai_open/r824.txt; a new directory 12ai_close/ (the
  counterpart files); a new directory 12ai_CLOSE/ (brief_12ai_CLOSE.md, at item 0);
  ~/scratch/12ai-close/ (the lane line draft, the recount clone and scratch); the
  claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/, the codebook, docs/architecture/, CLAUDE.md, docs/inventory/, the tools
  (no gate and no unit is launched); the id list, the glosses file and
  STANDING_RULES.md (not one byte, R824(h)); 12ah_close/, 12ai_open/'s existing files
  and every earlier directory under docs/session-reports/ (read, never rewritten); the
  dated units lines and the rows already in the plan; the lane line in the primers
  (drafted to scratch, applied only by the wrap ruling); review.db (mode=ro
  instruments only); ollama.service and the health job (read only); every entry under
  ~/scratch but ~/scratch/12ai-close/ (read only; nothing removed, the kept clones
  included, R824(e); no git command inside a kept clone).

PATHS: a name of the form 12xx_<name>/ is a directory of docs/session-reports/session-12/;
  a tool is named by its full path.

FORM (R784(g)(iv)): 12ah_CLOSE/brief_12ah_CLOSE.md (sha256
  f860358d1b8f80876430d6cf57e946b32faea91cc0af57059fb763a70d5aaa6a, 377 lines), Section
  2, is this brief's form, with the forms it names in turn — steps 0, 1 and 2 (2a–2h),
  the acceptance gates, constraints, final report format and contradiction handling,
  read whole before anything is composed, with the 12ah closeout it produced
  (b13b5eb23c86c5da9df566d9e59a9764c7831335: the closure and the "Expected values at
  session-12ai open" block; 12ah_close/'s files) as the worked example. Session 12ah
  landed no unit and moved no row; this one landed two and moved five. For what that
  changes — a new recount script, a new expected-nightly file, the units and rows in
  the closure — the form is 12af_CLOSE/brief_12af_CLOSE.md and the 12af closeout
  7ddabca71511374aacdaefdd95a6de4eb651a268 with 12af_close/. This brief states only
  what differs. Where the forms and this brief differ, this one governs; where a form
  names a 12ah or 12af source, the 12ai source listed below takes its place. A step of
  the form with no 12ai source is reported as not done, with its reason. Item 0's
  model for one block is 3a65641 (Block 221).

TIMING: issued at 22:56 UTC on Friday 2026-11-20. Valid outside the day's window
  (R812(a)); the next opens at 09:00 UTC on Saturday 2026-11-21. The clock is read
  before each commit. If it reaches 15 minutes before an opening before a commit, hold
  with the work on disk uncommitted and resume in this brief after 10:35 UTC; say so.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ai-CLNB final report; R824): HEAD
    366de1e7b60e37d804fab9c8161a3d41be2e1b41, parent
    3a65641ee9c16cf1e50e40eb9560adf375cecc3e, clean, level; claude-config at 78bd0eb or
    an accepted descendant (R663(b)). From the 12ai-U30B final report (R821): the gate
    12ai_U30B/gate_u30b/, 3,607 / 1 / 17 on 3,625 / 3,608, launched as a unit, no hold.
  READ (the architect's clone at 366de1e, 2026-11-20 22:53 UTC; re-read on the box):
    R1. The plan: sha256
        9caf09778b7c70d79c03ac7436ad6f60a40027f72f1e4362811a627cba37f785, 3469210 bytes,
        20832 lines; last line "The decision log in this file ends at R823."; last block
        221; 178 lines begin "The decision log in this file ends at R". "Expected
        values at session-12ai open" starts at plan line 5859 and "## Decision log" is
        at 5900.
    R2. Rules files: STANDING_RULES.md
        d56dbdaeafb3659527cb77e1e81744eb01dadbe871fd96944104bb9e01b7e3ff; the id list
        a28da0611e1a4d62cbc8185ff89df22d2df7a2a37340da0eb164dec678e75257; the glosses
        7f7374b25f7efd2c1df97cfbe3fb9b828c7fec5bce4e948d751cec08a4fa72ed.
    R3. The session's commits, git log b13b5eb..HEAD, fourteen: 7ff68fe, 35fc9ec,
        bec3545, 483969d, ee20dba, 339370d, 03abfaf, b998dfb, 7fad72a, d7a0bf7, f4427d5,
        b204649, 3a65641, 366de1e. Two are engine commits, 339370d (U41) and 7fad72a
        (U30 with N46). The session opened on
        b13b5eb23c86c5da9df566d9e59a9764c7831335, the 12ah closeout. git diff --stat
        b13b5eb HEAD on docs/session-reports/tools is empty; on engine tests scripts
        analysis review_specs it reads "18 files changed, 1282 insertions(+), 121
        deletions(-)", and from f4a5586 "102 files changed, 7861 insertions(+), 436
        deletions(-)"; from 7fad72a it is empty.
    R4. The session's rulings: 12ah_close/r816.txt (made at the 12ah wrap, transcribed
        at 12ai-OPEN); 12ai_open/r817.txt to r823.txt on disk; R824 in Section 1. None
        stated a standing rule.
    R5. Six-cell "| INT-" rows in the plan: 215 (213 at b13b5eb). Opened in 12ai:
        INT-ai12-1 and INT-ai12-2. Closed in 12ai: INT-ai12-1 (on 339370d), INT-i12-71
        and INT-j12-51 (on 7fad72a). U41 entered and landed; U30 landed. The last
        dated units line is 12ai-CLNA item 0's: fix units 39, done 35.
    R6. 12af_close/recount_12af_close.py (sha256
        a86247439038b1610b989e1ed08f6744d5322ec98c55858bda7fbc9f68f03a47), whose own
        expectation no longer holds since rows and units moved (the 12ai-U41B report's
        finding 7).
    R7. gate_wall.txt reads "wall: 978.222568504 s" in 12ah_open/gate_12ah_open/, "wall:
        995.533181647 s" in 12ai_open/gate_12ai_open/, "wall: 963.330798171 s" in
        12ai_U41B/gate_u41b/ and "wall: 977.539204053 s" in 12ai_U30B/gate_u30b/, which
        also holds collected_ids.txt and durations_12ai_u30b.txt.
    R8. 12ah_close/ now holds 24 files: the closeout's seventeen, r816.txt,
        lane_line_12ah.txt and the 2026-11-19 job reads. It holds lane_line_12ah.txt
        (the lane line's form) and host_state_close.txt.
    R9. 12ai_open/between_sessions_read.txt holds the open's host readings; R821(e)
        records the dgx-infra lane's removal of the sudoers grant on 2026-11-20, that
        lane's statement.
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. The engine tree is unchanged since 7fad72a.
    I2. transcribe_ruling.py --append only appends.
  INFERRED — expectation (report a mismatch, continue with the measured value):
    E1. The new recount reads 289 rows (94 / 59 / 136, 0 unclassed) and the dated
        lines' fix units 39, done 35; R510 for 12ai is in 2, out 0, the six-cell INT-
        rows 213 at b13b5eb and 215 at the item-0 commit.
    E2. The band computation, from the last four standard gates on this host state —
        the 12ah-OPEN gate 978.22, the 12ai-OPEN gate 995.53, the U41B gate 963.33 and
        the U30B gate 977.54, each read from its gate_wall.txt — gives a mean of
        978.66 s and, rounded inward, 881–1076 s. The series has forty-six members.
    E3. The three diff stats as R3 gives them.
    E4. Retention 23 MATCH / 0 MISMATCH by file sha256, none opened, no new entry.
    E5. The fingerprint IDENTICAL; dbchecks_close.txt, pin_tuples_close.json and the
        codebook and spec hashes byte-identical to 12ah_close/'s.
    E6. One job pair has run since the 2026-11-19 pair when 2b reads on 2026-11-20: the
        2026-11-20 pair, its health job a light check, its nightly run on a HEAD whose
        engine tree is 339370d's, at the U41B gate's counts (3,593 / 1 / 17 on 3,611 /
        3,594).
    E7. The host's four band values equal 12ah_close/host_state_close.txt's. If one
        differs, the band paragraph says so and the figure is computed and marked.

CONTRACT (the form's steps, with these 12ai terms; evidence under ~/scratch/12ai-close/):

  STEP 0 — Pre-check, as the form's: HEAD and parent proved by git cat-file -e
    <hash>^{commit}; clean; fetch; 0 0; one worktree. R1, R2, R5, R8. claude-config: the
    MEASURED commit or an accepted descendant, level; a pending foreign row carried and
    named. I1. No gate-* unit loaded. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (the model quoted before composing; the check written for this brief,
    R794(i)).
    1a. 12ai_open/r824.txt from Section 1, one write, hashed straight after against
        Section 1's hash. A mismatch = STOP (R738(b), R739(b)).
    1b. block222.json (R824) in block221.json's key set: made_by PI; made_date
        2026-11-20; session 12ai; report as its STATUS names it, "the 12ai-CLNB final
        report (item 0 <full hash>; evidence <full hash>)", the hashes in backticks, in
        Block 215's form for two hashes; item "item 0 of the 12ai-CLOSE brief"; sentence
        model, followed "12ai-CLOSE brief"; "beside" "the brief file it orders", in the
        model's words, quoted; transcribed = the commit's UTC date; prev_ends R823.
        Render, compare the element order with the model's, then --append.
    1c. No row note and no dated units line: the units and rows of 12ai are already
        noted and dated at their own item 0s (R5).
    1d. The brief file (R784(g)(v)): 12ai_CLOSE/brief_12ai_CLOSE.md from this whole
        pasted block, "CLAUDE CODE" through the STOP-12AI-CLOSE line, one trailing
        newline, written once; sha256, bytes and lines straight after the write,
        reported.
    1e. The item-0 check, as the form's 1e with these terms: the last line "…ends at
        R824." once; the plan equals HEAD's plus Block 222 appended, nothing else (the
        "−" count read and matched to zero); six-cell INT- rows 215 before and after;
        r824.txt equal to 1a; the three rules files at R2's hashes and unstaged; the
        staged set exactly the plan, r824.txt and the brief file.
    1f. <check> && <commit>, one line; then fingerprint, push, level and the ledger row
        as the form's 1f; the ledger message built from the diff.

  STEP 2 — The closeout commit, every element in the form's order, each with its file
    under 12ai_close/ in 12ah_close/'s counterpart name; a name with no 12ai source is
    listed as not written, with the reason.
    2a. Measurements as the form's 2a, dated names carrying the read's UTC date.
        Differences: the diff files are diff_engine_since_b13b5eb.txt,
        diff_since_977c499.txt and diff_since_f4a5586.txt (E3), and none of them is
        empty; no tools diff file is written, the tools being unchanged (R3), and the
        closure says so; collected_ids_12ai_close.txt is a copy of
        12ai_U30B/gate_u30b/collected_ids.txt and durations_12ai_u30b.txt a copy of
        that gate's durations file, each cmp equal to its source, the ids also set
        against 12ah_close/collected_ids_12ah_close.txt with the only-ref and
        only-gate ids listed (the thirty-four new ids and the renames of U41B and
        U30B, read from the two gates' own comparisons, not from this line); dbchecks,
        pins and the codebook and spec hashes compared with 12ah_close/'s (E5); the
        host state with E7, and in it, as readings, ollama.service's start time,
        MainPID, NRestarts, RestartUSec and OLLAMA_KEEP_ALIVE (no other environment
        value), the health job's crontab line and its script's sha256, the user
        manager's state and the absence of any gate-* unit. No sudo: whether the
        sudoers grant exists is not read by this lane (R821(e)).
    2b. The jobs (E6): every health job and nightly run after the 2026-11-19 pair and
        before this read, read as 12ai-OPEN's step 1 read 2026-11-19, their files into
        12ai_close/ in those names, a light check's file saying which form ran. Each
        nightly is read against the gate of the engine tree it ran on, its HEAD found
        by the reflog method: on 339370d's tree, 12ai_U41B/gate_u41b/ (counts and ids);
        on 7fad72a's, 12ai_U30B/gate_u30b/. 12af_close/expected_nightly_20261111.md no
        longer governs, the tree having changed; the jobs paragraph says so, and which
        file governs from here (2e). A nightly that does not match its tree's gate =
        STOP before the closeout commit, both values reported.
    2c. The recount (E1): rows and units moved in 12ai, so a new recount script is
        written, 12ai_close/recount_12ai_close.py, in recount_12af_close.py's form and
        by the chain the 12af closeout used (its baseline the committed
        12af_close/recount_12af_close.json; the model quoted first), run in a scratch
        clone made under ~/scratch/12ai-close/ on the plan at the item-0 commit; its
        JSON committed beside it. r510_12ai.txt in r510_12ah.txt's form, computed at
        b13b5eb and at the item-0 commit, naming the rows in and out. A row count
        other than 289, dated lines other than 39 / 35, or R510 other than in 2, out 0
        = STOP before the closeout commit, both values reported.
    2d. band_computation.txt in 12ah's form: the four inputs of E2, each with its
        evidence path, its gate_wall.txt value, how it was launched (a user unit, all
        four) and the concurrent jobs from its snapshots; the test counts each ran
        (3,574, 3,574, 3,594 and 3,608 selected) stated beside them as a reading;
        mean, ×0.9, ×1.1, the inward rounding, the figure; the series of forty-six
        listed — 12ah's forty-three plus 995.53, 963.33 and 977.54, each read from its
        file; "a figure, to be set by the wrap ruling; the band in force stays
        897–1095 s".
    2e. A new expected-nightly file, 12ai_close/expected_nightly_<the read's UTC
        date>.md, in 12af_close/expected_nightly_20261111.md's form, from the U30B
        gate: it governs each nightly on a tree equal to 7fad72a's (R749(b)).
    2f. The lane line, ~/scratch/12ai-close/lane_line_12ai.txt, in
        12ah_close/lane_line_12ah.txt's form: its five "−" lines are
        ~/claude-config/primer.md's lines 41–45 as they stand, taken from primer.md
        itself; its five "+" lines carry 12ai's sources — HEAD the closeout commit
        (filled after 2h's commit, so the file is written last, is not in the commit,
        and is pasted whole in the report), the plan's sha256, bytes and lines, the
        decision log ending R824 (R825 at 12aj-OPEN's item 0), and the session's
        summary: two fix units landed, U41 (the Ollama restart paths opt-in, 339370d)
        and U30 with N46 (7fad72a); fix units 39, done 35; rows 289; the interim
        inventory 12ai-SCRI committed after a publication read-out; fifty-three
        scratch clones removed on the PI's approval; the sudoers grant for restarting
        Ollama removed by the dgx-infra lane; no standing rule; one stop, no pause;
        three gates and the band figure; pins unchanged; no live write; the next steps
        — 12aj-OPEN, then U31's Phase A, then U32–U36. Each "+" line read whole after
        writing, every word separated from its neighbour and its code marks paired.
        Not applied.
    2g. The closeout block and "Expected values at session-12aj open", placed after
        the 12ai expected-values block's last line and before the decision-log section
        (the position read and matched, R1), in the 12ah block's element order with
        12ai's content, the units and rows in the 12af block's manner:
        — the closure paragraph (opened on b13b5eb; this CC session's id and times read
          from the record; the interim CC session bf3224f8 before the open, read-only,
          counted with this session, R817(c));
        — the briefs in order — 12ai-OPEN, 12ai-SCRI-RECA, 12ai-SCRI-REC, 12ai-U41B,
          12ai-U30B, 12ai-CLNA and its resume R1, 12ai-CLNB, this closeout — each with
          the ruling at its item 0 or carried in it and its committed file's sha256 as
          the rulings record its match with the architect's copy; the interim
          session's brief was not saved (12ai_SCRI/12ai_SCRI.md's own words);
        — the commits in order from git log b13b5eb..HEAD with their ledger commits;
          the diff lists;
        — the rulings R816–R824 one line each (R816 made at the 12ah wrap and
          transcribed at 12ai-OPEN);
        — the units: U41 entered by R819(b) and landed at 339370d; U30 landed with N46
          at 7fad72a; each with its acceptance file and its gate;
        — the rows: opened INT-ai12-1 and INT-ai12-2; closed INT-ai12-1, INT-i12-71 and
          INT-j12-51; INT-ai12-2 open, Class 2, P4;
        — the stops, listed from the rulings (one: 12ai-CLNA at its step 2j, R822(a),
          the architect's), the pauses (none), and the architect's errors as the
          rulings count them (fourteen, R824(g)), with the two kinds the rulings name:
          a statement about a file or a code path not read for it, and a mechanism's
          behaviour taken from its name;
        — the tools paragraph (no tools change; no regeneration of the standing-rules
          file; the nine tools items carried unchanged; two readings beside them: the
          open's step-1 helper omits three host readings, R817(g); a read-only pass
          over clones must not run git diff inside one, R822(e));
        — one paragraph for the restart change: R819's design from the committed
          read-out 12ah_OLRA/; U41's build; the seventh call ending; pdf_quality_check's
          bound; the open-time check withdrawn (R819(h)); the dgx-infra lane's removal
          of the grant and what a run that asks now does (R821(e)); CLAUDE.md's lines
          corrected at 339370d and f4427d5, the no-live-write line among them
          (R812(c) discharged); the text still stale and HK-ARCH's (R820(e)); the
          read-out R819(k) owed before the freeze; the removal of both paths a
          candidate after the tag;
        — one for U30 with N46: R805(d) and R807(b) as built; what R807(d) leaves open;
        — one for the interim inventory: the session, the publication read-out
          (12ai-SCRI-RECA), the PI's ruling R818, the ninety-seven files and the
          twenty-one, the scans and their controls, the lists verified from the
          commit; the notes the inventory carries with no rule change — the retention
          ledger has no row after the 12o closeout; fifteen database copies are in no
          retention file; a 0-byte file matches any empty committed file;
        — one for the clones: R817(f), the check (12ai-CLNA) and its failed no-write
          test, R822, the PI's approval R823, the removal (12ai-CLNB): fifty-three
          removed, 5,646,884,864 bytes freed by df, the main repository whole; four
          kept and this closeout's own, left as they are (R824(e));
        — "Carried forward": U31's Phase A, then U32–U36; INT-ai12-2; R819(k)'s
          read-out; the tools items; R807(d)'s three items; the items the 12ah
          closure carried that 12ai did not touch, read from that closure and listed;
          the cross-lane readings for dgx-infra; what is owed between lanes (R824(f));
        — the gates table (three: 12ai-OPEN, U41B, U30B); the band paragraph; the jobs
          paragraph (2b, 2e); the recount paragraph; retention; ledger; counterpart
          files; the host; live; "Session 12ai closed. Next: session 12aj, fresh.";
        — then the 12aj expected-values block in the 12ai block's form, with these
          differences: the references are collected_ids_12ai_close.txt and
          durations_12ai_u30b.txt; the gate's counts 3,607 / 1 / 17 on 3,625 / 3,608;
          the engine tree unchanged since 7fad72a; the band figure of 2d; the
          nightlies per 2e's file, with whatever 2b read; the health jobs as readings
          in their two forms, the count of NV_ERR_NO_MEMORY lines this boot as 2a
          reads it; Ollama's service start, MainPID and keep-alive as readings from
          2a; "The decision log in this file ends at R824" and R825, the wrap ruling,
          at 12aj-OPEN's item 0; the rules files' three hashes (R2); standing rules
          stated in 12ai: none; the recount by 12ai_close/recount_12ai_close.py (289;
          39 done 35); the pins file; this session's scratch directories, each named
          from ~/scratch's listing (the 12ai-* entries), and the fifty-three clone
          directories of 12ai_CLNB/to_remove.tsv absent; the kept clones named; 12aj's
          first items — 12aj-OPEN, then U31's Phase A.
        Every figure from a file in 12ai_close/ or an evidence directory, named beside
        it; nothing from this brief's text where a file exists.
    2h. The closeout check and commit as the form's 2h, with these terms: the last
        "ends at" line "…ends at R824." once; the staged set exactly the plan and
        12ai_close/; the rules files, 12ah_close/ and every earlier directory
        unchanged; the message in b13b5eb's form. Then 2f's lane line completed and
        pasted whole in the report.

ACCEPTANCE GATES: the form's G1–G3 with these terms — I1 held; item 0 one commit that
  passed 1e in one chain with r824.txt at its hash and Block 222 by the tool; the
  closeout commit with every element of the 12ah block present in order with its 12ai
  source and the units and rows as 2g lists them, every counterpart file written or
  listed as not written with its reason, the new recount at E1 or stopped, each nightly
  matched to its tree's gate or stopped, the band computed from the files, the new
  expected-nightly file written, E3–E7 held or reported, the check passed in one chain;
  both pushed, level, fingerprint IDENTICAL; ledgers; the lane line pasted whole, not
  applied.

OUT OF SCOPE: the wrap ruling (R825: the band set, the lane line approved and applied,
  the stops counted — after this report); U31; any engine, test, script,
  docs/architecture, CLAUDE.md or tools edit; any gate or unit; removing anything from
  ~/scratch; the tools items; /wrap, /exit and the archive (after the wrap ruling).

CONSTRAINTS (task-only): no model call, no provider call, no live write, no sudo; a count
  arrived at by eye is INFERRED, the computed value given with its command; every figure
  in the block beside its file; where this brief or its form describes a tool or record,
  the record governs and a difference is a finding (R732(a)).

FINAL REPORT FORMAT: the form's — (0) pre-check table; (1) item 0; (2) the closeout: the
  measurements with their values, each job read with the gate it was set against, the
  recount script's expectations and output, band_computation.txt verbatim, the new
  expected-nightly file verbatim, the check verbatim, chain, hashes, fingerprint,
  ledgers, the plan's sha256, bytes and lines, the two blocks' line ranges in the plan
  (the blocks themselves need not be pasted, being committed), the lane line verbatim
  with its sha256, bytes and lines and its fifth "+" line quoted whole; (3) every place
  this brief's FORM clause left a choice, and the choice made; (4) contradictions and
  findings, numbered, each with its quoted line; (5) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 or I2 false, a 1a mismatch, or a check
  failure or crash at 1e = STOP before the item-0 commit. A recount or R510 off E1, a
  nightly that does not match its tree's gate, or a check failure at 2h = STOP before
  the closeout commit, the work on disk reported. An expectation mismatch (E2–E7) is
  reported and the closeout continues with the measured value in the block. A spec
  premise that does not survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not apply the lane line, do not run /wrap, do not start
  12aj or U31. Await the wrap ruling in this session.
  STOP-12AI-CLOSE
