CLAUDE CODE

================================================================================
SECTION 1 — RULINGS TO TRANSCRIBE (R829–R832, PI rulings 2026-11-23 UTC; not on disk)
================================================================================

Four rulings are transcribed at item 0 of the brief below, by
docs/session-reports/tools/transcribe_ruling.py, as the next four blocks of the decision
log (Blocks 227–230), in order. Never retype a ruling.
  R829, R830 and R831 were pasted in this session's three resumes and stand, byte for
  byte, between the "===== BEGIN r8xx.txt =====" and "===== END r8xx.txt =====" marker
  lines of the committed 12aj_U31R/brief_12aj_U31R_R1.md, _R2.md and _R3.md. Each file
  is written from those lines of its committed resume, exclusive of the markers, one
  trailing newline, and hashed against the architect's hash:
    r829.txt: sha256 c31847e0e0a779da4809181f88a92b686f1e8c6dfdbda56f583060c54d9cd560,
              4524 bytes, 60 lines.
    r830.txt: sha256 13ade72f7df5064455783154e1c73f14466b31b0999d875beb86107a0e99a050,
              3022 bytes, 40 lines.
    r831.txt: sha256 0ba9ec101651271bbe101a4f5655ed27a1a77a71fdd67d1d753d21596472244b,
              4408 bytes, 57 lines.
  R832 is below. The lines between its two marker lines, exclusive of the markers, are
  the byte content of r832.txt: same characters, same line breaks, one trailing newline.
    r832.txt: sha256 64bdbbf13086e4e2c8aa60d892c422e079b3bf98207ec45f41946e28cebc9dd9,
              4703 bytes, 63 lines.

===== BEGIN r832.txt =====
RULING [evidence-engine 12aj-R832] — 12aj-U31R accepted as resumed; the PI's word: "Wrap after
  this evidence commit"; session 12aj closes with U31 half landed; INT-l12-2 closes with U31,
  not at the closeout; no closeout gate; the closeout's retention file in the 12ah form; no
  standing rule
STATUS: PI ruling, 2026-11-23 UTC, session 12aj, on architect recommendation, on the
  12aj-U31R-R3 final report (retirement 30b7f16ed676cbd204ac3a3dc11b1551e89484cf; evidence
  e5a844e23f43b0788e2c85fa5cdaa6854d529973). Transcribe at item 0 of the 12aj-CLOSE brief
  below, by transcribe_ruling.py, in the block after R831's.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12aj-U31R-R3 final report, quoted; wrapped lines joined): "Commit:
    e5a844e23f43b0788e2c85fa5cdaa6854d529973, parent 30b7f16…; pushed, 0 0; fingerprint
    IDENTICAL before and after."; "Staged set exactly 28 new files (25 copies and the 3
    resume files), +13,415; every copy sha256-equal to its scratch source; no path named
    crontab_l.txt or timers.txt."; "Ledger: 09885a47ffc71b47f7788000a8365e25f13c6417, parent
    4311b60…, no foreign rows."; "The read-out: sha256
    f1d77dc2cc11a9c7f9230846f1a5624845c1c7427d4e4438f79b4c2498ce1c0e, 108 lines."; the three
    resume files' sha256, bytes and lines as its table gives them (the architect's copies:
    the same); "U31's engine fix is not started, no row is closed, and CLAUDE.md is
    unedited."
  READ (CC's): findings 1–4.
  READ (the architect's clone at e5a844e): the two full hashes in STATUS in a straight chain
    on 0c6a7b7; the three committed resume files equal to the architect's copies by cmp;
    the plan's sha256 4b48f4ff1371b9b96b32ff780724cb5e6c400577bc9b6b92954db31710483e08,
    3555965 bytes, 21469 lines, last line "The decision log in this file ends at R828.";
    git diff --stat 30b7f16 HEAD on engine tests scripts analysis review_specs, empty;
    12aj_U31R/gate_u31r/gate_wall.txt "wall: 974.876594091 s"; row INT-l12-2's package
    cell, "P1, closing with unit U31 (INT-j12-18)"; 12ah_close/retention_close.txt's lines
    26–29 and 12ai_close/retention_close.txt's lines 26–30 as R826 quotes them.
  INFERRED: none.

R832(a) — The 12aj-U31R-R3 final report is accepted as printed: the evidence commit e5a844e
  (the retirement's read-out, the summary of I2's reads, the seven removed ids, the pins, the
  gate's nineteen files and its two side files, the three resume files equal to the
  architect's copies), ledger 09885a4. Finding 1, the inventory summary's three counts
  against a hand-scan baseline, is a reading for the tools items. Finding 2, the read-out's
  section of expected values, is accepted as written; the expected-nightly file is the
  closeout's.
R832(b) — The PI's ruling, in his words: "Wrap after this evidence commit". Session 12aj
  closes under the brief below and the wrap ruling that follows its report. U31 stands half
  landed: its Phase A read-out, its design (R828(c)–(i), (k)) and the retirement (30b7f16)
  are done; its engine fix is not started and is the next session's, after its open. Fix
  units stay 39, done 35.
R832(c) — INT-l12-2. R828(j)'s last sentence is replaced: the row closes with its unit, at the
  item 0 after U31's engine commit lands, as its own cell states, and not at this closeout.
  The route it describes is closed in the tree (R831(c)); the closure says so. The closeout
  writes no row note and no dated units line.
R832(d) — No closeout gate. The engine tree is unchanged since 30b7f16, and the gate of record
  on it is 12aj_U31R/gate_u31r/: 3,600 passed / 1 xfailed / 17 deselected on 3,618 / 3,601,
  974.88 s, launched as a unit, no hold.
R832(e) — What R826(f) left open. The closeout's retention file is written in
  12ah_close/retention_close.txt's form: the hash lines, the total line, and the probe
  scratch and stage readings as plain lines that the open check's reader does not take for a
  hash line; the tool's own output is not pasted into it. 12ai_close/retention_close.txt
  stays as committed. The reader's filter, which takes any line not beginning "#" that
  holds " MATCH", joins the tools items.
R832(f) — Carried, open with the PI and not ruled: CLAUDE.md's two lines on the retired
  backfill script; whether ~/scratch/12aj-open/between_sessions_read.txt and
  ~/scratch/12aj-u31r/'s crontab_l.txt and timers.txt are published. Carried between lanes:
  deepseek-r1:32b is not in the Evidence Engine block of dgx-infra's lane-model list
  (R826(d)).
R832(g) — This ruling states no standing rule: the id list, the glosses file and
  STANDING_RULES.md are untouched at the item 0 that transcribes it, asserted by content.
===== END r832.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12aj-CLOSE] — Close session 12aj, in 12ai-CLOSE's form read from
  the record: item 0 (R829–R832 as the next four blocks; this brief committed as pasted;
  no standing rule; no row note and no dated line), then the closeout commit (the
  closure paragraph, the "Expected values at session-12ak open" block, 12aj_close/'s
  measurements, every job run since the 2026-11-22 pair, the band computation, the
  recount, R510, retention in the 12ah form, a new expected-nightly file, the lane line
  drafted and not applied). Docs only. No engine edit. No live write. No model call. No
  closeout gate (R832(d)).
PURPOSE: Leave the record in the state the next session opens on: every fact of 12aj in
  the plan's closeout block with its evidence file beside it, the expected values for
  12ak's open stated from the 12aj-U31R gate, and the band figure computed for the wrap
  ruling.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Blocks 227–230; then the closeout block
  and the 12ak expected-values block); 12aj_open/r829.txt to r832.txt; a new directory
  12aj_close/ (the counterpart files); a new directory 12aj_CLOSE/ (brief_12aj_CLOSE.md,
  at item 0); ~/scratch/12aj-close/ (the lane line draft, the recount clone and
  scratch); the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): the form's list, read with 12aj for 12ai
  and 12ai for 12ah (so: engine/, tests/, scripts/, analysis/, review_specs/, the
  codebook, docs/architecture/, CLAUDE.md, docs/inventory/, the tools — no gate and no
  unit is launched; the three rules files, not one byte; 12ai_close/, 12aj_open/'s
  existing files and every earlier directory under docs/session-reports/, read, never
  rewritten; the dated units lines and the rows already in the plan; the lane line in
  the primers, drafted to scratch and applied only by the wrap ruling; review.db,
  mode=ro instruments only; ollama.service and the health job, read only; every entry
  under ~/scratch but ~/scratch/12aj-close/, read only, the three held files and every
  clone included, no git command inside a clone this step did not make).

PATHS: a name of the form 12xx_<name>/ is a directory of docs/session-reports/session-12/;
  a tool is named by its full path.

FORM (R784(g)(iv)): 12ai_CLOSE/brief_12ai_CLOSE.md (sha256
  a1b8218a3d80f60e81cb7845bafe56c632886974db7d02917bf1bc5b2c468a5b, 420 lines), Section
  2, is this brief's form, with the forms it names in turn, read whole before anything
  is composed, and the 12ai closeout it produced
  (dccc546155da11777916522882691e6aea9af817: the closure and the "Expected values at
  session-12aj open" block; 12ai_close/'s twenty-six closeout files) as the worked
  example. Session 12ai landed two units and moved five rows; this one landed none and
  moved none, and changed the engine tree's five paths once, by a removal. This brief
  states only what differs. Where the forms and this brief differ, this one governs;
  where a form names a 12ai source, the 12aj source listed below takes its place. A
  step of the form with no 12aj source is reported as not done, with its reason. Item
  0's model for more than one block is this session's fed257b (Blocks 224–225) with
  ~/scratch/12aj-u31a/'s block files and check.

TIMING: issued at 07:10 UTC on Monday 2026-11-23. Valid outside the day's window
  (R812(a)); the next opens at 09:00 UTC today. The clock is read before each commit.
  If it reaches 08:45 UTC before a commit, hold with the work on disk uncommitted and
  resume in this brief after 10:35 UTC; say so. A hold changes what 2b reads: the
  2026-11-23 nightly will then have run.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12aj-U31R-R3 final report, as R832 quotes it; 12aj-U31R-R2 stop report, as
    R831 quotes it): HEAD e5a844e23f43b0788e2c85fa5cdaa6854d529973, parent
    30b7f16ed676cbd204ac3a3dc11b1551e89484cf, clean, level; claude-config at
    09885a47ffc71b47f7788000a8365e25f13c6417 or an accepted descendant (R663(b)); the
    gate 12aj_U31R/gate_u31r/, 3,600 / 1 / 17 on 3,618 / 3,601, launched as a unit, no
    hold.
  READ (the architect's clone at e5a844e, 2026-11-23 07:10 UTC; re-read on the box):
    R1. The plan: sha256
        4b48f4ff1371b9b96b32ff780724cb5e6c400577bc9b6b92954db31710483e08, 3555965 bytes,
        21469 lines; last line "The decision log in this file ends at R828."; last block
        226 (its header at :21379); 183 lines begin "The decision log in this file ends
        at R". "Expected values at session-12aj open" starts at plan line 6047 and "##
        Decision log" is at 6087.
    R2. Rules files: STANDING_RULES.md
        d56dbdaeafb3659527cb77e1e81744eb01dadbe871fd96944104bb9e01b7e3ff; the id list
        a28da0611e1a4d62cbc8185ff89df22d2df7a2a37340da0eb164dec678e75257; the glosses
        7f7374b25f7efd2c1df97cfbe3fb9b828c7fec5bce4e948d751cec08a4fa72ed.
    R3. The session's commits, git log dccc546..HEAD, seven: 46339be, cd6f9a8, fed257b,
        8ae879f, 0c6a7b7, 30b7f16, e5a844e. One changes the five paths: 30b7f16, the
        retirement (scripts/ and tests/; nothing under engine/). The session opened on
        dccc546155da11777916522882691e6aea9af817, the 12ai closeout. git diff --stat
        dccc546 HEAD on docs/session-reports/tools is empty; on engine tests scripts
        analysis review_specs it reads "3 files changed, 6 insertions(+), 241
        deletions(-)"; from 977c499, "21 files changed, 1288 insertions(+), 362
        deletions(-)"; from f4a5586, "104 files changed, 7867 insertions(+), 677
        deletions(-)"; from 30b7f16 it is empty.
    R4. The session's rulings: 12ai_close/r825.txt (made at the 12ai wrap, transcribed
        at 12aj-OPEN); 12aj_open/r826.txt, r827.txt and r828.txt on disk; R829–R832 in
        Section 1. None stated a standing rule.
    R5. Six-cell "| INT-" rows in the plan: 215 at dccc546 and at HEAD. No row opened
        or closed in 12aj. No unit landed. The last dated units line is 12ai-CLNA item
        0's: fix units 39, done 35. The plan's other change since the open, beside the
        blocks: two rows of the retention ledger (:1732–:1733), at 30b7f16.
    R6. 12ai_close/recount_12ai_close.py (sha256
        6f8595520de10e9df3988f3fbad30a248bb89d1ba5a74bcf6684b35401d5d3a0) and its JSON
        (7f1de39c4b6e1f09c60e904db1c66336d9a5efdcca34f92fc5d86b775f8f0f00); no row or
        dated line has moved, so its expectation still holds and no new script is
        written.
    R7. gate_wall.txt reads "wall: 963.330798171 s" in 12ai_U41B/gate_u41b/, "wall:
        977.539204053 s" in 12ai_U30B/gate_u30b/, "wall: 1017.852609782 s" in
        12aj_open/gate_12aj_open/ and "wall: 974.876594091 s" in 12aj_U31R/gate_u31r/,
        which also holds collected_ids.txt and durations_12aj_u31r.txt.
    R8. 12ai_close/ now holds 37 files: the closeout's twenty-six, r825.txt,
        lane_line_12ai.txt and the 2026-11-21 and 2026-11-22 job reads with the NVRM
        read. It holds lane_line_12ai.txt (the lane line's form), host_state_close.txt
        and expected_nightly_20261120.md (the expected file's form).
        12ah_close/retention_close.txt (sha256
        8b6e6e749d5444ce2f045d9c7ddc74b40bee5454ac79b04fcc1003ded519e434) is the
        retention file's form (R832(e)).
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. The engine tree is unchanged since 30b7f16.
    I2. transcribe_ruling.py --append only appends.
  INFERRED — expectation (report a mismatch, continue with the measured value):
    E1. The recount by R6's script on the plan at the item-0 commit reads 289 rows (94
        / 59 / 136, 0 unclassed) and the dated lines' fix units 39, done 35, its JSON
        byte-equal to the committed one; R510 for 12aj is in 0, out 0, the six-cell
        INT- rows 215 at dccc546 and 215 at the item-0 commit.
    E2. The band computation, from the last four standard gates on this host state —
        the U41B gate 963.33, the U30B gate 977.54, the 12aj-OPEN gate 1017.85
        (confounded, R827(b)) and the U31R gate 974.88, each read from its
        gate_wall.txt — gives a mean of 983.40 s and, rounded inward, 886–1081 s. The
        series has forty-eight members.
    E3. The diff stats as R3 gives them.
    E4. Retention 23 MATCH / 0 MISMATCH by file sha256, none opened, no new entry.
    E5. The fingerprint IDENTICAL; dbchecks_close.txt, pin_tuples_close.json and the
        codebook and spec hashes byte-identical to 12ai_close/'s.
    E6. Read before 09:00 UTC on 2026-11-23, one job has run since the 2026-11-22
        pair: the 2026-11-23 health job, a light check. Read after 10:35 UTC, the
        2026-11-23 nightly too, on a HEAD whose engine tree is 30b7f16's, at the U31R
        gate's counts.
    E7. The host's four band values equal 12ai_close/host_state_close.txt's. If one
        differs, the band paragraph says so and the figure is computed and marked.

CONTRACT (the form's steps, with these 12aj terms; evidence under ~/scratch/12aj-close/):

  STEP 0 — Pre-check, as the form's: HEAD and parent proved by git cat-file -e
    <hash>^{commit}; clean; fetch; 0 0; one worktree. R1, R2, R5, R8. claude-config: the
    MEASURED commit or an accepted descendant, level; a pending foreign row carried and
    named. I1. No gate-* unit loaded. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (the model quoted before composing; the check written for this brief,
    R794(i)).
    1a. 12aj_open/r829.txt, r830.txt and r831.txt, each from the lines between the
        markers of its committed resume file as Section 1 states, one write each;
        12aj_open/r832.txt from Section 1, one write; each hashed straight after
        against Section 1's hash, with its bytes and lines. A mismatch on any = STOP
        (R738(b), R739(b)).
    1b. block227.json to block230.json in block225.json's key set. All four: made_by
        PI; made_date 2026-11-23; session 12aj; item "item 0 of the 12aj-CLOSE brief";
        sentence model; transcribed = the commit's UTC date; each report as its STATUS
        names it, the hashes in backticks, in the form of the record's nearest block
        for a stop report or a final report (Blocks 224 and 225, quoted). Followed:
        R829 "12aj-U31R-R1 resume"; R830 "12aj-U31R-R2 resume"; R831 "12aj-U31R-R3
        resume"; R832 "12aj-CLOSE brief". prev_ends R828, R829, R830, R831 in turn.
        Each "beside" composed by CC in Block 224's and 225's words, naming the other
        three blocks and the brief file. Quote the composed values. Render all four,
        compare the element order with the models', then --append in order.
    1c. No row note and no dated units line (R832(c)).
    1d. The brief file (R784(g)(v)): 12aj_CLOSE/brief_12aj_CLOSE.md from this whole
        pasted block, "CLAUDE CODE" through the STOP-12AJ-CLOSE line, one trailing
        newline, written once; sha256, bytes and lines straight after the write,
        reported.
    1e. The item-0 check, as the form's 1e with these terms: the last line "…ends at
        R832." once; the plan equals HEAD's plus Blocks 227–230 appended, nothing else
        (the "−" count read and matched to zero); six-cell INT- rows 215 before and
        after; the four ruling files equal to 1a's; the three rules files at R2's
        hashes and unstaged; the staged set exactly the plan, the four ruling files
        and the brief file.
    1f. <check> && <commit>, one line; then fingerprint, push, level and the ledger row
        as the form's 1f; the ledger message built from the diff.

  STEP 2 — The closeout commit, every element in the form's order, each with its file
    under 12aj_close/ in 12ai_close/'s counterpart name; a name with no 12aj source is
    listed as not written, with the reason.
    2a. Measurements as the form's 2a, dated names carrying the read's UTC date.
        Differences: the diff files are diff_engine_since_dccc546.txt,
        diff_since_977c499.txt and diff_since_f4a5586.txt (E3); no tools diff file is
        written, the tools being unchanged (R3), and the closure says so;
        collected_ids_12aj_close.txt is a copy of
        12aj_U31R/gate_u31r/collected_ids.txt and durations_12aj_u31r.txt a copy of
        that gate's durations file, each cmp equal to its source, the ids also set
        against 12ai_close/collected_ids_12ai_close.txt with the only-ref and only-gate
        ids listed, read from the comparison and not from this line; dbchecks, pins
        and the codebook and spec hashes compared with 12ai_close/'s (E5); the host
        state with E7 and the form's readings. retention_close.txt in
        12ah_close/retention_close.txt's form (R832(e)): the hash lines, the total
        line, then the probe-scratch and stage readings as plain lines; before it is
        staged, the open check's retention reader's rule is applied to it by a
        helper, and the lines it would take are counted: EXPECTED twenty-three, three
        tokens each, and no other. Another count = STOP before the closeout commit.
    2b. The jobs (E6): every health job and nightly run after the 2026-11-22 pair and
        before this read, read as 12aj-OPEN's step 1 read 2026-11-21, their files into
        12aj_close/ in those names, a light check's file saying which form ran. A
        nightly found is read against 12aj_U31R/gate_u31r/ (counts and ids), its HEAD
        found by the reflog method. 12ai_close/expected_nightly_20261120.md no longer
        governs, the tree having changed at 30b7f16; the jobs paragraph says so, and
        which file governs from here (2e). A nightly that does not match its tree's
        gate = STOP before the closeout commit, both values reported.
    2c. The recount (E1), by R6's script as 12aj-OPEN's step 3c ran it, in a scratch
        clone made under ~/scratch/12aj-close/ on the plan at the item-0 commit; no new
        script. r510_12aj.txt in r510_12ai.txt's form, computed at dccc546 and at the
        item-0 commit. A row count other than 289, dated lines other than 39 / 35, a
        JSON that is not byte-equal, or R510 other than in 0, out 0 = STOP before the
        closeout commit, both values reported.
    2d. band_computation.txt in 12ai's form: the four inputs of E2, each with its
        evidence path, its gate_wall.txt value, how it was launched (a user unit, all
        four) and the concurrent jobs from its snapshots, the 12aj-OPEN gate marked
        confounded with its confounder as R827(b) names it; the test counts each ran
        (3,594, 3,608, 3,608 and 3,601 selected) beside them as a reading, each read
        from its gate; mean, ×0.9, ×1.1, the inward rounding, the figure; the series
        of forty-eight listed — 12ai's forty-six plus 1017.85 and 974.88, each read
        from its file; "a figure, to be set by the wrap ruling; the band in force
        stays 881–1076 s".
    2e. A new expected-nightly file, 12aj_close/expected_nightly_<the read's UTC
        date>.md, in 12ai_close/expected_nightly_20261120.md's form, from the U31R
        gate: it governs each nightly on a tree equal to 30b7f16's (R749(b)), the
        first being 2026-11-23's.
    2f. The lane line, ~/scratch/12aj-close/lane_line_12aj.txt, in
        12ai_close/lane_line_12ai.txt's form: its five "−" lines are
        ~/claude-config/primer.md's lines 41–45 as they stand, taken from primer.md
        itself; its five "+" lines carry 12aj's sources — HEAD the closeout commit
        (filled after 2h's commit, so the file is written last, is not in the commit,
        and is pasted whole in the report), the plan's sha256, bytes and lines, the
        decision log ending R832 (R833 at 12ak-OPEN's item 0), and the session's
        summary: no fix unit landed; U31's Phase A read-out and its design ruled
        (R828); the two cloud-span repair scripts retired (30b7f16), U31's engine fix
        next; fix units 39, done 35; rows 289; no standing rule; four stops, no pause;
        two gates and the band figure; pins unchanged; no live write; three files held
        in scratch, their publication the PI's; the next steps — 12ak-OPEN, then
        U31's build, then U32–U36. Each "+" line read whole after writing, every word
        separated from its neighbour and its code marks paired. Not applied.
    2g. The closeout block and "Expected values at session-12ak open", placed after
        the 12aj expected-values block's last line and before the decision-log section
        (the position read and matched, R1), in the 12ai block's element order with
        12aj's content:
        — the closure paragraph (opened on dccc546; this CC session's id and times
          read from the record; no interim session);
        — the briefs in order — 12aj-OPEN and its resume R1, 12aj-U31A, 12aj-U31R and
          its resumes R1, R2 and R3, this closeout — each with the ruling at its item
          0 or carried in it and its committed file's sha256 as the rulings record its
          match with the architect's copy;
        — the commits in order from git log dccc546..HEAD with their ledger commits;
          the diff lists;
        — the rulings R825–R832 one line each (R825 made at the 12ai wrap and
          transcribed at 12aj-OPEN);
        — the units: none landed. U31: its Phase A read-out (8ae879f), its design
          (R828(c)–(i), (k)), its retirement half landed at 30b7f16 with its gate; its
          engine fix not started;
        — the rows: none opened, none closed. INT-l12-2's route closed in the tree at
          30b7f16, the row closing with U31 (R832(c)); the row for R828(h)'s finding
          composed at the build's item 0;
        — the stops, listed from the rulings (four: R826(a), R829(a), R830(a),
          R831(a), each with whose it was as its ruling states), the pauses (none),
          and the architect's errors as the rulings count them (four, R831(a)), with
          what each was in the rulings' words;
        — the tools paragraph (no tools change; no regeneration of the standing-rules
          file; the nine tools items carried unchanged, and beside them: the open
          check's retention reader's filter, which joins them (R832(e)); the
          inventory summary's hand-scan baseline counts (R832(a)); the brief's
          exception for the open check's health section, not used and untested
          (R827(c)));
        — one paragraph for the open: the stop at 3b, its cause in
          12ai_close/retention_close.txt's form, R826's second config, the open check
          then whole; the first sweep written by the light/full script, read; the
          gate confounded by another lane's run inside its window (R827(b));
        — one for U31's Phase A and design: what the read-out found, in its own
          blocking answer's words; the PI's word "package" and the eight clauses;
          R800(c) answered and left (R828(i)); what was not established (its §9);
        — one for the retirement: R828(j), the three stops and their rulings, the
          seven ids and how they divide (four by the files, three by the list entry),
          the commit's seven paths, both scripts recoverable at 0c6a7b7, the two
          ledger rows, the gate; CLAUDE.md's two lines left for the PI's word; of
          R807(d)'s three items the second gone, the first and third open;
        — one for what is held and not published: the three scratch files with their
          sha256, bytes and lines as the reports give them, and why (R826(e),
          R829(d)); and what is owed between lanes (R832(f));
        — "Carried forward": U31's build under R828, with INT-l12-2's close and the
          row of R828(h) at its item 0; then U32–U36; INT-ai12-2; R819(k)'s read-out;
          the tools items; R807(d)'s first and third items; CLAUDE.md's two lines; the
          held files; the items the 12ai closure carried that 12aj did not touch,
          read from that closure and listed; the cross-lane readings for dgx-infra
          (the eighteen loads of 2026-11-22, the run inside the gate's window, the
          temp root's rotation);
        — the gates table (two: 12aj-OPEN, U31R); the band paragraph; the jobs
          paragraph (2b, 2e); the recount paragraph; retention; ledger; counterpart
          files; the host; live; "Session 12aj closed. Next: session 12ak, fresh.";
        — then the 12ak expected-values block in the 12aj block's form, with these
          differences: the references are collected_ids_12aj_close.txt (3,601 ids)
          and durations_12aj_u31r.txt; the gate's counts 3,600 / 1 / 17 on 3,618 /
          3,601; the engine tree unchanged since 30b7f16; the band figure of 2d; the
          nightlies per 2e's file, with whatever 2b read; the health jobs as readings
          in their two forms, the next Sunday named from the calendar on the box, the
          count of NV_ERR_NO_MEMORY lines this boot as 2a reads it; Ollama's service
          start, MainPID and keep-alive as readings from 2a; "The decision log in
          this file ends at R832" and R833, the wrap ruling, at 12ak-OPEN's item 0;
          the rules files' three hashes (R2); standing rules stated in 12aj: none;
          the recount by 12ai_close/recount_12ai_close.py (289; 39 done 35); the
          retention file in the form the open check reads (R832(e)); the pins file;
          the diff list of R3; this session's scratch directories, each named from
          ~/scratch's listing (the 12aj-* entries), the clones under them named and
          left as they are beside 12ai's five; the three held files named with their
          hashes; 12ak's first items — 12ak-OPEN, then U31's build.
        Every figure from a file in 12aj_close/ or an evidence directory, named beside
        it; nothing from this brief's text where a file exists.
    2h. The closeout check and commit as the form's 2h, with these terms: the last
        "ends at" line "…ends at R832." once; the staged set exactly the plan and
        12aj_close/; the rules files, 12ai_close/ and every earlier directory
        unchanged; the message in dccc546's form. Then 2f's lane line completed and
        pasted whole in the report.

ACCEPTANCE GATES: the form's G1–G3 with these terms — I1 held; item 0 one commit that
  passed 1e in one chain with the four ruling files at their hashes and Blocks 227–230
  by the tool; the closeout commit with every element of the 12ai block present in
  order with its 12aj source, every counterpart file written or listed as not written
  with its reason, the recount at E1 or stopped, the retention file passing the
  reader's rule at twenty-three or stopped, any nightly found matched to its tree's
  gate or stopped, the band computed from the files, the new expected-nightly file
  written, E3–E7 held or reported, the check passed in one chain; both pushed, level,
  fingerprint IDENTICAL; ledgers; the lane line pasted whole, not applied.

OUT OF SCOPE: the wrap ruling (R833: the band set, the lane line approved and applied,
  the stops counted — after this report); U31's build; closing INT-l12-2; any engine,
  test, script, docs/architecture, CLAUDE.md or tools edit; any gate or unit; removing
  or publishing anything from ~/scratch; the tools items; /wrap, /exit and the archive
  (after the wrap ruling).

CONSTRAINTS (task-only): no model call, no provider call, no live write, no sudo; a count
  arrived at by eye is INFERRED, the computed value given with its command; every figure
  in the block beside its file; a check that reads a diff or a file line by line is
  shown what it does with an empty line and with a header line before it is run; where
  this brief or its form describes a tool or record, the record governs and a
  difference is a finding (R732(a)).

FINAL REPORT FORMAT: the form's — (0) pre-check table; (1) item 0; (2) the closeout: the
  measurements with their values, each job read with the gate it was set against, the
  recount's output, the retention file's reader count, band_computation.txt verbatim,
  the new expected-nightly file verbatim, the check verbatim, chain, hashes,
  fingerprint, ledgers, the plan's sha256, bytes and lines, the two blocks' line ranges
  in the plan (the blocks themselves need not be pasted, being committed), the lane
  line verbatim with its sha256, bytes and lines and its fifth "+" line quoted whole;
  (3) every place this brief's FORM clause left a choice, and the choice made; (4)
  contradictions and findings, numbered, each with its quoted line; (5) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 or I2 false, a 1a mismatch, or a check
  failure or crash at 1e = STOP before the item-0 commit. A recount or R510 off E1, a
  retention file the reader's rule does not read as twenty-three, a nightly that does
  not match its tree's gate, or a check failure at 2h = STOP before the closeout
  commit, the work on disk reported. An expectation mismatch (E2–E7) is reported and
  the closeout continues with the measured value in the block. A spec premise that does
  not survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not apply the lane line, do not run /wrap, do not start
  12ak or U31's build. Await the wrap ruling in this session.
  STOP-12AJ-CLOSE
