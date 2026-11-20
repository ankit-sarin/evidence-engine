CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R821, PI ruling 2026-11-20 UTC; not on disk)
================================================================================

R821 is transcribed at item 0 of the brief below, by
docs/session-reports/tools/transcribe_ruling.py, as the next block of the decision log
(Block 219). The lines between the two marker lines, exclusive of the markers, are the
byte content of r821.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's
independent hash:
  r821.txt: sha256 ce0f18c08ece79d8ff1a2cb4536260be80b33a86804ae7462a75bafe16d4c2e5,
            6976 bytes, 87 lines, one trailing newline.

===== BEGIN r821.txt =====
RULING [evidence-engine 12ai-R821] — 12ai-U30B accepted; U30 landed with N46; INT-i12-71 and
  INT-j12-51 close at the next item 0; the gate's 977.54 s enters the series; the dgx-infra
  lane's removal of the sudoers rule recorded; the clone check before any removal; readings
  on the findings; no standing rule
STATUS: PI ruling, 2026-11-20 UTC, session 12ai, on architect recommendation, on the 12ai-U30B
  final report (item 0 b998dfb33b8c2c804dd9597bcd76c002d4596eea; engine
  7fad72aa42ba1239911f95d91e186269b0fe2f7c; evidence
  d7a0bf7f37319fd6315a1249f4ac2afb16b078f9). Transcribe at item 0 of the 12ai-CLNA brief
  below, by transcribe_ruling.py, in the block after R820's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12ai-U30B final report, quoted; wrapped lines joined): "Commit:
    b998dfb33b8c2c804dd9597bcd76c002d4596eea (parent 03abfaf), +644 −2."; "Ledger: b670557.";
    "r820.txt: 1b1ee4a4…76ab, 6,577 B, 82 lines. Equal to the architect's hash."; "Brief file:
    b36dbcb1b13c6cc3bf54b79804575815c15da5fcd0a86170afbcf2cb548b9c0a, 34,970 B, 465 lines."
    (the architect's copy: the same); "7fad72aa42ba1239911f95d91e186269b0fe2f7c (parent
    b998dfb33b8c2c804dd9597bcd76c002d4596eea)"; "On the parent, 10 are red and 2 green (cases
    7 and 8), which is E1."; "After the edits all 12 pass."; "pin_tuples_u30b.json is
    byte-identical to pin_tuples_u41b.json"; "3607 passed, 17 deselected, 1 xfailed"; "wall
    977.5 s, inside 897–1095"; "collected 3,625, selected 3,608 = 3,594 + 14"; "NRestarts=0.
    No load, no restart."; "Commit: d7a0bf7f37319fd6315a1249f4ac2afb16b078f9 (parent 7fad72a),
    26 files, +13,277"; "Ledgers: e89d13b for the engine commit and 29c54c9 for the evidence
    commit, no foreign rows."; "P1's fix units 39, done 34 (U01–U29, U37–U41). Rows 289 (94 /
    59 / 136, 0 unclassed)".
  READ (CC's): findings 1–5; I2–I5 as the report gives them; the CLOSED form's model,
    "INT-i12-31's at 5e38485".
  READ (the architect's clone at d7a0bf7): the three hashes in STATUS in a straight chain on
    03abfaf; 12ai_U30B/brief_12ai_U30B.md and 12ai_open/r820.txt equal to the architect's
    copies by cmp; the plan's last line "The decision log in this file ends at R820.";
    12ai_U30B/gate_u30b/gate_wall.txt "wall: 977.539204053 s"; the engine commit's diff of
    engine/cloud/base.py and scripts/run_cloud_extraction.py, read whole against R805(d) and
    R807(b); CLAUDE.md:156–159 as the brief below quotes them.
  READ (the PI's relay of the dgx-infra lane's notice, 2026-11-20; that lane's statements,
    not this lane's measurement): "/etc/sudoers.d/ollama-restart was deleted and committed
    in etckeeper at b804e5d on 20 November 2026"; "Watchdog: not built".
  INFERRED: none that a clause below rests on.

R821(a) — The 12ai-U30B final report is accepted as printed. Item 0 b998dfb (Block 218;
  INT-ai12-1 CLOSED with its note; INT-i12-71's notes; r820.txt and the brief file equal to
  the architect's copies), ledger b670557. The engine commit 7fad72a, as R805(d)(1)–(4) and
  R807(b) rule it, read by the architect: run_arm rolls back and closes interrupted with
  rm.interrupt_reason only a run not already closed, and does the same with failed on any
  other exit; main runs the arms under rm.interrupt_signals and returns 128 plus the
  signal's number for exit_process; send records a provider call an interrupt ends and
  re-raises; store_result rolls back on any exit that did not commit; the spans are reduced
  by drop_unexpected after the completeness check and before the citation check. Ledger
  e89d13b. The evidence commit d7a0bf7, ledger 29c54c9. I1–I5 held; E1, E2 and E4–E6 held;
  E3 is two over ((d)). U30 has landed with N46: P1's fix units 39, done 35.
R821(b) — The gate's 977.54 s is the forty-sixth member of the series: a standard gate
  launched as a unit, no hold. The band stays 897–1095 s until the closeout cuts the next.
R821(c) — INT-i12-71 closes at the item 0 that transcribes this ruling, in the record's CLOSED
  form, on 7fad72a. INT-j12-51 closes there on the same commit, with N46 noted as the same
  finding, as R807(c) ordered in the PI's words; its class and package are read from the
  row, and the count's effect is the recount's to state. Left open and not these rows':
  R807(d)'s three items.
R821(d) — Readings, no row. Finding 1 is the architect's: case 7 said an Exception from the
  store closes the run failed, and through the transports' loop it never reaches run_arm;
  CC drove the case from the run's body and it holds there. A statement about a code path
  no one had read the path for. Finding 2: an interrupt with no message is recorded as
  "KeyboardInterrupt: ", send's existing form applied to an empty message. Finding 3: the
  gate's counts are two over E3, from one parametrized test that counts
  run_cloud_extraction.py's imports of run_manifest; an expectation, and no red. Finding
  4 is the cost R805(d)(2) accepted: a call's row not yet committed when the interrupt
  lands is rolled back with the rest. Finding 5: imports the named edits need. One more
  from the architect's read: on the path that returns, run_arm closes completed without a
  rollback first; nothing is expected open there.
R821(e) — The dgx-infra lane's notice, relayed by the PI: the passwordless grant for
  restarting Ollama is removed from this host on 2026-11-20, by that lane, on the PI's
  ruling there; no watchdog is built; R819(k)'s read-out stays where it is ruled. From
  now a run that asks for a restart on this host has it refused, and behaves as
  ollama_client.py and extractor.py at 339370d handle a failed command: the call ends
  restart_command_failed, or the proactive failure is logged and tried again after
  restart_every more papers. CLAUDE.md's lines that say the grant exists are corrected
  at the item 0 that transcribes this ruling, by the text the brief carries.
R821(f) — The scratch clones (R817(f)). First a read-only check, the brief below: every
  clone set against the committed inventory 12ai_SCRI/, every path a session step or a
  retention record reads set against the clones, and a removal list. Nothing is removed
  by that brief. The list is committed and put to the PI, and a removal brief follows only
  on his approval of the list in words. Not proposed for removal: the clones this session
  made, which stay until it closes; the two pytest fixture repositories, which are
  records and not clones.
R821(g) — The count so far in 12ai: no stop, no pause; nine architect's errors (R820(g)'s
  eight; finding 1), one reaching a commit.
R821(h) — This ruling states no standing rule: the id list, the glosses file and
  STANDING_RULES.md are untouched at the item 0 that transcribes it, asserted by content.
  Next: the clone check (the brief below); the PI's word on its list; the removal brief;
  the report to the workbench of what was removed; then the closeout.
===== END r821.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ai-CLNA] — the scratch clones, checked before any removal: item 0
  (R821 as the next block; INT-i12-71 and INT-j12-51 CLOSED; the dated units line;
  CLAUDE.md's four lines on the sudoers grant; this brief committed as pasted), then,
  read-only, every clone of evidence-engine under ~/scratch set against the committed
  inventory and against every path the session's steps read, and a removal list; the
  read-out committed. Nothing is removed.
PURPOSE: Give the PI a removal list he can approve in words, each line of it resting on a
  measurement of today. Mixed: one docs commit, reads, one evidence commit. No engine,
  test, script, spec, codebook, tools or rules-file edit. No live write. No model call.
  No gate. No deletion, move or clean of anything.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 219; two rows closed; one dated
  line); CLAUDE.md (lines 156–159 only); 12ai_open/r821.txt; a new directory 12ai_CLNA/;
  ~/scratch/12ai-clna/ (new); the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): every clone and every other entry under
  ~/scratch except ~/scratch/12ai-clna/ (read only; no rm, mv, git clean, git gc, git
  stash, git checkout, git fetch or git status without GIT_OPTIONAL_LOCKS=0 inside any
  of them; no file created under any of them); every git command of step 2 runs with
  GIT_OPTIONAL_LOCKS=0; the main repository's .git (read only in step 2); every file
  already under docs/session-reports/; the three rules files; any other line of
  CLAUDE.md; review.db and every other database (none is opened: a database is named by
  path only); ollama.service. No sudo. No script found in scratch is run.

PATHS: a name of the form 12xx_<name>/ is a directory of docs/session-reports/session-12/;
  a tool is named by its full path. INV is 12ai_SCRI/, the committed inventory of
  2026-11-19. OUT is ~/scratch/12ai-clna/.

FORM: item 0's model is b998dfb33b8c2c804dd9597bcd76c002d4596eea (12ai-U30B's item 0: a
  block, a row CLOSED with a note, a dated units line, the brief file), with its CLOSED
  form's own model as that item 0 found it. Step 2's method follows INV's 12ai_SCRI.md,
  sections S3 and "Gates" (G1 and G4), read whole before step 2; INV committed no
  script, so the helpers are written for this brief and committed with the read-out.
  Step 3's model is d7a0bf7. Quote a model before composing its counterpart; where this
  brief disagrees with a tool or the record, the tool and the record govern and the
  difference is a finding (R732(a)).

TIMING: issued at 22:06 UTC on Friday 2026-11-20. Valid outside the day's window
  (R812(a)); the next opens at 09:00 UTC on Saturday 2026-11-21. If the clock at step 0
  is inside a window or within 15 minutes before one opens, STOP at once and write
  nothing. If the clock reaches 15 minutes before an opening with a commit pending, hold
  with the work on disk uncommitted and resume after 10:35 UTC.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ai-U30B final report, as R821 quotes it): HEAD
    d7a0bf7f37319fd6315a1249f4ac2afb16b078f9, parent
    7fad72aa42ba1239911f95d91e186269b0fe2f7c, clean, level; claude-config at 29c54c9 or
    an accepted descendant (R663(b)); fix units 39, done 34 before this item 0.
  READ (the architect's clone at d7a0bf7; re-read on the box):
    R1. docs/plan/ENGINE_REFACTOR_PLAN.md: sha256
        4fd4e39c13dadf79fde6e0bb13719f8490b0b58ca57dbc5d31ddeb42d849fa2b, 3,439,499 B,
        20,585 lines; ends "…ends at R820."; last block 218; 175 "ends at" lines;
        six-cell INT- rows 215; row INT-i12-71 at :600 and row INT-j12-51 at :685.
    R2. CLAUDE.md:156–159, quoted in step 1e.
    R3. INV/s3_clones.tsv: a header line and 55 rows; its columns "repo, origin, head,
        branch, reachable_from_origin_main, unpushed_vs_own_remotes,
        commits_not_in_main_origin, stash, status_lines, modified_or_staged, untracked,
        ignored, diff_files, diff_plus, diff_minus"; its first row "12aa-close/clone"
        with origin "/home/ankitsarin/projects/evidence-engine/.". INV/clone_status/
        holds 55 files, INV/patches/ 17, INV/untracked/ one file.
    R4. INV/12ai_SCRI.md, S3: "53 clones of `~/projects/evidence-engine`" and "2
        pytest-fixture repos"; "Clean 24, non-clean 29"; "17 have a non-empty `git diff
        HEAD`"; "28 files; 27 are byte-identical to a file in the tree at HEAD"; its
        finding 3, "Clone objects are hardlinked to the main repository's
        `.git/objects`". Its S2: the CONSUMER paths, each read through a config file by
        docs/session-reports/tools/session_open_check.py.
    R5. INV/s2_citations.tsv: two header lines; columns "source_file:line, cited_path,
        exists_now, label_reading, ledger_row, reader".
    R6. No 12ai_CLNA/ exists; 12ai_open/ holds no r821.txt.
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. The main repository borrows no object from a scratch clone: its
        .git/objects/info/alternates is absent or names no path under ~/scratch, and
        git worktree list shows one worktree. If it borrows from one, STOP: removing
        that clone would damage the main repository.
    I2. Step 2 writes nothing under ~/scratch but OUT: every clone's .git/index mtime
        in nanoseconds, and every in-scope entry's newest mtime, are equal before step
        2 and after it (INV's G1 method).
  INFERRED — expectation (report a mismatch, continue):
    E1. The 53 clones of INV are all present, each at INV's head, with INV's status.
    E2. The clones not in INV are this session's and the 12ah close's and open's: each
        named, with the step that made it.
    E3. No path a session step, a retention record or the plan's expected-values block
        reads lies inside a clone.
    E4. No process has its working directory or an open file inside a clone.
    E5. Inside the clones, every file git does not track is a cache, an environment
        link, a copy of a committed file, or the one file INV/untracked/ holds.

CONTRACT (steps, in order; every output under OUT):

  STEP 0 — Pre-check (read-only). date -u. HEAD and parent proved by git cat-file -e
    <hash>^{commit}; clean; fetch; 0 0; one worktree. R1, R2, R6; the three rules files
    at the 12ai-OPEN report's hashes. claude-config level; a pending foreign row
    carried and named. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; the check written for
    this brief, R794(i)).
    1a. 12ai_open/r821.txt from Section 1, one write, hashed straight after (R738(b))
        against Section 1's hash. Mismatch = STOP (R739(b)).
    1b. block219.json in block218.json's key set: tag R821; transcribed = the commit's
        UTC date (R775(a)); item "item 0 of the 12ai-CLNA brief"; made_by PI; made_date
        2026-11-20; session 12ai; report as R821's STATUS names it, in Block 218's form
        for a report with three hashes; sentence model, followed "12ai-CLNA brief";
        prev_ends R820; "beside" composed by CC from Block 218's words for what this
        commit carries. Render to stdout, compare the element order with Block 218's,
        then --append.
    1c. INT-i12-71 and INT-j12-51 (R821(c)), each closed in the model's CLOSED form on
        7fad72aa42ba1239911f95d91e186269b0fe2f7c, tagged with the commit's UTC date and
        "(12ai-CLNA item 0, R821(c))", with one sentence of the architect's each:
        ----- BEGIN on INT-i12-71 -----
        U30 landed at `7fad72a`: an interrupted cloud run rolls back and closes `interrupted` with its signal's reason; the store rolls back on any exit that did not commit; `send` records a provider call an interrupt ends; the cloud entry point handles SIGTERM and SIGHUP (R805(d)(1)–(4)). The first half was closed as answered (R805(d)(5)).
        ----- END on INT-i12-71 -----
        ----- BEGIN on INT-j12-51 -----
        Closed on U30's build (`7fad72a`), with N46 noted as the same finding (R807(c)): the cloud store drops a field the codebook did not ask for before any write, by `drop_unexpected`, and `extracted_data` stays the response as received (R807(b)). Left open and not this row's: whether live holds such a row, the two repair scripts' insert path, and the three analysis readers that count every row (R807(d)).
        ----- END on INT-j12-51 -----
        Each row's class and package are read from the row and not changed.
    1d. The dated units line, in b998dfb's line's form, tagged "(12ai-CLNA item 0,
        R821(a))": U30 landed with N46 at 7fad72a; INT-i12-71 and INT-j12-51 CLOSED on
        it; P1's fix units 39, done 35 (U01–U30, U37–U41); the row counts computed from
        the plan after 1c by the method the model's line used.
    1e. CLAUDE.md lines 156–159 (R821(e)), which must read exactly as between the first
        pair of markers, replaced by the text between the second pair; a mismatch = STOP
        before the commit.
          ----- BEGIN CLAUDE.md:156–159 as it stands -----
          Two independent code paths can run `sudo -n systemctl restart ollama`, and a
          `NOPASSWD` sudoers rule for exactly that command means both can fire. Both are off
          by default and run only for a run that asks (U41, R819(c)–(e)); both are gated, and
          the suite is fenced. Do not add a third ungated path.
          ----- END -----
          ----- BEGIN CLAUDE.md:156–159 replacement -----
          Two independent code paths can run `sudo -n systemctl restart ollama`. Both are off
          by default and run only for a run that asks (U41, R819(c)–(e)); both are gated, and
          the suite is fenced. On the DGX the passwordless grant for that command was removed
          on 2026-11-20 by the dgx-infra lane (R821(e)), so there a run that asks has its
          restart refused: the call ends `restart_command_failed`, or the proactive failure
          is logged and tried again later. Do not add a third ungated path.
          ----- END -----
          (The markers and the ten-space indent are this brief's; the file's lines begin
          at their first word.)
    1f. The brief file (R784(g)(v)): 12ai_CLNA/brief_12ai_CLNA.md from this whole pasted
        block, "CLAUDE CODE" through the STOP-12AI-CLNA line, one trailing newline,
        written once; sha256, bytes and lines straight after the write, reported.
    1g. The item-0 check, as b998dfb's, with: --check-plan byte-identical for Block
        219; the plan's numstat read, its "−" matched to the two rows 1c replaces and
        to nothing else; every earlier "ends at" line kept (175 before); the last line
        "…ends at R821." once; the plan equal to HEAD's plus those rows replaced, the
        dated line and Block 219, nothing else (computed); CLAUDE.md equal to HEAD's
        but for lines 156–159, its numstat read and reported; r821.txt equal to 1a's;
        the three rules files unchanged and unstaged (R777(b)); the staged set exactly
        the plan, CLAUDE.md, r821.txt and the brief file. Quote the check.
    1h. <check> && <commit>, one line; the message from the staged set, docs scope.
        Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers (R801(f)); push; level.

  STEP 2 — The clone check (read-only; helpers under OUT/helpers/, python3 -I -B; every
    git command with GIT_OPTIONAL_LOCKS=0). Its read-out is OUT/12ai_CLNA.md with one
    TSV per table beside it. In scope: the ~/scratch entries named 12*, retained, wr5-b,
    10b-close and e_exec_probe (INV's rule).
    2a. I2's baseline, then I1.
    2b. The repositories now: every directory in scope that holds a .git, found
        without following a symbolic link. For each: its path relative to ~/scratch,
        its origin URL, HEAD, branch. Classed by a stated rule and the rule quoted:
        a clone of evidence-engine (its origin is the main repository's path or its
        GitHub remote); a fixture repository (no origin); anything else, named.
    2c. Against INV/s3_clones.tsv: the rows of INV not found now; the repositories now
        not in INV, each with its birth time and the step or session that made it
        (E2).
    2d. For each clone that is in INV, measured now and set beside INV's row: HEAD;
        whether HEAD is reachable from the main repository's origin/main; the count of
        commits on any of its branches not in origin/main; the stash count; its
        status --porcelain --ignored against its file in INV/clone_status/ (how a
        clone maps to a file name is read from the files and stated), byte for byte;
        for a clone with a file in INV/patches/, git diff HEAD against that patch by
        INV's G4 method (git apply --numstat of the patch equal to git diff HEAD
        --numstat now). One verdict each: UNCHANGED, or CHANGED with the differing
        value.
    2e. For every clone, in INV or not: every path git reports untracked or ignored
        that is not under __pycache__, .pytest_cache or a .venv link, each with its
        bytes, its git blob id, and whether that id is in git ls-tree -r of the main
        repository's HEAD or of any commit on origin/main (INV's S4 method; a 0-byte
        file is reported as such and never counted as a match) or equals INV's
        untracked copy by sha256 (E5); every symbolic link inside the clone whose
        target is outside it; every commit, stash or changed tracked file whose
        content is in no commit of origin/main and no file of INV/patches/.
    2f. The readers (E3). The set of scratch paths that something reads: every
        cited_path in INV/s2_citations.tsv whose label is CONSUMER or LEDGER; every
        scratch path in ~/scratch/12ai-open/expected_12ai.json; every path in
        12ah_close/retention_close.txt resolved as session_open_check.py resolves it;
        every scratch path in the plan's "Expected values at session-12ai open" block;
        every scratch path the closeout's form, 12ah_CLOSE/brief_12ah_CLOSE.md, names.
        For each, whether it lies at or under a clone's top directory. A hit is
        listed with its source line.
    2g. In use (E4): from /proc, every process of this user whose working directory or
        an open file is at or under a clone, with its pid and command.
    2h. Sizes, per clone: apparent bytes; bytes in files whose link count is 1 (what a
        removal frees); bytes in files linked more than once. Totals.
    2i. The removal list, OUT/removal_list.tsv, one row per clone of 2b: its top
        directory (the directory that holds .git, absolute), HEAD, the three byte
        figures, a verdict and its reason. REMOVE only if all hold: it is in INV and
        UNCHANGED under 2d; 2e finds nothing in it whose content is in no commit and
        no file of INV; no path of 2f lies in it; nothing of 2g uses it; it is not
        under an entry named 12ai-*. Otherwise KEEP, the reason named. A fixture
        repository is not a row. For each REMOVE row, what else its parent entry
        holds beside the clone's directory, by count and bytes, as a check that the
        row names the clone alone.
    2j. I2 again, compared with 2a's baseline. A difference = STOP before step 3, the
        differing path reported.

  STEP 3 — The read-out commit (R731(c)). 12ai_CLNA/: 12ai_CLNA.md, its tables,
    removal_list.tsv and the helpers; the staged set exactly those, every sha256 equal
    to its scratch source; chained check && commit; fingerprint IDENTICAL before and
    after the push; level; ledger.

ACCEPTANCE GATES:
  G1. Step 0 matched; every full hash proved.
  G2. Item 0 one commit that passed 1g in one chain: Block 219; two rows closed; the
      dated line; CLAUDE.md changed in lines 156–159 only; pushed, level, fingerprint
      IDENTICAL; ledger.
  G3. Step 2: I1 and I2 held; every clone of 2b has one row in the list with a verdict
      and a reason; every count in the read-out produced by a helper whose path and
      sha256 the report gives; nothing under ~/scratch but OUT written, removed or
      moved.
  G4. The read-out commit as step 3 states.

OUT OF SCOPE: removing, moving or cleaning anything (a brief of its own, after the PI's
  word on the list); any record, log, database, test database or pytest temp directory,
  inside a clone's entry or not, except as counted in 2i's last check; the two fixture
  repositories; other lanes' scratch entries and home folders (not read); the retention
  ledger and the retention files (read, not edited); INT-ai12-2; U31; the tools items;
  the closeout; /wrap.

CONSTRAINTS (task-only): .venv/bin/python for repository scripts; OUT is the one place a
  helper may write to or delete from (R658(c)). A count CC arrives at by reading rather
  than by a helper is marked "CC's reading". A verdict is computed by 2i's rule from the
  tables, never chosen by eye.

FINAL REPORT FORMAT: (0) the pre-check. (1) Item 0: r821.txt's hash; block219.json with
  its model quoted; the two rows as closed; the dated line; CLAUDE.md's four lines
  before and after; the brief file's sha256, bytes and lines; the check; hashes,
  numstats, fingerprint, ledger. (2) I1 and I2 with their reads; 2b's counts by class;
  2c's two lists; 2d's verdict counts and every CHANGED row whole; 2e's, 2f's and 2g's
  findings whole, or "none" with the count searched; 2h's totals. (3) The removal list
  printed whole, one line per clone: top directory, HEAD (short), bytes freed, verdict,
  reason; then the totals: clones to remove, clones kept, bytes freed. (4) The read-out
  commit. Then findings, numbered, each with its quoted line, including anything in
  R1–R6 or E1–E5 the box contradicts. Then STOP.

CONTRADICTION HANDLING: a step-0 mismatch, a 1a mismatch, a 1e mismatch, or a check
  failure or crash at 1g = STOP before the item-0 commit. I1 false = STOP before the
  rest of step 2. I2 false at 2j = STOP before step 3. An expectation mismatch (E1–E5)
  is reported, turns the affected rows to KEEP by 2i's rule, and the work continues. A
  spec premise that does not survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Remove nothing. Do not start the closeout or U31. Await the
  next brief or ruling in this session. STOP-12AI-CLNA
