CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R823, PI ruling 2026-11-20 UTC; not on disk)
================================================================================

R823 is transcribed at item 0 of the brief below, by
docs/session-reports/tools/transcribe_ruling.py, as the next block of the decision log
(Block 221). The lines between the two marker lines, exclusive of the markers, are the
byte content of r823.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's
independent hash:
  r823.txt: sha256 66956e9bedcdc37d999cf60cec057543690c306e8557033fd71b32799baa28e0,
            4870 bytes, 64 lines, one trailing newline.

===== BEGIN r823.txt =====
RULING [evidence-engine 12ai-R823] — 12ai-CLNA-R1 accepted; the PI approves the removal list:
  the fifty-three scratch clones are removed, each by its own top directory; four clones and
  every record, log and database stay; readings on the findings; no standing rule
STATUS: PI ruling, 2026-11-20 UTC, session 12ai, on architect recommendation, on the
  12ai-CLNA-R1 final report (commit b20464906be863cc3612aa28b5c1a61c0da5bd56). Transcribe at
  item 0 of the 12ai-CLNB brief below, by transcribe_ruling.py, in the block after R822's,
  not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12ai-CLNA-R1 final report, quoted; wrapped lines joined): "Commit:
    b20464906be863cc3612aa28b5c1a61c0da5bd56 (parent f4427d5)."; "Ledger: ccbb6db (parent
    8af21ef)."; "r822.txt: 59c2435d…a347, 4,777 B, 61 lines. Equal to the architect's hash.";
    "brief_12ai_CLNA_R1.md,
    6cd57dd6f2c4a33aa39714ba1277e69bdc59ca12bee0647c3127ad34d12dc549, 11,669 B, 159 lines"
    (the architect's copy: the same); "Index mtimes, by stat only: 58 of 58 equal to 2j's
    after values, so none moved again."; "removal_list.tsv is 799f82dc…62d1e7"; "staged set
    exactly 25 files"; "REMOVE: 53 clones; 4,933,649,532 B freed at least, of 8,044,632,155 B
    apparent."; "KEEP: 4 clones"; the reason on every REMOVE row, "in INV and UNCHANGED;
    nothing unique; no reader; not in use; not this session's".
  READ (CC's): findings 1–3.
  READ (the architect's clone at b204649): the full hash in STATUS, its parent f4427d5;
    12ai_CLNA/brief_12ai_CLNA_R1.md and 12ai_open/r822.txt equal to the architect's copies
    by cmp; the plan's last line "The decision log in this file ends at R822.";
    12ai_CLNA/removal_list.tsv, sha256
    799f82dc533f5ce31286595123f94f906672752698f522e02f194c83f262d1e7, its header "top, head,
    apparent, nlink1, nlink_many, verdict, reason, parent, beside_files, beside_bytes" and
    57 rows; set by the architect against the report's printed table row by row, by top
    directory, head, bytes and verdict: 57 of 57 equal; 53 rows REMOVE and 4 KEEP; the
    REMOVE rows' nlink1 sum 4,933,649,532.
  INFERRED — expectation (reported if false): removing a clone's directory leaves the main
    repository whole, the two sharing files by hard link and the main repository borrowing
    no object (12ai_CLNA/i1.txt). The brief's checks after the removal are the test.

R823(a) — The 12ai-CLNA-R1 final report is accepted as printed: commit b204649 (Block 220;
  r822.txt and the resume file equal to the architect's copies; the read-out with its
  tables, the removal list and the helpers), ledger ccbb6db. No clone's index moved after
  step 2j.
R823(b) — The removal, the PI's words: "Approve the 53". The list is the fifty-three rows
  whose verdict is REMOVE in the committed 12ai_CLNA/removal_list.tsv at the sha256 above,
  and no other path. Each is removed by its own top directory, the value of the row's "top"
  column, never by its parent entry, never by a pattern. Not removed: the four clones this
  session made; the two pytest fixture repositories; every record, log, database and test
  database, inside those entries or anywhere else under ~/scratch (R817(f)).
R823(c) — Each clone is checked again immediately before it is removed, without any git
  command: it is the directory the row names, a real directory under ~/scratch and not a
  link; its HEAD, read from its files, is the row's; its index time and its bytes are the
  check's; no process uses it. A clone that fails any of these is not removed and is
  reported; the others go on. After each removal its parent entry's other files are
  counted again against the row; a difference stops the task at once.
R823(d) — What is given up, as put to the PI: the working copies. Their uncommitted content
  is in the record (12ai_SCRI/patches/ and untracked/), set against the clones byte for
  byte on 2026-11-20 (12ai_CLNA/t2d_patch_bytes.tsv). A clone can be made again from its
  commit and its patch.
R823(e) — Readings, no row. Findings 1 and 2 are the architect's: the resume ordered a
  section appended to a read-out file the stop had not yet written, and said fifty-one
  inventory clones where there are fifty-three. Finding 3: one untracked test file in
  12ab-u21b/parent matches a commit on origin/main and no longer HEAD's tree, since U41
  rewrote that test; its content is in the record.
R823(f) — The count so far in 12ai: one stop, the architect's; no pause; thirteen
  architect's errors (R822(f)'s eleven; findings 1 and 2), one reaching a commit.
R823(g) — This ruling states no standing rule: the id list, the glosses file and
  STANDING_RULES.md are untouched at the item 0 that transcribes it, asserted by content.
  Next: the removal (the brief below); the PI's report to the workbench of what was
  removed; then the closeout.
===== END r823.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ai-CLNB] — remove the fifty-three scratch clones the PI approved:
  item 0 (R823 as the next block; this brief committed as pasted), then each clone
  checked again and removed by its own top directory, then the checks after, and the
  evidence in a commit of its own.
PURPOSE: Carry out R823(b) and nothing beside it. Operational: one docs commit, fifty-three
  directory removals under ~/scratch, reads, one evidence commit. No engine, test,
  script, spec, codebook, tools or rules-file edit. No live write. No model call. No
  gate.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: high
  This is a deletion that cannot be undone. R823(b), in Section 1, authorizes the
  removal of the fifty-three directories named by the REMOVE rows of the committed list
  and of nothing else.
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 221 appended by the tool, nothing
  else); 12ai_open/r823.txt; a new directory 12ai_CLNB/; ~/scratch/12ai-clnb/ (new);
  the fifty-three directories; the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): every path under ~/scratch that is not
  one of the fifty-three "top" directories or under ~/scratch/12ai-clnb/ — the four
  KEEP clones, the two fixture repositories, every parent entry's other content, every
  record, log, database and pytest temp directory, retained/, and every other lane's
  entry; ~/scratch/12ai-scri/ and ~/scratch/12ai-clna/; the main repository's .git
  (read only); every file already under docs/session-reports/; the three rules files;
  CLAUDE.md; review.db (the fingerprint compare only); ollama.service. No sudo. No
  removal by a pattern, a wildcard, a find -delete, a git clean or a parent directory.
  No git command inside any clone. No mv. No removal of anything not yet checked.

PATHS: a name of the form 12xx_<name>/ is a directory of docs/session-reports/session-12/;
  a tool is named by its full path. LIST is 12ai_CLNA/removal_list.tsv as committed. OUT
  is ~/scratch/12ai-clnb/.

FORM: item 0's model is bec3545d8740da769b764894f234b302d9fd0be6 (one block, its rNNN.txt,
  the brief file, and its check). The evidence commit's model is b204649. No committed
  brief has removed anything from scratch; steps 2 and 3 are this brief's own. Where
  this brief disagrees with a tool or the record, the tool and the record govern and
  the difference is a finding (R732(a)).

TIMING: issued at 22:44 UTC on Friday 2026-11-20. Valid outside the day's window
  (R812(a)); the next opens at 09:00 UTC on Saturday 2026-11-21. If the clock at step 0
  is inside a window or within 30 minutes before one opens, STOP at once and do
  nothing. Step 2 is not begun unless it can end 15 minutes before an opening; once
  begun it is carried to its end or to a stop of its own.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ai-CLNA-R1 final report, as R823 quotes it): HEAD
    b20464906be863cc3612aa28b5c1a61c0da5bd56, parent f4427d5, clean, level;
    claude-config at ccbb6db or an accepted descendant (R663(b)); every clone's index
    time equal to 12ai_CLNA/i2_after.tsv's.
  READ (the architect's clone at b204649; re-read on the box):
    R1. docs/plan/ENGINE_REFACTOR_PLAN.md: sha256
        27de863c99094405297bd4866c18226ca7d9badffd2f156deabc2c079bf82f29, 3,461,226 B,
        20,757 lines; ends "…ends at R822."; last block 220; 177 "ends at" lines.
    R2. LIST: sha256 799f82dc533f5ce31286595123f94f906672752698f522e02f194c83f262d1e7; a
        header and 57 rows; columns "top, head, apparent, nlink1, nlink_many, verdict,
        reason, parent, beside_files, beside_bytes"; 53 rows REMOVE, 4 KEEP; every "top"
        begins "/home/ankitsarin/scratch/"; the REMOVE rows' nlink1 sum 4,933,649,532
        and apparent sum 8,044,632,155.
    R3. 12ai_CLNA/i2_after.tsv: sha256
        1cd341a3c82cdc174f35b9421e4dec0c80f2df78c0f41c4bc359a880634d8c0b; 225 lines; its
        ENTRY lines read "ENTRY, name, files, bytes, newest mtime in ns"; its lines for
        the clones' index times are read by their own form.
    R4. No 12ai_CLNB/ exists; 12ai_open/ holds no r823.txt.
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. The main repository borrows no object from a scratch clone (as 12ai_CLNA/i1.txt
        read it): .git/objects/info/alternates absent or naming no path under
        ~/scratch; one worktree. Read again at 2a.
    I2. The command of 2d removes a directory tree without following a symbolic link
        out of it and without crossing to another filesystem: shown first on a
        fixture under OUT (2b). If the fixture's outside file is touched, STOP.
  INFERRED — expectation (report a mismatch, continue):
    E1. All fifty-three pass 2c.
    E2. After the removals the main repository reads whole: HEAD and status as before;
        git fsck --connectivity-only reports nothing missing.
    E3. The retention records, the probe scratch and the stage directory read as the
        12ai-OPEN check read them.
    E4. The filesystem's free space rises by at least the REMOVE rows' nlink1 sum.

CONTRACT (steps, in order; evidence under OUT):

  STEP 0 — Pre-check (read-only). date -u. HEAD and parent proved by git cat-file -e
    <hash>^{commit}; clean; fetch; 0 0; one worktree. R1–R4; the three rules files at
    the 12ai-OPEN report's hashes. claude-config level; a pending foreign row carried
    and named. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; the check written for
    this brief, R794(i)).
    1a. 12ai_open/r823.txt from Section 1, one write, hashed straight after (R738(b))
        against Section 1's hash. Mismatch = STOP (R739(b)).
    1b. block221.json in block220.json's key set: tag R823; transcribed = the commit's
        UTC date (R775(a)); item "item 0 of the 12ai-CLNB brief"; made_by PI; made_date
        2026-11-20; session 12ai; report "the 12ai-CLNA-R1 final report (commit <full
        hash>)", composed in the form of the record's nearest block whose report
        carries one hash, quoted; sentence model, followed "12ai-CLNB brief";
        prev_ends R822; "beside" in Block 215's words. Render to stdout, compare the
        element order with Block 220's, then --append.
    1c. 12ai_CLNB/brief_12ai_CLNB.md from this whole pasted block, "CLAUDE CODE" through
        the STOP-12AI-CLNB line, one trailing newline, written once; sha256, bytes and
        lines reported.
    1d. The item-0 check, as bec3545's: --check-plan byte-identical for Block 221; the
        plan's numstat read, its "−" matched to zero; every earlier "ends at" line kept
        (177 before); the last line "…ends at R823." once; r823.txt equal to 1a's; the
        three rules files unchanged and unstaged (R777(b)); the staged set exactly the
        plan, r823.txt and the brief file. Then <check> && <commit>, one line.
        Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers (R801(f)); push; level.

  STEP 2 — The removal (helpers under OUT/helpers/, python3 -I -B; no git command inside
    any clone).
    2a. Before anything is removed: LIST's sha256 again; the fifty-three REMOVE rows
        read into OUT/to_remove.tsv by the helper, never typed; I1; the main
        repository's HEAD, git status --porcelain, git count-objects -v and the count
        and bytes of the files under its .git/objects/pack; df for the filesystem that
        holds ~/scratch, in bytes; every parent entry's file count and bytes; for the
        four KEEP clones and the two fixture repositories, file count, bytes and index
        time. All to OUT/before.txt. And the parent check of 2d proved before it is
        relied on: for every REMOVE row, the helper's count and bytes of what the
        parent holds beside the clone, computed now by the definition
        12ai_CLNA/helpers/clna_check.py used (quoted), equal the row's "beside_files"
        and "beside_bytes"; fifty-three of fifty-three, or STOP before any removal.
    2b. I2, on a fixture: under OUT/fixture/ a directory holding a file, a
        subdirectory, a hard link to a file outside it and a symbolic link to a
        directory outside it; the command of 2d run on it; the two outside targets
        present and unchanged after, by sha256 and listing. Quote the command and the
        result. A touched target = STOP.
    2c. For each row of OUT/to_remove.tsv, in the list's order, immediately before its
        removal, by stat and file reads only:
        (1) "top" is absolute, begins /home/ankitsarin/scratch/, has at least two
            path components below scratch, and its first component there does not
            begin 12ai-;
        (2) it exists, is a directory, is not a symbolic link, and its resolved path
            equals itself;
        (3) it holds a directory named .git;
        (4) its HEAD equals the row's "head": read from .git/HEAD and, where that
            names a ref, from the loose ref file or .git/packed-refs;
        (5) its .git/index mtime in nanoseconds equals R3's value for it;
        (6) its apparent bytes, by an lstat walk that follows no link, equal the
            row's "apparent";
        (7) no process of this user has its working directory or an open file at or
            under it (/proc).
        All seven written to OUT/removal_log.tsv with their values. A clone that
        fails one is not removed; its row is logged SKIPPED with the failing value;
        the next row goes on.
    2d. The removal of one clone that passed 2c: rm -rf --one-file-system -- followed
        by the row's "top", passed as one argument from the helper, never built by a
        shell from a variable or a pattern. Then: the path no longer exists; its
        parent still exists; the parent's remaining file count and bytes equal the
        row's "beside_files" and "beside_bytes" by 2a's definition, less whatever
        this step has already removed from that parent (four parents hold two
        clones of the list each; the arithmetic is stated in the log). Logged with
        the UTC time. A parent that does not
        match = STOP at once: no further removal, the entry's listing reported.
    2e. After the last row: every REMOVE "top" absent or logged SKIPPED; the four KEEP
        clones and the two fixture repositories with the file count, bytes and index
        time of 2a; every parent entry present; the reads of 2a again, to
        OUT/after.txt; df's change beside the list's nlink1 sum (E4).

  STEP 3 — The checks after (read-only).
    3a. The main repository (E2): HEAD, git status --porcelain and git count-objects
        -v against 2a's; the pack files by count and bytes against 2a's; git fsck
        --connectivity-only, its output whole. A missing object = STOP and report;
        nothing is repaired.
    3b. The retention records (E3): docs/session-reports/tools/session_open_check.py
        run from the repository root on a config written under OUT that holds only
        the retention, probe_scratch and stage sections, their values copied from
        ~/scratch/12ai-open/expected_12ai.json and quoted; every row MATCH. The
        sections it omits print SKIPPED. A MISMATCH = STOP and report.
    3c. Fingerprint --compare IDENTICAL.

  STEP 4 — The evidence commit (R731(c)). 12ai_CLNB/: to_remove.tsv, removal_log.tsv,
    before.txt, after.txt, the fixture's result, 3a's and 3b's outputs and the
    helpers; the staged set exactly those, every sha256 equal to its scratch source;
    chained check && commit; fingerprint IDENTICAL before and after the push; level;
    ledger.

ACCEPTANCE GATES:
  G1. Step 0 matched; every full hash proved.
  G2. Item 0 one commit that passed 1d in one chain; pushed, level, fingerprint
      IDENTICAL; ledger.
  G3. Step 2: the fixture's targets untouched; every removed path was a REMOVE row's
      "top" that passed all seven checks at its turn; no other path under ~/scratch
      removed, moved or changed, shown by 2e against 2a; every parent entry's other
      files equal to the list's count and bytes.
  G4. Step 3: the main repository whole; the retention, probe and stage rows MATCH;
      fingerprint IDENTICAL.
  G5. The evidence commit as step 4 states.

OUT OF SCOPE: the four KEEP clones (the closeout's or a later session's); any record,
  log, database, test database or pytest temp directory; the two fixture repositories;
  emptying or removing a parent entry; git gc, repack or prune in the main repository;
  other lanes' entries; the retention ledger; INT-ai12-2; U31; the tools items; the
  closeout; /wrap.

CONSTRAINTS (task-only): OUT is the one place a helper may write to (R658(c)); the
  fifty-three directories are the one thing removed. A path is taken from the list
  file by the helper and compared as a whole string; none is typed, completed or
  globbed. The removals run one at a time, in order, each after its own check.

FINAL REPORT FORMAT: (0) the pre-check. (1) Item 0: r823.txt's hash; Block 221's
  metadata with its model quoted; the brief file's sha256, bytes and lines; hashes,
  numstat, fingerprint, ledger. (2) 2a's reads; the fixture's command and result; the
  removal log's counts: removed, skipped, each skipped row whole with its failing
  value; the first and last removal's UTC time; 2e's comparison; df before and after
  and the bytes freed beside the list's sum. (3) 3a's output whole; 3b's rows; the
  fingerprint. (4) The evidence commit. Then one block for the PI to pass to the
  workbench: the count removed, the count kept and why, the bytes freed as measured,
  and the two commit hashes. Then findings, numbered. Then STOP.

CONTRADICTION HANDLING: a step-0 mismatch, a 1a mismatch, or a check failure or crash at
  1d = STOP before the item-0 commit. I1 false, or I2 false at 2b = STOP before any
  removal. A parent that does not match at 2d, a missing object at 3a, or a MISMATCH at
  3b = STOP at once and report; nothing is repaired or restored without a ruling. A
  clone that fails 2c is skipped and reported, and the work continues. E4 false is
  reported with both figures. A spec premise that does not survive contact with disk
  is a finding, not an obstacle.

HANDBACK: Stop and report. Do not remove anything else, and do not start the closeout or
  U31. Await the next brief or ruling in this session. STOP-12AI-CLNB
