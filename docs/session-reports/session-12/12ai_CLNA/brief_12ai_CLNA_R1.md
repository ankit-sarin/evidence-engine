CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R822, PI ruling 2026-11-20 UTC; not on disk)
================================================================================

R822 is transcribed in the commit of the resume below, by
docs/session-reports/tools/transcribe_ruling.py, as the next block of the decision log
(Block 220). The lines between the two marker lines, exclusive of the markers, are the
byte content of r822.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's
independent hash:
  r822.txt: sha256 59c2435d76e71737b3f8876fdbccb91159affa2cd2c0779de0d105fb07ada347,
            4777 bytes, 61 lines, one trailing newline.

===== BEGIN r822.txt =====
RULING [evidence-engine 12ai-R822] — the 12ai-CLNA stop is correct and the architect's; the
  PI's ruling: the clone check's read-out is committed as it stands, its failed no-write test
  recorded in it; nothing is removed until the PI approves the list in words; no standing
  rule
STATUS: PI ruling, 2026-11-20 UTC, session 12ai, on architect recommendation, on the 12ai-CLNA
  stop report (item 0 f4427d5306582823c6a7b63cfc3e7900d1bca5f1 committed; stopped at step
  2j, before step 3; nothing of step 2 committed; nothing removed). Transcribe in the commit
  of the resume below, by transcribe_ruling.py, in the block after R821's.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the stop report, quoted; wrapped lines joined): "Commit:
    f4427d5306582823c6a7b63cfc3e7900d1bca5f1 (parent d7a0bf7). Numstat: plan +102 −2,
    CLAUDE.md +5 −3, r821.txt +87, brief +364."; "Ledger: 8af21ef."; "r821.txt: ce0f18c0…c2e5,
    6,976 B, 87 lines. Equal to the architect's hash."; "Brief file:
    e64ff22ae1d3dc679cedeb09cae30accdf750c7c60c94d6042c59161c877d71b, 26,660 B, 364 lines."
    (the architect's copy: the same); "~/scratch/12ai-u30b/clone/.git/index:
    1795209061260464430 → 1795212793131805356 ns"; "~/scratch/12ai-u41b/clone/.git/index:
    1795152047054457187 → 1795212793232805623 ns"; "(i2_diff.txt: 8 lines, all these two)";
    "I1 holds: .git/objects/info/alternates is absent; one worktree."; "59 repositories: 57
    clones, 2 fixture repositories, 0 other."; "None of INV's 55 is missing."; "All 53 INV
    clones are UNCHANGED, 0 CHANGED"; "equal to git diff HEAD byte for byte"; "112 reader
    paths"; "0 lie inside a clone."; "0 processes use a clone."; "53 REMOVE, 4 KEEP."; "the 53
    REMOVE clones' link-count-1 bytes total 4,933,649,532 B, of 8,044,632,155 B apparent";
    "Every REMOVE clone's parent entry holds other files beside it (79,482 files,
    3,395,304,236 B in all)".
  READ (CC's, marked so in the report): the cause, "git diff seems to have refreshed and
    rewritten the index anyway in the two clones whose working files were stat-dirty", not
    tested; findings 1–4.
  READ (the architect's clone at f4427d5): the full hash in STATUS, its parent d7a0bf7;
    12ai_CLNA/brief_12ai_CLNA.md and 12ai_open/r821.txt equal to the architect's copies by
    cmp; the plan's last line "The decision log in this file ends at R821."; CLAUDE.md's
    lines on the grant as the brief carried them.
  INFERRED: none that a clause below rests on. The cause of the two rewrites is not
    established; (c) does not rest on it.

R822(a) — The stop is correct, at the step the brief set, and nothing of step 2 was
  committed. The cause is the architect's: the brief gave GIT_OPTIONAL_LOCKS=0 as what
  keeps every git command of the check from writing, and ordered git diff HEAD inside the
  clones; what that setting does for git diff was not read. A mechanism's behaviour stated
  from its name. Item 0 f4427d5 stands and is accepted as committed (Block 219; INT-i12-71
  and INT-j12-51 CLOSED on 7fad72a; fix units 39, done 35; CLAUDE.md changed in the lines
  on the grant only), ledger 8af21ef.
R822(b) — The PI's ruling, in his word: "1". Of the two options put to him, 1 is: the two
  index rewrites are accepted and the read-out is committed as it stands, with the failed
  no-write test recorded in it as a finding. The check is not run again.
R822(c) — What the failure touches, from the measured lines: two .git/index files, both in
  clones this session made, both KEEP by the list's rule; their HEAD and tracked content
  unchanged. No clone of the committed inventory and no other entry under ~/scratch
  changed, by the same test that failed for those two. The measurements of the fifty-three
  inventory clones stand.
R822(d) — Nothing is removed by the resume. Its report prints the removal list whole; the PI
  approves it in words, or not; a removal brief follows only on that. A removal names each
  clone's own top directory and never its parent entry. The bytes freed are a lower bound.
R822(e) — Readings, no row. Finding 2: a later read-only pass over clones reads a diff
  without git diff in the clone, or through a copy of the index; this is task guidance, not
  a rule, until the cause is measured. Finding 4 is the architect's: the brief expected the
  12ah open's and close's clones among those not in the inventory, and they are in it.
R822(f) — The count so far in 12ai: one stop, this one, the architect's; no pause; eleven
  architect's errors (R821(g)'s nine; the setting of (a); finding 4), one reaching a commit.
R822(g) — This ruling states no standing rule: the id list, the glosses file and
  STANDING_RULES.md are untouched in the commit that transcribes it, asserted by content.
===== END r822.txt =====

================================================================================
SECTION 2 — RESUME [evidence-engine 12ai-CLNA-R1] under R822 above, in this block
================================================================================

Same session, same task, same scratch directory (OUT, ~/scratch/12ai-clna/). MACHINE: DGX,
~/projects/evidence-engine. 12ai-CLNA's PERMISSIONS, PATHS, CONSTRAINTS and STOP / DO NOT
TOUCH stand, with these changes (R822(b)): step 2 is not run again; step 2j's stop is
lifted for the two files R822 names and for no other; step 3's commit also carries Block
220, 12ai_open/r822.txt and this resume as 12ai_CLNA/brief_12ai_CLNA_R1.md. No git
command is run inside any clone under ~/scratch by this resume. Nothing under ~/scratch
is removed, moved or cleaned. The form for a resume is
12ah_OLRA/brief_12ah_OLRA_REC_R1.md (sha256
ef71492456f49dea253433be3248504286abe2c6e08cda810f49aefba7c56fab) and its commit
6dc36b312879bdd16c682093d86a9e2b3c331c86; quote its block's header before composing.
TIMING: issued at 22:36 UTC on Friday 2026-11-20; valid outside the day's window
(R812(a)); hold uncommitted from 15 minutes before an opening.

R0. date -u. evidence-engine HEAD f4427d5306582823c6a7b63cfc3e7900d1bca5f1 (cat-file -e
    with ^{commit}), parent d7a0bf7, clean, level, one worktree; the plan sha256
    c4a66a723b2a567ed30c5c7f19b3ad508a9f508127aeffa13483d86f3b9de10e, 3,453,539 B, 20,685
    lines, ends "…ends at R821.", last block 219, no Block 220; the three rules files at
    the 12ai-OPEN report's hashes; claude-config at 8af21ef or a descendant acceptable
    under R663(b), reported; fingerprint IDENTICAL; no 12ai_open/r822.txt; 12ai_CLNA/
    holding brief_12ai_CLNA.md only. OUT's listing as the stop left it, with bytes and
    sha256, printed. By stat only, no git: every clone's .git/index mtime in
    nanoseconds against the "after" values of step 2j; a file that moved again is
    reported with both values and is a STOP. A mismatch = STOP.
R1. The read-out, before it is copied: one section appended to OUT/12ai_CLNA.md, headed
    "I2 at 2j: false (R822)", holding i2_diff.txt's eight lines quoted whole, the two
    files with their before and after values, the sentence "The cause is CC's reading
    and was not tested", what the same test shows unchanged (the other fifty-one
    inventory clones' indexes, 12ai-open/recount, and every other in-scope entry, each
    count from the tables), and R822(b)'s word. Nothing else in the read-out or its
    tables changes: the file before and after, by diff, differs by the appended section
    alone, and removal_list.tsv's sha256 is the one R0 printed.
R2. 12ai_open/r822.txt from Section 1, one write, hashed straight after (R738(b))
    against the architect's hash: sha256
    59c2435d76e71737b3f8876fdbccb91159affa2cd2c0779de0d105fb07ada347, 4777
    bytes, 61 lines, one trailing newline. A mismatch = STOP (R739(b)).
    block220.json in block219.json's key set: tag R822; transcribed = the commit's UTC
    date (R775(a)); item and "beside" composed by CC from the form's block's words for
    a ruling transcribed in a resume's commit with the task's files; made_by PI;
    made_date 2026-11-20; session 12ai; report "the 12ai-CLNA stop report (item 0
    <full hash>)", the hash in backticks; sentence model, followed "12ai-CLNA-R1
    resume"; prev_ends R821. Render to stdout, compare the element order with Block
    219's, then --append.
R3. 12ai_CLNA/brief_12ai_CLNA_R1.md from this whole pasted block, "CLAUDE CODE" through
    the STOP-12AI-CLNA-R1 line, one trailing newline, written once; sha256, bytes and
    lines reported. Then cp into 12ai_CLNA/, each sha256-equal to its source:
    12ai_CLNA.md as R1 left it; every table beside it; removal_list.tsv; the I2
    baseline, its second read and i2_diff.txt; the step-2 helpers under helpers/. Not
    copied: OUT/clone/ and anything under it; block and check files of item 0; any
    other file of OUT, each named in the report.
R4. The check, written for this resume (R794(i)), then <check> && <commit> in one
    line: --check-plan byte-identical for Block 220; the plan's numstat read, its "−"
    matched to zero; every earlier "ends at" line kept; the last line "…ends at R822."
    once; r822.txt equal to R2's; the three rules files unchanged and unstaged
    (R777(b)); every copy's sha256 from the index (git show :<path>) equal to its
    source's; the staged set exactly the plan, r822.txt, the resume file and R3's
    copies, its count stated, no path under a directory named clone. The message from
    the staged set, docs scope. Fingerprint --compare before the push; push; level;
    --compare after (IDENTICAL). The ledger row with trailers (R801(f)); push; level.
R5. The report: R0's reads; R1's appended section as written; r822.txt's and the resume
    file's sha256, bytes and lines; Block 220's metadata with its model quoted; the
    commit and ledger hashes with parents; the staged count. Then the removal list
    printed whole from the committed removal_list.tsv, one line per clone, all
    fifty-seven: the clone's top directory, HEAD (short), the bytes a removal frees,
    the verdict and its reason; then the totals by verdict. Then findings, numbered.

ACCEPTANCE GATES: one commit that passed R4 in one chain, holding Block 220, r822.txt,
  the resume file and the read-out's files and nothing else; the read-out differing from
  the stop's by the appended section alone; no clone's index moved since step 2j; pushed,
  level, fingerprint IDENTICAL; the ledger row; the list printed whole.

CONTRADICTION HANDLING: an R0 mismatch, an index that moved again, an R2 mismatch, or a
  check failure or crash (R763(c)) = STOP before the commit. A spec premise that does not
  survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Remove nothing. Do not start the removal, the closeout or U31.
  Await the next brief or ruling in this session. STOP-12AI-CLNA-R1
