CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R817, PI ruling 2026-11-19 UTC; not on disk)
================================================================================

R817 is transcribed at item 0 of the brief below, by
docs/session-reports/tools/transcribe_ruling.py, as the next block of the decision log
(Block 215). The lines between the two marker lines, exclusive of the markers, are the
byte content of r817.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's
independent hash:
  r817.txt: sha256 bc10edf7f6f9cf775db5a5144c7c5a536e069de1d079479412dc7a21cfa50b64,
            8012 bytes, 100 lines, one trailing newline.

===== BEGIN r817.txt =====
RULING [evidence-engine 12ai-R817] — 12ai-OPEN accepted; the gate's 995.53 s enters the series;
  the interim session 12ai-SCRI counted with 12ai; its fetch recorded; the PI's decisions on
  the scratch clones and on the interim read-out's publication; readings on the findings; no
  standing rule
STATUS: PI ruling, 2026-11-19 UTC, session 12ai, on architect recommendation, on the 12ai-OPEN
  final report (item 0 7ff68fe697ee94562c540158a43bc81f77523e75; evidence
  35fc9ec694f07c05db787f3a504611f7fdbb8b67). Transcribe at item 0 of the 12ai-SCRI-RECA brief
  below, by transcribe_ruling.py, in the block after R816's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12ai-OPEN final report, quoted; wrapped lines joined): "(2) Item 0:
    7ff68fe697ee94562c540158a43bc81f77523e75, parent b13b5eb"; "Ledger commit
    36c173231c7dddb86bb559540b8f605251ebefeb (parent 60ae7bf)"; "(5) Evidence commit:
    35fc9ec694f07c05db787f3a504611f7fdbb8b67, parent 7ff68fe"; "Ledger commit
    96cfd37396336dc434cb60d1ae986a8c76fb5949 (parent 36c1732)"; "r816.txt: 15c8f6f2…2f9e,
    8,523 B, 105 lines. This equals the architect's hash."; "brief_12ai_OPEN.md:
    f7e8501a259e7b4f9d3cb7e06e0236216707ee65ab9f3d9e9430de111d929302, 41,318 B, 557 lines."
    (the architect's copy: the same sha256, bytes and lines); "Plan +116 / −0 (I4)"; "Plan now
    c0d7e7e5…4a42, 3,380,978 B."; "The three rules files are byte-equal to HEAD's."; "now = 287
    rows (94 / 57 / 136, 0 unclassed); dated lines 38 with 33 done; expected_holds true";
    "R510: b13b5eb 213; 7ff68fe 213; in 0, out 0, no class or package move."; "Wall time
    995.53 s, inside 897–1095 s (E4 holds)."; "the hold was not needed"; "27 new files,
    +13,309 / −0"; "FETCH_HEAD was written at 2026-11-19 14:37:49.90 UTC"; "Every ref is at
    b13b5eb. I6 holds."; "Recursive listing: 97 files in 9 directories, 7,477,599 B.";
    "SHA256SUMS: sha256 1d834e29bd0b10a05bc536677f481f5b4cd6963850bfffdc3208d5317ab074d2,
    9,914 B, 96 lines."; "96 lines, 96 match, 0 mismatch"; the interim session's record,
    first entry "2026-11-19T14:13:44.358Z", last entry "2026-11-19T14:38:56.375Z"; "this lane
    has 678 ledger rows".
  READ (CC's): findings 1–7; the health script's lines as CC read them; "No file content of
    the older clones was read."
  READ (the architect's clone at 35fc9ec): the two full hashes in STATUS, a straight chain on
    b13b5eb; 12ai_open/brief_12ai_OPEN.md and 12ah_close/r816.txt equal to the architect's
    copies by cmp; the plan's sha256
    c0d7e7e5788cbde300add7e832e237bf2e135b3462811ccc10a05e4d83a74a42, 3,380,978 bytes, 20,123
    lines, last line "The decision log in this file ends at R816."; Block 214's header in
    Block 201's form; 12ai_open/gate_12ai_open/gate_wall.txt "wall: 995.533181647 s";
    12ai_open/between_sessions_read.txt "SHA256SUMS: lines 96 match 96 mismatch 0 (distinct
    names 96)".
  READ (the PI's upload of 12ai_SCRI.md, sha256
    7e3bdda1dc59c1c3a3a2d53d70af378d6fa52a6364e660217389b0a9518301ad, 22,178 bytes, equal to
    its line in the list the open verified): its own words "Writes went only to
    `~/scratch/12ai-scri/`" and "There was no model call, no Ollama request, no lock
    acquisition, no pytest run".
  INFERRED: none that a clause below rests on. The architect has read no file of
    ~/scratch/12ai-scri/ but 12ai_SCRI.md; (e) therefore rules nothing on their content.

R817(a) — The 12ai-OPEN final report is accepted as printed, the PI's word: "Accept the open
  and count the interim session." Item 0 7ff68fe (Block 214; r816.txt equal to the
  architect's hash; the 12ah lane-line copy; the rules files untouched; the brief file equal
  to the architect's copy), ledger 36c1732; the evidence commit 35fc9ec (the gate's nineteen
  files and its two side files, the 2026-11-19 job reads, the between-session readings),
  ledger 96cfd37. I1–I6 held. E1–E7 held; E8 held as to file content (finding 5).
R817(b) — The gate's 995.53 s is the forty-fourth member of the series: a standard gate, the
  third launched as a unit, with no hold and nothing beside it but surgical-cv's uvicorn. The
  band stays 897–1095 s until the 12ai closeout cuts the next one in R683(c)'s form.
R817(c) — The interim CC session bf3224f8-1d94-4728-9a0a-13db7c9ffb8d ran task 12ai-SCRI, a
  read-only inventory of this lane's scratch, between the 12ah wrap and this open, and
  committed nothing. It is counted with session 12ai's record from the commit of its files,
  as R813(f) counted 12ah-OLRA with 12ah. Its window is its record's: 2026-11-19 14:13:44 to
  14:38:56 UTC.
R817(d) — A finding, recorded: during the interim session CC ran one git fetch in
  ~/projects/evidence-engine that its brief forbade. Measured at this open before the open's
  own fetch: FETCH_HEAD alone was written, at 14:37:49 UTC inside that session's window; no
  reflog entry falls after the 12ah closeout's push; every ref was at b13b5eb. CC's
  deviation, without effect. No row and no rule.
R817(e) — The interim read-out's publication, the PI's word: "Do the readout first". Nothing
  under ~/scratch/12ai-scri/ is copied or committed until a read-only publication read-out
  (the brief below) has been reported and the PI has ruled on it in words. That read-out
  runs a credential scan with a negative control, lists every name, path and quoted line
  that comes from outside this lane, and profiles the two large tables. It goes to scratch
  and to the report, not to the repository. The repository is public.
R817(f) — The PI's decision of 2026-11-19 on the scratch clones, as he stated it to the
  architect: the scratch clones of evidence-engine are deleted late in session 12ai, under a
  brief of their own, after the interim read-out is committed. Every record, log and
  database stays until the freshman freeze, and test databases are not touched before it.
  That brief re-checks each clone against the inventory, confirms that no open or close
  step reads a clone, and prints the removal list for the PI's approval in words before
  anything is removed. A reading for it, from this open: each open and close makes its own
  recount clone and read no file content of an older one; this open made one more.
R817(g) — Readings, no row. Finding 1: the interim window is the record's, as (c) gives it.
  Finding 2 is the architect's: the brief's R11 said no file named expected_12*.json is
  committed, which is true, in support of a wider point that is not; a committed
  expected-values config exists, 12aa_SETUP/open_expected_validation.json, and no value of
  the open came from it. Findings 3 and 4 are the two tools items of R813(e), unchanged.
  Finding 5: CC's git rev-parse in two older clones answered the brief's own request for a
  list of paths by kind; a metadata read, accepted. Finding 6: the three readings the
  step-1 helper omits were read (0 earlyoom kill lines, boot abf99132, NRestarts 0) and
  stand in the report. One more, the architect's, and it reached a commit: the committed
  12ai-OPEN brief names four home folders of other lanes in its DO NOT TOUCH list, before
  the PI had been asked to accept publishing other lanes' names. The file stands as
  committed; the PI is told.
R817(h) — The count so far in 12ai: no stop and no pause; two architect's errors (finding 2;
  the four folder names), the second reaching a commit.
R817(i) — This ruling states no standing rule: the id list, the glosses file and
  STANDING_RULES.md are untouched at the item 0 that transcribes it, asserted by content.
  Next, in this order: the publication read-out (the brief below); the PI's ruling on it;
  the commit of the interim read-out's files; the restart change's design ruling, quoting
  12ah_OLRA/12ah_OLRA.md and its addendum; its build as one engine-commit brief; U30's build
  with N46; the clone deletion brief, before the close; then the report to the workbench of
  what was removed and of the restart change's merge sha.
===== END r817.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ai-SCRI-RECA] — the publication read-out on the interim session's
  files: item 0 (R817 as the next block by transcribe_ruling.py; this brief committed as
  pasted), then, read-only, a source check, a credential scan with a negative control,
  and a census of what the ninety-seven files of ~/scratch/12ai-scri/ would publish.
PURPOSE: Give the PI what he needs to rule on committing those files to a public
  repository. Mixed: one docs commit, then reads only. Nothing under ~/scratch/12ai-scri/
  is copied or committed by this brief, and this brief's own read-out is not committed
  either. No engine, test, script, spec, codebook, tools or rules-file edit. No live
  write. No model call. No gate.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 215 appended by the tool, nothing
  else); docs/session-reports/session-12/12ai_open/r817.txt; a new directory
  docs/session-reports/session-12/12ai_SCRI_RECA/ (brief_12ai_SCRI_RECA.md only);
  ~/scratch/12ai-scri-reca/ (new); the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): ~/scratch/12ai-scri/ (read in place;
  nothing written into it or under it, no helper, no output, no bytecode cache; nothing
  copied out of it into the repository); every file already under docs/session-reports/;
  the three rules files; CLAUDE.md; every clone of evidence-engine under ~/scratch and
  every other scratch entry (not entered); the home folders of other lanes (not read);
  review.db (the fingerprint compare only); ollama.service. No value the scan matches
  is printed, written or committed in clear. The names, paths and quoted lines step 3
  finds are printed in the final report and written under ~/scratch/12ai-scri-reca/
  only: none goes into a commit message, a ledger row or any tracked file.

PATHS: a name of the form 12xx_<name>/ is a directory of docs/session-reports/session-12/;
  a tool is named by its full path. SRC is ~/scratch/12ai-scri/. OUT is
  ~/scratch/12ai-scri-reca/.

FORM: no committed brief is this task's form whole. Item 0's model is the 12ah closeout's
  item 0, 9fed14881a114107f65090f17aa77a432dc8dc80: one block, its rNNN.txt beside the
  session's other rulings, the brief file, and its check. Steps 2a–2c follow steps 1a–1c
  of 12ah_OLRA/brief_12ah_OLRA_REC.md (sha256
  86a9e5f06d8ddee28cf5b94229559dbbf84779ce124a7b9234e0f14dc1017141); read that brief's
  Section 2 before step 2. Quote a model before composing its counterpart; where this
  brief disagrees with a tool or the record, the tool and the record govern and the
  difference is a finding (R732(a)).

TIMING: issued at 15:46 UTC on Thursday 2026-11-19. Valid outside the day's window
  (R812(a)). If the clock at step 0 is inside a day's window or within 15 minutes before
  one opens, STOP at once, write nothing and say so. If the clock reaches 15 minutes
  before an opening with the commit pending, hold with the work on disk uncommitted and
  resume after 10:35 UTC. Block 215's transcribed date is the item-0 commit's UTC date
  (R775(a), (d)); its made date is R817's own, 2026-11-19.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ai-OPEN final report, as R817 quotes it in Section 1): evidence-engine HEAD
    35fc9ec694f07c05db787f3a504611f7fdbb8b67, parent
    7ff68fe697ee94562c540158a43bc81f77523e75; claude-config at
    96cfd37396336dc434cb60d1ae986a8c76fb5949 or an accepted descendant (R663(b)); SRC's
    list at 1d834e29bd0b10a05bc536677f481f5b4cd6963850bfffdc3208d5317ab074d2 with 96
    lines, 96 match, 0 mismatch, by ~/scratch/12ai-open/helpers/scri_check.py; 97 files
    in 9 directories, 7,477,599 B.
  READ (the architect's clone of origin at 35fc9ec, 2026-11-19 15:45 UTC; re-read on the
    box):
    R1. docs/plan/ENGINE_REFACTOR_PLAN.md: sha256
        c0d7e7e5788cbde300add7e832e237bf2e135b3462811ccc10a05e4d83a74a42, 3380978 bytes,
        20123 lines; last line "The decision log in this file ends at R816."; last block
        214; 171 lines begin "The decision log in this file ends at R"; six-cell "| INT-"
        rows 213.
    R2. Rules files: docs/plan/STANDING_RULES.md
        d56dbdaeafb3659527cb77e1e81744eb01dadbe871fd96944104bb9e01b7e3ff;
        docs/plan/standing_rules_ids.txt
        a28da0611e1a4d62cbc8185ff89df22d2df7a2a37340da0eb164dec678e75257;
        docs/plan/standing_rules_glosses.json
        7f7374b25f7efd2c1df97cfbe3fb9b828c7fec5bce4e948d751cec08a4fa72ed.
    R3. 12ai_open/between_sessions_read.txt (sha256
        1fc7ce21332bedb4775f4a25b6064d109405f6beb4362e3feff5290a3de9c108), section (iv),
        lists SRC on 2026-11-19, recursive: at the top level 12ai_SCRI.md 22178,
        SHA256SUMS 9914 and twenty-three other files, the largest
        s4_records_without_committed_copy.tsv 4378258 and
        s4_records_with_committed_copy.tsv 2508415, and g3_random.bin 4096; clone_status/
        with fifty-five files; patches/ with seventeen; untracked/ with one file five
        directories down.
    R4. No 12ai_SCRI_RECA/ exists; 12ai_open/ holds no r817.txt. r812.txt to r815.txt
        are in 12ah_open/, the directory of the session they were made in.
    R5. Block 213's header reads "in its own commit with the brief file it orders";
        Block 211's names its report "the 12ah-OPEN final report (item 0 `<full hash>`;
        evidence `<full hash>`)".
    R6. 12ah_OLRA/rec_scan.py (sha256
        14328ba6ba6c8c593b8c8039c7f88dc89ca44acdfc7d67dabe69c4621a86080b): "usage:
        python3 -I rec_scan.py <out.txt> <file>..."; "A file that is not valid UTF-8 is
        read as bytes"; "Exit 0 if no stop-class hit, 1 if any"; its header names
        thirteen stop classes and four reading classes; its reading class
        abs_path_outside does not count a path under /home/ankitsarin/.
  READ (the PI's upload of SRC's 12ai_SCRI.md, sha256
    7e3bdda1dc59c1c3a3a2d53d70af378d6fa52a6364e660217389b0a9518301ad, equal to its line
    in the list; the architect has read no other file of SRC):
    P1. Its S1: "In scope: 161 entries (157 `12*`, plus `retained`, `wr5-b`, `10b-close`,
        `e_exec_probe`)" and "Out of scope: 22 entries, 86,112,348 B, by name and size
        only", followed by those entries' names.
    P2. Its E7 and S6(b) name four home folders of other lanes and cite lines of
        surgical-cv's primer.md and CLAUDE.md and of claude-config's primer.md.
    P3. Its S5 quotes lines 798–804 and line 851 of the project primer.md, a file this
        repository ignores, and lines of primer-history/.
    P4. Its S2: "119 citations of 93 distinct paths", in s2_citations.tsv, with sources
        that include primer.md.
    P5. Its S4: the two large tables list records "with an identical committed copy"
        (12,960 files) and "without one" (30,462 files), among them "paper texts 2,103".
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. SRC is as the open read it: the list at the MEASURED hash, every line matching
        its file, 97 files, nothing unlisted but the list.
    I2. The scan can see what it looks for: on a fixture built for this brief it flags
        every class its header names, and flags nothing in a clean fixture file.
    I3. transcribe_ruling.py --append only appends (the numstat's "−" read and matched
        to zero).
  INFERRED — expectation (report a mismatch, continue):
    E1. No file of the ninety-seven holds a credential.
    E2. g3_random.bin is the only file that is not valid UTF-8.
    E3. Every record path in the two large tables lies under an in-scope entry (P1).
    E4. The names of other lanes' entries occur only in 12ai_SCRI.md, s1_toplevel.tsv,
        g1_before.tsv and g1_after.tsv.
    E5. Every file a patch touches is under docs/, tests/, engine/ or scripts/.
    E6. ~/scratch/12ah-close/ still holds block213.json and the item-0 check of 9fed148.
        If not, Block 213's header and transcribe_ruling.py's docstring govern.

CONTRACT (steps, in order; every output under OUT):

  STEP 0 — Pre-check (read-only). date -u. HEAD and parent proved by git cat-file -e
    <hash>^{commit}; clean; fetch; 0 0; one worktree. R1, R2, R4. claude-config: the
    MEASURED commit or an accepted descendant, level; a pending foreign row carried and
    named. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; the check written for
    this brief, R794(i)).
    1a. 12ai_open/r817.txt from Section 1, one write, hashed straight after (R738(b))
        against Section 1's hash. Mismatch = STOP.
    1b. OUT/block215.json in block213.json's key set (E6): tag R817; transcribed = the
        commit's UTC date; item "item 0 of the 12ai-SCRI-RECA brief"; made_by PI;
        made_date 2026-11-19; session 12ai; report as R817's STATUS names it, "the
        12ai-OPEN final report (item 0 <full hash>; evidence <full hash>)", the hashes
        in backticks in Block 211's form (R5); report_short the same with short hashes;
        sentence model, followed "12ai-SCRI-RECA brief"; prev_ends R816; "beside" in
        Block 213's words (R5), read from block213.json or Block 213's header. Render to
        stdout, compare the header's element order with Block 213's, then --append.
    1c. 12ai_SCRI_RECA/brief_12ai_SCRI_RECA.md from this whole pasted block, "CLAUDE
        CODE" through the STOP-12AI-SCRI-RECA line, one trailing newline, written once;
        sha256, bytes and lines straight after the write, reported.
    1d. The item-0 check, as 9fed148's, with: --check-plan byte-identical for Block 215;
        the plan's numstat read, its "−" matched to zero (I3); the last line "The
        decision log in this file ends at R817." once, every earlier "ends at" line
        kept (171 before); r817.txt equal to 1a's; the three rules files byte-equal to
        HEAD's and none staged (R777(b)); six-cell INT- rows 213 before and after; the
        staged set exactly the plan, r817.txt and the brief file. Quote the check.
    1e. <check> && <commit>, one line, no pipe; the message from the staged set, docs
        scope. Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers, its message built from the diff
        (R801(f)); push; level. Record hashes and parents.

  STEP 2 — The source check and the scan (read-only on SRC).
    2a. I1: SRC's recursive listing with bytes against R3; the list's sha256;
        scri_check.py on it (path, sha256 and output reported). The directories' mtimes
        before step 2 and after step 3, as the open took them. A difference = STOP.
    2b. The scan: 12ah_OLRA/rec_scan.py, run from the repository by its path with
        python3 -I -B, unedited, over all ninety-seven files, its output to
        OUT/reca_credential_scan.txt. Because the two large tables may give many
        reading-class hits, the report carries totals per class and per file, and the
        full hit list stays in the output file.
    2c. The negative control (I2): a fixture under OUT/fixture/ built for this brief,
        one invented, non-working example of each class the scanner's header names,
        and a second file holding none; one example placed past line 30,000 of a long
        fixture file and one inside a tab-separated row, since SRC holds such files.
        Every class flagged and the clean file silent, or STOP before step 3 with the
        class named.
    2d. A stop-class hit in SRC is the report's first finding, by file, line, class and
        mask; the run continues, since nothing of SRC is committed here.

  STEP 3 — The publication census (read-only on SRC), by helpers kept under OUT/helpers/,
    each run with python3 -I -B. Its output is OUT/12ai_SCRI_RECA.md with one TSV per
    table beside it. In scope means P1's rule: a ~/scratch entry named 12*, retained,
    wr5-b, 10b-close or e_exec_probe.
    3a. Other lanes' scratch entries. The out-of-scope top-level entries as
        s1_toplevel.tsv gives them (quote its header line and say how a row marks
        scope; if no column does, apply P1's rule to the names). Then, for each of the
        ninety-seven files, the count of lines that hold any of those names, by name.
        One table: file by name. The names printed in full, once (E4).
    3b. Paths outside this lane. Every absolute or ~-relative path in the ninety-seven
        files that is not under ~/projects/evidence-engine or an in-scope scratch
        entry: grouped by its first three components, with the count per file and one
        example line number per file. The four home folders, ~/claude-config, other
        directories of ~/projects, ~/scripts and ~/logs are expected groups.
    3c. Citations and quoted text from files this repository does not track. For
        s2_citations.tsv: its header line, its row count, and per source file the
        count of rows, whether the file is tracked at HEAD, and whether the row carries
        the cited line's text or only its number, with the total bytes of such text.
        For 12ai_SCRI.md: each block or sentence that quotes a file not tracked at HEAD
        (P2, P3), by line range and source file, the text not repeated in the report.
    3d. The two large tables, each: bytes, rows, header line and column names; the
        distinct top-level entries its paths fall under, with rows per entry; rows
        whose path is not under an in-scope entry (E3), listed in full if any; the
        rows by kind, as s4_unique_by_kind.tsv names kinds; five first rows and ten
        rows drawn with a stated seed, printed whole; twenty distinct file names of
        the kind the read-out calls paper texts, printed whole; the count of rows
        whose path holds an e-mail address, or the word patient, phi or mrn as a whole
        path component in any case, with those rows listed if any.
    3e. The ignored and untracked names. From g1_mainstatus_before.txt: its line count,
        and every line with the "!!" mark, printed whole (these are names of files the
        main repository ignores). From s3_untracked_ignored.tsv and clone_status/: the
        distinct path names that are not under __pycache__ or .venv, printed whole.
    3f. The patches. For each of the seventeen: bytes, and git apply --numstat's lines
        (run with the patch as input and no --apply; nothing is applied anywhere);
        every touched path not under docs/, tests/, engine/ or scripts/ (E5). The
        untracked JSON: its bytes and top-level keys.
    3g. Bytes and encoding. Every file that is not valid UTF-8 (E2). SRC's bytes by
        group: 12ai_SCRI.md and the list; the two large tables; the other tables and
        gate files; clone_status/; patches/; untracked/. For the two large tables, the
        byte count of each under gzip -9, measured through a pipe to wc -c with no
        file written.
    3h. What the list requires. For each file the read-out's text names as evidence
        (its line 13–19 inventory and its Gates section), whether SHA256SUMS names it.
        Then one sentence: which lines of SHA256SUMS would name no committed file if
        the two large tables alone were left out.

ACCEPTANCE GATES:
  G1. Step 0 matched; every full hash proved.
  G2. Step 1: one commit that passed 1d in one chain, holding Block 215, r817.txt and the
      brief file and nothing else; pushed, level, fingerprint IDENTICAL; the ledger row.
  G3. Step 2: SRC equal to R3 and its list; the scan run on all ninety-seven files with
      its control passed for every class; SRC's mtimes unchanged at the end of step 3.
  G4. Step 3: 3a–3h each written, each count produced by a helper whose path and sha256
      the report gives; every table that lists names is in OUT and in the report and in
      no tracked file; git status clean at the end.

OUT OF SCOPE: copying or committing any file of SRC or of OUT; any judgement on whether
  a name or a line may be published (the PI's); any edit to the interim read-out.
  Deleting, moving or cleaning anything under ~/scratch. Opening any database. The
  restart change, U30, the tools items. Any gate, model call or Ollama restart. /wrap.

CONSTRAINTS (task-only): .venv/bin/python for repository scripts; OUT is the one place a
  helper may write to or delete from (R658(c)). A count CC arrives at by reading rather
  than by a helper is marked "CC's reading". No database file is opened: the tables
  name databases by path only.

FINAL REPORT FORMAT: (0) the pre-check. (1) Item 0: r817.txt's sha256, bytes and lines;
  Block 213 as quoted and block215.json as composed; the plan's numstat and new sha256,
  bytes and lines; the brief file's sha256, bytes and lines; the commit and ledger
  hashes with parents; the fingerprint results. (2) The source check; the control's
  result class by class; the scan's totals per class and per file; any stop-class hit
  first. (3) 3a–3h in order, each as a short table or list, with every other-lane name,
  out-of-lane path group and untracked source file printed in full, and quoted text
  given by line range only. (4) One closing table for the PI, one row per group of 3g:
  files, bytes, what it would publish from outside this lane (from 3a–3e), and the
  scan's result. Then findings, numbered, each with its quoted line, including anything
  in R1–R6, P1–P5 or E1–E6 the box contradicts. Then STOP.

CONTRADICTION HANDLING: a step-0 mismatch, a hash cat-file -e does not prove, I1 false or
  a class the control misses (I2) = STOP where the step says. A check that fails or
  crashes (R763(c)) = STOP before the commit. An expectation mismatch (E1–E6) is
  reported and the run continues. A spec premise that does not survive contact with
  disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not copy or commit anything from SRC or OUT, and do not
  start the restart change or U30. Await the next brief or ruling in this session.
  STOP-12AI-SCRI-RECA
