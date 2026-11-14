CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R805, PI ruling 2026-11-14 UTC; not on disk)
================================================================================

R804 and R805 are transcribed at item 0 of the brief below, by transcribe_ruling.py, as
the next two blocks of the decision log. R804 is taken from the committed resume file
(step 1a). The lines between the two marker lines below, exclusive of the markers, are
the byte content of r805.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's hash:
  r805.txt: sha256 28d71b14666662e8e1bddbb3e002dc8f59e8abbb6775e499fc80858d2a91125b,
            5227 bytes, 61 lines, one trailing newline.

===== BEGIN r805.txt =====
RULING [evidence-engine 12ag-R805] — 12ag-OPEN accepted; the series gains 1042.65 s, confounded;
  R667(e) narrowed, identifiers allowed; U30's design ruled on the PI's five decisions; N46
  folded into U30 after a read of its readers; the gate hold a standing rule; two standing rules
STATUS: PI ruling, 2026-11-14 UTC, session 12ag, on architect recommendation, on the 12ag-OPEN-R1
  final report (evidence 60a63314118dc9f9ea41a009e5b5197676922b4c). Transcribe at item 0 of the
  12ag-N46A brief below, by transcribe_ruling.py, in the block after R804's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12ag-OPEN-R1 final report, quoted): "exit 0 at 06:05:39"; "3573 passed, 17
    deselected, 1 xfailed, 2 warnings"; "ids 3,574 against 3,574, 0 / 0"; "Wall time: 1042.65 s,
    inside 872–1065 s."; "no model loads and no requests"; "No-marker collection: 3,591."; "Commit:
    60a63314118dc9f9ea41a009e5b5197676922b4c (parent 16cb307), 34 files, all new, +21,611 / −0";
    "Ledger: 89b4efdadca69b880096a91c8921b6c9a7182dc9 (parent 0663a61)"; "brief_12ag_OPEN_R1.md is
    7bee59aa1972b7647201015ba609d5d5a219ca7d226eb4394ae8b63e71e8b356, 7,250 B, 97 lines" (the
    architect's copy: the same sha256, bytes and lines); "48 items under BLOCKING headings: 31
    VALUE, 17 FACT."; "pytest-905 to -909 were created during the gate, with surgical-cv's
    app_db_template* dirs from 05:49:55 to about 05:58".
  READ (CC's): findings 1–4; the labels of r667e_read.txt, CC's judgement by a fixed rule.
  READ (the architect's clone at 60a6331): gate_12ag_open/gate_wall.txt "wall: 1042.650800830 s";
    12af_U30A/12af_U30A.md §7, options "(S-b)", "(R-c) Both.", "(P-b) Yes", items 4 and 5;
    engine/core/completeness.py:220 "def drop_unexpected(spans, expected: tuple[str, ...]) -> list:".
  INFERRED: none.

R805(a) — The 12ag-OPEN-R1 final report is accepted as printed: the gate at the 12ag block's
  counts, ids 0 / 0, no load in its window; step 3f matched; the evidence commit 60a6331
  (nineteen gate files, the job reads for 2026-11-11 to 11-13 in 12af_close/, r667e_read.txt,
  the resume file equal to the architect's copy), ledger 89b4efd. Session 12ag is open. CC's
  findings 2, 3 and 4 are readings: two slips caught before anything rested on them, and a
  scratch file with no committed counterpart. The architect's errors this session stay two.
R805(b) — The series gains 1042.65 s as its forty-first member, confounded (R677(f)): another
  lane's test suite started inside the gate's window after both snapshots, seen only in the
  temp root. The band in force stays 872–1065 s until the closeout recuts it. That a suite can
  start after the launch joins the cross-lane item for dgx-infra.
R805(c) — The PI's ruling, in his words: "Narrow, and allow identifiers". Rule from here,
  narrowing R667(e): R667(e) applies to INFERRED — BLOCKING items only. A blocking item names
  the fact that must hold, and may name the thing the fact is about by its identifier (a
  commit, a file, a session). A quantity, a time or a line number the brief wants checked is a
  separate reading. An expectation item may carry its value.
R805(d) — U30's design, on the PI's word ("Decision 1-5 agree"), from 12af_U30A/12af_U30A.md
  §7: (1) an interrupted cloud run closes interrupted with its signal's reason, the form the
  local runners use (S-b); (2) the rollback sits in both places: store_result rolls back on any
  exit that did not commit, and run_arm rolls back an open transaction before its close and
  closes only a run not already closed (R-c); (3) send records a provider call that an
  interrupt ends, as an error naming the interrupt, and re-raises — this amends R800(b)'s
  exclusion for interrupts (P-b); (4) the cloud entry point handles SIGTERM and SIGHUP as the
  other mains do; (5) INT-i12-71's first half is closed as answered: no activation is owed.
  The dated notes on INT-i12-71 are written at the U30 build brief's item 0, not here.
R805(e) — N46, the PI's ruling, in his words: "Fold it into U30: one build closes both." A
  read-only look at the legacy cloud table's readers comes first: the 12ag-N46A read-out. No
  design clause for N46 is written before its read-out is accepted and the PI has ruled.
R805(f) — The hold, the PI's ruling: "Yes". Rule from here: before a standard gate is launched,
  when the snapshot names another lane's full test suite or an encode, the launch waits for
  it — one wait, by pid, nothing signalled, bounded at sixty minutes and by the R86 clock. A
  launch made at the bound is reported with the job named and its value enters the series as
  confounded (R677(f)). A gate brief does not restate it.
R805(g) — Order, after the 12ag-N46A read-out: the tools change that lets the gate wrapper
  launch a gate as a systemd-run --user unit (R804(b)), two-phase; then U30's build, one
  brief, from 12af_U30A.md, the N46 read-out and the rulings on them.
R805(h) — This ruling states two standing rules, (c) and (f). The id list gains "A R805(c)" and
  "A R805(f)" and STANDING_RULES.md is regenerated at the item 0 that transcribes this ruling
  (R754(c)(iii), R766(b)). R667(e) stays in the list; R805(c) is read with it.
===== END r805.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ag-N46A] — N46's Phase A read-out: who reads the legacy cloud
  tables, and what an unexpected field name does to each. Item 0: R804 and R805 as the
  next two blocks; the two standing rules of R805 into the id list and STANDING_RULES.md
  regenerated; this brief committed as pasted. Read-only on engine/, scripts/, analysis/
  and tests/; STOP before any edit.
PURPOSE: Put on disk, before N46 is designed into U30, where a field name outside the
  codebook goes on the cloud path and what every reader of the legacy cloud tables would
  do with it. Mixed: one docs commit (item 0), one docs commit (the read-out). No engine,
  test, script, spec or codebook change. No live write. No model call. No provider call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Blocks 202 and 203 appended by the tool,
  nothing else); docs/plan/standing_rules_ids.txt (two lines), standing_rules_glosses.json
  (two entries) and STANDING_RULES.md (regenerated by extract_rules.py); docs/
  session-reports/session-12/12ag_open/ (r804.txt, r805.txt); a new directory docs/
  session-reports/session-12/12ag_N46A/ (brief_12ag_N46A.md at item 0; the read-out, the
  census and the probe with their outputs at step 2); ~/scratch/12ag-n46a/; the
  claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/ (R71), the codebook, docs/architecture/, CLAUDE.md, docs/inventory/, the
  tools (run, never edited); review.db (mode=ro instruments only; no new query on live);
  any .env or API key; every committed read-out and brief; any scratch directory other
  than ~/scratch/12ag-n46a/; the working tree inside 07:00–10:35 UTC (R86).

FORM: 12af_U30A/brief_12af_U30A.md and its read-out 12af_U30A/12af_U30A.md are the forms
  of this brief's steps and of the read-out; census_u30a.py and fake_probe_u30a.py are the
  instruments' forms. Quote each model before composing its counterpart. The rules
  regeneration's model is 12ae-OPEN's item 0, ca9d348 ("A R783(d)", the file regenerated).
  The model for two rulings in one commit is not named: find the record's most recent
  commit that transcribed more than one block, quote its element order and clause form,
  and compose from it (R732(a)).

TIMING: issued at 06:30 UTC on 2026-11-14. Valid outside 07:00–10:35 UTC. If the clock at
  step 0 is inside 06:30–10:35 UTC, STOP at once, write nothing, and say so; the brief is
  then run as pasted after 10:35 UTC. If the clock reaches 06:45 UTC before a commit on a
  later day, hold with the work on disk uncommitted and resume after 10:35 UTC (R789(a)).

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ag-OPEN-R1 final report; R805): evidence-engine HEAD
    60a63314118dc9f9ea41a009e5b5197676922b4c, parent
    16cb307164c9203c695c3a4f842cf6d294c771ad; claude-config at
    89b4efdadca69b880096a91c8921b6c9a7182dc9 or an accepted descendant (R663(b)).
  READ (the architect's clone at 60a6331, 2026-11-14 06:28 UTC; re-read on the box):
    R1. The plan: sha256 9195c82d8571a6844a5f6428061ca107732a6cd8ac681daaa420b5b60a3b45f6,
        3205501 bytes, 18783 lines; last line "The decision log in this file ends at
        R803."; last block 201; 158 lines begin "The decision log in this file ends at R".
    R2. The three rules files as 12ag-OPEN's 0c read them (STANDING_RULES.md 0be5aa94…8d5f,
        the id list 0657ba83…cd62 with last A line "A R784(g)", the glosses d0dd6dc6…8ac0,
        43 entries, a JSON object keyed by clause id). The id list holds "A R667(e)".
    R3. R804's text is on disk only inside 12ag_open/brief_12ag_OPEN_R1.md, between its
        "===== BEGIN r804.txt =====" and "===== END r804.txt =====" lines; that content
        hashes to d2ccb5ef7d253a04f541e37f790fbb5632a96863aa4b80ac623ff6ddc54b681e, 4625
        bytes, 55 lines (R805's own hash is in Section 1).
    R4. engine/core/completeness.py: enforce_completeness logs "unexpected field(s) %s
        dropped (R118)" (:214) and returns; "def drop_unexpected(spans, expected:
        tuple[str, ...]) -> list:" is at :220, its docstring "R118: the spans whose field
        was asked for. An unexpected field is the response inventing a field; it is
        dropped before any write". engine/cloud/base.py: enforce_completeness is called
        at :422, and store_result's loop "for span in spans:" (:461) inserts into
        cloud_evidence_spans.
    R5. A grep of engine scripts analysis for "cloud_evidence_spans", migrations and
        INSERTs left out, names these readers: engine/cloud/schema.py;
        scripts/backfill_cloud_spans.py; scripts/reparse_cloud_spans.py;
        analysis/paper1/adjudication.py:58; analysis/paper1/judge_provenance.py:306;
        analysis/paper1/spanloss_autopsy.py:139 and :176;
        analysis/paper1/pi_audit_sampler.py:272; analysis/eval/analyze_schema_eval.py:118;
        analysis/provenance/census.py:77. The architect read the matching lines only,
        not what each does.
    R6. 12af_U30A/12af_U30A.md Q5: "An unexpected field therefore reaches
        `cloud_evidence_spans` and never reaches the claims." The plan has no table row
        beginning "| N46 "; where N46 is recorded is not known to the architect.
  INFERRED — BLOCKING (verify before anything downstream; false = STOP):
    I1. The engine tree is unchanged since 977c499 (git diff --stat on engine tests
        scripts analysis review_specs empty).
    I2. transcribe_ruling.py --append only appends (the "−" count read from the numstat).
    I3. extract_rules.py, run in ca9d348's form with the two ids added, changes
        STANDING_RULES.md only by the two new clauses, its header lines and "Named by"
        lines that now name R805.
  INFERRED — expectation (report a mismatch, continue):
    E1. The cloud store does not call drop_unexpected, and the local path does.
    E2. On a scratch review, a cloud store given one span with a name outside the
        codebook leaves that span in cloud_evidence_spans, logs it as dropped, and
        writes no claim for it.
    E3. Nothing is predicted about what any reader in R5 does with such a row, or about
        whether cloud_extractions.extracted_data carries the name: to be measured.

CONTRACT (steps, in order; evidence under ~/scratch/12ag-n46a/):

  STEP 0 — Pre-check (read-only). date -u (TIMING). HEAD and parent proved by git
    cat-file -e; clean; fetch; 0 0; one worktree. R1, R2, R3. claude-config: the MEASURED
    commit or an accepted descendant, level; a pending foreign row carried and named
    (R654(d)). I1. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; the check written for
    this brief, R794(i)).
    1a. docs/session-reports/session-12/12ag_open/r804.txt: the lines between the two
        marker lines of R3's file, exclusive, extracted by a command and never retyped,
        one write, hashed straight after against R3's hash. r805.txt beside it, from
        Section 1, one write, hashed straight after against Section 1's hash. A mismatch
        on either = STOP (R738(b), R739(b)).
    1b. block202.json (R804) and block203.json (R805) in block201.json's key set
        (~/scratch/12ag-open/), the two-block wording from the pattern commit the FORM
        section has CC find. R804: made_date 2026-11-14, session 12ag, report as its
        STATUS names it, the 12ag-OPEN stop report with its item-0 hash. R805: made_date
        2026-11-14, session 12ag, report the 12ag-OPEN-R1 final report with its evidence
        hash. transcribed = the commit's UTC date; followed "12ag-N46A brief"; prev_ends
        R803 then R804. Render both to stdout, compare the element order with the
        pattern's, then --append in order.
    1c. The standing rules (R805(h); model ca9d348, quoted first): append "A R805(c)" and
        "A R805(f)" to the id list in its Tier A form; add one gloss for each to the
        glosses file, composed by CC in that file's form from the clause's own words and
        quoted in the report; regenerate STANDING_RULES.md by extract_rules.py with the
        arguments ca9d348's regeneration used, read from that record and the tool's
        docstring. Report the header's counts before and after (to be measured), the
        diff's numstat, and every changed line that is not one of I3's three kinds.
    1d. The brief file (R784(g)(v)): 12ag_N46A/brief_12ag_N46A.md from this whole pasted
        block, "CLAUDE CODE" through the STOP-12AG-N46A line, one trailing newline,
        written once; sha256, bytes and lines straight after the write, reported.
    1e. The item-0 check: --check-plan byte-identical; the plan's numstat read ("−"
        matched to zero); every earlier "ends at" line kept; the last line "The decision
        log in this file ends at R805." once; r804.txt and r805.txt equal to 1a; the id
        list equals HEAD's plus the two lines, nothing else; the glosses file equals
        HEAD's plus the two keys, nothing else; STANDING_RULES.md holds a section for
        each of the two ids, each clause's text found in r805.txt (asserted by content,
        R777(b)); I3; six-cell INT- rows 213 before and after; the staged set exactly the
        plan, the three rules files, r804.txt, r805.txt and the brief file. Evidence by
        a redirect inside the check; quote the check.
    1f. <check> && <commit>, one line; the message from the staged set (R778(d)), docs
        scope. Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers, its message built from the diff
        (R801(f)); push; level. Record hashes and parents.

  STEP 2 — The read-out (read-only; no server; no provider; no live database). Every code
    claim beside a quoted line with its path and line at HEAD, labelled CODE ORDER or FAKE.
    2a. §1 Inputs, quoted: N46 wherever the plan records it, whole, with every dated note
        and its owner (R6); R118; R802(c)'s finding 5; 12af_U30A.md Q5 and F4; U28's
        carry of N46, from its read-out.
    2b. Q1 — THE WRITE. From the response to the tables, in code order on the cloud path
        and, beside it, the local path: where the expected names come from; what
        enforce_completeness does with an unexpected name; every caller of
        drop_unexpected (E1); what store_result inserts into cloud_extractions
        (extracted_data included) and cloud_evidence_spans; what legacy_record passes to
        the claims. A table: one row per store, unexpected name kept or dropped.
    2c. Q2 — THE READERS. A census by script (patterns named in the read-out) of every
        read of cloud_evidence_spans and of cloud_extractions.extracted_data in engine/,
        scripts/, analysis/ and tests/, R5 as its starting list and not its limit. For
        each reader, quoted: whether it selects rows by a field name, counts rows, or
        takes every row; what a row with an unexpected name would change in its output;
        and whether it is on a path a run or an export reaches, or is a one-off script.
        The class of each reader is CC's judgement and is marked so; the counts are
        computed.
    2d. Q3 — WHAT EXISTS. Whether any committed instrument or report already states how
        many rows on live carry a name outside the codebook. No new query on live: if
        none states it, say so under "what could not be established".
    2e. Q4 — THE PROBE (FAKE, on a throwaway scratch review, in fake_probe_u30a.py's
        form): one cloud store with one unexpected span beside a complete expected set.
        Report the rows in each table, extracted_data's keys, the log line, the claims.
    2f. Q5 — U30's SEAM. Where a fix would sit beside R805(d)'s design: in store_result
        before its INSERTs, or earlier; whether dropping changes any pin, any request
        byte or any stored hash; the tests that pin today's behaviour (census).
    2g. Options for the ruling, each with what it records and what it costs, nothing
        decided: at least "drop before any write, as R118's docstring states" and
        "refuse the response"; what each does to extracted_data; the acceptance test's
        shape, failing on the unmodified tree; what N46 would still leave open.
    2h. Findings, numbered; then what could not be established read-only.
    2i. The read-out commit: 12ag_N46A/12ag_N46A.md with the census and the probe and
        their outputs; the check asserts the staged set and reads its numstat; <check>
        && <commit>; fingerprint before and after the push; the ledger row. Record
        hashes and parents.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 verified.
  G2. Item 0: one commit that passed 1e in one chain; both ruling files equal to their
      hashes; the two ids in the list and in STANDING_RULES.md; pushed, level,
      fingerprint IDENTICAL; the ledger row.
  G3. The read-out committed with every reader of 2c quoted and classed, E1 and E2
      answered from the code and the probe, and 2g's options written with nothing
      decided.

OUT OF SCOPE: any design or edit for N46 or U30; the notes on INT-i12-71 (R805(d)); the
  tools change (R805(g)); any gate; any query on live; /wrap.

CONSTRAINTS (task-only): no live write, no model call, no provider call. Any pytest run
  passes --basetemp under ~/scratch/12ag-n46a/. Nothing deleted outside that directory.
  A count arrived at by eye is INFERRED; a computed value is reported with its command.
  Where this brief describes a tool or a record, the tool and the record govern and a
  difference is a finding (R732(a)). A tempting fix is a finding, not an edit.

FINAL REPORT FORMAT: (0) the pre-check table and the clock; (1) item 0 — the two ruling
  files' hashes, both block files, the pattern commit as found and quoted, the rules
  regeneration (the two glosses, the header before and after, the numstat, any line
  outside I3), the brief file's sha256, bytes and lines, the check verbatim, hashes and
  parents, the plan's new sha256, bytes and lines, the ledger commit; (2) the read-out's
  path and commit, its Q1 table, the reader census as a table, the probe's result, the
  options by name; (3) E1–E3; (4) findings, numbered, each with its quoted line,
  including anything in R1–R6 the box contradicts; (5) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 false, a ruling file off its hash, I2
  false, or a check failure or crash = STOP before the commit it guards. I3 false = STOP
  before the item-0 commit, the changed lines reported. An expectation mismatch is
  reported and the run continues. A spec premise that does not survive contact with
  disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not design or build. Await the next brief or ruling in
  this session. STOP-12AG-N46A
