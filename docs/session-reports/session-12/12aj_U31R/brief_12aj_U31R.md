CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R828, PI ruling 2026-11-23 UTC; not on disk)
================================================================================

R828 is transcribed at item 0 of the brief below, by
docs/session-reports/tools/transcribe_ruling.py, as the next block of the decision log
(Block 226). The lines between the two marker lines, exclusive of the markers, are the
byte content of r828.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's
independent hash:
  r828.txt: sha256 084b9642e59808a5933db6e70d225e0e966bf4617a722d102c4d87fe87511712,
            6391 bytes, 81 lines, one trailing newline.

===== BEGIN r828.txt =====
RULING [evidence-engine 12aj-R828] — U31's Phase A accepted; the PI's word on its eight
  decisions: "package"; U31's design; the two repair scripts retire first, in a commit of
  their own; no standing rule
STATUS: PI ruling, 2026-11-23 UTC, session 12aj, on architect recommendation, on the 12aj-U31A
  final report (item 0 fed257bd169d374b75c5035e1b76da609180ae56; read-out
  8ae879f3a316fda1e74e74d574cb1ec2009ac073). Transcribe at item 0 of the 12aj-U31R brief
  below, by transcribe_ruling.py, in the block after R827's.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12aj-U31A final report, quoted; wrapped lines joined): "Commit:
    fed257bd169d374b75c5035e1b76da609180ae56, parent cd6f9a8…; pushed, 0 0"; "Plan +169 / −0;
    "ends at" lines 180 → 182; six-cell INT- rows 215 / 215"; "Plan now:
    28d8ed36c67d5949675db6318579c498e391dc68716712f197a1d3497297d020, 3,543,659 B, 21,375
    lines."; "Ledger: aeefffa3d3f198ef989d421ca51206de377efc04"; "Commit:
    8ae879f3a316fda1e74e74d574cb1ec2009ac073, parent fed257b…"; "Ledger:
    4cf8d55a6d3eb659e05b6828a21b9012f00b9f78"; "The acceptance test's three statements at
    HEAD: (i) False, (ii) False, (iii) True"; "Seven of the ten shapes are stored"; "T5 last
    span with no value key | completed with NR and stored"; "the real SDK client never
    constructed, neither script imported".
  READ (the committed read-out, 12aj_U31A/12aj_U31A.md, sha256
    04979891d037e7f5a0c92f46ec1800c187649abca9c0587b6b6fba4553b14d45; its blocking answer and
    its §7, §8 and §9 read whole, its Q5 read whole, the rest not read line by line; quoted,
    wrapped lines joined): §7.1 "(a) Drop the span before validation, so its field reads
    missing."; §7.2 "Joining the list with `"; "` would store the codebook's own form, and
    would invent the separator and the claim that the list is the multi-value answer"; §7.3
    "`sample_size` is the one numeric field"; §7.6 "parse a deep copy so the row holds what
    was received"; §7.7 "removing step 9's missing-key repair closes the one path by which a
    repair completes a cut-off span"; §7.8 "It moves: seven collected ids (3,608 → 3,601
    selected)"; Q5 "each script names none" and "Nothing under `engine/` or `analysis/` names
    either."; finding 6, "The local path already refuses these shapes".
  READ (the architect's clone at 8ae879f): the two full hashes in STATUS in a straight chain
    on cd6f9a8; 12aj_U31A/brief_12aj_U31A.md equal to the architect's copy by cmp; seven
    lines of 12ai_close/collected_ids_12ai_close.txt name a script, three of
    tests/test_review_paths.py and four of tests/test_api_parity.py; CLAUDE.md:613–614 "#
    Cloud span backfill (for extractions missing span rows)" and "PYTHONPATH=. python
    scripts/backfill_cloud_spans.py --review surgical_autonomy [--dry-run]"; the plan's rows
    that name extracted_data in their finding, one, INT-j12-51, closed, a different finding.
  INFERRED: none that a clause below rests on. How often a provider returns each shape is
    not measured (the read-out's §9); no clause rests on a rate.

R828(a) — The 12aj-U31A final report is accepted as printed: item 0 fed257b (Blocks 224 and
  225; r826.txt and r827.txt at the architect's hashes; the brief file equal to the
  architect's copy; the rules files untouched), ledger aeefffa; the read-out 8ae879f with
  its census and probe, ledger 4cf8d55. The architect's errors in 12aj now number two: the
  brief's "eleven" branches are twelve steps by the code.
R828(b) — The PI's ruling on the eight decisions put to him from the read-out's §7, in his
  word: "package". Each as recommended, (c)–(j) below. U31's title is the test of scope: the
  cloud parser stops writing a value the model did not give; what it writes besides stays.
R828(c) — A span with no value key (§7.1(a)): it is dropped before validation, so its field
  reads missing and the completeness check re-asks as it re-asks any missing field. It is
  never completed with the sentinel. The parser's test becomes a test on the key; an
  explicit null still yields the codebook's canonical sentinel.
R828(d) — A list, a dict or a bool (§7.2(a)): the span is dropped the same way. A list of
  strings is not joined, on any field: the separator would be the engine's. bool is tested
  before any numeric test.
R828(e) — An int or a finite float keeps str() (§7.3(a)), on any field. A non-finite float is
  dropped as (d).
R828(f) — The snippet patches stay as they are (§7.4): a missing source_snippet key becomes
  the empty string, and the snippet beside an explicit null is cleared.
  test_null_converted_to_nr is not edited.
R828(g) — No record of a repair is added in U31 (§7.5). A column or a token for it is a schema
  change and 023's.
R828(h) — extracted_data holds the response as received (§7.6): the parser works on a copy and
  writes nothing into the object the transport stores. This makes the comment at
  engine/cloud/base.py:440–442 true, as R807(b) had it. The finding has no row in the plan;
  its row is composed at the build's item 0 and closes with U31.
R828(i) — R800(c) is answered: at HEAD a response recorded as truncated is stored when its
  text parses to a complete field set. U31 leaves that as it is (§7.7(a)); with (c) the one
  path by which a repair completed a cut-off span is closed. A response is not refused for
  its finish in U31. A call row that reads error / truncated beside a stored claim goes to
  023 with the outcome-token question.
R828(j) — The retirement (§7.8; R612(o), R31): scripts/reparse_cloud_spans.py and
  scripts/backfill_cloud_spans.py retire in a commit of their own with its own gate, before
  U31's engine fix, under the brief below (12aj-U31R). CLAUDE.md's two lines on the backfill
  script are not in that commit: they are removed at the next item 0 on the PI's approval of
  the text in words. INT-l12-2 closes at the item 0 after the retirement lands.
R828(k) — Then U31's build (12aj-U31B), one engine commit confined to
  engine/cloud/base.py's parser and store and their tests, its acceptance test the entry's
  three statements in the shape of the read-out's §7.12, its pins unmoved. This ruling
  states no standing rule: the id list, the glosses file and STANDING_RULES.md are untouched
  at the item 0 that transcribes it, asserted by content.
===== END r828.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12aj-U31R] — The retirement half of U31 (R828(j); R612(o), R31):
  scripts/reparse_cloud_spans.py and scripts/backfill_cloud_spans.py are removed, with
  the one test-list entry, the two rows of the architecture table, the inventory and two
  retention-ledger rows; one commit with its gate, the evidence in a commit of its own.
  Item 0: R828 as the next block; this brief committed as pasted; no standing rule; no
  row note and no dated line.
PURPOSE: Close the one route by which the cloud response parser's output could be
  written to a review database outside a run, the opt-in and the guards. A removal. No
  engine/ change. No live write. No model call. No provider call. Neither script is run.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
  The session 12aj-U31A ran in.
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  This brief authorizes one transient systemd-run --user unit, gate-12aj_u31r, launched
  by docs/session-reports/tools/launch_gate.sh at step 2f, and nothing else of systemd.
  It authorizes git rm of the two scripts named, and of no other file.
  Touched: scripts/reparse_cloud_spans.py and scripts/backfill_cloud_spans.py (removed);
  tests/test_review_paths.py (one list entry and the list's count comment);
  docs/architecture/modules.md (two table rows marked retired); docs/inventory/
  (regenerated); docs/plan/ENGINE_REFACTOR_PLAN.md (Block 226 at item 0; two rows
  appended to the retention ledger at step 2); 12aj_open/r828.txt; a new directory
  12aj_U31R/; ~/scratch/12aj-u31r/ (new); the claude-config ledger; git push on both
  repos; one standard gate.
  STOP / DO NOT TOUCH (beyond the global list): everything under engine/ and analysis/;
  tests/test_api_parity.py (its cases are generated from the scripts and go with them;
  the file is not edited); every other script; CLAUDE.md (its two lines on the backfill
  script wait for the PI's word, R828(j)); rows INT-j12-18 and INT-l12-2 (no note, no
  close here); review_specs/ (R71); the codebook; the tools; the id list, the glosses
  file and STANDING_RULES.md (not one byte); review.db (the fingerprint instrument only;
  no query on live); any .env or API key; ~/scratch/12aj-open/between_sessions_read.txt;
  the five kept clones (R822(e)); every scratch directory other than ~/scratch/12aj-u31r/
  except where a step names a file to read as a model.

PATHS: a name of the form 12xx_<name>/ is a directory of docs/session-reports/session-12/;
  a tool is named by its full path under docs/session-reports/tools/.

FORM (R784(g)(iv)): for a build with a gate launched as a unit and its evidence beside
  it, 12ai_U30B/brief_12ai_U30B.md (sha256
  b36dbcb1b13c6cc3bf54b79804575815c15da5fcd0a86170afbcf2cb548b9c0a, 465 lines); read its
  Section 2 whole before step 0. For a retirement, the record's last one: commit
  954796edf49b3ff57ae4cde41559811655d22c14 (2026-10-13, 12m-U12-RET), its read-out
  12m_U12/12m_U12_RET.md and its 12m_U12/ret_evidence/. That commit carried its evidence
  with it; here the evidence has a commit of its own (R731(c)). Where a form and this
  brief disagree, this brief governs; where either disagrees with a tool or the record,
  the tool and the record govern and the difference is a finding (R732(a)).

TIMING: issued at 05:54 UTC on Monday 2026-11-23. Valid outside the day's window
  (R812(a)): the next opens at 09:00 UTC today. No edit in the working tree inside a
  day's window. The gate must exit before a window opens: launch by 08:30 UTC, a hold
  included, or after 10:35 UTC. If the clock reaches that bound before the retirement
  commit is gated, hold with the commit unpushed and resume in this brief after 10:35
  UTC (R789(a)); say so in the report. If the clock at step 0 is inside a window or
  within 15 minutes before one opens, STOP at once and write nothing.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12aj-U31A final report, as R828 quotes it in Section 1; 12aj-OPEN-R1 final
    report, as R827 quotes it): HEAD 8ae879f3a316fda1e74e74d574cb1ec2009ac073, parent
    fed257bd169d374b75c5035e1b76da609180ae56, clean, level; claude-config at
    4cf8d55a6d3eb659e05b6828a21b9012f00b9f78; the gate of record on this tree
    12aj_open/gate_12aj_open/ (3,607 passed / 1 xfailed / 17 deselected on 3,625 /
    3,608); band 881–1076 s (R825(b)); rows 289; fix units 39, done 35.
  READ (the architect's clone at 8ae879f, 2026-11-23 05:54 UTC; re-read on the box, not
    trusted):
    R1. The plan at fed257b, unchanged at 8ae879f: sha256
        28d8ed36c67d5949675db6318579c498e391dc68716712f197a1d3497297d020 (the 12aj-U31A
        report's figure; 3,543,659 B, 21,375 lines); last line "The decision log in
        this file ends at R827."; no "**Block 226.**". The retention ledger's heading
        "### Retention ledger (R31)" is at :1627 and its table's last row at :1731.
    R2. The model's ledger row, plan :1717: "| `scripts/rescreen_original_251.py` and
        its 7 generated test cases: 3 `tests/test_review_paths.py` cases and 4
        `tests/test_api_parity.py::test_engine_import_resolves[rescreen_original_251.py::…]`
        entries | R625(a)(6) (R31, R47; 12m-U12-RET) | **Retired 2026-10-13**,
        12m-U12-RET (unit U12). …" ending "Recoverable at
        `9cf9318:scripts/rescreen_original_251.py` |".
    R3. tests/test_review_paths.py:199 is "    "scripts/reparse_cloud_spans.py"," inside
        SPEC_BEARING_ENTRY_POINTS; the list's comment at :164–168 holds "16 after the
        2026-10-13 retirement (22 before it, measured the same way)."; the model's
        dated comment in the list, :190–194, begins "# scripts/ft_screening_smoke_test.py,
        q8_validation.py, …" and ends "Their 18 parametrized cases retire with them
        (R47); see the retention ledger." No other file under tests/ names either
        script.
    R4. docs/architecture/modules.md:455–456: "| `reparse_cloud_spans.py` | Re-parse
        0-span cloud extractions from stored JSON | `--review` |" and "|
        `backfill_cloud_spans.py` | Backfill cloud extractions missing span rows |
        `--review`, `--dry-run` |". A retired row's form, :436: "|
        `run5_extract_and_audit.py` | **Retired 2026-09-25** (R113, 9b cut-over) —
        recoverable at `4756bd2` | — |".
    R5. Seven lines of 12ai_close/collected_ids_12ai_close.txt name a script:
        tests/test_api_parity.py::test_engine_import_resolves[backfill_cloud_spans.py::engine.cloud.base.CloudExtractorBase],
        the same test for reparse_cloud_spans.py with
        engine.cloud.base.CloudExtractorBase, engine.core.review_paths.load_spec_for
        and engine.core.review_paths.spec_path_for, and
        tests/test_review_paths.py::test_entry_point_builds_no_spec_path_of_its_own,
        ::test_entry_point_names_no_hardcoded_review and
        ::test_entry_point_uses_the_resolver, each [scripts/reparse_cloud_spans.py].
        tests/test_api_parity.py builds its cases from each script's own imports
        (_collect_engine_imports, :50–60).
    R6. The committed read-out 12aj_U31A/12aj_U31A.md, Q5 (read whole; not repeated):
        "each script names none" of run_arm's twelve path elements; "Nothing under
        `engine/` or `analysis/` names either."; the references it lists, among them
        docs/inventory/entry_points.json and entry_points.md and CLAUDE.md:614; the
        inventory's command "python -m engine.tools.inventory --write".
    R7. 12ai_close/pin_tuples_close.json is
        aea193540e52487550ddb0121f5491c4cffae2272ecd248c80839f5d262bf24f.
  INFERRED — BLOCKING (read at 2a before any removal; false = STOP):
    I1. The engine tree is unchanged since 7fad72a (git diff --stat 7fad72a HEAD on
        engine tests scripts analysis review_specs, empty).
    I2. Nothing imports or runs either script: no file under engine/, analysis/,
        scripts/ or tests/ imports one or names one, beyond R3's entry and the
        scripts' own lines; no crontab line, no user or system timer or unit and no
        file under dgx-infra's scripts/ names one (read-only reads; the model's
        ret_evidence/i2_regrep_9cf9318.txt and 12m_U12/crontab_l.txt and timers.txt
        are the forms).
    I3. The seven ids of R5 are every collected id that depends on either script:
        collected on the parent and again with both files absent from a scratch
        clone, the two id sets differ by exactly those seven.
  INFERRED — expectation (report a mismatch, continue):
    E1. After the removal the gate collects 3,618, deselects 17 and selects 3,601;
        3,600 passed, 1 xfailed, 2 warnings; against the reference ids, seven only
        in the reference (R5's) and none only in the gate.
    E2. The inventory's entries for the two scripts go and no other entry changes; a
        stamp-only change elsewhere in the two files is the tool's known behaviour
        (R793(e)) and is reported, not corrected.
    E3. Pins by 12l_U04/pin_tuples.py are byte-identical to R7's file; the audit
        prompt_hash is unchanged.
    E4. No test other than R5's seven changes its result, and no test asserts the
        length of SPEC_BEARING_ENTRY_POINTS as a number.
    E5. The 2026-11-23 nightly, if it runs on this tree before the closeout, is read
        against this brief's gate and not against
        12ai_close/expected_nightly_20261120.md, as that file's last paragraph states.
        Nothing is done about it here.

CONTRACT (steps, in order; evidence under ~/scratch/12aj-u31r/):

  STEP 0 — Pre-check (read-only). date -u. HEAD and parent proved by git cat-file -e
    <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. R1's values, with the
    "ends at" lines counted. The three rules files at the hashes 12aj-OPEN's R3 gave.
    claude-config at 4cf8d55 or an accepted descendant (R663(b)), level; a pending
    foreign row carried and named (R654(d)). I1. Fingerprint --compare IDENTICAL. No
    12aj_U31R/ exists; no r828.txt in 12aj_open/. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; the check written for
    this brief, R794(i)). Model, quoted before its counterpart is composed: this
    session's 12aj-U31A item 0, fed257b, with ~/scratch/12aj-u31a/block225.json and its
    item-0 check, read for one block in place of two.
    1a. 12aj_open/r828.txt from Section 1, one write, hashed straight after (R738(b))
        against Section 1's hash. A mismatch = STOP (R739(b)).
    1b. ~/scratch/12aj-u31r/block226.json in block225.json's key set: transcribed = the
        commit's UTC date (R775(a)); item "item 0 of the 12aj-U31R brief"; made_by PI;
        made_date 2026-11-23; session 12aj; report as R828's STATUS names it, "the
        12aj-U31A final report (item 0 <full hash>; read-out <full hash>)", the hashes
        in backticks, in Block 205's form (quote that header first); sentence model,
        followed "12aj-U31R brief"; prev_ends R827; "beside" the brief file, in the
        words of a block transcribed alone with its brief (Block 222's, quoted).
        Render to stdout, compare the element order with the model's, then --append.
    1c. 12aj_U31R/brief_12aj_U31R.md from this whole pasted block, "CLAUDE CODE" through
        the STOP-12AJ-U31R line, one trailing newline, written once; sha256, bytes and
        lines straight after the write, reported.
    1d. The item-0 check, as fed257b's for one block: --check-plan byte-identical; the
        plan's numstat read, its "−" matched to zero; the "ends at" lines counted
        before and after, every earlier one kept; the last line "The decision log in
        this file ends at R828." once; r828.txt equal to 1a's; the three rules files
        byte-equal to HEAD's and unstaged (R777(b)); six-cell INT- rows 215 before
        and after; the staged set exactly the plan, r828.txt and brief_12aj_U31R.md.
        Quote the check.
    1e. <check> && <commit>, one line; the message from the staged set (R778(d)), docs
        scope. Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers, its message built from the diff
        (R801(f)); push; level. Record hashes and parents. This commit is PARENT below.

  STEP 2 — The retirement (one commit).
    2a. Before any removal, read and quote: both scripts' sha256, bytes and lines at
        PARENT; R3's and R4's lines at their present numbers; the retention ledger's
        heading, its last row and the model's row (R1, R2); I2's reads, written to
        ~/scratch/12aj-u31r/i2_regrep.txt in the model's form, with the crontab and the
        timers beside it; R6's reference list re-run by grep over the working tree
        outside docs/session-reports/ and docs/plan/, every hit listed and placed in
        one of: removed with the scripts, edited at 2c, regenerated at 2c, CLAUDE.md's
        (left, R828(j)), or other. A hit placed in "other", or I2 false, = STOP.
    2b. I3: collect on PARENT (no run) to ids_parent.txt; in a scratch clone made by
        this step under ~/scratch/12aj-u31r/, remove the two files and collect to
        ids_without.txt; the set difference both ways to collected_ids_removed.txt.
        EXPECTED R5's seven and nothing else. Any other id in either direction =
        STOP, the ids quoted.
    2c. Edit, and nothing else:
        — git rm scripts/reparse_cloud_spans.py scripts/backfill_cloud_spans.py.
        — tests/test_review_paths.py: R3's entry replaced, in its place in the list,
          by a dated comment in the form of the model's (:190–194): the script's
          path, "retired <the commit's UTC date> under R828(j) (R612(o), R31)", one
          sentence of why from this brief's PURPOSE, and its three parametrized
          cases retiring with it (R47), "see the retention ledger". The list's count
          comment (:164–168) brought to the fact in its own form, the new count
          measured by len() and by collection, before and after quoted.
        — docs/architecture/modules.md: R4's two rows each replaced by a row in
          :436's form, "**Retired <date>** (R828(j); R612(o), R31) — recoverable at
          `<PARENT, short>`".
        — The inventory: python -m engine.tools.inventory --write, then --check in
          sync; the diff read against E2.
        — The plan: two rows appended after the retention ledger's last row, one per
          script, in R2's three-cell form. Cell 1: the script and its generated test
          cases, counted by file from 2b (reparse: three of tests/test_review_paths.py
          and three of tests/test_api_parity.py; backfill: one of
          tests/test_api_parity.py). Cell 2: "R828(j) (R612(o), R31, R47;
          12aj-U31R)". Cell 3: "**Retired <the commit's UTC date>**, 12aj-U31R (unit
          U31).", then what the script did and why it goes, each sentence taken from
          the read-out's Q5 and quoted from it in the report, then "Recoverable at
          `<PARENT, short>:scripts/<name>`". Nothing else in the plan changes: the
          "−" count is read and matched to zero, and six-cell INT- rows stay 215.
        git diff --stat EXPECTED: the two scripts deleted, tests/test_review_paths.py,
        docs/architecture/modules.md, the two inventory files, the plan. Any other
        path = STOP.
    2d. Pins (E3): 12l_U04/pin_tuples.py with both arguments, compared with R7's file;
        the audit prompt_hash. A moved value = STOP.
    2e. Selected runs green: tests/test_review_paths.py and tests/test_api_parity.py
        whole, the inventory's own tests, and any test file 2a's grep placed. A red
        is classified before anything else: a test that fails only because it
        counted the list or named a retired script is coupled, is rewritten to the
        fact with its assertion's purpose kept, and is named in the report; any
        other red = STOP. Commit (chore scope, as the model's message: what was
        removed and why, where each is recoverable, what went with them, the seven
        cases by file), from the staged set, with a pre-commit check that the staged
        set is 2c's and nothing under engine/ or analysis/. Do not push before the
        gate.
    2f. The standard gate, launched as the unit gate-12aj_u31r by
        docs/session-reports/tools/launch_gate.sh as 12ai-U30B's step 2h launched its
        own: evidence_dir ~/scratch/12aj-u31r/gate_u31r/, name 12aj_u31r, ref_ids
        12ai_close/collected_ids_12ai_close.txt, ref_durations
        12ai_close/durations_12ai_u30b.txt, no selection; never a CC background task;
        no tree change while it runs. EXPECTED E1; 1 xfailed; 17 deselected; wall
        inside 881–1076 s, outside it reported with the concurrent jobs, not adapted
        to and not re-run; no phase more than 10 s slower; no model load in the
        gate's journal window (R696(b)); the unit's start line; the nineteen files.
        A red that is coupled in 2e's sense is fixed under 2e's rule and the gate
        re-run once (R746(c), R772(b)); any other red = STOP, the commit standing
        and unpushed, reported.
    2g. Fingerprint --compare; push; level; --compare after (IDENTICAL). Ledger row;
        push; level.

  STEP 3 — Evidence commit (R731(c)). 12aj_U31R/: a read-out 12aj_U31R.md in
    12m_U12/12m_U12_RET.md's form (what was retired, with the per-file case counts;
    what else the commit changed, each before and after; what was left and whose it
    is — CLAUDE.md's two lines, rows INT-l12-2 and INT-j12-18, the expected-nightly
    file; the gate); i2_regrep.txt with the crontab and timers reads;
    collected_ids_removed.txt; gate_u31r/ (the nineteen files) with the launcher's
    stdout and the hold snapshot beside it; pin_tuples_u31r.json. The staged set
    exactly those, every sha256 equal to its scratch source; chained check && commit;
    fingerprint IDENTICAL before and after the push; level; ledger.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1, I2 and I3 held, each beside its quoted output.
  G2. Item 0 one commit that passed 1d in one chain: r828.txt at the architect's hash;
      Block 226 by the tool; the rules files untouched by content; the brief file
      hashed; pushed, level, fingerprint IDENTICAL; ledger.
  G3. One retirement commit whose diff is 2c's paths and no other; both scripts
      recoverable at PARENT, shown by git cat-file -e PARENT:scripts/<name>; no file
      under engine/ or analysis/ changed; tests/test_api_parity.py unedited; CLAUDE.md
      unedited; the ids changed by R5's seven and no other; pins unmoved; the gate at
      E1's counts or the difference reported, no load, nineteen files; pushed, level,
      fingerprint IDENTICAL; ledger.
  G4. The evidence commit as step 3 states.
  Negative control for I2's grep (a check that could pass on nothing): the same
  patterns run at PARENT must find R3's entry and the scripts' own lines; a grep that
  finds neither there is reported UNPROVEN and = STOP before 2c.

OUT OF SCOPE: U31's engine fix and its acceptance test (12aj-U31B); CLAUDE.md; closing
  INT-l12-2 or noting INT-j12-18 (the next item 0); the row for R828(h)'s finding; a new
  expected-nightly file and a new reference id file (the closeout's); whether live
  holds a zero-span row (R807(d)); the three analysis readers; the tools items; the
  between-session file; any other script, however stale it reads; /wrap.

CONSTRAINTS (task-only): no model call, no provider call, no network request, no API key
  read, no live write, no query on live; neither script is run or imported, before or
  after; no edit in the working tree inside a day's window. A count arrived at by eye
  is INFERRED; a computed value is reported with its command.

FINAL REPORT FORMAT: (0) the pre-check; (1) item 0 — block226.json with its models
  quoted, the render comparison, r828.txt's hash, the brief file's sha256, bytes and
  lines, the check, hashes, numstat, the plan's new sha256, bytes and lines,
  fingerprint, ledger; (2) the retirement — 2a's quoted lines and the placed reference
  list; I2 with its negative control; I3's two collections and the seven ids; each
  edit before and after, the two ledger rows whole; the inventory diff against E2; the
  pin recompute; the selected runs; the commit's full hash and parent, its diff stat;
  (3) the gate — what the launcher printed, the unit's start line, the hold, counts,
  the id comparison, wall time against the band, durations, the journal window, the
  nineteen files; (4) the evidence commit; (5) contradictions and findings, numbered,
  each with its quoted line; (6) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, a 1a mismatch, or a check failure or crash at
  1d = STOP before the item-0 commit. I1, I2 or I3 false, an unproven negative
  control, a reference placed in "other", a path outside 2c's in the diff, a moved pin,
  or a red that is not coupled = STOP before the retirement commit. A non-coupled red
  in the gate = STOP, the commit standing, unpushed and reported. A line not found as
  R2–R4 quote it is a finding; the file governs and the edit is made to the line as it
  stands, quoted. An expectation mismatch (E1–E5) is reported and the work continues.
  A spec premise that does not survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not start U31's engine fix, close a row or edit CLAUDE.md.
  Await the next brief or ruling in this session. STOP-12AJ-U31R
