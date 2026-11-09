CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R795, PI ruling 2026-11-09 UTC; not on disk)
================================================================================

R795 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r795.txt: sha256 4d4ad734617766f8e7163edf22f147137624ba1d82029b3989f949c9397723bb,
            11764 bytes, 131 lines, one trailing newline.

===== BEGIN r795.txt =====
RULING [evidence-engine 12ae-R795] — 12ae-U28B accepted: U28 landed (D, B, C), B9 and C28 wait for
  U40; the series gains 963.43 s; readings, one the architect's; session 12ae closes now on U28 —
  the 12ae-CLOSE brief follows; no closeout gate; the band figure computed at the closeout
STATUS: PI ruling, 2026-11-09 UTC, session 12ae, on architect recommendation, on the 12ae-U28B
  final report (item 0 6bfbe36; engine 24ce26bc3a39cd28ad0f38a71f4c4b6b31a2f39c; evidence
  b14d0da8cb40f3830f9443ac377cfda616e6a706). Transcribe at item 0 of the 12ae-CLOSE brief below,
  by transcribe_ruling.py, in the block after R794's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the report):
  MEASURED: "12ae_U28B/brief_12ae_U28B.md is c67c1cf27a613419c0baafeca01a2a1fc60906841b9f87994f8a37e65b463fb2,
    37,552 B, 457 lines" (the architect's copy: equal); "Plan +188 / −2; the "−" lines are exactly
    B9's and C28's rows"; "5 failed, 2 passed in 5.64s"; "base.py 13 / 0; distribution_monitor.py
    11 / 0; CLAUDE.md 3 / 3; pipeline.md 2 / 2; test_cloud_extraction.py 18 / 29; the new test 160
    / 0; entry_points.json 9 / 9; entry_points.md 4 / 4"; ""line": 509 → 520 … 526 → 535";
    "pin_tuples_u28b.json is aea19354…f24f, cmp byte-identical to pin_tuples_u27b.json"; "163
    passed"; "Wall 963.43 s, inside 837–1021 s"; "==== 3544 passed, 17 deselected, 1 xfailed, 2
    warnings in 961.57s (0:16:01) ===="; "collected 3562 items / 17 deselected / 3545 selected";
    "only-gate 8 (the 7 new ids plus …::test_an_empty_value_is_refused_at_the_write_boundary);
    only-ref 1 (…::test_empty_string_normalized_to_null)"; "none slower by more than 10 s"; "--
    No entries --"; "/api/ps is {"models": []} both times"; "surgical-cv uvicorn (pid 2021661,
    restarted 20:00:49) only, before and after"; "all nineteen present"; "21 files, all added";
    "Ledger 84fd685 … 422df0b … b108211".
  READ (CC's): findings 1–7; "extractor.py:541–542: cb = load_codebook(codebook_path) / return
    cb.escape_token, frozenset(s.strip().upper() for s in cb.absence_sentinels)"; ":525: return
    load_codebook(codebook_path).raw.get("contract_unmet_token")"; "base.py:91 self._codebook =
    load_codebook(self._review_dir / CODEBOOK_FILENAME)"; "anthropic_extractor.py:108–113: if
    span.get("value") == "": span["value"] = None; span["notes"] = "empty_string_to_null""; "Paper
    4: store_result failed: … study_type (BLANK_VALUE)"; "claude-config 422df0b, at 20:48:04
    during the gate, has the message "ledger: rows through 2026-11-09T20:41:46Z" and carries only
    my row … It has no trailers"; "test_distribution_collapse_not_caught calls
    extractor.run(max_papers=0), which processes papers because if max_papers: treats 0 as
    unlimited".
  INFERRED: none.

R795(a) — The 12ae-U28B final report is accepted as reported: item 0 6bfbe36 (Block 192; the B9 and
  C28 notes; the dated units line with U40 entered; the brief file byte-equal to the architect's
  copy, the eleventh match of eleven; the item-0 check written for its brief), the engine commit
  24ce26b (eight files: D's two keyword arguments, B's summary rule and CLI line, C's guard call
  and import, the five docs lines as authored, the rewritten test and the docstring word, the
  inventory regenerated under R786(b) for moved recorded lines; the zero-value branch, the two
  protected monitor tests, effective.py, scripts/ and the transports untouched), the gate 3,544 /
  1 / 17 on 3,562 / 3,545 at 963.43 s inside 837–1021 s, only-gate the 7 new ids plus the renamed
  test's new id, only-ref its old id, no load, uvicorn the one concurrent job, the nineteen files,
  green at the first run; the evidence commit b14d0da; ledgers 84fd685, 422df0b and b108211. Both
  pins and the audit prompt_hash byte-identical. U28 is landed; P1's fix units 38, done 31
  (U01–U28, U37, U38, U39). I3 held: the three tokens come from the codebook on both paths. The
  acceptance tests tests/test_u28_cloud_gate_and_boundary.py (5 cases red on the parent 6bfbe36
  in 12ae_U28B/b0_parent_acceptance.txt, 2 preservation cases green on both; green in
  12ae_U28B/gate_u28b/) are U28's acceptance. B9 and C28 stay open for U40 (R794(b), (h)); their
  notes at the next item 0 record U28's landing and these tests, not a close.
R795(b) — The series on this host state gains 963.43 s: thirty-seven. The band in force stays
  837–1021 s (R783(b)) until the wrap ruling sets 12af's.
R795(c) — Readings, no row: finding 1 (E3 was the architect's — two insertions above recorded
  entries move their lines, as U25 and U26 showed; the expectation was written against the
  previous unit's stamp-only result, not against the edits' positions; regenerated under R786(b)
  as R794(g) provided); finding 2 (the empty-string normalization branch is in
  anthropic_extractor.py:108–113, not store_result — the architect placed it from the test's name;
  it is reachable and its output is now refused at the boundary as BLANK_VALUE, so an empty value
  on the Anthropic path is refused twice over; housekeeping: the branch is removed at U40's build
  if anthropic_extractor.py is among U40's files, else it joins P7 — a dated note on C28 at the
  next item 0 records where it is); finding 3 (a test-file slice that swallowed two neighbouring
  tests, caught by the diff stat before any commit — the "−" count read and matched is the check
  that caught it; no rule); finding 4 (the engine check's "−" expectation replaced by content
  equality — the stronger form; kept for the next build); finding 5 (claude-config 422df0b: another
  session's ledger commit carried this lane's engine row, with no trailers; the row and message are
  true; pushed, no amend (R680(b)); the mirror of R793(e) finding 4 — the ledger is one shared
  file, and whichever session commits next carries what is pending; recorded, no rule); finding 6
  (test_distribution_collapse_not_caught's max_papers=0 processes papers, confirming C28's text on
  the if max_papers: incidental, which keeps its owner, R598(b)); finding 7 (E1 and E2 held).
R795(d) — Session 12ae closes now on U28. Nothing is in flight: five units landed (U24, U25, U26,
  U27, U28), three rows closed (C49, INT-i12-31, C56), two opened (INT-ae12-1, INT-ae12-2), one
  unit entered (U40), and the next step — U40's Phase A — is a fresh read that belongs to a fresh
  session. The 12ae-CLOSE brief follows 12ad-CLOSE's form read from the record (the 12ad closeout
  block at docs/plan/ENGINE_REFACTOR_PLAN.md "Session 12ad closed 2026-11-02 UTC" and
  docs/session-reports/session-12/12ad_close/): item 0 (this ruling by transcribe_ruling.py; the
  B9 and C28 landing notes and C28's note of (c) finding 2; the dated units line; no standing rule
  — this ruling states none, so the id list, the glosses file and STANDING_RULES.md are untouched,
  asserted by content, R777(b)), the counterpart files in 12ad_close/'s names with 12ae's sources,
  the closure paragraph, the "Expected values at session-12af open" block, the lane line written
  and not applied, the closeout block in the record's element order, the rNNN.txt table r783–r795.
R795(e) — No closeout gate: the expected gate values for 12af are U28's gate (12ae_U28B/gate_u28b/:
  3,544 passed / 1 xfailed / 17 deselected, collected 3,562 / selected 3,545, its collected_ids as
  the reference, its durations file as the comparison). The engine tree changed last at 24ce26b, so
  under R749(b) the closeout writes its own expected nightly file,
  12ae_close/expected_nightly_20261110.md, in 12ad_close/expected_nightly_20261102.md's form with
  U28's counts, and it governs the 2026-11-10 nightly and, while the tree stays unchanged,
  2026-11-11's. The 2026-11-09 07:00 health job and 09:00 nightly have run (the closeout runs after
  10:35 UTC): the closeout reads them as R749(b)–(c) and R779(c) place such reads — the HEAD each
  ran on by the reflog method with the lock line; the nightly's counts and ids against the gate of
  that tree (the 12ae unit whose engine commit was HEAD at 09:00, named from the reflog, its
  collected_ids.txt the comparison); the health job's loads and outcome with the NVRM grep —
  readings. The 2026-11-02 to 11-08 files read at 12ae-OPEN stay in 12ad_close/ and are not
  rewritten.
R795(f) — The band figure: computed by CC at the closeout from the last four standard gates on this
  host state, read from the record (the series' last four members: 921.98 (U25), 945.63 (U26),
  1059.11 (U27, confounded by the surgical-cv full suite, R793(c)), 963.43 (U28)), ±10% of their
  mean, rounded inward as R683(c) rounded, stated as a computation with its proposed figure in
  12ae_close/band_computation.txt, each gate's concurrent jobs described from its evidence
  (R748(b)); the confounded member named as such beside its value (R677(f)); it is set by the wrap
  ruling. The architect's arithmetic, to be matched, not copied: mean 972.5375 s, ×0.9 875.28375,
  ×1.1 1069.79125, inward 876–1069 s. The band in force stays 837–1021 s until then.
R795(g) — The recount script is written at this closeout in R651(a)'s chained form
  (12ad_close/recount_12ad_close.py as the model, read), extended to read section "L (12ae
  additions, R785)" as the 12ad table is read, its output the recount of record: 286 rows (94 / 56
  / 136, 0 unclassed) expected — 284 at 6cb4ab6 plus INT-ae12-1 and INT-ae12-2, each Class 3, as
  12ad's three L-section rows were counted by R782(g); P1, P6 and P7 as the script reads them; fix
  units 38, done 31; R510 for 12ae: rows in 2 (INT-ae12-1, INT-ae12-2), rows out 0, no class or
  package move; C49, INT-i12-31 and C56 CLOSED and counted. The dated units lines of this session
  that read "Rows 284 (94 / 56 / 134, 0 unclassed) unchanged" after the two rows were opened were
  the architect's carry-forward error, written from the previous count rather than from a count:
  not rewritten (the record stands); the closeout's dated line states the measured count and names
  the error, and the closure paragraph records it as the architect's. A count other than 286 for
  the rows, or other than 2 / 0 for R510, is a stop before the closeout commit, both values
  reported.
R795(h) — The stops, for the closure: listed by CC from the rulings on disk and this brief's item 0
  (each stop ruling names its step and its cause), never from this ruling's memory; the
  architect's and CC's counted apart; a pause at the R86 window that resumed in the same brief
  (12ae-U25B's item 0) is listed as a pause, not a stop. The architect reads one stop this session
  (12ae-U24B before its first edit, R786, the architect's — the inventory question); the record
  decides.
R795(i) — Carried to the wrap ruling and the plan: the tools items (R752(c), R765(d), R766(d),
  R784(d), R791(c) finding 4, R793(e) finding 3 — six, for one tools change); the standing rules
  added this session (R783(d), R784(g); STANDING_RULES.md 87 / 8 / 95); U40 entered after U28 and
  before U29 (R794(b)); INT-ae12-2 (P7); the housekeeping of (c) finding 2; the two shared-ledger
  readings (R793(e) finding 4, (c) finding 5). The 12ae lane line is drafted in 12ad's form with
  12ae's sources and written to ~/scratch/12ae-close/lane_line_12ae.txt, not applied; the wrap
  ruling approves it.
R795(j) — Timing: the closeout runs now, outside 07:00–10:35 UTC; if the clock reaches 06:30 UTC on
  2026-11-10 before the closeout commit, STOP with the work on disk uncommitted and report. No
  model call, no provider call, no live write (mode=ro instruments only); the fingerprint
  IDENTICAL before and after the closeout push. This ruling states no standing rule.
===== END r795.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ae-CLOSE] — Close session 12ae on U28, in 12ad-CLOSE's form read
  from the record: item 0 (R795 as the next block; the B9 and C28 landing notes and C28's
  housekeeping note; the dated units line with the measured count; this brief committed as
  pasted; no standing rule), then the closeout commit (the closure paragraph, the
  "Expected values at session-12af open" block, 12ae_close/'s measurements, the expected
  nightly file, the band computation, the recount, R510, retention, the 2026-11-09 jobs
  read, the lane line drafted and not applied). Docs only. No engine edit. No live write.
  No model call. No closeout gate (R795(e)).
PURPOSE: Leave the record in the state the next session opens on: every fact of 12ae in the
  plan's closeout block with its evidence file beside it, the expected values for 12af's
  open stated from U28's gate, and the band figure computed for the wrap ruling.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 193; the B9 and C28 notes; the dated
  units line; then the closeout block and the 12af expected-values block); docs/session-
  reports/session-12/12ae_open/r795.txt; a new directory docs/session-reports/session-12/
  12ae_close/ (the counterpart files); docs/session-reports/session-12/12ae_CLOSE/
  brief_12ae_CLOSE.md (at item 0); ~/scratch/12ae-close/ (the lane line draft and
  scratch); the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/, the codebook, docs/architecture/, CLAUDE.md, docs/inventory/, the tools;
  the id list, the glosses file and STANDING_RULES.md (not one byte — R795 states no rule);
  12ad_close/ and every earlier close directory (read, never rewritten — R795(e)); the
  dated units lines already in the plan (R795(g): not rewritten); the lane line in the
  project primer (drafted to scratch, applied only by the wrap ruling); review.db (mode=ro
  instruments only); the working tree inside 07:00–10:35 UTC (R86); the clock (R795(j)).

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ae-U28B report; R795): HEAD b14d0da8cb40f3830f9443ac377cfda616e6a706, parent
    24ce26bc3a39cd28ad0f38a71f4c4b6b31a2f39c, clean, level; plan ends "…ends at R794.", last
    block 192; STANDING_RULES.md 87 / 8 / 95, glosses 43, the id list's last A line
    "A R784(g)"; claude-config at b108211 or an accepted descendant (R663(b)); the session
    opened on 6cb4ab6 (the 12ad closeout commit); U28's gate 12ae_U28B/gate_u28b/: 3,544 /
    1 / 17 on 3,562 / 3,545, 963.43 s; pins 12ae_U28B/pin_tuples_u28b.json. Architect's hash
    of r795.txt: Section 1.
  READ (the record): R1. The 12ad closeout block, docs/plan/ENGINE_REFACTOR_PLAN.md from
    "Session 12ad closed 2026-11-02 UTC" through the end of "Expected values at session-12ae
    open" — the form, element by element, quoted before each element is composed. R2.
    12ad_close/'s file list (the counterpart names), band_computation.txt,
    recount_12ad_close.py, expected_nightly_20261102.md, lane_line_12ad.txt and
    retention_close.txt — the forms. R3. The rulings r783.txt–r794.txt in 12ae_open/ and
    12ad_close/r783.txt — the stops, the readings and the rows, listed from them (R795(h)).
    R4. The session's commit list from git log 6cb4ab6..HEAD, each hash read, its ledger
    commit found in ~/claude-config's log by its row.
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 24ce26b. I2. The 2026-11-09
    07:00 health job and 09:00 nightly have run and their logs and journal windows are
    readable (R795(e)); if either did not run, that is reported as the reading and the
    expected-values block says so.
  INFERRED — expectation: E1. The recount reads 286 (94 / 56 / 136, 0 unclassed), P1's fix
    units 38 done 31, R510 in 2 / out 0 (R795(g)); E2. The band computation reproduces
    876–1069 s from 921.98, 945.63, 1059.11, 963.43 (R795(f)); E3. git diff --stat 6cb4ab6
    HEAD -- engine tests scripts analysis review_specs lists the files of the six engine
    commits (94ab7a8, 1f6bc16, 168199a, 69e9db8, 077cb0f, 24ce26b) and nothing else; git
    diff --stat 24ce26b HEAD on the same paths is empty; E4. Retention 23 MATCH / 0 MISMATCH
    by file sha256, none opened, no new entry (12ae ran no rehearsal and no probe with a
    server); E5. The fingerprint IDENTICAL; dbchecks_close.txt and pin_tuples_close.json
    byte-identical to 12ad_close/'s.

CONTRACT (steps, in order; evidence under ~/scratch/12ae-close/):

  STEP 0 — Pre-check (read-only). date -u (outside 07:00–10:35 UTC; R795(j)'s 06:30 UTC
    limit noted). HEAD and parent proved by git cat-file -e <hash>^{commit} (R783(d));
    clean; fetch; 0 0; one worktree. Plan: last line "…ends at R794.", last block 192, no
    Block 193. Rules files: the three counts above. claude-config: b108211 or an accepted
    descendant, clean, level. I1. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12ae-U28B's item 0;
    the check written for this brief).
    1a. 12ae_open/r795.txt from Section 1; hash straight after the write: EXPECTED
        4d4ad734617766f8e7163edf22f147137624ba1d82029b3989f949c9397723bb, 11764, 131.
        Mismatch = STOP.
    1b. block193.json in block192.json's key set: tag R795; item "item 0 of the 12ae-CLOSE
        brief"; beside = "the B9 and C28 notes, the dated units line and the brief file it
        orders" (composed; quoted); made_date 2026-11-09; session 12ae; report = the
        12ae-U28B final report (item 0 6bfbe36; engine 24ce26b; evidence b14d0da),
        final-report form; sentence model, followed "12ae-CLOSE brief"; prev_ends R794.
        Render to stdout, compare with Block 192's order, then --append.
    1c. Notes (R795(a), (c)), each appended at the end of its row's status cell in that
        row's note form: (i) B9, "*Note 2026-11-09 (12ae-CLOSE item 0, R795(a)):*" — U28
        landed at 24ce26b (D and B delivered); the acceptance tests named with their b0 and
        gate paths; the row stays OPEN for U40 (R794(b)). (ii) C28, "*Note 2026-11-09
        (12ae-CLOSE item 0, R795(a), R795(c)):*" — U28 landed at 24ce26b (C delivered); the
        tests as (i); the empty-string normalization branch is anthropic_extractor.py:108–113,
        reachable, its output refused at the boundary as BLANK_VALUE since U28; removed at
        U40's build if that file is among U40's files, else P7 (R795(c) finding 2); the row
        stays OPEN for U40. The dated units line after 12ae-U28B's, in its form: U28 landed
        at 24ce26b; B9 and C28 open for U40 (R794(b), R795(a)); fix units 38, done 31
        (U01–U28, U37, U38, U39); rows as the recount of step 2 measures them — write this
        line AFTER step 2c's recount has run on the item-0 candidate plan (run it on the
        working tree first, read the count, then write the line with the measured count and
        the sentence "the earlier 12ae dated lines' 'Rows 284 … unchanged' was the
        architect's carry-forward error (R795(g))"); rows out 0. Two rows replaced, one
        line inserted; the "−" count read and matched to two.
    1d. The brief file (R784(g)(v)): 12ae_CLOSE/brief_12ae_CLOSE.md from this whole pasted
        block, "CLAUDE CODE" through the last HANDBACK line, one trailing newline, written
        once; sha256, bytes, lines straight after the write, reported.
    1e. The item-0 check, written for this brief: --check-plan byte-identical; the plan's
        numstat read ("−" matched to the two rows); every earlier "ends at" line kept; last
        line "…ends at R795." once; the plan equals HEAD's plus the two rows replaced, the
        line inserted and Block 193 appended, nothing else (computed); each row's last note
        begins with its 1c tag; r795.txt equal to 1a; the three rules files unchanged and
        unstaged; the brief file hashed; the staged set exactly the plan, r795.txt and the
        brief file. Evidence by redirect; quote the check.
    1f. <check> && <commit>, one line; message from the staged set (R778(d)), docs scope.
        Fingerprint --compare before the push; push; level; --compare after (IDENTICAL).
        Ledger row with trailers (foreign pending rows named if any); push; level. Record
        hashes and parents.

  STEP 2 — The closeout commit (one evidence-engine commit, one ledger commit). Every
    element in 12ad-CLOSE's order (R1), composed from this session's sources, each with its
    file under 12ae_close/ in 12ad_close/'s counterpart name (R2); a name with no 12ae
    source is listed as not written, with the reason, as 12ad's "Counterpart files"
    paragraph did.
    2a. Measurements, read-only, to 12ae_close/: host_state_close.txt (the four band values,
        boot, Ollama service start and NRestarts, earlyoom state and kill lines this boot,
        the NVRM grep this boot — earlyoom_grep_20261109.txt, nvrm_this_boot_20261109.txt);
        temp_root_close.txt (entries and bytes, nothing emptied); ollama_reads.txt
        (version, tags, ps, timestamped); fingerprint_close.txt (--compare, IDENTICAL);
        dbchecks_close.txt by 12f_repro/dbchecks.py (cmp to 12ad_close/'s — E5);
        pin_tuples_close.json by 12l_U04/pin_tuples.py (cmp to 12ad_close/'s and to
        12ae_U28B/pin_tuples_u28b.json — E5); codebook_spec_sha256.txt (the live spec and
        codebook, EXPECTED unchanged: aeb90ba8…8b33 and 89dbfa91…ad82 — matched to
        12ad_close/'s file); diff_engine_since_6cb4ab6.txt and diff_since_24ce26b.txt (E3;
        the file names carry this session's bases) and diff_since_f4a5586.txt (the
        cumulative list); collected_ids_12ae_close.txt (a copy of 12ae_U28B/gate_u28b/
        collected_ids.txt, cmp equal) and durations_12ae_u28b.txt (a copy of its durations
        file, cmp equal); retention_close.txt (R675(b), E4).
    2b. The 2026-11-09 jobs (R795(e)): health_job_20261109.txt and
        health_journal_20261109.txt (the 07:00 job's log and its Ollama journal window,
        loads and outcome; the NVRM lines inside its window counted); nightly_20261109.txt
        and nightly_ids_20261109.txt (the 09:00 nightly's log, its counts and last line, the
        HEAD it ran on by the reflog method with the lock line (R779(c)), its ids compared
        as a set (R655(b)) against the collected_ids.txt of the gate run on that same engine
        tree — the unit named from the reflog; the journal 09:00–09:25 UTC; the lock file's
        mtime). Readings; a difference from the matched gate is a finding with both values.
    2c. The recount (R795(g)): recount_12ae_close.py in 12ad_close/recount_12ad_close.py's
        chained form, extended to read "L (12ae additions, R785)" as it reads the 12ad
        table; run in a scratch clone on the plan at the item-0 commit; the chain reproduces
        12ad's committed JSON and through it the chain back; output recount_12ae_close.json;
        both sha256s recorded. EXPECTED E1; a row count other than 286, or R510 other than
        2 / 0, = STOP before the closeout commit, both values reported. r510_12ae.txt (the
        six-cell INT- rows at 6cb4ab6 and at the item-0 commit; rows in, rows out).
    2d. band_computation.txt (R795(f)): the four inputs each with its evidence path,
        gate_wall.txt value and concurrent jobs from its snapshots, the confounded member
        named; mean, ×0.9, ×1.1, the inward rounding, the proposed figure (E2); the series
        of thirty-seven listed (12ad's thirty-one plus 940.16, 930.54, 921.98, 945.63,
        1059.11, 963.43 — each read from its gate_wall.txt, the 12ae-OPEN and U24B-R1 values
        confirmed from their files, not from this brief); "a figure, to be set by the wrap
        ruling; the band in force stays 837–1021 s".
    2e. expected_nightly_20261110.md in 12ad_close/expected_nightly_20261102.md's form with
        U28's counts (R795(e)): collected 3562 / 17 deselected / 3545 selected; 3544 passed,
        17 deselected, 1 xfailed, 2 warnings; exit code 0; ids equal to
        collected_ids_12ae_close.txt; governs 2026-11-10 and, while the tree stays
        unchanged, 2026-11-11.
    2f. The lane line, ~/scratch/12ae-close/lane_line_12ae.txt (R795(i)): the 12ad line's
        form (12ad_close/lane_line_12ad.txt, the "+>" lines) with 12ae's sources — HEAD the
        closeout commit (its hash filled after 2h's commit, so the file is written last and
        is not in the commit; it is pasted whole in the report), the plan's sha256/bytes/
        lines, the decision log ending R795 (R796 at 12af-OPEN's item 0), the session's
        summary (five units, the rows, the rules, the stops and the pause, the gates and
        the band figure with its confounded member, the pins, no live write, the ledger
        readings, U40 entered, the next steps: 12af-OPEN, then U40's Phase A). Not applied.
    2g. The closeout block and the expected-values block, in the plan, after the 12ad
        block's last line and before the decision-log section (where 12ad's sits relative
        to 12ac's — read and matched): the closure paragraph ("Session 12ae closed
        2026-11-09 UTC; opened … on the 12ad closeout commit 6cb4ab6…, CC session …"); the
        briefs in order (12ae-OPEN, U24A, U24B (stopped, R786), U24B-R1, U25A, U25B (the
        R86 pause), U26A, U26B, U27A, U27B, U28A, U28B, this closeout), each with its item-0
        ruling; the commits in order from git log 6cb4ab6..HEAD with their ledger commits;
        the diff lists; the rulings R783–R795 one line each (R783 made at the 12ad wrap and
        transcribed at 12ae-OPEN, R775(a)); notes and readings recorded; the rows opened
        (R510: in 2) and closed (C49 on U24, INT-i12-31 on U25, C56 on U27); the stops and
        the pause (R795(h), from the rulings); the tools paragraph (the six items of
        R795(i); extract_rules.py regenerated once at 12ae-OPEN (R783(d)) and once at U24B
        (R784(g)) — read from the record); the units paragraph for each of U24–U28 (design,
        acceptance test, not taken) in 12ad's U23 paragraph's form, one short paragraph each;
        "Carried forward" (U40 and its Phase A questions from 12ae_U28A §9; INT-ae12-1 and
        -2; the housekeeping of R795(c) finding 2; the shared-ledger readings; the
        stage-key question of 12f_C56-A.md:717; the nightly files' succession); the gates
        table (12ae-OPEN, U24B run 1 (red, class (c)) and run 2, U25, U26, U27 with its
        confounder, U28); the band paragraph; the 2026-11-09 jobs paragraph and the
        2026-11-10/11 governance; the recount paragraph with the corrected count and the
        architect's error named (R795(g)); retention; ledger (the foreign-row and
        own-row-carried readings); counterpart files; the host; live; "Session 12ae closed.
        Next: session 12af, fresh."; then "Expected values at session-12af open" in the
        12ae block's form with U28's gate, the band figure, the 2026-11-10 and 11-11
        nightlies per expected_nightly_20261110.md, the 2026-11-10 and 11-11 health jobs,
        the temp root, collection 3,562 and inventory --check in sync, the diff lists, the
        fingerprint, dbchecks. Every figure from a file in 12ae_close/ or an evidence
        directory, named beside it; nothing from this brief's text where a file exists.
    2h. The closeout check, written for this brief: the plan equals the item-0 commit's
        plus the two blocks inserted at the matched position, nothing else; every "ends at"
        line kept, the last "…ends at R795." once; every 12ae_close/ file named in the
        block exists and is staged; the counts of 2c equal the block's; the band figure of
        2d equals the block's; the staged set exactly the plan and 12ae_close/; the rules
        files, 12ad_close/ and every earlier close directory unchanged. <check> && <commit>,
        one line; docs scope; message in 6cb4ab6's form. Fingerprint --compare before the
        push; push; level; --compare after (IDENTICAL). Ledger row with trailers; push;
        level. Then 2f's lane line completed with the closeout hash and the plan's
        sha256/bytes/lines at that commit, pasted whole in the report.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 held; I2 read.
  G2. Item 0 one commit: r795.txt at the architect's hash; Block 193 by the tool; the two
      notes and the dated line in the record's form with the measured count; the plan equal
      to HEAD's plus exactly those edits and the block; the three rules files untouched by
      content; the brief file hashed; one chain; pushed, level, fingerprint IDENTICAL;
      ledger.
  G3. The closeout commit: every element of 12ad-CLOSE's block present in order with its
      12ae source; every counterpart file written or listed as not written with its reason;
      the recount at E1 (or stopped); the band computation reproducing E2 from the files;
      the expected nightly file; the 2026-11-09 jobs read; E3–E5 held or reported; the
      check passed in one chain; pushed, level, fingerprint IDENTICAL; ledger; the lane
      line pasted whole in the report, not applied.

OUT OF SCOPE: the wrap ruling (R796: the band set, the lane line approved and applied, the
  stops counted — the architect's, after this report); U40's Phase A (12af); any engine,
  test, script, docs/architecture, CLAUDE.md or tools edit; the tools items; /wrap of the
  CC session (the PI's, after the wrap ruling).

CONSTRAINTS (task-only): no model call, no provider call, no live write; the clock read
  before each commit (R795(j)); a count arrived at by eye is INFERRED, the computed value
  given with its command; every figure in the block beside its file; where this brief
  describes a tool or record, the record governs and a difference is a finding (R732(a)).

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — metadata, render comparison, the
  two rows' last notes, the dated line, the brief file's hash, the check verbatim, chain,
  hashes, fingerprint, ledger; (2) the closeout — the measurements with their values, the
  2026-11-09 jobs' readings, the recount output and chain, band_computation.txt verbatim,
  the expected nightly file verbatim, the closeout block as committed (verbatim), the
  check verbatim, chain, hashes, fingerprint, ledger, the lane line verbatim; (3)
  contradictions and findings, numbered, each with its quoted line; (4) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 false, a 1a mismatch, or a check failure or
  crash at 1e = STOP before the item-0 commit. A recount other than E1, or a check failure
  at 2h = STOP before the closeout commit, the work on disk reported. The clock at 06:30 UTC
  = STOP (R795(j)). An expectation mismatch (E2–E5) is reported and the closeout continues
  with the measured value in the block. A spec premise that does not survive contact with
  disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not apply the lane line, do not start 12af or U40. Await the
  wrap ruling in this session. STOP-12AE-CLOSE
