CLAUDE CODE

================================================================================
SECTION 1 — RULINGS TO TRANSCRIBE (R826 and R827, PI rulings 2026-11-23 UTC; not on disk)
================================================================================

Both are transcribed at item 0 of the brief below, by
docs/session-reports/tools/transcribe_ruling.py, as the next two blocks of the decision
log (Blocks 224 and 225), in that order. For each, the lines between its two marker
lines, exclusive of the markers, are the byte content of its file. Write each file from
those lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hashes:
  r826.txt: sha256 ced28e0e1fb4e5c2156a95612c1a2068eef4f67a0f465f746e81435cc81fc8ae,
            6837 bytes, 87 lines, one trailing newline. The same bytes stand between the
            markers of the committed 12aj_open/brief_12aj_OPEN_R1.md.
  r827.txt: sha256 96dc47af7136e5934571faf0e38543e465cfeb788f10b69d2b348a2493f7a25a,
            4820 bytes, 60 lines, one trailing newline.

===== BEGIN r826.txt =====
RULING [evidence-engine 12aj-R826] — the 12aj-OPEN stop at step 3b is correct; the PI's word:
  the open check is run again whole with its retention reference pointed at the 12ah
  closeout's file; item 0 accepted; steps 0 and 1 accepted; the between-session file held out
  of the evidence commit; readings; no standing rule; the open resumes at 3b
STATUS: PI ruling, 2026-11-23 UTC, session 12aj, on architect recommendation, on the 12aj-OPEN
  stop report (item 0 46339bebf2a63e981fd9a94c2dc2fbc374e46661; stopped at step 3b, before
  the gate). Transcribe at the next item 0, by transcribe_ruling.py, in the block after
  R825's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the stop report, quoted; wrapped lines joined): "session_open_check.py crashed in
    its retention section"; "ValueError: too many values to unpack (expected 3)"; "repos,
    plan, fingerprint, hashes, pins and dbchecks all MATCH (22 rows)"; "Not run:
    probe_scratch, stage, nightly, health, ollama, temp_root, cc_sessions."; "Commit:
    46339bebf2a63e981fd9a94c2dc2fbc374e46661, parent dccc546…; pushed, 0 0. Fingerprint
    IDENTICAL before and after."; "Ledger: b13d194ea4cf81c470de734dac613fe6cc1122c8, parent
    7c88f52…, row 2026-11-23T03:52:17Z, no foreign rows; pushed, 0 0."; "r825.txt:
    f08a80d6bec8f7ab6948ac7791a123bc72d3834385480972499c23fe03801417, 8,687 B, 105 lines;
    equal to the architect's hash, one write."; "sha256
    9ee21bdad33f6602f2488f36b66925c2e284ae21e047e057184a0df1fa08295a, 45,125 B, 603 lines."
    (the architect's copy: the same); "Plan +116 / −0; "ends at" lines 179 → 180; six-cell
    INT- rows 215 / 215"; "Plan now:
    c03d2356ebe2a32c35859f74c19ec14fd75569160892057da49f11e9c99d7f6c, 3,525,551 B, 21,206
    lines."; "HEAD 7c88f52 at the open, level and clean"; "primer.md unchanged since
    42fabff"; "No session of this lane after it except this one"; "kept present 5 of 5;
    removed absent 53 of 53"; "Both nightlies ran on a tree equal to 7fad72a's (I3 TRUE)";
    "20 of 20 OK, 0 FAIL"; "No new NV_ERR_NO_MEMORY line; still 55 this boot"; "Loads outside
    the health windows: 18, on 2026-11-22 05:31:59–05:48:22 UTC"; "deepseek-r1:32b is not in
    that block."; "Last commit touching the file:
    b5af93d854060402a33bdf767adfa0ff286d8dbe"; "At 0h(vii) I quoted the whole lane-model
    file where the brief asked for the Evidence Engine block."
  READ (CC's): the cause as the report gives it; findings 1–7; the proposed revised check.
  READ (the architect's clone at 46339be): the full hash in STATUS, its parent dccc546;
    12aj_open/brief_12aj_OPEN.md and 12ai_close/r825.txt equal to the architect's copies by
    cmp; the plan's sha256, bytes and lines as the report gives them; session_open_check.py's
    retention reader, "if l.startswith("#") or " MATCH" not in l:" then "s, p, _ =
    l.split()"; 12ai_close/retention_close.txt (sha256
    18704ed119d6305516d3c705ec340fda8ccb2f8d11113fa4ad553dca8513d596, 30 lines), its line 27
    "MATCH     retention MATCH / MISMATCH: [23, 0]", its lines 28–30 beginning "MATCH" too;
    12ah_close/retention_close.txt (sha256
    8b6e6e749d5444ce2f045d9c7ddc74b40bee5454ac79b04fcc1003ded519e434, 29 lines), its lines
    27–28 "requests: 10 of 10 equal" and "responses: 53 files; name-equal 53/53;
    committed-equal 53/53"; lines 2–24 of the two files equal by diff, twenty-three lines;
    the tool's probe_scratch and stage readers take their values from the config and read
    neither file; 12ai_close/health_job_20261120.txt, which holds the light check's list of
    thirteen lane models with no lane named.
  INFERRED — BLOCKING (verified by the resume's R1 before the check is run; false = STOP):
    read by the tool's rule, 12ah_close/retention_close.txt yields twenty-three lines of
    three tokens each and no other.

R826(a) — The stop at step 3b is correct: a check that crashes is a failed check (R763(c)).
  The cause is the form of 12ai_close/retention_close.txt, whose lines 27–30 are the open
  check's own output pasted under the probe-scratch heading: CC's, at the 12ai closeout. It
  is the architect's as well: the closeout was accepted on that directory's listing (R825's
  READ), and the 12aj-OPEN brief named the directory's files as references, with this file
  not read against the tool that reads it. A statement about a file that no one had read it
  for. The count so far in 12aj: one stop; no pause; one architect's error, reaching a
  commit (the brief's).
R826(b) — The PI's ruling, in his word: "Re-run". The open check is run again, whole, from a
  second config equal to the first but for one value: the retention section's reference is
  12ah_close/retention_close.txt. Its twenty-three hash lines equal the 12ai file's lines
  2–24. session_open_check.py is not edited. 12ai_close/retention_close.txt is not edited
  and gets no successor here. The crashed run's output and the first config are kept as they
  are.
R826(c) — Item 0 is accepted as reported: 46339be (Block 223 by transcribe_ruling.py, its
  header carrying the PI's word "wrap" before the report, rendered as one sentence; r825.txt
  equal to the architect's hash; the 12ai lane-line copy; the brief file equal to the
  architect's copy; the plan +116 / −0; the rules files untouched), ledger b13d194.
  claude-config at 7c88f52, two commits past 42fabff, both another lane's ledger rows only,
  is accepted under R663(b).
R826(d) — Steps 0 and 1 are accepted as reported: I1, I2, I3, I5 and I6 held; E1, E3 and E6
  held. Readings, no row: the 2026-11-22 sweep is the first written by the light/full
  script, twenty of twenty, with no new NV_ERR_NO_MEMORY line; its log holds no lock line, so
  that the lock was taken is read from the script and not from the log (finding 3); the
  eighteen model loads of 2026-11-22 05:31:59–05:48:22 UTC are another lane's and join the
  cross-lane readings for dgx-infra (finding 4); deepseek-r1:32b is not in the Evidence
  Engine block of dgx-infra's lane-model list and the file is unchanged since b5af93d
  (finding 2), which the PI carries to the workbench; CC's two slips of finding 6 and its
  memory note (finding 7).
R826(e) — ~/scratch/12aj-open/between_sessions_read.txt quotes the whole lane-model file, other
  lanes' blocks among them. The repository is public, and whether those lines are published
  is the PI's and is not ruled here. Until he rules, the file is held out of the evidence
  commit: it stays in scratch, unedited, its sha256, bytes and lines reported. Nothing else
  of step 4 waits on it.
R826(f) — Not ruled here: the form of the retention file the 12aj closeout writes; whether the
  reader's filter joins the tools items. This ruling states no standing rule. The open
  resumes at step 3b under the resume below.
===== END r826.txt =====

===== BEGIN r827.txt =====
RULING [evidence-engine 12aj-R827] — 12aj-OPEN accepted as resumed; the gate's 1017.85 s enters
  the series as confounded; readings on the findings; the between-session file stays held; no
  standing rule; next, U31's Phase A
STATUS: PI ruling, 2026-11-23 UTC, session 12aj, on architect recommendation, on the
  12aj-OPEN-R1 final report (evidence cd6f9a895cae052bd89d7d871bbcd759014dbd97). Transcribe
  at item 0 of the 12aj-U31A brief below, by transcribe_ruling.py, in the block after R826's.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12aj-OPEN-R1 final report, quoted; wrapped lines joined): "By the reader's
    rule the 12ah file yields 23 lines, three tokens each, lines 2–24, and no other."; "Open
    check (R3): exit 0, RESULT: PASS (0 mismatches), 42 MATCH, no SKIPPED"; "Health section
    passed (health last 20 models OK: 20), so D8 was not needed."; "rows 289, 94 / 59 / 136,
    0 unclassed; dated lines 39 with 35 done; expected_holds true"; "dccc546: 215; 46339be:
    215; rows in 0; rows out 0; no class or package move"; "Hold: not needed. The snapshot at
    04:04:58 UTC named surgical-cv's uvicorn only."; "3607 passed, 17 deselected, 1 xfailed,
    2 warnings; exit 0; ids 0 / 0"; "Wall time: 1017.85 s, inside 881–1076 s."; "another
    lane's pytest run started at 04:19:24 UTC, in the gate's last three minutes, and was
    still running at 04:22"; "Durations: no phase slower by more than 10 s."; "Commit:
    cd6f9a895cae052bd89d7d871bbcd759014dbd97, parent 46339be…; 31 new files, +16,000; pushed,
    0 0. Fingerprint IDENTICAL before and after."; "sha256
    e169195ce7a76830b48f15ec59b0b0049add464d98d96a114cf2d5e85d469fe9, 11,477 B, 155 lines"
    (the architect's copy: the same); "between_sessions_read.txt, held out (R826(e)): sha256
    7fd1a1724e3aa88b8cf341fba87a40f1f33de742849a1ffca44a0987d62d462b, 22,029 B, 311 lines; in
    scratch, unedited."; "Ledger: b3b5f8ffc0beea5898e1b2d56e1266484f0e2588, parent b13d194…";
    "It carries five foreign pending rows"; "health_journal_20261122.txt is 402,940 B".
  READ (CC's): findings 1–4.
  READ (the architect's clone at cd6f9a8): the full hash in STATUS, its parent 46339be;
    12aj_open/brief_12aj_OPEN_R1.md equal to the architect's copy by cmp;
    12aj_open/gate_12aj_open/gate_wall.txt "wall: 1017.852609782 s"; 12aj_open/ holding
    twenty-three files, none named between_sessions_read.txt;
    12ag_close/health_journal_20261115.txt, the earlier sweep day's journal, 395,665 bytes,
    committed; the plan's series table for the 12ag open, "1042.65 s, **confounded**
    (R805(b))", its confounder "another lane's suite started inside the window after both
    snapshots".
  INFERRED: none that a clause below rests on.

R827(a) — The 12aj-OPEN-R1 final report is accepted as printed. R826's blocking item held. The
  evidence commit cd6f9a8 (the gate's nineteen files and its two side files, the 2026-11-21
  and 2026-11-22 job reads, the NVRM read, the resume file equal to the architect's copy),
  ledger b3b5f8f carrying five rows of another lane, named. The open check passed whole on
  the second config; the recount stands at 289 with 39 / 35; R510 215 / 215, in 0, out 0.
  With item 0 46339be (R826(c)), session 12aj is open on a verified base.
R827(b) — The gate's 1017.85 s is the forty-seventh member of the series and is carried as
  confounded (R677(f)), its confounder named: another lane's pytest run that started at
  04:19:24 UTC, inside the gate's window and after both snapshots, as the 12ag open's member
  was carried (R805(b)). It was launched as a unit, with no hold. The band stays 881–1076 s
  until the 12aj closeout cuts the next one in R683(c)'s form.
R827(c) — Readings, no row. The open check's health section read the sweep written by the
  light/full script and passed, so the brief's D8 was not used and expires untested.
  Finding 2: the five foreign rows were carried and named as R654(d) has it. Finding 3: a
  sweep day's journal of this size is in the record already (2026-11-15's); it holds no
  lane's name by CC's reading; no action. Finding 4: another lane's runs rotated three
  basetemps out of the shared temp root during the open; with finding 1 it joins the
  cross-lane readings for dgx-infra.
R827(d) — ~/scratch/12aj-open/between_sessions_read.txt stays held as R826(e) left it, at the
  sha256 above. Whether it is published, and in what form, is the PI's and is not ruled here.
R827(e) — The count so far in 12aj: one stop (R826(a)); no pause; one architect's error. This
  ruling states no standing rule: the id list, the glosses file and STANDING_RULES.md are
  untouched at the item 0 that transcribes it, asserted by content. Next: U31's Phase A (the
  brief below), read-only; then the PI's decisions on its read-out; then its build.
===== END r827.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12aj-U31A] — U31's Phase A read-out ("Cloud response repair does not
  invent a value"; INT-j12-18, with INT-l12-2's two repair scripts). Item 0: R826 and R827
  as the next two blocks; this brief committed as pasted; no standing rule; no row note
  and no dated line. Read-only on engine/, scripts/ and tests/; STOP before any edit.
PURPOSE: Put on disk, before the design, what the cloud response parser changes between
  the response a model returned and the spans the engine stores: every place it writes a
  value, a snippet or a shape the model did not give, what records that it did, and what
  reaches the store. With it: what the two repair scripts do and what retiring them
  touches, and an answer by code order to R800(c)'s carried question. Mixed: one docs
  commit (item 0), one docs commit (the read-out). No engine, test, script, spec or
  codebook change. No live write. No model call. No provider call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
  The session 12aj-OPEN ran in.
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Blocks 224 and 225 appended by the tool,
  nothing else); docs/session-reports/session-12/12aj_open/ (r826.txt and r827.txt); a
  new directory docs/session-reports/session-12/12aj_U31A/ (brief_12aj_U31A.md at item 0;
  the read-out, census and probe with outputs at step 2); ~/scratch/12aj-u31a/ (new); the
  claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/ (R71), the codebook, docs/architecture/, CLAUDE.md, docs/inventory/, the
  tools; the id list, the glosses file and STANDING_RULES.md (not one byte); review.db
  (the fingerprint instrument only; no query on live, the count of zero-span rows
  included); any .env or API key; the two repair scripts (read, never run, not with
  --dry-run either); ~/scratch/12aj-open/between_sessions_read.txt (R827(d)); the five
  kept clones (no git command inside one, R822(e)); any scratch directory other than
  ~/scratch/12aj-u31a/, except where a step names a file to read as a model.

PATHS: a name of the form 12xx_<name>/ is a directory of docs/session-reports/session-12/;
  a tool is named by its full path under docs/session-reports/tools/.

FORM (R784(g)(iv)): 12af_U30A/brief_12af_U30A.md (sha256
  fd161193679f686a034675a92db9aafa7a4d070240a2a3c9e3f9e3708b662b9f, 340 lines) is this
  brief's form, and 12af_U30A/12af_U30A.md (sha256
  5514b6510f04ed2161990de3e8a1cc64a8c8e3def2fc33067976c29812d3b9b4) the read-out's:
  "The blocking answer" first, then §0 scope, §1 inputs quoted, the questions, §4 census,
  §5 probe, §7 options, §8 findings, §9 what could not be established read-only.
  census_u30a.py and fake_probe_u30a.py are the instruments' forms. Verify the two
  hashes and read the form's Section 2 whole before step 0. Where the form and this brief
  disagree, this brief governs; where either disagrees with the code or the record, the
  code and the record govern and the difference is a finding (R732(a)).

TIMING: valid from issue, outside the day's window (R812(a)). At issue the architect's
  clock read 04:33 UTC on Monday 2026-11-23; that day's window opens at 09:00 UTC. If the
  clock reaches 15 minutes before an opening with a commit pending, hold with the work
  on disk uncommitted and resume in this brief after 10:35 UTC (R789(a)); say so. No
  gate is run.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12aj-OPEN-R1 final report, as R827 quotes it in Section 1): HEAD
    cd6f9a895cae052bd89d7d871bbcd759014dbd97, parent
    46339bebf2a63e981fd9a94c2dc2fbc374e46661, clean, level; claude-config at
    b3b5f8ffc0beea5898e1b2d56e1266484f0e2588; 289 rows (94 / 59 / 136, 0 unclassed), fix
    units 39, done 35; six-cell INT- rows 215.
  READ (the architect's clone at cd6f9a8, 2026-11-23 04:33 UTC; re-read on the box, not
    trusted):
    R1. The plan at HEAD: sha256
        c03d2356ebe2a32c35859f74c19ec14fd75569160892057da49f11e9c99d7f6c, 3525551 bytes,
        21206 lines; last line "The decision log in this file ends at R825."; last block
        223 (its header at :21092); no "**Block 224.**"; 180 lines begin "The decision
        log in this file ends at R". git ls-files lists 49 files named brief_*.md. The
        three rules files at the hashes 12aj-OPEN's R3 gave.
    R2. U31's entry, 12l_P1A/12l_P1A.md:449–456: rows INT-j12-18; pin effect not
        affecting; files "`engine/cloud/base.py`, `scripts/reparse_cloud_spans.py`,
        `scripts/backfill_cloud_spans.py`"; acceptance test
        "`test_a_span_with_no_value_key_or_a_non_scalar_value_is_not_repaired` — A span
        with no `value` key yields no sentinel claim and its field is reported missing;
        a list, dict or bool value is rejected, not stored as its repr; an explicit JSON
        null still yields the canonical sentinel. Fails on the unmodified tree."; its
        note "Cloud. Has a live route outside the opt-in through two repair scripts (§2
        D14)." The entry's row at :551 states the pin condition: "provided the fix stays
        in `engine/cloud/base.py` and leaves `ExtractionOutput` / `EvidenceSpan` alone
        (shared with `extract_pass2`'s format hash)".
    R3. Row INT-j12-18 (plan :652): "Response repair writes a value the model did not
        give: a span with no `value` key becomes the absence sentinel; a list, dict or
        bool becomes its Python repr"; Class 1 (conditional), P1; its source
        "`12j_review_w2.md` row 26 (L12-C2 …; evidence level: reproduced …)", with
        12j_review_w2/readers/L12.md and 12j_review_w2/repro/L12/L12_1_parse_coercions.py
        and .out. Row INT-l12-2 (plan :704): the two scripts "call the cloud response
        parser and insert into `cloud_evidence_spans` with no run manifest, no
        completeness guard and no `enabled_arms` check"; R612(o): "D14: Option A.
        reparse_cloud_spans.py and backfill_cloud_spans.py retire under R31. New row,
        Class 1 (conditional), P1, closing with U31."; its condition "conditional on a
        zero-span `cloud_extractions` row existing", "which was not counted".
    R4. engine/cloud/base.py at HEAD (642 lines), parse_response_to_spans at :267. Its
        last two repairs before validation:
        :362–364 "for span in response_json.get("fields", []):" / "if isinstance(span,
        dict) and "source_snippet" not in span:" / "span["source_snippet"] = """;
        :373 "if isinstance(span, dict) and span.get("value") is None:" then :378
        "span["value"] = self._codebook.canonical_absence_sentinel" and :379
        "span["source_snippet"] = """;
        :380 "elif isinstance(span, dict) and not isinstance(span.get("value"), str):"
        then :385 "span["value"] = str(span["value"])";
        :388 "output = ExtractionOutput.model_validate(response_json)", a failure
        returning [] (:389–391).
        Before them, by the function's own comments: a markdown fence strip; a bare list
        wrapped as fields; recovery from a "raw" key when fields is empty; nine
        alternate top-level keys; "single_span_dict" and "flat_field_dict", the two
        branches that set self._last_salvage.
    R5. The callers outside tests: engine/cloud/openai_extractor.py:127,
        engine/cloud/anthropic_extractor.py:132, scripts/reparse_cloud_spans.py:70 and
        scripts/backfill_cloud_spans.py:66. Tests that call it:
        tests/test_cloud_extraction.py and tests/test_u30_cloud_interrupt.py.
    R6. store_result (:404) after a non-empty spans check: enforce_completeness (defined
        at engine/core/completeness.py:181) with salvage=self._last_salvage; then
        drop_unexpected (N46); then the citation guard (U28), its comment "an uncited
        non-sentinel value or an empty value is refused and nothing is inserted; a
        sentinel keeps its "" (R132)".
    R7. The two scripts are named outside the record at CLAUDE.md:614,
        tests/test_review_paths.py:199, docs/architecture/modules.md:455–456,
        docs/inventory/entry_points.json and entry_points.md, and in parametrized test
        ids of tests/test_api_parity.py as an older collected-ids file shows them. R31
        is in the plan's decision table at :6125; the plan at :1629 reads "R31 says a
        retirement is recorded in a ledger and executed by the session that owns the
        artifact."
    R8. R800(c), plan :18980–:18981: "whether a truncated response that a repair
        completes may be stored is U31's question, not answered here." R807(d), in
        12ag_open/r807.txt: "Left open by N46 and not U30's: whether live holds such a
        row; the two repair scripts' insert path from extracted_data; the three analysis
        readers that count every row."
    R9. Block 204's header (12ag-GATEUNIT-A's item 0,
        12c11080e8e83685b20817f66868083ee21e6782, two rulings and a brief file) reads "in
        its own commit with Block 205 and the brief file it orders"; Block 202's names a
        stop report "the 12ag-OPEN stop report (item 0 `<full hash>`)" and its sentence
        "The 12ag-OPEN-R1 resume that followed the ruling in the paste is not part of
        the block."; Block 203's names "the 12ag-OPEN-R1 final report (evidence `<full
        hash>`)".
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 7fad72a (git diff --stat
    7fad72a HEAD on engine tests scripts analysis review_specs empty).
  INFERRED — expectation (report a mismatch, continue):
    E1. At HEAD a span with no value key takes the same branch as an explicit null
        (R4's :373) and comes out as the canonical sentinel with an empty snippet; by
        R6's comment the store then accepts it as an absence.
    E2. At HEAD a list, a dict and a bool each come out of :385 as Python's str() of
        the value. Whether the store then accepts or refuses each is not predicted.
    E3. The acceptance test's three statements, run as a probe at HEAD: the first two
        false, the third true.
    E4. The row count after item 0 is 289 (94 / 59 / 136, 0 unclassed), six-cell INT-
        rows 215, in 0, out 0.
    E5. Nothing is predicted about Q3–Q6. The architect has not read
        enforce_completeness, drop_unexpected, the two transports' loops, read_response,
        the local extractor's handling of the same shapes, either script past the lines
        a grep shows, R31's ledger, or the pin instrument.

CONTRACT (steps, in order; evidence under ~/scratch/12aj-u31a/):

  STEP 0 — Pre-check (read-only). date -u. HEAD and parent proved by git cat-file -e
    <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. R1's values. claude-config
    at b3b5f8f or an accepted descendant (R663(b)), level; a pending foreign row carried
    and named (R654(d)). I1. Fingerprint --compare IDENTICAL. No 12aj_U31A/ exists; no
    r826.txt or r827.txt in 12aj_open/. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; the check written for
    this brief, R794(i)). Two models, each quoted before its counterpart is composed:
    for two rulings and a brief file in one item 0, 12c1108 (Blocks 204–205, R9); for
    the files, the check and the ledger message, this session's own item 0, 46339be,
    with ~/scratch/12aj-open/block223.json and its item-0 check.
    1a. 12aj_open/r826.txt and 12aj_open/r827.txt from Section 1, each one write, each
        hashed straight after (R738(b)) against Section 1's hash. Then r826.txt against
        the lines between the markers of the committed 12aj_open/brief_12aj_OPEN_R1.md,
        by cmp. A mismatch on any of the three = STOP (R739(b)).
    1b. ~/scratch/12aj-u31a/block224.json and block225.json in block223.json's key set.
        Both: transcribed = the commit's UTC date (R775(a)); item "item 0 of the
        12aj-U31A brief"; made_by PI; made_date 2026-11-23; session 12aj; sentence
        model. R826: report as its STATUS names it, in Block 202's form (R9), the hash
        in backticks; followed "12aj-OPEN-R1 resume"; prev_ends R825. R827: report as
        its STATUS names it, in Block 203's form, the hash in backticks; followed
        "12aj-U31A brief"; prev_ends R826. Each block's "beside" composed by CC in
        Block 204's and 205's words: the other block and the brief file. Quote the
        composed values. Render both to stdout, compare the element order with the
        models', then --append in order.
    1c. 12aj_U31A/brief_12aj_U31A.md from this whole pasted block, "CLAUDE CODE" through
        the STOP-12AJ-U31A line, one trailing newline, written once; sha256, bytes and
        lines straight after the write, reported.
    1d. The item-0 check: --check-plan byte-identical for both blocks; the plan's
        numstat read, its "−" matched to zero; the "ends at" lines counted before (R1)
        and after, every earlier one kept; the last line "The decision log in this file
        ends at R827." once; r826.txt and r827.txt equal to 1a's; the three rules files
        byte-equal to HEAD's and unstaged (R777(b)); six-cell INT- rows 215 before and
        after; both blocks' transcribed value equal to the commit-time UTC date; the
        staged set exactly the plan, r826.txt, r827.txt and brief_12aj_U31A.md. Evidence
        by a redirect inside the check; quote the check.
    1e. <check> && <commit>, one line; the message from the staged set (R778(d)), docs
        scope. Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers, its message built from the diff
        (R801(f)); push; level. Record hashes and parents.

  STEP 2 — The read-out (read-only; no server; no provider; no live database). Every
    code claim beside a quoted line with its path and line at HEAD, labelled CODE ORDER
    or FAKE.
    2a. §1 Inputs quoted: U31's entry and its row at :551 (R2); rows INT-j12-18 and
        INT-l12-2 whole with every dated note (R3); 12j_review_w2.md row 26 and the L12
        reader's passage; L12_1_parse_coercions.py and its .out as committed (not
        re-run); 12l_P1A.md §2 D14 and §5 N50; R599, R612(o), R132 and R31, each as the
        decision log states it; R800(c) and R807(d) (R8).
    2b. Q1 — WHAT THE PARSER CHANGES. One table, one row per branch of
        parse_response_to_spans from the response as received to the returned list, in
        code order (R4 names eleven; the code governs the count): the input shape that
        takes it; what it writes that the model did not send (a value, a snippet, a
        key, a wrapper, a field name); whether it sets _last_salvage; where, if
        anywhere, the change is recorded (a log line and its level, a run_calls column,
        extracted_data, a claim's fields); and what the store does with the result.
        Then one sentence per branch: is it a change of shape only, or can it change
        what a claim says.
    2c. Q2 — THE ROW'S TWO CASES, AND THEIR NEIGHBOURS. For a span with no value key, an
        explicit null, an empty string, an int, a float, a bool, a list of strings, a
        list of mixed types, a dict, and a span with no source_snippet: the branch
        taken, the value and snippet that come out, and what the three guards of R6 do
        with each, quoted. State that bool is an int subclass in Python and what that
        means for any test on type. From the codebook, counted by a script: the fields
        whose declared type is numeric, and the fields that may hold more than one
        value; from the prompt builder, quoted: what the model is told about the type
        of value and about a field not found.
    2d. Q3 — WHAT "REJECTED" AND "REPORTED MISSING" WOULD DO. enforce_completeness and
        drop_unexpected quoted whole. If a span were dropped before validation: what
        completeness reports and raises, what each transport's loop does next (the
        re-send, its count, what the re-send tells the model), what run_calls and
        extracted_data hold afterwards, and how the run ends for that paper. If the
        whole response were refused instead: the same four. How the local path treats
        the same shapes — a missing value, a non-string value — from quoted code, with
        its retry.
    2e. Q4 — R800(c)'S QUESTION. Where a truncated response is detected (the transports'
        read_response, quoted) and whether, by code order at HEAD, a response recorded
        as truncated can still reach parse_response_to_spans and store_result. If it
        can: which branches of Q1 could make a cut-off response storable, each shown or
        ruled out by the probe. State the answer in one sentence before the detail.
    2f. Q5 — THE TWO SCRIPTS. For each: main quoted in its parts — how it opens the
        database, its SELECT, the parser call, its INSERT and commit, its flags — and
        what it lacks of run_arm's path (the manifest, the lock, the completeness
        guard, the enabled_arms check, the event store), each from a quoted line or its
        absence by census. Every reference to either script (R7), by grep and by the
        gate's committed collected ids, with the count of test ids that carry a
        script's name. R31 whole, the retirement ledger's place and form, and the most
        recent retirement in the record with its commit, as the model. What a
        retirement would change in the collected ids, the inventory and CLAUDE.md,
        listed and not done. Whether retirement leaves anything of R807(d)'s second
        item or of INT-l12-2's uncounted condition standing; the count on live is not
        taken.
    2g. Q6 — THE PIN AND THE SHARED TYPES. Where ExtractionOutput and EvidenceSpan are
        defined and every importer of each (census); what the pin instrument
        (12l_U04/pin_tuples.py) hashes, from its code; whether a change confined to
        engine/cloud/base.py can move either pin, argued from those lines. The
        instrument is not run.
    2h. §4 Census: census_u31a.py in census_u30a.py's form (AST; declared completeness
        stated): every caller of parse_response_to_spans; every write and read of
        _last_salvage; every use of canonical_absence_sentinel on the cloud path; every
        importer of the two models; every reference to the two scripts under engine/,
        scripts/, tests/ and analysis/; the tests coupled to the null, the missing-key
        and the coercion behaviour (tests/test_cloud_extraction.py's among them), each
        with the assertion it makes and its fixture.
    2i. §5 The probe (FAKE, fake_probe_u31a.py → .txt, on fake_probe_u30a.py's scratch
        setup): Q2's ten shapes through parse_response_to_spans at HEAD, then through
        store_result on the scratch database, the rows that result in
        cloud_extractions, cloud_evidence_spans, field_events and claim_inputs quoted
        for each; the acceptance test's three statements as E3 states them; Q4's
        cut-off shapes. The output records that no SDK client was constructed and
        appends the Ollama journal for its window.
    2j. §7 Options for the ruling, each with what it records and what it costs: a span
        with no value key (dropped so its field reads missing; or the whole response
        refused); a list, dict or bool (the same two; and whether a list of strings for
        a multi-value field is a third case, with what joining it would invent); int
        and float (kept as str() or not, with Q2's field counts); the snippet patch;
        whether a repair that changes what a claim says is recorded and where; the
        answer to R800(c); the scripts' retirement, its steps and what it moves; where
        the fix sits against R2's pin condition; every file touched; the tests that
        move; the acceptance test's shape and seams; what U31 does not fix and whose it
        is. §8 findings; §9 what could not be established read-only.
    2k. Commit the read-out (12aj_U31A/12aj_U31A.md), census_u31a.py/.txt and
        fake_probe_u31a.py/.txt: the staged set exactly those, nothing under engine/,
        tests/, scripts/ or data/ (asserted); fingerprint IDENTICAL before and after
        the push; level; the ledger row as 1e states. Record hashes.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 held.
  G2. Item 0 one commit that passed 1d in one chain: r826.txt and r827.txt at the
      architect's hashes, r826.txt equal to the committed resume's section; Blocks 224
      and 225 by the tool; the rules files untouched by content; the brief file hashed;
      pushed, level, fingerprint IDENTICAL; ledger.
  G3. Q1–Q6 each answered from quoted code before §7 is written; Q1's table complete in
      code order; the census and probe committed with their declared completeness; no
      SDK client constructed; neither script run; no query on live; the journal window
      empty; no file outside 12aj_U31A/ in the commit; pushed, level, fingerprint
      IDENTICAL; ledger.

OUT OF SCOPE: U31's design and build; the retirement itself and any edit it implies; any
  schema change; U32 and later units; any edit under engine/, tests/, scripts/,
  analysis/, docs/architecture/, docs/inventory/ or CLAUDE.md; a count or any other
  query on live; the tools items; the between-session file; any job read; /wrap.

CONSTRAINTS (task-only): no model call, no provider call, no API key read, no network
  request, no restart, no live write, no query on live. A count arrived at by eye is
  INFERRED; a computed value is reported with its command. The read-out states options
  and what each costs; it recommends nothing as decided — the decision is the PI's
  after it (R801(g)).

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — both block files, the render
  comparison, both ruling files' hashes and the cmp against the resume's section, the
  brief file's sha256, bytes and lines, the check verbatim, chain, hashes, numstat, the
  plan's new sha256, bytes and lines, fingerprint, the ledger message; (2) the answers
  to Q1–Q6 in plain sentences first, Q4's one sentence among them, Q1's table whole,
  Q2's ten shapes as one table, then the read-out's §7–§9 as committed and the probe's
  outputs quoted; (3) contradictions and findings, numbered, each with its quoted line;
  (4) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 false, a 1a mismatch, a model line not
  found as R9 quotes it, or a check failure or crash at 1d = STOP before the item-0
  commit. An expectation mismatch (E1–E4) is reported and the work continues. A code
  line not found as R4–R6 quote it is a finding; the code governs and the read-out
  proceeds from it. A spec premise that does not survive contact with disk is a
  finding, not an obstacle.

HANDBACK: Stop and report. Do not start U31's design or build. Await the next brief or
  ruling in this session. STOP-12AJ-U31A
