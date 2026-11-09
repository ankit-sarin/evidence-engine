CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R790, PI ruling 2026-11-09 UTC; not on disk)
================================================================================

R790 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r790.txt: sha256 b79f11f5be4521f418f9bb77f2404c9b61fd5b0db287f99d10fa44783aff82a8,
            8200 bytes, 94 lines, one trailing newline.

===== BEGIN r790.txt =====
RULING [evidence-engine 12ae-R790] — 12ae-U26A accepted; the committed brief equal to the
  architect's copy; U26's design: option A at all six parse handlers in U02's form, the ledger
  row recorded before the re-raise at 4–5; UnknownStage not added to the tuple; B and C not
  taken; notes on C56 and L07-C4; readings, one the architect's; the build opens as one brief
STATUS: PI ruling, 2026-11-09 UTC, session 12ae, on architect recommendation, on the 12ae-U26A
  final report (item 0 5e38485; read-out e5d9fed, docs/session-reports/session-12/12ae_U26A/
  12ae_U26A.md, sha256 96b70fbcad4e0710ba5b661297386c6f7fd93d749762eac75e8554648147734c, 271
  lines). Transcribe at item 0 of the 12ae-U26B brief below, by transcribe_ruling.py, in the
  block after R789's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the read-out and the report):
  MEASURED: "12ae_U26A/brief_12ae_U26A.md is fdd161a0677fdc38c28021153e98319be76a418094373767aa0e900f59d64d4a,
    23,633 B, 308 lines" (the architect's copy: equal, cmp byte for byte); the mapping, "4 … #14
    … pdf_parser.py:717; 5 … #19 … :806; 6 … #20 … :901; 7 … #23 … :1120; 12 … #10 … :520; 13 …
    #11 … :525"; "all 24 cases (6 handlers × 3 run-fault members + KeyError) are converted at
    HEAD, with nothing propagated"; handler 6 FAKE: "paper events: [], asset rows: 1,
    papers.status: ('PDF_EXCLUDED', 'PARSE_QUALITY'), stats.failed=0"; "159 patch sites on the
    parser, 0 with a run-fault side_effect"; "The Ollama journal for the window was empty"; the
    tuple at "extraction_events.py:468–474: sqlite3.Error, CeilingUnavailable, ConnectionError,
    httpx.ConnectError, httpx.ConnectTimeout, UndeclaredCall, UndeclaredOverride, RunRefused,
    RunAborted, EventRefused, CodebookContractError, ExhaustedWithoutRecord, SnippetNotExact".
  READ (the read-out, §2–§8): "U02's form, if is_run_fault(exc): raise at extractor.py:439 and
    :1223"; §3 point 4, "On the success path the attempt rows are inserted inside the asset
    transaction (:972–973), so a failure there rolls back the asset too (handler :978).
    _commit_attempts runs only when no asset was written"; §7 A, "recording the attempt row
    first and then re-raising keeps the ledger. The outer :997 then commits it through
    _commit_attempts"; §7 C, "At HEAD the extract loop does not close the run: handler 2
    re-raises (extractor.py:1223–1224), and run_pipeline closes it (:199–216)"; §4, the two
    pinning tests "test_parse_failures.py::test_T3_unclassified… (handler 7, KeyError) and
    test_parse_sanitized_retry.py::test_a_raising_reroute_is_recorded_and_ends_the_loop (handler
    6, RuntimeError). Both survive option A unchanged"; "test_parse_gate.py:383 drives a sqlite3
    error into handler 12 through :997, and stays green under A by code order"; F2, "class
    UnknownStage(KeyError): (effective_config.py:75) is outside the tuple"; F3, handler 6
    "overlaps L07-C4 (C56's note, Class 1, P4)".
  INFERRED: none.

R790(a) — The 12ae-U26A final report is accepted as printed: item 0 5e38485 (Block 187;
  INT-i12-31 CLOSED in INT-g12-6's form; the dated units line, 37 done 28; brief_12ae_U26A.md),
  the read-out e5d9fed with census and probe, ledgers 4b3b43f and 97dd091 with trailers. No stop.
  The brief file is byte-equal to the architect's copy: the sixth match of six. The I2 mapping
  stands as the handler numbering of record for U26.
R790(b) — U26's design: option A, U02's form (`if is_run_fault(exc): raise` as the first
  statement of the conversion) at all six handlers — :717 and :806 (handlers 4–5), :901 (6),
  :1120 (7), :520 and :525 (12–13), line numbers at e5d9fed, anchors beside the quoted clauses.
  At 4–5 the re-raise follows `_record_error`, so the attempt ledger keeps the failed vision
  row and the outer :997 commits it through `_commit_attempts` (the read-out's "recording
  first" form); at 6 it precedes the `break`, so a run fault in a re-routed tier propagates
  instead of the gate-failed attempt being accepted and the paper excluded; at 7 and 12–13 it is
  the first statement. R612(g)'s "handlers 12–13 with U26" is met. The `except ParseFailed` at
  :1104, the asset handler at :978 and the outer :997 are unchanged. `run_pipeline` keeps the
  close (:199–216): a propagated store fault closes `failed`, a `RunAborted` or server refusal
  `aborted`, as the extract path does.
R790(c) — UnknownStage stays outside the tuple (F2). It is a KeyError subclass raised by
  `stage_config` for a stage the spec does not declare — a configuration error, present before
  the first paper and the same for every paper, not an environment fault that appears mid-run.
  Under A it is still converted to `parse_unclassified_error` at handler 7, as today; whether a
  spec-level error should refuse the run at open rather than per paper is 023/P6's question (the
  stage set is declared at `open_run`). A dated note on C56 records it; no tuple change in U26
  (a tuple change is U02's contract and would be its own ruling).
R790(d) — Not taken: B (4–7 only) — it leaves R612(g) unmet, so C56's parse half could not close
  on U26, and the swallow at 12 hides the first symptom of a locked database while handler 7
  then propagates the second. Not taken: C (a run close inside `parse_all_pdfs`) — the extract
  loop does not close the run; `run_pipeline` does, and C would hand it an already-closed run.
R790(e) — Fail-first and callers: tests/test_u26_parse_run_faults.py::
  test_run_faults_leave_the_parse_handlers_as_themselves, parametrized over the six handlers ×
  {a sqlite3.Error, an UndeclaredCall, a CeilingUnavailable (or the three members the probe
  used, named), KeyError} in fake_probe_u26a.py's seams (no server, a scratch ReviewDatabase or
  the event writer on a temp file; handler 6's vision route forced as the probe forced it,
  stated): a tuple member propagates out of `parse_all_pdfs` (or out of `parse_pdf` for 4–6 when
  driven there) with no `parse_failed` event, no asset row and no `PDF_EXCLUDED` write for that
  paper — and at 4–5 the attempt row present; KeyError keeps today's outcome
  (`parse_unclassified_error` at 7; `parse_vision_exhausted` at 4–5; the exclusion at 6; the
  lost ledger at 12–13). Red on the parent for every tuple-member case (24 → 18 red, 6 KeyError
  green by construction, the preservation half), recorded in b0_parent_acceptance.txt; green in
  the gate. The two pinning tests and test_parse_gate.py:383 stay as they are (no run fault in
  any; the read-out's §4). No existing test moves.
R790(f) — Notes at 12ae-U26B's item 0: on C56, this ruling's (c) (UnknownStage) and that handlers
  4–7 and 12–13 land in U26 under A; on L07-C4's note (C56's row, Class 1, P4): U26 makes a run
  fault at the re-route propagate; "try the next tier" for a non-run-fault error stays P4's, and
  P4's change will move test_parse_sanitized_retry's "ends the loop" assertion (F3). INT-ae12-2
  is not opened: F5 (pipeline.md:407 says `parse_pdf()` "raises ValueError" for empty text, at
  HEAD `ParseFailed(parse_cascade_empty)`) joins the HK-ARCH line as a dated note (Class 3, P7),
  the file not edited before P7.
R790(g) — Readings, no row: F1 (the architect's R4 listed 14 except lines, the census found 21
  — a grep at a different offset, the quoted-line rule held and the read-out's list governs);
  F4 (test_parse_gate.py:383 already drives a sqlite3 error into handler 12 — green under A by
  code order, confirmed in the gate); F6 (E1–E3 held; the entry's second pinning test is
  test_parse_sanitized_retry's re-route test). The read-out's §9 limits stand.
R790(h) — The build opens as one brief, 12ae-U26B (R785(h)'s form): six re-raise lines in one
  module, every handler quoted at HEAD with its mapping, the seams proven by the probe. The brief
  re-reads the six clauses before the first edit and stops on any difference. The inventory is
  regenerated under R786(b)'s form if the edit moves a recorded line (R789(d) finding 2). No
  model call, no live write; the standard gate against 837–1021 s. This ruling states no
  standing rule.
===== END r790.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ae-U26B] — U26's build: `if is_run_fault(exc): raise` at the six
  parse handlers (R790(b)), the acceptance test red on the parent, one engine commit,
  the standard gate, the evidence commit. Item 0: R790 as the next block; the C56,
  L07-C4 and HK-ARCH notes; this brief committed as pasted; no standing rule.
PURPOSE: Land U26 (C56's parse half) as R790(b) and (e) rule it. Code task: fail-first
  acceptance test, then the edits, one engine commit with its gate, evidence in its own
  commit. No live write. No model call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  Touched: engine/parsers/pdf_parser.py (six re-raise insertions, one import if needed);
  tests/test_u26_parse_run_faults.py (new); docs/inventory/entry_points.{json,md} only
  under R786(b)'s form if --check drifts (line moves; EXPECTED, since lines are inserted);
  docs/plan/ENGINE_REFACTOR_PLAN.md (Block 188; the C56 note; the L07-C4 note; the HK-ARCH
  line); docs/session-reports/session-12/12ae_open/r790.txt; a new directory
  docs/session-reports/session-12/12ae_U26B/ (brief_12ae_U26B.md at item 0;
  b0_parent_acceptance.txt, gate_u26b/ and the pin recompute at step 3);
  ~/scratch/12ae-u26b/; the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): any other file under engine/, tests/,
  scripts/, analysis/; review_specs/, the codebook; engine/core/extraction_events.py's
  tuple (R790(c): no tuple change); the tools; the id list, the glosses file and
  STANDING_RULES.md (not one byte — R790 states no rule); review.db (mode=ro instruments
  only); the working tree inside 07:00–10:35 UTC (R86); any scratch directory other than
  ~/scratch/12ae-u26b/.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ae-U26A report; R790): HEAD e5d9fed, parent 5e38485, clean, level; the engine
    tree equal to 168199a's; plan ends "…ends at R789.", last block 187; STANDING_RULES.md
    87 / 8 / 95, glosses 43, the id list's last A line "A R784(g)"; claude-config at
    97dd091 or an accepted descendant (R663(b)); gate reference U25's
    (12ae_U25B/gate_u25b/: 3,501 passed / 1 xfailed / 17 deselected on 3,519 / 3,502; its
    collected_ids.txt and durations file, name quoted); band 837–1021 s (R783(b)); pins
    12ae_U25B/pin_tuples_u25b.json. Architect's hash of r790.txt: Section 1.
  READ (12ae_U26A/12ae_U26A.md at e5d9fed: the I2 mapping, §2's clauses, §3's seams and
    §5's probe form — not repeated; step 2a re-reads each at HEAD).
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 168199a. I2. The six clauses
    read at HEAD as the read-out quotes them at :717, :806, :901, :1120, :520, :525, and
    extractor.py:439 shows U02's form as quoted.
  INFERRED — expectation: E1. 18 of the 24 parametrized cases are red on the parent (every
    tuple-member case), the 6 KeyError cases green; E2. The new ids are 24 (or the
    probe's count, read); the gate collects 3,519 + N and selects 3,502 + N, only-gate
    exactly those; no other test moves (the read-out's §4; the two pinning tests and
    test_parse_gate.py:383 green by code order). E3. inventory --check drifts on
    pdf_parser.py's recorded line numbers only (six insertions), regenerated under
    R786(b)'s form.

CONTRACT (steps, in order; evidence under ~/scratch/12ae-u26b/):

  STEP 0 — Pre-check (read-only). date -u (outside 07:00–10:35 UTC; the gate must exit
    before 06:30 UTC or launch after 10:35). HEAD and parent proved by git cat-file -e
    <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. Plan: last line "…ends at
    R789.", last block 187, no Block 188. Rules files: the three counts above.
    claude-config: 97dd091 or an accepted descendant, clean, level. I1. Fingerprint
    --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12ae-U25B's item 0
    — a block, one note, the brief file — ~/scratch/12ae-u25b/check_item0_u25b.py and its
    chain, quoted before composing).
    1a. 12ae_open/r790.txt from Section 1; hash straight after the write: EXPECTED
        b79f11f5be4521f418f9bb77f2404c9b61fd5b0db287f99d10fa44783aff82a8, 8200, 94.
        Mismatch = STOP.
    1b. block188.json in block187.json's key set: tag R790; item "item 0 of the
        12ae-U26B brief"; beside = "the C56, L07-C4 and HK-ARCH notes and the brief file it
        orders" (composed; quoted); made_date 2026-11-09; session 12ae; report = the
        12ae-U26A final report (item 0 5e38485; read-out e5d9fed), final-report form;
        sentence model, followed "12ae-U26B brief"; prev_ends R789. Render to stdout,
        compare with Block 187's order, then --append ("−" EXPECTED 0 for the append, read
        at 1e).
    1c. Notes (R790(c), (f)): (i) on C56's status cell, "*Note 2026-11-09 (12ae-U26B item 0,
        R790(c), R790(b)):*" — UnknownStage outside the tuple, 023/P6's question; handlers
        4–7 and 12–13 land in U26 under A, in CC's words from the ruling; (ii) on the
        L07-C4 note inside C56's row (the Wave 1 row 26 note), appended in its own form,
        "*Note 2026-11-09 (12ae-U26B item 0, R790(f)):*" — U26 propagates a run fault at
        the re-route; "try the next tier" stays P4's and will move
        test_parse_sanitized_retry's "ends the loop" assertion; (iii) on the HK-ARCH
        housekeeping line, a dated note in that line's form — pipeline.md:407's
        "raises ValueError" is `ParseFailed(parse_cascade_empty)` at HEAD (F5), not edited
        before P7. C56's row is one row: (i) and (ii) are one row replaced; (iii) one line
        replaced. The "−" count read and matched to two.
    1d. The brief file (R784(g)(v)): 12ae_U26B/brief_12ae_U26B.md from this whole pasted
        block, "CLAUDE CODE" through the last HANDBACK line, one trailing newline, written
        once; sha256, bytes, lines straight after the write, reported.
    1e. The item-0 check in 12ae-U25B's content form, adapted: --check-plan
        byte-identical; the plan's numstat read ("−" matched to the two rows); every
        earlier "ends at" line kept; last line "…ends at R790." once; the plan equals
        HEAD's plus the two rows replaced and Block 188 appended, nothing else (computed);
        r790.txt equal to 1a; the three rules files unchanged and unstaged; the brief file
        hashed; the staged set exactly the plan, r790.txt and the brief file. Evidence by
        redirect; quote the check.
    1f. <check> && <commit>, one line; message from the staged set (R778(d)), docs scope.
        Fingerprint --compare before the push; push; level; --compare after (IDENTICAL).
        Ledger row with trailers; push; level. Record hashes and parents.

  STEP 2 — The build (one engine commit).
    2a. Re-read at HEAD and quote: the six clauses with their bodies (I2, by the mapping
        table's lines), `_record_error`'s call at :717/:806 and the `raise ParseFailed`
        after it, `select_attempt`'s call at :907, extractor.py:439–441 (U02's form), and
        fake_probe_u26a.py's seams (how each handler was reached, how handler 6's vision
        route was forced, the scratch database form). Any difference from the read-out =
        STOP.
    2b. Write tests/test_u26_parse_run_faults.py, fail-first (R790(e)):
        test_run_faults_leave_the_parse_handlers_as_themselves, parametrized over the six
        handlers × {the three tuple members the probe used, KeyError}, in the probe's
        seams (no server; the conftest fence on; a scratch ReviewDatabase or the event
        writer on a temp file; the vision route forced for 4–6 as the probe forced it,
        stated in the docstring). Tuple-member cases: the exception propagates out of the
        entry point driven (parse_all_pdfs, or parse_pdf for 4–6 — state which per
        handler); no parse_failed event, no asset row, no PDF_EXCLUDED write for that
        paper; at 4–5 the attempt row present. KeyError cases: today's outcome asserted
        (the read-out's §3 per handler). Run on the parent: EXPECTED E1 — 18 red, 6
        green; output to b0_parent_acceptance.txt. A tuple-member case green on the
        parent = STOP.
    2c. Edit engine/parsers/pdf_parser.py, R790(b) exactly: `if is_run_fault(exc): raise`
        (importing is_run_fault from engine.core.extraction_events as extractor.py does,
        quoted) — at :717 and :806 after `_record_error(...)` and before the `raise
        ParseFailed`; at :901 before the `break` (the handler's exception bound to a name
        if it is not); at :1120 first; at :520 and :525 first. No other line changes. git
        diff --stat EXPECTED: pdf_parser.py and the new test — two files, plus the two
        inventory files under R786(b)'s form (E3: --check first, the drift quoted whole,
        EXPECTED line moves in pdf_parser.py's recorded entries only, no data/ listing
        change, no entry-point change; --write; --check in sync).
    2d. Pins: 12l_U04/pin_tuples.py with both arguments, byte-identical to
        12ae_U25B/pin_tuples_u25b.json; the audit prompt_hash unchanged. A moved value =
        STOP.
    2e. Selected runs green: the new file; tests/test_parse_failures.py,
        tests/test_parse_sanitized_retry.py and tests/test_parse_gate.py whole; every test
        file census_u26a.txt lists as patching the parser (the census's file list, count
        quoted). Commit the engine change (fix scope; the message names C56/U26, the six
        handlers by the mapping, and the inventory if regenerated), from the staged set.
        Do not push before the gate.
    2f. The standard gate by launch_gate.sh: evidence_dir ~/scratch/12ae-u26b/gate_u26b/,
        session name, ref_ids 12ae_U25B/gate_u25b/collected_ids.txt, ref_durations the
        durations file in 12ae_U25B/gate_u25b/ (name quoted), no selection. Ollama reads
        ≥5 s before and after (R660(b)). EXPECTED: 3,501 + N passed / 1 xfailed / 17
        deselected on 3,519 + N / 3,502 + N (E2; N read from the new file's collection),
        only-gate the N new ids, only-ref 0; wall inside 837–1021 s; no phase >10 s slower;
        no model load in the window (R696(b)); the nineteen files. A red: class (c) per
        R746(c)/R772(b), re-run once; any other red = STOP, the engine commit standing.
    2g. Fingerprint --compare; push; level; --compare after (IDENTICAL). Ledger row; push;
        level.

  STEP 3 — Evidence commit (R731(c)). 12ae_U26B/: b0_parent_acceptance.txt, gate_u26b/
    (the nineteen files), pin_tuples_u26b.json; the staged set exactly those, every sha256
    equal to its scratch source; chained check && commit; fingerprint IDENTICAL before and
    after the push; level; ledger. Record hashes and parents.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 and I2 held.
  G2. Item 0 one commit: r790.txt at the architect's hash; Block 188 by the tool; the
      three notes in the record's form, the plan equal to HEAD's plus exactly those edits
      and the block; the three rules files untouched by content; the brief file hashed;
      one chain; pushed, level, fingerprint IDENTICAL; ledger.
  G3. The tuple-member cases red on the parent and green in the gate; the KeyError cases
      green on both; the diff exactly two files plus the inventory under R786(b)'s form;
      pins and audit prompt_hash unchanged; the gate at the E2 counts inside the band, no
      load, nineteen files; engine commit pushed, level, fingerprint IDENTICAL; ledger.
  G4. The evidence commit as step 3 states; pushed, level, fingerprint IDENTICAL; ledger.

OUT OF SCOPE: C56's CLOSED note (C56 closes when U27's cloud handlers also land — R542;
  the parse half's landing is noted at the next item 0, not a close); U27; handlers 3
  and 14–15 (P7); the tuple (R790(c)); L07-C4's "next tier" (P4); pipeline.md:407
  (HK-ARCH); any other edit under engine/, tests/, scripts/, analysis/,
  docs/architecture/ or CLAUDE.md; the tools items; /wrap.

CONSTRAINTS (task-only): no model call, no restart, no live write; no edit in the
  working tree between 07:00 and 10:35 UTC; a count arrived at by eye is INFERRED, the
  computed value given with its command; where this brief describes a tool or record,
  the record governs and a difference is a finding (R732(a)); a handler is named only by
  the I2 mapping.

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — metadata, render comparison, the
  two rows before and after, the brief file's hash, the check verbatim, chain, hashes,
  fingerprint, ledger; (2) the build — 2a's quotes, the test as written,
  b0_parent_acceptance.txt verbatim, the six insertions as a diff, the diff --stat, the
  inventory drift and regeneration, the pin recompute, the selected runs, the engine
  commit hash and parent; (3) the gate — launch line, timestamps, wall vs 837–1021, the
  summary line verbatim, collected/selected, the id comparison, durations >10 s or none,
  basetemp, journal window, Ollama reads, concurrent jobs, the nineteen files; (4) the
  evidence commit; (5) contradictions and findings, numbered, each with its quoted line;
  (6) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 or I2 false, a 1a mismatch, or a check
  failure or crash at 1e = STOP before the item-0 commit. A tuple-member case green on the
  parent, a diff outside the two files (inventory excepted under R786(b)'s form), or a
  moved pin = STOP before the engine commit. A non-(c) red in the gate = STOP, the engine
  commit standing and reported. An expectation mismatch (E1–E3) is reported and the work
  continues. A spec premise that does not survive contact with disk is a finding, not an
  obstacle.

HANDBACK: Stop and report. Do not start U27 without explicit sign-off. Await the next
  brief or ruling in this session. STOP-12AE-U26B
