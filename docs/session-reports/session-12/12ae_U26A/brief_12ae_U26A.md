CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R789, PI ruling 2026-11-09 UTC; not on disk)
================================================================================

R789 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r789.txt: sha256 a3dba267675ba18a8677badf0bf89c6c4ce2459e52b8662099cc860881d45365,
            5957 bytes, 69 lines, one trailing newline.

===== BEGIN r789.txt =====
RULING [evidence-engine 12ae-R789] — 12ae-U25B accepted: U25 landed, INT-i12-31 closes; the series
  gains 921.98 s; readings, one the architect's; U26's Phase A opens
STATUS: PI ruling, 2026-11-09 UTC, session 12ae, on architect recommendation, on the 12ae-U25B
  final report (item 0 4a4c850; engine 168199accbd308e232cc881c3477edc233a668a6; evidence
  0854f294491ee8f359c446abfbf37d3d14fc2e7f). Transcribe at item 0 of the 12ae-U26A brief below,
  by transcribe_ruling.py, in the block after R788's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the report):
  MEASURED: "Item 0 landed at 06:43:45 UTC (4a4c850)"; "the clock at 2a read 16:11:17 UTC";
    "12ae_U25B/brief_12ae_U25B.md is b22fcbaf1ddabffbb9f6690c72a3b98181254575e01991dfaa5483de16ed8b8d,
    23,727 B, 310 lines" (the architect's copy: equal); "b0 on the parent … 5 failed, 1 passed";
    "value_yes, sentinel_no, sentinel_null and value_one: Failed: DID NOT RAISE"; "instruction_no:
    assert 'quote' in "…: field 'study_type' has an empty `instruction`.""; "codebook.py +26 /
    −0"; "3 JSON paths in codebook.py (path_construction, spec_access, yaml_loads), each a
    line-number move of +26"; "No data/ listing changed and no entry point changed"; "Diff. 6
    files"; "pin_tuples_u25b.json is byte-identical to 12ae_U24B/pin_tuples_u24b.json"; "The new
    file: 6 passed"; "110 passed"; "93 files: 1,546 passed"; "168199accbd308e232cc881c3477edc233a668a6,
    parent 4a4c850"; "wall 921.98 s"; "==== 3501 passed, 17 deselected, 1 xfailed, 2 warnings in
    919.97s (0:15:19) ===="; "collected 3,519 / 17 deselected / 3,502 selected"; "only-gate exactly
    the six U25 ids, only-ref 0"; "Journal window: -- No entries --"; "the idle surgical-cv uvicorn
    (pid 1017068) only"; "all 19 present"; "Ledger 3614d25, parent 4e9fa58";
    "0854f294491ee8f359c446abfbf37d3d14fc2e7f, parent 168199a"; "21 files, added only (+12,543)";
    "Ledger 7b473a9, parent 3614d25"; "the live codebook was never written (89dbfa91…ad82
    throughout)"; "The 09:00 nightly ended pytest exit code: 0".
  READ (CC's): findings 1–5; "_validate_fields:327–330's str(f[key] or "") turns False into ""
    and raises "…has an empty instruction.""; "The committed message says "quote the value as
    written in the YAML""; "Your first resume came at 06:51 UTC, inside the window, so I declined
    it and edited nothing".
  INFERRED: none.

R789(a) — The 12ae-U25B final report is accepted as reported: item 0 4a4c850 (Block 186; the
  INT-i12-31 note; the brief file byte-equal to the architect's copy, the fifth match of five),
  the two fixtures derived from the live text by derive_fixtures.py, the engine commit 168199a
  (six files: codebook.py +26 / −0, the test, the two fixtures, the inventory regenerated under
  R786(b)'s form with the drift read whole first — line numbers only), the gate 3,501 / 1 / 17 on
  3,519 / 3,502 at 921.98 s inside 837–1021 s, only-gate the six U25 ids, no load, the nineteen
  files, green at the first run; the evidence commit 0854f29; ledgers 3614d25 and 7b473a9 with
  trailers. Both pins, the audit prompt_hash, the live codebook's sha256 and its semantic hash
  byte-identical. CC's refusal of the 06:51 UTC resume inside the window was right (R86; the
  brief's TIMING); nothing was edited. U25 is landed; P1's fix units 37, done 28.
R789(b) — INT-i12-31 CLOSES on U25 (168199a), by R514: the row's stated fix is delivered as
  R788(b) widened it (one `_require_str` predicate in the three validators, over the values and
  definitions, every sentinel, the tokens, and each field's rendered text) and its acceptance test
  passes (tests/test_u25_codebook_scalars.py::test_unquoted_yaml_booleans_and_nulls_are_refused_at_load,
  five cases red on the parent 4a4c850 in 12ae_U25B/b0_parent_acceptance.txt, green in
  12ae_U25B/gate_u25b/, with ::test_the_live_codebook_loads_unchanged beside it, green on both).
  Noted CLOSED at 12ae-U26A's item 0 in INT-g12-6's note form; counted until P1 closes (R607(d),
  R753(d)(ii)). The same item 0 writes the dated units line (37, done 28: U01–U25, U37, U38, U39).
R789(c) — The series on this host state gains 921.98 s: thirty-four. The band stays 837–1021 s
  (R783(b)).
R789(d) — Readings, no row: finding 1 (the instruction_no case raised on the parent with the
  wrong message — `str(f[key] or "")` turns False into "" and the "empty instruction" refusal
  fires; R788(d)'s "nothing raises at load" was the architect's, wrong for that one case, and
  fail-first held because the case was red on the "quote" assertion; the type check now runs
  first, so the author gets the right message — the mechanism F5 found for escape_token: No);
  finding 2 (E3 false: a 26-line insertion moves line numbers the inventory records; the
  regeneration under R786(b)'s form is the rule for any engine edit that moves a recorded line);
  finding 3 (the message says "quote the value as written in the YAML", not the coerced word —
  CC's wording accepted; both forms meet R788(d)); finding 4 (an unquoted date or version is
  refused — inside R788(b)(iii); the 93-file run and the gate show no existing codebook refused);
  finding 5 (the reference durations file is durations_12ae_u24b_rerun.txt, named by the re-run's
  session — the wrapper's form, quoted). The 2026-11-09 nightly's exit code 0 is a reading; the
  next open reads it in full against 12ad_close/expected_nightly_20261102.md's successor, which
  the 12ae close writes.
R789(e) — U26's Phase A opens: 12l_P1A.md §3's U26, "Run-fault predicate at the parse handlers
  (P1's tail)" (C56, handlers 4–7 and 12–13 in 12f_triage.md's converting-handler numbering,
  R542, R612(g)), read-only, in 12ae_U25A.md's form, its entry quoted first. Not on Run 7's path
  (the 190 papers are parsed). No engine, test, script, spec or codebook edit; no model call; no
  live write. This ruling states no standing rule.
===== END r789.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ae-U26A] — U26's Phase A read-out (C56's parse-path handlers:
  12f_triage.md's converting handlers 4–7 and 12–13, in engine/parsers/pdf_parser.py).
  Item 0: R789 as the next block; INT-i12-31 CLOSED; the dated units line; this brief
  committed as pasted; no standing rule. Read-only on engine/ and tests/; STOP before
  any edit.
PURPOSE: Put on disk, before the design, what each parse-path handler catches and
  converts today, which of U02's run-fault classes can reach it and by what route, what
  the paper and the run record when one does, which tests pin the converting behaviour
  (the entry names two), and whether the acceptance test can be written at HEAD and
  fails. Mixed: one docs commit (item 0), one docs commit (the read-out). No engine,
  test, script, spec or codebook change. No live write. No model call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 187; INT-i12-31's CLOSED note; the
  dated units line); docs/session-reports/session-12/12ae_open/r789.txt; a new directory
  docs/session-reports/session-12/12ae_U26A/ (brief_12ae_U26A.md at item 0; the read-out,
  census and probe scripts with outputs at step 2); ~/scratch/12ae-u26a/; the
  claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/, the codebook, docs/architecture/, CLAUDE.md, docs/inventory/, the tools;
  the id list, the glosses file and STANDING_RULES.md (not one byte — R789 states no
  rule); review.db (mode=ro instruments only; the probe uses a scratch database or none);
  any scratch directory other than ~/scratch/12ae-u26a/; the working tree inside
  07:00–10:35 UTC (R86).

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ae-U25B report; R789): HEAD 0854f294491ee8f359c446abfbf37d3d14fc2e7f, parent
    168199accbd308e232cc881c3477edc233a668a6, clean, level; the engine tree equal to
    168199a's; plan ends "…ends at R788.", last block 186; STANDING_RULES.md 87 / 8 / 95,
    glosses 43, the id list's last A line "A R784(g)"; claude-config at 7b473a9 or an
    accepted descendant (R663(b)). Architect's hash of r789.txt: Section 1.
  READ (the clone at 0854f29): R1. 12l_P1A.md §3's U26 entry, whole: "### U26 — Run-fault
    predicate at the parse handlers (P1's tail) / Rows: C56 / Pin effect: not affecting /
    Files: engine/parsers/pdf_parser.py / Depends on: U02 / Acceptance test (synthetic;
    discharges R542): test_run_faults_leave_the_parse_handlers_as_themselves — The
    predicate's classes raised inside the vision route, the re-route and parse_all_pdfs'
    per-paper handler propagate, and the paper is not recorded parse_failed; an
    unclassified error such as KeyError still is. Fails on the unmodified tree. / Note:
    Handlers 4–7; 12–13 with them if ruled (§2 D6). Not on the Run 7 path: the 190 papers
    are already parsed. Two existing tests pin today's converting behaviour and are
    callers of this change." R2. 12f_triage.md's converting-handler table (its rows 4–5,
    6, 7 and 12–13 name pdf_parser.py::parse_pdf's two vision routes, the gate-driven
    re-route, parse_all_pdfs' per-paper handler and _commit_attempts' outer and inner
    handlers) and 12f_C56-A.md's 95-handler census are two numberings of the same
    handlers; row C56's notes (R542, R612(g) "D6: … One declared run-fault tuple, exact
    names fixed in U02's handoff; handlers 12–13 with U26; 14–15 to P7 with handler 3")
    use the triage numbering. R3. U02 landed at 13822e6: engine/core/extraction_events.py
    defines run_fault_classes() (:429) and is_run_fault(exc) (:478), the one predicate
    re-raised ahead of extract-path handlers 1 and 2 (12l_U02.md). R4. pdf_parser.py at
    0854f29 has broad except clauses at :453, :459, :464, :481, :520, :525, :672, :708,
    :717, :733, :756, :764, :786, :806 (grep; anchors only beside the quoted line at 2b).
    R5. 12ae_U25A/12ae_U25A.md is the form (§0–§9); §2 "Sites" here means the handlers
    and the raise sites inside them.
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 168199a. I2. The handler
    numbering is reconciled on disk before any handler is named in the read-out: each of
    the triage table's handlers 4–7 and 12–13 mapped to its 12f_C56-A.md census row and to
    its except clause at HEAD by quoted line, in a table — a handler that cannot be mapped
    is a STOP, not a guess.
  INFERRED — expectation: E1. The two tests the entry says pin the converting behaviour
    are found by the census and named; E2. A probe with a fake parser raising a run-fault
    class (one of U02's tuple) inside each handler shows the paper recorded parse_failed
    at HEAD, with no server and no live database (a scratch ReviewDatabase or the event
    writer on a temp file); E3. parse_all_pdfs is reachable only from run_pipeline's PARSE
    stage and parse_expanded_corpus.py (the 12l_P1A note: not on Run 7's path).

CONTEXT: U25 landed at 168199a (R789); INT-i12-31 closes at this item 0. U26 is C56's
  parse half (R542: the predicate lands first with the extract handlers — U02, done —
  then the parse handlers 4–7 in their own commit; R612(g) adds 12–13 to U26). Phase A
  of the two-phase form; the design ruling follows; one build brief if Phase A is
  complete (R785(h)'s form).

CONTRACT (steps, in order; evidence under ~/scratch/12ae-u26a/):

  STEP 0 — Pre-check (read-only). date -u (outside 07:00–10:35 UTC). HEAD and parent
    proved by git cat-file -e <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree.
    Plan: last line "…ends at R788.", last block 186, no Block 187. Rules files: the
    three counts above. claude-config: 7b473a9 or an accepted descendant, clean, level.
    I1. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12ae-U25A's item
    0 — the one with a CLOSED note and a dated line — ~/scratch/12ae-u25a/check_item0_u25a.py
    and its chain, quoted before composing).
    1a. 12ae_open/r789.txt from Section 1; hash straight after the write: EXPECTED
        a3dba267675ba18a8677badf0bf89c6c4ce2459e52b8662099cc860881d45365, 5957, 69.
        Mismatch = STOP.
    1b. block187.json in block186.json's key set: tag R789; item "item 0 of the
        12ae-U26A brief"; beside = "the INT-i12-31 CLOSED note, the dated units line and
        the brief file it orders" (composed; quoted); made_date 2026-11-09; session 12ae;
        report = the 12ae-U25B final report (item 0 4a4c850; engine 168199a; evidence
        0854f29), final-report form; sentence model, followed "12ae-U26A brief"; prev_ends
        R788. Render to stdout, compare with Block 186's order, then --append ("−"
        EXPECTED 0 for the append, read at 1e).
    1c. INT-i12-31's CLOSED note (R789(b)) in its Evidence cell, in INT-g12-6's and C49's
        note form, naming the commit, both tests, the b0 file and the gate directory;
        "Counted until P1 closes (R607(d), R753(d)(ii))". The dated units line after the
        2026-11-09 one of 12ae-U25A's item 0, in its form: U25 landed at 168199a;
        INT-i12-31 CLOSED on it; fix units 37, done 28 (U01–U25, U37, U38, U39); rows 284
        (94 / 56 / 134, 0 unclassed) unchanged; rows out 0. One row replaced (the
        section-L row at its own text, not the owner line — 12ae-U25B's finding 1), one
        line inserted; the "−" count read and matched to the one row.
    1d. The brief file (R784(g)(v)): 12ae_U26A/brief_12ae_U26A.md from this whole pasted
        block, "CLAUDE CODE" through the last HANDBACK line, one trailing newline, written
        once; sha256, bytes, lines straight after the write, reported.
    1e. The item-0 check in 12ae-U25A's content form, adapted: --check-plan
        byte-identical; the plan's numstat read ("−" matched to the one row); every
        earlier "ends at" line kept; last line "…ends at R789." once; the plan equals
        HEAD's plus the row replaced, the line inserted and Block 187 appended, nothing
        else (computed); r789.txt equal to 1a; the three rules files unchanged and
        unstaged; the brief file hashed; the staged set exactly the plan, r789.txt and the
        brief file. Evidence by redirect; quote the check.
    1f. <check> && <commit>, one line; message from the staged set (R778(d)), docs scope.
        Fingerprint --compare before the push; push; level; --compare after (IDENTICAL).
        Ledger row with trailers; push; level. Record hashes and parents.

  STEP 2 — The read-out (read-only; no server; no live database; a scratch database or
    the event writer on a temp file for the probe).
    2a. §1: the U26 entry with its neighbours (R1); row C56 whole with every dated note
        (long — quote the row text and the notes that name handlers 4–7, 12–13, U02, U26,
        R531–R533, R542, R612(g), R614(d)); 12f_triage.md's converting-handler table rows
        4–5, 6, 7, 12–13 and its C56 row; 12f_C56-A.md's census rows for the same handlers
        (I2's mapping table, built here); 12l_U02.md's §§ on the predicate (the tuple's
        exact members at HEAD, quoted from extraction_events.py:429–481) and how handlers
        1 and 2 were changed (the re-raise form, quoted from extractor.py at HEAD); every
        CLAUDE.md and docs/architecture line on the parse stage's error handling; §1 2e:
        the questions the inputs raise that Phase A does not answer.
    2b. §2 Sites: every broad except in pdf_parser.py (R4), path:line, the clause and its
        body quoted, classified: a converting handler of U26 (4–5, 6, 7, 12–13 by the
        mapping), a handler of U26's scope the triage table did not number (reported, not
        changed), or outside U26 (a narrower except, or a handler the triage table placed
        elsewhere). For each U26 handler: what it catches, what it does with the exception
        (records parse_failed with which reason code? logs? breaks the cascade? swallows?),
        and whether it re-raises anything today.
    2c. §3 Mechanism (CODE ORDER unless FAKE): for each U26 handler, the route by which a
        member of run_fault_classes() can reach it — the vision path's ollama_chat (its
        UndeclaredCall, UndeclaredOverride, CallDigestChanged, the env-drift refusal,
        CeilingUnavailable and the input-fit family, a recorder sqlite3.Error), the
        store's sqlite3 errors, EventRefused, StageNotInRun, UnknownStage — each named
        from the tuple as read, with the call chain quoted; what the paper's record
        becomes (the parse_failed event, its reason code, the parse_attempts rows, the
        papers.status write) and what the run's record becomes (does the run continue?
        does the manifest close completed over a run fault?); the gate-driven re-route
        (handler 6): why it raises nothing and what "the least-bad gate-failed attempt is
        accepted" means at HEAD, with the lines; _commit_attempts (12–13): what is lost
        and whether a run fault there can leave a committed parse with no attempt rows.
        FAKE where a fake parser or a fake client can show it with no server (the U16/U21
        fakes' form, quoted before reuse). State the U02 form for comparison: how
        extractor.py re-raises ahead of its handlers, quoted, so the ruling can choose the
        same form or say why not.
    2d. §4 Census: tests coupled to pdf_parser.py's handlers and to parse_all_pdfs,
        parse_pdf, _commit_attempts and the reason codes they write — by an AST
        instrument census_u26a.py in census_u25a.py's form (declared completeness as that
        file states it), classes of R722(a), R723(c), R724(a), R725(b); the two tests the
        entry names found and quoted (E1), with what a re-raise would do to each (a
        rewrite in words, or a new assertion); any test that drives a run fault into a
        parse handler today (EXPECTED none; a hit is a finding).
    2e. §5 The acceptance test against HEAD (FAKE, fake_probe_u26a.py → .txt): for each
        U26 handler, a fake raising one run-fault member and one unclassified error
        (KeyError) inside it; at HEAD both are converted (EXPECTED: parse_failed recorded,
        or the cascade broken, or the rows lost — per handler); the test the entry names
        can be written iff each handler can be reached by a fake without a server and the
        conversion observed in the record. State which handlers it covers and which need a
        different seam.
    2f. §6 Cases: each run-fault class at each handler; an unclassified error at each;
        the re-route with and without a run fault; _commit_attempts with a sqlite3 error
        before and after the parse file is published (B-F01's family — report the overlap,
        change nothing); under an active run and outside one (parse_expanded_corpus.py).
    2g. §7 Options for the ruling: A — U02's form at handlers 4–5, 6 and 7: `if
        is_run_fault(exc): raise` ahead of each conversion (and for 6, a raise where the
        re-route would otherwise accept a gate-failed attempt under a run fault); 12–13
        the same; B — A for 4–7 only, 12–13 left as Class 3 for P7 (the triage table's
        class), stating what a lost attempt row under a run fault costs; C — A plus
        parse_all_pdfs closing the run 'aborted' on the first run fault (C26's token), as
        the extract loop does — read whether the extract loop does that at HEAD and quote
        it. For each: sites, the two pinning tests' rewrites, the acceptance test's shape,
        and what it does not fix. Classified sites (3g). §8 findings; §9 limits.
    2h. Commit the read-out (12ae_U26A/12ae_U26A.md), census_u26a.py/.txt,
        fake_probe_u26a.py/.txt: the staged set exactly those, nothing under engine/,
        tests/ or data/ (asserted); fingerprint IDENTICAL before and after the push; level;
        the ledger row. Record hashes.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 held.
  G2. Item 0 one commit: r789.txt at the architect's hash; Block 187 by the tool; the
      CLOSED note and the dated line in the record's form, the plan equal to HEAD's plus
      exactly those edits and the block; the three rules files untouched by content; the
      brief file hashed; one chain; pushed, level, fingerprint IDENTICAL; ledger.
  G3. I2's mapping table present before any handler is named; the read-out in R5's form
      with every section present; every code claim beside a quoted line with path and
      line read at HEAD; CODE ORDER / FAKE labelled; the census and probe instruments and
      outputs committed with their declared completeness; the Ollama journal for the
      probe window empty; no file outside 12ae_U26A/ in the commit; pushed, level,
      fingerprint IDENTICAL; ledger.

OUT OF SCOPE: U26's design and build; U27 (the cloud handlers 8–11); handlers 3 and
  14–15 (P7); any edit under engine/, tests/, scripts/, analysis/, docs/architecture/ or
  CLAUDE.md; B-F01's fix; the tools items; the 2026-11-09 job reads (the next open's);
  /wrap.

CONSTRAINTS (task-only): no model call, no restart, no live write; the probe opens no
  live database; a count arrived at by eye is INFERRED, the computed value given with its
  command; where this brief describes a tool or record, the record governs and a
  difference is a finding (R732(a)); a handler is named only through I2's mapping.

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — metadata, render comparison, the
  row and the dated line before and after, the brief file's hash, the check verbatim,
  chain, hashes, fingerprint, ledger; (2) the read-out's §0–§9 in full as committed, the
  mapping table first; (3) contradictions and findings, numbered, each with its quoted
  line; (4) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 false, a 1a mismatch, or a check failure or
  crash at 1e = STOP before the item-0 commit. A handler that I2 cannot map = STOP before
  the read-out is written. An expectation mismatch (E1–E3) is reported and the read-out
  continues. A spec premise that does not survive contact with disk is a finding, not an
  obstacle.

HANDBACK: Stop and report. Do not start U26's design or build without explicit
  sign-off. Await the next brief or ruling in this session. STOP-12AE-U26A
