CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R819, PI ruling 2026-11-20 UTC; not on disk)
================================================================================

R819 is transcribed at item 0 of the brief below, by
docs/session-reports/tools/transcribe_ruling.py, as the next block of the decision log
(Block 217). The lines between the two marker lines, exclusive of the markers, are the
byte content of r819.txt. Write the file from those lines exactly: same characters,
same line breaks, one trailing newline. Never retype a ruling. The architect's
independent hash:
  r819.txt: sha256 b962a51997821c4c76895648c9e985fe8b99247fd363164aa6155c08c19e03e6,
            10574 bytes, 130 lines, one trailing newline.

===== BEGIN r819.txt =====
RULING [evidence-engine 12ai-R819] — 12ai-SCRI-REC accepted; the Ollama restart change's design:
  both restart paths opt-in and off by default; a run asks through two flags recorded under
  one manifest key; a seventh call ending; a bound on pdf_quality_check; U41, Class 2, before
  U30; two rows; no standing rule
STATUS: PI ruling, 2026-11-20 UTC, session 12ai, on architect recommendation, on the
  12ai-SCRI-REC final report (item 0 483969d524b081c83b21deba2fda9c095a09b158) and on the
  committed read-out docs/session-reports/session-12/12ah_OLRA/12ah_OLRA.md (sha256
  f054eedbacc34d22168bf1c3b9d70a1fcbbc4dfe222f67410bd351bed3c09790) with its addendum
  12ah_OLRA_add1.md (sha256 568b179dc23253d0e4e285a06e564145b7b32d460a71d2a41af46a9c62e34ef1).
  Transcribe at item 0 of the 12ai-U41B brief below, by transcribe_ruling.py, in the block
  after R818's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12ai-SCRI-REC final report, quoted): "Commit
    483969d524b081c83b21deba2fda9c095a09b158 (parent bec3545)"; "Ledger
    a84a03e7ab9fbb07f581b0e14acec66851fc8d93 (parent 427d54d)"; "r818.txt: 5a83b282…eef0,
    7,638 B, 95 lines. Equal to the architect's hash."; "Brief file:
    e3257f3cdfb9dde6e9c0db5fe65e7a89ec6d8361ecb5a9727386d3134e58164a, 25,004 B, 350 lines."
    (the architect's copy: the same); "Now
    d81e330644285b58dc2897560108d934b0f7b8d21828a4dee21a474ada1ab243, 3,406,820 B, 20,340
    lines."; "12ai_SCRI/SHA256SUMS 96 of 96 match; 12ai_SCRI_RECA/SHA256SUMS 21 of 21 match";
    "The push went through with no refusal".
  READ (12ah_OLRA/12ah_OLRA.md at its committed bytes, each line the read-out's own):
    Q1.1 "engine/agents/extractor.py:61:   RESTART_EVERY_N = 25"; "**No `-n`.**"; Q1.2 "A read
    timeout is never retried inside `ollama_chat`"; Q3 "No spec field and no environment
    variable sets the cadence."; "changing `RESTART_EVERY_N` from 25 to 0, or making the
    recovery restart default-off through code, moves **no pin, no body key set and no
    prompt_hash or request_hash**"; Q4 "**No retry and no restart.**"; "`pdf_quality_check`
    has no bound."; Q5 "**`papers_since_restart` is not reset**" (CC's reading) and, for
    elicit01 and capture01, "**not caught**"; Q6 "**No test pins the value 25.**" and its
    "(c) coupled" rows; Q8's option D and "D alone does not reach any entry point that
    calls `ollama_chat` outside an active run (finding 8)"; finding 8 "hits only
    `engine/agents/ft_screener.py` (1) and `scripts/run_pipeline.py` (3)"; Q7's list of
    text. From the addendum: Q11 "The only load-capable send is `_client.chat`".
  READ (the architect's clone at 483969d, whose engine tree equals 977c499's):
    engine/core/run_manifest.py:93–96, the tuple of six; engine/migrations/
    022_run_kinds_and_audit_tables.py:33 "`outcome_detail TEXT` (nullable, unconstrained)";
    engine/utils/ollama_client.py:822 "Ollama call timed out after {attempt + 1} attempts
    + restart "; :55–56, joined, "this ends a request (U21, R761(c)(i)), the watchdog only
    marks it late"; engine/acquisition/pdf_quality_check.py:42 "MAX_RETRIES = 1", its
    per-paper "except Exception as exc:" and a main() with no exit status after its report;
    engine/core/review_spec.py:734–735 "default=120" and "Ollama request timeout in
    seconds"; engine/core/extraction_events.py:138 "CONSECUTIVE_FAILURE_ABORT = 3" and its
    run_fault_classes(); scripts/parse_expanded_corpus.py:63 "AND
    p.pdf_quality_check_status = 'HUMAN_CONFIRMED'"; engine/core/run_manifest.py:1062
    "_ACTIVE: contextvars.ContextVar"; under engine/, analysis/, scripts/, tests/ and
    docs/session-reports/tools/, only run_manifest.py, ollama_client.py and
    tests/test_u21_no_overlap.py hold a call-ending name.
  THE ARCHITECT'S RUN (off the box, in a clone at 483969d with the model call stubbed; not
    a CC measurement; the build's fail-first tests re-measure it): pdf_quality_check's loop
    with every call raising TimeoutError made 20 calls for 10 papers and 200 for 100 and
    returned normally; the same with ConnectionError and with RunRefused; main() returned
    normally with 3 of 3 papers failed.
  INFERRED — BLOCKING (the build reads each at HEAD before any edit; false = STOP):
    (i) _extract_selected's proactive restart runs inside the run's active context; (ii)
    the manifest body carries no key-set version or hash that a new optional key must
    move; (iii) the read-out's reading of extractor.py:1267–1280, that a failed restart
    leaves the counter unreset; (iv) which exception classes reach pdf_quality_check's
    loop from ollama_chat for a read timeout and for a refused connection.
  INFERRED — expectation (reported if false): sudo -n fails at once where sudo would
    prompt; the fifteen entry points finding 8 names open no run by any path.

R819(a) — The 12ai-SCRI-REC final report is accepted as printed: item 0 483969d (Block 216;
  the ninety-seven files and the publication read-out's twenty-one with its list; r818.txt
  and the brief file equal to the architect's copies), ledger a84a03e. Finding 1 (sixteen
  tables, not fourteen) and finding 4 (two words short in a header's wording) are the
  architect's, each settled by the brief's own rule; findings 2 and 3 are readings. The
  count so far in 12ai: no stop, no pause; six architect's errors, one reaching a commit.
R819(b) — The unit and its row, the PI's words: "4 U41 Class 2". A new fix unit, U41, "The
  Ollama restart paths are opt-in", in P1, built before U30; P1's fix units 39, done 33. A
  new row, INT-ai12-1, Class 2, P1 with U41: both Ollama restart paths are armed by default
  on a host other lanes share, and with them off pdf_quality_check has no bound on a hung
  server. Class 2 because nothing stored is made wrong: a performed restart is followed by
  the environment re-check, and a paper whose quality check failed stays unmarked.
R819(c) — The direction, the PI's decision of 2026-11-15 as plan v106 §5.2 lists it: both
  restart paths are opt-in and off by default. RESTART_EVERY_N becomes 0. The recovery
  restart runs only for a run that asked for it; a call outside any run never restarts.
  With it, as §5.2 lists them: no retry is added for a read timeout; the restart command
  is run with sudo -n in both paths; elicit01 and capture01 default to 0 and a failed
  restart no longer ends them; the engine's keep_alive −1 stays and the analysis harnesses
  accept the host's default; no per-model unload replaces a restart.
R819(d) — How a run asks, the PI's words: "2 two flags, one key". scripts/run_pipeline.py
  gains --restart-every N (default 0), passed to run_extraction, and
  --restart-on-read-timeout (default off). When either is given, open_run records one
  optional body key, ollama_restarts, with two members, proactive_every (the integer) and
  on_read_timeout (true or false); when neither is given the key is absent, as
  selection_bound is. The recovery path reads the active run's recorded value; no active
  run, no key, or on_read_timeout false means not enabled. No Review Spec field is added.
  No other entry point gains a flag. EVIDENCE_ENGINE_NO_OLLAMA_RESTART stays and wins over
  the key.
R819(e) — A seventh call ending, the PI's word: "3yes". restart_not_enabled is added to
  RUN_CALL_ENDING_DETAILS after the six, whose names and order do not change; this amends
  the closed set of R761(c)(iv) and R762(c) from six to seven. The recovery path's order:
  the opt-out variable (restart_refused_opt_out, as now), then not enabled
  (restart_not_enabled), then the foreign lock, then the command. The TimeoutError raised
  where no restart was attempted does not say "+ restart".
R819(f) — A failed proactive restart resets the cadence counter, so the next attempt comes
  after restart_every more papers and not on every paper. It is logged as now.
R819(g) — pdf_quality_check's bound, the PI's words: "1 A", and "proceed" on the two
  additions put to him. Three papers in a row ending in a read timeout stop the loop,
  CONSECUTIVE_FAILURE_ABORT reused. A run fault, by the engine's own run_fault_classes(),
  stops it at the first paper and is not retried. A stopped loop ends the process with a
  non-zero status and a message naming the papers and the cause. Any other per-paper
  failure is counted and the loop continues, as now, and does not count toward the three;
  a success resets the count. The one retry of a failed call stays for everything but a
  run fault. PDFQualityCheck.timeout's description is corrected to the fact.
R819(h) — Withdrawn by the architect before the build: the check at run open that the host
  permits the restart. It needs a third sudo call site, which CLAUDE.md's invariant and
  tests/test_lock_census.py forbid, and a sudo command Claude Code may not run to measure
  it. A run that asks for a restart the host refuses records restart_command_failed, or
  logs the failed proactive restart, at the first attempt.
R819(i) — A second row, INT-ai12-2, Class 2, P4 by the precedent of INT-j12-8 (the package
  is read at item 0 against the plan's package list; if acquisition's rows are not P4's,
  STOP before the commit): pdf_quality_check's entry point exits 0 when papers failed and
  the loop was not stopped. Not U41's.
R819(j) — Text. In the build: CLAUDE.md's "Ops Invariants — Ollama service safety" lines
  that state the restart paths' behaviour, each to the fact; engine/core/review_spec.py's
  sentence "-1 equals the service value, so behaviour is unchanged."; and CLAUDE.md's
  no-live-write line, replaced by the text the brief carries, which the PI has before him
  with this ruling (R812(c)). The docs/architecture lines stay with HK-ARCH.
R819(k) — Readings, no row. Before the freeze, a read-only read-out of what the record shows
  restarts have done: calls ended restart_recovered, restarts in the journal, per-call wall
  time across a long run. It informs whether Ollama is restarted by hand before Run 7 and
  whether both paths are removed after the tag. Run 7 runs with no restart available once
  the sudoers rule is gone; a hung server then aborts the run after three papers, loudly
  and resumably.
R819(l) — This ruling states no standing rule: the id list, the glosses file and
  STANDING_RULES.md are untouched at the item 0 that transcribes it, asserted by content.
  Next: U41's build (the brief below); its engine commit's hash reported to the workbench;
  INT-ai12-1 closed at the item 0 after it lands; then U30's build with N46.
===== END r819.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ai-U41B] — Land U41 as R819(c)–(g) and (j) rule it: both Ollama
  restart paths off by default; a run asks through two run_pipeline flags recorded under
  one optional manifest key; restart_not_enabled as the seventh call ending; sudo -n; a
  failed proactive restart not retried on every paper; elicit01 and capture01 at 0 and
  not ended by a failed restart; pdf_quality_check bounded; the fail-first acceptance
  file; the coupled tests rewritten; the text lines; one engine commit with its gate,
  evidence in its own commit. Item 0: R819 as the next block; rows INT-ai12-1 and
  INT-ai12-2; the dated units line (U41 entered); this brief committed as pasted; no
  standing rule.
PURPOSE: Stop the engine from restarting a service other lanes share unless a run asks,
  record that a run asked, and keep one loop from hanging without end once the restart
  is off. Code task. No live write. No model call. No Ollama request. No restart. No
  sudo.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  This brief authorizes one transient systemd-run --user unit, gate-12ai_u41b, launched
  by docs/session-reports/tools/launch_gate.sh at step 2h, and nothing else of systemd.
  Touched: engine/agents/extractor.py (the constant, the restart command's argv, the
  cadence counter; nothing else); engine/utils/ollama_client.py (the recovery path's
  not-enabled branch, its argv, the TimeoutError text; nothing else);
  engine/core/run_manifest.py (the seventh name; open_run's optional parameter and body
  key; one accessor for the active run's value; nothing else); scripts/run_pipeline.py
  (the two flags and their pass-through; nothing else); analysis/eval/elicit01/runner.py
  and analysis/eval/run_capture01.py (the catch around the restart call; nothing else);
  engine/acquisition/pdf_quality_check.py (the loop's bound and the exit status of
  R819(g); nothing else); engine/core/review_spec.py (two description strings; nothing
  else); tests/test_u41_restart_opt_in.py (new); the coupled tests the census and the
  selected runs find; CLAUDE.md (R819(j)'s lines only); docs/inventory/ (regenerated if
  a recorded entry moves, R786(b)); docs/plan/ENGINE_REFACTOR_PLAN.md (Block 217; two
  rows; one dated line); 12ai_open/r819.txt; a new directory 12ai_U41B/;
  ~/scratch/12ai-u41b/; the claude-config ledger; git push on both repos; one standard
  gate.
  STOP / DO NOT TOUCH (beyond the global list): any other file under engine/, scripts/
  or analysis/ — analysis/paper1/pass2_full.py among them (its default is 0 and its
  call is caught already); in the touched files, restart_ollama's and
  _restart_ollama_and_retry's stand-down on the foreign lock, the opt-out variable's
  name and reading, the post-restart digest and environment checks,
  _recheck_env_after_restart, the retry branches for the other timeout members, the
  worker and its bound, record_call, the pin tuple and every hash input;
  pdf_quality_check's prompt, its message builder, its UPDATE and its paper query; the
  migrations; review_specs/; the codebook; the tools; docs/architecture/ (HK-ARCH's);
  tests/conftest.py's fence and tests/test_service_call_fence.py; the id list, the
  glosses file and STANDING_RULES.md (not one byte); review.db (mode=ro instruments
  only); ollama.service; the sudoers rule (the workbench's, after this lands); every
  scratch directory other than ~/scratch/12ai-u41b/. No test, helper or step runs sudo
  or systemctl: a test that would reach either unpatched is a STOP.

PATHS: a name of the form 12xx_<name>/ is a directory of docs/session-reports/session-12/;
  a tool is named by its full path.

FORM: 12af_U29B/brief_12af_U29B.md (sha256
  cb9ae249537d5d652e3d8df50b30494a01f60914b4c19bf01f47fae35c1b50a9) is this brief's form
  for a build: its steps 0–3, its gates, its report sections and its contradiction
  handling, read with this brief's values and with the gate launched as a unit as
  12ai_open/brief_12ai_OPEN.md's step 3e launched it. Read its Section 2 whole before
  step 0. Where a form and this brief disagree, this brief governs; where either
  disagrees with a tool or the record, the tool and the record govern and the
  difference is a finding (R732(a)). The design is named by its ruling clause,
  R819(c)–(g) and (j), throughout; the ruling is in Section 1 of this block.

TIMING: issued at 05:02 UTC on Friday 2026-11-20. Valid outside the day's window
  (R812(a)): today's opens at 09:00 UTC. No edit in the working tree inside a day's
  window. The gate must exit before the window opens: launch by 08:30 UTC or after
  10:35 UTC. If the clock reaches 08:30 before the engine commit is gated, hold with
  the commit unpushed and resume in this brief after 10:35 UTC (R789(a)); say so in the
  report. If the clock at step 0 is inside a window or within 15 minutes before one
  opens, STOP at once and write nothing.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ai-SCRI-REC final report, as R819 quotes it; 12ai-OPEN final report):
    HEAD 483969d524b081c83b21deba2fda9c095a09b158, parent
    bec3545d8740da769b764894f234b302d9fd0be6, clean, level; plan sha256
    d81e330644285b58dc2897560108d934b0f7b8d21828a4dee21a474ada1ab243, 3,406,820 B,
    20,340 lines, ends "…ends at R818.", last block 216; claude-config at
    a84a03e7ab9fbb07f581b0e14acec66851fc8d93 or an accepted descendant (R663(b)); the
    three rules files at the 12ai-OPEN report's hashes; gate reference
    12ai_open/gate_12ai_open/ (3,573 passed / 1 xfailed / 17 deselected on 3,591 /
    3,574; its collected_ids.txt and durations_12ai_open.txt); band 897–1095 s
    (R816(b)); pins 12ah_close/pin_tuples_close.json; recount 287 with 38 / 33;
    six-cell INT- rows 213.
  READ (12ah_OLRA/12ah_OLRA.md and 12ah_OLRA_add1.md at their committed bytes — Q1's two
    paths, Q2's census, Q3's cadence sources, Q4's callers, Q5's failed-command table,
    Q6's test census and fence, Q7's text, Q8's option D, findings 1–8 — not repeated;
    step 2a re-reads each site at HEAD). From them, the anchors: extractor.py:61, :744,
    :760, :775, :789–792, :1000, :1267–1285; ollama_client.py:44–49, :801–827,
    :912–924, :931–936, :944–948; run_manifest.py:84–96, :490, :634–656;
    run_pipeline.py:404, :549–557, :595–626; elicit01/runner.py:148, :191–193;
    run_capture01.py:184, :225–227; pdf_quality_check.py:42, :103–146, :254–256;
    review_spec.py:675–681; tests named in Q6; CLAUDE.md:156–180.
  READ (the architect's clone at 483969d): ollama_client.py:822, the text R819(e)
    changes; review_spec.py:734–735, the description R819(g) corrects; CLAUDE.md:252–253,
    the two lines step 2d replaces, quoted there; pdf_quality_check.py's main(), which
    returns after its report with no exit call; tests/test_selection_bound.py:161 and
    tests/test_u19_env_recheck.py:196, each asserting an optional body key absent when
    not given; tests/test_ft_adjudication_import.py:319, a comment over a pinned key
    set; the models for item 0: ffc6c38 (12af-U29A item 0: a block, a new row, the
    dated line, the brief file) and 6bfbe36 (12ae-U28B item 0: the dated units line
    that entered U40).
  INFERRED — BLOCKING (read at 2a before any edit; false = STOP):
    I1. The engine tree is unchanged since 977c499 (git diff --stat on engine tests
        scripts analysis review_specs, empty).
    I2. _extract_selected's proactive restart call runs with the run's active context
        set, so the run's recorded value is readable there and in ollama_chat's
        timeout branch (the lines that set and reset the active run, quoted).
    I3. The manifest body carries no key-set version, schema number or whole-body hash
        that an added optional key must move, and no reader refuses an unknown body
        key (every reader of the body's key set, quoted). If one exists, STOP.
    I4. A failed restart at extractor.py:1267–1280 leaves papers_since_restart at or
        above restart_every (the lines quoted, and shown by the parent run of case 8).
    I5. For a read timeout, ollama_chat raises the builtin TimeoutError to
        pdf_quality_check's loop; for a refused connection, a class inside
        extraction_events.run_fault_classes(). Each quoted from the code path; the
        classes the loop's bound tests are the ones read here, not the ones this
        line names.
    I6. run_fault_classes(), or the predicate is_run_fault beside it
        (extraction_events.py:429, :478), can be imported from engine/acquisition/ without
        a circular import and without loading a model or opening a database. If it
        cannot, STOP with the import chain; do not copy the tuple.
    I7. P4 is the package of acquisition's rows by the plan's own package list
        (R819(i)); read at step 1 before the row is composed.
  INFERRED — expectation (report a mismatch, continue):
    E1. On the parent, the acceptance file's cases 1, 2, 3, 5, 6, 7, 8, 9, 10a, 10b and
        11 are red and cases 4, 10c and 10d are green; N_red and N_green read from the
        run.
    E2. Coupled tests (c) are those Q6 marks coupled, in tests/test_ollama_client.py,
        tests/test_restart_recovery_guard.py, tests/test_restart_ollama_guard.py and
        tests/test_u21_no_overlap.py; the census and the selected runs decide, and a
        file not named here is a (c), not a stop.
    E3. The gate collects 3,591 + N and selects 3,574 + N, N the new file's ids plus
        any id a rewrite renames (each rename an only-ref / only-gate pair, read and
        matched).
    E4. docs/inventory/'s entry for run_pipeline.py gains the two flags, and elicit01's
        and capture01's entries stay "dynamic: RESTART_EVERY_N": an entry change,
        regenerated and kept (R786(b)); a stamp-only diff is reverted (R793(e)).
    E5. Pins, the audit prompt_hash, spec_hash for the live spec, and every request
        hash the suite pins do not move.
    E6. A default run's manifest body has the same key set as before this change.

CONTRACT (steps, in order; evidence under ~/scratch/12ai-u41b/):

  STEP 0 — Pre-check (read-only). The form's step 0 with this brief's values: last block
    216, no Block 217; I1; the three rules files; claude-config; fingerprint --compare
    IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; the check written for
    this brief, R794(i)). Models, each quoted before its counterpart is composed:
    ffc6c38 for a block with a new row and the dated line; 6bfbe36 for the dated units
    line that enters a unit; bec3545 for the block's metadata keys.
    1a. 12ai_open/r819.txt from Section 1, one write, hashed straight after (R738(b))
        against Section 1's hash. Mismatch = STOP (R739(b)).
    1b. block217.json in block216.json's key set: tag R819; transcribed = the commit's
        UTC date (R775(a)); item "item 0 of the 12ai-U41B brief"; made_by PI; made_date
        2026-11-20; session 12ai; report as R819's STATUS names it, the 12ai-SCRI-REC
        final report with its item-0 hash and the committed read-out, composed by CC in
        the form of the record's nearest block whose ruling rests on a report and a
        read-out (found in the log, quoted, named in the report); sentence model,
        followed "12ai-U41B brief"; prev_ends R818; "beside" composed by CC from
        ffc6c38's block's words for what this commit carries: the two rows, the dated
        units line and the brief file. Render to stdout, compare the element order
        with the model's, then --append.
    1c. Two rows, each in INT-af12-1's six-cell form (quoted first), placed where the
        model placed a new row. The cell text between the markers is the architect's;
        the ruling and source cells are composed by CC in the model's form (R819(b) and
        R819(i); sources 12ah_OLRA/12ah_OLRA.md §Q1–Q5 for the first, R819's "THE
        ARCHITECT'S RUN" and pdf_quality_check.py's main() for the second).
        ----- BEGIN row INT-ai12-1, finding cell -----
        Both Ollama restart paths are armed by default on a host other lanes share: `extractor.restart_ollama` every `RESTART_EVERY_N = 25` papers and `ollama_client._restart_ollama_and_retry` on the first read timeout of any call, each `sudo systemctl restart ollama`. With them off, `pdf_quality_check`'s loop has no bound on a hung server.
        ----- END row INT-ai12-1, finding cell -----
        Class 2; package "P1, with U41".
        ----- BEGIN row INT-ai12-2, finding cell -----
        `engine/acquisition/pdf_quality_check.py`'s entry point exits 0 when papers failed and the loop was not stopped: `main()` prints "Failed: N" and returns.
        ----- END row INT-ai12-2, finding cell -----
        Class 2; package P4 (I7).
    1d. The dated units line, in 6bfbe36's line's form (quoted first), tagged with the
        commit's UTC date and "(12ai-U41B item 0, R819)": U41 entered by R819(b) ("The
        Ollama restart paths are opt-in"; INT-ai12-1; before U30); P1's fix units 39,
        done 33 (U01–U29, U37–U40); rows 289 (the class counts computed from the plan
        after 1c, not from this line).
    1e. The brief file (R784(g)(v)): 12ai_U41B/brief_12ai_U41B.md from this whole pasted
        block, "CLAUDE CODE" through the STOP-12AI-U41B line, one trailing newline,
        written once; sha256, bytes and lines straight after the write, reported.
    1f. The item-0 check, as ffc6c38's, with: --check-plan byte-identical for Block
        217; the plan's numstat read, its "−" matched to zero; every earlier "ends at"
        line kept (173 before); the last line "…ends at R819." once; the plan equal to
        HEAD's plus the two rows, the dated line and Block 217, nothing else
        (computed); six-cell INT- rows 213 before and 215 after; r819.txt equal to
        1a's; the three rules files unchanged and unstaged (R777(b)); the staged set
        exactly the plan, r819.txt and the brief file. Quote the check.
    1g. <check> && <commit>, one line; the message from the staged set, docs scope.
        Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers (R801(f)); push; level.

  STEP 2 — The build (one engine commit).
    2a. Re-read at HEAD and quote, before any edit: each anchor the ledger names, whole
        functions for restart_ollama, _restart_ollama_and_retry, the timeout branch of
        ollama_chat, open_run's signature and body assembly, run_pipeline's parser and
        its open and extract calls, both harnesses' restart lines, and
        pdf_quality_check's _classify_page, loop and main(); I2–I6, each beside its
        lines; every line in CLAUDE.md that states what the restart paths do or when
        (found by text, listed with line numbers); review_spec.py:675–681 and
        :734–735. A difference from the read-out that changes an edit below, or I2,
        I3 or I6 false, = STOP.
    2b. Census (run before the edit; its script and output committed with the
        evidence): every test Q6's pattern finds, with what it pins and its class
        against this change; every test that asserts a manifest body's key set or
        calls open_run; every test that imports pdf_quality_check; every test that
        reads RESTART_EVERY_N or a --restart-every default.
    2c. Write tests/test_u41_restart_opt_in.py, fail-first, the conftest fence on, no
        sudo, systemctl, model, server or socket reached (subprocess.run patched at the
        restart sites' own seam, as Q6's tests patch it); each docstring states its
        case and its seam. The cases:
         1. Inside a run that did not ask: a read timeout runs no command and raises
            TimeoutError; the run_calls row is error with its detail beginning
            restart_not_enabled; the message does not contain "restart".
         2. Outside any run: a read timeout runs no command and raises TimeoutError
            carrying restart_not_enabled.
         3. Inside a run whose manifest records on_read_timeout true: the command runs
            once, its argv exactly sudo, -n, systemctl, restart, ollama.
         4. The same run with the opt-out variable set: no command;
            restart_refused_opt_out, as now.
         5. RUN_CALL_ENDING_DETAILS is the six in their present order, then
            restart_not_enabled.
         6. run_pipeline with neither flag: the manifest body has no ollama_restarts
            key and run_extraction receives restart_every 0. With --restart-every 3
            and --restart-on-read-timeout: the key is {proactive_every: 3,
            on_read_timeout: true} and run_extraction receives 3. With one flag only:
            the other member at its off value.
         7. extractor.RESTART_EVERY_N is 0, and elicit01's and capture01's parsers
            default --restart-every to 0.
         8. The proactive path with restart_every 2 over six papers, the command
            failing every time: three attempts, each argv carrying -n (the parent
            makes more; its count read and reported, I4).
         9. In elicit01 and in capture01, a restart_ollama that raises RuntimeError
            does not end the loop: the next unit is processed. At each harness's own
            seam with the model call faked; if a harness has no seam that runs
            without a model, say so, pin the catch by a test of the smallest
            function that holds it, and report which.
        10. pdf_quality_check, the model call stubbed, time.sleep patched: (a) every
            call a read timeout, ten papers: the loop stops after the third paper,
            at most six calls, and main() ends with a non-zero status and a message
            naming the three papers; (b) a refused connection, and separately a
            RunRefused, on the first paper: one call, the loop stops, non-zero
            status; (c) three papers whose page cannot be rendered, then readable
            ones: no stop, the readable ones checked; (d) timeout, success, timeout,
            timeout, success: no stop.
        11. A call made from a thread that does not carry the run's context, while a
            run that asked is active elsewhere: not enabled, no command.
        Run on the parent: EXPECTED E1; the red and green ids listed; output to
        b0_parent_acceptance.txt. A red case of E1 green on the parent = STOP.
    2d. Edit, R819(c)–(g) and (j) exactly, and nothing else:
        — extractor.py: RESTART_EVERY_N = 0 with its comment to the fact; -n in the
          restart command; the cadence counter reset whether the restart succeeded,
          was refused or failed, so a failure is retried after restart_every more
          papers (R819(f)). The U19 re-check stays after a performed restart only.
        — run_manifest.py: restart_not_enabled declared and appended as the seventh
          member (R819(e)); open_run takes one optional keyword for the declaration
          and writes it under ollama_restarts only when given, in selection_bound's
          manner (R819(d)); one accessor returns whether the active run recorded
          on_read_timeout true, reading the run's own manifest, false with no active
          run. The value may be held for the run's life; it is never taken from the
          environment or a module default.
        — ollama_client.py: in the recovery path, after the opt-out check and before
          the foreign-lock check, the not-enabled ending by that accessor (R819(e));
          -n in its command; the TimeoutError text for an ending in which no restart
          was attempted does not say "+ restart" and names the ending.
        — run_pipeline.py: --restart-every (int, default 0, refused below 0) and
          --restart-on-read-timeout (flag); the declaration passed to open_run when
          either is set, absent otherwise; restart_every passed to run_extraction.
        — elicit01/runner.py and run_capture01.py: the restart call's RuntimeError
          caught and logged, the loop continuing (R819(c)); nothing else.
        — pdf_quality_check.py, R819(g): a run fault, by run_fault_classes(), is not
          retried by _classify_page and stops the loop at once; a read timeout that
          survives the one retry counts one, a success resets the count, and at
          CONSECUTIVE_FAILURE_ABORT (imported, not copied) the loop stops; any other
          failure is counted in stats and neither counts nor resets; a stopped loop
          closes its connection and main() exits non-zero with a message naming the
          papers and the cause, after printing the report. A loop that was not
          stopped exits as it does today (INT-ai12-2 is not this unit's).
        — review_spec.py: the keep_alive description's last sentence to the fact (the
          host default is no longer −1; the value sent is unchanged), and
          PDFQualityCheck.timeout's description to the fact (it sets the watchdog
          that marks a call late; the request ends by the read timeout). Strings
          only: the defaults, bounds and field names do not change.
        — CLAUDE.md: the lines 2a listed, each to the fact, before and after quoted
          in the report; a statement that restarts are armed by default, or that
          the proactive cadence is 25, may not survive. And lines 252–253, which
          must read exactly as quoted between the first pair of markers, replaced
          by the text between the second pair; a mismatch = STOP for this edit
          only, reported, the rest of the build standing.
          ----- BEGIN CLAUDE.md:252–253 as it stands -----
          - **No live write to review.db between 07:00 and 10:35 UTC** (R86: 07:00 Ollama health
            check, 09:00 nightly suite, 10:30 dgx-snapshot-user timer +0–2 min, all read-only);
          ----- END -----
          ----- BEGIN CLAUDE.md:252–253 replacement -----
          - **No live write to review.db between 09:00 and 10:35 UTC Monday to Saturday, or
            between 07:00 and 10:35 UTC on Sunday, by UTC day** (R86 as amended by R812(a): the
            09:00 nightly suite and the 10:30 dgx-snapshot-user timer +0–2 min every day, and
            the full Ollama health sweep at 07:00 on Sunday; all read-only);
          ----- END -----
          (The markers and the ten-space indent before each quoted line are this
          brief's; the file's lines begin at "- **" and at two spaces.)
        — Tests: each (c) test rewritten under R725(b), its assertion kept and its
          arming made explicit (a run that asked, or the argument passed); the
          exact-set pin at tests/test_u21_no_overlap.py:279–282 becomes the seven,
          its docstring naming R819(e); argv pins gain -n. A test whose assertion
          cannot be kept: STOP with the test quoted.
        git diff --stat EXPECTED: the eight engine, script and analysis files named
        under Touched, the new test file, the (c) test files, CLAUDE.md, and
        docs/inventory/ under E4. The "−" lines of each of the eight quoted and
        matched to the edits above.
    2e. Pins: 12l_U04/pin_tuples.py with both arguments, byte-identical to
        12ah_close/pin_tuples_close.json; the audit prompt_hash unchanged; the live
        spec's spec_hash before and after, computed without opening a run (E5). A
        moved value = STOP.
    2f. E6: a default run's manifest key set, from a test that opens a run with no
        flag, equal to the parent's (the parent's read first and quoted).
    2g. Selected runs green: the new file; the four files of E2;
        tests/test_u19_env_recheck.py, tests/test_extractor.py,
        tests/test_lock_census.py, tests/test_selection_bound.py,
        tests/test_run_manifest.py, tests/test_ft_adjudication_import.py,
        tests/test_extraction_signatures.py, tests/test_effective_config.py,
        tests/test_request_capture.py and tests/test_qualgap01.py whole; every other
        file the 2b census lists. A red is classified before anything else: (c)
        coupled → rewritten under 2d's rule; (r) a regression — any test of the
        foreign-lock stand-down, the U19 re-check, the fence, the lock census's
        two-site pin, or a pin or hash — = STOP. Commit the engine change (fix
        scope; the message names U41, INT-ai12-1 and R819, the default, the two
        flags and the key, the seventh ending, -n, the counter, the two harnesses,
        the quality check's bound, the text lines, the rewritten tests and the
        inventory if regenerated), from the staged set. Do not push before the gate.
    2h. The standard gate, launched as the unit gate-12ai_u41b by
        docs/session-reports/tools/launch_gate.sh as 12ai-OPEN's step 3e launched
        its own: evidence_dir ~/scratch/12ai-u41b/gate_u41b/, name 12ai_u41b, ref_ids
        12ai_open/gate_12ai_open/collected_ids.txt, ref_durations
        12ai_open/gate_12ai_open/durations_12ai_open.txt, no selection; never a CC
        background task. EXPECTED E3; 1 xfailed; 17 deselected; wall inside
        897–1095 s, outside it reported with the concurrent jobs, not adapted to; no
        phase >10 s slower; no model load and no service restart in the gate's
        journal window (R696(b)); the unit's start line; the nineteen files. A red:
        class (c) per R746(c) and R772(b), fixed under 2d's rule and the gate re-run
        once; any other red = STOP, the engine commit standing.
    2i. Fingerprint --compare; push; level; --compare after (IDENTICAL). Ledger row;
        push; level. journalctl -u ollama over the whole of step 2: no restart, no
        model load (a reading taken with systemctl show and the journal; no sudo).

  STEP 3 — Evidence commit (R731(c)). 12ai_U41B/: b0_parent_acceptance.txt, the 2b
    census script and output, gate_u41b/ (the nineteen files) with the launcher's
    stdout and the hold snapshot beside it, pin_tuples_u41b.json, the 2f key-set
    read; the staged set exactly those, every sha256 equal to its scratch source;
    chained check && commit; fingerprint IDENTICAL before and after the push; level;
    ledger. Record hashes and parents.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1–I7 held, each beside its quoted lines.
  G2. Item 0 one commit that passed 1f in one chain: r819.txt at the architect's hash;
      Block 217 by the tool; two rows and one dated line; the rules files untouched;
      the brief file hashed; pushed, level, fingerprint IDENTICAL; ledger.
  G3. E1's red cases red on the parent and green in the gate, its green cases green on
      both; the diff confined to the files named and to the edits named; exactly two
      sites still issue the restart command, each with -n, and the lock census's
      two-site test green unedited in its assertion; no test reached sudo or
      systemctl; pins, the audit prompt_hash, the live spec_hash and a default run's
      manifest key set unchanged; the gate at E3's counts, no load and no restart in
      its window, nineteen files; pushed, level, fingerprint IDENTICAL; ledger.
  G4. The evidence commit as step 3 states.

OUT OF SCOPE: removing either restart path (a candidate after the tag, R819(k)); a check
  at run open that the host permits a restart (withdrawn, R819(h)); a flag on any entry
  point but run_pipeline; a Review Spec field; a retry for a read timeout; a per-model
  unload; the sudoers rule and every file of dgx-infra; INT-ai12-2's exit status; the
  other loops' handling of a timeout (screening, full-text, audit, vision parse);
  INT-l12-16; docs/architecture/; the stale lines finding 6 names outside the files
  touched; the read-out of what restarts have done (R819(k)); closing INT-ai12-1 (the
  next item 0); U30; the tools items; /wrap.

CONSTRAINTS (task-only): no model call, no Ollama request beyond the gate tool's own
  reads, no restart, no sudo, no live write, no new query on live; no edit in the
  working tree inside a day's window. A count arrived at by eye is INFERRED; a computed
  value is reported with its command. An item the read-out marks "CC's inference" or
  "CC's reading" is re-read at HEAD and quoted before an edit rests on it.

FINAL REPORT FORMAT: the form's sections (0)–(6) with this brief's contents: (0) the
  pre-check; (1) item 0 — block217.json with its models quoted, the two rows and the
  dated line as written, the package list line that places INT-ai12-2 (I7), r819.txt's
  hash, the brief file's sha256, bytes and lines, the check, hashes, numstat,
  fingerprint, ledger; (2) the build — I2–I6 each with its quoted lines; the census;
  the new tests as written; b0_parent_acceptance.txt verbatim; the eight files' diffs
  whole; each (c) test before and after; the CLAUDE.md and review_spec.py lines
  before and after; the inventory diff; the pin, spec_hash and key-set reads; the
  selected runs; the engine commit's full hash and parent, on a line of its own for
  the workbench; (3) the gate; (4) the evidence commit; (5) contradictions and
  findings, numbered, each with its quoted line; (6) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, a 1a mismatch, I7 false, or a check failure or
  crash at 1f = STOP before the item-0 commit. I2, I3 or I6 false, a difference from the
  read-out at 2a that changes an edit, a red case of E1 green on the parent, an (r)
  red, a (c) test whose assertion cannot be kept, a diff outside the files named, a
  third site that issues the restart command, a test that would reach sudo or
  systemctl, or a moved pin or hash = STOP before the engine commit. I4 or I5 false is
  reported with the lines and the edit follows the code as read, named in the report.
  A non-(c) red in the gate = STOP, the engine commit standing and reported. An
  expectation mismatch (E1–E6) is reported and the work continues. A spec premise that
  does not survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not close INT-ai12-1 and do not start U30. Await the next
  brief or ruling in this session. STOP-12AI-U41B
