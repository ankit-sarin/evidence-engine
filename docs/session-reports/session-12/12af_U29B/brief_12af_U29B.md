CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R800, PI ruling 2026-11-11 UTC; not on disk)
================================================================================

R800 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r800.txt: sha256 aaa70ee3a0d19b49538f214bd4d01f0ba9a1caf7e26590b2ee503f2cbf53d583,
            11412 bytes, 132 lines, one trailing newline.

===== BEGIN r800.txt =====
RULING [evidence-engine 12af-R800] — U29A accepted; U29's design: send records every call it
  makes — a raised call and a truncated or refused response as error with a detail, a complete
  response as completed, each returned response with a content digest; one accessor per
  transport; the SDK's own retries off; U40's guards and fallback removed; no schema change; no
  standing rule; the build opens as one brief
STATUS: PI ruling, 2026-11-11 UTC, session 12af, on architect recommendation, on the 12af-U29A
  final report (item 0 ffc6c387f3d5ebaaaff768f2836fba25d53b577f; read-out
  1e3b74d50c10aa8e126da84fad2c4c53d3c1c769) and the read-out it committed. Transcribe at item 0
  of the 12af-U29B brief below, by transcribe_ruling.py, in the block after R799's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12af-U29A final report, quoted): "ffc6c38 (parent 0814545)"; "1e3b74d (parent
    ffc6c38)"; ledgers "491d0c6", "389adfd"; "brief_12af_U29A.md:
    de2d2745afd602662208c1d4ffe92852fc9e7ae9e93e5b591249e3db9396831b, 28,278 B, 363 lines" (the
    architect's copy: equal, cmp byte for byte against the clone); "check_item0_u29a.py → RESULT
    PASS"; "Plan numstat +134/−5; the five "−" lines are the five rows, in file order."; "Six-cell
    INT- rows: 212 before, 213 after."; "The plan is now 870455ca…b66e, 3,112,451 B, 18,206
    lines."; "measured 287 (94 / 57 / 136), as E2 expected"; "E1 and E2 held".
  READ (12af_U29A/12af_U29A.md at 1e3b74d, sha256
    0ced7f60bad1a8b30b2892ec5373de5f0889be67ba05522be7a3e783bfb8a8a1, quoted): "openai 2.24.0 and
    anthropic 0.84.0"; "An exception from `_create` passes straight through, so no `run_calls` row
    is written"; the tokens, "`"completed", "refused_input_overflow",
    "refused_ceiling_unavailable", "refused_input_truncated", "refused_input_dropped", "error"`";
    "A refused completed call is recorded with outcome 'error' and a detail naming the mismatch;
    no outcome is added (022's CHECK)"; "The local path records an OUTPUT-length stop as
    `completed`"; "`outcome_detail = f"{type(exc).__name__}: {exc}"`"; "`response_digest` is
    `sha256_canonical({"content": content or "", "thinking": thinking or ""})`"; "for OpenAI,
    `fields that differ: ['created', 'id', 'usage']`"; "It then calls `conn.commit()` (`:1058`).";
    "`finish_reason: Literal["stop", "length", "tool_calls", "content_filter", "function_call"]`";
    "`StopReason: TypeAlias = Literal["end_turn", "max_tokens", "stop_sequence", "tool_use",
    "pause_turn", "refusal"]`"; "Nothing in the engine reads `.refusal`"; "Both SDKs default to
    `max_retries = 2`"; "both clients are constructed with the key only"; "a recorder fault
    replaces the provider exception. It propagates, with the provider error as `__context__`";
    "The three getattr guards: no test needs them."; "Their assertions are unchanged."; "So today
    a claim stored after a raise-then-succeed sequence carries, in `context_chain`, a hash with no
    `run_calls` row."
  READ (the architect's clone at 1e3b74d): the two full hashes in STATUS; the plan's sha256
    870455cad9ba77e03100275b10b887184c627ed5ea2e1e6247bf40d51eb7b66e, 18,206 lines.
  INFERRED (each verified at the build's step 2a before any edit; false = STOP): what a provider
    really returns on truncation or refusal was not established (read-out §9) — the accessor is
    built on the pinned SDK types' declared fields; the two transports' own loops retry a
    connection or status error that the SDK no longer retries (the handlers, quoted).

R800(a) — The 12af-U29A final report and its read-out are accepted as committed: item 0 ffc6c38
  (Block 197; B9 and C28 CLOSED; the notes on A15, A16 and INT-i12-73; INT-af12-1 in its own
  section-L table, the record's convention — the brief's count of inserted lines was the
  architect's and the convention governs; the dated line at the measured 287 (94 / 57 / 136);
  the brief file byte-equal to the architect's copy), the read-out 1e3b74d with its census and
  probe, ledgers 491d0c6 and 389adfd. No stop. recount_12ae_close.py prints expected_holds False
  on this plan because it expects 286: a reading for the closeout's recount script, which states
  its own expectation.
R800(b) — Where the record is written. In send, and nowhere else: the provider call sits in a
  try; on an Exception, inside a run, one run_calls row is written with outcome error and the
  detail in the local recorder's form, the exception's class name, a colon and its message; then
  the exception is re-raised unchanged. A recorder fault there is not caught: it replaces the
  provider's exception, which stays as its context, and closes the run through U27's handlers.
  Outside a run nothing is recorded, as today. KeyboardInterrupt is not this unit's (U30). No
  call of run_manifest.record_call is added anywhere but send, and send is never reached inside
  store_result's transaction, because record_call commits the connection it is given.
R800(c) — The outcome of a returned response, with the six tokens as they stand: complete →
  completed; truncated → error with the detail "truncated: <field>=<value>"; refused → error
  with the detail "refused: <field>=<value>", <field> the provider's own name for its finish
  reason. Truncated is finish_reason "length" (OpenAI) or stop_reason "max_tokens" (Anthropic).
  Refused is finish_reason "content_filter" or a non-empty message.refusal (OpenAI; when the
  refusal text is the only sign the detail ends "; refusal text present"), or stop_reason
  "refusal" (Anthropic). The refusal text is not stored. This is R745(d)'s form: no token is
  added and no schema changes. The row records what the provider returned; whether a truncated
  response that a repair completes may be stored is U31's question, not answered here. A token
  of its own for a truncated or refused output, on both paths, is 023's to decide with R745(d);
  the local path's output-length stop recorded completed (F2) goes with it.
R800(d) — The digest. Every returned response's row carries rm.response_digest of the response's
  text and thinking — the local path's function — whatever its outcome. OpenAI: the message's
  content and its reasoning content when present. Anthropic: the joined text blocks and the
  joined thinking blocks. A digest of the whole response object is not taken (it varies with id,
  created and usage), and the provider's response id is not recorded.
R800(e) — The accessor. Each transport supplies one method that reads a response and returns its
  text, its thinking and its finish classified as complete, truncated or refused with the
  field and value for the detail; send calls it once, after the call returns and before the row
  is written. base.py holds no provider branch. An accessor that cannot read what it is given
  raises; send records that as error with the class and message and re-raises — a returned
  response the engine cannot read is not recorded completed. It is not written to tolerate a
  test's stand-in: a fake provider returns a response in the SDK's shape. extract_paper's own
  reading of the response is not edited.
R800(f) — N47. Both SDK clients are constructed with max_retries=0, so one recorded call is one
  HTTP send; the transports' own loops do the retrying and every attempt has its row. The SDK's
  default timeout is not changed. That a live provider honours this on every path is not
  established (cloud arms are off, R71); the run's record is what this buys, and INT-ae12-2's
  loops are unchanged.
R800(g) — U40's leftovers (R799(f)). The three getattr guards on the kept reference and chain go:
  no test takes their default. The stage-name fallback and its getattr go, and the three storing
  tests that reach it, with U28's fixture and helper, move to a provider instance in the form of
  U40's acceptance file, their assertions unchanged. base.py:495's getattr on model_string
  predates U40 and is not touched; it joins P7.
R800(h) — Tests. A new file, tests/test_u29_cloud_call_record.py, fail-first on the parent, both
  transports parametrised, on U40's seam, no SDK client constructed: (1)
  test_a_cloud_send_that_raises_still_records_its_call, the entry's test — a raising provider
  call leaves one error row whose request hash is the chain's last hash and whose detail names
  the class, and the same exception reaches the caller; (2) a truncated response and a refused
  one, each provider's forms, the refusal-text-only form included, are error rows with the
  details of R800(c), and carry a digest; (3) a complete response is completed with
  rm.response_digest of its text and thinking, equal for two responses that differ only in id
  and usage; (4) after a raise then a success and a store, every hash in the stored claims'
  chain has a run_calls row; (5) each client is constructed with max_retries=0; (6) a recorder
  fault while a provider exception is in flight surfaces as the run fault with the provider's
  exception as its context; (7) outside a run no row is written and the exception passes (green
  on the parent: a preservation case); (8) an unreadable response is an error row and raises.
  Coupled tests, class (c), rewritten in the same commit: tests/test_u27_cloud_run_faults.py's
  provider-route count of zero calls, which pinned the defect C24 names and is superseded by
  this ruling — it becomes one error row per raised send — and its constructor keywords;
  tests/test_run_manifest.py's C16 fake and tests/test_request_capture.py's two cloud fakes,
  given SDK-shaped responses, their assertions kept; the R800(g) moves; any other the census or
  the selected runs find.
R800(i) — Docs, in the engine commit: docs/architecture/pipeline.md's :251 line (R799(f)) and its
  description of what a cloud call records, and each CLAUDE.md line that states what send
  records or how the SDK clients are built, brought to the fact. Lines found by their text and
  quoted before and after.
R800(j) — Rows, at 12af-U29B's item 0: a dated note on C24 — U29's design is ruled (R800(b)–(f));
  the row closes at the item 0 after it lands. A dated note on INT-ae12-2 — extract_with_
  completeness re-issues an identical request after a refusal, three paid sends (12af_U29A.md §7
  item 10); with the SDK's retries off its loops are the only retrying; class and package
  unchanged. Readings, no row: finding 1 (the section-L table, (a)); finding 2 ((a)); findings 3
  and 4 (answered by (c) and by (h)(4)); finding 5 (the accessor reads the refusal text's
  presence, (c)); finding 6 ((b)); finding 7 ((g)); finding 8 (a census line corrected before
  commit); F7 (two functions named record_call — the census resolves by import). The architect's
  errors this session so far: four (R797(e)'s two, R798(h), and the inserted-line count of (a));
  none stopped a brief.
R800(k) — The build opens as one brief, 12af-U29B, in R785(h)'s form: the read-out quoted send,
  the recorder, the schema, the SDK sources and every coupled test at ffc6c38 and the engine tree
  is unchanged since. No model call, no provider call, no network request, no live write; the
  standard gate by the wrapper against 876–1069 s with R797(d)'s hold. This ruling states no
  standing rule: the id list, the glosses file and STANDING_RULES.md are untouched at item 0,
  asserted by content.
===== END r800.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12af-U29B] — Land U29 as R800(b)–(i) rule it: send records every call
  it makes — error with a detail for a raised call and for a truncated or refused response,
  completed for a complete one, a content digest on every returned response; one accessor
  per transport; max_retries=0 at both client constructions; U40's guards and fallback
  removed; the fail-first acceptance file; the coupled tests rewritten; the docs lines; one
  engine commit with its gate, evidence in its own commit. Item 0: R800 as the next block;
  the C24 and INT-ae12-2 notes; this brief committed as pasted; no standing rule.
PURPOSE: Make the run's record of a cloud call complete and honest: no provider call inside
  a run without its row, no truncated or refused response recorded as a good one, and one
  recorded call equal to one HTTP send. Behind R71 on live. Code task. No live write. No
  model call. No provider call. No network request. No API key read.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  Touched: engine/cloud/base.py (send; the guards and the fallback of R800(g); nothing
  else); engine/cloud/openai_extractor.py and engine/cloud/anthropic_extractor.py (the
  accessor of R800(e); the client construction line of R800(f); nothing else);
  tests/test_u29_cloud_call_record.py (new); the coupled tests R800(h) names and any the
  census and the selected runs find; docs/architecture/pipeline.md and CLAUDE.md
  (R800(i)'s lines only); docs/inventory/ (regenerated if a recorded entry moves,
  R786(b)); docs/plan/ENGINE_REFACTOR_PLAN.md (Block 198; the two notes);
  docs/session-reports/session-12/12af_open/r800.txt; a new directory
  docs/session-reports/session-12/12af_U29B/ (brief_12af_U29B.md at item 0;
  b0_parent_acceptance.txt, the census, gate_u29b/ and the pin recompute at step 3);
  ~/scratch/12af-u29b/; the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): any other file under engine/ —
  run_manifest.py and its record_call, the migrations and the outcome CHECK, the event
  writer, ollama_client.py, the monitor; in the three touched files, extract_paper's
  reading of the response, the retry loops, extract_with_completeness, store_result's
  checks, INSERTs and writer call, request_payload, and base.py:495's getattr on
  model_string; scripts/ and analysis/; review_specs/ (R71); the codebook; the tools; the
  installed SDK packages; the id list, the glosses file and STANDING_RULES.md (not one
  byte); review.db (mode=ro instruments only; no new query on live); any .env or API key
  (a test that would construct a real SDK client or open a socket is a STOP); the working
  tree inside 07:00–10:35 UTC (R86); any scratch directory other than ~/scratch/12af-u29b/.

TIMING: the gate must exit before 07:00 UTC: launch by 06:30 UTC or after 10:35 UTC. If the
  clock reaches 06:30 before the engine commit is gated, hold with the commit unpushed and
  resume in this brief after 10:35 UTC (R789(a)); say so in the report.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12af-U29A report; R800): HEAD 1e3b74d50c10aa8e126da84fad2c4c53d3c1c769, parent
    ffc6c387f3d5ebaaaff768f2836fba25d53b577f, clean, level; plan sha256
    870455cad9ba77e03100275b10b887184c627ed5ea2e1e6247bf40d51eb7b66e, 18,206 lines, ends
    "…ends at R799.", last block 197; claude-config at 389adfd or an accepted descendant
    (R663(b)); the three rules files as 12af-OPEN's 0c read them; gate reference
    12af_U40B/gate_u40b/ (3,554 passed / 1 xfailed / 17 deselected on 3,572 / 3,555; its
    collected_ids.txt and durations_12af_u40b.txt); band 876–1069 s (R796(b)); pins
    12af_U40B/pin_tuples_u40b.json.
  READ (12af_U29A/12af_U29A.md at 1e3b74d — §2's send, record_call, schema and local
    recorder; §3's finish-reason sites, SDK literals, digest, N47, exception classes and
    guard table; §4's coupled tests; §5's probe; §7's options — not repeated; step 2a
    re-reads each site at HEAD). From it, the anchors: base.py send :141–162, the guards
    :148, :431, :432, the fallback :434 and :444; the client constructions
    openai_extractor.py:37 and anthropic_extractor.py:38; the finish-reason reads at :59
    in each; run_manifest.py record_call :1032–1059 and response_digest :1028–1029;
    ollama_client.py:725–727, the detail's form; tests/test_u27_cloud_run_faults.py:214
    and :219; tests/test_run_manifest.py:385; docs/architecture/pipeline.md:251.
  INFERRED — BLOCKING (read at 2a before any edit; false = STOP):
    I1. The engine tree is unchanged since 53eabfd.
    I2. Each pinned SDK's client constructor accepts max_retries=0 and its request path
        makes exactly one attempt with it (the constructor signature and the retry
        decision, quoted from the installed source with file and line).
    I3. The two transports' loops, as they stand, retry a connection, timeout or status
        error raised by a client that no longer retries (each handler quoted with the
        class it catches); a class that would now end a paper where the SDK used to retry
        is named in the report and is a finding, not a stop.
    I4. No send is reached while store_result's transaction is open (the call order in
        both transports' run and in extract_with_completeness, quoted).
    I5. An SDK response in each provider's declared shape can be built in a test from the
        installed types without a client or a network (the probe's fakes, quoted).
  INFERRED — expectation (report a mismatch, continue):
    E1. On the parent, R800(h)'s cases (1)–(6) and (8) are red and (7) is green; N_red and
        N_green read from the run.
    E2. Coupled tests (c) as R800(h) names them; the census and the selected runs decide,
        and a file not named there is a (c), not a stop.
    E3. The gate collects 3,572 + N and selects 3,555 + N, N the new file's ids plus any
        id a rewrite renames (each rename an only-ref / only-gate pair, read and matched).
    E4. The inventory's recorded entries in base.py move (send grows above them): an entry
        change, regenerated and kept (R786(b)); a stamp-only diff is reverted (R793(e)).
    E5. Pins and the audit prompt_hash do not move, and each transport's request payload is
        byte-identical (tests/test_request_capture.py's assertions unchanged).

CONTRACT (steps, in order; evidence under ~/scratch/12af-u29b/):

  STEP 0 — Pre-check (read-only). date -u. HEAD and parent proved by git cat-file -e
    <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. Plan: sha256, lines, last
    line, last block 197, no Block 198. Rules files: the three sha256s. claude-config:
    389adfd or an accepted descendant, level; a pending foreign row carried and named
    (R654(d)). I1. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12af-U40B's item 0,
    27fda1c — a block, two row notes and the brief file; ~/scratch/12af-u40b/
    check_item0_u40b.py and block197.json quoted before composing; the check written for
    this brief, R794(i)).
    1a. docs/session-reports/session-12/12af_open/r800.txt from Section 1, one write,
        hashed straight after (R738(b)): EXPECTED aaa70ee3a0d19b49538f214bd4d01f0ba9a1caf7e26590b2ee503f2cbf53d583,
        11412, 132. Mismatch = STOP (R739(b)).
    1b. block198.json in block197.json's key set: tag R800; transcribed = the commit's UTC
        date (R775(a)); item "item 0 of the 12af-U29B brief"; beside = "the C24 and
        INT-ae12-2 notes and the brief file it orders" (composed; quote it); made_by PI;
        made_date 2026-11-11; session 12af; report = the 12af-U29A final report with its
        item-0 and read-out hashes, in Block 196's form (quote that header first);
        sentence model, followed "12af-U29B brief"; prev_ends R799. Render to stdout,
        compare the element order with Block 196's, then --append.
    1c. Notes (R800(j)), each appended at the end of its row's status cell in that row's
        note form, quoted before composing, tagged "*Note <the commit's UTC date>
        (12af-U29B item 0, R800(j)):*" — on C24: U29's design is ruled, the facts of
        R800(b)–(f) in one sentence each; no schema change; the row closes at the item 0
        after U29 lands. On INT-ae12-2: the two facts R800(j) gives; class and package
        unchanged. Two rows replaced; the "−" count read and matched to two.
    1d. The brief file (R784(g)(v)): 12af_U29B/brief_12af_U29B.md from this whole pasted
        block, "CLAUDE CODE" through the STOP-12AF-U29B line, one trailing newline, written
        once; sha256, bytes and lines straight after the write, reported.
    1e. The item-0 check: --check-plan byte-identical; the plan's numstat read ("−" matched
        to the two rows); every earlier "ends at" line kept; the last line "…ends at R800."
        once; the plan equals HEAD's plus the two rows replaced and Block 198 appended,
        nothing else (computed); each row's last note begins with its 1c tag; r800.txt
        equal to 1a; the three rules files unchanged and unstaged; the brief file hashed;
        block198.json's transcribed value equal to the commit-time UTC date; the staged set
        exactly the plan, r800.txt and the brief file. Evidence by a redirect inside the
        check; quote the check.
    1f. <check> && <commit>, one line; the message from the staged set (R778(d)), docs
        scope. Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers; push; level. Record hashes and parents.

  STEP 2 — The build (one engine commit).
    2a. Re-read at HEAD and quote, before any edit: send whole; record_call and
        response_digest; the two client constructions and each SDK's constructor and
        retry decision (I2); both transports' loops and handlers (I3); the call order to
        store_result (I4); each transport's extract_paper from send to its finish-reason
        and text reads (what the accessor must agree with); the SDK response types the
        accessor reads and the probe's fakes (I5); the five guard and fallback lines;
        every coupled test R800(h) names, whole; pipeline.md:251 and every line in
        pipeline.md and CLAUDE.md that states what send records or how the clients are
        built (found by text, listed). Any difference from the read-out, or I2, I4 or I5
        false, = STOP.
    2b. Census (census_u29a.py's form, run before the edit): every test that reaches send
        — directly, through extract_paper, or through a transport's run — with its fake's
        return shape and what it asserts about run_calls; every test that constructs or
        patches an SDK client and what keywords it asserts; every test that stores through
        a provider-less instance (R800(g)). Output committed with the evidence.
    2c. Write tests/test_u29_cloud_call_record.py, fail-first, R800(h)'s eight cases as it
        names them, both transports parametrised; the conftest fence on; each docstring
        states its case and its seam; the provider call faked at the instance's own seam;
        the SDK client classes replaced by recorders for case (5) so that no client is
        constructed and no socket opened (asserted). Run on the parent: EXPECTED E1; the
        red and green ids listed; output to b0_parent_acceptance.txt. A red case of E1
        green on the parent = STOP.
    2d. Edit, R800(b)–(g) exactly, and nothing else:
        — base.py send: the provider call in a try; on Exception, inside a run, the error
          row in R800(b)'s form, then a bare raise; after a return, the accessor called
          once, the outcome and detail of R800(c), the digest of R800(d), one row; an
          accessor failure recorded and raised as R800(e) states. One record_call per
          send on every path inside a run; none outside a run.
        — The two transports: the accessor of R800(e), reading what R800(c) and (d) name
          from the pinned types' declared fields; max_retries=0 added to the client
          construction (R800(f)), no other keyword changed.
        — base.py, R800(g): the three getattr guards replaced by plain attribute reads;
          the stage-name getattr and fallback replaced by the instance's stage; the three
          storing tests, U28's fixture and its helper moved to a provider instance.
        — Docs, R800(i): the lines 2a listed, each to the fact; a line not found as
          quoted = STOP for the docs step.
        — Tests, R800(h): each (c) test rewritten; where R800(h) says an assertion is
          superseded, the new assertion is the fact R800(h) states, and the docstring
          names R800; every other (c) test keeps its assertion. A test whose assertion
          cannot be kept and is not named as superseded: STOP with the test quoted.
        git diff --stat EXPECTED: the three engine files, the new test file, the (c) test
        files, pipeline.md, CLAUDE.md, and docs/inventory/ under E4. The "−" lines of the
        three engine files quoted and matched to the edits above.
    2e. Pins: 12l_U04/pin_tuples.py with both arguments, byte-identical to
        12af_U40B/pin_tuples_u40b.json; the audit prompt_hash unchanged. A moved value =
        STOP.
    2f. Selected runs green: the new file; tests/test_cloud_extraction.py,
        tests/test_u27_cloud_run_faults.py, tests/test_u28_cloud_gate_and_boundary.py,
        tests/test_u40_cloud_claim_events.py, tests/test_run_manifest.py,
        tests/test_request_capture.py, tests/test_completeness_guard.py,
        tests/test_parsed_text_resolver.py and tests/test_corpus_authority.py whole; every
        other file the 2b census lists. A red is classified before anything else: (c)
        coupled → rewritten under 2d's rule; (r) a regression — any test of
        run_manifest's recorder, of the local path, or U40's acceptance file — = STOP.
        Commit the engine change (fix scope; the message names C24 and U29, the error
        row, the outcomes, the digest, the accessor, max_retries=0, the guards removed,
        the docs lines, the rewritten tests and the inventory if regenerated), from the
        staged set. Do not push before the gate.
    2g. The standard gate by launch_gate.sh: evidence_dir ~/scratch/12af-u29b/gate_u29b/,
        a session name, ref_ids 12af_U40B/gate_u40b/collected_ids.txt, ref_durations
        12af_U40B/gate_u40b/durations_12af_u40b.txt, no selection. Before it: the host
        snapshot and the Ollama reads ≥5 s ahead (R660(b)); the hold of R797(d) — when the
        snapshot names another lane's full test suite or an encode, the launch waits for
        it, bounded at sixty minutes and by the clock above; a launch at the bound is
        reported with the job named. EXPECTED E3; 1 xfailed; 17 deselected; wall inside
        876–1069 s, outside it reported with the concurrent jobs, not adapted to; no phase
        >10 s slower; no model load in the window (R696(b)); the nineteen files. A red:
        class (c) per R746(c) and R772(b), fixed under 2d's rule and the gate re-run once;
        any other red = STOP, the engine commit standing.
    2h. Fingerprint --compare; push; level; --compare after (IDENTICAL). Ledger row; push;
        level.

  STEP 3 — Evidence commit (R731(c)). 12af_U29B/: b0_parent_acceptance.txt, the 2b census
    script and output, gate_u29b/ (the nineteen files), pin_tuples_u29b.json; the staged
    set exactly those, every sha256 equal to its scratch source; chained check && commit;
    fingerprint IDENTICAL before and after the push; level; ledger. Record hashes and
    parents.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1–I5 held, each beside its quoted lines (I3's finding,
      if any, named).
  G2. Item 0 one commit that passed 1e in one chain: r800.txt at the architect's hash;
      Block 198 by the tool; the two notes; the rules files untouched by content; the brief
      file hashed; pushed, level, fingerprint IDENTICAL; ledger.
  G3. E1's red cases red on the parent and green in the gate, its green case green on both;
      the engine diff confined to the three files and to the edits named; record_call
      called from send only; run_manifest.py, the migrations and U40's acceptance file
      untouched and green; pins, the audit prompt_hash and both request payloads unchanged;
      no SDK client constructed and no socket opened in any test; the gate at E3's counts,
      no load, nineteen files; pushed, level, fingerprint IDENTICAL; ledger.
  G4. The evidence commit as step 3 states.

OUT OF SCOPE: a new outcome token and any schema change (023's, R800(c)); the local path's
  output-length stop (F2, with 023); whether a truncated response may be stored after a
  repair (U31's); INT-ae12-2's loops and the refusal re-issue; U30's interrupts; recording
  the provider's response id; the SDK timeout; base.py:495's getattr; closing C24 (the next
  item 0); R71; the tools items; the 2026-11-11 job reads; /wrap.

CONSTRAINTS (task-only): no model call, no provider call, no network request, no API key
  read, no restart, no live write, no new query on live; no edit in the working tree
  between 07:00 and 10:35 UTC. A count arrived at by eye is INFERRED; a computed value is
  reported with its command. Where this brief describes a tool, a record or a line of
  code, the record and the code govern and a difference is a finding (R732(a)). An SDK's
  behaviour is stated from its installed source with the file and line. The design is
  named by its ruling clause, R800(b)–(g), throughout.

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — block198.json, the render
  comparison, the two rows' last notes, r800.txt's hash, the brief file's sha256, bytes and
  lines, the check verbatim, chain, hashes, numstat, fingerprint, ledger; (2) the build —
  I2–I5 each with its quoted lines; the census; the new tests as written;
  b0_parent_acceptance.txt verbatim; the three engine files' diffs whole; each (c) test
  with its assertion before and after, the superseded one marked; the docs lines before
  and after; the inventory diff; the pin recompute; the selected runs; the engine commit's
  hash and parent; (3) the gate — launch line, any hold, wall against the band, the summary
  line verbatim, collected and selected, the id comparison, durations, journal window,
  concurrent jobs, the nineteen files; (4) the evidence commit; (5) contradictions and
  findings, numbered, each with its quoted line; (6) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, a 1a mismatch, or a check failure or crash at 1e
  = STOP before the item-0 commit. I2, I4 or I5 false, a difference from the read-out at
  2a, a red case of E1 green on the parent, an (r) red, a (c) test whose assertion cannot
  be kept and is not named as superseded, a diff in an engine file outside the three, a
  test that would construct a real SDK client or open a socket, or a moved pin = STOP
  before the engine commit. A non-(c) red in the gate = STOP, the engine commit standing
  and reported. An expectation mismatch (E1–E5) and I3's finding are reported and the work
  continues. A spec premise that does not survive contact with disk is a finding, not an
  obstacle.

HANDBACK: Stop and report. Do not close C24 and do not start U30. Await the next brief or
  ruling in this session. STOP-12AF-U29B
