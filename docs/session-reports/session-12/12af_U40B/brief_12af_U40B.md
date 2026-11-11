CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R798, PI ruling 2026-11-11 UTC; not on disk)
================================================================================

R798 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r798.txt: sha256 e293ebe48b2f376c6db4e2dd0cddad30d543fe87bfb2587f2dc5c98f2f5f7bbe,
            13588 bytes, 153 lines, one trailing newline.

===== BEGIN r798.txt =====
RULING [evidence-engine 12af-R798] — U40A accepted; U40's design: the cloud store writes the arm's
  claim events and no paper event (A1-β); A1-α refused; the writer gains one keyword; the text
  reference and the request-hash chain are kept on the instance by paper; a failed paper still
  writes nothing; four findings wait for rows; no standing rule; the build opens as one brief
STATUS: PI ruling, 2026-11-11 UTC, session 12af, on architect recommendation, on the 12af-U40A
  final report (item 0 9b9f3d6f60f8c98dba7e4f09fcbe93b627dba156; read-out
  aa97c6af278d91ec0bba5e1a1ff7330397bf842f) and the read-out it committed. Transcribe at item 0
  of the 12af-U40B brief below, by transcribe_ruling.py, in the block after R797's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12af-U40A final report, quoted): "9b9f3d6f60f8c98dba7e4f09fcbe93b627dba156, parent
    3310738, at 00:53:20Z"; "plan: 79f1c72038d34b8bfed8e2f7946c3c424eaafa0e4ba27412fbafb9b805e3e347
    3066737 bytes 17913 lines"; "RESULT: PASS"; "brief_12af_U40A.md:
    d71ce43e7d7b1a7078010c745191d2b0a9d37c5c5004acae02affdc0c1fb5045, 26,276 B, 339 lines" (the
    architect's copy: equal, cmp byte for byte against the clone); "f0db725, parent 96f776c, row
    2026-11-11T00:53:20Z"; "aa97c6af278d91ec0bba5e1a1ff7330397bf842f, parent 9b9f3d6, at
    01:10:00Z"; "Ledger 9e3fd22, parent f0db725, row 01:10:00Z"; "E2 held".
  READ (12af_U40A/12af_U40A.md at aa97c6a, sha256
    8d835294ca15cb36746ba9b4546fd83a3556c76092ca341a6dac1c292e6410ab, quoted): "It always writes
    one paper-level processing event as well as the claims. `paper_events` has no arm column, so
    that event moves the paper's single processing axis, which the local pipeline reads."; "20
    field events (['asserted']); paper event extracted -> extracted from None
    payload.arm=openai_o4_mini_2025_04_16_high"; "papers the local arm holds no claim on: [1, …
    12, 15]"; "paper 13 processing=extracted … paper 14 processing=extraction_failed
    reason=model_call_failed"; "the writer's savepoint nests in `store_result`'s transaction. Its
    commit then commits both."; "Its one production caller passes the local arm"; "No run-level
    gate or stage-complete condition reads a registered cloud arm's unclaimed cells as a failure
    of the local run."; "No grid reader filters cloud arms out."; "No engine consumer reads both
    stores for one arm's values."; "That is a per-arm branch in `effective_state` (against R12) or
    a schema change to `paper_events` (an arm column, a migration)."; "Or the existing writer
    gains a keyword that omits the paper event."; "RAISED DistributionCollapseError: Distribution
    collapse detected in 1 field(s): study_type".
  READ (the architect's clone at aa97c6a, engine tree as at 24ce26b): engine/core/
    extraction_events.py:701 "write_paper_event(conn, **plan.paper_event, commit=False)"; :468–474,
    the run-fault tuple, which names "EventRefused"; engine/core/events.py:100 "class
    RunLinkRefused(EventRefused):", :123 "class ClaimWithoutInputIdentity(EventRefused):", :138
    "class ClaimWithoutPresentedContext(EventRefused):"; extraction_events.py:104 "class
    ReuseKeyAlreadyClaimed(EventRefused):"; engine/core/parsed_text.py:181–182 "def
    load_parsed_text(conn: sqlite3.Connection, paper_id: int) -> str:" / "return
    read_parsed_text(resolve_parsed_text(conn, paper_id))"; extraction_events.py:287–294,
    legacy_record's keywords, among them "expected: tuple[str, ...], spans" and "stage_name: str =
    "extract_pass2""; engine/agents/extractor.py:718 "sentinels=frozenset(cb.absence_sentinels),
    review_dir=Path(db.db_path).parent)"; engine/cloud/openai_extractor.py's store handler "except
    Exception as exc:" / "if is_run_fault(exc):" / "raise".
  INFERRED (each verified at the build's step 2a before any edit; false = STOP): the β column of
    the read-out's consumer table is CODE ORDER — no consumer was driven with claims and no
    paper event, because no such writer existed; legacy_record takes the span dicts store_result
    holds; the last send for a paper is the accepted attempt's.

R798(a) — The 12af-U40A final report and its read-out are accepted as committed: item 0 9b9f3d6
  (Block 195; the brief file byte-equal to the architect's copy; the rules files untouched),
  the read-out aa97c6a with its census and probe, ledgers f0db725 and 9e3fd22. No stop. The trace
  R794(b) asked for is complete for the writer and for every consumer.
R798(b) — A1-α is refused. Calling the writer as it stands from store_result writes a paper event
  for a cloud store, and the paper's one processing axis is the local pipeline's: the probe shows
  vacuous local audits on papers the local arm never extracted, a locally audited paper turned
  extraction_failed by a cloud failure, and the audit stage held pending by a cloud store. Those
  are wrong results on the local arm caused by a comparison arm. Whether processing is recorded
  per arm is a real question of the event store's design and it is not U40's: it needs its own
  row and ruling (h), and nothing in U40 depends on it.
R798(c) — A1-β is the design. A cloud store writes the arm's claim events and no paper event. The
  writer's form: write_extraction_events gains one keyword-only argument, paper_event, default
  True. With the default, the function's behaviour is unchanged in every respect and none of its
  tests moves. With paper_event=False it plans and writes exactly as today — the same planner,
  the same savepoint, the same refusals, the same claim_inputs, the same supersession — and
  omits the one write_paper_event call; a PaperFailure passed with paper_event=False is refused
  before anything is read or written. Its docstring says both. The one caller that passes the
  keyword is engine/cloud/base.py, and a census assertion in the acceptance file pins that. A
  second writer function beside it was the alternative; it is not taken, because the savepoint
  and the refusals must be one code path, not two that can drift.
R798(d) — The cloud side, in engine/cloud/base.py and nowhere else:
  (i) The text reference. CloudExtractorBase.load_parsed_text resolves the paper's current
  reference and reads the text through it — the two calls parsed_text.load_parsed_text makes —
  and keeps the reference with its paper id. The identity a claim carries is the text the
  request was built from. Its return value and its FileNotFoundError behaviour do not change.
  (ii) The chain. send keeps each request hash it computes, in call order, with its paper id; a
  send for another paper starts a new chain. last_request_hash stays as it is.
  (iii) The write. store_result, after its span INSERTs and before its commit, builds one
  record with legacy_record — paper_id; arm; run_id the instance's; a minted extraction_uid; the
  kept reference; model the model string; model_digest None; expected the instance's expected
  fields; the spans it was given; attempts the chain's length; stage_name the instance's stage
  (cloud:<arm>); presented_context_sha256 the chain's last hash; context_chain the chain — and
  calls write_extraction_events on its own connection with the codebook's absence sentinels, as
  the local call passes them, and paper_event=False. One transaction then holds the legacy rows
  and the claims.
  (iv) Refusals. A reference or a chain kept for another paper is treated as absent, never used;
  the writer's own refusals then fire (ClaimWithoutInputIdentity, ClaimWithoutPresentedContext,
  RunLinkRefused with no run_id, ReuseKeyAlreadyClaimed on a second store of the same text) and
  nothing is stored for the paper in either store. No new refusal class. Each is an EventRefused,
  so U27's handlers pass it up and the run closes failed with its reason: a cloud store that the
  event store refuses is a run fault, not a paper to skip. The tuple is not edited. After a
  successful store the kept reference and chain for that paper are dropped.
R798(e) — Unchanged by U40: a failed or exhausted paper writes nothing to the database, as today
  (whether the cloud path should store an exhausted attempt's held fields is recorded, not ruled);
  resume stays on cloud_extractions (get_pending_papers is not edited); the cloud run is not
  activated (enforce_completeness already refuses unknown and missing names); the two legacy
  tables stay the cloud artefact store (R794(c)); the two transports are not edited, so
  anthropic_extractor.py:108–113 joins P7 as R795(c) provides; the reader, the monitor, the
  audit and every exporter are not edited.
R798(f) — Tests. A new file, tests/test_u40_cloud_claim_events.py, fail-first on the parent,
  no server, no provider, no key, no gitignored fixture, on the scratch seam of
  fake_probe_u40a.py: (1) test_cloud_gate_sees_what_store_result_wrote, the entry's test —
  twelve papers stored through the instance's own load_parsed_text, send (its provider call
  faked) and store_result with one identical categorical value: the gate raises
  DistributionCollapseError; the arm holds the claims with a claim_inputs row per paper; each
  claim's presented context is the paper's last request hash and its chain lists every send for
  that paper; (2) the processing axis is untouched — for every paper effective_state's
  processing and the count of paper_events rows are the same before and after the twelve stores
  (green on the parent: a preservation case); (3) refusals leave nothing in either store — no
  reference kept, no send made, state kept for another paper, no run_id, a second store of the
  same text — each raising its EventRefused class; (4) the legacy rows and the claims agree per
  field and value for a stored paper, a sentinel's "" snippet stored NULL in the claim; (5) the
  keyword — a PaperFailure with paper_event=False refused with nothing written, and an AST census
  finding the keyword passed by engine/cloud/base.py only. Tests that reach store_result and
  expect it to store, in whatever file, are coupled (c): found by the census and the selected
  runs, rewritten in the same commit with their assertions kept, on a seam that gives the
  instance a run, a reference and a chain. test_a_cloud_run_with_no_claims_is_reported_skipped_not_ok
  keeps its assertion for its true subject, a cloud run that stored no paper; its "until U40"
  wording goes.
R798(g) — Docs, in the engine commit: docs/architecture/pipeline.md's store_result line and each
  CLAUDE.md line that describes what the cloud store writes or what the cloud gate reads are
  brought to the fact — the store writes the legacy rows and the arm's claim events in one
  transaction, no paper event; the gate reads those claims. Lines found by their text and quoted
  before and after. No other docs edit.
R798(h) — Rows. At 12af-U40B's item 0, a dated note on B9 and on C28: U40's design is ruled
  (A1-β, R798(c), (d)); A1-α refused (R798(b)). Both rows close on U40 at the item 0 after it
  lands, in C49's form. Four findings are not U40's and each needs a row whose class and package
  are ruled from the record's precedent, not from a description: the processing axis has no arm,
  so a comparison arm's per-paper outcome has no event (F2, and (b)); the judge loader gets no
  span for a claim the pipeline's audit never located (F4); export_disagreement_pairs refuses a
  registry holding any model arm outside three pre-manifest names, the spec's own local arm
  included (F5); check_schema_parity's name test sends the spec's local arm to the cloud table
  (F6). The build's report quotes, for each, the nearest existing rows on the same file or
  function with their class and package; the rows are ruled after that report and entered at the
  next item 0. F5 and F6 touch the local arm on any manifest registry, so they are not behind R71.
R798(i) — Readings, no row: the pending foreign hook row at step 0, carried and named (finding 1,
  the fourth shared-ledger reading); E1 (the brief's twelve files were a grep and the census
  counts ten, the two others docstring mentions — the expectation was labelled a grep and the
  census is the count); F1 (12ae_U28A.md §7's "the writer's preconditions hold" was true of the
  arm and the run and not of the claim — the sentence R794(b) declined to build on, and the
  reason this Phase A existed); finding 6 (2h said the monitor CLI was to be left as read if the
  code gave no scratch root; CC drove it through a patched module global, labelled it FAKE, and
  listed the live data root before and after — accepted, the safeguard was the right one; a
  probe step a brief rules out is named as a deviation in the report, as this one was); F3 and
  F7. §9's first item, whether the live registry holds a manifest-registered arm, is answered at
  the rows ruling from a committed instrument's output if one shows the arms table, and stays
  open otherwise; no new query on live is ordered here.
R798(j) — The build opens as one brief, 12af-U40B, in R785(h)'s form: the read-out quoted the
  writer, the store, the refusals and the transaction at 9b9f3d6 and the engine tree is unchanged
  since; the three INFERRED items above are read at HEAD before the first edit and a difference
  stops the build. No model call, no provider call, no live write; the standard gate by the
  wrapper against 876–1069 s with R797(d)'s hold. This ruling states no standing rule: the id
  list, the glosses file and STANDING_RULES.md are untouched at item 0, asserted by content.
===== END r798.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12af-U40B] — Land U40 as R798(c)–(g) rule it: the cloud store writes
  the arm's claim events and no paper event; one keyword on the writer; the text reference
  and the request-hash chain kept on the cloud instance by paper; the fail-first acceptance
  file; the coupled tests rewritten; the docs lines; one engine commit with its gate,
  evidence in its own commit. Item 0: R798 as the next block; the B9 and C28 notes; this
  brief committed as pasted; no standing rule.
PURPOSE: Make the cloud arm's distribution gate see what the cloud path stored, by putting
  the arm's claims in the event store the gate reads, without letting a comparison arm move
  the paper's processing state. Behind R71 on live. Code task. No live write. No model
  call. No provider call. No API key read.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  Touched: engine/core/extraction_events.py (the paper_event keyword on
  write_extraction_events and its docstring, nothing else); engine/cloud/base.py
  (load_parsed_text, send, store_result and the instance state they share, the imports they
  need, nothing else); tests/test_u40_cloud_claim_events.py (new); the coupled tests the
  census and the selected runs find (R798(f)); docs/architecture/pipeline.md and CLAUDE.md
  (R798(g)'s lines only); docs/inventory/ (regenerated if a recorded entry moves, R786(b));
  docs/plan/ENGINE_REFACTOR_PLAN.md (Block 196; the B9 and C28 notes);
  docs/session-reports/session-12/12af_open/r798.txt; a new directory
  docs/session-reports/session-12/12af_U40B/ (brief_12af_U40B.md at item 0;
  b0_parent_acceptance.txt, gate_u40b/, the pin recompute and the post-build probe output at
  step 3); ~/scratch/12af-u40b/; the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): any other file under engine/ — the two
  transports, effective.py, events.py, the planner (plan_extraction_events) and
  legacy_record, the monitor, the audit, the exporters, parsed_text.py, run_manifest.py,
  the run-fault tuple; scripts/ and analysis/; get_pending_papers and get_progress;
  review_specs/ (R71: enabled_arms stays empty on disk; a test's spec copy lives under its
  temp directory); the codebook; the tools; the id list, the glosses file and
  STANDING_RULES.md (not one byte — R798 states no rule); review.db (mode=ro instruments
  only; no new query on live); any .env or API key (a test that would construct a real SDK
  client is a STOP); the working tree inside 07:00–10:35 UTC (R86); any scratch directory
  other than ~/scratch/12af-u40b/.

TIMING: the gate must exit before 07:00 UTC: launch by 06:30 UTC or after 10:35 UTC. If the
  clock reaches 06:30 before the engine commit is gated, hold with the commit unpushed and
  resume in this brief after 10:35 UTC (R789(a)); say so in the report.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12af-U40A report; R798): HEAD aa97c6af278d91ec0bba5e1a1ff7330397bf842f, parent
    9b9f3d6f60f8c98dba7e4f09fcbe93b627dba156, clean, level; plan sha256
    79f1c72038d34b8bfed8e2f7946c3c424eaafa0e4ba27412fbafb9b805e3e347, 17,913 lines, ends
    "…ends at R797.", last block 195; claude-config at 9e3fd22 or an accepted descendant
    (R663(b)); the three rules files as 12af-OPEN's 0c read them; gate reference
    12af_open/gate_12af_open/ (3,544 passed / 1 xfailed / 17 deselected on 3,562 / 3,545;
    its collected_ids.txt and durations_12af_open.txt); band 876–1069 s (R796(b)); pins
    12ae_close/pin_tuples_close.json.
  READ (12af_U40A/12af_U40A.md at aa97c6a — §2's writer, payload table, preconditions and
    transaction; §3's consumer table; §4's coupled-test counts; §5's probe; §7's mapping —
    not repeated; step 2a re-reads each site at HEAD). READ (the architect's clone, same
    commit): base.py send :135 with "self.last_request_hash = sha256_canonical(payload)" at
    :139, load_parsed_text :171, store_result :324, "self._conn.commit()" :409;
    extraction_events.py legacy_record :287, write_extraction_events :670,
    "write_paper_event(conn, **plan.paper_event, commit=False)" :701; docs/architecture/
    pipeline.md:248 "6. `store_result()` — atomic insert to cloud_extractions +
    cloud_evidence_spans."; tests/test_u28_cloud_gate_and_boundary.py:13 and :115, the two
    "until U40" lines; docs/inventory/entry_points.md records engine/cloud/base.py line 520.
  INFERRED — BLOCKING (read at 2a before any edit; false = STOP):
    I1. The engine tree is unchanged since 24ce26b.
    I2. legacy_record builds a record of value outcomes from the span dicts store_result
        is given (the shape enforce_citations already takes there), with expected the
        instance's expected fields; fake_probe_u40a.py's accepted write (ii)(d) is the
        worked form — quote it.
    I3. store_result is reached only after an accepted attempt, so the last hash send
        computed for the paper is the accepted request's (the two transports' loops and
        extract_with_completeness, quoted).
    I4. Every refusal R798(d)(iv) names is an EventRefused and the two transports' store
        handlers re-raise it through is_run_fault (the class lines and the handlers,
        quoted).
    I5. With paper_event=False the plan's field events alone leave the store consistent:
        no reader joins a claim's extraction_uid or reuse key to an `extracted` paper
        event of the same arm (grep and the census's reader list; incomplete_extractions
        and claim_text_ref read first). If one does, STOP with the finding.
  INFERRED — expectation (report a mismatch, continue):
    E1. On the parent, R798(f)'s cases (1), (3), (4) and (5) are red and (2) is green;
        N_red and N_green read from the run.
    E2. Coupled tests (c): the six store tests in tests/test_cloud_extraction.py, the
        store-reaching tests in tests/test_u28_cloud_gate_and_boundary.py and
        tests/test_u27_cloud_run_faults.py; the census and the selected runs decide, and
        a file not named here is a (c), not a stop.
    E3. The gate collects 3,562 + N and selects 3,545 + N, N the new file's ids plus any
        id a rewrite renames (each rename an only-ref / only-gate pair, read and matched).
    E4. The inventory's recorded entry in base.py moves (lines are inserted above :520):
        an entry change, regenerated and kept (R786(b)); a stamp-only diff is reverted
        (R793(e)).
    E5. Pins and the audit prompt_hash do not move: no request byte changes.

CONTRACT (steps, in order; evidence under ~/scratch/12af-u40b/):

  STEP 0 — Pre-check (read-only). date -u. HEAD and parent proved by git cat-file -e
    <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. Plan: sha256, lines, last
    line, last block 195, no Block 196. Rules files: the three sha256s. claude-config:
    9e3fd22 or an accepted descendant, level; a pending foreign row is carried and named
    (R654(d)). I1. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12ae-U28B's item 0,
    a block, two row notes and the brief file; ~/scratch/12af-u40a/check_item0_u40a.py and
    block195.json quoted before composing; the check written for this brief, R794(i)).
    1a. docs/session-reports/session-12/12af_open/r798.txt from Section 1, one write,
        hashed straight after (R738(b)): EXPECTED e293ebe48b2f376c6db4e2dd0cddad30d543fe87bfb2587f2dc5c98f2f5f7bbe,
        13588, 153. Mismatch = STOP (R739(b)).
    1b. block196.json in block195.json's key set: tag R798; transcribed = the commit's UTC
        date (R775(a)); item "item 0 of the 12af-U40B brief"; beside = "the B9 and C28
        notes and the brief file it orders" (composed; quote it); made_by PI; made_date
        2026-11-11; session 12af; report = the 12af-U40A final report with its item-0 and
        read-out hashes, in Block 192's form (R794 on the 12ae-U28A final report — quote
        that header first); sentence model, followed "12af-U40B brief"; prev_ends R797.
        Render to stdout, compare the element order with Block 192's, then --append.
    1c. Notes (R798(h)), each appended at the end of its row's status cell in that row's
        note form, quoted before composing, tagged "*Note <the commit's UTC date>
        (12af-U40B item 0, R798(b), (c), (d)):*" — on B9: U40's design is ruled; the cloud
        store writes the arm's claim events and no paper event (A1-β); A1-α refused, the
        processing axis having no arm; Wave 2 row 25 lands with U40's engine commit; the
        row closes at the item 0 after it. On C28: the same design in one sentence; the
        legacy tables stay the artefact store (R794(c)); resume stays on cloud_extractions;
        anthropic_extractor.py:108–113 joins P7 (R795(c), R798(e)); the row closes at the
        item 0 after U40 lands. Two rows replaced; the "−" count read and matched to two.
    1d. The brief file (R784(g)(v)): 12af_U40B/brief_12af_U40B.md from this whole pasted
        block, "CLAUDE CODE" through the STOP-12AF-U40B line, one trailing newline, written
        once; sha256, bytes and lines straight after the write, reported.
    1e. The item-0 check: --check-plan byte-identical; the plan's numstat read ("−" matched
        to the two rows); every earlier "ends at" line kept; the last line "…ends at R798."
        once; the plan equals HEAD's plus the two rows replaced and Block 196 appended,
        nothing else (computed); each row's last note begins with its 1c tag; r798.txt
        equal to 1a; the three rules files unchanged and unstaged; the brief file hashed;
        block196.json's transcribed value equal to the commit-time UTC date; the staged set
        exactly the plan, r798.txt and the brief file. Evidence by a redirect inside the
        check; quote the check.
    1f. <check> && <commit>, one line; the message from the staged set (R778(d)), docs
        scope. Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers, foreign pending rows named; push; level.
        Record hashes and parents.

  STEP 2 — The build (one engine commit).
    2a. Re-read at HEAD and quote, before any edit: write_extraction_events whole;
        legacy_record's signature and body (I2); base.py's send, load_parsed_text,
        store_result and extract_with_completeness; both transports from load_parsed_text
        to their store handler (I3, I4); parsed_text.py's resolve_parsed_text,
        read_parsed_text and load_parsed_text; the extractor.py:709–718 call (how the
        local path passes sentinels); the EventRefused class lines of R798(d)(iv) and the
        run-fault tuple (I4); I5's readers; pipeline.md:248 and every CLAUDE.md line that
        states what the cloud store writes or what the cloud gate reads (found by text,
        listed); the `cloud` fixture and helpers of tests/test_u28_cloud_gate_and_boundary.py
        and fake_probe_u40a.py's seams (the scratch database with recorded parsed texts,
        open_run's registration, the counting refusers). Any difference from the read-out,
        or I2–I5 false, = STOP.
    2b. Census (census_u40a.py's form, run before the edit): every test function that
        reaches store_result — directly, through a transport's run, or through a helper —
        with its fixture and whether it expects a store; every test naming
        write_extraction_events. Output committed with the evidence.
    2c. Write tests/test_u40_cloud_claim_events.py, fail-first, R798(f)'s five cases as it
        names them; the conftest fence on; each docstring states its case and its seam; the
        provider call faked at the instance's own seam so that send runs and computes its
        hash (no SDK client constructed — asserted). Run on the parent: EXPECTED E1; the
        red and green ids listed; output to b0_parent_acceptance.txt. A red case of E1
        green on the parent = STOP.
    2d. Edit, R798(c) and (d) exactly, and nothing else:
        — extraction_events.py: the keyword-only paper_event=True on
          write_extraction_events; False omits the write_paper_event call and nothing
          else; a PaperFailure with paper_event=False refused before any read; the
          docstring states both. No other line of the file changes.
        — base.py: (d)(i) load_parsed_text keeps the resolved reference with its paper
          id, its return value and exceptions unchanged; (d)(ii) send keeps the chain by
          paper id; (d)(iii) store_result builds the record and calls the writer between
          the span INSERTs and its commit, on self._conn; (d)(iv) state kept for another
          paper is treated as absent; the kept state for a paper is dropped after its
          successful store. store_result's signature, its checks, its INSERTs and its
          commit and rollback lines do not change.
        — Docs, R798(g): the lines 2a listed, each to the fact; a line not found as
          quoted = STOP for the docs step.
        — Tests, R798(f): each (c) test rewritten with its assertion kept, on a seam that
          gives the instance a run, a reference and a chain; the U28 test's subject and
          wording as R798(f) states. A test whose assertion cannot be kept is not
          rewritten: STOP with the test quoted.
        git diff --stat EXPECTED: the two engine files, the new test file, the (c) test
        files, pipeline.md, CLAUDE.md, and docs/inventory/ under E4. The "−" lines of the
        two engine files quoted and matched to the edits above.
    2e. Pins: 12l_U04/pin_tuples.py with both arguments, byte-identical to
        12ae_close/pin_tuples_close.json; the audit prompt_hash unchanged. A moved value =
        STOP.
    2f. Selected runs green: the new file; tests/test_cloud_extraction.py,
        tests/test_u27_cloud_run_faults.py, tests/test_u28_cloud_gate_and_boundary.py whole;
        every file the 2b census lists for write_extraction_events; the monitor's and the
        write-boundary guard's files as U28B's 2e named them. A red here is classified
        before anything else: (c) coupled → rewritten under 2d's rule; (r) a regression of
        the default path (any test of the local writer, the reader, the audit or the
        monitor) = STOP. Commit the engine change (fix scope; the message names B9, C28
        and U40, the keyword, the cloud write, the docs lines, the rewritten tests and the
        inventory if regenerated), from the staged set. Do not push before the gate.
    2g. The post-build probe (FAKE, fake_probe_u40b.py → .txt, on the committed engine
        tree, fake_probe_u40a.py's seams): fourteen papers stored by the cloud arm through
        the built path beside a local arm's audited papers, then each consumer the
        read-out's table marks under A1-β driven once — audit_run for the local arm
        (EXPECTED: no audited_ai on a paper the local arm holds no claim on), the workflow
        gate's states, generate_prisma_flow, get_pipeline_stats, the evidence-table and
        docx exports for the cloud arm and for the local arm, concordance's load_arm and a
        pair, judge_loader.load_grid, validate_all with the cloud arm. The β column is
        confirmed or contradicted line by line. A consumer that raises, or a local-arm
        result that differs from the same probe without the cloud stores, = STOP before
        the gate, the engine commit standing and unpushed.
    2h. The standard gate by launch_gate.sh: evidence_dir ~/scratch/12af-u40b/gate_u40b/,
        a session name, ref_ids 12af_open/gate_12af_open/collected_ids.txt, ref_durations
        12af_open/gate_12af_open/durations_12af_open.txt, no selection. Before it: the
        host snapshot and the Ollama reads ≥5 s ahead (R660(b)); the hold of R797(d) —
        when the snapshot names another lane's full test suite or an encode, the launch
        waits for it, bounded at sixty minutes and by the clock above; a launch at the
        bound is reported with the job named. EXPECTED E3; 1 xfailed; 17 deselected; wall
        inside 876–1069 s, outside it reported with the concurrent jobs, not adapted to;
        no phase >10 s slower; no model load in the window (R696(b)); the nineteen files.
        A red: class (c) per R746(c) and R772(b), fixed under 2d's rule and the gate
        re-run once; any other red = STOP, the engine commit standing.
    2i. Fingerprint --compare; push; level; --compare after (IDENTICAL). Ledger row; push;
        level.

  STEP 3 — Evidence commit (R731(c)). 12af_U40B/: b0_parent_acceptance.txt, the 2b census
    script and output, fake_probe_u40b.py and .txt, gate_u40b/ (the nineteen files),
    pin_tuples_u40b.json; the staged set exactly those, every sha256 equal to its scratch
    source; chained check && commit; fingerprint IDENTICAL before and after the push;
    level; ledger. Record hashes and parents.

  STEP 4 — For the rows ruling (read-only, in the report, no edit): for each of R798(h)'s
    four findings, the nearest existing inventory rows on the same file or function,
    quoted with their class and package; and whether any committed instrument output
    (the fingerprint record, dbchecks) shows the live arms table's rows — quoted if so,
    "not shown" if not.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1–I5 held, each beside its quoted lines.
  G2. Item 0 one commit that passed 1e in one chain: r798.txt at the architect's hash;
      Block 196 by the tool; the two notes; the rules files untouched by content; the brief
      file hashed; pushed, level, fingerprint IDENTICAL; ledger.
  G3. E1's red cases red on the parent and green in the gate, its green case green on both;
      the engine diff confined to the two files and to the edits named; every local-writer
      test green untouched; pins and the audit prompt_hash unchanged; 2g's β column
      confirmed with no local-arm result changed; the gate at E3's counts, no load, nineteen
      files; pushed, level, fingerprint IDENTICAL; ledger.
  G4. The evidence commit as step 3 states; step 4's quotes in the report.

OUT OF SCOPE: whether processing is recorded per arm (R798(b), (h)); F4, F5 and F6 (rows
  after this report); storing an exhausted cloud attempt's held fields; moving resume to
  the reuse-key read; activating the cloud run; retiring the legacy cloud tables; the two
  transports and anthropic_extractor.py:108–113; U29; R71; the tuple; the tools items;
  closing B9 or C28 (the next item 0); the 2026-11-11 job reads; /wrap.

CONSTRAINTS (task-only): no model call, no provider call, no API key read, no restart, no
  live write, no new query on live; no edit in the working tree between 07:00 and 10:35
  UTC. A count arrived at by eye is INFERRED; a computed value is reported with its
  command. Where this brief describes a tool, a record or a line of code, the record and
  the code govern and a difference is a finding (R732(a)). The design is named by its
  ruling clause, R798(c) or (d)(i)–(iv), throughout.

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — block196.json, the render
  comparison, the two rows' last notes, r798.txt's hash, the brief file's sha256, bytes and
  lines, the check verbatim, chain, hashes, numstat, fingerprint, ledger; (2) the build —
  I2–I5 each with its quoted lines; the census; the new tests as written;
  b0_parent_acceptance.txt verbatim; the two engine files' diffs whole; each (c) test with
  its assertion before and after; the docs lines before and after; the inventory diff; the
  pin recompute; the selected runs; the engine commit's hash and parent; (3) the post-build
  probe, consumer by consumer against the β column; (4) the gate — launch line, any hold,
  wall against the band, the summary line verbatim, collected and selected, the id
  comparison, durations, journal window, concurrent jobs, the nineteen files; (5) the
  evidence commit; (6) step 4's quotes; (7) contradictions and findings, numbered, each
  with its quoted line; (8) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, a 1a mismatch, or a check failure or crash at 1e
  = STOP before the item-0 commit. I2–I5 false, a difference from the read-out at 2a, a red
  case of E1 green on the parent, an (r) red, a (c) test whose assertion cannot be kept, a
  diff in an engine file outside the two, a test that would construct a real SDK client, or
  a moved pin = STOP before the engine commit. A 2g contradiction = STOP before the gate.
  A non-(c) red in the gate = STOP, the engine commit standing and reported. An expectation
  mismatch (E1–E5) is reported and the work continues. A spec premise that does not survive
  contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not close B9 or C28 and do not start U29. Await the next
  brief or ruling in this session. STOP-12AF-U40B
