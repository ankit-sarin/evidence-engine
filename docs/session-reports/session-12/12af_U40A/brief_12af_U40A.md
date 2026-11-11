CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R797, PI ruling 2026-11-10 UTC; not on disk)
================================================================================

R797 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r797.txt: sha256 4cfee52d788d6556f57233ec6365306ae7cb703d4e145da4fbb617238f29121b,
            7675 bytes, 87 lines, one trailing newline.

===== BEGIN r797.txt =====
RULING [evidence-engine 12af-R797] — 12af-OPEN accepted; the series gains 966.10 s; claude-config
  0a479b6 accepted; the gate launch held for another lane's suite was right and is a reading; two
  of the brief's values were the architect's; no standing rule; U40's Phase A opens
STATUS: PI ruling, 2026-11-10 UTC, session 12af, on architect recommendation, on the 12af-OPEN
  final report (item 0 91a619b833d177e27b2570d030e4e9e476238f81; evidence
  3310738c92f2dca32e407f8bef7d102813c1fda9). Transcribe at item 0 of the 12af-U40A brief below,
  by transcribe_ruling.py, in the block after R796's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the 12af-OPEN final report):
  MEASURED: "22:55:54 UTC on 2026-11-10, so the item-0 commit carries 2026-11-10"; "HEAD was
    0a479b6, a child of 34e422a, level and clean"; "Its ctime is 2026-11-09 23:40:06 UTC; the
    transcript runs 02:57:16Z to 23:39:58Z"; "the 2026-11-10 pair only"; "20/20 loads OK, no FAIL,
    67 GIN lines with no non-200, 20 loads. NV_ERR_NO_MEMORY still 31 this boot"; "matches line for
    line on 840db24: collection 3562/17/3545, "3544 passed, 17 deselected, 1 xfailed, 2 warnings in
    964.91s", exit 0 as the last line"; "r796.txt is 7805916b…f1, 10,200 B, 111 lines, equal to
    your hash"; "The render's element order equals Block 181's"; "brief_12af_OPEN.md is
    d6f66cff023f65eb1e59944a06c9c51e3cf72490ccb2237818e07307b6ea6d5f, 34,437 B, 430 lines" (the
    architect's copy: equal, cmp byte for byte against the clone); "Plan numstat +122/−0. "ends at"
    lines 150 before, 151 after. The plan is now 17,815 lines,
    a6263dc99c7f758eb8cd5324904760ecf58af69d28ff4108adef797a259c97c9, 3,054,151 B, and ends at
    R796"; "91a619b (parent 840db24), pushed and level, fingerprint IDENTICAL before and after";
    "5beed3d (parent 0a479b6), row 23:02:07Z, no foreign pending rows"; "RESULT PASS, 0 mismatches,
    no SKIPPED"; "rows 286 (94/56/136/0); P1 62 (25/25/12); P6 12; P7 121; fix units 35 / 37,19 /
    38 done 31; R510 2 in, 0 out; expected_holds true. Its JSON is byte-equal to the committed
    0b9f41f9…"; "212 at 840db24 and 212 at 91a619b, 0 in, 0 out, no move"; "Launch:
    2026-11-10T23:08:50Z … Exit 23:24:56Z, rc 0"; "966.10 s, inside 876–1069 s"; "3544 passed, 17
    deselected, 1 xfailed, 2 warnings (sklearn) on 3562 / 3545. Ids 0/0. 2,620 phases, none slower
    by more than 10 s"; ""-- No entries --", no load"; "uvicorn only, before and after the gate";
    "the nineteen files, names equal to gate_12ae_open/'s"; "24 copies (19 gate files, plus 5
    job-read files into 12ae_close/), every one sha256-equal to its source"; "3310738 (parent
    91a619b), pushed and level, fingerprint IDENTICAL before and after"; "96f776c (parent 5beed3d),
    row 23:26:17Z"; "the four band values are unchanged".
  READ (CC's): findings 1–7; "authored by the PI's account with no trailers. It adds six
    surgical-cv-r53-b1 rows, removes no row and leaves primer.md 41–45 intact"; "It holds
    expected_nightly_20261110.md, the file this brief itself cites"; "I waited on their PIDs with a
    60-minute bound, and they cleared at 23:08:36"; "pytest-614, appeared at 23:13:05, inside the
    gate window. Its origin is not attributed"; "I ran it on 91a619b; the JSON is byte-equal".
  READ (the architect's clone at 3310738): the two full hashes in STATUS; gate_wall.txt "wall:
    966.095243901 s".
  INFERRED: none.

R797(a) — The 12af-OPEN final report is accepted as printed: item 0 91a619b (Block 194 by
  transcribe_ruling.py; r796.txt in 12ae_close/ beside the lane-line copy; the brief file
  byte-equal to the architect's copy; the three rules files untouched by content), the evidence
  commit 3310738 (the nineteen gate files under 12af_open/gate_12af_open/; the five job-read files
  under 12ae_close/), ledgers 5beed3d and 96f776c; the open check 0 MISMATCH, 0 SKIPPED; the
  recount 286 (94 / 56 / 136), fix units 35 / 37, 19 / 38 done 31, its JSON byte-equal; R510 212 /
  212, in 0, out 0; the 2026-11-10 nightly a line-for-line match on 840db24, ids 0/0, its window
  empty; the health job 20/20. No stop.
R797(b) — The series on this host state gains 966.10 s (the 12af-OPEN gate, beside uvicorn only
  in both snapshots): the thirty-seven of R796(b) plus 966.10, thirty-eight. It is a standard
  member, not a confounded one: the foreign suite had exited before the launch. The temp-root
  entry pytest-614 that appeared inside its window is carried beside the value as an unattributed
  reading. The band in force stays 876–1069 s (R796(b)) until the wrap ruling sets 12ag's.
R797(c) — claude-config at 0a479b6, one commit past 34e422a, is accepted under R663(b) (finding
  1): six rows of another lane, no row of this lane removed, primer.md lines 41–45 intact. The
  next check reads claude-config at 96f776c or an accepted descendant.
R797(d) — Finding 4, the hold. CC held the gate's launch from 23:04 to 23:08:50 UTC while another
  lane's full test suite ran, bounded the wait at sixty minutes, and launched when it exited. The
  brief did not provide for this and CC was right: the suite is the confounder R793(c) named, the
  wait was short, bounded and reported, and nothing was signalled. A reading, no row. For the
  gates of session 12af each gate brief carries the same step as a task gate: when the snapshot
  before launch names another lane's full test suite or an encode, the launch waits for it,
  bounded at sixty minutes and by the R86 clock; a launch made at the bound is reported with the
  job named and its value enters the series as confounded (R677(f)). Whether this becomes a
  standing rule is for the 12af wrap, on the session's gates; none is stated here. That a job
  can start inside a gate's window unseen by either snapshot (pytest-614) joins the cross-lane
  item for dgx-infra.
R797(e) — Readings, no row: finding 2 (the brief's I2 gave session 53a21c5b's end as 23:24 UTC —
  the unified plan's wrap time, not the session's last entry; the record reads 23:39:58Z and the
  archive 23:40:06; CC's window from the record stands, and the plan's figure is corrected at its
  next version); finding 3 (the brief's R4 said 12ae_close/ held no file dated 20261110: the
  architect's, written from the names the open would add and not from the listing it had read —
  expected_nightly_20261110.md is one of the 24; nothing was overwritten, which was the thing the
  line was for); finding 5 (the 12af block says the recount runs on the plan at df3e721 and the
  brief named the item-0 commit as the eighth argument, as 12ae-OPEN ran it; the JSON is
  byte-equal either way, as the block's own sentence on a later plan says); finding 6 (the diff
  stats compared by content, the layout a difference of form — the right comparison); finding 7
  (E1–E5 held). The architect's errors this session so far: two (findings 2 and 3), each caught
  by CC reading the record over the brief (R732(a)); neither stopped the brief.
R797(f) — U40's Phase A opens: "The cloud path writes claim events" (B9 and C28 for Wave 2 row
  25; R794(b)), read-only, in 12ae_U28A.md's form, its entry quoted first, 12ae_U28A.md §9's
  questions answered before any option is written: the event writer's form for a write with a
  run_id and no activation; what each consumer of the grid does with a cloud arm's claims;
  is_assigned for a model-kind arm; whether any reader filters cloud arms or still reads the two
  legacy tables. No engine, test, script, spec or codebook edit; no model call; no provider call;
  no live write. The design ruling follows its read-out. This ruling states no standing rule.
===== END r797.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12af-U40A] — U40's Phase A read-out ("The cloud path writes claim
  events"; B9 and C28 for Wave 2 row 25). Item 0: R797 as the next block; this brief
  committed as pasted; no standing rule. Read-only on engine/ and tests/; STOP before any
  edit.
PURPOSE: Put on disk, before the design, what U28's Phase A could not trace: the exact form
  the engine's event writer needs for a cloud write, and what every consumer of the grid
  would do once a cloud arm holds claims. R794(c) made A1 the design of record; this
  read-out says whether A1 can be built as one change in store_result or drags other files
  with it. Mixed: one docs commit (item 0), one docs commit (the read-out). No engine,
  test, script, spec or codebook change. No live write. No model call. No provider call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 195 appended by the tool, nothing
  else); docs/session-reports/session-12/12af_open/r797.txt; a new directory
  docs/session-reports/session-12/12af_U40A/ (brief_12af_U40A.md at item 0; the read-out,
  census and probe scripts with outputs at step 2); ~/scratch/12af-u40a/; the claude-config
  ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/ (R71: enabled_arms stays empty), the codebook, docs/architecture/,
  CLAUDE.md, docs/inventory/, the tools; the id list, the glosses file and
  STANDING_RULES.md (not one byte — R797 states no rule); rows B9 and C28 (quoted, not
  edited: no note is ordered here); review.db (mode=ro instruments only; no new query on
  live — the probe uses a scratch database); any .env or API key (never read; the probe
  constructs no SDK client); any scratch directory other than ~/scratch/12af-u40a/; the
  working tree inside 07:00–10:35 UTC (R86).

TIMING: outside 07:00–10:35 UTC. If the clock reaches 06:45 UTC before the read-out commit,
  hold with the work on disk uncommitted and resume in this brief after 10:35 UTC (the
  pause of R789(a)); say so in the report.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12af-OPEN report; R797): HEAD 3310738c92f2dca32e407f8bef7d102813c1fda9, parent
    91a619b833d177e27b2570d030e4e9e476238f81, clean, level; plan sha256
    a6263dc99c7f758eb8cd5324904760ecf58af69d28ff4108adef797a259c97c9, 17,815 lines, ends
    "…ends at R796.", last block 194; claude-config at 96f776c or an accepted descendant
    (R663(b)); the three rules files as 12af-OPEN's step 0c read them. (12ae-U28A, I3) a
    cloud arm is registered and pinned on a scratch ReviewDatabase by rm.open_run(…
    cloud_arms=[arm] …) with no SDK client built; fake_probe_u28a.py is that probe.
  READ (the architect's clone at 3310738; re-read on the box, not trusted):
    R1. U40's entry, 12ae_open/r794.txt:60–66: "U40 — "The cloud path writes claim events"
        (rows B9 and C28 for Wave 2 row 25; files engine/cloud/base.py and whatever U40's
        Phase A names; depends on U28; pin effect not affecting), two-phase, its Phase A
        tracing the writer's form and the grid's consumers (concordance, the exports, the
        judge loader, the audit; is_assigned for a model-kind arm, F6) before any design."
        and "The entry's named test test_cloud_gate_sees_what_store_result_wrote is U40's".
        R794(c): A2 and A3 refused; "the two legacy tables stay as the cloud artefact
        store".
    R2. 12ae_U28A/12ae_U28A.md §9, five items: the consumers under A1 "not traced";
        "`write_extraction_events`' exact form for a cloud write (the payload,
        `extraction_uid`, `run_id` without an activation) was not read line by line"; the
        CLI's N44 half "read, not driven"; the `strict` case "read, not driven"; "Whether
        any reader filters cloud arms out of the grid was not searched beyond
        `registered_arms`' own SQL". Its §7 A1 and §8 F5 and F6 are this unit's ground.
    R3. The writer: engine/core/extraction_events.py:670 "def write_extraction_events(conn,
        outcome, *, sentinels=frozenset(),". Its callers under engine/, scripts/ and
        analysis/: engine/agents/extractor.py:709, :1087 ("write_extraction_events(db._conn,
        PaperFailure("), :1178; engine/elicitation/pipeline.py:663. None under
        engine/cloud/.
    R4. The reader, engine/core/effective.py: is_assigned :160 (its last line "return row[0]
        == "model""), live_claim_events :242, effective_value :305, registered_arms :495,
        eligible_paper_ids :520, iter_grid :580. store_result: engine/cloud/base.py:324.
    R5. Files outside effective.py that name iter_grid, effective_value, live_claim_events
        or registered_arms (a grep, not a census): analysis/paper1/export_disagreement_pairs.py,
        analysis/paper1/judge_loader.py, engine/agents/audit_events.py,
        engine/agents/auditor.py, engine/analysis/concordance.py, engine/core/events.py,
        engine/core/extraction_events.py, engine/core/selection.py,
        engine/exporters/docx_export.py, engine/exporters/evidence_table.py,
        engine/validators/distribution_monitor.py, engine/validators/extraction_validator.py.
    R6. 12ae_U28A/12ae_U28A.md is the form (the blocking answer first, then §0–§9);
        census_u28a.py and fake_probe_u28a.py are the instruments' forms.
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 24ce26b (git diff --stat
    24ce26b HEAD on engine tests scripts analysis review_specs empty).
  INFERRED — expectation (report a mismatch, continue):
    E1. The census finds R5's twelve files and R3's four call sites; more or fewer is a
        finding with the difference listed.
    E2. At HEAD the entry's test is red in U28's way: after twelve store_result writes the
        gate reports skipped on a population of 0 and does not raise.
    E3. No expectation is stated on Q1–Q4 below. They are the questions; the architect
        has not read those code paths and predicts nothing about them.

CONTEXT: session 12af opened at 3310738 (R797). U28 landed D, B and C at 24ce26b; U40 is A1,
  entered by R794(b), next before U29. The cloud path is unreachable on live (R71) and on
  Run 7. Phase A of the two-phase form; the design ruling follows; one build brief if the
  trace is complete, and if it is not, the read-out says what is still untraced (R794(b)).

CONTRACT (steps, in order; evidence under ~/scratch/12af-u40a/):

  STEP 0 — Pre-check (read-only). date -u (outside 07:00–10:35 UTC). HEAD and parent
    proved by git cat-file -e <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree.
    Plan: sha256, lines, last line, last block 194, no Block 195. Rules files: the three
    sha256s equal to 12af-OPEN's 0c. claude-config: 96f776c or an accepted descendant,
    clean, level. I1. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12af-OPEN's item 0,
    91a619b, less its lane-line copy; ~/scratch/12af-open/check_item0_12af.py and
    block194.json quoted before composing; the check written for this brief, R794(i)).
    1a. docs/session-reports/session-12/12af_open/r797.txt from Section 1, one write,
        hashed straight after (R738(b)): EXPECTED 4cfee52d788d6556f57233ec6365306ae7cb703d4e145da4fbb617238f29121b,
        7675, 87. Mismatch = STOP (R739(b)).
    1b. block195.json in block194.json's key set: tag R797; transcribed = the commit's UTC
        date (R775(a)); item "item 0 of the 12af-U40A brief"; beside = "the brief file it
        orders" (composed; quote it); made_by PI; made_date 2026-11-10; session 12af;
        report = the 12af-OPEN final report with its item-0 and evidence hashes, in Block
        182's form (R784 on the 12ae-OPEN final report — quote that header first);
        sentence model, followed "12af-U40A brief"; prev_ends R796. Render to stdout,
        compare the element order with Block 182's, then --append.
    1c. The brief file (R784(g)(v)): 12af_U40A/brief_12af_U40A.md from this whole pasted
        block, "CLAUDE CODE" through the STOP-12AF-U40A line, one trailing newline, written
        once; sha256, bytes and lines straight after the write, reported.
    1d. The item-0 check: --check-plan byte-identical; the plan's numstat read ("−" matched
        to 0); every earlier "ends at" line kept; the last line "…ends at R797." once; line
        count 17815 + N; r797.txt equal to 1a; the three rules files unchanged and
        unstaged; the brief file hashed; block195.json's transcribed value equal to the
        commit-time UTC date; the staged set exactly the plan, r797.txt and the brief file.
        Evidence by a redirect inside the check; quote the check.
    1e. <check> && <commit>, one line; the message from the staged set (R778(d)), docs
        scope. Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers, foreign pending rows named (R654(d));
        push; level. Record hashes and parents.

  STEP 2 — The read-out (read-only; no server; no provider; no live database). Every code
    claim beside a quoted line with its path and line at HEAD, labelled CODE ORDER or FAKE.
    2a. §1 Inputs quoted: U40's entry (R1) with R794(b), (c) and (h); rows B9 and C28 whole
        with every dated note; Wave 2 row 25 (12j_review_w2.md); 12ae_U28A.md §7's A1 and
        §8's F5 and F6; R12, R13 and R30's blocks and migration 016's docstring on the
        event store and on the two cloud tables, as far as they speak of arms, assignment
        and readers.
    2b. Q1 — THE WRITER. write_extraction_events read line by line (R3): its signature and
        every keyword; the outcome types it accepts and what each local caller builds and
        passes (the four sites quoted, the failure form at :1087 included); the payload of
        a claim event, field by field, against what a cloud span carries (field_name,
        value, source_snippet, confidence, tier, notes) and what store_result has in hand
        (paper_id, arm, model string, tokens, cost, codebook hashes); where extraction_uid
        and run_id come from on the local path and what the cloud run holds at store_result
        (R68's run_id; what "activation" is, who performs it, and whether the writer
        requires one); the writer's preconditions on the arm and the run; its transaction
        behaviour (what it commits or leaves open) against store_result's own commit and
        rollback; what a second write for the same paper and arm does (supersession), and
        how the cloud path decides a paper is already done today (which table it reads on
        resume). Each answered from quoted code. A precondition the cloud run cannot meet
        is the finding, stated plainly.
    2c. Q2 — THE CONSUMERS. For every file the census finds (E1), and for the concordance,
        the exports, the judge loader and the audit by name: which arms it reads (every
        registered arm, a kind, a named list, a run's arms) with the line quoted, and what
        it would do the day a cloud arm holds claims — include it in a table, score it,
        select it for audit, export it, gate on it. Mark each consumer CHANGES or DOES NOT
        CHANGE under A1, with the reason. The audit stage is answered first and in most
        detail: whether a cloud arm's claims would be selected for audit, and what would
        then be called on them.
    2d. Q3 — ASSIGNMENT. is_assigned for a model-kind arm (R4, F6): every caller, and where
        a cell that is assigned and unclaimed surfaces as missing — a count, a denominator,
        a completeness or close condition, an export row. State whether any run-level gate
        or stage-complete condition would read a registered cloud arm's unclaimed cells as
        a failure of the local run.
    2e. Q4 — FILTERS AND THE LEGACY TABLES. Every read of cloud_extractions or
        cloud_evidence_spans under engine/, scripts/ and analysis/ (SELECTs, by the census),
        each with what it feeds. Every place a reader tests an arm's name, kind, prefix,
        run or the spec's cloud_arms / enabled_arms to include or exclude it. State whether
        any consumer would see a cloud arm's values twice under A1 (once from the legacy
        tables, once from the grid), and whether the two would agree.
    2f. §4 Census: census_u40a.py in census_u28a.py's form (AST; its declared completeness
        stated): calls of the writer; calls of each reader function of R4; SQL literals
        naming field_events, cloud_extractions or cloud_evidence_spans; tests coupled to
        store_result, to the writer and to each consumer marked CHANGES, with the fixtures
        they use.
    2g. §5 The probe (FAKE, fake_probe_u40a.py → .txt, on fake_probe_u28a.py's scratch
        setup): (i) the entry's test at HEAD — twelve papers through store_result with one
        identical categorical value, then the gate as base.py calls it: the summary quoted
        (E2); (ii) the same twelve values handed to write_extraction_events for the
        registered cloud arm in the form Q1 found, with the run's own run_id: accepted or
        refused, the row or the error quoted; if accepted, the gate's summary after; (iii)
        each consumer of 2c that runs on a scratch database without a model or provider
        call, driven once with the cloud arm holding those claims, its output for that arm
        quoted; a consumer that cannot be driven so is left CODE ORDER and listed in §9.
        The probe's output records that no SDK client was constructed and appends the
        Ollama journal for its window.
    2h. The two items left from U28: the monitor CLI's main with a registered arm holding
        no claims, and the strict case — driven on a scratch data root if the code allows
        it without touching the live root (U23's root resolution, quoted), else left as
        read, with the reason.
    2i. §7 Options for the ruling: where the event write sits (inside store_result, the
        transaction it shares); the payload mapping of 2b; what is written for a failed
        paper; what resume reads afterwards; every file A1 touches beyond base.py, from
        2c–2e; the tests that move; the acceptance test's shape and seams; what A1 does
        not fix; whether anthropic_extractor.py:108–113 falls among its files (R795(c)).
        If the trace shows A1 changes a consumer's behaviour, say whether that change
        belongs in U40, in another row, or needs a ruling of its own. §8 findings; §9 what
        could still not be established read-only.
    2j. Commit the read-out (12af_U40A/12af_U40A.md), census_u40a.py/.txt and
        fake_probe_u40a.py/.txt: the staged set exactly those, nothing under engine/,
        tests/ or data/ (asserted); fingerprint IDENTICAL before and after the push; level;
        the ledger row. Record hashes.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 held.
  G2. Item 0 one commit that passed 1d in one chain: r797.txt at the architect's hash;
      Block 195 by the tool; the rules files untouched by content; the brief file hashed;
      pushed, level, fingerprint IDENTICAL; ledger.
  G3. Q1–Q4 each answered from quoted code before §7 is written, the audit stage first in
      Q2; every consumer marked CHANGES or DOES NOT CHANGE; the census and probe committed
      with their declared completeness; no SDK client constructed; the journal window
      empty; no file outside 12af_U40A/ in the commit; pushed, level, fingerprint
      IDENTICAL; ledger.

OUT OF SCOPE: U40's design and build; any note on B9 or C28; U29 (send/record_call; C24);
  retiring or backfilling the legacy cloud tables (R794(c)); R71; any edit under engine/,
  tests/, scripts/, analysis/, docs/architecture/ or CLAUDE.md; the tools items; the
  2026-11-11 job reads (the next open's or the closeout's); /wrap.

CONSTRAINTS (task-only): no model call, no provider call, no API key read, no restart, no
  live write, no new query on live. A count arrived at by eye is INFERRED; a computed value
  is reported with its command. Where this brief describes a tool, a record or a line of
  code, the record and the code govern and a difference is a finding (R732(a)). A consumer
  is named by its file and function throughout.

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — block195.json, the render
  comparison, r797.txt's hash, the brief file's sha256, bytes and lines, the check verbatim,
  chain, hashes, numstat, the plan's new sha256, bytes and lines, fingerprint, ledger; (2)
  the answers to Q1–Q4 in plain sentences first, then the read-out's §0–§9 in full as
  committed, the consumer table and the probe's outputs quoted; (3) contradictions and
  findings, numbered, each with its quoted line; (4) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 false, a 1a mismatch, or a check failure or
  crash at 1d = STOP before the item-0 commit. The writer refusing the probe's cloud write
  is an answer to Q1, not a stop. An expectation mismatch (E1, E2) is reported and the
  read-out continues. A spec premise that does not survive contact with disk is a finding,
  not an obstacle.

HANDBACK: Stop and report. Do not start U40's design or build. Await the next brief or
  ruling in this session. STOP-12AF-U40A
