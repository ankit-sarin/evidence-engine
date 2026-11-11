CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R799, PI ruling 2026-11-11 UTC; not on disk)
================================================================================

R799 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r799.txt: sha256 987973b7ee2269c573592c8e2a2d9a76e5594ce197aa76aa51d1bffffd8a32d3,
            9462 bytes, 110 lines, one trailing newline.

===== BEGIN r799.txt =====
RULING [evidence-engine 12af-R799] — U40 landed and accepted; B9 and C28 close on it; the series
  gains 965.92 s; the four findings placed — notes on A15, A16 and INT-i12-73, one new row
  INT-af12-1; R798(h)'s "each needs a row" corrected; no standing rule; U29's Phase A opens
STATUS: PI ruling, 2026-11-11 UTC, session 12af, on architect recommendation, on the 12af-U40B
  final report (item 0 27fda1cbd95f73b4955ab2530affd7a813b866d5; engine commit
  53eabfd0a9d466461b54c5ae2a531b4f69cda085; evidence 0814545c6be6f0fdeb84ab89682f4b0896b28a1b).
  Transcribe at item 0 of the 12af-U29A brief below, by transcribe_ruling.py, in the block after
  R798's, not now.

ASSUMPTIONS IN THIS RULING:
  MEASURED (the 12af-U40B final report, quoted): "27fda1c (parent aa97c6a)"; "53eabfd (parent
    27fda1c)"; "0814545 (parent 53eabfd)"; ledgers "7059f48", "ba61752", "f1f1a7e"; "Each push had
    the fingerprint IDENTICAL before and after."; "r798.txt: e293ebe4…7bbe, 13,588 B, 153 lines.
    Equal to your hash."; "brief_12af_U40B.md:
    e0ce8ddf5a10085fea7295b4c608bd1561e54f4f42c510dbea75a1aaba94526e, 35,743 B, 450 lines" (the
    architect's copy: equal, cmp byte for byte against the clone); "Plan numstat +166/−2; the "−"
    lines are exactly B9 and C28."; "The plan is now ad054b49…f183, 3,091,442 B, 18,077 lines.";
    "Parent run (b0): 9 red, 1 green, as E1 expected."; "Engine diff: extraction_events.py +18/−3
    and base.py +47/−2."; "Pins: recomputed byte-identical to pin_tuples_close.json (aea19354…).
    The audit prompt_hash is unchanged."; "Selected runs: 23 files, 450 passed."; "cloud_extractions
    14, field_events 280, claim_inputs 14, paper events naming the arm 0"; "Local arm, W0 (no cloud
    stores) against W1 (with them): equal on all twelve readings."; "no consumer raised, and the β
    column is confirmed"; "Launch: 03:01:42Z. No hold: the snapshot showed uvicorn only, before and
    after."; "Wall: 965.92 s, inside 876–1069."; "3554 passed, 17 deselected, 1 xfailed, 2 warnings
    in 963.80s"; "collected 3,572 and selected 3,555"; "only-gate is the 10 new ids; only-ref is
    empty"; "0814545, 25 files"; "Class 2, P2 (R541)"; "arms row_count 3"; "live holds no
    manifest-registered arm".
  READ (CC's): findings 1–9; "Two already have rows: F2 is A15 and F6 is A16"; "This shape pin was
    not in the census; the selected run found it."; "store_result falls back to f"cloud:{arm}" for
    a CloudExtractorBase with no provider, which has no stage_cfg".
  READ (the architect's clone at 0814545): the three full hashes in STATUS; the plan's sha256
    ad054b4950af3e48e3604465979bed6b9ec3532ea32b798efdf26dcee839f183, 18,077 lines; row A15's status
    cell, "LATENT — owner F15 (rule whether the axis becomes per (paper, arm) or the cloud arms
    write field events only)"; row INT-i12-73's note of 2026-10-22, "the second-arm half is
    latent"; the engine diff 27fda1c..53eabfd whole, in it "kept_ref = getattr(self, "_text_ref",
    None)", "stage_cfg = getattr(self, "stage_cfg", None)" and "stage_name=stage_cfg.stage if
    stage_cfg is not None else f"cloud:{arm}","; extraction_events.py's planner, whose field
    events carry no stage name (the stage name is a paper-event field); gate_wall.txt "wall:
    965.920872468 s"; the package list, "P5 — exports and the human importer".
  INFERRED: none.

R799(a) — The 12af-U40B final report is accepted as printed: item 0 27fda1c (Block 196; the B9 and
  C28 notes; the brief file byte-equal to the architect's copy; the rules files untouched), the
  engine commit 53eabfd (the keyword on the writer; the reference, the chain and the write in
  base.py; the new file's ten ids; the coupled tests rewritten with their assertions kept; four
  docs lines; the inventory regenerated for a moved entry), the gate 3,554 / 1 / 17 on 3,572 /
  3,555 at 965.92 s inside 876–1069 s, green at its first run, only-gate the ten new ids, no load,
  the nineteen files; the evidence commit 0814545; ledgers 7059f48, ba61752 and f1f1a7e. Pins and
  the audit prompt_hash unchanged. I2–I5 held, each on quoted lines. The post-build probe
  confirmed the β column: fourteen cloud stores wrote 280 claims and no paper event, and every
  local-arm reading was equal with and without them. U40 is landed: P1's fix units 38, done 32
  (U01–U28, U37, U38, U39, U40). No stop, no pause.
R799(b) — The series gains 965.92 s (the U40B gate, beside uvicorn only): thirty-nine. The band in
  force stays 876–1069 s.
R799(c) — B9 and C28 are CLOSED on U40 at 12af-U29A's item 0, in C49's form, by R514, each for
  the Wave 2 findings it was reopened for (R598(b)): B9 for Wave 2 row 25, delivered by U28's D
  and B and U40's claim events; C28 for Wave 2 rows 25 and 27, delivered by U40 and by U28's C.
  The acceptance test named is tests/test_u40_cloud_claim_events.py::
  test_cloud_gate_sees_what_store_result_wrote, red on the parent in 12af_U40B/
  b0_parent_acceptance.txt and green in 12af_U40B/gate_u40b/, with the file's other cases beside
  it. The census miss is named in the note: tests/test_parsed_text_resolver.py's shape pin, class
  (c), found by the selected run. C28's max_papers incidental keeps its owner. Both rows are
  counted until P1 closes (R607(d)).
R799(d) — The four findings of R798(h), placed from the record as the report quotes it:
  F2 is row A15, and A15's status cell already states the choice — the axis per (paper, arm), or
  the cloud arms writing field events only. R798(c) ruled the second. A note on A15 says so: the
  cloud store writes field events only (U40, 53eabfd); the per-(paper, arm) axis is not built;
  T5 stays deferred; a cloud arm's export still shows the shared processing column (the report's
  finding 8); class and owner unchanged. R798(b) and (h) said this question needed its own row:
  that was the architect's, written from the read-out's findings without a search of the
  inventory; CC's step 4 found the row.
  F6 is row A16. A note: reproduced on a manifest registry (12af_U40A.md §8 F6, FAKE); class and
  package unchanged.
  F4 has no row of its own and needs none: it is the consequence of INT-i12-73's second-arm half.
  A note on INT-i12-73: since U40 a cloud arm can hold claims (behind R71), the pipeline audits
  the spec's local arm only, so those claims are never located and the judge loader has no span
  for them (12af_U40A.md §8 F4); class and package unchanged.
  F5 is a new row, INT-af12-1, in section L in INT-ae12-2's form, its text cell this sentence
  unchanged: "`export_disagreement_pairs.arms_in_scope` refuses any registry holding a model arm
  outside `ARM_VALUE_COLUMN`'s three pre-manifest names, the spec's own local arm included
  (`analysis/paper1/export_disagreement_pairs.py:38–42`, `:70–77`). It does not fire on live
  today, whose registry holds the three pre-manifest arms only; it fires from the first manifest
  run that registers an arm, Run 7 included. The refusal is loud and no stored value is wrong."
  Class 2 (a path that refuses loudly, corrupting nothing); package P5 (exports and the human
  importer); governing ruling R799(d); evidence 12af_U40A.md §8 F5 and its FAKE (iii-b), and
  12ae_close/dbchecks_close.txt:2 for the live registry. Rows in 1, rows out 0.
R799(e) — The dated units line at the same item 0, after the 12ae-CLOSE line and in its form:
  U40 landed at 53eabfd; B9 and C28 CLOSED on it and counted until P1 closes; P1's fix units 38,
  done 32; the row count measured on the plan as that item 0 leaves it, by the recount's counting
  method, the method named in the line (EXPECTED 287, classes 94 / 57 / 136, 0 unclassed — an
  expectation; the measured figures are the line's); rows in 1 (INT-af12-1), rows out 0.
R799(f) — Readings, no row: finding 2 (a census miss, class (c), as R799(c) names it); finding 3
  (two censuses counted different things, and CC ran every file grep finds); finding 4
  (pipeline.md:251's "separate tables" is now incomplete — brought to the fact with the docs
  lines of U29's engine commit, the next commit in the cloud path); finding 5; finding 6 (a
  ValueError for the refused PaperFailure stands. The stage-name fallback and the three getattr
  guards in store_result and send exist for a cloud instance built without a provider or
  without its constructor; the stage name is not written when no paper event is, so no stored
  value depends on the fallback. They are test-shaped code in an engine file: U29's Phase A says
  which tests build such an instance, and U29's build removes the guards and the fallback if
  those tests can be given a real instance, else they join P7); finding 7; finding 8 (A15);
  finding 9. The item-0 check failed its dry run on CC's own slicing and passed after the fix:
  the check did its job. The architect's errors this session so far: three (R797(e)'s two, and
  R798(h)); none stopped a brief.
R799(g) — U29's Phase A opens: "A cloud call that raises still records its call" (C24 for Wave 2
  row 28; N47 with it), read-only, in 12af_U40A.md's form, its entry quoted first. Since U40 a
  send that raises leaves its hash in the paper's chain and still no run_calls row. No engine,
  test, script, spec or codebook edit; no model call; no provider call; no live write. This
  ruling states no standing rule: the id list, the glosses file and STANDING_RULES.md are
  untouched at item 0, asserted by content.
===== END r799.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12af-U29A] — U29's Phase A read-out ("A cloud call that raises still
  records its call"; C24 for Wave 2 row 28; N47). Item 0: R799 as the next block; B9 and
  C28 CLOSED; notes on A15, A16 and INT-i12-73; the new row INT-af12-1; the dated units
  line; this brief committed as pasted; no standing rule. Read-only on engine/ and tests/;
  STOP before any edit.
PURPOSE: Put on disk, before the design, what the cloud path records about a provider call
  and when: what a raised, truncated or refused call leaves in run_calls, what a response
  digest would be made of, and whether one recorded call can be several HTTP sends. Mixed:
  one docs commit (item 0), one docs commit (the read-out). No engine, test, script, spec
  or codebook change. No live write. No model call. No provider call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 197; five rows' notes; one new row in
  section L; one dated line); docs/session-reports/session-12/12af_open/r799.txt; a new
  directory docs/session-reports/session-12/12af_U29A/ (brief_12af_U29A.md at item 0; the
  read-out, census and probe with outputs at step 2); ~/scratch/12af-u29a/; the
  claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/ (R71), the codebook, docs/architecture/, CLAUDE.md, docs/inventory/, the
  tools; the id list, the glosses file and STANDING_RULES.md (not one byte); review.db
  (mode=ro instruments only; no new query on live); any .env or API key (never read; the
  probe constructs no SDK client and sends nothing); the installed SDK packages (read,
  never edited or upgraded); any scratch directory other than ~/scratch/12af-u29a/; the
  working tree inside 07:00–10:35 UTC (R86).

TIMING: outside 07:00–10:35 UTC. If the clock reaches 06:45 UTC before a commit, hold with
  the work on disk uncommitted and resume in this brief after 10:35 UTC (R789(a)); say so.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12af-U40B report; R799): HEAD 0814545c6be6f0fdeb84ab89682f4b0896b28a1b, parent
    53eabfd0a9d466461b54c5ae2a531b4f69cda085, clean, level; plan sha256
    ad054b4950af3e48e3604465979bed6b9ec3532ea32b798efdf26dcee839f183, 18,077 lines, ends
    "…ends at R798.", last block 196; claude-config at f1f1a7e or an accepted descendant
    (R663(b)); the three rules files as 12af-OPEN's 0c read them.
  READ (the architect's clone at 0814545; re-read on the box, not trusted):
    R1. U29's entry, 12l_P1A/12l_P1A.md:431–438: rows C24; pin effect not affecting; files
        "`engine/cloud/base.py`, `engine/cloud/openai_extractor.py`,
        `engine/cloud/anthropic_extractor.py`"; acceptance test
        "`test_a_cloud_send_that_raises_still_records_its_call` — Inside an open run a
        transport that raises leaves one `run_calls` row with the request hash and an error
        outcome; a truncated response is not recorded completed; a returned response has a
        response digest. Fails on the unmodified tree."
    R2. Row C24 (plan :255), CLOSED 2026-09-29 for the local path and reopened for Wave 2
        row 28: "`send()` records a call only after it returns, always as `completed`, with
        no response digest; a request that raises leaves no `run_calls` row". Its closing
        text names "`run_calls.outcome` NOT NULL CHECK, six tokens".
    R3. 12l_P1A.md:3396, N47: "the pinned SDK clients are constructed with default retry
        settings … so one _create may be several HTTP sends recorded as one call … the SDK
        default was not re-read here".
    R4. engine/cloud/base.py at 53eabfd: send computes "self.last_request_hash =
        sha256_canonical(payload)", appends it to the paper's chain (U40), calls
        "response = self._create(**payload)", and only then, "if self.run_id is not None:",
        calls rm.record_call. Two different functions are named record_call:
        run_manifest's (run_calls) and extraction_telemetry's (the JSONL row).
    R5. 12ae_U27A/12ae_U27A.md is the cloud path's handler read-out (the retry loops, the
        recorder's sqlite fault, INT-ae12-2); 12af_U40A/12af_U40A.md is the form (the
        blocking answer first, then §0–§9); census_u40a.py and fake_probe_u40a.py are the
        instruments' forms.
    R6. A15's status cell, "LATENT — owner F15 (rule whether the axis becomes per (paper,
        arm) or the cloud arms write field events only)"; C49's closing note begins
        "**CLOSED** on U24 (`<full hash>` …), by R514:" and ends "Counted until P1 closes
        (R607(d), R753(d)(ii))."; the dated units lines stand at plan :918–:930, the last
        "*Dated 2026-11-09 (12ae-CLOSE item 0, R795):*"; INT-ae12-2's row has six cells
        (id, text, class, package, governing ruling, evidence).
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 53eabfd (git diff --stat
    53eabfd HEAD on engine tests scripts analysis review_specs empty).
  INFERRED — expectation (report a mismatch, continue):
    E1. At HEAD, inside an open run: a _create that raises leaves 0 run_calls rows and one
        hash in the paper's chain; a returned response is recorded "completed" with a NULL
        response digest whatever its finish reason.
    E2. The row count after item 0 is 287 (94 / 57 / 136, 0 unclassed).
    E3. Nothing is predicted about Q2–Q5 below: the architect has not read the SDK
        sources, run_manifest.record_call or the outcome CHECK.

CONTRACT (steps, in order; evidence under ~/scratch/12af-u29a/):

  STEP 0 — Pre-check (read-only). date -u. HEAD and parent proved by git cat-file -e
    <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. Plan: sha256, lines, last
    line, last block 196, no Block 197. Rules files: the three sha256s. claude-config:
    f1f1a7e or an accepted descendant, level; a pending foreign row carried and named
    (R654(d)). I1. Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; models: 12ae-U25A's item
    0 for a CLOSED note and a dated line, 12ae-U27B's for a row opened in section L; each
    model's lines quoted before composing; ~/scratch/12af-u40b/check_item0_u40b.py and
    block196.json quoted; the check written for this brief, R794(i)).
    1a. docs/session-reports/session-12/12af_open/r799.txt from Section 1, one write,
        hashed straight after (R738(b)): EXPECTED 987973b7ee2269c573592c8e2a2d9a76e5594ce197aa76aa51d1bffffd8a32d3,
        9462, 110. Mismatch = STOP (R739(b)).
    1b. block197.json in block196.json's key set: tag R799; transcribed = the commit's UTC
        date (R775(a)); item "item 0 of the 12af-U29A brief"; beside = "the B9 and C28
        closing notes, the notes on A15, A16 and INT-i12-73, the row INT-af12-1, the dated
        units line and the brief file it orders" (composed; quote it); made_by PI;
        made_date 2026-11-11; session 12af; report = the 12af-U40B final report with its
        item-0, engine and evidence hashes, in the form of the block that transcribed a
        ruling on a build's final report (Block 193's, R795 on 12ae-U28B — quote that
        header first); sentence model, followed "12af-U29A brief"; prev_ends R798. Render
        to stdout, compare the element order with that block's, then --append.
    1c. The rows, each note appended at the end of its row's status cell in that row's
        note form, quoted before composing, tagged "*Note <the commit's UTC date>
        (12af-U29A item 0, R799(<clause>)):*":
        (i) B9 and C28, R799(c): **CLOSED** on U40 in C49's form (R6) — the full engine
        hash; by R514; what was delivered, per row, as R799(c) states it; the acceptance
        test with its red and green evidence paths; the census miss; C28's max_papers
        incidental keeping its owner; counted until P1 closes.
        (ii) A15, A16, INT-i12-73, R799(d): the facts R799(d) gives for each, in CC's
        words, nothing added; class, package and owner unchanged, said so.
        (iii) INT-af12-1, R799(d): a row in section L in INT-ae12-2's six-cell form, its
        text cell R799(d)'s quoted sentence unchanged (its line wrap joined), class 2,
        package P5, governing ruling R799(d), the evidence cell as R799(d) names it; the
        section's lead amended only if the record's convention for a new session's rows
        requires it (the 12ae precedent, R793's account, quoted).
        (iv) The dated units line, R799(e), after the 12ae-CLOSE line: the row count and
        classes MEASURED on the plan as this item 0 leaves it, the method named in the
        line (the recount script's counting, or the six-cell method with the class
        column, whichever the record's last measured line used — quoted); E2.
        Five rows replaced, one row and one line inserted; the "−" count read and matched
        to the replaced rows (plus the lead if amended).
    1d. The brief file (R784(g)(v)): 12af_U29A/brief_12af_U29A.md from this whole pasted
        block, "CLAUDE CODE" through the STOP-12AF-U29A line, one trailing newline, written
        once; sha256, bytes and lines straight after the write, reported.
    1e. The item-0 check: --check-plan byte-identical; the plan's numstat read; every
        earlier "ends at" line kept; the last line "…ends at R799." once; the plan equals
        HEAD's plus the rows replaced, the row and the line inserted and Block 197
        appended, nothing else (computed); each replaced row's last note begins with its
        1c tag; INT-af12-1's text cell equal to R799(d)'s sentence, computed from r799.txt;
        the six-cell INT- row count 212 before and 213 after; r799.txt equal to 1a; the
        three rules files unchanged and unstaged; the brief file hashed; block197.json's
        transcribed value equal to the commit-time UTC date; the staged set exactly the
        plan, r799.txt and the brief file. Evidence by a redirect inside the check; quote
        the check.
    1f. <check> && <commit>, one line; the message from the staged set (R778(d)), docs
        scope. Fingerprint --compare before the push; push; level; --compare after
        (IDENTICAL). The ledger row with trailers; push; level. Record hashes and parents.

  STEP 2 — The read-out (read-only; no server; no provider; no live database). Every code
    claim beside a quoted line with its path and line at HEAD, labelled CODE ORDER or FAKE.
    2a. §1 Inputs quoted: U29's entry (R1); row C24 whole with every dated note; Wave 2
        row 28 (12j_review_w2.md) and its reader file's passage; N47 (R3); R216, R745(d)
        and R768(c)'s blocks as far as they speak of run_calls' outcome tokens and counts;
        12ae_U27A.md's rows on the recorder; INT-ae12-2.
    2b. Q1 — WHAT IS RECORDED TODAY. send whole at HEAD; run_manifest.record_call's
        signature, every column it writes and the outcome CHECK's tokens as the schema
        states them; what the local path's recorder (ollama_chat's _record / _finish)
        writes for a raised, a refused and a truncated call — the model C24's closing
        note names; the cloud path against it, case by case: _create raises before
        transmission, raises after, returns a truncated response, returns a refusal,
        returns complete.
    2c. Q2 — TRUNCATED AND REFUSED. Where each transport learns a response's finish
        reason or refusal (extract_paper and what it returns), relative to send's return;
        whether send can know it, or only its caller; which existing outcome token fits
        each case, quoted from the CHECK — and if none fits, say so plainly: a new token
        is a schema change and 023's (R745(d)), not U29's.
    2d. Q3 — THE RESPONSE DIGEST. What the local path hashes as response_digest and with
        which function; what a cloud response object offers to hash deterministically for
        each pinned SDK (the method, quoted from the installed source); what would vary
        between two identical answers (ids, timestamps, usage); the options.
    2e. Q4 — N47. For each pinned SDK, read from the installed package: the client's
        default max_retries and timeout, what it retries on, and where the two clients are
        constructed in this repository with what arguments. State whether one _create can
        be several HTTP sends, and what each option costs: retries off at construction so
        one recorded call is one send, or the count recorded. Quote versions from the
        environment, not from memory.
    2f. Q5 — WHERE THE RECORD GOES. If the row is written when _create raises: which
        exception classes arrive at send, what outcome each would carry, what happens when
        the recorder itself raises while an exception is in flight (U27's handlers and the
        run-fault tuple), and what U40's chain holds for a raised send. What the two
        transports must change, if anything, or whether base.py alone suffices.
    2g. Q6 — THE GUARDS (R799(f)). Every test that builds a cloud instance without a
        provider or without its constructor, by file and fixture (census); for each of the
        three getattr guards and the stage-name fallback in base.py, which tests reach it;
        whether those tests can be given a real instance, and what that moves.
    2h. §4 Census: census_u29a.py in census_u40a.py's form (AST; declared completeness
        stated): calls of rm.record_call and of the telemetry record_call; reads and
        writes of run_calls; constructions of the SDK clients; tests coupled to send, to
        record_call on the cloud path and to run_calls, with their fixtures.
    2i. §5 The probe (FAKE, fake_probe_u29a.py → .txt, on fake_probe_u40a.py's scratch
        setup, an open cloud run, _create replaced at the instance's seam): the entry's
        test at HEAD — a _create that raises (E1: run_calls rows, the chain); a returned
        response with a truncated finish reason, and one with a refusal, each recorded how;
        a complete response's response_digest. The output records that no SDK client was
        constructed and appends the Ollama journal for its window.
    2j. §7 Options for the ruling: where the record is written; the outcome per case with
        the token it uses; the digest's content; N47's choice; every file touched; the
        tests that move; the acceptance test's shape and seams; what U29 does not fix and
        whose it is; pipeline.md:251 quoted for the docs lines (R799(f)); Q6's answer.
        §8 findings; §9 what could not be established read-only.
    2k. Commit the read-out (12af_U29A/12af_U29A.md), census_u29a.py/.txt and
        fake_probe_u29a.py/.txt: the staged set exactly those, nothing under engine/,
        tests/ or data/ (asserted); fingerprint IDENTICAL before and after the push; level;
        the ledger row. Record hashes.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 held.
  G2. Item 0 one commit that passed 1e in one chain: r799.txt at the architect's hash;
      Block 197 by the tool; the five notes, the new row and the dated line in the record's
      forms with the row count measured; the rules files untouched by content; the brief
      file hashed; pushed, level, fingerprint IDENTICAL; ledger.
  G3. Q1–Q6 each answered from quoted code or installed source before §7 is written; the
      census and probe committed with their declared completeness; no SDK client
      constructed; the journal window empty; no file outside 12af_U29A/ in the commit;
      pushed, level, fingerprint IDENTICAL; ledger.

OUT OF SCOPE: U29's design and build; any schema change (023's); INT-ae12-2's retry loops;
  U30 and later units; A15's per-arm axis; any edit under engine/, tests/, scripts/,
  analysis/, docs/architecture/ or CLAUDE.md; upgrading or pinning an SDK; the tools items;
  the 2026-11-11 job reads (the closeout's); /wrap.

CONSTRAINTS (task-only): no model call, no provider call, no API key read, no network
  request, no restart, no live write, no new query on live. A count arrived at by eye is
  INFERRED; a computed value is reported with its command. Where this brief describes a
  tool, a record or a line of code, the record and the code govern and a difference is a
  finding (R732(a)). An SDK's behaviour is stated from its installed source with the file
  and line, never from its documentation or from memory.

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — block197.json, the render
  comparison, each of the five notes, the new row, the dated line with its measured count
  and method, r799.txt's hash, the brief file's sha256, bytes and lines, the check
  verbatim, chain, hashes, numstat, the plan's new sha256, bytes and lines, fingerprint,
  ledger; (2) the answers to Q1–Q6 in plain sentences first, then the read-out's §0–§9 in
  full as committed, the probe's outputs quoted; (3) contradictions and findings, numbered,
  each with its quoted line; (4) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 false, a 1a mismatch, a row or model line not
  found as R6 quotes it, or a check failure or crash at 1e = STOP before the item-0 commit.
  An expectation mismatch (E1, E2) is reported and the work continues; a measured row count
  other than 287 is written as measured and reported, not adapted to. A spec premise that
  does not survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not start U29's design or build. Await the next brief or
  ruling in this session. STOP-12AF-U29A
