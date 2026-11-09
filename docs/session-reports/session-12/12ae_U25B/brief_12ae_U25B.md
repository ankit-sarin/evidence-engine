CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R788, PI ruling 2026-11-09 UTC; not on disk)
================================================================================

R788 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash:
  r788.txt: sha256 980e53724af96c7ae5784a76e55a768d6ffe1aa3a93088bdf94e2c28ae93ec58,
            7685 bytes, 88 lines, one trailing newline.

===== BEGIN r788.txt =====
RULING [evidence-engine 12ae-R788] — 12ae-U25A accepted; the committed brief equal to the
  architect's copy; U25's design: option A written as "not a str", over the valid_values values,
  every absence sentinel and the rendered string fields (option C's reach, the row widened by
  note); B not taken; readings, two the architect's; the build opens as one brief, held across
  the no-live-write window
STATUS: PI ruling, 2026-11-09 UTC, session 12ae, on architect recommendation, on the 12ae-U25A
  final report (item 0 d488573; read-out b599667, docs/session-reports/session-12/12ae_U25A/
  12ae_U25A.md, sha256 ed37d9a79195dded643dc930f42b524cd19ce149538b1155f98874029c446bc5, 263
  lines). Transcribe at item 0 of the 12ae-U25B brief below, by transcribe_ruling.py, in the
  block after R787's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the read-out and the report):
  MEASURED: "12ae_U25A/brief_12ae_U25A.md is d8d689aade66b083519b8886e030d814baa922b75f0cc19a2d1b772ee11ee715,
    28,842 B, 372 lines" (the architect's copy: equal, cmp byte for byte); "item 0 landed
    06:14:06"; "Plan +143 / −1, the one "−" line being C49's row"; "71 valid_values entries, all
    str; 6 sentinels, all str and quoted in the text; the tokens and version all str; 0 unquoted
    value: lines out of 92"; "The live file loads, with semantic hash f261af76…5646 equal to
    12ad_close/pin_tuples_close.json"; "The Ollama journal for the window was empty"; "the live
    codebook was never written (sha256 89dbfa91…ad82 throughout)".
  READ (the read-out, §2–§8): the readers table — "raises TypeError: sequence item 0: expected
    str instance, bool found at :90 (FAKE)" (extractor._build_field_block, both prompt paths and
    open_run's prompt_hash render); "silent: the tuple holds 'False' / 'None';
    is_absence_sentinel('NO') is False and ('False') True" (FAKE); "hashes JSON true, not "Yes":
    the hash differs from the quoted text's (FAKE)"; §3 point 1, PyYAML 6.0.3's resolver quoted
    ("^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$"; "^(?: ~
    |null|Null|NULL | )$"; "Bare y / n are not in the bool pattern"; "int, float and timestamp"
    also resolve); §3 point 2, where a check can sit ("Between :448's yaml.safe_load and :460's
    model build. _validate_valid_values already walks every entry (:350 for item in vv:), and
    _validate_top_level already holds doc["absence_sentinels"] (:262)"); §3 point 4, "Option A's
    check, run over the live document, reports no finding"; F3 ("option A's check written as "not
    a str" covers both"); F4 ("The canonical sentinel is already guarded … :269"); F5 ("an
    unquoted No there becomes the word False in the prompt. escape_token: No raises (falsy) but
    escape_token: Yes becomes "True""); §4 "E2 holds. No test fixture carries an unquoted bool or
    null … A refusal moves no existing test"; §7 B: "the entry's test as written ("each raises at
    load") does not hold under B".
  INFERRED: none.

R788(a) — The 12ae-U25A final report is accepted as printed: item 0 d488573 (Blocks 184–185 by
  transcribe_ruling.py in order; C49 CLOSED in INT-g12-6's form; the dated units line, 37 done
  27; the red run's nineteen files; brief_12ae_U25A.md), the read-out b599667 with its census,
  probe, live census and three fixtures, ledgers 6dc6cc8 and 4e9d6eb with trailers. No stop. The
  brief file is byte-equal to the architect's copy: the fourth paste-fidelity match of four.
R788(b) — U25's design: option A, the check written as "is not a str", placed in the loader's
  existing validators, refusing with a CodebookError that names the path, the field (its `where`)
  and the key, quotes the offending scalar with its Python type, and says to quote it in the
  YAML. Its scope is every scalar the loader exposes as a string: (i) each `valid_values` entry's
  `value` and `definition` (in _validate_valid_values's loop); (ii) each member of
  `absence_sentinels` (in _validate_top_level, beside the canonical-sentinel check that already
  holds, F4); (iii) the top-level tokens `escape_token`, `contract_unmet_token`, `review`,
  `version` and `date`; (iv) each field's `definition`, `instruction` and `decision_criteria`
  where present (in _validate_fields). (iii) and (iv) are option C's reach, taken because the
  silent half of the row — a sentinel coerced to 'False' — has the same shape in a prompt field
  coerced to the word False (F5), the check is one predicate in three loops the loader already
  runs, and the live codebook's scalars are all str (the live census; the acceptance test's live
  load is the proof). The row INT-i12-31 is widened by a dated note to name (iii) and (iv), as
  R491 widened C49. The `str()` coercions at :460–473 stay: after the check they meet only
  strings.
R788(c) — Not taken: B (a loader without the bool and null resolvers). It accepts a scalar the
  author did not quote and silently makes it the word written; numbers and timestamps would still
  resolve; the entry's acceptance test as written would not hold; and the codebook would parse
  under a YAML no other loader on the box uses. A loud refusal that tells the author to quote is
  the entry's form and the engine's (fail-fast, R-class of C41/C52).
R788(d) — Fail-first and callers: tests/test_u25_codebook_scalars.py::
  test_unquoted_yaml_booleans_and_nulls_are_refused_at_load, parametrized over the three committed
  fixtures (12ae_U25A/fixtures/value_yes, sentinel_no, sentinel_null — read from the repository,
  not re-authored) plus two the build adds in the same form (an unquoted `1` as a value, F3; an
  unquoted `No` as a field's `instruction`, (iv)): each raises CodebookError at load_codebook,
  the message naming the key and containing the word "quote"; and ::test_the_live_codebook_loads_unchanged:
  load_codebook on data/surgical_autonomy/extraction_codebook.yaml succeeds, its sha256
  89dbfa91…ad82 and its semantic_hash f261af76…5646 (the pin record's codebook_semantic_hash).
  On the parent the five refusal cases are red (nothing raises at load — the value cases raise
  later or not at all; the sentinel cases load silently) and the live case is green by
  construction (a preservation pin, exempt from fail-first); recorded in
  b0_parent_acceptance.txt; all green in the gate. No existing test moves (E2, §4).
R788(e) — Readings, no row: F1 (codebook.py:7's "eleven bare yaml.safe_load calls" is the
  pre-CODEBOOK-AUTH-01 tree, three today; the module docstring is history by its own tense and is
  not edited in U25; 12l_P1A.md §1's figure is a dated reading); F2 (the architect cited R124/R131
  for the semantic hash; they govern sentinel authority, and the hash is compute_semantic_hash
  with codebook_semantic_hash in the pin record — the architect's, corrected here); F3, F4 and F5
  are taken into (b); F6 and F7 (E1–E3 held). The two other yaml.safe_load sites
  (review_spec.py:987, categorizer.py:46) are outside U25 and outside the row; the spec's own
  validation is pydantic's (a reading, no row).
R788(f) — The build opens as one brief, 12ae-U25B (R785(h)'s form): the read-out quoted every
  loader site, every reader and every coupled fixture at HEAD, and the change is three validator
  loops in one module. The no-live-write window 07:00–10:35 UTC governs the gate and, because the
  09:00 nightly runs the suite in the checkout, every working-tree edit: the brief's item 0 lands
  before 06:55 UTC or waits; the build itself starts after 10:35 UTC under the same brief
  (R782(i)'s precedent). No model call, no live write. This ruling states no standing rule.
===== END r788.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ae-U25B] — U25's build: the "not a str" check in the codebook
  loader's three validators (R788(b)), the acceptance tests red on the parent, one
  engine commit, the standard gate, the evidence commit. Item 0: R788 as the next
  block; INT-i12-31 widened by note; this brief committed as pasted; no standing rule.
  TIMING below governs: item 0 now if before 06:55 UTC, the build after 10:35 UTC.
PURPOSE: Land U25 (INT-i12-31) as R788(b)–(d) rule it. Code task: fail-first acceptance
  tests, then the edits, one engine commit with its gate, evidence in its own commit.
  No live write. No model call. The live codebook is read, hashed, never written.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  Touched: engine/core/codebook.py (the three validators); tests/test_u25_codebook_scalars.py
  (new); docs/session-reports/session-12/12ae_U25A/fixtures/ (two fixtures added, in the
  three existing ones' form; the existing three unchanged); docs/plan/ENGINE_REFACTOR_PLAN.md
  (Block 186; INT-i12-31's dated note); docs/session-reports/session-12/12ae_open/r788.txt;
  a new directory docs/session-reports/session-12/12ae_U25B/ (brief_12ae_U25B.md at item 0;
  b0_parent_acceptance.txt, gate_u25b/ and the pin recompute at step 3);
  ~/scratch/12ae-u25b/; the claude-config ledger; git push on both repos. docs/inventory/
  is regenerated by inventory --write only if --check reports drift after the edits
  (R786(b)'s form: the drift quoted first; no entry point changes here, so EXPECTED in
  sync and nothing written).
  STOP / DO NOT TOUCH (beyond the global list): any other file under engine/, tests/,
  scripts/, analysis/; review_specs/; the live codebook
  (data/surgical_autonomy/extraction_codebook.yaml — read and hashed only); the tools;
  the id list, the glosses file and STANDING_RULES.md (not one byte — R788 states no
  rule); review.db (mode=ro instruments only); the working tree inside 07:00–10:35 UTC
  (no edit, no uncommitted file: the 09:00 nightly runs the suite in this checkout); any
  scratch directory other than ~/scratch/12ae-u25b/.

TIMING: The architect's clock read 06:45 UTC on 2026-11-09 at issue. If step 1's commit
  chain can land before 06:55 UTC, run steps 0–1 now, then STOP and report "item 0
  landed; the build waits for 10:35 UTC", and resume at step 2 after 10:35 UTC under
  this brief when told to. If it cannot, STOP after step 0 and report; the whole brief
  then runs after 10:35 UTC. Nothing is edited in the working tree between 07:00 and
  10:35 UTC. The gate must launch before 06:30 UTC on the day it runs or after 10:35.
  The 2026-11-09 health job and nightly are read at the next open, not here.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ae-U25A report; R788): HEAD b599667, parent d488573, clean, level; the engine
    tree equal to 1f6bc16's; plan ends "…ends at R787.", last block 185; STANDING_RULES.md
    87 / 8 / 95, glosses 43, the id list's last A line "A R784(g)"; claude-config at
    4e9d6eb or an accepted descendant (R663(b)); the live codebook 89dbfa91…ad82, its
    semantic hash f261af76…5646; gate reference U24's (12ae_U24B/gate_u24b/: 3,495 passed
    / 1 xfailed / 17 deselected on 3,513 / 3,496; collected_ids.txt and the durations file
    there); band 837–1021 s (R783(b)); pins 12ae_U24B/pin_tuples_u24b.json (byte-equal to
    12ad_close/pin_tuples_close.json). Architect's hash of r788.txt: Section 1.
  READ (12ae_U25A/12ae_U25A.md at b599667; §2 and §3 quote the validators, the readers,
    the resolver and the fixtures — not repeated; step 2a re-reads each at HEAD).
  INFERRED — BLOCKING: I1. The engine tree is unchanged since 1f6bc16. I2. codebook.py's
    _validate_valid_values, _validate_top_level and _validate_fields read at HEAD as the
    read-out quotes them (:335–371, :258–276, :281–332).
  INFERRED — expectation: E1. The five refusal cases are red on the parent as R788(d)
    states (the three committed fixtures and the two new ones); the live case is green on
    the parent. E2. The new ids are six (five parametrized refusals, one live); the gate
    collects 3,513 + 6 and selects 3,496 + 6, only-gate exactly those; no other test
    moves (§4 of the read-out). E3. inventory --check stays in sync (no entry point
    changes).

CONTRACT (steps, in order; evidence under ~/scratch/12ae-u25b/):

  STEP 0 — Pre-check (read-only). date -u against TIMING. HEAD and parent proved by git
    cat-file -e <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. Plan: last
    line "…ends at R787.", last block 185, no Block 186. Rules files: the three counts
    above. claude-config: 4e9d6eb or an accepted descendant, clean, level. I1 (empty
    diff since 1f6bc16). Fingerprint --compare IDENTICAL. The live codebook's sha256
    EXPECTED 89dbfa91…ad82. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12ae-U25A's item
    0, ~/scratch/12ae-u25a/check_item0_u25a.py and its chain, quoted before composing).
    1a. 12ae_open/r788.txt from Section 1; hash straight after the write: EXPECTED
        980e53724af96c7ae5784a76e55a768d6ffe1aa3a93088bdf94e2c28ae93ec58, 7685, 88.
        Mismatch = STOP.
    1b. block186.json in block185.json's key set: tag R788; item "item 0 of the
        12ae-U25B brief"; beside = "the INT-i12-31 note and the brief file it orders"
        (composed; quoted); made_date 2026-11-09; session 12ae; report = the 12ae-U25A
        final report (item 0 d488573; read-out b599667), final-report form; sentence
        model, followed "12ae-U25B brief"; prev_ends R787. Render to stdout, compare with
        Block 185's order, then --append ("−" EXPECTED 0 for the append, read at 1e).
    1c. INT-i12-31's dated note (R788(b)): inside its last cell (its Evidence cell, the
        section L form, R783(e) finding 3), "*Note 2026-11-09 (12ae-U25B item 0,
        R788(b)):*" in CC's words from R788(b): the check's scope (i)–(iv), (iii) and (iv)
        option C's reach taken, F3's numbers covered by "not a str", F4's canonical
        sentinel already guarded. One row replaced; the "−" count read and matched.
    1d. The brief file (R784(g)(v)): 12ae_U25B/brief_12ae_U25B.md from this whole pasted
        block, "CLAUDE CODE" through the last HANDBACK line, one trailing newline, written
        once; sha256, bytes, lines straight after the write, reported.
    1e. The item-0 check in 12ae-U25A's content form, adapted: --check-plan
        byte-identical; the plan's numstat read ("−" matched to the one row); every
        earlier "ends at" line kept; last line "…ends at R788." once; the plan equals
        HEAD's plus the row replaced and Block 186 appended, nothing else (computed);
        r788.txt equal to 1a; the three rules files unchanged and unstaged; the brief file
        hashed; the staged set exactly the plan, r788.txt and the brief file. Evidence by
        redirect; quote the check.
    1f. <check> && <commit>, one line; message from the staged set (R778(d)), docs scope.
        Fingerprint --compare before the push; push; level; --compare after (IDENTICAL).
        Ledger row with trailers; push; level. Record hashes and parents. Then TIMING: if
        the clock is past 06:55 UTC or the build cannot finish its gate before 06:30 UTC,
        STOP here and report; resume at step 2 after 10:35 UTC when told to.

  STEP 2 — The build (one engine commit).
    2a. Re-read at HEAD and quote the three validators whole (I2), the model build at
        :460–473, and the three committed fixtures (12ae_U25A/fixtures/value_yes,
        sentinel_no, sentinel_null: their form and how fake_probe_u25a.py derived them
        from the live text). Any difference from the read-out = STOP.
    2b. Write the two new fixtures into 12ae_U25A/fixtures/ in the three's form, derived
        the same way: value_one (an unquoted 1 as a valid_values value, F3) and
        instruction_no (an unquoted No as one field's instruction, R788(b)(iv)). Hash all
        five.
    2c. Write tests/test_u25_codebook_scalars.py, fail-first (R788(d)): (i)
        test_unquoted_yaml_booleans_and_nulls_are_refused_at_load, parametrized over the
        five fixture files read from the repository path (the test resolves the
        fixtures directory relative to REPO_ROOT, R780(c)'s object; it writes nothing
        under data/); each: pytest.raises(CodebookError), the message containing the
        fixture's offending key and the word "quote"; (ii) test_the_live_codebook_loads_unchanged:
        load_codebook on the live path succeeds; .sha256 == "89dbfa91…ad82" (full, from
        the pin record or 12ad_close/codebook_spec_sha256.txt, quoted); .semantic_hash ==
        the pin record's codebook_semantic_hash (read from 12ae_U24B/pin_tuples_u24b.json,
        quoted). Run on the parent: EXPECTED (i) red on all five (no CodebookError), (ii)
        green; output to b0_parent_acceptance.txt. A refusal case green on the parent =
        STOP (the gap is not where the read-out said).
    2d. Edit engine/core/codebook.py, R788(b) exactly: one predicate ("is not a str")
        applied in _validate_valid_values's loop to item["value"] and, where present,
        item["definition"]; in _validate_top_level to each member of
        doc["absence_sentinels"] (beside :268–269) and to escape_token,
        contract_unmet_token, review, version and date; in _validate_fields to each
        field's definition, instruction and decision_criteria where present. Each refusal
        a CodebookError naming the path, the `where`, the key, the scalar with its type
        (f"{v!r} ({type(v).__name__})") and the sentence that YAML 1.1 resolves it
        implicitly and the author must quote it. No other line changes; the :460–473
        coercions stay. git diff --stat EXPECTED: codebook.py, the new test, the two new
        fixtures — four files; inventory --check EXPECTED in sync (E3; drift = quote it
        and apply R786(b)'s form).
    2e. Pins: 12l_U04/pin_tuples.py with both arguments, byte-identical to
        12ae_U24B/pin_tuples_u24b.json; the audit prompt_hash unchanged; the live
        codebook's sha256 and semantic hash unchanged (the test of 2c(ii) now runs on the
        edited loader). A moved value = STOP.
    2f. Selected runs green: the new file; tests/test_codebook_loader.py whole; any test
        file the read-out's §4 census lists as writing codebook YAML text. Commit the
        engine change (fix scope; the message names INT-i12-31/U25 and the five cases),
        from the staged set. Do not push before the gate.
    2g. The standard gate by launch_gate.sh: evidence_dir ~/scratch/12ae-u25b/gate_u25b/,
        session name, ref_ids 12ae_U24B/gate_u24b/collected_ids.txt, ref_durations the
        durations file in 12ae_U24B/gate_u24b/ (its name quoted), no selection. Ollama
        reads ≥5 s before and after (R660(b)). EXPECTED: 3,501 passed / 1 xfailed / 17
        deselected on 3,519 / 3,502 (E2; the counts read, the arithmetic stated),
        only-gate the six new ids, only-ref 0; wall inside 837–1021 s; no phase >10 s
        slower; no model load in the window (R696(b)); the nineteen files. A red: class (c)
        per R746(c)/R772(b), re-run once; any other red = STOP, the engine commit standing.
    2h. Fingerprint --compare; push; level; --compare after (IDENTICAL). Ledger row; push;
        level.

  STEP 3 — Evidence commit (R731(c)). 12ae_U25B/: b0_parent_acceptance.txt, gate_u25b/
    (the nineteen files), pin_tuples_u25b.json; the staged set exactly those, every sha256
    equal to its scratch source; chained check && commit; fingerprint IDENTICAL before and
    after the push; level; ledger. Record hashes and parents.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 and I2 held; TIMING obeyed.
  G2. Item 0 one commit: r788.txt at the architect's hash; Block 186 by the tool; the
      INT-i12-31 note in the record's form, the plan equal to HEAD's plus exactly that
      edit and the block; the three rules files untouched by content; the brief file
      hashed; one chain; pushed, level, fingerprint IDENTICAL; ledger.
  G3. Five refusal cases red on the parent and green in the gate; the live case green on
      both; the diff exactly the four files (plus docs/inventory/ only under R786(b)'s
      form); pins, audit prompt_hash, the live codebook's sha256 and semantic hash
      unchanged; the gate at the E2 counts inside the band, no load, nineteen files;
      engine commit pushed, level, fingerprint IDENTICAL; ledger.
  G4. The evidence commit as step 3 states; pushed, level, fingerprint IDENTICAL; ledger.

OUT OF SCOPE: INT-i12-31's CLOSED note (the next item 0, R514); option B; the two other
  yaml.safe_load sites; codebook.py:7's historical docstring (R788(e)); any codebook
  edit (R26); the tools items; /wrap.

CONSTRAINTS (task-only): no model call, no restart, no live write; the live codebook
  never written or copied over; no edit and no uncommitted file in the working tree
  between 07:00 and 10:35 UTC; a count arrived at by eye is INFERRED, the computed value
  given with its command; where this brief describes a tool or record, the record
  governs and a difference is a finding (R732(a)).

FINAL REPORT FORMAT: (0) pre-check table and the TIMING decision taken; (1) item 0 —
  metadata, render comparison, the row before and after, the brief file's hash, the
  check verbatim, chain, hashes, fingerprint, ledger; (2) the build — 2a's quotes, the
  two new fixtures, the test as written, b0_parent_acceptance.txt verbatim, the diff
  --stat, the inventory check, the pin recompute and the live hashes, the selected runs,
  the engine commit hash and parent; (3) the gate — launch line, timestamps, wall vs
  837–1021, the summary line verbatim, collected/selected, the id comparison, durations
  >10 s or none, basetemp, journal window, Ollama reads, concurrent jobs, the nineteen
  files; (4) the evidence commit; (5) contradictions and findings, numbered, each with
  its quoted line; (6) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 or I2 false, a 1a mismatch, or a check
  failure or crash at 1e = STOP before the item-0 commit. A refusal case green on the
  parent, a diff outside the four files (inventory excepted under R786(b)'s form), or a
  moved pin or codebook hash = STOP before the engine commit. A non-(c) red in the gate =
  STOP, the engine commit standing and reported. An expectation mismatch (E1–E3) is
  reported and the work continues. A spec premise that does not survive contact with
  disk is a finding, not an obstacle.

HANDBACK: Stop and report at the TIMING stop or at the end. Do not start U26 or
  INT-i12-31's close without explicit sign-off. Await the next brief or ruling in this
  session. STOP-12AE-U25B
