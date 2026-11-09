CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R785, PI ruling 2026-11-09 UTC; not on disk)
================================================================================

R785 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash, computed before this brief was
issued:
  r785.txt: sha256 8163758b174083b16df9d548f42649e92415d6e56d1cbedbb9ef4e04baea61af,
            10565 bytes, 118 lines, one trailing newline.

===== BEGIN r785.txt =====
RULING [evidence-engine 12ae-R785] — 12ae-U24A accepted; the committed brief equal to the
  architect's copy; U24's design: option A at the three sites, the acceptance test's census over
  every probe entry point under engine/, the CLI given --review; B not taken; C not taken, a note
  on C54; INT-ae12-1 opened; notes on I20; readings; the build opens as one brief
STATUS: PI ruling, 2026-11-09 UTC, session 12ae, on architect recommendation, on the 12ae-U24A
  final report (item 0 2b0badd; read-out d7f5905, docs/session-reports/session-12/12ae_U24A/
  12ae_U24A.md, sha256 646389efe1678c2370c501ce55a46b8f2988ed882e15213faf5b536e3024acf5, 260
  lines). Transcribe at item 0 of the 12ae-U24B brief below, by transcribe_ruling.py, in the block
  after R784's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the read-out and the report):
  MEASURED: "12ae_U24A/brief_12ae_U24A.md is c99d1410831f01641daa4a01ddc354a0978d05c52401b0920f357c90663963bf,
    29,898 B, 390 lines" (the architect's copy: equal, cmp byte for byte); "engine sites under a
    non-default spec: 12 captures, 12 unequal to their row"; "control (spec passed) under every
    variant: all equal = True"; "live spec, engine sites: all equal = True (R491: the coincidence
    of defaults)"; "The Ollama journal from the probe's start (04:26:09 UTC) to the read at
    04:26:16 UTC: -- No entries --"; "Tier A 87, Tier B 8, 95 of 1524"; "The plan's numstat is +120
    / −1, and the one "−" line is the I4 row".
  READ (the read-out, §2–§8): "S1 engine/agents/screener.py:171 … omits … yes: the default start
    runs SCREEN"; "S2 engine/agents/screener.py:259 … omits … no: no production caller (E9)"; "S3
    engine/agents/ft_screener.py:325–328 … omits … yes: the FT runner"; "S6
    engine/utils/ollama_preflight.py:321 main (the CLI) result = preflight_check(args.models,
    timeout=args.timeout) … omits (no spec exists in this scope by construction: the parser takes
    --models and --timeout only, :314–315)"; "S7 analysis/paper1/judge_codebook_smoke.py:1183 …
    outside any manifest"; "check_declared_call reads only the row's model"; "grep -n options_hash
    engine/utils/ollama_client.py returns nothing. So a spec-less probe under an active run is not
    refused"; "run_calls.request_hash hashes the spec-less options while
    run_stage_configs.options_hash for the same key hashes the spec's. The inconsistency is
    recorded but never detected"; the three call-argument assertions, "tests/test_screener.py:263–265
    … [spec.screening_models.primary], runner_name="Abstract screening"," / ":278–280 …
    [spec.screening_models.verification], runner_name="Abstract verification"," /
    "tests/test_ollama_preflight.py:272–275 … [spec.ft_screening_models.primary,
    spec.ft_screening_models.verifier], runner_name="FT screening","; "Each runner calls the
    probe before its first database read … So db=None reaches the probe"; "A single non-default
    num_predict suffices to fail all three captures"; "under Option B (spec required) each of
    these 9 calls is a TypeError"; "this widens C54's check for every stage … and it touches the
    send path. That reaches beyond C49's row"; F3: "At HEAD it patches
    engine.utils.ollama_client._client.ps (tests/test_ollama_preflight.py:226–227)"; F7:
    "default_factory gives every loaded spec a PreflightConfig instance, so _block returns it as
    the source and the row's sources read spec for values the YAML never set".
  INFERRED: none.

R785(a) — The 12ae-U24A final report is accepted as printed: item 0 2b0badd (Block 182 by
  transcribe_ruling.py; "A R784(g)" with its gloss, STANDING_RULES.md at 95 clauses, Tier A 87 /
  Tier B 8, the diff classified other 0, the regeneration invocation in its evidence file; the I4
  note; brief_12ae_U24A.md), the read-out d7f5905 with census_u24a.py/.txt and
  fake_probe_u24a.py/.txt, ledgers 3096c4a and 57b58df with trailers. No stop. The committed brief
  file is byte-equal to the architect's copy: R784(g)(v)'s first check passes, and paste fidelity
  is measured once.
R785(b) — U24's design: option A. S1 (screener.py:171), S2 (:259) and S3 (ft_screener.py:325–328)
  pass spec=spec. The acceptance test's census half covers every probe entry point under engine/
  — a call to require_preflight, preflight_check or check_model with no spec= keyword — not
  require_preflight alone (F2 answered: the entry's "three engine sites" counted require_preflight
  callers; the census counts the probe). So the CLI, S6, is in U24: ollama_preflight.main gains
  --review <id> (required) and --spec <path> (optional, the FT runner's form, ft_screener.py:659),
  loads the spec with load_spec_for(args.review, args.spec) and passes spec= to preflight_check;
  the CLI stays outside any manifest and under the experiment lock. S7 (analysis/, outside any
  manifest, no engine census covers it) is not in U24: a note on the HK-RETIRE line's neighbour is
  not written; the entry's own note already places it outside, and it stands.
R785(c) — Not taken: B (spec required keyword-only on the probe functions) — the census in the
  acceptance test is the guard the entry names; B adds nine test moves and a declared spec-less
  form that is itself a fallback, for a probe whose every engine caller A fixes. Not taken: C (the
  send refuses a probe whose options differ from its row) — after A no engine path resolves a
  stage's options other than through stage_config with the spec (with_options refuses an
  undeclared override, R223; the probe was the one path outside it), so C guards against no
  known trigger and widens C54's check into the send path beyond C49's row. A dated note on C54
  records the fact the read-out measured: the send check compares the model (check_declared_call)
  and the digest (check_call_digest) only; options and keep_alive equality holds by construction
  once U24 lands, and run_calls.request_hash against the row's options_hash is the check a later
  review may add. No row.
R785(d) — Fail-first and callers: the acceptance test
  tests/test_u24_preflight_spec.py::test_every_engine_preflight_probe_sends_the_spec_preflight_block
  (the capture half, in fake_probe_u24a.py's form: a fake engine client, check_ollama_env
  replaced, db=None, the live spec with preflight {num_predict 7, temperature 0.5} and ollama
  {keep_alive "5m"}; each of run_screening, run_verification and run_ft_screening's captured
  requests equal in options and keep_alive to stage_config("preflight", spec, model=m).kwargs())
  and ::test_no_engine_probe_call_omits_the_spec (the census half, AST over engine/, the empty
  set) are red on the parent — the first at the capture inequality, the second at S1, S2, S3 and
  S6 — recorded in b0_parent_acceptance.txt, green in the gate. The three call-argument
  assertions (test_screener.py:263–265 and :278–280; test_ollama_preflight.py:272–275) gain
  spec=spec with their subjects kept, class (c), the form of their siblings (:297–299, :319–320).
  The nine spec-less direct calls in test_ollama_preflight.py stay as they are (A leaves the
  default). A test of the CLI's --review is not required; the census covers its call.
R785(e) — Docs made false by (b), authored here, applied in the engine commit verbatim: CLAUDE.md
  line "python -m engine.utils.ollama_preflight --models qwen3.5:27b gemma3:27b deepseek-r1:32b"
  becomes "python -m engine.utils.ollama_preflight --review surgical_autonomy --models qwen3.5:27b
  gemma3:27b deepseek-r1:32b"; docs/architecture/modules.md line "- `preflight_check(models,
  timeout)` — Load test + VRAM budget (100 GB limit)" becomes "- `preflight_check(models, timeout,
  *, spec)` — Load test + VRAM budget (100 GB limit); the probe's options and `keep_alive` resolve
  from the spec's `preflight` and `ollama` blocks"; and "- `require_preflight(models,
  runner_name)` — Orchestrator: env check → model check → abort on failure" becomes "-
  `require_preflight(models, runner_name, timeout, *, spec)` — Orchestrator: env check → model
  check → abort on failure; every engine caller passes the spec (U24)". Each line is located by
  its text, not its number; a line not found as quoted is a stop.
R785(f) — Rows and notes, at 12ae-U24B's item 0: INT-ae12-1 opened in a new section "L (12ae
  additions, R785)", Class 3, P7, from F7: the preflight and ollama source labels read "spec" for
  values the YAML never set, because default_factory supplies the block (effective_config.py's
  _block, review_spec.py:816, :818); telemetry only, no stored value wrong. A dated note on I20
  from F3: test_raises_on_failure now patches engine.utils.ollama_client._client.ps
  (test_ollama_preflight.py:226–227); the instance I20 names no longer reads as described; when it
  changed is not established; I20's other content stands. The C54 note of (c). C49 is not closed
  here; it closes at the item 0 after the gate (R514), by its acceptance test passing.
R785(g) — Readings, no row: F4 (the brief's :291 is :289 — the architect's line number, carried
  from a grep at a different offset; a line number is an anchor only beside its quoted line, and
  the quoted line held); F5 (R5 dropped the row's backticks; CC appended to the row as it stands —
  the record's form); F6 (transcribe_ruling.py renders "on architect recommendation" from a fixed
  template, so R784's STATUS phrase is in the fence only — a tools item beside R765(d) for the
  tools change); F8 (E1 and E2 held; the row is written by open_run → resolve_run from both
  _open_run_manifest and open_screening_manifest). The read-out's §9 limits stand as recorded;
  the stored row for a non-default spec is read back in the acceptance test's gate run only if the
  test opens a scratch database, which (d) does not require.
R785(h) — The build opens as one brief, 12ae-U24B, not a Phase B reads brief and a RESUME: the
  read-out quoted every site, every coupled test and the acceptance test's seams at 95dad8d, the
  engine tree is unchanged since, and the change is three keyword arguments, one CLI argument pair
  and three docs lines. The brief re-reads the sites and the three assertions at HEAD before the
  first edit and stops on any difference from the read-out. No model call, no live write; the
  standard gate by the wrapper against 837–1021 s. This ruling states no standing rule: the id
  list, the glosses file and STANDING_RULES.md are untouched at item 0, asserted by content.
===== END r785.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ae-U24B] — U24's build: spec= at the three probe sites, the
  CLI's --review/--spec, the two acceptance tests red on the parent, the three
  call-argument rewrites, the three docs lines, one engine commit, the standard gate,
  the evidence commit. Item 0: R785 as the next block; INT-ae12-1; the I20 and C54
  notes; this brief committed as pasted; no standing rule.
PURPOSE: Land U24 (C49) as R785(b)–(e) rule it. Code task: fail-first acceptance tests,
  then the edits, one engine commit with its gate, evidence in its own commit. No live
  write. No model call (the probe is faked in tests; the gate runs under the suite's
  fence).
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: medium
  Touched: engine/agents/screener.py (two call lines), engine/agents/ft_screener.py
  (one call), engine/utils/ollama_preflight.py (main: two arguments, the spec load, the
  call); tests/test_u24_preflight_spec.py (new); tests/test_screener.py (two
  assertions) and tests/test_ollama_preflight.py (one assertion); CLAUDE.md (one line)
  and docs/architecture/modules.md (two lines), by R785(e)'s text verbatim;
  docs/plan/ENGINE_REFACTOR_PLAN.md (Block 183; the new section L table; the I20 and C54
  notes); docs/session-reports/session-12/12ae_open/r785.txt; a new directory
  docs/session-reports/session-12/12ae_U24B/ (brief_12ae_U24B.md at item 0;
  b0_parent_acceptance.txt, gate_u24b/ and the pin recompute at step 3);
  ~/scratch/12ae-u24b/; the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): any other file under engine/, tests/,
  scripts/, analysis/; review_specs/, the codebook, docs/inventory/ (run inventory
  --check only), the tools; the id list, the glosses file and STANDING_RULES.md (not one
  byte — R785 states no rule); review.db (mode=ro instruments only); the experiment lock
  (not taken: no model call); any process or tmux session found; any scratch directory
  other than ~/scratch/12ae-u24b/. No test under tests/ beyond the four named.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ae-U24A final report; R785): HEAD d7f5905, parent 2b0badd, clean, level; the
    engine tree equal to 9ae1fde's; plan ends "…ends at R784.", last block 182;
    STANDING_RULES.md 87 / 8 / 95, glosses 43, the id list's last A line "A R784(g)";
    claude-config at 57b58df or an accepted descendant (R663(b)). Gate reference: U23's,
    3,493 passed / 1 xfailed / 17 deselected on 3,511 / 3,494
    (12ad_close/collected_ids_12ad_close.txt, durations_12ad_u23b.txt); band 837–1021 s
    (R783(b)); the 12ae-OPEN gate 940.16 s beside it. Pins: 12ad_close/pin_tuples_close.json
    (elicited d70bf688…59f0, non-elicited d3672ab6…ff5c), audit prompt_hash d972c46d…838d.
    Architect's hash of r785.txt: 8163758b…61af, 10565 bytes, 118 lines.
  READ (12ae_U24A/12ae_U24A.md at d7f5905; the sites, the three assertions, the capture
    method and the fixture spec are quoted there at §2, §4 and §5 — the brief does not
    repeat them; step 1 re-reads each at HEAD). The three docs lines exist exactly once
    each at d7f5905 (the architect's grep -c: 1, 1, 1). ft_screener.py:659–663 is the
    --review / --spec argument form the CLI copies.
  INFERRED — BLOCKING: I1. HEAD's engine tree is unchanged since 95dad8d (git diff --stat
    95dad8d HEAD -- engine tests scripts analysis review_specs empty). I2. The sites and
    the three assertions read at HEAD as the read-out quotes them.
  INFERRED — expectation: E1. The acceptance test's census half, reusing census_u24a.py's
    call walker over engine/ for the three probe names, is red on the parent at exactly
    S1, S2, S3 and S6. E2. The new ids are two; the gate collects 3,513 and selects
    3,496 (3,494 + 2), only-gate the two. E3. No test outside the four named goes red
    (the no-op mocks accept the keyword, §4).

CONTRACT (steps, in order; evidence under ~/scratch/12ae-u24b/):

  STEP 0 — Pre-check (read-only). date -u outside 07:00–10:35 UTC (R86); the gate must
    launch by 06:30 UTC or the brief stops after step 2 and reports. HEAD and parent
    proved by git cat-file -e <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree.
    Plan: last line, last block 182, no Block 183. Rules files: the three counts above.
    claude-config: 57b58df or an accepted descendant, clean, level. I1 (empty diff).
    Fingerprint --compare IDENTICAL. Mismatch = STOP.

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit; model 12ae-U24A's item
    0, ~/scratch/12ae-u24a/check_item0_u24a.py and its chain, quoted before composing).
    1a. 12ae_open/r785.txt from Section 1; hash straight after the write: EXPECTED
        8163758b174083b16df9d548f42649e92415d6e56d1cbedbb9ef4e04baea61af, 10565, 118.
        Mismatch = STOP.
    1b. block183.json in block182.json's key set: tag R785; transcribed = commit-time UTC
        date; item "item 0 of the 12ae-U24B brief"; beside = "the row INT-ae12-1, the I20
        and C54 notes and the brief file it orders" (composed; quoted); made_by PI;
        made_date 2026-11-09; session 12ae; report = the 12ae-U24A final report (item 0
        2b0badd; read-out d7f5905), in Block 182's final-report form; sentence model,
        followed "12ae-U24B brief"; prev_ends R784. Render to stdout, compare with Block
        182's order, then --append ("−" EXPECTED 0 for the append, read at 1f).
    1c. Rows and notes (R785(f), (c)): (i) a new section "L (12ae additions, R785)" after
        "L (12ad additions, R780)", in its form (quote that section's heading paragraph
        and INT-ad12-1's row first), with one six-cell row INT-ae12-1 (Class 3, P7, origin
        R785(f), source 12ae_U24A.md §3 point 3 and §8 F7), its text cell composed from
        R785(f)'s words and the read-out's quoted lines; (ii) a dated note "*Note
        2026-11-09 (12ae-U24B item 0, R785(f)):*" inside I20's status cell from F3 as
        R785(f) states it; (iii) a dated note "*Note 2026-11-09 (12ae-U24B item 0,
        R785(c)):*" inside C54's status cell from R785(c)'s last two sentences. Two rows
        replaced, one section inserted, nothing else; each "−" line read and matched to
        the row it replaces. Grep the plan for every place INT-ad12-1 appears (an index
        or coverage map) and add INT-ae12-1 in the same places, or report that the record
        placed it in its table only.
    1d. The brief file (R784(g)(v)): 12ae_U24B/brief_12ae_U24B.md from this whole pasted
        block, "CLAUDE CODE" through the last HANDBACK line, one trailing newline, written
        once; sha256, bytes, lines straight after the write, reported.
    1e. The item-0 check in 12ae-U24A's content form, adapted: --check-plan
        byte-identical; the plan's numstat read ("−" matched to the two replaced rows);
        every earlier "ends at" line kept; last line "…ends at R785." once; the plan
        equals HEAD's plus the two rows replaced, the section inserted and Block 183
        appended, nothing else (computed); r785.txt equal to 1a; the id list, the glosses
        file and STANDING_RULES.md unchanged and unstaged (R785 states no rule); the brief
        file present and hashed; the staged set exactly the plan, r785.txt and the brief
        file. Evidence by redirect; quote the check.
    1f. <check> && <commit>, one line; message from the staged set (R778(d)), docs scope.
        Fingerprint --compare before the push; push; level; --compare after (IDENTICAL).
        Ledger row in claude-config with trailers; push; level. Record hashes and parents.

  STEP 2 — The build (one engine commit).
    2a. Re-read at HEAD and quote: screener.py's two require_preflight lines,
        ft_screener.py's call, ollama_preflight.py's main (the parser lines and the
        preflight_check call), the three call-argument assertions, ft_screener.py's
        --review/--spec arguments, and the three docs lines of R785(e) by text (grep -n
        each, EXPECTED one hit). Any difference from 12ae_U24A.md §2/§4 or a docs line
        not found exactly once = STOP (I2).
    2b. Write tests/test_u24_preflight_spec.py, fail-first (R785(d)): (i)
        test_every_engine_preflight_probe_sends_the_spec_preflight_block — the fake
        engine client and check_ollama_env replacement in fake_probe_u24a.py's form
        (quote the form reused; no server; the conftest fence stays on), db=None, the
        fixture spec = the live spec with preflight {num_predict 7, temperature 0.5} and
        ollama {keep_alive "5m"}; for each of run_screening, run_verification and
        run_ft_screening, every captured probe request's options and keep_alive equal
        stage_config("preflight", spec, model=m).kwargs()'s, m the model the request
        names; (ii) test_no_engine_probe_call_omits_the_spec — the AST census over
        engine/ for calls to require_preflight, preflight_check or check_model with no
        spec= keyword (census_u24a.py's walker, reused or copied; declared completeness
        stated in the test's docstring), asserting the empty set. Run both on the parent:
        EXPECTED red, (i) at the inequality, (ii) naming S1, S2, S3 and S6 (E1); output
        to b0_parent_acceptance.txt. Green anywhere = STOP.
    2c. Edits, R785(b) and (e) exactly: spec=spec at S1, S2, S3; main gains --review
        (required) and --spec (optional) in ft_screener.py's form, loads
        load_spec_for(args.review, args.spec) before the lock and passes spec= to
        preflight_check; the three docs lines replaced by R785(e)'s text, located by text.
        The three assertions gain spec=spec, subjects kept (class (c), the siblings' form).
        No other line changes; git diff --stat read into the evidence, EXPECTED the
        eight files named in Touched and no other.
    2d. Pins: 12l_U04/pin_tuples.py with both arguments, byte-identical to
        12ad_close/pin_tuples_close.json; the audit prompt_hash unchanged (the request
        path, 12ae_U24A.md §3 point 7). A moved pin = STOP (R117/R224a).
    2e. The two new tests and the three rewritten ones, selected run: green. Then commit
        the engine change (Conventional Commits, fix scope, naming C49/U24 and the CLI
        argument); the message from the staged set. Do not push before the gate.
    2f. The standard gate by launch_gate.sh from the repository root: evidence_dir
        ~/scratch/12ae-u24b/gate_u24b/, session name, ref_ids
        12ad_close/collected_ids_12ad_close.txt, ref_durations
        12ad_close/durations_12ad_u23b.txt, no selection. Ollama reads ≥5 s before and
        after (R660(b)). EXPECTED: 3,495 passed / 1 xfailed / 17 deselected on 3,513
        collected / 3,496 selected (E2), only-gate the two new ids, only-ref 0; wall time
        inside 837–1021 s (outside: report with concurrent jobs, do not adapt); no phase
        slower by >10 s; no model load in the journal window (R696(b)); the nineteen
        files. A red: a census miss is class (c), fixed in P1's form and re-run once
        (R746(c), R772(b)); any other red = STOP with the engine commit standing, reported,
        not reverted.
    2g. Fingerprint --compare; push the engine commit; level; --compare after
        (IDENTICAL). Ledger row; push; level.

  STEP 3 — Evidence commit (R731(c)). 12ae_U24B/: b0_parent_acceptance.txt, gate_u24b/
    (the nineteen files), pin_tuples_u24b.json; the staged set exactly those, every
    sha256 equal to its scratch source, numstat read; chained check && commit; fingerprint
    IDENTICAL before and after the push; level; ledger. Record hashes and parents.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; I1 and I2 held.
  G2. Item 0 one commit: r785.txt at the architect's hash; Block 183 by the tool; the
      section and two notes in the record's form, the plan equal to HEAD's plus exactly
      those edits and the block; the three rules files untouched by content; the brief
      file hashed; one chain; pushed, level, fingerprint IDENTICAL; ledger.
  G3. Both acceptance tests red on the parent at the stated points and green in the gate;
      the three rewrites with subjects kept; the diff exactly the eight files; pins and
      audit prompt_hash byte-identical; the gate 3,495 / 1 / 17 on 3,513 / 3,496 inside the
      band, no load, nineteen files; engine commit pushed, level, fingerprint IDENTICAL;
      ledger.
  G4. The evidence commit as step 3 states; pushed, level, fingerprint IDENTICAL; ledger.

OUT OF SCOPE: C49's CLOSED note (the next item 0, R514); S7 (analysis/); options B and C;
  any other preflight or send-path change; the stale docs lines outside R785(e) (HK-ARCH);
  the tools items; /wrap.

CONSTRAINTS (task-only): no model call, no restart, no live write; the text of R785(e)
  applied verbatim, never paraphrased; a line number is an anchor only beside its quoted
  line; a count arrived at by eye is INFERRED, the computed value given with its command;
  where this brief describes a tool or record, the record governs and a difference is a
  finding (R732(a)).

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — metadata, render comparison, the
  section and the two rows before and after, the brief file's hash, the check verbatim,
  chain, hashes, fingerprint, ledger; (2) the build — 2a's quotes, the two tests as
  written, b0_parent_acceptance.txt verbatim, the diff --stat, the three docs lines before
  and after, the pin recompute, the selected run, the engine commit hash and parent; (3)
  the gate — launch line, timestamps, wall vs 837–1021, the summary line verbatim,
  collected/selected, the id comparison, durations >10 s or none, basetemp, journal
  window, Ollama reads, concurrent jobs, the nineteen files; (4) the evidence commit; (5)
  contradictions and findings, numbered, each with its quoted line; (6) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, I1 or I2 false, a 1a mismatch, or a check
  failure or crash at 1e = STOP before the item-0 commit. A green acceptance test on the
  parent, a diff outside the eight files, or a moved pin = STOP before the engine commit.
  A non-(c) red in the gate = STOP, the engine commit standing and reported. An
  expectation mismatch (E1–E3) is reported and the work continues. A spec premise that
  does not survive contact with disk is a finding, not an obstacle.

HANDBACK: Stop and report. Do not start U25 or C49's close without explicit sign-off.
  Await the next brief or ruling in this session. STOP-12AE-U24B
