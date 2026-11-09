CLAUDE CODE

================================================================================
SECTION 1 — RULING TO TRANSCRIBE (R784, PI ruling 2026-11-09 UTC; not on disk)
================================================================================

R784 is transcribed at item 0 of the brief below, by transcribe_ruling.py, as the
next block of the decision log. The lines between the two marker lines, exclusive
of the markers, are the byte content of the named file. Write the file from those
lines exactly: same characters, same line breaks, one trailing newline. Never
retype a ruling. The architect's independent hash, computed before this brief was
issued:
  r784.txt: sha256 f0abc9e9db3873c790174b63ef0534a10969543e9afddc13cff828f673ff419a,
            9441 bytes, 108 lines, one trailing newline.

===== BEGIN r784.txt =====
RULING [evidence-engine 12ae-R784] — 12ae-OPEN accepted; the series gains 940.16 s; claude-config
  e909961 accepted; the open check's health section reads the latest run only (a recorded limit);
  the wrap time corrected; a brief carries only what is new and is committed at its item 0 (a
  standing rule, PI's word); U24's Phase A opens
STATUS: PI ruling, 2026-11-09 UTC, session 12ae, on architect recommendation and on the PI's
  approval in words of the brief-efficiency recommendation, on the 12ae-OPEN final report (item 0
  ca9d3486ae0a7c0d3291bb477f4e894d5cc5a247; evidence 95dad8d37e0b7da65caa6f0dc263827e500c439e).
  Transcribe at item 0 of the 12ae-U24A brief below, by transcribe_ruling.py, in the block after
  R783's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the 12ae-OPEN final report):
  MEASURED: "02:57:23 UTC, 2026-11-09; commit date 2026-11-09"; "HEAD e909961, 13 commits past
    866115c (rc 0 for both hashes)"; "fac6237e archive time: 2026-11-02 14:33:44 UTC, read from the
    archived transcript's ctime"; "Seven pairs ran on this HEAD (11-02 to 11-08)"; "All seven ran
    20/20 with 0 FAIL lines and 0 non-200 responses"; "NV_ERR_NO_MEMORY: 31 lines this boot, 5 of
    them new since 11-02. All five fall inside health-job windows"; "Foreign loads since the
    closeout: 6"; "Each matches the expected file line for line"; "HEAD 6cb4ab6, because the
    reflog's last move is at 11-02 05:37:10"; "Wall times were 929.52, 934.11, 953.71, 930.96,
    946.35, 933.67 and 933.36 s"; "r783.txt: 52cbd571…aebe, 7258 B, 84 lines, equal to the
    architect's hash"; "Append: +95 / −0 to the plan. The "ends at" lines go from 137 to 138"; "The
    plan is now 16204 lines, 4e33d5d9…bb65, 2805498 B, ending "…ends at R783.""; "Id list: one added
    line, "A R783(d)", after "A R780(b)" (numstat 1/0)"; "wrote …: 94 clauses of 1516"; "3 header
    lines changed, 1 new section, 17 "Named by" lines that only append ids, other 0"; "The commit is
    ca9d3486ae0a7c0d3291bb477f4e894d5cc5a247, parent 6cb4ab6"; "The ledger commit is 7a5adc1b,
    parent e909961, for the row at 2026-11-09T03:04:18Z"; "session_open_check.py passed with 0
    mismatches and no SKIPPED section"; "284 rows, 94 / 56 / 134, 0 without a class"; "37, done 26
    (dated lines, measured)"; "R510: 210 rows at 6cb4ab6 and 210 at ca9d348. 0 rows in, 0 out";
    "Exit at 03:21:59.5 after 940.16 s, inside the band"; "Summary 3493 passed, 17 deselected, 1
    xfailed, 2 warnings; collected 3511, selected 3494, ids 0/0"; "The only concurrent job was the
    idle surgical-cv uvicorn (pid 1017068)"; "all 19 files are present"; "48 files added only: 19
    into 12ae_open/gate_12ae_open/ and 29 into 12ad_close/"; "The commit is
    95dad8d37e0b7da65caa6f0dc263827e500c439e, parent ca9d348"; "The ledger commit is 2f1a53b4,
    parent 7a5adc1, for the row at 03:22:55Z"; "The fingerprint was IDENTICAL before and after the
    push" (both commits).
  READ (CC's): findings 1–8 of that report; "The section compares the cron log's mtime date and
    reads only the last 20 Testing lines, so it can only ever read the latest run"; "The lane-line
    commit 866115c is at 14:28:46, and the archive ctime is 14:33:44"; "session-archive moves only
    *.jsonl files and job directories"; "The exact extract_rules command line for the 12ad-U23B
    regeneration is not recorded anywhere"; "It split ids on whitespace … I fixed it before any read
    was written or relied on".
  INFERRED: none.

R784(a) — The 12ae-OPEN final report is accepted as printed: item 0 ca9d348 (Block 181 by
  transcribe_ruling.py, beside Block 163's; "A R783(d)" with its gloss, STANDING_RULES.md at 94
  clauses, Tier A 86 / Tier B 8, its diff classified other 0; the lane-line copy), the evidence
  commit 95dad8d (the nineteen gate files under 12ae_open/gate_12ae_open/; the twenty-nine job-read
  files under 12ad_close/), ledgers 7a5adc1 and 2f1a53b with trailers; the open check 0 MISMATCH, 0
  SKIPPED; the recount 284 (94 / 56 / 134), P1 62 (25 / 25 / 12), fix units 35 / 37 / 19 / 37 done
  26; R510 210 / 210, in 0, out 0; seven nightlies on 6cb4ab6 each a line-for-line match, ids 0/0,
  windows empty. No stop.
R784(b) — The series on this host state gains 940.16 s (the 12ae-OPEN gate, beside the idle
  uvicorn only): the thirty-one of R782(c) plus 940.16, thirty-two. The band in force stays
  837–1021 s (R783(b)). The seven nightly wall times are readings, not members (a nightly is not a
  wrapper gate).
R784(c) — claude-config at e909961, thirteen commits past 866115c, is accepted under R663(b): ten
  carry ledger rows of other lanes (surgical-cv's and hello-ai-v2's, named in the report), none
  removing a row of this lane; three are the PI's rules-review commits (ecbea4d, 0e63927,
  8b29276), their primer hunks at lines 1002 and 1555, lines 41–45 untouched. The next check reads
  claude-config at 2f1a53b or an accepted descendant.
R784(d) — Finding 1: session_open_check.py's health section reads the latest run only, by the log's
  mtime date and its last twenty lines. A recorded limit of the tool, joining R752(c)'s two for the
  next tools change. CC's setting of the section's date to 2026-11-08 was right under R732(a): the
  tool governs, and the 11-02 run stood as step 1's explicit read. The brief's "the script's
  health section repeats the 11-02 read" was the architect's premise, written from the 12ad
  precedent in which the latest run and the first governed run were the same day; the premise is
  withdrawn, and an open after a gap names the latest run in the script's health section and reads
  every earlier run explicitly.
R784(e) — Readings, no row: finding 2 (the 12ad wrap landed at 14:28:46 UTC on 2026-11-02 and the
  archive at 14:33:44, not "about 05:45" — the unified plan's figure, corrected at v102; the brief
  asked for the archive time to be read from the record, and it was); finding 3 (a
  fac6237e…/tool-results/ directory survives the archive because session-archive moves only *.jsonl
  files and job directories — a dated note on inventory row I4, which already records that the
  tool leaves tool-results/ undetected, written at 12ae-U24A's item 0; the directory is left in
  place); finding 4 (the 12ad-U23B regeneration's command line is recorded nowhere; from this
  ruling the item-0 check's evidence file carries the regeneration invocation as run, and the
  committed brief under (g) carries it as directed); finding 5 (R782(f)'s file governs every
  nightly on the unchanged tree from its date, as the brief read it; the file's "2026-11-03"
  wording is a dated wording, not edited; R749(b) reads so); finding 6 (the id list's header
  comment stays, R766(d)); finding 7 (CC's first id parser split on whitespace and was corrected
  before any read was written — a reading, in CC's own words); finding 8 (1489 → 1516 clauses in
  the log, a measurement).
R784(f) — The 12ae-OPEN brief's length: 900 lines before its preface, of which the architect counts
  about 350 as new to this open; the rest restated clauses STANDING_RULES.md holds (43 of the 62
  ids the brief cites), quoted tool docstrings and README text CC reads at the step that uses
  them, quoted the "Expected values" block CC reads whole at its step 0c, and carried the 12ac-OPEN
  and 12ad-OPEN scaffolding verbatim because no brief is on disk to point at. The PI, in words on
  2026-11-09: the recommendation is agreed, "as long as context and fidelity of chat and work
  stays". Clause (g) is that agreement as a rule.
R784(g) — Rule from here, a standing rule (PI's word, 2026-11-09): a brief carries only what is new
  to its task. (i) A clause STANDING_RULES.md holds is cited by its id and not restated; a brief
  restates a convention only when the file lacks it. (ii) A tool's docstring or the README is not
  quoted into a brief; the brief names the tool and the step that reads it. (iii) The "Expected
  values at session-N open" block is quoted into an open brief only where a value gates a stop;
  the block is read whole by CC at its step 0. (iv) The form of a deliverable is named by the path
  of a committed read-out or brief, with the deltas stated, never by pasting the form. (v) Each
  brief is committed as pasted, at its own item 0, into the evidence directory it governs, as
  brief_<task id>.md (the whole pasted block, Section 1 included), written once from the paste and
  hashed straight after the write (R738(b)), the hash reported; the architect compares it against
  its own copy at the next report, and a mismatch is a finding, not an edit (R739(b)). (vi) The
  stop conditions, the expected values and the steps are the brief; the rest is a pointer. From
  the 12ae-U24A brief on; the 12ae-OPEN brief stays in the architect's record and the archived
  session transcript and is not reconstructed. Fidelity is not reduced by this rule: a rule cited
  by id is the same rule, and a form named by path is the same form. "A R784(g)" joins the list at
  12ae-U24A's item 0 (R754(c)(iii)), the file regenerated there, the diff classified (R766(b)).
R784(h) — U24's Phase A opens: 12l_P1A.md §3's U24, "Every preflight probe is sent the spec's
  block" (C49), read-only, in 12ad_U23A.md's form (§0–§9), its entry quoted first. No engine, test,
  script, spec or codebook edit; no model call; no live write. The design ruling follows its
  read-out.
===== END r784.txt =====

================================================================================
SECTION 2 — THE BRIEF
================================================================================

TASK [evidence-engine 12ae-U24A] — U24's Phase A read-out: every preflight probe
  site at HEAD, the mechanism the spec's block changes, the test census, the
  acceptance test against HEAD, options for the design ruling. Item 0: R784 as the
  next block; "A R784(g)" into the standing rules with the regeneration; the I4 note;
  this brief committed as pasted. Read-only on engine/ and tests/; STOP before any
  edit.
PURPOSE: Put on disk what C49's fix changes before it is designed: which callers reach
  the probe without the spec, what the spec's block alters in the request and in the
  run's preflight row, which tests are coupled to those sites, and whether the
  acceptance test 12l_P1A.md names can be written against HEAD and fails. Mixed: one
  docs commit (item 0), one docs commit (the read-out). No engine, test, script, spec
  or codebook change. No live write. No model call.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (also ~/claude-config)
SKILL: none
PERMISSIONS: auto mode ON, commit steps included (R780(b)) | blast-radius: low
  Touched: docs/plan/ENGINE_REFACTOR_PLAN.md (Block 182 appended by the tool; one dated
  note on inventory row I4); docs/plan/standing_rules_ids.txt (one line);
  docs/plan/standing_rules_glosses.json (one entry); docs/plan/STANDING_RULES.md
  (regenerated); docs/session-reports/session-12/12ae_open/r784.txt; a new directory
  docs/session-reports/session-12/12ae_U24A/ (brief_12ae_U24A.md at item 0; the
  read-out, census script and output, probe script and output at step 2);
  ~/scratch/12ae-u24a/; the claude-config ledger; git push on both repos.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/, the codebook, docs/architecture/, CLAUDE.md, docs/inventory/, the tools
  (run, not edited); review.db (mode=ro through the named instruments only); any process
  or tmux session found (read, never signalled); the ~/.claude/projects/…/fac6237e…/
  tool-results/ directory (R784(e) finding 3: left in place); any scratch directory other
  than ~/scratch/12ae-u24a/.

ASSUMPTION LEDGER (load-bearing only):
  MEASURED (12ae-OPEN final report, quoted in R784):
    HEAD 95dad8d37e0b7da65caa6f0dc263827e500c439e, parent ca9d3486ae0a7c0d3291bb477f4e894d5cc5a247,
      clean, level; plan ends "…ends at R783.", last block 181 (to be re-read at 0b);
      STANDING_RULES.md Tier A 86 / Tier B 8 / 94 of 1516; glosses 42 entries; the id list's
      last A line "A R783(d)". claude-config at 2f1a53b (short, from the report) or an
      accepted descendant (R663(b)).
    Architect's hash of r784.txt (Section 1): f0abc9e9…419a, 9441 bytes, 108 lines.
  READ (from the clone at 95dad8d):
    R1. 12l_P1A/12l_P1A.md §3, the U24 entry, whole: "### U24 — Every preflight probe is
        sent the spec's block / Rows: C49 / Pin effect: not affecting / Files:
        engine/agents/screener.py, engine/agents/ft_screener.py / Depends on: — /
        Acceptance test (synthetic; discharges R491, R541):
        test_every_engine_preflight_probe_sends_the_spec_preflight_block — With a spec
        whose preflight block sets a non-default option, the probe request captured from
        run_screening, run_verification and the FT screener carries it and equals the
        options recorded on the run's preflight row; an AST census finds no engine call
        site without spec=. Fails on the unmodified tree. / Note: Three engine sites lack
        spec=; a fourth is an analysis harness outside any manifest."
    R2. grep -n require_preflight engine/ at 95dad8d: extractor.py:1149
        "require_preflight([extractor_model], runner_name="Extraction", spec=spec)";
        ft_screener.py:325–328 "require_preflight([spec.ft_screening_models.primary,
        spec.ft_screening_models.verifier], runner_name="FT screening",)";
        screener.py:171 "require_preflight([primary_model], runner_name="Abstract
        screening")"; screener.py:259 "require_preflight([verification_model],
        runner_name="Abstract verification")"; the definition
        ollama_preflight.py:279–280 "def require_preflight(models: list[str], runner_name:
        str, timeout: int = 30, *, spec=None) -> None:"; :291 "result =
        preflight_check(models, timeout=timeout, spec=spec)"; :106 "def
        preflight_check(models: list[str], timeout: int = 30, *, spec=None) ->
        PreflightResult:"; :111 "result = check_model(name, timeout=timeout, spec=spec)".
        review_spec.py:693–697: "class PreflightConfig(_SpecModel): """The model-health
        probe check_model sends (R63).""" temperature: Temperature = Field(default=0)
        num_predict: int = Field(default=4, ge=1)"; :818 "preflight: PreflightConfig =
        Field(default_factory=PreflightConfig)". Files under analysis/ naming preflight:
        analysis/paper1/judge_codebook_smoke.py, analysis/eval/run_qualgap01.py,
        analysis/eval/elicit01/runner.py, analysis/eval/run_capture01.py.
    R3. Inventory row C49 (section C) at HEAD, its text and its 12e-CLOSE amendment
        (R491: "widened — run_screening and run_verification call require_preflight
        without spec= too; three sites in all. Only the probe's options and keep_alive
        resolve spec-less; the models come from the spec; equal on the live spec, which
        sets neither a preflight nor an ollama block (12e_C55-A Q4, computed)"); its
        12g note: Class 2, P1 (R541, R540).
    R4. 12ad_U23A/12ad_U23A.md is the form of the read-out: §0 Scope; §1 Inputs quoted
        (2a the unit entry and its order, 2b the attached item, 2c the row and its
        acceptance test, 2d CLAUDE.md and docs/architecture lines, 2e questions the
        inputs raise that this step does not ask); §2 Sites (3a); §3 Mechanism at HEAD
        (3b), each statement labelled CODE ORDER or FAKE; §4 The census (3c), by an AST
        instrument in census_u23a.py's form with its declared completeness; §5 The
        acceptance test against HEAD (3d); §6 The cases (3e); §7 Options (3f) and
        classified sites (3g); §8 Findings; §9 What could not be established read-only.
        Deltas for U24: §2 lists probe callers, not root readers; §3's mechanism is the
        request the probe sends and the row the run records, not path resolution.
    R5. Inventory row I4 at HEAD: "| I4 | The session-archive tool leaves tool-results/
        undetected | v49 ARCHIVE-FIX-01 | — |".
  INFERRED — BLOCKING: none beyond step 0's re-reads.
  INFERRED — expectation: E1. ~/scratch/12ae-open/ holds the 12ae-OPEN item-0 check,
    block181.json and the regeneration's evidence file (the models for item 0; if gone,
    the committed evidence and 12ad-U23B's item 0 as recorded in 12ad_U23B.md govern).
    E2. check_model builds the probe request from a PreflightConfig resolved with or
    without the spec and the run's preflight row is written by _open_run_manifest
    (R489) — stated from R63/R489/C54's text, not read; §3 reads it. E3. No test under
    tests/ pins a spec-less require_preflight call by name; the census decides.

CONTEXT: Session 12ae opened at 95dad8d (R784). C49 is Class 2, P1, the next unit after
  U23 (12l_P1A.md §3). This is Phase A of the two-phase form; the design ruling, Phase B
  reads and the build follow it.

CONTRACT (steps, in order; evidence under ~/scratch/12ae-u24a/):

  STEP 0 — Pre-check (read-only).
    0a. date -u; outside 07:00–10:35 UTC (R86).
    0b. evidence-engine: HEAD 95dad8d…439e, parent ca9d348…a247, each proved by git
        cat-file -e <hash>^{commit} (R783(d)); clean; fetch; 0 0; one worktree. Plan: last
        line "…ends at R783."; last "**Block N.**" line N = 181; grep -c "**Block 182.**"
        = 0. Rules files: the id list's last A line "A R783(d)"; glosses 42 entries;
        STANDING_RULES.md header Tier A 86 / Tier B 8 / 94. Mismatch = STOP.
    0c. claude-config: HEAD 2f1a53b or a descendant accepted under R663(b) (each commit
        attributed; lines 41–45 of primer.md unchanged); clean; level. Else STOP.
    0d. Fingerprint --compare: IDENTICAL (the instruments of R6 of the 12ae block; the
        value 0d3eedea…cb25).

  STEP 1 — Item 0 (one evidence-engine commit, one ledger commit). Model: 12ae-OPEN's
    item 0 (ca9d348; E1), its check and chain quoted before composing.
    1a. Write 12ae_open/r784.txt from Section 1, hash straight after the write (R738(b)):
        EXPECTED f0abc9e9db3873c790174b63ef0534a10969543e9afddc13cff828f673ff419a, 9441,
        108. Mismatch = STOP.
    1b. block182.json in block181.json's key set: tag R784; transcribed = the commit's
        UTC date; item "item 0 of the 12ae-U24A brief"; beside = "the I4 note, the
        standing-rules regeneration and the brief file it orders" (composed for this
        item; quote it); made_by PI; made_date 2026-11-09; session 12ae; report = the
        12ae-OPEN final report (item 0 ca9d3486ae0a7c0d3291bb477f4e894d5cc5a247; evidence
        95dad8d37e0b7da65caa6f0dc263827e500c439e), in Block 177's form for a final
        report; sentence model, followed "12ae-U24A brief"; prev_ends R783. Render to
        stdout, compare element order with Block 181's (a difference in form is a
        finding), then --append. The plan's "−" count read at 1g, EXPECTED 0 for the
        append (R766(c)).
    1c. Inventory row I4: quote it (R5); append inside its status cell one dated note
        "*Note 2026-11-09 (12ae-U24A item 0, R784(e)):*" in CC's words from R784(e)
        finding 3 (the fac6237e tool-results/ directory survives the archive;
        session-archive moves only *.jsonl files and job directories; left in place).
        One row replaced, no other line; the plan's "−" count for this edit is that one
        row, read and matched.
    1d. Standing rule: append "A R784(g)" after "A R783(d)" (quote before and after;
        numstat 1/0); one gloss entry "R784(g)" (CC's, rules nothing, quoted; the file's
        formatting kept); regenerate with extract_rules.py --plan … --ids … --glosses
        12aa_SETUP/rules_glosses.json --glosses docs/plan/standing_rules_glosses.json
        --out docs/plan/STANDING_RULES.md --adopted R754, stdout to an evidence file, the
        invocation as run written to that file (R784(e) finding 4); --spot 5 as 12ae-OPEN
        ran it. EXPECTED no refusal; header Tier A 87 / Tier B 8 / 95, the log total to
        be measured. Classify the diff under R766(b): header lines, one new section,
        "Named by" lines that only append ids, other = 0 (listed if not).
    1e. The brief file (R784(g)(v)): write
        docs/session-reports/session-12/12ae_U24A/brief_12ae_U24A.md from this whole
        pasted block — from the line "CLAUDE CODE" to the last line of HANDBACK,
        inclusive, one trailing newline — written once; sha256, bytes and lines straight
        after the write, reported (the architect compares at the next report; a
        mismatch is a finding, not an edit, R739(b)).
    1f. The item-0 check, in 12ae-OPEN's content form (R777(b), R772(c)), extended:
        --check-plan byte-identical; the plan's numstat read ("−" matched to the one I4
        row); every earlier "ends at" line kept; last line "…ends at R784." once; the
        plan equals HEAD's plus the I4 row replaced and Block 182 appended, nothing else
        (computed); r784.txt equal to 1a; the id list +1/−0 with the one line; the glosses
        parsed before and after differing by the key "R784(g)" only; STANDING_RULES.md
        classified other = 0 and its header counts read; brief_12ae_U24A.md present with
        its hash recorded; the staged set exactly the plan, the three rules files,
        r784.txt and brief_12ae_U24A.md. Evidence by redirect; non-zero on any failure.
        Quote the check.
    1g. <check> && <commit>, one line; message from the staged set (R778(d)), docs scope,
        naming Block 182, "A R784(g)", the I4 note and the brief file. Fingerprint
        --compare before the push; push; level; --compare after (IDENTICAL). Ledger row
        committed in claude-config with trailers (R680(b)); push; level. Record hashes
        and parents.

  STEP 2 — The read-out (read-only; no server, no database open).
    2a. §1: quote the U24 entry (R1) and the entries before and after it; row C49 whole
        with every dated note; R491 and R541 from their blocks; 12e_C55-A's Q4 lines on
        the preflight (the "equal on the live spec" computation and what it compared);
        every CLAUDE.md and docs/architecture line naming the preflight, with path and
        line; the live spec's preflight and ollama blocks or their absence (grep, quoted);
        §1 2e: the questions the inputs raise that Phase A does not answer.
    2b. §2 Sites: every call reaching check_model — require_preflight, preflight_check,
        check_model directly, and the CLI main — across engine/, scripts/, analysis/ and
        tests/, each with path:line and the line quoted, classified: passes spec= / omits
        it / is outside any manifest; the "fourth site" of R1's note named. State which
        of the three spec-less engine sites lie on Run 7's path (run_pipeline → SCREEN
        stage; the FT runner) and which on no production path (E9: run_verification has
        no production caller — quote E9's row and the grep).
    2c. §3 Mechanism (CODE ORDER unless FAKE): what check_model sends — the request's
        model, options, keep_alive, and how each is resolved with spec=None and with a
        spec (stage_config("preflight", spec)? effective_config? read it); where the
        run's preflight row (the stage key "preflight:<model>", R489, C54) is written and
        from what; whether a spec-less probe under an active run is refused by
        check_declared_call (C54: the row names the options? or the model only — read
        it), sends options differing from its row, or neither; what the manifest and
        run_calls record of the probe (request_hash, outcome). FAKE where a fake client
        can show it without a server (the engine client rebound as tests/test_u18_*.py
        do; ollama.ps patched as test_ollama_preflight.py's sibling does — quote both
        before reusing). The pins: not affecting by R1; confirm by the request path (the
        probe is not an extraction stage) and say so.
    2d. §4 Census: tests coupled to the three sites, require_preflight, preflight_check
        and check_model — by an AST instrument census_u24a.py in census_u23a.py's form
        (copy, adapt the names; declared completeness stated as that file states it),
        output census_u24a.txt: classes of R722(a), R723(c), R724(a), R725(b); each
        hit with path:line and its class, and what a spec= at the site would do to it
        (nothing / a rewrite stated in words / a new assertion). The live-server fence
        (I14, I20: test_raises_on_failure leaves ollama.ps unpatched) read and quoted
        where it touches the probe.
    2e. §5 The acceptance test against HEAD: can
        test_every_engine_preflight_probe_sends_the_spec_preflight_block be written at
        HEAD's seams with no server and no database, and does it fail (FAKE: a probe
        script fake_probe_u24a.py, output committed)? Which of its three captures fail
        at HEAD and which pass by coincidence of defaults (R491: equal on the live spec)
        — the test needs a non-default preflight value, so state the fixture spec it
        needs. Whether run_verification can be driven without a production caller.
    2f. §6 Cases: the spec with and without a preflight block; an ollama block (keep_alive)
        with and without; under an active run and outside one; the CLI main (no spec by
        construction?); the analysis harness.
    2g. §7 Options for the ruling, each with its sites, tests and what it does not fix:
        A — pass spec= at the three sites (the minimum the entry names); B — spec
        required keyword-only on require_preflight and preflight_check, spec-less
        callers a TypeError (C41/C52's form), the CLI resolving the spec or declaring
        its spec-lessness; C — the probe refused under an active run unless its options
        equal the run's preflight row (an extension of C54's check). Classified sites
        (3g) as 12ad_U23A §7 lists them. §8 findings; §9 limits.
    2h. Commit the read-out (12ae_U24A/12ae_U24A.md), census_u24a.py and .txt,
        fake_probe_u24a.py and its output: the staged set exactly those files, nothing
        under engine/ or tests/ (asserted); fingerprint IDENTICAL before and after the
        push; level; the ledger row. Record hashes.

ACCEPTANCE GATES:
  G1. Step 0 matched or stopped; every hash proved by cat-file -e.
  G2. Item 0 one commit: r784.txt at the architect's hash; Block 182 by the tool,
      --check-plan byte-identical; the I4 note the one row replaced; "A R784(g)" the one
      added line with its gloss; STANDING_RULES.md regenerated, header 87 / 8 / 95,
      classified other 0, the invocation in its evidence file; brief_12ae_U24A.md written
      once and hashed; one chain; pushed, level, fingerprint IDENTICAL; ledger.
  G3. The read-out in R4's form with every section present; every code claim beside a
      quoted line with path and line read at 95dad8d; CODE ORDER / FAKE labelled; the
      census instrument and output committed with their declared completeness; the
      probe output committed with the Ollama journal showing no load from this session
      and no database opened; no file outside 12ae_U24A/ in the commit; pushed, level,
      fingerprint IDENTICAL; ledger.

OUT OF SCOPE: U24's design and build (the ruling on this read-out, then Phase B); any
  edit under engine/, tests/, scripts/, analysis/, docs/architecture/ or CLAUDE.md; the
  experiment lock; any gate; the tools items (R752(c), R765(d), R766(d), R784(d));
  /wrap.

CONSTRAINTS (task-only): no model call, no generate, no restart; read-only on review.db;
  a proposed sentence for a docs line is proposed, never applied; a count arrived at by
  eye is INFERRED and the computed value is given with its command; where this brief
  describes a tool or record, the record governs and a difference is a finding
  (R732(a)).

FINAL REPORT FORMAT: (0) pre-check table; (1) item 0 — the metadata, the render
  comparison, the I4 row before and after, the id-list lines, the gloss, the
  regeneration invocation and header, the R766(b) table, the brief file's sha256 /
  bytes / lines, the check verbatim, the chain, hashes and parents, fingerprint,
  ledger; (2) the read-out's §0–§9 in full as committed (the report is the read-out
  plus the commit facts); (3) contradictions and findings, numbered, each with its
  quoted line; (4) STOP.

CONTRADICTION HANDLING: a step-0 mismatch, a hash cat-file -e does not prove, a 1a
  mismatch, a refusal or an "other" line at 1d, or a check failure or crash at 1f = STOP
  before the item-0 commit. An expectation mismatch (E1–E3) is reported and the read-out
  continues. A spec premise that does not survive contact with disk is a finding, not an
  obstacle.

HANDBACK: Stop and report. Do not start U24's design or build without explicit
  sign-off. Await the next brief or ruling in this session. STOP-12AE-U24A
