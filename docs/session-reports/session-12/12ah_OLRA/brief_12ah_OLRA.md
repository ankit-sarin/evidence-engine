TASK [evidence-engine 12ah-OLRA] — Ollama restart paths: Phase A read-out (read-only)
PURPOSE: Measure, on this lane's HEAD, every fact the PI's decision of 2026-11-15 (both Ollama
  restart paths become opt-in, off by default) rests on, so the design ruling can be written from
  a committed read-out. Task type: read-only read-out. No engine, test, script or analysis file
  is edited. No model call. One phase; it ends at a STOP.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (branch main)
SKILL: none (a read-out; the ollama skill is not needed because nothing is sent to the server)
PERMISSIONS: auto mode, commit steps included (R780(b))  |  blast-radius: low
  Touched: docs/session-reports/session-12/12ah_OLRA/ (new), and the hook's ledger row in
  claude-config. Scratch: ~/scratch/12ah-olra/.
  STOP / DO NOT TOUCH (beyond the global list): engine/, tests/, scripts/, analysis/,
  review_specs/, docs/plan/, docs/inventory/, CLAUDE.md; the experiment lock (not taken; state
  read only as R661(e) has it); the Ollama server (no request of any kind from this task; a read
  that would need one is a STOP); ~/scratch/ollama-OLR-1/ (read and copied, never edited);
  /etc/sudoers.d/ (not read with sudo; not touched).

SECTION 1 — THE PI'S DECISION (context for this read-out; not transcribed by this brief; its
ruling number and block come with the design ruling that follows the report)
BEGIN PI DECISION 2026-11-15
Both restart paths become opt-in, off by default:
1. RESTART_EVERY_N defaults to 0 everywhere it is used as a default (extractor and the three
   analysis scripts). A run that wants proactive restarts must ask for them explicitly.
2. The read-timeout path does not restart the server unless restarts are explicitly enabled for
   that run. By default, a read timeout fails the call through the existing error path and the
   run's normal retry or skip logic.
3. Do not substitute a per-model unload (keep_alive 0) for the restart. With one model slot,
   model names are shared across lanes, so it can unload another lane's model mid-use.
4. Evidence Engine's pinned keep_alive -1 stays as it is. The host default is moving from -1 to
   30m, which does not affect requests that send their own keep_alive.
After the change lands, /etc/sudoers.d/ollama-restart is removed by the PI; from then an
opted-in restart fails without a password.
END PI DECISION 2026-11-15

ASSUMPTION LEDGER (load-bearing claims only)
  MEASURED: none. The architect has read no report of this lane on these facts.
  READ: none that gates a step.
  INFERRED — BLOCKING (verify at step 0; STOP if false):
    B1. Session 12ah is open in this checkout and the 12ah-OPEN report has been accepted by a
        ruling in this session.
    B2. The tree is clean and main is level with origin.
    B3. No run is in progress and no process of this lane holds the experiment lock.
  INFERRED — expectation (report a mismatch, continue). All from the workbench handoff of
  2026-11-15, which summarises another lane's audit (OLR-1); line numbers are that day's:
    E1. ~/scratch/ollama-OLR-1/REPORT.md exists.
    E2. engine/agents/extractor.py: restart_ollama() def at :744, its subprocess call at
        :789-792, the cadence trigger at :1267-1268, RESTART_EVERY_N = 25 at :61, used as the
        default of run_extraction (:1000) and _extract_selected (:1107); a failed restart is
        caught at :1276-1280 and the run continues.
    E3. restart_ollama is also called from analysis/paper1/pass2_full.py:257 (--restart-every,
        default 0), analysis/eval/elicit01/runner.py:193 and analysis/eval/run_capture01.py:227.
    E4. engine/utils/ollama_client.py: _restart_ollama_and_retry() def at :878, its subprocess
        call at :931-936, called at :811 on httpx.ReadTimeout inside ollama_chat.
    E5. Both paths stand down when EVIDENCE_ENGINE_NO_OLLAMA_RESTART is set or a foreign
        experiment lock is held.
    E6. extractor.py:747-748 carries the stated reason ("a simple model unload/reload is not
        sufficient") and no measurement is cited for it.
    E7. engine/core/review_spec.py:675-676 pins keep_alive -1.
    E8. CONTAMINATED.md:36-38 records a restart that destroyed the inference-determinism Arm P
        rerun.
    E9. These are the only two live paths in this repository that restart the Ollama service.
        To be measured by the census in Q2, not assumed.

PROBLEM / CONTEXT:
  Ollama on this host is shared by six lanes. This lane is the only one whose code restarts the
  server; a restart drops every lane's in-flight request, and the experiment lock that guards
  this lane's restarts is taken by no other lane. The PI has decided the change in Section 1.
  The restart paths were last rebuilt by U19 (the environment re-check after a performed
  restart) and U21 (one route to the recovery restart; the closed set RUN_CALL_ENDING_DETAILS),
  so the design must be written from what the code holds today.

CONTRACT — the read-out answers each question from quoted source lines (file:line, the line as
it stands at HEAD). A classification CC makes by eye is labelled as CC's inference.
  Q1. The two paths as they stand. For each of E2-E7: the quoted lines, and "holds" or the
      difference. For each restart function: the exact command list passed to subprocess
      (whether sudo is called with -n, whether the call has a timeout, what stdin is), every
      stand-down condition in order, and what the function returns or raises in each case.
  Q2. Census. Every site in engine/, scripts/, analysis/ and tests/ (and any cron or nightly
      script this repository owns) that can restart, stop or reload the Ollama service or
      unload a model: service-manager calls, "ollama stop", any request sending keep_alive 0.
      For each: the entry points that reach it (entry_census.py's method), and whether it is
      live, test-only or dead. E9 is answered here.
  Q3. Where the cadence comes from. For every entry point that can reach restart_ollama: the
      source of its restart-every value (constant, argument default, CLI flag, spec field,
      environment). Whether the value, or any restart setting, is a key of the run-manifest
      body, a member of a pin tuple, or an input of any prompt_hash. State plainly whether
      changing the default alone would move a pin, a body key set or a hash.
  Q4. A read timeout with no restart. Following the path taken today when the opt-out variable
      is set: the ending recorded, the exception type surfaced from ollama_chat, and what each
      caller does with it (the extract loop, the auditor, both screeners, the FT runner, the
      preflight, pdf_quality_check): retry, skip the paper, or end the run, with the status the
      run closes on. State whether anything bounds consecutive read timeouts in a run; if
      nothing does, say so and give the worst-case wall time per paper from the constants.
  Q5. A restart command that fails. For restart_ollama's four callers (the extract loop and
      the three analysis scripts) and for _restart_ollama_and_retry: what happens when the
      command exits non-zero, and what would happen if sudo asked for a password (from Q1's
      command form: fail at once, or wait). Which tests pin each behaviour.
  Q6. Test census, in R725(b)'s classes, as a first pass for the build brief: tests that pin
      the default of 25, either restart path, the stand-down conditions, the U19 post-restart
      check, the U21 endings, and the lock census's "path to an Ollama restart" clause. For
      tests/conftest.py's fence and tests/test_service_call_fence.py: quote what they block
      and how. Classify each file as coupled to a default-off change or not (CC's inference,
      labelled). Collection only if needed (R703(j) for its basetemp); no test is run.
  Q7. Text that states the behaviour: CLAUDE.md, docs/architecture/, README and any spec or
      codebook comment that names the 25-paper restart, the recovery restart or the opt-out
      variable. Path and quoted line for each. Nothing is edited.
  Q8. Ways to say "restarts enabled for this run" that the code already supports or nearly
      supports (an existing flag, the existing variable read in the opposite sense, a spec
      field, a run-manifest key). One table: the option, the files it would touch, whether the
      setting would be recorded with the run. CC may state a preference, labelled as its own.
  Q9. The record. Copy ~/scratch/ollama-OLR-1/REPORT.md into the evidence directory unchanged
      (sha256 before and after equal), and quote E8's lines from CONTAMINATED.md. For E6: the
      commit that introduced the comment (hash, date, subject). Name which recorded fields of
      run_calls or run_telemetry would show per-call slowdown over a long run, by schema read
      only; no analysis of live data.

STEPS:
  0. pwd, branch, HEAD, status of both repositories; B1-B3. The session-start reads as R751(c)
     scopes them.
  1. Item 0: this block committed as brief_12ah_OLRA.md (R784(g)(v)), in the form of
     12af_U30A/brief_12af_U30A.md's item 0 less any ruling transcription (this brief carries
     none).
  2. Q1-Q9, read-only. Working files under ~/scratch/12ah-olra/.
  3. The read-out, 12ah_OLRA.md, with its census outputs beside it, committed in one commit
     (docs(session-reports) scope, R751(e)), pushed; the ledger row committed and pushed.
  4. Report and STOP.

ACCEPTANCE GATES:
  G1. git diff --stat <HEAD at step 0> HEAD -- engine tests scripts analysis review_specs
      docs/plan docs/inventory CLAUDE.md is empty.
  G2. Every answer in Q1-Q9 carries at least one quoted line with its path and line number,
      or the statement "no such site found" with the search pattern used.
  G3. The copied OLR-1 report's sha256 equals the source's.
  G4. The Ollama journal shows no request from this task's window (read as an open reads it).
  G5. Both repositories clean and level with origin at the STOP.

OUT OF SCOPE:
  Any edit that implements Section 1. Any design ruling. The sudoers file. The host's
  keep_alive default. Measuring CUDA fragmentation. Other lanes' code.

CONSTRAINTS (this task only):
  Read-only throughout; the only writes are the evidence directory, scratch and the ledger row.
  No pytest run other than collection. No model load, no server request, no lock acquisition.
  Valid at any time outside 07:00-10:35 UTC (R749(b)).

FINAL REPORT FORMAT:
  1. Step 0's readings and B1-B3.
  2. E1-E9, one line each: holds, or what the file shows instead.
  3. Q1-Q9, each in a few plain sentences, pointing at the read-out's section.
  4. Findings outside the questions, numbered, each marked computed or CC's inference.
  5. Commits (hash and parent, both repositories), the brief file's sha256, G1-G5.

CONTRADICTION HANDLING:
  A finding that contradicts B1-B3: STOP and report before item 0. A mismatch on E1-E9, or a
  restart site the handoff did not name: a finding, recorded, the read-out continues. A premise
  of this brief that does not survive contact with disk is a finding, not an obstacle.

HANDBACK:
  Stop and report. Do not edit any restart path and do not start U30's build without a ruling.
  Await the next brief or ruling in this session. STOP-12AH-OLRA
