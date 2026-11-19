RULING AND RESUME [evidence-engine 12ah-OLRA-R2] — the read-out accepted; the G5 stop upheld;
  the foreign ledger row left as found; three added reads; no close yet
STATUS: PI ruling, 2026-11-16 UTC, on architect recommendation, on the 12ah-OLRA-R1 final
  report. Interim, unnumbered, as R1 was; session 12ah commits it by hash.
MACHINE: DGX    SESSION DIRECTORY: ~/projects/evidence-engine (branch main)
PERMISSIONS: auto mode  |  blast-radius: low
  Written: ~/scratch/12ah-olra/ only (new files; no existing file there is edited).
  Committed: nothing, in either repository.

ASSUMPTIONS IN THIS RULING (quoted from the 12ah-OLRA-R1 final report)
  MEASURED: "Commits: none in evidence-engine; none in claude-config"; "SHA256SUMS:
    85850dccb00d6baeec7cb8201eb51ff09358439f2fcd87fd655cb58b624f7e5f, 18 entries"; "G1 passes.
    evidence-engine HEAD is still 35a914b"; "claude-config is at 7c74c2b, level with origin,
    but PROJECT_LEDGER.md has one uncommitted row"; the row "| 2026-11-16T00:04:30Z |
    surgical-cv-br-1 | br-1 | 11f67fc | fix(brand): visible row hover and Action Required
    panel |"; "All 673 of this lane's ledger rows are present at HEAD"; "primer.md lines 41–45
    are byte-equal at both ends".
  READ (CC's): "It is another lane's hook row, not this session's (this session made no
    commit)"; "At 00:06:51 an operator on TTY=pts/12 ran sudo systemctl restart ollama".
  INFERRED — BLOCKING: none.
  INFERRED — expectation (from the workbench addendum of 2026-11-16 and unified plan v105;
  report a mismatch, continue):
    Y1. The host's Ollama keep-alive default is now 30m; a request that sends its own
        keep_alive is unaffected.
    Y2. dgx-infra holds scripts/ollama_lane_models.txt, whose Evidence Engine block lists
        qwen3:8b, qwen3:32b, qwen3.5:27b, gemma3:27b and qwen2.5vl:7b.
    Y3. The engine's extraction model is deepseek-r1:32b.
    Y4. The Sunday health sweep stands down on ~/.ollama_experiment.lock.

R2-a — The 12ah-OLRA read-out is accepted as reported, to scratch, its eighteen files under
  SHA256SUMS 85850dcc…7e5f. The classifications the report marks as CC's own reading
  (findings 2 and 4, the Q6 coupling list, Q8's preference) stay inferences until the build
  brief measures them.
R2-b — The G5 stop is correct. The surgical-cv row is not committed by this session and is
  not touched; it is 12ah-OPEN's under R654(d). G5 reads from here: claude-config HEAD equal
  to the commit read at step 0 and level with origin, its only dirt uncommitted ledger rows
  of other lanes, each quoted in the report. Any other dirt is a STOP.
R2-c — Three reads, read-only, under the brief's constraints unchanged (no model load, no
  server request, no lock, no pytest run), each answered from quoted lines:
  Q11. Keep-alive. Every site in engine/, scripts/ and analysis/ that sends Ollama a request
       able to load a model or hold one resident: whether it sends keep_alive, the value, and
       where the value comes from; and the entry points that reach it. A plain list of the
       paths that send none. Quote review_spec.py's keep_alive default and its comment
       (the report's finding 9 names :680), and say which sentence the host change made
       untrue.
  Q12. Models and lock. Every model name the live spec, the engine's defaults and the
       analysis scripts name, with the stage each serves. The Evidence Engine block of
       dgx-infra's scripts/ollama_lane_models.txt, quoted from the checkout CC locates (not
       edited). The differences in both directions. The lock file path the engine's
       hold_experiment_lock uses, quoted, beside Y4's path.
  Q13. The close, read and not run. This session's id. The /wrap command's definition and
       the session-archive tool, read from disk: what each would write, commit or push for a
       session that made no commit of its own, while claude-config holds another lane's
       uncommitted ledger row; and whether either would commit that row. The archive command
       as it would be given after /exit. Neither is run.
R2-d — The answers go to one new file, ~/scratch/12ah-olra/12ah_OLRA_add1.md. A second list,
  ~/scratch/12ah-olra/SHA256SUMS_add1, names only the files this resume adds (this block,
  written once as brief_12ah_OLRA_R2.md and hashed straight after the write, included), in
  SHA256SUMS's form. The eighteen files and SHA256SUMS are not edited; SHA256SUMS is
  re-verified at the end and its result reported.

ACCEPTANCE GATES:
  G1. evidence-engine as at step 0: same HEAD, clean, no untracked file, level with origin.
  G2. Q11–Q13 each carry quoted lines with path and line number, or "no such site found" with
      the pattern used.
  G3. sha256sum -c on SHA256SUMS: eighteen OK.
  G4. No request to the Ollama server from this resume's window.
  G5. As R2-b states it.

FINAL REPORT FORMAT:
  1. Y1–Y4, one line each. 2. Q11–Q13 in plain sentences, pointing at the addendum's
  sections. 3. Findings outside the questions, each marked computed or CC's inference.
  4. The new files' hashes, SHA256SUMS_add1's hash, G1–G5.

CONTRADICTION HANDLING: a mismatch on Y1–Y4 is a finding; the reads continue. Dirt in
  claude-config other than R2-b's is a STOP, reported as found.

HANDBACK:
  Stop and report. Do not run /wrap, do not exit, do not commit. The close comes as its own
  ruling after this report.
  Await the next ruling in this session. STOP-12AH-OLRA-R2
