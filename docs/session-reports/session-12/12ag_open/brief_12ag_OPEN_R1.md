CLAUDE CODE

================================================================================
SECTION 1 — RULING (R804, PI ruling 2026-11-14 UTC; not on disk). Transcribed at the
next item 0, not now. The lines between the markers are the byte content of r804.txt
when that item 0 writes it; the architect's hash of that content:
  sha256 d2ccb5ef7d253a04f541e37f790fbb5632a96863aa4b80ac623ff6ddc54b681e,
  4625 bytes, 55 lines, one trailing newline.
================================================================================

===== BEGIN r804.txt =====
RULING [evidence-engine 12ag-R804] — the 12ag-OPEN stop before the gate is correct and is the
  architect's; the PI's one-time exception for this gate's launch; item 0 accepted; claude-config
  a93fc0b accepted; readings on findings 2–5; no standing rule; the open resumes at 3e
STATUS: PI ruling, 2026-11-14 UTC, session 12ag, on architect recommendation, on the 12ag-OPEN
  stop report (item 0 16cb307164c9203c695c3a4f842cf6d294c771ad; stopped before step 3e).
  Transcribe at the next item 0, by transcribe_ruling.py, in the block after R803's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the stop report):
  MEASURED: "item 0 is 16cb307164c9203c695c3a4f842cf6d294c771ad (parent 54266b7)"; "The ledger
    commit is 0663a61ed9cc7e0113cb4c8d4cf82528ce26ef96 (parent a93fc0b)"; "r803.txt:
    b53caeb9…216f, 9,203 B, 106 lines, equal to your hash."; "--append gave +117 / −0";
    "brief_12ag_OPEN.md: 6cb11d640e5eacbcfaafaab176b8a44b285f7c77efdf41e7567f51365b6df573,
    32,055 B, 422 lines" (the architect's copy: the same sha256, bytes and lines); "The "ends
    at" lines went from 157 to 158. The plan is now 9195c82d…45f6, 3,205,501 B, 18,783 lines.";
    "PASS (0 mismatches)"; "now is 287 (94 / 57 / 136, 0 unclassed), P5 24, 38 dated fix-unit
    lines with 33 done"; "The JSON is byte-equal to the committed one"; "213 rows at 54266b7
    and 213 at 16cb307, in 0, out 0"; "HEAD was a93fc0b, nine commits on from 79c4213"; "Eight
    are ledger-only, adding rows and removing none. The ninth is 9f78b9a"; "primer.md is
    unchanged since 79c4213"; "both are archived in ~/claude-session-archive/, and nothing
    started in either window survives"; "NV_ERR_NO_MEMORY is at 34 this boot (31 + 3), all
    three new lines inside health windows"; "The snapshot at 05:32:00 shows surgical-cv's
    uvicorn only".
  READ (CC's, the global CLAUDE.md at 9f78b9a): "If it is unattended and a later step depends
    on its result, or it loads a model (model, batch, benchmark and scheduled runs), it runs as
    a transient systemd-run --user unit."; "If the PI may need to watch it or answer prompts
    (installs, exploratory commands, long downloads), it runs in a named tmux session".
  READ (CC's): findings 1–5; the three nightlies "match, ran on 7ddabca".
  INFERRED: none.

R804(a) — The stop before step 3e is correct and is the architect's: the brief's E7 expected
  the global rule to allow a tmux home for a test gate, written without the rule's text, and
  the text sends an unattended job a later step depends on to a systemd-run --user unit. The
  architect's errors this session so far: two — E7, and I1's "clean" where the form's 0d
  expects other lanes' uncommitted rows (finding 3). Neither reached a commit wrong.
R804(b) — The PI's ruling, in his words: "One-time exception tonight." The 12ag-OPEN gate, and
  no other, is launched as the brief's step 3e wrote it: by launch_gate.sh, in the tmux session
  12ag_open. This is an exception to the global CLAUDE.md's long-running-jobs rule for this one
  launch; the rule is unchanged. A change to the wrapper so that a gate launches as a unit is a
  tools change in a brief of its own; no later gate brief relies on this exception.
R804(c) — Item 0 is accepted as reported: 16cb307 (Block 201 by transcribe_ruling.py; r803.txt
  equal to the architect's hash; the lane-line copy; the brief file equal to the architect's
  copy by sha256, bytes and lines; the plan +117 / −0; the rules files untouched), ledger
  0663a61 carrying two other lanes' rows, named. The pre-check, the three job pairs, the open
  check, the recount and R510 are accepted as reported. The five WR5 commits stand on the
  record as checked at 0b: docs only, seven new files.
R804(d) — claude-config at a93fc0b, nine commits past 79c4213, is accepted under R663(b): no
  row of this lane removed, primer.md lines 41–45 intact, the ninth commit the PI's rule.
  Finding 4 (five of another lane's ledger commits without trailers; one message's time behind
  its row's) is a reading for the cross-lane item, not this lane's to correct.
R804(e) — Readings, no row: finding 2 (the 12ag block names the closeout as HEAD; the block is
  its day's and is not edited; the brief's D1 governed); finding 5 (another lane's pytest
  before the open, none running at the snapshot); the model load of 2026-11-11 22:33:50Z,
  another lane's; NV_ERR_NO_MEMORY thirty-four this boot.
R804(f) — The open resumes at step 3e under the resume below. This ruling states no standing
  rule. Whether R797(d)'s hold becomes one is still the PI's and is not ruled here.
===== END r804.txt =====

================================================================================
SECTION 2 — RESUME [evidence-engine 12ag-OPEN-R1] under the ruling above, in this block
================================================================================

Same session, same task, same scratch directory (~/scratch/12ag-open/). MACHINE: DGX,
~/projects/evidence-engine. 12ag-OPEN's PERMISSIONS, ASSUMPTION LEDGER, CONSTRAINTS,
FINAL REPORT FORMAT and CONTRADICTION HANDLING stand with these changes: E7 is withdrawn
(R804(a)); the gate's tmux launch is allowed for this gate only (R804(b)); Touched gains
docs/session-reports/session-12/12ag_open/brief_12ag_OPEN_R1.md; I1's "clean" reads "clean
but for other lanes' uncommitted hook rows".

R0. date -u. Confirm evidence-engine HEAD 16cb307164c9203c695c3a4f842cf6d294c771ad
    (cat-file -e), clean, level; fingerprint IDENTICAL; claude-config at
    0663a61ed9cc7e0113cb4c8d4cf82528ce26ef96 or a descendant carrying only hook rows,
    reported. A mismatch = STOP. If the clock is past 06:30 UTC, or a hold would pass it,
    STOP before R1 and report; the gate then waits for a resume after 10:35 UTC.
R1. Step 3e as 12ag-OPEN wrote it, the hold included: a fresh snapshot (E5), then
    launch_gate.sh with evidence_dir ~/scratch/12ag-open/gate_12ag_open/ and session name
    12ag_open. EXPECTED and stops as written there.
R2. Steps 3f and 3g as written.
R3. Step 4 as written. The evidence commit also carries this resume as pasted,
    docs/session-reports/session-12/12ag_open/brief_12ag_OPEN_R1.md ("CLAUDE CODE" through
    the STOP-12AG-OPEN-R1 line, one trailing newline, written once, hashed straight
    after, reported): R784(g)(v) for a resume, which has no item 0 of its own. R804 is
    not written to disk here; it is transcribed at the next item 0.

HANDBACK: Stop and report in 12ag-OPEN's format, sections (4) onward, with R0 added and
  the resume file's sha256, bytes and lines. Do not start U30. Await the next brief or
  ruling in this session. STOP-12AG-OPEN-R1
