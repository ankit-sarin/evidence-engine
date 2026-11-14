CLAUDE CODE

================================================================================
SECTION 1 — RULING (R806, PI ruling 2026-11-14 UTC; not on disk). Transcribed at the
next item 0 after this brief's, not now. The lines between the markers are the byte
content of r806.txt when that item 0 writes it; the architect's hash of that content:
  sha256 ded7382da2d4f880f5b4f94c38813a1daebe00b22e10c058fba0fc8e485c9599,
  3227 bytes, 41 lines, one trailing newline.
================================================================================

===== BEGIN r806.txt =====
RULING [evidence-engine 12ag-R806] — the 12ag-N46A stop before item 0 is correct and is the
  architect's; the fourteen "Named by" lines accepted under R766(b); the rehearsal's forms
  accepted; claude-config 98fedf6 accepted; no standing rule; item 0 resumes on the real tree
STATUS: PI ruling, 2026-11-14 UTC, session 12ag, on architect recommendation, on the 12ag-N46A
  stop report (no commit; HEAD 60a63314118dc9f9ea41a009e5b5197676922b4c). Transcribe at the next
  item 0 after this brief's, by transcribe_ruling.py, in the block after R805's, not now.

ASSUMPTIONS IN THIS RULING (quoted from the stop report):
  MEASURED: "Regenerating STANDING_RULES.md moves 14 "Named by" lines that name no R805 clause";
    "evidence-engine is clean at 60a6331, level 0 0"; "r804.txt is
    d2ccb5ef7d253a04f541e37f790fbb5632a96863aa4b80ac623ff6ddc54b681e, 4,625 B, 55 lines"; "r805.txt
    is 28d71b14666662e8e1bddbb3e002dc8f59e8abbb6775e499fc80858d2a91125b, 5,227 B, 61 lines"; "The
    plan's numstat is 138 0"; "After: "Tier A 89, Tier B 8, 97 of 1677 clauses""; "Numstat:
    STANDING_RULES.md 53 / 21, glosses 3 / 1"; "id list 2 / 0"; "Four "Named by" lines that append
    an R805 clause: R86, R667(e), R677(f) and R766(b)"; "Fourteen "Named by" lines that append only
    ids from R785 to R804"; "Nothing else changed."; "claude-config is at 98fedf6, four commits
    past 89b4efd".
  READ (CC's): findings 1–4; the pattern commit d488573 and its form; the regeneration command
    with its two --glosses arguments; the two glosses as composed.
  READ (the architect's clone, plan :15105): R766(b), "a "Named by" line that only appends ids;
    any other changed line is a stop".
  INFERRED: none.

R806(a) — The stop is correct and is the architect's: the brief's I3 allowed only "Named by"
  lines that name R805, narrower than R766(b), which allows any that only appends ids. It is
  the error R766(a) records, repeated, with R766(b) cited in the same brief. The architect's
  errors this session: four — E7, I1's "clean", I3, and one "followed" wording given to both
  blocks (finding 2). None reached a commit.
R806(b) — The PI's ruling, in his words: "Accept the 14 lines and resume". The fourteen lines
  are accepted under R766(b): each only appends ids from blocks that landed with no
  regeneration. I3 is withdrawn and R766(b)'s classification stands in its place.
R806(c) — The rehearsal's forms are accepted as reported and are used on the real tree: each
  block's "beside" naming the other block first and its "followed" naming what followed that
  ruling in its own paste (the 12ag-OPEN-R1 resume for R804, the 12ag-N46A brief for R805), as
  d488573 has them; the regeneration command with both glosses files; the two glosses as
  composed, which keep the clauses' words.
R806(d) — claude-config at 98fedf6 is accepted under R663(b): three ledger-only commits of
  other lanes and one skills commit, none touching this lane's rows or primer.md lines 41–45.
  Finding 4 (earlier hash checks without the ^{commit} form) is a reading; R783(d)'s form is
  the one used from here, as this pre-check used it.
R806(e) — This ruling states no standing rule. Item 0 resumes under the resume below.
===== END r806.txt =====

================================================================================
SECTION 2 — RESUME [evidence-engine 12ag-N46A-R1] under the ruling above, in this block
================================================================================

Same session, same task, same scratch directory (~/scratch/12ag-n46a/). MACHINE: DGX,
~/projects/evidence-engine. 12ag-N46A's PERMISSIONS, ASSUMPTION LEDGER, CONSTRAINTS,
FINAL REPORT FORMAT and CONTRADICTION HANDLING stand with these changes: I3 is withdrawn
and R766(b)'s classification stands in its place (R806(b)); the block wording and the
regeneration command are the rehearsal's (R806(c)); Touched gains
docs/session-reports/session-12/12ag_N46A/brief_12ag_N46A_R1.md; R806 is not written to
disk here.

R0. date -u (outside 07:00–10:35 UTC, or hold as the brief's TIMING says). Confirm HEAD
    60a63314118dc9f9ea41a009e5b5197676922b4c (cat-file -e with ^{commit}), clean, level;
    I1; fingerprint IDENTICAL; claude-config at 98fedf6 or a descendant acceptable under
    R663(b), reported. The rehearsal's r804.txt and r805.txt in scratch still hash to the
    brief's values. A mismatch = STOP.
R1. Steps 1a–1c on the real tree, from the rehearsal: each ruling file written once and
    hashed straight after; block202.json and block203.json as rehearsed, their
    transcribed value the commit's UTC date, re-read at commit time; the two id lines;
    the two glosses as reported; the regeneration by the command the stop report
    printed. Expectation, reported if false: the regenerated STANDING_RULES.md and the
    plan are byte-equal to the rehearsal's.
R2. Step 1d as written: the brief file from 12ag-N46A's own paste, not from this one.
R3. Step 1e as written, with I3's assertion replaced by R766(b)'s: the check classifies
    every changed line of STANDING_RULES.md and allows the header's three hash and
    Selection lines, a new section for each id the list gained, and a "Named by" line
    that only appends ids; any other changed line = STOP. The check writes the
    classification into its own evidence file.
R4. Step 1f, then step 2 whole, as written. The read-out commit (2i) also carries this
    resume as pasted, 12ag_N46A/brief_12ag_N46A_R1.md ("CLAUDE CODE" through the
    STOP-12AG-N46A-R1 line, one trailing newline, written once, hashed straight after,
    reported): R784(g)(v) for a resume. R806 is transcribed at the next item 0.

HANDBACK: Stop and report in 12ag-N46A's format, with R0 added and the resume file's
  sha256, bytes and lines. Do not design or build. Await the next brief or ruling in
  this session. STOP-12AG-N46A-R1
