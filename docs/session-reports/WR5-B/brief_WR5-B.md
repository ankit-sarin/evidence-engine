RULING [evidence-engine WR5-A] — read-out accepted (f936933c949a679012ac5f4aafd23a90cbd5ffa5)
STATUS: accepted as reported. Workflow review item 5, outside unit
  numbering; this ruling carries no R number and is not transcribed
  into the decision log.
ASSUMPTIONS IN THIS RULING: none about the box.
  (a) Finding 2: the brief file is owed under R784(g)(v) (each brief
      committed as pasted). The one-file gate was the architect's and
      is withdrawn. WR5-B item 0 writes it.
  (b) Finding 4 (stale header of STANDING_RULES.md): a reading for the
      lane's next regeneration; no action in WR5.
  (c) Findings 1, 3, 5: the architect's premises, written from a
      description; the record governs.
  (d) Finding 6: applying the process check by hand is accepted.

TASK [evidence-engine WR5-B] — Prototype brief checker, measured on past briefs
PURPOSE: Code in scratch plus one evidence directory. Builds a prototype
  that checks a brief's text against mechanical brief-form rules, and
  measures how often each check fires on past briefs. No install, no
  rule change. The adopt decision is the PI's, after the report.
MACHINE: DGX    SESSION DIRECTORY: /home/ankitsarin/projects/evidence-engine
SKILL: none (lane tooling, not an app or deploy)
PERMISSIONS: auto mode, commit steps included  |  blast-radius: low
  Touches: docs/session-reports/brief_WR5-A.md (new);
  docs/session-reports/WR5-B/ (new: brief file, prototype copy, hits
  file, read-out); ~/scratch/wr5-b/. Reads: the plan, STANDING_RULES.md,
  committed briefs, ~/claude-session-archive/ (read only).
  STOP / DO NOT TOUCH: engine/, scripts/, tests/, review.db (no access
  of any kind), any settings or hook file, docs/session-reports/tools/,
  STANDING_RULES.md and its id list.

ASSUMPTION LEDGER (load-bearing claims only):
  MEASURED (WR5-A, docs/session-reports/WR5-A_readout.md; quoted):
    M1. "$ pwd -> /home/ankitsarin/projects/evidence-engine"
    M2. "git ls-files | grep -E '(^|/)brief[^/]*\.(md|txt)$' | wc -l"
        gave 19 committed brief files. A count of that day: recount.
    M3. Archived transcripts whose first cwd is this checkout, and user
        turns matching the scan pattern "TASK \[([^\]]*)\]\s*[—-]":
        recount both; do not carry the WR5-A figures.
    M4. Lane session tools live in docs/session-reports/tools/ and are
        "validated by committed evidence files, not by tests/".
  INFERRED — BLOCKING (verify before acting downstream; stop if false):
    B1. In ~/claude-session-archive/dc9f61b7-39e2-4cc6-ae86-dd391b660667.jsonl
        the user turns at the JSONL lines WR5-A section 4 tables for
        the five briefs that ran into a U18 stop each open with a TASK
        title naming 12y-U18B or 12y-U18C. Read the lines from that
        table; quote each title.
  INFERRED — expectation (report a mismatch, continue):
    X1. HEAD is the WR5-A read-out commit named in the ruling title, or
        a later lane commit.
    X2. The prototype will hit few of the five stop briefs. To be
        measured; no stop rests on it.

PROBLEM / CONTEXT:
  WR5-A found that briefs are committed as files and that nothing
  checks them. Before a checker is proposed to the lane, the architect
  needs its hit rate on past briefs: does it flag briefs that led to
  stops, and does it wrongly flag briefs that ran clean.

CONTRACT:
  Item 0 (two commits, in this order).
    0a. Write docs/session-reports/brief_WR5-A.md once from the WR5-A
        paste in this session's transcript, in R784(g)(v)'s form. Report
        sha256, bytes and line count. Commit it alone.
    0b. Write docs/session-reports/WR5-B/brief_WR5-B.md the same way
        from this paste. Report the same three values. Commit it alone.
  Step 1. Verify B1. Recount M2 and M3.
  Step 2. Build the prototype in ~/scratch/wr5-b/: one Python script,
    standard library only, read-only on everything it inspects. Input:
    a brief's text, and optionally a repository path and a commit. For
    each hit it emits: check id, line number, matched text (at most
    200 characters). Exit status is non-zero when any check hits.
    The checks. Each names the clause it serves; read the clause in
    STANDING_RULES.md before writing the check and quote it in the
    script's comment for that check. A check whose clause you cannot
    turn into a text test without judgement is reported as NOT BUILT,
    with the reason; do not approximate it silently.
    Text-only:
      K1  a full commit hash that is not 40 hexadecimal characters
          (R730(c), hash length)
      K3  a pipe or tee inside a check-and-commit chain
          (R720(c), chain form)
      K4  a repository script run with an interpreter other than
          .venv/bin/python (R656(d), interpreter)
      K5  a deletion command on a path outside ~/scratch/
          (R658(c), deletions)
      K6  a process stopped by pattern, not by pid (R697(b), pkill)
      K7  an ad-hoc pytest run with no --basetemp (R676(d)(iii))
      K8  a Touched list naming a path under engine/ and not
          docs/inventory/ (R705(c), R725(c), inventory in Touched)
      K9  a numeral inside an INFERRED item of the assumption ledger
          (R667(e), an INFERRED item names only the fact)
      K10 a predicted count: "exactly N", or a stated "−" count
          (R723(a), R750(c), R772(c), counts are computed)
    Needing the repository (run only when a path and commit are given,
    against the tree at that commit, through git, with no checkout):
      K2  a 40-character hash that does not exist (R783(d))
      K11 a READ or MEASURED item whose quoted text is not found,
          whitespace-normalised (R756(b)), in the file it cites
          (R733(a), quotes come from the source)
      K12 a named repository path that does not exist and that the
          brief does not declare as new (R731(b), names from a summary)
      K13 a run of twelve or more consecutive words shared with a
          clause in STANDING_RULES.md (R784(g)(i), cite, do not restate)
      K14 a cited rule id absent from the plan (the architect's own
          addition; no clause)
  Step 3. Negative controls. For every built check, one planted
    fragment in scratch that must hit and one near-miss that must not.
    A check with no passing control pair is reported UNPROVEN and is
    left out of step 4's totals.
  Step 4. Measure, on three sets.
    S1. The five stop-brief turns of B1. Text-only checks.
    S2. Every committed brief file (M2's recount). All checks; the
        repository checks run against the tree at the commit that
        added that brief file.
    S3. Every archived brief turn (M3's recount). Text-only checks.
        Split by month. Where the plan's next ruling title for that
        brief contains "stop" or "accepted", split by that word too,
        and say how many briefs could not be matched to a title.
    Write every hit on S1 and S2 to hits file(s) in the evidence
    directory, verbatim. For S3 write counts only.

ACCEPTANCE GATES (this task only):
  G1. Working tree differs from step 0 only by the files named under
      Touches. Expected: no other path.
  G2. Every count is followed by the command or script call that
      produced it. Expected: none stated from reading.
  G3. Every built check has a control pair, hit and near-miss, with
      the output shown. Expected: all pass, or UNPROVEN stated.
  G4. No hit is classified true or false by CC. Expected: hits are
      printed; the architect reads them.
  G5. The prototype copy committed in the evidence directory has the
      sha256 of the script that produced the report's numbers.

OUT OF SCOPE:
  Installing the checker anywhere. A hook. Any edit to a rule, the id
  list, CLAUDE.md or a tool under docs/session-reports/tools/. Tuning
  a check to reduce hits after seeing them: report first.

CONSTRAINTS (expire with this brief):
  No model load. The archive is read only. Scratch under
  ~/scratch/wr5-b/ only. If a check design needs a choice the clause
  does not settle, make the narrower reading and say so.

FINAL REPORT FORMAT:
  1. Item 0: both brief files' sha256, bytes, lines, commit hashes.
  2. B1 verdict with the five quoted titles; recounts for M2 and M3.
  3. A table: check id, clause, built / NOT BUILT / UNPROVEN.
  4. A table per set: check id, briefs hit, briefs total. For S3 the
     month split and the stop/accepted split.
  5. For S1: per stop brief, the list of check ids that hit.
  6. Paths of the hits files; the read-out's path and commit hash.
  7. Anything that contradicts this brief.

CONTRADICTION HANDLING:
  B1 false: STOP and report. An expectation mismatch, or a finding
  outside the changed path: report and continue. A premise of this
  brief that does not survive contact with disk is a finding.

HANDBACK:
  Stop and report. Do not install, tune or extend the checker without
  explicit sign-off. Await the next brief or ruling in this session.
  STOP-WR5-B
