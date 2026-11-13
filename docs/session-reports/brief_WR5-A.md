TASK [evidence-engine WR5-A] — Read-out for mechanical brief checks
PURPOSE: Read-only read-out. Operational, no new code. Measures the facts
  needed to design a checker that rejects a brief carrying values not
  quoted from the record. Workflow review item 5, outside unit numbering.
MACHINE: DGX    SESSION DIRECTORY: the evidence-engine checkout this
  session is open in (path not known to the architect; step 0 confirms)
SKILL: none (read-only inspection; no deploy or app pattern involved)
PERMISSIONS: auto mode  |  blast-radius: low
  Touches: one new report file. Reads: repo docs, the decision log,
  session archives, Claude Code settings (read only).
  STOP / DO NOT TOUCH: no engine code, no review.db access of any kind,
  no settings or hook changes, no test hook installed.

ASSUMPTION LEDGER (load-bearing claims only):
  MEASURED: none.
  READ: none.
  INFERRED — BLOCKING (verify before acting downstream; stop if false):
    B1. This checkout is the evidence-engine repo and contains
        docs/plan/STANDING_RULES.md.
  INFERRED — expectation (report a mismatch, continue):
    E1. STANDING_RULES.md holds 95 clauses and is generated from the
        decision log (from a PI note dated 2026-11-11, not a task report).
    E2. The decision log holds entries R722(a), R723(a), R724(a),
        R725(b), R727(a) and R728(e): the six stops of session 12y on
        unit U18 (from architect memory).
    E3. Brief text is not stored as files in the repo; it survives, if
        at all, as the opening prompt in session transcripts or archives.
    E4. The installed Claude Code supports a hook that runs when a
        prompt is submitted and can reject that prompt (architect's
        general knowledge, unverified on this box).

PROBLEM / CONTEXT:
  Briefs have carried counts, file lists and code behaviour predicted
  from descriptions instead of read from the record. The rules against
  this are prose. Before any checker is designed, the architect needs
  measured facts on five things: the rule text, where brief text exists
  on disk, the stop records, what checks already exist, and whether a
  pasted prompt can be rejected automatically.

CONTRACT (numbered sections of the report; quote, do not paraphrase):
  0. Step 0. Print pwd, branch, HEAD and working-tree state. Verify B1.
     State whether a run is in progress, by this lane's own method.
  1. Rule home. Path of STANDING_RULES.md; the clause count as computed
     by a command (print the command and its output); the generator
     script path and its source file, quoting the lines that show it;
     the CLAUDE.md lines that point to the rules file.
  2. Brief-related clauses. Run one case-insensitive search of
     STANDING_RULES.md for these terms: brief, EXPECTED, Touched,
     census, quote, ruling, pipe, block, /wrap, INFERRED. Print the
     command, then every matching clause with its id and full text
     verbatim, and the computed count. Add no classification. If you
     see clauses about brief form that the search missed, list them
     separately under the heading "by eye".
  3. Brief text on disk. Search the repo, docs/session-reports, the
     session-archive output location and the Claude Code transcript
     store for stored brief text. Report locations as path patterns
     with file counts. For three examples, print only the section
     header lines of the brief, to show how regular the format is.
  4. Stop corpus. For each id in E2, quote the decision-log entry in
     full and give its path and line. For each, say whether the brief
     that triggered the stop is recoverable verbatim, and its path.
     Then list up to ten briefs from the same period that ran with no
     stop, by path only.
  5. Existing checks. Quote any hooks section in the Claude Code
     settings files that apply to this repo (user, project, local), any
     git hooks, any CI config, and any script that lints docs or briefs.
     "None found" with the paths searched is a valid answer.
  6. Prompt hook capability. Print the Claude Code version. From a
     source readable on this box (built-in help, bundled docs, the
     settings schema), quote what shows whether a prompt-submit hook
     exists, what input it receives and how it rejects a prompt. If no
     readable source settles it, write UNPROVEN. Do not install a hook.
  7. Tooling conventions. List the directory that holds this repo's
     tool modules (the one db_fingerprint lives in), the Python version,
     and how existing tools are invoked and tested, quoting one example.

ACCEPTANCE GATES (this task only):
  G1. Working tree differs from step 0 only by the report file.
      Expected: one new file.
  G2. Every count in the report is followed by the command that
      produced it. Expected: no count stated from reading.
  G3. Sections 1, 2 and 4 contain verbatim quotes with path and line.
      Expected: no paraphrased rule or log entry.
  G4. Every item marked E1 to E4 is answered as holds, fails or
      UNPROVEN, with the deciding quote.

OUT OF SCOPE:
  Designing or writing a checker. Proposing rule changes. Editing
  STANDING_RULES.md, the decision log, CLAUDE.md or any settings file.

CONSTRAINTS (expire with this brief):
  Read-only apart from the report. Write the report where this lane's
  rules place read-outs and commit it under those rules; if a rule
  forbids a commit now, leave the file uncommitted and say so. Print
  no brief body text beyond the header lines asked for in section 3.

FINAL REPORT FORMAT:
  Sections 0 to 7 as numbered above, then: the E1 to E4 verdicts in a
  four-line list; the report path and commit hash (or "uncommitted" with
  the reason); anything found that contradicts this brief.

CONTRADICTION HANDLING:
  B1 false: STOP and report. An expectation mismatch, or a finding
  outside this read-out's scope: report and continue. A premise of this
  brief that does not survive contact with disk is a finding.

HANDBACK:
  Stop and report. Do not start any checker work without explicit
  sign-off. Await the next brief or ruling in this session.
  STOP-WR5-A
