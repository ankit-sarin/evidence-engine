# 12i internal review, wave 1 — rules for every reader (read this fully before anything else)

You are one of twelve read-only reviewers of the evidence-engine repository. Two outside
assessments and an earlier triage (session 12f) already read parts of this code. Your lot is code
that NO outside assessment read. Your job is to read your lot's files end to end at HEAD and
report CANDIDATE DEFECTS — with the code quoted — not to fix anything. "I read it and found
nothing in this file" is a valid and valuable result; say it per file. Do not pad.

Repository: /home/ankitsarin/projects/evidence-engine   (HEAD b711683, clean, pushed)
Python:     /home/ankitsarin/projects/evidence-engine/.venv/bin/python
Your lot:   ~/scratch/12i-w1/inputs/<LOT>.md  — your files, any excluded block, the existing
            inventory rows that name your files, and the 12f triage sections that name them.
Project architecture notes: /home/ankitsarin/projects/evidence-engine/CLAUDE.md (read the
sections relevant to your files; its sentences are STATED GUARANTEES to check, see pattern i).
The plan: docs/plan/ENGINE_REFACTOR_PLAN.md is 850 KB — NEVER read it whole. Inventory rows are
table lines starting `| <ID> |` under "## Step 2"; grep for an ID to read a full row.
12f triage: docs/session-reports/session-12/12f_triage_rows.md (sections `### <ID> — …`).

## HARD LIMITS — read-only

- NO edit, creation, move or deletion of any file inside the repository. No git command that
  changes anything (status/log/show/grep/diff/blame are fine). The tree must be clean when you finish.
- NEVER create anything under the repo's `data/` or `tests/`.
- NEVER open `data/surgical_autonomy/review.db` (the live database) or any `*.bak-*` file. You
  do not need live data. Never construct `ReviewDatabase("surgical_autonomy")` or any
  `ReviewDatabase` whose files would land under the repo's `data/` — the constructor WRITES.
  Read its signature first and point it at your scratch directory.
- NO model call (no Ollama request of any kind, no cloud API), NO network call. Stub the boundary.
- NO `sudo`, no `systemctl start/stop/restart`, no crontab edits.
- Do NOT run the pytest suite or any test file. Reading tests is expected.
- Reproducers: optional, only on SYNTHETIC temporary databases or stubs, in
  `~/scratch/12i-w1/repro/<LOT>/`, run with cwd set to that directory and
  `PYTHONPATH=/home/ankitsarin/projects/evidence-engine`. Each a short self-contained script
  `<LOT>_<n>_<what>.py` that prints its result; save its output beside it as `<same>.out`.
  If a candidate cannot be shown by a short script inside these limits, do NOT stretch — record
  it as "code order" with the code quoted.
- Do not chase adjacent defects. If you notice one in a file OUTSIDE your lot, record it in your
  "Adjacent" section in two lines (file, function, what) and move on.
- Do not spawn sub-agents.

## What is already known — cite, do not re-find

1. The existing inventory rows and 12f triage sections listed in your lot file. If what you see
   IS one of those, do not report it as a candidate; list it under "Known, seen again" with the
   row ID. If you see something that goes BEYOND a row (same area, new consequence or new site),
   report it as a candidate that EXTENDS that row.
2. Where your lot file names an EXCLUDED function or block, that block was triaged in 12f: read
   it for context, report nothing that is located only inside it.
3. This list, quoted verbatim from the unified plan (v78 §2) — every item is already a row:

   **Known false or silent at HEAD** (each a pre-tag row; the stated text is corrected with its
   fix): "parse writes file + asset + ref in one transaction" (B-F01); "the refusal comes before
   any write" (B-F02); "restore refuses an open target" (B-F03); "a materialized quote is ANCHORED
   by construction" (A-10, INT-g6-1); "no partial extraction is stored" (A-11); "long runs wrap
   themselves in the experiment lock" (A-1); `truncate_paper_text`'s section prioritisation
   (B-F11); PRISMA "duplicates tracked externally" (B-F06); **the evidence table's snippet column
   and the judge loader's span are empty on every claim** — the located payload has no `snippet`
   key (INT-h12-1); **a claim stores only the first contiguous run of the units it cited**, the
   rest only in a telemetry sidecar (INT-g12-1); the methods text's "dual-pass" abstract
   screening is the same request sent twice (RD-6, folds into B-F07).

## The five patterns — check every file against each

(i)   a STATED GUARANTEE (a docstring, a comment, a CLAUDE.md sentence, generated methods text, a
      "refuses any …" / "never …" / "always …" claim) that the behaviour does not meet;
(ii)  a FAULT CONVERTED to a paper outcome, or swallowed (a broad except that turns an engine or
      environment error into "this paper failed / has no value", or logs and continues);
(iii) a WRITE SEQUENCE that is not one transaction, where a crash or exception between steps
      leaves a state no reader distinguishes from a legitimate one;
(iv)  PROVENANCE or INPUT IDENTITY not recorded, or recorded and never checked (which text,
      which model/digest, which config, which run produced a stored value);
(v)   a READER that drops or empties data while its tests pass — for these, NAME THE TEST that
      should have seen it and say why it did not (e.g. the fixture hand-builds the key the
      writer never writes).
A candidate may fit none of the five; report it anyway under pattern "other" if it can make a
stored result wrong, or a path fail.

## Evidence rules

- Quote the code that decides the matter, by CONTENT ANCHOR: file path + function/class name +
  the quoted lines. Never cite a line number. A grep hit is a location, not a semantics — read
  the function, and its callers where the claim depends on them, before describing what it does.
- Evidence level, exactly one: `reproduced` (you ran a reproducer and saw it) · `code order`
  (established by reading the code path; quote it) · `conditional` (real only under a stated
  condition you could not establish — state the condition).
- Before reporting, try to refute your own candidate: is there a guard upstream, a caller that
  never passes that input, a test that pins the behaviour as intended, a ruling (grep the plan
  for the function name) that accepted it as built? Say what you checked. A candidate you could
  not confirm is reported as `conditional`, not dropped and not inflated.
- Tests are evidence too: say whether a test covers the path, and name it.

## Defect classes (R508) — propose one, with a one-line reason

- Class 1 — integrity: can make any review's stored results, their provenance, or a human gate
  wrong or silent.
- Class 2 — function: a path fails loudly, refuses, or is unusable without corrupting anything.
- Class 3 — wording, telemetry, docs, test hygiene.
(Engine-wide: any review, any entry path — not only the live surgical_autonomy review.)

## Packages (R540) — propose one

P1 Run-7 path (pin-affecting first) · P2 durable state and instruments · P3 search front and
reporting · P4 screening and acquisition · P5 exports and the human importer · P6 migration
session · P7 the Class 3 batch.

## Output

Write ONE file: `~/scratch/12i-w1/readers/<LOT>.md`, with these sections in this order:

```
# <LOT> — reader report
## Files read
| file | lines | read whole? | candidates | note |
## Candidates
### <LOT>-C<n> — <short title>
- **File / function:** …
- **Pattern:** i | ii | iii | iv | v | other
- **Evidence level:** reproduced | code order | conditional (condition: …)
- **Evidence:** the quoted code by content anchor; the reproducer path and its output if any
- **What goes wrong:** the concrete trigger and the concrete consequence, in two or three lines
- **Refutation tried:** what you checked that could have made this a non-finding
- **Tests:** the test that covers or should have covered it
- **Existing row:** new | extends <ID> (how) | duplicates <ID>
- **Proposed class / package:** <1|2|3> / <P1..P7> — one-line reason
## Known, seen again
- <row ID> — <file/function> — one line
## Adjacent (outside this lot; not chased)
## Could not establish
```

Write the file as you go (after each file is read), not all at the end. Order candidates by
class (1 first). Be exact and compact: quote only the lines that decide the matter.

Your FINAL MESSAGE back must be compact — do NOT paste the report. Give only:
1. a table of your candidates: ID · file/function · pattern · evidence level · class/package ·
   new or extends/duplicates <row> · one-line statement;
2. the list of files you read whole, and any you did not finish (say why);
3. adjacent items in one line each.
