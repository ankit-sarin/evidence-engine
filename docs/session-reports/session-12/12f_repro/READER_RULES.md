# 12f verification read — rules for every reader (read this fully before anything else)

You are verifying findings from two OUTSIDE assessments of the evidence-engine repository against
the code as it is at HEAD. The assessments were written against an unknown earlier snapshot, so
every claim in them is INFERRED until you have read the code yourself. Their line numbers are
search hints only. Your job is to establish, per item, whether the defect is present at HEAD and
at what evidence level — not to fix anything, and not to agree with the assessment by default.
A finding that is NOT present, or only partly present, is as valuable a result as a confirmed one.
Say plainly what you could not establish.

Repository: /home/ankitsarin/projects/evidence-engine  (HEAD 297667c, clean, pushed)
Python:     /home/ankitsarin/projects/evidence-engine/.venv/bin/python
Originals (the governing text for each finding — read your items there first, in full):
  docs/session-reports/session-12/outside/assessment_B_17_findings.md   (F01–F17)
  docs/session-reports/session-12/outside/assessment_A_architectural.md (A's claims)
Existing inventory rows live in docs/plan/ENGINE_REFACTOR_PLAN.md, "Step 2 — Problem inventory"
(table rows start `| <ID> |`; grep for them — the file is 740 KB, never read it whole).

## HARD LIMITS — this session is read-only

- NO edit, creation, move or deletion of any file inside the repository. No `git` command that
  changes anything (status/log/show/grep/diff are fine). The tree must be clean when you finish.
- NEVER create anything under `data/`. NEVER put anything under `tests/`.
- NEVER open `data/surgical_autonomy/review.db` (the live database) or any `*.bak-*` restore point,
  unless your task explicitly says so — and then ONLY as
  `sqlite3.connect("file:<path>?mode=ro", uri=True)`. Never `immutable=1`. Never construct
  `ReviewDatabase("surgical_autonomy")` or any `ReviewDatabase` whose files would land under the
  repo's `data/` — the constructor WRITES (it creates dirs, sets WAL, runs migrations). Read the
  constructor's signature first and point it at your scratch directory.
- NO model call (no Ollama request of any kind, no cloud API), NO network call (no PubMed, OpenAlex,
  Unpaywall, publisher, pip, or any HTTP). Stub or fake the boundary instead.
- NO acquisition or search tool run against anything. NO `sudo`, `systemctl start/stop/restart`,
  crontab edits. (`crontab -l` and `systemctl show/cat` are reads and are fine if your task needs them.)
- Do NOT run the pytest suite or any test file (a gate run is in progress elsewhere; reading tests is fine).
- Reproductions: only on SYNTHETIC temporary databases or stubs, in YOUR scratch directory
  `~/scratch/12f/<your-tag>/`, run with the cwd set to that directory and
  `PYTHONPATH=/home/ankitsarin/projects/evidence-engine`. Keep each reproducer a short,
  self-contained script (`~/scratch/12f/repro/<ID>_<what>.py`) that prints its result; save its
  output beside it as `<same name>.out`. If a finding cannot be shown by a short script inside
  these limits, do NOT stretch — record it as "code order" with quoted anchors.
- Do not chase adjacent defects. If you notice one while reading, write a separate short row file
  for it (ID `INT-<your-tag>-<n>`, source "internal-12f", class stated) and move on.

## Evidence rules

- Quote the code that decides the matter, by CONTENT ANCHOR: file path + function/class name +
  the quoted lines. Never cite a line number. A grep hit is a location, not a semantics — read
  the function before describing what it does.
- Evidence levels (pick exactly one per item):
  `reproduced` (you ran it at HEAD and saw it) · `code order` (established by reading the code
  path; quote it) · `conditional` (real only under a stated condition you could not establish) ·
  `not present at HEAD`.
- "Present at HEAD": yes / partial / no. If an assessment bundles several sub-claims, give a
  verdict per sub-claim.
- Check stated guarantees against behaviour: docstrings, comments, CLAUDE.md sentences and
  "refuses any …" claims that the code does not honour are part of the finding.
- For each overlap pairing you are given with an existing inventory row, read the row's text in
  the plan and CONFIRM or REJECT the pairing (same defect? broader? narrower? different?).

## Defect classes (assign one, with a one-line reason)

- Class 1 — integrity: can make any review's stored results, their provenance, or a human gate
  wrong or silent.
- Class 2 — function: a path fails loudly, refuses, or is unusable without corrupting anything.
- Class 3 — wording, telemetry, docs, test hygiene.
(Engine-wide: any review, any entry path — not only the live surgical_autonomy review.)

Size: S = one commit, mechanism already read · M = one brief with a read-only Phase A first ·
L = multi-commit, migration-dependent or multi-session.

## Output — one file per item: `~/scratch/12f/rows/<ID>.md`, exactly these headed fields

```
### <ID> — <short title>
- **Source:** outside A | outside B | architect-derived | existing row | internal-12f
- **Present at HEAD:** yes | partial | no   (per sub-claim where bundled)
- **Evidence level:** reproduced | code order | conditional | not present at HEAD
- **Evidence:** quoted code by content anchor, and/or the reproducer's path, command and output
- **Overlap:** each given pairing: CONFIRMED / REJECTED + one line why; any other row you found
- **Proposed class:** 1 | 2 | 3 — one-line reason against the definitions
- **Owning state:** pre-tag (R512) unless you see a reason to flag otherwise (say why)
- **Minimum loud-failure fix:** the smallest change that turns a silent wrong result into a refusal or a human review
- **Full fix:** in two or three lines
- **Acceptance test:** what must be observed
- **Size:** S | M | L — why
- **PI-decision flag:** no | yes — the question, stated so it can be answered in one line
- **Differences from the original / the brief's note:** anything the assessment or the note got wrong or that has changed
```

Write each row file as soon as that item is finished (do not batch them at the end). Be exact
and compact: quote only the lines that decide the matter. Your final message back should be a
table of your IDs with: present · evidence level · class · size · one-line verdict — plus a list
of anything you could not establish and any internal-12f rows you wrote. Do not paste the row
files back; they are on disk.
