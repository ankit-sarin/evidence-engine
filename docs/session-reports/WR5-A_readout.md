# WR5-A — read-out for mechanical brief checks

Task `evidence-engine WR5-A` (workflow review item 5, outside unit numbering). Read-only. Written 2026-11-13 by Claude Code,
session `c4f7c3c7-8e58-4a82-9b30-25d20e8fb125`. No engine code, `review.db`, settings or hook was touched. Every count below is
followed by the command that produced it. Line numbers are as of HEAD `7ddabca`, and a content anchor (a clause id or a
quoted line) is given beside each one.

## 0. Step 0

```text
$ pwd                         -> /home/ankitsarin/projects/evidence-engine
$ git branch --show-current   -> main
$ git rev-parse HEAD          -> 7ddabca71511374aacdaefdd95a6de4eb651a268
$ git status --porcelain=v1 | wc -l   -> 0        (clean; `git status -sb` -> "## main...origin/main", level)
$ ls docs/plan/STANDING_RULES.md      -> docs/plan/STANDING_RULES.md
```

**B1 holds.** This is the evidence-engine checkout, and `docs/plan/STANDING_RULES.md` exists.

**Run in progress: none.** I used this lane's own predicate, `session_open_check.py`'s `cc_sessions` section
(`docs/session-reports/tools/session_open_check.py`, the comment line
`# this lane's kind (R750(b)): the repository's path or its .venv, pytest, or the gate wrapper`). I also checked
the experiment lock and the Ollama `ps` endpoint. I did not run the whole script, because it fingerprints `review.db`
and this brief forbids any access to that database.

```text
lane processes (repo path | pytest | run_gate.sh, self and ancestors excluded): 0     [ps -u $USER scan, needles assembled at runtime]
$ grep -c ":$(stat -c %i ~/.ollama_experiment.lock) " /proc/locks   -> 0   (no holder of the experiment lock)
$ curl -s http://127.0.0.1:11434/api/ps                              -> {"models":[]}
$ tmux ls                                                            -> no server running on /tmp/tmux-1000/default
```

## 1. Rule home

- Path: `docs/plan/STANDING_RULES.md`.
- Clause count:
  ```text
  $ grep -c '^## R' docs/plan/STANDING_RULES.md      -> 95
  $ grep -c '^```text$' docs/plan/STANDING_RULES.md  -> 95     (one fence per clause)
  $ grep -cE '^[AB] ' docs/plan/standing_rules_ids.txt -> 95   (A: `grep -cE '^A '` 87, B: `grep -cE '^B '` 8, D: `grep -cE '^D '` 5)
  ```
- Generator and source, quoted from `docs/plan/STANDING_RULES.md:5–8`:
  ```text
  - Generator: `docs/session-reports/tools/extract_rules.py`, sha256 `fc95e62a246a4571295a99180f61b93dd10fc11a528f98d4320dcb8bb82bd7f2`
  - Plan blob: `docs/plan/ENGINE_REFACTOR_PLAN.md`, sha256 `fb14a3fd3270ab89894bf87ce1a90bc7d7b9d97b04dbf6eafedf9c11e74f155c`
  - Id list: `docs/plan/standing_rules_ids.txt`, sha256 `0657ba83ca1dd54871ed7634a866cc9de3c5286038b46d81f8911fa6dd2bcd62`
  - Selection: the id list: Tier A 87, Tier B 8, 95 of 1524 clauses in the log.
  ```
  `docs/session-reports/tools/extract_rules.py:2`:
  ```text
  """Extract the decision log's standing-rule clauses into docs/plan/STANDING_RULES.md (R746(e)(i)).
  ```
  `docs/session-reports/tools/extract_rules.py:8` and `:19–20`:
  ```text
    extract_rules.py --plan <plan.md> --ids <ids.txt> --glosses <glosses.json> [--glosses <more.json>] --out <rules.md>
  With --ids the selection is a committed list, not the phrase (R747(c)). One entry per line, blank lines and "#" comments
  allowed: "A <id>" and "B <id>" select a clause into Tier A or Tier B; "D <id>" records a drop, so that a gloss for it in
  ```
  `sha256sum` today: the generator is `fc95e62a…d7f2` and the id list `0657ba83…2bcd62`, both equal to the header. The plan
  is **`328edeff…1e2d`, not the header's `fb14a3fd…`**. The header's plan blob is the one at `2b0badd` (`git show
  2b0badd:docs/plan/ENGINE_REFACTOR_PLAN.md | sha256sum` → `fb14a3fd…5c`), the 12ae-U24A commit that last regenerated the file.
- Regeneration check, read-only, written to scratch:
  `python3 docs/session-reports/tools/extract_rules.py --plan docs/plan/ENGINE_REFACTOR_PLAN.md --ids docs/plan/standing_rules_ids.txt --glosses docs/session-reports/session-12/12aa_SETUP/rules_glosses.json --glosses docs/plan/standing_rules_glosses.json --adopted R754 --out <scratch>/rules_regen.md`
  printed `95 clauses of 1655`. In `diff` against the committed file, only line 6 (the plan sha), line 8 ("95 of 1524" →
  "95 of 1655") and 18 "Named by:" lines differ. **No clause text differs.** The committed file is current in its rules and
  stale in its metadata.
- CLAUDE.md pointer (`CLAUDE.md:3–7`):
  ```text
  Read `docs/plan/STANDING_RULES.md` at session start: the standing rules of this lane,
  each a decision-log clause quoted verbatim with a gloss that rules nothing, adopted under
  R754. A figure, date or session condition inside a quoted clause is that day's; the
  current value is the "Expected values at session-N open" block's. Read
  `docs/plan/ENGINE_REFACTOR_PLAN.md` in the scope R751(c) sets — its Status table, the
  ```
  (`grep -n "STANDING_RULES" CLAUDE.md` → line 3 only.)

## 2. Brief-related clauses

Command: one case-insensitive regex search over the text inside each clause's ` ```text ` fence. A clause is the fence
under a `## R…` heading. Glosses and the preface are not searched.

```python
terms=["brief","EXPECTED","Touched","census","quote","ruling","pipe","block","/wrap","INFERRED"]
pat=re.compile("|".join(re.escape(t) for t in terms),re.I)   # applied to each ```text fence of docs/plan/STANDING_RULES.md
```

The line given is the clause's first line in `docs/plan/STANDING_RULES.md`. The computed count is at the end of the
list. For scale, `grep -ci -- <term> docs/plan/STANDING_RULES.md` gives these line counts over the whole file, glosses
included: brief 106, EXPECTED 19, Touched 13, census 19, quote 19, ruling 39, pipe 4, block 39, /wrap 0, INFERRED 5.

**R86** — `docs/plan/STANDING_RULES.md:18`

```text
R86 — **The no-live-write window is 07:00–10:35 UTC**: the 07:00 health check, the 09:00 nightly suite, and the 10:30 `dgx-snapshot-user` timer with its 2-minute jitter (row I15). Appendix B and CLAUDE.md's ops block carry the new window. *(Closeout note, 2026-09-23: the plan's addenda carry it; Appendix B lives in the unified plan, and the CLAUDE.md half was not applied at this closeout — see the I13 stop in the session-7b closure paragraph)*
```

**R370** — `docs/plan/STANDING_RULES.md:42`

```text
R370 — **The no-push reversal (issued outside a block, in the 12a-CLOSE brief).** "Reversal recorded here: the "no push until the freeze" instruction in 12a-0b-R2 and later briefs is withdrawn; sessions close pushed and measured at origin, as before." *Transcribed:* evidence-engine pushed at this closeout; claude-config by the 12a wrap
```

**R647(c)** — `docs/plan/STANDING_RULES.md:66`

```text
R647(c) — From the 12p closeout, every "Expected values at session-N open" block states the
  temp root at the open as the nightly's basetemp directory (one pytest-N directory plus the
  pytest-current link), with the size as a reading not matched, never as "empty". What a
  closeout does with the root is ruled at that closeout, as R645(d) was.
```

**R654(d)** — `docs/plan/STANDING_RULES.md:81`

```text
R654(d) — From this ruling on, "another lane's rows found uncommitted in PROJECT_LEDGER.md at a
  session open" is case B's neighbour, not case C: they are committed by this lane's next
  ledger step, named in its message, and recorded in the session's closure. A dirty
  claude-config for any other reason (edits to primer.md or any file other than
  PROJECT_LEDGER.md, a non-row change inside PROJECT_LEDGER.md, divergence from origin, this
  lane's rows missing) stays a STOP.
```

**R655(b)** — `docs/plan/STANDING_RULES.md:98`

```text
R655(b) — "0/0 against <file>" means, here and in every later expected-values block, the set
  difference in both directions between the gate's collected ids and the file's ids: 0 ids
  added, 0 removed, computed on sorted lists. Order is not compared.
```

**R656(b)** — `docs/plan/STANDING_RULES.md:112`

```text
R656(b) — The cell rule from this ruling on: in a decision-log table cell, "|" is written
  "\|", and a backslash immediately before a pipe is written "\\". A backslash anywhere else
  is written as it is, as the four earlier cells did. Earlier cells are left as printed
  (R102).
```

**R656(d)** — `docs/plan/STANDING_RULES.md:127`

```text
R656(d) — The interpreter is .venv/bin/python; the first fingerprint attempt's exit 127 is a
  reading, and the 22:43:00Z run is the one of record. Briefs from here name
  .venv/bin/python for every repository script. No row.
```

**R658(b)** — `docs/plan/STANDING_RULES.md:141`

```text
R658(b) — The seven `review.db` files below `data/<dir>/` depth are row I21's seven nested
  paths (debris_2026-09-09 and the elicit_design01 smoke copies), already recorded and owned
  by P7. The plan's wording ("No second data/<dir>/review.db") governs; the brief's `find`
  literal was the architect's. From here the check is written as `ls data/*/review.db` and
  expects one path. No row.
```

**R658(c)** — `docs/plan/STANDING_RULES.md:157`

```text
R658(c) — The refused `rm -rf /tmp/pytest-of-ankitsarin`: nothing ran, and it is recorded as
  the architect reads it — the brief's DO NOT TOUCH was the constraint, and the classifier's
  refusal is not a guard this lane relies on. From here, no helper command composed for a
  brief deletes any path outside ~/scratch/<session>/; a deletion elsewhere is its own named
  step with the PI's ruling. No row.
```

**R661(e)** — `docs/plan/STANDING_RULES.md:203`

```text
R661(e) — Notes, no rows: F2, the throwaway id ra_12q is accepted and is the name the
  rehearsal's read-out uses. F3, the lock's state is read through /proc/locks and fuser,
  never by a probe acquisition, in any brief that forbids taking the lock; the I7 wording
  "flock -n probe" is withdrawn for such briefs. F4, 12g's source string in ra_12q's import
  manifest (build_input.py's hard-coded source) is recorded in the rehearsal read-out's
  first section beside the confounder statement; a reader goes by the eligibility events'
  actor path. F5, the sidecars travel with the directory. The amended-before-push ledger
  commit is within the rule (never amend after pushing); hook commits carry the row's exact
  timestamp from here.
```

**R662(a)** — `docs/plan/STANDING_RULES.md:223`

```text
R662(a) — The 12q-CLOSE closeout block is accepted as printed. The block's element list (the
  12q-CLOSE brief's step 9) is the closeout block's form from here; the 12p block's form was
  in an archived transcript, not on disk (F7), and this ruling makes the on-disk form the
  one that governs. The retention file's appended "Staged, not retained" section is accepted:
  the 12p portion is byte-identical and the staged line is below it, as ordered.
```

**R662(e)** — `docs/plan/STANDING_RULES.md:239`

```text
R662(e) — Notes, no rows: F1, the mode=ro fingerprint of the staged copy is accepted —
  review.db and its fingerprint unchanged, the sidecar mtime moved; the restore checks by
  the two hashes (R661(b)); from here a staged or retained copy is read by file hash only,
  and a fingerprint of it is its own named step. F2, both request sets are recorded in the
  12q closure's journal attribution as CC's inference; 12r-OPEN's bracketing reads attribute
  their own window as usual. F3, five scratch directories (12q-close/ included) is the count
  the block records. F4, dbchecks_py_sha256.txt stays. F5, the gitignored __pycache__/ in
  12q_close/ is left as found. F6, pytest-1's origin unknown is a reading.
```

**R664(b)** — `docs/plan/STANDING_RULES.md:275`

```text
R664(b) — R656(c) as applied from here: a plan commit that carries N dated notes inside table-row
  cells has exactly N "−" lines, each verified by prefix_check.py's rule; a commit that carries no
  cell note (an appended block, a note paragraph after a closure, a new file) is verified by its "−"
  count alone, which must be 0. Item 0 of 12r is the second kind. Acceptance gate 2 of 12r-OPEN reads:
  0 "−" lines in the plan diff, measured by the staged diff.
```

**R666(c)** — `docs/plan/STANDING_RULES.md:291`

```text
R666(c) — Three host findings, each a dated paragraph in the 12r closure and in the 12r host-state
  record, no row (host, not engine):
  (i) the 2026-10-17 stall (18:56 → 22:49 UTC, memory-pressure lines only, no model loaded) and
      the reboot at 23:03:59 UTC; cause not this lane's to find;
  (ii) earlyoom as installed 2026-10-18 05:43 UTC with --prefer ^ollama$ (SIGTERM ≤ 6%, SIGKILL ≤ 3%
      available memory), ollama.service oom_score_adj 0, vm.min_free_kbytes 1048576 — from here a
      launch brief states these in its confounder statement (R650(d)) and records available memory
      and GPU memory at launch and at exit; an earlyoom kill of ollama during a run is a run fault
      (U02's ConnectionError path), the run is reported as stopped and is not re-launched without a
      ruling;
  (iii) the two NV_ERR_NO_MEMORY lines of the 2026-10-17 07:00 job under the old health-check design
      (num_ctx 262144, keep-alive forever) with every model reporting OK: recorded; the 10-18 run's
      journal is grepped for NVRM lines by the restore brief below (2(c)); a dgx-infra item, not a
      row.
```

**R667(e)** — `docs/plan/STANDING_RULES.md:316`

```text
R667(e) — I5: the premise ("CLAUDE.md carries … watchdog 900 s") was an architect's embellished
  value; the fact that mattered (the section exists and names the watchdog and restart paths) held.
  CC's proceed is accepted this once and recorded; from here an INFERRED item names only the fact that
  must hold, and a brief that wants a value checked names it as a separate reading. The global
  CLAUDE.md's "300s" stays on the HK-ARCH line (F5, R650(g)).
```

**R672(f)** — `docs/plan/STANDING_RULES.md:332`

```text
R672(f) — Notes, no rows: the baseline recount runs on the plan at d7bafc4 by sha256, as the 12r
  script's header says — the chain form from here; the -shm mtime movement is F1's effect; the
  seven scratch directories are the count the block records.
```

**R673(a)** — `docs/plan/STANDING_RULES.md:346`

```text
R673(a) — The 2(d) clause "line count = 7,168 + the '+' count" was an architect premise that ignored
  the "−" line the same brief required; withdrawn. The measured form is the rule from here: a plan
  commit's line count is the prior count plus its "+" lines minus its "−" lines (7,168 + 63 − 1 =
  7,230). The stop was correct, at the earliest point that left both repositories clean.
```

**R676(c)** — `docs/plan/STANDING_RULES.md:379`

```text
R676(c) — The extra docs commit 9f1809a (variant_measurement.md before any engine edit) is accepted
  and is the form from here: a brief's conditional proceed commits its measurement and the branch it
  states before the edit that depends on it. The variant JSON landing with the read-out is a
  reading, no action.
```

**R676(d)** — `docs/plan/STANDING_RULES.md:394`

```text
R676(d) — Notes, no rows: (i) F5 — docs/architecture/state-machine.md's invalid_snippet line is
  stale for audit_span since 7074305; onto the HK-ARCH line (Class 3, P7). (ii) F6 — the 12s_U14A
  reproducer and variant_measurement.py are bound to locator-1 by design and are not edited;
  12s_U14B/post_fix_check is the locator-1.1 instrument; a dated note on INT-i12-7 with its CLOSED
  note. (iii) F7 — ad-hoc pytest runs rotated three gate basetemps out of the shared temp root;
  nothing deleted by hand; each basetemp had been read at its gate (R660(c)); from here an ad-hoc
  pytest run in a brief passes --basetemp under the session's scratch directory, so the shared
  root holds gates' and the nightly's directories only. (iv) I3 — docs/inventory/ censuses no test
  file; a dated note on B11; the 12s_U14A AST census is the instrument until the inventory does.
```

**R678(f)** — `docs/plan/STANDING_RULES.md:430`

```text
R678(f) — Notes, no rows: the recount chain now runs 12q → 12r → 12s by sha256 and is the form from
  here, one script per ruling that opens rows; the 12i L03 reproducer stays in scratch,
  uncommitted, and 12s_U15A/'s is the instrument; the six ruling files of this session live where
  the closeout tables them.
```

**R683(b)** — `docs/plan/STANDING_RULES.md:489`

```text
R683(b) — The element-order record: the 12t-CLOSE brief's I1 named two places that did not hold it
  (an architect premise); the record is the archived 12s transcript, now committed as
  12t_close/closeout_12s_found.txt. From here, docs/session-reports/session-12/12t_close/
  closeout_12s_found.txt is the on-disk record of the closeout block's element order that R678(a)
  governs, and a closeout brief names it. No row.
```

**R684(e)** — `docs/plan/STANDING_RULES.md:536`

```text
R684(e) — The item-0 transcription check is restated: a brief's 2a self-diff confirms the copy,
  not the transcription (CC's finding 4, an architect premise). From here the ruling that accepts
  an item-0 transcription carries the architect's independently hashed copy of the ruling text
  (the R679(c) form), and the brief's 2a self-diff clause is dropped. For R683 the architect's
  copy hashed identically (bf271df1…fd0a, 5,675 bytes, 67 lines); Block 81 stands.
```

**R693(d)** — `docs/plan/STANDING_RULES.md:552`

```text
R693(d) — Finding 3, no row: the health job's log records no lock line, so "lock taken or
  skipped" cannot be read from it. Open briefs stop asking it; whether the health job takes the
  experiment lock is the dgx-infra item's question.
```

**R697(b)** — `docs/plan/STANDING_RULES.md:584`

```text
R697(b) — Accepted as the standing form, no row: the old template taken by commit hash (37bb382),
  not by a HEAD-relative name; committed requests carry the paper text's sha256 and length in
  place of the text, the full requests in scratch by hash (the 12r-U13-PROBE form); a read-only
  data symlink inside a scratch worktree, not committed. The pkill -f self-match is a reading:
  briefs name a pid, never a pattern, when a process is to be stopped.
```

**R700(c)** — `docs/plan/STANDING_RULES.md:600`

```text
R700(c) — Note, no row: the tree check before 731ebf2 compared the wrong lines and the commit
  proceeded on a separate command chain; the check made afterwards (diff hash and test-file
  hash equal to the gate-launch record) establishes the commit as the gate's tree. From here a
  fix brief states the tree check as the one command whose success the commit is conditioned
  on, never two chains.
```

**R702(b)** — `docs/plan/STANDING_RULES.md:616`

```text
R702(b) — Readings, no row: (i) claude-config's 1ac7f58 (surgical-cv-spike rows 09cf951 and
  1a0c649, primer lines 41–45 intact) is a foreign commit accepted under R663(b); the open was
  right to write no note. (ii) The two duplicate wait loops ended on their own and left nothing
  running; from here a brief that waits on a background job starts one wait, and the report
  names its pid. (iii) 12v_close/ holding files beyond I7's list is the closeout's shape; I7 is
  met. (iv) The recount on the plan at 1557600 reproducing the 12v output is the expected
  result: a block appended after the decision log changes no row.
```

**R704(e)** — `docs/plan/STANDING_RULES.md:650`

```text
R704(e) — Readings, no row: (i) ledger 5a9ae9e's message timestamp (17:12:22Z) differs from its
  row (17:11:21Z); the row is the record; the message stands, not amended. (ii) The second wait
  at the census broke R702(b)(ii): one wait means one waiting process of any kind, foreground or
  background, for one job. (iii) The census cannot see a test asserting a manifest-body key is
  absent; the grep found none.
```

**R705(c)** — `docs/plan/STANDING_RULES.md:666`

```text
R705(c) — The inventory regeneration is accepted (U38's precedent at 731ebf2). From here a fix
  brief that edits engine/ lists docs/inventory/ among its touched paths.
```

**R705(d)** — `docs/plan/STANDING_RULES.md:679`

```text
R705(d) — The tree check: the first version failed and was revised before the commit. The
  revision is accepted on its evidence (HEAD and every file's sha256 equal to the gate-launch
  record). From here a check command that fails is a stop, including when the check itself is
  at fault; the revised check is proposed in the report and run only under a ruling.
```

**R707(a)** — `docs/plan/STANDING_RULES.md:708`

```text
R707(a) — The stop is upheld. R706(d)'s "A and C should; B alone does not" was the architect's
  prediction written as an expected value; that was an architect error, not CC's (as R703's I3
  fallback and V1A's Q2 test were). From here a ruling or brief states a predicted outcome only
  as "to be measured", never as an expected value that gates a stop, unless the brief names it
  as an acceptance criterion.
```

**R714(c)** — `docs/plan/STANDING_RULES.md:740`

```text
R714(c) — Readings, no row: ten non-audit keys, not eleven (the brief's count was the
  architect's; the census governs); the before-gate reads at 3.4 s, a deviation with an empty
  journal window; the landed file's first docstring line names Phase 1 and scratch, corrected
  by quoted text at the next commit that touches that file; the chained recount script's
  fix_units_dated_lines field is a literal reading done 19, so the dated lines are the count of
  record (R701(c)) and the script is not edited (R678(f)).
```

**R716(b)** — `docs/plan/STANDING_RULES.md:757`

```text
R716(b) — The INT-j12-15 note's target is the inventory row at :649, the one with an Evidence
  cell. The coverage-map row at :1382 is not touched. From here a note's target is the row with
  the named ID and an Evidence cell, matched by its cell count, and a brief that names a row
  says so.
```

**R720(b)** — `docs/plan/STANDING_RULES.md:772`

```text
R720(b) — Finding 2 is the architect's: the brief's R15 clause omitted the partner-block naming the
  pair form carries, and Block 116's "it orders" names R719's copies. Blocks 116 and 117 are not
  edited (R102; a header paragraph once committed is record). 12y-U18A's item 0 appends one dated
  addendum, in the text the brief carries, after Block 117's "ends at R719." line and before Block
  118, stating that Blocks 116 and 117 were transcribed in one commit (ec544a2) with the lane-line
  copy and the 12x-OPEN gate evidence copy ordered by R719(c) and R719(e). From here a brief that
  transcribes two rulings in one commit carries the pair's clause form, read by CC from the model
  pair and quoted before use; the architect does not write it.
```

**R720(c)** — `docs/plan/STANDING_RULES.md:791`

```text
R720(c) — Finding 1 is CC's and is accepted as reported: the commit is sound because the check
  printed PASS, and the chain did not meet R706(e). From here a check-and-commit chain contains no
  pipe: the check writes its own evidence file (a redirect inside the check, or the script writes
  it), and the chain is <check> && <commit>, one line, links joined by && (R716(e)). A tee in the
  chain is a defect of the chain whatever the check printed. No row; a note on R706(e)'s rule in the
  standing session rules at the next closeout.
```

**R722(a)** — `docs/plan/STANDING_RULES.md:808`

```text
R722(a) — The stop is correct and the cause is the architect's: the brief's Touched list named the
  tests the change adds and the fence it edits, and omitted the tests that pin the two seams
  R721(b) removes. A ruling that moves a call off a patched name moves its tests; the brief
  should have asked CC to census the patch sites before writing the list. From here a brief that
  removes or renames a call site carries a pre-step that greps tests/ for patches of that name and
  puts every file found in the Touched list before any edit.
```

**R723(a)** — `docs/plan/STANDING_RULES.md:825`

```text
R723(a) — Both stops are correct and both causes are the architect's, of one kind: the brief
  predicted a count ("exactly 3") and a census result (the files A3's grep would find) instead of
  asking for the census and taking its count — the 12x lesson (4), repeated. From here a brief
  states a "−" count as "one per noted row, N rows named" and lets CC compute N from the rows; and
  a census pre-step names its patterns and asks for classification, with the STOP condition
  written on the classification (a patch of a seam the commit removes, found in a file outside the
  Touched list), not on the file count.
```

**R724(a)** — `docs/plan/STANDING_RULES.md:843`

```text
R724(a) — The stop is correct; the cause is the architect's, the fourth of this unit: the census rule
  of R722(a) looked for patches of removed seams and not for pins of a record the commit extends.
  From here that pre-step also greps tests/ for exact-set assertions and hard-coded hashes over any
  record the commit adds a key to (the record's existing key names as the pattern), and classifies
  what it finds as (iii) a shape pin — in the Touched list, rewritten to the new shape with its
  purpose kept, or STOP. R723(a)'s rule covers the counting.
```

**R725(b)** — `docs/plan/STANDING_RULES.md:860`

```text
R725(b) — The rule, reversing the form the 12y-U18B brief and R722(a)–R724(a) built one census at a
  time: for an engine commit, the test-side Touched list is not predicted. It is "every test that
  pins a seam, a record shape or a class set the commit changes, found by census or by the gate,
  each rewrite reported with its assertion kept". A red test in the gate is classified by CC: (c)
  coupled to the change — rewritten under B5 in the same commit, reported before and after, no
  STOP; or (r) a regression — a test red for a reason the change did not pin — a STOP with the
  tree uncommitted. The STOP conditions that remain are a rewrite that drops or weakens an
  assertion, a class-(r) red, a moved pin or prompt_hash, and a model load in the gate window.
  The census pre-step stays (patch names, module-alias patches, record key sets, class membership
  sets) as a first pass that puts most couplings in the list before the gate; its misses are
  class (c), not stops. Engine files stay predicted and I5's STOP stays for them.
```

**R725(c)** — `docs/plan/STANDING_RULES.md:882`

```text
R725(c) — docs/inventory/ regeneration in an engine commit is standing practice (e81d699, 731ebf2,
  2c3822b, 5dbd3c4) and is accepted here; from here every engine-commit brief lists docs/inventory/
  in its Touched list by default.
```

**R727(a)** — `docs/plan/STANDING_RULES.md:896`

```text
R727(a) — The stop is correct; the cause is the architect's: R721(b) wrote "closing the run
  'aborted'" from C26's vocabulary without reading which exception each runner closes 'aborted'
  on. The brief's I3 caught it before an edit, which is what an INFERRED item is for.
```

**R729(c)** — `docs/plan/STANDING_RULES.md:910`

```text
R729(c) — Readings and notes, no row: the R117 addendum's label naming its row and its placement in
  the ends-at-era form (findings 1, 2) are the record's form from here; the "one worktree" element
  (finding 4) is accepted — the closeout block's element order of record is 12x's and 12y's as
  printed, with that element, and closeout_12s_found.txt stands as the earlier form; the gate
  confounding (finding 5) goes to the dgx-infra cross-lane item with the foreign ledger commit
  4525a44 (R726(b)); NVRM seventeen this boot; R728(f) is superseded by the PI's choice to close
  12y after U18 — recorded in the closure, the ruling text not edited (R102). The fix-unit count is
  21 by R728(b); the dated lines read 20: 12z-OPEN's item 0 writes one dated fix-unit line for U18
  in the form the dated lines take (read and quote it), so the dated lines are the count of record
  again (R714(c)); its "−" count is whatever the form shows, computed by CC (R723(a)).
```

**R730(c)** — `docs/plan/STANDING_RULES.md:931`

```text
R730(c) — Rule from here: every full commit hash a brief or a ruling carries is 40 hexadecimal
  characters, checked by the architect before issue; CC resolves every full hash a brief names
  with git rev-parse --verify at the step that first uses it, and a hash that does not resolve
  is a stop. §8 gains it as rule 8 at v97.
```

**R731(b)** — `docs/plan/STANDING_RULES.md:946`

```text
R731(b) — Finding F4 is the architect's: the brief named the 12y-U18C gate directory from unified
  plan v96's summary ("gate/") instead of having CC list it; the record's "gate_round1/" governed
  (R18). v97 corrects the plan's name. Finding F6: the 12y record carries no tmux launch line, so
  the "standard detached form" CC used is accepted as the gate's launch; from here every gate's
  evidence directory records the complete launch line, tmux wrapper included, and the next
  brief's I3 quotes it from there. No row.
```

**R731(c)** — `docs/plan/STANDING_RULES.md:963`

```text
R731(c) — The 12z-OPEN gate's evidence under ~/scratch/12z-open/ is not under docs/, as 12x-OPEN's
  was not (R719(c)). 12z-U19A's item 0 copies the gate's files into
  docs/session-reports/session-12/12z_open/gate_12z_open/ (cp; names, sizes and sha256 listed
  before the copy, every sha256 equal after), in the item-0 commit with r730.txt and r731.txt.
  The scratch directory is not moved or edited. From here a session-open brief's step 3 commits
  its gate evidence under the session's open directory in a commit of its own, so no later item 0
  carries it.
```

**R732(a)** — `docs/plan/STANDING_RULES.md:981`

```text
R732(a) — The stop is correct and is the architect's: the brief named Blocks 116–117 as the model
  pair from memory, although R720(b)'s addendum records that their clauses do not name each other.
  From here a brief that transcribes more than one ruling in one commit names no model block: it
  names the pattern — the record's most recent commit that transcribed more than one block — and
  CC finds it in the log, quotes its element order and clause form, and composes from it (R720(b)
  as it should have been applied). A brief that names a block by number for its form still has CC
  quote it, and a quoted form that contradicts the brief's description is a finding, the record
  governing (R18).
```

**R733(a)** — `docs/plan/STANDING_RULES.md:1017`

```text
R733(a) — The stop is correct and is CC's: the 12z-U19A stop report's quotation of the 12z-OPEN
  report was not what that report printed, and R732(d) carried it as a READ item. R732(d)'s
  purpose stands; its source does not. launch_line.txt line 1 reads "Written at 12z-U19A item 0
  (R733(a)) from this CC session's transcript, the command as executed at 21:49:44Z; not captured
  at launch."; line 2 is the executed line with its variable stated on the same line, in the form
  the report tabled it: the assignment G=~/scratch/12z-open/gate, then the tmux line exactly as
  executed. No elided or expanded variant is written. From here a READ item quoting a report is
  quoted from that report's text, not from a later report's recollection of it.
```

**R735(c)** — `docs/plan/STANDING_RULES.md:1036`

```text
R735(c) — The note label form CC used, naming the ordering sub-clause (R734(f)), is the record's
  form and stands; I9's literal label was the architect's shorthand. From here a brief carries a
  note's label as "the record's form, the sub-clause named", not a literal.
```

**R736(c)** — `docs/plan/STANDING_RULES.md:1050`

```text
R736(c) — Finding 1 is accepted as the code required: ServerEnvChanged is raised by the extract
  loop, so its CLAUDE.md sentence sits beside ModelDigestChanged's per-call sentence, not in
  open_run's refusal list; R734(d)(iii)'s "refusal list" was the architect's description written
  ahead of the code, the record's form governs (R18). Finding 3 is accepted and made a rule: an
  acceptance id that pins a non-refusal or an unchanged shape is green on its parent by
  construction; the brief names it a pin and CC reports it so, with no STOP (R707(a)'s form; A3's
  precedent). Finding 2 (the fourteenth id, test_api_parity's) is a reading. Finding 4: the kept
  name test_skip_returns_none_and_does_not_raise is inaccurate; the id is record and stays; one
  line in commit 1's record (already there, or added at the closeout) says so; a rename, if any,
  is P7's with the test-name housekeeping. Finding 5's two self-corrected defects are readings.
```

**R737(c)** — `docs/plan/STANDING_RULES.md:1071`

```text
R737(c) — Readings and notes, no row: Block 133's header sentence (finding 1) is the record's form
  for a ruling whose text first reached disk through a later brief's paste, and stands; the two
  check-scope stops before commits (finding 2) are CC's, caught by the chain, no stop of the
  session; the health cron log carries no dates, so a day's run is identified by mtime and tail
  (finding 3) — a dgx-infra reading, carried with the cross-lane item; the 105-line closure given
  by path and range (finding 4) is accepted, the plan at 2b592ae being the text; the equal wall
  times (finding 5) a coincidence of two readings. The 39-character hash of R729's STATUS line
  leaves primer.md with this lane line (R730(b)); the record keeps it in r729.txt and the lane-line
  copy only, with Block 127's addendum naming it.
```

**R738(c)** — `docs/plan/STANDING_RULES.md:1108`

```text
R738(c) — Finding 3: I3's premise that a wrapper writes the launch line was the architect's, taken
  from R732(d)'s wording, which no gate on disk implements. CC proceeded on a false INFERRED item
  instead of stopping; the method used is accepted this once because it meets R731(b) and is
  recorded, and the rule that a false INFERRED item stops the brief is unchanged. The form from
  here: the launching step holds the launch line in one variable, writes the timestamp sentence
  and that variable's value into launch_line.txt, then executes that variable, so the line
  written and the line run cannot differ.
```

**R739(b)** — `docs/plan/STANDING_RULES.md:1140`

```text
R739(b) — The mismatched file is not edited and not deleted: it is moved by its path to
  ~/scratch/12aa-u20a/r738_first_write.txt, its sha256 verified there. r738.txt is then written
  once more from the 12aa-U20A brief's Section 1 and hashed immediately; a second mismatch is a
  stop. From here a ruling file that fails its hash is cleared this way, under a ruling.
```

**R742(b)** — `docs/plan/STANDING_RULES.md:1155`

```text
R742(b) — Rule from here: a check script takes every value that varies by commit (ruling
  numbers, N, file names, expected hashes) from its arguments or from the files it checks; no
  such value is carried as a literal from an earlier script.
```

**R746(c)** — `docs/plan/STANDING_RULES.md:1169`

```text
R746(c) — Finding 2 is accepted and is the rule from here: a red at the gate caused by a census miss
  is class (c); it is fixed without changing any test's assertion and the gate is re-run once; the
  red run's evidence is committed beside the green one and is not a member of the series. Any
  other red at the gate is a stop.
```

**R746(d)** — `docs/plan/STANDING_RULES.md:1184`

```text
R746(d) — Readings, no row: finding 3 (a red caused only by a missing name is reported as such in
  every record from here); findings 4, 5, 6, 7, 9 and 10; finding 8's handler list is the starting
  list of C56's census in U26, untraced.
```

**R747(b)** — `docs/plan/STANDING_RULES.md:1198`

```text
R747(b) — transcribe_ruling.py is accepted for every block from here. session_open_check.py is
  accepted for what it checks; the recount, the R510 count, the nightly's ids against the expected
  set and the health job's journal read stay explicit steps of an open until the script covers
  them (finding 4). The gate wrapper is accepted pending its first full gate, where a wrapper fault
  is reported as the wrapper's (finding 8).
```

**R749(b)** — `docs/plan/STANDING_RULES.md:1214`

```text
R749(b) — An open does not wait for a day's scheduled jobs. A brief is valid at any time outside
  07:00–10:35 UTC (R86), and a gate must exit before 07:00 UTC. An open or closeout reads every
  scheduled job that has run on the current HEAD since the last recorded read, each against the
  expected file that governs it; a nightly that has not yet run is read by the first open,
  closeout or other brief after it runs, as an explicit step, against the same expected file
  while the engine tree is unchanged, and against the closeout's new expected file after a
  change. 12aa_U20C/expected_nightly_20261031.md governs the 2026-10-31 nightly whenever it is
  read. The readings themselves, the line-for-line match, the ids comparison (R655(b)) and the
  stop on a mismatch are unchanged. This amends R748(e) and every earlier wrap ruling's "reads the
  … health job and … nightly" clause in timing only.
```

**R750(c)** — `docs/plan/STANDING_RULES.md:1235`

```text
R750(c) — D = 0 is accepted (finding 2). The brief's "EXPECTED: the ends-at R747 line" was the
  architect's prediction from the record's description, against afd179e's own numstat, and is
  withdrawn; the tool and the record governed, as the constraints say. 12ab-OPEN's rule-14 list
  gains "a tool's write semantics", and a brief that names a tool states no "−" count the tool's
  own run does not supply.
```

**R751(c)** — `docs/plan/STANDING_RULES.md:1251`

```text
R751(c) — Finding 8: the session-start instruction "Read docs/plan/ENGINE_REFACTOR_PLAN.md" is read
  as scoped, from this ruling: the Status table, the latest closure paragraph and its "Expected
  values" block, the decision log's last block, and every block, row or read-out a brief names. No
  other part of the plan is taken as checked by a session, and a brief that relies on an earlier
  ruling quotes it. The CLAUDE.md line is corrected to this scope, by text quoted in that commit's
  handoff for PI approval, in the commit that adopts STANDING_RULES.md (R747(c)); not before.
```

**R751(e)** — `docs/plan/STANDING_RULES.md:1268`

```text
R751(e) — Readings, no row: finding 10 (stale primer lines, the dgx-infra lane's); finding 11 (the
  temp root rotated pytest-166 out at pytest-169's creation; R647(c) stands — the root is read,
  never matched); finding 12 (an evidence commit's scope is docs(session-reports) from here, the
  model 5c1474c's docs(plan) being for commits that touch the plan); finding 13 (a brief's named
  waiting process governs over a memory note; the note is CC's to reconcile, not a stop).
```

**R752(d)** — `docs/plan/STANDING_RULES.md:1284`

```text
R752(d) — Finding 9: the memory note feedback_tmux_no_poll.md is CC's own, outside every repository,
  and R751(e) left it to CC to reconcile; the edit is a reading, not a Touched-list breach. From
  here a brief's DO NOT TOUCH does not reach CC's own memory files, and CC reports an edit to one.
```

**R753(d)** — `docs/plan/STANDING_RULES.md:1298`

```text
R753(d) — Three rules carried only by citations are stated here, so the file can quote a clause
  (finding 6): (i) the model-load stop is R696(b)'s, already in the list, and the blocks' citation of
  R666(a) for it is read as R696(b) from here; (ii) a closed pre-tag row is counted on the list until
  P1 closes (R607(d) as the record has applied it since R660(a)); (iii) an open's pre-check reads the
  process table and tmux for processes of the archived CC sessions it names, each inside a stated
  window, attributes every one found to its lane, and stops only on a survivor of this lane ("R692's
  form"). This ruling enters the list at the regeneration that follows its transcription, not at
  2d's; the list file carries it under a "#" line until then.
```

**R756(b)** — `docs/plan/STANDING_RULES.md:1317`

```text
R756(b) — Readings, no row: finding 20 (the status line is one line in the file; a brief's wrapped
  quote is compared whitespace-normalised, the words verbatim); finding 21; finding 22 (primer.md's
  "read it at session start" line is working state, corrected at the 12ab wrap under R737(d)'s
  lane-line form, not before).
```

**R763(c)** — `docs/plan/STANDING_RULES.md:1332`

```text
R763(c) — Finding 50: the notes after the gate, in U20's form, is the record's order; the brief's H3
  was the architect's. Finding 51: the crash of check_notes.py on an empty diff line was a fault of
  the check, not a failed condition; the correction is accepted after the fact on the evidence
  committed (3h_check_crash.txt, the corrected script, no condition weakened). R705(d) is restated,
  not narrowed: a check that crashes is a check that failed, and from here a crash is reported before
  the check is corrected and re-run, under a ruling; this once stands.
```

**R766(b)** — `docs/plan/STANDING_RULES.md:1349`

```text
R766(b) — The 17 "Named by" lines are accepted: each only appends ids, no existing id is dropped or
  reordered, and no clause text moved. Rule from here: a regeneration of STANDING_RULES.md after
  new blocks changes its "Named by" lines, which are derived; a pre-commit check classifies every
  changed line and allows the header's three hash and Selection lines, a new section for an id the
  list gained, and a "Named by" line that only appends ids; any other changed line is a stop. The
  classification is written by the check into its own evidence file, never by eye.
```

**R772(c)** — `docs/plan/STANDING_RULES.md:1366`

```text
R772(c) — check_cfix_v2.py is accepted as proposed (two lines: the numstat expectation read as +2
  / −1; one "−" line, both "+" lines required); no other line changes, the diff quoted. Rule from
  here: a pre-commit check asserts what a diff contains — the lines it must and must not carry —
  and reads its numstat into the evidence; it matches a count only when a record supplies that
  count (a tool's own report, a prior run's figure), never a prediction of the diff's shape made
  before the diff exists. R750(c) stated this for a tool's write; this extends it to every
  check-and-commit chain.
```

**R780(b)** — `docs/plan/STANDING_RULES.md:1384`

```text
R780(b) — The ledger-commit question, on the PI's reading of the amendment. Rule from here: in this
  lane, a git commit and git push that a brief directs — the engine repository's commits on main,
  the evidence commits, and the commit of the hook-written ledger row in claude-config — is not a
  print-and-review step; a brief states "auto mode ON, commit steps included" as before. The
  amendment's "edits to claude-config" line is read as the PI reads it: it does not reach the
  commit of a row the hook wrote, and its sanctioned pushes include this lane's. Its "commits on a
  branch or in a temporary worktree" wording is clarified in the claude-config lane by the PI,
  where an edit to CLAUDE.md belongs; no brief of this lane edits it. R779(d)'s interim reading is
  confirmed; report finding 1's three lines are answered. This clause is a standing rule: "A
  R780(b)" joins the list at the next item 0 (R754(c)(iii)).
```

**R783(d)** — `docs/plan/STANDING_RULES.md:1405`

```text
R783(d) — Finding 1. Rule from here: a full hash is proved to exist by git cat-file -e
  <hash>^{commit} in the repository it belongs to; git rev-parse --verify on a 40-character hash
  proves its form only and is not the existence check. R730(c)'s "resolved with git rev-parse
  --verify" reads as this from here; every brief's step-0 hash check uses the cat-file form, and a
  hash that does not exist is a stop as before. The hashes of 12ad stand: each was also matched
  against HEAD or git log. This clause is a standing rule: "A R783(d)" joins the list at 12ae-OPEN's
  item 0 (R754(c)(iii)), the file regenerated there.
```

**R784(g)** — `docs/plan/STANDING_RULES.md:1423`

```text
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
```

**R541** — `docs/plan/STANDING_RULES.md:1512`

```text
R541 — **The 12f triage table, as amended by R537 and R538, is the pre-tag list.** Its outside and internal rows are opened as inventory section L; a finding after this ruling enters only with its class and package stated (R509). The finish line is this list closed.
```

**Computed count: 69 of 95 clauses match** (the script's `len(hits)`).

### By eye

The search missed these clauses about brief form. Each one rules on a clause, a section or a step of a brief:

**R679(b)** — `docs/plan/STANDING_RULES.md:445`

```text
R679(b) — The prefix_check.py clause of step 2e was an architect clause and is withdrawn; R664(a)
  had already withdrawn the same premise in 12r, and R664(b) governs: a commit carrying no cell
  note is verified by its "−" count alone, which must be 0, read from git diff --numstat of the
  plan file. The rest of 2e stands.
```

**R696(b)** — `docs/plan/STANDING_RULES.md:566`

```text
R696(b) — The gate is accepted as run. The empty-window clause of 2g was written wider than the
  rule it serves: R666(a) stops a gate for a model load during it (the health job's case). From
  here, a read-only request in the window (HEAD /, GET /api/ps, /api/tags, /api/version) with
  nothing resident before and after is a reading, recorded with its timestamp and, where known,
  its sender; a load or a generate during the gate is the stop. The 16:59:32 ollama ps came from
  an interactive shell on the box, not from this lane's jobs or the suite; its sender is a
  reading the PI may name by a later note. The series gains 883.80 s. No re-run.
```

**R738(b)** — `docs/plan/STANDING_RULES.md:1091`

```text
R738(b) — Finding 1 is CC's: r737.txt was first written with a line not in Section 1 and patched
  before its hash, against "written once from Section 1 and then only read". The final file
  equals the architect's hash and nothing wrong reached a commit; no stop of the session, no row.
  A patch before the hash removes the hash's power to stop. Rule from here: an rNNN.txt is hashed
  immediately after its one write, before it is read back or edited; a mismatch at that hash is a
  stop, and the file is not edited to reach a match.
```

## 3. Brief text on disk

| location (pattern) | count | command |
|---|---|---|
| repo: `docs/session-reports/session-12/<task>/brief_<task>.md` | **19** (12ae: 12 incl. one `_R1`; 12af: 7) | `git ls-files \| grep -E '(^\|/)brief[^/]*\.(md\|txt)$' \| wc -l` |
| repo: any other path naming "brief" | 0 | the same `git ls-files` list, filtered to exclude the line above |
| live transcript store `~/.claude/projects/-home-ankitsarin-projects-evidence-engine/*.jsonl` | 1 (this session only) | `ls …/*.jsonl \| wc -l` |
| session archive `~/claude-session-archive/*.jsonl` (`DEFAULT_ARCHIVE = Path.home() / "claude-session-archive"`, `~/projects/dgx-infra/session_audit/archive.py:73`), flat, all repos | 242 files | `find ~/claude-session-archive -name '*.jsonl' \| wc -l` |
| … of which the first `cwd` is the evidence-engine checkout | **71** | scan script below |
| … user turns there carrying `TASK [<id>] —` | **374**, in 65 transcripts (2026-09: 163, 2026-10: 186, 2026-11: 25) | scan script below |

The scan script is `<scratch>/scan.py`, sha256 `ea0fb4c2…a51cc1`, run with `python3 -I scan.py ~/claude-session-archive
~/.claude/projects/-home-ankitsarin-projects-evidence-engine`. For each `*.jsonl` it reads the first `cwd`. It then counts
user turns with string content, joining list content from its `text` items, that match `TASK \[([^\]]*)\]\s*[—-]`. The 374
can include a brief repeated inside a later turn: `12o-U12-C8` appears twice. Every 12y brief turn checked in §4 carries
`<pasted_content` (`"<pasted_content" in c` → True). The archived turn is the paste, verbatim. A committed brief file
exists only from 12ae-U24A on, under R784(g)(v). The 12ae-OPEN brief is excluded by that clause's own text ("the 12ae-OPEN
brief stays in the architect's record and the archived session transcript").

Section header lines of three committed briefs. These are the labels only, from
`grep -noE '^(CLAUDE CODE$|(TASK|RULING) \[[^]]*\]|[A-Z][A-Z0-9 /-]{2,}( \(|:))'`:

```text
12ae_U26A/brief_12ae_U26A.md: 1:CLAUDE CODE 16:RULING [evidence-engine 12ae-R789] 18:STATUS: 23:ASSUMPTIONS IN THIS RULING 91:TASK [evidence-engine 12ae-U26A] 96:PURPOSE: 102:MACHINE: 103:SKILL: 104:PERMISSIONS: 117:ASSUMPTION LEDGER 157:CONTEXT: 163:CONTRACT 272:ACCEPTANCE GATES: 285:OUT OF SCOPE: 290:CONSTRAINTS 295:FINAL REPORT FORMAT: 301:CONTRADICTION HANDLING: 307:HANDBACK:
12af_U29B/brief_12af_U29B.md: 1:CLAUDE CODE 16:RULING [evidence-engine 12af-R800] 21:STATUS: 26:ASSUMPTIONS IN THIS RULING: 154:TASK [evidence-engine 12af-U29B] 161:PURPOSE: 165:MACHINE: 166:SKILL: 167:PERMISSIONS: 190:TIMING: 194:ASSUMPTION LEDGER 237:CONTRACT 359:ACCEPTANCE GATES: 373:OUT OF SCOPE: 379:CONSTRAINTS 387:FINAL REPORT FORMAT: 399:CONTRADICTION HANDLING: 409:HANDBACK:
12af_CLOSE/brief_12af_CLOSE.md: 1:CLAUDE CODE 16:RULING [evidence-engine 12af-R802] 19:STATUS: 25:ASSUMPTIONS IN THIS RULING: 91:TASK [evidence-engine 12af-CLOSE] 98:PURPOSE: 101:MACHINE: 102:SKILL: 103:PERMISSIONS: 118:FORM 128:TIMING: 135:ASSUMPTION LEDGER 171:CONTRACT 298:ACCEPTANCE GATES: 306:OUT OF SCOPE: 311:CONSTRAINTS 316:FINAL REPORT FORMAT: 324:CONTRADICTION HANDLING: 331:HANDBACK:
```

All three files have the same shape: a ruling (STATUS, ASSUMPTIONS) comes before the TASK, followed by the fixed sequence
PURPOSE … HANDBACK. TIMING and FORM appear only in some briefs.

## 4. Stop corpus (session 12y, unit U18)

Each id appears twice in `docs/plan/ENGINE_REFACTOR_PLAN.md`: once in the ruling's fence and once in the session's
decision-log table row. Each ruling has one table row, which holds all of its lettered clauses. Both locations come from `grep -nF "<id> —"`. The fence copy is quoted below.

**R722(a)** — fence `ENGINE_REFACTOR_PLAN.md:12247`, row `:12281`. Block title `:12215` `RULING [evidence-engine 12y-R722] — 12y-U18B stop at 0d…`

```text
R722(a) — The stop is correct and the cause is the architect's: the brief's Touched list named the
  tests the change adds and the fence it edits, and omitted the tests that pin the two seams
  R721(b) removes. A ruling that moves a call off a patched name moves its tests; the brief
  should have asked CC to census the patch sites before writing the list. From here a brief that
  removes or renames a call site carries a pre-step that greps tests/ for patches of that name and
  puts every file found in the Touched list before any edit.
```

**R723(a)** — fence `:12313`, row `:12341`. Block title `:12288` `RULING [evidence-engine 12y-R723] — 12y-U18B second stop before item 0…`

```text
R723(a) — Both stops are correct and both causes are the architect's, of one kind: the brief
  predicted a count ("exactly 3") and a census result (the files A3's grep would find) instead of
  asking for the census and taking its count — the 12x lesson (4), repeated. From here a brief
  states a "−" count as "one per noted row, N rows named" and lets CC compute N from the rows; and
  a census pre-step names its patterns and asks for classification, with the STOP condition
  written on the classification (a patch of a seam the commit removes, found in a file outside the
  Touched list), not on the file count.
```

**R724(a)** — fence `:12377`, row `:12407`. Block title `:12348` `RULING [evidence-engine 12y-R724] — 12y-U18B third stop, before step 2…`

```text
R724(a) — The stop is correct; the cause is the architect's, the fourth of this unit: the census rule
  of R722(a) looked for patches of removed seams and not for pins of a record the commit extends.
  From here that pre-step also greps tests/ for exact-set assertions and hard-coded hashes over any
  record the commit adds a key to (the record's existing key names as the pattern), and classifies
  what it finds as (iii) a shape pin — in the Touched list, rewritten to the new shape with its
  purpose kept, or STOP. R723(a)'s rule covers the counting.
```

**R725(b)** — fence `:12447`, row `:12477` (one table row per ruling, carrying R725(a)–(f)). Block title `:12414` `RULING [evidence-engine 12y-R725] — 12y-U18B gate stop…`

```text
R725(b) — The rule, reversing the form the 12y-U18B brief and R722(a)–R724(a) built one census at a
  time: for an engine commit, the test-side Touched list is not predicted. It is "every test that
  pins a seam, a record shape or a class set the commit changes, found by census or by the gate,
  each rewrite reported with its assertion kept". A red test in the gate is classified by CC: (c)
  coupled to the change — rewritten under B5 in the same commit, reported before and after, no
  STOP; or (r) a regression — a test red for a reason the change did not pin — a STOP with the
  tree uncommitted. The STOP conditions that remain are a rewrite that drops or weakens an
  assertion, a class-(r) red, a moved pin or prompt_hash, and a model load in the gate window.
  The census pre-step stays (patch names, module-alias patches, record key sets, class membership
  sets) as a first pass that puts most couplings in the list before the gate; its misses are
  class (c), not stops. Engine files stay predicted and I5's STOP stays for them.
```

The stop itself is R725(a), fence `:12443` and row `:12477`, quoted because R725(b) is not a stop entry:

```text
R725(a) — The stop is correct. tests/test_ft_bridge.py and tests/test_run_fault_predicate.py join the
  Touched list with the one-line fixes as proposed: the ps patch aimed at oc._client with
  raising=False (oc imported), and rm.OllamaHostNotLoopback added to RUN_REFUSED_MEMBERS in U17's
  form with the comment "# U18, R721(c)". Every assertion in both files is kept.
```

**R727(a)** — fence `:12576`, row `:12603`. Block title `:12546` `RULING [evidence-engine 12y-R727] — 12y-U18C stop at 0c…`

```text
R727(a) — The stop is correct; the cause is the architect's: R721(b) wrote "closing the run
  'aborted'" from C26's vocabulary without reading which exception each runner closes 'aborted'
  on. The brief's I3 caught it before an edit, which is what an INFERRED item is for.
```

**R728(e)** — fence `:12668`, row `:12685` (one table row per ruling, carrying R728(a)–(f)). Block title `:12610` `RULING [evidence-engine 12y-R728] — U18 commit 2 accepted (32fc09e); U18 closed…`

```text
R728(e) — Readings, no row: the check-to-send window, a replacement during a call and the restart
  retry's resend without a re-check (finding 1) are recorded limits of U18, carried to Run 7's
  confounder statement; ModelDigestChanged described beside the active-run sentence, not in
  open_run's list (finding 2); the message form set by E1 and E5 together (finding 4); the
  constructor convention RUN_REFUSED_MEMBERS pins (finding 5); the acceptance ids run red twice,
  the second against an archive export of the parent, accepted as the red evidence. The six stops
  of U18 — five the architect's, one the gate's — are one line on the closeout's most-violated
  list: scope, counts and close semantics written from a description instead of read from the
  record. R510 for 12y: rows in 0, rows out 0, no class move.
```

**Recoverability of the triggering briefs.** All of them are recoverable verbatim from one archived transcript,
`~/claude-session-archive/dc9f61b7-39e2-4cc6-ae86-dd391b660667.jsonl`. Each is a user turn, as pasted. Line numbers are
JSONL lines. The mapping comes from the resume briefs' own titles, `TASK [… resumed under <R>]`:

| stop entry | brief that ran into it | jsonl line | verbatim? |
|---|---|---|---|
| R722(a) | `12y-U18B` | 766 | yes (pasted turn, 29,221 chars) |
| R723(a) | `12y-U18B, resumed under R722` | 862 | yes (20,217 chars) |
| R724(a) | `12y-U18B, resumed under R722 and R723` | 909 | yes (3,858 chars) |
| R725(a)/(b) | `12y-U18B, resumed at step 2 under R724` | 1039 | yes (3,450 chars) |
| R727(a) | `12y-U18C` | 1517 | yes (39,987 chars) |
| R728(e) | none: a reading on the `12y-U18C, resumed at item 0 under R727` report, which ran with no stop | 1612 | yes (4,061 chars) |

None of these briefs is committed in the repository: 12y predates R784(g)(v). The paste includes the ruling and the
brief, and some turns carry earlier rulings for transcription: turn 1517 holds R723–R726. The character counts come from
`len(content)` in the per-turn scan.

**Briefs from the same period that ran with no stop**, by path. A brief counts here when its next ruling's title reads
"accepted" (`grep -nE "^RULING \[evidence-engine 12(x|y|z)-"` on the plan):

1. `~/claude-session-archive/dc9f61b7-39e2-4cc6-ae86-dd391b660667.jsonl` line 6 (`12y-OPEN`; R720 "accepted")
2. same file, line 452 (`12y-U18A`; R721 "accepted")
3. same file, line 1373 (`12y-U18B, resumed at step 3h under R725`; R726 "U18 commit 1 accepted")
4. same file, line 1612 (`12y-U18C, resumed at item 0 under R727`; R728 "accepted")
5. same file, line 1933 (`12y-CLOSE`; R729 "closeout accepted")
6. `~/claude-session-archive/f7992c76-8dfc-4e06-a449-fcfbae6c3f8e.jsonl`, the `12x-AQA` turn (R715 "accepted")
7. `~/claude-session-archive/14aff8b0-ad80-4c8f-8412-782035bf3f13.jsonl`, the `12z-U19B-R1` turn (R736 "accepted")
8. `docs/session-reports/session-12/12af_U40A/brief_12af_U40A.md`
9. `docs/session-reports/session-12/12af_U29A/brief_12af_U29A.md`
10. `docs/session-reports/session-12/12af_U30A/brief_12af_U30A.md`

(12af ran "No stop, no pause", per the primer's 12af close block. Items 8–10 come two weeks after 12y and are included
because they are the committed form.)

## 5. Existing checks

- `~/.claude/settings.json` `hooks`:
  ```json
  {"PostToolUse": [{"matcher": "Write|Edit", "hooks": [{"type": "command", "command": "bash /home/ankitsarin/.claude/hooks/phi-check.sh", "timeout": 10000}]}]}
  ```
  This is the only hook in any settings file. `~/.claude/settings.local.json` has no `hooks` key. `.claude/settings.json`
  in the repo is absent. `.claude/settings.local.json` in the repo has no `hooks` key. `/etc/claude-code/managed-settings.json`
  is absent. `enabledPlugins` is null or absent in both the user and local settings, so the marketplace plugins' `hooks.json`
  files (for example `hookify`) are not active.
- Git hooks: `ls .git/hooks | grep -v '\.sample$'` → `post-commit` only. Its header is
  `# post-commit-ledger.sh — Appends a one-line entry to the global PROJECT_LEDGER.md`, and its active line is
  `echo "| ${TIMESTAMP} | ${PROJECT} | ${BRANCH} | ${SHORT_HASH} | ${SUBJECT} |" >> "$LEDGER"`. It checks nothing.
  `core.hooksPath` is unset in both the repo and the global config.
- CI: none found. Searched for `.github`, `.gitlab-ci.yml`, `.pre-commit-config.yaml`, `.circleci`, `Jenkinsfile`,
  `tox.ini` and `noxfile.py`, all absent. The only scheduled check is the crontab line
  `0 9 * * * /bin/bash /home/ankitsarin/projects/evidence-engine/scripts/nightly_tests.sh`, which runs the test suite.
- Doc or brief linters: none found.
  `git ls-files | grep -iE 'lint|check_brief|brief_check|doc_check|check_docs'` → empty, and no test names
  `STANDING_RULES` or `brief_` (`grep -lE "STANDING_RULES|brief_" tests/*.py` → empty). The closest existing checks are
  validators of the record, not of briefs:
  `extract_rules.py` refuses an id-list line or a gloss that does not resolve. `transcribe_ruling.py --check-plan` checks
  that a regenerated block is byte-identical. `session_open_check.py <expected.json>` prints MATCH or mismatch for each
  expected value. All three are in `docs/session-reports/tools/README.md`.
  `~/claude-config/BRIEF-FORMAT.md` is 2 lines (`wc -l`): `Current brief-format version: 1.0` plus a version-bump note. It
  is not a checker.

## 6. Prompt hook capability

`claude --version` → `2.1.325 (Claude Code)`. The binary is `/home/ankitsarin/.local/share/claude/versions/2.1.325`, an ELF
aarch64 executable. The readable source is the documentation text bundled in the binary, read with `strings -n 8 <binary>`.
`strings … | grep -c UserPromptSubmit` → 63. The quotes below are verbatim from that output.

- The event exists, from the bundled hook-events table:
  ```text
  | UserPromptSubmit | - | When user submits |
  ```
- Its input and its rejection behaviour:
  ```text
  When the user submits a prompt
  Input to command is JSON with original user prompt text.
  Exit code 0 - stdout shown to Claude
  Exit code 2 - block processing, erase original prompt, and show stderr to user only
  Other exit codes - show stderr to user only
  ```
- The input schema:
  ```text
  hook_event_name:T("UserPromptSubmit"),prompt:o(),source:H(["user","sdk","system","loop_wakeup","schedule_wakeup","poll_event"]).optional().describe("Who authored/injected the prompt: `user` = submitted from the interactive composer, …
  ```
- The JSON route:
  ```text
  - `decision` - "block" for PostToolUse/Stop/UserPromptSubmit hooks (deprecated for PreToolUse, use hookSpecificOutput.permissionDecision instead)
  - `reason` - Explanation for decision
  ```
  and `blocked by a UserPromptSubmit hook` / `Prompt blocked: the UserPromptSubmit hooks did not run over the submitted text.`

**UNPROVEN:** whether a `<pasted_content>` block reaches the hook's `prompt` string expanded, as the archived transcripts
store it, or collapsed. No readable source settles it, and no hook was installed to test it.

## 7. Tooling conventions

- Tool modules: `engine/tools/` (`ls` → `__init__.py`, `db_fingerprint.py`, `inventory.py`). Lane session tools are kept
  separately in `docs/session-reports/tools/` (`extract_rules.py`, `transcribe_ruling.py`, `session_open_check.py`,
  `launch_gate.sh`, `run_gate.sh`, `README.md`). These are validated by committed evidence files, not by `tests/`
  (README: "Nothing here changes `engine/`, `scripts/` or `tests/`").
- Python: `.venv/bin/python --version` → `Python 3.12.3`, and the system `python3 --version` → `Python 3.12.3`.
- Invocation: `python -m engine.tools.<name>` with argparse. Example from `engine/tools/db_fingerprint.py:407–413`:
  `parser = argparse.ArgumentParser(` … `parser.add_argument("database", help="path to the database file")` …
  `parser.add_argument("--out", metavar="JSON",` … `parser.add_argument("--compare", metavar="JSON",`. CLAUDE.md runs it as
  `python -m engine.tools.db_fingerprint data/surgical_autonomy/review.db [--out fp.json] [--compare fp.json]`.
- Tests: one `tests/test_<tool>.py` per engine tool (`tests/test_db_fingerprint.py`, `tests/test_inventory.py`), which
  import the module's functions directly. From `tests/test_db_fingerprint.py:18–22`:
  `from engine.tools.db_fingerprint import (` / `CANONICAL_SERIALIZATION,` / `compare,` / `encode_cell,` / `fingerprint,`.
  The standard gate is `python -m pytest tests/ … -m "$STANDARD_GATE"`, with
  `STANDARD_GATE="not network and not ollama and not integration"` (`scripts/nightly_tests.sh:25`, `:40`).

## Verdicts

- **E1: holds.** `grep -c '^## R'` → 95, and the file is generated by `extract_rules.py --plan
  docs/plan/ENGINE_REFACTOR_PLAN.md` from the decision log. Selection is by the committed id list, not by the log alone. The
  file's header metadata is stale against the current plan (§1).
- **E2: fails as stated.** All six ids exist (§4), but they are not six stop entries. R725(b) is a rule, and the stop is
  R725(a). R728(e) is "Readings, no row" and is the entry that counts "The six stops of U18". R723(a) covers two stops
  ("Both stops are correct"). The six stops are R722(a) 1 + R723(a) 2 + R724(a) 1 + R725(a) 1 + R727(a) 1.
- **E3: fails.** Since 12ae-U24A, briefs have been committed as files:
  `git ls-files | grep -E '(^|/)brief[^/]*\.(md|txt)$' | wc -l` → 19, under R784(g)(v) "Each brief is committed as pasted,
  at its own item 0, into the evidence directory it governs, as brief_<task id>.md". Earlier briefs survive only in
  `~/claude-session-archive/*.jsonl`.
- **E4: holds.** The bundled text quoted in §6: "Exit code 2 - block processing, erase original prompt, and show stderr to
  user only". The input is JSON with a `prompt` string. Whether pasted blocks arrive expanded is UNPROVEN.

## Findings that contradict the brief

1. **E3's premise is false.** See the verdict above.
2. **R784(g)(v) and this brief's scope disagree.** The standing rule requires this brief to be committed at its item 0 as
   `brief_WR5-A.md`. The brief allows one new file (G1, "Touches: one new report file"). I followed the brief and did not
   write the brief file. If the architect rules it is owed, it can be written from the paste in a follow-up commit.
3. **E2's six ids do not match the six stops.** See the verdict above.
4. **The header of `STANDING_RULES.md` is stale.** It records the plan sha `fb14a3fd…` and "1524 clauses". Today they are
   `328edeff…` and 1655, and 18 "Named by" lists have grown. No clause text differs (§1). A checker that trusts the
   header's plan hash would refuse the current plan.
5. **The brief named "the Claude Code transcript store"**, which now holds only the current session. Archived
   transcripts are moved to `~/claude-session-archive/` (flat, every repo mixed together, selectable by `cwd`).
6. **`session_open_check.py` was not run whole**, because its `fingerprint` section reads `review.db`. Its `cc_sessions`
   predicate was applied by hand (§0).
