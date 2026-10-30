# Standing rules (extracted, not adopted)

**Status: extracted, not adopted.** Under R746(e) this file is adopted only by a later PI ruling after the architect has read it; until then every brief stays in its present form. Clause text is the plan's, by script; only the lines headed "Gloss (CC's, not ruled):" are composed, and they rule nothing.

- Generator: `docs/session-reports/tools/extract_rules.py`, sha256 `b3aaf6894856e04760c089ee4398ba28c02ab376ea2d8b0ef49e6a898d8387ef`
- Plan blob: `docs/plan/ENGINE_REFACTOR_PLAN.md`, sha256 `86b852a454109b18099dc23210db64f2dbbe991c64a4b73a1626c4d5e85f0c39`
- Selection: clauses containing "from here" or "the standing form", whitespace normalised ("Rule from here" contains the first): 57 of 1293 clauses in the log.
- "Named by" lists later clauses that carry the exact id; a range or a bare ruling number does not count.

## R656(d)

2026-10-16, session 12q.

```text
R656(d) — The interpreter is .venv/bin/python; the first fingerprint attempt's exit 127 is a
  reading, and the 22:43:00Z run is the one of record. Briefs from here name
  .venv/bin/python for every repository script. No row.
```

Named by: none.

Gloss (CC's, not ruled): Briefs name .venv/bin/python for every repository script.

## R657(b)

2026-10-16, session 12q.

```text
R657(b) — The host maintenance of 2026-10-16 (apt full-upgrade 22:45–22:47 UTC, reboot
  23:05 UTC, by the PI) is a dated fact of 12q's closure, not a defect and not a stop
  condition for the remaining steps. Everything the 12q expected-values block states about
  the host is a reading from here: Ollama service start time (now 2026-10-16 23:05:57 UTC,
  NRestarts=0), the temp root (cleared at boot; the 0(i) reading of 22:43:26Z is the last
  reading of the nightly's basetemp and answers step 3(e): pytest had pruned to three
  basetemps), and the gate's wall time.
```

Named by: R663(a).

Gloss (CC's, not ruled): One day's host facts (service start, temp root, wall time) were treated as readings, not checks; mostly a dated fact, not a lasting rule.

## R658(b)

2026-10-16, session 12q.

```text
R658(b) — The seven `review.db` files below `data/<dir>/` depth are row I21's seven nested
  paths (debris_2026-09-09 and the elicit_design01 smoke copies), already recorded and owned
  by P7. The plan's wording ("No second data/<dir>/review.db") governs; the brief's `find`
  literal was the architect's. From here the check is written as `ls data/*/review.db` and
  expects one path. No row.
```

Named by: none.

Gloss (CC's, not ruled): The one-review-database check is `ls data/*/review.db`, expecting one path.

## R658(c)

2026-10-16, session 12q.

```text
R658(c) — The refused `rm -rf /tmp/pytest-of-ankitsarin`: nothing ran, and it is recorded as
  the architect reads it — the brief's DO NOT TOUCH was the constraint, and the classifier's
  refusal is not a guard this lane relies on. From here, no helper command composed for a
  brief deletes any path outside ~/scratch/<session>/; a deletion elsewhere is its own named
  step with the PI's ruling. No row.
```

Named by: none.

Gloss (CC's, not ruled): No helper command deletes anything outside the session's scratch without its own named, PI-ruled step.

## R660(b)

2026-10-17, session 12q.

```text
R660(b) — The three GET entries at the launch second are this session's bracketing reads and
  the window is attributed; from here the before-gate reads are made at least five seconds
  before the launch so the window starts empty.
```

Named by: none.

Gloss (CC's, not ruled): Take the before-gate Ollama reads at least five seconds before launch, so the gate window starts empty.

## R661(e)

2026-10-17, session 12q.

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

Named by: R668(f).

Gloss (CC's, not ruled): Hook-written ledger commits carry the row's exact timestamp.

## R662(a)

2026-10-17, session 12q.

```text
R662(a) — The 12q-CLOSE closeout block is accepted as printed. The block's element list (the
  12q-CLOSE brief's step 9) is the closeout block's form from here; the 12p block's form was
  in an archived transcript, not on disk (F7), and this ruling makes the on-disk form the
  one that governs. The retention file's appended "Staged, not retained" section is accepted:
  the 12p portion is byte-identical and the staged line is below it, as ordered.
```

Named by: none.

Gloss (CC's, not ruled): The closeout block's form is the element list in the on-disk brief, not one found in an archived transcript.

## R662(e)

2026-10-17, session 12q.

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

Named by: R674(b), R675(b).

Gloss (CC's, not ruled): A staged or retained database copy is checked by file hash only; fingerprinting one is its own named step.

## R664(b)

2026-10-18, session 12r.

```text
R664(b) — R656(c) as applied from here: a plan commit that carries N dated notes inside table-row
  cells has exactly N "−" lines, each verified by prefix_check.py's rule; a commit that carries no
  cell note (an appended block, a note paragraph after a closure, a new file) is verified by its "−"
  count alone, which must be 0. Item 0 of 12r is the second kind. Acceptance gate 2 of 12r-OPEN reads:
  0 "−" lines in the plan diff, measured by the staged diff.
```

Named by: R675(g), R678(d), R679(b), R683(f), R684(g), R692(f), R701(f), R709(f), R719(e).

Gloss (CC's, not ruled): A plan commit with N notes in table cells has exactly N minus lines; one with only appended text has none.

## R666(c)

2026-10-18, session 12r.

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

Named by: none.

Gloss (CC's, not ruled): A launch brief states the earlyoom settings as confounders and records free memory at launch and exit; an earlyoom kill of Ollama is a run fault.

## R667(e)

2026-10-19, session 12r.

```text
R667(e) — I5: the premise ("CLAUDE.md carries … watchdog 900 s") was an architect's embellished
  value; the fact that mattered (the section exists and names the watchdog and restart paths) held.
  CC's proceed is accepted this once and recorded; from here an INFERRED item names only the fact that
  must hold, and a brief that wants a value checked names it as a separate reading. The global
  CLAUDE.md's "300s" stays on the HK-ARCH line (F5, R650(g)).
```

Named by: none.

Gloss (CC's, not ruled): An assumption to verify names only the fact that must hold; a value to check is a separate reading.

## R672(a)

2026-10-19, session 12r.

```text
R672(a) — The 12r-CLOSE closeout block is accepted as printed. The brief's step-7 element list stood in
  for the 12q list (I6, a reading) and is the on-disk form from here, as printed in this block.
```

Named by: none.

Gloss (CC's, not ruled): The closeout element list printed at 12r became the on-disk form at that time (later superseded by R678(a) and R683(b)).

## R672(f)

2026-10-19, session 12r.

```text
R672(f) — Notes, no rows: the baseline recount runs on the plan at d7bafc4 by sha256, as the 12r
  script's header says — the chain form from here; the -shm mtime movement is F1's effect; the
  seven scratch directories are the count the block records.
```

Named by: R675(e), R695(c), R707(e).

Gloss (CC's, not ruled): The baseline recount runs on a named plan version by sha256, as a chain.

## R673(a)

2026-10-19, session 12s.

```text
R673(a) — The 2(d) clause "line count = 7,168 + the '+' count" was an architect premise that ignored
  the "−" line the same brief required; withdrawn. The measured form is the rule from here: a plan
  commit's line count is the prior count plus its "+" lines minus its "−" lines (7,168 + 63 − 1 =
  7,230). The stop was correct, at the earliest point that left both repositories clean.
```

Named by: none.

Gloss (CC's, not ruled): A plan commit's line count is the prior count plus its plus lines minus its minus lines.

## R674(b)

2026-10-19, session 12s.

```text
R674(b) — 3(i)'s "No database is opened for this" was an architect clause that, applied to the five
  restore points, forbade the only instrument that can verify them; withdrawn for restore points.
  From here, retention is two checks with two instruments: retained throwaway reviews and U06's copy
  by review.db file sha256 only, never opened (R662(e), F9, unchanged); the five restore points by
  db_fingerprint --compare against their recorded records, opened mode=ro, as 12r-OPEN did, a named
  step. The five are re-verified at item 0 of 12s-U14A. The sizes, mtimes and file sha256s CC
  recorded are readings and become a second record for the five, written into the 12s-U14A
  evidence directory.
```

Named by: R675(b).

Gloss (CC's, not ruled): Retention had two instruments: hash for throwaway copies, fingerprint for the five restore points (the second part amended by R675(b)).

## R675(b)

2026-10-19, session 12s.

```text
R675(b) — R674(b) is amended on the measured fact that 12r-OPEN's retention record holds text lines
  and --compare takes a JSON record (I5 false). From here the five restore points are verified by
  review.db file sha256 against docs/session-reports/session-12/12s_U14A/restore_points_12s.txt,
  the record of reference for them, as the throwaways are (R662(e)); they are never opened. Their
  fingerprint values stay recorded in the plan as what they were when each copy was taken, and
  the JSON records that equal them by hash (read-out §13) are not paired to the files. One
  instrument for every never-opened copy.
```

Named by: none.

Gloss (CC's, not ruled): Every never-opened copy, restore points included, is verified by file hash and never opened.

## R676(c)

2026-10-19, session 12s.

```text
R676(c) — The extra docs commit 9f1809a (variant_measurement.md before any engine edit) is accepted
  and is the form from here: a brief's conditional proceed commits its measurement and the branch it
  states before the edit that depends on it. The variant JSON landing with the read-out is a
  reading, no action.
```

Named by: R685(g).

Gloss (CC's, not ruled): A brief's conditional proceed commits its measurement and chosen branch before the edit that depends on them.

## R676(d)

2026-10-19, session 12s.

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

Named by: R700(b).

Gloss (CC's, not ruled): Ad-hoc pytest runs pass --basetemp under scratch, so the shared temp root holds only gates and the nightly.

## R677(f)

2026-10-19, session 12s.

```text
R677(f) — The band (R657(d), R674(c), R676(e)): cut from the last four standard gates on this host
  state, 871.59, 906.50, 858.50 and 895.02 s (mean 882.90 s), confounded readings included with
  their confounders recorded: 795–971 s. The session-12t startup verify's gate is matched against
  it; a gate outside it is reported with its concurrent jobs, not adapted to. A confounded reading
  enters the mean from here; the host's concurrent encodes are its usual state (R676(e)).
```

Named by: R678(e), R680(c), R682(d), R683(c).

Gloss (CC's, not ruled): A confounded gate reading still counts toward the wall-time band's mean, with its confounder recorded.

## R678(a)

2026-10-19, session 12s.

```text
R678(a) — The 12s-CLOSE closeout block is accepted as printed. Its element order, read from the 12r
  transcript, is the on-disk form from here. The closure's two-stop count stands; the brief's "one"
  was the architect's. INT-s12-1's text cell as quoted from 12s_U14A.md stands (R664(d)); R675(e)'s
  parenthesis was the report's summary, not the read-out's sentence.
```

Named by: R683(b).

Gloss (CC's, not ruled): The closeout block's element order is the one read from the 12r transcript, on disk from then on.

## R678(f)

2026-10-19, session 12s.

```text
R678(f) — Notes, no rows: the recount chain now runs 12q → 12r → 12s by sha256 and is the form from
  here, one script per ruling that opens rows; the 12i L03 reproducer stays in scratch,
  uncommitted, and 12s_U15A/'s is the instrument; the six ruling files of this session live where
  the closeout tables them.
```

Named by: R685(f), R691(f), R701(c), R714(c).

Gloss (CC's, not ruled): The recount chain runs by sha256, one script per ruling that opens rows.

## R680(b)

2026-10-20, session 12t.

```text
R680(b) — Reading, no row: 00a1dc5 stands as pushed (no amend after a push). The "a356f73 form"
  means the subject line and its trailers; from here a ledger commit carries both. A dated line in
  the 12t closure; the hook row itself is correct.
```

Named by: R683(a), R691(d), R692(a), R701(a).

Gloss (CC's, not ruled): A ledger commit carries its subject line and its trailers.

## R680(c)

2026-10-20, session 12t.

```text
R680(c) — Reading, no row: 872.27 s is the fifth gate on this host state (871.59, 906.50, 858.50,
  895.02, 872.27 s), the first with no concurrent job; the band stays 795–971 s until the 12t
  closeout cuts it in R677(f)'s form. The concurrent-jobs read excludes the snapshot shell's own
  PIDs from here; attributed self-matches are not jobs.
```

Named by: none.

Gloss (CC's, not ruled): The concurrent-jobs read leaves out the snapshot shell's own process ids.

## R683(b)

2026-10-20, session 12t.

```text
R683(b) — The element-order record: the 12t-CLOSE brief's I1 named two places that did not hold it
  (an architect premise); the record is the archived 12s transcript, now committed as
  12t_close/closeout_12s_found.txt. From here, docs/session-reports/session-12/12t_close/
  closeout_12s_found.txt is the on-disk record of the closeout block's element order that R678(a)
  governs, and a closeout brief names it. No row.
```

Named by: none.

Gloss (CC's, not ruled): 12t_close/closeout_12s_found.txt is the on-disk record of the closeout element order, and a closeout brief names it.

## R684(d)

2026-10-22, session 12u.

```text
R684(d) — "earlyoom kill lines this boot" means, from here, kernel-journal lines matching the
  pattern sending SIG(TERM|KILL) to; startup configuration lines are not kills. The 12u-open
  recorded file is the form. No row.
```

Named by: none.

Gloss (CC's, not ruled): An earlyoom kill line means a kernel line saying `sending SIG(TERM|KILL) to`; configuration lines are not kills.

## R684(e)

2026-10-22, session 12u.

```text
R684(e) — The item-0 transcription check is restated: a brief's 2a self-diff confirms the copy,
  not the transcription (CC's finding 4, an architect premise). From here the ruling that accepts
  an item-0 transcription carries the architect's independently hashed copy of the ruling text
  (the R679(c) form), and the brief's 2a self-diff clause is dropped. For R683 the architect's
  copy hashed identically (bf271df1…fd0a, 5,675 bytes, 67 lines); Block 81 stands.
```

Named by: R692(a), R693(a), R694(a).

Gloss (CC's, not ruled): The ruling accepting a transcription carries the architect's own hashed copy of the ruling, instead of a self-diff in the brief.

## R696(b)

2026-10-23, session 12v.

```text
R696(b) — The gate is accepted as run. The empty-window clause of 2g was written wider than the
  rule it serves: R666(a) stops a gate for a model load during it (the health job's case). From
  here, a read-only request in the window (HEAD /, GET /api/ps, /api/tags, /api/version) with
  nothing resident before and after is a reading, recorded with its timestamp and, where known,
  its sender; a load or a generate during the gate is the stop. The 16:59:32 ollama ps came from
  an interactive shell on the box, not from this lane's jobs or the suite; its sender is a
  reading the PI may name by a later note. The series gains 883.80 s. No re-run.
```

Named by: R697(g), R698(f), R699(f), R701(f), R702(d), R709(f), R711(c), R719(e), R720(f), R729(e), R731(f), R737(e).

Gloss (CC's, not ruled): A read-only Ollama request in a gate window with nothing loaded is a reading; a load or generate is the stop.

## R697(b)

2026-10-23, session 12v.

```text
R697(b) — Accepted as the standing form, no row: the old template taken by commit hash (37bb382),
  not by a HEAD-relative name; committed requests carry the paper text's sha256 and length in
  place of the text, the full requests in scratch by hash (the 12r-U13-PROBE form); a read-only
  data symlink inside a scratch worktree, not committed. The pkill -f self-match is a reading:
  briefs name a pid, never a pattern, when a process is to be stopped.
```

Named by: none.

Gloss (CC's, not ruled): Probe templates are taken by commit hash, committed requests carry the paper text's hash instead of the text, and a process to stop is named by pid.

## R700(c)

2026-10-24, session 12v.

```text
R700(c) — Note, no row: the tree check before 731ebf2 compared the wrong lines and the commit
  proceeded on a separate command chain; the check made afterwards (diff hash and test-file
  hash equal to the gate-launch record) establishes the commit as the gate's tree. From here a
  fix brief states the tree check as the one command whose success the commit is conditioned
  on, never two chains.
```

Named by: R706(e).

Gloss (CC's, not ruled): A fix brief states its tree check as the one command the commit depends on, never two chains.

## R702(b)

2026-10-24, session 12w.

```text
R702(b) — Readings, no row: (i) claude-config's 1ac7f58 (surgical-cv-spike rows 09cf951 and
  1a0c649, primer lines 41–45 intact) is a foreign commit accepted under R663(b); the open was
  right to write no note. (ii) The two duplicate wait loops ended on their own and left nothing
  running; from here a brief that waits on a background job starts one wait, and the report
  names its pid. (iii) 12v_close/ holding files beyond I7's list is the closeout's shape; I7 is
  met. (iv) The recount on the plan at 1557600 reproducing the 12v output is the expected
  result: a block appended after the decision log changes no row.
```

Named by: R704(e).

Gloss (CC's, not ruled): A brief waiting on a background job starts one wait, and the report names its pid.

## R703(j)

2026-10-24, session 12w.

```text
R703(j) — Readings, no row: CC's finding 10 — from here any pytest run outside a standard gate
  passes --basetemp under its own scratch directory; another lane rotating a gate's basetemp out
  of the shared root is harmless because the basetemp is read immediately after the gate
  (R660(c)). Finding 11 — the direct mode=ro PRAGMA schema reads, with the fingerprint
  IDENTICAL, are accepted.
```

Named by: none.

Gloss (CC's, not ruled): Any pytest run outside a standard gate passes --basetemp under its own scratch.

## R705(c)

2026-10-25, session 12w.

```text
R705(c) — The inventory regeneration is accepted (U38's precedent at 731ebf2). From here a fix
  brief that edits engine/ lists docs/inventory/ among its touched paths.
```

Named by: none.

Gloss (CC's, not ruled): A fix brief that edits engine/ lists docs/inventory/ among its touched paths.

## R705(d)

2026-10-25, session 12w.

```text
R705(d) — The tree check: the first version failed and was revised before the commit. The
  revision is accepted on its evidence (HEAD and every file's sha256 equal to the gate-launch
  record). From here a check command that fails is a stop, including when the check itself is
  at fault; the revised check is proposed in the report and run only under a ruling.
```

Named by: R706(e), R716(a).

Gloss (CC's, not ruled): A failing check is a stop even if the check is what's wrong; a revised check runs only under a ruling.

## R706(e)

2026-10-25, session 12w.

```text
R706(e) — The check deviation is accepted on its evidence (index unchanged since the PASS line).
  From here a check and its commit run as one chain, the commit executing only on the check's
  exit 0 in the same invocation (R700(c), R705(d)); a recorded PASS line is not a condition.
```

Named by: R720(c).

Gloss (CC's, not ruled): A check and its commit run as one chain, so the commit runs only if the check exits 0 in that invocation.

## R707(a)

2026-10-25, session 12w.

```text
R707(a) — The stop is upheld. R706(d)'s "A and C should; B alone does not" was the architect's
  prediction written as an expected value; that was an architect error, not CC's (as R703's I3
  fallback and V1A's Q2 test were). From here a ruling or brief states a predicted outcome only
  as "to be measured", never as an expected value that gates a stop, unless the brief names it
  as an acceptance criterion.
```

Named by: R708(a), R736(c).

Gloss (CC's, not ruled): A brief states a predicted outcome as 'to be measured', unless it names it as an acceptance criterion.

## R711(c)

2026-10-26, session 12x.

```text
R711(c) — Readings, no row: the two read-only requests at 02:30:56 (HEAD /, GET /api/ps) are
  unattributed and a reading under R696(b); the inventory-exit slip was corrected before any
  check rested on it; a gate in either lane rotates the other lane's basetemps out of the
  shared temp root, which the dgx-infra item already carries; dbchecks.py exists at both paths
  with one sha256, and 12f_repro/dbchecks.py is the instrument from here.
```

Named by: R720(e).

Gloss (CC's, not ruled): 12f_repro/dbchecks.py is the instrument of record for database checks.

## R716(b)

2026-10-26, session 12x.

```text
R716(b) — The INT-j12-15 note's target is the inventory row at :649, the one with an Evidence
  cell. The coverage-map row at :1382 is not touched. From here a note's target is the row with
  the named ID and an Evidence cell, matched by its cell count, and a brief that names a row
  says so.
```

Named by: none.

Gloss (CC's, not ruled): A note's target is the row with the named id and an Evidence cell, matched by cell count, and a brief naming a row says which.

## R720(b)

2026-10-28, session 12y.

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

Named by: R722(g), R731(a), R732(a).

Gloss (CC's, not ruled): A brief transcribing two rulings in one commit carries the pair's clause form (refined by R732(a)).

## R720(c)

2026-10-28, session 12y.

```text
R720(c) — Finding 1 is CC's and is accepted as reported: the commit is sound because the check
  printed PASS, and the chain did not meet R706(e). From here a check-and-commit chain contains no
  pipe: the check writes its own evidence file (a redirect inside the check, or the script writes
  it), and the chain is <check> && <commit>, one line, links joined by && (R716(e)). A tee in the
  chain is a defect of the chain whatever the check printed. No row; a note on R706(e)'s rule in the
  standing session rules at the next closeout.
```

Named by: none.

Gloss (CC's, not ruled): A check-and-commit chain has no pipe: `<check> && <commit>`, one line, with the check writing its own evidence file.

## R720(e)

2026-10-28, session 12y.

```text
R720(e) — Readings, no row: the 16:19:34 HEAD / and GET /api/ps pair in the gate window, as 12x's
  at 02:30:56 (R711(c)); the dgx-snapshot-user timer's read-only requests at 10:31 on both days
  (I15); NV_ERR_NO_MEMORY seventeen this boot, the new line inside the 2026-10-28 llama3.3:70b load,
  which succeeded (dgx-infra; Run 7's confounder statement); the durations reference 2,486 phases
  against the gate's 2,454 with no phase slower by more than 10 s; the temp root at the open (five
  entries, 4,141,734,999 B) and after (pytest-130, -131, -132, current); the I12 clauses, all
  matched at 3e; finding 5's corrected id extraction, the wrong first lines left in place as
  evidence (never edited); finding 6's widened concurrent-jobs pattern (nvenc, uvicorn added) stands
  from here as the snapshot's pattern, recorded with the 12y host snapshot. R510 for 12y so far: rows
  in 0, rows out 0, no class move.
```

Named by: none.

Gloss (CC's, not ruled): The concurrent-jobs pattern includes nvenc and uvicorn.

## R722(a)

2026-10-28, session 12y.

```text
R722(a) — The stop is correct and the cause is the architect's: the brief's Touched list named the
  tests the change adds and the fence it edits, and omitted the tests that pin the two seams
  R721(b) removes. A ruling that moves a call off a patched name moves its tests; the brief
  should have asked CC to census the patch sites before writing the list. From here a brief that
  removes or renames a call site carries a pre-step that greps tests/ for patches of that name and
  puts every file found in the Touched list before any edit.
```

Named by: R724(a), R725(b), R725(e), R731(f), R738(f).

Gloss (CC's, not ruled): A brief removing or renaming a call site first greps tests/ for patches of that name and lists every file found in Touched.

## R723(a)

2026-10-28, session 12y.

```text
R723(a) — Both stops are correct and both causes are the architect's, of one kind: the brief
  predicted a count ("exactly 3") and a census result (the files A3's grep would find) instead of
  asking for the census and taking its count — the 12x lesson (4), repeated. From here a brief
  states a "−" count as "one per noted row, N rows named" and lets CC compute N from the rows; and
  a census pre-step names its patterns and asks for classification, with the STOP condition
  written on the classification (a patch of a seam the commit removes, found in a file outside the
  Touched list), not on the file count.
```

Named by: R724(a), R725(e), R729(c), R729(e), R731(d), R737(e).

Gloss (CC's, not ruled): A brief states a minus count as a rule for CC to compute, and writes a census's stop condition on the classification, not the file count.

## R724(a)

2026-10-28, session 12y.

```text
R724(a) — The stop is correct; the cause is the architect's, the fourth of this unit: the census rule
  of R722(a) looked for patches of removed seams and not for pins of a record the commit extends.
  From here that pre-step also greps tests/ for exact-set assertions and hard-coded hashes over any
  record the commit adds a key to (the record's existing key names as the pattern), and classifies
  what it finds as (iii) a shape pin — in the Touched list, rewritten to the new shape with its
  purpose kept, or STOP. R723(a)'s rule covers the counting.
```

Named by: R725(b), R725(e).

Gloss (CC's, not ruled): The census also finds exact-set and hard-coded-hash pins over any record the commit extends; each is rewritten keeping its purpose, or it stops.

## R725(c)

2026-10-28, session 12y.

```text
R725(c) — docs/inventory/ regeneration in an engine commit is standing practice (e81d699, 731ebf2,
  2c3822b, 5dbd3c4) and is accepted here; from here every engine-commit brief lists docs/inventory/
  in its Touched list by default.
```

Named by: none.

Gloss (CC's, not ruled): Every engine-commit brief lists docs/inventory/ in Touched by default.

## R729(c)

2026-10-28, session 12y.

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

Named by: none.

Gloss (CC's, not ruled): The R117 addendum's label form and its placement are the record's form.

## R730(c)

2026-10-29, session 12z.

```text
R730(c) — Rule from here: every full commit hash a brief or a ruling carries is 40 hexadecimal
  characters, checked by the architect before issue; CC resolves every full hash a brief names
  with git rev-parse --verify at the step that first uses it, and a hash that does not resolve
  is a stop. §8 gains it as rule 8 at v97.
```

Named by: R736(f), R737(e).

Gloss (CC's, not ruled): Every full commit hash is 40 characters, checked by the architect, and rev-parsed by CC at first use; one that doesn't resolve is a stop.

## R731(b)

2026-10-29, session 12z.

```text
R731(b) — Finding F4 is the architect's: the brief named the 12y-U18C gate directory from unified
  plan v96's summary ("gate/") instead of having CC list it; the record's "gate_round1/" governed
  (R18). v97 corrects the plan's name. Finding F6: the 12y record carries no tmux launch line, so
  the "standard detached form" CC used is accepted as the gate's launch; from here every gate's
  evidence directory records the complete launch line, tmux wrapper included, and the next
  brief's I3 quotes it from there. No row.
```

Named by: R732(d), R738(c).

Gloss (CC's, not ruled): Every gate's evidence directory records the complete launch line, tmux wrapper included.

## R731(c)

2026-10-29, session 12z.

```text
R731(c) — The 12z-OPEN gate's evidence under ~/scratch/12z-open/ is not under docs/, as 12x-OPEN's
  was not (R719(c)). 12z-U19A's item 0 copies the gate's files into
  docs/session-reports/session-12/12z_open/gate_12z_open/ (cp; names, sizes and sha256 listed
  before the copy, every sha256 equal after), in the item-0 commit with r730.txt and r731.txt.
  The scratch directory is not moved or edited. From here a session-open brief's step 3 commits
  its gate evidence under the session's open directory in a commit of its own, so no later item 0
  carries it.
```

Named by: none.

Gloss (CC's, not ruled): A session-open brief commits its gate evidence under the session's open directory, in a commit of its own.

## R732(a)

2026-10-29, session 12z.

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

Named by: R736(f), R739(c), R741(d).

Gloss (CC's, not ruled): A multi-ruling brief names the pattern, not a model block; CC finds the most recent such commit and quotes its form, and the record wins over the brief.

## R732(d)

2026-10-29, session 12z.

```text
R732(d) — Finding 3: the 12z-OPEN gate ran before R731(b) set the rule, so its evidence carries no
  launch line. The copy under 12z_open/gate_12z_open/ gains one file, launch_line.txt, carrying
  the tmux line as the 12z-OPEN report printed it, with one leading line stating that it was
  written at 12z-U19A's item 0 from that report and not captured at launch. It is not one of the
  16 copied files and is listed apart from them in the report. From here the gate's own wrapper
  writes the launch line into the evidence directory at launch (R731(b)'s rule, made concrete).
```

Named by: R733(a), R738(c).

Gloss (CC's, not ruled): The gate's own wrapper writes the launch line at launch (made concrete by R738(c)).

## R733(a)

2026-10-29, session 12z.

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

Named by: R738(d).

Gloss (CC's, not ruled): A READ item quoting a report is quoted from that report's own text.

## R735(c)

2026-10-30, session 12z.

```text
R735(c) — The note label form CC used, naming the ordering sub-clause (R734(f)), is the record's
  form and stands; I9's literal label was the architect's shorthand. From here a brief carries a
  note's label as "the record's form, the sub-clause named", not a literal.
```

Named by: none.

Gloss (CC's, not ruled): A brief gives a note's label as 'the record's form, the sub-clause named', not as a literal.

## R738(b)

2026-10-30, session 12aa.

```text
R738(b) — Finding 1 is CC's: r737.txt was first written with a line not in Section 1 and patched
  before its hash, against "written once from Section 1 and then only read". The final file
  equals the architect's hash and nothing wrong reached a commit; no stop of the session, no row.
  A patch before the hash removes the hash's power to stop. Rule from here: an rNNN.txt is hashed
  immediately after its one write, before it is read back or edited; a mismatch at that hash is a
  stop, and the file is not edited to reach a match.
```

Named by: R739(a), R739(c), R741(d).

Gloss (CC's, not ruled): A ruling file is hashed straight after its one write, before any read-back; a mismatch stops, and the file is not edited to match.

## R738(c)

2026-10-30, session 12aa.

```text
R738(c) — Finding 3: I3's premise that a wrapper writes the launch line was the architect's, taken
  from R732(d)'s wording, which no gate on disk implements. CC proceeded on a false INFERRED item
  instead of stopping; the method used is accepted this once because it meets R731(b) and is
  recorded, and the rule that a false INFERRED item stops the brief is unchanged. The form from
  here: the launching step holds the launch line in one variable, writes the timestamp sentence
  and that variable's value into launch_line.txt, then executes that variable, so the line
  written and the line run cannot differ.
```

Named by: none.

Gloss (CC's, not ruled): The launch step holds the line in one variable, writes it to launch_line.txt, and runs that variable, so the two cannot differ.

## R738(d)

2026-10-30, session 12aa.

```text
R738(d) — Finding 4: gate_12aa_open/'s nineteen files (gate_c2's set: collected_ids.txt and
  tree_at_launch.txt included) are the gate-evidence set of record from here; 12z_open's
  seventeen stand as the earlier form, its launch line reconstructed (R733(a)).
```

Named by: none.

Gloss (CC's, not ruled): gate_12aa_open's nineteen files are the gate evidence set of record.

## R739(b)

2026-10-30, session 12aa.

```text
R739(b) — The mismatched file is not edited and not deleted: it is moved by its path to
  ~/scratch/12aa-u20a/r738_first_write.txt, its sha256 verified there. r738.txt is then written
  once more from the 12aa-U20A brief's Section 1 and hashed immediately; a second mismatch is a
  stop. From here a ruling file that fails its hash is cleared this way, under a ruling.
```

Named by: none.

Gloss (CC's, not ruled): A ruling file that fails its hash is moved to scratch, verified there and rewritten, only under a ruling.

## R742(b)

2026-10-30, session 12aa.

```text
R742(b) — Rule from here: a check script takes every value that varies by commit (ruling
  numbers, N, file names, expected hashes) from its arguments or from the files it checks; no
  such value is carried as a literal from an earlier script.
```

Named by: none.

Gloss (CC's, not ruled): A check script takes per-commit values from its arguments or the files it checks, never as literals carried over.
