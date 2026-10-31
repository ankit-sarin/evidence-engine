# Standing rules (adopted, R754)

**Status: adopted under R754, selected by the committed id list.** A quoted clause is the rule's source; a figure, date, session condition or named commit inside it is that day's, and the current value is the "Expected values at session-N open" block's. A rule this file lacks is a rule a brief must restate. Clause text is the plan's, by script; only the lines headed "Gloss (CC's, not ruled):" are composed, and they rule nothing.

- Generator: `docs/session-reports/tools/extract_rules.py`, sha256 `fc95e62a246a4571295a99180f61b93dd10fc11a528f98d4320dcb8bb82bd7f2`
- Plan blob: `docs/plan/ENGINE_REFACTOR_PLAN.md`, sha256 `21b9e51c5f68281a590bdd3c65da01feed647f053dd6a3386bff8c1ad9d58f06`
- Id list: `docs/plan/standing_rules_ids.txt`, sha256 `e648a692094ca103f59266e553d226f93bca1437763433b79a1ef07908858ad5`
- Selection: the id list: Tier A 81, Tier B 8, 89 of 1362 clauses in the log.
- "Named by" lists later clauses that carry the exact id; a range or a bare ruling number does not count.

# Tier A — process rules

## R86

2026-09-23.

```text
R86 — **The no-live-write window is 07:00–10:35 UTC**: the 07:00 health check, the 09:00 nightly suite, and the 10:30 `dgx-snapshot-user` timer with its 2-minute jitter (row I15). Appendix B and CLAUDE.md's ops block carry the new window. *(Closeout note, 2026-09-23: the plan's addenda carry it; Appendix B lives in the unified plan, and the CLAUDE.md half was not applied at this closeout — see the I13 stop in the session-7b closure paragraph)*
```

Named by: R96, R648(c), R747(c), R749(b).

Gloss (CC's, not ruled): The window with no live write to review.db is 07:00–10:35 UTC.

## R102

2026-09-23.

```text
R102 — The in-place authority-cell edit on R90–R94 is accepted once, as the record of their confirmation; decision-log rows otherwise remain append-only.
```

Named by: R372, R465, R575, R640(c), R653(a), R656(a), R656(b), R659(f), R678(b), R679(d), R720(b), R729(c), R730(b), R747(c), R757(d).

Gloss (CC's, not ruled): Decision-log rows are append-only; the one in-place edit on R90–R94 is not a precedent.

## R370

2026-10-01, session 12a.

```text
R370 — **The no-push reversal (issued outside a block, in the 12a-CLOSE brief).** "Reversal recorded here: the "no push until the freeze" instruction in 12a-0b-R2 and later briefs is withdrawn; sessions close pushed and measured at origin, as before." *Transcribed:* evidence-engine pushed at this closeout; claude-config by the 12a wrap
```

Named by: R598.

Gloss (CC's, not ruled): Sessions close pushed and measured at origin; the earlier no-push instruction is withdrawn.

## R514

2026-10-08.

```text
R514 — **A finding is closed when its own acceptance test passes** (the test in the original assessment or in `12f_triage_rows.md`). Capability work beyond that test stays in its owning state. Clarifies R512; does not narrow it. Applied: B-F13's run and configuration identity stays in S4 (junior); B-F10's terminal-state writer stays D20's.
```

Named by: R517, R676(b), R682(b), R688(f), R691(b), R721(f), R728(b), R736(b).

Gloss (CC's, not ruled): A finding closes when its own acceptance test passes, and capability beyond that test stays in its owning state.

## R647(c)

2026-10-15, session 12p.

```text
R647(c) — From the 12p closeout, every "Expected values at session-N open" block states the
  temp root at the open as the nightly's basetemp directory (one pytest-N directory plus the
  pytest-current link), with the size as a reading not matched, never as "empty". What a
  closeout does with the root is ruled at that closeout, as R645(d) was.
```

Named by: R648(b), R650(h), R652(c), R653(d), R662(d), R751(e), R753(e).

Gloss (CC's, not ruled): Expected-values blocks state the temp root at the open as a reading, size not matched, never as "empty".

## R654(d)

2026-10-16, session 12q.

```text
R654(d) — From this ruling on, "another lane's rows found uncommitted in PROJECT_LEDGER.md at a
  session open" is case B's neighbour, not case C: they are committed by this lane's next
  ledger step, named in its message, and recorded in the session's closure. A dirty
  claude-config for any other reason (edits to primer.md or any file other than
  PROJECT_LEDGER.md, a non-row change inside PROJECT_LEDGER.md, divergence from origin, this
  lane's rows missing) stays a STOP.
```

Named by: R662(c), R663(b), R672(d), R678(d), R683(f), R688(a), R692(f), R698(a), R698(f), R699(a), R700(a), R701(a), R701(f), R709(f), R719(e), R729(e), R737(e), R755(a).

Gloss (CC's, not ruled): Another lane's uncommitted ledger rows found at an open are committed by this lane's next ledger step and named; any other dirt in claude-config is a stop.

## R655(b)

2026-10-16, session 12q.

```text
R655(b) — "0/0 against <file>" means, here and in every later expected-values block, the set
  difference in both directions between the gate's collected ids and the file's ids: 0 ids
  added, 0 removed, computed on sorted lists. Order is not compared.
```

Named by: R749(b).

Gloss (CC's, not ruled): "0/0 against a file" is a set difference both ways on sorted id lists; order is not compared.

## R656(b)

2026-10-16, session 12q.

```text
R656(b) — The cell rule from this ruling on: in a decision-log table cell, "|" is written
  "\|", and a backslash immediately before a pipe is written "\\". A backslash anywhere else
  is written as it is, as the four earlier cells did. Earlier cells are left as printed
  (R102).
```

Named by: R658(e), R659(f), R660(c).

Gloss (CC's, not ruled): In a decision-log table cell a pipe and a backslash before a pipe are escaped; the fenced text stays verbatim, and earlier cells stay as printed.

## R656(d)

2026-10-16, session 12q.

```text
R656(d) — The interpreter is .venv/bin/python; the first fingerprint attempt's exit 127 is a
  reading, and the 22:43:00Z run is the one of record. Briefs from here name
  .venv/bin/python for every repository script. No row.
```

Named by: none.

Gloss (CC's, not ruled): Briefs name .venv/bin/python for every repository script.

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

## R660(c)

2026-10-17, session 12q.

```text
R660(c) — F1: the band series on the upgraded host holds 868.02 and 871.59 s; R657(d) stands.
  F2: a gate's basetemp is read immediately after the gate, before any other pytest
  invocation; no row. F3: the R656(b) cell renderer is committed once at 12q's closeout as
  docs/session-reports/session-12/12q_close/render_cell.py, the scratch copies retired;
  tooling, Class 3, no row.
```

Named by: R664(c), R676(d), R703(j).

Gloss (CC's, not ruled): A gate's basetemp is read immediately after the gate, before any other pytest run; the clause's other two items are dated.

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

## R663(b)

2026-10-17, session 12r.

```text
R663(b) — S2: claude-config 9aa59b3 is accepted as found. The ledger commit is R653(c)'s case B. The
  primer commit 2466df3 is a new case, ruled here and from now on: a committed change by another lane
  to claude-config that leaves this lane's ledger rows and primer.md lines 41–45 intact is accepted as
  found and recorded by a dated note naming each commit by hash and author lane; it is not a stop.
  R654(d)'s stops are unchanged (any change to lines 41–45, a missing row of this lane, uncommitted
  non-row dirt, divergence). Item 0(c)'s note is written, naming 2466df3 and 9aa59b3.
```

Named by: R672(d), R678(d), R683(f), R691(d), R692(f), R693(b), R701(f), R702(b), R709(c), R709(f), R710(c), R717(a), R719(e), R726(b), R729(e), R730(d), R737(e), R738(e), R751(d).

Gloss (CC's, not ruled): Another lane's committed claude-config change that leaves this lane's rows and primer lines 41–45 intact is accepted as found, noted by hash and lane.

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

## R679(b)

2026-10-20, session 12t.

```text
R679(b) — The prefix_check.py clause of step 2e was an architect clause and is withdrawn; R664(a)
  had already withdrawn the same premise in 12r, and R664(b) governs: a commit carrying no cell
  note is verified by its "−" count alone, which must be 0, read from git diff --numstat of the
  plan file. The rest of 2e stands.
```

Named by: R684(g).

Gloss (CC's, not ruled): prefix_check.py's clause is withdrawn; a commit with no cell note is verified by its plan "−" count, which must be 0.

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

## R683(c)

2026-10-20, session 12t.

```text
R683(c) — The band (R657(d), R677(f), R682(d)): cut from the last four standard gates on this host
  state, 858.50, 895.02, 872.27 and 887.58 s (mean 878.3425 s), the first two beside a surgical-cv
  encode, the last two on a quiet box, confounded readings included with their confounders
  recorded: 791–966 s. The session-12u startup verify's gate is matched against it; a gate
  outside it is reported with its concurrent jobs, not adapted to. The series stands at 871.59,
  906.50, 858.50, 895.02, 872.27, 887.58 s.
```

Named by: R692(c), R693(f), R700(e), R701(b), R702(c), R707(g), R709(b), R719(b), R729(b), R737(b), R748(b).

Gloss (CC's, not ruled): The band is cut from the last four standard gates on the host state, confounded readings included and recorded, rounded inward; a gate outside it is reported, not adapted to; its figures are that day's.

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

## R693(d)

2026-10-23, session 12v.

```text
R693(d) — Finding 3, no row: the health job's log records no lock line, so "lock taken or
  skipped" cannot be read from it. Open briefs stop asking it; whether the health job takes the
  experiment lock is the dgx-infra item's question.
```

Named by: none.

Gloss (CC's, not ruled): The health job's log has no lock line, so open briefs do not ask whether the lock was taken.

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

Named by: R697(g), R698(f), R699(f), R701(f), R702(d), R709(f), R711(c), R719(e), R720(f), R729(e), R731(f), R737(e), R753(d).

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

Named by: R753(e).

Gloss (CC's, not ruled): Any pytest run outside a standard gate passes --basetemp under its own scratch.

## R704(e)

2026-10-24, session 12w.

```text
R704(e) — Readings, no row: (i) ledger 5a9ae9e's message timestamp (17:12:22Z) differs from its
  row (17:11:21Z); the row is the record; the message stands, not amended. (ii) The second wait
  at the census broke R702(b)(ii): one wait means one waiting process of any kind, foreground or
  background, for one job. (iii) The census cannot see a test asserting a manifest-body key is
  absent; the grep found none.
```

Named by: none.

Gloss (CC's, not ruled): One wait means one waiting process of any kind for one job (item (ii)); the clause's other two items are dated readings.

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

## R714(c)

2026-10-26, session 12x.

```text
R714(c) — Readings, no row: ten non-audit keys, not eleven (the brief's count was the
  architect's; the census governs); the before-gate reads at 3.4 s, a deviation with an empty
  journal window; the landed file's first docstring line names Phase 1 and scratch, corrected
  by quoted text at the next commit that touches that file; the chained recount script's
  fix_units_dated_lines field is a literal reading done 19, so the dated lines are the count of
  record (R701(c)) and the script is not edited (R678(f)).
```

Named by: R715(c), R717(c), R729(c).

Gloss (CC's, not ruled): The recount script's fix_units_dated_lines is a literal, so the dated lines are the count of record and the script is not edited; the clause's other items are dated.

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

## R725(b)

2026-10-28, session 12y.

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

Named by: R726(d), R728(f), R731(f), R734(g), R738(f), R747(c).

Gloss (CC's, not ruled): An engine commit's test-side Touched list is not predicted: tests pinning what changes are found by census or the gate, and a red is classified as coupled or as a regression.

## R725(c)

2026-10-28, session 12y.

```text
R725(c) — docs/inventory/ regeneration in an engine commit is standing practice (e81d699, 731ebf2,
  2c3822b, 5dbd3c4) and is accepted here; from here every engine-commit brief lists docs/inventory/
  in its Touched list by default.
```

Named by: none.

Gloss (CC's, not ruled): Every engine-commit brief lists docs/inventory/ in Touched by default.

## R727(a)

2026-10-28, session 12y.

```text
R727(a) — The stop is correct; the cause is the architect's: R721(b) wrote "closing the run
  'aborted'" from C26's vocabulary without reading which exception each runner closes 'aborted'
  on. The brief's I3 caught it before an edit, which is what an INFERRED item is for.
```

Named by: R744(e), R747(c).

Gloss (CC's, not ruled): An inferred item caught a code-behaviour premise before an edit, which is what such an item is for; the clause itself records one stop.

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

## R736(c)

2026-10-30, session 12z.

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

Named by: R747(c).

Gloss (CC's, not ruled): An acceptance id that pins a non-refusal or an unchanged shape is green on its parent by construction, so the brief names it a pin.

## R737(c)

2026-10-30, session 12z.

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

Named by: R742(a).

Gloss (CC's, not ruled): Block 133's header sentence is the form for a ruling first reaching disk through a later brief, and the health log's day is read by mtime and tail; the clause also carries dated readings.

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

## R746(c)

2026-10-30, session 12aa.

```text
R746(c) — Finding 2 is accepted and is the rule from here: a red at the gate caused by a census miss
  is class (c); it is fixed without changing any test's assertion and the gate is re-run once; the
  red run's evidence is committed beside the green one and is not a member of the series. Any
  other red at the gate is a stop.
```

Named by: none.

Gloss (CC's, not ruled): A red at the gate from a census miss is class (c): fixed without changing an assertion, re-run once, its evidence kept outside the series; any other red is a stop.

## R746(d)

2026-10-30, session 12aa.

```text
R746(d) — Readings, no row: finding 3 (a red caused only by a missing name is reported as such in
  every record from here); findings 4, 5, 6, 7, 9 and 10; finding 8's handler list is the starting
  list of C56's census in U26, untraced.
```

Named by: none.

Gloss (CC's, not ruled): A red caused only by a missing name is reported as such in every record; the clause's other items are dated readings.

## R747(b)

2026-10-30, session 12aa.

```text
R747(b) — transcribe_ruling.py is accepted for every block from here. session_open_check.py is
  accepted for what it checks; the recount, the R510 count, the nightly's ids against the expected
  set and the health job's journal read stay explicit steps of an open until the script covers
  them (finding 4). The gate wrapper is accepted pending its first full gate, where a wrapper fault
  is reported as the wrapper's (finding 8).
```

Named by: R748(e), R753(e).

Gloss (CC's, not ruled): The tools in force and their limits: transcribe_ruling.py for every block, session_open_check.py for what it checks, the gate wrapper pending its first full gate.

## R749(b)

2026-10-31, session 12ab.

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

Named by: none.

Gloss (CC's, not ruled): An open does not wait for a day's jobs; each job is read by the first brief after it runs, against its governing expected file, and a gate must exit before 07:00 UTC.

## R750(c)

2026-10-31, session 12ab.

```text
R750(c) — D = 0 is accepted (finding 2). The brief's "EXPECTED: the ends-at R747 line" was the
  architect's prediction from the record's description, against afd179e's own numstat, and is
  withdrawn; the tool and the record governed, as the constraints say. 12ab-OPEN's rule-14 list
  gains "a tool's write semantics", and a brief that names a tool states no "−" count the tool's
  own run does not supply.
```

Named by: none.

Gloss (CC's, not ruled): A brief that names a tool states no "−" count that the tool's own run does not supply.

## R751(c)

2026-10-31, session 12ab.

```text
R751(c) — Finding 8: the session-start instruction "Read docs/plan/ENGINE_REFACTOR_PLAN.md" is read
  as scoped, from this ruling: the Status table, the latest closure paragraph and its "Expected
  values" block, the decision log's last block, and every block, row or read-out a brief names. No
  other part of the plan is taken as checked by a session, and a brief that relies on an earlier
  ruling quotes it. The CLAUDE.md line is corrected to this scope, by text quoted in that commit's
  handoff for PI approval, in the commit that adopts STANDING_RULES.md (R747(c)); not before.
```

Named by: R754(c).

Gloss (CC's, not ruled): The plan's session-start read is scoped to the Status table, the latest closure and expected values, the last block and whatever a brief names; a brief relying on an earlier ruling quotes it.

## R751(e)

2026-10-31, session 12ab.

```text
R751(e) — Readings, no row: finding 10 (stale primer lines, the dgx-infra lane's); finding 11 (the
  temp root rotated pytest-166 out at pytest-169's creation; R647(c) stands — the root is read,
  never matched); finding 12 (an evidence commit's scope is docs(session-reports) from here, the
  model 5c1474c's docs(plan) being for commits that touch the plan); finding 13 (a brief's named
  waiting process governs over a memory note; the note is CC's to reconcile, not a stop).
```

Named by: R752(d), R753(e).

Gloss (CC's, not ruled): An evidence commit's scope is docs(session-reports), with docs(plan) for commits that touch the plan, and a brief's named wait governs over a memory note; the clause's other items are dated.

## R752(d)

2026-10-31, session 12ab.

```text
R752(d) — Finding 9: the memory note feedback_tmux_no_poll.md is CC's own, outside every repository,
  and R751(e) left it to CC to reconcile; the edit is a reading, not a Touched-list breach. From
  here a brief's DO NOT TOUCH does not reach CC's own memory files, and CC reports an edit to one.
```

Named by: none.

Gloss (CC's, not ruled): A brief's DO NOT TOUCH does not reach CC's own memory files, and CC reports an edit to one.

## R753(d)

2026-10-31, session 12ab.

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

Named by: R755(a), R755(c).

Gloss (CC's, not ruled): The model-load stop is R696(b)'s, a closed pre-tag row is counted until P1 closes, and an open's pre-check reads the process table and tmux for the named archived sessions, stopping only on a survivor of this lane.

## R756(b)

2026-10-31, session 12ab.

```text
R756(b) — Readings, no row: finding 20 (the status line is one line in the file; a brief's wrapped
  quote is compared whitespace-normalised, the words verbatim); finding 21; finding 22 (primer.md's
  "read it at session start" line is working state, corrected at the 12ab wrap under R737(d)'s
  lane-line form, not before).
```

Named by: R757(d).

Gloss (CC's, not ruled): A brief's wrapped quote is compared whitespace-normalised, the words verbatim; the clause's other items are dated.

# Tier B — engine governance, pending the adoption ruling

## R25

2026-09-21.

```text
R25 — Seed, do not migrate. The event store begins empty of field-level history. Session 5 seeds it with: papers; current corpus membership as one "state at migration" paper event per paper (no reconstruction of how the state arose); parsed-text references; spec and codebook identities. Existing extractions, spans, cloud extractions, auditor verdicts, adjudication tables and workflow stamps are NOT imported — they remain in place, read-only, as a regression fixture and telemetry. The reader is validated on fixtures and then on the freshman smoke run and Run 7, not by agreement with legacy readers. Superseded as moot: R18's Q8 and Q9 clauses; R17's legacy-backfill clause; v2.1 row 17 at field level; the session-5 gate clause "new reader agrees with each old reader on the live data"; the session-6 judge target of 3,802 legacy cells. Cloud comparator arms, if needed for Paper 1, are re-run on the tagged state under declared arms (decision deferred to Run 7). Scope reset recorded in `docs/session-reports/effective-result-01/S2_phase1_readout_addendum4_20260921.md`, which supersedes read-out §5 and §7
```

Named by: R28, R31, R94, R111, R113, R188, R210, R238.

Gloss (CC's, not ruled): The event store starts empty of field-level history, seeded with papers, corpus membership, text references and identities; legacy tables stay read-only.

## R26

2026-09-21.

```text
R26 — The extraction field set is frozen for Paper 1: codebook field names and the definitions given to human extractors do not change. Elicitation, validation, tokens and field-state handling remain engine work and may change
```

Named by: R128, R571, R612.

Gloss (CC's, not ruled): The extraction field names and definitions are frozen for Paper 1, while elicitation, validation, tokens and field-state handling may change.

## R71

2026-09-22.

```text
R71 — R22-U4 extended: no cloud extraction until session 9 (unlinked writer) and session 8 (glob resolver) both land; cloud opt-in is built and tested on fixtures, the live spec keeps `cloud.enabled_arms` empty
```

Named by: R210, R237, R599.

Gloss (CC's, not ruled): No cloud extraction until sessions 8 and 9 land, and the live spec keeps cloud.enabled_arms empty.

## R510

2026-10-07.

```text
R510 — **Y4.** "Each close records pre-tag rows in / out and counts by class."
```

Named by: R540, R607, R632(a), R634(c), R635(a), R643(b), R648(e), R651(a), R659(d), R683(d), R692(d), R695(c), R698(g), R700(f), R705(e), R707(e), R720(e), R721(f), R728(e), R731(e), R736(e), R738(e), R747(b), R747(f), R751(a).

Gloss (CC's, not ruled): Each close records pre-tag rows in and out and the counts by class.

## R539

2026-10-08.

```text
R539 — **The pin is the ordering constraint:** a change to what the elicited run sends (unit construction, prompts, format schemas, stage set) lands before the freeze smoke records the binding pin. Audit-side changes (locator, verdicts) are outside the pin and may be re-run on Run 7.
```

Named by: R558, R589, R590, R675(e), R694(c), R694(h).

Gloss (CC's, not ruled): Anything that changes what the elicited run sends lands before the freeze smoke records the binding pin; audit-side changes are outside the pin.

## R541

2026-10-08.

```text
R541 — **The 12f triage table, as amended by R537 and R538, is the pre-tag list.** Its outside and internal rows are opened as inventory section L; a finding after this ruling enters only with its class and package stated (R509). The finish line is this list closed.
```

Named by: none.

Gloss (CC's, not ruled): The 12f triage table, as amended, is the pre-tag list, and a later finding enters only with its class and package stated.

## R543

2026-10-08, session 12g.

```text
R543 — **12g-OPEN.** E-TEST stays in P1 although it is Class 3: it is the acceptance test of the elicited path and lands with or immediately after INT-g6-1. A Class 3 item may ride with its subject; P7 holds the rest of the Class 3 batch.
```

Named by: R609, R612, R624, R650(e).

Gloss (CC's, not ruled): A Class 3 item may ride with its subject, and P7 holds the rest of the Class 3 batch.

## R601

2026-10-10, session 12j.

```text
R601 — Human-gate page and workbook files (narrows X3 for these rows only): - Set: rows 6, 7, 8, 9, 13, 18, 20, 36, 37, 38 and 39, and the page/workbook halves of row 19. Files: abstract_adjudication_html.py, ft_adjudication_html.py, pdf_quality_html.py, review_workbook.py, and the xlsx/JSON file-format handling of the screening importers. - Pre-tag, each row in the set closes on its minimum loud-failure fix: a wrong, incomplete or unverifiable round trip is refused or shown to the person, never applied silently. - Each row's full fix becomes a dated requirement note on J1 (the S7 operator app, sophomore). - Not in the set, with full fixes pre-tag: rows 5, 12, 14, 15, 16, 17 and 35, the importers' key cross-check (R596(d)), and every other Wave 2 row. - The R546 re-screen's FT adjudication runs only after the FT page and FT importer rows in the set have their loud-failure fixes.
```

Named by: R609, R610.

Gloss (CC's, not ruled): The named human-gate page and workbook rows close pre-tag on a minimum loud-failure fix: a wrong round trip is refused or shown, never applied silently.
