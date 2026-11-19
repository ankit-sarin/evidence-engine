# 12ai-SCRI — evidence-engine scratch inventory (read-only, to scratch)

Run 2026-11-19 14:13:52 → about 14:30 UTC, DGX `spark-59e4`, under the 12ai-SCRI brief (interim, between
sessions 12ah and 12ai). The brief answers the Digital Surgeon workbench handover of 2026-11-19, whose
inventory CLN-1 ran 2026-11-16. evidence-engine was at `b13b5eb23c86c5da9df566d9e59a9764c7831335` throughout
(main, clean, 0/0 with origin by `git ls-remote origin`). claude-config was fast-forward clean, "Already up to date".
"CC's reading" marks a classification made by reading, not by a command. Every other value names its
command or file. No database was opened: a `review.db` is identified by file sha256 only. There was no model
call, no Ollama request, no lock acquisition, no pytest run, and no script found in scratch was executed.
Every git command ran with `GIT_OPTIONAL_LOCKS=0`. Writes went only to `~/scratch/12ai-scri/`; the brief file
itself was not saved here.

Files beside this one (tables as TSV): `g1_before.tsv`, `g1_after.tsv`, `g1_diff.txt` (empty),
`g1_mainstatus_before.txt`, `g1_mainstatus_after.txt`, `s1_toplevel.tsv`, `s2_citations.tsv`,
`s3_clones.tsv`, `s3_clone_sizes.tsv`, `s3_modified_files.tsv`, `s3_untracked_ignored.tsv`,
`s3_dirty_sentences.tsv`, `clone_status/` (55 porcelain status files, `--ignored`), `patches/` (17),
`untracked/` (1 file), `s4_entries.tsv`, `s4_records_with_committed_copy.tsv`,
`s4_records_without_committed_copy.tsv`, `s4_unique_by_kind.tsv`, `s6_files.tsv`, `s7_classes.tsv`,
`s7_class_totals.tsv`, `g3_match_proof.txt`, `g3_random.bin`, `g4_numstat_check.tsv`, `SHA256SUMS` (G5, written last).

---

## S0 and E1–E8

S0: `pwd` `/home/ankitsarin/projects/evidence-engine`; branch `main`; HEAD `b13b5eb2…`; `git status --porcelain` empty;
`origin/main` = HEAD = `ls-remote origin` `refs/heads/main`. G1's baseline was taken right after these four values and the
12ah-OLRA form read, before any other read.

| | verdict | measured |
|---|---|---|
| E1 | HELD | HEAD `b13b5eb` (the 12ah closeout, `docs(plan): 12ah-CLOSE …`), clean, 0/0, equal to `ls-remote origin` |
| E2 | HELD | 157 entries named `12*` (excluding `12ai-scri`) = 154 + 3. Birth times (`stat %w`) put `12ah-open` (2026-11-19 04:40), `12ah-olra-rec` (05:46) and `12ah-close` (06:23) after the handover's inventory; `12ah-olra` (2026-11-15 23:58) predates it and is inside the 154 |
| E3 | HELD | `~/scratch/12q-u13-build/stage/` holds `RESTORE.txt` only (`find -maxdepth 2`). `~/scratch/12r-u13/run/ra_12q/review.db` has sha256 `ecad5e658fa90e950676bbf04ef9838062beff5bda38c56d3bfa4fca81e6b75f`, equal to `12r_close/retention_close.txt:23` |
| E4 | HELD | `ENGINE_REFACTOR_PLAN.md:1691`: `~/scratch/e_exec_probe/` · R368 (12a-E-EXEC-A) · "**RETAINED to the freshman freeze**" |
| E5 | HELD | no retention-ledger row names `10b-close`. It is cited once, at `ENGINE_REFACTOR_PLAN.md:3244` ("the fifteen present at the 12l open (the fourteen the 12k close names and `10b-close/`)"). Its patch's pre-image blobs are `0e0bd8f^`'s (§S6) |
| E6 | HELD | `12ah_close/retention_close.txt`: "# total: 23 MATCH / 0 MISMATCH" and one "12v probe scratch (R700(d))" entry |
| E7 | HELD | all four folders exist, and evidence-engine cites none of them (tracked files at HEAD, `primer.md`, `CLAUDE.md`). They are cited only by surgical-cv `primer.md:104,106,108`, surgical-cv `CLAUDE.md:108,182`, and claude-config `primer.md:1403,1405,1406` |
| E8 | HELD as to kind; the count differs | The lines are porcelain status lines (paths), not diff lines. The maximum, excluding `!!` ignored lines, is 11 (`12ae-u24b/invclone`: 9 modified + 2 untracked), which matches "1 to 11". **The count differs:** 29 non-clean clones of 53, and 29 of the handover's 51 (both 12ah clones are clean), against the handover's 32. Cause: cannot tell from here |

## S1 — top level (`s1_toplevel.tsv`; computed by `os.walk`/`lstat`, the G1 script)

- 183 top-level entries; 14,208,953,624 B apparent; `du -sb` (which counts hardlinked inodes once) 11,305,285,004 B.
- **In scope: 161 entries** (157 `12*`, plus `retained`, `wr5-b`, `10b-close`, `e_exec_probe`): **374,325 files, 14,122,841,276 B**.
- Out of scope: 22 entries, 86,112,348 B, by name and size only (brand-theme-*, cm*, council-A, dgx-infra-OHC-1, helloai-A, inv-a/b, lists-A,
  llm-council-LCH-1, ollama-OLR-1, opreports-A, cm-read-1 and three loose cm106b files).
- **Reconciliation with the handover (computed):** outside the repositories, the 154 entries of 2026-11-16 hold 43,554 files and 5,614,307,970 B
  when the two pytest-fixture repos are counted as repositories. Counted as ordinary files they add 64 files and 54,624 B (2 × 32 files,
  2 × 27,312 B), giving **43,618 files and 5,614,362,594 B, the handover's figures exactly**. The handover's "51 repositories" are the
  evidence-engine clones; the two fixture repos were inside its file count.

## S2 — who depends on what (`s2_citations.tsv`; the label is CC's reading, not a computed value)

Sources: `primer.md`, `CLAUDE.md` (0 hits), `STANDING_RULES.md`, the plan's retention ledger (lines 1614–1751) and the 12ai
expected-values block (5846–5938), and `12ah_close/retention_close.txt` (its relative paths resolved under `~/scratch/retained/`, as
`session_open_check.py:retention` does). Code under `docs/session-reports/tools`, `scripts`, `engine`, `tests` and `analysis`: the only
reader of scratch is `docs/session-reports/tools/session_open_check.py`, which takes every path from a config file. Every other "scratch"
hit is a test docstring about `tmp_path`.

119 citations of 93 distinct paths. **Distinct paths by label: LEDGER 31, CONSUMER 32, HISTORY 30** (citations 46 / 38 / 35).
CONSUMER takes precedence where a ledger row also governs; the row is still given in the table.

**CONSUMER, and its reader.** Every one is read by `session_open_check.py`, through the config `~/scratch/12ah-open/expected_12ah.json`.
CC's reading is that 12ai-OPEN's startup verify does the same, since that is the pattern of `12ag-open/expected_12ag.json`:
- section `retention`: root `~/scratch/retained` (8 throwaways), plus the absolute paths in `retention_close.txt`:
  `~/scratch/12r-u13/run/ra_12q/review.db`, `~/scratch/12l-u06/throwaway/u06_floor_20261012/review.db`, and the eight
  `~/scratch/12u-u16a/run{1,2}/…/u16a/review.db`;
- section `probe_scratch`: `~/scratch/12v-u37r/requests`, `~/scratch/12v-u37r/out/responses`, `~/scratch/12v-u37r2/out/responses`;
- section `stage`: `~/scratch/12q-u13-build/stage` (expected entries `["RESTORE.txt"]`), also named in the 12ai block at plan:5867.
- **Not in the brief's sources, and a consumer by CC's reading:** `~/scratch/12ah-open/expected_12ah.json` itself, which is **uncommitted**
  (`git ls-files | grep expected_12a` → 0).
- The 12ai block's recount line ("run in a scratch clone, R673(b)") makes a new clone and reads no existing one.

**Cited paths that do not exist (10):**
- seven `Retired 2026-09-29` by ledger rows under R238: `rehearsal-10b/source_copy.db`, `rehearsal-10b/fresh/fresh10b/review.db`,
  `rehearsal-10b-2/source_copy.db`, `rehearsal-10b-2/fresh/fresh10b2/review.db`, `p3ii-fresh/fresh_p3ii/review.db` and
  `backup-review-10b/`; the rows are at plan:1667, 1669, 1670, 1671 and 1673;
- two `Retired 2026-09-30` by R299: `retained/bridge_smoke_11b/review.db.bak-smoke11b-source-20260930-092559` and
  `retained/adj_smoke_11b/review.db.bak-smoke11c-source-20260930-114627`;
- `~/scratch/ee_smoke_11c_input/…` (plan:1687), cited as "the pre-relocation directory" that the manifest's stored input paths name. HISTORY;
- `~/scratch/12q-u13-build/stage/ra_12q/` (`primer.md:851`). HISTORY (§S5).

## S3 — clones (`s3_clones.tsv`, `s3_modified_files.tsv`, `s3_untracked_ignored.tsv`, `s3_dirty_sentences.tsv`)

- **55 repositories:** 53 clones of `~/projects/evidence-engine` (the handover's 51, plus `12ah-open/recount` and `12ah-close/clone`) and
  2 pytest-fixture repos (`12aa-u20c/bt_sweep/test_t6_git_state_reads_the_st0/repo`, `12w-u17b/census/basetemp/test_t6_git_state_reads_the_st0/repo`).
  The fixture repos have no origin, branch `master`, and one commit of their own each, which no origin could hold.
- **All 53 clones:** HEAD reachable from the main repository's `origin/main` (`merge-base --is-ancestor`); 0 commits of their own outside
  `origin/main` (every commit in `rev-list HEAD --branches` checked); **0 unpushed; 0 stashes** (the fixture repos: 0 stashes).
- **Clean 24, non-clean 29** (of the 53). 17 have a non-empty `git diff HEAD` (all 17 captured, G4); 12 hold untracked files only.
- Untracked and ignored files outside environments and caches: **28 files; 27 are byte-identical to a file in the tree at HEAD**. One is not:
  `12ae-close/pre_clone/…/12ae_close/recount_12ae_close.json` (1,962 B, `7f5dcb69…3163`), copied to `untracked/12ae-close__pre_clone/` and verified.
  Environment and cache: 325 files, 4,838,250 B (`__pycache__`, plus six `.venv` symlinks to the main repository's `.venv`; the `.venv/` pattern in
  `.gitignore` does not match a symlink, which is why they show as untracked).
- **Clones whose uncommitted content matches no committed version (5):** `12ab-u21p/clone`, `12ae-close/pre_clone`, `12ae-u24b/invclone`,
  `12af-u29a/clone`, `12af-u30a/clone`. In each of the other 24 non-clean clones, every changed or untracked file is byte-identical
  to a committed blob in the main repository.

| clone | what the uncommitted change is (one sentence each; the full table is `s3_dirty_sentences.tsv`) |
|---|---|
| 12ab-close/clone | The 12ab closeout draft of the plan (+144), byte-identical to 228ee31's, and its recount pair, committed. |
| 12ab-u21b/parent | One untracked test, byte-identical to the committed `tests/test_u21_no_overlap.py`. |
| **12ab-u21p/clone** | A rehearsal of the R757 transcription (Block 155, row INT-ab12-1; plan +62) and a regenerated 12w recount JSON; neither is byte-identical to anything committed, though both the Block and the row are at HEAD. |
| 12ac-close/clone | The 12ac closeout draft of the plan (+125), byte-identical to 6095e6c's, and its recount pair, committed. |
| 12ad-close/recount/clone · 12ae-close/recount · 12af-close/clone · 12s-close/clone | Each holds only its session's untracked recount pair, committed. |
| **12ae-close/pre_clone** | A 12ae-CLOSE item 0 draft of the plan (Block 193 with the B9 and C28 notes, +144/−2), matching no committed version, and a recount JSON that differs from the committed one in `expected_holds` (false) and `done` (30). |
| **12ae-u24b/invclone** | The U24 preflight-from-spec work in progress. Six of nine files match 94ab7a8; `entry_points.json/.md` and `ollama_preflight.py` (adding `--review`/`--spec`) match no committed version. |
| **12af-u29a/clone · 12af-u30a/clone** | `recount_12ae_close.json` regenerated against a later plan (rows 287; done 32 and 33), generated output matching no committed version. |
| 12ag-n46a/regen_clone | A regeneration of the plan and the three rules files plus r804/r805, all byte-identical to dcf623e's. |
| 12r-open/pc_clone | Staged plan and `lane_line_12q.txt`, byte-identical to c748129's. |
| 12s-close/clone2 · 12s-open/clone · 12s-u14b/clone · 12s-u15a/clone | Staged plan drafts, byte-identical to 54499d5 / 1ddff37 / 4938501 / 01751e7 respectively. |
| 12s-u14b/oldclone | Three test files, byte-identical to 7074305's. |
| 12t-u15b/prefix · 12u-u16b/checkout | One untracked test each, byte-identical to a committed file (the second to `12u_U16B/test_audit_stage_completion_phase1.py`). |
| 12u-close/clone · 12w-close/clone · 12w-u17b/clone | Plan drafts, byte-identical to 0ceec48 / f7a16f3 / c5ab505; 12w-close's untracked files are committed too. |
| 12v-close · 12v-open · 12v-u38a · 12v-u38b · 12v-u37b (clone) | A `.venv` symlink only; 12v-u37b also has its committed recount pair. |

## S4 — files outside clones (`s4_entries.tsv`; per-file lists beside it)

Method: each file's git blob id (`sha1("blob <n>\0" + bytes)`) checked against `git ls-tree -r HEAD` of the main repository, plus sha256.
Proof in G3. Symlinks were not followed. A `review.db` and its sidecars were hashed as bytes.

| | files | bytes |
|---|---|---|
| in clones (S3) | 330,343 | 8,044,686,779 |
| environments and caches | 560 | 12,367,996 |
| records with an identical committed copy | 12,960 | 384,080,261 |
| records without one | **30,462** | **5,681,706,240** |
| total = S1 in-scope | 374,325 | 14,122,841,276 |

1,020 of the "committed copy" records are 0-byte files, which match any empty committed file (G3 note).

Records without a committed copy, by kind (`s4_unique_by_kind.tsv`, kind by filename, CC's reading): SQLite `*.db`, sidecar and `.bak`
7,218 files / 5,166,967,080 B; text, md, tsv and csv 2,423 / 409,517,123; pytest-basetemp content 16,492 / 39,682,411; paper texts 2,103 / 16,590,031;
json 634 / 13,960,002; logs 98 / 17,970,207; scripts 775 / 7,490,738; patches 11 / 206,907; other 708 / 9,321,741.
There are 6,467 `review.db` files (5,086,916,608 B; 6,389 distinct sha256).

Entries holding the most bytes without a committed copy: `12w-u17b` 1,481,567,975 · `12aa-u20c` 1,478,074,207 · `12w-v1a` 875,302,840 ·
`retained` 417,584,305 · `12z-u19b` 224,778,644 · `12aa-u20b` 138,724,310 · `12l-u06` 135,240,362 · `12u-u16c` 108,519,877. In each,
SQLite files are more than 95% of the bytes.

## S5 — the primer citation

`primer.md` 798–804 as they stand (the file was rewritten 2026-11-19 06:41:24 UTC by the 12ah wrap):

```
> 906.50 s** (third beside a concurrent surgical-cv encode); no band until four gates.
>
> **Retained:** the post-run `ra_12q` at `~/scratch/12r-u13/run/ra_12q/` (review.db `ecad5e65…b75f`, fingerprint
> `50e97bb6…82cd`), the ninth retained throwaway; `~/scratch/12q-u13-build/stage/` holds `RESTORE.txt` only.
> `deepseek-r1:32b` was resident at the close (the 07:00 health job unloads it).
>
> **Things a later session must know:** (1) **Recount chain form:** a closeout recount runs its baseline script on
```

The handover's line is now **`primer.md:851`**: "> **staged out of `data/`** at `~/scratch/12q-u13-build/stage/ra_12q/` (review.db sha256 `9461bf26…7705`,".
In `primer-history/2026-11-14/primer.md`, the primer in force on 2026-11-16, that sentence is line 801 and the "holds `RESTORE.txt` only" sentence is line 751.
The wrap added 50 lines above both.

Listing (`find -maxdepth 2`): `12q-u13-build/` holds 25 files at depth 1 and 8 at depth 2, in `input/` (`entry.json`, `id_map.json`, `texts/`), `ra_12g_copy/` (a `review.db` with `-shm`/`-wal`, `spec.yaml`, `extraction_codebook.yaml`, `parsed_text/`) and `stage/`; `stage/` holds `RESTORE.txt` only.

The plan on the stage directory: plan:3763, "moved out of `data/` at 00:47 UTC on 2026-10-19 to `~/scratch/12r-u13/run/ra_12q/` after the run: `review.db`
file sha256 `ecad5e65…75f`, … the ninth retained throwaway (R667(d), `12r_close/retention_close.txt`)"; plan:5867, "`~/scratch/12q-u13-build/stage/`
holds `RESTORE.txt` only."; `12r_close/retention_close.txt:25`, "~/scratch/12q-u13-build/stage/ now holds RESTORE.txt only".

**Conclusion: the citation is stale, and the evidence is at `~/scratch/12r-u13/run/ra_12q/`, its hash equal to `12r_close/retention_close.txt:23`.**
Line 851 is the dated 12q record of the pre-run staging. The current statement, already in the primer at line 801, agrees with the plan and the disk.

## S6 — unattributed

**(a) `~/scratch/10b-close`: claimed for evidence-engine (session 10b-CLOSE, 2026-09-29).** 10 files, 173,913 B, each hashed in `s6_files.tsv`.
- Its only citation is plan:3244, quoted under E5.
- `10b-close-uncommitted.patch` (51,946 B, `87ff924e…0499`) changes `CLAUDE.md` and `ENGINE_REFACTOR_PLAN.md`. Its pre-image blobs `496349e` and `c80ac95`
  are exactly `0e0bd8f^`'s, where 0e0bd8f is "docs(plan): 10b closeout — … retention ledger, R114 rewrite (R237), closure paragraph". Its post-image blobs
  are absent from the repository, and `git apply --check --reverse` against HEAD fails.
- 2 of 3 added CLAUDE.md lines and 107 of 116 added plan lines appear verbatim in 0e0bd8f (97 at HEAD). The lines that differ are the R238/R241
  text before amendment (the throwaway still under `data/`, "13 database files" for the committed 14).
- Reading: the patch is the pre-amendment draft of the 10b closeout. It is superseded by 0e0bd8f, and none of its content is otherwise unrecorded.

**`~/scratch/e_exec_probe`: claimed** under ledger row plan:1691 (R368, "RETAINED to the freshman freeze"; the evidence behind `d4d64c2`, which exists).
It holds 6 files, 9,275 B: `probe.py`, `run.sh`, `V1.log`–`V4.log`, each hashed in `s6_files.tsv`. It is also cited at `primer.md:1127,1175,1223`.

**(b) The four home folders:** each exists; none is cited by evidence-engine (E7); no file inside was opened; no listing was taken.

## S7 — proposed classes (CC's inference; the ruling is the PI's; `s7_classes.tsv`, `s7_class_totals.tsv`)

Protection order used: CONSUMER > RETAINED-BY-LEDGER > UNIQUE-RECORDS > WORKING-COPY-DIRTY > CITED-HISTORY > WORKING-COPY-CLEAN > UNKNOWN.
Every fitting class is listed per entry.

| class | entries | bytes |
|---|---|---|
| CONSUMER | 8 (`retained`, `12v-u37r`, `12v-u37r2`, `12q-u13-build`, `12r-u13`, `12l-u06`, `12u-u16a`, `12ah-open`) | 855,618,033 |
| RETAINED-BY-LEDGER | 14 (`e_exec_probe`, `12f`, `12g-ra`, `12g-rb`, `12g-meas`, `12h-rd`, `12i-scope`, `12i-w1`, `12j-w2`, `12o-open`, `12o-c6a`, `12o-c7`, `12o-c8`, `12o-close`) | 40,415,429 |
| UNIQUE-RECORDS | 137 | 13,226,343,435 |
| WORKING-COPY-DIRTY · CITED-HISTORY · WORKING-COPY-CLEAN | 0 · 0 · 0 | 0 |
| UNKNOWN | 2 (`12ah-olra`, `12m-U12`): every record committed, no clone, uncited, so no listed class fits | 464,379 |

UNIQUE-RECORDS takes nearly everything, because almost every working directory holds at least one log, output or database with no
committed copy. It does not separate on its own. The bytes are 91% SQLite (S4), and `s7_classes.tsv` gives each entry's unique bytes and files
so the PI can sort by them. `wr5-b` (414,464 B; cited at `primer.md:110`, "`~/scratch/wr5-b/` left as is") is UNIQUE-RECORDS|CITED-HISTORY,
with 42 files and 69,939 B without a committed copy. No deletion or move is proposed.

## Gates

- **G1 NO WRITE: PASS.** `g1_before.tsv` (S0) and `g1_after.tsv` (after S7, 14:23:31 UTC): 183 entries with files, bytes and newest mtime, and 55 `.git/index`
  mtimes in ns. `diff` is empty (`g1_diff.txt`, 0 lines). The main repository's `status --porcelain --ignored` is identical before and after; HEAD is unchanged.
- **G2 TOTALS RECONCILE: PASS.** S3 clone files and bytes plus S4's three classes equal S1's in-scope 374,325 files / 14,122,841,276 B, per entry and in total
  (`s4_entries.tsv` columns 2–3 against `s1_toplevel.tsv`; `s3_clone_sizes.tsv` sums to 330,343 / 8,044,686,779).
- **G3 THE MATCH IS PROVEN: PASS** (`g3_match_proof.txt`).
  - Positive: `~/scratch/12f/dbchecks.py` (2,141 B, `909e3ed2…4ff5`) matches `docs/session-reports/session-12/12f_repro/dbchecks.py`, whose `git show HEAD:` sha256 is the same.
  - Negative: `g3_random.bin` (4,096 random bytes) gives NO MATCH.
  - A first positive picked by the script, `12f/pins_ad.err`, was 0 bytes and matched `analysis/__init__.py`. It proves nothing and is kept in the file as a note.
- **G4 EVERY DIRTY CLONE IS CAPTURED: PASS** (`g4_numstat_check.tsv`). There are 17 patch files for 17 clones with a non-empty diff. For each, `git apply --numstat`
  of the patch equals the table's numstat and a fresh `git diff HEAD --numstat` of the clone (17 ok / 0 differ). The one copied untracked file's
  sha256 equals its source's (`7f5dcb69…3163`).
- **G5 OWN FILES LISTED:** `SHA256SUMS`, written last.

## Findings not expected

1. **The handover's `primer.md:801` is now line 851** (the primer was rewritten 2026-11-19 06:41 UTC, after CLN-1). Line 801 now carries the correct current statement (§S5).
2. **Twelve identical 134,361,088 B `review.db` files** (sha256 `108a5f6d3acce29a3806e8ac5aed6cd1830fe47e1739b24b1a73e1b6fe3fbc1f`, six each) sit in pytest
   basetemp sweeps: `12aa-u20c/bt_sweep/test_013_*`, `test_migration_*` and `test_existing_rows_*`, and `12w-u17b/census/basetemp/…`. The size equals live's
   file size after 018 (plan:1802). They are in no retention file and were not opened. Also unlisted: three copies of the retained `ra_12g`
   (`12p-u13a/ra_copy/`, `12q-u13-build/ra_12g_copy/`, `12r-u13-pa/ra_12g_copy/`), the second with `-shm`/`-wal`.
3. **Clone objects are hardlinked to the main repository's `.git/objects`** (pack `f465b42b…` has link count 30, the same inode as the main repo's). Of the clones'
   8,044,686,779 B apparent, 5,139,752,073 B are distinct inodes, and 3,110,982,623 B sit in multiply-linked files. Deleting clones frees less than their byte count.
   A main-repository write also surfaces as a scratch mtime: the shared newest mtime `2026-11-19T06:31:34Z` on many entries is that pack being freshened at the 12ah close.
4. **Many clones' `.git` directory mtimes read 2026-11-16 05:29:18–19 UTC**, consistent with CLN-1 creating and removing a file in each `.git`, such as an index lock
   from a `git status` run with optional locks on (CC's inference). It was not this lane's.
5. **12ai-OPEN's startup config lives only in scratch** (`12ah-open/expected_12ah.json`, uncommitted), and none of the S2 sources names it.
6. **Retention outside the latest file and the ledger:** `12r_close/retention_close.txt:25` names "kept with it" `12q-u13-build/ra_12q_fingerprint_built.json`,
   `stage_before.txt` and `~/scratch/12r-u13-probe/out/`. The retention ledger has no row after the 12o closeout (2026-10-15). `ra_12q`, U06's copy,
   12u-U16A's eight and the 12v probe scratch are governed by closeout retention files and rulings (R667(d), R670(g), R675(b), R700(d)), not by ledger rows.
7. **The non-clean count is 29 against the handover's 32** (E8).
8. **0-byte files match trivially** (1,020 of the committed-copy records). A future identical-copy check should not count a 0-byte match as evidence.
