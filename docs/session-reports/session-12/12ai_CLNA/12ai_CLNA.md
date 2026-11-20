# 12ai-CLNA — the scratch clones, checked before any removal (read-only; the step-2 read-out)

Run 2026-11-20 22:12:52 → 22:14:11 UTC under the 12ai-CLNA brief, at evidence-engine `f4427d5` (item 0), by
`helpers/clna_check.py` (phases baseline, check, after) and `helpers/patch_bytes.py`. Every value is in the tables beside this
file: `clna_check.json` (the totals), `t2b_repos.tsv`, `t2c_missing.tsv`, `t2c_new.tsv`, `t2d_against_inv.tsv`, `t2d_patch_bytes.tsv`,
`t2e_untracked_ignored.tsv`, `t2e_links.tsv`, `t2e_unique.tsv`, `t2f_readers.tsv`, `t2f_hits.tsv`, `t2g_in_use.tsv`, `t2h_sizes.tsv`,
`removal_list.tsv`, `i1.txt`, `i2_before.tsv`, `i2_after.tsv`, `i2_diff.txt`. Nothing was removed, moved or cleaned.

This file was not written before the stop at 2j: the stop left the tables and no prose read-out. It is written at the 12ai-CLNA-R1
resume (R822) and holds this header and the section below; the stop report of 2026-11-20 carries the step-2 results in prose.

## I2 at 2j: false (R822)

`i2_diff.txt`, quoted whole:

```
- ENTRY	12ai-u30b	8621	296898483	1795210846693105457
- ENTRY	12ai-u41b	8531	293889390	1795210842607492700
- INDEX	12ai-u30b/clone	1795209061260464430
- INDEX	12ai-u41b/clone	1795152047054457187
+ ENTRY	12ai-u30b	8621	296898483	1795212793132693582
+ ENTRY	12ai-u41b	8531	293889390	1795212793233805626
+ INDEX	12ai-u30b/clone	1795212793131805356
+ INDEX	12ai-u41b/clone	1795212793232805623
```

The two files that moved, before (2a's baseline) and after (2j), `.git/index` mtime in nanoseconds:
- `~/scratch/12ai-u30b/clone/.git/index`: 1795209061260464430 → 1795212793131805356 (2026-11-20 22:13:13.13 UTC).
- `~/scratch/12ai-u41b/clone/.git/index`: 1795152047054457187 → 1795212793232805623 (2026-11-20 22:13:13.23 UTC).
Their entries' newest mtime moved with them; the entries' file counts and bytes did not (8,621 files / 296,898,483 B and
8,531 files / 293,889,390 B, before and after). Both are clones this session made, both KEEP by the list's rule.

The cause is CC's reading and was not tested: `git diff HEAD --numstat`, run with GIT_OPTIONAL_LOCKS=0 in 2e, appears to have
refreshed and rewritten the index of the two clones whose working files were stat-dirty from the item-0 recounts' copies.

What the same test shows unchanged, counted from `i2_before.tsv` and `i2_after.tsv`: 56 of 58 `.git/index` mtimes — the 53
inventory clones', the inventory's 2 fixture repositories', and `12ai-open/recount`'s — and 165 of 167 in-scope entries' file
counts, bytes and newest mtimes (OUT is excluded from the test; the two that differ are `12ai-u30b` and `12ai-u41b`). At the
resume's R0, by stat alone, all 58 index mtimes equal 2j's after values.

R822(b), the PI's word: "1" — the two index rewrites are accepted, the read-out is committed as it stands with the failed
no-write test recorded in it, and the check is not run again.
