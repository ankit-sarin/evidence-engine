# VERIFY-EXIT-01 (ELICIT-01) — Exit-state verification

**Task:** VERIFY-EXIT-01, scoped to the ELICIT-01 exit session. **Date:** 2026-08-31.
**Type:** operational, **read-only**. No commits, pushes, fetches, file writes, DB writes,
tmux sessions or blocking flock acquisitions were made during verification; the flock was
probed non-blocking and released, and SQLite was opened `immutable=1` only.

ELICIT-01 completed overnight in tmux and its results were available to the architect only as a
completion summary. Standing rule: exit-session deliverables are verified at next-session start
before any downstream work. The STATED evidence-mechanism decision depends on the report's §6
side-by-sides, so this task also returned that section verbatim (delivered in-session; not
reproduced here — see `ELICIT-01_report.md` §5.4 and §6, which are the authoritative text).

**Outcome: every claim in the ELICIT-01 completion summary survived contact with disk. Zero
deltas, zero mismatches, no unexpected findings.**

---

## 1. Gates

| # | Check | Result |
|---|---|---|
| **P1** | Repo present, tree clean | **PASS** |
| **C1** | local HEAD == remote == `6371e87…`; both commits present | **PASS** |
| **C2** | Report file tracked and committed in `6371e87` | **PASS** (I1 confirmed) |
| **C3** | Invariants: `review.db`, `parsed_text`, tmux, flock, Ollama | **PASS** |
| **C4** | Independent recount from the store | **PASS** (I2 confirmed) — **zero deltas** |
| **C5** | Offline test gate | **PASS** |
| **C6** | §6 and the uncited-assertion section returned verbatim | **PASS** |

Both INFERRED items held: **I1** — the report is at the expected path under
`docs/session-reports/`; **I2** — the store carries machine-readable per-call rows sufficient to
recount independently.

## 2. C1 — git

| item | expected | observed |
|---|---|---|
| local HEAD | `6371e87df72d…` | `6371e87df72d5985c0ae4114d4457f5e6cd95740` |
| `ls-remote origin refs/heads/main` | same | `6371e87df72d5985c0ae4114d4457f5e6cd95740` |
| ahead / behind | 0 / 0 | `0 0` |
| working tree | clean | `git status --porcelain` empty |

| commit | subject | files |
|---|---|---|
| `6371e87` (01:29:34 +0000) | `docs: ELICIT-01 report -- COPY 36.1% anchored, INDEX 100% index-valid` | 1 file, **336 insertions** |
| `d99a2b2` (01:29:19 +0000) | `feat(eval): COPY vs INDEX quote elicitation harness for STATED fields (ELICIT-01)` | 7 files, **1,148 insertions** |

`d99a2b2` contents: `analysis/eval/elicit01/{__init__,analyze,manifest,prompts,runner,units}.py`
and `tests/test_elicit01.py`.

## 3. C2 — report

`docs/session-reports/ELICIT-01_report.md` — tracked, sole file of `6371e87`,
**336 lines / 18,216 bytes**, worktree byte-identical to HEAD.

## 4. C3 — invariants

| item | expected | observed |
|---|---|---|
| `review.db` mtime | 2026-07-27 19:47:48 | **2026-07-27 19:47:48** |
| `parsed_text` | 2026-03-14 | newest file **2026-03-14 03:34**; dir 03:34:37 |
| tmux | no run session | `no server running on /tmp/tmux-1000/default` |
| experiment flock | free | **FREE** (non-blocking probe, immediately released) |
| Ollama | 0.21.0 | `ollama version is 0.21.0`; API `{"version":"0.21.0"}` |

## 5. C4 — independent recount

Recomputed from `eval/elicit01/elicit01.jsonl` and `unit_maps.json` using the committed parser
and the unmodified `classify_span` — deliberately **not** read back from `analysis_summary.json`,
which is the artifact under verification.

| count | expected | observed | Δ |
|---|---|---|---|
| calls total / ok / failed | 76 / 76 / 0 | **76 / 76 / 0** | **0** |
| truncated (tripwire) | 0 | **0** | **0** |
| by condition | COPY 38, INDEX 38 | **COPY 38, INDEX 38** | **0** |
| COPY ladder spans | 341 | **341** | **0** |
| COPY ANCHORED | 36.1% | **123 = 36.1%** | **0** |
| INDEX indices / valid | 505 / 505 | **505 / 505** | **0** |
| INDEX out-of-range / malformed | 0 / 0 | **0 / 0** | **0** |
| field coverage | COPY 324/342, INDEX 342/342 | **COPY 324, INDEX 342** | **0** |
| unparseable COPY containers | exactly p445, p522 | **p445, p522** | **0** |

Full recounted ladder: ANCHORED 123 (36.1%), DRIFTED 53 (15.5%), ABSENCE_DECLARED 48 (14.1%),
UNTRACEABLE_NO_BASIS 44 (12.9%), STITCHED 43 (12.6%), MISSING_SNIPPET 22 (6.5%),
UNTRACEABLE_PARTIAL 7 (2.1%), ABSENCE_CLAIM 1 (0.3%).

## 6. C5 — tests

| item | expected | observed |
|---|---|---|
| offline gate | 1556 passed / 15 deselected | **1556 passed, 15 deselected** (208.78 s) |
| ELICIT-01 tests | +24 | **24 collected** |

The 1532 → 1556 movement is exactly the 24 ELICIT-01 tests, confirming the summary's arithmetic.

## 7. Observations — neither a discrepancy

- `smoke_summary.json` carries mtime 22:58, after the full run began at 22:58. Expected: the
  analyzer was re-run over the smoke rows when the four reporting additions landed, before the
  full-run analysis. Not evidence of a later write.
- Store mtimes were identical before and after this verification, confirming the read-only
  constraint held.

## 8. Scope

**Out of scope and not done:** the STATED evidence-mechanism decision, ELICIT-02 (judge-based
supportedness scoring), any priming or elicitation design work, corpus repair, and any fix for a
mismatch — none was found. No finding required remediation.
