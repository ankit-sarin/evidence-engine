# SESSION-HYGIENE-01 — recovered read-out

**Provenance header.** This file is a verbatim recovery of one Claude Code
assistant message. Nothing below the header has been edited, summarised or
reformatted.

- **Source transcript:** `/home/ankitsarin/claude-session-archive/2885f251-8a7f-442c-84dd-cd1350683f33.jsonl`
- **Session id:** `2885f251-8a7f-442c-84dd-cd1350683f33`
- **Source message line:** L422 (NDJSON record, `type: assistant`)
- **Brief at transcript line:** L352, 2026-09-09T23:02:52.138Z
- **Message timestamp:** 2026-09-09T23:04:59.927Z
- **HEAD stated in read-out:** `HEAD 633e56b` (evidence-engine row of §5 "Porcelain / HEAD")
- **Architect acknowledgement:** L425, 2026-09-09T23:06:42.545Z — "MEASURED: none for this brief beyond HEAD 633e56b, tree clean (SESSION-HYGIENE-01)."
  *(user-authored; architect turn, not CC output)*
- **Trust:** Transcript-derived. Values in this report that trace to a tool call in the source transcript at the stated HEAD may be cited as MEASURED. Classifications, counts arrived at by reading, and judgements are INFERRED until re-measured.
- **Recovered by:** READOUT-ARCHIVE-01, 2026-09-11, recovering session `410e9ecc-67a0-44c6-9523-ffad8f528ca3`.
  Compiled from transcript `/home/ankitsarin/claude-session-archive/2885f251-8a7f-442c-84dd-cd1350683f33.jsonl` by session 410e9ecc… (READOUT-ARCHIVE-01) on 2026-09-11.
  *(files written 2026-09-12 00:49 UTC; the session opened 2026-09-11 23:29 UTC and crossed midnight)*

---

# SESSION-HYGIENE-01 — report

## 1. session-archive output, verbatim

```
archived e0a7d38c (e0a7d38c-a025-416a-933f-0538bbc6e653): transcript=e0a7d38c-a025-416a-933f-0538bbc6e653.jsonl job=none -> /home/ankitsarin/claude-session-archive [verified absent from both scan roots]
EXIT=0
```

**I3 true** — AWAITING was accepted; no refusal. Transcript preserved intact at `~/claude-session-archive/e0a7d38c-a025-416a-933f-0538bbc6e653.jsonl`, 2,469,112 B, mtime unchanged at 2026-09-09 17:03:20.795 UTC (moved, not copied or rewritten). Archive now holds 58 transcripts.

## 2. Audit one-liner

`-- Claude Code sessions (5 transcripts, 0 job-only, 1 live) --` — `LIVE 2885f251` is now the **only** evidence-engine session in any state. The remaining four are other lanes (surgical-cv ×2 AWAITING, hello-ai-v2 ×2 MID-TURN), untouched. `Orphan process candidates: none`. Verdict moved from `warn — 2 DIRTY_TREE` to **`warn — 1 DIRTY_TREE`**; the one left is surgical-cv, out of scope.

## 3. Commits

| # | hash | subject |
|---|---|---|
| 1 | **`73890f1`** | `chore(ledger): commit hook-appended rows 658165d…633e56b` |
| 2 | **`2c66f32`** | `docs(claude-md): bounded-wait rule; pgrep self-match` |

Pushed together: `d303627..2c66f32  main -> main`. Two commits, no more; neither amended.

Verified before committing: `claude-config` has **no** `post-commit` hook of its own (`~/claude-config/.git/hooks/` holds only samples), so `hooks/post-commit-ledger.sh` — which appends to `$HOME/.claude/PROJECT_LEDGER.md` and is installed in project repos like evidence-engine — did not fire on either commit and did not re-dirty the ledger.

## 4. The CLAUDE.md block, quoted in full

Inserted at lines 150–159, immediately after "…if it passes there, it is measuring nothing.**" and before "**RUN THE REMEDIATION YOU WRITE**", inside `## Working Practices`. **Ten lines.** Nothing else in the file was moved or reworded (`git diff --stat`: `CLAUDE.md | 10 ++++++++++`, 10 insertions, 0 deletions; 371 → 381 lines).

```markdown
- **NEVER WAIT ON `pgrep -f <pattern>` — the pattern sits in the waiting shell's OWN argv.** The
  Bash tool runs `/bin/bash -c '<the whole command>'`, so `pgrep -f` matches the poller itself,
  returns 0 forever, and the loop never exits; when its session ends it is reparented to PID 1 and
  wakes every few seconds indefinitely. **Seven loops, two sessions, two repos have already done
  this** — evidence-engine `e0a7d38c` (three; two ran wedged ~6 h) and surgical-cv 2026-07-08
  (four, hit at `/exit`) — and the idiom is written in **no file on this box**, so it is
  re-improvised every time. **Wait on the job's own ARTIFACT** — `until [ -s <output> ]`, a PID
  file, or its flock — **never on a process-name search.** **Every wait carries a maximum duration
  and reports timeout as a DISTINCT outcome; unbounded polling is a defect.** Background waits run
  as CC background tasks whose completion is observed — a wait that outlives its session is orphaned.
```

**I1 true** — `## Working Practices (surfaced by TASK M8A-3, extended by M8A-4)` spans lines 64–172 and the anchor bullet was at 143–149. No restructuring was needed.

## 5. Porcelain / HEAD

```
claude-config     porcelain: (empty)   HEAD 2c66f32   origin/main 2c66f32   ahead 0 behind 0
evidence-engine   porcelain: (empty)   HEAD 633e56b   (untouched — no commit, no edit)
dgx-infra         porcelain: (empty)   HEAD 64a0e24   (read only)
```

**I2 true** — `git pull --ff-only` reported *Already up to date* before either commit.

## 6. dgx-infra defects — recorded, not fixed

**(a) The session state model has no `continued-in` value.** `session_audit` renders LIVE / AWAITING / MID-TURN. A session whose final transcript record is `{"type": "continued-in", "continuedInSessionId": …}` has handed its work to a successor and owes nobody a reply, but it reads **AWAITING** — indistinguishable from a session where Claude spoke and the operator never returned. e0a7d38c sat that way for 6 h and was reported to you as needing disposition when it needed none. A fourth state, or a suppression when the last record is `continued-in`, would remove a false positive that recurs on every continued session.

**(b) `session_audit/audit.py:900-913` — the logind-scope exclusion short-circuits the `origin-session=` attribution at 914-918.** The loop `continue`s when `_SCOPE_RE`'s match is in `login_sessions`, so a wedged process from a dead Claude session inside a **still-open** login is skipped before the code that would have named its origin session ever runs. The attribution regex `/tmp/claude-\d+/[^/\s]+/([0-9a-f-]{36})` matched both pollers' cmdlines exactly; it never executed. The predicate is behaving as its own docstring intends ("a process in a session scope that logind still knows about belongs to that login, however old it is" — written after an earlier version fired six false alarms), so this is a scope question, not a bug to patch blind: the auditor cannot currently distinguish *my login's process* from *a dead session's process inside my login*, and the attribution it already computes is precisely the signal that could.

## One correction to ORPHAN-POLL-01

I reported that **19de242b has no transcript on disk**. It is not on disk *in the scan roots* — but it is in the archive: `~/claude-session-archive/19de242b-004e-48f8-956b-4209717ba1f2.jsonl`, 3,355,582 B, mtime 2026-09-09 20:35:29 UTC. It was archived normally, as I9 assumed last brief, and the FONT-AUDIT-02 record is preserved. My step-2 idiom search therefore did not cover it; the seven-loop count stands on `e0a7d38c` (3) and surgical-cv (4) and may undercount.

Stopping here. GENERALIZE-READOUT-01 not started; `primer.md` left for `/wrap`.
