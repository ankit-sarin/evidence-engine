# ORPHAN-POLL-01 — recovered read-out

**Provenance header.** This file is a verbatim recovery of one Claude Code
assistant message. Nothing below the header has been edited, summarised or
reformatted.

- **Source transcript:** `/home/ankitsarin/claude-session-archive/2885f251-8a7f-442c-84dd-cd1350683f33.jsonl`
- **Session id:** `2885f251-8a7f-442c-84dd-cd1350683f33`
- **Source message line:** L342 (NDJSON record, `type: assistant`)
- **Brief at transcript line:** L193, 2026-09-09T22:49:25.018Z
- **Message timestamp:** 2026-09-09T22:55:23.590Z
- **HEAD stated in read-out:** `HEAD 633e56b` (evidence-engine row of the read-out's porcelain/HEAD block)
- **Architect acknowledgement:** L352, 2026-09-09T23:02:52.138Z — "TASK [evidence-engine SESSION-HYGIENE-01] — close out ORPHAN-POLL-01 findings"
  *(user-authored; architect turn, not CC output)*
- **Trust:** Transcript-derived. Values in this report that trace to a tool call in the source transcript at the stated HEAD may be cited as MEASURED. Classifications, counts arrived at by reading, and judgements are INFERRED until re-measured.
- **Recovered by:** READOUT-ARCHIVE-01, 2026-09-11, recovering session `410e9ecc-67a0-44c6-9523-ffad8f528ca3`.
  Compiled from transcript `/home/ankitsarin/claude-session-archive/2885f251-8a7f-442c-84dd-cd1350683f33.jsonl` by session 410e9ecc… (READOUT-ARCHIVE-01) on 2026-09-11.
  *(files written 2026-09-12 00:49 UTC; the session opened 2026-09-11 23:29 UTC and crossed midnight)*

---

# ORPHAN-POLL-01 — report

## Kill ledger

| PID | pre-check (22:49:33 UTC) | kill | post-check (22:50:24 UTC) |
|---|---|---|---|
| **3218188** | PPID 1 · state `Ss` · etime 06:09:24 · started Wed 2026-09-09 16:40:08 · cwd `~/projects/evidence-engine` · exe `/usr/bin/bash` · pgid=sid=3218188 · child `[3492201 sleep 5]` · fd0 `/dev/null`, fd1/2 → `tasks/beajagjos.output` (0 B) · argv unchanged, contains `until ! pgrep -f e3_run.py >/dev/null; do sleep 5; done` | `kill 3218188` → exit 0 | **GONE** |
| **3224033** | PPID 1 · state `Ss` · etime 05:55:27 · started Wed 2026-09-09 16:54:05 · cwd `~/projects/evidence-engine` · exe `/usr/bin/bash` · pgid=sid=3224033 · child `[3492203 sleep 5]` · fd0 `/dev/null`, fd1/2 → `tasks/b6ic6rco3.output` (0 B) · argv unchanged, contains `until ! pgrep -f "pytest tests/" >/dev/null 2>&1; do sleep 5; done` | `kill 3224033` → exit 0 | **GONE** |

Pre-kill pattern check: `pgrep -a -f 'e3_run.py'` and `pgrep -a -f 'pytest tests/'` each returned **only the poller itself plus my own probe shell**; `ps -eo pid,comm,args | awk '$2 ~ /python/'` showed no `e3_run.py` and no pytest — only the four long-lived Gradio/app services. Exactly two PPID-1 user bash processes existed; after the kills, **zero**. Both output files remain **0 B at their original mtimes** — nothing wrote on exit.

**I2 — substance holds, wording false.** Each poller *did* have a child: a transient `sleep 5` (etime 00:00, no grandchildren) — unavoidable in a sleep loop, respawned every 5 s, not a persistent child. Traps: `SigCgt=0x10000` (SIGCHLD only), `SigIgn=0x4` (SIGQUIT) — **byte-identical to a plain bash's defaults**, no user trap; and the only post-loop command was a `tail`, never reached. Confirmed by the unchanged 0-B output files.

---

## 🔴 I3 is FALSE — the idiom is improvised, not templated

| # | location | line | classification |
|---|---|---|---|
| 1 | `~/claude-config` (all files, incl. `CLAUDE.md`, `primer.md`, `skills/`, `commands/`, `hooks/`, `agents/`) | — | **zero hits for `pgrep`**, let alone the idiom |
| 2 | `~/projects/dgx-infra` | — | **zero hits for `pgrep`** |
| 3 | evidence-engine tracked tree | `docs/session-reports/VERIFY-EXIT-01_font-audit_report.md:272-286` | **my own report from 3 h ago** — describes the bug; not a template |
| 4 | evidence-engine working tree, `.claude/`, `scripts/`, `docs/` | — | no other hit |
| 5 | `~/.claude/shell-snapshots/snapshot-bash-1788993179646-cls4wp.sh:140` | `command pgrep … \| command grep -qx "${CLAUDE_PID}"` | **Claude Code's own `pkill` wrapper** — harness code, and it is a *guard against this exact class*: it refuses when the pattern matches the CLI's own PID. Present in 1 of the snapshots (newest CC). Not a template for waiting. |
| 6 | `~/.claude/history.jsonl:1503` | pasted CC exit warning, **2026-07-08 19:30:04 UTC**, project `surgical-cv`, session `f65b7538` | **four** `while pgrep -f "…"` shells listed as "Background work is running" — patterns `oob_classify.py.*reverify`, `oob-b9-determinism` ×2, and **`"pytest tests/"`**. Operator's prompt: *"When I try to exit I get … Evaluate"*. Prior occurrence, different repo, two months earlier. |
| 7 | transcript `e0a7d38c…jsonl:934` | `until ! pgrep -f e3_run.py >/dev/null; do sleep 5; done` | improvised in-turn → became **PID 3218188** |
| 8 | transcript `e0a7d38c…jsonl:959` | `until ! pgrep -f "python.*e3_run.py" >/dev/null 2>&1; do sleep 10; done` | improvised → task `blvw5ic6p`, **manually stopped** at line 976 |
| 9 | transcript `e0a7d38c…jsonl:1006` | `until ! pgrep -f "pytest tests/" >/dev/null 2>&1; do sleep 5; done` | improvised → became **PID 3224033** |
| 10 | transcript `e0a7d38c…jsonl:1073` | `until [ -s <output-file> ]; do sleep 15; done` | **the correct idiom** — file-based, used in the same hour, and it returned correctly at 16:57:05 |
| 11 | transcripts `19de242b`, `291609ef` | — | **no transcript exists on disk for either** (see below) |

**No hit sits in a hook.** The only configured hook is `PostToolUse: Write|Edit → hooks/phi-check.sh`, in `claude-config/hooks/`, which has zero `pgrep` hits. No live hazard there.

**Conclusion for I3:** the idiom is documented nowhere CC reads. It is generated fresh each time — three times in one transcript, four more in a different repo two months earlier. There is no canonical "wait for a background job" pattern on this box; the nearest thing is the artifact-wait at hit 10, which that same session used and which worked. **There is nothing to patch — the fix is a documented correct idiom.**

Incidental: `19de242b` has **no `.jsonl` on disk** (only `session-env/` and `file-history/` residue) and `291609ef` exists only as a directory. The FONT-AUDIT lane's record is its commits and read-outs, not a transcript.

---

## 🔴 I4 is FALSE — the predicate, quoted

`~/projects/dgx-infra/session_audit/audit.py:900-913`:

```python
    live_by_pid = {s.pid: s for s in live}
    for p in procs:
        if p.ppid != 1:
            continue
        if ".service" in p.cgroup:
            continue
        if p.comm in _USER_MANAGER_COMMS:
            continue
        if p.pid in live_by_pid:
            continue        # a live Claude session, accounted for by name
        scope = _SCOPE_RE.search(p.cgroup)
        if scope and scope.group(1) in login_sessions:
            continue        # logind still accounts for this login
```

with `_USER_MANAGER_COMMS = {"systemd", "(sd-pam)", "sd-pam"}` (line 781), `_SCOPE_RE = re.compile(r"/session-(c?\d+)\.scope")` (line 864), and `login_sessions` = every id from `loginctl list-sessions --no-legend`, **`closing` scopes included, deliberately** (docstring, 887-891).

It does **not** exclude PPID-1 bash by construction, and it is keyed on neither process name `claude` nor tmux nor cwd.

**Why the two pollers passed it, in one sentence:** they were `bash`, not in a `.service` cgroup, not a live Claude session, so the only exclusion that could have fired is the last — their `session-cNNN.scope` was still listed by logind, because the login that spawned them is still open (`loginctl` now shows `c2` and `c27` as `closing`, and `closing` counts as accounted-for by design).

Two notes the fix brief will want. First, this is the predicate working **as its own docstring intends** — "a process in a session scope that logind still knows about belongs to that login, however old it is" — a rule written after an earlier version fired six false alarms on a healthy box. It cannot distinguish "my still-open login's process" from "a wedged poller from a dead Claude session inside my still-open login". Second, the machinery to attribute these already exists and never runs: lines 914-918 extract `origin-session=` from `/tmp/claude-<uid>/<project>/<session-id>/` in the cmdline — **exactly** what both pollers carried — but the scope check short-circuits first.

*Evidentiary limit, stated plainly:* I killed the pollers before capturing their `cgroup`, so the scope exclusion is established **by elimination** across the four branches, not by reading their cgroup line.

---

## e0a7d38c — the four answers

**What it was launched for.** `TASK [evidence-engine PARSE-GATE-09] — read-out: font forensics on 586, 699, 719 — can unresolved glyphs be recovered deterministically?` (line 6). It then ran on through the FONT-AUDIT-01 work in the same session. 536 user/assistant turns; active 2026-09-09 ~16:33–17:03 UTC.

**What it ran that spawned the pollers.** A long PyMuPDF font-exposure experiment `/tmp/fa01/e3_run.py` (task `bg8p96a5m`) and two standard-gate runs (tasks `bdz0k8e5e`, `bxe7juu3y`). To wait on each it improvised the three `until ! pgrep` loops at lines 934 / 959 / 1006. At line 1027 its own `pgrep -af "pytest"` output **listed PID 3224033** — the poller — and the session read past it. Only after those did it reach the working `until [ -s <file> ]` form (line 1073), which returned correctly.

**Final CC turn.** Line 1099, 16:58:32 UTC: the **FONT-AUDIT-01 FINAL REPORT** — R1–R4 outcomes, the block-size table (8/12/16 → `exposure_lo` 1.019/0.978/0.974 %, 12 kept), the E3 table over the eleven signature papers plus 455, and the files-changed table for `6bdb1ea` / `6264ae1` / `6b6260a`. Immediately before it, line 1096-1097 is its own close-state verification: `review.db unchanged`, parsed_text sha256 identical, `pip unchanged`, `NRestarts=0`.

**Anything not at HEAD? No.** All three commits are ancestors of `633e56b`. The final report's content is committed in `docs/session-reports/FONT-AUDIT-01_readout.md` — the block-size table at line 80, the E3 table at line 105, plus §5 "🔴 A finding that contradicts the assumption ledger" and §6 "Close-state ledger". Nothing is transcript-only.

**And the state label is wrong.** The file's **last record (line 1103) is `continued-in` → `19de242b-004e-48f8-956b-4209717ba1f2`**, at 2026-09-09T17:03:20.771Z. e0a7d38c was not abandoned — it is the **first half of the arc that 19de242b closed as FONT-AUDIT-02**, and the session I verified last brief. It reads AWAITING only because the auditor's state model has three values and none of them is "continued". **I5 is therefore half false**: its last turn is a CC turn, but nobody is waiting on it. Nothing is pending; it is safe to archive whenever you choose. Not archived, not resumed.

---

## 🔴 I1 is FALSE — R3's precondition is not met

`git -C ~/claude-config diff PROJECT_LEDGER.md` is **9 appended rows, not one**:

```
+| 2026-09-08T23:29:40Z | evidence-engine | main | 658165d | docs(session-reports): PARSE-GATE-08 and -09 read-outs |
+| 2026-09-09T15:48:58Z | evidence-engine | main | 581568c | docs(session-reports): PARSE-GATE-10 corpus font audit |
+| 2026-09-09T16:23:34Z | evidence-engine | main | 6bdb1ea | refactor(parse): move RE_GLYPH to markers.py |
+| 2026-09-09T16:57:25Z | evidence-engine | main | 6264ae1 | feat(parse): font-signature exposure audit + tests |
+| 2026-09-09T16:57:38Z | evidence-engine | main | 6b6260a | docs(session-reports): FONT-AUDIT-01 read-out |
+| 2026-09-09T19:27:30Z | evidence-engine | main | ebdbd68 | feat(parse): FONT_EXPOSURE criterion + tests |
+| 2026-09-09T19:48:30Z | evidence-engine | main | d6972da | feat(db): migration for parse_attempts.font_audit + cascade ledger write + tests |
+| 2026-09-09T19:50:51Z | evidence-engine | main | cd35560 | docs(session-reports): PARSE-GATE-10 addendum and FONT-AUDIT-02 read-out |
+| 2026-09-09T22:43:22Z | evidence-engine | main | 633e56b | docs(session-reports): VERIFY-EXIT-01 font-audit close |
```

Only the **last** row is this session's. The preceding **eight belong to e0a7d38c and 19de242b** and have sat uncommitted since 23:29 on 2026-09-08 — the FONT-AUDIT lane closed and exited without committing the ledger, twice. The hook keeps appending correctly; nothing is lost, but "it commits at `/wrap`" now means *this* session's `/wrap` commits two dead lanes' rows as well. Reported per the ruling's own false branch; **not acted on** — claude-config is read-only in this brief.

---

## G5 — nothing modified in any repo

```
evidence-engine   git status --porcelain : (empty)          HEAD 633e56b
claude-config     git status --porcelain :  M PROJECT_LEDGER.md   HEAD d303627   ← pre-existing, untouched by me
dgx-infra         git status --porcelain : (empty)          HEAD 64a0e24
```

The `claude-config` entry is the same 9-row hook append that was already there at the start of this brief; I read it and wrote nothing.

**One write beyond the two kills, and it is outside every repo:** per **R6**, now that the origin is established, `~/.claude/projects/-home-ankitsarin-projects-evidence-engine/memory/feedback_pgrep_self_match.md` (+ its `MEMORY.md` index line). It names the source — improvised, documented nowhere, three times in `e0a7d38c` and four in surgical-cv on 2026-07-08 — the CC `pkill` guard that covers the sibling case, why the auditor cannot see it, and the artifact-wait form as the replacement. I flag it because G5 says "no file modified anywhere"; the memory tree is under no git repo, so the three porcelains above are unaffected.

---

## Recommendation for the fix brief

Target the **documented idiom**, not a patch — there is no template to fix; and pair it with the auditor's blind spot, since the predicate's own `origin-session=` attribution already identifies these processes but is unreachable behind the logind-scope short-circuit.
