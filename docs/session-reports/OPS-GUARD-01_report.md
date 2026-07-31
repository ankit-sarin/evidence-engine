# OPS-GUARD-01 — Ollama experiment guard (parts C + B)

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-27
Repo commit: **`f182f0c`** · out-of-repo edit: `~/scripts/ollama_health_check.sh` (§3)

**Zero Ollama API calls.** No inference, no `/api/*`, no real `ollama` CLI invocation —
the end-to-end script test uses a PATH stub that records calls instead of making them.
**No crontab, systemd, sudoers, or service changes.**

---

## 1. Lock path and semantics

### 1.1 Path

`$HOME/.ollama_experiment.lock`, overridable via `OLLAMA_EXPERIMENT_LOCK`.

Rejected alternatives, with reasons (documented in the module docstring so the choice
survives me):

| candidate | why not |
|---|---|
| `/run/user/$UID/…` | Correct reboot semantics, but systemd-logind may remove the directory when the user has no login session — and the guard's most important consumer is a **cron job**, which often has neither `XDG_RUNTIME_DIR` set nor a session. |
| `/tmp/…` | World-writable. Another uid could create the file first with permissions we cannot open for write, silently disabling the guard. |
| `$HOME/…` ✅ | Always exists for both interactive and cron contexts; single-owner. The file surviving a reboot is harmless **precisely because nothing reads its existence** — only its flock state. |

### 1.2 Semantics

**flock(2) only. Never an existence check.** The file is created once and left on disk
forever; its presence carries no information. Ownership is the kernel's flock state on an
open file description, so a holder that dies — crash, SIGKILL, power loss — releases the
instant its fd closes. There is no stale-lock condition and no cleanup path to get wrong.

API (`engine/utils/ollama_lock.py`, dependency-free):

| function | meaning |
|---|---|
| `hold_experiment_lock(blocking=True)` | context manager; blocking `LOCK_EX` for experiment use. Re-entrant. |
| `check_experiment_lock()` | non-blocking probe → "held by anyone" bool. Leaves lock state untouched. |
| `self_holds_lock()` | is the holder *this* process? |
| `foreign_lock_held()` | `check_experiment_lock() and not self_holds_lock()` — the predicate destructive actions gate on. |

### 1.3 Self-vs-foreign resolution — why a flag is *required*, not a shortcut

flock binds a lock to the **open file description**, not the process. A second `open()` +
`LOCK_EX|LOCK_NB` from inside the very process that already holds the lock fails with
`BlockingIOError` **exactly as a foreign holder's would**. (This is the opposite of `fcntl`
POSIX record locks, which are per-process and would have silently succeeded.) So a probe
alone genuinely cannot distinguish "someone else has it" from "I have it", and the
module-level depth counter is the correct answer rather than a convenience.

Two consequences, both tested:

- **`restart_ollama()` gates on `foreign_lock_held()`, not `check_experiment_lock()`.**
  `run_extraction()` holds the lock for its own duration (part 5), and the periodic restart
  is *how* a long extraction clears CUDA context fragmentation. Gating on "held at all"
  would disable the restart on every real run — the currently intended behaviour must keep
  working, and it does.
- **`hold_experiment_lock()` is re-entrant.** A nested blocking acquire on a second fd would
  deadlock the process against itself; nested calls bump the counter and reuse the outer fd.

**Fail-safe direction:** an unopenable lock file reads as **HELD** (`check_experiment_lock`
logs a warning and returns `True`). A guard we cannot consult must not license the
destructive branch.

**Residual race, closed:** the health check keeps fd 9 open for its whole run, so once it
acquires it *holds* the lock rather than probe-and-releasing. That eliminates the window
where an experiment starts a millisecond after a probe. The cost is that an experiment
starting mid-health-check blocks until it finishes — bounded by the sum of per-model
timeouts (~74 min worst case with the current model dict; ~2–5 min typical). This is the
deliberate trade: **a blocked experiment is visible and recoverable; a clobbered one is a
lost multi-hour run.** Experiments block by design (`hold_experiment_lock` is blocking).

---

## 2. Test evidence per gate

### Gate 1 — lock module + tests green; full offline suite green

`tests/test_ollama_lock.py` — **13 passed**. Coverage: free-at-start; acquire/release;
file-persists-but-lock-free (existence means nothing); probe idempotence; release on
exception; **re-entrancy**; foreign holder seen as held+foreign; self holder held but not
foreign; **auto-release when the holder is SIGKILLed**; non-blocking acquire raising under
foreign hold without corrupting self-state; blocking acquire waiting then succeeding;
probe-failure-fails-safe; env override.

Full offline suite: **1369 passed, 15 deselected** (`-m "not network and not ollama and not
integration"`), up from 1350 — no regressions, and `run_extraction` now taking a real flock
did not hang or slow the suite.

### Gate 2 — restart_ollama foreign-skip and self-proceed

`tests/test_restart_ollama_guard.py` — **6 passed**:

| test | asserts |
|---|---|
| `test_restart_skipped_under_foreign_lock` | with a real child process holding the lock: `subprocess.run` **not called**, `httpx.get` **not called**, log contains `RESTART SKIPPED — experiment lock held` |
| `test_restart_proceeds_under_self_lock` | inside `hold_experiment_lock()`: `check_experiment_lock()` is True but `foreign_lock_held()` is False, and `sudo systemctl restart ollama` **is** invoked |
| `test_restart_proceeds_when_lock_is_free` | unchanged behaviour when nothing holds the lock |
| `test_skip_returns_none_and_does_not_raise` | a skip is a no-op, not an error — extraction continues |
| `test_run_extraction_holds_the_lock` | during the loop: held=True, self=True, foreign=False; released afterwards |
| `test_run_extraction_can_opt_out` | `experiment_lock=False` escape hatch |

### Gate 3 — health script skip path demonstrated

Run manually against an isolated `HOME` (so the real `~/logs` was untouched) with a PATH
stub for `ollama` that records invocations. Three paths exercised:

| path | trigger | stdout | ollama invocations |
|---|---|---|---:|
| **HELD** | child process holding the lock | `SKIPPED — experiment lock held (…)`, exit 0 | **0** |
| **ACQUIRED** | lock free | `Testing qwen3:8b (120s) ... OK`, exit 0 | 2 (stub `list` + `run`) |
| **ERROR** | `OLLAMA_EXPERIMENT_LOCK=/proc/nonexistent-dir/x.lock` | `WARN: cannot open experiment lock … (FAIL: guard unavailable)`, exit 0 | **0** |

Skip-path log, verbatim:

```
Ollama Health Check — Mon Jul 27 23:20:42 UTC 2026
========================================
Ollama systemd Environment:
Environment=PATH=… OLLAMA_KV_CACHE_TYPE=f16 OLLAMA_MAX_LOADED_MODELS=1 OLLAMA_NUM_PARALLEL=1 OLLAMA_FLASH_ATTENTION=true OLLAMA_KEEP_ALIVE=-1
========================================
SKIPPED — experiment lock held (/…/test.lock)
No models were exercised; no Ollama requests were issued.
========================================
Done: Mon Jul 27 23:20:42 UTC 2026
```

Note the systemd env snapshot still runs on a skipped day: `systemctl show` touches systemd,
not Ollama, and the env-drop canary (the Apr 19 0.21.0 failure mode) is worth keeping.

Real log untouched — `~/logs/ollama_health_20260727.log` SHA-256 identical before and after
all three runs (`9dfe3109…fe7b80`).

**Repo shell-test convention:** none exists (no `tests/*.sh`, no bats/shellcheck harness), so
the above is documented manual verification plus `bash -n` syntax validation. The commands
are reproducible from this report.

### Gate 4 — digest treats the skip as non-FAIL

Parser inspected at `~/scripts/morning_digest.sh:206-222` (**not modified**). Logic:

1. `is_stale "$latest_ollama_log" "$OLLAMA_STALE_HOURS"` (26 h) — a missing, **zero-byte**, or
   >26 h-old log is `STALE`, which flips `overall_pass=false` on the same plumbing as FAIL.
2. Otherwise `problems=$(grep -i -E 'TIMEOUT|FAIL' "$log")` — **any** hit ⇒ `ollama_status="FAIL"`.
3. Otherwise PASS, "all models OK".

Both constraints drove the wording: the skip line must be **written to a fresh, non-empty
log** (or it goes STALE, not PASS) and must contain **neither** `TIMEOUT` nor `FAIL`
case-insensitively. `SKIPPED — experiment lock held` satisfies both.

Verdict simulation running that exact logic against the three produced logs:

| log | digest verdict |
|---|---|
| skip path | **PASS** |
| acquired path | **PASS** |
| error path | **FAIL** — matched `WARN: cannot open experiment lock … (FAIL: guard unavailable)` |

The ERROR-path FAIL is deliberate: a broken guard should be loud in the morning digest
rather than a silent loss of health monitoring. The script also writes `ALERT` on that path,
matching the existing convention.

### Gate 5 — no infrastructure changes, no Ollama calls

`crontab -l`, systemd units, sudoers, and services were never read or written. The only
out-of-repo change is the script file itself (§3).

---

## 3. Health script diff

`~/scripts/ollama_health_check.sh` — **not under version control**, so the diff is reproduced
here in full (72 lines, two hunks). Original backed up to the session scratchpad as
`ollama_health_check.sh.orig`.

```diff
--- ollama_health_check.sh.orig
+++ /home/ankitsarin/scripts/ollama_health_check.sh
@@ -38,6 +38,31 @@
 
 ALERT=false
 
+# ── OPS-GUARD-01 (B): experiment lock ────────────────────────────────────
+# Stand down while a long-running Ollama experiment holds the lock. Root cause
+# (OPS-OLLAMA-02): this cron's inference loop destroyed an in-flight Arm P run.
+#
+# flock(2) only — never an existence check. The lock file is created once and
+# left behind; its presence means nothing, so a holder that dies releases
+# instantly and there are no stale locks to clean up. Keeping fd 9 open for the
+# rest of the script means that once we DO acquire, we hold it for the whole
+# health check, which closes the race where an experiment starts a millisecond
+# after a probe-and-release.
+#
+# Fail-safe direction: if the lock file cannot be opened at all we skip
+# inference (protecting experiments) AND emit a line the digest reads as FAIL,
+# so a broken guard is loud rather than a silent loss of health monitoring.
+LOCKFILE="${OLLAMA_EXPERIMENT_LOCK:-$HOME/.ollama_experiment.lock}"
+LOCK_OK=true
+exec 9>>"$LOCKFILE" 2>/dev/null || LOCK_OK=false
+if ! $LOCK_OK; then
+  LOCK_STATE="ERROR"
+elif flock -n 9; then
+  LOCK_STATE="ACQUIRED"
+else
+  LOCK_STATE="HELD"
+fi
+
 HEADER="Ollama Health Check — $(date)"
 SEPARATOR="========================================"
 
@@ -63,6 +88,37 @@
   echo "$SEPARATOR"
 } > "$LOGFILE"
 
+# Skip all inference when an experiment owns the box. Note the systemd env
+# snapshot above still runs: `systemctl show` touches systemd, not Ollama, and
+# the env-drop canary is the one check worth keeping on a skipped day.
+if [[ "$LOCK_STATE" == "HELD" ]]; then
+  # Wording matters: morning_digest.sh greps the log for /TIMEOUT|FAIL/i and
+  # reports FAIL on any hit, so this line must contain neither substring. It
+  # must also be non-empty and fresh, or is_stale() reports STALE instead.
+  msg="SKIPPED — experiment lock held ($LOCKFILE)"
+  printf '%s\n' "$msg"
+  {
+    printf '%s\n' "$msg"
+    echo "No models were exercised; no Ollama requests were issued."
+    echo "$SEPARATOR"
+    echo "Done: $(date)"
+  } >> "$LOGFILE"
+  exit 0
+fi
+
+if [[ "$LOCK_STATE" == "ERROR" ]]; then
+  msg="WARN: cannot open experiment lock $LOCKFILE (FAIL: guard unavailable)"
+  printf '%s\n' "$msg" >&2
+  {
+    printf '%s\n' "$msg"
+    echo "Skipped inference because experiment-lock state is unknown."
+    echo "$SEPARATOR"
+    echo "ALERT"
+    echo "Done: $(date)"
+  } >> "$LOGFILE"
+  exit 0
+fi
+
 MODELS=$(ollama list 2>/dev/null | tail -n +2 | awk '{print $1}')
 
 if [[ -z "$MODELS" ]]; then
```

**Follow-up flag:** `~/scripts/` holds three cron-driven scripts
(`morning_digest.sh` 25 KB, `ollama_health_check.sh`, `service_health_check.sh`) that are
load-bearing for daily ops and **have no version control, no review trail, and no backup**.
The guard I just added there is exactly the kind of change that wants history. Recommend
bringing `~/scripts` under git as its own small repo. Not done here — out of scope, and it
touches nothing this task was authorized for.

---

## 4. Note for the determinism project

The determinism harness needs **one context manager around its run loop** to be protected.
From `~/projects/inference-determinism`, with `~/projects/evidence-engine` importable
(`PYTHONPATH`), the whole change is:

```python
from engine.utils.ollama_lock import hold_experiment_lock

with hold_experiment_lock():          # blocking; waits for any other experiment
    ...existing probes.cache_arms body...
```

If cross-repo import is undesirable — and for a determinism harness it probably is, since it
would couple two projects — the dependency-free alternative is six lines with no import at
all, because the protocol *is* the flock and nothing else:

```python
import fcntl, os
fd = os.open(os.path.expanduser("~/.ollama_experiment.lock"), os.O_RDWR | os.O_CREAT, 0o644)
fcntl.flock(fd, fcntl.LOCK_EX)        # blocks until any other experiment finishes
try:
    ...existing run...
finally:
    os.close(fd)                      # releasing is just closing the fd
```

Either form makes the next Arm P attempt immune to both failure modes that killed the
previous two: this repo's `restart_ollama()` will log `RESTART SKIPPED — experiment lock
held` and continue without restarting, and the 07:00 health cron will log `SKIPPED —
experiment lock held`, issue zero Ollama requests, and still report PASS in the morning
digest. The only behavioural cost is that starting a run *during* a health check will block
until that check finishes (bounded by the per-model timeout sum), which is the intended
trade. **The harness must acquire the lock before its first Ollama call**, not after — the
guard protects only what the lock covers.

---

## Acceptance gates

| gate | status |
|---|---|
| 1. Lock module + tests green; full offline suite green | ✅ 13 lock tests; **1369 passed, 15 deselected** |
| 2. restart_ollama foreign-skip and self-proceed demonstrated | ✅ 6 tests, §2 gate 2 |
| 3. Health script skip path demonstrated, no Ollama calls | ✅ §2 gate 3 — 0 stub invocations on skip; real log SHA unchanged |
| 4. Digest treats the skip as non-FAIL | ✅ §2 gate 4 — parser inspected at `morning_digest.sh:206-222`, unmodified; skip ⇒ PASS |
| 5. No crontab/systemd/sudoers/service changes; no Ollama API calls | ✅ |

**Out of scope and not done:** the determinism-harness edit (note only, §4), Arm P third
attempt, validator enforcement, the `ReviewDatabase` footgun, stray `data/data` cleanup, any
prompt/codebook changes.
