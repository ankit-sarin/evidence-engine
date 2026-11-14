# 12ag-GATEUNIT-A — Phase A read-out: the gate wrapper launching a gate as a `systemd-run --user` unit

R805(g), R807(e). Read-only: no tool, engine, test or script edit; neither gate script run; no unit launched, started or stopped;
no tmux session created or killed; no live write; no model call. Reference HEAD `12c11080e8e83685b20817f66868083ee21e6782`
(12ag-GATEUNIT-A item 0). Every claim is beside a quoted line with its path and line, or beside the command whose output is saved
under `reads/` (`box_reads.sh` → `box_reads.txt`, captured 2026-11-14T20:41:46Z; `other_lane_units.txt`; `manager_misc.txt`;
`man_oompolicy.txt`; `inputs_rulings.txt`). This read-out states options and their costs; it decides nothing.

## The answer

The two scripts assume tmux in exactly three lines: the refusal `tmux has-session` (`launch_gate.sh:8`), the launch line
(`:36`) and its `eval` (`:38`). `run_gate.sh` needs no terminal. It finds its repository by itself, runs the interpreter by path,
sends all output to files, and writes `gate_rc.txt` last. A caller waits on that file, and that does not change with the launch.

This box can host a unit: Linger=yes, systemd 255, and no memory or CPU cap on the user's slices. A unit would differ from today's
tmux launch in four ways that matter:
- **Its soft open-files limit is 1024**, against this shell's 524288.
- **Its processes inherit `OOMScoreAdjust=100`.**
- **Under a memory cap, the manager's `DefaultOOMPolicy=stop` stops the whole unit** if one process is OOM-killed. `run_gate.sh`
  would die with pytest and never write `gate_rc.txt`.
- **Its environment is the user manager's, not this Claude Code session's.** It has no `.venv` on PATH, no `TMUX`, LANG
  `en_US.utf8`, and neither provider key. No test reads anything it would lose.

What each difference does to a gate's wall time cannot be established without a launch.

## §0 Scope

The gate wrapper's launch as a `systemd-run --user` unit, and nothing else (R807(e)). Out of scope: any tool edit; launching
anything; the other tools items; the cross-lane item for dgx-infra; U30.

## §1 Inputs, quoted

**The global rule** (`~/claude-config/CLAUDE.md:254–262`, added by claude-config `9f78b9a`, "docs(rules): add long-running jobs rule
(PI 2026-11-11)", 2026-11-12T04:38:10Z; the last commit to touch the file), whole: "**Long-running jobs (PI, 2026-11-11).** A job
expected to run longer than about 15 minutes, or due to start later than launch, never runs as a Claude Code background task. If it
is unattended and a later step depends on its result, or it loads a model (model, batch, benchmark and scheduled runs), it runs as a
transient `systemd-run --user` unit. The brief names the unit and any memory cap. At launch Claude Code reports `systemctl --user
status <unit>` and confirms Linger=yes. The run logs to the journal and to its own folder. If the PI may need to watch it or answer
prompts (installs, exploratory commands, long downloads), it runs in a named tmux session, and the brief names the session. A tmux
window may follow a unit's log."

**The rulings** (fence copies, plan lines; `reads/inputs_rulings.txt`):
- **R804(b)** (`:18822`): "The 12ag-OPEN gate, and no other, is launched as the brief's step 3e wrote it: by launch_gate.sh, in the
  tmux session 12ag_open. … A change to the wrapper so that a gate launches as a unit is a tools change in a brief of its own; no
  later gate brief relies on this exception."
- **R805(g)** (`:18909`): "the tools change that lets the gate wrapper launch a gate as a systemd-run --user unit (R804(b)),
  two-phase".
- **R731(b)** (`:12898`): "from here every gate's evidence directory records the complete launch line, tmux wrapper included, and the
  next brief's I3 quotes it from there."
- **R732(d)** (`:12988`): "From here the gate's own wrapper writes the launch line into the evidence directory at launch (R731(b)'s
  rule, made concrete)."
- **R738(c)** (`:13475`): "the launching step holds the launch line in one variable, writes the timestamp sentence and that variable's
  value into launch_line.txt, then executes that variable, so the line written and the line run cannot differ."
- **R738(d)** (`:13482`): "gate_12aa_open/'s nineteen files (gate_c2's set: collected_ids.txt and tree_at_launch.txt included) are the
  gate-evidence set of record from here".
- **R746(e)(iv)** (`:13903`): "(iv) the standard gate wrapper, writing its launch line and the gate_12aa_open evidence set itself."

**The tools README** (`docs/session-reports/tools/README.md`):
- `:10`: "| `launch_gate.sh` | `<evidence_dir> <session_name> <ref_ids\|-> <ref_durations\|-> [selection...]`, run from the repository
  root | Dry run on `tests/test_u20_modelfile_num_ctx.py`: the nineteen files, and the launch line's inner command equal to
  `gate_launch.txt`'s line 2 | `dry_run/`, `2d_check.txt` |".
- `:11`: "| `run_gate.sh` | as `launch_gate.sh`, which calls it inside tmux | The same dry run. It writes `gate_rc.txt` last, so a wait
  watches that file. …".

## §2 Q1 — The two scripts (CODE ORDER, read whole: `launch_gate.sh` 39 lines, `run_gate.sh` 50)

**`launch_gate.sh`:**
- **`:5`** `set -e`.
- **`:6`** Resolves the evidence directory `E` (`readlink -f`), the session name, both references and the selection, and `HERE`, the
  tools directory.
- **`:7`** `mkdir -p "$E"`, and refuses if `gate_rc.txt` exists ("REFUSED: $E already holds a gate").
- **`:8`** `tmux has-session -t "$SESSION" … && { echo "REFUSED: tmux session $SESSION exists"; exit 2; }`. **tmux assumed.**
- **`:9`** Copies `run_gate.sh` into `E` (file 1).
- **`:10–18`** Writes `host_snapshot.sh` (file 2). It reads the date, uptime, top, memory, kernel, Python, libsqlite3 and the driver;
  the boot id and `systemctl show ollama`; earlyoom; Ollama's `oom_score_adj`; MemAvailable; and the concurrent jobs. That last read
  excludes "this script and its ancestors" by walking `ppid` to 1 (`:17`).
- **`:19–30`** Writes `ollama_reads.sh` (file 3).
- **`:31`** Writes `ollama_reads_before.txt` (4).
- **`:32`** Writes `host_snapshot_before_gate.txt` (5).
- **`:33`** Writes `tree_at_launch.txt` (6), with `git rev-parse HEAD; git status --short` in the caller's working directory.
- **`:34`** Writes `temp_root_before.txt` (7) from `/tmp/pytest-of-$USER`.
- **`:35`** `sleep 6` (R660(b)).
- **`:36`** `LINE="tmux new-session -d -s $SESSION \"bash $E/run_gate.sh $E $SESSION $REF $REFD $*\""`. **tmux assumed; the session
  name is the tmux session's name.**
- **`:37`** Writes `launch_line.txt` (8): a timestamp sentence, then `$LINE` (R738(c)).
- **`:38`** `eval "$LINE"`. The launcher returns as soon as tmux has created the detached session.
- **`:39`** `cat "$E/launch_line.txt"`.

**`run_gate.sh`:**
- **`:2`** The header: "run inside tmux by launch_gate.sh". The comment is the only tmux in the file.
- **`:6–7`** `cd` to `git -C <its own directory> rev-parse --show-toplevel`, else `$HOME/projects/evidence-engine`; and again to the
  latter if there is no `.venv`. The copy runs from `E`, under scratch, so the fallback is what applies. It needs `HOME`, not a
  terminal or the caller's working directory.
- **`:8`** Writes `gate_launch.txt` (9): a timestamp, then the command as run. Then `s=$(date +%s.%N)`.
- **`:9`** No selection: `tests/` in the shared temp root. With a selection: `--basetemp="$E.basetemp"`.
- **`:10`** `.venv/bin/python -m pytest … > "$E/gate.log" 2>&1; rc=$?` writes `gate.log` (10). The interpreter is called by path,
  stdout and stderr go to the file, and nothing reads stdin.
- **`:11`** Writes `gate_exit.txt` (11).
- **`:12`** Appends "pytest exit code: $rc" to `gate.log`, and writes `gate_wall.txt` (12) from `bc`.
- **`:13`** Writes `durations_$SESSION.txt` (13). **The session name is in the file name.**
- **`:14`** Writes `temp_root_after.txt` (14), from `/tmp/pytest-of-$USER` (`USER` needed).
- **`:15`** Writes `host_snapshot_after_gate.txt` (15).
- **`:16`** `sleep 6`, then writes `ollama_reads_after.txt` (16).
- **`:17–49`** `python3 -` (the PATH's python3) writes `id_and_durations_compare.txt` (17) and, inside it, `collected_ids.txt` (18);
  it reads `durations_{session}.txt` (`:39`).
- **`:50`** `echo "$rc" > "$E/gate_rc.txt"` (19), **last**.

**The nineteen files by writer:**

| Writer | Files, in order written |
| --- | --- |
| `launch_gate.sh`, before the launch (8) | `run_gate.sh`, `host_snapshot.sh`, `ollama_reads.sh`, `ollama_reads_before.txt`, `host_snapshot_before_gate.txt`, `tree_at_launch.txt`, `temp_root_before.txt`, `launch_line.txt` |
| `run_gate.sh`, inside the launch (11) | `gate_launch.txt`, `gate.log`, `gate_exit.txt`, `gate_wall.txt`, `durations_<session>.txt`, `temp_root_after.txt`, `host_snapshot_after_gate.txt`, `ollama_reads_after.txt`, `collected_ids.txt` and `id_and_durations_compare.txt` (one heredoc), `gate_rc.txt` |

The 12ag-OPEN gate directory lists exactly these nineteen (`ls … | wc -l` → 19).

**How the exit code reaches `gate_rc.txt`.** `rc=$?` straight after pytest (`:10`), carried through the later steps, and written last
(`:50`). `run_gate.sh` has no `set -e`, so a failing later step does not stop the write. But if `run_gate.sh` itself is killed,
nothing writes the file.

**What a caller waits on.** `gate_rc.txt` (`launch_gate.sh:4`, "Wait on <evidence_dir>/gate_rc.txt, which run_gate.sh writes last";
README `:11`). Not the tmux session or a process. A unit launch leaves this unchanged.

## §3 Q2 — What the rule asks of a launch

| Item (the rule's words) | What the present launch records | What it does not |
| --- | --- | --- |
| "a transient `systemd-run --user` unit" | a tmux session (`launch_gate.sh:36`) | no unit |
| "The brief names the unit" | the brief names the tmux session (the second argument) | no unit name |
| "and any memory cap" | none; no cap is set anywhere on the path | no cap, and no statement that there is none |
| "At launch Claude Code reports `systemctl --user status <unit>`" | `launch_line.txt` and the before-gate snapshots | no status read, since there is no unit |
| "confirms Linger=yes" | nothing | Linger is never read (Q3: it is yes) |
| "The run logs to the journal" | nothing goes to the journal: every output is redirected to `E` (`run_gate.sh:10`, `:17`) | no journal entries |
| "and to its own folder" | the evidence directory `E`, the nineteen files | — (met) |
| "A tmux window may follow a unit's log" | — | (permissive; nothing owed) |

## §4 Q3 — This box (`reads/box_reads.txt` unless named)

- **Linger:** `loginctl show-user ankitsarin` → "Linger=yes", "State=active", "Service=user@1000.service", "Slice=user-1000.slice".
- **systemd:** "systemd 255 (255.4-1ubuntu8.18)".
- **The user manager's state:** `systemctl --user is-system-running` → "degraded" (exit 1), with "Failed: 2 units":
  - `run-u134.scope`, "Failed with result 'oom-kill'" on 2026-10-18. Its command was `python3 -I -c "b=bytearray(400*1024*1024)…"`,
    another lane's test of a capped scope (`reads/other_lane_units.txt`).
  - `update-notifier-crash.service`.
- **Manager defaults:** "DefaultStandardOutput=journal", "DefaultLimitNOFILE=500000", "DefaultLimitNOFILESoft=1024",
  "DefaultTasksMax=153552", "DefaultMemoryAccounting=yes", "DefaultCPUAccounting=yes", "DefaultTimeoutStopUSec=1min 30s";
  "DefaultOOMPolicy=stop" (`reads/manager_misc.txt`). The OOMPolicy text from `man systemd.service` is in `reads/man_oompolicy.txt`:
  "If set to stop the event is logged but the unit is terminated cleanly by the service manager."
- **Slices:**
  - `user@1000.service`: "Delegate=yes", "MemoryMax=infinity", "MemoryHigh=infinity", "CPUWeight=[not set]",
    "OOMScoreAdjust=100", "LimitNOFILE=524288".
  - `user-1000.slice`: "MemoryMax=infinity", "TasksMax=337815".
  - `app.slice`: "CPUWeight=100", "MemoryMax=infinity".
  - `background.slice`: "CPUWeight=30".
  - `systemd-oomd` is "inactive" (`reads/manager_misc.txt`).
- **This shell:** `cat /proc/self/cgroup` → "0::/user.slice/user-1000.slice/session-c382.scope"; `ulimit -n` → 524288;
  `oom_score_adj` 0; nice 0.
- **The environments.** Values are printed for an allow-list only; every other variable appears by name with its value withheld.

  | Variable | `systemctl --user show-environment` | this session's env (what a tmux gate launched from here inherits) |
  | --- | --- | --- |
  | PATH | `/home/ankitsarin/.local/bin:/usr/local/cuda/bin:…:/usr/bin:/bin:…` (no `.venv`) | `/home/ankitsarin/projects/evidence-engine/.venv/bin:` first, then the VS Code server's paths, then the same |
  | HOME, USER, LOGNAME, SHELL | the same | the same |
  | LANG | `en_US.utf8` | `C.UTF-8` |
  | TMPDIR, TMP | unset | unset |
  | XDG_RUNTIME_DIR | `/run/user/1000` | unset |
  | VIRTUAL_ENV | unset | `/home/ankitsarin/projects/evidence-engine/.venv` |
  | PYTHONSTARTUP | unset | set (a VS Code pythonrc) |
  | TMUX | unset | unset in the session; set inside any tmux pane |
  | OLLAMA_HOST, EVIDENCE_ENGINE_* | unset | unset |
  | `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` | absent | present (by name) |
  | desktop variables (DISPLAY, XAUTHORITY, SSH_AUTH_SOCK, …) | present | absent |

- **User units and timers:** user timers `surgical-cv-health.timer`, `surgical-cv-worker.timer`, `surgical-cv-backup.timer`,
  `launchpadlib-cache-clean.timer` and a snap notifier. The two surgical-cv services are `Type=oneshot` user services with
  `WorkingDirectory=%h/projects/surgical-cv`, `Environment=PYTHONUNBUFFERED=1`, an `EnvironmentFile=%h/projects/surgical-cv/.env`
  and `ExecStart=%h/projects/surgical-cv/.venv/bin/python -m app.worker --once` / `-m app.health` (`reads/other_lane_units.txt`).
- **The 07:00 health job and the 09:00 nightly** are cron jobs, not units (`crontab -l`): "0 9 * * * /bin/bash
  /home/ankitsarin/projects/evidence-engine/scripts/nightly_tests.sh" and "0 7 * * * /bin/bash
  /home/ankitsarin/scripts/ollama_health_check.sh >> /home/ankitsarin/logs/ollama_health_cron.log 2>&1".
  - Their environment is cron's. `/etc/pam.d/cron` loads `pam_env.so` (`/etc/environment`), `pam_env.so
    envfile=/etc/default/locale` and `pam_limits.so` (`reads/manager_misc.txt`).
  - The nightly does `cd "$PROJECT_DIR"`, `source .venv/bin/activate`, then `python -m pytest tests/ -v --tb=short --durations=0 -m
    "$STANDARD_GATE"` (`scripts/nightly_tests.sh:37–40`).
- **Does any lane launch a job by `systemd-run --user` already?** No code path in any repository on this box launches a long job as a
  `systemd-run --user` service. The grep (`box_reads.txt`, last section; 175 lines outside the plan) finds:
  - surgical-cv's capped scope, in its design and primers: "`systemd-run --user --scope --expand-environment=no … MemoryHigh=24G
    MemoryMax=32G MemorySwapMax=0`" (`surgical-cv/docs/Edit_Feature_Paper_Design_v7.md:249`), "A capped scope works for this user"
    (its primer history);
  - one test docstring, "(``systemd-run --user --no-block --service-type=oneshot /bin/sleep 20``)"
    (`surgical-cv/tests/test_health_probes.py:11`);
  - a root `systemd-run` script writer (`dgx-infra/tools/r4b_replica.py:221`);
  - the rule itself.
- **tmux:** "no server running on /tmp/tmux-1000/default". Which cgroup a tmux server started from this shell lands in cannot be read
  with none running.

## §5 Q4 — What a gate needs from its environment

- **Working directory.** None from the caller for `run_gate.sh` (`:6–7`, its own `cd`). `launch_gate.sh` itself runs `git` in the
  caller's directory (`:33`), and its usage says "from the repository root" (`:3`). A launch does not change that.
- **Interpreter.** `.venv/bin/python` by path (`run_gate.sh:10`), so PATH and `VIRTUAL_ENV` do not choose it. The comparison step's
  `python3 -` (`:17`) and `host_snapshot.sh`'s `python3 --version` resolve by PATH. Today that is the venv's python3, under a unit
  `/usr/bin/python3`. The venv's interpreter resolves to the system's (`readlink -f .venv/bin/python3` → `/usr/bin/python3.12`;
  `/usr/bin/python3 --version` → "Python 3.12.3"), so the version line reads the same.
- **The shared temp root.** `/tmp/pytest-of-$USER` (`:14`; `launch_gate.sh:34`), which needs `USER` (present in both) and no `TMPDIR`
  (unset in both).
- **The experiment lock.** No test takes `~/.ollama_experiment.lock`. The suite gives each test its own lock path through
  `OLLAMA_EXPERIMENT_LOCK` (14 reads in tests; `tests/test_ollama_lock.py:233–235` passes `PATH`, `PYTHONPATH` and
  `OLLAMA_EXPERIMENT_LOCK` explicitly to its child).
- **stdout and stderr.** To `gate.log` (`:10`) and the other files. Nothing goes to a terminal.
- **Anything read from a terminal.** Nothing. No test reads `sys.stdin` or `isatty` (grep: no hit).
- **The environment variables the suite and the engine read** (grep over `engine/`, `scripts/`, `tests/`):
  - `OLLAMA_EXPERIMENT_LOCK` and `OLLAMA_HOST`, set by the tests themselves or unset in both environments.
  - `TMUX`, read by `engine/utils/background.py:38` and set or popped by `tests/test_background.py` itself.
  - `OPENAI_API_KEY` / `ANTHROPIC_API_KEY`, read only when no `api_key` is passed (`openai_extractor.py:39`,
    `anthropic_extractor.py:40`). Every test construction passes one (`api_key="test-key"` / `"k"`, e.g.
    `tests/test_request_capture.py:364–365`).
  - `PATH`, read by `tests/test_ollama_lock.py:234`.
  - Nothing found reads LANG. Whether any test's output depends on the locale is not established.

## §6 Q5 — What else depends on the present launch (`census_gua.py` → `census_gua.txt`)

Places: every file under `docs/session-reports/tools/`, `docs/plan/STANDING_RULES.md`, and the plan's 12ag expected-values block
(`:5480–5515`). Patterns: `tmux`, the session name, `launch_line.txt`, `gate_launch.txt`, `run_gate.sh` and the durations file's name.
Computed: 41 hits in 9 places scanned (6 of them with hits). The classes are CC's judgement.

| Where | What it says | Under a unit launch (CC's class) |
| --- | --- | --- |
| `launch_gate.sh:8` | the `tmux has-session` refusal | **breaks**: it would test a tmux session that never exists; the refusal would need the unit's name |
| `launch_gate.sh:36`, `:38` | the tmux launch line and its `eval` | **breaks**: this is the line the change replaces |
| `launch_gate.sh:37` (R738(c)) | the line held in one variable, written, then run | **stays true**: the form survives with a different line |
| `launch_gate.sh:3`, `:4`; `README.md:10` | `<session_name>`; "Wait on … gate_rc.txt" | **stays true**: the argument can name the unit; the wait is unchanged |
| `run_gate.sh:2`; `README.md:11` | "run inside tmux by launch_gate.sh"; "which calls it inside tmux" | **goes stale** (comments and README) |
| `run_gate.sh:8`; `README.md:10`, `:13` | `gate_launch.txt` line 2, "the command as run", compared with the launch line's inner command | **stays true** if the unit runs the same `bash $E/run_gate.sh …` command; the dry-run check would read the inner command out of a `systemd-run` line instead of a tmux line |
| `run_gate.sh:13`, `:39` | `durations_$SESSION.txt` | **stays true** while the second argument is kept; its value becomes the unit's stem |
| `session_open_check.py:157`; `README.md:17` | "this lane's kind … pytest, or the gate wrapper" (`"run_gate.sh"` in a process's arguments) | **stays true**: the unit's process arguments still carry `run_gate.sh` |
| `STANDING_RULES.md:948–956` (R731(b)) | "the complete launch line, tmux wrapper included" | **goes stale** in its wording: the complete line would carry a `systemd-run` wrapper; the rule's intent (the whole line recorded) stays true |
| `STANDING_RULES.md:1001–1022` (R732(d), R733) | the 12z-OPEN reconstructed tmux line | **stays true**: history of one gate |
| `STANDING_RULES.md:1113–1119` (R738(c)) | the one-variable form | **stays true** |
| `STANDING_RULES.md:1302–1310` | "an open's pre-check reads the process table and tmux for the named archived sessions" | **stays true**: about archived CC sessions, not gates; a unit gate would also need `systemctl --user list-units` to be seen by such a read (not owed by the text) |
| plan `:5488` (12ag block) | "the nineteen files (R738(d)), by … launch_gate.sh and run_gate.sh, the launch line written at launch (R738(c))" | **stays true** |

## §7 Q6 — Wall time

Each difference is between a gate started by `systemd-run --user` and one started from tmux in this Claude Code session. None is
measured here.

1. **cgroup and CPU share.**
   - A unit runs under `user@1000.service/app.slice` ("CPUWeight=100"). A tmux gate runs where its tmux server runs; from this shell
     that is "session-c382.scope" under `user-1000.slice` (INFERRED: no server is running to read).
   - Under contention the two compete differently. A unit shares `user@1000.service`'s share with that service's other units (the
     surgical-cv user timers); a session scope is that service's sibling.
   - **Not established** whether this moves a gate's wall time with the box quiet.
2. **Open-files limit.** "DefaultLimitNOFILESoft=1024" for a unit, against `ulimit -n` 524288 here. A suite that needed more than 1024
   descriptors at once would fail, not slow. **Not established** whether it does.
3. **OOM score.** `user@1000.service` "OOMScoreAdjust=100", which the unit's processes inherit (INFERRED from systemd's inheritance;
   not read from a running unit), against 0 here. earlyoom runs as "-m 6 -s 100,100 … --prefer ^ollama$". This affects which process
   dies under memory pressure, not speed.
4. **Nice and IO weight.** Nice 0 here; a unit's default nice is 0 unless `--nice` is given. IOWeight is "[not set]" on every slice
   read. **No difference found.**
5. **Environment.** LANG, PATH, `VIRTUAL_ENV`, `PYTHONSTARTUP` and the provider keys differ (§4). No read of them by the suite changes
   a test's path (§5). **Not established** whether LANG changes any test's speed.
6. **Output.** All gate output goes to files in both launches (`run_gate.sh:10`). A unit's own stdout would go to the journal
   ("DefaultStandardOutput=journal"), and `run_gate.sh` prints nothing else. **No difference found.**
7. **Accounting.** CPU and memory accounting are on for the user manager ("DefaultCPUAccounting=yes", "DefaultMemoryAccounting=yes").
   Their cost to a gate is **not established**.

**Readings beside it:**
- The three nightlies, cron-launched (§4), report pytest's own times of 985.50 s, 984.60 s and 983.84 s
  (`12af_close/nightly_20261111.txt` to `nightly_20261113.txt`, the summary lines).
- The four gates of the band in force are 963.43, 966.10, 965.92 and 979.95 s of `run_gate.sh` wall (R803(b)).
- The 12ag-OPEN gate was 1042.65 s of wall and 1040.78 s by pytest's own summary, confounded (R805(b)).
- The series already mixes launch forms: tmux gates and cron nightlies. The nightlies are not series members.

## §8 Options for the ruling (each with what it records and what it costs; nothing decided here)

**(A) A unit launch replacing tmux in `launch_gate.sh`.**
- **The launch line:** `systemd-run --user --unit=<unit> --collect [--property=…] bash $E/run_gate.sh $E <name> $REF $REFD $*`, held
  in `LINE`, written to `launch_line.txt` and run (R738(c) unchanged). `launch_line.txt` would hold the timestamp sentence and that
  line.
- **The unit's name:** from the second argument, e.g. `gate-<name>`. The `:8` refusal becomes a read of that unit (a `systemctl
  --user` status or list form).
- **A memory cap or none:**
  - With `MemoryMax`, a gate that crosses it is OOM-killed inside the cgroup, as `run-u134.scope` was. Under "DefaultOOMPolicy=stop"
    the whole unit is then stopped, `run_gate.sh` with it, and `gate_rc.txt` is never written. The caller's wait would end only at its
    bound, as a distinct timeout.
  - `--property=OOMPolicy=continue` would let `run_gate.sh` survive to write the killed pytest's code.
  - With no cap, nothing changes from today (`MemoryMax=infinity` on every slice).
- **The evidence set:** the nineteen files stay, with `launch_line.txt`'s line in the new form. Gains, if the brief adds them: the
  rule's `systemctl --user status <unit>` at launch and the Linger read, as a twentieth file or inside `launch_line.txt`; and the
  journal's start and stop lines for the unit. Loses: nothing of the nineteen.
- **What the caller waits on:** `gate_rc.txt`, unchanged.
- **Costs:** three lines of `launch_gate.sh`, the `run_gate.sh:2` comment, README `:11`, R731(b)'s wording, and the soft-limit and
  environment differences of §7, unless the line sets them (`--property=LimitNOFILE=…`, `--setenv=…`).

**(B) A mode switch on `launch_gate.sh`** (a unit by default and tmux on request, or the reverse).
- **Records:** the same as (A) in unit mode; today's form in tmux mode. The rule keeps a tmux home for runs the PI watches.
- **Costs:** a branch in the one launcher, two launch-line forms in the record, and Phase B validating both.

**(C) A second launcher beside it** (`launch_gate_unit.sh`, sharing `run_gate.sh`).
- **Records:** the same as (A), with `launch_gate.sh` untouched.
- **Costs:** the before-gate reads (`:9–35`) exist twice unless they move into a shared file. That is "one predicate, two programs",
  copied, and itself a further tools change.

**For every option: Phase B and the series.**
- **Phase B in 12aa-SETUP's form.** The dry run on `tests/test_u20_modelfile_num_ctx.py`, checking:
  - the nineteen files;
  - the launch line's inner command equal to `gate_launch.txt`'s line 2 (`12aa_SETUP/2d_check.txt`: "launch line's inner == what
    ran: True … nineteen files: True 19 … basetemp under scratch: True");
  - plus the unit's status, Linger, the unit's environment and its `LimitNOFILE` read from the running unit.
- **Whether a standard gate belongs in Phase B.** A dry run measures no wall time. A standard gate under the unit would be the first
  wall-time reading of the new launch.
- **The first unit-launched gate and the band series.** The band in force was cut from tmux gates (R803(b)). The first unit gate is
  either a series member read beside §7's differences, or marked by its launch form until a second confirms it. That is the ruling's
  call.

## §9 Findings

- **F1.** R4 holds on disk:
  - `launch_gate.sh` is 39 lines and `run_gate.sh` 50;
  - `:8` and `:36` read as quoted;
  - `run_gate.sh:2` says "run inside tmux by launch_gate.sh";
  - the durations file is `durations_$SESSION.txt` (`:13`);
  - `gate_rc.txt` is written last (`:50`).
  R6 holds against `CLAUDE.md:254–262` at `9f78b9a`, the last commit to the file.
- **F2.** The user manager is "degraded", with two failed units. One is another lane's capped scope killed by the OOM killer in
  October; the other is `update-notifier-crash.service`. Whether "degraded" refuses a transient start is not established; the state
  is read here, not cleared.
- **F3.** "DefaultOOMPolicy=stop". A capped gate unit that crosses its cap is stopped whole, `run_gate.sh` with it, so `gate_rc.txt`
  is never written and the wait ends at its bound (§8 (A)).
- **F4.** A unit's soft open-files limit is 1024 against this shell's 524288 (§7 2).
- **F5.** A tmux gate launched from Claude Code inherits that session's environment, the two provider keys by name included; a unit
  would not (§4). No test reads them (§5).
- **F6.** The 07:00 health job and the 09:00 nightly are cron jobs, not units (§4). The R85/R86 exclusivity reads name them as
  scheduled jobs, which stays true.
- **F7.** No lane on this box launches a long job as a `systemd-run --user` service in code. surgical-cv's known-good form is a capped
  `--scope` (§4). A scope runs in the caller's process and would not detach as tmux does.
- **F8.** R731(b)'s standing wording "tmux wrapper included" (`STANDING_RULES.md:950`, `:956`) would go stale under a unit launch;
  its intent would not (§6).

## §10 What could not be established without launching a unit

- Whether a transient user service starts with the manager "degraded".
- A unit's actual cgroup, environment, `LimitNOFILE` and `oom_score_adj`, read from a running unit.
- Any effect of the launch on a gate's wall time (§7 1, 2, 5, 7).
- Which cgroup a tmux server started from Claude Code lands in (no server running).
- Whether the suite ever holds more than 1024 descriptors at once.
