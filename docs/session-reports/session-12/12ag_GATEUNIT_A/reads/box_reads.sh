#!/bin/bash
# 12ag-GATEUNIT-A Q3 — this box's user manager, read by show / status / list / cat forms only (and crontab -l).
# Nothing is started, stopped, enabled or set. Environment values are printed only for the allow-list below; every
# other variable is listed by name, its value withheld (a variable may carry a credential).
ALLOW='^(PATH|HOME|USER|LOGNAME|SHELL|LANG|LC_[A-Z]+|LANGUAGE|TMPDIR|TMP|TEMP|XDG_RUNTIME_DIR|XDG_[A-Z_]+|PWD|VIRTUAL_ENV|PYTHON[A-Z_]*|PYTEST[A-Z_]*|OLLAMA_HOST|EVIDENCE_ENGINE_[A-Z_]*|TERM|TMUX|TZ|DBUS_SESSION_BUS_ADDRESS|MANAGERPID|INVOCATION_ID|SYSTEMD_EXEC_PID|JOURNAL_STREAM)='
r() { echo; echo "\$ $*"; "$@" 2>&1; echo "[exit $?]"; }
filt() { while IFS= read -r l; do if [[ "$l" =~ $ALLOW ]]; then echo "$l"; else echo "${l%%=*}=<value withheld>"; fi; done; }
echo "# captured $(date -u +%FT%T.%NZ) on $(hostname)"
r systemctl --version
r systemctl --user is-system-running
r systemctl --user status --no-pager
r loginctl show-user "$USER"
r loginctl list-sessions --no-pager
echo; echo "\$ systemctl --user show-environment | (allow-list filter)"; systemctl --user show-environment 2>&1 | filt | sort; echo "[exit ${PIPESTATUS[0]}]"
echo; echo "\$ env | (allow-list filter)   # this Claude Code session's Bash tool"; env | filt | sort
r systemctl --user show -p DefaultLimitNOFILE -p DefaultLimitNOFILESoft -p DefaultTasksMax -p DefaultMemoryAccounting -p DefaultCPUAccounting -p DefaultIOAccounting -p DefaultTimeoutStopUSec -p DefaultStandardOutput -p DefaultStandardError -p Environment
r systemctl show "user@$(id -u).service" -p MemoryMax -p MemoryHigh -p CPUWeight -p IOWeight -p TasksMax -p Delegate -p OOMScoreAdjust -p ManagedOOMMemoryPressure -p LimitNOFILE -p Slice -p ControlGroup
r systemctl show "user-$(id -u).slice" -p MemoryMax -p MemoryHigh -p CPUWeight -p IOWeight -p TasksMax
r systemctl --user show app.slice -p MemoryMax -p MemoryHigh -p CPUWeight -p IOWeight -p TasksMax -p ControlGroup
r systemctl --user show background.slice -p MemoryMax -p CPUWeight -p IOWeight -p ControlGroup
r systemctl --user list-units --all --no-pager
r systemctl --user list-unit-files --no-pager
r systemctl --user list-timers --all --no-pager
r systemctl list-timers --all --no-pager
r crontab -l
echo; echo "\$ cat /proc/self/cgroup; cat /proc/\$PPID/cgroup   # this shell and its parent"; cat /proc/self/cgroup; cat /proc/$PPID/cgroup
echo; echo "\$ ulimit -a   # this shell"; ulimit -a
echo; echo "\$ cat /proc/self/oom_score_adj; nice"; cat /proc/self/oom_score_adj; nice
r ps -o pid=,args= -C earlyoom
r systemctl cat earlyoom --no-pager
r cat /etc/default/earlyoom
echo; echo "\$ tmux list-sessions; for each tmux server pid: its cgroup   # read only"; tmux list-sessions 2>&1; for p in $(pgrep -x tmux 2>/dev/null; pgrep -f '^tmux' 2>/dev/null | sort -u); do echo "tmux pid $p: $(cat /proc/$p/cgroup 2>/dev/null) oom_score_adj $(cat /proc/$p/oom_score_adj 2>/dev/null) nice $(ps -o ni= -p $p)"; done
echo; echo "\$ grep -rn 'systemd-run' over ~/projects ~/claude-config ~/scripts/ (*.sh *.py *.md *.service, docs/session-reports excluded)"
grep -rnI --include=*.sh --include=*.py --include=*.md --include=*.service --include=*.timer -e 'systemd-run' ~/projects ~/claude-config ~/scripts/ 2>/dev/null | grep -v '/docs/session-reports/' | grep -v '/.git/' | grep -v '/node_modules/' | grep -v '/.venv/' | cut -c1-300
echo "[end]"
