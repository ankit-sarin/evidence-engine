#!/bin/bash
# 12g Rehearsal A checkpoint: read-only. Waits (bounded) on the run manifest's end_status,
# then prints the state. Usage: checkpoint.sh <max_wait_seconds>
cd ~/projects/evidence-engine
DB="file:data/ra_12g/review.db?mode=ro"
MAX=${1:-0}; t=0; outcome=TIMEOUT
while [ $t -lt $MAX ]; do
  s=$(sqlite3 "$DB" "select coalesce(end_status,'') from run_manifests where run_id=2" 2>/dev/null)
  [ -n "$s" ] && { outcome=CLOSED; break; }
  sleep 20; t=$((t+20))
done
[ "$MAX" = 0 ] && outcome=SNAPSHOT
echo "wait outcome: $outcome after ${t}s | now $(date -u +%FT%TZ) | launched $(cat ~/scratch/12g-ra/launch_utc.txt)"
sqlite3 "$DB" "select 'manifest', run_id, coalesce(end_status,'OPEN'), coalesce(end_reason,''), coalesce(ended_at,'') from run_manifests where run_id=2;
select 'paper_events', to_state, coalesce(json_extract(payload_json,'\$.reason_code'),reason,''), count(*), group_concat(paper_id) from paper_events where run_id=2 group by 2,3;
select 'run_calls', stage, outcome, count(*) from run_calls where run_id=2 group by 2,3;
select 'last_call', stage, paper_id, outcome, started_at, ended_at from run_calls where run_id=2 order by call_id desc limit 1;
select 'field_events', event_type, count(*) from field_events where run_id=2 group by 2;"
echo "stage: $(grep -a 'STAGE:' ~/scratch/12g-ra/run.log | tail -1 | cut -c1-80)"
echo "flags: undeclared=$(grep -a -c -E 'UndeclaredCall|UndeclaredOverride' ~/scratch/12g-ra/run.log) truncated_or_dropped=$(grep -a -c -E 'input_fit (TRUNCATED|DROPPED)' ~/scratch/12g-ra/run.log) refused=$(grep -a -c 'input_fit REFUSED' ~/scratch/12g-ra/run.log) timeouts=$(grep -a -c -i 'TimeoutError\|watchdog' ~/scratch/12g-ra/run.log) restarts_logged=$(grep -a -c -i 'systemctl restart\|Ollama restart' ~/scratch/12g-ra/run.log) tracebacks=$(grep -a -c 'Traceback' ~/scratch/12g-ra/run.log)"
systemctl show ollama --property=NRestarts --property=ActiveEnterTimestamp --no-pager | tr '\n' ' '; echo
ollama ps | tail -n +2
tmux ls 2>&1 | head -2
tail -3 ~/scratch/12g-ra/run.log | cut -c1-200
