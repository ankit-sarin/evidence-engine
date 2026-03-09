#!/usr/bin/env bash
DB="/home/ankitsarin/projects/evidence-engine/data/surgical_autonomy/review.db"

while true; do
    count=$(sqlite3 "$DB" "SELECT COUNT(*) FROM papers WHERE status='AI_AUDIT_COMPLETE';")
    ts=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$ts] AI_AUDIT_COMPLETE: $count / 96"
    sqlite3 "$DB" "SELECT status, COUNT(*) FROM papers GROUP BY status;" | while IFS='|' read -r status cnt; do
        echo "  $status: $cnt"
    done
    if [ "$count" -eq 96 ]; then
        echo ""
        echo "============================================"
        echo "  RUN 4 COMPLETE at $ts"
        echo "============================================"
        break
    fi
    sleep 300
done
