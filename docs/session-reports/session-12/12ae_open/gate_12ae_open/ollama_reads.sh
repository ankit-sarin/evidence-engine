#!/bin/bash
# Ollama version / tags / ps reads (R660(b)), outside any gate window.
for ep in version tags ps; do
  echo "$(date -u +%FT%T.%NZ) /api/$ep"
  curl -s --max-time 10 http://localhost:11434/api/$ep | python3 -c '
import json,sys
d=json.load(sys.stdin); ep=sys.argv[1]
if ep=="tags": d={m["name"]:m["digest"] for m in d.get("models",[]) if m["name"] in ("gemma3:27b","qwen3:32b","deepseek-r1:32b")}
print(json.dumps(d))' $ep
done
