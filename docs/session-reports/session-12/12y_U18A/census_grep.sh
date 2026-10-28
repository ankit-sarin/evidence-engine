#!/bin/bash
# 12y-U18A 3a: declared grep census (the AST inventory has no symbol query). Read-only.
cd ~/projects/evidence-engine
PAT='OLLAMA_HOST|ollama\.Client\(|Client\(host|AsyncClient\(|11434|11435|\b_client\s*=|_client\b.*=\s*ollama|OLLAMA_BASE_URL|httpx\.(get|post|Client|request)|requests\.(get|post)|urllib\.request|/api/(tags|show|ps|version|generate|chat)|localhost|127\.0\.0\.1|_LOCAL_SERVICE_HOSTS'
grep -rnE --include='*.py' "$PAT" engine scripts analysis tests
