#!/bin/bash
# 12y-U18A 3a, second pattern: module-level ollama calls (the library's default client), _client uses, Client host reads. Read-only.
cd ~/projects/evidence-engine
grep -rnE --include='*.py' '\bollama\.(chat|generate|show|ps|list|pull|embed|embeddings|create|delete|copy|push)\(|_client\.(chat|show|ps|list|generate|_client)|_client_host|\.base_url|_client\b *= ' engine scripts analysis
