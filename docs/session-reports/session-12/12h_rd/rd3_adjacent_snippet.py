"""12h RD-3, adjacent finding — what a reader gets from provenance["located"].
Read-only (mode=ro) on the retained ra_12g review. usage, from the repository root:
    rd3_adjacent_snippet.py <review.db>   (stdout: JSON)"""
import json, os, sqlite3, sys
from collections import Counter
sys.path.insert(0, os.getcwd())
from engine.core.effective import effective_value
assert not any(m == "ollama" or m.startswith("ollama.") for m in sys.modules), "ollama imported"

c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
cells = c.execute("SELECT DISTINCT paper_id, field_name, arm FROM field_events "
                  "WHERE event_type = 'asserted' ORDER BY 1, 2").fetchall()
keys = Counter(); with_snip = 0; located_dict = 0; states = Counter()
for pid, field, arm in cells:
    ev = effective_value(c, pid, field, arm, sentinels=frozenset())
    states[ev.state] += 1
    loc = ev.provenance.get("located")
    if isinstance(loc, dict):
        located_dict += 1
        keys.update(loc.keys())
        with_snip += bool(loc.get("snippet"))
print(json.dumps({"cells_with_an_asserted_event": len(cells), "derived_state": dict(states),
                  "provenance_located_is_a_dict": located_dict,
                  "provenance_located_keys": dict(sorted(keys.items())),
                  "provenance_located_with_a_nonempty_snippet": with_snip},
                 indent=1, sort_keys=True))
