"""12p-U13A — which papers a --max-papers 12 run on an R235 copy of live would select. Read-only (mode=ro).
usage, from the repository root: u13a_live_first12.py data/surgical_autonomy/review.db   (stdout: JSON)
Selection order is effective.eligible_paper_ids (bound_selection takes to_extract[:N]); this
reports that order's first 12 and compares them with Rehearsal A's 12 live ids (12g_ra/id_map.json).
It does not run select_for_extraction (no arm, no reuse key): every live eligible paper is held
only by pre-manifest arms, which a new arm's selection does not see."""
import json, sqlite3, sys
sys.path.insert(0, ".")
from engine.core.effective import eligible_paper_ids
conn = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
ids = list(eligible_paper_ids(conn))
idmap = json.load(open("docs/session-reports/session-12/12g_ra/id_map.json"))
print(json.dumps({"eligible": len(ids), "first_12_by_order": ids[:12], "id_map_raw": idmap}, indent=1, default=str))
