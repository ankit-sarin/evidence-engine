"""12p-U13A — the keys at which ra_12g's pinned arm differs from the tuple HEAD resolves. Read-only.
usage, from the repository root: u13a_pin_diff.py <ra_12g review.db> <pin_tuples_throwaway.json>   (stdout: JSON)
The db is opened mode=ro (run on a byte copy, so the retained copy's -shm is not touched). The key
list is run_manifest._diff_keys, the function ArmPinMismatch names its keys with."""
import json, sqlite3, sys
sys.path.insert(0, ".")
from engine.core.run_manifest import _diff_keys
from engine.core.effective_config import sha256_canonical
c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
name, cfg, sha = c.execute("SELECT arm_name, configuration_json, pinned_sha256 FROM arms").fetchone()
head = json.load(open(sys.argv[2]))["elicited"]
old = json.loads(cfg)
print(json.dumps({"arm": name, "pinned_sha256": sha, "recomputed_from_stored_tuple": sha256_canonical(old),
                  "head_pin": head["pin"], "differs_at": _diff_keys(old, head["tuple"])}, indent=1))
