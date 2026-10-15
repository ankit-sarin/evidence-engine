"""12p-U13A — does the paper text the elicited Pass 1 is shown differ between 12g and HEAD? Read-only.
usage, from the repository root: u13a_units.py <12g input dir> <ra_12g dir>   (stdout: JSON)
For each of Rehearsal A's 12 texts (12g_ra/id_map.json; each file's sha256 checked against it):
units at HEAD (engine.elicitation.units.build_unit_map) against the unit map 12g persisted
(<ra_12g>/elicitation/run_20261008T182516Z/unit_maps/<throwaway id>.json): unit counts, how many
units are identical at the same index, the first differing index, and whether the numbered text
the model is shown (UnitMap.render's form, "[S<i>] <unit>" joined by newlines) is byte-equal."""
import hashlib, json, os, sys
sys.path.insert(0, os.getcwd())
from engine.elicitation.units import build_unit_map, UNITS_VERSION, MIN_UNIT_TOKENS
inp, ra = sys.argv[1], sys.argv[2]
idmap = json.load(open("docs/session-reports/session-12/12g_ra/id_map.json"))
out = []
for e in idmap:
    raw = open(os.path.join(inp, e["text_path"]), "rb").read()
    assert hashlib.sha256(raw).hexdigest() == e["sha256"], e["text_path"]
    text = raw.decode("utf-8")
    head = build_unit_map(e["throwaway_paper_id"], text)
    old = json.load(open(os.path.join(ra, "elicitation", "run_20261008T182516Z", "unit_maps",
                                      f"{e['throwaway_paper_id']}.json")))
    ou, nu = old["units"], list(head.units)
    same = sum(1 for a, b in zip(ou, nu) if a == b)
    first = next((i + 1 for i, (a, b) in enumerate(zip(ou, nu)) if a != b), None)
    render_old = "\n".join(f"[S{i}] {u}" for i, u in enumerate(ou, start=1))
    out.append({"throwaway_id": e["throwaway_paper_id"], "live_id": e["live_paper_id"],
                "units_12g": old["n_units"], "units_head": head.n, "identical_at_index": same,
                "first_differing_index": first, "render_equal": render_old == head.render(),
                "render_chars_12g": len(render_old), "render_chars_head": len(head.render()),
                "version_12g": old.get("version"), "version_head": UNITS_VERSION})
print(json.dumps({"min_unit_tokens_head": MIN_UNIT_TOKENS, "papers": out,
                  "papers_render_equal": sum(p["render_equal"] for p in out),
                  "papers_same_unit_count": sum(p["units_12g"] == p["units_head"] for p in out)}, indent=1))
