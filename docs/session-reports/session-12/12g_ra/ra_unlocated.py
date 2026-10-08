"""ra_unlocated.py — every unlocated claim classified by 12g_RA-A.md's first-match procedure
(A-10 / INT-g6-1 / A-9 by sub-type / unexplained), and the same tests over every located
claim, because the locator compares NORMALISED text and so can hide the same mismatches
(the read-out's "located-by-FUZZY" clause, extended to normalised-exact). Writes
ra_unlocated.json and ra_unlocated_claims.csv. Read-only on the retained copy."""
import collections
import csv
import glob
import hashlib
import io
import json
import os
import re
import sys
import unicodedata

sys.path.insert(0, os.getcwd())
from ra_common import HERE, LABEL, LIVE_IDS, RUN_ID, db, retained, telemetry, write

from engine.core.locator import locate, normalize
from engine.elicitation.materialize import contiguous_runs
from engine.elicitation.units import strip_comments

conn = db()
tel = {t["paper_id"]: t for t in telemetry()}
um_dirs = glob.glob(os.path.join(retained(), "elicitation", "run_*"))
assert len(um_dirs) == 1, um_dirs           # one run, one directory: the join is by file name
EOL_HYPHEN = re.compile(r"(?<=[A-Za-z])-\n(?=[a-z])")
LIG = re.compile("[ﬀ-ﬆ]")


def a9_variants(raw):
    """Each A-9 repair applied to the raw text on its own."""
    return {
        "dehyphenated": EOL_HYPHEN.sub("", raw),
        "ligatures_folded": unicodedata.normalize("NFKC", raw),
        "soft_hyphens_removed": raw.replace("­", ""),
    }


texts = {}
for pid in range(1, 13):
    ref = conn.execute("SELECT parsed_text_version, parsed_text_sha256 FROM parsed_text_refs WHERE paper_id = ?", (pid,)).fetchone()
    data = open(os.path.join(retained(), "parsed_text", f"{pid}_v{ref['parsed_text_version']}.md"), "rb").read()
    assert hashlib.sha256(data).hexdigest() == ref["parsed_text_sha256"], pid
    raw = io.TextIOWrapper(io.BytesIO(data), encoding="utf-8").read()
    um = json.load(open(os.path.join(um_dirs[0], "unit_maps", f"{pid}.json")))
    texts[pid] = {"raw": raw, "stripped": strip_comments(raw), "units": um["units"], "sha": ref["parsed_text_sha256"],
                  "comments": raw.count("<!--"), "eol_hyphens": len(EOL_HYPHEN.findall(raw)), "ligatures": len(LIG.findall(raw))}

claims = conn.execute(
    "SELECT a.paper_id, a.field_name, a.claim_id, a.value, a.source_snippet, a.payload_json AS ap, l.payload_json AS lp "
    "FROM field_events a JOIN field_events l ON l.claim_id = a.claim_id AND l.event_type = 'citation_located' "
    "WHERE a.event_type = 'asserted' AND a.run_id = ? ORDER BY a.paper_id, a.event_id", (RUN_ID,)).fetchall()
verdicts = {r["claim_id"]: (r["verdict"], r["rationale"]) for r in conn.execute("SELECT claim_id, verdict, rationale FROM audit_verdicts")}

rows = []
for c in claims:
    pid, t = c["paper_id"], texts[c["paper_id"]]
    loc = json.loads(c["lp"])
    assert loc["parsed_text_sha256"] == t["sha"]
    snippet = c["source_snippet"] or ""
    e = tel[pid]["extra"]
    rec = e["fields"].get(c["field_name"], {})
    indices = rec.get("indices") or []
    runs = contiguous_runs(tuple(indices)) if indices else []
    first = runs[0] if runs else []
    units = [t["units"][i - 1] for i in first if 1 <= i <= len(t["units"])]
    rebuilt = " ".join(units).strip()
    raw_exact = bool(snippet) and snippet in t["raw"]
    stripped_exact = bool(snippet) and snippet in t["stripped"]
    units_not_in_stripped = [i for i, u in zip(first, units) if u not in t["stripped"]]
    units_not_in_raw = [i for i, u in zip(first, units) if u not in t["raw"]]
    # first-match classification (12g_RA-A.md section 4)
    if not snippet:
        cls = "no_snippet"
    elif raw_exact:
        cls = "raw_exact"
    elif stripped_exact:
        cls = "A-10_comment_adjacency"
    else:
        a9 = [k for k, v in a9_variants(t["raw"]).items() if snippet in v or normalize(snippet) in normalize(v)]
        # INT-g6-1 comes before A-9 in the procedure: not exact in the stripped text either
        if units_not_in_stripped:
            cls = "INT-g6-1_unit_rewritten"
        elif len(units) > 1:
            cls = "INT-g6-1_join_of_units_not_in_text"
        elif a9 and not loc["located"]:
            cls = "A-9_" + "+".join(a9)
        else:
            cls = "unexplained"
    ws = lambda x: re.sub(r"\s+", " ", x)
    ws_only = bool(snippet) and not raw_exact and ws(snippet) in ws(t["raw"])
    ws_only_stripped = bool(snippet) and not stripped_exact and ws(snippet) in ws(t["stripped"])
    a9_hit = [k for k, v in a9_variants(t["raw"]).items() if (not loc["located"]) and normalize(snippet) in normalize(v)] if snippet else []
    rows.append({
        "paper_id": pid, "live_paper_id": LIVE_IDS[pid - 1], "field_name": c["field_name"], "value": c["value"],
        "located": int(bool(loc["located"])), "kind": loc["kind"], "score": loc["score"],
        "snippet_supplied": int(bool(loc["snippet_supplied"])), "bridged": int(bool(loc["bridged"])),
        "snippet_chars": len(snippet), "cited_indices": " ".join(map(str, indices)), "first_run": " ".join(map(str, first)),
        "n_runs": len(runs), "snippet_equals_rebuilt_first_run": int(snippet == rebuilt),
        "raw_exact": int(raw_exact), "stripped_exact": int(stripped_exact),
        "units_in_first_run": len(units), "units_not_in_stripped": " ".join(map(str, units_not_in_stripped)),
        "units_not_in_raw": " ".join(map(str, units_not_in_raw)), "a9_repairs_that_locate": "+".join(a9_hit),
        "exact_after_whitespace_collapse_raw": int(ws_only),
        "exact_after_whitespace_collapse_stripped": int(ws_only_stripped),
        "class": cls, "state_at_write": json.loads(c["ap"]).get("state_at_write"),
        "audit_verdict": verdicts.get(c["claim_id"], ("", ""))[0], "snippet": snippet,
    })
with open(os.path.join(HERE, "ra_unlocated_claims.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

unloc = [r for r in rows if not r["located"]]
loc = [r for r in rows if r["located"]]


def tab(rs, key):
    return dict(collections.Counter(r[key] for r in rs).most_common())


per_paper = []
for pid in range(1, 13):
    rs = [r for r in rows if r["paper_id"] == pid]
    if not rs:
        continue
    t = texts[pid]
    per_paper.append({"paper_id": pid, "live_paper_id": LIVE_IDS[pid - 1], "claims": len(rs),
                      "unlocated": sum(not r["located"] for r in rs),
                      "located_not_raw_exact": sum(r["located"] and not r["raw_exact"] for r in rs),
                      "classes": tab(rs, "class"), "docling_comments": t["comments"],
                      "eol_hyphens": t["eol_hyphens"], "ligatures": t["ligatures"]})
summary = {
    "label": LABEL, "asserted_claims": len(rows), "located": len(loc), "unlocated": len(unloc),
    "locator_kind": tab(rows, "kind"), "state_at_write": tab(rows, "state_at_write"),
    "snippet_equals_rebuilt_first_run": sum(r["snippet_equals_rebuilt_first_run"] for r in rows),
    "claims_citing_more_than_one_run": sum(r["n_runs"] > 1 for r in rows),
    "unlocated_classes": tab(unloc, "class"),
    "unlocated_claims": [{k: r[k] for k in ("live_paper_id", "field_name", "value", "kind", "score", "class", "cited_indices",
                                           "first_run", "units_not_in_stripped", "a9_repairs_that_locate", "audit_verdict", "snippet")} for r in unloc],
    "located_claims": {"total": len(loc), "raw_exact": sum(r["raw_exact"] for r in loc),
                       "not_raw_exact": sum(not r["raw_exact"] for r in loc),
                       "not_raw_exact_classes": tab([r for r in loc if not r["raw_exact"]], "class")},
    "all_claims_classes": tab(rows, "class"),
    "would_fail_a_materialisation_time_EXACT_check_against_raw_text": sum(not r["raw_exact"] for r in rows if r["snippet_supplied"]),
    "would_fail_an_EXACT_check_against_comment_stripped_text": sum(not r["stripped_exact"] for r in rows if r["snippet_supplied"]),
    "not_raw_exact_but_exact_after_whitespace_collapse": {
        "against_raw": sum(r["exact_after_whitespace_collapse_raw"] for r in rows),
        "against_comment_stripped": sum(r["exact_after_whitespace_collapse_stripped"] for r in rows),
        "neither": sum((not r["raw_exact"]) and r["snippet_supplied"] and not r["exact_after_whitespace_collapse_raw"]
                       and not r["exact_after_whitespace_collapse_stripped"] for r in rows)},
    "per_paper": per_paper,
    "examples_located_not_raw_exact": [{k: r[k] for k in ("live_paper_id", "field_name", "class", "first_run", "units_not_in_stripped", "exact_after_whitespace_collapse_raw", "exact_after_whitespace_collapse_stripped", "snippet")}
                                       for r in loc if not r["raw_exact"]][:12],
}
write("ra_unlocated.json", summary)
print(json.dumps({k: v for k, v in summary.items() if k not in ("examples_located_not_raw_exact",)}, indent=1, sort_keys=True)[:6000])
