"""12l-U01 gate 2 — the unit contract over every parsed file. Read-only: no model, no database.
usage, from the repository root:
    corpus_check.py data/surgical_autonomy/parsed_text <units_before.json>   (stdout: JSON)

For each *.md file builds the unit map and checks:
  V1  every unit's presented text is presented(raw[start:end]), and raw[start:end] is trimmed and non-empty;
  V2  units are ordered and do not overlap;
  V3  no unit overlaps a comment span (no comment text in any unit);
  V4  coverage — every non-whitespace character outside comment spans lies in exactly one unit;
  V5  every adjacent pair's materialised run is raw[start_i:end_{i+1}] and an exact substring of raw
      (all pairs).
Reported beside the gate, not part of it: for the first LOCATE_SAMPLE pairs of each file, what the
existing locator (locator-1, untouched here) returns for the snippet against the raw text — a sample,
because locate() re-normalises the whole paper on every call. A raw-exact snippet can come back
"fuzzy": locator-1's normalisation replaces a period glued between two word characters with a space,
so a snippet that ends or starts inside such a token ("tanner.a." cut from "tanner.a.watts@…")
normalises differently from the same characters inside the paper. Recorded as an R509 finding for U14.
<units_before.json> holds the per-file unit counts measured on the unmodified tree (40496003).
"""
import glob, hashlib, json, os, sys
from collections import Counter
sys.path.insert(0, os.getcwd())
from engine.core.locator import locate
from engine.elicitation import materialize as M
from engine.elicitation.contracts import FieldRecord
from engine.elicitation.units import (MIN_UNIT_TOKENS, UNITS_VERSION, build_unit_map,
                                      comment_spans, presented)

LOCATE_SAMPLE = 25
root, before_path = sys.argv[1:3]
before = json.load(open(before_path, encoding="utf-8"))
viol = Counter(); examples = {}; loc = Counter(); loc_examples = []
def bad(code, name, detail):
    viol[code] += 1; examples.setdefault(code, []).append(f"{name}: {detail}"[:300])

files = sorted(glob.glob(os.path.join(root, "*.md")))
per, stats = {}, {}
tot = Counter()
for path in files:
    name = os.path.basename(path)
    raw = open(path, encoding="utf-8").read()
    assert hashlib.sha256(raw.encode()).hexdigest() == before["files"][name]["sha256"], name
    st = {}
    um = build_unit_map(0, raw, stats=st)
    for k, v in st.items(): stats[k] = stats.get(k, 0) + v
    print(f"{len(per) + 1}/{len(files)} {name} units={um.n}", file=sys.stderr, flush=True)
    comments = comment_spans(raw)
    covered = bytearray(len(raw))
    prev = 0
    for i, ((s, e), shown) in enumerate(zip(um.spans, um.units), start=1):
        piece = raw[s:e]
        if not piece or piece != piece.strip() or shown != presented(piece): bad("V1", name, f"unit {i}")
        if s < prev or e <= s: bad("V2", name, f"unit {i} ({s},{e}) after {prev}")
        prev = e
        if any(cs < e and s < ce for cs, ce in comments): bad("V3", name, f"unit {i} overlaps a comment")
        for k in range(s, e): covered[k] += 1
    in_comment = bytearray(len(raw))
    for cs, ce in comments:
        for k in range(cs, ce): in_comment[k] = 1
    for k, ch in enumerate(raw):
        if in_comment[k]:
            if covered[k]: bad("V4", name, f"offset {k} is comment text inside a unit")
        elif not ch.isspace() and covered[k] != 1:
            bad("V4", name, f"offset {k} {ch!r} covered {covered[k]} times")
    pairs = exact = located = 0
    for i in range(1, um.n):
        rec = FieldRecord(field_name="f", field_class="stated", value="v", is_escape=False, indices=(i, i + 1),
                          inference="", steps=(), violations=(), bad_indices=(), duplicate_indices=())
        snip = M.source_snippet(rec, um, raw)
        pairs += 1
        if snip != raw[um.spans[i - 1][0]:um.spans[i][1]] or snip not in raw: bad("V5", name, f"pair {i},{i+1}")
        else:
            exact += 1
            if i <= LOCATE_SAMPLE:
                located += 1
                r = locate(raw, snip)
                loc[r.kind] += 1
                if r.kind != "exact":
                    loc_examples.append({"file": name, "pair": [i, i + 1], "kind": r.kind, "located": r.located,
                                         "score": r.score, "snippet_tail": snip[-40:]})
    short = sum(1 for s, e in um.spans if len(raw[s:e].split()) < MIN_UNIT_TOKENS)
    per[name] = {"before": before["files"][name]["n_units"], "after": um.n, "comments": len(comments),
                 "short_units": short, "pairs": pairs}
    tot.update(units=um.n, pairs=pairs, pairs_exact=exact, pairs_located=located, comments=len(comments), short_units=short,
               single_units_exact=sum(1 for s, e in um.spans if raw[s:e] in raw))

changed = {n: v for n, v in per.items() if v["before"] != v["after"]}
largest = max(changed.items(), key=lambda kv: abs(kv[1]["after"] - kv[1]["before"])) if changed else None
print(json.dumps({
    "root": root, "units_version": UNITS_VERSION, "files": len(files),
    "units_before": sum(v["before"] for v in per.values()), "units_after": tot["units"],
    "files_whose_unit_count_changed": len(changed),
    "files_with_more_units": sum(1 for v in changed.values() if v["after"] > v["before"]),
    "files_with_fewer_units": sum(1 for v in changed.values() if v["after"] < v["before"]),
    "largest_change": {"file": largest[0], **largest[1]} if largest else None,
    "comment_spans": tot["comments"], "adjacent_pairs": tot["pairs"], "adjacent_pairs_exact": tot["pairs_exact"],
    "locator_1_on_a_sample": {"sample_per_file": LOCATE_SAMPLE, "pairs": tot["pairs_located"], "kind": dict(loc),
                              "not_located": sum(1 for x in loc_examples if not x["located"]),
                              "not_exact_examples": loc_examples},
    "units_shorter_than_min_tokens": tot["short_units"],
    "files_with_a_short_unit": sum(1 for v in per.values() if v["short_units"]),
    "segmenter_alignment_fallbacks": stats,
    "violations": {k: viol.get(k, 0) for k in ("V1", "V2", "V3", "V4", "V5")},
    "violation_examples": {k: v[:5] for k, v in examples.items()},
    "per_file": per,
}, indent=0, sort_keys=True))
sys.exit(1 if sum(viol.values()) else 0)
