"""12j-REVIEW-W2 — lot assembly (W2-a). Read-only.
usage, from the repository root: lots_w2.py <12i_scope dir> <12i_review_w1/lots_w1.json> <out.json>
Takes lots L08–L14 from 12i_scope/lots.json; line counts and Run 7 reachability from
12i_scope/census.csv; grades from coverage.csv. R578: C counts as N (every unit of these lots is N
or C, so all are in); an F unit is in no lot by construction. I3: lists any unit also in Wave 1's
lots. L09 is split between two readers as 12i_scope proposed (readers est. 2).
"""
import csv, json, os, sys
scope, w1path, out = sys.argv[1:4]
lots = {l["lot"]: l for l in json.load(open(os.path.join(scope, "lots.json")))["lots"]}
cen = {r["unit"]: r for r in csv.DictReader(open(os.path.join(scope, "census.csv")))}
cov = {r["unit"]: r for r in csv.DictReader(open(os.path.join(scope, "coverage.csv")))}
w1 = json.load(open(w1path))
w1units = {u["unit"] for l in w1["lots"] for u in l["units"] + l["extra"]}
SPLIT = {"L09": [("L09a", "Human screening adjudication: the two adjudicators and the categorizer",
                  ["engine/adjudication/categorizer.py", "engine/adjudication/ft_screening_adjudicator.py",
                   "engine/adjudication/screening_adjudicator.py"]),
                 ("L09b", "Human screening adjudication: the two HTML generators",
                  ["engine/adjudication/abstract_adjudication_html.py", "engine/adjudication/ft_adjudication_html.py"])]}
res, seen = [], set()
for name in ["L08", "L09", "L10", "L11", "L12", "L13", "L14"]:
    l = lots[name]
    parts = SPLIT.get(name, [(name, l["title"], l["units"])])
    assert sorted(u for _, _, us in parts for u in us) == sorted(l["units"])
    for rid, title, units in parts:
        us = []
        for u in units:
            assert u not in seen; seen.add(u)
            n_disk = sum(1 for _ in open(u, encoding="utf-8"))
            us.append({"unit": u, "in_scope_lines": int(cen[u]["lines"]), "lines_on_disk_now": n_disk, "F": None,
                       "run7": cen[u].get("run7", cen[u].get("RUN7", "")), "grade_A": cov[u].get("A", ""), "grade_B": cov[u].get("B", ""),
                       "also_in_wave1": u in w1units})
        res.append({"lot": rid, "from_lot": name, "title": title, "units": us, "extra": [],
                    "lines": sum(x["in_scope_lines"] for x in us), "n_units": len(us)})
tot = {"readers": len(res), "units": len(seen), "lines": sum(r["lines"] for r in res),
       "units_also_in_wave1": sorted(u for u in seen if u in w1units),
       "line_count_changed_since_census": [x["unit"] for r in res for x in r["units"] if x["in_scope_lines"] != x["lines_on_disk_now"]]}
json.dump({"lots": res, "totals": tot, "census_columns": list(next(iter(cen.values())).keys()),
           "coverage_columns": list(next(iter(cov.values())).keys())}, open(out, "w"), indent=1)
print(json.dumps(tot, indent=1))
for r in res:
    print(r["lot"], r["n_units"], r["lines"], "|", r["title"])
