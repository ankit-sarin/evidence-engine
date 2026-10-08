"""12g-MEAS M2 (R536) — did the live corpus's OpenAlex retrieval hit pyalex's 10,000 cap?

Read-only: live review.db opened mode=ro; the stored search files read, never written.
No network call. Run from the repository root:

    .venv/bin/python <this file> <out_dir>

Writes <out_dir>/m2_summary.json. Every count in the read-out's M2 section is in it.
The files are the ones migration 003 names (EXPANDED_DIR) plus the rest of that directory.
"""
import collections
import csv
import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone

OUT = sys.argv[1]
DB = "data/surgical_autonomy/review.db"
DIR = "data/surgical_autonomy/expanded_search"
csv.field_size_limit(10 ** 9)

files = {}
for name in sorted(os.listdir(DIR)):
    p = os.path.join(DIR, name)
    data = open(p, "rb").read()
    files[name] = {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data),
                   "mtime_utc": datetime.fromtimestamp(os.path.getmtime(p), timezone.utc).isoformat()}


def rows(name):
    return list(csv.DictReader(open(os.path.join(DIR, name), newline="")))


def by_source(rs):
    return dict(collections.Counter(r.get("source", "") for r in rs))


stats = json.load(open(os.path.join(DIR, "stats.json")))
results = rows("expanded_search_results.csv")
net_new = rows("net_new_papers.csv")
screening = rows("screening_results.csv")
verification = rows("verification_results.csv")
abstracts = [json.loads(l) for l in open(os.path.join(DIR, "abstracts.jsonl"))]

for name, rs in (("expanded_search_results.csv", results), ("net_new_papers.csv", net_new),
                 ("screening_results.csv", screening), ("verification_results.csv", verification),
                 ("abstracts.jsonl", abstracts)):
    files[name].update(records=len(rs), by_source=by_source(rs), columns=sorted(rs[0].keys()))
files["rescreen_original_251.csv"].update(records=len(rows("rescreen_original_251.csv")))
files["stats.json"]["content"] = stats

# any field anywhere that could carry an advertised total or pagination state
advert_keys = [k for k in stats if any(t in k.lower() for t in ("count", "meta", "advertis", "cursor", "page"))]
all_cols = set()
for name in files:
    all_cols |= set(files[name].get("columns", []))
advert_cols = sorted(c for c in all_cols if any(t in c.lower() for t in ("count", "meta", "cursor", "page", "total")))

oa = [r for r in results if r["source"] == "openalex"]
pm = [r for r in results if r["source"] == "pubmed"]

conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
live = conn.execute("SELECT id, doi, pmid, title, source, created_at FROM papers").fetchall()
live_by_source = dict(collections.Counter(r["source"] for r in live))
doi_map, pmid_map, title_map = {}, {}, {}
for r in live:  # the same three keys, in the same order, as migration 003's _find_paper
    if r["doi"]:
        doi_map[r["doi"].strip().lower()] = r
    if r["pmid"]:
        pmid_map[r["pmid"].strip()] = r
    if r["title"]:
        title_map[r["title"].strip().lower()] = r


def find(rec):
    d, p, t = rec.get("doi", "").strip(), rec.get("pmid", "").strip(), rec.get("title", "").strip()
    return (doi_map.get(d.lower()) if d else None) or (pmid_map.get(p) if p else None) or \
           (title_map.get(t.lower()) if t else None)


def trace(rs):
    out = {"records": len(rs), "found_on_live": 0, "not_found_on_live": 0, "distinct_live_ids": 0,
           "live_source_of_found": {}, "file_source_vs_live_source": {}}
    ids = set()
    for rec in rs:
        hit = find(rec)
        if hit is None:
            out["not_found_on_live"] += 1
            continue
        out["found_on_live"] += 1
        ids.add(hit["id"])
        out["live_source_of_found"][hit["source"]] = out["live_source_of_found"].get(hit["source"], 0) + 1
        k = f"{rec['source']}->{hit['source']}"
        out["file_source_vs_live_source"][k] = out["file_source_vs_live_source"].get(k, 0) + 1
    out["distinct_live_ids"] = len(ids)
    return out, ids


t_results, ids_results = trace(results)
t_netnew, ids_netnew = trace(net_new)
t_oa, ids_oa = trace(oa)
not_from_files = [r for r in live if r["id"] not in ids_results]
created_days = dict(collections.Counter(r["created_at"][:10] for r in live))

summary = {
    "generated_utc": datetime.now(timezone.utc).isoformat(),
    "files": files,
    "stats_json": stats,
    "arithmetic": {
        "pubmed_total + openalex_total": stats["pubmed_total"] + stats["openalex_total"],
        "minus duplicates_removed": stats["pubmed_total"] + stats["openalex_total"] - stats["duplicates_removed"],
        "equals unique_total": stats["pubmed_total"] + stats["openalex_total"] - stats["duplicates_removed"] == stats["unique_total"],
        "results_csv_records == unique_total": len(results) == stats["unique_total"],
        "results_csv pubmed == pubmed_total": len(pm) == stats["pubmed_total"],
        "openalex_total - results_csv openalex": stats["openalex_total"] - len(oa),
        "== duplicates_removed": stats["openalex_total"] - len(oa) == stats["duplicates_removed"],
        "unique_total - overlap_with_existing == net_new": stats["unique_total"] - stats["overlap_with_existing"] == stats["net_new"],
        "net_new_csv_records == net_new": len(net_new) == stats["net_new"],
        "openalex_total mod 200 (per-page size)": stats["openalex_total"] % 200,
        "openalex_total < 10000": stats["openalex_total"] < 10000,
        "10000 - openalex_total": 10000 - stats["openalex_total"],
    },
    "advertised_total_fields_in_stats_json": advert_keys,
    "advertised_total_or_pagination_columns_in_any_file": advert_cols,
    "openalex_records_in_results_csv": {
        "records": len(oa), "empty_title": sum(not r["title"].strip() for r in oa),
        "empty_doi": sum(not r["doi"].strip() for r in oa), "with_pmid": sum(bool(r["pmid"].strip()) for r in oa),
        "year_min": min(r["year"] for r in oa if r["year"]), "year_max": max(r["year"] for r in oa if r["year"]),
    },
    "live": {"papers": len(live), "by_source": live_by_source, "created_at_by_day": created_days},
    "trace_results_csv_to_live": t_results,
    "trace_net_new_csv_to_live": t_netnew,
    "trace_openalex_rows_of_results_csv_to_live": t_oa,
    "live_papers_matching_no_results_csv_row": {
        "count": len(not_from_files), "by_source": dict(collections.Counter(r["source"] for r in not_from_files)),
        "ids_le_251": sum(r["id"] <= 251 for r in not_from_files),
        "ids": [r["id"] for r in not_from_files][:60],
    },
    "live_ids_le_251_by_source": dict(collections.Counter(r["source"] for r in live if r["id"] <= 251)),
    "live_ids_gt_251_by_source": dict(collections.Counter(r["source"] for r in live if r["id"] > 251)),
}
json.dump(summary, open(os.path.join(OUT, "m2_summary.json"), "w"), indent=1, sort_keys=True)
s = dict(summary)
s["files"] = {k: {x: v[x] for x in v if x in ("records", "by_source", "sha256")} for k, v in files.items()}
print(json.dumps(s, indent=1, sort_keys=True))
