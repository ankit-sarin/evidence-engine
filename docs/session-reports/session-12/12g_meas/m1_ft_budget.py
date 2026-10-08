"""12g-MEAS M1 (R516) — full-text decisions made on input longer than the FT budget.

Read-only: live review.db opened mode=ro; parsed-text files read, never written.
No model call, no network call. Run from the repository root:

    .venv/bin/python <this file> <out_dir>

Writes <out_dir>/12g_ft_budget_versions.csv (one row per paper x parsed-text file on
disk), <out_dir>/12g_ft_budget_decisions.csv (one row per FT decision row) and
<out_dir>/m1_summary.json. Every count in the read-out's M1 section is in m1_summary.json.

The quantity measured is the one `truncate_paper_text` tests (engine/agents/ft_screener.py):
it cuts when  len(full_text) > max_chars - len(header),  header = "Title: …\\n\\n" +
"Abstract: …\\n\\n", max_chars = FT_MAX_TEXT_CHARS. `full_text` is the parsed file decoded
as UTF-8 with universal newlines (Path.read_text() in March 2026; read_parsed_text today).
R516's wording — "parsed text exceeds the 32,000-character budget" — is len(full_text) >
32,000, a narrower test; both are reported.

Version rule. In March 2026 the loader read `sorted(glob("{id}_v*.md"), reverse=True)[0]`.
The version a decision read is therefore the reverse-sorted first of the files that existed
at `decided_at`: those whose mtime <= decided_at. If no file on disk predates the decision,
or the chosen file's full_text_assets.parsed_at is later than the decision, the paper is
UNDECIDABLE for that decision.
"""
import csv
import glob
import hashlib
import io
import json
import os
import re
import sqlite3
import statistics
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.getcwd())
from engine.agents.ft_screener import truncate_paper_text  # the function under measurement
from engine.core.constants import FT_MAX_TEXT_CHARS
from engine.core.effective import eligible_paper_ids

OUT = sys.argv[1]
DB = "data/surgical_autonomy/review.db"
PDIR = "data/surgical_autonomy/parsed_text"
REF_RE = re.compile(r"^(?:#{1,4}\s+)?(?:references|bibliography|acknowledgements?)\b",
                    re.IGNORECASE | re.MULTILINE)  # copied from truncate_paper_text, to label the cut only

conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row


def ts(s):
    d = datetime.fromisoformat(s)
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


papers = {r["id"]: r for r in conn.execute("SELECT id, title, abstract, status FROM papers")}
fta = {}
for r in conn.execute("SELECT * FROM full_text_assets WHERE parsed_text_path IS NOT NULL"):
    fta.setdefault((r["paper_id"], os.path.basename(r["parsed_text_path"])), []).append(r)
refs = {(r["paper_id"], os.path.basename(r["parsed_text_path"])): r
        for r in conn.execute("SELECT * FROM parsed_text_refs")}

ft_out = sorted(r[0] for r in conn.execute("SELECT id FROM papers WHERE status = 'FT_SCREENED_OUT'"))
eligible = sorted(eligible_paper_ids(conn))
primary = conn.execute("SELECT id, paper_id, model, decision, reason_code, decided_at "
                       "FROM ft_screening_decisions ORDER BY id").fetchall()
verifier = conn.execute("SELECT id, paper_id, model, decision, decided_at "
                        "FROM ft_verification_decisions ORDER BY id").fetchall()
adjud = {r["paper_id"]: r for r in conn.execute("SELECT * FROM ft_screening_adjudication")}

scope = sorted(set(ft_out) | set(eligible) | {r["paper_id"] for r in primary} | {r["paper_id"] for r in verifier})

_cache = {}


def versions(pid):
    """Every parsed-text file on disk for the paper, reverse-sorted as the March loader sorted."""
    if pid in _cache:
        return _cache[pid]
    p = papers[pid]
    header = ""
    if p["title"]:
        header += f"Title: {p['title']}\n\n"
    if p["abstract"]:
        header += f"Abstract: {p['abstract']}\n\n"
    out = []
    for path in sorted(glob.glob(os.path.join(PDIR, f"{pid}_v*.md")), reverse=True):
        base = os.path.basename(path)
        rec = {"paper_id": pid, "status": p["status"], "file": path, "readable": 1, "error": ""}
        try:
            data = open(path, "rb").read()
            text = io.TextIOWrapper(io.BytesIO(data), encoding="utf-8").read()
        except Exception as e:  # unreadable is counted, never dropped
            rec.update(readable=0, error=f"{type(e).__name__}: {e}")
            out.append(rec)
            continue
        remaining = FT_MAX_TEXT_CHARS - len(header)
        sent = truncate_paper_text(text, title=p["title"] or "", abstract=p["abstract"] or "")
        over_code = len(text) > remaining
        m = REF_RE.search(text) if over_code else None
        cut = "none" if not over_code else ("references_line" if (m and m.start() <= remaining) else "prefix")
        a = fta.get((pid, base), [])
        rec.update(
            sha256=hashlib.sha256(data).hexdigest(), bytes=len(data), text_chars=len(text),
            header_chars=len(header), remaining_budget=remaining,
            over_budget_code=int(over_code), over_32000_text=int(len(text) > FT_MAX_TEXT_CHARS),
            cut=cut, body_chars_sent=len(sent) - len(header) if remaining > 0 else 0,
            body_chars_omitted=len(text) - (len(sent) - len(header)) if remaining > 0 else len(text),
            mtime_utc=datetime.fromtimestamp(os.path.getmtime(path), timezone.utc).isoformat(),
            fta_rows=len(a), fta_parsed_at=";".join(x["parsed_at"] or "" for x in a),
            fta_version=";".join(str(x["parsed_text_version"]) for x in a),
            fta_parser=";".join(x["parser_used"] or "" for x in a),
            has_ref_row=int((pid, base) in refs),
        )
        out.append(rec)
    _cache[pid] = out
    return out


def read_at(pid, decided_at):
    """(version record or None, how) under the version rule in the module docstring."""
    vs = versions(pid)
    if not vs:
        return None, "MISSING_no_file_on_disk"
    if any(not v["readable"] for v in vs):
        return None, "UNREADABLE"
    d = ts(decided_at)
    then = [v for v in vs if ts(v["mtime_utc"]) <= d]
    if not then:
        return None, "UNDECIDABLE_no_file_predates_decision"
    v = then[0]
    if any(x and ts(x) > d for x in v["fta_parsed_at"].split(";")):
        return None, "UNDECIDABLE_parsed_at_after_decision"
    how = "only_file" if len(vs) == 1 else f"latest_of_{len(then)}_existing_at_decision_({len(vs)}_on_disk)"
    return v, how


# ---- per-version CSV
vcols = ["paper_id", "status", "file", "readable", "error", "sha256", "bytes", "text_chars", "header_chars",
         "remaining_budget", "over_budget_code", "over_32000_text", "cut", "body_chars_sent",
         "body_chars_omitted", "mtime_utc", "fta_rows", "fta_parsed_at", "fta_version", "fta_parser",
         "has_ref_row", "in_ft_screened_out", "in_eligible"]
with open(os.path.join(OUT, "12g_ft_budget_versions.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=vcols)
    w.writeheader()
    for pid in scope:
        vs = versions(pid)
        if not vs:
            w.writerow({"paper_id": pid, "status": papers[pid]["status"], "file": "", "readable": 0,
                        "error": "no parsed-text file on disk",
                        "in_ft_screened_out": int(pid in ft_out), "in_eligible": int(pid in eligible)})
        for v in vs:
            w.writerow({**v, "in_ft_screened_out": int(pid in ft_out), "in_eligible": int(pid in eligible)})

# ---- per-decision CSV
dcols = ["table", "decision_id", "paper_id", "paper_status", "model", "decision", "reason_code", "decided_at",
         "version_file", "version_rule", "text_chars", "header_chars", "over_budget_code", "over_32000_text",
         "cut", "body_chars_omitted", "files_on_disk", "any_version_over_code", "pi_adjudication"]
drows = []
for table, rows in (("ft_screening_decisions", primary), ("ft_verification_decisions", verifier)):
    for r in rows:
        pid = r["paper_id"]
        v, how = read_at(pid, r["decided_at"])
        vs = versions(pid)
        drows.append({
            "table": table, "decision_id": r["id"], "paper_id": pid, "paper_status": papers[pid]["status"],
            "model": r["model"], "decision": r["decision"],
            "reason_code": r["reason_code"] if "reason_code" in r.keys() else "",
            "decided_at": r["decided_at"], "version_file": v["file"] if v else "", "version_rule": how,
            "text_chars": v["text_chars"] if v else "", "header_chars": v["header_chars"] if v else "",
            "over_budget_code": v["over_budget_code"] if v else "",
            "over_32000_text": v["over_32000_text"] if v else "",
            "cut": v["cut"] if v else "", "body_chars_omitted": v["body_chars_omitted"] if v else "",
            "files_on_disk": len(vs),
            "any_version_over_code": int(any(x.get("over_budget_code") for x in vs)),
            "pi_adjudication": adjud[pid]["adjudication_decision"] if pid in adjud else "",
        })
with open(os.path.join(OUT, "12g_ft_budget_decisions.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=dcols)
    w.writeheader()
    w.writerows(drows)

# ---- M1 b/c: the 171 FT_SCREENED_OUT papers, each placed in exactly one bucket
prim_by_paper = {}
for r in primary:
    prim_by_paper.setdefault(r["paper_id"], []).append(r)
ver_by_paper = {r["paper_id"]: r for r in verifier}


def bucket(pid):
    """One bucket per paper, from the paper's last primary decision row."""
    rows = prim_by_paper.get(pid)
    if not rows:
        return "NO_PRIMARY_DECISION_ROW", None
    v, how = read_at(pid, rows[-1]["decided_at"])
    if v is None:
        return how.split("_")[0], None  # MISSING / UNREADABLE / UNDECIDABLE
    return ("OVER" if v["over_budget_code"] else "UNDER"), v


def tally(ids):
    t = {"papers": len(ids), "OVER": 0, "UNDER": 0, "UNDECIDABLE": 0, "MISSING": 0, "UNREADABLE": 0,
         "NO_PRIMARY_DECISION_ROW": 0, "over_32000_text_among_OVER_or_UNDER": 0,
         "over_any_version_on_disk": 0, "over_no_version_on_disk": 0, "no_readable_version": 0,
         "cut_prefix": 0, "cut_references_line": 0, "files_on_disk": {}, "over_ids": [],
         "over_only_with_header_ids": [], "undecidable_ids": []}
    lens = []
    for pid in ids:
        b, v = bucket(pid)
        t[b] += 1
        vs = [x for x in versions(pid) if x["readable"]]
        k = str(len(versions(pid)))
        t["files_on_disk"][k] = t["files_on_disk"].get(k, 0) + 1
        if not vs:
            t["no_readable_version"] += 1
        elif any(x["over_budget_code"] for x in vs):
            t["over_any_version_on_disk"] += 1
        else:
            t["over_no_version_on_disk"] += 1
        if b == "UNDECIDABLE":
            t["undecidable_ids"].append(pid)
        if v is not None:
            lens.append(v["text_chars"])
            t["over_32000_text_among_OVER_or_UNDER"] += v["over_32000_text"]
            if b == "OVER":
                t["over_ids"].append(pid)
                t["cut_" + v["cut"]] += 1
                if not v["over_32000_text"]:
                    t["over_only_with_header_ids"].append(pid)
    assert t["OVER"] + t["UNDER"] + t["UNDECIDABLE"] + t["MISSING"] + t["UNREADABLE"] + t["NO_PRIMARY_DECISION_ROW"] == len(ids)
    assert t["over_any_version_on_disk"] + t["over_no_version_on_disk"] + t["no_readable_version"] == len(ids)
    if lens:
        q = statistics.quantiles(lens, n=4)
        t["text_chars"] = {"n": len(lens), "min": min(lens), "p25": q[0], "median": statistics.median(lens),
                           "p75": q[2], "max": max(lens), "mean": round(statistics.mean(lens), 1)}
    return t


def omitted_stats(ids):
    om, frac = [], []
    for pid in ids:
        b, v = bucket(pid)
        if b == "OVER":
            om.append(v["body_chars_omitted"])
            frac.append(v["body_chars_omitted"] / v["text_chars"])
    if not om:
        return {}
    return {"n": len(om), "min": min(om), "median": statistics.median(om), "max": max(om),
            "median_fraction_of_text_omitted": round(statistics.median(frac), 4),
            "max_fraction_of_text_omitted": round(max(frac), 4),
            "omitted_over_half": sum(f > 0.5 for f in frac)}


def decision_tally(table):
    rows = [d for d in drows if d["table"] == table]
    t = {"rows": len(rows), "papers": len({d["paper_id"] for d in rows}),
         "over_budget_code": sum(d["over_budget_code"] == 1 for d in rows),
         "under_budget_code": sum(d["over_budget_code"] == 0 for d in rows),
         "no_version_decided": sum(d["over_budget_code"] == "" for d in rows),
         "over_32000_text": sum(d["over_32000_text"] == 1 for d in rows), "by_decision": {}, "by_paper_status": {}}
    assert t["over_budget_code"] + t["under_budget_code"] + t["no_version_decided"] == t["rows"]
    for d in rows:
        for key, val in (("by_decision", d["decision"]), ("by_paper_status", d["paper_status"])):
            e = t[key].setdefault(val, {"rows": 0, "over_budget_code": 0, "no_version_decided": 0})
            e["rows"] += 1
            e["over_budget_code"] += d["over_budget_code"] == 1
            e["no_version_decided"] += d["over_budget_code"] == ""
    return t


route = {"primary_FT_EXCLUDE": 0, "primary_FT_ELIGIBLE_then_verifier_FLAGGED_then_PI_out": 0, "other": 0}
route_over = dict.fromkeys(route, 0)
for pid in ft_out:
    last = prim_by_paper.get(pid, [None])[-1]
    if last is not None and last["decision"] == "FT_EXCLUDE":
        k = "primary_FT_EXCLUDE"
    elif (last is not None and pid in ver_by_paper and ver_by_paper[pid]["decision"] == "FT_FLAGGED"
          and pid in adjud and adjud[pid]["adjudication_decision"] == "FT_SCREENED_OUT"):
        k = "primary_FT_ELIGIBLE_then_verifier_FLAGGED_then_PI_out"
    else:
        k = "other"
    route[k] += 1
    route_over[k] += bucket(pid)[0] == "OVER"

summary = {
    "generated_utc": datetime.now(timezone.utc).isoformat(),
    "budget_FT_MAX_TEXT_CHARS": FT_MAX_TEXT_CHARS,
    "ft_screened_out": tally(ft_out),
    "ft_screened_out_omitted_chars_among_OVER": omitted_stats(ft_out),
    "ft_screened_out_route": route, "ft_screened_out_route_OVER": route_over,
    "eligible_informational": tally(eligible),
    "eligible_omitted_chars_among_OVER": omitted_stats(eligible),
    "primary_decision_rows": decision_tally("ft_screening_decisions"),
    "verifier_decision_rows": decision_tally("ft_verification_decisions"),
    "papers_with_ft_decision_rows": len({d["paper_id"] for d in drows}),
    "version_csv_rows": sum(max(1, len(versions(p))) for p in scope), "scope_papers": len(scope),
}
json.dump(summary, open(os.path.join(OUT, "m1_summary.json"), "w"), indent=1, sort_keys=True)
print(json.dumps(summary, indent=1, sort_keys=True))
