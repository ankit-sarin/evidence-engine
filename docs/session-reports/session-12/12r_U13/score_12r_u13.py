"""12r-U13 — the comparison rehearsal's scoring: first Pass-1 attempt per paper and field, against 12h.
Read-only (the review database opened mode=ro; every other input opened for reading). usage, from the
repository root:
    score_12r_u13.py --review-dir <dir> --run-id <n> --baseline <rd1_attempts_cells.csv>
                     --baseline-source ra_12g
(stdout: the JSON summary; also writes, beside this script: u13_cells.csv, u13_papers.csv,
u13_duplicates.csv, u13_stored.csv and u13_summary.json.)

Derived from 12h's rd1_attempts.py (docs/session-reports/session-12/12h_rd/), with these changes,
each named (12p_U13A.md §(c)):
  1. Attempt 1 is extra.attempts[0] of the paper's FIRST telemetry row (file order), not of the last
     row (rd1_attempts.py took ex[-1]); every outer row is still read, and a paper with more than one
     outer row is flagged, not excluded (R650(c)).
  2. Attempt 1's raw response is extra.attempts[0].raw_content (R571(e), a0b818e), not the accepted
     attempt's pass1_raw_content; it is re-parsed with the engine's own contracts.check_response
     against the run's persisted unit map, and the re-check is compared with the recorded codes (a
     consistency count, reported).
  3. Two verdicts per field: HEAD's check (the recorded codes) and the pre-dccd679 verdict
     reconstructed from the raw response. dccd679 changed one clause: HEAD raises
     VALUE_WITHOUT_CITATION when "not cited_t or (cls == JUDGMENT and not any(s.indices for s in
     steps))"; before it, "not cited_t". So the pre-dccd679 codes are HEAD's codes with
     VALUE_WITHOUT_CITATION removed wherever the union of field-level and step citations is
     non-empty. A field fails a verdict when that verdict holds a FATAL code (contracts.FATAL).
  4. R617(a)'s split: each JUDGMENT field carrying VALUE_WITHOUT_CITATION is split by whether its
     entry carries a field-level citation (a valid field-level unit index), under both verdicts.
  5. Duplicates, two locations counted as separate cases (R622(b), R659(b)): an attempt-1 duplicate is
     an outer row whose extra has no "attempts" and carries "duplicated_fields" (b3f0072: the row's
     extra is the DuplicateFieldError's attached telemetry); an attempt-2 duplicate is
     extra.attempts[1].duplicated_fields. A paper whose first row is an attempt-1 duplicate has no
     per-field attempt-1 verdict (the response was refused whole); it is listed, not scored.
  6. Stored outcomes, reported, not compared: the run's paper events, its field events by type, the
     final row's terminal_states, and run_extraction_summary(conn, run_id).
  7. The comparison: per field and per paper against the baseline CSV's attempt-1 rows of
     --baseline-source (paper ids are the same in both throwaways: 12g's id_map order). The two
     concentrations are STATED fields whose codebook prose names an absence sentinel (rd1_attempts.py's
     FIELD_FLAGS rule, copied unchanged) and JUDGMENT fields.
"""
import csv, hashlib, json, os, re, sqlite3, sys
from collections import Counter, defaultdict

sys.path.insert(0, os.getcwd())
from engine.core.codebook import load_codebook
from engine.core.extraction_events import run_extraction_summary
from engine.elicitation import contracts as K
from engine.elicitation.units import UnitMap
assert not any(m == "ollama" or m.startswith("ollama.") for m in sys.modules), "ollama imported"

HERE = os.path.dirname(os.path.abspath(__file__))
args = sys.argv[1:]
def opt(name):
    i = args.index(name); v = args[i + 1]; del args[i:i + 2]; return v
REVIEW = opt("--review-dir"); RUN_ID = int(opt("--run-id"))
BASELINE = opt("--baseline"); BASE_SRC = opt("--baseline-source")
CB_PATH = os.path.join(REVIEW, "extraction_codebook.yaml")
TEL = os.path.join(REVIEW, "telemetry", "extraction_calls.jsonl")
DB = os.path.join(REVIEW, "review.db")
codebook = load_codebook(CB_PATH).raw


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


# rd1_attempts.py's FIELD_FLAGS rule, unchanged.
SENTINELS = set(codebook["absence_sentinels"])
SENT_RE = re.compile(r"\b(?:" + "|".join(re.escape(x) for x in sorted(SENTINELS, key=len, reverse=True)) + r")\b")
FLAGS = {}
for f in codebook["fields"]:
    prose = " ".join(str(f.get(k) or "") for k in ("definition", "instruction", "decision_criteria"))
    FLAGS[f["name"]] = {"field_class": f.get("field_class"), "prose_names_a_sentinel": bool(SENT_RE.search(prose))}


def concentration(name, cls):
    if cls == "judgment":
        return "judgment"
    if cls == "stated" and FLAGS[name]["prose_names_a_sentinel"]:
        return "stated_with_sentinel"
    return "other"


rows = [json.loads(l) for l in open(TEL)]
by_paper = defaultdict(list)
for r in rows:
    by_paper[r["paper_id"]].append(r)

unit_maps = {}
def unit_map(pid, dir_name):
    if pid not in unit_maps:
        j = json.load(open(os.path.join(REVIEW, "elicitation", dir_name, "unit_maps", f"{pid}.json")))
        unit_maps[pid] = UnitMap(paper_id=j["paper_id"], units=tuple(j["units"]),
                                 spans=tuple(tuple(s) for s in j["spans"]), source_raw="",
                                 min_unit_tokens=j["min_unit_tokens"], version=j["version"])
    return unit_maps[pid]


cells, papers, dups, unscored = [], [], [], []
recheck = Counter()
for pid in sorted(by_paper):
    rs = by_paper[pid]
    for n, r in enumerate(rs, start=1):
        e = r.get("extra") or {}
        if "attempts" not in e and e.get("duplicated_fields"):
            dups.append({"paper_id": pid, "outer_row": n, "location": "attempt_1",
                         "key": 'row["extra"]["duplicated_fields"]', "fields": "|".join(e["duplicated_fields"]),
                         "outcome": r["outcome"], "request_hash": e.get("pass1_request_hash")})
        for a in e.get("attempts") or []:
            if a.get("attempt") == 2 and a.get("duplicated_fields"):
                dups.append({"paper_id": pid, "outer_row": n, "location": "attempt_2",
                             "key": 'row["extra"]["attempts"][1]["duplicated_fields"]',
                             "fields": "|".join(a["duplicated_fields"]), "outcome": r["outcome"],
                             "request_hash": a.get("request_hash")})
    first = rs[0].get("extra") or {}
    last = rs[-1].get("extra") or {}
    papers.append({"paper_id": pid, "outer_rows": len(rs), "flag_multiple_outer_rows": int(len(rs) > 1),
                   "outcomes": "|".join(r["outcome"] for r in rs),
                   "pass1_attempts_per_row": "|".join(str(len((r.get("extra") or {}).get("attempts") or [])) for r in rs),
                   "first_row_accepted_attempt": first.get("accepted_attempt"),
                   "last_row_accepted_attempt": last.get("accepted_attempt"),
                   "attempt1_n_failed": (first.get("attempts") or [{}])[0].get("n_failed")})
    if not first.get("attempts"):
        unscored.append({"paper_id": pid, "first_row_outcome": rs[0]["outcome"],
                         "first_row_error": rs[0].get("error"),
                         "reason": "attempt-1 duplicate (refused whole)" if first.get("duplicated_fields")
                                   else "first row carries no elicitation attempts"})
        continue
    a1 = first["attempts"][0]
    um = unit_map(pid, first["unit_map_dir"])
    expected = tuple(a1["fields"])
    entries, _ = K.parse_container(a1["raw_content"])
    by_name = {str(x.get(K.KEY_FIELD)): x for x in entries if x.get(K.KEY_FIELD)}
    try:
        re_res = K.check_response(a1["raw_content"], um, codebook, expected)
    except K.DuplicateFieldError:
        re_res = None
    failed = set(a1["failed_fields"])
    for name, f in a1["fields"].items():
        head = tuple(f["violations"])
        if re_res is not None:
            recheck["equal" if tuple(re_res.records[name].violations) == head else "differs"] += 1
        ent = by_name.get(name) or {}
        fidx = K._resolve_indices(ent.get(K.KEY_INDICES), um)[0]
        sidx = ()
        if f["class"] == "judgment":
            steps = K._parse_steps(ent.get(K.KEY_STEPS), um)[0]
            sidx = tuple(i for s in steps for i in s.indices)
        cited = bool(fidx) or bool(sidx)
        pre = tuple(c for c in head if not (c == K.VALUE_WITHOUT_CITATION and cited))
        cells.append({"paper_id": pid, "field_name": name, "field_class": f["class"],
                      "concentration": concentration(name, f["class"]),
                      "head_failed": int(name in failed), "head_codes": "|".join(head),
                      "pre_dccd679_failed": int(any(c in K.FATAL for c in pre)),
                      "pre_dccd679_codes": "|".join(pre),
                      "field_level_citation": int(bool(fidx)), "step_citation": int(bool(sidx)),
                      "escape": int(bool(f.get("escape")))})

base = [r for r in csv.DictReader(open(BASELINE)) if r["source"] == BASE_SRC and r["attempt"] == "1"]
for r in base:
    r["concentration"] = concentration(r["field_name"], r["field_class"])


def tally(sel, key):
    return {"field_attempts": len(sel), "failed": sum(int(c[key]) for c in sel)}


conc = {}
for k in ("stated_with_sentinel", "judgment", "other"):
    conc[k] = {"u13_head": tally([c for c in cells if c["concentration"] == k], "head_failed"),
               "u13_pre_dccd679": tally([c for c in cells if c["concentration"] == k], "pre_dccd679_failed"),
               "baseline_12g": tally([r for r in base if r["concentration"] == k], "failed")}
per_field = {}
for name in [f["name"] for f in codebook["fields"]]:
    u = [c for c in cells if c["field_name"] == name]; b = [r for r in base if r["field_name"] == name]
    if u or b:
        per_field[name] = {"class": FLAGS[name]["field_class"], "concentration": concentration(name, FLAGS[name]["field_class"]),
                           "u13_head": tally(u, "head_failed"), "u13_pre_dccd679": tally(u, "pre_dccd679_failed"),
                           "baseline_12g": tally(b, "failed")}
per_paper = {}
for p in sorted({c["paper_id"] for c in cells} | {int(r["paper_id"]) for r in base}):
    u = [c for c in cells if c["paper_id"] == p]; b = [r for r in base if int(r["paper_id"]) == p]
    per_paper[str(p)] = {"u13_head": tally(u, "head_failed"), "u13_pre_dccd679": tally(u, "pre_dccd679_failed"),
                         "baseline_12g": tally(b, "failed")}
codes = {"u13_head": Counter(), "u13_pre_dccd679": Counter(), "baseline_12g": Counter()}
for c in cells:
    for x in filter(None, c["head_codes"].split("|")): codes["u13_head"][x] += 1
    for x in filter(None, c["pre_dccd679_codes"].split("|")): codes["u13_pre_dccd679"][x] += 1
for r in base:
    for x in filter(None, r["violation_codes"].split("|")): codes["baseline_12g"][x] += 1
vwc = [c for c in cells if c["field_class"] == "judgment"]
split = {}
for verdict, key in (("head", "head_codes"), ("pre_dccd679", "pre_dccd679_codes")):
    hit = [c for c in vwc if K.VALUE_WITHOUT_CITATION in c[key].split("|")]
    split[verdict] = {"judgment_value_without_citation": len(hit),
                      "with_field_level_citation": sum(c["field_level_citation"] for c in hit),
                      "without_field_level_citation": sum(1 - c["field_level_citation"] for c in hit)}

stored = []
conn = sqlite3.connect(f"file:{os.path.abspath(DB)}?mode=ro", uri=True)
pev = conn.execute("SELECT paper_id, event_type, to_state, reason_code, occurred_at FROM paper_events "
                   "WHERE run_id = ? ORDER BY event_id", (RUN_ID,)).fetchall()
fev = Counter(conn.execute("SELECT paper_id, field_name, event_type FROM field_events WHERE run_id = ?",
                           (RUN_ID,)).fetchall())
for pid in sorted(by_paper):
    ts = (by_paper[pid][-1].get("extra") or {}).get("terminal_states") or {}
    names = sorted(set(ts) | {k[1] for k in fev if k[0] == pid})
    for name in names:
        stored.append({"paper_id": pid, "field_name": name, "final_row_terminal_state": ts.get(name),
                       "field_events": "|".join(f"{k[2]}:{v}" for k, v in sorted(fev.items())
                                                if k[0] == pid and k[1] == name)})
summary_db = run_extraction_summary(conn, RUN_ID)
manifest = conn.execute("SELECT run_id, run_uid, started_at, ended_at, end_status, end_reason FROM run_manifests "
                        "WHERE run_id = ?", (RUN_ID,)).fetchone()
conn.close()


def write(name, recs):
    with open(os.path.join(HERE, name), "w", newline="") as fh:
        if recs:
            w = csv.DictWriter(fh, fieldnames=list(recs[0])); w.writeheader(); w.writerows(recs)
    return name


out = {
    "inputs": {p: sha(p) for p in (TEL, CB_PATH, BASELINE, os.path.abspath(__file__))},
    "run": {"run_id": RUN_ID, "manifest": manifest},
    "telemetry_rows": len(rows), "papers_with_rows": len(by_paper),
    "attempt1_cells": len(cells), "attempt1_failed_head": sum(c["head_failed"] for c in cells),
    "attempt1_failed_pre_dccd679": sum(c["pre_dccd679_failed"] for c in cells),
    "baseline": {"source": BASE_SRC, "attempt1_cells": len(base), "failed": sum(int(r["failed"]) for r in base),
                 "papers": sorted({int(r["paper_id"]) for r in base})},
    "recheck_against_recorded_codes": dict(recheck),
    "concentrations": conc, "per_field": per_field, "per_paper": per_paper,
    "codes": {k: dict(v.most_common()) for k, v in codes.items()},
    "r617a_judgment_vwc_split": split,
    "papers": papers, "unscored_papers": unscored,
    "duplicates": {"attempt_1": sum(d["location"] == "attempt_1" for d in dups),
                   "attempt_2": sum(d["location"] == "attempt_2" for d in dups), "cases": dups},
    "stored": {"paper_events": pev, "run_extraction_summary": summary_db,
               "field_events_by_type": dict(Counter(k[2] for k in fev.elements()))},
    "outputs": [write("u13_cells.csv", cells), write("u13_papers.csv", papers),
                write("u13_duplicates.csv", dups), write("u13_stored.csv", stored)],
}
text = json.dumps(out, indent=1, sort_keys=True, default=str)
open(os.path.join(HERE, "u13_summary.json"), "w").write(text + "\n")
print(text)
