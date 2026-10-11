"""12j-REVIEW-W2 — lead synthesis (W2-c). The ROWS are the lead's data; the script renders and counts.
usage: synthesis.py <reader_candidates.json> <out_dir>      writes candidates_table.md, counts.md, synthesis.json

A row: (reader ids, class, package cell, existing-row cell or None for new, pattern, evidence level, re-derived).
class: "1" · "1c" (Class 1 the reader, or the lead, qualified: conditional / latent / weak) · "2" · "3".
The statement is the title the reader gave the candidate, quoted from its report (titles joined by " // "
for a merged row). File / function is the reader's own cell. New rows take INT-j12-n in table order.
"""
import json, sys
from collections import Counter

cands = json.load(open(sys.argv[1]))
cands["L09a-C14"] = {"title": "`_collect_ft_flagged` reads the excerpt outside the resolver (W1 adjacent item 10) — verified",
                     "lot": "L09a", "File / function": "`engine/adjudication/ft_screening_adjudicator.py::_collect_ft_flagged`"}
R, CR, C = "Y code+rerun", "Y code", "N"
CLOUD = "P1 (cloud; not reachable on live while R71 keeps `enabled_arms` empty)"
ROWS = [
 (["L14-C1"], "1", "P5", None, "ii / i", "reproduced", R),
 (["L14-C2"], "1", "P5", None, "i (R28)", "reproduced", R),
 (["L14-C3"], "1", "P5", "extends B2", "i", "reproduced", R),
 (["L14-C4"], "1", "P5", "extends B2", "i", "reproduced", R),
 (["L09a-C2"], "1", "P4", "extends C42 (closed for FT only) and A2", "i / ii", "reproduced", R),
 (["L09b-C1", "L09a-C8"], "1", "P4 (the collector half was proposed 3 / P7)", None, "v / i", "reproduced (the page); code order (the xlsx collector)", R),
 (["L09b-C4"], "1", "P4 (the reader: the lead may weigh it as 2)", None, "other", "code order (no JS runtime on the box)", CR),
 (["L09b-C5"], "1", "P4", None, "iv", "code order", CR),
 (["L09b-C6"], "1", "P4 (3 if ruled an intended excerpt)", None, "i / other", "reproduced", R),
 (["L10-C1"], "1", "P4", None, "vi / iii", "reproduced", R),
 (["L10-C2"], "1", "P4", None, "vi / i", "reproduced", R),
 (["L10-C3"], "1", "P4", None, "i / other", "reproduced", R),
 (["L10-C4"], "1", "P4 (2 if the gate is ruled advisory)", None, "ii / v", "reproduced", R),
 (["L10-C5"], "1", "P5 (or P2 with INT-g6-2)", "extends INT-g6-2; a Phase A site for INT-i12-77 (R596(d))", "vi / i", "reproduced", R),
 (["L08-C1"], "1", "P5", "extends C47 (two new sites) and C37", "iii (i)", "reproduced", R),
 (["L08-C3"], "1", "P2 (3 if the log line is ruled the audit trail)", None, "i (iv)", "reproduced", R),
 (["L08-C4"], "1", "P4 for (a) (operator-initiated: the reader says the lead may read it as 3); (b) 3 / P7", None, "i", "code order", CR),
 (["L09a-C1"], "1c", "P4", None, "vi / other", "reproduced; conditional (a workbook with no `PI_decision` header)", R),
 (["L09a-C3", "L09b-C2", "L13-C1"], "1c", "P5, with INT-i12-77", "extends INT-i12-77 (its Phase A list: six import paths, the two pages' export, the workbook builder)", "vi (iv)",
  "reproduced (both xlsx imports, the workbook builder); code order (both JSON imports, the pages' export); conditional (file rows not matching this database's flagged papers)", R),
 (["L09b-C3"], "1c", "P4", None, "vi / iv", "conditional (same paper id on a later page for the same review name, same browser profile); code order", CR),
 (["L10-C6"], "1c", "P4", None, "iv / i", "code order; conditional (the quality-check CLI is run)", CR),
 (["L10-C10"], "1c", "P3", None, "ii / i", "code order (the drops); conditional (the trigger)", CR),
 (["L11-C1"], "1c", "P1 (the reader: the lead may lower it to 2)", None, "i / iv", "conditional (override edited without restart, or applied by the run's own restart; or `OLLAMA_HOST` elsewhere)", CR),
 (["L11-C9"], "1c", "P1, with Wave 1 row 8 (the progress-side fix alone is Class 3)", "extends C56 (Wave 1 row 8's note)", "ii", "conditional (the tee end of the pipe gone while Python lives)", CR),
 (["L12-C1"], "1c", CLOUD, "extends C28 and B9", "i / v", "reproduced", R),
 (["L12-C2"], "1c", CLOUD, None, "ii / other", "reproduced", R),
 (["L12-C3"], "1c", CLOUD, "extends C28", "i", "code order", CR),
 (["L12-C4"], "1c", CLOUD, "extends C24 (cloud half)", "iv / i", "code order", CR),
 (["L12-C5"], "1c", "P6", "extends C10", "iii / i", "reproduced; conditional (a pre-018 database with NULLs; dead on live)", R),
 (["L12-C6"], "1c", CLOUD + " (weak: pinned as intended by a test; could be read as Class 3)", None, "other", "reproduced", R),
 (["L14-C5"], "1c", "P5 (the reader: the lead may weigh it as 3)", None, "other", "reproduced, on a narrow input", R),
 (["L14-C9"], "1c", "P5, with G2 (the report site is 3)", "extends G3, under G2's condition", "ii", "code order", CR),
 (["L08-C2"], "2", "P5", "extends C47", "i", "reproduced", R),
 (["L08-C8"], "2", "P2", None, "iv", "conditional (the caller passes another review's spec; whether `open_run` refuses was not established)", CR),
 (["L09a-C4"], "2", "P4 (1 if the caller continues on the connection)", None, "iii / i", "reproduced", R),
 (["L09a-C5", "L13-C4"], "2", "P4 (the builder-text half was proposed 3 / P5)", None, "i / ii", "reproduced (FT import, the builder text); code order (abstract import)", R),
 (["L09b-C8", "L09a-C7"], "2", "P4 (the exporter half was proposed 3 / P7)", None, "iv / i", "code order; conditional (the caller's database is not `DATA_ROOT/<name>/review.db`, or the name is omitted)", CR),
 (["L09b-C9"], "2", "P4", None, "ii", "code order (no JS runtime on the box)", CR),
 (["L10-C7"], "2", "P5", None, "v / i", "reproduced", R),
 (["L10-C8"], "2", "P4", None, "ii / iii", "code order", CR),
 (["L10-C9"], "2", "P4", "extends INT-i12-36", "i", "code order", CR),
 (["L10-C11"], "2", "P3", None, "other", "conditional (a term with a top-level OR and no parentheses; live's terms are wrapped)", CR),
 (["L11-C2"], "2", "P4", None, "i / other", "reproduced (synthetic package, tmux stubbed); code order for the `ft_screener` site", R),
 (["L11-C3"], "2", "P7", None, "i / other", "conditional (a tmux server already running; tmux's documented behaviour, not run)", CR),
 (["L11-C4"], "2", "P1, with A-1", "extends A-1", "i / other", "code order; conditional (a probe over 60 s)", CR),
 (["L12-C7"], "2", CLOUD, None, "i", "reproduced", R),
 (["L12-C8"], "2", CLOUD, None, "other", "conditional (a provider sends a non-integer header); the path reproduced", R),
 (["L12-C12"], "2", CLOUD, None, "i", "conditional (a caller other than `run_arm`; none at HEAD)", CR),
 (["L13-C2"], "2", "P5 (the label alone could be 3 / P7)", None, "v", "reproduced (the expression); code order (the path)", R),
 (["L13-C5"], "2", "P5", "extends INT-g3-1", "other", "reproduced at the python-docx boundary; conditional for the engine path", R),
 (["L08-C5"], "3", "P7", None, "i", "code order", C),
 (["L08-C6"], "3", "P7", None, "iv", "conditional (the file changes between two reads)", C),
 (["L08-C7"], "3", "P3", "extends B-F07 and B-F06 (through Wave 1 row 34)", "iv", "code order", C),
 (["L09a-C6"], "3", "P7", None, "iv / i / iii", "code order", C),
 (["L09a-C9"], "3", "P7", None, "iv", "reproduced (abstract); code order (FT)", C),
 (["L09a-C10"], "3", "P7", None, "other", "code order", C),
 (["L09a-C11"], "3", "P7", None, "i", "reproduced", C),
 (["L09a-C12"], "3", "P7", None, "i", "reproduced", C),
 (["L10-C12", "L09a-C13"], "3", "P2, with INT-i12-74 (the categorizer half was proposed P7)", "extends INT-i12-74", "other / ii", "code order", C),
 (["L09a-C14"], "3", "P7 (or P2)", None, "iv", "code order", C),
 (["L09b-C7"], "3", "P7", None, "ii", "reproduced (the no-row case)", C),
 (["L09b-C10"], "3", "P7", None, "i", "code order", C),
 (["L10-C13"], "3", "P7", None, "i / other", "code order", C),
 (["L11-C5"], "3", "P7", None, "ii", "reproduced", C),
 (["L11-C6"], "3", "P7", None, "other", "reproduced (the status); code order", C),
 (["L11-C7"], "3", "P7", None, "i", "reproduced", C),
 (["L11-C8"], "3", "P7", None, "i", "reproduced", C),
 (["L11-C10"], "3", "P7", None, "i", "code order", C),
 (["L12-C9"], "3", "P7", None, "i", "reproduced", C),
 (["L12-C10"], "3", "P7", None, "i", "reproduced", C),
 (["L12-C11"], "3", "P7", None, "iv", "code order (the pinned SDK types read offline)", C),
 (["L13-C3"], "3", "P5", "extends B-F09 (the per-artifact half)", "iv", "code order", C),
 (["L13-C6"], "3", "P7", None, "i", "reproduced (the saved XML)", C),
 (["L13-C7"], "3", "P7", None, "i / other", "reproduced (a, d, e); code order (the rest)", C),
 (["L14-C6"], "3", "P7", None, "i", "reproduced (the behaviour); attribution by formula", C),
 (["L14-C7"], "3", "P7", None, "i / v", "code order", C),
 (["L14-C8"], "3", "P5 (1 if derived statistics count as provenance)", None, "iv", "code order", C),
]
LABEL = {"1": "1", "1c": "1 (conditional / latent / weak)", "2": "2", "3": "3"}
used = [c for r in ROWS for c in r[0]]
assert len(used) == len(set(used)), "a candidate is in two rows"
assert set(used) == set(cands), (set(cands) ^ set(used))
order = {"1": 0, "1c": 1, "2": 2, "3": 3}
assert [order[r[1]] for r in ROWS] == sorted(order[r[1]] for r in ROWS)
out, n_new = [], 0
for i, (ids, cls, pkg, existing, pat, ev, red) in enumerate(ROWS, 1):
    if existing is None:
        n_new += 1
        mapping, new_id = f"new — **INT-j12-{n_new}**", f"INT-j12-{n_new}"
    else:
        mapping, new_id = existing, None
    assert not (cls != "3" and red == C), ids
    out.append({"row": i, "reader_ids": ids, "lots": sorted({cands[c]["lot"] for c in ids}), "class": cls, "package": pkg,
                "mapping": mapping, "proposed_id": new_id, "pattern": pat, "evidence": ev, "rederived": red,
                "file_function": " // ".join((cands[c].get("File / function") or "batch across the lot's files") for c in ids),
                "statement": " // ".join(cands[c]["title"] for c in ids)})
esc = lambda s: s.replace("|", "\\|")
lines = ["| # | reader ID(s) | lot | file / function | pattern | evidence level | class / package | existing row, or new (proposed ID) | re-derived by lead | statement |",
         "| ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
for r in out:
    lines.append("| %d | %s | %s | %s | %s | %s | %s / %s | %s | %s | %s |" % (
        r["row"], " + ".join(r["reader_ids"]), ", ".join(r["lots"]), esc(r["file_function"]), r["pattern"], esc(r["evidence"]),
        LABEL[r["class"]], esc(r["package"]), r["mapping"], r["rederived"], esc(r["statement"])))
by = Counter(); new = Counter(); pk = Counter()
for r in out:
    by[r["class"]] += 1; new[r["class"]] += r["proposed_id"] is not None
    pk[(r["package"][:2], r["class"])] += 1
cl = ["| proposed class | rows after merging | of which new (proposed INT-j12-n) | of which extend an existing row |", "| --- | ---: | ---: | ---: |"]
for k in ("1", "1c", "2", "3"):
    cl.append(f"| {LABEL[k]} | {by[k]} | {new[k]} | {by[k] - new[k]} |")
cl.append(f"| all | {len(out)} | {sum(new.values())} | {len(out) - sum(new.values())} |")
c12 = [r for r in out if r["class"] != "3"]
cl += ["", f"Reader candidates: {len(used)}, in {len(out)} rows after {len(used) - len(out)} merges (rows with more than one reader ID: "
       f"{sum(len(r['reader_ids']) > 1 for r in out)}). Class 1 / 2 rows: {len(c12)}, every one marked re-derived by the lead "
       f"({sum(r['rederived'] == R for r in c12)} by code and rerun, {sum(r['rederived'] == CR for r in c12)} by code).", "",
       "By package (first-named), Class 1 and 1-conditional / Class 2 / Class 3: " + "; ".join(
           f"{p} {pk[(p,'1')] + pk[(p,'1c')]} / {pk[(p,'2')]} / {pk[(p,'3')]}" for p in ("P1", "P2", "P3", "P4", "P5", "P6", "P7"))]
od = sys.argv[2]
open(od + "/candidates_table.md", "w").write("\n".join(lines) + "\n")
open(od + "/counts.md", "w").write("\n".join(cl) + "\n")
json.dump({"rows": out, "by_class": by, "new_by_class": new}, open(od + "/synthesis.json", "w"), indent=1, ensure_ascii=False)
print("\n".join(cl))
