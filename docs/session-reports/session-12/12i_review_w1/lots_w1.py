"""12i-REVIEW-W1, step W1-a — wave-1 lot assembly (R578, R580). Read-only.

usage, from the repository root:  lots_w1.py <repo_root> <12i_scope_dir> <out_dir>
writes <out_dir>/lots_w1.json and lots_w1.md

In scope (R578, R580): lots L01–L07 of 12i_scope (lots.json), plus every unit graded F by either
assessment that is on the Run 7 import graph (RUN7 top or lazy) — MINUS, per F unit, "the function
or block their finding located" (EXCLUDE below, the lead's reading; line spans by ast). Where the
assessment names no function as the location, the WHOLE FILE is in scope (ruling A2's fallback).
The assignment of F units to lots (ASSIGN below) is the lead's judgement.
"""
import ast, json, os, re, sys

root, scope, out = sys.argv[1], sys.argv[2], sys.argv[3]
cov = {r["unit"]: r for r in json.load(open(os.path.join(scope, "coverage.json")))["table"]}
lots = {l["lot"]: l for l in json.load(open(os.path.join(scope, "lots.json")))["lots"]}
S12 = os.path.join(root, "docs/session-reports/session-12/outside")


def clean(s):
    return s.replace("\\_", "_")


TEXT = {"A": clean(open(os.path.join(S12, "assessment_A_architectural.md"), encoding="utf-8").read()),
        "B": clean(open(os.path.join(S12, "assessment_B_17_findings.md"), encoding="utf-8").read())}
# The excluded block per F unit — the LEAD'S READING of the assessment text (judgement), because a
# bare name match over-excludes (it caught `main`, `receipts`, the whole `ReviewDatabase` class and
# the exception `PendingMigrations`). A function is excluded only when a finding locates its defect
# IN that function by name. Every other F unit names no function and is in scope whole (ruling A2).
EXCLUDE = {
 "engine/agents/extractor.py": (["restart_ollama"], "A-§1.2 names `restart_ollama()`"),
 "engine/agents/models.py": (["ExtractionOutput"], "A-§3.2 names `ExtractionOutput.model_json_schema()`"),
 "engine/core/database.py": (["ReviewDatabase.__init__", "ReviewDatabase._run_migrations",
                              "ReviewDatabase.add_papers"],
                             "F02 names `ReviewDatabase.__init__` and `_run_migrations`; F05 names `add_papers`; "
                             "F13 cites lines only"),
 "engine/search/dedup.py": (["_exact_match"], "F04 names `_exact_match`"),
 "engine/search/openalex.py": (["_paginate_with_retry"], "F08 names `_paginate_with_retry`"),
 "engine/exporters/__init__.py": (["export_all"], "F09 names `export_all`"),
 "scripts/run_pipeline.py": (["_stage_search", "_stage_screen"], "F06 names `_stage_search`; F12 names `_stage_screen`"),
 "engine/parsers/pdf_parser.py": ([], "F01 names `parse_pdf` and locates the temp-file / commit / rename block "
                                      "inside it; ONLY THAT BLOCK is excluded and the rest of the 449-line "
                                      "function is in scope, so no line is subtracted here"),
}
BLOCK_ONLY = {"engine/parsers/pdf_parser.py"}


def spans(unit, names):
    tree = ast.parse(open(os.path.join(root, unit), encoding="utf-8").read())
    out = {}

    def walk(node, prefix):
        for n in ast.iter_child_nodes(node):
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                q = prefix + n.name
                if q in names or (not prefix and n.name in names):
                    out[q] = (n.lineno, n.end_lineno)
                if isinstance(n, ast.ClassDef):
                    walk(n, q + ".")
    walk(tree, "")
    assert set(out) == set(names), (unit, names, out)
    return out


f_units = [u for u, r in cov.items() if r["either"] == "F" and r["RUN7"] in ("top", "lazy")]
f_rows = {}
for u in f_units:
    r = cov[u]
    cites = []
    for who in ("A", "B"):
        if r[who] == "F":
            cites += ["%s:%s" % (who, i) for i in dict.fromkeys(
                re.findall(r"(A-§\d\.\d|A-FP\d|A-T\d|F\d\d)", r[who + "_cite"]))]
    names, why = EXCLUDE.get(u, ([], "no function is named as the location of the finding"))
    sp = spans(u, names) if names else {}
    ex_lines = sum(b - a + 1 for a, b in sp.values())
    f_rows[u] = {"lines": r["lines"], "RUN7": r["RUN7"], "cites": cites,
                 "excluded": {k: {"spans": [list(v)]} for k, v in sp.items()}, "why": why,
                 "excluded_lines": ex_lines, "in_scope_lines": r["lines"] - ex_lines,
                 "whole_file": not names and u not in BLOCK_ONLY, "block_only": u in BLOCK_ONLY}

ASSIGN = {   # lead's judgement: F unit -> lot (by subject), new lots W1-F1..F5 where none fits
 "engine/core/completeness.py": "L01", "engine/core/paper_state.py": "L01",
 "engine/agents/models.py": "L02",
 "engine/core/corpus.py": "L06",
 "engine/parsers/pdf_parser.py": "L07",
 "engine/agents/extractor.py": "W1-F1",
 "engine/core/database.py": "W1-F2", "engine/migrations/runner.py": "W1-F2",
 "engine/cloud/__init__.py": "W1-F2",
 "engine/agents/screener.py": "W1-F3", "engine/agents/ft_screener.py": "W1-F3",
 "engine/search/dedup.py": "W1-F3", "engine/search/openalex.py": "W1-F3",
 "engine/acquisition/download.py": "W1-F3",
 "scripts/run_pipeline.py": "W1-F4", "engine/exporters/__init__.py": "W1-F4",
 "engine/exporters/evidence_table.py": "W1-F4", "engine/exporters/methods_section.py": "W1-F4",
 "engine/exporters/prisma.py": "W1-F4",
 "engine/utils/ollama_client.py": "W1-F5", "engine/utils/ollama_lock.py": "W1-F5",
}
assert set(ASSIGN) == set(f_units), (set(ASSIGN) ^ set(f_units))
TITLES = {"W1-F1": "The two-pass extractor", "W1-F2": "Database construction and the migration runner",
          "W1-F3": "Screening agents, dedup, OpenAlex client, download",
          "W1-F4": "The pipeline runner and the exporters", "W1-F5": "The Ollama client and the experiment lock"}
EXTRA = {"L07": [("analysis/provenance/segment.py", 64,
                  "outside the census; imported at module level by parse_quality.py, contracts.py, units.py (row A-5)")]}
res = []
for lid in ["L01", "L02", "L03", "L04", "L05", "L06", "L07", "W1-F1", "W1-F2", "W1-F3", "W1-F4", "W1-F5"]:
    base = [(u, cov[u]["lines"], None) for u in lots[lid]["units"]] if lid in lots else []
    fu = [(u, f_rows[u]["in_scope_lines"], f_rows[u]) for u in f_units if ASSIGN[u] == lid]
    extra = EXTRA.get(lid, [])
    res.append({"lot": lid, "title": lots[lid]["title"] if lid in lots else TITLES[lid],
                "units": [{"unit": u, "in_scope_lines": n,
                           "F": None if f is None else {"cites": f["cites"], "whole_file": f["whole_file"], "block_only": f["block_only"],
                                                        "why": f["why"], "excluded": f["excluded"], "file_lines": f["lines"]}}
                          for u, n, f in base + fu],
                "extra": [{"unit": u, "in_scope_lines": n, "note": note} for u, n, note in extra],
                "n_units": len(base) + len(fu) + len(extra),
                "lines": sum(n for _, n, _ in base + fu) + sum(n for _, n, _ in extra)})
all_units = [x["unit"] for l in res for x in l["units"]]
assert len(all_units) == len(set(all_units))
tot = {"lots": len(res), "readers": len(res), "units": len(all_units),
       "extra_units": sum(len(l["extra"]) for l in res), "lines": sum(l["lines"] for l in res),
       "f_units": len(f_units), "f_units_whole_file": sum(1 for u in f_units if f_rows[u]["whole_file"]),
       "f_units_function_named": sum(1 for u in f_units if not f_rows[u]["whole_file"]),
       "f_excluded_lines": sum(f_rows[u]["excluded_lines"] for u in f_units)}
json.dump({"lots": res, "totals": tot, "f_units": f_rows}, open(os.path.join(out, "lots_w1.json"), "w"), indent=1)
with open(os.path.join(out, "lots_w1.md"), "w", encoding="utf-8") as f:
    f.write("| lot / reader | content | units | in-scope lines | F units added (excluded block) |\n| --- | --- | ---: | ---: | --- |\n")
    for l in res:
        fs = []
        for x in l["units"]:
            if x["F"]:
                ex = ("whole file in scope — no function named" if x["F"]["whole_file"] else
                      "excluding only the F01 block inside `parse_pdf`" if x["F"].get("block_only") else
                      "excluding " + ", ".join("`%s`" % k for k in x["F"]["excluded"]))
                fs.append("`%s` (%d of %d; %s; %s)" % (x["unit"], x["in_scope_lines"], x["F"]["file_lines"],
                                                       ", ".join(x["F"]["cites"]), ex))
        for x in l["extra"]:
            fs.append("also `%s` (%d; %s)" % (x["unit"], x["in_scope_lines"], x["note"]))
        f.write("| %s | %s | %d | %s | %s |\n" % (l["lot"], l["title"], l["n_units"], format(l["lines"], ","),
                                                 "; ".join(fs) or "—"))
    f.write("| all | %d readers | %d + %d | %s | |\n" % (tot["readers"], tot["units"], tot["extra_units"],
                                                        format(tot["lines"], ",")))
print(json.dumps(tot))
