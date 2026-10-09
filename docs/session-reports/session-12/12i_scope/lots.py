"""12i scoping read, step 3e — PROPOSED review lots over the N and C units. Nothing is started.

usage: lots.py <out_dir>      reads coverage.json; writes lots.json and lots.md
A unit is in scope for a lot when NEITHER assessment locates a finding to it (column "either" is
N or C). The lot definitions below are CC's proposal (judgement); the script only checks that
every in-scope unit is in exactly one lot and that no F unit is in any, and computes each lot's
units, lines and Run 7 share. "readers" is CC's estimate, not a measurement.
"""
import fnmatch, json, os, sys

out = sys.argv[1]
table = json.load(open(os.path.join(out, "coverage.json")))["table"]
T = {r["unit"]: r for r in table}

LOTS = [
 ("L01", "Extraction write path: claims as events, guards, selection, locator, telemetry", 1,
  ["engine/core/extraction_events.py", "engine/core/events.py", "engine/core/citation_guard.py",
   "engine/core/selection.py", "engine/core/reuse_key.py", "engine/core/locator.py",
   "engine/core/extraction_telemetry.py", "engine/core/run_telemetry.py",
   "engine/core/audit_telemetry.py"]),
 ("L02", "Elicitation (the Run 7 extraction design)", 1, ["engine/elicitation/*.py"]),
 ("L03", "Audit and the distribution gate", 1,
  ["engine/agents/auditor.py", "engine/agents/audit_events.py",
   "engine/validators/distribution_monitor.py", "engine/validators/__init__.py"]),
 ("L04", "Resolver and run manifest", 1,
  ["engine/core/effective_config.py", "engine/core/run_manifest.py"]),
 ("L05", "Spec, codebook, review identity, eligibility rendering", 1,
  ["engine/core/review_spec.py", "engine/core/codebook.py", "engine/core/review_paths.py",
   "engine/core/eligibility_render.py", "engine/core/constants.py"]),
 ("L06", "Readers: the one reader, the parsed-text resolver, naming", 1,
  ["engine/core/effective.py", "engine/core/parsed_text.py", "engine/core/naming.py"]),
 ("L07", "Parser support: font audit, parse-quality verdict, markers, models", 1,
  ["engine/parsers/font_audit.py", "engine/parsers/parse_quality.py", "engine/parsers/markers.py",
   "engine/parsers/models.py"]),
 ("L08", "Workflow, adjudication schema and the two entry importers", 1,
  ["engine/adjudication/__init__.py", "engine/adjudication/workflow.py",
   "engine/adjudication/schema.py", "engine/adjudication/import_extraction_entry.py",
   "engine/adjudication/import_screening_entry.py"]),
 ("L09", "Human screening adjudication: adjudicators, HTML generators, categorizer", 2,
  ["engine/adjudication/screening_adjudicator.py", "engine/adjudication/ft_screening_adjudicator.py",
   "engine/adjudication/abstract_adjudication_html.py", "engine/adjudication/ft_adjudication_html.py",
   "engine/adjudication/categorizer.py"]),
 ("L10", "Front half remainder: PubMed client, search models, acquisition except download.py", 1,
  ["engine/search/pubmed.py", "engine/search/models.py", "engine/acquisition/*.py"]),
 ("L11", "Local-model utilities and run support: preflight, tmux background, progress", 1,
  ["engine/utils/ollama_preflight.py", "engine/utils/background.py", "engine/utils/progress.py"]),
 ("L12", "Cloud arms (R71: no cloud extraction yet)", 1,
  ["engine/cloud/anthropic_extractor.py", "engine/cloud/base.py", "engine/cloud/openai_extractor.py",
   "engine/cloud/schema.py"]),
 ("L13", "Export remainder: DOCX and the shared review workbook", 1,
  ["engine/exporters/docx_export.py", "engine/exporters/review_workbook.py"]),
 ("L14", "Concordance: engine/analysis", 1, ["engine/analysis/*.py"]),
 ("L15", "Numbered migrations and the migrations CLI (applied text is frozen by receipt)", 1,
  ["engine/migrations/[0-9]*.py", "engine/migrations/__init__.py", "engine/migrations/__main__.py"]),
 ("L16", "Developer tools and the read-only validator", 1,
  ["engine/tools/inventory.py", "engine/tools/__init__.py",
   "engine/validators/extraction_validator.py"]),
 ("L17", "Scripts off the Run 7 import graph", 2, ["scripts/*.py", "scripts/pdf_acquisition/*.py"]),
 ("L18", "Empty package markers (0 lines each; nothing to read)", 0,
  ["engine/__init__.py", "engine/agents/__init__.py", "engine/core/__init__.py",
   "engine/parsers/__init__.py", "engine/search/__init__.py", "engine/utils/__init__.py"]),
]

scope = [r["unit"] for r in table if r["either"] in ("N", "C")]
assigned, res = {}, []
for lid, title, readers, globs in LOTS:
    us = [u for u in scope if any(fnmatch.fnmatch(u, g) for g in globs) and u not in assigned]
    for u in us:
        assigned[u] = lid
    lines = sum(T[u]["lines"] for u in us)
    top = [u for u in us if T[u]["RUN7"] == "top"]
    lazy = [u for u in us if T[u]["RUN7"] == "lazy"]
    res.append({"lot": lid, "title": title, "readers": readers, "units": us, "n_units": len(us),
                "lines": lines,
                "run7_top_units": len(top), "run7_top_lines": sum(T[u]["lines"] for u in top),
                "run7_lazy_units": len(lazy), "run7_lazy_lines": sum(T[u]["lines"] for u in lazy),
                "run7_share_lines": round((sum(T[u]["lines"] for u in top + lazy) / lines), 3) if lines else None,
                "N_units": sum(1 for u in us if T[u]["either"] == "N"),
                "C_units": sum(1 for u in us if T[u]["either"] == "C"),
                "not_recorded_12F": sum(1 for u in us if T[u]["12F"] == "not recorded")})
missing = [u for u in scope if u not in assigned]
assert not missing, missing
assert sum(r["n_units"] for r in res) == len(scope)
excluded = [{"unit": r["unit"], "lines": r["lines"], "RUN7": r["RUN7"], "A": r["A"], "B": r["B"],
             "12F": r["12F"]} for r in table if r["either"] == "F"]
tot = {"lots": len(res), "units": len(scope), "lines": sum(r["lines"] for r in res),
       "readers": sum(r["readers"] for r in res),
       "excluded_F_units": len(excluded), "excluded_F_lines": sum(e["lines"] for e in excluded)}
json.dump({"lots": res, "totals": tot, "excluded_F": excluded}, open(os.path.join(out, "lots.json"), "w"),
          indent=1)
with open(os.path.join(out, "lots.md"), "w", encoding="utf-8") as f:
    f.write("| lot | PROPOSED content | units (N / C) | lines | Run 7 share of lines (top + lazy) | 12F not recorded | readers (est.) |\n")
    f.write("| --- | --- | ---: | ---: | --- | ---: | ---: |\n")
    for r in res:
        share = "—" if not r["lines"] else "%d%% (%s top + %s lazy)" % (
            round(100 * r["run7_share_lines"]), format(r["run7_top_lines"], ","), format(r["run7_lazy_lines"], ","))
        f.write("| %s | %s | %d (%d / %d) | %s | %s | %d | %d |\n" % (
            r["lot"], r["title"], r["n_units"], r["N_units"], r["C_units"], format(r["lines"], ","), share,
            r["not_recorded_12F"], r["readers"]))
    f.write("| all | | %d | %s | | | %d |\n\n" % (tot["units"], format(tot["lines"], ","), tot["readers"]))
    for r in res:
        f.write("- **%s** — %s\n" % (r["lot"], ", ".join("`%s` (%d)" % (u, T[u]["lines"]) for u in r["units"])))
print(json.dumps(tot))
for r in res:
    print(r["lot"], r["n_units"], r["lines"], r["run7_share_lines"], r["readers"])
