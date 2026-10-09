"""W1-F4-C1: an extraction-entry review (corpus screened elsewhere, R204) exported
through the real PRISMA and methods writers.

Synthetic review under this directory only. Real import_extraction_entry, real
generate_prisma_flow / validate_prisma_counts / generate_methods_section. No model,
no network: the import opens an `import` manifest with zero stages; digest_fn raises
if it is ever called.
"""
import json, re, shutil, sys, tempfile
from pathlib import Path

REPO = Path("/home/ankitsarin/projects/evidence-engine")
work = Path(tempfile.mkdtemp(prefix="c1_", dir=Path(__file__).parent))
RID = "w1f4_entry"

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.adjudication.import_extraction_entry import import_extraction_entry
from engine.exporters.prisma import generate_prisma_flow, validate_prisma_counts
from engine.exporters.methods_section import generate_methods_section

# spec + codebook: copies of the repo's, review id changed and nothing else
spec_txt = (REPO / "review_specs/surgical_autonomy.yaml").read_text()
spec_txt = re.sub(r"(?m)^review_id:.*$", f"review_id: {RID}", spec_txt)
(work / "spec.yaml").write_text(spec_txt)
spec = load_review_spec(work / "spec.yaml")

db = ReviewDatabase(RID, data_root=work / "data")
cb_txt = (REPO / "data/surgical_autonomy/extraction_codebook.yaml").read_text()
cb_txt = re.sub(r'(?m)^review:.*$', f'review: "{RID}"', cb_txt)
(Path(db.db_path).parent / "extraction_codebook.yaml").write_text(cb_txt)

src = work / "entry"; src.mkdir()
papers = []
for i in range(1, 4):
    (src / f"p{i}.md").write_text(f"# Paper {i}\n\nSynthetic body text number {i}.\n")
    papers.append({"title": f"Synthetic paper {i}", "pmid": str(900000 + i), "doi": None,
                   "text_path": f"p{i}.md"})
(src / "entry.json").write_text(json.dumps(
    {"source": "external_screening_tool", "papers": papers}))

def no_digest(model):
    raise AssertionError(f"digest requested for {model}")

res = import_extraction_entry(db, src / "entry.json", spec=spec, digest_fn=no_digest)
print("import:", res["imported"] if "imported" in res else res)
print("tables with screening rows:",
      {t: db._conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
       for t in ("abstract_screening_decisions", "ft_screening_decisions", "run_calls")})
print("papers.status / source:",
      [tuple(r) for r in db._conn.execute("SELECT status, source, COUNT(*) FROM papers GROUP BY 1,2")])

flow = generate_prisma_flow(db)
for k in ("records_identified", "records_by_source", "duplicates_removed", "records_screened",
          "records_excluded", "reports_sought", "full_text_retrieved", "full_text_assessed",
          "n_eligible", "studies_included"):
    print(f"  {k}: {flow[k]}")
print("validate_prisma_counts valid:", validate_prisma_counts(db, flow)["valid"])
print()
print("METHODS (run_id = the import run):")
print(generate_methods_section(db, spec, run_id=res["run_id"]))
db.close()
shutil.rmtree(work)
