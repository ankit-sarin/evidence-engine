"""F06 — PRISMA identification / duplicate / exclusion-reason counts on a SYNTHETIC db.
cwd = ~/scratch/12f/g3 ; PYTHONPATH = repo. No model, no network."""
import shutil, logging
from pathlib import Path
logging.disable(logging.CRITICAL)
root = Path.cwd() / "f06_data"
shutil.rmtree(root, ignore_errors=True)
from engine.core.database import ReviewDatabase
from engine.search.models import Citation
from engine.search.dedup import deduplicate
from engine.exporters.prisma import generate_prisma_flow, validate_prisma_counts

db = ReviewDatabase("synth", data_root=root)
W = ["hepatectomy outcomes in cirrhosis", "robotic suturing of bowel anastomosis", "laparoscopic camera navigation trial",
     "ureteroscopy laser stone ablation", "cochlear implant insertion forces", "orthopaedic drilling haptics study",
     "retinal vein cannulation platform", "endovascular catheter steering", "prostate brachytherapy needle guidance",
     "neurosurgical tumour margin imaging", "tracheal intubation automation device", "skin closure stapling comparison",
     "thyroid nodule ultrasound biopsy", "cardiac valve repair simulation", "dental implant placement accuracy"]
pm = [Citation(pmid=str(1000+i), doi=f"10.1/p{i}", title=W[i],
               abstract="a", authors=["A"], journal="J", year=2020, source="pubmed") for i in range(10)]
oa = [Citation(pmid=None, doi=f"10.1/p{i}", title=W[i],
               abstract="a", authors=["A"], journal="J", year=2020, source="openalex") for i in range(3)]
oa += [Citation(pmid=None, doi=f"10.2/o{i}", title=W[10+i],
                abstract="a", authors=["A"], journal="J", year=2021, source="openalex") for i in range(5)]
res = deduplicate(pm, oa)
print("dedup stats (what _stage_search returns and logs):", res.stats)
print("added:", db.add_papers(res.unique_citations))

# one report excluded by BOTH primary passes (the only route run_screening has to SCREENED_OUT)
pid = db._conn.execute("SELECT id FROM papers ORDER BY id LIMIT 1").fetchone()[0]
db.add_screening_decision(pid, 1, "exclude", "Not surgical", "m")
db.add_screening_decision(pid, 2, "exclude", "Wrong population", "m")
db.update_status(pid, "ABSTRACT_SCREENED_OUT")

flow = generate_prisma_flow(db)
for k in ("records_identified", "records_by_source", "duplicates_removed",
          "records_screened", "records_excluded", "exclusion_reasons"):
    print(f"{k}: {flow[k]}")
print("sum(exclusion_reasons) =", sum(flow["exclusion_reasons"].values()),
      "vs records_excluded =", flow["records_excluded"])
print("validate_prisma_counts valid:", validate_prisma_counts(db, flow)["valid"])
