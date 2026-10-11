"""verify_downloads on a review whose papers have no ee_identifier (no engine writer assigns one):
two papers, same first-author surname and year -> one canonical name -> paper 2 is pointed at paper 1's PDF."""
import shutil
from pathlib import Path
from engine.core.database import ReviewDatabase
from engine.search.models import Citation
from engine.acquisition.verify_downloads import verify_downloads
root = Path("vd_root2"); shutil.rmtree(root, ignore_errors=True)
db = ReviewDatabase("r", data_root=root)
for i, t in enumerate(["Alpha paper", "Beta paper"]):
    db.add_papers([Citation(title=t, pmid=str(100+i), source="pubmed", authors=["Smith J"], year=2024)])
ids = [r["id"] for r in db._conn.execute("SELECT id FROM papers ORDER BY id")]
print("ee_identifier after add_papers:", [tuple(r) for r in db._conn.execute("SELECT id, ee_identifier FROM papers")])
for p in ids: db.update_status(p, "ABSTRACT_SCREENED_IN")
db.close()
pdfs = root / "r" / "pdfs"
(pdfs / f"{ids[0]}.pdf").write_bytes(b"%PDF-1.4\n" + b"ALPHA " * 5000)
(pdfs / f"{ids[1]}.pdf").write_bytes(b"%PDF-1.4\n" + b"BETA " * 5000)
out = verify_downloads("r", data_root=root)
db = ReviewDatabase("r", data_root=root)
for r in db._conn.execute("SELECT p.id, p.title, p.download_status, p.pdf_local_path, f.pdf_path FROM papers p JOIN full_text_assets f ON f.paper_id=p.id ORDER BY p.id"):
    print(dict(r), "| file head:", Path(r["pdf_local_path"]).read_bytes()[9:20])
db.close()
print("dir:", sorted(x.name for x in pdfs.iterdir()))
out2 = verify_downloads("r", data_root=root)
print("second run unmatched:", out2["unmatched_files"], "valid:", out2["valid"])
