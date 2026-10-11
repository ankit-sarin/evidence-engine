"""verify_downloads: (a) success recorded against an invalid canonical file; (b) key-only joins."""
import shutil
from pathlib import Path
from engine.core.database import ReviewDatabase
from engine.search.models import Citation
from engine.acquisition.verify_downloads import verify_downloads
root = Path("vd_root"); shutil.rmtree(root, ignore_errors=True)
db = ReviewDatabase("r", data_root=root)
for i, t in enumerate(["Alpha paper", "Beta paper", "Gamma never screened"]):
    db.add_papers([Citation(title=t, pmid=str(100+i), source="pubmed", authors=["Smith J"], year=2024)])
ids = [r["id"] for r in db._conn.execute("SELECT id FROM papers ORDER BY id")]
db.update_status(ids[0], "ABSTRACT_SCREENED_IN"); db.update_status(ids[1], "ABSTRACT_SCREENED_IN")
# the two numberings: ee_identifier deliberately NOT EE-{id}
db._conn.execute("UPDATE papers SET ee_identifier='EE-002' WHERE id=?", (ids[0],))
db._conn.execute("UPDATE papers SET ee_identifier='EE-001' WHERE id=?", (ids[1],))
db._conn.commit(); db.close()
pdfs = root / "r" / "pdfs"
good = b"%PDF-1.4\n" + b"0" * 20000
# (a) paper ids[0]: canonical name already holds an HTML error page; a good file arrives as <id>.pdf
(pdfs / "EE-002_Smith_2024.pdf").write_text("<!DOCTYPE html><html>403 Forbidden</html>")
(pdfs / f"{ids[0]}.pdf").write_bytes(good)
# (b) a human saves "EE-001.pdf" meaning papers.id 1 (= ids[0]); and a file for a never-screened paper
(pdfs / "EE-001.pdf").write_bytes(b"%PDF-1.4\n" + b"THIS IS THE ALPHA PAPER " * 1000)
(pdfs / f"{ids[2]}.pdf").write_bytes(good)
out = verify_downloads("r", data_root=root)
db = ReviewDatabase("r", data_root=root)
for r in db._conn.execute("SELECT id, title, status, ee_identifier, download_status, pdf_local_path FROM papers ORDER BY id"):
    p = r["pdf_local_path"]; head = Path(p).read_bytes()[:24] if p and Path(p).exists() else None
    print(dict(r), "| file head:", head)
print("invalid_files:", out["invalid_files"], "| dir:", sorted(x.name for x in pdfs.iterdir()))
