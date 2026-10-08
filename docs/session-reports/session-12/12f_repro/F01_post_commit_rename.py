"""F01: parse_pdf commits asset/ref/attempt rows, THEN renames tmp -> final.
Rename is made to raise (OSError). Synthetic DB under ~/scratch/12f/g1/f01. No model call:
docling is stubbed, OCR and vision are stubbed to raise."""
import shutil, sqlite3, sys
from pathlib import Path
from unittest.mock import patch
from fpdf import FPDF

REPO = Path("/home/ankitsarin/projects/evidence-engine")
ROOT = Path.home() / "scratch/12f/g1/f01"
shutil.rmtree(ROOT, ignore_errors=True); ROOT.mkdir(parents=True)

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.core.parsed_text import load_parsed_text
from engine.parsers import pdf_parser as pp
from engine.search.models import Citation

spec = load_review_spec(str(REPO / "review_specs/surgical_autonomy.yaml"))
db = ReviewDatabase("f01", data_root=ROOT)
db.add_papers([Citation(title="P", source="pubmed", pmid="1")])
pid = db.get_papers_by_status("INGESTED")[-1]["id"]
db.update_status(pid, "ABSTRACT_SCREENED_IN"); db.update_status(pid, "PDF_ACQUIRED")
pdf = FPDF(); pdf.add_page(); pdf.set_font("Helvetica", size=12)
pdf.multi_cell(w=0, text="Autonomous robotic suturing systematic review sentence. " * 40)
pdf_path = ROOT / "f01" / "pdfs" / f"{pid}.pdf"; pdf.output(str(pdf_path))
parsed_dir = ROOT / "f01" / "parsed_text"

def counts():
    c = db._conn
    return {t: c.execute(f"SELECT COUNT(*) FROM {t} WHERE paper_id=?", (pid,)).fetchone()[0]
            for t in ("full_text_assets", "parsed_text_refs", "parse_attempts")}

def files():
    return sorted(p.name for p in parsed_dir.iterdir())

real_rename = Path.rename
def boom(self, target):
    if str(self).endswith(".md.tmp"):
        raise OSError("injected: rename failed after commit")
    return real_rename(self, target)

MD = "# Title\n\n" + ("Long content sentence number one is here. " * 60)
stubs = [patch.object(pp, "parse_with_docling", return_value=MD),
         patch.object(pp, "parse_with_docling_ocr", side_effect=RuntimeError("ocr stubbed")),
         patch.object(pp, "parse_with_vision", side_effect=RuntimeError("vision stubbed"))]
for s in stubs: s.start()

print("before:", counts(), files())
with patch.object(Path, "rename", boom):
    try:
        pp.parse_pdf(str(pdf_path), pid, "f01", db, spec=spec)
        print("parse_pdf returned (unexpected)")
    except OSError as e:
        print("parse_pdf raised:", type(e).__name__, e)
print("after injected rename failure:", counts(), "files:", files())
print("  in_transaction after handler rollback:", db._conn.in_transaction)
row = db._conn.execute("SELECT parsed_text_path, parsed_text_version FROM parsed_text_refs WHERE paper_id=?", (pid,)).fetchone()
print("  committed ref ->", Path(row[0]).name, "v", row[1], "| exists on disk:", (parsed_dir / Path(row[0]).name).exists())
chk = sqlite3.connect(f"file:{db.db_path}?mode=ro", uri=True)
print("  seen by a second connection (i.e. committed):",
      chk.execute("SELECT COUNT(*) FROM parsed_text_refs").fetchone()[0], "ref row(s)"); chk.close()
try:
    load_parsed_text(db._conn, pid)
except Exception as e:
    print("resolver read:", type(e).__name__, "-", str(e)[:90])

# Retry, rename now healthy, no force: same-hash short-circuit
r = pp.parse_pdf(str(pdf_path), pid, "f01", db, spec=spec)
print("retry parse_pdf (no force): version", r.version, "parsed_markdown", r.parsed_markdown,
      "attempts", len(r.attempts or []), "| files:", files())
# The batch driver on the same state
print("status before parse_all_pdfs:", db._conn.execute("SELECT status FROM papers WHERE id=?", (pid,)).fetchone()[0])
stats = pp.parse_all_pdfs(db, "f01", spec=spec)
print("parse_all_pdfs stats:", {k: v for k, v in stats.items() if v})
print("status after parse_all_pdfs:", db._conn.execute("SELECT status FROM papers WHERE id=?", (pid,)).fetchone()[0],
      "| files:", files())
try:
    load_parsed_text(db._conn, pid)
except Exception as e:
    print("resolver read after PARSED:", type(e).__name__)
# force=True: does a re-parse repair or overwrite?
r = pp.parse_pdf(str(pdf_path), pid, "f01", db, spec=spec, force=True)
print("force re-parse: version", r.version, "| files:", files(), "| counts:", counts())
