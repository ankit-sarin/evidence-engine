"""Quality gate: (a) HUMAN_CONFIRMED re-checked and reset; (b) a failed check drops the paper from
every section of the review page; (c) importer ignores review/complete and cannot revise."""
import json, shutil, sqlite3
from pathlib import Path
from unittest.mock import patch
from engine.core.database import ReviewDatabase
from engine.search.models import Citation
from engine.acquisition import pdf_quality_check as QC, pdf_quality_html as H
from engine.acquisition.pdf_quality_import import import_dispositions
shutil.rmtree("data", ignore_errors=True)
root = Path("data")                      # cwd is the repro directory, not the repo
db = ReviewDatabase("q", data_root=root)
for i in range(3):
    db.add_papers([Citation(title=f"Paper {i}", pmid=str(200+i), source="pubmed", authors=["Smith J", "Doe A"], year=2024)])
ids = [r["id"] for r in db._conn.execute("SELECT id FROM papers ORDER BY id")]
for pid in ids:
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    f = root / "q" / "pdfs" / f"{pid}.pdf"; f.write_bytes(b"%PDF-1.4\n" + b"0" * 20000)
    db._conn.execute("UPDATE papers SET pdf_local_path=? WHERE id=?", (str(f), pid))
db._conn.commit(); db.close()
A, B, C = ids
calls = []
def fake_classify(img, **kw):
    calls.append(img)
    if img == str(B): raise TimeoutError("ollama not reachable")
    return {"language": "German", "content_type": "full_manuscript", "confidence": 0.9, "note": "x"}
def st():
    c = sqlite3.connect("data/q/review.db")
    return c.execute("SELECT id, status, pdf_quality_check_status, pdf_ai_language FROM papers ORDER BY id").fetchall()
with patch.object(QC, "_render_first_page", lambda p, dpi=150: Path(p).stem), patch.object(QC, "_classify_page", fake_classify):
    s = QC.run_quality_check("q"); print("run 1:", {k: s[k] for k in ("total", "checked", "failed")}); print(st())
    _, hs = H.generate_quality_html("q", output_path="q1.html"); print("page 1 sections:", hs)
    html = Path("q1.html").read_text(); print("paper B (failed check) has a row on the page:", f'data-pid="{B}"' in html)
    print("first-author cell:", html.split('<td class="ee">')[1].split("<td>")[2][:30])
    # human: A is fine (AI wrong) -> PROCEED.  File says review "some_other_review", complete False (a draft).
    Path("d1.json").write_text(json.dumps({"review": "some_other_review", "mode": "quality_check", "complete": False,
        "papers": [{"paper_id": A, "disposition": "PROCEED"}, {"paper_id": C, "disposition": "EXCLUDE_NON_ENGLISH"}]}))
    print("import 1 (other review's draft):", import_dispositions("q", "d1.json", data_root=root)); print(st())
    # final file revises C to PROCEED
    Path("d2.json").write_text(json.dumps({"review": "q", "complete": True, "papers": [{"paper_id": C, "disposition": "PROCEED"}]}))
    print("import 2 (final, C revised to PROCEED):", import_dispositions("q", "d2.json", data_root=root)); print(st())
    calls.clear(); s = QC.run_quality_check("q"); print("run 2:", {k: s[k] for k in ("total", "checked", "failed")}, "classified:", calls); print(st())
    _, hs = H.generate_quality_html("q", output_path="q2.html"); print("page 2 sections:", hs)
