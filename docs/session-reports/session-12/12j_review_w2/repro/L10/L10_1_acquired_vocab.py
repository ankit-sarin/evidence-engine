"""Acquisition-mode HTML offers a disposition the importer rejects."""
import inspect, re, sqlite3
from engine.acquisition import pdf_quality_html as H, pdf_quality_import as I
src = inspect.getsource(H.generate_acquisition_html)
opts = [o for o in re.findall(r'<option value="([^"]*)">', src) if o]
print("acquisition-mode options:", opts)
print("not in VALID_DISPOSITIONS:", [o for o in opts if o not in I.VALID_DISPOSITIONS])
conn = sqlite3.connect(":memory:"); conn.row_factory = sqlite3.Row
conn.execute("CREATE TABLE papers (id INTEGER PRIMARY KEY)"); conn.execute("INSERT INTO papers VALUES (1)")
# what buildPapers() emits for value 'ACQUIRED' (the final else branch)
print(I.validate_disposition_json({"review": "r", "mode": "acquisition", "complete": True,
    "papers": [{"paper_id": 1, "disposition": "ACQUIRED", "exclude_reason": None, "exclude_detail": None}]}, conn))
