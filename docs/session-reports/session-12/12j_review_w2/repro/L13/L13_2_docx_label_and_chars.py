"""L13 repro 2 — docx_export: the Study-label expression and python-docx on control chars.
No database: the expression is copied verbatim from export_evidence_docx."""
import inspect
from docx import Document
from engine.exporters import docx_export
src = inspect.getsource(docx_export.export_evidence_docx)
EXPR = 'study_label = f"{authors[0].split()[-1]} et al." if len(authors) > 1 else authors[0]'
print("[a] expression present in source:", EXPR in src)
def label(authors):
    study_label = f"{authors[0].split()[-1]} et al." if len(authors) > 1 else authors[0]
    return study_label
for a in (["Smith JA", "Jones B"],            # PubMed AU format (engine/search/pubmed.py: authors=rec.get("AU", []))
          ["John A. Smith", "B Jones"],       # OpenAlex display_name
          ["van der Berg P", "X Y"],
          ["Smith JA"],
          ["", "Jones B"], [" ", "Jones B"]):
    try:
        print("    ", a, "->", repr(label(a)))
    except Exception as e:
        print("    ", a, "->", type(e).__name__, e)
d = Document(); t = d.add_table(rows=1, cols=1)
for s in ("ok\x0cvalue", "ok\x0bvalue", "ok\x00", "a\tb\nc"):
    try:
        t.rows[0].cells[0].text = s; print("[b] cell.text", repr(s), "-> ok")
    except Exception as e:
        print("[b] cell.text", repr(s), "->", type(e).__name__, str(e)[:70])
print("[c] _cell_text keys vs effective constants:")
from engine.core import effective as E
for name in ("DECLINED", "CONTRACT_UNMET", "UNRESOLVED_REREVIEW", "UNRESOLVED_DUPLICATE", "WITHDRAWN", "MISSING", "OUT_OF_SCOPE"):
    v = getattr(E, name); print("    ", name, repr(v), "->", repr(docx_export._MARKED_STATES.get(v, "")))
