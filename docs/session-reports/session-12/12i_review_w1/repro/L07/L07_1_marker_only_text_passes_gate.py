"""L07-C: a 'document' made only of the parser's own page markers is not EMPTY_TEXT.

Pure: imports engine.parsers.parse_quality only (pysbd; no docling, no fitz, no DB).
The strings below are built exactly as pdf_parser.parse_with_vision / parse_with_pymupdf
build them:  f"<!-- Page {n} -->\n{page_text}"  joined by "\n\n---\n\n",
with page_text = "" (vision model returned nothing / PyMuPDF on an image-only page).
"""
from engine.parsers.parse_quality import assess, Thresholds
from engine.core.review_spec import PDFParsing

thr = PDFParsing().scanned_text_threshold
print("scanned_text_threshold default:", thr)
for pages in (1, 2, 3, 4, 5, 6, 12, 60):
    for label, body in (("empty", ""), ("newline", "\n"), ("space", " ")):
        text = "\n\n---\n\n".join(f"<!-- Page {i + 1} -->\n{body}" for i in range(pages))
        stripped = text.strip()
        v = assess(text, Thresholds())
        print(f"pages={pages:>2} body={label:<7} len(strip)={len(stripped):>4} "
              f"empty={not stripped} sparse(<thr)={len(stripped) < thr} "
              f"is_empty_metric={v.metrics['is_empty']} verdict={v.describe()} "
              f"units={v.metrics['pysbd_units']} short%={v.metrics['short_unit_share_pct']} "
              f"cpu={v.metrics['chars_per_unit']}")
