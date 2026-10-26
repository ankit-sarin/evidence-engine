"""12x-AQA P5 — read-only (mode=ro): papers counted by each pdf_quality column value.
The classifier writes no timestamp (its UPDATE sets four columns, not updated_at), so the
latest updated_at per pdf_quality_check_status is reported as the nearest the rows carry,
not as the classifier's time."""
import sqlite3
DB = "data/surgical_autonomy/review.db"
c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
q = lambda s: c.execute(s).fetchall()
print("papers", q("SELECT count(*) FROM papers")[0][0])
for col in ("pdf_quality_check_status", "pdf_ai_content_type", "pdf_ai_language"):
    print(f"-- {col}")
    for v, n in q(f"SELECT {col}, count(*) FROM papers GROUP BY {col} ORDER BY count(*) DESC, {col}"):
        print(f"   {v!r}: {n}")
print("-- pdf_ai_confidence: non-null, min, max, mean")
print("  ", q("SELECT count(pdf_ai_confidence), min(pdf_ai_confidence), max(pdf_ai_confidence), round(avg(pdf_ai_confidence),4) FROM papers")[0])
print("-- AI fields set but status not AI_CHECKED/HUMAN_CONFIRMED:",
      q("SELECT count(*) FROM papers WHERE pdf_ai_content_type IS NOT NULL AND coalesce(pdf_quality_check_status,'') NOT IN ('AI_CHECKED','HUMAN_CONFIRMED')")[0][0])
print("-- status x pdf_quality_check_status")
for r in q("SELECT status, pdf_quality_check_status, count(*) FROM papers WHERE pdf_quality_check_status IS NOT NULL GROUP BY 1,2 ORDER BY 3 DESC"):
    print("  ", r)
print("-- latest updated_at per pdf_quality_check_status (not the classifier's time)")
for r in q("SELECT pdf_quality_check_status, max(updated_at), min(updated_at) FROM papers GROUP BY 1 ORDER BY 1"):
    print("  ", r)
c.close()
