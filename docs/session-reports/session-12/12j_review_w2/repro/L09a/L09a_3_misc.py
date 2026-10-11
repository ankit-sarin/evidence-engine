"""L09a reproducer 3 — SYNTHETIC temp database / pure functions.
(a) abstract JSON import: unknown paper_id raises mid-apply with the transaction left open;
    the next commit on the same connection persists the partial file
(b) categorizer: regex mode is matched against lowercased text; regex presence disables keywords
"""
import json, tempfile, io, contextlib, sqlite3
from pathlib import Path
from engine.core.database import ReviewDatabase
from engine.search.models import Citation
from engine.adjudication.screening_adjudicator import import_adjudication_decisions
from engine.adjudication.workflow import complete_stage
from engine.adjudication.categorizer import CategoryConfig, categorize_paper

with tempfile.TemporaryDirectory() as t:
    db = ReviewDatabase("synthetic", data_root=Path(t))
    db.add_papers([Citation(title=f"Paper {i}", abstract="a", pmid=f"P{i}", source="pubmed",
                            authors=["A"], journal="J", year=2024) for i in (1, 2)])
    for i in (1, 2): db.update_status(i, "ABSTRACT_SCREEN_FLAGGED")
    p = Path(t) / "a.json"
    p.write_text(json.dumps([{"paper_id": 1, "decision": "EXCLUDE"},
                             {"paper_id": 999, "decision": "INCLUDE"},
                             {"paper_id": 2, "decision": "INCLUDE"}]))
    print("== (a)")
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            import_adjudication_decisions(db, p)
    except sqlite3.IntegrityError as e:
        print("  raised:", type(e).__name__, e, "| in_transaction:", db._conn.in_transaction)
    # any later committing call on the same ReviewDatabase
    complete_stage(db._conn, "ABSTRACT_QUEUE_EXPORTED", metadata="unrelated later call")
    other = sqlite3.connect(str(db.db_path))
    print("  seen by a second connection:",
          other.execute("SELECT id,status FROM papers").fetchall(),
          other.execute("SELECT paper_id, adjudication_decision FROM abstract_screening_adjudication").fetchall())
    other.close(); db.close()

print("== (b)")
mk = lambda **k: CategoryConfig([{"name": "c", "description": "", "regex_patterns": [],
                                  "title_keywords": [], "abstract_keywords": [], "exclude_if_also": [], **k}])
print("  regex 'CNN' on title 'A CNN for tools'      ->", categorize_paper("A CNN for tools", "", mk(regex_patterns=["CNN"])))
print("  regex 'cnn' on the same title               ->", categorize_paper("A CNN for tools", "", mk(regex_patterns=["cnn"])))
print("  regex 'zzz' + title_keywords ['cnn']        ->", categorize_paper("A CNN for tools", "", mk(regex_patterns=["zzz"], title_keywords=["cnn"])))
