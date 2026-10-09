"""12i scoping read, step 3d — papers 4, 23, 168 on live: counts only. Read-only (mode=ro).

usage, from the repository root:
    papers_4_23_168.py data/surgical_autonomy/review.db > papers_4_23_168.json
Counts per paper: papers.status; abstract decision rows by pass (and by decision within the pass);
abstract verification and abstract adjudication rows; FT decision rows by role (primary =
ft_screening_decisions, verifier = ft_verification_decisions, each by decision); FT adjudication
rows; paper events by axis and to_state (axis from engine.core.paper_state's two sets);
parsed_text_refs rows; full_text_assets and parse_attempts rows. No interpretation.
"""
import json, sqlite3, sys
from engine.core.paper_state import ELIGIBILITY_STATES, PROCESSING_STATES

c = sqlite3.connect("file:%s?mode=ro" % sys.argv[1], uri=True)
IDS = (4, 23, 168)
out = {"database": sys.argv[1], "papers_id_exists": {}, "papers": {}}
for pid in IDS:
    row = c.execute("SELECT id, status, source FROM papers WHERE id = ?", (pid,)).fetchone()
    out["papers_id_exists"][pid] = row is not None
    if row is None:
        continue
    q = lambda sql: c.execute(sql, (pid,)).fetchall()
    ev = {}
    for to_state, n in q("SELECT to_state, COUNT(*) FROM paper_events WHERE paper_id = ? GROUP BY 1"):
        axis = ("eligibility" if to_state in ELIGIBILITY_STATES else
                "processing" if to_state in PROCESSING_STATES else "neither")
        ev.setdefault(axis, {})[to_state] = n
    out["papers"][pid] = {
        "status": row[1], "source": row[2],
        "abstract_decisions_by_pass": {
            str(p): {"rows": n} for p, n in q(
                "SELECT pass_number, COUNT(*) FROM abstract_screening_decisions WHERE paper_id = ? GROUP BY 1")},
        "abstract_decisions_by_pass_and_decision": [
            list(r) for r in q("SELECT pass_number, decision, COUNT(*) FROM abstract_screening_decisions "
                               "WHERE paper_id = ? GROUP BY 1, 2 ORDER BY 1, 2")],
        "abstract_decisions_by_model": [
            list(r) for r in q("SELECT model, COUNT(*), MIN(decided_at), MAX(decided_at) FROM "
                               "abstract_screening_decisions WHERE paper_id = ? GROUP BY 1 ORDER BY 1")],
        "abstract_verification_rows": q(
            "SELECT COUNT(*) FROM abstract_verification_decisions WHERE paper_id = ?")[0][0],
        "abstract_adjudication_rows": q(
            "SELECT COUNT(*) FROM abstract_screening_adjudication WHERE paper_id = ?")[0][0],
        "ft_decisions_primary": [
            list(r) for r in q("SELECT decision, COUNT(*) FROM ft_screening_decisions WHERE paper_id = ? "
                               "GROUP BY 1 ORDER BY 1")],
        "ft_decisions_verifier": [
            list(r) for r in q("SELECT decision, COUNT(*) FROM ft_verification_decisions WHERE paper_id = ? "
                               "GROUP BY 1 ORDER BY 1")],
        "ft_adjudication_rows": q(
            "SELECT COUNT(*) FROM ft_screening_adjudication WHERE paper_id = ?")[0][0],
        "paper_events_by_axis_and_to_state": ev,
        "paper_events_rows": q("SELECT COUNT(*) FROM paper_events WHERE paper_id = ?")[0][0],
        "parsed_text_refs_rows": q("SELECT COUNT(*) FROM parsed_text_refs WHERE paper_id = ?")[0][0],
        "full_text_assets_rows": q("SELECT COUNT(*) FROM full_text_assets WHERE paper_id = ?")[0][0],
        "parse_attempts_rows": q("SELECT COUNT(*) FROM parse_attempts WHERE paper_id = ?")[0][0],
    }
# the population the three belong to: ABSTRACT_SCREENED_OUT papers with any FT decision row
out["abstract_screened_out_with_ft_rows"] = [r[0] for r in c.execute(
    "SELECT id FROM papers WHERE status = 'ABSTRACT_SCREENED_OUT' AND (id IN (SELECT paper_id FROM "
    "ft_screening_decisions) OR id IN (SELECT paper_id FROM ft_verification_decisions)) ORDER BY id")]
json.dump(out, sys.stdout, indent=1)
