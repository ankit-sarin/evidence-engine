"""C2: at budget exhaustion the stored outcome is the LAST attempt's alone.
(a) three attempts, a different field uncited in each -> the field uncited only on
    attempt 3 is stored contract_unmet with attempts=3; the fields that failed on
    attempts 1 and 2 store attempt 3's values.
(b) attempts 1-2 produce 19 cited fields each, attempt 3 is unparseable -> nothing
    stored, paper extraction_failed/response_unparseable."""
import json
from unittest.mock import patch
from _w1f1_setup import *
from engine.agents import extractor as ex

def run(case, responses):
    tmp, db, cb, spec, run_id = make_review()
    names = [f["name"] for t in (1, 2, 3, 4) for f in cb.fields_by_tier(t)]
    A, B, C = names[0], names[1], names[2]
    seq = responses(cb, A, B, C)
    with client(seq), patch("engine.utils.ollama_preflight.require_preflight"):
        ex.run_extraction(db, spec, "w1f1", restart_every=0, experiment_lock=False, run_id=run_id)
    print(f"--- case {case}: fields A={A} B={B} C={C}")
    print("extractor paper events:", paper_events(db))
    for n in (A, B, C):
        r = db._conn.execute("SELECT event_type, value, payload_json FROM field_events "
                             "WHERE field_name = ? AND event_type IN ('asserted','contract_unmet')",
                             (n,)).fetchone()
        if r is None:
            print(f"  {n}: no claim")
        else:
            p = json.loads(r["payload_json"])
            print(f"  {n}: {r['event_type']} value={r['value']!r} attempts={p.get('attempts')} "
                  f"violation_codes={p.get('violation_codes')}")
    n_claims = db._conn.execute("SELECT COUNT(*) FROM field_events WHERE event_type IN "
                                "('asserted','contract_unmet','declined')").fetchone()[0]
    print("  claim-bearing events stored:", n_claims)
    cleanup(tmp, db)

unc = lambda v: dict(value=v, source_snippet="")
run("a", lambda cb, A, B, C: [
    resp("d"), pass2(cb, value="attempt1", overrides={A: unc("attempt1")}),
    resp("d"), pass2(cb, value="attempt2", overrides={B: unc("attempt2")}),
    resp("d"), pass2(cb, value="attempt3", overrides={C: unc("attempt3")})])
run("b", lambda cb, A, B, C: [
    resp("d"), pass2(cb, value="attempt1", overrides={A: unc("attempt1")}),
    resp("d"), pass2(cb, value="attempt2", overrides={A: unc("attempt2")}),
    resp("d"), resp('{"fields": [{"field_name": "trunc')])
