"""C3: a snippet-retry call that was sent, answered and recorded in run_calls, but
whose answer is not a JSON object, contributes no hash to the claims' context_chain."""
import json
from _w1f1_setup import *
from engine.agents.extractor import extract_paper
from engine.core.events import PAYLOAD_CONTEXT_CHAIN
from engine.core.parsed_text import resolve_parsed_text

tmp, db, cb, spec, run_id = make_review()
first = cb.fields_by_tier(1)[0]["name"]
p2 = pass2(cb, overrides={first: dict(value="NR", source_snippet="Twenty participants... completed.")})
with client([resp("draft"), p2, resp("Sorry, no single sentence."), resp("[]")]):
    with rm.active_run(db._conn, run_id):
        extract_paper(1, TEXT, spec, db, parsed_text_ref=resolve_parsed_text(db._conn, 1), run_id=run_id)

calls = [(r["stage"], r["outcome"]) for r in db._conn.execute(
    "SELECT stage, outcome FROM run_calls ORDER BY call_id")]
row = db._conn.execute("SELECT payload_json, source_snippet FROM field_events WHERE field_name = ? "
                       "AND event_type = 'asserted'", (first,)).fetchone()
chain = json.loads(row["payload_json"])[PAYLOAD_CONTEXT_CHAIN]
hashes = [r[0] for r in db._conn.execute("SELECT request_hash FROM run_calls ORDER BY call_id")]
print("run_calls rows:", calls)
print("context_chain length on the retried field's claim:", len(chain))
print("retry call hashes present in the chain:", [h in chain for h in hashes[2:]])
print("stored snippet for the retried field:", repr(row["source_snippet"]))
cleanup(tmp, db)
