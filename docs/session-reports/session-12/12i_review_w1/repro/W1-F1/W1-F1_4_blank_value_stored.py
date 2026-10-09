"""C3: a blank value that carries a non-empty snippet passes both write-boundary
guards on the legacy path and is stored as an asserted (non-sentinel) value."""
from _w1f1_setup import *
from engine.agents.extractor import extract_paper
from engine.core.effective import effective_value, effective_state
from engine.core.parsed_text import resolve_parsed_text

tmp, db, cb, spec, run_id = make_review()
names = [f["name"] for t in (1, 2, 3, 4) for f in cb.fields_by_tier(t)]
p2 = pass2(cb, overrides={names[0]: dict(value=""), names[1]: dict(value="   ")})
with client([resp("draft"), p2]):
    with rm.active_run(db._conn, run_id):
        extract_paper(1, TEXT, spec, db, parsed_text_ref=resolve_parsed_text(db._conn, 1), run_id=run_id)
arm = spec.extraction_models.arm
for n in names[:2]:
    r = db._conn.execute("SELECT event_type, value, source_snippet FROM field_events "
                         "WHERE field_name = ?", (n,)).fetchone()
    ev = effective_value(db._conn, 1, n, arm, sentinels=frozenset(cb.absence_sentinels))
    print(f"{n}: event={r['event_type']} value={r['value']!r} snippet={r['source_snippet']!r} "
          f"-> reader row {ev.rule_row if hasattr(ev, 'rule_row') else ev[2]}, state {ev.state if hasattr(ev, 'state') else ev[1]}, "
          f"is_absence_sentinel={cb.is_absence_sentinel(r['value'])}")
print("paper processing:", effective_state(db._conn, 1).processing)
cleanup(tmp, db)
