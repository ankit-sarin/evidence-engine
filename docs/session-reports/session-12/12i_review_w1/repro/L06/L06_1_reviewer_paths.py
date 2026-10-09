"""L06 reproducer — reviewer paths in engine/core/effective.py::effective_value.

Synthetic temporary database only (tempfile, removed at exit). No model, no
network, no repo write. Histories are built through events.write_field_event,
the same way tests/test_effective_reader.py builds them.
"""
import sqlite3, sys, tempfile, os
sys.dont_write_bytecode = True
REPO = "/home/ankitsarin/projects/evidence-engine"
sys.path.insert(0, os.path.join(REPO, "tests"))
from _event_store_fixture import (FIXTURE_CONTEXT_SHA, FIXTURE_TEXT_SHA, claim_identity,
                                  fixture_run, upgrade_event_store)
from engine.core import events
from engine.core.effective import effective_value

SENT = frozenset({"NR", "N/A", "NA", "NOT_FOUND", "NOT FOUND", "NOT REPORTED"})
FIELD = "primary_outcome_value"

def make_db(d, name):
    path = os.path.join(d, name)
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE papers (id INTEGER PRIMARY KEY, status TEXT)")
    c.executemany("INSERT INTO papers (id, status) VALUES (?, 'FT_ELIGIBLE')", [(i,) for i in range(1, 6)])
    c.commit(); c.close()
    upgrade_event_store(path)
    c = sqlite3.connect(path)
    events.register_arm(c, "local", "model", configuration={"model": "m"})
    fixture_run(c, "local"); c.commit()
    return c

def claim(c, paper, value, cid=None, uid=None):
    uid = uid or events.mint_extraction_uid()
    cid = cid or events.make_claim_id("local", uid, FIELD)
    events.write_field_event(c, event_type="asserted", paper_id=paper, field_name=FIELD,
        arm="local", claim_id=cid, extraction_uid=uid, value=value, source_snippet="q",
        actor_kind="model", actor_role="extractor", actor_name="m", sentinels=SENT,
        payload=claim_identity("local", paper), run_id=fixture_run(c),
        presented_context_sha256=FIXTURE_CONTEXT_SHA)
    return cid, uid

def review(c, paper, etype, against=(), against_decisions=(), value=None, who="PI"):
    return events.write_field_event(c, event_type=etype, paper_id=paper, field_name=FIELD,
        arm="local", claim_id=sorted(against)[0] if against else "n/a", value=value,
        actor_kind="human", actor_role="reviewer", actor_name=who, against_claims=against,
        against_decisions=against_decisions, presented_context_sha256="ctx-" + etype,
        sentinels=SENT, run_id=fixture_run(c))

def supersede(c, paper, ids):
    events.write_field_event(c, event_type="superseded", paper_id=paper, field_name=FIELD,
        arm="local", claim_id=sorted(ids)[0], actor_kind="engine", actor_role="system",
        actor_name="write-path", against_claims=ids, sentinels=SENT, run_id=fixture_run(c))

def show(label, r):
    keys = sorted(k for k in r.provenance)
    print(f"{label}: value={r.value!r} state={r.state!r} row={r.rule_row} prov_keys={keys}")

with tempfile.TemporaryDirectory(dir=os.getcwd()) as d:
    c = make_db(d, "a.db")

    print("== A. row 2's exit written with against_decisions only (no against_claims)")
    cid, _ = claim(c, 1, "5")
    a = review(c, 1, "human_corrected", against={cid}, value="6", who="PI-a")
    b = review(c, 1, "human_withdrew", against={cid}, who="PI-b")
    show("A before exit", effective_value(c, 1, FIELD, "local", sentinels=SENT))
    review(c, 1, "human_corrected", against_decisions={a, b}, value="7", who="PI-c")
    print("   writer accepted the decision-only resolving event (no refusal)")
    show("A after exit ", effective_value(c, 1, FIELD, "local", sentinels=SENT))

    print("== B1. ACCEPT on ONE claim id carrying two differing asserted values (row 6)")
    cid, uid = claim(c, 2, "5")
    claim(c, 2, "50", cid=cid, uid=uid)
    show("B1 before    ", effective_value(c, 2, FIELD, "local", sentinels=SENT))
    try:
        review(c, 2, "human_accepted", against={cid})
        print("   writer accepted ACCEPT (no AcceptAgainstMultipleClaims)")
    except events.EventRefused as e:
        print("   writer REFUSED:", type(e).__name__)
    r = effective_value(c, 2, FIELD, "local", sentinels=SENT)
    show("B1 after     ", r); print("   endorsed:", r.provenance.get("endorsed"))

    print("== B2. ACCEPT first, a second differing asserted event on the same claim id later")
    cid, uid = claim(c, 3, "5")
    review(c, 3, "human_accepted", against={cid})
    show("B2 accepted  ", effective_value(c, 3, FIELD, "local", sentinels=SENT))
    claim(c, 3, "50", cid=cid, uid=uid)
    r = effective_value(c, 3, FIELD, "local", sentinels=SENT)
    show("B2 after dup ", r); print("   endorsed:", r.provenance.get("endorsed"))

    print("== C. row 3's stated exit ('a new decision against the current claim')")
    c1, _ = claim(c, 4, "5")
    review(c, 4, "human_accepted", against={c1})
    supersede(c, 4, {c1})
    events.write_field_event  # (new claim under a new extraction)
    c2, _ = claim(c, 4, "6")
    r = effective_value(c, 4, FIELD, "local", sentinels=SENT)
    show("C stale      ", r); print("   exit text:", r.provenance.get("exit"))
    review(c, 4, "human_corrected", against={c2}, value="9")
    r = effective_value(c, 4, FIELD, "local", sentinels=SENT)
    show("C after exit ", r); print("   exit text:", r.provenance.get("exit"))

    print("== D. sentinel row number: exact membership vs Codebook.is_absence_sentinel")
    for v in ("NR", "nr", "Not reported", " NR"):
        cid, _ = claim(c, 5, v)
        r = effective_value(c, 5, FIELD, "local", sentinels=SENT)
        print(f"   value={v!r}: row={r.rule_row} state={r.state!r}")
        supersede(c, 5, {cid})
    c.close()
