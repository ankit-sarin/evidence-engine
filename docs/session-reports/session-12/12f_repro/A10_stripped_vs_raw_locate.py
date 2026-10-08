"""A10 — the engine's unit map is built from COMMENT-STRIPPED text; the audit's locator is
run against the RAW parsed text. Pure functions on synthetic strings; no DB, no model."""
from types import SimpleNamespace
from engine.elicitation.units import build_unit_map
from engine.elicitation.materialize import source_snippet
from engine.core.locator import locate

def show(label, raw, cite):
    um = build_unit_map(1, raw)
    snip = source_snippet(SimpleNamespace(indices=cite), um)
    r_raw, r_str = locate(raw, snip), locate(um.source_stripped, snip)
    print(f"\n[{label}] units={um.n} cite={cite}")
    for i, u in enumerate(um.units, 1): print(f"   S{i}: {u!r}")
    print(f"   stored snippet : {snip!r}")
    print(f"   locate vs RAW parsed text  : located={r_raw.located} kind={r_raw.kind} score={r_raw.score and round(r_raw.score,3)}")
    print(f"   locate vs stripped text    : located={r_str.located} kind={r_str.kind}")

show("control: no comment", "The trial enrolled 40 patients in total. Robotic suturing was fully autonomous here.", (1, 2))
show("comment INSIDE one short unit",
     "The trial enrolled <!-- image --> 40 patients in total. Robotic suturing was fully autonomous here.", (1,))
show("comment BETWEEN two cited adjacent units",
     "The trial enrolled 40 patients. <!-- formula-not-decoded --> Suturing was fully autonomous.", (1, 2))
long = ("The prospective single centre trial enrolled forty consecutive adult patients who underwent "
        "robot assisted <!-- image --> laparoscopic intestinal anastomosis with the autonomous suturing system during the study period.")
show("comment inside one LONG unit", long + " A second sentence follows here.", (1,))
