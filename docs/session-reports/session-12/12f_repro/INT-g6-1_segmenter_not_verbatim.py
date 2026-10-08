"""INT-g6-1 — units from the engine's unit map are not always verbatim text of their input
(independently of comment stripping). Pure functions on synthetic strings."""
from engine.elicitation.units import build_unit_map
from engine.core.locator import locate
for raw in ("It was used to fit a neural network model, i.e., the model was trained to fit the data. As a result it works.",
            "Springer Nature 2021. Vol.:(0123456789) is the footer line here. The next sentence follows it.",
            "The first half of the claim... and the second half of the claim. Another sentence here."):
    um = build_unit_map(1, raw)
    print(repr(raw))
    for i, u in enumerate(um.units, 1):
        r = locate(raw, u)
        print(f"   S{i}: {u!r}\n        verbatim in input: {u in raw} | locate: {r.kind} {r.score and round(r.score, 3)}")
    pair = " ".join(um.units[:2])
    print(f"   run S1+S2 joined verbatim in input: {pair in raw} | locate: {locate(raw, pair).kind}")
