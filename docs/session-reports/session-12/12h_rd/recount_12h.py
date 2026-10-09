"""12h — the R510 recount of the pre-tag list, from section L of the plan. Read-only.
usage, from the repository root: recount_12h.py docs/plan/ENGINE_REFACTOR_PLAN.md   (stdout: JSON)

Baseline: the count the plan records at the 12g close (parsed from its own sentence).
Deltas, all read from section L:
  - a row whose cell carries a "Note 2026-10-09 (12h rulings, R…)" that names a class: its
    class before is the leading digit of its Class cell (none = without a class), its class
    after is the first "Class N" in that note;
  - every row of the "L (12h additions)" table enters with the class in its Class cell.
"""
import json, re, sys
from collections import Counter

text = open(sys.argv[1], encoding="utf-8").read()
L = text[text.index("\n### L. "):text.index("\n### Not engine defects")]

m = re.search(r"\*\*Now (\d+) rows: Class 1 / 2 / 3 = (\d+) / (\d+) / (\d+), and (\d+) without a class\*\* "
              r"\(([^)]*)\)\. Rows out in 12g", L)
total, c1, c2, c3, unc = map(int, m.groups()[:5])
unclassed = {x.strip() for x in m.group(6).split(",")}
counts = Counter({"1": c1, "2": c2, "3": c3, "none": unc})
baseline = {"rows": total, "class_1": c1, "class_2": c2, "class_3": c3, "without_class": unc,
            "without_class_ids": sorted(unclassed)}

head, add = L.split("**L (12h additions).**")
moves, kept = [], []
for line in head.split("\n"):
    if not line.startswith("| "):
        continue
    cells = [c.strip() for c in line.strip().strip("|").split(" | ")]
    note = re.search(r"\*Note 2026-10-09 \(12h rulings, (R\d+)\):\*(.*?)(?=\*Note |$)", line)
    if not note or len(cells) < 3:
        continue
    after = re.search(r"Class (\d)", note.group(2))
    if not after:
        continue                                  # a fold-in note, not a class statement
    rid = cells[0]
    before = cells[2][0] if cells[2][:1].isdigit() else "none"
    assert (before == "none") == (rid in unclassed), (rid, before)
    rec = {"id": rid, "ruling": note.group(1), "before": before, "after": after.group(1)}
    if before != after.group(1):
        counts[before] -= 1; counts[after.group(1)] += 1
        if before == "none":
            unclassed.discard(rid)
        moves.append(rec)
    else:
        kept.append(rec)

entered = []
for line in add.split("\n"):
    if line.startswith("| INT-"):
        cells = [c.strip() for c in line.strip().strip("|").split(" | ")]
        counts[cells[2][0]] += 1
        entered.append({"id": cells[0], "class": cells[2][0], "ruling": cells[4]})

now = {"rows": total + len(entered), "class_1": counts["1"], "class_2": counts["2"],
       "class_3": counts["3"], "without_class": counts["none"],
       "without_class_ids": sorted(unclassed)}
assert now["rows"] == now["class_1"] + now["class_2"] + now["class_3"] + now["without_class"]
print(json.dumps({"baseline_12g_close": baseline, "class_changes": moves, "class_kept": kept,
                  "rows_in": entered, "rows_out": [], "now": now,
                  "r510": {"rows_in": len(entered), "rows_out": 0}}, indent=1, sort_keys=True))
