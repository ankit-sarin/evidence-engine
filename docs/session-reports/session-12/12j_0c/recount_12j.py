"""12j item 0c — the R510 recount of the pre-tag list, from the plan. Read-only.
usage, from the repository root: recount_12j.py docs/plan/ENGINE_REFACTOR_PLAN.md   (stdout: JSON)

Baseline: the count the plan records at the 12h rulings (parsed from its own sentence), and the
list's membership as the plan gives it: the rows of section L's tables above "L (12i additions)",
plus the 21 inventory rows the 12g paragraph names ("Of the 61: …").
Deltas, all read from the plan:
  - every row of the "L (12i additions)" table enters with the leading digit of its Class cell;
  - a "Note 2026-10-10 (12j item 0c, …): Wave 1 row N (…) extends this row — Class C" on a row
    states a candidate's class. R597(d): the row takes the higher of its own class and the
    candidate's (Class 1 is the highest); a note never lowers a class.
      * a row on the list: its class before is the leading digit of its Class cell (section L)
        or the class in its 12g note (an inventory row); none = without a class;
      * a row not on the list: it has no class of its own, takes the candidate's, and so carries
        a dated note with a class and a package — the form by which the 12g paragraph counts an
        inventory row on the list. Counted as entering, and listed apart.
"""
import json, re, sys
from collections import Counter

text = open(sys.argv[1], encoding="utf-8").read()
step2 = text[text.index("\n## Step 2 "):text.index("\n### Coverage map")]
L = step2[step2.index("\n### L. "):step2.index("\n### Not engine defects")]
head, add = L.split("**L (12i additions).**")
add = add.split("**The pre-tag list, recounted 2026-10-10")[0]

def cells(line):
    return [c.strip() for c in line.strip().strip("|").split(" | ")]

m = re.search(r"\*\*Now (\d+) rows: Class 1 / 2 / 3 = (\d+) / (\d+) / (\d+), and (\d+) without a class\*\* "
              r"\(([^,)]*)[^)]*\)\. \*\*R510: rows in 2, rows out 0\.\*\*", head)
total, c1, c2, c3, unc = map(int, m.groups()[:5])
unclassed = {m.group(6).strip()}
counts = Counter({"1": c1, "2": c2, "3": c3, "none": unc})
baseline = {"rows": total, "class_1": c1, "class_2": c2, "class_3": c3, "without_class": unc,
            "without_class_ids": sorted(unclassed), "source": "the plan's 12h-rulings recount sentence"}

# membership of the list at the baseline
own = {}                                           # id -> class before ("1"/"2"/"3"/"none")
for line in head.split("\n"):
    if line.startswith("| ") and not line.startswith(("| #", "| ---", "| 12f ID")):
        c = cells(line)
        own[c[0]] = c[2][0] if c[2][:1].isdigit() else "none"
inv = re.search(r"21 existing inventory rows, each carrying a dated note of this date with its class and package \(([^)]*\)?[^)]*)\); and", head)
inv_ids = [x.strip() for x in inv.group(1).split(",")]
assert len(inv_ids) == 21, inv_ids
for line in step2.split("\n"):
    if line.startswith("| "):
        rid = cells(line)[0]
        if rid in inv_ids:
            k = re.search(r"12g item 0c, R541\):\* 12f triage — Class (\d)", line)
            own[rid] = k.group(1) if k else "unread"
for rid in inv_ids:
    own.setdefault(rid, "unread")
assert len(own) == total, (len(own), total)
assert {r for r, c in own.items() if c == "none"} >= unclassed

# the 12j candidate notes
NOTE = re.compile(r"\*Note 2026-10-10 \(12j item 0c, ([^)]*(?:\([a-z]\))?[^)]*)\):\* Wave 1 row (\d+)(, items? .*?)? \(((?:L|W1)[^)]*)\) "
                  r"extends this row — Class (\d)")
noted = {}
for line in step2.split("\n"):
    if not line.startswith("| "):
        continue
    rid = cells(line)[0]
    for k in NOTE.finditer(line):
        noted.setdefault(rid, []).append({"wave1_row": int(k.group(2)), "part": (k.group(3) or "").strip(", "),
                                          "reader_id": k.group(4), "candidate_class": k.group(5), "ruling": k.group(1)})

changes, kept, entering = [], [], []
for rid, cands in sorted(noted.items()):
    best = min(c["candidate_class"] for c in cands)
    cause = [f"Wave 1 row {c['wave1_row']}" + (f" {c['part']}" if c["part"] else "") + f" (Class {c['candidate_class']})" for c in cands]
    if rid in own:
        before = own[rid]
        assert before != "unread", rid
        after = best if before == "none" else min(before, best)
        rec = {"id": rid, "before": before, "after": after, "candidates": cause, "rule": "R597(d)"}
        if after != before:
            counts[before] -= 1; counts[after] += 1
            unclassed.discard(rid)
            changes.append(rec)
        else:
            kept.append(rec)
    else:
        counts[best] += 1
        entering.append({"id": rid, "class": best, "candidates": cause,
                         "rule": "R597(d): no class of its own, takes the candidate's; on the list by the 12g form"})

opened = []
for line in add.split("\n"):
    if line.startswith("| INT-"):
        c = cells(line)
        counts[c[2][0]] += 1
        opened.append({"id": c[0], "class": c[2][0], "conditional": c[2].startswith("1 (conditional") or c[2].startswith("1 (latent"),
                       "ruling": c[4]})
by_class = Counter(o["class"] for o in opened)

rows_now = total + len(opened) + len(entering)
now = {"rows": rows_now, "class_1": counts["1"], "class_2": counts["2"], "class_3": counts["3"],
       "without_class": counts["none"], "without_class_ids": sorted(unclassed)}
assert now["rows"] == now["class_1"] + now["class_2"] + now["class_3"] + now["without_class"]
expected = total + 66 + 0 + 10 + 1
print(json.dumps({
    "baseline_12h_rulings": baseline,
    "rows_opened": {"count": len(opened), "class_1": by_class["1"], "class_2": by_class["2"], "class_3": by_class["3"],
                    "class_1_conditional": sum(o["conditional"] for o in opened), "rows": opened},
    "existing_rows_entering_by_a_classed_note": entering,
    "class_changes": changes,
    "class_kept": kept,
    "now": now,
    "r510": {"rows_in": len(opened) + len(entering), "rows_in_opened": len(opened),
             "rows_in_existing_rows_entering": len(entering), "rows_out": 0},
    "expected_by_the_ruling": {"formula": "73 + 66 + n + 10 + 1, n = 0 (R597 assumption A8 did not hold; nothing opened under R597(b))",
                               "rows": expected, "difference": rows_now - expected,
                               "cause": "existing rows not on the list that carry a Wave 1 note with a class and package: "
                                        + ", ".join(e["id"] for e in entering)},
}, indent=1, sort_keys=True, ensure_ascii=False))
