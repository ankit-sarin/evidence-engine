"""12k item 0c — the R510 recount of the pre-tag list, from the plan. Read-only.
usage, from the repository root: recount_12k.py docs/plan/ENGINE_REFACTOR_PLAN.md   (stdout: JSON)

Baseline: the count the plan records at 12j-0c-FIX (parsed from its own sentence: 168 rows), and the
list's membership as the plan gives it: the rows of section L's tables above "L (12j additions)",
the 21 inventory rows the 12g paragraph names, and the six existing rows that entered at 12j item 0c
by a classed Wave 1 note (R598(b)).
Deltas, all read from the plan:
  - every row of the "L (12j additions)" table enters with the leading digit of its Class cell;
  - a note of 2026-10-11 (12k item 0c) "Wave 2 row N (…) extends this row — Class C, Pk" or
    "Wave 2 adjacent item(s) N (…) is/are noted on this row — Class C, Pk" states a candidate's class.
    R597(d): a row on the list takes the higher of its own class and the candidate's (Class 1 is the
    highest); a note never lowers a class.
  - a row not on the list that carries such a note has no class of its own, takes the highest
    candidate's, and enters (R598(b), applied to Wave 2 by the architect's reading in the 12k-OPEN brief).
  A note that states no class (a pointer, R603's notes, the J1 requirement notes) moves nothing.
Counts by package: the package is the first-named one — a section L row's coverage-map line (a dated
note on that line, where one moves the package, is read instead), else its
Owning-package cell; an inventory row's 12g note ("owning package Pk"), else the package of its highest
classed note. A row's class is its Class cell (section L) or its 12g note (inventory), moved only where
a recount paragraph of the plan says so (MOVED below, each with its ruling); the per-row classes are
checked against the plan's own total before anything is counted from them.
"""
import json, re, sys
from collections import Counter

text = open(sys.argv[1], encoding="utf-8").read()
step2 = text[text.index("\n## Step 2 "):text.index("\n### Coverage map")]
covmap = text[text.index("\n### Coverage map"):text.index("\n### Architect's assumptions in this step")]
L = step2[step2.index("\n### L. "):step2.index("\n### Not engine defects")]
head, add = L.split("**L (12j additions).**")
add = add.split("**The pre-tag list, recounted 2026-10-11")[0]

def cells(line):
    return [c.strip() for c in line.strip().strip("|").split(" | ")]
def rowline(rid, where=step2):
    rid = {"D26(b)": "D26"}.get(rid, rid)                    # the list names half (b) of row D26
    x = [l for l in where.split("\n") if l.startswith(f"| {rid} |")]
    return x[0] if x else None

m = re.search(r"\*\*Now (\d+) rows: Class 1 / 2 / 3 = (\d+) / (\d+) / (\d+), and (\d+) without a class\.\*\* "
              r"\*\*R510 \(2026-10-10, 12j-0c-FIX\)", head)
total, c1, c2, c3, unc = map(int, m.groups())
baseline = {"rows": total, "class_1": c1, "class_2": c2, "class_3": c3, "without_class": unc,
            "source": "the plan's 12j-0c-FIX recount sentence"}

# ---- membership and per-row class at the baseline ----
MOVED = {"A-8": ("3", "R564"), "INT-g12-3": ("3", "R566"), "INT-g12-5": ("3", "R570"), "INT-g12-4": ("2", "R569"),
         "A-9": ("3", "R572"), "A-6": ("3", "R597(d), Wave 1 row 86")}      # as the plan's 12g, 12h and 12j recount paragraphs state
own, kind = {}, {}
for line in head.split("\n"):
    if line.startswith("| ") and not line.startswith(("| #", "| ---", "| 12f ID")):
        c = cells(line)
        own[c[0]] = c[2][0] if c[2][:1].isdigit() else "none"; kind[c[0]] = "section L"
inv = re.search(r"21 existing inventory rows, each carrying a dated note of this date with its class and package \(([^)]*\)?[^)]*)\); and", head)
inv_ids = [x.strip() for x in inv.group(1).split(",")]
assert len(inv_ids) == 21
for rid in inv_ids:
    k = re.search(r"12g item 0c,[^)]*\):\* 12f triage[^—|]*— Class (\d)", rowline(rid))
    own[rid] = k.group(1); kind[rid] = "inventory (12g)"
W1 = re.compile(r"\*Note 2026-10-10 \(12j item 0c, [^)]*(?:\([a-z]\))?[^)]*\):\* Wave 1 row \d+(?:, items? .*?)? \((?:L|W1)[^)]*\) extends this row — Class (\d).*?, (P\d)")
entered_12j = {}
for line in step2.split("\n"):
    if line.startswith("| "):
        rid = cells(line)[0]
        if rid not in own and W1.search(line):
            cands = W1.findall(line)
            entered_12j[rid] = min(cands)                      # (class, package) of the highest-class note
for rid, (c, p) in entered_12j.items():
    own[rid] = c; kind[rid] = "entered at 12j by a note"
for rid, (c, _) in MOVED.items():
    own[rid] = c
assert len(own) == total, (len(own), total)
per = Counter(own.values())
assert (per["1"], per["2"], per["3"], per["none"]) == (c1, c2, c3, unc), per   # per-row classes reproduce the plan's total

# ---- the 12k notes ----
N2 = re.compile(r"\*Note 2026-10-11 \(12k item 0c, [^)]*(?:\([a-z]\))?[^)]*\):\* (Wave 2 row (\d+) \(([^)]*)\) extends this row|"
                r"Wave 2 adjacent items? ([\d and]+) \(seen by [^)]*\) (?:is|are) noted on this row) — Class (\d), (P\d)")
noted = {}
for line in step2.split("\n"):
    if not line.startswith("| "):
        continue
    rid = cells(line)[0]
    for k in N2.finditer(line):
        src = f"Wave 2 row {k.group(2)}" if k.group(2) else f"Wave 2 adjacent item {k.group(4)}"
        noted.setdefault(rid, []).append({"source": src, "reader_id": k.group(3), "candidate_class": k.group(5), "package": k.group(6)})

opened = []
for line in add.split("\n"):
    if line.startswith("| INT-"):
        c = cells(line)
        opened.append({"id": c[0], "class": c[2][0], "conditional": c[2].startswith("1 (conditional"),
                       "package": re.search(r"P\d", c[3]).group(0), "ruling": c[4]})
new_ids = {o["id"]: o for o in opened}

counts = Counter({"1": c1, "2": c2, "3": c3, "none": unc})
changes, kept, entering = [], [], []
for rid, cands in sorted(noted.items()):
    best = min(c["candidate_class"] for c in cands)
    cause = [f"{c['source']} (Class {c['candidate_class']}, {c['package']})" for c in cands]
    if rid in new_ids:                                         # a note on a row opened in this item
        o = new_ids[rid]
        assert best >= o["class"], rid                         # no candidate is higher than the row as opened
        kept.append({"id": rid, "before": o["class"], "after": o["class"], "candidates": cause, "rule": "R597(d)", "row": "opened at 12k item 0c"})
    elif rid in own:
        before = own[rid]; after = min(before, best)
        rec = {"id": rid, "before": before, "after": after, "candidates": cause, "rule": "R597(d)", "row": kind[rid]}
        if after != before:
            counts[before] -= 1; counts[after] += 1; own[rid] = after; changes.append(rec)
        else:
            kept.append(rec)
    else:
        counts[best] += 1
        pk = [c["package"] for c in cands if c["candidate_class"] == best][0]
        status = re.search(r"\*Note 2026-10-11 \(12k item 0c, R598\(b\), applied to Wave 2[^)]*\):\* (this row is reopened|only the classed Wave 2 finding)", rowline(rid))
        entering.append({"id": rid, "class": best, "package": pk, "candidates": cause,
                         "limiting_note": {"this row is reopened": "reopened for this finding only",
                                           "only the classed Wave 2 finding": "only this finding is pre-tag, in its package"}[status.group(1)],
                         "rule": "R597(d): no class of its own, takes the candidate's; on the list by R598(b) as applied to Wave 2"})
        own[rid] = best; kind[rid] = "entered at 12k by a note"
for o in opened:
    counts[o["class"]] += 1; own[o["id"]] = o["class"]; kind[o["id"]] = "opened at 12k item 0c"

rows_now = total + len(opened) + len(entering)
now = {"rows": rows_now, "class_1": counts["1"], "class_2": counts["2"], "class_3": counts["3"], "without_class": counts["none"]}
assert rows_now == sum(counts.values()) == len(own)

# ---- counts by first-named package and class ----
def package(rid):
    if rid in new_ids:
        return new_ids[rid]["package"], "Owning-package cell"
    for e in entering:
        if e["id"] == rid:
            return e["package"], "highest classed note (12k)"
    if rid in entered_12j:
        return entered_12j[rid][1], "highest classed note (12j)"
    line = rowline(rid)
    if kind[rid] == "inventory (12g)":
        k = re.search(r"12g item 0c,[^)]*\):\* 12f triage[^|]*?(?:owning package|names it in) (P\d)", line)
        if k:
            return k.group(1), "12g note"
    cl = rowline(rid, covmap)
    if cl and re.search(r"P\d", cells(cl)[1]):
        parts = [x for x in cells(cl)[1].split("*Note") if re.search(r"P\d", x)]      # a dated note on the line moves the package
        return re.search(r"P\d", parts[-1]).group(0), "coverage-map line" + (" (its dated note)" if len(parts) > 1 else "")
    c = cells(line)
    if kind[rid] == "section L" and re.search(r"P\d", c[3]):
        return re.search(r"P\d", c[3]).group(0), "Owning-package cell"
    k = re.search(r"(?:package|Class \d,) (P\d)", line)
    return (k.group(1), "a note on the row") if k else ("none stated", "-")
pk = {rid: package(rid) for rid in own}
table = {}
for rid, (p, _) in pk.items():
    table.setdefault(p, Counter())[own[rid]] += 1
by_package = {p: {"class_1": t["1"], "class_2": t["2"], "class_3": t["3"], "rows": sum(t.values())} for p, t in sorted(table.items())}
delta = {}
for o in opened: delta.setdefault(o["package"], Counter())[o["class"]] += 1
for e in entering: delta.setdefault(e["package"], Counter())[e["class"]] += 1
by_package_in = {p: {"class_1": t["1"], "class_2": t["2"], "class_3": t["3"], "rows": sum(t.values())} for p, t in sorted(delta.items())}

by_class = Counter(o["class"] for o in opened)
expected = total + 59 + len(entering)
print(json.dumps({
    "baseline_12j_0c_fix": baseline,
    "rows_opened": {"count": len(opened), "class_1": by_class["1"], "class_2": by_class["2"], "class_3": by_class["3"],
                    "class_1_conditional": sum(o["conditional"] for o in opened), "rows": opened},
    "existing_rows_entering_by_a_classed_note": entering,
    "class_changes": changes,
    "class_kept": kept,
    "now": now,
    "r510": {"rows_in": len(opened) + len(entering), "rows_in_opened": len(opened),
             "rows_in_existing_rows_entering": len(entering), "rows_out": 0},
    "by_package_and_class": {"rule": "first-named package; see the docstring", "whole_list": by_package, "rows_in_at_12k": by_package_in,
                             "package_source_counts": dict(Counter(s for _, s in pk.values())),
                             "rows_with_no_package_stated": sorted(r for r, (p, _) in pk.items() if p == "none stated")},
    "moved_classes_applied_to_the_baseline": {k: {"class": v[0], "ruling": v[1]} for k, v in MOVED.items()},
    "expected_by_the_brief": {"formula": "168 + 59 + k, k = existing rows entering by a note", "k": len(entering),
                              "rows": expected, "difference": rows_now - expected},
}, indent=1, sort_keys=True, ensure_ascii=False))
