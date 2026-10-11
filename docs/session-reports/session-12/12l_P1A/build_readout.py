"""12l-P1A — renders the read-out from its parts.

usage, from the repository root:
    build_readout.py <head.md> <out.md> <quotes_checked>

<head.md> is the lead's prose for §0–§2 (its {COUNTS} and {QUOTES} marks are filled here).
§1's counts and §3–§5 are rendered from p1_plan.py (the proposed plan), p1_rows.json (the
plan's rows) and p1_evidence.json (the reads, whose quotes check_quotes.py verifies). Evidence
quotes are copied byte for byte from p1_evidence.json into fenced blocks.
"""
import json, os, sys
from collections import Counter
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import p1_plan as P

head, out, nq = sys.argv[1:4]
rows = {r["id"]: r for r in json.load(open(f"{HERE}/p1_rows.json", encoding="utf-8"))["rows"]}
ev = {r["id"]: r for r in json.load(open(f"{HERE}/p1_evidence.json", encoding="utf-8"))}
assert set(rows) == set(ev) == set(P.ROWS), (set(rows) ^ set(ev), set(rows) ^ set(P.ROWS))
unit_rows = [r for u in P.UNITS for r in u[2]]
assert set(unit_rows) == set(rows), set(unit_rows) ^ set(rows)
assert all(c == 1 or r == "C56" for r, c in Counter(unit_rows).items())
order = list(rows)

def one(s): return " ".join(str(s).split())
def cell(s): return one(s).replace("|", "\\|")

# ---- §1
st = Counter(ev[r]["status"] for r in order)
aff = [r for r in order if P.ROWS[r][0].startswith("affecting")]
moved = [r for r in order if "moved" in P.ROWS[r][2]]
cls_now = Counter(rows[r]["class"] for r in order)
cls_new = Counter(P.ROWS[r][2][0] for r in order)
fix_units = [u for u in P.UNITS if u[2]]
reach = Counter(ev[r].get("reachable_on_run7_path") for r in order)
counts = f"""| measure | count | rows |
| --- | ---: | --- |
| rows in P1 | {len(order)} | Class 1 / 2 / 3 = {cls_now['1']} / {cls_now['2']} / {cls_now['3']} as the plan carries them |
| confirmed at HEAD | {st['confirmed']} | every row not named in the next two lines |
| partly confirmed | {st['partly']} | {', '.join(r for r in order if ev[r]['status'] == 'partly')} |
| not confirmed | {st['not_confirmed']} | — |
| pin: affecting | {len(aff)} | {', '.join(aff)} |
| pin: not affecting | {len(order) - len(aff)} | the rest; reasons in §4 |
| class moves proposed | {len(moved)} | {', '.join(moved)} — P1 would be {cls_new['1']} / {cls_new['2']} / {cls_new['3']} (§2 D11) |
| fix units proposed | {len(fix_units)} | against 12k-EST's 37 (§2 D12); plus the comparison rehearsal as a placed step |
| on the Run 7 path: yes / conditional / no | {reach['yes']} / {reach['conditional']} / {reach['no']} | the readers' reading, per row in §4; "no" is the ten cloud rows (R71), the legacy-path row INT-i12-6, C49 (screening sites), and INT-i12-31 and INT-i12-38 (the live codebook does not trigger either) |

The four partly confirmed rows: B-F17 (item (f)'s "ends `exit 0`" does not hold — §2 D13); INT-i12-11 (the code is as described; the trigger is absent on this host today); INT-i12-8 (the locator half holds; the auditor half is off the pipeline — §2 D11); INT-i12-10 (the cloud half holds; the local path does not have the split).

Of the pin-affecting rows, INT-i12-1 is proposed against its row's own statement (§2 D2) and INT-g12-4 is affecting by arm-binding only. INT-i12-38 is stated "pin-affecting if changed" and is not affecting on this review, because its fix renders byte-identical text (A5 verified: the live codebook's canonical sentinel is "NR").

Sub-findings carried on the rows' dated notes were each read too: {sum(len(ev[r].get('sub_findings', [])) for r in order)} in all, none not confirmed except B-F17's item (f) and INT-i12-10's local half."""

# ---- §3
s3 = ["## 3. Unit plan, in order", "",
      "One handoff per unit (R607(c)). U01–U05 are the pin-affecting units; U06–U11 change what the extract path stores; U12 protects every model-calling run; U13 is the rehearsal; U14–U17 are audit-side; U18–U25 are the run's environment and loaders; U26–U36 are P1's tail (R542) and the cloud rows, none reachable on live while R71 holds. \"Files\" are the non-test files the readers expect a fix to touch; each unit's handoff confirms them.", ""]
for uid, name, urows, pin, tests, deps, note in P.UNITS:
    files = []
    for r in urows:
        for f in ev[r].get("files_a_fix_would_touch", []):
            f = f.split(" ")[0]
            if f.startswith(("engine/", "scripts/", "analysis/", "review_specs/", "requirements", "pyproject")) and f not in files:
                files.append(f)
    if uid == "U02": files = [f for f in files if "pdf_parser" not in f and "cloud" not in f]
    if uid == "U26": files = ["engine/parsers/pdf_parser.py"]
    if uid == "U27": files = ["engine/cloud/openai_extractor.py", "engine/cloud/anthropic_extractor.py"]
    s3 += [f"### {uid} — {name}", "",
           f"- **Rows:** {', '.join(urows) if urows else 'none'}",
           f"- **Pin effect:** {pin}",
           f"- **Files:** {', '.join('`' + f + '`' for f in files) if files else '—'}",
           f"- **Depends on:** {deps}"]
    for tname, asserts, kind, ruling in tests:
        s3.append(f"- **Acceptance test** ({kind}; discharges {ruling}): `{tname}` — {asserts}" if tname != "—"
                  else f"- **Step** ({kind}; {ruling}): {asserts}")
    if note: s3.append(f"- **Note:** {note}")
    s3.append("")

# ---- §4
s4 = ["## 4. Per-row table, with evidence", "",
      "Status is against the code at `82a33d7`. Class is the plan's unless marked moved. The evidence blocks follow the table, one per row, in the table's order; each quote is verbatim from the file and lines named. Where a row has more quotes than are printed here, the rest are in `p1_evidence.json`.", "",
      "| row | status | pin effect | reason | class | conditional trigger and reach on the Run 7 path | unit |", "| --- | --- | --- | --- | --- | --- | --- |"]
for r in order:
    pin, reason, cls, unit = P.ROWS[r]; e = ev[r]
    trig = f"{one(e.get('trigger', ''))} — reachable: {e.get('reachable_on_run7_path')} ({one(e.get('reachable_reason', ''))})"
    s4.append(f"| {r} | {e['status'].replace('_', ' ')} | {cell(pin)} | {cell(reason)} | {cell(cls)} | {cell(trig)} | {unit} |")
s4.append("")
def block(e_list, cap):
    out = []
    for q in e_list[:cap]:
        out += [f"`{q['file']}` lines {q['line_start']}–{q['line_end']}:", "", "````", q["quote"].rstrip("\n"), "````", ""]
    if len(e_list) > cap: out += [f"({len(e_list) - cap} more quote(s) in `p1_evidence.json`.)", ""]
    return out
for r in order:
    e = ev[r]
    s4 += [f"### {r} — {e['status'].replace('_', ' ')}", "", one(e["summary"]), ""]
    s4 += block(e.get("evidence", []), 4)
    for s in e.get("sub_findings", []):
        s4 += [f"**{s.get('label')}** — {str(s.get('status', '')).replace('_', ' ')}. {one(s.get('summary', ''))}", ""]
        s4 += block(s.get("evidence", []), 1)
    if e.get("existing_tests"):
        s4 += ["Existing tests that bear on it: " + "; ".join(one(t) for t in e["existing_tests"]) + ".", ""]

# ---- §5
s5 = ["## 5. New findings under R509 (class and package proposed, not acted on)", "",
      "The readers' side findings, in the order read. \"Seen at\" is the row being read when it was seen. None was searched for; none is acted on. Those marked \"new row proposed\" would enter the list; those marked \"inside\" or \"with\" a unit close with it and need no row unless ruled otherwise.", "",
      "| # | seen at | finding | class proposed | package | disposition proposed |", "| --- | --- | --- | --- | --- | --- |"]
i = 0
for f in ("A1", "A2", "B1", "B2", "C", "D"):
    pass
for r in [x["id"] for x in json.load(open(f"{HERE}/p1_evidence.json", encoding="utf-8"))]:
    for n in ev[r].get("new_findings", []):
        i += 1; c, p, d = P.SIDE[i]
        s5.append(f"| N{i:02d} | {r} | {cell(n)} | {c} | {p} | {cell(d)} |")
assert i == len(P.SIDE), (i, len(P.SIDE))
new_rows = [k for k, v in P.SIDE.items() if v[2].startswith("new row proposed")]
s5 += ["", f"New rows proposed outright: {len(new_rows)} ({', '.join('N%02d' % k for k in new_rows)}), all Class 3, P7. Two more are proposed through §2: the stage-set item (D3) and the repair scripts (D14).", "",
       "**Rows outside P1 seen to be pin-affecting (noted, not searched for):**",
       "- INT-i12-41 (P7): its prompt-text item is already ruled into the INT-h12-2 batch (R589) and is carried in U04.",
       "- N42, the unsendable stage in the elicited stage set: no row yet; §2 D3.",
       "- N02, `UNITS_VERSION` outside the pin: inside INT-i12-4's fix.",
       "- A-5, INT-i12-39 and INT-i12-40 (P7) name `units`, `contracts` and `prompts`, the files U01 and U04 edit. They were not read here; whoever writes U01's and U04's handoffs should read those three rows first, since a wording fix in those files after the binding pin would move it (R608).", ""]

text = open(head, encoding="utf-8").read().replace("{COUNTS}", counts).replace("{QUOTES}", nq)
open(out, "w", encoding="utf-8").write(text.rstrip("\n") + "\n\n" + "\n".join(s3) + "\n" + "\n".join(s4) + "\n" + "\n".join(s5))
print(json.dumps({"rows": len(order), "status": st, "affecting": aff, "moved": moved, "class_new": cls_new,
                  "fix_units": len(fix_units), "reach": reach, "side": i, "new_rows": len(new_rows)}, default=dict))
