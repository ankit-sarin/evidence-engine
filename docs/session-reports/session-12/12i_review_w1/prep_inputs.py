"""12i-REVIEW-W1 — per-reader input files (R584). Read-only on the repository.

usage, from the repository root:  prep_inputs.py <repo_root> <lots_w1.json> <out_dir>
For each lot writes <out_dir>/<LOT>.md with: the lot's files, in-scope lines, and any excluded
function with the reason; the inventory rows (plan Step 2, sections A–L) that name one of the
lot's files; the 12f_triage_rows.md sections that name one.
A row "names" a file when its text contains `<stem>.py`, the dotted `<package>.<stem>`, or — for
a stem containing an underscore — the bare stem. Row text is cut to 600 characters; the reader
greps the plan for the ID to read the whole row. "closed?" is whether the row text contains
CLOSED (a hint, not a status reading).
"""
import json, os, re, sys

root, lots_path, out = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(out, exist_ok=True)
plan = open(os.path.join(root, "docs/plan/ENGINE_REFACTOR_PLAN.md"), encoding="utf-8").read()
step2 = plan[plan.index("\n## Step 2 "):plan.index("\n## Step 3 ")]
rows = []
for line in step2.split("\n"):
    m = re.match(r"\| ([A-Za-z][A-Za-z0-9\-]*) \| (.*)", line)
    if m and m.group(1) not in ("ID", "Row"):
        rows.append((m.group(1), m.group(2)))
tri = open(os.path.join(root, "docs/session-reports/session-12/12f_triage_rows.md"), encoding="utf-8").read()
sections = re.split(r"\n(?=### )", tri)
sections = [(re.match(r"### (\S+) — (.*)", s).groups(), s) for s in sections if re.match(r"### \S+ — ", s)]


def patterns(unit):
    stem = os.path.basename(unit)[:-3]
    pkg = os.path.basename(os.path.dirname(unit))
    pats = [re.escape(stem + ".py"), r"\b" + re.escape(pkg + "." + stem) + r"\b"]
    if "_" in stem:
        pats.append(r"(?<![\w/])" + re.escape(stem) + r"(?![\w])")
    return re.compile("|".join(pats))


data = json.load(open(lots_path))
index = {}
for lot in data["lots"]:
    units = lot["units"] + lot["extra"]
    with open(os.path.join(out, lot["lot"] + ".md"), "w", encoding="utf-8") as f:
        f.write("# %s — %s\n\nYour lot: %d files, %s in-scope lines. Read every file below end to end.\n\n"
                % (lot["lot"], lot["title"], len(units), format(lot["lines"], ",")))
        f.write("| file | in-scope lines | scope |\n| --- | ---: | --- |\n")
        for u in units:
            F = u.get("F")
            if "note" in u:
                scope = "whole file (%s)" % u["note"]
            elif not F:
                scope = "whole file"
            elif F["whole_file"]:
                scope = "whole file (outside findings %s touch this file but name no function; 12f triaged them — see the sections below)" % ", ".join(F["cites"])
            elif F.get("block_only"):
                scope = "whole file EXCEPT the block B-F01 located inside `parse_pdf`: the temp-file write → INSERTs → commit → rename sequence under the comment `# Atomic write: temp file → DB commit → rename`. The rest of `parse_pdf` IS in scope."
            else:
                scope = "whole file EXCEPT %s (%s; triaged in 12f)" % (", ".join("`%s`" % k for k in F["excluded"]), F["why"])
            f.write("| `%s` | %d | %s |\n" % (u["unit"], u["in_scope_lines"], scope))
        hit_rows, hit_secs = [], []
        for u in units:
            p = patterns(u["unit"])
            for rid, text in rows:
                if p.search(text):
                    hit_rows.append((rid, text, u["unit"]))
            for (sid, title), body in sections:
                if p.search(body):
                    hit_secs.append((sid, title, u["unit"]))
        f.write("\n## Existing inventory rows naming your files (plan, Step 2)\n\n"
                "`grep -n '^| <ID> |' docs/plan/ENGINE_REFACTOR_PLAN.md` then read that line for the full row.\n\n")
        seen = {}
        for rid, text, u in hit_rows:
            seen.setdefault(rid, [text, []])[1].append(os.path.basename(u))
        for rid, (text, us) in seen.items():
            f.write("- **%s** (names %s; closed? %s) — %s%s\n" % (
                rid, ", ".join(sorted(set(us))), "yes" if "CLOSED" in text else "no",
                text[:600].replace("\n", " "), "…" if len(text) > 600 else ""))
        if not seen:
            f.write("- none found by the matcher\n")
        f.write("\n## 12f triage sections naming your files (docs/session-reports/session-12/12f_triage_rows.md)\n\n")
        seen2 = {}
        for sid, title, u in hit_secs:
            seen2.setdefault((sid, title), []).append(os.path.basename(u))
        for (sid, title), us in seen2.items():
            f.write("- **%s** — %s (names %s)\n" % (sid, title, ", ".join(sorted(set(us)))))
        if not seen2:
            f.write("- none found by the matcher\n")
    index[lot["lot"]] = {"rows": sorted(seen), "triage_sections": sorted(s for s, _ in seen2)}
json.dump(index, open(os.path.join(out, "index.json"), "w"), indent=1)
for k, v in index.items():
    print(k, len(v["rows"]), "rows;", len(v["triage_sections"]), "12f sections")
