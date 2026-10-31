#!/usr/bin/env python3
"""Extract the decision log's standing-rule clauses into docs/plan/STANDING_RULES.md (R746(e)(i)).

usage, from the repository root:
  extract_rules.py --plan <plan.md> --glosses <glosses.json> --out <rules.md>
  extract_rules.py --plan <plan.md> --list                     (ids and text, no file)
  extract_rules.py --plan <plan.md> --spot <n>                 (n clauses at fixed stride, each grepped in the plan)
  extract_rules.py --plan <plan.md> --ids <ids.txt> --glosses <glosses.json> [--glosses <more.json>] --out <rules.md>
  (--ids also narrows --list and --spot to the listed clauses)

A clause is a ruling's unit of text: `R<n>(<letter>) — ...` at a fence line's start or inside a table row's Ruling
cell, or `R<n> — ...` for a ruling with no such marker, whole. Text before a fence's first clause (STATUS,
ASSUMPTIONS) is not a clause. A clause is selected if, whitespace normalised, it contains "from here" or "the
standing form" ("Rule from here" contains the first). Fence and row copies of one id are de-duplicated; where they
differ, both are quoted. Every clause text is taken from the plan; only the glosses are composed, and they are read
from the file named, keyed by id. A selected clause with no gloss, or a gloss for no selected clause, is a refusal.

With --ids the selection is a committed list, not the phrase (R747(c)). One entry per line, blank lines and "#" comments
allowed: "A <id>" and "B <id>" select a clause into Tier A or Tier B; "D <id>" records a drop, so that a gloss for it in
a named file is no refusal and it stays out of the output. A listed id with no clause, or a line in no other form, is a
refusal. parse() and clauses() are the phrase mode's.
"""
import argparse, hashlib, json, re, subprocess, sys
from pathlib import Path

ROW = re.compile(r"^\| (20\d\d-\d\d-\d\d) \| (.*)$")
MARK = re.compile(r"^R(\d+)(\([a-z]\))? — ")
PHRASE = re.compile(r"\bfrom here\b|\bthe standing form\b", re.I)


def norm(text: str) -> str:
    return " ".join(text.split()).replace("\\\\\\|", "|").replace("\\|", "|")


def parse(lines: list[str]):
    """Every clause copy in the log, in plan order: {id, n, date, session, source, line, text}."""
    start = lines.index("## Decision log")
    copies, heading_session, i = [], None, start
    status = (None, None)
    while i < len(lines):
        line = lines[i]
        if line.startswith("## Decision log"):
            m = re.search(r"session (\S+)", line)
            heading_session = m.group(1) if m else None
        if line == "```text" and lines[i + 1].startswith("RULING"):
            j = next(k for k in range(i + 1, len(lines)) if lines[k] == "```")
            body = lines[i + 1:j]
            head = " ".join(body[:8])
            d = re.search(r"20\d\d-\d\d-\d\d", head); s = re.search(r"session (\w[\w-]*)", head)
            status = (d.group(0) if d else None, s.group(1) if s else None)
            cur = None
            for k, l in enumerate(body):
                m = MARK.match(l)
                if m:
                    cur = {"id": f"R{m.group(1)}{m.group(2) or ''}", "n": int(m.group(1)), "date": status[0],
                           "session": status[1], "source": "fence", "line": i + 2 + k, "text": [l]}
                    copies.append(cur)
                elif cur is not None:
                    cur["text"].append(l)
            i = j + 1
            continue
        m = ROW.match(line)
        if m:
            cells = re.split(r"(?<!\\) \| ", m.group(2))
            cell = cells[0]
            first = re.match(r"\*{0,2}R(\d+)(\([a-z]\))?", cell)
            sess = re.search(r"during session (\w[\w-]*)", " ".join(cells[1:]))
            session = sess.group(1) if sess else heading_session
            if first:
                n = first.group(1)
                parts = re.split(rf"(?:^|(?<=\s))(?=R{n}\([a-z]\) — )", cell) if first.group(2) else [cell]
                for p in (x for x in parts if x.strip()):
                    pm = re.match(rf"R{n}(\([a-z]\)) — ", p)
                    copies.append({"id": f"R{n}{pm.group(1) if pm else ''}", "n": int(n), "date": m.group(1),
                                   "session": session, "source": "row", "line": i + 1, "text": [p.strip()]})
        i += 1
    return copies


def clauses(copies):
    """One entry per id, in first-seen order; `copies` keeps each distinct text."""
    out = {}
    for c in copies:
        e = out.setdefault(c["id"], {"id": c["id"], "n": c["n"], "date": c["date"], "session": c["session"], "copies": []})
        e["date"] = e["date"] or c["date"]; e["session"] = e["session"] or c["session"]
        e["copies"].append(c)
    for e in out.values():
        e["all"] = " ".join(norm("\n".join(c["text"])) for c in e["copies"])
        e["distinct"] = len({norm("\n".join(c["text"])) for c in e["copies"]})
    return list(out.values())


def named_by(e, every):
    pat = re.compile(re.escape(e["id"]) + (r"(?![a-z0-9])" if "(" in e["id"] else r"(?![a-z0-9(])"))
    return [o["id"] for o in every if o["n"] > e["n"] and pat.search(o["all"])]


def quote(c) -> str:
    return "```text\n" + "\n".join(c["text"]) + "\n```"


def selected(lines):
    every = clauses(parse(lines))
    return every, [e for e in every if PHRASE.search(e["all"])]


def read_ids(path):
    """The id list: {"A": [...], "B": [...], "D": [...]}, each id once."""
    tiers = {"A": [], "B": [], "D": []}
    for n, raw in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        m = re.fullmatch(r"([ABD]) (R\d+(?:\([a-z]\))?)", line)
        if not m:
            sys.exit(f"REFUSED: {path} line {n} is not '<A|B|D> <id>': {raw!r}")
        if any(m.group(2) in t for t in tiers.values()):
            sys.exit(f"REFUSED: {path} line {n} repeats {m.group(2)}")
        tiers[m.group(1)].append(m.group(2))
    return tiers


def write_rules(path, preface, groups, every, glosses):
    """The output file: the preface lines, then each group's clauses under its heading, if it has one."""
    out = list(preface)
    for heading, sel in groups:
        if heading:
            out += [heading, ""]
        for e in sel:
            out += [f"## {e['id']}", "", f"{e['date']}, session {e['session']}.", ""]
            seen = {}
            for c in e["copies"]:
                seen.setdefault(norm("\n".join(c["text"])), c)
            if len(seen) > 1:
                out += [f"*The fence and the table row differ; {len(seen)} texts are quoted.*", ""]
            for c in seen.values():
                out += [quote(c), ""]
            out += [f"Named by: {', '.join(named_by(e, every)) or 'none'}.", "", f"Gloss (CC's, not ruled): {glosses[e['id']]}", ""]
    Path(path).write_text("\n".join(out), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True); ap.add_argument("--glosses", action="append"); ap.add_argument("--out")
    ap.add_argument("--list", action="store_true"); ap.add_argument("--spot", type=int); ap.add_argument("--ids")
    a = ap.parse_args()
    raw = Path(a.plan).read_bytes(); lines = raw.decode("utf-8").split("\n")
    every, sel = selected(lines)
    if a.ids:
        tiers = read_ids(a.ids); known = {e["id"] for e in every}
        missing = [i for t in tiers.values() for i in t if i not in known]
        if missing:
            sys.exit(f"REFUSED: listed id with no clause {missing}")
        groups = {t: [e for e in every if e["id"] in tiers[t]] for t in "AB"}
        sel = groups["A"] + groups["B"]
    if a.list:
        for e in sel:
            print(f"{e['id']} [{e['date']} {e['session']}] copies {len(e['copies'])} distinct {e['distinct']}")
            print("   ", norm("\n".join(e["copies"][0]["text"])))
        print(f"# clauses in the log {len(every)}; selected {len(sel)}")
        return 0
    if a.spot:
        ok = True; stride = max(1, len(sel) // a.spot)
        for e in sel[::stride][:a.spot]:
            for c in e["copies"]:
                good = True
                for l in c["text"]:
                    r = subprocess.run(["grep", "-cF", "--", l.strip(), a.plan], capture_output=True, text=True)
                    good &= r.returncode == 0 and int(r.stdout) >= 1
                ok &= good
                print(f"{e['id']} {c['source']} line {c['line']} ({len(c['text'])} lines): verbatim {'True' if good else 'False'}")
        print("RESULT:", "PASS" if ok else "FAIL"); return 0 if ok else 1
    glosses = {}
    for g in a.glosses:
        glosses.update(json.loads(Path(g).read_text(encoding="utf-8")))
    ids = {e["id"] for e in sel}
    spare = set(tiers["D"]) if a.ids else set()
    if ids - set(glosses) or set(glosses) - ids - spare:
        sys.exit(f"REFUSED: no gloss for {sorted(ids - set(glosses))}; gloss for no clause {sorted(set(glosses) - ids - spare)}")
    me = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    preface = ["# Standing rules (extracted, not adopted)", "",
               "**Status: extracted, not adopted" + (", selected by the committed id list" if a.ids else "") + ".** Under R746(e) this file is adopted only by a later PI ruling after the "
               "architect has read it; until then every brief stays in its present form. Clause text is the plan's, "
               "by script; only the lines headed \"Gloss (CC's, not ruled):\" are composed, and they rule nothing.", "",
               f"- Generator: `docs/session-reports/tools/extract_rules.py`, sha256 `{me}`",
               f"- Plan blob: `{a.plan}`, sha256 `{hashlib.sha256(raw).hexdigest()}`"]
    if a.ids:
        preface += [f"- Id list: `{a.ids}`, sha256 `{hashlib.sha256(Path(a.ids).read_bytes()).hexdigest()}`",
                    f"- Selection: the id list: Tier A {len(groups['A'])}, Tier B {len(groups['B'])}, {len(sel)} of {len(every)} clauses in the log."]
        out_groups = [("# Tier A — process rules", groups["A"]), ("# Tier B — engine governance, pending the adoption ruling", groups["B"])]
    else:
        preface += [f"- Selection: clauses containing \"from here\" or \"the standing form\", whitespace normalised (\"Rule from here\" contains the first): {len(sel)} of {len(every)} clauses in the log."]
        out_groups = [(None, sel)]
    preface += ["- \"Named by\" lists later clauses that carry the exact id; a range or a bare ruling number does not count.", ""]
    write_rules(a.out, preface, out_groups, every, glosses)
    print(f"wrote {a.out}: {len(sel)} clauses of {len(every)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
