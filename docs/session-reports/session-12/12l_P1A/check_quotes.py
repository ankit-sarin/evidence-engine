"""12l-P1A step 2 — every quote in the per-row evidence is an exact substring of its file
at the commit named.

usage, from the repository root:
    check_quotes.py <commit> <evidence.json> [<evidence.json> ...]

Each evidence file is a list of row objects; a row carries `evidence` and `sub_findings[*].evidence`,
each item {"file", "line_start", "line_end", "quote"}. A repo-relative file is read from git at
<commit> (`git show <commit>:<file>`), so the working tree cannot stand in for it; an absolute
path (a file outside the repository, e.g. the nightly script) is read from disk and reported as such.
Also checks that the quote lies within the stated line range (±3 lines). Exits 1 on any failure.
"""
import json, subprocess, sys

commit, files = sys.argv[1], sys.argv[2:]
cache = {}
def text(path):
    if path not in cache:
        if path.startswith("/"):
            cache[path] = ("disk", open(path, encoding="utf-8").read())
        else:
            p = subprocess.run(["git", "show", f"{commit}:{path}"], capture_output=True, text=True)
            cache[path] = ("git", p.stdout) if p.returncode == 0 else ("missing", None)
    return cache[path]

n = bad = outside = 0
fails = []
for f in files:
    for row in json.load(open(f, encoding="utf-8")):
        groups = [(row["id"], row.get("evidence", []))] + [
            (f'{row["id"]} / {s.get("label", "?")}', s.get("evidence", [])) for s in row.get("sub_findings", [])]
        for label, evs in groups:
            for e in evs:
                n += 1
                src, t = text(e["file"])
                if t is None:
                    bad += 1; fails.append((label, e["file"], "file not at commit")); continue
                q = e["quote"]
                i = t.find(q)
                if i < 0 or not q.strip():
                    bad += 1; fails.append((label, e["file"], "quote not an exact substring: " + q[:80].replace("\n", "\\n"))); continue
                # line range: any occurrence may satisfy it
                ok = False; j = i
                while j >= 0:
                    line = t.count("\n", 0, j) + 1
                    end = line + q.count("\n")
                    if e.get("line_start") is None or (line >= e["line_start"] - 3 and end <= e.get("line_end", e["line_start"]) + 3):
                        ok = True; break
                    j = t.find(q, j + 1)
                if not ok:
                    outside += 1
                    first = t.count("\n", 0, i) + 1
                    fails.append((label, e["file"], f"quote found at line {first}, stated {e.get('line_start')}-{e.get('line_end')}"))
print(json.dumps({"commit": commit, "quotes_checked": n, "not_exact": bad, "outside_stated_lines": outside,
                  "files_read": {k: v[0] for k, v in sorted(cache.items())}}, indent=1))
for x in fails: print("FAIL", *x, sep=" | ")
sys.exit(1 if (bad or outside) else 0)
