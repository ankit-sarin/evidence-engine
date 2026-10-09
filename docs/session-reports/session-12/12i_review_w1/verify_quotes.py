"""12i-REVIEW-W1, W1-c aid — are the readers' quoted code lines present at HEAD? Read-only.

usage, from the repository root:  verify_quotes.py <repo_root> <readers_dir> > verify_quotes.json
For every candidate section (`### <LOT>-C<n> — …`) of every reader report: every fenced code line
and every inline `code span` of 16+ characters is looked up, whitespace-normalised, in the tracked
text files of the repository (engine/, scripts/, analysis/, tests/, CLAUDE.md, docs/architecture,
review_specs/, engine/migrations/README.md). Reports per candidate: quotes found / not found.
A span not found is not an error by itself (readers also quote reproducer output, paraphrase with
"…", or quote the plan); the list is what the lead reads. This is an aid, not the re-derivation.
"""
import json, os, re, subprocess, sys

root, rd = sys.argv[1], sys.argv[2]
files = subprocess.run(["git", "-C", root, "ls-files", "engine", "scripts", "analysis", "tests", "CLAUDE.md",
                        "docs/architecture", "review_specs"], capture_output=True, text=True).stdout.split("\n")
norm = lambda s: re.sub(r"\s+", " ", s).strip()
corpus = {}
for f in files:
    if f.endswith((".py", ".md", ".yaml", ".yml", ".toml", ".txt", ".sh")):
        try:
            corpus[f] = norm(open(os.path.join(root, f), encoding="utf-8").read())
        except Exception:
            pass
out = {}
for rep in sorted(os.listdir(rd)):
    if not rep.endswith(".md"):
        continue
    text = open(os.path.join(rd, rep), encoding="utf-8").read()
    cands = re.split(r"\n(?=### )", text)
    for c in cands:
        m = re.match(r"### (\S+-C\d+)\b[^\n]*", c)
        if not m:
            continue
        body = c.split("\n## ")[0]
        spans = []
        for block in re.findall(r"```[a-z]*\n(.*?)```", body, re.S):
            spans += [l for l in block.split("\n")]
        nofence = re.sub(r"```.*?```", "", body, flags=re.S)
        spans += re.findall(r"`([^`\n]{16,})`", nofence)
        found, missing = 0, []
        for s in spans:
            for piece in re.split(r"\s*(?:…|\.\.\.)\s*", s):
                p = norm(piece)
                if len(p) < 16:
                    continue
                if any(p in t for t in corpus.values()):
                    found += 1
                else:
                    missing.append(p[:140])
        cls = re.search(r"\*\*Proposed class / package:\*\*\s*([^\n]*)", body)
        out[m.group(1)] = {"title": m.group(0)[4:][:150], "class_line": cls.group(1)[:160] if cls else None,
                           "chars": len(body), "quotes_found": found, "quotes_missing": len(missing),
                           "missing": missing[:12]}
json.dump(out, sys.stdout, indent=1)
