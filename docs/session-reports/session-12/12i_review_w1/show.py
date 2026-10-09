"""print candidate sections of a reader report, trimmed. usage: show.py <report.md> <ID> [<ID>…] [--max N]"""
import re, sys
args = sys.argv[1:]; mx = 2600
if "--max" in args:
    i = args.index("--max"); mx = int(args[i + 1]); del args[i:i + 2]
text = open(args[0], encoding="utf-8").read()
for cid in args[1:]:
    m = re.search(r"\n### %s\b.*?(?=\n### |\n## |\Z)" % re.escape(cid), text, re.S)
    print(m.group(0)[:mx] if m else "?? " + cid); print("-" * 60)
