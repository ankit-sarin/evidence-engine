"""12ai-U41B 2b census, before the edit. Read-only. usage, from the repo root: python3 -I -B census_u41b.py > census_u41b.txt
Four sets over tests/: (A) Q6's pattern (12ah_OLRA.md §Q6, quoted); (B) a manifest body's key set asserted, or open_run called;
(C) pdf_quality_check imported; (D) RESTART_EVERY_N or a --restart-every default read. Per file: the hit lines (number and text,
cut at 160). The class against this change is the census's first reading, given per file in CLASS below and decided by the
selected runs (2g): (c) coupled — its assertion assumes a restart armed by default, the six endings, or the argv without -n;
(n) not coupled."""
import re, subprocess
from pathlib import Path
A = (r"restart_ollama|_restart_ollama_and_retry|RESTART_OPT_OUT_ENV|NO_OLLAMA_RESTART|restart_disabled|DETAIL_RESTART|DETAIL_POST_RESTART|"
     r"DETAIL_WORKER_ALIVE|RUN_CALL_ENDING_DETAILS|_recheck_env_after_restart|ollama_env_after_restart|check_call_env|restart_every|"
     r"RESTART_EVERY_N|ReadTimeout|systemctl")
B = r"open_run\(|manifest_json|\bset\(\w*manifest\w*\)|sorted\(\w*manifest\w*\)|manifest\[.\w+.\]|\.keys\(\)\s*==|not in \w*manifest\w*|in \w*manifest\w*\b"
C = r"pdf_quality_check"
D = r"RESTART_EVERY_N|--restart-every|restart_every"
CLASS = {
    "tests/test_ollama_client.py": "(c) read-timeout -> restart tests assume armed by default; argv pin without -n (Q6)",
    "tests/test_restart_recovery_guard.py": "(c) 'proceeds' and unset-leaves-armed tests assume armed by default (Q6)",
    "tests/test_restart_ollama_guard.py": "(c) argv pins without -n, if any; proactive gates otherwise (n) (Q6; decided by the run)",
    "tests/test_u21_no_overlap.py": "(c) restart-reaching tests outside a run; the six-ending exact-set pin :279-282 (Q6)",
}
files = sorted(Path("tests").rglob("*.py"))
out = {}
for f in files:
    lines = f.read_text(encoding="utf-8", errors="replace").split("\n")
    for tag, pat in (("A", A), ("B", B), ("C", C), ("D", D)):
        hits = [(i + 1, l.strip()[:160]) for i, l in enumerate(lines) if re.search(pat, l)]
        if hits:
            out.setdefault(str(f), {})[tag] = hits
for tag in "ABCD":
    print(f"== set {tag}: {sum(1 for v in out.values() if tag in v)} files")
print()
for f, sets in out.items():
    print(f"### {f}  sets {''.join(sorted(sets))}  class: {CLASS.get(f, '(n) unless the selected run is red')}")
    for tag in sorted(sets):
        for n, l in sets[tag][:12]:
            print(f"  {tag} :{n}  {l}")
        if len(sets[tag]) > 12:
            print(f"  {tag} … {len(sets[tag]) - 12} more")
