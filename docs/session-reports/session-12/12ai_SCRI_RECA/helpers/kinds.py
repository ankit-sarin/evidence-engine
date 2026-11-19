"""12ai-SCRI-RECA 3d: a kind rule written by CC from s4_unique_by_kind.tsv's kind NAMES. The read-out records no rule ("kind by
filename, CC's reading"), so this rule is CC's and its counts are reported beside the table's, not as the table's.
Order: sqlite (name .db / -shm / -wal / -journal / .sqlite / .bak / review.db); patches (.patch); paper texts (a file under a
parsed_text/ or texts/ directory, or a .pdf, the broad reading); pytest basetemp content (a directory component starting test_,
or named basetemp, pytest-N, bt, bt_*); logs (.log .out .err .stderr .stdout, or "journal" in the name); json (.json .jsonl);
scripts (.py .sh); text/md/tsv (.md .txt .tsv .csv); other.
Run alone it prints its counts against the table: usage: python3 -I -B kinds.py <SRC>"""
import re, sys
SQL = re.compile(r"(\.db|\.db-shm|\.db-wal|\.db-journal|\.sqlite3?)$|\.db\.bak|\.bak($|[-.])|review\.db")
def is_paper_text(path):
    ps = path.split("/"); return any(x in ("parsed_text", "texts") for x in ps[:-1]) or ps[-1].lower().endswith(".pdf")
def kind(path):
    ps = path.split("/"); name = ps[-1]
    if SQL.search(name): return "sqlite db/sidecar/bak"
    if name.endswith(".patch"): return "patches"
    if is_paper_text(path): return "paper texts"
    if any(x.startswith("test_") or re.fullmatch(r"basetemp|pytest-\d+|bt|bt_.*", x) for x in ps[1:-1]): return "pytest basetemp content"
    if re.search(r"\.(log|out|err|stderr|stdout)$", name) or "journal" in name: return "logs"
    if re.search(r"\.(json|jsonl)$", name): return "json"
    if re.search(r"\.(py|sh)$", name): return "scripts"
    if re.search(r"\.(md|txt|tsv|csv)$", name): return "text/md/tsv"
    return "other"
if __name__ == "__main__":
    from collections import Counter
    src = sys.argv[1]; c = Counter(); b = Counter()
    for l in open(f"{src}/s4_records_without_committed_copy.tsv").read().splitlines()[1:]:
        p, n, _ = l.split("\t"); k = kind(p); c[k] += 1; b[k] += int(n)
    for l in open(f"{src}/s4_unique_by_kind.tsv").read().splitlines()[1:]:
        k, f, by = l.split("\t"); print(f"{k:26} table {f:>6} {by:>11} | rule {c[k]:>6} {b[k]:>11} | {'equal' if (c[k], b[k]) == (int(f), int(by)) else 'differs'}")
