"""12ai-SCRI-RECA step 3 — the publication census of ~/scratch/12ai-scri/ (SRC), read-only on SRC. Writes only under OUT:
one TSV per table (3a_*.tsv … 3h_*.tsv) and the census sections as OUT/census_sections.md, which assemble.py joins into
OUT/12ai_SCRI_RECA.md. Reads the repository with git read commands only (ls-files, cat-file). Opens no database.
usage: python3 -I -B census.py <SRC> <OUT> <repo> <committed brief naming the four home folders>"""
import gzip, json, random, re, subprocess, sys
from collections import Counter, defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent)); import kinds  # noqa: E402  (OUT/helpers/kinds.py)
SRC, OUT, REPO, BRIEF = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4])
HOME = "/home/ankitsarin"
FILES = sorted(str(p.relative_to(SRC)) for p in SRC.rglob("*") if p.is_file())
assert len(FILES) == 97, len(FILES)
def text(rel):
    b = (SRC / rel).read_bytes()
    try: return b.decode("utf-8"), True
    except UnicodeDecodeError: return b.decode("latin-1"), False
TXT = {f: text(f) for f in FILES}
LINES = {f: TXT[f][0].split("\n")[:-1] if TXT[f][0].endswith("\n") else TXT[f][0].split("\n") for f in FILES}
git = lambda *a: subprocess.run(["git", "-C", str(REPO), *a], capture_output=True, text=True)
TRACKED = set(git("ls-files", "-z").stdout.split("\0"))
def in_scope_entry(e): return e.startswith("12") or e in ("retained", "wr5-b", "10b-close", "e_exec_probe")
md = []; W = lambda s="": md.append(s)
def tsv(name, header, rows):
    (OUT / name).write_text("\t".join(header) + "\n" + "".join("\t".join(str(c) for c in r) + "\n" for r in rows)); return name

# ---------------------------------------------------------------- 3a other lanes' scratch entries
s1 = LINES["s1_toplevel.tsv"]; hdr = s1[0].split("\t")
out_rows = [r.split("\t") for r in s1[1:] if r.split("\t")[hdr.index("in_scope")] != "yes"]
names = [r[0] for r in out_rows]
home = sorted(set(re.findall(r"~/([a-z0-9-]+-scratch)\b", BRIEF.read_text())))
pat = {n: re.compile(r"(?<![A-Za-z0-9_-])" + re.escape(n) + r"(?![A-Za-z0-9_-]|\.[A-Za-z0-9])") for n in names + home}
partial = {"brand-theme-* (prefix form)": re.compile(r"brand-theme-(?![A-F](?![A-Za-z0-9_-]))"),
           "cm* (prefix form)": re.compile(r"(?<![A-Za-z0-9_-])cm\*")}
pat.update(partial)
by_file = defaultdict(Counter)
for f in FILES:
    for ln in LINES[f]:
        for n, rx in pat.items():
            if rx.search(ln): by_file[f][n] += 1
cols = names + home + list(partial)
tsv("3a_names_by_file.tsv", ["file"] + cols, [[f] + [by_file[f][n] for n in cols] for f in FILES if by_file[f]])
W("## 3a — other lanes' scratch entries"); W()
W(f"s1_toplevel.tsv header: `{s1[0].replace(chr(9), '⇥')}`. The column `in_scope` marks scope: `yes` on {sum(1 for r in s1[1:] if r.split(chr(9))[hdr.index('in_scope')] == 'yes')} rows, "
  f"`{Counter(r[hdr.index('in_scope')] for r in out_rows).most_common(1)[0][0]}` on {len(out_rows)}.")
W(); W(f"The {len(names)} out-of-scope entries, in full, once: " + ", ".join(f"`{n}`" for n in names) + ".")
W(); W(f"Also counted: the four home folders named in the committed 12ai-OPEN brief ({', '.join('`~/' + h + '`' for h in home)}), and two prefix forms.")
W(); W("| file | lines naming each (name: lines) |"); W("|---|---|")
for f in FILES:
    if by_file[f]: W(f"| {f} | " + "; ".join(f"{n}: {c}" for n, c in by_file[f].items()) + " |")
holders = sorted(f for f in FILES if any(by_file[f][n] for n in names))
W(); W(f"Files naming an out-of-scope entry by its full name: {len(holders)}: {', '.join(holders)}. "
  f"Files naming a home folder: {sorted(f for f in FILES if any(by_file[f][h] for h in home)) or 'none'}. Table: `3a_names_by_file.tsv`.")
A_HOLDERS = holders

# ---------------------------------------------------------------- 3b paths outside this lane
ABS = re.compile(r"(?<![\w.~:/@*)}…-])/[A-Za-z0-9_.@+-]+(?:/[A-Za-z0-9_.@+*{},…-]*)*")
TIL = re.compile(r"(?<![\w.~/-])~/[A-Za-z0-9_.@+*{},-]+(?:/[A-Za-z0-9_.@+*{},…-]*)*")
def norm(p):
    p = p.rstrip(".,;:)`'\"…")
    return (HOME + p[1:]) if p.startswith("~/") else p
def in_lane(p):
    c = [x for x in p.split("/") if x]
    if c[:3] == ["home", "ankitsarin", "projects"] and len(c) >= 4 and c[3] == "evidence-engine": return True
    if c[:3] == ["home", "ankitsarin", "scratch"] and len(c) >= 4 and in_scope_entry(c[3]): return True
    return False
grp_file = defaultdict(lambda: defaultdict(lambda: [0, None])); grp_paths = defaultdict(Counter)
for f in FILES:
    if not TXT[f][1]: continue  # not valid UTF-8 (g3_random.bin): no path search; reported in 3g
    for i, ln in enumerate(LINES[f], 1):
        for rx in (ABS, TIL):
            for m in rx.finditer(ln):
                p = norm(m.group(0))
                if not [x for x in p.split("/") if x] or in_lane(p): continue
                c = [x for x in p.split("/") if x]; g = "/" + "/".join(c[:3])
                e = grp_file[g][f]; e[0] += 1; e[1] = e[1] or i; grp_paths[g][p] += 1
rows = [[g, f, n, ex] for g in sorted(grp_file) for f, (n, ex) in sorted(grp_file[g].items())]
tsv("3b_paths_by_group_file.tsv", ["group (first three components; ~ read as /home/ankitsarin)", "file", "paths", "example_line"], rows)
tsv("3b_distinct_paths.tsv", ["group", "path", "occurrences"], [[g, p, n] for g in sorted(grp_paths) for p, n in sorted(grp_paths[g].items())])
W(); W("## 3b — paths outside this lane"); W()
W("Every absolute or `~/` path not under `~/projects/evidence-engine` or an in-scope scratch entry; `~` read as `/home/ankitsarin`; "
  "grouped by the first three components. Per group: total occurrences, files (count, example line). A match after `)`, `*`, `}` or `…` is a "
  "fragment of a relative path and is not counted; a file that is not valid UTF-8 is not searched.")
W(); W("| group | occurrences | files: paths (example line) |"); W("|---|---|---|")
for g in sorted(grp_file):
    W(f"| `{g}` | {sum(v[0] for v in grp_file[g].values())} | " + "; ".join(f"{f}: {n} (:{ex})" for f, (n, ex) in sorted(grp_file[g].items())) + " |")
W(); W("Distinct paths per group, in full:"); W()
for g in sorted(grp_paths):
    W(f"- `{g}` ({len(grp_paths[g])}): " + ", ".join(f"`{p}`" for p in sorted(grp_paths[g])))
W(); W("Tables: `3b_paths_by_group_file.tsv`, `3b_distinct_paths.tsv`.")
B_GROUPS = {g: set(grp_file[g]) for g in grp_file}

# ---------------------------------------------------------------- 3c citations and quoted text from untracked files
s2 = LINES["s2_citations.tsv"]; hi = next(i for i, l in enumerate(s2) if l.startswith("source_file"))
h2 = s2[hi].split("\t"); data = [l.split("\t") for l in s2[hi + 1:]]
textcol = [c for c in h2 if re.search(r"text|quote|line_content|excerpt", c, re.I)]
per = defaultdict(lambda: [0, 0])
for r in data:
    src = r[0].rsplit(":", 1)[0]; per[src][0] += 1; per[src][1] += len(r[1].encode())
W(); W("## 3c — citations and quoted text from files this repository does not track"); W()
W(f"s2_citations.tsv: {len(s2)} lines; header lines: " + " / ".join(f"`{l.replace(chr(9), '⇥')}`" for l in s2[:hi + 1]) + f"; data rows {len(data)}.")
W(f"Columns: {', '.join('`' + c + '`' for c in h2)}. A column carrying the cited line's text: {textcol or 'none'}; every row carries the "
  "line NUMBER in `source_file:line`, and the path the line cites in `cited_path`, not the line's text. Bytes of cited-line text: 0.")
W(f"Distinct `cited_path` values: {len({r[1] for r in data})} (the read-out's S2 gives \"119 citations of 93 distinct paths\").")
W(); W("| source file | rows | tracked at HEAD | carries | bytes of `cited_path` in its rows |"); W("|---|---|---|---|---|")
crow = []
for s, (n, b) in sorted(per.items()):
    t = s in TRACKED; crow.append([s, n, "yes" if t else "no", "line number only", b])
    W(f"| {s} | {n} | {'yes' if t else 'no'} | line number + cited path | {b} |")
tsv("3c_citations_by_source.tsv", ["source_file", "rows", "tracked_at_HEAD", "carries", "cited_path_bytes"], crow)
# 12ai_SCRI.md: lines that reference a file, with whether the file is tracked and whether the line quotes
R = LINES["12ai_SCRI.md"]; fence = False; refs = []
REF = re.compile(r"(surgical-cv `?primer\.md`?|surgical-cv `?CLAUDE\.md`?|claude-config `?primer\.md`?|primer-history/[^\s`]*|(?<![\w/-])`?primer\.md`?(?::[\d,–-]+)?|(?<![\w/-])`?CLAUDE\.md`?(?::[\d,]+)?)")
for i, ln in enumerate(R, 1):
    if ln.startswith("```"): fence = not fence; refs.append((i, "fence " + ("open" if fence else "close"), "", "")); continue
    for m in REF.finditer(ln):
        s = m.group(0).strip("`")
        if s.startswith("surgical-cv") or s.startswith("claude-config"): file, tr = s.replace("`", ""), "no (another repository)"
        elif s.startswith("primer-history"): file, tr = "primer-history/…", "yes" if any(x.startswith("primer-history/") for x in TRACKED) else "no"
        elif s.startswith("primer.md"): file, tr = "primer.md (this repo)", "yes" if "primer.md" in TRACKED else "no (ignored)"
        else: file, tr = "CLAUDE.md (this repo)", "yes" if "CLAUDE.md" in TRACKED else "no"
        q = "quote" if (re.search(r'"[^"]{3,}"', ln) or fence) else "number/name only"
        refs.append((i, file, tr, q))
tsv("3c_scri_md_refs.tsv", ["line", "file referenced", "tracked_at_HEAD", "line quotes text"], refs)
W(); W("12ai_SCRI.md, every line that references primer.md, CLAUDE.md, primer-history/ or another repository's file "
  "(helper: `3c_scri_md_refs.tsv`; whether a line QUOTES is the helper's test, a double-quoted span of 3+ characters or a fenced block):")
W(); W("| line | file referenced | tracked at HEAD | quotes text |"); W("|---|---|---|---|")
for i, f_, t, q in refs: W(f"| {i} | {f_} | {t} | {q} |")

# ---------------------------------------------------------------- 3d the two large tables
W(); W("## 3d — the two large tables"); W()
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
WORDS = {"patient", "phi", "mrn"}
for T in ("s4_records_with_committed_copy.tsv", "s4_records_without_committed_copy.tsv"):
    L = LINES[T]; h = L[0].split("\t"); rows = [l.split("\t") for l in L[1:]]
    ent = Counter(r[0].split("/")[0] for r in rows); outside = [r for r in rows if not in_scope_entry(r[0].split("/")[0])]
    kc = Counter(kinds.kind(r[0]) for r in rows); kb = Counter()
    for r in rows: kb[kinds.kind(r[0])] += int(r[1])
    rnd = random.Random(20261119); draw = sorted(rnd.sample(range(len(rows)), 10))
    pt = sorted({r[0].rsplit("/", 1)[-1] for r in rows if kinds.is_paper_text(r[0])})
    pt20 = sorted(random.Random(20261119).sample(pt, min(20, len(pt))))
    comp = [r for r in rows if any(re.sub(r"\.[^.]*$", "", c).lower() in WORDS for c in r[0].split("/"))]
    wordin = [r for r in rows if any(w in re.split(r"[^a-z0-9]+", r[0].lower()) for w in WORDS)]
    mail = [r for r in rows if EMAIL.search(r[0])]
    stem = T[:-4]
    tsv(f"3d_{stem}_entries.tsv", ["entry", "rows"], sorted(ent.items()))
    tsv(f"3d_{stem}_kinds.tsv", ["kind (CC's rule, kinds.py)", "rows", "bytes"], [[k, kc[k], kb[k]] for k in sorted(kc)])
    W(f"### {T}"); W()
    W(f"- {(SRC / T).stat().st_size} B; {len(rows)} rows + 1 header line; header `{L[0].replace(chr(9), '⇥')}`; columns {h}.")
    W(f"- Distinct top-level entries: {len(ent)} (rows per entry: `3d_{stem}_entries.tsv`); largest: " + ", ".join(f"{e} {n}" for e, n in ent.most_common(8)) + ".")
    W(f"- Rows whose path is not under an in-scope entry (E3): {len(outside)}" + (": " + "; ".join("\t".join(r) for r in outside) if outside else "."))
    W(f"- Rows by kind (CC's rule, `kinds.py`; s4_unique_by_kind.tsv's names): " + "; ".join(f"{k} {kc[k]} / {kb[k]} B" for k in sorted(kc)) + ".")
    W("- The first five rows, whole:"); W("```"); [W(l) for l in L[1:6]]; W("```")
    W("- Ten rows drawn with `random.Random(20261119).sample(range(rows), 10)`, whole (row index from 0 after the header):"); W("```")
    [W(f"[{j}] " + L[j + 1]) for j in draw]; W("```")
    W(f"- Paper-text names (CC's broad rule: under parsed_text/ or texts/, or .pdf): {sum(1 for r in rows if kinds.is_paper_text(r[0]))} rows, {len(pt)} distinct names; "
      f"twenty drawn with `random.Random(20261119)`, whole: " + ", ".join(f"`{n}`" for n in pt20) + ".")
    W(f"- Paths holding an e-mail address: {len(mail)}{': ' + '; '.join(r[0] for r in mail) if mail else ''}. "
      f"Paths with patient, phi or mrn as a whole component (any case, extension ignored): {len(comp)}{': ' + '; '.join(r[0] for r in comp) if comp else ''}. "
      f"Wider reading, the word as a token anywhere in the path (split on non-alphanumerics): {len(wordin)}{': ' + '; '.join(r[0] for r in wordin[:20]) if wordin else ''}.")
    W()
s4k = [l.split("\t") for l in LINES["s4_unique_by_kind.tsv"][1:]]
W("s4_unique_by_kind.tsv against CC's rule on s4_records_without_committed_copy.tsv:"); W(); W("| kind | table files / bytes | CC's rule files / bytes |"); W("|---|---|---|")
rows = [l.split("\t") for l in LINES["s4_records_without_committed_copy.tsv"][1:]]
kc = Counter(kinds.kind(r[0]) for r in rows); kb = Counter()
for r in rows: kb[kinds.kind(r[0])] += int(r[1])
for k, f_, b in s4k: W(f"| {k} | {f_} / {b} | {kc[k]} / {kb[k]} |")

# ---------------------------------------------------------------- 3e ignored and untracked names
W(); W("## 3e — the ignored and untracked names"); W()
g1 = LINES["g1_mainstatus_before.txt"]; bang = [l for l in g1 if l.startswith("!!")]
W(f"g1_mainstatus_before.txt: {len(g1)} lines; {len(bang)} with the `!!` mark; other lines: {len(g1) - len(bang)}"
  + (f" ({Counter(l[:2] for l in g1 if not l.startswith('!!'))})" if len(g1) != len(bang) else "") + ". Every `!!` line, whole:")
W("```"); [W(l) for l in bang]; W("```")
tsv("3e_mainstatus_ignored.tsv", ["line"], [[l] for l in bang])
skip = lambda p: "__pycache__" in p.split("/") or ".venv" in [x.rstrip("/") for x in p.split("/")]
s3 = [l.split("\t") for l in LINES["s3_untracked_ignored.tsv"][1:]]; hs3 = LINES["s3_untracked_ignored.tsv"][0].split("\t")
s3p = sorted({r[hs3.index("path")] for r in s3 if not skip(r[hs3.index("path")])})
cs = set()
for f in FILES:
    if f.startswith("clone_status/"):
        for l in LINES[f]:
            p = l[3:].split(" -> ")[-1].strip('"')
            if p and not skip(p): cs.add(p)
cs = sorted(cs)
tsv("3e_untracked_ignored_names.tsv", ["source", "path"], [["s3_untracked_ignored.tsv", p] for p in s3p] + [["clone_status/", p] for p in cs])
W(); W(f"s3_untracked_ignored.tsv: {len(s3)} rows; distinct paths not under `__pycache__` or `.venv`: {len(s3p)}, whole:"); W("```"); [W(p) for p in s3p]; W("```")
W(f"clone_status/ (55 files): distinct paths not under `__pycache__` or `.venv`: {len(cs)}, whole:"); W("```"); [W(p) for p in cs]; W("```")

# ---------------------------------------------------------------- 3f patches
W(); W("## 3f — the patches"); W()
W("| patch | bytes | git apply --numstat (+, −, path) | touched paths outside docs/ tests/ engine/ scripts/ |"); W("|---|---|---|---|")
prow = []; outside_all = []
for f in FILES:
    if not f.startswith("patches/"): continue
    r = subprocess.run(["git", "apply", "--numstat"], input=(SRC / f).read_bytes(), capture_output=True, cwd=str(OUT))
    ns = [l.split("\t") for l in r.stdout.decode().splitlines()]
    out = [x[2] for x in ns if not re.match(r"(docs|tests|engine|scripts)/", x[2])]; outside_all += out
    prow += [[f, (SRC / f).stat().st_size] + x for x in ns]
    W(f"| {f} | {(SRC / f).stat().st_size} | " + "; ".join(f"+{x[0]} −{x[1]} {x[2]}" for x in ns) + (f" (exit {r.returncode}: {r.stderr.decode().strip()})" if r.returncode else "")
      + f" | {', '.join(out) or 'none'} |")
tsv("3f_patch_numstat.tsv", ["patch", "bytes", "plus", "minus", "path"], prow)
uj = [f for f in FILES if f.startswith("untracked/")][0]; uo = json.loads(TXT[uj][0])
W(); W(f"Every touched path outside the four directories (E5): {sorted(set(outside_all)) or 'none'}.")
W(f"The untracked JSON `{uj}`: {(SRC / uj).stat().st_size} B; top-level keys: {list(uo) if isinstance(uo, dict) else type(uo).__name__}.")

# ---------------------------------------------------------------- 3g bytes and encoding
W(); W("## 3g — bytes and encoding"); W()
nonutf = [f for f in FILES if not TXT[f][1]]
W(f"Files that are not valid UTF-8: {nonutf} (E2).")
BIG = ["s4_records_with_committed_copy.tsv", "s4_records_without_committed_copy.tsv"]
def group(f):
    if f in ("12ai_SCRI.md", "SHA256SUMS"): return "1 12ai_SCRI.md and the list"
    if f in BIG: return "2 the two large tables"
    if "/" not in f: return "3 the other tables and gate files"
    return {"clone_status": "4 clone_status/", "patches": "5 patches/", "untracked": "6 untracked/"}[f.split("/")[0]]
G = defaultdict(list)
for f in FILES: G[group(f)].append(f)
W(); W("| group | files | bytes |"); W("|---|---|---|")
for g in sorted(G): W(f"| {g[2:]} | {len(G[g])} | {sum((SRC / f).stat().st_size for f in G[g])} |")
W(f"| total | {len(FILES)} | {sum((SRC / f).stat().st_size for f in FILES)} |")
gz = {}
for T in BIG:
    r = subprocess.run(f"gzip -9 -c '{SRC / T}' | wc -c", shell=True, capture_output=True, text=True); gz[T] = int(r.stdout.strip())
    W(f"- `{T}` under `gzip -9 -c … | wc -c`: {gz[T]} B (of {(SRC / T).stat().st_size} B).")
tsv("3g_groups.tsv", ["group", "file", "bytes", "utf8"], [[g[2:], f, (SRC / f).stat().st_size, TXT[f][1]] for g in sorted(G) for f in G[g]])

# ---------------------------------------------------------------- 3h what the list requires
W(); W("## 3h — what the list requires"); W()
listed = {l.rsplit(None, 2)[0] for l in LINES["SHA256SUMS"] if l.strip()}
ev = []
for i in list(range(13, 20)) + [i for i in range(1, len(R) + 1) if i >= next(k for k, l in enumerate(R, 1) if l.startswith("## Gates")) and i < next(k for k, l in enumerate(R, 1) if l.startswith("## Findings"))]:
    for m in re.finditer(r"`([A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]*)?)`", R[i - 1]):
        n = m.group(1)
        if n in FILES or n.rstrip("/") in ("clone_status", "patches", "untracked"): ev.append((i, n))
seen = []; hrow = []
W("| named on line | name | SHA256SUMS names it |"); W("|---|---|---|")
for i, n in ev:
    if n in [s for _, s in seen]: continue
    seen.append((i, n))
    if n.endswith("/"): got = f"{sum(1 for x in listed if x.startswith(n))} of {sum(1 for x in FILES if x.startswith(n))} files under it"
    else: got = "yes" if n in listed else ("no (the list itself)" if n == "SHA256SUMS" else "no")
    hrow.append([i, n, got]); W(f"| {i} | `{n}` | {got} |")
tsv("3h_evidence_listed.tsv", ["line", "name", "listed"], hrow)
big_lines = [l for l in LINES["SHA256SUMS"] if l.rsplit(None, 2)[0] in BIG]
W(); W(f"If the two large tables alone were left out, the lines of SHA256SUMS naming no committed file would be {len(big_lines)}: "
  + "; ".join(f"`{l.rsplit(None, 2)[0]}  {l.rsplit(None, 2)[1]}  {l.rsplit(None, 2)[2][:8]}…`" for l in big_lines)
  + f"; the other {len(listed) - len(big_lines)} lines name files that would be committed.")

# ---------------------------------------------------------------- the closing table
scan = (OUT / "reca_credential_scan.txt").read_text()
sh = Counter(); rh = Counter()
for m in re.finditer(r"^  (STOP|READING)\s+(\S+):\d+ ", scan, re.M):
    (sh if m.group(1) == "STOP" else rh)[m.group(2)] += 1
W(); W("## The closing table"); W()
W("| group | files | bytes | from outside this lane (3a–3e) | scan |"); W("|---|---|---|---|---|")
for g in sorted(G):
    fs = G[g]; b = sum((SRC / f).stat().st_size for f in fs)
    nm = sorted({n for f in fs for n in by_file[f] if by_file[f][n]})
    pg = sorted({gr for gr, ff in B_GROUPS.items() if ff & set(fs)})
    st = sum(sh[Path(f).name] for f in fs); rd = sum(rh[Path(f).name] for f in fs)
    W(f"| {g[2:]} | {len(fs)} | {b} | names: {', '.join(nm) or 'none'}; path groups: {', '.join(pg) or 'none'} | stop {st}, reading {rd} |")
(OUT / "census_sections.md").write_text("\n".join(md) + "\n")
print(f"census: {len(md)} lines to census_sections.md; TSVs written under {OUT}")
