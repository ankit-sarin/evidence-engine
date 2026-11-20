"""12ai-CLNA step 2 — the scratch clones, checked before any removal (read-only on ~/scratch but OUT).

usage, from the evidence-engine repository root:  python3 -I -B clna_check.py <OUT> <phase>
  phase "baseline": 2a's I2 baseline (writes OUT/i2_before.tsv) and I1 (OUT/i1.txt); stops if I1 is false.
  phase "check":    2b–2i (the tables, OUT/removal_list.tsv, OUT/clna_check.json).
  phase "after":    2j — I2 again (OUT/i2_after.tsv), compared with the baseline (OUT/i2_diff.txt); exit 1 on a difference.

Every git command runs with GIT_OPTIONAL_LOCKS=0 and only reads: rev-parse, config --get, symbolic-ref, rev-list,
for-each-ref, status --porcelain, diff --numstat, reflog, ls-tree, merge-base --is-ancestor (in the main repository),
and `git apply --numstat` on a committed patch with no repository. No file is created, removed or moved outside OUT;
no database is opened; nothing found in scratch is executed. Blob ids are computed in Python (sha1 of "blob <n>\\0" +
bytes), as `git hash-object` without -w computes them. Scope (INV's rule): the ~/scratch entries named 12*, retained,
wr5-b, 10b-close, e_exec_probe. OUT itself is in scope by name and is excluded from I2's comparison: it is the one
place this step writes.
"""
import hashlib, json, os, re, stat, subprocess, sys
from pathlib import Path

OUT = Path(sys.argv[1]).resolve(); PHASE = sys.argv[2]
HOME = Path.home(); SCR = HOME / "scratch"; MAIN = Path.cwd().resolve()
INV = MAIN / "docs/session-reports/session-12/12ai_SCRI"
ENV = {**os.environ, "GIT_OPTIONAL_LOCKS": "0", "LC_ALL": "C"}
NAMED = {"retained", "wr5-b", "10b-close", "e_exec_probe"}


def git(repo, *args, check=True, text=True):
    r = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=text, env=ENV)
    if check and r.returncode != 0:
        raise RuntimeError(f"git {args} in {repo}: {r.stderr.strip()}")
    return r


def scope():
    return sorted(e for e in SCR.iterdir() if (e.name.startswith("12") or e.name in NAMED) and not e.is_symlink())


def walk(top):
    """(dirpath, dirnames, filenames) without following symbolic links."""
    yield from os.walk(top, followlinks=False)


def newest(entry):
    files = byt = 0; m = os.lstat(entry).st_mtime_ns
    for d, dn, fn in walk(entry):
        m = max(m, os.lstat(d).st_mtime_ns)
        for f in fn:
            st = os.lstat(os.path.join(d, f)); files += 1; m = max(m, st.st_mtime_ns)
            if stat.S_ISREG(st.st_mode): byt += st.st_size
    return files, byt, m


def find_repos():
    """Every directory in scope holding a `.git`, found without following a symbolic link; `.git` itself not entered."""
    out = []
    for e in scope():
        for d, dn, fn in walk(e):
            if ".git" in dn or ".git" in fn:
                out.append(Path(d))
            if ".git" in dn:
                dn.remove(".git")
    return sorted(out)


def i2_rows(repos):
    rows = []
    for e in scope():
        if e == OUT:
            continue
        f, b, m = newest(e); rows.append(f"ENTRY\t{e.name}\t{f}\t{b}\t{m}")
    for r in repos:
        if OUT in r.parents or r == OUT:
            continue
        ix = r / ".git" / "index"
        rows.append(f"INDEX\t{r.relative_to(SCR)}\t{os.lstat(ix).st_mtime_ns if ix.exists() else 'absent'}")
    return rows


if PHASE == "baseline":
    repos = find_repos()
    (OUT / "i2_before.tsv").write_text("\n".join(i2_rows(repos)) + "\n")
    alt = MAIN / ".git/objects/info/alternates"
    alt_text = alt.read_text() if alt.exists() else None
    wt = git(MAIN, "worktree", "list").stdout
    i1 = (alt_text is None or "scratch" not in alt_text) and len(wt.strip().splitlines()) == 1
    (OUT / "i1.txt").write_text(f"alternates: {'absent' if alt_text is None else alt_text!r}\nworktree list:\n{wt}I1 holds: {i1}\n")
    print((OUT / "i1.txt").read_text()); sys.exit(0 if i1 else 1)

if PHASE == "after":
    repos = find_repos()
    after = "\n".join(i2_rows(repos)) + "\n"; (OUT / "i2_after.tsv").write_text(after)
    before = (OUT / "i2_before.tsv").read_text()
    b, a = set(before.splitlines()), set(after.splitlines())
    diff = [f"- {l}" for l in sorted(b - a)] + [f"+ {l}" for l in sorted(a - b)]
    (OUT / "i2_diff.txt").write_text("\n".join(diff) + ("\n" if diff else ""))
    print(f"I2 rows before {len(b)}, after {len(a)}, differing {len(diff)}"); print("\n".join(diff)); sys.exit(1 if diff else 0)

# ── phase "check" ────────────────────────────────────────────────────────
main_url = git(MAIN, "remote", "get-url", "origin").stdout.strip()
origin_main = git(MAIN, "rev-parse", "origin/main").stdout.strip()
main_paths = {str(MAIN), str(MAIN) + "/.", str(MAIN) + "/", str(MAIN) + "/.git"}
ORIGIN_COMMITS = set(git(MAIN, "rev-list", "origin/main").stdout.split())
ORIGIN_OBJECTS = {l.split()[0] for l in git(MAIN, "rev-list", "--objects", "origin/main").stdout.splitlines() if l}
HEAD_BLOBS = {l.split()[2] for l in git(MAIN, "ls-tree", "-r", "HEAD").stdout.splitlines() if l.split()[1] == "blob"}
INV_ROWS = {}
inv_lines = (INV / "s3_clones.tsv").read_text().splitlines()
inv_head = inv_lines[0].split("\t")
for l in inv_lines[1:]:
    c = dict(zip(inv_head, l.split("\t"))); INV_ROWS[c["repo"]] = c
inv_untracked = {}
for p in (INV / "untracked").rglob("*"):
    if p.is_file():
        inv_untracked[hashlib.sha256(p.read_bytes()).hexdigest()] = str(p.relative_to(INV))


def blob_id(data):
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


repos = find_repos()
CLASS_RULE = ("clone of evidence-engine: `origin` is the main repository's path (with or without a trailing '/.' or '/') or its "
              f"GitHub remote ({main_url}); fixture repository: no `origin` remote; anything else: named as other")
rep = []
for r in repos:
    rel = str(r.relative_to(SCR))
    o = git(r, "config", "--get", "remote.origin.url", check=False).stdout.strip()
    head = git(r, "rev-parse", "HEAD", check=False).stdout.strip()
    br = git(r, "symbolic-ref", "--short", "-q", "HEAD", check=False).stdout.strip() or "(detached)"
    cls = "fixture" if not o else ("clone" if (o.rstrip("/") in {p.rstrip("/") for p in main_paths} or o == main_url
                                              or o.rstrip("/").removesuffix("/.") == str(MAIN)) else "other")
    rep.append({"repo": rel, "top": str(r), "origin": o, "head": head, "branch": br, "class": cls})
clones = [x for x in rep if x["class"] == "clone"]

# 2c
now_names = {x["repo"] for x in rep}
missing = sorted(set(INV_ROWS) - now_names)
new = []
for x in rep:
    if x["repo"] not in INV_ROWS:
        st = os.stat(Path(x["top"]) / ".git")
        birth = None
        try:
            r = subprocess.run(["stat", "-c", "%W", str(Path(x["top"]) / ".git")], capture_output=True, text=True)
            birth = int(r.stdout.strip()) if r.stdout.strip() not in ("", "0", "-") else None
        except Exception:
            pass
        new.append({**x, "birth_utc": subprocess.run(["date", "-u", "-d", f"@{birth}", "+%FT%TZ"], capture_output=True,
                                                     text=True).stdout.strip() if birth else "unknown",
                    "git_dir_mtime_utc": subprocess.run(["date", "-u", "-d", f"@{int(st.st_mtime)}", "+%FT%TZ"],
                                                        capture_output=True, text=True).stdout.strip()})


# 2d
def status_text(top):
    return git(top, "status", "--porcelain", "--ignored").stdout


def stash_count(top):
    r = git(top, "rev-parse", "--verify", "-q", "refs/stash", check=False)
    if r.returncode != 0:
        return 0
    return len(git(top, "reflog", "show", "--format=%H", "refs/stash").stdout.split())


def own_commits(top):
    return [c for c in git(top, "rev-list", "--branches").stdout.split() if c not in ORIGIN_COMMITS]


def reachable(head):
    return git(MAIN, "merge-base", "--is-ancestor", head, "origin/main", check=False).returncode == 0


def numstat_now(top):
    return git(top, "diff", "HEAD", "--numstat").stdout


def numstat_patch(p):
    return subprocess.run(["git", "apply", "--numstat", str(p)], capture_output=True, text=True, env=ENV, cwd=str(OUT)).stdout


d_rows = []
for x in clones:
    if x["repo"] not in INV_ROWS:
        continue
    inv = INV_ROWS[x["repo"]]; top = Path(x["top"]); key = x["repo"].replace("/", "__")
    diffs = []
    if x["head"] != inv["head"]:
        diffs.append(f"head {inv['head'][:12]} -> {x['head'][:12]}")
    reach = "yes" if reachable(x["head"]) else "no"
    if reach != inv["reachable_from_origin_main"]:
        diffs.append(f"reachable {inv['reachable_from_origin_main']} -> {reach}")
    oc = own_commits(top)
    if str(len(oc)) != inv["commits_not_in_main_origin"]:
        diffs.append(f"commits_not_in_main_origin {inv['commits_not_in_main_origin']} -> {len(oc)}")
    sc = stash_count(top)
    if str(sc) != inv["stash"]:
        diffs.append(f"stash {inv['stash']} -> {sc}")
    sf = INV / "clone_status" / f"{key}.txt"
    st_now = status_text(top)
    st_same = sf.exists() and sf.read_text() == st_now
    if not st_same:
        diffs.append("status differs from " + (f"clone_status/{sf.name}" if sf.exists() else "(no INV status file)"))
    pf = INV / "patches" / f"{key}.patch"
    if pf.exists():
        a, b = numstat_patch(pf), numstat_now(top)
        if a != b:
            diffs.append(f"diff HEAD numstat differs from patches/{pf.name}")
        patch_state = "patch numstat equal" if a == b else "patch numstat differs"
    else:
        b = numstat_now(top)
        if b:
            diffs.append("a diff HEAD with no INV patch")
        patch_state = "no patch; diff HEAD empty" if not b else "no patch; diff HEAD non-empty"
    d_rows.append({"repo": x["repo"], "head": x["head"], "reachable": reach, "own_commits": len(oc), "stash": sc,
                   "status_file": sf.name if sf.exists() else "", "status_equal": st_same, "patch": patch_state,
                   "verdict": "UNCHANGED" if not diffs else "CHANGED", "difference": "; ".join(diffs)})


# 2e
SKIP_PARTS = ("__pycache__", ".pytest_cache")


def untracked_ignored(top):
    """Paths git reports untracked (-uall) or ignored, one per file (an ignored directory walked without following links)."""
    out = []
    raw = git(top, "status", "--porcelain=v1", "-z", "--untracked-files=all", "--ignored=matching", text=False).stdout
    for ent in raw.split(b"\0"):
        if not ent or ent[:2] not in (b"??", b"!!"):
            continue
        kind = "untracked" if ent[:2] == b"??" else "ignored"; p = ent[3:].decode()
        full = top / p
        if any(s in Path(p).parts for s in SKIP_PARTS):
            continue
        if full.is_symlink():
            out.append((kind, p, full)); continue
        if full.is_dir():
            for d, dn, fn in walk(full):
                dn[:] = [n for n in dn if n not in SKIP_PARTS]
                for f in fn:
                    q = Path(d) / f; out.append((kind, str(q.relative_to(top)), q))
        else:
            out.append((kind, p, full))
    return out


e_files, e_links, e_unique = [], [], []
for x in clones:
    top = Path(x["top"])
    for kind, p, full in untracked_ignored(top):
        if full.is_symlink():
            tgt = os.path.realpath(full)
            if p == ".venv" or p.endswith("/.venv"):
                e_links.append({"repo": x["repo"], "path": p, "target": os.readlink(full), "class": ".venv link"}); continue
            e_links.append({"repo": x["repo"], "path": p, "target": os.readlink(full),
                            "class": "outside" if not tgt.startswith(str(top) + "/") else "inside"})
            continue
        data = full.read_bytes(); bid = blob_id(data); sha = hashlib.sha256(data).hexdigest()
        if len(data) == 0:
            match = "0-byte (never counted as a match)"
        elif bid in HEAD_BLOBS:
            match = "in HEAD's tree"
        elif bid in ORIGIN_OBJECTS:
            match = "in a commit on origin/main"
        elif sha in inv_untracked:
            match = f"INV copy {inv_untracked[sha]}"
        else:
            match = "NONE"
        row = {"repo": x["repo"], "kind": kind, "path": p, "bytes": len(data), "blob": bid, "sha256": sha, "match": match}
        e_files.append(row)
        if match == "NONE" or match.startswith("0-byte"):
            e_unique.append({**row, "what": "untracked/ignored file in no commit and no INV file" if match == "NONE"
                             else "0-byte untracked/ignored file (empty: no content to lose)"})
    # every other symbolic link inside the clone whose target is outside it (tracked or not)
    for d, dn, fn in walk(top):
        if ".git" in dn:
            dn.remove(".git")
        for n in dn + fn:
            q = Path(d) / n
            if q.is_symlink():
                rel = str(q.relative_to(top))
                if any(l["repo"] == x["repo"] and l["path"] == rel for l in e_links):
                    continue
                tgt = os.path.realpath(q)
                if not tgt.startswith(str(top) + "/"):
                    e_links.append({"repo": x["repo"], "path": rel, "target": os.readlink(q), "class": "outside (other)"})
    # commits, stash and changed tracked files whose content is in no commit of origin/main and no INV patch
    for c in own_commits(top):
        e_unique.append({"repo": x["repo"], "kind": "commit", "path": c, "bytes": "", "blob": "", "sha256": "", "match": "NONE",
                         "what": "a commit not in origin/main"})
    if stash_count(top):
        e_unique.append({"repo": x["repo"], "kind": "stash", "path": "refs/stash", "bytes": "", "blob": "", "sha256": "",
                         "match": "NONE", "what": "a stash"})
    key = x["repo"].replace("/", "__"); pf = INV / "patches" / f"{key}.patch"
    patch_equal = pf.exists() and numstat_patch(pf) == numstat_now(top)
    for l in numstat_now(top).splitlines():
        p = l.split("\t", 2)[2]
        full = top / p
        if full.exists() and full.is_file() and not full.is_symlink():
            bid = blob_id(full.read_bytes())
            if bid in ORIGIN_OBJECTS or bid in HEAD_BLOBS:
                continue
            if patch_equal:
                continue
            e_unique.append({"repo": x["repo"], "kind": "changed tracked", "path": p, "bytes": full.stat().st_size, "blob": bid,
                             "sha256": "", "match": "NONE", "what": "a changed tracked file in no commit and no INV patch"})


# 2f — readers
readers = []


def add_reader(path, src):
    p = os.path.expanduser(path.strip().strip("`").rstrip(".,;:)"))
    if p.startswith(str(SCR)):
        readers.append({"path": p, "source": src})


s2 = (INV / "s2_citations.tsv").read_text().splitlines()
hdr = s2[1].split("\t")
for l in s2[2:]:
    c = dict(zip(hdr, l.split("\t")))
    if c.get("label_reading") in ("CONSUMER", "LEDGER"):
        add_reader(c["cited_path"], f"INV/s2_citations.tsv {c['source_file:line']} ({c['label_reading']})")


def scratch_paths(text):
    return set(re.findall(r"(?:~|/home/ankitsarin)/scratch/[A-Za-z0-9_.\-/{},*]+", text))


exp = SCR / "12ai-open" / "expected_12ai.json"
for p in sorted(scratch_paths(exp.read_text())):
    add_reader(p, "~/scratch/12ai-open/expected_12ai.json")
cfg = json.loads(exp.read_text())
ret = cfg["retention"]; root = os.path.expanduser(ret["root"])
for ln, l in enumerate(open(MAIN / ret["reference"], encoding="utf-8"), 1):
    if l.startswith("#") or " MATCH" not in l:
        continue
    s, p, _ = l.split()
    q = p if p.startswith(("/", "data/")) else os.path.join(root, p)
    add_reader(q, f"{ret['reference']}:{ln} (resolved as session_open_check.py:retention resolves it)")
plan = (MAIN / "docs/plan/ENGINE_REFACTOR_PLAN.md").read_text().split("\n")
st = next(i for i, l in enumerate(plan) if l.startswith("Expected values at session-12ai open"))
en = next(i for i in range(st + 1, len(plan)) if plan[i].startswith(("## ", "**Block ", "*Dated ", "**L (")) or
          plan[i].startswith("Expected values at session-"))
for i in range(st, en):
    for p in scratch_paths(plan[i]):
        add_reader(p, f"plan:{i + 1} (the 12ai expected-values block, lines {st + 1}–{en})")
brief = MAIN / "docs/session-reports/session-12/12ah_CLOSE/brief_12ah_CLOSE.md"
for i, l in enumerate(brief.read_text().split("\n"), 1):
    for p in scratch_paths(l):
        add_reader(p, f"12ah_CLOSE/brief_12ah_CLOSE.md:{i}")
tops = [(x["repo"], x["top"]) for x in clones]
hits = []
for r in readers:
    base = r["path"].split("{")[0].split("*")[0].rstrip("/")
    for repo, top in tops:
        if base == top or base.startswith(top + "/"):
            hits.append({**r, "clone": repo})


# 2g — in use
in_use = []
uid = os.getuid()
for pid in os.listdir("/proc"):
    if not pid.isdigit():
        continue
    try:
        if os.stat(f"/proc/{pid}").st_uid != uid:
            continue
        paths = [os.readlink(f"/proc/{pid}/cwd")]
        for fd in os.listdir(f"/proc/{pid}/fd"):
            try:
                paths.append(os.readlink(f"/proc/{pid}/fd/{fd}"))
            except OSError:
                pass
        cmd = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode(errors="replace")[:200]
    except (OSError, PermissionError):
        continue
    for p in paths:
        for repo, top in tops:
            if p == top or p.startswith(top + "/"):
                in_use.append({"pid": int(pid), "cmd": cmd, "path": p, "clone": repo})


# 2h — sizes
sizes = {}
for x in clones:
    app = one = many = 0; n = 0
    for d, dn, fn in walk(x["top"]):
        for f in fn:
            stt = os.lstat(os.path.join(d, f))
            if not stat.S_ISREG(stt.st_mode):
                continue
            n += 1; app += stt.st_size
            if stt.st_nlink == 1: one += stt.st_size
            else: many += stt.st_size
    sizes[x["repo"]] = {"files": n, "apparent": app, "nlink1": one, "nlink_many": many}


# 2i — the removal list
d_by = {r["repo"]: r for r in d_rows}
uniq_by = {}
for u in e_unique:
    if u["what"].startswith("0-byte"):
        continue
    uniq_by.setdefault(u["repo"], []).append(u)
hit_by, use_by = {}, {}
for h in hits: hit_by.setdefault(h["clone"], []).append(h)
for u in in_use: use_by.setdefault(u["clone"], []).append(u)
rows = []
for x in clones:
    reasons = []
    if x["repo"] not in INV_ROWS: reasons.append("not in INV")
    elif d_by[x["repo"]]["verdict"] != "UNCHANGED": reasons.append(f"CHANGED under 2d ({d_by[x['repo']]['difference']})")
    if x["repo"] in uniq_by: reasons.append(f"2e: {len(uniq_by[x['repo']])} item(s) in no commit and no INV file")
    if x["repo"] in hit_by: reasons.append(f"2f: read by {len(hit_by[x['repo']])} path(s)")
    if x["repo"] in use_by: reasons.append(f"2g: in use by {len(use_by[x['repo']])} process(es)")
    if x["repo"].split("/")[0].startswith("12ai-"): reasons.append("under a 12ai-* entry (this session's)")
    verdict = "REMOVE" if not reasons else "KEEP"
    reason = "in INV and UNCHANGED; nothing unique; no reader; not in use; not this session's" if not reasons else "; ".join(reasons)
    s = sizes[x["repo"]]
    parent = Path(x["top"]).parent; beside_n = beside_b = 0
    if verdict == "REMOVE":
        for child in parent.iterdir():
            if child == Path(x["top"]):
                continue
            if child.is_symlink() or child.is_file():
                beside_n += 1; beside_b += os.lstat(child).st_size if not child.is_symlink() else 0
            else:
                f_, b_, _ = newest(child); beside_n += f_; beside_b += b_
    rows.append({"top": x["top"], "repo": x["repo"], "head": x["head"], **s, "verdict": verdict, "reason": reason,
                 "parent": str(parent), "beside_files": beside_n if verdict == "REMOVE" else "",
                 "beside_bytes": beside_b if verdict == "REMOVE" else ""})


def tsv(name, cols, data):
    with open(OUT / name, "w") as f:
        f.write("\t".join(cols) + "\n")
        for r in data:
            f.write("\t".join(str(r.get(c, "")) for c in cols) + "\n")


tsv("t2b_repos.tsv", ["repo", "class", "origin", "head", "branch", "top"], rep)
tsv("t2c_missing.tsv", ["repo"], [{"repo": m} for m in missing])
tsv("t2c_new.tsv", ["repo", "class", "head", "birth_utc", "git_dir_mtime_utc", "top"], new)
tsv("t2d_against_inv.tsv", ["repo", "head", "reachable", "own_commits", "stash", "status_file", "status_equal", "patch", "verdict",
                            "difference"], d_rows)
tsv("t2e_untracked_ignored.tsv", ["repo", "kind", "path", "bytes", "blob", "sha256", "match"], e_files)
tsv("t2e_links.tsv", ["repo", "path", "target", "class"], e_links)
tsv("t2e_unique.tsv", ["repo", "kind", "path", "bytes", "blob", "match", "what"], e_unique)
tsv("t2f_readers.tsv", ["path", "source"], readers)
tsv("t2f_hits.tsv", ["path", "source", "clone"], hits)
tsv("t2g_in_use.tsv", ["pid", "cmd", "path", "clone"], in_use)
tsv("t2h_sizes.tsv", ["repo", "files", "apparent", "nlink1", "nlink_many"], [{"repo": k, **v} for k, v in sizes.items()])
tsv("removal_list.tsv", ["top", "head", "apparent", "nlink1", "nlink_many", "verdict", "reason", "parent", "beside_files",
                         "beside_bytes"], rows)
summary = {"class_rule": CLASS_RULE, "origin_main": origin_main, "repos": len(rep),
           "by_class": {c: sum(1 for x in rep if x["class"] == c) for c in ("clone", "fixture", "other")},
           "inv_rows": len(INV_ROWS), "missing": missing, "new": [n["repo"] for n in new],
           "d_verdicts": {v: sum(1 for r in d_rows if r["verdict"] == v) for v in ("UNCHANGED", "CHANGED")},
           "e_files": len(e_files), "e_links": len(e_links), "e_unique": len(e_unique),
           "e_unique_nonzero": sum(1 for u in e_unique if not u["what"].startswith("0-byte")),
           "readers": len(readers), "reader_hits": len(hits), "in_use": len(in_use),
           "sizes_total": {k: sum(v[k] for v in sizes.values()) for k in ("files", "apparent", "nlink1", "nlink_many")},
           "remove": sum(1 for r in rows if r["verdict"] == "REMOVE"), "keep": sum(1 for r in rows if r["verdict"] == "KEEP"),
           "bytes_freed": sum(r["nlink1"] for r in rows if r["verdict"] == "REMOVE"),
           "apparent_remove": sum(r["apparent"] for r in rows if r["verdict"] == "REMOVE")}
(OUT / "clna_check.json").write_text(json.dumps(summary, indent=1) + "\n")
print(json.dumps(summary, indent=1))
