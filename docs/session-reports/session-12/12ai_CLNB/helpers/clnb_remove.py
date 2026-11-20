"""12ai-CLNB step 2 — the removal of the fifty-three clones R823(b) approved, one at a time, each after its own check.

usage, from the evidence-engine repository root:  python3 -I -B clnb_remove.py <OUT> <phase>
  prep     2a: LIST's sha256; the REMOVE rows to OUT/to_remove.tsv (read from the committed list, never typed); I1; the main
           repository's HEAD, status, count-objects, pack files; df; every parent's and top-level entry's files and bytes; the four
           KEEP clones' and two fixture repositories' files, bytes and index time — all to OUT/before.txt; then the parent check proved:
           for every REMOVE row, what its parent holds beside the clone, computed now, equals the row's beside_files / beside_bytes.
  fixture  2b: I2 on a fixture under OUT/fixture/ (a file, a subdirectory, a hard link to a file outside, a symlink to a directory
           outside); the removal command run on it; the two outside targets present and unchanged. Result to OUT/fixture_result.txt.
  remove   2c + 2d: for each row in the list's order, the seven checks by stat and file reads only (no git command in any clone),
           then `rm -rf --one-file-system -- <top>` as one argv element, then the parent's remaining files and bytes against the
           row's beside figures less what this step already removed from that parent. Log: OUT/removal_log.tsv. A parent mismatch
           stops at once (exit 3).
  after    2e: every REMOVE top absent or logged SKIPPED; the KEEP clones and fixture repositories as 2a read them; every parent
           present; 2a's reads again to OUT/after.txt; df's change beside the list's nlink1 sum.

The parent definition is 12ai_CLNA/helpers/clna_check.py's, quoted:
    for child in parent.iterdir():
        if child == Path(x["top"]):
            continue
        if child.is_symlink() or child.is_file():
            beside_n += 1; beside_b += os.lstat(child).st_size if not child.is_symlink() else 0
        else:
            f_, b_, _ = newest(child); beside_n += f_; beside_b += b_
with newest() counting every non-directory entry of an os.walk(followlinks=False) as a file and every regular file's st_size as bytes.
"""
import hashlib, json, os, stat, subprocess, sys, time
from pathlib import Path

OUT = Path(sys.argv[1]).resolve(); PHASE = sys.argv[2]
SCR = Path("/home/ankitsarin/scratch"); MAIN = Path.cwd().resolve()
LIST = MAIN / "docs/session-reports/session-12/12ai_CLNA/removal_list.tsv"
I2A = MAIN / "docs/session-reports/session-12/12ai_CLNA/i2_after.tsv"
LIST_SHA = "799f82dc533f5ce31286595123f94f906672752698f522e02f194c83f262d1e7"
KEEP_FIXTURE = ["12ai-clna/clone", "12ai-open/recount", "12ai-u30b/clone", "12ai-u41b/clone",
                "12aa-u20c/bt_sweep/test_t6_git_state_reads_the_st0/repo", "12w-u17b/census/basetemp/test_t6_git_state_reads_the_st0/repo"]
ENV = {**os.environ, "GIT_OPTIONAL_LOCKS": "0", "LC_ALL": "C"}
now = lambda: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def newest(entry):
    files = byt = 0
    for d, dn, fn in os.walk(entry, followlinks=False):
        for f in fn:
            st = os.lstat(os.path.join(d, f)); files += 1
            if stat.S_ISREG(st.st_mode): byt += st.st_size
    return files, byt


def beside(parent, exclude):
    n = b = 0
    for child in Path(parent).iterdir():
        if exclude is not None and child == Path(exclude):
            continue
        if child.is_symlink() or child.is_file():
            n += 1; b += os.lstat(child).st_size if not child.is_symlink() else 0
        else:
            f_, b_ = newest(child); n += f_; b += b_
    return n, b


def apparent(top):
    a = 0
    for d, dn, fn in os.walk(top, followlinks=False):
        for f in fn:
            st = os.lstat(os.path.join(d, f))
            if stat.S_ISREG(st.st_mode): a += st.st_size
    return a


def rows():
    lines = LIST.read_text().splitlines(); h = lines[0].split("\t")
    return [dict(zip(h, l.split("\t"))) for l in lines[1:]]


def git_main(*a):
    return subprocess.run(["git", "-C", str(MAIN), *a], capture_output=True, text=True, env=ENV).stdout


def df():
    return subprocess.run(["df", "-B1", "--output=source,size,used,avail", str(SCR)], capture_output=True, text=True).stdout


def reads():
    pack = MAIN / ".git/objects/pack"; pf = sorted(p for p in pack.iterdir() if p.is_file())
    out = [f"# reads at {now()}", f"main HEAD: {git_main('rev-parse', 'HEAD').strip()}",
           f"main status --porcelain lines: {len(git_main('status', '--porcelain').splitlines())}",
           "main count-objects -v:", git_main("count-objects", "-v").rstrip(),
           f"main .git/objects/pack: {len(pf)} files, {sum(p.stat().st_size for p in pf)} B",
           "df -B1 (the filesystem holding ~/scratch):", df().rstrip(), "# parents (each REMOVE row's parent) and their top-level entries: files, bytes"]
    seen = set()
    for r in rows():
        if r["verdict"] != "REMOVE":
            continue
        for p in (Path(r["parent"]), SCR / Path(r["top"]).relative_to(SCR).parts[0]):
            if p in seen:
                continue
            seen.add(p)
            f, b = newest(p) if p.exists() else ("absent", "absent")
            out.append(f"ENTRY\t{p}\t{f}\t{b}")
    out.append("# the four KEEP clones and two fixture repositories: files, bytes, .git/index mtime ns")
    for k in KEEP_FIXTURE:
        t = SCR / k; f, b = newest(t); ix = t / ".git/index"
        out.append(f"KEEP\t{k}\t{f}\t{b}\t{os.lstat(ix).st_mtime_ns if ix.exists() else 'absent'}")
    return "\n".join(out) + "\n"


def index_times():
    d = {}
    for l in I2A.read_text().splitlines():
        p = l.split("\t")
        if p[0] == "INDEX": d[p[1]] = p[2]
    return d


def head_of(top):
    h = (top / ".git/HEAD").read_text().strip()
    if not h.startswith("ref: "):
        return h, "detached"
    ref = h[5:]; loose = top / ".git" / ref
    if loose.is_file():
        return loose.read_text().strip(), f"loose {ref}"
    pr = top / ".git/packed-refs"
    if pr.is_file():
        for l in pr.read_text().splitlines():
            if l and not l.startswith(("#", "^")) and l.split()[1] == ref:
                return l.split()[0], f"packed-refs {ref}"
    return None, f"unresolved {ref}"


def in_use(top):
    uid = os.getuid(); t = str(top); hits = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            if os.stat(f"/proc/{pid}").st_uid != uid:
                continue
            ps = [os.readlink(f"/proc/{pid}/cwd")] + [os.readlink(f"/proc/{pid}/fd/{fd}") for fd in os.listdir(f"/proc/{pid}/fd")]
        except OSError:
            continue
        if any(p == t or p.startswith(t + "/") for p in ps):
            hits.append(pid)
    return hits


if PHASE == "prep":
    sha = hashlib.sha256(LIST.read_bytes()).hexdigest(); assert sha == LIST_SHA, f"LIST sha256 {sha}"
    rm = [r for r in rows() if r["verdict"] == "REMOVE"]; assert len(rm) == 53, len(rm)
    with open(OUT / "to_remove.tsv", "w") as f:
        f.write("\t".join(["top", "head", "apparent", "nlink1", "parent", "beside_files", "beside_bytes"]) + "\n")
        for r in rm:
            f.write("\t".join(r[k] for k in ["top", "head", "apparent", "nlink1", "parent", "beside_files", "beside_bytes"]) + "\n")
    alt = MAIN / ".git/objects/info/alternates"; alt_t = alt.read_text() if alt.exists() else None
    wt = git_main("worktree", "list"); i1 = (alt_t is None or "scratch" not in alt_t) and len(wt.strip().splitlines()) == 1
    chk = []; bad = 0
    for r in rm:
        n, b = beside(r["parent"], r["top"]); ok = (str(n), str(b)) == (r["beside_files"], r["beside_bytes"]); bad += not ok
        chk.append(f"PARENTCHECK\t{r['top']}\t{n}\t{b}\t{r['beside_files']}\t{r['beside_bytes']}\t{'equal' if ok else 'DIFFERS'}")
    text = (f"# 12ai-CLNB 2a, {now()}\nLIST sha256 {sha} (expected {LIST_SHA})\nREMOVE rows: {len(rm)}\n"
            f"I1: alternates {'absent' if alt_t is None else repr(alt_t)}; worktree list: {wt.strip()!r}; holds: {i1}\n"
            + reads() + "# the parent check proved (now vs the row): top, files, bytes, row files, row bytes, verdict\n" + "\n".join(chk)
            + f"\nparent check: {53 - bad} of 53 equal\n")
    (OUT / "before.txt").write_text(text); print(text[-400:])
    sys.exit(0 if i1 and bad == 0 else 1)

if PHASE == "fixture":
    fx = OUT / "fixture"; outside = OUT / "fixture_outside"
    for p in (fx, outside):
        assert not p.exists(), f"{p} exists"
    (outside / "target_dir").mkdir(parents=True)
    (outside / "target_file.txt").write_text("outside file, hard-linked from the victim\n")
    (outside / "target_dir" / "inner.txt").write_text("inside the outside directory a symlink points at\n")
    victim = fx / "victim"; (victim / "sub").mkdir(parents=True)
    (victim / "file.txt").write_text("a file\n"); (victim / "sub" / "deep.txt").write_text("a deeper file\n")
    os.link(outside / "target_file.txt", victim / "hardlink.txt"); os.symlink(outside / "target_dir", victim / "symlink_dir")

    def state():
        return {str(p.relative_to(outside)): (hashlib.sha256(p.read_bytes()).hexdigest(), os.lstat(p).st_nlink)
                for p in sorted(outside.rglob("*")) if p.is_file()}
    before = state(); listing_before = sorted(str(p.relative_to(fx)) for p in fx.rglob("*"))
    cmd = ["rm", "-rf", "--one-file-system", "--", str(victim)]
    r = subprocess.run(cmd, capture_output=True, text=True); after = state()
    same = {k: v[0] for k, v in before.items()} == {k: v[0] for k, v in after.items()} and set(before) == set(after)
    text = (f"# 12ai-CLNB 2b, {now()}\ncommand (argv): {cmd}\nexit {r.returncode}; stderr {r.stderr!r}\n"
            f"victim listing before: {listing_before}\nvictim exists after: {victim.exists()}; fixture dir exists after: {fx.exists()}\n"
            f"outside targets before (path: sha256, nlink): {before}\noutside targets after: {after}\n"
            f"outside targets present and unchanged by sha256: {same} (the hard-linked file's link count falls by one, its content unchanged)\n")
    (OUT / "fixture_result.txt").write_text(text); print(text); sys.exit(0 if same and r.returncode == 0 and not victim.exists() else 1)

if PHASE == "remove":
    ix = index_times(); rm = [dict(zip(l.split("\t")[:0] or ["top", "head", "apparent", "nlink1", "parent", "beside_files", "beside_bytes"],
                                       l.split("\t"))) for l in (OUT / "to_remove.tsv").read_text().splitlines()[1:]]
    removed_from = {}   # parent -> (files, bytes) removed so far, in beside()'s terms
    log = open(OUT / "removal_log.tsv", "w")
    log.write("\t".join(["n", "top", "c1_path", "c2_dir", "c3_git", "c4_head", "c5_index_ns", "c6_apparent", "c7_in_use", "result",
                         "utc", "parent_now_files", "parent_now_bytes", "parent_expected_files", "parent_expected_bytes", "arithmetic"]) + "\n")
    for i, r in enumerate(rm, 1):
        top = Path(r["top"]); res = {}
        rel = top.relative_to(SCR).parts if str(top).startswith(str(SCR) + "/") else ()
        res["c1"] = (top.is_absolute() and str(top).startswith(str(SCR) + "/") and len(rel) >= 2 and not rel[0].startswith("12ai-"),
                     f"components below scratch {len(rel)}; first {rel[0] if rel else '-'}")
        res["c2"] = (top.exists() and top.is_dir() and not top.is_symlink() and os.path.realpath(top) == str(top), f"realpath {os.path.realpath(top)}")
        res["c3"] = ((top / ".git").is_dir() and not (top / ".git").is_symlink(), ".git dir")
        h, how = head_of(top) if res["c2"][0] and res["c3"][0] else (None, "-")
        res["c4"] = (h == r["head"], f"{h} ({how})")
        key = "/".join(rel); ixn = str(os.lstat(top / ".git/index").st_mtime_ns) if (top / ".git/index").exists() else "absent"
        res["c5"] = (ix.get(key) == ixn, f"{ixn} vs {ix.get(key)}")
        ap = apparent(top) if res["c2"][0] else -1
        res["c6"] = (str(ap) == r["apparent"], f"{ap} vs {r['apparent']}")
        iu = in_use(top) if res["c2"][0] else []
        res["c7"] = (not iu, f"pids {iu}")
        cells = [f"{'ok' if v[0] else 'FAIL'}: {v[1]}" for v in (res[k] for k in ("c1", "c2", "c3", "c4", "c5", "c6", "c7"))]
        if not all(v[0] for v in res.values()):
            log.write("\t".join([str(i), str(top), *cells, "SKIPPED", now(), "", "", "", "", ""]) + "\n"); log.flush(); continue
        own_f, own_b = newest(top)          # the clone's contribution to its parent, in beside()'s terms
        p = r["parent"]; prior = removed_from.get(p, (0, 0))
        cmd = ["rm", "-rf", "--one-file-system", "--", str(top)]
        rr = subprocess.run(cmd, capture_output=True, text=True)
        t = now()
        gone = not os.path.lexists(top); parent_ok = Path(p).is_dir()
        nf, nb = beside(p, None) if parent_ok else (-1, -1)
        ef, eb = int(r["beside_files"]) - prior[0], int(r["beside_bytes"]) - prior[1]
        removed_from[p] = (prior[0] + own_f, prior[1] + own_b)
        arith = f"row beside {r['beside_files']} / {r['beside_bytes']} less this step's earlier removals from the parent {prior[0]} / {prior[1]}"
        result = "REMOVED" if (rr.returncode == 0 and gone and parent_ok and (nf, nb) == (ef, eb)) else "STOP"
        log.write("\t".join([str(i), str(top), *cells, result + (f" (rm exit {rr.returncode} {rr.stderr.strip()!r})" if rr.returncode else ""),
                             t, str(nf), str(nb), str(ef), str(eb), arith]) + "\n"); log.flush()
        if result == "STOP":
            listing = "\n".join(sorted(str(x) for x in Path(p).iterdir())) if parent_ok else "(parent absent)"
            (OUT / "stop_parent_listing.txt").write_text(f"{p}\n{listing}\n"); print(f"STOP at row {i} {top}"); sys.exit(3)
    log.close()
    lines = (OUT / "removal_log.tsv").read_text().splitlines()[1:]
    print(f"removed {sum(1 for l in lines if l.split(chr(9))[9].startswith('REMOVED'))}, skipped {sum(1 for l in lines if l.split(chr(9))[9] == 'SKIPPED')}")

if PHASE == "after":
    rm = [l.split("\t") for l in (OUT / "to_remove.tsv").read_text().splitlines()[1:]]
    logd = {l.split("\t")[1]: l.split("\t")[9] for l in (OUT / "removal_log.tsv").read_text().splitlines()[1:]}
    absent = [r[0] for r in rm if not os.path.lexists(r[0])]
    odd = [r[0] for r in rm if os.path.lexists(r[0]) and logd.get(r[0]) != "SKIPPED"]
    parents_present = all(Path(r[4]).is_dir() for r in rm)
    text = reads()
    b = {l.split("\t")[1]: l for l in (OUT / "before.txt").read_text().splitlines() if l.startswith("KEEP\t")}
    a = {l.split("\t")[1]: l for l in text.splitlines() if l.startswith("KEEP\t")}
    keep_same = b == a
    def avail(t):
        ln = [l for l in t.splitlines() if l.startswith("/dev/")][0].split(); return int(ln[2]), int(ln[3])
    ub, ab = avail((OUT / "before.txt").read_text()); ua, aa = avail(text)
    nl1 = sum(int(r[3]) for r in rm if r[0] in absent)
    summary = (f"# 2e, {now()}\nREMOVE tops absent: {len(absent)} of {len(rm)}; present and not SKIPPED: {odd}\n"
               f"every parent present: {parents_present}\nKEEP clones and fixture repositories as 2a read them: {keep_same}\n"
               f"df avail before {ab} B, after {aa} B, rise {aa - ab} B; used before {ub}, after {ua}, fall {ub - ua} B\n"
               f"the removed rows' nlink1 sum {nl1} B; rise >= sum: {aa - ab >= nl1} (E4)\n")
    (OUT / "after.txt").write_text(text + summary); print(summary)
    sys.exit(0 if not odd and parents_present and keep_same else 1)
