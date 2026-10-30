#!/usr/bin/env python3
"""The session-open check (R746(e)(iii)). usage, from the repository root: session_open_check.py <expected.json>
Runs each read, prints one table row per value (MATCH, MISMATCH or READING), and exits 0 only if every expected value
matches. Every expected value comes from the file (R742(b)); a section the file omits is not run and is printed as
SKIPPED, so a short file cannot pass silently as a full open. Read-only: the database is touched only by the
fingerprint instrument and dbchecks (mode=ro), retained copies by file hash, Ollama by version, tags and ps.
"""
import datetime as dt, hashlib, json, os, re, subprocess, sys
from pathlib import Path

E = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
PY = ".venv/bin/python"
SECTIONS = ("repos", "plan", "fingerprint", "hashes", "pins", "dbchecks", "retention", "probe_scratch", "stage",
            "nightly", "health", "ollama", "temp_root", "cc_sessions")
bad = 0


def row(name, want, got, reading=False):
    global bad
    status = "READING" if reading else ("MATCH" if want == got else "MISMATCH")
    bad += status == "MISMATCH"
    print(f"{status:9} {name}: {got!r}" + ("" if status == "MATCH" or reading else f"  (expected {want!r})"))


def sh(*a, cwd=None):
    r = subprocess.run(a, capture_output=True, text=True, cwd=cwd)
    return r.returncode, r.stdout, r.stderr


def H(p):
    h = hashlib.sha256()
    with open(os.path.expanduser(p), "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def repos(v):
    for name, x in v.items():
        c = os.path.expanduser(x["path"])
        sh("git", "fetch", "-q", cwd=c)
        row(f"{name} HEAD", x["head"], sh("git", "rev-parse", "HEAD", cwd=c)[1].strip())
        row(f"{name} parent", x["parent"], sh("git", "rev-parse", "HEAD^", cwd=c)[1].strip())
        row(f"{name} status", x.get("status", []), sh("git", "status", "--porcelain", cwd=c)[1].splitlines())
        row(f"{name} ahead/behind", "0 0", " ".join(sh("git", "rev-list", "--left-right", "--count", "HEAD...@{u}", cwd=c)[1].split()))
        row(f"{name} worktrees", x["worktrees"], len(sh("git", "worktree", "list", cwd=c)[1].splitlines()))


def plan(v):
    b = Path(v["path"]).read_bytes()
    row("plan sha256", v["sha256"], hashlib.sha256(b).hexdigest()); row("plan bytes", v["bytes"], len(b))
    row("plan lines", v["lines"], b.count(b"\n")); row("plan last line", v["last_line"], b.decode().rstrip("\n").split("\n")[-1])


def fingerprint(v):
    rc, out, _ = sh(PY, "-m", "engine.tools.db_fingerprint", v["db"], "--compare", v["record"])
    row("fingerprint", "IDENTICAL", "IDENTICAL" if rc == 0 and "IDENTICAL" in out else f"rc {rc}")


def hashes(v):
    for p, want in v.items():
        row(f"sha256 {p}", want, H(p))


def pins(v):
    rc, out, _ = sh(PY, v["script"], v["spec"], v["codebook"])
    row("pin_tuples byte-identical", True, rc == 0 and out == Path(v["reference"]).read_text(encoding="utf-8"))


def dbchecks(v):
    rc, out, _ = sh(PY, v["script"])
    row("dbchecks byte-identical", True, rc == 0 and out == Path(v["reference"]).read_text(encoding="utf-8"))


def retention(v):
    root = os.path.expanduser(v["root"]); m = x = 0
    for l in open(v["reference"], encoding="utf-8"):
        if l.startswith("#") or " MATCH" not in l:
            continue
        s, p, _ = l.split()
        q = p if p.startswith(("/", "data/")) else os.path.join(root, p)
        same = os.path.exists(q) and H(q) == s
        m += same; x += not same
        if not same:
            print(f"          retention differs: {p}")
    row("retention MATCH / MISMATCH", [v["match"], 0], [m, x])


def probe_scratch(v):
    idx = json.load(open(v["requests_index"]))
    R = os.path.expanduser(v["requests_dir"])
    eq = sum(H(f"{R}/{k}.json") == x["full_request_file_sha256"] for k, x in idx.items())
    row("probe requests equal", [len(idx), v["requests"]], [eq, len(idx)])
    n = ne = ce = 0
    for sc, cm in v["responses"]:
        d = os.path.expanduser(sc)
        for f in sorted(os.listdir(d)):
            h = H(f"{d}/{f}"); n += 1; ne += f.startswith(h)
            c = f"{cm}/{f}"; ce += os.path.exists(c) and H(c) == h
    row("probe responses files / name-equal / committed-equal", [v["response_files"]] * 3, [n, ne, ce])


def stage(v):
    row(f"stage {v['path']}", v["entries"], sorted(os.listdir(os.path.expanduser(v["path"]))))


def nightly(v):
    log = Path(v["log"]).read_text(encoding="utf-8", errors="replace")
    md = Path(v["expected"]).read_text(encoding="utf-8")
    cells = {m.group(1).strip(): m.group(2) for m in re.finditer(r"^\| ([^|]+?) \| (.+?) \| [^|]* \|$", md, flags=re.M)}
    tick = lambda k: re.search(r"`([^`]+)`", cells[k]).group(1)
    row("nightly collection line", True, tick("Collection line") in log)
    row("nightly summary line", True, tick("Summary line") in log)
    row("nightly exit-code line", tick("Exit-code line"), log.rstrip().split("\n")[-1])
    row("nightly durations file present", True, Path(v["durations"]).stat().st_size > 0)
    key = next(k for k in cells if k.startswith("Ollama journal"))
    a, b = re.search(r"(\d\d:\d\d)–(\d\d:\d\d)", key).groups()
    j = sh("journalctl", "-u", "ollama", "--utc", "--since", f"{v['date']} {a}:00", "--until", f"{v['date']} {b}:00", "--no-pager")[1]
    loads = [l for l in j.splitlines() if re.search(r"llama_context|POST|starting runner|load", l)]
    row("nightly journal window loads", 0, len(loads))
    row("nightly journal window lines", "-- No entries --", j.strip().splitlines()[-1] if j.strip() else "-- No entries --", reading=True)
    lock_key = next(k for k in cells if "lock file" in k)
    want = re.search(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", cells[lock_key]).group(0)
    row("the real lock file's mtime", want, dt.datetime.fromtimestamp(os.stat(os.path.expanduser(v["lock"])).st_mtime, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))


def health(v):
    p = os.path.expanduser(v["log"])
    row("health log mtime date", v["date"], dt.datetime.fromtimestamp(os.stat(p).st_mtime, dt.timezone.utc).strftime("%Y-%m-%d"))
    tests = [l for l in open(p, encoding="utf-8", errors="replace") if l.startswith("Testing ")][-v["models"]:]
    row(f"health last {v['models']} models OK", v["models"], sum(" OK " in l for l in tests))


def ollama(v):
    get = lambda ep: json.loads(sh("curl", "-s", "--max-time", "10", f"{v['host']}/api/{ep}")[1])
    row("ollama version", v["version"], get("version")["version"])
    tags = {m["name"]: m["digest"] for m in get("tags")["models"]}
    for name, d in v["digests"].items():
        row(f"ollama digest {name}", d, tags.get(name))
    row("ollama tags", v["tags"], len(tags), reading=True)
    row("ollama ps", [], [m["name"] for m in get("ps")["models"]], reading=True)
    row("ollama NRestarts", v["nrestarts"], int(sh("systemctl", "show", "ollama", "--property=NRestarts", "--value")[1].strip()))


def temp_root(v):
    p = os.path.expanduser(v["path"])
    print(f"READING   temp root: {sorted(os.listdir(p))}, {sh('du', '-sb', p)[1].split()[0]} B")


def cc_sessions(v):
    anc, p = set(), os.getpid()
    while p > 1:
        anc.add(p); p = int(open(f"/proc/{p}/stat").read().rsplit(")", 1)[1].split()[1])
    out = sh("ps", "-u", os.environ["USER"], "-o", "pid=,ppid=,lstart=,etime=,args=")[1]
    lo, hi = (dt.datetime.fromisoformat(x) for x in v["window"])
    found = []
    for l in out.splitlines():
        f = l.split(None, 8)
        if int(f[0]) in anc:
            continue
        t = dt.datetime.strptime(" ".join(f[2:7]), "%a %b %d %H:%M:%S %Y")
        if lo <= t <= hi or any(s in l for s in v["ids"]):
            found.append(l[:200]); print(f"          leftover: {l[:200]}")
    row(f"processes of sessions {v['ids']} (started {v['window'][0]}..{v['window'][1]})", 0, len(found))


print(f"session_open_check {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} on {sys.argv[1]}")
for s in SECTIONS:
    if s in E:
        globals()[s](E[s])
    else:
        print(f"SKIPPED   {s}: not in the expected file")
print(f"RESULT: {'PASS' if not bad else 'FAIL'} ({bad} mismatches)")
sys.exit(0 if not bad else 1)
