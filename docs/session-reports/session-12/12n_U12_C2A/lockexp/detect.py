"""12n-U12-C2A A8(ii) — what three candidate detectors see, on the SCRATCH lock only.
D1 = a descriptor on the lock inode already open in this process (inherited);
D2 = /proc/locks names an ANCESTOR pid as the FLOCK holder of the inode;
D3 = an ANCESTOR process has a descriptor open on the lock inode (/proc/<pid>/fd, same uid)."""
import os, sys
p = os.environ.get("OLLAMA_EXPERIMENT_LOCK", "")
assert p.startswith(os.path.expanduser("~/scratch/12n-c2a/")), "scratch lock path required"
st = os.stat(p); ino = (st.st_dev, st.st_ino)
def fds(pid):
    out = []
    try:
        for fd in os.listdir(f"/proc/{pid}/fd"):
            try:
                s = os.stat(f"/proc/{pid}/fd/{fd}")
                if (s.st_dev, s.st_ino) == ino: out.append(int(fd))
            except OSError: pass
    except OSError as e: return f"unreadable({e.errno})"
    return out
anc = []; pid = os.getpid()
while pid > 1:
    pid = int(open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()[1]); anc.append(pid)
key = f"{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}:{st.st_ino}"
holders = [int(l.split()[4]) for l in open("/proc/locks") if key in l and "FLOCK" in l]
d1 = fds(os.getpid()); d2 = [h for h in holders if h in anc]; d3 = {a: fds(a) for a in anc if fds(a)}
print(f"  D1 inherited_fd={bool(d1)} {d1} | D2 ancestor_in_proc_locks={bool(d2)} | D3 ancestor_fd={bool(d3)} {d3} | holders={holders}")
