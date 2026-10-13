"""12n-U12-C2A A2 — one child process for the lock experiments. Never touches the real lock:
refuses unless OLLAMA_EXPERIMENT_LOCK points under ~/scratch/12n-c2a/."""
import os, sys, time
p = os.environ.get("OLLAMA_EXPERIMENT_LOCK", "")
assert p.startswith(os.path.expanduser("~/scratch/12n-c2a/")), "scratch lock path required"
from engine.utils import ollama_lock as L
assert str(L.lock_path()) == p
mode = sys.argv[1]
def inherited_fds():
    st = os.stat(p); out = []
    for fd in os.listdir("/proc/self/fd"):
        try:
            s = os.stat(f"/proc/self/fd/{fd}")
            if (s.st_dev, s.st_ino) == (st.st_dev, st.st_ino): out.append(int(fd))
        except OSError: pass
    return out
def proc_locks():
    st = os.stat(p); key = f"{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}:{st.st_ino}"
    return [l.split() for l in open("/proc/locks") if key in l]
def ancestors():
    out = []; pid = os.getpid()
    while pid > 1:
        pid = int(open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()[1]); out.append(pid)
    return out
print(f"  pid={os.getpid()} inherited_fds_on_lock_inode={inherited_fds()} "
      f"probe.check_experiment_lock={L.check_experiment_lock()} self_holds={L.self_holds_lock()} "
      f"foreign_lock_held={L.foreign_lock_held()}", flush=True)
rows = proc_locks(); anc = ancestors()
print("  /proc/locks:", [(r[1], r[3], r[4], "ANCESTOR" if int(r[4]) in anc else ("SELF" if int(r[4]) == os.getpid() else "other")) for r in rows], flush=True)
if mode == "blocking":
    print("  acquiring, blocking ...", flush=True)
    t = time.time()
    with L.hold_experiment_lock():
        print(f"  ACQUIRED after {time.time()-t:.2f}s", flush=True)
elif mode == "nonblocking":
    try:
        with L.hold_experiment_lock(blocking=False): print("  ACQUIRED (non-blocking)", flush=True)
    except BlockingIOError as e: print(f"  BlockingIOError: {e}", flush=True)
elif mode == "holder_spawns":      # a Python holder, then a subprocess child and a fork child
    import subprocess, multiprocessing
    with L.hold_experiment_lock():
        print(f"  holder fd={L._SELF_FD} inheritable={os.get_inheritable(L._SELF_FD)}", flush=True)
        print("  -- subprocess child (exec):", flush=True)
        subprocess.run([sys.executable, __file__, "nonblocking"], timeout=20)
        def forked():
            print(f"  -- fork child: self_holds={L.self_holds_lock()} foreign={L.foreign_lock_held()} "
                  f"inherited_fds={inherited_fds()}", flush=True)
            with L.hold_experiment_lock(blocking=False): print("  -- fork child: nested acquire yielded without flock", flush=True)
        pr = multiprocessing.get_context("fork").Process(target=forked); pr.start(); pr.join(20)
