"""L04-1: git_state() ignores the return code of `git status --porcelain`.
A throwaway repo under a temp dir; its index is corrupted after one commit and a
tracked file is then modified, so the tree IS dirty and `git status` fails."""
import subprocess, tempfile, pathlib
from engine.core.run_manifest import git_state

with tempfile.TemporaryDirectory() as d:
    r = pathlib.Path(d)
    def g(*a):
        return subprocess.run(["git", "-C", d, "-c", "user.name=t", "-c", "user.email=t@t", *a],
                              capture_output=True, text=True)
    g("init", "-q"); (r / "a.py").write_text("x = 1\n"); g("add", "a.py"); g("commit", "-qm", "c")
    (r / "a.py").write_text("x = 2  # uncommitted edit\n")
    print("healthy repo, edited file  :", git_state(r))
    (r / ".git" / "index").write_bytes(b"not an index")
    st = g("status", "--porcelain")
    print("git status rc / stdout / err:", st.returncode, repr(st.stdout), st.stderr.strip()[:80])
    gs = git_state(r)
    print("corrupt index, edited file :", gs)
    print("commit is 40 hex:", len(gs.commit) == 40, "| dirty:", gs.dirty,
          "-> open_run's DirtyTree check would", "REFUSE" if gs.dirty else "PASS")
