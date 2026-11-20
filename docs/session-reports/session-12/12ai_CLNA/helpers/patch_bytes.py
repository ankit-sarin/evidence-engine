"""12ai-CLNA 2d, beside G4: for each clone with a patch in INV/patches/, `git diff HEAD` now (GIT_OPTIONAL_LOCKS=0) against the
committed patch, byte for byte (a stronger test than G4's numstat). Read-only. usage, from the repo root: python3 -I -B patch_bytes.py <OUT>"""
import os, subprocess, sys
from pathlib import Path
OUT = Path(sys.argv[1]); INV = Path("docs/session-reports/session-12/12ai_SCRI"); SCR = Path.home() / "scratch"
ENV = {**os.environ, "GIT_OPTIONAL_LOCKS": "0", "LC_ALL": "C"}
rows = []
for p in sorted((INV / "patches").glob("*.patch")):
    top = SCR / p.stem.replace("__", "/")
    now = subprocess.run(["git", "-C", str(top), "diff", "HEAD"], capture_output=True, env=ENV).stdout
    rows.append(f"{p.stem.replace('__', '/')}\t{len(p.read_bytes())}\t{len(now)}\t{'EQUAL' if now == p.read_bytes() else 'DIFFERS'}")
(OUT / "t2d_patch_bytes.tsv").write_text("repo\tpatch_bytes\tdiff_bytes_now\tbyte_for_byte\n" + "\n".join(rows) + "\n")
print("\n".join(rows))
