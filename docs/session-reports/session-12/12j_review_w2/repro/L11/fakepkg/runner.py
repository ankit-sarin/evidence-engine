"""Synthetic stand-in for engine/agents/ft_screener.py: absolute package imports
at the top, no sys.path insert, maybe_background called from main()."""
import json, os, sys
from fakepkg.sibling import X          # like `from engine.core import ...`

def main():
    import shutil, subprocess
    from unittest.mock import patch
    from engine.utils.background import maybe_background
    captured = {}
    def fake_run(argv, **kw):          # the tmux boundary: never spawned
        captured["argv"] = argv
    os.environ.pop("TMUX", None)
    with patch("shutil.which", return_value="/usr/bin/tmux"), patch("subprocess.run", fake_run):
        try:
            maybe_background("ft_screening", review_name="synthetic")
        except SystemExit as e:
            captured["exit"] = e.code
    if captured:
        print(json.dumps(captured))
    else:
        print("RELAUNCHED-OK: imports resolved, running in foreground")

if __name__ == "__main__":
    main()
