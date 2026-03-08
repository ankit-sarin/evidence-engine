"""Launch long-running scripts in a detached tmux session with logging.

Usage — add one line at the top of main(), before argparse:

    from engine.utils.background import maybe_background
    maybe_background("screening", review_name="surgical_autonomy")

If --background is in sys.argv and the process is NOT already inside tmux,
re-launches the same command in a new detached tmux session with output
tee'd to a log file, then exits. Otherwise returns and the script runs
normally (foreground or already-in-tmux).
"""

import os
import shlex
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def maybe_background(stage: str, review_name: str = "review") -> None:
    """Re-launch in tmux if --background was passed and not already in tmux.

    Args:
        stage: Short label for the session name (e.g., "screening", "extraction").
        review_name: Review directory name under data/ for the log file.
    """
    if "--background" not in sys.argv:
        return

    # Strip --background so downstream argparse (or the re-launched process)
    # never sees it — prevents infinite re-exec loops.
    sys.argv.remove("--background")

    if os.environ.get("TMUX"):
        # Already inside tmux — just run in foreground within this session.
        return

    if not shutil.which("tmux"):
        print("Warning: tmux not found — running in foreground.", file=sys.stderr)
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_name = f"ee_{stage}_{timestamp}"

    log_dir = Path("data") / review_name / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{session_name}.log"

    # Rebuild the command without --background.
    cmd_parts = [sys.executable] + sys.argv
    cmd_str = " ".join(shlex.quote(p) for p in cmd_parts)
    # Tee stdout+stderr to the log file so it's both visible in tmux and saved.
    tmux_cmd = f"{cmd_str} 2>&1 | tee {shlex.quote(str(log_path))}"

    subprocess.run(
        ["tmux", "new-session", "-d", "-s", session_name, "bash", "-c", tmux_cmd],
        check=True,
    )

    print(f"Launched in background tmux session: {session_name}")
    print(f"Log: {log_path}")
    print(f"Reconnect: tmux attach -t {session_name}")
    sys.exit(0)
