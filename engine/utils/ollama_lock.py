"""Cross-process advisory lock guarding long-running Ollama experiments.

Root cause this exists for (OPS-OLLAMA-02): two independent things reached into
a shared Ollama server and destroyed multi-hour experiment runs — the Evidence
Engine's own proactive `restart_ollama()` (RESTART_EVERY_N=25) killed the Arm P
rerun, and the 07:00 health cron killed the attempt before it. Neither knew an
experiment was in flight, because there was nothing to ask.

Design, pinned:

  * **flock(2) only. Never existence checks.** The lock file is created once and
    left on disk forever; its presence means nothing. Ownership is the kernel's
    flock state on an open file description, so a holder that dies — crash,
    SIGKILL, power loss — releases the lock the instant its fd closes. There is
    no such thing as a stale lock here, and no cleanup path to get wrong.

  * **One well-known path**, shared by this package and by out-of-repo shell
    tooling (`~/scripts/ollama_health_check.sh`) and, in future, by other
    projects on this box. See LOCK_PATH below for why $HOME.

  * **Self vs. foreign.** flock associates a lock with the *open file
    description*, not the process, so a second `open()` + `LOCK_EX|LOCK_NB` from
    inside the very process that already holds the lock fails exactly like a
    foreign holder's would. A probe therefore cannot tell "someone else has it"
    from "I have it" — so this module tracks self-ownership explicitly in
    `_SELF_DEPTH`. `check_experiment_lock()` answers "is it held by anyone",
    `self_holds_lock()` answers "is that me", and `foreign_lock_held()` is the
    conjunction callers almost always want.

Usage:

    from engine.utils.ollama_lock import hold_experiment_lock

    with hold_experiment_lock():
        ...run the multi-hour experiment...

No Ollama calls, no network, no third-party dependencies.
"""

from __future__ import annotations

import fcntl
import logging
import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

# Why $HOME and not /run/user/$UID or /tmp:
#   * /run/user/$UID (XDG_RUNTIME_DIR) is tmpfs and correctly disappears on
#     reboot, but systemd-logind may remove it when the user has no login
#     session — and the guard's most important consumer is a *cron* job, which
#     often has neither XDG_RUNTIME_DIR set nor a session.
#   * /tmp is world-writable; another uid could create the file first with
#     permissions we cannot open for write, silently disabling the guard.
#   * $HOME always exists for both interactive and cron contexts and is
#     single-owner. The file surviving a reboot is harmless precisely because
#     nothing ever reads its existence — only its flock state.
# Override exists for tests and for anyone wanting a per-box path.
_DEFAULT_LOCK_PATH = Path.home() / ".ollama_experiment.lock"


def lock_path() -> Path:
    """Resolve the lock path, honouring the OLLAMA_EXPERIMENT_LOCK override."""
    override = os.environ.get("OLLAMA_EXPERIMENT_LOCK")
    return Path(override) if override else _DEFAULT_LOCK_PATH


# Self-ownership bookkeeping. Guarded by a mutex so a threaded caller cannot
# corrupt the depth count; the flock itself is process-wide either way.
_STATE_LOCK = threading.RLock()
_SELF_DEPTH = 0
_SELF_FD: int | None = None


def _open_lock_file() -> int:
    """Open (creating if needed) the lock file and return a raw fd."""
    path = lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    # O_RDWR|O_CREAT, never truncate: the file's contents are irrelevant, and
    # truncating would race harmlessly but pointlessly with other openers.
    return os.open(path, os.O_RDWR | os.O_CREAT, 0o644)


def self_holds_lock() -> bool:
    """True if *this process* currently holds the experiment lock."""
    with _STATE_LOCK:
        return _SELF_DEPTH > 0


def check_experiment_lock() -> bool:
    """Non-blocking probe: is the experiment lock held by anyone?

    Returns True if an exclusive lock could NOT be taken — i.e. some open file
    description somewhere (possibly this process's own) holds it. Returns False
    if the lock is free.

    Never blocks, never raises on contention, and leaves the lock state exactly
    as it found it: on success the probe lock is released immediately.
    """
    with _STATE_LOCK:
        if _SELF_DEPTH > 0:
            # Short-circuit: we hold it, so it is definitionally held. Probing
            # with a second fd would also report "held", but this avoids the
            # pointless open() and makes the intent explicit.
            return True
    fd = None
    try:
        fd = _open_lock_file()
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    except BlockingIOError:
        return True
    except OSError as exc:
        # A lock we cannot even open is a lock we must not pretend to have
        # cleared. Fail safe: report "held" so callers take the cautious branch.
        logger.warning("Experiment lock probe failed (%s) — assuming held", exc)
        return True
    finally:
        if fd is not None:
            os.close(fd)


def foreign_lock_held() -> bool:
    """True if the lock is held by someone *other than this process*.

    This is the predicate destructive actions should gate on: the engine
    restarting Ollama while it itself holds the lock is intended behaviour;
    restarting under someone else's experiment is the bug this module prevents.
    """
    return check_experiment_lock() and not self_holds_lock()


@contextmanager
def hold_experiment_lock(blocking: bool = True) -> Iterator[Path]:
    """Hold the experiment lock for the duration of the block.

    Blocking by default: an experiment that has to wait is safe, whereas an
    experiment that proceeds unguarded can be destroyed. Pass blocking=False to
    raise BlockingIOError instead of waiting.

    Re-entrant. A nested acquire in the same process must NOT re-flock: flock is
    per open file description, so a second blocking acquire from the same
    process would deadlock against itself. Nested calls bump a depth counter and
    reuse the outermost fd.
    """
    global _SELF_DEPTH, _SELF_FD

    with _STATE_LOCK:
        nested = _SELF_DEPTH > 0
        if nested:
            _SELF_DEPTH += 1

    if nested:
        try:
            yield lock_path()
        finally:
            with _STATE_LOCK:
                _SELF_DEPTH -= 1
        return

    fd = _open_lock_file()
    flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
    try:
        fcntl.flock(fd, flags)
    except Exception:
        os.close(fd)
        raise

    with _STATE_LOCK:
        _SELF_DEPTH = 1
        _SELF_FD = fd
    logger.info("Experiment lock acquired: %s (pid %d)", lock_path(), os.getpid())

    try:
        yield lock_path()
    finally:
        with _STATE_LOCK:
            _SELF_DEPTH = 0
            _SELF_FD = None
        # Closing the fd releases the flock; the explicit LOCK_UN is belt and
        # braces for readability, not correctness.
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
        logger.info("Experiment lock released: %s (pid %d)", lock_path(), os.getpid())
