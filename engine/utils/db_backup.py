"""WAL-aware backup and restore for the review database.

SAFE-GROUND-01. This module used to be one `shutil.copy2` of the main database
file. Under the journal mode the engine actually runs in — `ReviewDatabase`
sets `PRAGMA journal_mode=WAL` in `__init__` and holds its connection open for
its lifetime — that copies whatever SQLite happened to have checkpointed and
leaves everything since in the `-wal`, which is not copied. Measured in that
shape, the "backup" of a database holding one paper, one extraction and one
span was a 4,096-byte file **with no tables at all**: the schema itself had not
been checkpointed yet.

The old test could not see it. Its fixture closed the connection and never
enabled WAL, so its three assertions — readable copy, filename format, size
equality — all passed against the defect. A fixture more permissive than
production proves a path production cannot be in.

So: the backup is taken through SQLite's online backup API, which reads the
database the way a reader does and therefore sees every committed frame in the
WAL. The result is one self-contained file; no `-wal`/`-shm` sidecar is written
beside it, because the copy is switched to a rollback journal before it is
closed. Taking it does not checkpoint the source.

**A backup is not a backup until it has been read back.** Every backup is
fingerprinted against its source before this function returns, and a mismatch
deletes the file and raises rather than leaving something that looks like a
safety net. The fingerprint is `engine.tools.db_fingerprint`'s, imported — not
a second copy of the same idea.

`restore` is a function and deliberately has no CLI. The one operation that
overwrites a database should not be reachable by tab-completing a shell
history entry.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from engine.tools.db_fingerprint import compare, fingerprint, read_snapshot

logger = logging.getLogger(__name__)


class BackupVerificationError(RuntimeError):
    """A backup did not match its source, and has been deleted.

    Raised before any caller can treat the file as a safety net. The message
    names every table that differed, because "the backup is wrong" without
    saying where is a diagnosis handed to whoever has the least context.
    """


class RestoreRefused(RuntimeError):
    """A restore was refused before anything was written."""


@dataclass(frozen=True)
class BackupResult:
    """What `auto_backup` returns: the file, and the proof it is complete."""

    path: Path
    fingerprint: dict

    @property
    def overall_sha256(self) -> str:
        return self.fingerprint["overall_sha256"]

    @property
    def table_count(self) -> int:
        return self.fingerprint["table_count"]


# ── Backup ───────────────────────────────────────────────────────────


def _remove_sidecars(db_path: Path) -> None:
    """Remove any `-wal`/`-shm` beside `db_path`.

    A rollback-journal database has none, so after the mode switch this is a
    no-op in the success path; it is the cleanup that makes the failure path
    leave nothing behind.
    """
    for suffix in ("-wal", "-shm"):
        (db_path.parent / (db_path.name + suffix)).unlink(missing_ok=True)



def auto_backup(db_path_or_connection, reason: str) -> BackupResult:
    """Back up a SQLite database, verify it, and return the path and fingerprint.

    Args:
        db_path_or_connection: the database file, or an open `sqlite3.Connection`
            whose view is to be captured. Passing the live connection is the
            honest form when the caller has one: it makes "back up what this
            connection can see" explicit rather than implicit in statement
            order.
        reason: short label for the filename — must be filesystem-safe.

    Returns:
        `BackupResult(path, fingerprint)`.

    Raises:
        BackupVerificationError: the backup's content differs from the source's.
            The backup file is deleted first.
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    with read_snapshot(db_path_or_connection) as (src, uri, db_path):
        backup_path = db_path.parent / f"{db_path.name}.bak-{reason}-{timestamp}"
        if backup_path.exists():
            raise FileExistsError(
                f"refusing to overwrite an existing backup: {backup_path}"
            )

        # Taken inside the snapshot the fingerprint below is computed under, so
        # the two describe one state of the database rather than two moments.
        dst = sqlite3.connect(str(backup_path))
        try:
            src.backup(dst)
            # The backup API copies the source's header, journal mode included,
            # so a backup of a WAL database is itself a WAL database — and
            # merely READING one creates `-shm` and `-wal` beside it. That is
            # how the untracked sidecars in `data/<review>/` got there. A
            # backup should be one file you can copy anywhere and open, so the
            # copy is switched to a rollback journal. No content changes; the
            # fingerprint below is taken after this and proves it.
            dst.execute("PRAGMA journal_mode=DELETE")
        finally:
            dst.close()
        _remove_sidecars(backup_path)

        source_fp = fingerprint(src)

    backup_fp = fingerprint(backup_path)

    diffs = compare(source_fp, backup_fp, left_label="source", right_label="backup")
    if diffs:
        backup_path.unlink(missing_ok=True)
        _remove_sidecars(backup_path)
        raise BackupVerificationError(
            f"backup of {db_path} did not match its source and was deleted; "
            f"{len(diffs)} difference(s): " + "; ".join(diffs[:10])
        )

    size_mb = backup_path.stat().st_size / (1024 * 1024)
    logger.info(
        "DB backup created and verified: %s (reason=%s, %.1f MB, %d tables, "
        "overall=%s)",
        backup_path.name, reason, size_mb,
        backup_fp["table_count"], backup_fp["overall_sha256"][:16],
    )
    return BackupResult(path=backup_path, fingerprint=backup_fp)


# ── Restore ──────────────────────────────────────────────────────────


def _refuse_if_open(target: Path) -> None:
    """Refuse when any connection — this process's or another's — holds `target`.

    The check is an exclusive lock, not a scan of `/proc/*/fd`: it asks SQLite
    the question that actually matters ("can I have this database to myself?")
    instead of a proxy for it. Measured to refuse against an idle open
    connection, an open read transaction and an open write transaction alike,
    and to succeed only once every connection has closed.
    """
    if not target.exists():
        return
    probe = sqlite3.connect(str(target), timeout=0.2)
    try:
        probe.execute("PRAGMA locking_mode=EXCLUSIVE")
        probe.execute("BEGIN IMMEDIATE")
        probe.execute("COMMIT")
    except sqlite3.OperationalError as exc:
        raise RestoreRefused(
            f"refusing to restore over {target}: it is open elsewhere "
            f"({exc}). Close every connection to it — including the "
            f"ReviewDatabase that owns it — and retry."
        ) from exc
    finally:
        probe.close()


def restore(
    backup_path: str | Path,
    target_path: str | Path,
    *,
    expected_fingerprint: dict | None = None,
) -> dict:
    """Replace `target_path`'s database with `backup_path`'s content.

    Verifies the result: the restored database is fingerprinted and compared
    against the backup's, and against `expected_fingerprint` when one is given.
    A mismatch leaves `target_path` untouched.

    The restore is written to a sibling temporary file and moved into place with
    `os.replace`, so an interrupted restore cannot leave a half-written database
    at `target_path`. Any stale `-wal`/`-shm` beside the target is removed: they
    belong to the database that was there, and a new main file with an old WAL
    is a corrupt pair.

    Raises:
        RestoreRefused: the target is open, or the result did not verify.
    """
    backup_path = Path(backup_path).resolve()
    target_path = Path(target_path).resolve()

    if not backup_path.exists():
        raise RestoreRefused(f"no such backup: {backup_path}")
    if backup_path == target_path:
        raise RestoreRefused("backup and target are the same file")

    backup_fp = fingerprint(backup_path)
    if expected_fingerprint is not None:
        diffs = compare(expected_fingerprint, backup_fp,
                        left_label="expected", right_label="backup")
        if diffs:
            raise RestoreRefused(
                f"backup {backup_path.name} does not match the expected "
                f"fingerprint; {len(diffs)} difference(s): " + "; ".join(diffs[:10])
            )

    _refuse_if_open(target_path)

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    tmp_path = target_path.parent / f"{target_path.name}.restore-tmp-{stamp}"
    if tmp_path.exists():
        raise RestoreRefused(f"restore temporary already exists: {tmp_path}")

    try:
        src = sqlite3.connect(f"file:{backup_path}?mode=ro", uri=True)
        dst = sqlite3.connect(str(tmp_path))
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()

        staged_fp = fingerprint(tmp_path)
        diffs = compare(backup_fp, staged_fp, left_label="backup", right_label="restored")
        if diffs:
            raise RestoreRefused(
                f"restored content differs from {backup_path.name}; target left "
                f"untouched; {len(diffs)} difference(s): " + "; ".join(diffs[:10])
            )

        os.replace(tmp_path, target_path)
    except BaseException:
        # Nothing has moved into place unless os.replace ran, so removing the
        # staging file is the whole cleanup.
        Path(tmp_path).unlink(missing_ok=True)
        raise

    for sidecar in ("-wal", "-shm"):
        stale = target_path.parent / (target_path.name + sidecar)
        if stale.exists():
            stale.unlink()
            logger.info("Removed stale %s beside the restored database", sidecar)

    final_fp = fingerprint(target_path)
    diffs = compare(backup_fp, final_fp, left_label="backup", right_label="target")
    if diffs:
        raise RestoreRefused(
            f"the database now at {target_path} does not match "
            f"{backup_path.name}; {len(diffs)} difference(s): " + "; ".join(diffs[:10])
        )

    logger.info(
        "Restored %s -> %s (%d tables, overall=%s)",
        backup_path.name, target_path, final_fp["table_count"],
        final_fp["overall_sha256"][:16],
    )
    return final_fp
