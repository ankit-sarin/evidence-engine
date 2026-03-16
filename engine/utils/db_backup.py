"""Auto-backup for SQLite databases before destructive operations."""

import logging
import shutil
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def auto_backup(db_path: str | Path, reason: str) -> Path:
    """Create a timestamped backup copy of a SQLite database.

    Args:
        db_path: Path to the database file.
        reason: Short label for the backup (e.g., "pre-cleanup", "pre-reset").
            Used in the backup filename — must be filesystem-safe.

    Returns:
        Path to the backup file.
    """
    db_path = Path(db_path)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = db_path.parent / f"{db_path.name}.bak-{reason}-{timestamp}"

    shutil.copy2(str(db_path), str(backup_path))

    size_mb = backup_path.stat().st_size / (1024 * 1024)
    logger.info(
        "DB backup created: %s (reason=%s, %.1f MB)",
        backup_path.name, reason, size_mb,
    )

    return backup_path
