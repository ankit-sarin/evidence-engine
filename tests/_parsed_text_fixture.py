"""Record a fixture's parsed text the way the engine does (INPUT-IDENTITY-01).

Six engine sites used to find "the current parsed text" by listing
`parsed_text/` on disk, so a fixture that dropped a `{pid}_v1.md` file beside its
database was enough. They read through `engine.core.parsed_text` now, which
answers from `parsed_text_refs` and verifies the recorded hash on every read —
so a fixture's text must be RECORDED, not just written. This writes the file
and records it through the engine's own writer, `record_parsed_text`, never a
hand-built INSERT (a fixture that hand-types another component's row is the
vocabulary defect).

Underscore-prefixed so pytest does not collect it.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from engine.core.parsed_text import next_version, record_parsed_text


def write_parsed(target, paper_id: int, text: str, *, version: int | None = None) -> Path:
    """Write `text` as the paper's next parsed version beside the database and
    record it. `target` is a ReviewDatabase, a sqlite3 connection, or a path."""
    own = False
    if hasattr(target, "db_path") and hasattr(target, "_conn"):
        conn, db_path = target._conn, Path(target.db_path)
    elif isinstance(target, sqlite3.Connection):
        conn = target
        db_path = Path(conn.execute("PRAGMA database_list").fetchone()[2])
    else:
        db_path = Path(target)
        conn, own = sqlite3.connect(str(db_path)), True
    try:
        v = version if version is not None else next_version(conn, paper_id)
        path = db_path.parent / "parsed_text" / f"{paper_id}_v{v}.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        record_parsed_text(conn, paper_id=paper_id, path=path, version=v,
                           data=path.read_bytes(), source_asset_id=None)
        conn.commit()
        return path
    finally:
        if own:
            conn.close()
