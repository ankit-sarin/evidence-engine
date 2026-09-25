"""The one parsed-text resolver (S3e) and the one reference writer (D8).

Before this, six engine sites each chose "the current parsed text" by listing
the versioned files in `parsed_text/` and sorting the NAMES in reverse — so `_v9` beat
`_v10` (row D1) — and nothing recorded what a chosen file contained, so a text
edited in place was consumed as though it were the one that had been screened
and extracted (rows D2, D3). The filesystem was the authority; now the
database is.

* **`resolve_parsed_text(conn, paper_id)`** — the `parsed_text_refs` row with the
  greatest `parsed_text_version`, compared as an INTEGER in SQL. Migration
  021's `UNIQUE (paper_id, parsed_text_version)` is what makes that one row. No
  glob, no directory listing.
* **`read_parsed_text(ref)` / `load_parsed_text(conn, paper_id)`** — read the
  file and recompute its SHA-256 on EVERY read (R95). A mismatch raises
  `ParsedTextModified` naming the uid, both hashes and the remedy: record the
  changed text as a new version. It is never consumed in place.
* **`record_parsed_text(conn, ...)`** — the D8 writer's half: one
  `parsed_text_refs` row, version = previous max + 1 from THIS table (R99), path
  in canonical form (R100), hash of the bytes written. It does not commit; the
  caller's unit of work does.

**Paths (R100).** Stored relative to `REPO_ROOT` when the file is under it,
absolute otherwise; a relative stored path is resolved against `REPO_ROOT`,
never against the working directory.
"""

from __future__ import annotations

import hashlib
import io
import os
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from engine.core.paper_state import (
    REASON_PARSED_TEXT_MISSING,
    REASON_PARSED_TEXT_MODIFIED,
    REASON_PARSED_TEXT_NOT_RECORDED,
)

#: N1: the engine had nine private copies of this anchor and no shared one.
#: This is the one the parsed-text store resolves against, derived from this
#: file's own location (engine/core/parsed_text.py → three parents up).
REPO_ROOT = Path(__file__).resolve().parent.parent.parent


class ParsedTextError(RuntimeError):
    """Base: the current parsed text for a paper cannot be handed out.

    Each refusal carries its `reason_code` (R122 as widened by 9b-2a R2), from
    the closed set `engine.core.paper_state.EXTRACTION_REASON_CODES` (F9).
    """

    reason_code: str


class NoParsedText(ParsedTextError):
    """The paper has no `parsed_text_refs` row at all."""

    reason_code = REASON_PARSED_TEXT_NOT_RECORDED

    def __init__(self, paper_id: int):
        self.paper_id = paper_id
        super().__init__(f"paper {paper_id}: no parsed text is recorded "
                         "(no parsed_text_refs row)")


class ParsedTextMissing(ParsedTextError):
    """The recorded file is not on disk."""

    reason_code = REASON_PARSED_TEXT_MISSING

    def __init__(self, uid: str, path: Path):
        self.uid, self.path = uid, path
        super().__init__(f"parsed text {uid}: the recorded file is missing at {path}")


class ParsedTextModified(ParsedTextError):
    """The file's bytes no longer hash to what was recorded (R95)."""

    reason_code = REASON_PARSED_TEXT_MODIFIED

    def __init__(self, uid: str, path: Path, recorded: str, observed: str):
        self.uid, self.path = uid, path
        self.recorded, self.observed = recorded, observed
        super().__init__(
            f"parsed text {uid} at {path} was modified after it was recorded: "
            f"recorded sha256 {recorded}, observed {observed}. Remedy: record the "
            "changed text as a NEW version (engine.core.parsed_text."
            "record_parsed_text); a recorded text is never consumed in place.")


@dataclass(frozen=True)
class ParsedTextRef:
    parsed_text_uid: str
    paper_id: int
    path: Path          # resolved, ready to open
    stored_path: str    # as recorded (canonical form, R100)
    version: int
    sha256: str


def canonical_path(path: str | os.PathLike, repo_root: Path = REPO_ROOT) -> str:
    """R100: relative to the repo root when under it, absolute otherwise.

    `os.path.normpath` rather than `Path.resolve`, so a symlink inside the tree
    does not turn a path that is under the root into one that is not. A
    relative input is taken as relative to the repo root.
    """
    root = os.path.normpath(str(repo_root))
    p = os.fspath(path)
    p = os.path.normpath(p if os.path.isabs(p) else os.path.join(root, p))
    if p == root or p.startswith(root + os.sep):
        return Path(os.path.relpath(p, root)).as_posix()
    return p


def resolve_stored_path(stored: str, repo_root: Path = REPO_ROOT) -> Path:
    return Path(stored) if os.path.isabs(stored) else Path(repo_root) / stored


def resolve_parsed_text(conn: sqlite3.Connection, paper_id: int) -> ParsedTextRef:
    """The paper's current parsed text: its greatest recorded version."""
    row = conn.execute(
        "SELECT parsed_text_uid, paper_id, parsed_text_path, parsed_text_version, "
        "parsed_text_sha256 FROM parsed_text_refs WHERE paper_id = ? "
        "ORDER BY parsed_text_version DESC LIMIT 1",
        (paper_id,),
    ).fetchone()
    if row is None:
        raise NoParsedText(paper_id)
    uid, pid, stored, version, sha = tuple(row)
    return ParsedTextRef(uid, pid, resolve_stored_path(stored), stored, int(version), sha)


def read_parsed_bytes(ref: ParsedTextRef) -> bytes:
    """The file's bytes, verified against the recorded hash."""
    try:
        data = ref.path.read_bytes()
    except FileNotFoundError as exc:
        raise ParsedTextMissing(ref.parsed_text_uid, ref.path) from exc
    observed = hashlib.sha256(data).hexdigest()
    if observed != ref.sha256:
        raise ParsedTextModified(ref.parsed_text_uid, ref.path, ref.sha256, observed)
    return data


def read_parsed_text(ref: ParsedTextRef) -> str:
    """The verified text, decoded exactly as `Path.read_text()` decoded it at the
    six sites this replaced (UTF-8, universal newlines), so nothing a caller
    hands to a model changes (R66)."""
    data = read_parsed_bytes(ref)
    return io.TextIOWrapper(io.BytesIO(data), encoding="utf-8").read()


def load_parsed_text(conn: sqlite3.Connection, paper_id: int) -> str:
    return read_parsed_text(resolve_parsed_text(conn, paper_id))


def next_version(conn: sqlite3.Connection, paper_id: int) -> int:
    """R99: COALESCE(MAX(version), 0) + 1 over this paper's parsed_text_refs rows."""
    (v,) = conn.execute(
        "SELECT COALESCE(MAX(parsed_text_version), 0) FROM parsed_text_refs "
        "WHERE paper_id = ?", (paper_id,)).fetchone()
    return int(v) + 1


def record_parsed_text(conn: sqlite3.Connection, *, paper_id: int, path: str | os.PathLike,
                       version: int, data: bytes, source_asset_id: int | None,
                       recorded_at: str | None = None) -> ParsedTextRef:
    """Insert one reference for text already written (or about to be renamed) to
    `path`. Does NOT commit — it belongs to the caller's unit of work (D8)."""
    uid = str(uuid.uuid4())
    # A filesystem path (relative to the working directory, as ReviewDatabase's
    # default data root is) — made absolute before it is canonicalised.
    stored = canonical_path(os.path.abspath(path))
    sha = hashlib.sha256(data).hexdigest()
    conn.execute(
        "INSERT INTO parsed_text_refs (parsed_text_uid, paper_id, parsed_text_path, "
        "parsed_text_version, source_full_text_assets_id, recorded_at, parsed_text_sha256) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (uid, paper_id, stored, int(version), source_asset_id,
         recorded_at or datetime.now(timezone.utc).isoformat(), sha))
    return ParsedTextRef(uid, paper_id, resolve_stored_path(stored), stored, int(version), sha)
