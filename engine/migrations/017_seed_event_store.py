"""Migration 017: seed the event store with identities and eligibility only.

EFFECTIVE-RESULT-02 (session 5, S2 core). **A data migration** — declared as
such in `runner.KINDS`, so it is never executed on a fresh database.

**R25: seed, do not migrate.** The event store begins *empty of field-level
history*. This migration writes exactly what addendum 4 §A lists and nothing
else:

* **papers** — by reference, not by row. `field_events.paper_id` and
  `paper_events.paper_id` are foreign keys to `papers(id)`, so all 10,039 are
  addressable without a single row being copied. Writing 10,039 `identified`
  events would be reconstructing how the state arose, which R25 forbids.
* **corpus membership** — one `state_at_migration` **paper** event per corpus
  paper, `to_state = 'eligible'`, `from_state` NULL, **eligibility only, no
  processing fact**. Membership comes from `engine.core.corpus`, the single
  authority, never from a status literal copied to here.
* **parsed-text references** — every version, no choice made between them.
* **spec and codebook identities** — the seed of the S3a run manifest
  (session 7).
* **the three existing arms** — `local`, `anthropic_sonnet_4_6`,
  `openai_o4_mini_high`, registered with `configuration_marker` =
  `not recorded (pre-manifest)` per R10 and addendum 2 §C.4, which is what makes
  v2.1 row 7 reachable. Nothing is backfilled.

**What stays behind, untouched:** `extractions` (190), `evidence_spans` (3,760),
`cloud_extractions` (379), `cloud_evidence_spans` (7,257), every auditor verdict,
every screening and adjudication table, and `workflow_state`. They become the
regression fixture the new reader is tested *against*, never a source it reads
*from*.

**Atomic, because the append-only triggers make a partial seed uncleanable.**
The migration runner does *not* wrap a migration in a transaction — it closes
its own connection and calls `run_migration(db_path)`, and each migration opens
and commits its own (see `runner.run`: `conn.close()  # migrations open their
own connection`). So this one manages its own: one `BEGIN`, the marker row
written **last**, `ROLLBACK` on any exception. A failure therefore leaves the
store exactly as it was, with no rows a trigger would refuse to delete.

**Idempotent by marker.** On entry it looks for `review_identities('seed','017')`
and returns having inserted nothing if it is there. `paper_events` has no natural
key and `INSERT OR REPLACE` is unusable anywhere (it deletes, and a delete is
refused by the append-only trigger), so the marker is the guard.

**No review literal.** Migration 003's defect — a source directory and a default
target both naming one review — is avoided by deriving every path from the
`db_path` handed in: the review directory is its parent, and the review id is
that directory's name.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from engine.core.corpus import corpus_status_sql
from engine.core.effective import PRE_MANIFEST
from engine.core.events import mint_extraction_uid, register_arm, write_paper_event

SEED_KIND = "seed"
SEED_KEY = "017"

#: The arms that hold claims on this database today, per addendum 2 §C.4. They
#: are registered, not backfilled: under R25 none of their legacy extractions
#: becomes a claim, so these rows are declarations of identity, not history.
EXISTING_ARMS = ("local", "anthropic_sonnet_4_6", "openai_o4_mini_high")

SEED_NOTE = "history not reconstructable from the record"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def already_seeded(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM review_identities WHERE kind = ? AND key = ?",
        (SEED_KIND, SEED_KEY)).fetchone()
    return row is not None


def corpus_paper_ids(conn: sqlite3.Connection) -> list[int]:
    frag, params = corpus_status_sql()
    return [r[0] for r in conn.execute(
        f"SELECT id FROM papers WHERE {frag} ORDER BY id", params)]


def parsed_text_rows(conn: sqlite3.Connection, paper_ids) -> list[tuple]:
    frag, params = corpus_status_sql("p.status")
    return conn.execute(
        "SELECT f.paper_id, f.parsed_text_path, f.parsed_text_version, f.id "
        "FROM full_text_assets f JOIN papers p ON p.id = f.paper_id "
        f"WHERE f.parsed_text_path IS NOT NULL AND {frag} "
        "ORDER BY f.paper_id, f.parsed_text_version, f.id", params).fetchall()


def build_seed(conn, *, corpus_ids, parsed_rows, spec_identity,
               codebook_identity, arms=EXISTING_ARMS) -> dict:
    """Write the seed in one transaction. Returns the rows inserted per table."""
    zero = {"arms": 0, "paper_events": 0, "parsed_text_refs": 0,
            "review_identities": 0, "papers": 0}
    if already_seeded(conn):
        return dict(zero, already_seeded=True)

    conn.execute("BEGIN")
    try:
        counts = dict(zero)
        now = _now()

        for name in arms:
            register_arm(conn, name, "model", configuration=None,
                         configuration_marker=PRE_MANIFEST, registered_at=now)
            counts["arms"] += 1

        for pid in corpus_ids:
            write_paper_event(
                conn, event_type="state_at_migration", paper_id=pid,
                to_state="eligible", from_state=None,
                actor_kind="engine", actor_role="system",
                actor_name="017_seed_event_store", occurred_at=now,
                run_marker="pre-manifest",
                payload={"source": "state at migration", "note": SEED_NOTE},
                commit=False)
            counts["paper_events"] += 1

        for paper_id, path, version, fta_id in parsed_rows:
            conn.execute(
                "INSERT OR IGNORE INTO parsed_text_refs (parsed_text_uid, paper_id, "
                "parsed_text_path, parsed_text_version, source_full_text_assets_id, "
                "recorded_at) VALUES (?, ?, ?, ?, ?, ?)",
                (mint_extraction_uid(), paper_id, path, version, fta_id, now))
            counts["parsed_text_refs"] += 1

        for kind, key, value in (("spec", "review_spec", spec_identity),
                                 ("codebook", "extraction_codebook", codebook_identity)):
            conn.execute(
                "INSERT OR IGNORE INTO review_identities (kind, key, value_json, "
                "recorded_at) VALUES (?, ?, ?, ?)",
                (kind, key, json.dumps(value, sort_keys=True), now))
            counts["review_identities"] += 1

        # The marker is the LAST statement of the transaction: if anything above
        # failed, the rollback removes it too, and the next run seeds cleanly.
        conn.execute(
            "INSERT INTO review_identities (kind, key, value_json, recorded_at) "
            "VALUES (?, ?, ?, ?)",
            (SEED_KIND, SEED_KEY,
             json.dumps({"migration": "017_seed_event_store",
                         "rule_version": "v2.1",
                         "seeded": {k: v for k, v in counts.items() if v},
                         "note": SEED_NOTE}, sort_keys=True), now))
        counts["review_identities"] += 1
        conn.commit()
        return dict(counts, already_seeded=False)
    except Exception:
        conn.rollback()
        raise


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_migration(db_path: str | None = None, *, review_root=None,
                  spec_path=None, codebook_path=None) -> dict:
    if db_path is None:
        raise ValueError("017 requires an explicit db_path")
    db_path = Path(db_path)
    review_root = Path(review_root) if review_root else db_path.parent
    review_id = review_root.name
    spec_path = Path(spec_path) if spec_path else (
        Path("review_specs") / f"{review_id}.yaml")
    codebook_path = Path(codebook_path) if codebook_path else (
        review_root / "extraction_codebook.yaml")

    # Through the two loaders, never a bare yaml.safe_load: C8's guard leaves
    # exactly three raw-YAML sites in the repository and this is not one of them.
    from engine.core.codebook import load_codebook
    from engine.core.review_spec import load_review_spec

    spec = load_review_spec(spec_path)
    codebook = load_codebook(codebook_path)
    spec_identity = {"path": str(spec_path), "file_sha256": _sha256(spec_path),
                     "screening_hash": spec.screening_hash(),
                     "review_id": spec.review_id}
    codebook_identity = {"path": str(codebook_path),
                         "sha256": codebook.sha256,
                         "semantic_hash": codebook.semantic_hash,
                         "field_count": len(codebook.fields)}

    conn = sqlite3.connect(str(db_path))
    try:
        ids = corpus_paper_ids(conn)
        rows = parsed_text_rows(conn, ids)
        result = build_seed(conn, corpus_ids=ids, parsed_rows=rows,
                            spec_identity=spec_identity,
                            codebook_identity=codebook_identity)
        result["papers_addressable"] = conn.execute(
            "SELECT COUNT(*) FROM papers").fetchone()[0]
        return result
    finally:
        conn.close()
