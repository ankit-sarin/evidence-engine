"""SCREEN-AUTH-01 Phase 2f: export the abstract-stage inputs for the 86-paper set.

The ONE component of the 2f smoke allowed to name review.db (ruling R3). The
harness reads what this writes and nothing else, so the model sees the text the
pipeline screens — the database abstract — not the workbook's 500-char prefix.

What is exported is exactly what the abstract stage consumes. The runner loads
`SELECT * FROM papers` rows as dicts and `_build_prompt` reads `title` and
`abstract` (both at 83defc5 and HEAD); the runner itself uses `id`. So the field
set is {id, title, abstract}.

Stops, writing nothing, when:
  * J2 — the database file's own byte size or mtime moves across the read-only
    open (only -shm/-wal may move; they are recorded, not gated);
  * J1 — any of the 86 ids is missing, or has an empty title or abstract (the
    pipeline would have screened something other than an abstract).

Cross-checks (J4 and the workbook prefix) are findings, not stops; they are
written to the manifest.

Usage:
    PYTHONPATH=. python -m analysis.eval.screen2f_export
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB = Path("data/surgical_autonomy/review.db")
DEFAULT_WORKBOOK = Path("data/surgical_autonomy/adjudication/specialty_rescreen_flagged_86.xlsx")
DEFAULT_JSONL = Path("data/surgical_autonomy/expanded_search/abstracts.jsonl")
DEFAULT_OUT = Path("docs/session-reports/screen-auth-2f-smoke")

EXPORT_FIELDS = ("id", "title", "abstract")


def _stat(path: Path) -> dict | None:
    if not path.exists():
        return None
    st = path.stat()
    return {"size": st.st_size, "mtime_ns": st.st_mtime_ns}


def _norm_title(s: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def read_workbook(path: Path) -> list[dict]:
    import openpyxl

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    rows = list(wb.worksheets[0].iter_rows(values_only=True))
    header = [str(h) for h in rows[0]]
    out = [dict(zip(header, r)) for r in rows[1:] if r and r[0] is not None]
    wb.close()
    return out


def read_db(db: Path, ids: list[int]) -> tuple[dict[int, dict], dict]:
    """Read the export fields read-only; return rows and the before/after stats."""
    sidecars = {s: Path(f"{db}{s}") for s in ("-shm", "-wal")}
    before = {"file": _stat(db), **{k: _stat(p) for k, p in sidecars.items()}}
    conn = sqlite3.connect(f"file:{db.resolve()}?mode=ro", uri=True)
    try:
        conn.row_factory = sqlite3.Row
        marks = ",".join("?" * len(ids))
        rows = conn.execute(
            f"SELECT {', '.join(EXPORT_FIELDS)} FROM papers WHERE id IN ({marks})", ids
        ).fetchall()
    finally:
        conn.close()
    after = {"file": _stat(db), **{k: _stat(p) for k, p in sidecars.items()}}
    return {r["id"]: dict(r) for r in rows}, {"before": before, "after": after}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK)
    ap.add_argument("--abstracts-jsonl", type=Path, default=DEFAULT_JSONL)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args(argv)

    wb_rows = read_workbook(args.workbook)
    ids = [int(r["paper_id"]) for r in wb_rows]
    if len(ids) != 86 or len(set(ids)) != 86:
        print(f"STOP: workbook has {len(ids)} ids ({len(set(ids))} distinct), expected 86", file=sys.stderr)
        return 2

    rows, stats = read_db(args.db, ids)

    if stats["before"]["file"] != stats["after"]["file"]:
        print(f"STOP (J2 falsified): database file moved across a read-only open: {stats}", file=sys.stderr)
        return 2

    missing = [i for i in ids if i not in rows]
    empty = [i for i in ids if i in rows and not (rows[i]["abstract"] or "").strip()]
    no_title = [i for i in ids if i in rows and not (rows[i]["title"] or "").strip()]
    if missing or empty or no_title:
        print(
            f"STOP (J1 falsified): missing={missing} empty_abstract={empty} empty_title={no_title}",
            file=sys.stderr,
        )
        return 2

    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.out_dir / "papers_86.jsonl"
    with open(out_path, "w", encoding="utf-8") as f:
        for i in ids:
            f.write(json.dumps({k: rows[i][k] for k in EXPORT_FIELDS}, ensure_ascii=False, sort_keys=True) + "\n")
    sha = hashlib.sha256(out_path.read_bytes()).hexdigest()

    # Workbook prefix cross-check (86).
    prefix_mismatch = []
    for r in wb_rows:
        pid, wb_abs = int(r["paper_id"]), str(r["abstract"] or "")
        db_abs = rows[pid]["abstract"]
        if db_abs[: len(wb_abs)] != wb_abs:
            k = next((j for j, (a, b) in enumerate(zip(db_abs, wb_abs)) if a != b), min(len(db_abs), len(wb_abs)))
            prefix_mismatch.append({"paper_id": pid, "ee": r["ee_identifier"], "first_diff_index": k,
                                    "db_len": len(db_abs), "wb_len": len(wb_abs)})

    # abstracts.jsonl cross-check (J4), matched by normalized title.
    by_title: dict[str, list[dict]] = {}
    with open(args.abstracts_jsonl, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rec = json.loads(line)
                by_title.setdefault(_norm_title(rec.get("title")), []).append(rec)
    jsonl_equal, jsonl_mismatch, jsonl_unmatched = [], [], []
    for r in wb_rows:
        pid = int(r["paper_id"])
        cands = by_title.get(_norm_title(rows[pid]["title"]), [])
        if not cands:
            jsonl_unmatched.append({"paper_id": pid, "ee": r["ee_identifier"]})
            continue
        j_abs = cands[0].get("abstract") or ""
        d_abs = rows[pid]["abstract"]
        if j_abs == d_abs:
            jsonl_equal.append(pid)
        else:
            jsonl_mismatch.append({"paper_id": pid, "ee": r["ee_identifier"], "db_len": len(d_abs),
                                   "jsonl_len": len(j_abs), "equal_after_whitespace_norm":
                                   " ".join(d_abs.split()) == " ".join(j_abs.split()),
                                   "candidates": len(cands)})

    lens = sorted(len(rows[i]["abstract"]) for i in ids)
    manifest = {
        "task": "SCREEN-AUTH-01 Phase 2f export (R3)",
        "db_path": str(args.db),
        "db_stats": stats,
        "db_file_unchanged": True,
        "fields": list(EXPORT_FIELDS),
        "papers_file": out_path.name,
        "papers_sha256": sha,
        "n": len(ids),
        "abstract_len": {"min": lens[0], "median": lens[len(lens) // 2], "max": lens[-1]},
        "workbook": str(args.workbook),
        "workbook_prefix_mismatch": prefix_mismatch,
        "abstracts_jsonl": str(args.abstracts_jsonl),
        "jsonl_equal": len(jsonl_equal),
        "jsonl_mismatch": jsonl_mismatch,
        "jsonl_unmatched": jsonl_unmatched,
    }
    (args.out_dir / "papers_86_export_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({k: v for k, v in manifest.items() if k not in ("jsonl_unmatched",)}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
