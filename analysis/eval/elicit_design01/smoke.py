"""ELICIT-DESIGN-01 STEP 2 — three-paper smoke of the elicited two-pass design.

Local arm only, both passes, the production code path. This is a GATE, not a
run: three papers spanning short/medium/long parsed text, and a failure is a
finding to report rather than something to patch mid-smoke.

**`review.db` is never written.** The smoke builds its own `ReviewDatabase`
under the gitignored eval store and exercises the real `add_extraction_atomic`
against it, so the write boundary, the completeness guard and the citation guard
are all genuinely executed rather than mocked. The production database is opened
read-only (`immutable=1`) once, to read three papers' metadata, and
`parsed_text/` is read-only throughout.

Ops: whole run under `hold_experiment_lock()`; Ollama pinned to 0.21.0 exactly
and its version and model digest recorded per extraction; prompt sizes and
`prompt_eval_count` captured per call so the C4 estimator can be checked against
what the runtime actually counted.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("elicit_design01")

LABEL = "elicit_design01"
EXPECTED_SERVER_VERSION = "0.21.0"

# ELICIT-DESIGN-01 section 7, ratified: one paper per length stratum, all three
# in the 190-paper extracted corpus, all three drawn from the ELICIT-01 manifest
# so their unit counts and fit are already measured. p498 is the largest paper in
# that sample and is the real test of the C4 sizing estimator.
SMOKE_PAPERS = (121, 604, 498)


@dataclass
class PaperRow:
    paper_id: int
    ok: bool
    started_utc: str
    finished_utc: str
    latency_s: float
    server_version: str | None = None
    model: str | None = None
    model_digest: str | None = None
    parsed_chars: int | None = None
    n_units: int | None = None
    attempts: int | None = None
    error: str | None = None
    error_type: str | None = None
    fields: dict = field(default_factory=dict)
    telemetry: list = field(default_factory=list)


def newest_parsed(parsed_dir: Path, pid: int) -> Path | None:
    fs = sorted(parsed_dir.glob(f"{pid}_v*.md"),
                key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
    return fs[-1] if fs else None


def preflight(oc) -> tuple[str, str | None]:
    """Refuse to run on the wrong runtime. Version is recorded per extraction."""
    version = oc._client._client.get("/api/version").json()["version"]
    if version != EXPECTED_SERVER_VERSION:
        raise SystemExit(
            f"ABORT: Ollama {version!r}, expected {EXPECTED_SERVER_VERSION!r}. "
            "The smoke pins the runtime because a version change is exactly the "
            "confound QUALGAP-01 spent a batch ruling out."
        )
    from engine.agents.extractor import MODEL

    digest = None
    for m in oc._client.list().get("models", []):
        if m.get("model") == MODEL:
            digest = m.get("digest")
    return version, digest


def build_scratch(review_dir: Path, store: Path, papers: tuple[int, ...]):
    """A throwaway ReviewDatabase carrying only the smoke's three papers.

    Papers rows are copied because `extractions.paper_id` is a real foreign key
    and `PRAGMA foreign_keys=ON`; nothing else is copied, and nothing is written
    back.
    """
    from engine.core.database import ReviewDatabase

    scratch_root = store / "scratch"
    if scratch_root.exists():
        shutil.rmtree(scratch_root)
    scratch_root.mkdir(parents=True)
    db = ReviewDatabase(LABEL, data_root=scratch_root)
    scratch_dir = Path(db.db_path).parent
    shutil.copy2(review_dir / "extraction_codebook.yaml",
                 scratch_dir / "extraction_codebook.yaml")

    src = sqlite3.connect(f"file:{review_dir / 'review.db'}?immutable=1", uri=True)
    src.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc).isoformat()
    for pid in papers:
        r = src.execute(
            "SELECT id, title, source, status FROM papers WHERE id = ?", (pid,)
        ).fetchone()
        if r is None:
            raise SystemExit(f"ABORT: paper {pid} not in the review database")
        db._conn.execute(
            "INSERT INTO papers (id, title, source, status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (r["id"], r["title"], r["source"], r["status"], now, now),
        )
    db._conn.commit()
    src.close()
    return db, scratch_dir


def field_report(unit_map, p1_extra: dict | None, spans: list[dict]) -> dict:
    """Per-field: class, citations and validity, escape use, accompaniment."""
    out: dict[str, dict] = {}
    by_field = (p1_extra or {}).get("fields", {})
    stored = {s["field_name"]: s for s in spans}
    for name, rec in by_field.items():
        span = stored.get(name, {})
        out[name] = {
            "class": rec["class"],
            "n_citations": len(rec["indices"]),
            "indices": rec["indices"],
            "n_invalid_indices": len(rec["bad_indices"]),
            "invalid_indices": rec["bad_indices"],
            "n_duplicate_indices": len(rec["duplicate_indices"]),
            "escape_used": rec["escape"],
            "declared_inference": rec["has_inference"],
            "n_steps": rec["n_steps"],
            "violations": rec["violations"],
            "value": span.get("value"),
            "snippet_chars": len(span.get("source_snippet") or ""),
            "snippet_verbatim": bool(
                span.get("source_snippet")
                and span["source_snippet"] in unit_map.source_stripped
            ),
        }
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="ELICIT-DESIGN-01 smoke")
    ap.add_argument("--review", default="surgical_autonomy")
    ap.add_argument("--data-root", default="data")
    ap.add_argument("--spec", default="review_specs/surgical_autonomy_v1.yaml")
    ap.add_argument("--papers", default=",".join(str(p) for p in SMOKE_PAPERS))
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    from engine.agents.extractor import MODEL, extract_paper_with_completeness
    from engine.core.review_spec import load_review_spec
    from engine.elicitation import classes as C
    from engine.elicitation.units import build_unit_map
    from engine.utils import ollama_client as oc
    from engine.utils.ollama_lock import hold_experiment_lock

    review_dir = Path(args.data_root) / args.review
    store = review_dir / "eval" / LABEL
    store.mkdir(parents=True, exist_ok=True)
    papers = tuple(int(p) for p in args.papers.split(","))

    version, digest = preflight(oc)

    spec = load_review_spec(args.spec)
    spec.extraction_models.elicitation = True
    logger.info("elicitation=%s pass1_think=%s pass2_think=%s",
                spec.extraction_models.elicitation,
                spec.extraction_models.pass1_think,
                spec.extraction_models.pass2_think)

    db, scratch_dir = build_scratch(review_dir, store, papers)
    codebook = C.load(scratch_dir / "extraction_codebook.yaml")
    tel_path = scratch_dir / "telemetry" / "extraction_calls.jsonl"

    run_id = datetime.now(timezone.utc).strftime("smoke_%Y%m%dT%H%M%SZ")
    out = store / f"{LABEL}_{run_id}.jsonl"
    logger.info("ELICIT-DESIGN-01 SMOKE | Ollama %s | %s (%s) | papers %s | -> %s",
                version, MODEL, (digest or "?")[:12], list(papers), out)

    rows: list[PaperRow] = []
    with hold_experiment_lock():
        logger.info("experiment lock acquired")
        for pid in papers:
            f = newest_parsed(review_dir / "parsed_text", pid)
            if f is None:
                raise SystemExit(f"ABORT: no parsed text for paper {pid}")
            text = f.read_text()
            um = build_unit_map(pid, text)
            before = tel_path.read_text().splitlines() if tel_path.exists() else []
            t0 = time.time()
            started = datetime.now(timezone.utc).isoformat()
            base = dict(paper_id=pid, started_utc=started, server_version=version,
                        model=MODEL, model_digest=digest, parsed_chars=len(text),
                        n_units=um.n)
            logger.info("p%d: %d chars, %d units", pid, len(text), um.n)
            try:
                result = extract_paper_with_completeness(pid, text, spec, db,
                                                         model_digest=digest)
                ok, err, etype = True, None, None
            except Exception as exc:            # a smoke failure is a finding
                logger.exception("p%d FAILED", pid)
                result, ok = None, False
                err, etype = f"{exc}"[:800], type(exc).__name__

            after = tel_path.read_text().splitlines() if tel_path.exists() else []
            new_rows = [json.loads(l) for l in after[len(before):] if l.strip()]
            spans = ([{"field_name": s.field_name, "value": s.value,
                       "source_snippet": s.source_snippet} for s in result.fields]
                     if result else [])
            last_extra = next((r.get("extra") for r in reversed(new_rows) if r.get("extra")), None)
            row = PaperRow(
                ok=ok, finished_utc=datetime.now(timezone.utc).isoformat(),
                latency_s=round(time.time() - t0, 1), attempts=len(new_rows),
                error=err, error_type=etype,
                fields=field_report(um, last_extra, spans),
                telemetry=[{k: v for k, v in r.items() if k != "raw_content"}
                           for r in new_rows],
                **base,
            )
            rows.append(row)
            with out.open("a") as fh:
                fh.write(json.dumps(asdict(row)) + "\n")
            logger.info("p%d ok=%s attempts=%d %.0fs", pid, ok, row.attempts or 0,
                        row.latency_s)

    n_ok = sum(1 for r in rows if r.ok)
    logger.info("SMOKE COMPLETE: %d/%d papers stored -> %s", n_ok, len(rows), out)
    print(json.dumps({"papers": len(rows), "ok": n_ok, "out": str(out),
                      "scratch": str(scratch_dir)}, indent=1))
    return 0 if n_ok == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main())
