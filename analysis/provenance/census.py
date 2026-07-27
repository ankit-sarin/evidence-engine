"""Run the evidence-provenance census over a review's extraction arms.

Classifies every stored evidence span (local + both cloud arms) under the
taxonomy pinned in DEFINITIONS.md, persists one row per span to
provenance_classifications, and writes summary tables.

Read-only with respect to every pre-existing table: the only writes are INSERTs
into the two tables created by migration 010.

No Ollama, no network, no inference — pure Python string matching.

Usage:
    PYTHONPATH=. python -m analysis.provenance.census --review surgical_autonomy
    PYTHONPATH=. python -m analysis.provenance.census --review surgical_autonomy \\
        --threshold 0.90 --strict-variant --legacy
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from multiprocessing import Pool
from pathlib import Path

from analysis.provenance import classifier as C
from analysis.provenance.absence import ABSENCE_PATTERN_VERSION
from analysis.provenance.field_class import FIELD_CLASS, STATUS, field_class
from analysis.provenance.legacy import grep_verify_fast
from analysis.provenance.normalize import NORMALIZATION_VERSION
from analysis.provenance.segment import (
    MIN_SENTENCE_TOKENS,
    TOKENIZER_NAME,
    TOKENIZER_VERSION,
)

logger = logging.getLogger(__name__)

LOCAL_ARM = "local_deepseek_r1_32b"
DEFINITIONS_PATH = Path(__file__).parent / "DEFINITIONS.md"

_ELLIPSIS_CHARS = ("...", "…")


# ── Loading ──────────────────────────────────────────────────────────────


def _connect_ro(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_spans(db_path: Path) -> dict[int, list[dict]]:
    """All stored spans for all arms, grouped by paper_id."""
    conn = _connect_ro(db_path)
    by_paper: dict[int, list[dict]] = defaultdict(list)
    try:
        for r in conn.execute(
            """SELECT e.paper_id AS paper_id, s.id AS span_row_id, s.field_name,
                      s.value, s.source_snippet
                 FROM evidence_spans s
                 JOIN extractions e ON e.id = s.extraction_id"""
        ):
            by_paper[r["paper_id"]].append(
                {**dict(r), "arm": LOCAL_ARM, "span_table": "evidence_spans"}
            )
        for r in conn.execute(
            """SELECT c.paper_id AS paper_id, c.arm AS arm, s.id AS span_row_id,
                      s.field_name, s.value, s.source_snippet
                 FROM cloud_evidence_spans s
                 JOIN cloud_extractions c ON c.id = s.cloud_extraction_id"""
        ):
            by_paper[r["paper_id"]].append(
                {**dict(r), "span_table": "cloud_evidence_spans"}
            )
    finally:
        conn.close()
    return dict(by_paper)


def load_parser_tiers(db_path: Path) -> dict[int, str]:
    """parser_used of the highest-version parse per paper (the consumed text)."""
    conn = _connect_ro(db_path)
    try:
        rows = conn.execute(
            """SELECT f.paper_id, f.parser_used
                 FROM full_text_assets f
                 JOIN (SELECT paper_id, MAX(parsed_text_version) mv
                         FROM full_text_assets GROUP BY paper_id) m
                   ON m.paper_id = f.paper_id AND m.mv = f.parsed_text_version"""
        ).fetchall()
    finally:
        conn.close()
    return {r["paper_id"]: (r["parser_used"] or "") for r in rows}


def parsed_text_path(review_dir: Path, paper_id: int) -> Path | None:
    files = sorted(
        (review_dir / "parsed_text").glob(f"{paper_id}_v*.md"),
        key=lambda p: int(p.stem.rsplit("_v", 1)[1]),
    )
    return files[-1] if files else None


# ── Worker ───────────────────────────────────────────────────────────────

_WORKER: dict = {}


def _init_worker(review_dir: str, threshold: float, strict: bool, legacy: bool) -> None:
    _WORKER.update(
        review_dir=Path(review_dir), threshold=threshold, strict=strict, legacy=legacy
    )


def _classify_paper(item: tuple[int, list[dict]]) -> list[dict]:
    paper_id, spans = item
    path = parsed_text_path(_WORKER["review_dir"], paper_id)
    if path is None:
        logger.warning("Paper %d: no parsed text — spans skipped", paper_id)
        return []
    raw = path.read_text()
    paper = C.PaperIndex.build(paper_id, raw, with_sentences=_WORKER["strict"])

    legacy_ok = None
    out: list[dict] = []
    for s in spans:
        res = C.classify_span(
            s.get("source_snippet"),
            s.get("value"),
            paper,
            threshold=_WORKER["threshold"],
            strict_variant=_WORKER["strict"],
        )
        snippet = s.get("source_snippet") or ""
        if _WORKER["legacy"]:
            legacy_ok = bool(snippet.strip()) and grep_verify_fast(snippet, raw)
        out.append(
            {
                "arm": s["arm"],
                "paper_id": paper_id,
                "field_name": s["field_name"],
                "span_table": s["span_table"],
                "span_row_id": s["span_row_id"],
                "value": s.get("value"),
                "snippet_chars": len(snippet),
                "taxonomy_class": res.taxonomy_class,
                "field_class": field_class(s["field_name"]),
                "n_sentences": res.n_sentences,
                "n_evaluated": res.n_evaluated,
                "n_exact": res.n_exact,
                "sentence_ratios": json.dumps([round(r, 4) for r in res.sentence_ratios]),
                "min_ratio": res.min_ratio,
                "strict_variant_class": res.strict_variant_class,
                "absence_pattern": res.absence_pattern,
                "has_ellipsis": int(any(e in snippet for e in _ELLIPSIS_CHARS)),
                "legacy_grep_verify": legacy_ok,
            }
        )
    return out


# ── Persistence ──────────────────────────────────────────────────────────


def persist(db_path: Path, run: dict, rows: list[dict]) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            """INSERT INTO provenance_census_runs
                 (census_run_id, review_name, created_at, definitions_sha256,
                  normalization_version, tokenizer, tokenizer_version,
                  threshold_primary, threshold_band, min_sentence_tokens,
                  ratio_ceiling, spans_total, notes, absence_pattern_version)
               VALUES (:census_run_id, :review_name, :created_at, :definitions_sha256,
                       :normalization_version, :tokenizer, :tokenizer_version,
                       :threshold_primary, :threshold_band, :min_sentence_tokens,
                       :ratio_ceiling, :spans_total, :notes, :absence_pattern_version)""",
            run,
        )
        now = run["created_at"]
        conn.executemany(
            """INSERT INTO provenance_classifications
                 (census_run_id, arm, paper_id, field_name, span_table, span_row_id,
                  value, snippet_chars, taxonomy_class, field_class, n_sentences,
                  n_evaluated, n_exact, sentence_ratios, min_ratio,
                  strict_variant_class, has_ellipsis, parser_tier,
                  absence_pattern, classified_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                (
                    run["census_run_id"], r["arm"], r["paper_id"], r["field_name"],
                    r["span_table"], r["span_row_id"], r["value"], r["snippet_chars"],
                    r["taxonomy_class"], r["field_class"], r["n_sentences"],
                    r["n_evaluated"], r["n_exact"], r["sentence_ratios"], r["min_ratio"],
                    r["strict_variant_class"], r["has_ellipsis"], r.get("parser_tier"),
                    r.get("absence_pattern"), now,
                )
                for r in rows
            ],
        )
        conn.commit()
    finally:
        conn.close()


# ── Summaries ────────────────────────────────────────────────────────────


def summarize(rows: list[dict], threshold: float) -> dict:
    arms = sorted({r["arm"] for r in rows})
    by_arm = {a: [r for r in rows if r["arm"] == a] for a in arms}

    dist = {
        a: {c: sum(1 for r in rs if r["taxonomy_class"] == c) for c in C.ALL_CLASSES}
        for a, rs in by_arm.items()
    }
    xtab: dict[str, dict[str, dict[str, int]]] = {}
    for a, rs in by_arm.items():
        xtab[a] = {}
        for fc in ("extractive", "interpretive"):
            sub = [r for r in rs if r["field_class"] == fc]
            xtab[a][fc] = {c: sum(1 for r in sub if r["taxonomy_class"] == c) for c in C.ALL_CLASSES}

    # Threshold sensitivity, re-derived from the stored per-sentence ratios.
    sens: dict[str, dict[str, int]] = {}
    for t in C.THRESHOLD_BAND:
        counts: Counter = Counter()
        for r in rows:
            counts[_reclassify(r, t)] += 1
        sens[f"{t:.2f}"] = dict(counts)

    worst = Counter(
        r["field_name"] for r in rows if r["taxonomy_class"] == C.UNTRACEABLE_NO_BASIS
    )
    per_field = {}
    for fname, _ in worst.most_common(5):
        sub = [r for r in rows if r["field_name"] == fname]
        per_field[fname] = {
            "field_class": field_class(fname),
            "spans": len(sub),
            **{c: sum(1 for r in sub if r["taxonomy_class"] == c) for c in C.ALL_CLASSES},
            "by_arm_no_basis": {
                a: sum(1 for r in sub if r["arm"] == a and r["taxonomy_class"] == C.UNTRACEABLE_NO_BASIS)
                for a in arms
            },
        }

    legacy = None
    if any(r.get("legacy_grep_verify") is not None for r in rows):
        with_snip = [r for r in rows if r["snippet_chars"] > 0]
        legacy = {
            "spans_with_snippet": len(with_snip),
            "legacy_unanchored": sum(1 for r in with_snip if not r["legacy_grep_verify"]),
            "by_arm": {
                a: {
                    "with_snippet": sum(1 for r in with_snip if r["arm"] == a),
                    "unanchored": sum(
                        1 for r in with_snip if r["arm"] == a and not r["legacy_grep_verify"]
                    ),
                }
                for a in arms
            },
            "cross_tab_vs_taxonomy": {
                f"{c}|legacy_{'anchored' if ok else 'unanchored'}": n
                for (c, ok), n in Counter(
                    (r["taxonomy_class"], bool(r["legacy_grep_verify"])) for r in with_snip
                ).items()
            },
        }

    strict_delta = None
    if any(r.get("strict_variant_class") for r in rows):
        sub = [r for r in rows if r.get("strict_variant_class")]
        strict_delta = {
            "compared": len(sub),
            "changed": sum(1 for r in sub if r["strict_variant_class"] != r["taxonomy_class"]),
            "transitions": {
                f"{a}->{b}": n
                for (a, b), n in Counter(
                    (r["taxonomy_class"], r["strict_variant_class"])
                    for r in sub
                    if r["strict_variant_class"] != r["taxonomy_class"]
                ).most_common()
            },
        }

    return {
        "threshold_primary": threshold,
        "spans_total": len(rows),
        "distribution_by_arm": dist,
        "taxonomy_by_field_class": xtab,
        "threshold_sensitivity": sens,
        "worst_fields_by_no_basis": per_field,
        "legacy_restatement": legacy,
        "strict_variant_delta": strict_delta,
        "field_classification_status": STATUS,
    }


def _reclassify(row: dict, threshold: float) -> str:
    """Recompute a row's class at a different threshold from stored ratios."""
    cls = row["taxonomy_class"]
    if cls in (C.ANCHORED, *C.NON_TAXONOMY_CLASSES):
        return cls
    ratios = json.loads(row["sentence_ratios"])
    if row["n_exact"] == row["n_evaluated"]:
        return C.STITCHED
    traceable = sum(1 for r in ratios if r >= threshold)
    if traceable == row["n_evaluated"]:
        return C.DRIFTED
    return C.UNTRACEABLE_PARTIAL if traceable > 0 else C.UNTRACEABLE_NO_BASIS


# ── CLI ──────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Evidence-provenance census")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    p.add_argument("--out-dir", default=None,
                   help="default: data/<review>/analysis/provenance")
    p.add_argument("--threshold", type=float, default=C.THRESHOLD_PRIMARY)
    p.add_argument("--strict-variant", action="store_true",
                   help="also compute the single-paper-sentence containment variant")
    p.add_argument("--legacy", action="store_true",
                   help="also compute the legacy grep_verify verdict for reconciliation")
    p.add_argument("--workers", type=int, default=10)
    p.add_argument("--no-persist", action="store_true",
                   help="compute and write files but do not INSERT into the DB")
    p.add_argument("--notes", default="")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    review_dir = Path(args.data_root) / args.review
    db_path = review_dir / "review.db"
    if not db_path.exists():
        logger.error("No review DB at %s", db_path)
        return 2
    out_dir = Path(args.out_dir) if args.out_dir else review_dir / "analysis" / "provenance"
    out_dir.mkdir(parents=True, exist_ok=True)

    spans_by_paper = load_spans(db_path)
    tiers = load_parser_tiers(db_path)
    total_spans = sum(len(v) for v in spans_by_paper.values())
    logger.info("Loaded %d spans across %d papers", total_spans, len(spans_by_paper))

    with Pool(
        args.workers,
        initializer=_init_worker,
        initargs=(str(review_dir), args.threshold, args.strict_variant, args.legacy),
    ) as pool:
        rows = [
            r
            for chunk in pool.imap_unordered(_classify_paper, spans_by_paper.items())
            for r in chunk
        ]
    for r in rows:
        r["parser_tier"] = tiers.get(r["paper_id"], "")

    if len(rows) != total_spans:
        logger.warning(
            "Coverage gap: %d spans loaded, %d classified (papers with no parsed text)",
            total_spans, len(rows),
        )

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    census_run_id = f"provcensus_{args.review}_{ts}"
    run = {
        "census_run_id": census_run_id,
        "review_name": args.review,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "definitions_sha256": hashlib.sha256(
            DEFINITIONS_PATH.read_bytes()
        ).hexdigest() if DEFINITIONS_PATH.exists() else "",
        "normalization_version": NORMALIZATION_VERSION,
        "tokenizer": TOKENIZER_NAME,
        "tokenizer_version": TOKENIZER_VERSION,
        "threshold_primary": args.threshold,
        "threshold_band": json.dumps(list(C.THRESHOLD_BAND)),
        "min_sentence_tokens": MIN_SENTENCE_TOKENS,
        "ratio_ceiling": C.RATIO_CEILING,
        "spans_total": len(rows),
        "notes": args.notes,
        "absence_pattern_version": ABSENCE_PATTERN_VERSION,
    }

    if not args.no_persist:
        persist(db_path, run, rows)
        logger.info("Persisted %d rows as %s", len(rows), census_run_id)

    summary = {"run": run, **summarize(rows, args.threshold),
               "spans_loaded": total_spans,
               "field_classification": {k: {"class": v[0], "justification": v[1]}
                                        for k, v in FIELD_CLASS.items()}}
    (out_dir / f"census_summary_{ts}.json").write_text(json.dumps(summary, indent=2))

    csv_path = out_dir / f"census_spans_{ts}.csv"
    fields = ["census_run_id", "arm", "paper_id", "field_name", "field_class",
              "span_table", "span_row_id", "taxonomy_class", "strict_variant_class",
              "n_sentences", "n_evaluated", "n_exact", "min_ratio", "has_ellipsis",
              "parser_tier", "snippet_chars", "sentence_ratios", "legacy_grep_verify"]
    with csv_path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({**r, "census_run_id": census_run_id})

    logger.info("Wrote %s and %s", csv_path.name, f"census_summary_{ts}.json")
    print(json.dumps(summary["distribution_by_arm"], indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
