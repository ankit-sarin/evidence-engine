"""Trace export module: quality report, per-paper markdown, disagreement pairs."""

import csv
import json
import logging
import re
import sqlite3
import statistics
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────


def _connect(db_path: str) -> sqlite3.Connection:
    """Open a read-only SQLite connection with Row factory."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _build_tier_map(conn: sqlite3.Connection) -> dict[str, int]:
    """Build field_name → tier mapping from extracted_data JSON."""
    tier_map: dict[str, int] = {}
    rows = conn.execute("SELECT extracted_data FROM extractions").fetchall()
    for row in rows:
        try:
            fields = json.loads(row["extracted_data"])
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(fields, list):
            for f in fields:
                if isinstance(f, dict) and "field_name" in f and "tier" in f:
                    tier_map.setdefault(f["field_name"], f["tier"])
        elif isinstance(fields, dict):
            # dict format: {field_name: value, ...} — no tier info
            pass
    return tier_map


def _first_author(authors_json: str | None) -> str:
    """Extract first author surname from a JSON array of author names."""
    if not authors_json:
        return "Unknown"
    try:
        authors = json.loads(authors_json)
    except (json.JSONDecodeError, TypeError):
        authors = []
    if not authors:
        return "Unknown"
    # Take first author, get surname (last word or before comma)
    first = authors[0]
    parts = re.split(r"[,;]", first)
    return parts[0].strip().split()[-1] if parts[0].strip() else "Unknown"


def _is_truncated(trace: str) -> bool:
    """Heuristic: trace doesn't end with sentence-ending punctuation or closing tag."""
    if not trace:
        return True
    stripped = trace.rstrip()
    if not stripped:
        return True
    return not stripped.endswith((".", "?", "!", ">"))


# ── Function 1: Trace Quality Report ────────────────────────────────


def export_trace_quality_report(db_path: str, output_path: str) -> dict:
    """Generate trace length distribution and quality metrics as JSON + Markdown."""
    conn = _connect(db_path)
    try:
        return _trace_quality_report(conn, output_path)
    finally:
        conn.close()


def _trace_quality_report(conn: sqlite3.Connection, output_path: str) -> dict:
    """Core logic for trace quality report."""
    tier_map = _build_tier_map(conn)

    # Gather reasoning traces
    extractions = conn.execute(
        "SELECT paper_id, reasoning_trace, extracted_at FROM extractions"
    ).fetchall()

    trace_lengths = []
    flagged_papers = []
    for ext in extractions:
        trace = ext["reasoning_trace"] or ""
        length = len(trace)
        trace_lengths.append(length)
        if length < 500:
            flagged_papers.append({
                "paper_id": ext["paper_id"],
                "trace_length": length,
            })

    corpus_size = len(extractions)

    # Trace stats
    if trace_lengths:
        trace_stats = {
            "total_traces": corpus_size,
            "min_chars": min(trace_lengths),
            "max_chars": max(trace_lengths),
            "mean_chars": round(statistics.mean(trace_lengths), 1),
            "median_chars": float(statistics.median(trace_lengths)),
            "std_chars": round(statistics.stdev(trace_lengths), 1) if len(trace_lengths) > 1 else 0.0,
            "under_500": sum(1 for l in trace_lengths if l < 500),
            "truncated": sum(
                1 for ext in extractions
                if _is_truncated(ext["reasoning_trace"] or "")
            ),
        }
    else:
        trace_stats = {
            "total_traces": 0, "min_chars": 0, "max_chars": 0,
            "mean_chars": 0.0, "median_chars": 0.0, "std_chars": 0.0,
            "under_500": 0, "truncated": 0,
        }

    # Length distribution buckets
    length_dist = {"<1k": 0, "1-2k": 0, "2-3k": 0, "3-4k": 0, "4-5k": 0, "5k+": 0}
    for l in trace_lengths:
        if l < 1000:
            length_dist["<1k"] += 1
        elif l < 2000:
            length_dist["1-2k"] += 1
        elif l < 3000:
            length_dist["2-3k"] += 1
        elif l < 4000:
            length_dist["3-4k"] += 1
        elif l < 5000:
            length_dist["4-5k"] += 1
        else:
            length_dist["5k+"] += 1

    # Per-field stats from evidence_spans
    spans = conn.execute(
        "SELECT field_name, audit_status, confidence FROM evidence_spans"
    ).fetchall()

    field_stats: dict[str, dict] = {}
    for s in spans:
        fname = s["field_name"]
        if fname not in field_stats:
            field_stats[fname] = {
                "total_spans": 0,
                "verified": 0,
                "flagged": 0,
                "confidences": [],
                "tier": tier_map.get(fname, 0),
            }
        fs = field_stats[fname]
        fs["total_spans"] += 1
        if s["audit_status"] == "verified":
            fs["verified"] += 1
        elif s["audit_status"] == "flagged":
            fs["flagged"] += 1
        if s["confidence"] is not None:
            fs["confidences"].append(s["confidence"])

    per_field_stats = {}
    for fname, fs in sorted(field_stats.items()):
        total = fs["total_spans"]
        verified = fs["verified"]
        per_field_stats[fname] = {
            "total_spans": total,
            "verified": verified,
            "flagged": fs["flagged"],
            "verification_rate": round(verified / total, 2) if total else 0.0,
            "mean_confidence": round(
                statistics.mean(fs["confidences"]), 2
            ) if fs["confidences"] else 0.0,
            "tier": fs["tier"],
        }

    # Per-tier stats
    tier_agg: dict[int, dict] = {}
    for fname, fs in field_stats.items():
        t = fs["tier"]
        if t not in tier_agg:
            tier_agg[t] = {"total_spans": 0, "verified": 0, "flagged": 0}
        tier_agg[t]["total_spans"] += fs["total_spans"]
        tier_agg[t]["verified"] += fs["verified"]
        tier_agg[t]["flagged"] += fs["flagged"]

    per_tier_stats = {}
    for t in sorted(tier_agg):
        ta = tier_agg[t]
        total = ta["total_spans"]
        per_tier_stats[str(t)] = {
            "total_spans": total,
            "verified": ta["verified"],
            "flagged": ta["flagged"],
            "verification_rate": round(ta["verified"] / total, 2) if total else 0.0,
        }

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "corpus_size": corpus_size,
        "trace_stats": trace_stats,
        "length_distribution": length_dist,
        "per_field_stats": per_field_stats,
        "per_tier_stats": per_tier_stats,
        "flagged_papers": flagged_papers,
    }

    # Write JSON
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    # Write companion Markdown
    md_path = output_path.replace(".json", ".md")
    _write_quality_report_md(result, md_path)

    logger.info("Trace quality report written to %s", output_path)
    return result


def _write_quality_report_md(data: dict, md_path: str) -> None:
    """Write companion Markdown summary for the trace quality report."""
    ts = data["trace_stats"]
    lines = [
        "# Reasoning Trace Quality Report",
        "",
        f"Generated: {data['generated_at']}",
        "",
        "## Summary",
        "",
        f"Corpus of **{data['corpus_size']}** papers with reasoning traces. "
        f"Trace lengths range from {ts['min_chars']} to {ts['max_chars']} characters "
        f"(mean {ts['mean_chars']}, median {ts['median_chars']}, "
        f"std {ts['std_chars']}). "
        f"{ts['under_500']} traces under 500 chars (suspect), "
        f"{ts['truncated']} appear truncated.",
        "",
        "## Trace Length Distribution",
        "",
        "| Bucket | Count |",
        "|--------|-------|",
    ]
    for bucket, count in data["length_distribution"].items():
        lines.append(f"| {bucket} | {count} |")

    lines.extend([
        "",
        "## Per-Tier Verification Rates",
        "",
        "| Tier | Total | Verified | Flagged | Rate |",
        "|------|-------|----------|---------|------|",
    ])
    for tier, ts_data in sorted(data["per_tier_stats"].items()):
        lines.append(
            f"| {tier} | {ts_data['total_spans']} | {ts_data['verified']} | "
            f"{ts_data['flagged']} | {ts_data['verification_rate']:.2f} |"
        )

    lines.extend([
        "",
        "## Per-Field Verification Rates",
        "",
        "| Field | Tier | Total | Verified | Flagged | Rate | Confidence |",
        "|-------|------|-------|----------|---------|------|------------|",
    ])
    # Sort by tier, then field name
    sorted_fields = sorted(
        data["per_field_stats"].items(),
        key=lambda x: (x[1]["tier"], x[0]),
    )
    for fname, fs in sorted_fields:
        lines.append(
            f"| {fname} | {fs['tier']} | {fs['total_spans']} | {fs['verified']} | "
            f"{fs['flagged']} | {fs['verification_rate']:.2f} | "
            f"{fs['mean_confidence']:.2f} |"
        )

    if data["flagged_papers"]:
        lines.extend([
            "",
            "## Flagged Papers (trace < 500 chars)",
            "",
        ])
        for fp in data["flagged_papers"]:
            lines.append(f"- Paper {fp['paper_id']}: {fp['trace_length']} chars")

    with open(md_path, "w") as f:
        f.write("\n".join(lines) + "\n")


# ── Function 2: Per-Paper Markdown Traces ────────────────────────────


def export_traces_markdown(db_path: str, output_dir: str) -> list[str]:
    """Export one Markdown file per paper with reasoning trace and structured extraction."""
    conn = _connect(db_path)
    try:
        return _traces_markdown(conn, output_dir)
    finally:
        conn.close()


def _traces_markdown(conn: sqlite3.Connection, output_dir: str) -> list[str]:
    """Core logic for per-paper markdown traces."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    tier_map = _build_tier_map(conn)

    # Get all papers with extractions
    papers = conn.execute(
        """SELECT p.*, e.id AS ext_id, e.reasoning_trace, e.model AS ext_model,
                  e.extracted_at, e.extraction_schema_hash
           FROM papers p
           JOIN extractions e ON e.paper_id = p.id
           WHERE e.id = (SELECT MAX(e2.id) FROM extractions e2 WHERE e2.paper_id = p.id)
           ORDER BY p.id"""
    ).fetchall()

    paths_created = []
    for paper in papers:
        pid = paper["id"]
        first_auth = _first_author(paper["authors"])
        year = paper["year"] or "XXXX"
        filename = f"{pid:03d}_{first_auth}_{year}.md"
        filepath = out / filename

        # Get evidence spans for this extraction
        spans = conn.execute(
            """SELECT field_name, value, source_snippet, confidence,
                      audit_status, audit_rationale
               FROM evidence_spans
               WHERE extraction_id = ?
               ORDER BY field_name""",
            (paper["ext_id"],),
        ).fetchall()

        # Sort spans by tier then field_name
        span_dicts = [dict(s) for s in spans]
        for sd in span_dicts:
            sd["tier"] = tier_map.get(sd["field_name"], 0)
        span_dicts.sort(key=lambda x: (x["tier"], x["field_name"]))

        # Parse authors for frontmatter
        try:
            authors_list = json.loads(paper["authors"] or "[]")
        except (json.JSONDecodeError, TypeError):
            authors_list = []

        lines = [
            "---",
            f"paper_id: {pid}",
            f"title: \"{(paper['title'] or '').replace('\"', '\\\"')}\"",
            f"authors: {json.dumps(authors_list)}",
            f"year: {year}",
            f"doi: {paper['doi'] or ''}",
            f"extraction_model: {paper['ext_model'] or ''}",
            f"extracted_at: {paper['extracted_at'] or ''}",
            f"schema_hash: {paper['extraction_schema_hash'] or ''}",
            "---",
            "",
            "# Reasoning Trace",
            "",
            paper["reasoning_trace"] or "(no trace)",
            "",
            "# Structured Extraction",
            "",
            "| Field | Tier | Value | Confidence | Audit Status |",
            "|-------|------|-------|------------|--------------|",
        ]

        for sd in span_dicts:
            value_short = (sd["value"] or "")[:80]
            lines.append(
                f"| {sd['field_name']} | {sd['tier']} | {value_short} | "
                f"{sd['confidence']} | {sd['audit_status']} |"
            )

        lines.extend(["", "# Evidence Spans (Detail)", ""])

        for sd in span_dicts:
            lines.extend([
                f"## {sd['field_name']} (Tier {sd['tier']})",
                f"- **Value:** {sd['value'] or ''}",
                f"- **Source Snippet:** {sd['source_snippet'] or ''}",
                f"- **Confidence:** {sd['confidence']}",
                f"- **Audit Status:** {sd['audit_status']}",
                f"- **Audit Rationale:** {sd['audit_rationale'] or 'N/A'}",
                "",
            ])

        filepath.write_text("\n".join(lines))
        paths_created.append(str(filepath))

    logger.info("Trace markdown files written to %s (%d files)", output_dir, len(paths_created))
    return paths_created


# ── Function 3: Disagreement Pairs (stub) ───────────────────────────

# The 15 shared field names (AI and human Excel use identical names)
SHARED_FIELDS = [
    "study_type", "robot_platform", "task_performed", "sample_size", "country",
    "autonomy_level", "validation_setting", "human_oversight_model", "fda_status",
    "study_design", "primary_outcome_metric", "primary_outcome_value",
    "comparison_to_human", "key_limitation", "clinical_readiness_assessment",
]

_TEMPLATE_COMMENTS = {
    "paper_id": "Integer paper ID matching the AI extraction database",
    "field_name": f"One of: {', '.join(SHARED_FIELDS)}",
    "human_value": "The value extracted by the human reviewer",
}

_OUTPUT_COLUMNS = [
    "paper_id", "field_name", "tier", "human_value", "ai_value", "match",
    "ai_confidence", "ai_source_snippet", "ai_reasoning_excerpt", "error_type",
]


def export_disagreement_pairs(
    db_path: str, human_csv_path: str | None, output_path: str
) -> str:
    """Generate concordance analysis CSV for PLUM Lab error taxonomy.

    If human_csv_path is None or doesn't exist, writes a template CSV.
    Otherwise, pairs human and AI extractions for concordance analysis.
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    if human_csv_path is None or not Path(human_csv_path).exists():
        return _write_template_csv(output_path)

    conn = _connect(db_path)
    try:
        return _build_disagreement_pairs(conn, human_csv_path, output_path)
    finally:
        conn.close()


def _write_template_csv(output_path: str) -> str:
    """Write a template CSV with headers and a comment row."""
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["paper_id", "field_name", "human_value"])
        # Comment row explaining each column
        writer.writerow([
            _TEMPLATE_COMMENTS["paper_id"],
            _TEMPLATE_COMMENTS["field_name"],
            _TEMPLATE_COMMENTS["human_value"],
        ])
    logger.info("Disagreement template CSV written to %s", output_path)
    return output_path


def _build_disagreement_pairs(
    conn: sqlite3.Connection, human_csv_path: str, output_path: str
) -> str:
    """Build disagreement pairs from human and AI data."""
    tier_map = _build_tier_map(conn)

    # Load human data
    human_data: dict[tuple[int, str], str] = {}
    with open(human_csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                pid = int(row["paper_id"])
            except (ValueError, KeyError):
                continue
            fname = row.get("field_name", "")
            human_data[(pid, fname)] = row.get("human_value", "")

    # Build AI lookup: (paper_id, field_name) -> span data
    ai_spans = conn.execute(
        """SELECT e.paper_id, es.field_name, es.value, es.confidence,
                  es.source_snippet, e.reasoning_trace
           FROM evidence_spans es
           JOIN extractions e ON e.id = es.extraction_id
           WHERE e.id IN (
               SELECT MAX(e2.id) FROM extractions e2 GROUP BY e2.paper_id
           )"""
    ).fetchall()

    ai_data: dict[tuple[int, str], dict] = {}
    for s in ai_spans:
        key = (s["paper_id"], s["field_name"])
        ai_data[key] = {
            "value": s["value"],
            "confidence": s["confidence"],
            "source_snippet": s["source_snippet"],
            "reasoning_trace": s["reasoning_trace"] or "",
        }

    # Build output rows
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(_OUTPUT_COLUMNS)

        for (pid, fname), human_val in sorted(human_data.items()):
            if (pid, fname) not in ai_data:
                continue

            ai = ai_data[(pid, fname)]

            # Extract reasoning excerpt mentioning the field
            trace = ai["reasoning_trace"]
            excerpt = _extract_reasoning_excerpt(trace, fname)

            match = human_val.strip().lower() == (ai["value"] or "").strip().lower()

            writer.writerow([
                pid,
                fname,
                tier_map.get(fname, 0),
                human_val,
                ai["value"],
                match,
                ai["confidence"],
                ai["source_snippet"] or "",
                excerpt,
                "",  # error_type — blank for PLUM Lab coding
            ])

    logger.info("Disagreement pairs written to %s", output_path)
    return output_path


def _extract_reasoning_excerpt(trace: str, field_name: str) -> str:
    """Extract first 500 chars of trace mentioning the field, or first 500 chars."""
    if not trace:
        return ""

    # Try to find a paragraph mentioning the field
    idx = trace.lower().find(field_name.lower())
    if idx >= 0:
        # Back up to start of sentence/paragraph
        start = max(0, trace.rfind("\n", 0, idx))
        return trace[start:start + 500].strip()

    # Fallback: first 500 chars
    return trace[:500].strip()
