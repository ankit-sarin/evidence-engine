#!/usr/bin/env python3
"""Re-parse cloud extractions that have 0 spans using stored extracted_data.

No API calls — reads raw JSON from cloud_extractions.extracted_data,
runs it through the (fixed) parse_response_to_spans(), and inserts
the resulting spans into cloud_evidence_spans.
"""

import json
import sqlite3
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.cloud.base import CloudExtractorBase

DB_PATH = "data/surgical_autonomy/review.db"
SPEC_PATH = "review_specs/surgical_autonomy_v1.yaml"


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    # Find all cloud extractions with 0 spans
    rows = conn.execute("""
        SELECT ce.id, ce.paper_id, ce.arm, ce.model_string, ce.extracted_data
        FROM cloud_extractions ce
        WHERE ce.extracted_data IS NOT NULL
          AND (SELECT COUNT(*) FROM cloud_evidence_spans cs
               WHERE cs.cloud_extraction_id = ce.id) = 0
        ORDER BY ce.id
    """).fetchall()

    if not rows:
        print("No cloud extractions with 0 spans found.")
        return

    print(f"Found {len(rows)} cloud extractions with 0 spans.\n")

    # Use the base class just for its parser
    extractor = CloudExtractorBase(DB_PATH, SPEC_PATH)

    results = []
    for row in rows:
        ext_id = row["id"]
        paper_id = row["paper_id"]
        arm = row["arm"]
        model = row["model_string"]
        raw_data = json.loads(row["extracted_data"])

        spans = extractor.parse_response_to_spans(raw_data)

        if spans:
            for span in spans:
                conn.execute(
                    """INSERT INTO cloud_evidence_spans
                       (cloud_extraction_id, field_name, value, source_snippet,
                        confidence, tier)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (ext_id, span["field_name"], span.get("value"),
                     span.get("source_snippet"), span.get("confidence"),
                     span.get("tier")),
                )
            conn.commit()

        results.append({
            "ext_id": ext_id,
            "paper_id": paper_id,
            "arm": arm,
            "model": model,
            "before": 0,
            "after": len(spans),
        })

    extractor.close()
    conn.close()

    # Report
    print(f"{'ext_id':>6}  {'paper_id':>8}  {'arm':<28}  {'model':<24}  {'before':>6}  {'after':>5}")
    print("-" * 90)
    still_zero = []
    for r in results:
        flag = " ⚠" if r["after"] == 0 else ""
        print(f"{r['ext_id']:>6}  {r['paper_id']:>8}  {r['arm']:<28}  {r['model']:<24}  {r['before']:>6}  {r['after']:>5}{flag}")
        if r["after"] == 0:
            still_zero.append(r)

    print()
    total_new = sum(r["after"] for r in results)
    print(f"Total new spans inserted: {total_new}")
    if still_zero:
        print(f"\n⚠  {len(still_zero)} paper(s) still have 0 spans after re-parse:")
        for r in still_zero:
            print(f"   ext_id={r['ext_id']} paper_id={r['paper_id']} arm={r['arm']}")
    else:
        print("All papers now have spans.")


if __name__ == "__main__":
    main()
