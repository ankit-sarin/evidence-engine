#!/usr/bin/env python3
"""Re-parse cloud extractions that have 0 spans using stored extracted_data.

No API calls — reads raw JSON from cloud_extractions.extracted_data,
runs it through the (fixed) parse_response_to_spans(), and inserts
the resulting spans into cloud_evidence_spans.
"""

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.core.review_paths import load_spec_for, spec_path_for
from engine.cloud.base import CloudExtractorBase

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")



def main():
    parser = argparse.ArgumentParser(description="Re-parse cloud extractions with 0 spans")
    parser.add_argument("--review", required=True, help="Review id. The review's identity — the spec file and the data root both derive from it.")
    parser.add_argument("--spec", default=None, help="Override the Review Spec path. Defaults to review_specs/<review>.yaml; an override must carry the same review_id.")
    args = parser.parse_args()

    review = args.review
    db_path = f"data/{review}/review.db"
    # Identity gate. load_spec_for refuses a spec naming a different review,
    # and it runs before anything opens a database (SPEC-AUTH-01).
    spec = load_spec_for(review, args.spec)
    spec_path = str(args.spec or spec_path_for(review))

    conn = sqlite3.connect(db_path)
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
    extractor = CloudExtractorBase(db_path, spec_path)

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
