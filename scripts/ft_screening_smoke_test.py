#!/usr/bin/env python3
"""Full-text screening smoke test — 5 papers, end-to-end integration check.

Runs primary FT screen (Qwen3.5:27b) + verification (Gemma3:27b) on 5 known
papers from the original corpus. Records decisions to ft_screening_decisions
and ft_verification_decisions tables.

Papers are AI_AUDIT_COMPLETE so we call the single-paper functions directly
rather than the batch pipeline (which expects PARSED status).
"""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.agents.ft_screener import (
    ft_screen_paper,
    ft_verify_paper,
    truncate_paper_text,
)
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec

# ── Config ──────────────────────────────────────────────────────────

PAPER_IDS = [9, 12, 168, 23, 4]
SPEC_PATH = "review_specs/surgical_autonomy_v1.yaml"
REVIEW_NAME = "surgical_autonomy"

LABELS = {
    9: "Abdominal (intestinal anastomosis)",
    12: "General surgery (appendectomy)",
    168: "Bench/preclinical (dVRK pick-and-place)",
    23: "Possible review (surgical robotics data age)",
    4: "Orthopedic (bone remodeling — specialty test)",
}

# ── Main ────────────────────────────────────────────────────────────

def main():
    spec = load_review_spec(SPEC_PATH)
    db = ReviewDatabase(REVIEW_NAME)
    parsed_dir = db.db_path.parent / "parsed_text"

    print("=" * 80)
    print("FULL-TEXT SCREENING SMOKE TEST — 5 papers")
    print(f"Primary model: {spec.ft_screening_models.primary}")
    print(f"Verifier model: {spec.ft_screening_models.verifier}")
    print(f"Think mode: {spec.ft_screening_models.think}")
    print(f"Temperature: {spec.ft_screening_models.temperature}")
    print("=" * 80)

    primary_results = {}
    primary_times = {}

    # ── Phase 1: Primary Screening ──────────────────────────────────

    print("\n── PHASE 1: Primary Full-Text Screening ──\n")

    for pid in PAPER_IDS:
        paper = db._conn.execute(
            "SELECT id, title, abstract FROM papers WHERE id = ?", (pid,)
        ).fetchone()
        title = paper["title"]
        abstract = paper["abstract"] or ""

        # Load parsed text
        md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)
        if not md_files:
            print(f"  Paper {pid}: NO PARSED TEXT — skipping")
            continue
        full_text = md_files[0].read_text()

        # Truncate
        truncated = truncate_paper_text(full_text, title=title, abstract=abstract)
        original_chars = len(full_text)
        truncated_chars = len(truncated)
        was_truncated = truncated_chars < original_chars + len(title) + len(abstract) + 30

        print(f"Paper {pid}: {LABELS.get(pid, '')}")
        print(f"  Title: {title[:80]}")
        print(f"  Full text: {original_chars:,} chars → truncated: {truncated_chars:,} chars"
              f" {'(TRUNCATED)' if was_truncated else '(no truncation needed)'}")

        # Run primary screen
        t0 = time.time()
        try:
            decision = ft_screen_paper(truncated, spec)
            elapsed = time.time() - t0
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  ERROR after {elapsed:.1f}s: {e}")
            import traceback
            traceback.print_exc()
            continue

        primary_results[pid] = decision
        primary_times[pid] = elapsed

        # Write to DB
        db.add_ft_screening_decision(
            pid,
            spec.ft_screening_models.primary,
            decision.decision,
            decision.reason_code,
            decision.rationale,
            decision.confidence,
        )

        print(f"  Decision: {decision.decision}")
        print(f"  Reason code: {decision.reason_code}")
        print(f"  Confidence: {decision.confidence:.2f}")
        print(f"  Rationale: {decision.rationale[:200]}")
        print(f"  Time: {elapsed:.1f}s")
        print()

    # ── Phase 2: Verification ───────────────────────────────────────

    print("\n── PHASE 2: Verification (FT_ELIGIBLE papers only) ──\n")

    eligible_pids = [pid for pid, d in primary_results.items() if d.decision == "FT_ELIGIBLE"]
    verification_results = {}
    verification_times = {}

    if not eligible_pids:
        print("  No FT_ELIGIBLE papers to verify.\n")
    else:
        for pid in eligible_pids:
            paper = db._conn.execute(
                "SELECT id, title, abstract FROM papers WHERE id = ?", (pid,)
            ).fetchone()
            title = paper["title"]
            abstract = paper["abstract"] or ""

            md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)
            full_text = md_files[0].read_text()
            truncated = truncate_paper_text(full_text, title=title, abstract=abstract)

            print(f"Paper {pid}: {LABELS.get(pid, '')}")
            print(f"  Title: {title[:80]}")

            t0 = time.time()
            try:
                v_decision = ft_verify_paper(truncated, spec)
                elapsed = time.time() - t0
            except Exception as e:
                elapsed = time.time() - t0
                print(f"  ERROR after {elapsed:.1f}s: {e}")
                import traceback
                traceback.print_exc()
                continue

            verification_results[pid] = v_decision
            verification_times[pid] = elapsed

            # Write to DB
            db.add_ft_verification_decision(
                pid,
                spec.ft_screening_models.verifier,
                v_decision.decision,
                v_decision.rationale,
                v_decision.confidence,
            )

            print(f"  Verification: {v_decision.decision}")
            print(f"  Confidence: {v_decision.confidence:.2f}")
            print(f"  Rationale: {v_decision.rationale[:200]}")
            print(f"  Time: {elapsed:.1f}s")
            print()

    # ── Summary ─────────────────────────────────────────────────────

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    print("\nPrimary Screening Results:")
    print(f"{'ID':>5}  {'Decision':<14}  {'Reason':<22}  {'Conf':>5}  {'Time':>6}  Label")
    print("-" * 90)
    for pid in PAPER_IDS:
        if pid in primary_results:
            d = primary_results[pid]
            t = primary_times[pid]
            print(f"{pid:>5}  {d.decision:<14}  {d.reason_code:<22}  {d.confidence:>5.2f}  {t:>5.1f}s  {LABELS.get(pid, '')}")

    if verification_results:
        print("\nVerification Results:")
        print(f"{'ID':>5}  {'Decision':<14}  {'Conf':>5}  {'Time':>6}  Label")
        print("-" * 70)
        for pid, d in verification_results.items():
            t = verification_times[pid]
            print(f"{pid:>5}  {d.decision:<14}  {d.confidence:>5.2f}  {t:>5.1f}s  {LABELS.get(pid, '')}")

    print("\nTiming:")
    if primary_times:
        times = list(primary_times.values())
        print(f"  Primary:  mean={sum(times)/len(times):.1f}s  min={min(times):.1f}s  max={max(times):.1f}s")
    if verification_times:
        times = list(verification_times.values())
        print(f"  Verifier: mean={sum(times)/len(times):.1f}s  min={min(times):.1f}s  max={max(times):.1f}s")
    if primary_times:
        mean_total = sum(primary_times.values()) / len(primary_times)
        if verification_times:
            mean_total += sum(verification_times.values()) / len(verification_times)
        print(f"  Estimated time for 648 papers: {mean_total * 648 / 60:.0f} minutes")

    # ── Dump DB tables ──────────────────────────────────────────────

    print("\n\n── ft_screening_decisions (all rows) ──\n")
    rows = db._conn.execute(
        "SELECT * FROM ft_screening_decisions ORDER BY id"
    ).fetchall()
    if rows:
        cols = rows[0].keys()
        print("  ".join(f"{c:<20}" for c in cols))
        print("-" * (22 * len(cols)))
        for r in rows:
            print("  ".join(f"{str(r[c])[:20]:<20}" for c in cols))
    else:
        print("  (empty)")

    print("\n\n── ft_verification_decisions (all rows) ──\n")
    rows = db._conn.execute(
        "SELECT * FROM ft_verification_decisions ORDER BY id"
    ).fetchall()
    if rows:
        cols = rows[0].keys()
        print("  ".join(f"{c:<20}" for c in cols))
        print("-" * (22 * len(cols)))
        for r in rows:
            print("  ".join(f"{str(r[c])[:20]:<20}" for c in cols))
    else:
        print("  (empty)")

    db.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
