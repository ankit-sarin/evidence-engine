"""Migration 003: Backfill expanded-corpus screening data into review.db.

Imports ~9,234 excluded papers and their screening/verification traces from
flat files in data/surgical_autonomy/expanded_search/ into the main database.

Idempotent: skips papers already present (by DOI, PMID, or title match),
skips screening decisions where paper_id + pass_number already has a row.

Usage:
    python -m engine.migrations.003_backfill_expanded_screening
"""

import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "surgical_autonomy"
EXPANDED_DIR = DATA_DIR / "expanded_search"

ABSTRACTS_FILE = EXPANDED_DIR / "abstracts.jsonl"
SCREENING_FILE = EXPANDED_DIR / "screening_results.csv"
VERIFICATION_FILE = EXPANDED_DIR / "verification_results.csv"
RESCREEN_FILE = EXPANDED_DIR / "rescreen_original_251.csv"
DB_PATH = DATA_DIR / "review.db"

SCREENING_MODEL = "qwen3:8b"
VERIFICATION_MODEL = "gemma3:27b"
BACKFILL_TS = datetime.now(timezone.utc).isoformat()


def _load_abstracts_by_key():
    """Load abstracts.jsonl into dicts keyed by DOI and by PMID."""
    by_doi, by_pmid = {}, {}
    with open(ABSTRACTS_FILE) as f:
        for line in f:
            rec = json.loads(line)
            doi = rec.get("doi", "").strip()
            pmid = rec.get("pmid", "").strip()
            if doi:
                by_doi[doi.lower()] = rec
            if pmid:
                by_pmid[pmid] = rec
    return by_doi, by_pmid


def _load_screening_rows():
    """Load all screening_results.csv rows as a list (preserving order)."""
    rows = []
    with open(SCREENING_FILE) as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def _load_verification_by_key():
    """Load verification_results.csv into dicts keyed by DOI and PMID."""
    by_doi, by_pmid = {}, {}
    with open(VERIFICATION_FILE) as f:
        for row in csv.DictReader(f):
            doi = row.get("doi", "").strip()
            pmid = row.get("pmid", "").strip()
            if doi:
                by_doi[doi.lower()] = row
            if pmid:
                by_pmid[pmid] = row
    return by_doi, by_pmid


def _load_rescreen():
    """Load rescreen_original_251.csv keyed by DB id."""
    records = {}
    with open(RESCREEN_FILE) as f:
        for row in csv.DictReader(f):
            records[int(row["id"])] = row
    return records


def _build_lookup_maps(conn):
    """Build DOI→pid, PMID→pid, and title→pid maps from existing papers."""
    doi_map, pmid_map, title_map = {}, {}, {}
    for row in conn.execute("SELECT id, doi, pmid, title FROM papers").fetchall():
        if row["doi"]:
            doi_map[row["doi"].strip().lower()] = row["id"]
        if row["pmid"]:
            pmid_map[row["pmid"].strip()] = row["id"]
        if row["title"]:
            title_map[row["title"].strip().lower()] = row["id"]
    return doi_map, pmid_map, title_map


def _find_paper(doi, pmid, title, doi_map, pmid_map, title_map):
    """Resolve a paper to its DB id using DOI, then PMID, then title."""
    if doi:
        pid = doi_map.get(doi.lower())
        if pid:
            return pid
    if pmid:
        pid = pmid_map.get(pmid)
        if pid:
            return pid
    if title:
        pid = title_map.get(title.strip().lower())
        if pid:
            return pid
    return None


def _determine_status(screening_decision, ver_row):
    """Determine the paper status from screening/verification decisions."""
    if screening_decision == "exclude":
        return "ABSTRACT_SCREENED_OUT"
    elif screening_decision == "flagged":
        return "ABSTRACT_SCREEN_FLAGGED"
    elif screening_decision == "include":
        if ver_row:
            final = ver_row.get("final_decision", "")
            if final == "include":
                return "ABSTRACT_SCREENED_IN"
            elif final == "flagged":
                return "ABSTRACT_SCREEN_FLAGGED"
        return "ABSTRACT_SCREENED_IN"
    return "ABSTRACT_SCREENED_OUT"


def _rationale_with_confidence(rationale, confidence):
    if confidence:
        return f"{rationale} [confidence: {confidence}]"
    return rationale


def run_migration(db_path=None):
    if db_path is None:
        db_path = DB_PATH

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    # Load flat files
    print("Loading flat files...")
    abs_by_doi, abs_by_pmid = _load_abstracts_by_key()
    screening_rows = _load_screening_rows()
    ver_by_doi, ver_by_pmid = _load_verification_by_key()
    rescreen = _load_rescreen()

    print(f"  abstracts.jsonl:           {len(abs_by_doi)} by DOI, {len(abs_by_pmid)} by PMID")
    print(f"  screening_results.csv:     {len(screening_rows)} rows")
    print(f"  verification_results.csv:  {len(ver_by_doi)} by DOI, {len(ver_by_pmid)} by PMID")
    print(f"  rescreen_original_251.csv: {len(rescreen)} records")

    # Get existing state
    existing_sd_keys = set()
    for r in conn.execute("SELECT paper_id, pass_number FROM abstract_screening_decisions"):
        existing_sd_keys.add((r[0], r[1]))
    existing_vd_keys = set()
    for r in conn.execute("SELECT paper_id FROM abstract_verification_decisions"):
        existing_vd_keys.add(r[0])

    pre_count = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    print(f"\nPapers before migration: {pre_count}")

    stats = {
        "papers_inserted": 0, "papers_skipped": 0, "papers_no_key": 0,
        "screening_inserted": 0, "screening_skipped": 0, "screening_no_pid": 0,
        "verification_inserted": 0, "verification_skipped": 0,
        "rescreen_inserted": 0, "rescreen_skipped": 0,
    }

    try:
        conn.execute("BEGIN")

        # ── Step 1: Insert missing papers ──────────────────────────────
        print("\n[Step 1] Inserting missing expanded-corpus papers...")

        doi_map, pmid_map, title_map = _build_lookup_maps(conn)

        for scr_row in screening_rows:
            doi = scr_row.get("doi", "").strip()
            pmid = scr_row.get("pmid", "").strip()
            title = scr_row.get("title", "").strip()

            # Already in DB?
            if _find_paper(doi, pmid, title, doi_map, pmid_map, title_map):
                stats["papers_skipped"] += 1
                continue

            # Look up abstract
            abs_data = {}
            if doi:
                abs_data = abs_by_doi.get(doi.lower(), {})
            if not abs_data and pmid:
                abs_data = abs_by_pmid.get(pmid, {})

            # Look up verification for status
            ver_row = None
            if doi:
                ver_row = ver_by_doi.get(doi.lower())
            if not ver_row and pmid:
                ver_row = ver_by_pmid.get(pmid)

            status = _determine_status(scr_row["screening_decision"], ver_row)

            year_str = scr_row.get("year", "") or abs_data.get("year", "")
            year = int(year_str) if year_str else None

            conn.execute(
                """INSERT INTO papers
                   (pmid, doi, title, abstract, authors, journal, year,
                    source, status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    pmid or None,
                    doi or None,
                    title or abs_data.get("title", "Unknown"),
                    abs_data.get("abstract"),
                    None,
                    scr_row.get("journal", abs_data.get("journal")),
                    year,
                    scr_row.get("source", abs_data.get("source", "pubmed")),
                    status,
                    BACKFILL_TS,
                    BACKFILL_TS,
                ),
            )
            stats["papers_inserted"] += 1

            new_pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            if doi:
                doi_map[doi.lower()] = new_pid
            if pmid:
                pmid_map[pmid] = new_pid
            if title:
                title_map[title.lower()] = new_pid

        print(f"  Inserted: {stats['papers_inserted']}, Skipped: {stats['papers_skipped']}")

        # Refresh maps after all inserts
        doi_map, pmid_map, title_map = _build_lookup_maps(conn)

        # ── Step 2: Screening decisions for expanded corpus ────────────
        print("\n[Step 2] Inserting screening decisions for expanded corpus...")

        for scr_row in screening_rows:
            doi = scr_row.get("doi", "").strip()
            pmid = scr_row.get("pmid", "").strip()
            title = scr_row.get("title", "").strip()

            pid = _find_paper(doi, pmid, title, doi_map, pmid_map, title_map)
            if pid is None:
                stats["screening_no_pid"] += 1
                continue

            for pass_num in (1, 2):
                if (pid, pass_num) in existing_sd_keys:
                    stats["screening_skipped"] += 1
                    continue

                decision = scr_row.get(f"pass{pass_num}_decision", "")
                rationale = scr_row.get(f"pass{pass_num}_rationale", "")
                confidence = scr_row.get(f"pass{pass_num}_confidence", "")

                if not decision:
                    continue

                conn.execute(
                    """INSERT INTO abstract_screening_decisions
                       (paper_id, pass_number, decision, rationale, model, decided_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (pid, pass_num, decision,
                     _rationale_with_confidence(rationale, confidence),
                     SCREENING_MODEL, BACKFILL_TS),
                )
                stats["screening_inserted"] += 1
                existing_sd_keys.add((pid, pass_num))

        print(f"  Inserted: {stats['screening_inserted']}, "
              f"Skipped: {stats['screening_skipped']}, "
              f"No PID: {stats['screening_no_pid']}")

        # ── Step 3: Rescreen data for original 251 ────────────────────
        print("\n[Step 3] Backfilling rescreen data for original 251 papers...")

        for db_id, rs_row in rescreen.items():
            for pass_num in (1, 2):
                if (db_id, pass_num) in existing_sd_keys:
                    stats["rescreen_skipped"] += 1
                    continue

                decision = rs_row.get(f"pass{pass_num}_decision", "")
                rationale = rs_row.get(f"pass{pass_num}_rationale", "")
                confidence = rs_row.get(f"pass{pass_num}_confidence", "")
                if not decision:
                    continue

                conn.execute(
                    """INSERT INTO abstract_screening_decisions
                       (paper_id, pass_number, decision, rationale, model, decided_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (db_id, pass_num, decision,
                     _rationale_with_confidence(rationale, confidence),
                     SCREENING_MODEL, BACKFILL_TS),
                )
                stats["rescreen_inserted"] += 1
                existing_sd_keys.add((db_id, pass_num))

        print(f"  Inserted: {stats['rescreen_inserted']}, "
              f"Skipped: {stats['rescreen_skipped']}")

        # ── Step 4: Verification decisions ─────────────────────────────
        print("\n[Step 4] Inserting verification decisions for expanded corpus...")

        ver_rows_all = []
        with open(VERIFICATION_FILE) as f:
            ver_rows_all = list(csv.DictReader(f))

        for ver_row in ver_rows_all:
            doi = ver_row.get("doi", "").strip()
            pmid = ver_row.get("pmid", "").strip()
            title = ver_row.get("title", "").strip()

            pid = _find_paper(doi, pmid, title, doi_map, pmid_map, title_map)
            if pid is None:
                continue

            if pid in existing_vd_keys:
                stats["verification_skipped"] += 1
                continue

            rationale = ver_row.get("verification_rationale", "")
            confidence = ver_row.get("verification_confidence", "")
            decision = ver_row.get("verification_decision",
                                   ver_row.get("final_decision", ""))

            conn.execute(
                """INSERT INTO abstract_verification_decisions
                   (paper_id, decision, rationale, model, decided_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (pid, decision,
                 _rationale_with_confidence(rationale, confidence),
                 VERIFICATION_MODEL, BACKFILL_TS),
            )
            stats["verification_inserted"] += 1
            existing_vd_keys.add(pid)

        print(f"  Inserted: {stats['verification_inserted']}, "
              f"Skipped: {stats['verification_skipped']}")

        # ── Step 5: Original-251 verification gap ──────────────────────
        print("\n[Step 5] Original-251 verification data:")
        print("  GAP: Original 251 have screening traces but no verification traces.")
        print("  Verification (gemma3:27b) was introduced for expanded corpus only.")

        conn.execute("COMMIT")
        print("\n  Transaction committed.")

    except Exception as e:
        conn.execute("ROLLBACK")
        print(f"\n  ROLLBACK — error: {e}")
        raise

    # ── Validation ─────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("VALIDATION")
    print("=" * 60)

    total = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    print(f"\nTotal papers: {total} (was {pre_count}, inserted {stats['papers_inserted']})")

    print("\nStatus breakdown:")
    for row in conn.execute(
        "SELECT status, COUNT(*) as cnt FROM papers GROUP BY status ORDER BY cnt DESC"
    ):
        print(f"  {row['status']:30s} {row['cnt']:>6d}")

    sd_total = conn.execute("SELECT COUNT(*) FROM abstract_screening_decisions").fetchone()[0]
    vd_total = conn.execute("SELECT COUNT(*) FROM abstract_verification_decisions").fetchone()[0]
    print(f"\nScreening decisions:     {sd_total}")
    print(f"Verification decisions:  {vd_total}")

    # Spot check
    print("\nSpot check — 3 random newly-inserted ABSTRACT_SCREENED_OUT papers:")
    spots = conn.execute(
        """SELECT p.id, p.title, p.abstract, p.doi, p.pmid,
                  sd.rationale, sd.decision
           FROM papers p
           JOIN abstract_screening_decisions sd ON sd.paper_id = p.id AND sd.pass_number = 1
           WHERE p.status = 'ABSTRACT_SCREENED_OUT' AND p.created_at = ?
           ORDER BY RANDOM() LIMIT 3""",
        (BACKFILL_TS,),
    ).fetchall()
    for s in spots:
        abs_ok = bool(s["abstract"] and len(s["abstract"]) > 10)
        rat_ok = bool(s["rationale"] and len(s["rationale"]) > 10)
        print(f"  ID {s['id']}: {s['title'][:70]}...")
        print(f"    DOI={s['doi'] or 'none'}, PMID={s['pmid'] or 'none'}")
        print(f"    abstract={'present' if abs_ok else 'MISSING'}, "
              f"rationale={'present' if rat_ok else 'MISSING'}")

    conn.close()

    stats["total_papers"] = total
    stats["total_screening_decisions"] = sd_total
    stats["total_verification_decisions"] = vd_total
    return stats


if __name__ == "__main__":
    result = run_migration()
    print("\n" + json.dumps(result, indent=2))
