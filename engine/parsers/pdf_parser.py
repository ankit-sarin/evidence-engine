"""PDF-to-Markdown parser: Docling → PyMuPDF fallback → Qwen2.5-VL for scanned.

CLI:
    python -m engine.parsers.pdf_parser --verify-hashes --review surgical_autonomy
"""

import argparse
import base64
import hashlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import fitz  # PyMuPDF
from engine.utils.ollama_client import ollama_chat
from docling.document_converter import DocumentConverter

from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec
from engine.parsers.models import ParseAttempt, ParsedDocument
from engine.parsers.parse_quality import (
    EMPTY_TEXT,
    GLYPH_DENSITY,
    REPLACEMENT_DENSITY,
    SHATTERED,
    Thresholds,
    assess,
)

logger = logging.getLogger(__name__)

# Defaults — overridden by ReviewSpec.pdf_parsing when available
_SCANNED_THRESHOLD = 100  # chars per page — below this, assume scanned
_VISION_MODEL = "qwen2.5vl:7b"
_VISION_MAX_PAGES = 60
_MAX_ATTEMPTS = 3

# Which parser to try next, by the defect that failed the current one. Ordered,
# and the order is a claim about causes rather than a preference.
#
# GLYPH_DENSITY / REPLACEMENT_DENSITY -> PyMuPDF first. Docling emits
# `GLYPH<...>` when an embedded font carries no ToUnicode map (docling #3081);
# the text layer itself may be perfectly readable by another extractor, so the
# cheap, deterministic, local one is tried before rendering pages to a model.
# Every glyph-damaged paper in this corpus (586, 699, 719) was Docling output.
#
# SHATTERED -> vision only. Shattering means the text layer's character
# positions no longer reconstruct words; p455 is PyMuPDF output already, and a
# second character-level extractor has nothing new to read. Only re-OCR does.
#
# EMPTY_TEXT -> PyMuPDF then vision, matching the pre-existing sparse-output
# cascade this gate sits behind.
_REROUTE: dict[str, tuple[str, ...]] = {
    GLYPH_DENSITY: ("pymupdf", "qwen2.5vl"),
    REPLACEMENT_DENSITY: ("pymupdf", "qwen2.5vl"),
    SHATTERED: ("qwen2.5vl",),
    EMPTY_TEXT: ("pymupdf", "qwen2.5vl"),
}


# ── Public API ───────────────────────────────────────────────────────


def compute_pdf_hash(pdf_path: str) -> str | None:
    """SHA-256 hash of the PDF file contents. Returns None if file doesn't exist."""
    path = Path(pdf_path)
    if not path.exists():
        return None
    h = hashlib.sha256()
    with open(pdf_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def is_scanned_pdf(pdf_path: str, threshold: int = _SCANNED_THRESHOLD) -> bool:
    """Heuristic: if extractable text is sparse relative to page count, it's scanned."""
    doc = fitz.open(pdf_path)
    try:
        num_pages = len(doc)
        if num_pages == 0:
            return True
        total_chars = sum(len(page.get_text()) for page in doc)
        chars_per_page = total_chars / num_pages
        return chars_per_page < threshold
    finally:
        doc.close()


def parse_with_docling(pdf_path: str) -> str:
    """Parse a digital PDF to Markdown using Docling."""
    converter = DocumentConverter()
    result = converter.convert(pdf_path)
    return result.document.export_to_markdown()


def parse_with_pymupdf(pdf_path: str) -> str:
    """Extract text from a digital PDF using PyMuPDF as a structural fallback."""
    doc = fitz.open(pdf_path)
    pages: list[str] = []
    try:
        for i, page in enumerate(doc):
            text = page.get_text("text")
            pages.append(f"<!-- Page {i + 1} -->\n{text}")
    finally:
        doc.close()
    return "\n\n---\n\n".join(pages)


def parse_with_vision(pdf_path: str, vision_model: str = _VISION_MODEL) -> str:
    """Parse a scanned PDF by sending page images to a vision model via Ollama."""
    doc = fitz.open(pdf_path)
    pages_md: list[str] = []

    try:
        for page_num in range(len(doc)):
            page = doc[page_num]
            # Render page to PNG at 200 DPI
            pix = page.get_pixmap(dpi=200)
            img_bytes = pix.tobytes("png")
            img_b64 = base64.b64encode(img_bytes).decode()

            response = ollama_chat(
                model=vision_model,
                messages=[
                    {
                        "role": "user",
                        "content": (
                            "Extract all text from this page. Preserve tables, "
                            "headings, and formatting. Output as Markdown."
                        ),
                        "images": [img_b64],
                    }
                ],
                options={"temperature": 0},
            )
            page_text = response.message.content
            pages_md.append(f"<!-- Page {page_num + 1} -->\n{page_text}")
            logger.info("Qwen2.5-VL parsed page %d/%d", page_num + 1, len(doc))
    finally:
        doc.close()

    return "\n\n---\n\n".join(pages_md)


def page_count(pdf_path: str) -> int:
    """Page count without rendering anything."""
    doc = fitz.open(pdf_path)
    try:
        return len(doc)
    finally:
        doc.close()


def _ratio_penalty(attempt: ParseAttempt) -> float:
    """Sum of value/threshold ratios over an attempt's failures, for tie-breaks.

    Every criterion is oriented so that a bigger ratio is worse. SHATTERED
    contributes the SHARE ratio only (its value is the pair
    ``(short_unit_share_pct, chars_per_unit)``), because the share is the half
    that grows with the damage; chars-per-unit shrinks, so mixing it in would
    make a worse document score better.
    """
    total = 0.0
    for criterion, value, threshold in attempt.failures:
        if criterion == SHATTERED:
            share, limit = value[0], threshold[0]
            total += (share / limit) if limit else float("inf")
        elif criterion == EMPTY_TEXT:
            total += float("inf")          # nothing is worse than no text
        else:
            total += (value / threshold) if threshold else float("inf")
    return total


def select_attempt(attempts: list[ParseAttempt]) -> ParseAttempt | None:
    """First passing attempt, else the least-bad one that actually ran.

    "Least bad" is fewest failures, ties broken by the smaller summed
    value/threshold ratio. Skipped attempts are never selectable: they carry no
    text. Returns None if nothing ran.
    """
    ran = [a for a in attempts if a.skipped_reason is None]
    if not ran:
        return None
    for a in ran:
        if a.passed:
            return a
    return min(ran, key=lambda a: (len(a.failures), _ratio_penalty(a)))


def format_exclusion_detail(attempt: ParseAttempt) -> str:
    """Contract 5's operator-facing string for `papers.pdf_exclusion_detail`."""
    parts = []
    for criterion, value, threshold in attempt.failures:
        parts.append(f"{criterion}={value} (limit {threshold})")
    return "parse_quality: " + "; ".join(parts)


def _run_parser(name: str, pdf_path: str, vision_model: str) -> str:
    if name == "docling":
        return parse_with_docling(pdf_path)
    if name == "pymupdf":
        return parse_with_pymupdf(pdf_path)
    if name == "qwen2.5vl":
        return parse_with_vision(pdf_path, vision_model=vision_model)
    raise ValueError(f"unknown parser: {name}")


def _next_parser(failures, tried: set[str]) -> str | None:
    """First untried parser named by any failed criterion, in criterion order."""
    for criterion, _, _ in failures:
        for candidate in _REROUTE.get(criterion, ()):
            if candidate not in tried:
                return candidate
    return None


def parse_pdf(
    pdf_path: str,
    paper_id: int,
    review_name: str,
    db: ReviewDatabase,
    spec: ReviewSpec | None = None,
    force: bool = False,
) -> ParsedDocument:
    """Parse a PDF, judging every attempt and re-routing on a quality failure.

    `force=True` bypasses the same-hash short-circuit so a stored parse can be
    redone; it does not change any paper status. Status is the caller's business
    (`parse_all_pdfs` sets it, `reparse_papers` deliberately does not).
    """
    # Read thresholds from spec if available
    scanned_threshold = _SCANNED_THRESHOLD
    vision_model = _VISION_MODEL
    vision_max_pages = _VISION_MAX_PAGES
    thresholds = Thresholds()
    if spec and hasattr(spec, "pdf_parsing"):
        scanned_threshold = spec.pdf_parsing.scanned_text_threshold
        vision_model = spec.pdf_parsing.vision_model
        vision_max_pages = getattr(spec.pdf_parsing, "vision_max_pages", _VISION_MAX_PAGES)
        # The only construction path: engine defaults and spec defaults are pinned
        # equal by test, so this cannot silently diverge from Thresholds().
        thresholds = Thresholds.from_mapping(
            getattr(spec.pdf_parsing, "parse_quality", None))

    pdf_hash = compute_pdf_hash(pdf_path)

    # Check for existing parse with same hash
    existing = None if force else db._conn.execute(
        "SELECT parsed_text_version, parser_used FROM full_text_assets "
        "WHERE paper_id = ? AND pdf_hash = ? ORDER BY parsed_text_version DESC LIMIT 1",
        (paper_id, pdf_hash),
    ).fetchone()

    if existing:
        version = existing[0]
        # The stored parser, never the literal "docling": six papers in this
        # corpus are PyMuPDF output, and reporting them as docling would
        # misattribute the very parser identity a re-parse decision turns on.
        stored_parser = existing[1] or "docling"
        md_path = (
            Path(db.db_path).parent / "parsed_text" / f"{paper_id}_v{version}.md"
        )
        logger.info("Paper %d already parsed (v%d, same hash) — skipping", paper_id, version)
        return ParsedDocument(
            paper_id=paper_id,
            source_pdf_path=pdf_path,
            pdf_hash=pdf_hash,
            parsed_markdown=md_path.read_text() if md_path.exists() else "",
            parser_used=stored_parser,
            parsed_at=datetime.now(timezone.utc),
            version=version,
            accepted_parser=stored_parser,
        )

    # Determine version number
    last_version = db._conn.execute(
        "SELECT MAX(parsed_text_version) FROM full_text_assets WHERE paper_id = ?",
        (paper_id,),
    ).fetchone()[0]
    version = (last_version or 0) + 1

    # ── Attempt 1: the original route, unchanged ────────────────────
    # The sparse-output length cascade below is deliberately left in front of
    # the quality gate. It answers "did this parser return anything at all",
    # which is cheaper than segmentation and is the question that must be
    # settled first; the gate then asks whether what came back has structure.
    attempts: list[ParseAttempt] = []
    tried: set[str] = set()

    if is_scanned_pdf(pdf_path, threshold=scanned_threshold):
        logger.info("Paper %d: scanned PDF detected, using %s", paper_id, vision_model)
        started = time.monotonic()
        markdown = parse_with_vision(pdf_path, vision_model=vision_model)
        parser_used = "qwen2.5vl"
    else:
        logger.info("Paper %d: digital PDF, using Docling", paper_id)
        started = time.monotonic()
        try:
            markdown = parse_with_docling(pdf_path)
            parser_used = "docling"
        except Exception as exc:
            logger.warning(
                "Paper %d: Docling failed (%s), falling back to PyMuPDF",
                paper_id, exc,
            )
            markdown = parse_with_pymupdf(pdf_path)
            parser_used = "pymupdf"

        # If output is sparse, try PyMuPDF (if not already), then vision model
        if len(markdown.strip()) < scanned_threshold and parser_used == "docling":
            logger.warning(
                "Paper %d: Docling output sparse (%d chars), falling back to PyMuPDF",
                paper_id, len(markdown.strip()),
            )
            markdown = parse_with_pymupdf(pdf_path)
            parser_used = "pymupdf"

        if len(markdown.strip()) < scanned_threshold:
            logger.warning(
                "Paper %d: %s output sparse (%d chars), falling back to %s",
                paper_id, parser_used, len(markdown.strip()), vision_model,
            )
            markdown = parse_with_vision(pdf_path, vision_model=vision_model)
            parser_used = "qwen2.5vl"

    # ── Judge, and re-route while it fails ──────────────────────────
    texts: dict[str, str] = {}
    # An attempt that produced nothing usable can be judged and recorded -- it is
    # evidence the parser was tried -- but must never be SELECTED.
    #
    # Empty output is unusable always. Sparse output is unusable only on a
    # GATE-DRIVEN re-route, never on the initial route: the pre-existing cascade
    # deliberately accepts whatever vision returns as the last resort, and
    # narrowing that would change behaviour this task is not asked to change.
    # The re-route case is real -- on a scanned PDF, PyMuPDF returns nothing but
    # its `<!-- Page N -->` markers, non-empty and worthless, and storing page
    # markers as a paper is worse than refusing the parse.
    unusable: set[str] = set()
    while True:
        is_reroute = bool(attempts)
        tried.add(parser_used)
        texts[parser_used] = markdown
        stripped = markdown.strip()
        if not stripped or (is_reroute and len(stripped) < scanned_threshold):
            unusable.add(parser_used)
        verdict = assess(markdown, thresholds)
        attempts.append(ParseAttempt(
            attempt_index=len(attempts) + 1,
            parser_used=parser_used,
            passed=verdict.passed,
            failures=list(verdict.failures),
            metrics=dict(verdict.metrics),
            elapsed_s=round(time.monotonic() - started, 3),
        ))
        logger.info(
            "Paper %d attempt %d: parser=%s verdict=%s elapsed=%.2fs",
            paper_id, attempts[-1].attempt_index, parser_used,
            verdict.describe(), attempts[-1].elapsed_s,
        )
        if verdict.passed or len(attempts) >= _MAX_ATTEMPTS:
            break

        nxt = _next_parser(verdict.failures, tried)
        if nxt is None:
            break
        if nxt == "qwen2.5vl":
            pages = page_count(pdf_path)
            if pages > vision_max_pages:
                reason = (f"vision skipped: {pages} pages exceeds "
                          f"vision_max_pages={vision_max_pages}")
                attempts.append(ParseAttempt(
                    attempt_index=len(attempts) + 1,
                    parser_used="qwen2.5vl",
                    skipped_reason=reason,
                ))
                logger.warning("Paper %d attempt %d: %s",
                               paper_id, attempts[-1].attempt_index, reason)
                break

        started = time.monotonic()
        markdown = _run_parser(nxt, pdf_path, vision_model)
        parser_used = nxt

    # ── Accept: first pass, else least-bad ──────────────────────────
    accepted = select_attempt(
        [a for a in attempts if a.parser_used not in unusable])
    if accepted is None:
        raise ValueError(
            f"Paper {paper_id}: all parsers returned empty text — "
            "no file written, no DB row created"
        )
    accepted.accepted = True
    markdown = texts[accepted.parser_used]
    parser_used = accepted.parser_used

    # Belt and braces: selection already refuses sparse output, so reaching this
    # means the accepted text changed under us.
    if not markdown.strip():
        raise ValueError(
            f"Paper {paper_id}: all parsers returned empty text — "
            "no file written, no DB row created"
        )

    # Atomic write: temp file → DB commit → rename
    review_dir = Path(db.db_path).parent
    md_filename = f"{paper_id}_v{version}.md"
    md_path = review_dir / "parsed_text" / md_filename
    tmp_path = md_path.with_suffix(".md.tmp")

    try:
        tmp_path.write_text(markdown)

        # Record in database (with final path, not temp)
        now = datetime.now(timezone.utc).isoformat()
        db._conn.execute(
            """INSERT INTO full_text_assets
               (paper_id, pdf_path, pdf_hash, parsed_text_path, parsed_text_version,
                parser_used, parsed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (paper_id, pdf_path, pdf_hash, str(md_path), version, parser_used, now),
        )
        db._conn.execute(
            "UPDATE papers SET pdf_content_hash = ? WHERE id = ?",
            (pdf_hash, paper_id),
        )
        # The attempt ledger goes in BEFORE the one commit that already covers
        # the asset row and the hash update, so a crash anywhere in here leaves
        # neither the asset nor its attempts -- and never an asset whose
        # provenance is missing.
        for att in attempts:
            db._conn.execute(
                """INSERT INTO parse_attempts
                   (paper_id, pdf_hash, parsed_text_version, attempt_index,
                    parser_used, passed, failures, metrics, elapsed_s,
                    accepted, skipped_reason, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (paper_id, pdf_hash, version, att.attempt_index,
                 att.parser_used, int(att.passed),
                 json.dumps(att.failures), json.dumps(att.metrics),
                 att.elapsed_s, int(att.accepted), att.skipped_reason, now),
            )
        db._conn.commit()

        # DB committed — now rename temp to final (atomic on POSIX)
        tmp_path.rename(md_path)
    except Exception:
        # Clean up temp file on any failure
        if tmp_path.exists():
            tmp_path.unlink()
        db._conn.rollback()
        raise

    return ParsedDocument(
        paper_id=paper_id,
        source_pdf_path=pdf_path,
        pdf_hash=pdf_hash,
        parsed_markdown=markdown,
        parser_used=parser_used,
        parsed_at=datetime.now(timezone.utc),
        version=version,
        verdict={"passed": accepted.passed, "failures": accepted.failures},
        attempts=attempts,
        accepted_parser=parser_used,
    )


def parse_all_pdfs(db: ReviewDatabase, review_name: str) -> dict:
    """Parse all PDF_ACQUIRED papers. Returns stats dict."""
    papers = db.get_papers_by_status("PDF_ACQUIRED")
    total = len(papers)
    logger.info("Starting PDF parsing for %d papers", total)

    stats = {"parsed": 0, "skipped_existing": 0, "failed": 0, "quality_excluded": 0,
             "docling": 0, "pymupdf": 0, "qwen2.5vl": 0}
    review_dir = Path(db.db_path).parent

    for i, paper in enumerate(papers, 1):
        pid = paper["id"]
        pdf_dir = review_dir / "pdfs"

        # Find the PDF file — prefer DB path (works after rename), fall back to glob
        pdf_path = None

        # 1. Check full_text_assets.pdf_path (set by parser/verify_downloads)
        ft_row = db._conn.execute(
            "SELECT pdf_path FROM full_text_assets WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()
        if ft_row and ft_row["pdf_path"]:
            candidate = Path(ft_row["pdf_path"])
            if candidate.exists():
                pdf_path = str(candidate)
            elif not candidate.is_absolute():
                joined = pdf_dir / candidate
                if joined.exists():
                    pdf_path = str(joined)

        # 2. Check papers.pdf_local_path (set by downloader/verify_downloads)
        if not pdf_path:
            lp_row = db._conn.execute(
                "SELECT pdf_local_path FROM papers WHERE id = ?", (pid,),
            ).fetchone()
            if lp_row and lp_row["pdf_local_path"]:
                candidate = Path(lp_row["pdf_local_path"])
                if candidate.exists():
                    pdf_path = str(candidate)
                elif not candidate.is_absolute():
                    joined = pdf_dir / candidate
                    if joined.exists():
                        pdf_path = str(joined)

        # 3. Fall back to filesystem glob (handles bare integer and prefixed names)
        if not pdf_path:
            pdf_candidates = list(pdf_dir.glob(f"{pid}_*.pdf")) + list(
                pdf_dir.glob(f"{pid}.pdf")
            )
            if pdf_candidates:
                pdf_path = str(pdf_candidates[0])

        if not pdf_path:
            logger.warning("Paper %d: no PDF found in %s", pid, pdf_dir)
            stats["failed"] += 1
            continue
        try:
            result = parse_pdf(pdf_path, pid, review_name, db)

            stats[result.parser_used] = stats.get(result.parser_used, 0) + 1

            accepted = next((a for a in result.attempts if a.accepted), None)
            if accepted is not None and not accepted.passed:
                # The text is stored either way -- an excluded paper still needs
                # its parse on disk for audit. What changes is that a document
                # the gate could not repair no longer enters the corpus silently.
                detail = format_exclusion_detail(accepted)
                db._conn.execute(
                    """UPDATE papers
                       SET status = 'PDF_EXCLUDED',
                           pdf_exclusion_reason = ?,
                           pdf_exclusion_detail = ?,
                           updated_at = ?
                       WHERE id = ?""",
                    ("PARSE_QUALITY", detail,
                     datetime.now(timezone.utc).isoformat(), pid),
                )
                db._conn.commit()
                stats["quality_excluded"] = stats.get("quality_excluded", 0) + 1
                logger.warning("Paper %d: PDF_EXCLUDED — %s", pid, detail)
            else:
                db.update_status(pid, "PARSED")
                stats["parsed"] += 1
        except Exception as exc:
            logger.exception("Paper %d: parsing failed — %s", pid, exc)
            stats["failed"] += 1

        if i % 10 == 0 or i == total:
            logger.info("Parsed %d/%d papers", i, total)

    logger.info(
        "Parsing complete: %d parsed (%d docling, %d pymupdf, %d qwen2.5vl), %d skipped, %d failed",
        stats["parsed"],
        stats["docling"],
        stats["pymupdf"],
        stats["qwen2.5vl"],
        stats["skipped_existing"],
        stats["failed"],
    )
    return stats


def reparse_papers(
    db: ReviewDatabase,
    paper_ids: list[int],
    spec: ReviewSpec | None = None,
    force: bool = True,
) -> dict[int, dict]:
    """Re-run the parse cascade for specific papers, at any lifecycle status.

    **Never calls `update_status`.** A paper already in the corpus that re-parses
    badly is a decision for a person, not for a batch job: the corresponding
    concordance, extraction and audit rows already exist downstream, and silently
    excluding it here would strand them. This function produces the evidence; the
    ruling is a separate, human step.

    `force=True` bypasses the same-hash short-circuit, which is the point --
    without it a re-parse of an unchanged PDF returns the stored parse and
    measures nothing.

    Returns {paper_id: {accepted_parser, passed, failures}}; a paper that raised
    is reported with an `error` key rather than aborting the batch.
    """
    review_dir = Path(db.db_path).parent
    out: dict[int, dict] = {}

    for pid in paper_ids:
        row = db._conn.execute(
            "SELECT pdf_path FROM full_text_assets WHERE paper_id = ? "
            "ORDER BY id DESC LIMIT 1", (pid,),
        ).fetchone()
        pdf_path = row["pdf_path"] if row and row["pdf_path"] else None
        if pdf_path and not Path(pdf_path).exists():
            candidate = review_dir / "pdfs" / Path(pdf_path).name
            pdf_path = str(candidate) if candidate.exists() else None
        if not pdf_path:
            out[pid] = {"error": "no PDF on disk"}
            logger.warning("Paper %d: no PDF found — skipped", pid)
            continue

        try:
            result = parse_pdf(pdf_path, pid, review_dir.name, db, spec=spec,
                               force=force)
        except Exception as exc:
            out[pid] = {"error": str(exc)}
            logger.exception("Paper %d: re-parse failed — %s", pid, exc)
            continue

        accepted = next((a for a in result.attempts if a.accepted), None)
        out[pid] = {
            "accepted_parser": result.accepted_parser,
            "passed": bool(accepted.passed) if accepted else None,
            "failures": list(accepted.failures) if accepted else [],
            "version": result.version,
        }
        logger.info("Paper %d: re-parsed v%d via %s — %s", pid, result.version,
                    result.accepted_parser,
                    "PASS" if out[pid]["passed"] else "FAIL")

    return out


# ── Hash Verification ────────────────────────────────────────────────


def verify_hashes(db: ReviewDatabase) -> list[dict]:
    """Check all papers with stored pdf_content_hash against current PDF files.

    Returns a list of mismatch dicts: {paper_id, stored_hash, current_hash, pdf_path}.
    """
    rows = db._conn.execute(
        """SELECT p.id, p.pdf_content_hash, p.pdf_local_path,
                  fta.pdf_path AS fta_pdf_path
           FROM papers p
           LEFT JOIN full_text_assets fta ON fta.paper_id = p.id
           WHERE p.pdf_content_hash IS NOT NULL
           ORDER BY p.id"""
    ).fetchall()

    # Deduplicate by paper_id (take the first/most recent fta path)
    seen: set[int] = set()
    mismatches: list[dict] = []

    for row in rows:
        pid = row["id"]
        if pid in seen:
            continue
        seen.add(pid)

        stored_hash = row["pdf_content_hash"]

        # Resolve PDF path: prefer fta, then papers.pdf_local_path
        pdf_path = row["fta_pdf_path"] or row["pdf_local_path"]
        if not pdf_path:
            mismatches.append({
                "paper_id": pid,
                "stored_hash": stored_hash,
                "current_hash": None,
                "pdf_path": None,
            })
            continue

        current_hash = compute_pdf_hash(pdf_path)
        if current_hash != stored_hash:
            mismatches.append({
                "paper_id": pid,
                "stored_hash": stored_hash,
                "current_hash": current_hash,
                "pdf_path": pdf_path,
            })

    return mismatches


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="PDF parser utilities")
    parser.add_argument("--verify-hashes", action="store_true",
                        help="Check stored PDF hashes against current files")
    parser.add_argument("--review", required=True, help="Review name")
    args = parser.parse_args()

    if args.verify_hashes:
        db = ReviewDatabase(args.review)
        try:
            mismatches = verify_hashes(db)
            if not mismatches:
                print("All PDF content hashes match current files.")
            else:
                print(f"\n{len(mismatches)} PDF hash mismatch(es) found:\n")
                for m in mismatches:
                    status = "MISSING" if m["current_hash"] is None else "CHANGED"
                    print(f"  Paper {m['paper_id']:>5d}: {status}")
                    print(f"    Stored:  {m['stored_hash'][:16]}...")
                    current = m['current_hash'] or 'N/A'
                    print(f"    Current: {current[:16] + '...' if m['current_hash'] else current}")
                    print(f"    Path:    {m['pdf_path'] or 'not found'}")
                sys.exit(1)
        finally:
            db.close()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
