"""PDF quality check — first-page AI classification via vision model.

For each paper with a PDF on disk, extracts page 0 as PNG, sends to
a vision model via Ollama, and classifies language + content type.

Model, DPI, and timeout are read from the Review Spec's
pdf_quality_check section (falls back to defaults if not provided).

Results are stored in the papers table columns:
  pdf_ai_language, pdf_ai_content_type, pdf_ai_confidence,
  pdf_quality_check_status = 'AI_CHECKED'

CLI:
    python -m engine.acquisition.pdf_quality_check --review surgical_autonomy --spec review_specs/surgical_autonomy_v1.yaml
    python -m engine.acquisition.pdf_quality_check --review surgical_autonomy --dry-run
    python -m engine.acquisition.pdf_quality_check --review surgical_autonomy --limit 5
"""

import argparse
import base64
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.core.review_spec import PDFQualityCheck
from engine.utils.ollama_client import ollama_chat

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger(__name__)

DATA_ROOT = Path("data")
MAX_RETRIES = 1

# Defaults used when no Review Spec is provided
_DEFAULTS = PDFQualityCheck()

_CLASSIFICATION_PROMPT = """\
Look at this first page of a scientific PDF. Classify it as JSON:

{
  "language": "<primary language of the body text, e.g. English, Chinese, German>",
  "content_type": "<one of: full_manuscript, abstract_only, trial_registration, editorial_erratum, conference_poster, other>",
  "confidence": <float 0.0 to 1.0>,
  "note": "<brief note explaining classification, especially if not full_manuscript or not English>"
}

Rules:
- language: identify the primary language of the BODY TEXT (not just the title or abstract).
- content_type meanings:
  - full_manuscript: a complete research article, review, or technical paper
  - abstract_only: only an abstract or extended abstract, no full paper body
  - trial_registration: a clinical trial registry entry (e.g. ClinicalTrials.gov)
  - editorial_erratum: an editorial, erratum, corrigendum, retraction notice, or letter to the editor
  - conference_poster: a poster, slide deck, or conference presentation
  - other: anything else (book chapter TOC, table of contents, cover page only, etc.)
- confidence: how confident you are in both the language AND content_type classification
- Respond ONLY with the JSON object, no other text."""


def _render_first_page(pdf_path: str, dpi: int = 150) -> str:
    """Render page 0 of a PDF to a base64 PNG string."""
    doc = fitz.open(pdf_path)
    try:
        if len(doc) == 0:
            raise ValueError(f"PDF has 0 pages: {pdf_path}")
        page = doc[0]
        pix = page.get_pixmap(dpi=dpi)
        img_bytes = pix.tobytes("png")
        return base64.b64encode(img_bytes).decode()
    finally:
        doc.close()


def _classify_page(
    img_b64: str,
    model: str = "qwen2.5vl:7b",
    timeout: float = 120.0,
) -> dict:
    """Send first-page image to vision model and parse JSON classification."""
    for attempt in range(1 + MAX_RETRIES):
        try:
            response = ollama_chat(
                model=model,
                messages=[
                    {
                        "role": "user",
                        "content": _CLASSIFICATION_PROMPT,
                        "images": [img_b64],
                    }
                ],
                options={"temperature": 0},
                max_retries=0,  # retries handled by outer loop
                wall_timeout=timeout,
            )
            raw = response.message.content or ""

            # Strip markdown fences if present
            text = raw.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                if text.endswith("```"):
                    text = text[:-3]
                text = text.strip()

            result = json.loads(text)

            # Validate content_type
            valid_types = {
                "full_manuscript", "abstract_only", "trial_registration",
                "editorial_erratum", "conference_poster", "other",
            }
            ct = result.get("content_type", "other")
            if ct not in valid_types:
                result["content_type"] = "other"

            # Clamp confidence
            conf = result.get("confidence", 0.5)
            result["confidence"] = max(0.0, min(1.0, float(conf)))

            return result

        except Exception as exc:
            if attempt < MAX_RETRIES:
                logger.warning(
                    "Classification failed (attempt %d/%d): %s — retrying",
                    attempt + 1, 1 + MAX_RETRIES, exc,
                )
                time.sleep(5)
            else:
                raise


def _get_papers_to_check(conn: sqlite3.Connection, limit: int | None = None) -> list[dict]:
    """Find papers with PDFs on disk that haven't been quality-checked yet."""
    query = """
        SELECT p.id, p.ee_identifier, p.title,
               COALESCE(ft.pdf_path, p.pdf_local_path) AS pdf_path
        FROM papers p
        LEFT JOIN full_text_assets ft ON ft.paper_id = p.id
        WHERE (ft.pdf_path IS NOT NULL OR p.pdf_local_path IS NOT NULL)
          AND (p.pdf_quality_check_status IS NULL
               OR p.pdf_quality_check_status != 'AI_CHECKED')
          AND p.status NOT IN ('ABSTRACT_SCREENED_OUT', 'REJECTED', 'PDF_EXCLUDED')
        ORDER BY p.id
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    rows = conn.execute(query).fetchall()
    return [dict(r) for r in rows]


def run_quality_check(
    review_name: str,
    dry_run: bool = False,
    limit: int | None = None,
    config: PDFQualityCheck | None = None,
) -> dict:
    """Run PDF quality check on papers needing classification."""
    cfg = config or _DEFAULTS
    logger.info(
        "Using model=%s, dpi=%d, timeout=%ds",
        cfg.ai_model, cfg.dpi, cfg.timeout,
    )

    db_path = DATA_ROOT / review_name / "review.db"
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    papers = _get_papers_to_check(conn, limit=limit)
    logger.info("Found %d papers to check", len(papers))

    stats = {
        "total": len(papers),
        "checked": 0,
        "skipped_no_file": 0,
        "failed": 0,
        "results": [],
    }

    for i, paper in enumerate(papers):
        pid = paper["id"]
        ee = paper["ee_identifier"] or f"id={pid}"
        pdf_path = paper["pdf_path"]

        if not pdf_path or not Path(pdf_path).exists():
            logger.warning("  %s: PDF not on disk (%s) — skipping", ee, pdf_path)
            stats["skipped_no_file"] += 1
            continue

        logger.info(
            "[%d/%d] %s: %s", i + 1, stats["total"], ee, Path(pdf_path).name
        )

        try:
            img_b64 = _render_first_page(pdf_path, dpi=cfg.dpi)
            result = _classify_page(img_b64, model=cfg.ai_model, timeout=cfg.timeout)

            lang = result.get("language", "unknown")
            ctype = result.get("content_type", "other")
            conf = result.get("confidence", 0.0)
            note = result.get("note", "")

            logger.info(
                "  → language=%s  content_type=%s  confidence=%.2f  note=%s",
                lang, ctype, conf, note[:80],
            )

            record = {
                "paper_id": pid,
                "ee_identifier": ee,
                "language": lang,
                "content_type": ctype,
                "confidence": conf,
                "note": note,
            }
            stats["results"].append(record)

            if not dry_run:
                conn.execute(
                    """UPDATE papers
                       SET pdf_ai_language = ?,
                           pdf_ai_content_type = ?,
                           pdf_ai_confidence = ?,
                           pdf_quality_check_status = 'AI_CHECKED'
                       WHERE id = ?""",
                    (lang, ctype, conf, pid),
                )
                conn.commit()

            stats["checked"] += 1

        except Exception as exc:
            logger.exception("  %s: FAILED — %s", ee, exc)
            stats["failed"] += 1

    conn.close()
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="PDF quality check — first-page AI classification"
    )
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument("--spec", default=None, help="Path to Review Spec YAML")
    parser.add_argument("--dry-run", action="store_true", help="Report only, no DB writes")
    parser.add_argument("--limit", type=int, default=None, help="Process only N papers")
    args = parser.parse_args()

    config = None
    if args.spec:
        from engine.core.review_spec import load_review_spec
        spec = load_review_spec(args.spec)
        config = spec.pdf_quality_check

    stats = run_quality_check(
        review_name=args.review,
        dry_run=args.dry_run,
        limit=args.limit,
        config=config,
    )

    # ── Report ──
    print(f"\n{'=' * 60}")
    print("PDF QUALITY CHECK REPORT")
    print(f"{'=' * 60}")
    print(f"  Papers to check:   {stats['total']}")
    print(f"  Checked:           {stats['checked']}")
    print(f"  Skipped (no file): {stats['skipped_no_file']}")
    print(f"  Failed:            {stats['failed']}")
    if args.dry_run:
        print("  (DRY RUN — no changes written)")

    if stats["results"]:
        print(f"\n  {'EE-ID':<12} {'Language':<12} {'Content Type':<22} {'Conf':>5}  Note")
        print(f"  {'─' * 12} {'─' * 12} {'─' * 22} {'─' * 5}  {'─' * 40}")
        for r in stats["results"]:
            print(
                f"  {r['ee_identifier']:<12} {r['language']:<12} "
                f"{r['content_type']:<22} {r['confidence']:5.2f}  "
                f"{r['note'][:40]}"
            )

    # Summary by content type
    if stats["results"]:
        type_counts: dict[str, int] = {}
        for r in stats["results"]:
            ct = r["content_type"]
            type_counts[ct] = type_counts.get(ct, 0) + 1
        print(f"\n  Content type summary:")
        for ct, cnt in sorted(type_counts.items(), key=lambda x: -x[1]):
            print(f"    {ct:<25} {cnt}")

        # Language summary
        lang_counts: dict[str, int] = {}
        for r in stats["results"]:
            lang = r["language"]
            lang_counts[lang] = lang_counts.get(lang, 0) + 1
        print(f"\n  Language summary:")
        for lang, cnt in sorted(lang_counts.items(), key=lambda x: -x[1]):
            print(f"    {lang:<25} {cnt}")


if __name__ == "__main__":
    main()
