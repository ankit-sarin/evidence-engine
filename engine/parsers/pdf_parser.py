"""PDF-to-Markdown parser: Docling → PyMuPDF fallback → Qwen2.5-VL for scanned.

CLI:
    python -m engine.parsers.pdf_parser --verify-hashes --review surgical_autonomy
"""

import argparse
import base64
import hashlib
import json
import logging
import os
import sys
import tempfile
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
_OCR_ENGINE = "rapidocr"
_OCR_MAX_PAGES = 100
_MAX_ATTEMPTS = 5  # longest path: docling, docling_sanitized, pymupdf,
                   # docling_ocr, qwen2.5vl.
_ERROR_MSG_CHARS = 300

# Which parser to try next, by the defect that failed the current one. Ordered,
# and the order is a claim about causes rather than a preference.
#
# The deterministic OCR tier comes FIRST on every criterion. Every defect this
# gate can name -- glyph encoding, replacement characters, shattering, emptiness
# -- is a property of a text layer that is present but unusable, and re-OCR-ing
# the page image is the remedy that does not consult that layer at all. It is
# also ~6 s/page, byte-reproducible, and needs no model server, where the vision
# tier is a per-page model call that PARSE-GATE-03 watched run for 30 minutes on
# one page. Vision stays as the last resort behind it.
#
# EMPTY_TEXT keeps PyMuPDF in front: an empty Docling result is often a
# structural extraction failure over a perfectly good text layer, which PyMuPDF
# reads directly and instantly.
#
# The orders are deliberately kept as a TABLE rather than collapsed into one
# list, because they are expected to diverge: a hybrid page/region remedy for
# glyph-encoded HEADINGS over an intact body (p586's shape) belongs on
# GLYPH_DENSITY alone, and that distinction has nowhere to live in a single
# shared order.
_REROUTE: dict[str, tuple[str, ...]] = {
    GLYPH_DENSITY: ("docling_ocr", "qwen2.5vl"),
    REPLACEMENT_DENSITY: ("docling_ocr", "qwen2.5vl"),
    SHATTERED: ("docling_ocr", "qwen2.5vl"),
    EMPTY_TEXT: ("pymupdf", "docling_ocr", "qwen2.5vl"),
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


def _docling_converter(pipeline_options=None) -> DocumentConverter:
    """The single Docling converter construction site.

    Both Docling tiers differ only in their pipeline options, so they share this
    rather than each building their own -- a second construction would drift.
    """
    if pipeline_options is None:
        return DocumentConverter()
    from docling.datamodel.base_models import InputFormat
    from docling.document_converter import PdfFormatOption
    return DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
    )


def parse_with_docling(pdf_path: str) -> str:
    """Parse a digital PDF to Markdown using Docling's text layer."""
    result = _docling_converter().convert(pdf_path)
    return result.document.export_to_markdown()


def parse_with_docling_ocr(pdf_path: str, ocr_engine: str = _OCR_ENGINE) -> str:
    """Parse a PDF by OCR-ing every page, ignoring its text layer entirely.

    The remedy for a text layer that is present but WRONG -- glyph-encoded
    (p719: 5,472 GLYPH tokens over Caesar-shifted gibberish) or otherwise
    undecodable. Forcing full-page OCR is what makes it a remedy rather than a
    fallback: the corrupt layer is not consulted at all.

    Deterministic, unlike the vision tier: PARSE-GATE-05 measured byte-identical
    output across repeated runs, ~5.7-6.3 s/page on CPU. Its known cost is
    word-gluing (`long_token_share_pct`), which the gate does not judge.
    """
    if ocr_engine != "rapidocr":
        raise ValueError(
            f"unsupported ocr_engine {ocr_engine!r}: only 'rapidocr' is wired. "
            "It ships with docling and its models are already on disk; adding an "
            "engine means wiring it here, not just naming it in the spec."
        )
    from docling.datamodel.pipeline_options import PdfPipelineOptions, RapidOcrOptions

    po = PdfPipelineOptions()
    po.do_ocr = True
    # `force_full_page_ocr` is the 2.74.0 field; `OcrMode` does not exist here.
    po.ocr_options = RapidOcrOptions(force_full_page_ocr=True)
    po.generate_page_images = False
    result = _docling_converter(po).convert(pdf_path)
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


def error_reason(exc: BaseException) -> str:
    """The `skipped_reason` text for a parser that RAISED rather than returned.

    A raising parser produces a row like any other attempt, because "docling was
    tried and blew up" and "docling was never tried" are different facts and the
    ledger must be able to tell them apart. It goes in `skipped_reason` because
    the row carries no text and must never be selectable.
    """
    return f"error: {type(exc).__name__}: {str(exc)[:_ERROR_MSG_CHARS]}"


def strip_links_to_temp(pdf_path: str) -> tuple[str | None, str | None]:
    """Write a link-annotation-free copy of `pdf_path`. Returns (path, reason).

    Exactly one of the two is None. The copy exists to get PAST a parser that
    chokes on a malformed link while leaving the text layer untouched, so the
    text layer is VERIFIED rather than assumed: every page's `get_text()` must
    be byte-identical to the original, and if any page differs the copy is
    discarded and the reason returned. A "fix" that silently altered the text
    would be worse than the crash it works around.

    Why link stripping helps at all: docling validates a PDF's URI actions
    through `PdfHyperlink.uri: AnyUrl`, and a scheme-less URI -- a bare
    `dx.doi.org/10.1016/...`, which real publisher PDFs do carry -- fails
    pydantic validation and fails the WHOLE conversion, every page of it.
    PyMuPDF surfaces such a link with `uri=None` (which is why it is easy to
    miss when enumerating), but `delete_link` removes it regardless.
    """
    src = fitz.open(pdf_path)
    try:
        before = [pg.get_text("text") for pg in src]
    finally:
        src.close()

    fd, tmp = tempfile.mkstemp(prefix="parse_sanitized_", suffix=".pdf")
    os.close(fd)
    doc = fitz.open(pdf_path)
    try:
        removed = 0
        for pg in doc:
            for _ in range(len(pg.get_links())):
                pg.delete_link(pg.get_links()[0])
                removed += 1
        doc.save(tmp, garbage=3, deflate=True)
    finally:
        doc.close()

    chk = fitz.open(tmp)
    try:
        after = [pg.get_text("text") for pg in chk]
    finally:
        chk.close()

    if after != before:
        Path(tmp).unlink(missing_ok=True)
        differing = [i + 1 for i, (a, b) in enumerate(zip(before, after)) if a != b]
        return None, (
            "sanitized retry skipped: link stripping changed the text layer on "
            f"page(s) {differing or 'unknown'}"
        )
    if removed == 0:
        Path(tmp).unlink(missing_ok=True)
        return None, "sanitized retry skipped: the PDF carries no link annotations"
    return tmp, None


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


def _insert_attempts(db, paper_id, pdf_hash, version, attempts, now) -> None:
    """INSERT the attempt rows. Does NOT commit -- the caller owns the transaction."""
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


def _commit_attempts(db, paper_id, pdf_hash, version, attempts) -> None:
    """Commit attempt rows with no asset row, for a parse that produced nothing.

    Never raises: a bookkeeping failure must not mask the parse failure the
    caller is about to report.
    """
    if not attempts:
        return
    try:
        _insert_attempts(db, paper_id, pdf_hash, version, attempts,
                         datetime.now(timezone.utc).isoformat())
        db._conn.commit()
    except Exception:
        logger.exception("Paper %d: could not record attempts for a failed parse",
                         paper_id)
        try:
            db._conn.rollback()
        except Exception:
            pass


def _run_parser(name: str, pdf_path: str, vision_model: str,
                ocr_engine: str = _OCR_ENGINE) -> str:
    if name == "docling":
        return parse_with_docling(pdf_path)
    if name in ("docling_ocr", "docling_ocr_sanitized"):
        return parse_with_docling_ocr(pdf_path, ocr_engine=ocr_engine)
    if name == "pymupdf":
        return parse_with_pymupdf(pdf_path)
    if name == "qwen2.5vl":
        return parse_with_vision(pdf_path, vision_model=vision_model)
    raise ValueError(f"unknown parser: {name}")


#: `docling_ocr_sanitized` is the same TIER as `docling_ocr`, run against the
#: link-stripped copy; it is a provenance label, not a second parser, so trying
#: either marks both as tried.
_TIER_ALIASES = {"docling_ocr_sanitized": "docling_ocr",
                 "docling_sanitized": "docling"}


def _next_parser(failures, tried: set[str]) -> str | None:
    """First untried parser named by any failed criterion, in criterion order."""
    seen = {_TIER_ALIASES.get(t, t) for t in tried}
    for criterion, _, _ in failures:
        for candidate in _REROUTE.get(criterion, ()):
            if _TIER_ALIASES.get(candidate, candidate) not in seen:
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
    ocr_engine = _OCR_ENGINE
    ocr_max_pages = _OCR_MAX_PAGES
    thresholds = Thresholds()
    if spec and hasattr(spec, "pdf_parsing"):
        scanned_threshold = spec.pdf_parsing.scanned_text_threshold
        vision_model = spec.pdf_parsing.vision_model
        vision_max_pages = getattr(spec.pdf_parsing, "vision_max_pages", _VISION_MAX_PAGES)
        ocr_engine = getattr(spec.pdf_parsing, "ocr_engine", _OCR_ENGINE)
        ocr_max_pages = getattr(spec.pdf_parsing, "ocr_max_pages", _OCR_MAX_PAGES)
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

    attempts: list[ParseAttempt] = []
    sanitized_path: str | None = None

    # Contract 2: the sanitized copy may be read by more than one attempt, so
    # its lifetime is the CALL, not the attempt -- and it must never outlive
    # the call, on any exit path.
    try:
        # ── Attempt 1: the original route, unchanged ────────────────────
        # The sparse-output length cascade below is deliberately left in front of
        # the quality gate. It answers "did this parser return anything at all",
        # which is cheaper than segmentation and is the question that must be
        # settled first; the gate then asks whether what came back has structure.
        tried: set[str] = set()

        def _record_error(name: str, exc: BaseException, t0: float) -> None:
            """A parser that raised is an attempt, not a silence."""
            attempts.append(ParseAttempt(
                attempt_index=len(attempts) + 1,
                parser_used=name,
                skipped_reason=error_reason(exc),
                elapsed_s=round(time.monotonic() - t0, 3),
            ))
            logger.warning("Paper %d attempt %d: parser=%s RAISED %s",
                           paper_id, attempts[-1].attempt_index, name, error_reason(exc))

        def _record_skip(name: str, reason: str) -> None:
            attempts.append(ParseAttempt(
                attempt_index=len(attempts) + 1, parser_used=name, skipped_reason=reason))
            logger.warning("Paper %d attempt %d: %s",
                           paper_id, attempts[-1].attempt_index, reason)

        if is_scanned_pdf(pdf_path, threshold=scanned_threshold):
            # PARSE-GATE-06b: a scanned page goes to the DETERMINISTIC OCR tier
            # first. It was the vision model, which is a per-page model call with a
            # 30-minute observed worst case; docling_ocr is ~6 s/page, reproducible,
            # and needs no model server. Vision remains available as a re-route.
            logger.info("Paper %d: scanned PDF detected, using docling_ocr", paper_id)
            started = time.monotonic()
            markdown = None
            pages = page_count(pdf_path)
            if pages > ocr_max_pages:
                _record_skip("docling_ocr",
                             f"ocr skipped: {pages} pages exceeds ocr_max_pages={ocr_max_pages}")
            else:
                try:
                    markdown = parse_with_docling_ocr(pdf_path, ocr_engine=ocr_engine)
                    parser_used = "docling_ocr"
                    if len(markdown.strip()) < scanned_threshold:
                        # OCR ran and found (almost) nothing. On a scanned page
                        # that is a failure of the tier, not a property of the
                        # document, so fall through to vision exactly as the
                        # digital branch falls through on sparse output.
                        _record_skip(
                            "docling_ocr",
                            f"ocr output sparse: {len(markdown.strip())} chars "
                            f"below scanned_text_threshold={scanned_threshold}")
                        markdown = None
                except Exception as exc:
                    _record_error("docling_ocr", exc, started)

            if markdown is None:
                started = time.monotonic()
                try:
                    markdown = parse_with_vision(pdf_path, vision_model=vision_model)
                    parser_used = "qwen2.5vl"
                except Exception as exc:
                    _record_error("qwen2.5vl", exc, started)
                    raise
        else:
            logger.info("Paper %d: digital PDF, using Docling", paper_id)
            started = time.monotonic()
            markdown = None
            try:
                markdown = parse_with_docling(pdf_path)
                parser_used = "docling"
            except Exception as exc:
                # PARSE-GATE-06a: a Docling crash used to be swallowed by this
                # `except` and answered with PyMuPDF, whose naive extraction shattered
                # p455 into 8,394 units of 7.1 chars. The crash is now recorded, and
                # the FIRST response is to retry Docling on a copy with the offending
                # link annotations removed -- which recovers that paper completely
                # (52,403 chars, gate PASS) rather than degrading it.
                _record_error("docling", exc, started)
                tmp, why = strip_links_to_temp(pdf_path)
                if tmp is None:
                    _record_skip("docling_sanitized", why)
                else:
                    # Contract 2: the copy outlives this attempt. A later Docling
                    # tier (docling_ocr) in the SAME call must read the sanitized
                    # copy too -- re-running the OCR pipeline against the original
                    # would just hit the same crash. parse_pdf's outer `finally`
                    # unlinks it; it never outlives the call.
                    sanitized_path = tmp
                    started = time.monotonic()
                    try:
                        markdown = parse_with_docling(tmp)
                        parser_used = "docling_sanitized"
                        logger.info("Paper %d: sanitized Docling retry succeeded", paper_id)
                    except Exception as exc2:
                        _record_error("docling_sanitized", exc2, started)

                if markdown is None:
                    started = time.monotonic()
                    try:
                        markdown = parse_with_pymupdf(pdf_path)
                        parser_used = "pymupdf"
                    except Exception as exc3:
                        _record_error("pymupdf", exc3, started)
                        raise

            # If output is sparse, try PyMuPDF (if not already), then vision model
            if (len(markdown.strip()) < scanned_threshold
                    and parser_used in ("docling", "docling_sanitized")):
                logger.warning(
                    "Paper %d: %s output sparse (%d chars), falling back to PyMuPDF",
                    paper_id, parser_used, len(markdown.strip()),
                )
                started = time.monotonic()
                try:
                    markdown = parse_with_pymupdf(pdf_path)
                    parser_used = "pymupdf"
                except Exception as exc:
                    _record_error("pymupdf", exc, started)
                    raise

            if len(markdown.strip()) < scanned_threshold:
                logger.warning(
                    "Paper %d: %s output sparse (%d chars), falling back to %s",
                    paper_id, parser_used, len(markdown.strip()), vision_model,
                )
                started = time.monotonic()
                try:
                    markdown = parse_with_vision(pdf_path, vision_model=vision_model)
                    parser_used = "qwen2.5vl"
                except Exception as exc:
                    _record_error("qwen2.5vl", exc, started)
                    raise

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
        judged = 0
        while True:
            # A re-route is a JUDGED attempt that failed the gate and sent us
            # round again -- not merely "some row exists". Error and skip rows
            # (PARSE-GATE-06a/06b) carry no verdict and now precede the first
            # judged attempt on the scanned branch, so counting rows here would
            # apply re-route sparse semantics to the INITIAL route.
            is_reroute = judged > 0
            tried.add(parser_used)
            texts[parser_used] = markdown
            stripped = markdown.strip()
            if not stripped or (is_reroute and len(stripped) < scanned_threshold):
                unusable.add(parser_used)
            verdict = assess(markdown, thresholds)
            judged += 1
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

            # Choose the next parser, stepping over any that a page cap rules
            # out. A cap is not a dead end -- it removes ONE candidate, and the
            # criterion's remaining order still applies. This is a selection
            # loop, not a re-entry of the judge loop: re-entering would re-assess
            # the same text and append a duplicate verdict row.
            nxt = None
            target_path = pdf_path
            while True:
                cand = _next_parser(verdict.failures, tried)
                if cand is None:
                    break
                if cand in ("qwen2.5vl", "docling_ocr"):
                    cap = vision_max_pages if cand == "qwen2.5vl" else ocr_max_pages
                    label = "vision" if cand == "qwen2.5vl" else "ocr"
                    setting = ("vision_max_pages" if cand == "qwen2.5vl"
                               else "ocr_max_pages")
                    pages = page_count(pdf_path)
                    if pages > cap:
                        _record_skip(cand, f"{label} skipped: {pages} pages exceeds "
                                           f"{setting}={cap}")
                        tried.add(cand)
                        continue
                if cand == "docling_ocr" and sanitized_path is not None:
                    # The original crashed Docling; the OCR pipeline is the same
                    # Docling. Feed it the copy that got past the crash.
                    target_path = sanitized_path
                    cand = "docling_ocr_sanitized"
                nxt = cand
                break

            if nxt is None:
                break

            started = time.monotonic()
            try:
                markdown = _run_parser(nxt, target_path, vision_model, ocr_engine=ocr_engine)
            except Exception as exc:
                _record_error(nxt, exc, started)
                break
            parser_used = nxt

        # ── Accept: first pass, else least-bad ──────────────────────────
        accepted = select_attempt(
            [a for a in attempts if a.parser_used not in unusable])
        if accepted is None:
            # Contract 7: a parse that fails ENTIRELY is the one most worth a record,
            # and until now it was the only outcome that left none -- the rows were
            # written inside the asset transaction, which never ran. Commit them on
            # their own, against the version this attempt would have taken, with no
            # asset row (parse_attempts has no FK to full_text_assets), then fail.
            _commit_attempts(db, paper_id, pdf_hash, version, attempts)
            raise ValueError(
                f"Paper {paper_id}: all parsers returned empty text — "
                f"{len(attempts)} attempt(s) recorded, no file written, "
                "no asset row created"
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
            _insert_attempts(db, paper_id, pdf_hash, version, attempts, now)
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
    finally:
        if sanitized_path is not None:
            Path(sanitized_path).unlink(missing_ok=True)


def parse_all_pdfs(db: ReviewDatabase, review_name: str) -> dict:
    """Parse all PDF_ACQUIRED papers. Returns stats dict."""
    papers = db.get_papers_by_status("PDF_ACQUIRED")
    total = len(papers)
    logger.info("Starting PDF parsing for %d papers", total)

    stats = {"parsed": 0, "skipped_existing": 0, "failed": 0, "quality_excluded": 0,
             "docling": 0, "docling_sanitized": 0, "pymupdf": 0, "qwen2.5vl": 0}
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
        "Parsing complete: %d parsed (%d docling, %d docling_sanitized, %d pymupdf, "
        "%d qwen2.5vl), %d skipped, %d failed",
        stats["parsed"],
        stats["docling"],
        stats["docling_sanitized"],
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
