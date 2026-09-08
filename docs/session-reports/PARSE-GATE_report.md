# PARSE-GATE — lane report (readouts 00 through 07)

**Lane:** PARSE-GATE — building, wiring, bounding and running a parse-quality gate for the
surgical_autonomy corpus.
**Session:** `00d781e5-7b67-47b3-93b3-1ce18e5d3bfc`, archived at
`~/claude-session-archive/00d781e5-7b67-47b3-93b3-1ce18e5d3bfc.jsonl` (10,038,099 B, 2,035
lines, zero unparseable lines).
**Date range:** 2026-09-07 21:04 UTC (PARSE-GATE-00 brief) to 2026-09-08 03:55 UTC (final
closeout). All timestamps in this report are UTC.
**Commit range (lane proper):** `167d481` … `5e58020` — six commits.
**HEAD at close:** `5e580205197fa30d2f7b4facea8c34bdf38a4027`, pushed, `origin/main` level.
**Suite:** 1,714 tests before the session, 1,734 after CORPUS-PRED-01, **1,851 at close**, all
at `-m "not network and not ollama and not integration"`.

Two commits inside the wider range belong to earlier tasks in the same session and are **not**
parse-gate work:

* `377b4ec` — `docs(session-reports): VERIFY-EXIT-01 close-state verification ledger`, from the
  VERIFY-EXIT-01 (elicit-design-02) close. Its own report is
  `docs/session-reports/VERIFY-EXIT-01_report.md`.
* `8388a1d` — `fix(corpus): single corpus-eligibility authority; retire three inline copies`,
  from CORPUS-PRED-01. Recorded in §7 of this report, because it has no committed report of its
  own.

*Compiled from transcript `00d781e5-7b67-47b3-93b3-1ce18e5d3bfc` and `primer.md` by session
`bf144ebb-265b-48a5-af21-f2a5719c6b4b` on 2026-09-08. Every readout section is sourced to the
transcript; no section required a primer fallback.*

---

## 1. Purpose of the lane

Four papers in the corpus had been parsed into text so damaged that no downstream extraction
could be trusted, and nothing in the pipeline noticed — the only check before a paper was
marked PARSED was that the text was not completely empty. This lane built a measurement that
can tell a good parse from a broken one, wired it into the parser so that a failing parse is
automatically retried through a different route, and bounded the routes that could hang or
crash. It then re-parsed the four papers and measured whether the new text is actually better
than the old, which for two of the four turned out to be the wrong question to answer
automatically.

---

## 2. The eleven readouts, in execution order

### PARSE-GATE-00 — read-out of the parse path

*Source: transcript line 479, 2026-09-07T21:08:46Z. Brief at line 352.*

**Asked:** describe the existing parse path, the routing rule, where segmentation happens,
where the PARSED status is committed, whether a gate needs a new lifecycle status, and whether
a small fixture can reproduce the p455 damage. Read-only; design nothing.

**Found.** The whole parse path is `engine/parsers/pdf_parser.py`; there is no separate ingest
module. The only quality check that existed before a paper became PARSED was an empty-text
`ValueError` at `pdf_parser.py:196`, and `db.update_status(pid, "PARSED")` at `:309` was
unconditional on any quality property. Routing was a single density heuristic —
`is_scanned_pdf` compares `total_chars / num_pages` against a threshold of 100 — and the
vision route, though fully wired, **had never executed on this corpus**: the `parser_used`
distribution over `full_text_assets` was `docling 784 · pymupdf 6 · qwen2.5vl 0`. Segmentation
units are never stored anywhere; all fourteen Phase-1 metrics therefore require the raw
Markdown file. No new lifecycle status is needed: `PDF_ACQUIRED → PDF_EXCLUDED` already exists
and is terminal, and `PARSED → FT_FLAGGED` gives a reversible alternative.

The four flagged papers measured, from `data/surgical_autonomy/eval/parse01/sweep.jsonl`:
p415 1,771,635 chars; p719 279,389 chars at 1,447.6 chars/unit with 5,472 glyph artifacts;
p586 69,214 chars with 542 glyph artifacts; **p455 59,964 chars at 7.1 chars/unit with a 98.8%
short-unit share** and zero glyph artifacts.

**Two findings that shaped everything after.** First, p455 was parsed by **PyMuPDF, not
Docling** (`full_text_assets` id 164, `parser_used='pymupdf'`, v2, 2026-03-14) — so the
shattering was a fallback-parser artifact, and the gate would be catching a Docling-failure
path rather than a bad PDF. Second, all six PyMuPDF papers hold the corpus's worst
segmentation (short-unit shares 34.3, 35.6, 39.9, 43.7 and 98.8 against a corpus median of
16.7), making the parser's identity nearly a free signal the gate ignores.

A 500-byte excerpt of p455 already reproduces the signature (85.5% short-unit share from the
head, 89.2% from the midpoint), against 0.0% for the length-matched clean control p561, so
fixtures are feasible and excerpt choice is not delicate.

**Commits:** none from this readout. It pushed the two commits already standing
(`dfe8cce..8388a1d`).

**Deviations:** none. `flag.py` was not run because it writes `flagged.jsonl`; the outlier
fences were recomputed read-only instead.

---

### PARSE-GATE-01 — parse-quality metrics and verdict module

*Source: transcript line 569, 2026-09-07T21:31:01Z. Brief at line 489.*

**Asked:** build a pure module that computes the Phase-1 metrics and returns an absolute
PASS/FAIL verdict, with thresholds overridable from the Review Spec. No wiring.

**Shipped.** `engine/parsers/parse_quality.py` — `compute_metrics(text)` returning the
seventeen Phase-1 metric names verbatim plus exactly two additions, `glyph_density_per_kchar`
(which makes p586's 542 artifacts in 69 KB comparable to p699's 419 in 48 KB) and `is_empty`;
and `assess(text, thresholds)` returning `passed`, a `failures` tuple of
`(criterion, value, threshold)` triples over a closed vocabulary, and the full metrics
mapping. No I/O, no database, no model calls. Thirty-two new tests, of which T7 reproduces
**all seventeen metrics exactly against `sweep.jsonl`** for nine papers — and passed on the
first run, so the semantic copy of the original definitions is exact.

Provisional defaults `short_unit_share_pct_max=50.0 · chars_per_unit_min=20.0 ·
glyph_density_per_kchar_max=5.0 · replacement_chars_max=0` failed **7 of 190** papers: 455,
562, 719, 262, 699, 532, 586.

**Deviations, both reported rather than adapted.** (1) The brief asked for Review-Spec
overrides while the diff allow-list excluded `review_spec.py`; resolved by implementing
`Thresholds.from_mapping()`, which accepts a mapping today and a pydantic object unchanged
once PARSE-GATE-02 adds the field — both tested. (2) The brief said "the three PyMuPDF papers
at 34–44%"; there are **four** (368 at 35.6, 378 at 39.9, 748 at 34.3, 780 at 43.7), and all
four were excerpted.

**Commit:** `167d481` — `feat(parse): parse-quality metrics and absolute PASS/FAIL verdict
module`. Suite 1,766 passed, 15 deselected (1,734 + 32).

---

### PARSE-GATE-01b — applying the threshold ruling

*Source: transcript line 655, 2026-09-07T21:59:06Z. Brief at line 573.*

**Asked:** apply the architect's threshold ruling — make the two segmentation criteria one
conjunctive criterion, replace the zero-tolerance replacement-character limit with a density,
and add ligature-escape telemetry.

**Shipped.** `SHORT_UNIT_SHARE` and `CHARS_PER_UNIT` retired as separate criteria and replaced
by one `SHATTERED` that fires only when the short-unit share is high **and** chars-per-unit is
low; `replacement_chars_max` replaced by `replacement_density_per_kchar_max = 1.0`;
`uni_escape_count` added as telemetry that is computed, returned and never judged. Forty-five
tests total, thirteen net new.

**The ruling's prediction held without tuning: exactly `{455, 586, 699, 719}` fail, 4 of 190.**
p455 on `SHATTERED=(98.8, 7.1)`; 586, 699 and 719 on `GLYPH_DENSITY` at 7.831, 8.631 and
19.586 against a limit of 5.0. The conjunction's margin is wide — only p562 has *either* half
true (61.9% short-unit share at 46.8 chars/unit) and it now passes. Both replacement-character
papers pass at 0.192 and 0.157 per kchar, five to six times below the limit.

A corpus survey before touching the regex found **2,546 real `/uni` escapes across 32 files,
every one four uppercase hex digits**, no other arity; a wider pattern was therefore not
needed, and the six non-escape `/uni` occurrences (`academic/university-affiliated`,
`gitlab.com/altairLab/unityflexml`, and four similar) are correctly ignored.

**Deviations:** none. One real bug was caught while writing fixtures: a hand-written "wrapped
text" fixture asserted a short-unit share that pysbd does not produce, because **pysbd does not
split on newlines**; the fixture was rebuilt to p562's actual shape.

**Commit:** `c1b2499` — `fix(parse): conjunctive shattering criterion, replacement density,
uni telemetry`. Suite 1,779 passed, 15 deselected (1,766 + 13).

---

### PARSE-GATE-02 — wiring the gate into the cascade

*Source: transcript line 834, 2026-09-07T22:46:41Z. Brief at line 659.*

**Asked:** wire the verdict into the parse cascade, add a per-attempt ledger table, add the
Review-Spec fields, and provide a targeted re-parse entry point.

**Shipped.** The pre-existing sparse-output check stays **in front of** the gate — it answers
"did this parser return anything at all", which is cheaper than segmentation. After it,
`assess()` judges; on failure `_next_parser` consults a `_REROUTE` table, skipping anything
already tried, capped at three attempts. `select_attempt` takes the first pass, else the
fewest failures with a summed value-over-threshold tie-break in which `SHATTERED` contributes
its share ratio only, because chars-per-unit *shrinks* as damage grows and including it would
score a worse document better. `parse_attempts` joins the schema as the twelfth table, its
rows inserted **before** the single commit that already covers the asset row and the hash
update, so a failure anywhere leaves no asset row, no attempt rows and no renamed file. The
new entry point `reparse_papers(db, paper_ids, spec=None, force=True)` runs the same loop with
the hash short-circuit bypassed and **never calls `update_status`** — a corpus paper that
re-parses badly already has extraction, concordance and audit rows downstream, and excluding
it in a batch job would strand them.

Twenty-five new tests. **One real defect surfaced while wiring:** `EMPTY_TEXT` re-routing
resurrected PyMuPDF on a scanned PDF whose entire output is `<!-- Page N -->` markers —
non-empty and worthless — so an existing test stopped raising. Sparse output from a
gate-driven re-route is now recorded but never selectable; the first fix was too broad and
broke three further tests, and the scoped version passes all twenty-one.

**Deviations:** the diff allow-list was widened by ruling (D1) to include `database.py`,
`review_spec.py` and `models.py`.

**Commit:** `3fdaa1e` — `feat(parse): quality-gated parse cascade, attempt ledger, targeted
re-parse`. Suite 1,804 passed, 15 deselected (1,779 + 25).

**A prediction recorded here that later came true.** The readout warned that the next time
*any* tool opened the live `review.db`, `executescript(_SCHEMA)` would create `parse_attempts`
and change the file's mtime — "a real schema change to the live corpus database that will
happen on first contact, not on a deliberate migration step". It did not happen in this task
(mtime `2026-07-27 19:47:48.448294416`, size 99,753,984, unchanged). It happened in
PARSE-GATE-03, and completed in PARSE-GATE-06c.

---

### PARSE-GATE-03 — smoke re-parse of p455 — **ABORTED**

*Source: transcript line 1022, 2026-09-07T23:27:07Z. Brief at line 838.*

**Asked:** run the first real re-parse of p455 through the gated cascade.

**What happened.** A verified online backup was taken first:
`data/backups/review_pre_PARSE-GATE-03_20260907T230211Z.db`, 99,753,984 B,
`integrity_check: ok`, 10,039 papers, 23 tables, sha256
`71a7d59e4985f3747c86cb0de61c1b92868680c627b56cbc21fe09bf48f3b2fc` — the same value the
file carries on disk today.

**The gate worked exactly as designed.** Docling failed on all twelve pages with a
`PdfHyperlink` validation error; PyMuPDF answered and reproduced the original measurement to
the decimal — `SHATTERED=(98.8, 7.1)`, 2.67 s — and `SHATTERED` routed to vision only, as
`_REROUTE` then specified. Page 1 transcribed in 33.4 s. **Page 2 did not return.** Two
consecutive wall-clock timeouts were logged at `elapsed=600s, limit=600s`, while the server's
own log showed the request running **15m00s**. The third attempt was already running and would
have reached `_restart_ollama_and_retry` at 23:33:34 by the readout's own estimate — an explicit abort condition and
on the do-not-touch list — so the run was killed with nine minutes of margin. `NRestarts=0`,
`ExecMainStartTimestamp` unchanged: **no restart occurred.**

**Two defects diagnosed and reported rather than fixed.** (a) `parse_with_vision` passed only
`{"temperature": 0}` — no output cap — so with `KvSize=128000` the model could generate until
context exhaustion; page 2 is the densest text page in the document and the only one of the
first three with no figures. (b) `_DEFAULT_WALL_TIMEOUT = 600.0` but `_HTTP_READ_TIMEOUT =
900.0`, so the wall clock does not bound the request and the log understates each attempt by
50%; the worst case per bad page is three attempts of 900 s and then a service restart.

**Database state after the abort.** No data lost, nothing partially written — the run died long
before the write block. Every table row count identical to baseline; p455 still
`AI_AUDIT_COMPLETE`; no v3 row; `455_v2.md` sha256 unchanged; `parse_attempts` existing with
zero rows. **One precise caveat the readout insisted on:** the main file was byte-identical
only because the schema change sat in an uncheckpointed WAL of 24,752 B. "review.db unchanged"
was true of the *file* and not of the *database*.

**Commits:** none.

**Recommendation carried forward:** do not run 586/699/719 as-is; cap vision output, reconcile
the timeouts, then re-smoke p455 alone.

---

### PARSE-GATE-04 — vision-route diagnosis

*Source: transcript line 1187, 2026-09-08T00:06:34Z. Brief at line 1026.*

**Asked:** find out why page 2 hung, whether an output cap is honoured, what OCR capability
exists on the box, and whether the wall-clock timeout can be made binding.

**The loop was observed directly.** Four bounded probes ran under
`EVIDENCE_ENGINE_NO_OLLAMA_RESTART=1`:

| probe | page | change | done_reason | eval_count | wall | looped |
|---|---:|---|---|---:|---:|---|
| A | 2 | temperature 0, cap 2048 | `length` | 2048 | 64.7 s | **yes** |
| B | 2 | temperature 0.2, seed, presence_penalty 1.5 | `stop` | 1246 | 30.2 s | no |
| C | 1 | temperature 0, cap 2048 | `stop` | 724 | 28.5 s | no |
| D | 2 | temperature 0, cap 2048, **minimal prompt** | `stop` | 1139 | 38.3 s | no |

Probe A's output begins as an accurate transcription and then collapses into a **211-character
cycle** repeated to the cap. At the measured 42.6 tokens/s an uncapped run to the
128,000-token context is **50 minutes**, which matches PARSE-GATE-03's 900 s producing no
completion.

**Probe D is the decisive result: only the prompt changed, at temperature 0, and the loop
stopped.** The failing instruction — "Extract all text from this page. Preserve tables,
headings, and formatting. Output as Markdown." — asks for reformatting on a page of plain
two-column prose with no tables and no figures. Probe C confirms a cap is safe: a good page
stopped at 724 of 2,048 tokens.

**OCR inventory.** `tesseract` absent; PyMuPDF's OCR entry point raises without it;
**`rapidocr` present and already used by docling**, with its three PP-OCRv4 ONNX models on
disk. A classical OCR path existed and needed no install.

**The timeout stack.** `MODEL_TIMEOUTS` is keyed by regex on the model name, so
`qwen2.5vl:7b` matches nothing and falls to the 600 s default while `deepseek-r1:32b` gets
900 s. The watchdog abandons the worker thread without cancelling the HTTP request, and with
one runner slot the retry **queues behind the abandoned generation that still owns it** — so
retries structurally cannot succeed. Real local calls do exceed 600 s (CAPTURE-01, n=40:
median 212.3 s, p99 768.2 s, max 817.5 s), but all of those are `deepseek-r1:32b`, which
already resolves to 900 s; giving the vision model its own entry changes only the vision path.

**A proposed loop detector was tested and rejected.** Unique-over-nonblank lines scores the
looped output **1.0000** — better than the clean control paper at 0.8543 — because the
repetition is entirely within a single line. `done_reason == "length"` is the free, exact
signal instead.

**Commits:** none.

**Deviation resolved in the readout:** the brief's "never issue a fourth call" could not mean a
literal four-call budget, since verification plus four probes is five; it was read as "no retry
of a failed probe", one call per probe, and flagged.

**Event:** the session ran `/wrap` after this readout and printed a closeout block at
transcript line 1259, 2026-09-08T00:11:14Z, at HEAD `3fdaa1e`. Work then resumed in the same
session.

---

### PARSE-GATE-05 — classical-OCR tier probes

*Source: transcript line 1420, 2026-09-08T02:09:20Z. Brief at line 1270.*

**Asked:** probe a docling+rapidocr tier on p455 and p719, check whether stripping the
hyperlink lets docling run, and measure fidelity and determinism.

**🔴 The lane's premise concerning p455 was reversed here, and the readout said so in those words:
"The central premise did not survive contact with disk."** The brief expected p455 to come
back SHATTERED because its text layer was thought broken at source. **False.** Once docling
can run at all, p455 parses cleanly and passes the gate with no OCR whatsoever — 52,403 chars,
13.4% short-unit share, 113.2 chars/unit, 20.6 s. p455's text layer was never irrecoverable:
PyMuPDF's naive extraction shattered it, and PyMuPDF only ever ran **because docling had
crashed on the hyperlink bug**. The shattering was a downstream artifact of the Docling
failure, not a property of the document. The wrong premise is left in this record deliberately,
flagged, because four earlier readouts were written on top of it.

**Both workarounds succeed**; the link-stripped copy (52,403 chars, 13.4% short) is markedly
cleaner than the pypdfium2 backend (52,799 chars, 28.4% short, which reproduces the paper's
genuine letter-spaced masthead and is penalised for it).

**The OCR tier is a regression on p455 and a rescue on p719 — the same tier, opposite verdicts.**
On p455 the OCR route loses 30% of the word count (8,541 words to 5,950) and produces
**80 glued tokens against 0**, and drops a whole clause on page 3 ("The surgical instruments
handled by the" — present in the non-OCR text, missing in OCR). On p719, whose stored text is
not merely glyph-littered but **Caesar-shifted by −3** (`FRPSOH[LW\ DQG FRVWV` = "complexity and
costs"), OCR recovers every heading that was lost and every body anchor checked: unreadable
becomes readable.

Both OCR outputs are **byte-identical across repeated runs** — rapidocr is fully deterministic
on CPU at 5.7–6.1 s per page, unlike the VLM.

**🔴 A finding the whole lane then had to live with:** the gate cannot see the OCR route's
worst defect. Glued words *raise* chars-per-unit (128.0 for OCR against 113.2 for clean text),
so OCR output scores **better** on the gate while being 30% shorter and dropping content. A
gate-passing verdict must not be read as "this text is good" for OCR output.

**Commits:** none.

---

### PARSE-GATE-06a — docling crash retry on a link-stripped copy

*Source: transcript line 1526, 2026-09-08T02:34:44Z. Brief at line 1430.*

**Asked:** make a docling crash retry on a link-stripped copy, surface parser exceptions in
the ledger, and pin docling.

**Shipped.** `parse_with_docling` raising now records an error row and triggers
`strip_links_to_temp()`, which copies the PDF, deletes every link annotation and **verifies
that every page's text is byte-identical** before the copy is used; on inequality the copy is
discarded with a recorded reason and the cascade proceeds unchanged. Every parser in the
cascade is now wrapped so a raise produces a `parse_attempts` row with
`skipped_reason="error: <Class>: <message>"`, truncated at 300 characters — **making a crash
visible in the ledger for the first time**. `parser_used` gains `docling_sanitized`;
`_MAX_ATTEMPTS` goes 3 to 4; `requirements.txt` pins `docling==2.74.0`, which had been the
bare unpinned token.

**The crash does not reproduce synthetically.** A hand-built PDF carrying a byte-equivalent
scheme-less URI object parses fine under real docling, while p455 still crashes on all twelve
pages — so T7 uses the real corpus PDF and the unit tests use stubs. p455 is a tagged PDF and
the synthetic fixture is not; that is the likeliest discriminator and is unverified.

**This readout also closed PARSE-GATE-05's open mechanism question:** PyMuPDF's `get_links()`
*does* return the offending link, but with `uri: None`, because it cannot parse a scheme-less
URI — which is why the earlier enumeration reported it as "not surfaced". `delete_link` removes
it regardless.

Twelve new tests plus one integration test that runs **real docling on the real p455** and
observes the crash, the sanitized retry and a passing verdict in 25.3 s.

**Commit:** `fe81ac2` — `fix(parse): retry docling on a link-stripped copy; record parser
exceptions; pin docling`. Suite 1,816 passed, 16 deselected (1,804 + 12).

---

### PARSE-GATE-06b — deterministic OCR tier and reroute rewrite

*Source: transcript line 1749, 2026-09-08T03:17:00Z. Brief at line 1530.*

**Asked:** add the docling+rapidocr tier, rewrite the reroute table around it, add glue
telemetry, and make the ledger survive a total parse failure.

**Shipped.** `parse_with_docling_ocr` runs the docling pipeline with `do_ocr=True` and
`RapidOcrOptions(force_full_page_ocr=True)`, sharing one converter factory with the text-layer
tier; naming any engine other than `rapidocr` raises with a message saying that naming an
engine in the spec is not the same as wiring one. `_REROUTE` now sends **every** criterion to
`docling_ocr` before `qwen2.5vl`, with `EMPTY_TEXT` keeping PyMuPDF in front; scanned PDFs lead
with OCR and fall through to vision on a raise or on sparse output. `_MAX_ATTEMPTS` goes 4 to
5. New telemetry `long_token_share_pct` measures glued tokens — the instrument PARSE-GATE-05
said was missing — and is returned but never judged: 0.0% for docling's text layer on p455,
2.525% for the same paper under OCR, 2.441% for p719 under OCR, and **0.0% for p719's stored
glyph text**, which is unreadable, so the metric measures gluing and not readability.
`_insert_attempts` / `_commit_attempts` split the insert from transaction ownership so a parse
where nothing was selectable still commits its ledger rows.

Twenty new tests, including one that runs **real OCR on the real p719** and confirms glyph
density 0.0 and the recovery of `PASSIVE WRIST EMULATION` in 40.2 s. That test asserts on
length-within-10% and content anchors rather than a pinned digest, because docling's export is
version-sensitive and a digest would fail on a legitimate upgrade without saying what changed.

**Deviation:** the brief assumed `parse_with_docling` already passed pipeline options; it built
a bare converter. Since the stop condition existed to prevent duplicated converter code and
there was none to prevent, the readout factored one shared converter and proceeded, reporting
the discrepancy.

**Two wiring bugs surfaced, both caught by stubbed tests.** `is_reroute` keyed off "any row
exists", but 06a's error rows and 06b's skip rows now precede the first judged attempt, so
re-route semantics were being applied to the initial route and three passing tests broke; it
counts judged attempts now. And a page cap used `continue` on the judge loop, re-assessing the
same text and appending a duplicate verdict row; cap handling is a selection loop now.

**Commit:** `b7c6803` — `feat(parse): deterministic OCR tier (docling+rapidocr), reroute
rewrite, glue telemetry, ledger on total failure`. Suite 1,836 passed, 17 deselected
(1,816 + 20).

---

### PARSE-GATE-06c — bounding the vision route

*Source: transcript line 1876, 2026-09-08T03:36:12Z. Brief at line 1753.*

**Asked:** bound the vision route using PARSE-GATE-04's measurements.

**Shipped.** `PDFParsing` gains `vision_num_predict` (2048), `vision_num_ctx` (8192) and
`vision_page_timeout_s` (240), each documented with the measurement behind it.
`parse_with_vision` now sends the cap and context, logs `done_reason`, `eval_count`,
`prompt_eval_count` and seconds per page, and **raises `VisionTruncatedError` on the first
page whose `done_reason == "length"`, before rendering any further page** — so no partially
looped document can survive. The prompt moves to a pinned constant carrying probe D's
transcription instruction, with a test asserting that the old "Output as Markdown" and
"Preserve tables, headings, and formatting" strings are absent, guarding against
reintroduction.

Fifteen new tests. **One real gap found while testing:** the paths that re-raise the actual
cause — a parser blowing up on the initial route, a truncated vision attempt — bypassed 06b's
total-failure ledger, so precisely the paths with the most diagnostic value lost their rows;
`parse_pdf` now commits attempts on any raise before propagating. **A second stub defect:**
three `MagicMock` vision stubs returned a `MagicMock` for `done_reason`, which happens not to
equal `"length"`, so those tests had been passing by accident; they now set it explicitly, with
no assertion changed.

**🔴 The G5 deviation.** The readout ran the integration suite as `pytest tests/ -m
"integration"`, not mirroring the standard gate's `ollama` exclusion.
`tests/analysis/paper1/test_judge_pass2.py:420` is marked **both** `ollama` and `integration`
and constructs `ReviewDatabase("surgical_autonomy")` — the live database, read-write. Two
consequences, both self-reported: `review.db` mtime moved from `2026-07-27 19:47:48` to
`2026-09-08 03:31:57` and size grew 99,753,984 to 99,762,176 as the WAL checkpointed; and
`gemma3:27b` was loaded, evicting the vision model. **No data was written** — every row count
matched the PARSE-GATE-03 baseline, `parse_attempts` still held zero rows, p455 was still
`AI_AUDIT_COMPLETE`, and the +8,192 B is exactly the schema page PARSE-GATE-03 predicted would
fold in at the next checkpoint. `NRestarts=0`, no restart. 06a's and 06b's integration runs
were file-scoped and did not select that test.

**Commit:** `5e58020` — `fix(parse): bound vision route — output cap, context, transcription
prompt, truncation abort`. Suite 1,851 passed, 17 deselected (1,836 + 15).

---

### PARSE-GATE-07 — re-parse of 455, 586, 699, 719

*Source: transcript line 1994, 2026-09-08T03:51:44Z. Brief at line 1880.*

**Asked:** re-parse the four gate-failing papers through the finished cascade and measure the
result against v2.

**All four re-parsed, in 180 s total, with no exceptions and vision never reached.** A verified
backup was taken first (`review_pre_PARSE-GATE-07_20260908T034445Z.db`, 99,762,176 B,
`integrity_check: ok`, 24 tables). p455 took the docling-crash path and was rescued by the
sanitized retry; the other three failed the gate on glyph density and were rescued by the OCR
tier. The full ledger is in §4.

**Metric movement**, from the readout's K3 tables:

| paper | chars v2 → v3 | chars/unit v2 → v3 | glyph/kchar v2 → v3 | long_token% v2 → v3 |
|---|---|---|---|---|
| 455 | 59,964 → 52,403 | 7.1 → **113.2** | 0.0 → 0.0 | 0.0 → 0.0 |
| 586 | 69,214 → 40,486 | 168.4 → 134.5 | 7.831 → **0.0** | 0.016 → **1.935** |
| 699 | 48,546 → 27,045 | 166.8 → 130.7 | 8.631 → **0.0** | 0.0 → **2.744** |
| 719 | 279,389 → 28,836 | 1,447.6 → **110.9** | 19.586 → **0.0** | 0.0 → **2.441** |

**The fidelity check split the three OCR papers two-to-one**, and the counts are in §4. p455 is
verbatim-accurate with zero glued tokens and no OCR involved; p719's v2 had 16 of 16 headings
and 91 of 91 body paragraphs corrupted and is recovered whole; **586 and 699 were already
clean in v2 and are degraded by OCR**, which is the open ruling in §5.

**Commits:** none. Statuses untouched, v2 files untouched, nothing adopted.

**Event:** the session then ran `/wrap` and printed its final closeout block at transcript line
2028, 2026-09-08T03:55:05Z, at HEAD `5e58020`, verdict CLEAN-TO-EXIT.

---

## 3. Defects found

| # | defect | where it came from | status |
|---|---|---|---|
| **D1** | **Docling crashes on a scheme-less DOI, and a bare `except` hid it.** `PdfHyperlink.uri` is an `AnyUrl`; under pydantic 2.12.5 a URI with no scheme fails validation, and docling fails the whole conversion. A bare `except Exception` answered with PyMuPDF, whose naive extraction shattered the text. This is the root cause of p455's `SHATTERED` verdict — the document was never broken. | diagnosed 03/04, proven 05, fixed 06a | **Fixed** — `fe81ac2` (sanitized retry) and the error rows that make a crash visible in the ledger. docling pinned to 2.74.0 in the same commit. |
| **D2** | **The vision client's wall-clock timeout does not bind the HTTP request.** `_DEFAULT_WALL_TIMEOUT = 600.0` against `_HTTP_READ_TIMEOUT = 900.0`; the watchdog abandons the worker thread without cancelling the request, which keeps the only runner slot, so retries queue behind it and cannot succeed. Worst case per bad page is three attempts of 900 s and then an Ollama restart. | 03 §6(b), quantified 04 §6 | **Open — belongs to OLLAMA-CLIENT-01.** Not fixed in this lane; 06c's per-call `vision_page_timeout_s=240` bounds the caller, not the request, and its docstring says so. |
| **D3** | **`parse_with_vision` set no output cap**, so a repetition loop could generate to a 128,000-token context — 50 minutes at the measured rate. | 03 §6(a), loop observed in 04 probe A | **Fixed** — `5e58020`: `num_predict=2048`, `num_ctx=8192`, the transcription prompt, and `VisionTruncatedError` on the first `done_reason == "length"`. |
| **D4** | **G5 deviation in 06c** — `pytest tests/ -m "integration"` without `and not ollama` selected the live-DB judge test, which opened `review.db` read-write and checkpointed its WAL (mtime and size moved, +8,192 B) and loaded `gemma3:27b`, evicting the vision model. | self-reported, 06c §6 | **Confirmed no rows changed** — every table count matched baseline, `parse_attempts` still zero rows, statuses unchanged, `NRestarts=0`. The correct invocation is `-m "integration and not ollama"`. The underlying test is unguarded; see §5. |
| **D5** | **Wiring bugs caught by stubbed tests.** 06b: `is_reroute` keyed off "any row exists" so error and skip rows made the initial route look like a re-route; and a page cap used `continue`, appending a duplicate verdict row. 06c: the total-failure ledger was bypassed on exactly the re-raise paths with the most diagnostic value; and three vision stubs returned a `MagicMock` for `done_reason`, so those tests had been passing by accident. 02 also found `EMPTY_TEXT` re-routing resurrecting PyMuPDF on scanned PDFs. **The brief anticipated two; the transcript records five, across 02, 06b and 06c.** | 02 §4, 06b §3, 06c §4 | All fixed in their own commits. |

---

## 4. Data state at close

### The PARSE-GATE-07 ledger — all eight `parse_attempts` rows

Read from `data/surgical_autonomy/review.db` in read-only mode on 2026-09-08. All eight rows
carry `parsed_text_version = 3` and a NULL `skipped_reason` except row 1; no row was a skip.

| id | paper | attempt | parser | passed | accepted | elapsed | verdict detail |
|---|---|---|---|---|---|---|---|
| 1 | 455 | 1 | `docling` | 0 | 0 | 2.1 s | `error: ConversionError: … PdfHyperlink … input_value='dx.doi.org/10.1016/j.cmpb.2013.01.017'` |
| 2 | 455 | 2 | `docling_sanitized` | 1 | **1** | 20.3 s | no failures |
| 3 | 586 | 1 | `docling` | 0 | 0 | 13.0 s | `GLYPH_DENSITY 7.831` against limit 5.0 |
| 4 | 586 | 2 | `docling_ocr` | 1 | **1** | 48.7 s | no failures |
| 5 | 699 | 1 | `docling` | 0 | 0 | 7.6 s | `GLYPH_DENSITY 8.631` against limit 5.0 |
| 6 | 699 | 2 | `docling_ocr` | 1 | **1** | 37.6 s | no failures |
| 7 | 719 | 1 | `docling` | 0 | 0 | 13.5 s | `GLYPH_DENSITY 19.586` against limit 5.0 |
| 8 | 719 | 2 | `docling_ocr` | 1 | **1** | 37.1 s | no failures |

`full_text_assets` holds 794 rows, four more than the pre-run 790. All four papers are
`AI_AUDIT_COMPLETE`, with `papers.updated_at` still 2026-03-18 — `update_status` was never
called.

### The four papers' files

Sizes and digests measured on disk on 2026-09-08, not taken from the transcript.

| paper | v2 sha256 | v2 bytes | v3 sha256 | v3 bytes |
|---|---|---:|---|---:|
| 455 | `1e459a15005549fc89250a34c3d35b417045d56c13893d36276e1836eb616e24` | 60,537 | `71d3448949a0952d4c08dd0b5ada3b6889115b534b5993d16636ce94916c6e51` | 52,503 |
| 586 | `dd8935f5f746e1febe213223040c77b75e1684e3e40f4f8097efd9bd096856fa` | 69,223 | `29eb250acb1db6c2e5bea63d1fd48fa5873581d3aa5285e2a1934e66dede2866` | 40,548 |
| 699 | `a83f443fb1e6b9ee2520007058837a75aa3c5ddf8aaff2ee25c4291f51fe9b8b` | 48,568 | `5313b9e201163cf5a79345a7a6a3325ab14c265c2f568501983c39be7ef54de7` | 27,069 |
| 719 | `4cbc3557ff5ef4b0d562f06eb01c23a7ab4536021f35021d13988056edc7cff3` | 279,426 | `37bfcdf8094609ff9397e8059b73f40588cd51214164c4d4cd40ee4b42f1500f` | 28,867 |

All four v2 digests are unchanged from the pre-run baselines recorded in PARSE-GATE-07's P5.
The v2 mtimes are all 2026-03-14; the v3 mtimes are 2026-09-08 03:45 to 03:48 and match their
ledger rows to the millisecond. Note that the readouts quote **character** counts (52,403 /
40,486 / 27,045 / 28,836) while this table gives **bytes**; the difference is multi-byte UTF-8.

### The 586/699 hybrid finding

From PARSE-GATE-07's K4 fidelity check (transcript line 1994):

| | v2 headings glyph-corrupted | v2 body paragraphs glyph-free | v3 glued tokens | v3 body paragraphs carrying a glued run |
|---|---:|---:|---:|---:|
| **586** | **1 / 16** | 30 / 54 | 38 | **25 / 51** |
| **699** | **0 / 15** | 37 / 40 | 33 | **19 / 39** |
| **719** | **16 / 16** | **0 / 91** | 51 | 26 / 50 |

For 586, OCR recovers **one** corrupted heading and degrades **25 of 51** body paragraphs; for
699 it recovers **none** and degrades **19 of 39**. Every heading and value checked on both
papers was already present and clean in v2. For 719 the trade runs the other way: v2 had no
readable content at all.

**586 is the hybrid case in concrete form.** Its v2 had exactly **one** glyph-corrupted heading
yet a document-wide density of 7.831 per kchar, so a per-document threshold routed the whole
paper to OCR for a localized defect.

### Backups

Both verified online backups are present, and neither is the live file (distinct inodes).

| path | bytes | sha256 |
|---|---:|---|
| `data/backups/review_pre_PARSE-GATE-03_20260907T230211Z.db` | 99,753,984 | `71a7d59e4985f3747c86cb0de61c1b92868680c627b56cbc21fe09bf48f3b2fc` |
| `data/backups/review_pre_PARSE-GATE-07_20260908T034445Z.db` | 99,762,176 | `bcf4fa1bcafedeeb0930413c73f41a521ca7adb0b9431cf78ed52510d4452901` |

### The live database

`review.db`: mtime 2026-09-08 03:48:48.732577380 UTC, size 99,770,368 B — the moment ledger row
8 was written, and unchanged since.

---

## 5. Open at close

**1. The 586/699 version ruling — the architect's, not the engine's.** Three options, stated
neutrally and without recommendation:

* **keep v2** for both papers — every heading and value checked is already present and clean,
  and the single corrupted heading on 586 is the only thing OCR recovers;
* **adopt v3** for both — glyph density falls to zero and the gate passes, at the cost of 25 of
  51 and 19 of 39 body paragraphs carrying glued runs;
* **build a page- or region-level hybrid (v4)** that applies OCR only to the pages or regions
  that are actually corrupted, which is the case 586 makes concrete but which no code exists
  for today.

455 and 719 are settled on the evidence: both are usable as v3. **Nothing has been adopted.**

**2. Fix-phase queue items originating in this lane.**

* **OLLAMA-CLIENT-01** — cancel the HTTP request when the wall-clock watchdog fires, and give
  the vision model its own `MODEL_TIMEOUTS` entry (D2). Until it lands, a hanging page still
  costs up to three attempts of 900 s of GPU time whatever the caller's timeout says.
* **A guard on the live-DB judge test** — `tests/analysis/paper1/test_judge_pass2.py:420`
  opens the live corpus database read-write from the suite (D4, and see item 3).
* **A page- or region-level OCR remedy for `GLYPH_DENSITY`**, which the `_REROUTE` table's own
  comment already anticipates.
* **A whole-word or space-density quality metric.** `long_token_share_pct` was added as
  telemetry in 06b and is deliberately unjudged; it is the only signal that dissents from the
  gate on 586/699, and the gate is otherwise blind to gluing.
* **Revisit the `docling==2.74.0` pin** — two `DeprecationWarning`s (`generate_table_images`,
  `rec_font_path`) fire on every OCR run.
* **`parse_pdf` is now near 200 lines inside one `try/finally`**; the attempt loop is a
  candidate for extraction, deliberately not done in 06b so as not to obscure that diff.

**3. The nightly test suite runs the full suite with no marker gate, and loads models.**
Crontab entry `0 9 * * * /bin/bash ~/projects/evidence-engine/scripts/nightly_tests.sh` runs
`python -m pytest tests/ -v` with **no marker expression at all**. On 2026-09-08 the Ollama
journal shows `gemma3:27b` loading at 09:00:48 and `Qwen3 8B` at 09:07:19, and
`logs/nightly_test_20260908.log` ends `1864 passed, 16 warnings, 4 errors in 457.14s`, the four
errors being network failures in `tests/test_openalex.py`. **That log confirms
`test_paper_366_grammar_prevents_four_element_emission` — the `ollama`+`integration` test at
`test_judge_pass2.py:420` that opened the live database in the 06c deviation — ran and
PASSED.** So the 06c deviation is not a one-off operator error: **the same run shape executes
every night at 09:00 UTC, unattended.** It is a Run 7 hazard on two counts — it occupies the
single runner slot and it changes which model is resident — and it is the reason the resident
model at the time of this report is `qwen3:8b` rather than the `gemma3:27b` the primer records.
The live database's mtime did not move across the 09:00 run, so no rows were changed by it.

**4. The plan's "fenced" claim is contradicted on the model-load half.** See §6.

**5. A second scheduled job also loads models.** Crontab entry `0 7 * * *
~/scripts/ollama_health_check.sh` produced a burst of runner starts between 07:00:02 and
07:05:13 on 2026-09-08, cycling through many models. Recorded as an observation; **no action is
taken or proposed here.**

**6. The primer's statement "the WAL is gone — checkpointed" is superseded, and is not a
defect.** `review.db-wal` and `review.db-shm` exist again, the WAL stamped
2026-09-08 10:30:35.021 — one second after `dgx-snapshot-user.service` started its nightly run
at 10:30:34, which captures `review.db` through `capture_sqlite`
(`dgx-infra/snapshot/user_tier.py:157`). The nightly snapshot reopens the database, which
recreates the sidecars; it does not modify it, and `review.db`'s own mtime is unchanged at
03:48:48. The sidecars will return every night. The primer's sentence was true when written and
has a one-night shelf life; it should not be quoted forward as a durable property.

---

## 6. Corrections appendix

**Dated 2026-09-08.** Recorded where the transcript and another account disagree. Where the
transcript and the primer disagree, the transcript is preferred, because the primer is a
working document that is rewritten each session while the transcript is a fixed record of what
was measured at the time.

**C1 — the plan's "fenced" claim for the nightly suite, contradicted on model loads.**
The Unified Plan states:

> "Nightly test suite exists outside /loop; fenced — it cannot restart the service or load
> models."

**The model-load half is false as measured.** `scripts/nightly_tests.sh` runs
`python -m pytest tests/ -v` with no marker expression, so `ollama`-marked tests are selected;
the Ollama journal for 2026-09-08 shows `architecture=gemma3` loading at 09:00:48 and
`architecture=qwen3 … name="Qwen3 8B"` at 09:07:19, both inside the nightly run's window, and
`logs/nightly_test_20260908.log` records the run finishing at 457.14 s with 1,864 passed.
**The restart half is untested here and is recorded as unverified, not false** — `NRestarts=0`
and `ExecMainStartTimestamp` is unchanged at 2026-08-31 00:41:59, which is consistent with the
claim but does not establish it, since no run in this window exercised the restart path.

**C2 — "review.db unchanged" meant two different things at two different times, and both
accounts are correct.** PARSE-GATE-02, 03, 04, 05 and 06a all report the file byte-identical at
mtime `2026-07-27 19:47:48.448294416` and size 99,753,984. From PARSE-GATE-03 onward that was
true of the **file** and false of the **database**: `parse_attempts` had been created and the
change sat in an uncheckpointed 24,752 B WAL, which PARSE-GATE-03's own readout stated
explicitly. The 06c deviation checkpointed it (+8,192 B) and PARSE-GATE-07's writes brought the
file to its closing size. No account is wrong; the distinction is between the file and the
database, and it is the reason the mtime should never be quoted alone as proof that nothing
happened.

**C3 — the resident Ollama model recorded in `primer.md` is stale.** The primer records
`gemma3:27b` resident, left from the 06c deviation, which was accurate when PARSE-GATE-07
closed. `ollama ps` on 2026-09-08 shows `qwen3:8b` (11 GB, context 40960, `Forever`), loaded by
the 09:00 nightly run. Transcript and primer do not disagree; the world moved.

**C4 — the lane's premise concerning p455 was wrong for four readouts and is left in the record.**
PARSE-GATE-00 through 04 proceeded on the belief that p455's text layer was broken at source.
PARSE-GATE-05 falsified it: the shattering was an artifact of the Docling hyperlink crash, and
p455 parses cleanly with no OCR once docling can run. The earlier readouts' reasoning is not
edited here — the wrong premise is part of the record, and PARSE-GATE-05's own words are
quoted in §2.

---

## 7. Same-session, non-lane tasks

These two tasks ran in the same session before PARSE-GATE-00 and have no committed report of
their own. They are recorded here so their reasoning is not lost with the transcript.
VERIFY-EXIT-01 (elicit-design-02), the session's first task, produced `377b4ec` and already has
its own report at `docs/session-reports/VERIFY-EXIT-01_report.md`; nothing is duplicated here.

### SELECT-SAMPLE-01 — stopped at diagnosis, no commit

*Source: transcript line 217, 2026-09-07T19:58:49Z. Brief at line 158.*

The task was to add an eligibility assertion to `select_sample()` in
`analysis/eval/schema_eval2.py`, whose "carried" admission route was known to bypass the
corpus filter. **Two independent stop conditions fired and no code was edited.** The brief
assumed a carried *parameter*; there is none — `CARRIED` is a frozen module-level tuple at line
52, so an assertion would not be a guard but an unconditional hard failure that bricks five
live eval runners, including the two that deliberately retain the offending papers. And the
retention is a documented decision, not an oversight: three of the carried papers (547, 629,
799) are `FT_SCREENED_OUT` with zero extractions and were **kept for CAPTURE-01
comparability**, a choice encoded in `analysis/eval/elicit01/manifest.py:34`, which lists an
`EXCLUDED` set of exactly `{415, 719}` and pointedly omits them.

The readout also established that **no reusable corpus predicate existed anywhere** — the
four-status set appeared only as inline SQL literals at four sites, two of which carry
deliberately *different* status lists for different questions. That finding is what
CORPUS-PRED-01 was then briefed to fix, so this task is superseded rather than abandoned.

### CORPUS-PRED-01 — single corpus-eligibility authority

*Source: transcript line 348, 2026-09-07T20:26:31Z. Brief at line 221. Commit `8388a1d`.*

**Asked:** derive one corpus-membership predicate and retire the inline copies.

**Found, and it changed the shape of the fix:** the status graph in `engine/core/database.py`
carries **no success/failure marking**, and its forward closure from `FT_ELIGIBLE` is **nine
statuses, not four** — it reaches backward to `PARSED` through the `EXTRACT_FAILED → PARSED`
repair edge. So "any downstream success state" is not derivable from the graph, and the
declared-constant fallback was taken as the brief directed, with the reason pinned by a test.

**Shipped:** `engine/core/corpus.py` holding `CORPUS_STATUSES` once and exposing it as two
views of one derivation — `is_corpus_member()` for Python and `corpus_status_sql(column)` for
SQL. The three adopting sites now build their WHERE clause from it. `schema_eval2` additionally
gains `CARRIED_NON_CORPUS = {547, 629, 799}` and a `CarriedCorpusDeclarationError` that raises
on a fourth offender *or* on one of the three becoming eligible, names each id with its actual
status, and **filters nothing** — the three stay in the sample, which is what preserves the
published studies' comparability.

Twenty new tests, all under the standard gate, including one that pins the fifteen-member
lifecycle status set whole so a new status turns the suite red, and one that greps every `.py`
file for the retired inline literal and finds **zero hits**. Every "unchanged" claim is pinned
against baselines captured by running the pre-patch code at `377b4ec` against the same fixture
builder the tests use, so the bytes proving it and the bytes asserting it share one source.

Suite 1,734 passed, 15 deselected (1,714 + 20). Typed `fix`, not `refactor`, because selection
is byte-identical on today's data but the declaration check is a new failure path.
