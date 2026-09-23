# INPUT-IDENTITY-01 — Phase 1 addendum 1: row D11 (two parses of four papers) and the K1 check

**Date:** 2026-09-23 · **Base:** read-out `INPUT-IDENTITY-01_phase1_readout_20260923.md` at
`8b14b28b07a86f2710f93d44972883259dd2ff04` (unchanged; this is a dated addendum, not an edit) ·
**Database:** `data/surgical_autonomy/review.db`, `mode=ro` only · **Files:** `parsed_text/{455,586,699,719}_v{2,3}.md`
and the four PDFs, read only.

No recommendation is made on which version is the intended input.

## Inferred premises

| id | premise | result | measured |
|---|---|---|---|
| K1 | the arm pin covers codebook, prompt, model digest and options, so `(arm, paper_id, parsed_text_hash)` loses nothing against S3d's six-tuple | **PARTLY TRUE** | all four are in the pin tuple; only two have a mismatch-refusal test; no arm is pinned on live yet (§3) |
| K2 | `full_text_assets` or `parse_attempts` records, per parsed version, the parser, its version/configuration and a timestamp | **PARTLY TRUE** | v3: parser, attempt ledger, gate metrics, `created_at`, `pdf_hash`. v2: parser, `parsed_at`, `pdf_hash` only, with no `parse_attempts` row. Parser version/configuration is recorded for **neither**: no such column exists |
| K3 | a character-level comparison is read-only, cheap and fails loudly | **TRUE** | 8 files read with `Path.read_text()`; the longest version is 279,389 characters; `difflib.SequenceMatcher` ran in seconds |

## 2(a) — Text comparison, v2 against v3

"LCP" is the longest common prefix, in characters. "Similarity" is `difflib.SequenceMatcher(autojunk=False).ratio()`
over both texts with all whitespace and every `GLYPH&lt;…&gt;` token removed. "Readable words" is the
count of whole-word `the`, `and` and `of` (case-insensitive), a crude probe for decoded English.

| paper | ver | bytes | chars | lines | `#` headings | table rows | GLYPH tokens (share of chars) | alpha tokens ≥ 25 chars | readable words | ends at |
|---:|---|---:|---:|---:|---:|---:|---|---:|---:|---|
| 455 | v2 | 60,537 | 59,964 | 8,528 | 0 | 2 | 0 (0.0) | 0 | 1,198 | last reference entry, one word per line |
| 455 | v3 | 52,503 | 52,403 | 376 | 22 | 14 | 0 (0.0) | 0 | 1,198 | last reference entry |
| 586 | v2 | 69,223 | 69,214 | 253 | 16 | 7 | 542 (0.371) | 0 | 851 | last reference entry |
| 586 | v3 | 40,548 | 40,486 | 252 | 16 | 7 | 0 (0.0) | 30 | 652 | last reference entry |
| 699 | v2 | 48,568 | 48,546 | 226 | 15 | 3 | 419 (0.406) | 0 | 809 | last reference entry |
| 699 | v3 | 27,069 | 27,045 | 186 | 15 | 3 | 0 (0.0) | 29 | 582 | last reference entry |
| 719 | v2 | 279,426 | 279,389 | 309 | 16 | 14 | 5,472 (0.833) | 0 | **0** | GLYPH tokens |
| 719 | v3 | 28,867 | 28,836 | 243 | 15 | 14 | 0 (0.0) | 46 | 447 | last reference entry |

| paper | LCP (fraction of v3) | similarity, stripped | v2 opening heading | v3 opening heading |
|---:|---|---:|---|---|
| 455 | 5 (0.0001) | 0.9489 | none (no headings) | `## A minimally invasive surgery robotic assistant …` |
| 586 | 0 (0.0000) | 0.9508 | `## I. INTRODUCTION` | `## A Framework for Sensorless and Autonomous Probe…` |
| 699 | 89 (0.0033) | 0.9953 | `## A Cooperative Suturing Strategy Based on AI and…` | the same |
| 719 | 3 (0.0001) | **0.028** | `## $GLYPH&lt;c=3,font=/JGFKKL+TimesNewRoman&gt;0XO…` | `## A Multi-Behavior Algorithm for Auto-Guided Move…` |

**No v3 is truncated.** No v3 is a prefix or near-prefix of its v2 (LCP ≤ 0.33% of v3), and every
v3 ends on its final reference entry. The last 200 characters of each v3 are a complete
citation: 455 ends "…IEEE International Conference on Robotics and Automation, 2004.", 586
"…2012,pp.1982-1987.", 699 "…Springer International Publishing,Cham,2015.", 719 "…IFAC Symposium on
Intelligent Autonomous Vehicles, 1998." The size drop is removed markup, not lost text:

- **586, 699:** v2 is 37–41% GLYPH tokens by characters. v3 has none, and once whitespace and
  GLYPH tokens are removed the two texts are 95.1% and 99.5% similar. v3 runs words together
  (`aquantitativemeasurerepresentative`, `controlstherobottocompletetheautomaticneedleinsertion`;
  29–30 alphabetic tokens ≥ 25 characters, none in v2), and its headings lose their spaces
  (`## A.QuantifyingImageQuality`).
- **455:** no GLYPH tokens in either. v2 puts one word per line (7,989 of 8,528 lines) and has
  no Markdown headings, while v3 has 22 headings and 14 table rows. The word content is the same:
  1,198 readable words each and 94.9% stripped similarity. The byte difference is mostly
  newlines. The word "References" appears in v2's body; v3 has no references heading but does
  end on the reference list.
- **719:** v2 carries no decoded English at all. Zero occurrences of the/and/of, and 83.3% of its
  characters are GLYPH tokens. The rest is a shifted encoding: the opening `0XOWL…%HKDYLRU` is
  "Multi-Behavior" once 29 is added to every code point (`0XOWL` → `Multi`, `%HKDYLRU` → `Behavior`; checked by script). v3 is readable, runs some words together
  (46 alphabetic tokens ≥ 25 characters), and has a `## REFERENCES` section. Stripped similarity
  is 0.028: the two texts share almost nothing.

## 2(b) — Provenance of each version (K2)

Both versions of each paper are parses of **the same PDF bytes**. `full_text_assets.pdf_hash` is
identical on the v2 and v3 rows and equals `papers.pdf_content_hash`, and a fresh SHA-256 of the
PDF on disk matches it:

| paper | PDF | sha256 (v2 row = v3 row = on disk) |
|---:|---|---|
| 455 | `data/surgical_autonomy/pdfs/EE-303_Bauzano_2013.pdf` | `5c16c7f71995c0aad1333a85734c337763111c245b5a34eee18dedd8cf0ebb2c` |
| 586 | `data/surgical_autonomy/pdfs/EE-434_Varghese_2017.pdf` | `103b6237c21736fd99b1972e16fe39bda043d90c0c4b9d10ac1fcf309603c203` |
| 699 | `data/surgical_autonomy/pdfs/EE-547_Han_2024.pdf` | `bd0d5dbce12a847f737834715ba5a7ef433c975325798c47c31004376b65cf34` |
| 719 | `data/surgical_autonomy/pdfs/EE-567_Bauzano_2010.pdf` | `d8837c637bc1c9d0de8d3baf42311cebbafe1f766acda63e63e40d4f5d61b733` |

`full_text_assets`, quoted as `(id, parser_used, parsed_text_version, parsed_at)`:

| paper | v2 row | v3 row |
|---:|---|---|
| 455 | `(164, 'pymupdf', 2, '2026-03-14T03:34:34.102095+00:00')` | `(791, 'docling_sanitized', 3, '2026-09-08T03:45:21.394350+00:00')` |
| 586 | `(672, 'docling', 2, '2026-03-14T02:40:09.922950+00:00')` | `(792, 'docling_ocr', 3, '2026-09-08T03:47:06.436023+00:00')` |
| 699 | `(748, 'docling', 2, '2026-03-14T03:02:47.386598+00:00')` | `(793, 'docling_ocr', 3, '2026-09-08T03:47:54.886835+00:00')` |
| 719 | `(759, 'docling', 2, '2026-03-14T03:06:08.698924+00:00')` | `(794, 'docling_ocr', 3, '2026-09-08T03:48:48.723172+00:00')` |

File mtimes agree with `parsed_at` to the millisecond. The v2 files still carry their
2026-03-14 mtimes, so the re-parse did not touch them.

**`parse_attempts` records only v3.** There are 8 rows, `parsed_text_version = 3`, two per paper,
all at the v3 `parsed_at`. The table has no row for any v2. Quoted as
`(attempt_index, parser_used, passed, failures, accepted)`:

- **455:** `(1, 'docling', 0, '[]', 0)` with `skipped_reason` `"error: ConversionError: Conversion
  failed for: EE-303_Bauzano_2013.pdf … 1 validation error for PdfHyperlink … input_value='dx.doi.org/10.1016/j.cmpb.2013.01.017'"`;
  `(2, 'docling_sanitized', 1, '[]', 1)`.
- **586:** `(1, 'docling', 0, '[["GLYPH_DENSITY", 7.831, 5.0]]', 0)`; `(2, 'docling_ocr', 1, '[]', 1)`.
- **699:** `(1, 'docling', 0, '[["GLYPH_DENSITY", 8.631, 5.0]]', 0)`; `(2, 'docling_ocr', 1, '[]', 1)`.
- **719:** `(1, 'docling', 0, '[["GLYPH_DENSITY", 19.586, 5.0]]', 0)`; `(2, 'docling_ocr', 1, '[]', 1)`.

In the rejected attempt-1 `metrics` for 586, 699 and 719, `"chars"` is 69,214, 48,546 and
279,389, and `"glyph_artifacts"` is 542, 419 and 5,472. Those are **exactly v2's character and
GLYPH counts** in 2(a): on 2026-09-08, plain `docling` reproduced v2 by both measures, and the
gate rejected it. The accepted attempt-2 metrics equal v3's measured chars (40,486, 27,045,
28,836 and, for 455, 52,403).

A committed report records the gate these versions came from. `docs/session-reports/PARSE-GATE_report.md`:
"**The ruling's prediction held without tuning: exactly `{455, 586, 699, 719}` fail, 4 of 190.**
p455 on `SHATTERED=(98.8, 7.1)`; 586, 699 and 719 on `GLYPH_DENSITY` at 7.831, 8.631 and 19.586
against a limit of 5.0." `docs/session-reports/VERIFY-EXIT-01_parse-gate_report.md` confirms the v2
hashes unchanged after the re-parse (its C6). `PARSE-GATE-09_readout.md` and
`PARSE-GATE-10_readout.md` are font forensics on 586/699/719 ("Type0 + `/Identity-*` + no
`/ToUnicode`"). Those reports are quoted here as records; this addendum infers no intent from
them.

**Not recorded anywhere, for either version:** the parser's library version or its
configuration. `parse_attempts` has no such column
(`id, paper_id, pdf_hash, parsed_text_version, attempt_index, parser_used, passed, failures,
metrics, elapsed_s, accepted, skipped_reason, created_at, font_audit`), and `font_audit` is NULL
on all 8 rows.

## 2(c) — Which version the recorded decisions saw

Every row about these four papers in a decision, extraction or judgment table has a timestamp
**after v2's `parsed_at` (2026-03-14) and before v3's (2026-09-08)**:

| table | 455 | 586 | 699 | 719 |
|---|---|---|---|---|
| `ft_screening_decisions.decided_at` | 03-14 04:31 | 03-14 05:29 | 03-14 22:25 | 03-14 22:29 |
| `ft_verification_decisions` | 03-14 23:10 | 03-14 23:34 | 03-14 23:47 | 03-14 23:49 |
| `ft_screening_adjudication.created_at` | — | 03-15 04:55 | — | 03-15 04:55 |
| `cloud_extractions.extracted_at` (2 each) | 03-16 | 03-16 | 03-16 | 03-16 |
| `extractions.extracted_at` (1 each) | 03-16 21:09 | 03-17 21:25 | 03-18 03:25 | 03-18 04:11 |
| `judge_ratings.created_at` | 04-20 | 04-20 | 04-21 | 04-21 |
| `fabrication_verifications.verified_at` | 04-21 → 06-04 | 04-22 → 06-04 | 04-22 → 06-04 | 04-21 → 06-04 |
| `provenance_classifications.classified_at` | 07-27 | 07-27 | 07-27 | 07-27 |

All dates are 2026. Abstract screening (2026-03-12/13) predates both parses and used no parsed
text. The only later rows are the four 017 `state_at_migration` paper events (2026-09-21), which
carry no text.

**Inference, labelled as such:** every text-bearing record on these papers was made on **v2**.
v3 did not exist on disk or in the database before 2026-09-08, and every engine resolver takes
the highest-version file present at call time. One consistent fact, not a proof: the local
extraction of 719 holds **1** `evidence_spans` row, against 20 for each of 455, 586 and 699. That
is what an extraction over a text with no decoded English would be expected to yield.

## 2(d) — Per-paper summary

**455 (EE-303, Bauzano 2013).** v2 (2026-03-14, `pymupdf`) and v3 (2026-09-08,
`docling_sanitized`) are parses of the same PDF. On 2026-09-08 plain `docling` failed on a
malformed hyperlink URI in the PDF, and the sanitized retry was accepted as v3. The word content
is the same (1,198 readable words each; 94.9% similar with whitespace removed). v2 has one word
per line and no headings; v3 has 22 headings and 14 table rows. v3 is not truncated and ends on
the last reference. The PARSE-GATE report records 455 failing the gate on `SHATTERED`. All screening,
extraction and judgment records predate v3.

**586 (EE-434, Varghese 2017).** Same PDF bytes. v2 (`docling`) is 37% GLYPH tokens by
characters. A 2026-09-08 `docling` re-run reproduced v2's size and GLYPH count exactly and failed
`GLYPH_DENSITY` at 7.831 against 5.0. v3 (`docling_ocr`) has no GLYPH tokens and is 95.1%
similar once they and whitespace are removed. It runs words together (30 tokens ≥ 25
characters) and removes spaces from headings. Both versions have a `## REFERENCES` section, and
v3 ends on its last reference. All records predate v3.

**699 (EE-547, Han 2024).** Same pattern as 586. v2 is 41% GLYPH tokens and failed
`GLYPH_DENSITY` at 8.631. v3 has none, is 99.5% similar with GLYPH tokens and whitespace
removed, and has 29 run-together tokens. The heading count (15) and table rows (3) are unchanged.
Both end on the same final reference (Ronneberger et al., U-Net). All records predate v3.

**719 (EE-567, Bauzano 2010).** Same PDF bytes. v2 has **no decoded text**: 83% of its 279,389
characters are GLYPH tokens, the remainder is a character-shifted encoding (code-point offset 29), and it
contains no occurrence of the/and/of. The `GLYPH_DENSITY` failure was 19.586 against 5.0. v3
(`docling_ocr`) is readable English with a references section and 46 run-together tokens. The
two share 2.8% similarity. The 2026-03-18 local extraction of 719, made while v2 was the only
parse, holds 1 evidence span against 20 for the other three papers. All records predate v3.

## 3 — K1: what the arm pins

`PRAGMA table_info(arms)`: `arm_name TEXT, arm_kind TEXT, configuration_json TEXT,
configuration_marker TEXT, registered_at TEXT, retired_at TEXT, pinned_run_id INTEGER,
pinned_sha256 TEXT`.

`configuration_json` holds the pin tuple built by `engine/core/run_manifest.py::pin_tuple`,
quoted:

```python
    stages = {
        key: {"model": r.config.model, "model_digest": r.model_digest,
              "options_hash": r.options_hash,
              "format_schema_hash": r.format_schema_hash,
              "prompt_hash": r.prompt_hash}
        for key, r in sorted(resolved.items()) if r.arm_name == arm_name
    }
    ...
        "codebook_hash": codebook_hash,
```

`pinned_sha256` is `sha256_canonical(tup)`. `open_run` compares it against the arm's stored
value: `if existing[name][2] != sha:` → `raise ArmPinMismatch(`, naming the differing keys via
`_diff_keys`.

| S3d component | in the pin tuple | refusal pinned by a test |
|---|---|---|
| `codebook_hash` | yes: `"codebook_hash"` (the codebook's `semantic_hash`, not its byte `sha256`) | **no dedicated test** |
| `prompt_hash` | yes, per stage | **no dedicated test** |
| `model_digest` | yes, per stage | `tests/test_run_manifest.py::test_a_changed_model_digest_is_a_pin_mismatch` (`match="model_digest"`) |
| `options_hash` | yes, per stage | `tests/test_run_manifest.py::test_a_pinned_arm_resolving_differently_is_refused_and_the_difference_named` (`match="options_hash"`) |

Adjacent event-writer refusals in `tests/test_event_writer_refusals.py`:
`test_r10_a_claim_on_a_model_arm_the_run_did_not_pin_is_refused`,
`test_r21_refuses_a_configuration_repin_once_the_arm_holds_a_claim`,
`test_r59_a_pre_manifest_arm_can_never_be_pinned`,
`test_r59_a_pinned_arm_is_frozen_before_it_holds_any_claim`.

**Findings (not stops):**

1. **All four components are pinned, but only two have a test for their refusal.** A change to
   `prompt_hash` or `codebook_hash` alters the tuple and hence `pinned_sha256`, so the same
   comparison refuses it by construction. That behaviour is untested by name.
2. **The codebook component is `semantic_hash`.** A byte change to the codebook that leaves its
   semantic hash unchanged does not refuse. That is the documented meaning of the semantic hash,
   and it is recorded here because R91 relies on the arm to carry the codebook.
3. **The pin covers per-stage `model` and `format_schema_hash` too,** so the arm carries more than
   S3d's four. Nothing in the six-tuple is lost.
4. **No arm is pinned on live today.** `arms` holds only the three pre-manifest arms (`local`,
   `anthropic_sonnet_4_6`, `openai_o4_mini_high`), with `configuration_marker` `'not recorded
   (pre-manifest)'` and both pin columns NULL. R59 says they can never be pinned. The live spec's
   three arms (`local_deepseek_r1_32b`, `openai_o4_mini_2025_04_16_high`,
   `anthropic_claude_sonnet_4_6`) are not yet in the table and pin at their first manifest. R91's
   key therefore relies on a pin that will exist from the first run of a live-spec arm onward,
   not on any row present today.
