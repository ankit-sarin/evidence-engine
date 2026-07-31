# DIAG-VISION-01 — Vision-tier use rate across Run 6

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-27
Scope: diagnosis only. No re-parsing, no fix design, no DB writes, no commits.

## Arm P rerun state — ACCEPTANCE GATE 4

`tmux d0s2_armP_rerun` checked once at task start: session alive, child process
`PID 3291661 .venv/bin/python -m probes.cache_arms --arm P --n 15 --label armPrerun`, state `S+`,
**32:27 elapsed**, log tail mid-run (`filler 3: paper 15`). **State: RUNNING** (not completed, not
failed). The hard zero-Ollama constraint applied.

> **Ollama call count for this task: 0.** No inference, no `/api/ps`, no `/api/tags`, no `ollama`
> CLI. Everything was file reads, read-only SQLite (`mode=ro`), filesystem metadata, and reuse of
> the DIAG-UNANCHOR-01 census JSONs. No Arm P results were read or analysed.

---

## 1. Where tier attribution is recorded

**It is persisted.** Three independent records agree:

| record | location | content |
|---|---|---|
| DB column | `full_text_assets.parser_used` (`engine/core/database.py:123`) | free-text; written by the parser |
| type contract | `engine/parsers/models.py:16` | `parser_used: Literal["docling", "pymupdf", "qwen2.5vl"]` |
| writers | `engine/parsers/pdf_parser.py:150,166,171,178,187,192`; `scripts/retry_parse_6.py:69,78,93-97` | sets the column at parse time |
| run logs | `data/surgical_autonomy/parse_log.txt`, `parse_resume.log`, `docling_parse.log` | per-run tier totals |

No reconstruction by re-parsing was needed or performed.

**Attribution integrity check.** `full_text_assets` holds 790 rows for 455 papers (449 at
`parsed_text_version=1`, 341 at `v2`). For all 190 Run 6 corpus papers, the maximum
`parsed_text_version` in the DB equals the maximum `_v*.md` version on disk — **zero mismatches,
zero missing files** — so the DB row at max version correctly attributes the file each arm
actually consumed.

*Caveat, non-material here:* `pdf_parser.py:145-153` (the "already parsed, same hash" early
return) hardcodes `parser_used="docling"` in the returned `ParsedDocument` regardless of the real
parser. That path returns before any DB write, so stored attribution is unaffected; only an
in-process caller of the skip path would see a wrong label.

---

## 2. Tier census — ACCEPTANCE GATE 1

Corpus = the 190 papers with a Run 6 extraction. Tier = `parser_used` of the row at the highest
`parsed_text_version`, i.e. the file the extractors actually read.

| tier | papers | % of 190 |
|---|---:|---:|
| Docling (tier 1) | 185 | 97.4% |
| PyMuPDF (tier 2 fallback) | 5 | 2.6% |
| **Qwen2.5-VL:7b vision (tier 3)** | **0** | **0.0%** |

**Vision-tier paper IDs: none. The set is empty.**

PyMuPDF-tier paper IDs: **368, 378, 455, 748, 780**. (A sixth PyMuPDF paper, 780's neighbour
`274`, exists in `full_text_assets` but has no Run 6 extraction and is outside the corpus.)

Three independent confirmations that the vision tier never fired:

1. `parser_used` across the entire table has exactly two distinct values — `docling` (784 rows)
   and `pymupdf` (6 rows). `qwen2.5vl` appears zero times.
2. Both parse runs logged the tier tally explicitly:
   - `parse_log.txt:2271` — `Parsing complete: 98 parsed (98 docling, 0 qwen2.5vl), 0 skipped, 1 failed`
   - `parse_resume.log:5666` — `Parsing complete: 238 parsed (238 docling, 0 qwen2.5vl), 0 skipped, 6 failed`
3. **The vision tier was structurally unreachable for most of the corpus.** 174 of the 190
   corpus papers (91.6%) consumed a `v2` parse, and every `v2` parse was produced by
   `scripts/retry_parse_6.py`, whose cascade is **two-tier only** — Docling, and PyMuPDF on
   exception or `<100` chars (`retry_parse_6.py:68-80`). There is no vision branch and no
   sparse→vision escalation in that script. Only the 16 papers still on `v1` were produced by the
   full three-tier `pdf_parser.py:161-193`, and for those the scanned-PDF probe never triggered.

| consumed parse version | docling | pymupdf | vision |
|---|---:|---:|---:|
| v2 (from `retry_parse_6.py`, two-tier) | 169 | 5 | n/a — no code path |
| v1 (from `pdf_parser.py`, three-tier) | 16 | 0 | 0 |

---

## 3. Shared-parse verdict — ACCEPTANCE GATE 2

**Definitively yes: all three arms consumed byte-identical text per paper. A single shared parse,
not arm-specific parses.**

Code evidence — three consumers, one resolution rule, implemented identically:

| consumer | file:line | code |
|---|---|---|
| local extractor | `engine/agents/extractor.py:536-544` | `md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)` → `md_files[0].read_text()` |
| cloud arms (both) | `engine/cloud/base.py:59-67` `load_parsed_text()` | same glob, same `reverse=True`, same `[0]` |
| auditor | `engine/agents/auditor.py:375-380` | same |

`OpenAIExtractor` and `AnthropicExtractor` both inherit `load_parsed_text()` from
`CloudExtractorBase` — neither overrides it — so there is one implementation serving both cloud
arms and a character-for-character duplicate of it serving the local arm. There is no per-arm
parse directory, no arm column on `full_text_assets`, and no arm parameter anywhere in the parse
path.

Timing evidence — no parse could have changed underneath an arm mid-run:

| event | timestamp (UTC) |
|---|---|
| last `v1` parse written | 2026-03-02T21:34:14 |
| last `v2` parse written (`parsed_at`, and corroborated by file mtime on `780_v2.md`) | **2026-03-14T03:34:37** |
| first Run 6 extraction (local) | 2026-03-15T21:15:25 |
| first cloud extraction (openai / anthropic) | 2026-03-16T03:18:48 / 03:20:33 |
| last extraction of any arm | 2026-03-18T21:55:34 |

All parsing finished **~41.7 hours before the earliest extraction**, and no `parsed_text` file has
an mtime inside the 03-15 → 03-18 extraction window. The arms read the same bytes.

*Footnote:* the shared resolution rule sorts version strings lexicographically
(`sorted(glob("…_v*.md"), reverse=True)`), so it would pick `v9` over `v10`. Max observed version
in this corpus is 2, so it is correct here — but it is a latent selection bug, not a Run 6 one.

---

## 4. Tier × untraceable cross-tab — ACCEPTANCE GATE 3

Reusing the DIAG-UNANCHOR-01 definitions read-only: *untraceable* = non-empty `source_snippet`
failing `grep_verify()` (normalized exact substring, else sliding word-window
`SequenceMatcher > 0.85`); *no-textual-basis* = the census C/C1 buckets, where no constituent
sentence of the snippet is traceable at all. Denominator throughout = spans with a non-empty
snippet.

| arm | tier | papers | spans w/ snippet | untraceable | rate | no-basis (C/C1) | no-basis rate |
|---|---|---:|---:|---:|---:|---:|---:|
| local_deepseek_r1_32b | docling | 185 | 3522 | 800 | 22.7% | 604 | 17.1% |
| local_deepseek_r1_32b | pymupdf | 5 | 97 | 28 | 28.9% | 24 | 24.7% |
| openai_o4_mini_high | docling | 183 | 3115 | 189 | 6.1% | 14 | 0.4% |
| openai_o4_mini_high | pymupdf | 5 | 94 | 5 | 5.3% | 0 | 0.0% |
| anthropic_sonnet_4_6 | docling | 185 | 3677 | 1282 | 34.9% | 54 | 1.5% |
| anthropic_sonnet_4_6 | pymupdf | 5 | 100 | 35 | 35.0% | 0 | 0.0% |
| **pooled** | docling | 185 | 10314 | 2271 | **22.0%** | 672 | 6.5% |
| **pooled** | pymupdf | 5 | 291 | 68 | **23.4%** | 24 | 8.2% |

The vision-tier row is absent because the stratum is empty.

Per-paper untraceable rate for the five PyMuPDF papers, against the Docling distribution:

| paper | rate (all arms pooled) |
|---|---:|
| 368 | 11.9% |
| 378 | 31.7% |
| 455 | 38.9% |
| 748 | 10.2% |
| 780 | 25.4% |
| **Docling papers (n=185)** | median **20.0%**, IQR 13.6–28.3%, mean 22.6% |

All five sit inside the Docling range; two are below the Docling median. Pooled difference is
1.4 percentage points on a stratum of 5 papers / 291 spans — well inside noise, and **this
comparison is underpowered by construction and should not be reported as evidence of no effect,
only as absence of a visible one.**

---

## 5. Spot-check

**The contract's condition ("for the vision-tier papers, if any") is not met — there are none.**
No vision-tier spot-check was possible or performed.

As the nearest available analogue I compared the five PyMuPDF-tier files against the Docling
corpus. The two tiers produce *structurally very different* markdown, which is worth recording
even though it did not move the untraceable rate:

| metric | 368 | 378 | 455 | 748 | 780 | Docling median (n=185) |
|---|---:|---:|---:|---:|---:|---:|
| headings (`#`) | 0 | 0 | 0 | 0 | 0 | **17** |
| table rows (`\|…`) | 0 | 0 | 2 | 3 | 0 | **6** |
| `<!-- image -->` placeholders | 0 | 0 | 0 | 0 | 0 | **10** |
| lines | 1217 | 1072 | **8528** | 1295 | 1603 | 283 |
| mean non-blank line length | 45.8 | 41.5 | **6.1** | 43.8 | 37.0 | **258.2** |
| hyphenation line-breaks | 7 | 1 | **106** | 15 | 3 | **0** |
| PDF ligature chars (`ﬁﬂﬀ`) | 92 | 91 | 77 | 64 | 127 | **0** |
| `(cid:N)` glyph artifacts | 0 | 0 | 0 | 0 | 0 | 0 |

Three concrete differences, visible in the first lines of each file:

- **No document structure.** PyMuPDF output has zero markdown headings, no table markup, and no
  image placeholders — it is raw text runs with `<!-- Page N -->` separators. Docling emits
  `## INTRODUCTION`-style headings, pipe tables, and image placeholders (paper 9, Docling:
  `## MEDICAL ROBOTS`, `## INTRODUCTION`).
- **PDF line breaks preserved rather than reflowed.** Docling reflows to paragraph lines
  (~258 chars mean); PyMuPDF keeps physical PDF lines (~37–46 chars). This is harmless for
  anchoring because `_normalize()` collapses all whitespace (`auditor.py:53-59`).
- **Paper 455 is a genuine parse pathology**, not merely a stylistic difference: 8,528 lines
  averaging 6.1 characters, because the journal's vertical spine banner was exploded one
  character per line (`c / o / m / p u / t e / r m / e t h o / d / s`). It also carries 106
  hyphenation line-breaks. It is the worst of the five on untraceable rate (38.9%) — the only
  PyMuPDF paper where a parse defect plausibly contributes.

Two normalization notes relevant to anchoring: ligatures (`ﬁ`, `ﬂ`) survive PyMuPDF but are
folded by the NFKC pass in `_normalize()`, so they cost nothing. Hyphenation breaks are **not**
repaired — `"ori-\nfice"` normalizes to `"ori- fice"` and will not match a snippet containing
`"orifice"` — so the 106 breaks in paper 455 are a real, if small, anchoring hazard.

---

## 6. Is parsing tier a live confound for the arm comparison?

**No — it can be ruled out, on a structural argument rather than a statistical one.** The
decisive fact is not that the tiers score similarly (with 5 papers in the non-Docling stratum,
that test has almost no power); it is that **all three arms read the identical file for every
paper**, enforced by one resolution rule implemented identically in the local extractor, the
shared cloud base class, and the auditor, with every parse written ~42 hours before the first
extraction and no file touched during the extraction window. Parsing quality is therefore a
property of the *paper*, held constant across arms, and cannot explain any between-arm
difference — it cannot be why local produces 22.9% untraceable spans and OpenAI 6.0% on the same
190 papers, nor why 90% of the no-textual-basis snippets are local's. Tier could in principle
inflate the *absolute* untraceable rate for everyone on badly parsed papers, and paper 455 shows
that mechanism is real (8,528 exploded lines, 106 unrepaired hyphenation breaks, 38.9% untraceable
across all arms); but it applies to 5 of 190 papers and 291 of 10,605 snippet-bearing spans, so it
cannot move the corpus-level figure materially. The one finding worth carrying forward is
incidental to the confound question: **the vision tier has never run on this corpus, and for the
174 papers re-parsed by `retry_parse_6.py` it has no code path at all** — so the "three-tier PDF
parsing" described in `CLAUDE.md` is, for Run 6 as executed, a two-tier pipeline, and any future
scanned PDF entering via the retry script would fall through to PyMuPDF rather than to vision.

---

## Provenance

Read-only inputs: `data/surgical_autonomy/review.db` (`mode=ro`), `data/surgical_autonomy/parsed_text/*.md`,
`data/surgical_autonomy/parse_log.txt`, `parse_resume.log`, `docling_parse.log`,
`engine/parsers/pdf_parser.py`, `engine/parsers/models.py`, `engine/core/database.py`,
`engine/agents/extractor.py`, `engine/agents/auditor.py`, `engine/cloud/base.py`,
`scripts/retry_parse_6.py`, and the DIAG-UNANCHOR-01 artifacts `anchor_results.json` / `census2.json`.

Scratchpad artifacts (not committed): `tier_map.json` plus ad-hoc census, cross-tab, and
structural-metric scripts.

Out of scope and not done: any re-parsing, any fix design, Arm P result analysis,
metric-definition changes, primer.md edits.
