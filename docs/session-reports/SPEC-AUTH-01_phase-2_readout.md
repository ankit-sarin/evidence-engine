# SPEC-AUTH-01 phase-2 — recovered read-out

**Provenance header.** This file is a verbatim recovery of one Claude Code
assistant message. Nothing below the header has been edited, summarised or
reformatted.

- **Source transcript:** `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl`
- **Session id:** `4500483d-106b-4e9c-989a-a3314c775cb2`
  - Note: the unified plan v46 attributes this session's work to 2885f251; the work was done in 4500483d, its continuation.
- **Source message line:** L597 (NDJSON record, `type: assistant`)
- **Brief at transcript line:** L169, 2026-09-09T23:42:12.390Z
- **Message timestamp:** 2026-09-10T00:04:42.916Z
- **HEAD stated in read-out:** **HEAD `fa6c738`**, pushed, **local == origin/main** (measured by `ls-remote`), tree clean.
- **Architect acknowledgement:** L601, 2026-09-10T00:09:34.512Z — "RULINGS on SPEC-AUTH-01 Phase 2 report — accepted at HEAD fa6c738."
  *(user-authored; architect turn, not CC output)*
- **Trust:** Transcript-derived. Values in this report that trace to a tool call in the source transcript at the stated HEAD may be cited as MEASURED. Classifications, counts arrived at by reading, and judgements are INFERRED until re-measured.
- **Recovered by:** READOUT-ARCHIVE-01, 2026-09-11, recovering session `410e9ecc-67a0-44c6-9523-ffad8f528ca3`.
  Compiled from transcript `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl` by session 410e9ecc… (READOUT-ARCHIVE-01) on 2026-09-11.
  *(files written 2026-09-12 00:49 UTC; the session opened 2026-09-11 23:29 UTC and crossed midnight)*

---

Phase 2 complete and pushed. Report below.

---

# SPEC-AUTH-01 — PHASE 2 FINAL REPORT

**HEAD `fa6c738`**, pushed, **local == origin/main** (measured by `ls-remote`), tree clean.

## I1 / I2 verification, before any edit

**I2 clean — the named stop condition did not fire.** No off-repo consumer of the spec path: `~/scripts` names only the evidence-engine *log* directory; the crontab's one evidence-engine entry is `scripts/nightly_tests.sh`, which contains no spec reference; no systemd unit mentions the project. The only off-repo hits were Claude Code's own transcript and paste caches.

**🔴 I1 falsified in scope — reported, not adapted around.** The ruling expected "only the sites Phase 1 enumerated plus `test_review_spec.py` and `test_cloud_extraction.py`". The instructed whole-repo grep found **43 files**: 26 test sites, 4 production files Phase 1 had not enumerated (`engine/analysis/normalize.py:44`, `engine/analysis/report.py:25`, `scripts/test_e2e_search_screen.py:29`, `scripts/test_extraction_validation.py:33`), 3 docstring/usage strings, and 6 doc sites. Since the ruling explicitly directed the grep to cover `docs/` and `CLAUDE.md` and C5 said "update every enumerated reference", I treated the widened set as what the grep was for and proceeded — no adaptation of the contract, only a larger reference list.

**A second I1 miss, and it was a live regression rather than a broken reference.** Nineteen entry points did not name the file; they *derived* `review_specs/{review}_v1.yaml` from the review name, so no grep for the old filename could see them. The rename silently invalidated all nineteen. Commit `fa6c738` resolves them by construction. One more, `analysis/paper1/adjudication.py:125`, derived the same path behind an `.exists()` guard — it would have degraded to `spec=None` **silently**, which is exactly the failure class this lane exists to remove, so I fixed the path there despite R1 assigning the file to PATH-AUTH-01. Its hardcoded review name is untouched and stays PATH-AUTH-01's. This is the one deviation from the ruling; flagging it rather than burying it.

## Commits

| commit | files | what |
|---|---|---|
| `e1f27af` | 2 (+144/−21) | **C1.** `_SpecModel` base, `extra='forbid'` on all 16 models. `load_review_spec` wraps `ValidationError` in `ReviewSpecError` and renders the dotted key path. T1: structural pin over every model in the module + one unknown-key case per model. |
| `64d9a6a` | 3 (+96) | **C2.** Required `review_id` (`^[a-z][a-z0-9_]*$`, max 64). Live spec gains `review_id: surgical_autonomy` and nothing else. T2 (15 cases), T6. |
| `fa4a7de` | 41 (+266/−45) | **C3 + C5.** `engine/core/review_paths.py`; `git mv` to `review_specs/surgical_autonomy.yaml`; 38 reference updates; `normalize.py`/`report.py` off literal paths onto the resolver. T3/T4/T5. |
| `fa6c738` | 32 (+331/−235) | **C4.** All 29 entry points on `load_spec_for`; the three R2 reorders; `run_pipeline --review` with `--name` as a deprecated alias; 11 `DEFAULT_REVIEW` constants removed. T7 + 87 entry-point pins. |

## Gates

| gate | result |
|---|---|
| **G1** suite | **2052 passed, 17 deselected in 255.73s** (was 1,894 / 17) — **+158 tests**, zero failures |
| **G2** review.db | `99770368 B @ 2026-09-09 19:48:46.784974707 UTC` — **unchanged**, verified before and after the gate |
| **G3** old filename | Clean in all code and live docs. Remaining: `.gitignore` (names `_original`), `docs/session-reports/` ×5 (historical, per ruling), one explanatory comment in `extraction_cleanup.py` naming `_original`, and **`CLAUDE.md` ×4 — see below** |
| **G4** commits | Four, logically separated, pushed. HEAD `fa6c738` |

**T6 hash pair, pinned as literals and green:**
```
extraction_hash  d311eb20d1f8c9ea47ef8038a18924198348efce49037292723b2c408b9a6790
screening_hash   0d97b9d61161eeca6c81dd82f895bfb8c6f933b8e8ea23f79056a69f0cf98b90
```

**Refusal verified live, outside the suite.** `--review other_review --spec review_specs/surgical_autonomy.yaml` raises `ReviewIdMismatchError` naming both ids and the path; `ls data/` shows **no `other_review` directory was created**.

## 🔴 G3 is not fully met, and it needs your hand not mine

`CLAUDE.md` is yours — the global rule is that I read it and never rewrite it without explicit instruction. C5's generic "update every enumerated reference" does not read as that instruction, so I left it. Seven command blocks in `## Running` are now wrong: four name the renamed file, and three more omit a `--review` that is now required. Replacement text:

```bash
# Full pipeline
python scripts/run_pipeline.py --review surgical_autonomy

# Expanded search screening
python scripts/screen_expanded.py --review surgical_autonomy                # all phases
python scripts/screen_expanded.py --review surgical_autonomy --screen-only  # primary dual-pass only
python scripts/screen_expanded.py --review surgical_autonomy --verify-only  # verification pass only

# PDF acquisition
python -m engine.acquisition.check_oa --review surgical_autonomy
python -m engine.acquisition.pdf_quality_check --review surgical_autonomy

# Full-text screening
python -m engine.agents.ft_screener --review surgical_autonomy

# Cloud extraction
PYTHONPATH=. python scripts/run_cloud_extraction.py --review surgical_autonomy --arm both --max-cost 25.00
PYTHONPATH=. python scripts/run_cloud_extraction.py --review surgical_autonomy --progress

# q8 KV cache validation
PYTHONPATH=. python scripts/q8_validation.py --review surgical_autonomy
PYTHONPATH=. python scripts/q8_validation_fast.py --review surgical_autonomy
```

A `--spec` line is now only needed for a spec kept outside `review_specs/`.

## Remaining for PATH-AUTH-01

**Seven `DEFAULT_REVIEW` constants**, all in name-only entry points: `advance_to_pdf_acquired.py`, `backfill_authors.py`, `backfill_cloud_spans.py`, `monitor_extraction.py`, `parse_expanded_corpus.py`, `prepare_concordance_pdfs.py`, `retry_parse_6.py`. (The eighth hit is my own assertion string in `test_review_paths.py`.)

**Literal review names still in code:** `normalize.py:48` `_FALLBACK_REVIEW_ID` and `report.py:28` (GENERALIZE **B2/B3** — the resolver removed the *path* authority, the hardcoded *review* is unchanged and now carries a comment saying so); `analysis/paper1/adjudication.py`; `migrations/003/004/005`; `analysis/eval/elicit01/{analyze,runner}.py`; `engine/utils/background.py` and `judge_codebook_smoke.py` docstrings.

**The 45 name-only entry points** are untouched, per R1. Twenty of them construct `ReviewDatabase` with no spec in scope, so no identity check is possible there yet.

**The seven `analysis/eval/run_*` harnesses still compose `Path(args.data_root) / args.review`** rather than calling `data_root_for`. Left deliberately: each has an explicit `--data-root` override flag that exists so an eval never writes near the live tree, and none constructs `ReviewDatabase`. Worth folding into PATH-AUTH-01.

**Minor, noted not fixed:** a missing spec surfaces as an uncaught `ReviewSpecError` traceback rather than a clean `sys.exit`. Pre-existing behaviour of `load_review_spec`; the message itself is clear.

## Debt notes (C6)

1. **`authors` and `prospero_id` are declared and never read.** Retained as metadata for the methods exporter. Now enforced by `extra='forbid'`, so they are load-bearing as *schema* even while dead as *behaviour* — removing them would break the live spec.
2. **`distribution_monitor` is declared and never read** (MONITOR-WIRE-01). A review that tunes `collapsed_min_papers` or `low_variance_threshold` in YAML still changes nothing. Worse than before in one respect: strictness now guarantees the key is *accepted*, which reads as confirmation that it took effect. Sharpest item in the lane.
3. **Additionally:** the live spec declares 17 of the 22 fields. `extraction_models`, `auditor_model`, `cloud_models`, `pdf_parsing` and `distribution_monitor` are all absent and run on Pydantic defaults — including the whole parse-quality gate, whose defaults are pinned against the engine by test.

## `_original.yaml`, reported once as instructed

`review_specs/surgical_autonomy_v1_original.yaml` — 4,445 B, untracked, gitignored at `.gitignore:8`, referenced by nothing. Not touched, not deleted, no `review_id` added; it would now fail to load. Its one live effect was removed in passing: `extraction_cleanup`'s `find_review_spec` globbed `{review}*.yaml` and took the first lexicographic match, which after the rename matched **both** it and `surgical_autonomy.yaml`. That glob is deleted. Your call on the file.

---

Stopping here. Not starting CODEBOOK-AUTH-01 or INVENTORY-01; awaiting the next brief or ruling.
