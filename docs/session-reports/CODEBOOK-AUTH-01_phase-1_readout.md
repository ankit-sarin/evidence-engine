# CODEBOOK-AUTH-01 phase-1 — recovered read-out

**Provenance header.** This file is a verbatim recovery of one Claude Code
assistant message. Nothing below the header has been edited, summarised or
reformatted.

- **Source transcript:** `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl`
- **Session id:** `4500483d-106b-4e9c-989a-a3314c775cb2`
  - Note: the unified plan v46 attributes this session's work to 2885f251; the work was done in 4500483d, its continuation.
- **Source message line:** L935 (NDJSON record, `type: assistant`)
- **Brief at transcript line:** L773, 2026-09-10T00:37:58.545Z
- **Message timestamp:** 2026-09-10T00:46:46.495Z
- **HEAD stated in read-out:** **HEAD `04a4c64`**, pushed, local == origin/main, tree clean.
- **Architect acknowledgement:** L954, 2026-09-10T20:37:40.483Z — "RULINGS on CODEBOOK-AUTH-01 Phase 1 — acknowledged at HEAD 04a4c64. Proceed to Phase 2."
  *(user-authored; architect turn, not CC output)*
- **Trust:** Transcript-derived. Values in this report that trace to a tool call in the source transcript at the stated HEAD may be cited as MEASURED. Classifications, counts arrived at by reading, and judgements are INFERRED until re-measured.
- **Recovered by:** READOUT-ARCHIVE-01, 2026-09-11, recovering session `410e9ecc-67a0-44c6-9523-ffad8f528ca3`.
  Compiled from transcript `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl` by session 410e9ecc… (READOUT-ARCHIVE-01) on 2026-09-11.
  *(files written 2026-09-12 00:49 UTC; the session opened 2026-09-11 23:29 UTC and crossed midnight)*

---

Read-out complete. Report below.

---

# CODEBOOK-AUTH-01 — PHASE 1 READ-OUT

**HEAD `04a4c64`**, pushed, local == origin/main, tree clean. No engine code, codebook, spec or `review.db` was touched; the only write was R3's move and the regenerated inventory.

## Assumption verdicts

| | verdict |
|---|---|
| **I1** | **TRUE on names and order, FALSE on "overlapping but not identical" being harmless** — 20 fields, same set, same order, `tier` agreeing on all 20 and `valid_values` on all 8 categoricals. But `type` is in **both** and **disagrees on 9 of 20**, one of them semantically. Neither is derived from the other. |
| **I2** | **TRUE** — `check_evidence_modality` on the live codebook returns **zero findings**. |
| **I3** | **TRUE** — `hashlib.sha256(Path(path).read_bytes()).hexdigest()`. A loader change cannot move it. |

**One MEASURED figure did not survive: "13 raw yaml load sites, 12 of them codebook readers" is wrong by one.** Of 13, **11** read the codebook, one is `load_review_spec` itself (`engine/core/review_spec.py:476`) and one reads a *category* config (`engine/adjudication/categorizer.py:45`) — a different file entirely.

---

## S1 — Every codebook reader

**Eleven raw-YAML readers.** None validates a schema; each reaches for the keys it happens to need.

| file:line | locates the file by | reads | validates |
|---|---|---|---|
| `engine/agents/extractor.py:47` `_load_codebook` | caller's path; `lru_cache(maxsize=4)` | whole doc | **no** — bare `yaml.safe_load` |
| `engine/agents/extractor.py:53-59` `_find_codebook_path` | `review_dir/extraction_codebook.yaml`, else **`Path("data").glob("*/extraction_codebook.yaml")` returning the first hit**, else `FileNotFoundError` | — | no |
| `engine/core/completeness.py:135` | explicit path arg | `fields[].name` only | **no, and swallows** — `except Exception → return None` (:139) |
| `engine/validators/distribution_monitor.py:55` | `args.codebook or data_dir/extraction_codebook.yaml` (:450) | `fields[].name` where `type=="categorical"` | no |
| `analysis/paper1/adjudication.py:37` | `args.codebook or data_dir/…` | `{name: type}` — **`fd["type"]` unguarded** | no |
| `analysis/paper1/consensus.py:54` | `args.codebook or data_dir/…` (:433) | `{name: type}` — unguarded | no |
| `analysis/paper1/human_import.py:137` | explicit path | categorical `valid_values[].value`, lowercased | no |
| `analysis/eval/adjud01_pairs.py:52` | explicit path | `{name: type}`, `.get(…,"unknown")` | no |
| `analysis/paper1/pi_audit_sampler_v2.py:217` | `--codebook` (required) | `judge_rubric_family`, `definition`, `instruction`, value enumeration | no — skips non-dict nodes |
| `analysis/paper1/judge_codebook_smoke.py:223` | `--codebook` (required) | raw field dicts | no |
| `analysis/paper1/judge_loader.py:99` `load_codebook` | explicit path | `name`, `type`, `definition`, `valid_values`, `tolerance` | **YES — the only eager validator.** Raises `LoaderError` on YAML error, missing `fields`, non-mapping entry, missing `name`, missing `type`, missing `definition`, unknown type, non-numeric tolerance |
| `analysis/paper1/judge_loader.py:151` `load_raw_codebook` | explicit path | raw dicts | partial — YAML error + missing `fields` only |

**Five resolution strategies, no single authority:** the `review_dir` join (`extractor.py:53`, `elicitation/pipeline.py:79`, `auditor.py:272`, `concordance.py:78`, `run5_extract_and_audit.py:149`, `extractor.py:544`, `extractor.py:657`); the **`data/` glob first-hit** (`extractor.py:57`); `args.codebook or data_dir/…`; a required `--codebook`; and a module constant (`scripts/_pass2_stability.py:27`).

**The elicitation accessors** (`engine/elicitation/classes.py`) are the second reader family — they take a *parsed dict*, so validation is per-accessor and lazy:

| accessor | line | behaviour |
|---|---|---|
| `load` | :84 | shares `extractor`'s cache; **no validation at all** |
| `escape_token` | :90 | **raises** `CodebookContractError` if absent/blank (:93) |
| `contract_unmet_token` | :105 | **raises** (:115) |
| `classes_by_field` | :149 | **raises** on unknown `field_class` values (:167) |
| `field_class` | :174 | **raises** on an unknown field name (:178) |
| `fields_by_class` | :181 | **raises** `ValueError` on an unknown class (:184); then `codebook["fields"]` and `f["name"]` unguarded (:186) |
| `absence_sentinels` | :100 | **silent** — `.get("absence_sentinels", ())`, empty set if absent |
| `non_value_tokens_for` | :189 | **swallows everything** — `except Exception → frozenset()` (:205-206), documented as deliberate read-side tolerance for legacy reviews |

`non_value_tokens_for` is the widest-reaching: called by `auditor.py:272`, `extraction_validator.py:64`, `distribution_monitor.py:177`, `concordance.py:77`. On a missing or malformed codebook all four degrade to an empty token set **silently**.

---

## S2 — `spec.extraction_schema` vs codebook

**Structure agrees.** 20 fields, identical name set, identical order, `tier` identical on all 20, `valid_values` identical on all 8 categoricals.

**Attributes overlap partially:**

- spec `ExtractionField`: `name, type, tier, description, enum_values`
- codebook union: `name, type, tier, definition, instruction, field_class, judge_rubric_family, valid_values, decision_criteria, dimension, examples, ordered_values`
- in *every* codebook field: `name, type, tier, definition, instruction, field_class, judge_rubric_family`
- **spec-only:** `description`, `enum_values` · **codebook-only:** the nine prompting/judging attributes, `field_class` chief among them

**🔴 `type` is duplicated across the two authorities and disagrees on 9 of 20 fields.**

| | spec | codebook | fields |
|---|---|---|---|
| vocabulary mismatch | `text` | `free_text` | 8 fields |
| **semantic mismatch** | **`text`** | **`numeric`** | **`sample_size`** |

Eight are a vocabulary split (`text` vs `free_text`) that no code reconciles. The ninth is a real disagreement: **`sample_size` is `text` to the spec and `numeric` to the codebook**, and which one a consumer sees depends purely on which file it opened. `judge_loader._parse_field_type` (:64) validates against `_VALID_FIELD_TYPES` — the *codebook's* vocabulary — so the spec's `text` would raise there.

**Consumers by source:**

- **spec `extraction_schema`** — `extractor.py:302` and `cloud/base.py:35` for the schema hash, `extraction_validator`, `completeness.expected_field_names`, `normalize._get_field_def`, `report._get_tier_map`, the exporters
- **codebook** — the entire prompt path (`_build_field_block`, all of `engine/elicitation/`), `distribution_monitor`, `human_import`, all Paper-1 judge and audit tooling
- **both** — `completeness.py:98-123`, the only place that cross-checks them, and it logs a divergence and **follows the spec** ("Guard follows the spec because that is what the prompt asks for")

**`check_schema_parity` does not compare these two at all.** `engine/analysis/concordance.py:158-200`:

> ```python
> def check_schema_parity(db_path: str, arms: list[str]) -> dict[str, set[str]]:
>     """Verify all arms used the same extraction schema hash.
>     Queries distinct extraction_schema_hash values for each arm from the
>     local ``extractions`` and ``cloud_extractions`` tables.
>     Returns ``{arm: {hash, ...}}`` mapping.  Logs a WARNING if hashes differ
>     across arms, but does not block execution.
>     """
> ```

It compares **stored hashes arm-to-arm**, all of them derived from the spec, and warns without blocking. The codebook is not an input. The architecture doc's "verifies hash consistency" is true and narrower than it reads.

---

## S3 — Provenance

**Codebook hash** — `analysis/paper1/judge_loader.py:59-61`:

> ```python
> def compute_codebook_sha256(path: Path) -> str:
>     """SHA-256 hex digest of the codebook file bytes (no normalization)."""
>     return hashlib.sha256(Path(path).read_bytes()).hexdigest()
> ```

Stored in **`judge_runs.codebook_sha256 TEXT NOT NULL`** (`engine/migrations/007_add_judge_tables.py:36`), written by `analysis/paper1/judge_storage.py:64,73`. Callers are all Paper-1: `judge_cli:222`, `pass1_inspection:591`, `pass2_smoke:851`, `pass2_full:727`, `judge_codebook_smoke:1189`, `pass2_retry_single`, `_pass2_stability`.

**Extraction-schema hash** — `spec.extraction_hash()` (`review_spec.py`), assigned at `engine/agents/extractor.py:302` / `:785` and `engine/cloud/base.py:35`, stored in `extractions.extraction_schema_hash` and `cloud_extractions.extraction_schema_hash`, plus `review_runs.{review_spec_hash, screening_hash, extraction_hash}`.

**Can a loader change move either? No — and that is the problem, not the reassurance.**

- `codebook_sha256` hashes **file bytes**, so no loader change touches it. **I3 TRUE.**
- `extraction_schema_hash` hashes `spec.extraction_schema.model_dump()`, so no *codebook* loader change touches it either.

**🔴 The consequence: no value in the extraction provenance chain covers the codebook.** The prompt is built from the codebook; the recorded hash comes from the spec. **The codebook can be edited — a definition, an instruction, a `field_class`, a `valid_values` entry — and every existing extraction's provenance stays byte-identical, so staleness detection does not fire and `check_schema_parity` stays silent.** `codebook_sha256` exists and would have caught it, but it is wired only into `judge_runs`, never into `extractions`.

---

## S4 — Lint and error posture

```
check_evidence_modality(live codebook) → 0 findings
EVIDENCE_MODALITY_FLAGS  = ('source_quote_required',)
_MODALITY_COMPATIBLE     = {'source_quote_required': ['stated']}
field_class distribution = {'stated': 9, 'inferable': 6, 'judgment': 5}   (20 classified)
```

**I2 TRUE.** The 9/6/5 split matches `CLAUDE.md`'s declared STATED(9)/INFERABLE(6)/JUDGMENT(5).

The **zero-production-caller** finding holds: the only callers are `tests/test_codebook_evidence_modality.py` (lines 44, 62, 79, 86, 137). The lint runs in the suite and nowhere in the pipeline. Its own docstring states the intent — "A LINT rather than a load-time raise: it describes a codebook that will elicit worse answers, not one the engine cannot run against."

Raise/swallow inventory is the `classes.py` table in S1. In short: **five accessors raise** (`escape_token`:93, `contract_unmet_token`:115, `classes_by_field`:167, `field_class`:178, `fields_by_class`:184) and **three are silent** (`load`:84 — no validation; `absence_sentinels`:100 — empty default; `non_value_tokens_for`:205 — bare `except Exception`), plus `completeness._codebook_field_names`:139, which swallows and returns `None`.

---

## S5 — Identity

```
codebook top-level keys : absence_sentinels, contract_unmet_token, date, escape_token, fields, review, version
codebook['review']      : 'surgical_autonomy'
spec.review_id          : 'surgical_autonomy'
EQUAL                   : True
```

**They match — and the key is read by nothing.** A repo-wide grep for `["review"]` / `.get("review")` against a codebook dict returns **zero** hits in `engine/`, `scripts/`, `analysis/` and `tests/`.

So the codebook is in exactly the position the spec was in before SPEC-AUTH-01: it declares its review, and no code checks that declaration against the review it is being used for. The `data/` glob in `extractor.py:57` is the live consequence — it takes the **first** `data/*/extraction_codebook.yaml` in glob order with no identity check, so on a two-review box it can hand one review's codebook to another review's extraction and nothing raises.

---

## S6 — The authority question

**Neither file is derived from the other, and the consumers imply opposite directions.** The spec owns what *validation* needs — `type`, `enum_values`, `tier` — and is the only input to the hash that provenance records; the codebook owns what *prompting and judging* need — `definition`, `instruction`, `field_class`, `judge_rubric_family`, `valid_values` — and is the only input to the text the model actually sees. Each is authoritative for its own consumers and neither is reproducible from the other: `field_class` and the prompting attributes exist nowhere in the spec, while `description` and `enum_values` exist nowhere in the codebook, and their one genuinely shared attribute, `type`, already disagrees on nine fields with `sample_size` disagreeing in substance rather than spelling. **The direction the consumers imply is codebook-as-source and spec-as-derived** — the codebook is a strict superset on the shared attributes, it is what the prompt is built from, and it is the file a new review would actually have to author — but taking that direction breaks the thing that makes the spec authoritative: **`extraction_schema_hash` is `spec.extraction_hash()`, so deriving the spec's schema from the codebook makes every stored provenance value a function of a file nothing currently hashes into it, and every existing extraction's hash would move the first time the generator ran.** Deriving the other way is worse — it would have to invent nine attributes the spec does not carry. The genuine third option is to keep both files and make the *relationship* enforced rather than assumed: one loader, eager validation, an identity check, a single reconciled `type` vocabulary, and the codebook's digest recorded alongside the schema hash in `extractions` so a codebook edit is visible in provenance. **I have not drafted that; it is Phase 2 and awaits your ruling.**

---

## R3 — Housekeeping (G2)

**I1 verified before moving, and it failed for four of the six.**

Contents as found:

| directory | contents |
|---|---|
| `data/test_verify` | `review.db` 131,072 B (2026-03-13 05:37:24), `review.db-shm` 32,768 B (2026-08-25 05:53:51), `review.db-wal` 0 B, `pdfs/`, `parsed_text/`, `vector_store/` |
| `data/data` | `data/surgical_autonomy/review.db/` **as a directory**, holding `review.db` 364,544 B (2026-07-27 22:48:49), `review.db-shm` 32,768 B, `review.db-wal` 0 B, `pdfs/`, `parsed_text/`, `vector_store/` |
| `data/my_review` | `logs/` only (2026-03-08 16:05:12) |
| `data/myreview` | `logs/` only (2026-03-08 16:05:12) |
| `data/review` | `logs/` only (2026-03-08 16:05:12) |
| `data/test_review` | `logs/` only (2026-03-08 16:05:12) |

The two `-shm` files dated 2026-08-25 05:53 are the snapshot lane opening these databases, consistent with M7's WAL finding.

**Moved:** `data/test_verify` and `data/data` → `data/backups/debris_2026-09-09/`, structure preserved, nothing deleted. `review_ids` is now exactly `['surgical_autonomy']`.

**🔴 Not moved, and not movable as specified: the four `logs`-only directories.** `engine/utils/background.py:48-49` is `log_dir = Path("data") / review_name / "logs"` followed by `mkdir(parents=True, exist_ok=True)` — it hardcodes `Path("data")` and ignores any `data_root`. `tests/test_background.py` calls `maybe_background` with `review_name="myreview"` (:63), `"test_review"` (:92), `"my_review"` (:111) and three times with no `review_name` at all, whose default is `"review"` (`background.py:23`). That is exactly the four, created within 3 ms of each other in one 2026-03-08 session and recreated on every suite run since. The test takes a `tmp_path` fixture it cannot use, because the function it calls does not accept a root.

Moving them would have been undone by the next `pytest`, and — because the drift test reads `data/*/` — it would have gone **red inside the same invocation meant to confirm it green**, making G2 unachievable. `background.py` is engine code and out of scope here, so I reported instead of forcing it. **This is a second live directory-creator that R4's DB-CREATE-01 does not name** (that ruling scopes `ReviewDatabase`); the logging path needs the same treatment.

**Drift confirmed green after a full gate run** — the four are recreated but are already in the committed inventory's exclusion set, so `--check` reports `inventory in sync` both before and after.

Commit **`04a4c64`** `chore(data): retire debris directories; regenerate inventory`, pushed.

## Gates

| gate | result |
|---|---|
| **G1** | S1–S6 delivered with `file:line`; I1 partly false (documented in S2), I2 true, I3 true |
| **G2** | R3 done for two of six with the reason for the other four; inventory regenerated; drift green post-gate; **2070 passed, 17 deselected in 259.15s** |
| **G3** | `review.db` = `99770368 B @ 2026-09-09 19:48:46.784974707 UTC` — **unchanged** |

---

Stopping here. Phase 2 not drafted. Awaiting the next brief or ruling.
