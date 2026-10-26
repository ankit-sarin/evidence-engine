# 12x-AQA — Phase A (read-only): audit's field-type variants and pdf_quality's hashing (R712(f))

Session 12x, 2026-10-26 UTC. Item 0 `6cb6ad62cefad6a6562f11fd31df635160b11193` (R713, R714 as Blocks
111–112). Engine and tests on main unchanged since `6368069`. No model call: every capture replaces
`ollama_chat` at the name the call site looks it up by. One live `mode=ro` read (P5) between two
IDENTICAL fingerprints. No fix, no row, no note on a row.

## A — audit's field-type variants

### A1. The builder (`engine/agents/auditor.py`, `def build_audit_messages`), in full

```python
def build_audit_messages(span: EvidenceSpan, field_type: str = "text") -> list[dict]:
    """The message list `semantic_verify` sends. One builder for the call and
    for the resolver's prompt hash (R60)."""
    if field_type == "categorical":
        verification_question = (
            f"Does the source snippet provide sufficient evidence to classify "
            f"this paper as '{span.value}' for the field '{span.field_name}'?\n"
            f"Note: This is a categorical/classification field. The exact phrase "
            f"'{span.value}' does NOT need to appear verbatim in the text. "
            f"The question is whether the described content reasonably supports "
            f"this classification."
        )
    else:
        verification_question = (
            f"Does the extracted value accurately represent what the source snippet states?\n"
            f"Consider:\n"
            f"- Is the value factually supported by the snippet?\n"
            f"- Is there any misinterpretation or hallucination?\n"
            f"- Is the value a reasonable extraction for this field?"
        )

    prompt = f"""/no_think
You are an audit agent verifying data extraction from a scientific paper.

Field: {span.field_name}
Field type: {field_type}
Extracted value: {span.value}
Source snippet from paper: {span.source_snippet}

{verification_question}

Respond with JSON: {{"status": "verified" or "flagged", "grep_found": true, "reasoning": "..."}}"""

    return [
        {"role": "system", "content": ("You are an audit agent verifying data extractions from "
            "scientific papers. For free-text fields, be strict: flag anything "
            "not clearly supported by the source snippet. For categorical fields, "
            "verify that the source text reasonably supports the chosen category — "
            "the category label does not need to appear verbatim. "
            "Respond ONLY with JSON.")},
        {"role": "user", "content": prompt},
    ]
```

(The system message is quoted with its string pieces joined onto fewer lines; its text is unchanged.)

There is **one branch**, `if field_type == "categorical":` / `else:`. Each side sets only `verification_question`:
- **Categorical:** a classification question carrying `span.value` (twice) and `span.field_name`.
- **Otherwise:** a fixed five-line value-support question with no substituted value.

The system message and the rest of the prompt are the same on both sides. The prompt substitutes four
values: `span.field_name`, `field_type`, `span.value` and `span.source_snippet`. `build_audit_messages`
calls nothing.

### A2. Where `field_type` comes from, and the render

- **The runtime:** `engine/agents/audit_events.py:166` reads
  `types = {f["name"]: f.get("type", "text") for f in codebook.fields}`, and `:234` reads
  `v = semantic_verify(span, field_type=types.get(c.field_name, "text"), cfg=cfg, paper_id=pid)`.
  `semantic_verify` passes it through unchanged (`messages=build_audit_messages(span, field_type=field_type)`, auditor.py:73).
  The legacy `audit_span(..., field_type: str = "text")` (auditor.py:134) passes its own argument; its callers are
  tests only.
- **The render** (`engine/core/effective_config.py`, `render_messages`):
  ```python
      if stage == "audit":
          from engine.agents.auditor import build_audit_messages
          from engine.agents.models import EvidenceSpan
          span = EvidenceSpan(field_name="f", value="v", source_snippet="s", confidence=0.5, tier=1)
          # Both field-type variants: the audit template branches on it.
          return (build_audit_messages(span, field_type="text")
                  + build_audit_messages(span, field_type="categorical"))
  ```
  The two variants are literals in the render. `"text"` is the builder's default argument and
  `audit_events`' fallback, not a codebook value.
- **Is `"text"` a codebook type? No.** `engine/core/codebook.py:43` declares
  `VALID_FIELD_TYPES = ("categorical", "numeric", "free_text")`. The loader refuses any other value
  (`:312` `if f["type"] not in VALID_FIELD_TYPES: raise CodebookError(...)`) and requires the key
  (`REQUIRED_FIELD_KEYS = ("name", "type", "tier", ...)`, `:54`). So `audit_events`' `f.get("type", "text")` default
  cannot be reached through a loaded codebook. No live field carries `"text"`: the live types are categorical 11,
  free_text 8 and numeric 1.

### A3. Runtime vs render, per live field

Harness: `a3/a3_capture.py`. It reuses U39's audit harness from `tests/test_u39_runtime_equals_hash.py` (`_Sent`, the
raising `_fake`, the call to `semantic_verify`) outside pytest. For each field it uses the runtime's own `field_type` expression,
the render's sentinel value and snippet ("v", "s"), and the field's real name. The full table is `a3/a3_table.md`, with
per-field sha256 values in `a3/a3_rows.json`.

| field | codebook type | branch | (i) identical to a variant | (ii) after the type value is replaced | (ii′) name also replaced | (iii) first difference after (ii) |
|---|---|---|---|---|---|---|
| study_type, surgical_domain, autonomy_level, validation_setting, task_monitor, task_generate, task_select, task_execute, system_maturity, study_design, clinical_readiness_assessment | categorical | categorical | False (11) | False (11) | **True (11)** | `Field: <name>` against `Field: f`, @475 |
| robot_platform, task_performed, country, primary_outcome_metric, primary_outcome_value, comparison_to_human, secondary_outcomes, key_limitation | free_text | else ("text") | False (8) | False (8) | **True (8)** | `Field: <name>` against `Field: f`, @475 |
| sample_size | numeric | else ("text") | False (1) | False (1) | **True (1)** | `Field: <name>` against `Field: f`, @475 |

- **Counts:** categorical 11 → (i) 0, (ii) 0, (ii′) 11; free_text 8 → 0, 0, 8; numeric 1 → 0, 0, 1. All 20 fields are covered.
- **Reading.** Column (ii) is never true, for a reason the brief's column cannot show: with the field's real name, the
  name is a second substituted value, and it differs from the render's `f`. Column (ii′) adds that one replacement, and
  then **all 20 fields equal their branch's rendered variant byte for byte**. So the runtime's messages differ from the
  render only in substituted values (name, value, snippet and the field-type string). No field reaches template text the
  render does not carry. The 9 free_text and numeric fields reach the else-branch, which is rendered under the string
  `"text"`; the runtime writes `free_text` or `numeric` in that one value slot.

### A4. Options, each applied alone in a worktree at `6368069` and reverted

The hash is computed as the manifest computes it: `resolve_run` → `prompt_hash("audit", spec, stage_config("audit", spec))`
→ `sha256_canonical({"messages": render_messages(...), "format": ...})` (effective_config.py `def prompt_hash`).
Requests were captured for three fields, one per type. The U39 test is `tests/test_u39_runtime_equals_hash.py::test_audit_runtime_equals_hash`,
run in the worktree. Pins come from `12l_U04/pin_tuples.py`. Driver: `a4/options.py`; per option a diff, a measurement JSON and the
test output; `a4/summary.json`.

| option | audit prompt_hash | request bytes changed? | U39 audit test | pins | edit size |
|---|---|---|---|---|---|
| O0, no change | `d972c46dde944f1381c4f2d5472c6264c3a1053bd23bdd3801bdf0fdc2a5838d` (= B23's recorded `d972c46d…`) | — (baseline: study_type `441c7b9c…`, robot_platform `2242aade…`, sample_size `eda46910…`) | passes | elicited `d70bf688…59f0`, non-elicited `d3672ab6…ff5c` | 0 |
| O1, the render's variants from `VALID_FIELD_TYPES` (3 variants: categorical, numeric, free_text) | `a1d355d443cf404a7b8725cadd0d4bfe48b13c194b6ad13edad9a144869ba491` | **no** (all three request hashes as O0) | **fails**: `variant count: runtime calls vs rendered variants` / `assert 2 == 3` | unchanged | 1 file, 2 + / 2 − |
| O2, one variant per builder branch, the type value a placeholder `"<field_type>"` in the else variant | `86098ebd12e0f0dfe5fc7974280746103bf017aa6ee7d7e2f7cf325ad5ef2c49` | **no** | **fails**: the test sends `field_type="text"`, the render now reads `Field type: <field_type>` | unchanged | 1 file, 1 + / 1 − |

Neither O1 nor O2 changes a request byte; both move audit's manifest `prompt_hash` and no pin (R712(e)). Both would need
U39's audit test changed in the same commit: O1 to send one call per codebook type, O2 to send the placeholder. Under
O1 the test's runtime calls would be three, and the categorical one would match only if the order followed
`VALID_FIELD_TYPES`.

Described, not built:
- **O3:** the caller maps the codebook type to the builder's two branch values (`"categorical"` / `"text"`) in `audit_events`.
  That changes the request bytes for the 9 non-categorical fields (`Field type: free_text`/`numeric` → `text`), so it
  is a change to what is sent; no hash moves.
- **O4:** the `Field type:` line is removed from the prompt, or the branch is passed as a boolean. That changes request
  bytes for every field and moves the hash.

The worktree was removed after the last revert (`worktree_after.txt`: the main worktree only); main is clean.

### A5. Rows that already name audit's prompt, its hash or `field_type`

| ID | sentence (quoted) |
|---|---|
| INT-w12-1 | Its text: "So a message appended, dropped or reordered at the call site, after the builder returns, is invisible to its prompt_hash at every stage."; its 12x-U39B note: "Audit's variant set stays open: the runtime sends the codebook's type as the field type (live: categorical 11, free_text 8, numeric 1), while the render's two variants use "text" and "categorical" … a read-only Phase A decides it (R712(f))." |
| B23 | "the cross-family auditor judges an unlocated claim from snippet, value and field only." … "The request is byte-identical (audit `prompt_hash` `d972c46d…`, both pins unchanged)." |
| C31 | "*The auditor's `ollama_chat` call records no `paper_id` on `run_calls`* (`semantic_verify` passes none)" (CLOSED 2026-09-29) |
| C52 | "`semantic_verify` (`audit`), `_classify_page` (`pdf_quality`) each fall back to `stage_config(<stage>)` without the spec when called without cfg." (CLOSED 2026-10-01) |
| C54 | "Under an active run a `model=` override at `semantic_verify`, `parse_with_vision` or `_classify_page` would send a model the manifest did not declare" … "No current caller passes `model=` to `semantic_verify` or `audit_span`." (CLOSED 2026-10-06) |
| B8 | "*The semantic auditor's verdict has no table.* Each `semantic_verify` verdict is run-linked telemetry" |

No row names the field-type string or `VALID_FIELD_TYPES` against the render.

## P — pdf_quality

### P1. Callers, by AST (engine/, scripts/, analysis/, tests/)

```
engine/acquisition/pdf_quality_check.py:106 call classification_messages     (in _classify_page)
engine/acquisition/pdf_quality_check.py:218 call _classify_page              (in run_quality_check)
engine/acquisition/pdf_quality_check.py:281 call run_quality_check           (in main)
engine/core/effective_config.py:456 import … classification_messages; :457 call (render_messages' branch)
tests/test_extraction_signatures.py:57 call _classify_page
tests/test_request_capture.py:306 call _classify_page
```

The only entry point is the module's own CLI, `python -m engine.acquisition.pdf_quality_check --review <id> [--dry-run]
[--limit N]` (`main`, under `if __name__ == "__main__"`). No script, no `run_pipeline` stage (its `STAGES` are
`search, screen, parse, extract, audit, export`), and no crontab line (`crontab -l | grep -i quality`: empty) calls it.
`engine/acquisition/pdf_quality_html.py` and `pdf_quality_import.py` read and write its columns but make no model call.

### P2. Under a manifest?

| entry point | manifest? | refused under a run? | run_calls row? |
|---|---|---|---|
| `python -m engine.acquisition.pdf_quality_check` (`main` → `run_quality_check` → `_classify_page` → `ollama_chat`) | **no**: `main` takes `hold_experiment_lock()` and calls `run_quality_check`, which opens `sqlite3.connect(str(db_path))` directly and never calls `open_run` / `active_run` | not applicable as run; it **would be** refused if called inside an active run, because no manifest declares `pdf_quality` | **no** |

- **Read:** `ollama_chat` calls `check_declared_call(stage, model)` (ollama_client.py, "C54: under an active run, a call
  the manifest did not declare is refused here"). `check_declared_call` returns at once when `_ACTIVE.get()` is None
  ("A no-op outside a run"). `record_active_ollama_call` is "A no-op outside a run". Inside an active run, the lookup
  `SELECT model_name FROM run_stage_configs WHERE run_id = ? AND stage = ?` finds no `pdf_quality` row, and
  `UndeclaredCall` is raised before anything is sent.
- **Exercised, cheap:** `check_declared_call("pdf_quality", "qwen2.5vl:7b")` outside a run returns `None`
  (`p2_exercise.txt`). The in-run refusal was read, not exercised.
- `pdf_quality` is in `OLLAMA_STAGES` (effective_config.py:57), so a manifest *could* declare it, but none does.

### P3. What it writes

- **One write site**, `run_quality_check` (pdf_quality_check.py:241), skipped under `--dry-run`:
  ```python
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
  ```
  The connection is a raw `sqlite3.connect`, not `ReviewDatabase`. It writes no event, no `papers.status` and no
  `updated_at`. No model, digest, prompt or run is stored beside the classification. The model's `note` is printed in the
  CLI report and not stored. There are no files and no telemetry.
- **Downstream readers and writers** of these columns: `pdf_quality_html.py` (sections select `AI_CHECKED` /
  `HUMAN_CONFIRMED`) and `pdf_quality_import.py:228`
  `UPDATE papers SET pdf_quality_check_status = 'HUMAN_CONFIRMED' WHERE id = ?` (and its PDF_EXCLUDED path,
  INT-g6-2).
- **Selection** (`_get_papers_to_check`): papers with a PDF path, status not `ABSTRACT_SCREENED_OUT` / `REJECTED` / `PDF_EXCLUDED`,
  and `pdf_quality_check_status` not `AI_CHECKED`.

### P4. The render's branch

```python
    if stage == "pdf_quality":
        from engine.acquisition.pdf_quality_check import classification_messages
        return classification_messages(SENTINEL_IMAGE)
```

The only path that reaches it is `prompt_hash` (rendered from `resolve_run` alone) for a declared stage. No manifest
declares `pdf_quality`, and no test renders it (`grep -rn "render_messages(" tests` lists `test_prompt_hash_coverage.py` only, whose stages
exclude it). So the branch is unreached at runtime and in the suite.

### P5. The live read (`mode=ro`, 04:37:19Z, outside 07:00–10:35)

Script `p5/p5_counts.py`; output `p5/p5_output.txt`. Fingerprint `--compare` before and after: **IDENTICAL**, exit 0, both.

```
papers 10039
-- pdf_quality_check_status: None 9658; 'HUMAN_CONFIRMED' 364; 'AI_CHECKED' 17
-- pdf_ai_content_type: None 9658; 'full_manuscript' 364; 'conference_poster' 9; 'other' 8
-- pdf_ai_language: None 9658; 'English' 354; 'Chinese' 9; 'German' 7; 'Spanish' 5; 'French' 2; 'Japanese' 2; 'Korean' 1; 'Russian' 1
-- pdf_ai_confidence: non-null 381, min 0.9, max 0.95, mean 0.949
-- AI fields set but status not AI_CHECKED/HUMAN_CONFIRMED: 0
-- status x pdf_quality_check_status: AI_AUDIT_COMPLETE/HUMAN_CONFIRMED 190; FT_SCREENED_OUT/HUMAN_CONFIRMED 171;
   PDF_EXCLUDED/AI_CHECKED 17; ABSTRACT_SCREENED_OUT/HUMAN_CONFIRMED 2; PDF_EXCLUDED/HUMAN_CONFIRMED 1
-- latest updated_at per pdf_quality_check_status (not the classifier's time): AI_CHECKED max 2026-03-14T00:11:50Z;
   HUMAN_CONFIRMED max 2026-03-18T20:52:58.786876+00:00
```

381 papers carry a classification, and no row records which model made it, or when. The nearest timestamp the rows
carry is `updated_at`, which the classifier does not write; its latest values are 2026-03-14 and 2026-03-18.

### P6. Rows that already name pdf_quality, `_classify_page`, the quality check or its review page

| ID | the sentence that names it (quoted) |
|---|---|
| **INT-j12-15** | "`pdf_quality_check` makes model calls outside any run manifest; the stored classification has no model, digest, prompt or run (W1 adjacent item 7)". Class "1 (conditional … Condition, as the read-out states it — evidence level: "code order; conditional (the quality-check CLI is run)")", package P4 |
| C52 | "`_classify_page` (`pdf_quality`) each fall back to `stage_config(<stage>)` without the spec when called without cfg." (CLOSED 2026-10-01) |
| C54 | "Under an active run a `model=` override at `semantic_verify`, `parse_with_vision` or `_classify_page` would send a model the manifest did not declare" (CLOSED 2026-10-06) |
| D20 | "`scripts/parse_expanded_corpus.py` (`update_status`, selecting `ABSTRACT_SCREENED_IN` with `pdf_quality_check_status = 'HUMAN_CONFIRMED'`)"; its note: "selects `pdf_quality_check_status = 'HUMAN_CONFIRMED'`, but papers that pass the AI check stay `AI_CHECKED` — an L17 file". |
| J1 | its note: "Wave 2 row 13 (INT-j12-10): "A paper whose AI check failed (or never ran) is on no section of the quality review page;" |
| INT-g6-2 | "`pdf_quality_import.import_dispositions` validates only that the paper exists (any status → terminal exclusion, no event, no manifest)" |
| INT-i12-74 | "`pdf_quality_check` declares a private `DATA_ROOT = Path("data")`" |
| INT-j12-9 | "`engine/acquisition/pdf_quality_check.py` — `_get_papers_to_check`, `run_quality_check`;" |
| INT-j12-10 | "`engine/acquisition/pdf_quality_check.py::run_quality_check`; … `engine/acquisition/pdf_quality_html.py::_query_section1/2/3`, `generate_quality_html`;" |
| INT-i12-77, INT-j12-26 | name `pdf_quality_html.py` (the review page), not the model call |

## Q — proposals (for the ruling; not acted on)

- **A: a gap in the value string only, not in template coverage.** A3 (ii′) shows every live field's runtime
  messages equal its branch's rendered variant once the substituted values are normalised. The builder has one branch
  (A1), and both branches are rendered. What differs is only the field-type string in a substituted slot. That string is
  exactly as much a substituted value as the field name, which the render also holds at a sentinel (`f`). The
  measurements favour **O0, no change**, with a dated note on INT-w12-1 recording that audit's variant set is covered
  (A3) and that its remaining item is the Anthropic arm (F15). This is the read-out's proposal; it is not measured beyond A3.
  - If the hash is meant to name every reachable field-type string, **O2** is the smaller edit (1 line). It moves only the
    manifest's audit `prompt_hash` (`86098ebd…`), changes no request byte and no pin, and needs U39's audit test to
    send the placeholder.
  - **O1** has the same properties (`a1d355d4…`), with a three-variant render tied to `VALID_FIELD_TYPES` and a three-call test.
  - **O3** and **O4** change what is sent and are not proposed.
- **P: a dated note on INT-j12-15 (first choice), with no new row.** Its text already states the finding: "makes model
  calls outside any run manifest; the stored classification has no model, digest, prompt or run". The note would add,
  measured here:
  - the CLI is the only entry point (P1);
  - an in-run call would be refused, `UndeclaredCall`, because no manifest declares `pdf_quality` (P2);
  - the write is a raw `UPDATE` of four `papers` columns with no event and no `updated_at` (P3);
  - the render's `pdf_quality` branch is unreached at runtime and in the suite (P4);
  - 381 classifications on live, none carrying a model or a time (P5).

  Class and package stay as the row has them (1 conditional, P4); R599 lets P4's Phase A move the class. Whether
  the render branch is retired (R31) or the stage gets a manifest is P4's decision. The read-out's proposal is that it
  be decided there, not by U39.

## Findings

1. **`"text"` is not a codebook type.** `VALID_FIELD_TYPES = ("categorical", "numeric", "free_text")` is enforced by the loader,
   and `type` is required. So `audit_events`' `f.get("type", "text")` and `types.get(c.field_name, "text")` defaults
   cannot fire through a loaded codebook. The render's `"text"` variant is the builder's default argument. Its other
   source, `audit_span`'s own `field_type: str = "text"` default, has no caller in `engine/`, `scripts/` or `analysis/`
   (`grep -rn "audit_span(" …` lists tests only). So no production path sends `Field type: text`.
2. **A3's column (ii) is false for every field** because the field name is a second substituted value. The brief's
   columns are reported as specified, and (ii′) is added beside them.
3. **O0's audit `prompt_hash` equals B23's recorded `d972c46d…`**, an independent check that the measurement
   computes the hash the manifest records.
4. **Every option that edits the render breaks U39's audit test** (O1, O2). A fix under either is a test-and-engine
   change in one commit.
5. **pdf_quality writes no `updated_at`.** The classification's time is unrecorded anywhere, so P5 can bound it only by
   `updated_at` set by other writers (latest AI_CHECKED 2026-03-14T00:11:50Z).
6. **INT-j12-15 already carries P's finding.** No new row is needed for it.

## Files

`a3/` (capture script, table, per-field rows), `a4/` (driver, measure script, per-option diff, measurement JSON, U39
test output, `summary.json`, run log), `p5/` (script, output, both fingerprints), `p2_exercise.txt`, `p6_rows.txt`,
`a5_p6_more.txt`, `worktree_after.txt`, `step0.txt`, `step0_cc_diff.txt`, `r713.txt`, `r714.txt` (item 0).
