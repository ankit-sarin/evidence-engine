<!-- PROVENANCE HEADER (12c-OPEN-R2 R-1) — everything below the END HEADER line is the body, byte-for-byte.
report: 12a-ELICIT-A (session 12a, CC session 2eb166e9), read-only; never written to a file in 12a
source: ~/claude-session-archive/2eb166e9-ff84-4140-9001-00929be1254e.jsonl, line 958 (1-based; index 957 0-based), the single assistant text block, message timestamp 2026-10-01T01:01:59.255Z
chars: 11198
written at tree: 198a349 (198a3492ff5babb3bcfc4c0f7144fa20e555bef0)
extracted: 2026-10-01 (session 12c, CC session bbd7293d)
body sha256 (UTF-8, no trailing newline added): da97b78c05a9794c07c0fd1c2085ac8cc0bed1cc3c35f7deac200a3d9c964a80
END HEADER -->
The trace is complete: nothing on the elicited path has run under the current write path. An elicited Run 7 would open, pin and record its calls correctly, with gaps in the methods prose, the pin's prompt coverage and the tag fields. I wrote nothing and the tree is unchanged at `198a349`.

## Assumptions I1–I4
- **I1 — TRUE.** No elicitation-on run since the write path landed.
  - All six retained smoke spec copies (`surgical_autonomy_smoke10b`, `smoke11b`, `smoke11c`, `ee_smoke_11c` ×2, `se_smoke_11c`) read `elicitation: false`. So do the 10b readout ("elicitation false"), the 11c P0 census ("`False`") and the live spec, line 62.
  - The only elicited runs on disk are the ELICIT-DESIGN smokes (`eval/elicit_design01/scratch/smoke_20260903T…` and `…20260905T…`, plus `run_20260905T011330Z` in the 02 report). They predate the cut-over `6e09166` (2026-09-25) and `e859b79` (2026-09-29).
  - The plan's only "elicitation on" mentions are forward-looking ("Run 7 turns elicitation on deliberately and the manifest records it").
- **I2 — TRUE.** `git ls-files --error-unmatch review_specs/surgical_autonomy.yaml` succeeds.
- **I3 — TRUE: the run refuses.** The manifest opener refuses a dirty tree before writing anything, and the column CHECK backs it up (quotes in Part 3).
- **I4 — TRUE, and harmless.** Both `_open_run_manifest` and the extractor's own preflight use `stage_config("extract_pass1", spec).model`. The resolver gives both stages the same model field, so the spec has no way to name a different model for elicitation.

## Part 2 — what an elicited run records
**2.1 Stages (`run_pipeline.py`).**
```python
"extract": ("extract_pass1", "extract_pass2", "extract_retry_snippet"),
…
if spec.extraction_models.elicitation and "extract_pass1" in stages:
    stages[stages.index("extract_pass1")] = "elicitation_pass1"
if any(s.startswith("extract") or s == "elicitation_pass1" for s in stages):
    preflight.append(stage_config("extract_pass1", spec).model)
```
`run_stage_configs` rows for `--skip-to extract`:
- **Elicitation off:** `extract_pass1, extract_pass2, extract_retry_snippet, audit, preflight`.
- **Elicitation on:** `elicitation_pass1, extract_pass2, extract_retry_snippet, audit, preflight`.
- The preflight models are `["deepseek-r1:32b", "gemma3:27b"]` either way.

**2.2 Resolved values (`effective_config.py:341–352`).**
```python
elif stage in ("extract_pass1", "extract_pass2", "extract_retry_snippet", "elicitation_pass1"):
    blk_src, blk = _block(spec, "extraction_models", ExtractionModels)
    chosen, sources["model"] = blk.extractor, …
    think_attr = {"extract_pass1": "pass1_think", "elicitation_pass1": "pass1_think", …}[stage]
    sent |= {"think"}
```
- With the live block (`extractor: "deepseek-r1:32b"`, `pass1_think: true`, `temperature: 0`), both stages resolve to model `deepseek-r1:32b`, options `{temperature: 0}` (integer), think `True`, and no format.
- **They differ only in `prompt_hash`.** `elicitation_pass1` hashes `el.pass1_messages(el.sentinel_pass1_prompt(spec, codebook_path))`; `extract_pass1` hashes `ex.pass1_messages(prompt)`.
- The spec cannot give `elicitation_pass1` its own model.

**2.3 Pin and `extraction_digest`.**
- `pin_tuple` keys `stages` by stage name, using `{"model", "model_digest", "options_hash", "format_schema_hash", "prompt_hash"}` for each stage `if r.arm_name == arm_name`. `_LOCAL_EXTRACTION_STAGES` includes `"elicitation_pass1"`.
- **The pin tuple contains no spec hash.** So turning elicitation on changes the pin **through the set of stages hashed**: the key `elicitation_pass1` replaces `extract_pass1` and carries a different `prompt_hash`. It does not change it through the spec bytes. The new value can only be known by computing it, which I didn't do.
- Live has no pinned `local_deepseek_r1_32b` (only the 3 pre-manifest arms), so Run 7 pins fresh and nothing refuses. That matches v70's "a different pin is then expected — record both".
- `extraction_stages(spec)` returns `("elicitation_pass1", "extract_pass2", "extract_retry_snippet")` when elicitation is on. `extraction_digest` raises `StageNotInRun` if `stages[0]` isn't declared, then checks each declared stage's **model digest only** against the pin.
- Order in the pipeline: `select_for_extraction` (read-only) runs in `_stage_extract` *before* `verify_extraction_run`. The check comes before any model call, not before selection.

**2.4 `run_calls` stages.** `kwargs()` puts `"stage": self.stage` into the call, and both elicited sites pass `paper_id`:
- Pass 1 records `elicitation_pass1` via `ollama_chat(paper_id=paper_id, messages=pass1_messages(prompt), …, **cfg.kwargs())`, with `cfg_p1 = stage_config("elicitation_pass1", spec)`. That is 1–2 rows per paper.
- Pass 2 records `extract_pass2` via `cfg_p2 = stage_config("extract_pass2", spec)`, only when a field survived.
- Preflight records `preflight`, and audit records `audit`.
- The elicited path never calls `extract_retry_snippet`: it is declared but has no rows.
- Every stage that writes a row is declared, and `FOREIGN KEY (run_id, stage) REFERENCES run_stage_configs` enforces it.

**2.5 Methods section.**
- **The model is correct.** `_run_extraction_models` selects `sc.stage_kind IN _LOCAL_EXTRACTION_STAGES`, which includes `elicitation_pass1`, with a LEFT JOIN on `run_calls`. That yields `deepseek-r1:32b (n=…)`.
- **The prose is wrong for an elicited run.** Line 173 is fixed text: `"…with a two-pass reasoning-then-structured-output approach…"`.

**2.6 Tests that turn elicitation on.** All 19 mock the model and nothing opens a real manifest.
- `test_elicitation_pipeline.py`, all 15 tests: mocks `PL.ollama_chat`, `E.ollama_chat`, and `write_extraction_events` (autouse capture). It uses stub spec and DB classes.
- `test_presented_context.py::test_T5_the_elicited_chain_carries_both_attempts_presented_is_pass2` and `::test_T5_pass2_skipped_presented_is_the_accepted_pass1_attempt`: same mocks, plus a stub DB.
- `test_extraction_run_link.py::test_t6_extract_paper_hands_the_unit_map_dir_name_to_the_elicited_path`: mocks `extract_paper_elicited` entirely.
- `test_request_capture.py::test_elicitation_pass1`: a fake client at `_client.chat`; calls `run_pass1` directly with no run or manifest.
- The manifest tests that use `extraction_stages(spec)` (`test_extraction_run_link.py:196`, `_event_store_fixture.py:329`) and `_open_run_manifest` (`test_selection_bound.py`) all run with elicitation off.
- **No test opens a manifest with `elicitation_pass1`, checks the digest against that pin, and writes through the real event writer.**

## Part 3 — the spec flip and the tag
**3.1 The tree check (`run_manifest.py`).** It runs at run open only.
```python
dirty = bool(_git("status", "--porcelain").stdout.strip())
…
if g.dirty:
    raise DirtyTree("run refused: the working tree has uncommitted changes, so the commit "
                    f"{g.commit[:12]} is not the code that would run. Commit or stash, then retry.")
```
Migration 020 adds `git_dirty INTEGER NOT NULL CHECK (git_dirty = 0)`. `--porcelain` also counts untracked files. The elicited path's unit maps go to `review_dir / "elicitation" / <run_UTC> / "unit_maps"` under the gitignored `data/` (`.gitignore:2:data/`), so they don't dirty the tree.

**3.2 What the manifest records.**
- `"git": {"commit": g.commit, "dirty": g.dirty, "tag": g.tag}`, where `tag` comes from `describe --exact-match --tags HEAD`.
- `"spec_hash": spec_hash(spec)`, which is `sha256_canonical(spec.model_dump(mode="json"))`. That is a hash of the parsed model, not a sha256 of the file bytes, so a comment-only edit doesn't change it.
- The codebook's semantic hash and sha256.
- A Run 7 on an edited, uncommitted spec **cannot open at all**. A committed flip is identified by `git_commit` plus `spec_hash`.
- **The flip has to be committed *before* the tag.** A flip committed after the tag moves HEAD off it, so `git_tag` records NULL.
- **New finding:** `engine_state` is hard-coded (`"engine_state": None` and `(…, g.tag, None, …)`), and nothing in engine/ or scripts/ sets it. Tagging freshman will not fill it, though the docstring says "`engine_state` stays NULL until the freshman tag".

**3.3 Smokes use their own spec copy.**
- `load_spec_for` does `path = Path(override) if override else spec_path_for(review_id)` and refuses a `review_id` mismatch.
- Smokes pass `--spec data/<id>/spec.yaml`, which sits under the gitignored `data/`. They never read `review_specs/surgical_autonomy.yaml`.
- So the live spec can be flipped and committed before the smoke. Under R235 the copy is taken *from* the live spec with only the id changed, so it would then carry `elicitation: true`. The smoke touches neither live nor the tree.

## Part 4 — the launch path
From `scripts/run_pipeline.py`:
```python
parser.add_argument("--spec", default=None, help=("Override the Review Spec path. Defaults to review_specs/<review>.yaml; …"))
parser.add_argument("--skip-to", choices=STAGES, default=None, help="Skip to a specific pipeline stage")
parser.add_argument("--max-papers", type=_positive_int, default=None, help=("Bound the EXTRACT stage … Absent: unbounded."))
```
The default path is `SPEC_ROOT / f"{review_id}.yaml"`, with `SPEC_ROOT = Path("review_specs")`. That path is relative to the working directory, while `git_state` uses `REPO_ROOT`, so the run must be launched from the repo root.

A full Run 7 is `scripts/run_pipeline.py --review surgical_autonomy --skip-to extract`. **`--spec` is not needed for live.** That run executes extract, then audit, then stops at the audit-review gate, which closes the manifest `('interrupted', NULL)` (C40).

## What would make an elicited Run 7's record wrong or incomplete
1. **Methods prose is wrong:** it says "two-pass reasoning-then-structured-output" for a unit-citation elicitation run. The model name is correct.
2. **The pin doesn't cover two prompt templates.** Neither the Pass-2 priming wrapper (`build_pass2_priming_message`) nor the attempt-2 feedback template feeds any stage's `prompt_hash`, because `extract_pass2` hashes `pass2_messages(prompt, "R")`. An edit to either would leave the pin and every stage hash unchanged; only `git_commit` would show it.
3. **`engine_state` stays NULL after the freshman tag.** Only `git_tag` records the tag, and only if HEAD is exactly the tag. So the spec flip has to land before the tag.
4. **The digest check compares model digests only**, not options or prompt hashes. Those are caught at open by the pin-tuple comparison instead.
5. **`extract_retry_snippet` is declared but never called** on the elicited path. A stage row with zero calls is harmless, and methods counts papers correctly.
6. **No end-to-end test** covers elicitation through a real manifest, the digest check and the event writer (2.6).
7. **The run closes `('interrupted', NULL)`** at the audit-review gate (C40/C47, out of scope here).
8. **The unit-map directory is keyed by the process UTC timestamp** (`run_%Y%m%dT%H%M%SZ`), not the manifest's `run_id`. Its only link back is `_LAST_PASS2_TELEMETRY["elicitation_run_id"]`. I didn't trace where that telemetry is stored.

Tree: `198a349`, clean. I'm stopping here for your ruling.