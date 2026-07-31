# DETFIX-01 — Determinism study: mirrored thinking-parse fix and contamination markers

Repo: `~/projects/inference-determinism` · Machine: DGX (spark-59e4) · Date: 2026-07-29
Commit **`dc39b75`** · study repo clean · **evidence-engine untouched** (still `8486f89`).
Study suite: **119 passed**.

---

## 1. Mirror fix — diff summary

**`probes/common.py`** — the mirrored parse, rewritten to match evidence-engine commit
**`9190e41`**, with the provenance named in the docstring:

> *"Mirror of evidence-engine `engine/agents/extractor.parse_thinking_trace` as repaired in
> commit **9190e41** (task REGRESSION-01). Keep the two in step: this function exists to
> reproduce what the production extractor does, and a divergence here silently invalidates
> every determinism claim the study makes about the extractor."*

Kept as a copy rather than an import, per the study's no-cross-repo-import convention — with
the reason recorded: the probe must not depend on the engine's installed state, because
characterising the runtime is the whole point.

| change | detail |
|---|---|
| signature | `parse_thinking_trace(content, thinking=None) -> (trace, branch)` |
| branch 1 `native` | `message.thinking` — the Ollama ≥ 0.12 shape, **primary** |
| branch 2 `legacy-tags` | inline `<think>…</think>`, retained and **printed as a warning** when it fires |
| branch 3 | **removed.** Absence raises `MissingThinkingChannelError` |
| think policy | new `PASS1_THINK = True` / `PASS2_THINK = False` constants, mirroring the engine's `extraction_models` block |

**`probes/cache_arms.py`** — passes the captured channel through instead of re-deriving it:

```python
thinking1 = msg1.get("thinking") or ""
# The captured thinking channel is passed in, not re-derived from content.
# Re-parsing content is exactly what fed pass 2 the answer (CONTAMINATED.md).
trace, parse_branch = C.parse_thinking_trace(content1, thinking1)
```

A missing channel is recorded as `pass1_no_thinking_channel` on the run record rather than
raising through the arm — failures are recorded, never retried, per the study's existing
convention. Two new record fields: `pass1_parse_branch`, `pass1_thinking_chars`. The warm arm
(`:225-231`) and the filler call (`:280`) got the same treatment.

**`probes/condition_a.py`** — same pass-through and the same two new fields.

**`think=` is now explicit at all 8 `C.chat()` call sites** (was: 5 of 8 inherited the default —
`cache_arms` pass-1/warm/filler and both `condition_a` pass-1 calls). A source-level test walks
`probes/*.py`, brace-matches every `C.chat(` call and fails if any lacks `think=`, so a *new*
call site cannot reintroduce the inheritance.

**Tests (§5 of the contract): the repo has a convention** (`tests/`, pytest, 4 existing suites),
so I added a fixture suite rather than relying on the probe. `tests/test_thinking_channel.py`,
11 tests: both response shapes, native-wins-over-legacy, blank/whitespace treated as absent,
**the regression test** asserting the raise and that the answer never becomes the trace, the
policy constants, the source-level call-site check, and a fake-client check that `think=`
reaches the request body.

**Two pre-existing tests encoded the bug and are inverted**, with the reason recorded in-line:
`test_pass_separation` asserted `pass1_trace == p1_content` (true only because of the
fallback), and the `test_cache_arms` / parse-failure fakes returned pass-1 responses with no
`thinking` field. Their fakes now use the modern shape.

---

## 2. Markers

`CONTAMINATED.md` at the study root (136 lines, committed), covering three independent defects
with per-run counts **measured from the stored records** (`pass1_trace != pass1_thinking_raw`
while `pass1_thinking_raw` is non-empty):

| set | run | defect | contaminated |
|---|---|---|---|
| 1 | `20260727T060654Z_d0s2_armP` | 07:00 health-cron eviction (already marked in-run) **+ parse** | 15 / 15 |
| 2 | `20260727T172518Z_d0s2_armPrerun` | **restart kill — marker was owed** **+ parse** | 7 / 7 completed |
| 3 | all post-2026-04-19 runs | parse | Arm S **15/15**, smoke **3/3** |

Set 2's record shows the kill precisely: runs 0–6 completed; runs 7–14 all died
`pass1_failed: ConnectError: [Errno 111] Connection refused` — eight of fifteen target runs
never executed.

**Stated usable:** `pass1_thinking_raw` and `pass1_content_raw` are genuine (the probe captured
the channel correctly, it simply did not forward it); runtime/cache measurements
(`load_duration`, `pass1_warm`, timing blocks, OPTIONS hash, model digest) are indifferent to
the parse; and determinism-of-hashes comparisons remain internally valid *for the input regime
they were run under*.

**Stated unusable:** no figure from these runs may enter a writeup — explicitly including the
committed Arm S headline *"steady state is bit-identical 15/15"*, which describes a pipeline
whose pass 2 was fed an answer. It may well replicate after the fix; it has not been shown to.
Nothing transfers to the production extractor. **No data deleted.**

---

## 3. Probe result

One paper, one repetition, `deepseek-r1:32b`, tmux background, experiment flock held as self,
launched 16:46 UTC — clear of every cron window.

| check | value |
|---|---|
| `error` | `None` |
| `parse_branch` | **`native`** |
| `thinking_chars` | **3,149** |
| `trace == thinking.strip()` | **True** |
| `trace == content` | **False** ← the contamination signature is gone |
| pass-2 parsed fields | **20 / 20** |
| elapsed | 393 s |

```
thinking[:160] : "Alright, I'm trying to extract data from this paper for a systematic
                  review. Let me go through each field step by step..."   ← now the trace
content[:160]  : "### Extraction Results\n\n1. **study_type**\n   - **value**: Original
                  Research\n   - **source_snippet**: \"Here we describe an enhanced..."
                                                                          ← no longer used
```

**The probe's first verdict was `FAIL`, and that was my script's bug, not the fix's.** It
compared `trace` against the *unstripped* `pass1_thinking_raw`: `parse_thinking_trace` returns
`thinking.strip()`, the raw ends in `\n`, so 3,149 vs 3,148 characters. Both the probe assertion
and one test assertion that shared the assumption are corrected to compare stripped, and the
verdict re-derived offline from the stored artifact — **no second model call** — is **PASS**.
Worth recording for future analyses: `pass1_trace_sha256` and `pass1_thinking_sha256`
legitimately differ by that strip.

Artifact: `runs/20260729T164604Z_detfix01_verify/verify.jsonl`.

---

## 4. Re-run readiness and the earliest viable window

**Both arms are code-ready.** The parse is fixed, `think=` is explicit everywhere, the record
now carries `pass1_parse_branch` so a future contamination is visible in the data rather than
only in the code, and OPS-GUARD-01 closes both mechanisms that killed the previous two Arm P
attempts (the health cron stands down on the lock; `restart_ollama()` stands down for a foreign
holder).

**But the re-runs should not be scheduled yet, and the reason is not scheduling.** Re-running
now risks a *third* contamination by contract change: SCHEMA-EVAL-02 is expected to settle the
local response contract, and if it moves the pass-2 schema — the named-property restructure is
a live candidate — an Arm S/P run executed beforehand characterises a pipeline that no longer
exists. Two arms have already been lost to running before the environment was stable. The
sequencing that avoids a third is: **SCHEMA-EVAL-02 → contract frozen → Arm S → Arm P attempt 3.**

**Earliest viable window, once the contract is frozen.** Cron on this box (verified from
`crontab -l`): 07:00 UTC Ollama health check (the only Ollama-touching job), 09:00 nightly
tests, 09:30 citation-mcp, 13:30 morning digest, service health every 6 h (00/06/12/18). An Arm
S run is 15 two-pass cycles; at the ~393 s/cycle this probe measured that is **≈100 minutes**,
so it needs a clear ~2 h block. **The cleanest window is 17:00–06:30 UTC** — after the evening
service-health tick and finishing before the 07:00 health check. Starting at ~17:30 UTC leaves
more than 13 hours of clear runway for both arms back to back. Note this is now belt-and-braces
rather than load-bearing: the experiment lock already makes the 07:00 collision impossible, and
choosing the window simply avoids contending for the GPU.

---

## 5. Acceptance gates

| gate | status |
|---|---|
| 1. Mirror updated, provenance comment names the source commit | ✅ §1 — docstring names `engine/agents/extractor.parse_thinking_trace`, commit `9190e41`, task REGRESSION-01 |
| 2. `think=` explicit on all calls | ✅ §1 — 8/8 call sites, enforced by a source-level test |
| 3. `CONTAMINATED.md` covers all three sets with usable/unusable stated | ✅ §2 — per-set counts measured from the records; nothing deleted |
| 4. Probe shows `trace_used == captured thinking` | ✅ §3 — `native`, 3,149 chars, `trace == thinking.strip()`, `trace != content` |
| 5. Study repo clean; evidence-engine untouched | ✅ study clean at `dc39b75`; evidence-engine unchanged at `8486f89` |

**Two deviations worth flagging.** (a) I intended two commits (fix, then docs); `git add -A`
swept everything into `dc39b75`, so the marker landed in the same commit as the fix. Content is
complete and the message covers both, but the split the brief allowed for did not happen.
(b) `runs/` is gitignored, so the in-run marker at
`runs/20260727T060654Z_d0s2_armP/CONTAMINATED.md` remains untracked — only the root
`CONTAMINATED.md` is under version control. That is consistent with the repo's existing
convention of not committing run artifacts, and the root marker names every affected run, so
the record survives independently of the untracked directory.

**Out of scope and not done:** re-running Arm S or Arm P attempt 3, SCHEMA-EVAL-02, the OpenAI
strict cutover, Run 7.
