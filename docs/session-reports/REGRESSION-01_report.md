# REGRESSION-01 — Thinking-channel parse regression: fix and consumer audit

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-29
Commits **`9190e41`** (parse fix + telemetry v2) → **`8486f89`** (post-fix smoke).
Clean tree. Full offline suite **1456 passed, 15 deselected**.

---

## 1. Upgrade date and affected artifacts

**The regression window opens 2026-04-19T04:48:14 UTC**, when Ollama went **0.17.7 → 0.21.0**.
Established from `journalctl -u ollama` "Listening on … (version X)" lines, corroborated by the
binary mtime (2026-04-17 03:37) and the systemd unit mtime (2026-04-19 04:48):

| version | first seen | last seen |
|---|---|---|
| 0.14.1 | 2026-01-14 | 2026-02-03 |
| 0.15.4 / 0.15.5 | 2026-02-04 | 2026-03-11 |
| **0.17.7** | **2026-03-12** | **2026-04-18** |
| **0.21.0** | **2026-04-19** | present |

### Affected set

| artifact | date | verdict |
|---|---|---|
| **Run 6 local extractions (190)** | 2026-03-15 → 03-18 | **PRE-upgrade, on 0.17.7 — unaffected** ✓ (as expected) |
| Cloud extractions (379) | 2026-03-16 → 03-18 | Unaffected — provider-side thinking, no `<think>` parsing |
| Judge runs (7: Pass 1 ×2, Pass 2 ×5) | 2026-04-20 → 06-04 | Post-upgrade but **unaffected**: gemma3:27b, `think=False` passed explicitly, and the judge never parses a thinking channel |
| **SCHEMA-EVAL-01 local A/B (20 extractions)** | 2026-07-28 | **AFFECTED** — the only affected artifact in this repo |
| Local extractions after the upgrade | — | **none exist** (0 rows) |

**Zero production data is corrupted.** The regression window contains no production local
extraction; it caught only the eval that discovered it.

### Cross-project exposure — the determinism study is affected, and worse

`~/projects/inference-determinism` carries its own copy of the bug:

- `probes/common.py:243-249` — a `parse_thinking_trace(content)` with the same `<think>` regex,
  documented in-file as *"Mirrors extractor.parse_thinking_trace."*
- `probes/cache_arms.py:116-117` — it **captures the native field** (`thinking1 =
  msg1.get("thinking")`) and stores its SHA-256, then passes **`parse_thinking_trace(content1)`**
  to Pass 2. It has the right value in hand and feeds the wrong one forward.
- `probes/cache_arms.py:218` — same pattern on the warm arm.

Its own stored data proves the defect without a new run. From
`runs/20260727T025130Z_condition_a_smoke/condition_a.jsonl`:

```
pass1_thinking_raw : "Alright, I'm trying to extract data from this paper for a systematic
                      review. Let me go through each field step by step..."   ← the reasoning
trace_used         : "### Extraction Results\n\n1. **study_type**: Original Research
                      - **source_snippet**: \"Here we describe an enhance..."  ← the ANSWER
```

Every Arm S/P run after 2026-04-19 — including the Arm P rerun that OPS-GUARD-01 protected —
primed Pass 2 with a first-draft answer. Fixing that project is out of scope here; flagging it
is not.

---

## 2. Consumer inventory

Every code path that parses a thinking channel or emits `/no_think`, with its verdict against
the 0.21.0 response shape:

| # | path | file:line | think handling | verdict |
|---|---|---|---|---|
| 1 | **Extraction Pass 1** (deepseek-r1) | `agents/extractor.py:219` → `parse_thinking_trace` | **no `think=` passed** → 0.21.0 auto-enables; thinking lands in `message.thinking`; regex misses; **silent whole-content fallback fires** | **BROKEN — the regression** |
| 2 | Extraction Pass 2 | `agents/extractor.py:274` | `think=False` explicit | Safe |
| 3 | Snippet retry | `agents/extractor.py:336` | `think=False` explicit | Safe |
| 4 | Abstract screening (qwen3:8b) | `agents/screener.py:167` + `/no_think` at `:110` | `think=False` explicit **and** prompt prefix | Safe — belt and braces |
| 5 | FT screening (qwen3:32b) | `agents/ft_screener.py:233,271` + `/no_think` at `:122,168` | `think=spec.ft_screening_models.think` (spec-declared, default False) | Safe — and it is the convention the fix adopts |
| 6 | Auditor (gemma3:27b) | `agents/auditor.py:164` + `/no_think` at `:134` | `think=False` explicit | Safe; gemma3 has no thinking channel |
| 7 | Judge Pass 1/2 (gemma3:27b) | `analysis/paper1/judge.py:125,234` | `think=False` explicit; **never parses a thinking channel** | Safe by construction |
| 8 | Pass 2 retry-single | `analysis/paper1/pass2_retry_single.py:209` | `think=False` explicit | Safe |
| 9 | Cloud Anthropic | `cloud/anthropic_extractor.py:94` | reads `block.thinking` (provider-native) | Unaffected |

**Exactly one exposed path.** Every other call site already passed `think=` explicitly — which
is why the blast radius stayed at one function. The screening paths carry no exposure at all:
they suppress thinking twice over (`/no_think` prefix *and* `think=False`) and never parse a
thinking channel.

**No auto-enable change was found for the screening models.** qwen3 emits nothing to parse under
`/no_think` + `think=False`, and gemma3 has no thinking channel to auto-enable.

---

## 3. Fix summary

**Contract item 1 — explicit `think` on every call, policy in one place.**
New `ExtractionModels` block in the Review Spec (`engine/core/review_spec.py:99-114`), following
the existing `ft_screening_models.think` convention:

```yaml
extraction_models:
  extractor: deepseek-r1:32b
  pass1_think: true    # Pass 1 exists to produce a reasoning trace
  pass2_think: false   # Pass 2 emits schema-constrained JSON
```

Resolved in `extract_paper` (`agents/extractor.py:453-460`) and threaded into
`extract_pass1_reasoning(prompt, think=…)` (`:206,228`) and
`extract_pass2_structured(…, think=…)` (`:301,330`). Nothing now relies on a version-dependent
default. `extraction_hash` is unchanged (`d311eb20d1f8…`), so no stale-schema churn.

**Contract item 2 — native-first parse, loud absence.**
`parse_thinking_trace(content, thinking) -> (trace, branch)` at `agents/extractor.py:246-273`:

1. `native` — `message.thinking`, the 0.21.0 shape (primary).
2. `legacy-tags` — inline `<think>…</think>`, retained and **logged at WARNING when it fires**.
3. **No third branch.** Absence raises `MissingThinkingChannelError`
   (`agents/extractor.py:230-244`), quoting the offending content for diagnosis but never
   returning it. The old fallback returned that content as the trace, which is what broke Pass 1.

**Contract item 3 — telemetry `extraction-telemetry-1` → `-2`.**
`engine/core/extraction_telemetry.py:30,61-63,95-101` adds `thinking_present`, `thinking_chars`,
`parse_branch` per call, populated from `_LAST_PASS1_TELEMETRY` by the retry driver
(`agents/extractor.py:221-226`, recorded at both the incomplete and stored outcomes). A silent
branch switch is now visible in the telemetry rather than invisible in the results.

**Contract item 4 — tests.** `tests/test_thinking_channel.py`, 19 tests: both response shapes,
native-wins-over-legacy precedence, blank/whitespace thinking treated as absent, **the fallback
regression test** asserting the raise and that the answer never leaks in as a trace, explicit-
`think` assertions on Pass 1, branch telemetry, and spec-policy tests. One pre-existing test
(`test_parse_thinking_trace_no_tags`) **asserted the buggy fallback**; it is inverted to assert
the raise, with a pointer to the new suite.

---

## 4. Smoke: before / after

3 papers from the SCHEMA-EVAL-01 sample, production path, `tmux --background`, experiment lock
held, `RESTART_EVERY_N` untouched. **Anchored spans, same papers, same code path, three
runtimes:**

| paper | Run 6 (0.17.7) | pre-fix (0.21.0) | **post-fix** |
|---|---:|---:|---:|
| p39 (long) | 16/20 | **0/20** | **13/20** |
| p466 (collapse-class) | 14/20 | **0/20** | **8/20** |
| p629 (ordinary) | n/a | 0/20 | 0/20 \* |
| **total** | **30/40 (75%)** | **0/60 (0%)** | **21/60 (35%)** |

All three ran `parse_branch=native` with 2,243 / 3,472 / 2,256 chars of real reasoning captured.

\* p629 is a 9.6k-char **editorial** ("Artificial intelligence-driven precision surgery") with
almost nothing extractable, and it has no Run 6 counterpart because it was never in Run 6's
extraction set. Post-fix it returns **11 `ABSENCE_DECLARED`** and 4 `DRIFTED` — it declares
absence instead of inventing snippets, which is the desired behaviour, not a miss. On the two
papers with a Run 6 baseline the recovery is **0/40 → 21/40 (52.5%)** against Run 6's 75%.

**Example snippet pair — p466 `robot_platform`:**

| run | snippet |
|---|---|
| Run 6 | "We implemented our approach on the Berkeley Surgical Robots (see Fig. 1), and applied it to two representative tasks, among which…" |
| pre-fix | "Mentioned in the abstract and implementation details." |
| **post-fix** | "We implemented our approach on the Berkeley Surgical Robots (see Fig. 1), and applied it to two representative tasks, among which…" |

Post-fix reproduces the Run 6 quote **verbatim**. Snippets are quotations again, not authored
prose — which was the check.

**Honest reading of the residual gap.** 35% (or 52.5% on comparable papers) is recovery in
direction and character but not full restoration of Run 6's 75%. At n=3 that gap is not
interpretable — it could be run-to-run variance, remaining 0.21.0 behavioural differences beyond
the parse, or sample composition. SCHEMA-EVAL-02 with larger n is where that gets settled; this
smoke establishes that the mechanism is fixed, not that the arm is back to baseline.

---

## 5. Screening-path exposure

**None found.** Paths 4 and 5 in §2 suppress thinking twice — `/no_think` in the prompt *and*
`think=` passed explicitly (FT screening resolves it from `spec.ft_screening_models.think`,
default `False`) — and neither parses a thinking channel, so there is nothing for the interface
change to break. The auditor and judge are likewise safe, additionally because gemma3 has no
thinking channel at all.

The screening convention is in fact what the fix adopted for extraction: declare `think` in the
Review Spec, resolve it at the call site, never inherit a runtime default.

---

## Acceptance gates

| gate | status |
|---|---|
| 1. Upgrade date established; affected-artifact list explicit | ✅ §1 — 0.17.7→0.21.0 at 2026-04-19T04:48:14; Run 6 predates it; SCHEMA-EVAL-01 is the only affected repo artifact; determinism project affected cross-project with proof |
| 2. Consumer inventory complete with per-path verdicts | ✅ §2 — 9 paths, 1 broken, 8 safe with reasons |
| 3. Fix contract 1–3 implemented; fallback removal verified by test | ✅ §3 — spec-declared policy, native-first parse, telemetry v2; `test_missing_thinking_channel_raises_and_never_substitutes_content` |
| 4. Smoke shows recovery (snippets verbatim) | ✅ §4 — 0/60 → 21/60; p466 reproduces the Run 6 quote exactly |
| 5. Full suite green | ✅ 1456 passed, 15 deselected |

**Out of scope and not done:** SCHEMA-EVAL-02 (local re-measure at larger n, incl. the
named-property restructure candidate), OpenAI strict production cutover, a version-pin ops
protocol, Run 7 — and the fix to `~/projects/inference-determinism`, which I recommend be
raised as its own task since its Pass-2 inputs are contaminated the same way.

---

## Addendum (2026-08-29): corrected account per QUALGAP-01

**Superseded:** §1's framing that "the regression window opens 2026-04-19T04:48:14 UTC,
when Ollama went 0.17.7 → 0.21.0", and the artifact table's verdict on the 190 Run 6 local
extractions — *"PRE-upgrade, on 0.17.7 — unaffected ✓"*. Both rest on the premise that the
0.21.0 upgrade is what moved `deepseek-r1`'s reasoning out of inline `<think>` tags and into
`message.thinking`. That premise is wrong. QUALGAP-01 ran raw HTTP probes against 0.17.7 —
no Python client in the path — and found `message.thinking` present and `<think>` absent
under `think` omitted and under `think=true` alike: **0.17.7 already used the native thinking
channel**, and 0.17.7 was installed 2026-03-12, three days before Run 6. **The corrected
account:** the silent whole-content fallback this report fixed was firing during Run 6 itself,
so Run 6 was contaminated, not a clean pre-upgrade control. Its stored `reasoning_trace` rows
confirm it directly — they are first-draft *answers* enumerating fields with
`**source_snippet**` labels, not reasoning. The upgrade date remains correct as a *fact about
the machine*; what is withdrawn is its role as the causal boundary. The defect's window opens
at the code, not at the upgrade. Consequently Run 6's ~54–58% anchored rate is an artifact of
Pass 2 being primed with a quote-rich answer, and the honest post-fix baseline is **~39–43%
anchored** — a target to beat, not a level to restore. The §2 consumer inventory, the §3 fix
contract, and the §4 smoke recovery are unaffected: they concern which code paths parse a
thinking channel and whether the fallback was removed, neither of which depends on when the
interface moved.

**See:** `docs/session-reports/QUALGAP-01_report.md` (§ raw 0.17.7 probes; outcome
`HYPOTHESIS_DEAD`, +4.3pp pooled and 0.0pp median paired, so the runtime is acquitted) and
`CLAUDE.md` as of commit `e54c07e`, "Extraction Quality Investigation → The standing finding".
`docs/session-reports/PRIME-01_report.md` measures the two channels' quote-richness directly.
Appended by task EXIT-REMED-01; all text above this heading is unchanged.
