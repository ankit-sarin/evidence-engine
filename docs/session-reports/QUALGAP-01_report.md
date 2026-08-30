# QUALGAP-01 — Runtime version A/B: does Ollama 0.17.7 restore Run 6 quality?

Project: evidence-engine · Machine: DGX (spark-59e4) · Batch 2026-08-01 · Report 2026-08-23
Commit **`1ffa04e`** (harness + analysis + tests). Clean tree. Suite **1470 passed, 15 deselected**.
Raw outputs in gitignored `data/surgical_autonomy/eval/qualgap01/`.

**78/80 extractions completed, 13.3 h.** Two failures, both paper 415, enumerated in §2.4.
No production code path, no `review.db` write, no change to the production Ollama service.

**Answer in one line: the runtime is not the cause, and pre-flight found what is.**

---

## 1. Pre-flight (Gate 1) — stated before batch spend

### 1.1 Binary and store compatibility

`ollama-linux-arm64.tar.zst` from the v0.17.7 GitHub release (published 2026-03-05),
SHA-256 **`b39fff6bf90a799816794ef94695a978fee82912edc93ce6e3d85323f342ecb7`**, matching the
release `sha256sum.txt`. Unpacked to `~/opt/ollama-0.17.7/pkg`, run as a user process on
**:11435**.

**The store is backward-compatible — no migration, no duplication.** With `OLLAMA_MODELS` pointed
at the existing store, 0.17.7 listed all 20 models and reported `deepseek-r1:32b` digest
`edba8017331d…`, byte-identical to 0.21.0's. Blobs are world-readable, so usage was read-only in
practice.

### 1.2 Environment parity

Both instances' effective config differ on exactly two keys: `OLLAMA_HOST` (the port, by design)
and `OLLAMA_DEBUG_LOG_REQUESTS:false`, which exists only in 0.21.0 and sits at its default.
`FLASH_ATTENTION=true`, `KV_CACHE_TYPE=f16`, `MAX_LOADED_MODELS=1`, `NUM_PARALLEL=1`,
`KEEP_ALIVE=-1`, `CONTEXT_LENGTH=0` all match, and both derive the same `default_num_ctx=262144`,
clamped identically against `n_ctx_train=131072`.

### 1.3 The probes falsified the premise the brief was built on

V1 was specified as a faithful Run 6 replication, expecting inline `<think>` tags and
`parse_branch=legacy-tags`. It is not one. Raw HTTP probes — no Python client in the path, so the
library cannot be a confound:

| probe against 0.17.7 | `message.thinking` | `<think>` in content |
|---|---|---|
| `think` omitted | present, 3,124 chars | **no** |
| `think=true` | present, 3,416 chars | **no** |
| `think=false` | absent | no |

**Ollama 0.17.7 already used the native thinking channel.** The interface did not move at the
0.21.0 upgrade, so REGRESSION-01's and SCHEMA-EVAL-02's account of *when* it moved is wrong.

The consequence was then confirmed from data already on disk, with zero model calls. The journal
shows 0.17.7 was installed **2026-03-12**, three days before Run 6 — so Run 6 ran on it, and the
pre-fix parser would have found no tags and returned the whole content. Run 6's stored
`reasoning_trace` rows confirm it: they are first-draft **answers**, not reasoning — field lists
carrying `**source_snippet**: "TYPE Original Research PUBLISHED 21 October 2025 DOI …"`, several in
fenced ` ```json ` blocks, with quotes already lifted verbatim from the paper.

**So Run 6 primed Pass 2 from the content channel; every post-fix run primes it from the thinking
channel.** That is a second difference between Run 6 and today, alongside the runtime version, and
the brief did not know it existed.

### 1.4 What that changed about the design

* **V1 still delivers the primary read.** It holds the code path at "fixed" and moves only the
  runtime version — exactly the comparison against Run 6 and condition B. It is simply not the
  Run 6 *code path*, so the "hypothesis dead" branch acquits the runtime without closing the
  question.
* **V2 − V1 cannot isolate a think-mode mechanism on this runtime**, because omitting `think` and
  passing `think=True` reach the same channel. The cell was kept as pinned and read additionally as
  a **same-condition replication** — this study's first internal noise band for the anchored rate.
* **Both Pass-1 channels are captured per call.** Free at run time, and it makes the code-path
  hypothesis measurable offline on these same 80 calls (§3).

### 1.5 Probe of record

One probe per cell on the production path, lock held: `parse_branch=native`, 1,926 thinking chars,
0 `<think>` tags, schema compiled and returned 2/2 keys. Both cells identical, as §1.3 predicts.

---

## 2. Measures

Every arm is restricted to the **36 papers all four arms have**, so no rate is computed over a
different corpus than another's. Of the 40: paper 415 failed both cells (§2.4); papers 547, 629 and
799 have no Run 6 counterpart (SCHEMA-EVAL-02 recorded the same 3).

### 2.1 M1 — Provenance (frozen v1.1 ladder), 36 matched papers

| arm | runtime | Pass 2 primed from | spans | **ANCHORED** | STITCHED | DRIFTED | UNTR_PARTIAL | **NO_BASIS** | ABS_CLAIM | ABS_DECL | MISS_SNIP |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Run 6** | 0.17.7 | **content** | 700 | **58.3%** | 0.6% | 18.7% | 0.7% | **14.0%** | 3.9% | 2.3% | 1.6% |
| **V1** (`think` omitted) | **0.17.7** | thinking | 705 | **43.5%** | 1.3% | 20.1% | 3.5% | 24.3% | 1.8% | 3.5% | 1.8% |
| **V2** (`think=True`) | **0.17.7** | thinking | 705 | **43.1%** | 1.0% | 20.4% | 3.5% | 24.4% | 1.8% | 3.7% | 2.0% |
| **cond B** (SCHEMA-EVAL-02) | 0.21.0 | thinking | 706 | **39.2%** | 3.5% | 16.3% | 3.0% | 28.2% | 1.0% | 5.8% | 3.0% |

### 2.2 M2 — Completeness / telemetry (36 matched papers per cell)

| cell | calls | errors | complete | guard pass | would-retry | parse branch | parse path |
|---|---:|---:|---:|---:|---:|---|---|
| V1 | 36 | 0 | 35 | **97.2%** | 1 | `native` 36 | `schema_valid` 36 |
| V2 | 36 | 0 | 35 | **97.2%** | 1 | `native` 36 | `schema_valid` 36 |
| cond B (0.21.0) | 36 | 0 | 31 | 86.1% | 5 | `native` 36 | `schema_valid` 36 |

`parse_branch=native` on **78/78** successful calls — confirming §1.3 at batch scale, not just on a
probe. The array schema parsed cleanly every time on both runtimes.

### 2.3 M3 — Value agreement and M5 — cost

| pair | disagreement (exact normalized match) |
|---|---:|
| **V1 vs V2** (same condition, replication) | **2.3%** |
| V1 vs Run 6 | 32.1% |
| cond B vs Run 6 (SCHEMA-EVAL-02) | 39.0% |
| V1 vs cond B | 40.7% |

| | 0.17.7 (V1/V2) | 0.21.0 (cond B) |
|---|---:|---:|
| median total latency | 528 s | 459 s |
| median pass 1 / pass 2 | 268 s / 256 s | — |
| median thinking chars | 2,463 / 2,531 | 2,581 |
| wall-clock per cell | 5.54 h / 5.56 h | — |

### 2.4 The two failures

Paper 415, **both cells**, identical mode: `deepseek-r1:32b` exceeded the 900 s wall-clock watchdog
on all 3 attempts. The last-resort restart branch was disarmed for this run, so it raised instead of
touching the production service — the rail worked as designed. Paper 415 completed under 0.21.0 in
SCHEMA-EVAL-02, and is also one of the two SPANLOSS-01 single-span collapses. It is the hardest
paper in the corpus and the only place 0.21.0 strictly beats 0.17.7 on reliability.

---

## 3. The channel census — what pre-flight bought for free

Both Pass-1 channels were captured on all 78 successful calls, so this costs nothing:

| channel | papers with quoted snippets | quoted snippets | anchored | no-basis |
|---|---:|---:|---:|---:|
| Run 6 stored trace (**content**) | 35/36 | 659 | **54.5%** | 15.8% |
| V1 Pass-1 **content** | 34/36 | 650 | 48.8% | 15.4% |
| V2 Pass-1 **content** | 34/36 | 649 | 50.5% | 15.7% |
| V1 Pass-1 **thinking** | **0/36** | **0** | — | — |
| V2 Pass-1 **thinking** | **0/36** | **0** | — | — |

**Verified independent of the regex.** The `source_snippet:` pattern is a structured-draft shape
that reasoning prose would not use even if it quoted the paper, so "0 snippets" could have been an
artifact. A direct check — counting 8-word windows appearing verbatim in the paper — settles it:
**thinking channel 6/1,728 windows (0.3%); content channel 1,220/3,240 (37.7%)**.

The thinking channel does not quote the paper. Pass 2 primed from it has essentially no verbatim
text to copy; primed from content it has ~650 quotes sitting in front of it.

---

## 4. Pinned-read outcome (Gate 3)

Applying the brief's pre-registered thresholds verbatim:

| clause | value | verdict |
|---|---|---|
| V1 within 5pp of Run 6 (58.3%)? | V1 43.5%, **−14.8pp** | no |
| V1 within 5pp of 0.21.0 cond B (39.2%)? | **+4.3pp** | **yes** |

**OUTCOME: `HYPOTHESIS_DEAD`.** The rule fired cleanly; this is not the ambiguous zone.

Two things sharpen it beyond the pooled rule. First, the same-condition replication band is
**0.4pp** (V2 − V1, 2.3% value disagreement), so the 4.3pp runtime effect is real but small.
Second, and more telling, **the 4.3pp does not survive pairing**: per paper, V1 beats cond B on 15,
loses on 13, ties on 8, **median delta 0.0pp**. The pooled gap is a span-count artifact, not a
consistent per-paper improvement. Against Run 6 the deficit is the opposite — broad and consistent:
**21 of 36 papers worse by >3pp, 9 better, median −10.0pp**.

### Plain English

The Ollama upgrade is not what broke Run 6 quality. Running the identical 40 papers, identical
prompts, identical model blob and identical response contract on the old 0.17.7 runtime recovered
about four points of anchoring — and even that four points vanishes when you compare paper by paper
instead of pooling spans. Fifteen of the nineteen missing points were still missing. What pre-flight
turned up instead is that the runtime was never the difference between then and now: 0.17.7 already
delivered reasoning on the same `message.thinking` channel 0.21.0 uses, so the interface never
moved. What moved was **our own code**. Before REGRESSION-01, Pass 1 had a bug that returned the
model's whole first-draft answer as the "reasoning trace" — and that draft happened to be stuffed
with sentences copied verbatim out of the paper. Pass 2 was handed those quotes and copied them
forward, which is why Run 6 looks so well-anchored. REGRESSION-01 correctly stopped Pass 1
substituting an answer for reasoning, but the genuine reasoning channel quotes the paper almost
never (0.3% of word-windows verbatim, against 37.7% for the draft). So Pass 2 went from copying
quotes to inventing paraphrase. **Run 6's 58% was an accident of a bug, not a quality level the
pipeline ever earned** — and the honest reading is that the pipeline has no verbatim-evidence
mechanism at all, only one it used to get for free by mistake.

The brief pins deeper diagnosis as a separate task, and this stops here. The channel census is
strong evidence for that mechanism but not a controlled test of it; the confirmatory experiment is
to prime Pass 2 from a deliberate quote-bearing draft and measure anchoring directly.

---

## 5. Recommended Run 7 local runtime config

**Do not downgrade.** Stay on **0.21.0**:

* the anchored difference is 4.3pp pooled and **0.0pp median paired** — inside the noise this study
  can resolve;
* 0.21.0 is **~15% faster** (459 s vs 528 s median), worth roughly 1.4 h per 40-paper cell;
* 0.21.0 **completed paper 415**, which 0.17.7 failed six times.

The one thing 0.17.7 did better — completeness guard 97.2% vs 86.1% — is worth watching but is
n=36 on a measure the contract-C required-slot schema already drives to 100% (SCHEMA-EVAL-02 §M2).

**Pin and record the runtime version per extraction regardless.** Not because 4pp matters much, but
because this whole investigation was only possible by reconstructing which version ran when from
journald, and that nearly produced a wrong answer.

**The real Run 7 lever is the input to Pass 2, not the runtime.** Reproduce Run 6's accidental win
deliberately — a drafting pass that emits verbatim snippets — rather than reverting REGRESSION-01.
Reverting would restore the anchoring number while re-introducing a defect that silently
substitutes an answer for reasoning, and the old fallback's own trace was only 54.5% anchored, so
even the accident was leaky.

---

## 6. Cleanup and production state

| check | state |
|---|---|
| user-space 0.17.7 process | **none running** |
| port 11435 | **not listening** |
| `~/opt/ollama-0.17.7` | **removed** (`~/opt` gone) |
| production service | **active**, enabled, **0.21.0** |
| `ollama.service` unit | mtime **2026-04-19** — predates this task |
| `override.conf` drop-in | mtime **2026-04-22** — predates this task |
| `/usr/local/bin/ollama` | mtime **2026-04-17**, 40,378,872 bytes — unchanged |
| drop-in `OLLAMA_*` vars | all four present, values unchanged |
| experiment lock | free |
| git tree | clean at `1ffa04e` |

---

## 7. Acceptance gates

| gate | status |
|---|---|
| 1. Pre-flight complete before batch spend; env parity dumped | ✅ §1 — checksum, store compatibility, two-key env diff, per-cell probes |
| 2. 80/80 or failures enumerated with telemetry | ✅ **78/80**; both failures paper 415, enumerated §2.4 |
| 3. Primary read stated against pinned thresholds | ✅ §4 — **HYPOTHESIS_DEAD**, +4.3pp vs 0.21.0, −14.8pp vs Run 6 |
| 4. Production service untouched (unit unchanged, same version after) | ✅ §6 |
| 5. Suite green; commit is harness + analysis only | ✅ **1470 passed, 15 deselected**; `1ffa04e` touches `analysis/` + `tests/` only |

**Deviations disclosed.**

1. **The batch ran 01:38 → 14:55 UTC, overrunning the 06:30 boundary.** At 40×2 and ~530 s/call,
   06:30 was unreachable from a 01:38 start; the alternative was losing the night. The flock stood
   the 07:00 health cron down.
2. **V1 is not the Run 6 code path** — §1.3. The brief's premise was falsified in pre-flight; the
   cell was run as pinned and the read adjusted rather than the design silently changed.
3. **Two production Ollama restarts occurred during the batch window (14:56:09, 15:49:23),
   neither issued by this harness** (its restart branch was disarmed and it was on :11435). Both
   trace to a pre-existing suite defect: `tests/test_ollama_client.py::TestTimeoutLogging::
   test_timeout_logs_warning` patches `_client` but not `subprocess.run`, and a `NOPASSWD` sudoers
   rule for `systemctl restart ollama` exists, so every offline suite run really restarts
   production. **Reconfirmed 2026-08-23: this report's own acceptance-gate suite run restarted
   production at 04:58:52.** Not fixed here — QUALGAP-01's commit is pinned to harness + analysis.

**Out of scope and not done:** contract C cutover with NOT_FOUND escape values; downgrading the
production service; determinism re-runs; Run 7; and any second diagnosis of the residual gap —
which, per the brief, is a new task with fresh reasoning.

---

## Addendum (2026-08-30): p719 input truncation, per PARSE-01

**Nothing in this report is withdrawn.** This addendum records a fact discovered later that a
reader of §2 should have, and explains why it leaves the verdict intact.

Task PARSE-01 swept all 190 EXTRACTED corpus papers for parse defects and input-limit
saturation. It found that **paper 719 saturates the local context ceiling**: its prompt reaches
`prompt_eval_count` = **131,072** exactly — `n_ctx_train` for `deepseek-r1:32b` — so roughly a
quarter of that document never reached the model. PARSE-01 also classified 719's parsed text as
an **extraction failure**: the PDF text is font-glyph encoded (5,472 `GLYPH<c=..,font=..>`
tokens, 8.84× inflation against `pdftext`), so what the model did receive is largely glyph noise
rather than prose. p719 is one of the **36 matched papers** every measure in §2 is computed over.

**The verdict is unaffected, for a reason this report already established.** §1.2 records that
both instances derive the same `default_num_ctx=262144`, "clamped identically against
`n_ctx_train=131072`". The ceiling is therefore a property of the model, not of the runtime, and
0.17.7 and 0.21.0 truncated p719 at exactly the same point. Every cell in the A/B saw the same
truncated text, so the comparison remains like-for-like and `HYPOTHESIS_DEAD` stands, as do the
+4.3 pp pooled / 0.0 pp median paired figures.

**§2's handling of the other affected papers was already correct** and needs no correction:
restricting every arm to the 36 papers all four arms have, noting that 415 failed both cells,
and noting that 547, 629 and 799 have no Run 6 counterpart. PARSE-01 adds only the reasons —
415's PDF is a 728-page conference proceedings volume acquired in place of one article, and
547/629/799 are `FT_SCREENED_OUT` papers that entered the sample through a filter gap in
`select_sample()`. Both are documented in `PARSE-01_report.md`.

**See:** `docs/session-reports/PARSE-01_report.md`. Appended by task PARSE-01; all text above
this heading is unchanged.
