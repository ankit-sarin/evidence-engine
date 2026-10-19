# 12r-U13-PROBE — reproduction of the attempt-1 STATED-only shape on papers 3 and 10

Session 12r, briefs 12r-U13-PROBE (Phase 1) and 12r-U13-PROBE-R1 (Phase 2, under R669), 2026-10-19 UTC.
An experiment outside any run manifest: six calls to `deepseek-r1:32b` through the engine's own client
(`ollama_chat`), under the experiment lock, writing to no review. Every count below is quoted from a
record under `out/`; anything else is labelled CC's inference. The scripts here ran from
`~/scratch/12r-u13-probe/` (their `out/` is that directory's); the copies are committed unchanged.

## §1 What was sent

**Composition.** Each request is HEAD's `pass1_messages(prompt)` — the Pass-1 system turn and the user
turn `prompt` — with `stage_config("elicitation_pass1", spec)` from ra_12q's spec: `model
deepseek-r1:32b`, `options {"temperature": 0}`, `think true`, `keep_alive -1` (`phase1_hashcheck.txt`).
`ollama_chat` was called outside any run, so `check_declared_call` and `record_active_ollama_call` were
no-ops ("A no-op outside a run") and nothing was recorded to any database.

**The reproduction requests** are 12r's attempt-1 user turns, re-rendered by HEAD's builders in
12r-U13-PA (`~/scratch/12r-u13-pa/render/head_p3.txt`, `head_p10.txt`, named in
`12r_U13_PA/scratch_extracts_sha256.txt`). Every call was sent with `--expect-hash` set to 12r's stored
attempt-1 request hash (`stored_hashes.json`) and would have refused on a mismatch:

| paper | request hash (would-send = stored) | calls |
|---|---|---|
| 3 | `9769dd264f358f223a1fd6041c8025d5d8aa8ef095289383ca7a1dbe00c4fe33` | 3 |
| 10 | `b86027c8a6b0aaefc97c4f6eff3546d345d9fc7c11a4be6d80a88f94e8525821` | 3 |

**The cross conditions** were composed by R669(b) option (ii) (`compose.py` → `compose_p3.json`,
`compose_p10.json`), with the identity recomposition of each tree's own render checked equal first.
They were **not sent**, because neither paper reproduced (§2). Their rewritten lines and would-send
hashes, for the record:

| paper | condition | rewritten line 4 | rewritten line 7 (start) | would-send hash |
|---|---|---|---|---|
| 3 | a — HEAD text + 90f4697 instructions | `marker of the form [S1], [S2], ... [S1520]. You cite evidence by unit` | `Every index must be a bare JSON integer between 1 and 1520.` | `02197ddb…5c6a` |
| 3 | b — 90f4697 text + HEAD instructions | `… [S1491]. You cite evidence by unit` | `… between 1 and 1491.` | `9a3dc665…13a5` |
| 10 | a | `… [S383]. You cite evidence by unit` | `… between 1 and 383.` | `314b558a…5842` |
| 10 | b | `… [S369]. You cite evidence by unit` | `… between 1 and 369.` | `23612e5a…b53d` |

A diff of each composed condition against its recipient render shows only the four instruction
differences of `12r_U13_PA.md` §3 (line 11's clause, the two T-a sentences, the T-b sentence).

**Call count: 6** (`phase2_log.txt`), sequential, in tmux session `12r-probe`, inside one
`hold_experiment_lock()` held by pid 1169525 from 01:28:10Z to 01:56:00Z ("lock held; NRestarts start
0" … "all calls done; releasing lock"). The Ollama journal for 01:28–01:57 holds 6 `POST "/api/chat"`,
1 `POST "/api/show"` (the input-fit guard's read) and 2 `GET "/api/ps"`, and nothing else
(`step8_after.txt`).

## §2 Reproduction

The decision rule (R668, the brief's step 6): REPRODUCES when the STATED-only shape (stated ≥ 8/9,
inferable 0/6, judgment 0/5) appears in at least 2 of 3 calls.

| label | start–end UTC | done_reason | raw chars | parses | present S/I/J | STATED-only | eval_count | prompt_eval_count | thinking chars | raw sha256 |
|---|---|---|---|---|---|---|---|---|---|---|
| repro_p3_1 | 01:28:10–01:35:28 | stop | 3,174 | true | 8/6/5 | false | 1,414 | 55,425 | 2,701 | `ddb9a83e…` |
| repro_p3_2 | 01:35:28–01:40:14 | stop | 3,451 | true | 8/6/5 | false | 1,523 | 55,425 | 3,087 | `0209ac8d…` |
| repro_p3_3 | 01:40:14–01:45:00 | stop | 3,451 | true | 8/6/5 | false | 1,523 | 55,425 | 3,087 | `0209ac8d…` |
| repro_p10_1 | 01:45:00–01:49:04 | stop | 3,854 | true | 9/6/5 | false | 1,528 | 18,384 | 2,410 | `8ad3fa73…` |
| repro_p10_2 | 01:49:04–01:52:32 | stop | 3,869 | true | 9/6/5 | false | 1,532 | 18,384 | 2,447 | `9d991541…` |
| repro_p10_3 | 01:52:33–01:56:00 | stop | 3,869 | true | 9/6/5 | false | 1,532 | 18,384 | 2,447 | `9d991541…` |

**Decisions** (`decision_p3.json`, `decision_p10.json`): "DECISION paper 3: NOT REPRODUCED (0 of 3
STATED-only)"; "DECISION paper 10: NOT REPRODUCED (0 of 3 STATED-only)". Per the brief, no ablation
call was made ("no ablation calls for paper 3 (NOT REPRODUCED)", the same for paper 10).

Beside 12r's own attempt 1 (12r-U13-PA §2): paper 3 1,095 chars, present 8/0/0, raw `0421ff25…`;
paper 10 2,601 chars, 9/0/0, raw `5659a772…`. The prompt was read identically: each call's
`prompt_eval_count` equals 12r's attempt 1 (55,425 and 18,384, run.log `input_fit`). Every paper-3 call
omits the same STATED field 12r's attempt 1 omitted, `primary_outcome_value`. In both papers calls 2 and
3 returned byte-identical text, and call 1 differed from them; call 1 of paper 3 followed the model
load (`load_duration` 17.57 s; later calls 0.08–0.13 s).

## §3 Ablation

Not run. Neither paper reproduced, so the conditions a, b and c had no shape to move.

## §4 What the results support

At temperature 0 on the same model digest, the exact request that produced 12r's STATED-only attempt
1 on papers 3 and 10 produced a complete response (all INFERABLE and JUDGMENT fields present) on six of
six resends. The shape is **not reproducible** from the request: on this evidence, 12r's two attempt-1
STATED-only responses are run-to-run variation of the model's output for one fixed request, not an
effect of the request's content. So the probe cannot attribute them to U01's text or to U04's
instructions, and it gives no reason to attribute them to either. Two limits. First, three resends per
paper bound a rate, they do not measure it: an event that occurred once in 12r and zero times in six
resends is consistent with a low per-call rate (CC's inference). Second, the six calls ran in a
different server state from 12r's — 12r's paper-3 attempt 1 followed paper 2's refused call and paper
1's calls on an already-loaded model, its paper-10 attempt 1 followed paper 9's Pass 2, while this
probe's first call followed a model swap — and output at temperature 0 was not identical even within
this probe (call 1 against calls 2
and 3 in both papers), which is the same non-determinism INT-g12-5 measured on 16 of 42 pairs.

## §5 Findings, for ruling

- **F1** — NOT REPRODUCED on both papers: 0 of 3 STATED-only each ("DECISION paper 3: NOT REPRODUCED
  (0 of 3 STATED-only)", "DECISION paper 10: NOT REPRODUCED (0 of 3 STATED-only)"), on the
  byte-identical request (hash checked before each send).
- **F2** — Identical requests did not give identical responses: calls 2 and 3 were byte-identical and
  call 1 differed, in both papers (`0209ac8d…` twice after `ddb9a83e…`; `9d991541…` twice after
  `8ad3fa73…`).
- **F3** — Paper 3's response omits `primary_outcome_value` on all three resends and on 12r's attempt 1,
  and every paper-3 call answers 8 of 9 STATED fields.
- **F4** — `eval_count` is available on the response (1,414–1,532 here) and is recorded by this probe;
  the engine still records it nowhere (INT-g12-6).
- **F5** — One kernel line during the model swap at the first call:
  `2026-10-19T01:28:24 … NVRM: nvCheckOkFailedNoLog: Check failed: Out of memory [NV_ERR_NO_MEMORY]`;
  the load completed (17.57 s) and the call returned `stop`. The third time on a completing load
  (2026-10-17 07:04:29, 2026-10-18 22:14:28).
- **F6** — The STATED-only re-emission after STATED-only feedback (INT-r12-1, proposed) was not probed:
  this brief sent attempt-1 requests only.

**Readings.** Step 5 (01:28:03Z): `/api/ps` `gemma3:27b`; `NRestarts=0`; earlyoom kills 0;
MemAvailable 81,805,004 kB; the mask job on `UCD-FIL-023` at 11:54; load 17.52; the lock free. Step 8
(01:56:42Z): `/api/ps` `deepseek-r1:32b` (resident, `keep_alive -1`); `NRestarts=0`; earlyoom kills 0;
MemAvailable 58,708,512 kB; the mask job on `UCD-FIL-023` at 40:34; the lock free; tmux `12r-probe`
closed. Model swap time on the first call: `load_duration` 17.57 s.

**Not committed** (named by sha256 in `out/scratch_extracts_sha256.txt`): the six raw responses and the
four composed condition prompts (they carry the full numbered paper text), under
`~/scratch/12r-u13-probe/out/`.
