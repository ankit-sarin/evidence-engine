# 12l-P1A — P1 Phase A read-out (R607, R608)

**Status: COMPLETE. Read-only.** No change to `engine/`, `tests/`, `scripts/`, `analysis/`, `review_specs/`, `CLAUDE.md` or the plan; no model call; no gate run; live `review.db` read `mode=ro` by the fingerprint compare only. Reference HEAD `82a33d751b3cb22cbad19c34299893af9d7cc884`.

This is P1's package plan for one PI ruling. Everything not listed in §2 is the proposed default.

How it was made:
- **The row table** is built by `p1_rows.py` from the plan (it runs `12k_est/est_12k.py` and `12k_0c/recount_12k.py` on the plan at HEAD) → `p1_rows.json`, `p1_rows.md`.
- **The reads** were gathered at HEAD by six read-only readers, one per code area, and are in `p1_evidence.json`. Every quote in it is checked by `check_quotes.py` to be an exact substring of its file at `82a33d7` (read from git, not the working tree) and to lie at the stated lines: **{QUOTES} quotes checked, 0 failed, 0 corrected**. One quoted file is outside the repository (`~/scripts/ollama_health_check.sh`, which resolves into `dgx-infra`) and is read from disk; the crontab line is reported in its row's summary, not as a checked quote.
- **The plan itself** — pin effect, class, unit and order per row — is the lead's, in `p1_plan.py`; `build_readout.py` renders §1's counts and §3–§5 from it and the evidence.
- Where a reader ran a pure in-process check on synthetic strings, its row says so. Nothing was run against a model or a database.

## 0. Pre-check and the brief's assumptions

- HEAD `82a33d7`, clean, level with origin; `db_fingerprint --compare` exit 0, IDENTICAL.
- **I1 holds.** P1 is 47 rows, Class 1 / 2 / 3 = 29 / 14 / 4, and 37 fix units by 12k-EST's rules, re-derived from the plan at `82a33d7`. The P1 rows' records equal 12k-EST's (`est_12k.json`) and `recount_12k.json`'s P1 line.
- **I2 holds, with a precision.** 16 P1 rows state nothing about the pin (A-1, A-10, A-11, A-1N, A-4b, A-7, B-F14, B-F17, C49, C56, E-ROOT, E-TEST, E-UNITMAP, INT-g12-4, INT-g12-6, INT-g12-7). A further 12 Wave 1 rows carry no statement on the row itself and 9 Wave 2 rows rest on the readers' block reading; 10 rows state one. This read-out states all 47.
- **I3 holds.** R588, quoted: "row 64: Class 2, P1, Phase A first (A4) — an audit model of the same family as the arm's extractor is refused or recorded at run open (the settled cross-family rule)." The row it governs is INT-i12-45: "'Cross-family' verification is stated but nothing compares the audit model with the arm's model."
- **I4 holds, with a precision.** R539, quoted: "a change to what the elicited run sends (unit construction, prompts, format schemas, stage set) lands before the freeze smoke records the binding pin. Audit-side changes (locator, verdicts) are outside the pin". In code the arm pin is `run_manifest.pin_tuple`: per extraction stage the model, model digest, options hash, format-schema hash and prompt hash, plus the arm's kind, provider and model, the codebook's semantic hash and the client library version. The prompt hash is rendered around fixed two-line sentinel inputs. The two senses can part: a change can alter what is sent on real papers without moving the hash, or move the hash without altering anything sent. Each row below says which. A row is called **affecting** if either is true, or if it changes arm-binding logic (the brief's fourth category, which R539 does not name and INT-g12-4 needs).

## 1. Counts

{COUNTS}

## 2. DECISIONS NEEDED

Fourteen items. Each gives the finding, the options, and a recommendation.

### D1 — What lands before the comparison rehearsal, and which unit is first

Two rulings each say "first": R540's P1 line puts INT-g6-1 "pin-affecting, first", and R542 says the run-fault predicate "lands first, with the extract-path handlers, among the pin-affecting items". The two units touch different files and neither depends on the other. Separately, R571 places the rehearsal "after (a), (b) and (e) land" and says nothing about the other extract-path fixes.

- **Option A (recommended):** U01 units as slices, then U02 the predicate; then every unit that changes what the elicited run sends, binds or stores (U03–U11) and the lock unit (U12), then the rehearsal. The rehearsal then measures the behaviour that will be tagged, and runs under a lock that the 07:00 and 09:00 jobs respect. Cost: the rehearsal waits on twelve units instead of five.
- **Option B:** the rehearsal straight after the pin-affecting units (U01–U05). It comes sooner, but the floor, the blank-value guard and the duplicate-field handling then change stored outcomes after it, and its per-field comparison against `12g_RA-B.md` is taken on behaviour that will not ship.

### D2 — INT-i12-1 is stated "checker-side, not pin-affecting", and its fix changes what is sent

The fix moves no template, schema or stage, and no hash unless a new violation code or feedback line is added. But JUDGMENT fields that pass today will fail, so attempt 2 is sent on more papers and those fields drop out of the Pass-2 priming message. It also shares its subject with R571(b), which rewrites the same contract sentence.

- **Option A (recommended):** treat it as pin-affecting and land it in U04 with R571(b), before the rehearsal. One change to the JUDGMENT contract, text and check together, measured once.
- **Option B:** keep it as its own unit after the rehearsal, as the row states. The rehearsal's JUDGMENT failure counts would then be taken with the check still lenient.

### D3 — An elicited run declares and pins a stage it cannot send (new, R509)

`extraction_stages` gives an elicited run `elicitation_pass1`, `extract_pass2` and `extract_retry_snippet`, and all three enter the pin. Nothing under `engine/elicitation` calls the snippet retry. R539 names the stage set as part of the pin.

- **Option A (recommended):** remove `extract_retry_snippet` from the elicited stage set in U04. The pin then names only what the run can send; C54's check already refuses an undeclared call. Proposed as a new row, Class 3, P1, pin-affecting. Cost: the pin value moves once, before any binding pin exists.
- **Option B:** leave it. Nothing wrong is stored; but a later edit to the snippet-retry template would force a new arm name for a run that never sends it.

### D4 — R571(a) names the STATED contract; the same collision sits on an INFERABLE field

R571(a): "A sentence in the STATED contract makes a field instruction to record a sentinel subject to the sentinel rule". The unconditional "record NR" instruction is on three STATED fields (`sample_size`, `comparison_to_human`, `secondary_outcomes`) and on `country`, which is INFERABLE. A sentence in the STATED contract does not reach it.

- **Option A (recommended):** put the sentence in the shared sentinel rule, which every class reads. It covers all four fields. It departs from R571's wording, not its intent.
- **Option B:** STATED only, as worded. `country` keeps the collision and the rehearsal will show it or not.

### D5 — R535's floor, as worded, does not close Wave 1 row 1; no rule says which attempt is stored

R535: "An exhausted record holding fewer than half of the expected fields is planned as `extraction_failed`". None of it is in the code yet, which is expected. Two gaps in the wording:
- The floor counts fields held. A record of 20 `contract_unmet` fields holds 20, so it passes. That is Wave 1 row 1's case, and it is produced by an empty or non-JSON Pass-1 response.
- That record is written directly on attempt 1 with no refusal, so it is never an "exhausted record".

Options for the floor:
- **Option A (recommended):** the floor counts fields that carry a value (an asserted value or a cited sentinel); `contract_unmet` and `declined` do not count; and it applies to every record at the write boundary, exhausted or not. A run-wide Pass-1 failure then reads as failures and reaches the abort. Cost: a paper the model genuinely declines on more than half its fields is recorded failed and reselected.
- **Option B:** `contract_unmet` does not count, `declined` does. Narrower; a record of 20 declines is still stored as extracted.
- **Option C:** R535 as worded, plus a separate rule for the all-unmet record.

Wave 1 row 9 — at budget exhaustion only the last attempt is stored — needs its own answer:
- **Option A (recommended):** the outer driver uses the inner loop's rule: a later attempt replaces an earlier one only if it holds strictly more valued fields; a tie keeps the earlier. One rule in both loops.
- **Option B:** last attempt wins, as now, and the docstring says so.

### D6 — The run-fault predicate: its class list, and handlers 12–15

R531–R533 name run faults by kind: `sqlite3.OperationalError`, store `sqlite3` errors, "connection errors", `CeilingUnavailable`. `TimeoutError` and the overflow family stay paper outcomes. Three things are not settled by the text:
- "Connection errors" has no class list. Today `outcome_for_exception` maps every `httpx.HTTPError` and every `ollama` client error to a paper outcome, and after a restart `ollama_client` relabels client errors as `TimeoutError`, which R532 keeps as a paper outcome.
- R533 says "handlers 7, 10–13 included"; R542 assigns commits to 4–7 and 8–11 only, yet closes the row "when every converting handler except 3 passes its test". Handlers 12–13 (`pdf_parser._commit_attempts`) and 14–15 (the preflight's two handlers, which produce no paper outcome) have no commit.
- The brief for this task listed "the input-fit family" among run faults; R532 makes only `CeilingUnavailable` one.

Recommended: the predicate is one function over a declared tuple — `sqlite3.Error`, `CeilingUnavailable`, `UndeclaredCall`, `UndeclaredOverride`, `RunRefused`, `RunAborted`, `EventRefused`, `CodebookContractError`, `ExhaustedWithoutRecord`, and connection-refused / connect-timeout errors by their `httpx` and `ollama` classes, the exact names fixed in U02's handoff from `ollama_client`'s raises. `TimeoutError` after the watchdog stays a paper outcome under R532, bounded by the abort. Handlers 12–13 go with U26; 14–15 go to P7 with handler 3. The alternative for 14–15 is U02, at the cost of touching the preflight in the first predicate commit.

### D7 — INT-g12-4: how the arm comes to be pinned at the extract stage (Phase A first, R569)

At HEAD `open_run` pins every arm a manifest names, inside the manifest's transaction, and a screen start declares the extraction stages. The pin tuple is resolved at open. Four designs, as read from the code:

| design | what changes | what it gets | what it costs |
| --- | --- | --- | --- |
| **A — pin written at the extract stage** | `open_run` registers the arm unpinned and compares against any existing pin; `_stage_extract` writes the pin before `verify_extraction_run` | R569(b) as worded, for every start stage; no migration (the unpinned-to-pinned update on a claimless arm is already legal and tested) | the pin cannot be written into that run's immutable manifest body, so the arm row's `pinned_run_id` is its record; the extract-time check must compare the whole tuple, not the digest alone, which ends R349's "not a defect" premise |
| B — pin only when the run starts at extract | `_open_run_manifest` tells `open_run` which arms to pin | the pin stays inside the manifest transaction | a screen start that runs through to extract in one process (as live would, its adjudication being complete) is refused as unpinned, so B alone does not meet the ruling |
| C — a screen start declares no extraction stage | the stage list stops at the gate | nothing in `open_run` changes | a run that passes the gate needs a second manifest, against one manifest per run (R73) |
| D — pin at the first call or claim | the pre-send check or the event writer writes the pin | — | later than "reaches the extract stage": a run that selects no paper leaves no pin |

Recommended: **A**. It is the only design that meets R569(b) for every start. It also makes `CLAUDE.md`'s R237 sentence ("The first run whose manifest declares an extraction stage pins the arm, whether or not it extracts") false, so that sentence needs the PI's edit in the same change. Cloud arms are unaffected: a cloud run's open is its extract start.

### D8 — INT-g12-1: the form of the citation record (R568: "The form is set by P1's Phase A")

The claim's `payload_json` is free-form and already takes named keys, so no migration is needed. Proposed form, one key on each asserted claim: an ordered list of the cited runs, in cited order, each with its first and last unit index and its start and end character offsets into the parsed text the claim already names by sha256, plus the units version. It resolves without the unit-map directory, which is what R568 asks. The stored snippet stays the first run.

- **Option A (recommended):** offsets and unit indices, as above.
- **Option B:** the runs' text copied into the payload. Self-contained, but it stores the paper's text a second time on 89 of 177 claims by 12g's measure.

### D9 — INT-i12-3: R588 says "recorded where a reader sees it"; no reader exists

The divergence flags are set on a dict nothing reads, and nothing reads claim payload keys either. A ruling needs to name the reader.

- **Option A (recommended):** the field is named on the claim's payload, and the run summary — the same reader R535 gives `incomplete_fields` — prints the count and the fields. One reader for both.
- **Option B:** the telemetry row only. It is on disk but no one is shown it.

### D10 — INT-i12-45: no model-family attribute exists (A4)

Nothing in the arm declaration, the audit-model declaration, the resolved stage, the pin or `open_run` carries a family. The fix must declare one. R588 leaves "refused or recorded" open.

- **Option A (recommended):** each model declaration in the spec gains a `family`; `open_run` refuses a run whose audit model shares the extractor's family, and the manifest records both. The family stays out of `pin_tuple`, so no pin moves. Cost: the live spec is edited and its sha256 changes; the audit model's `model=` override (side finding N15) must go through the same check.
- **Option B:** recorded only. Nothing can refuse; the cross-family rule stays a statement.

### D11 — Four class moves

| row | from | to | evidence | effect |
| --- | --- | --- | --- | --- |
| INT-i12-6 | 1 (conditional) | 3 | Legacy path only, and the trigger is a legacy-prompt response that returns the escape token — a token the legacy prompt never mentions. The elicited dispatch happens before any legacy code. | Rides with INT-i12-2 in U07; same function. |
| INT-i12-8 | 1 | 2 | The locator half is confirmed: an exact-located snippet returns `bridged=True`. But `located` stays correct and nothing reads `bridged`. The auditor half is off the pipeline: `audit_span` is called only by one script and tests. | Stays in U14 with INT-i12-7, as R590 rules. |
| C24 (Wave 2 row 28) | 1 (conditional) | 2 | What is wrong is the run's record of a call, not a stored value. Cloud only. | U29 unchanged. |
| INT-j12-19 | 1 (conditional, weak) | 3 | The raw response is kept; the difference is confined to the legacy cloud tables that nothing behind the one reader reads; a test pins the Anthropic half as intended. | U32 unchanged. |

If all four are accepted P1 is Class 1 / 2 / 3 = 25 / 16 / 6. The alternative is to leave any of them at Class 1; nothing in the unit plan changes either way.

Rows whose trigger was established and whose class is **kept**: INT-i12-11 (trigger measured absent today — neither Run 7 model's Modelfile sets `num_ctx` — kept Class 1 latent because a truncated paper would be extracted as if whole); A-10's Wave 1 row 7 note (0 instances in the 446 parsed files on disk; fixed inside U01 anyway); the cloud rows B9, C28, INT-i12-71, INT-j12-18 and INT-i12-10 (not reachable on live while R71 holds; each stores or accepts a wrong value once cloud runs); INT-i12-9 (Class 1 only for a wrongly copied file; the likelier case, a `review:` key left unedited, is refused loudly by the same fix).

### D12 — 35 fix units against 12k-EST's 37

| 12k-EST | proposed | why |
| --- | --- | --- |
| INT-g12-1 and E-UNITMAP, two units | one, U03 | E-UNITMAP is Class 1 only while the unit map is the sole route to a claim's citation set; once the record is on the claim it is the same change's housekeeping |
| INT-i12-1, its own unit | joins the INT-h12-2 batch, U04 | D2 |
| INT-i12-2 and INT-i12-6, two units | one, U07 | one function, `check_citations` |
| INT-i12-7 and INT-i12-8, two units | one, U14 | R590 rules them as one LOCATOR_VERSION bump |
| C56 with A-7, one unit | three: U02, U26, U27 | R542's three commits |

Every other 12k-EST unit is unchanged. The comparison rehearsal is a placed step (U13), not a fix unit. The alternative is 12k-EST's grouping as it stands, which costs two more handoffs on single functions and puts C56's three commits under one.

### D13 — Two row premises that did not survive contact with disk

- **B-F14's "Phase A measurement" was not made.** Whether closing the client cancels generation needs a model call, which this task bars. Recommended: U21's handoff opens with that measurement on its own terms. The fix does not depend on the answer; the answer decides only whether an abandoned request still occupies the server.
- **B-F17(f) is half true.** The row says the nightly "ends `exit 0`, so cron never sees a red run". The script has `set -euo pipefail`, so a failing pytest exits non-zero before that line; a synthetic check confirmed it. What is true is that the nightly runs every tier with no marker expression, and that nothing reads its status: the cron line has no redirect, wrapper or mail. R527's fix stands; "propagates its exit status" is already met and "runs the standard gate" is the change. Recommended: U12's test asserts the marker expression, and keeps the status check only as a regression pin.

### D14 — Two repair scripts write cloud spans on live outside the opt-in (new, R509)

`scripts/reparse_cloud_spans.py` and `scripts/backfill_cloud_spans.py --confirm` call the cloud response parser and insert into `cloud_evidence_spans` with no run manifest, no completeness guard and no `enabled_arms` check. They are the one route by which INT-j12-18's repair can write to live while R71 holds, conditional on a zero-span `cloud_extractions` row existing — not counted, since the database was not opened for it.

- **Option A (recommended):** retire both under R31. They served Run 6's cloud tables, which are legacy telemetry now. Proposed as a new row, Class 1 conditional, P1, closing with U31.
- **Option B:** keep them and put them behind `open_run` and the opt-in. More work for scripts with no forward use.

