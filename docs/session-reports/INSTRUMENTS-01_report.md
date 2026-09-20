# INSTRUMENTS-01 — correct kappa, correct scoring, tests that see them

**Date:** 2026-09-20. **Task type:** repair + regeneration. **No model call. The live
`review.db` was never opened for write.**
**Harness session:** `bd7ea59e-96e8-4b4d-a2a1-6f6adb1ecb93`. **HEAD at start:** `ed0d92a`.

**Tags.** **MEASURED** = produced here by running code. **READ** = quoted from a file at this
HEAD. **INFERRED** = a reading or a cause.

---

## Phase 1 read-out

### 1. Startup verify

| check | EXPECTED | measured | |
|---|---|---|---|
| HEAD / tree / origin | `ed0d92a`, clean, level | identical, `0 0` | ✅ |
| standard gate | 2,343 / 17 — 504/608/377/438/416, desel 0/0/10/6/1 | identical | ✅ |
| `review.db` | 101,978,112 B @ 2026‑09‑11 02:00:52.636956943 UTC | identical, `-wal` 0 B | ✅ |
| fingerprint `--compare` | exit 0 vs `f376562e…39e00` | **IDENTICAL**, exit 0 | ✅ |
| `tests/test_eligibility.py` | 91 passed | 91 passed | ✅ |

M1, M2, M3, M5 and M6 re-verified and hold. **M4 refined:** **61 of 63** rows equal
`1 − 1/(2·p_o)` to four decimals; the two exceptions are the degenerate endpoints where that
closed form is undefined and the old code special-cased them (`key_limitation/o4mini_vs_sonnet`,
p_o = 0 → κ 0; `field_1/o4mini_vs_sonnet`, p_o = 1 → κ **1.0**).

### 2. Kappa callers — I3 CONFIRMED

`cohens_kappa` had exactly one non-test caller, `metrics.field_summary`, which had two:

| caller | holds the labels? |
|---|---|
| `engine/analysis/concordance.py::run_concordance` | **yes** — `for pid, fname, val_a, val_b in aligned:` |
| `analysis/paper1/export_disagreement_pairs.py::build_disagreement_rows` | **yes** — `values = {arm: … for arm in ARMS}` |

No caller was verdict-only, so the STOP did not fire.

### 3. Numeric dispatch — I1 CONFIRMED, with divergences

The codebook declares `type` per field: **categorical 11, free_text 8, numeric 1**
(`sample_size`). Categorical dispatch already read it; numeric dispatch did not.

| dispatch | source | matched the codebook? |
|---|---|---|
| categorical | `field_def.type == "categorical"` | ✅ |
| numeric | hardcoded `_NUMERIC_FIELDS = {"sample_size"}` | agreed **by coincidence** |
| passthrough | hardcoded `_PASSTHROUGH_NUMERIC_FIELDS` | names a field the codebook types `free_text` |
| multi-value | hardcoded `_MULTI_VALUE_FIELDS` | no codebook source; its `secondary_outcomes` entry is **dead** |

Units and qualifiers were handled by `re.sub(r"[^\d]", "", raw)` — deleting every non-digit and
returning a **string**. MEASURED: `"2.5"` → `"25"`, `"-5"` → `"5"`, `"12.5%"` → `"125"`,
`"50-60"` → `"5060"`. So **`score_pair("sample_size", "2.5", "25")` was MATCH**, which is worse
than M2's "5 vs 50".

### 4. The March artifact — I2 PARTLY FALSE

The generator is committed; **the inputs are not.** The labels live in `review.db` (gitignored),
and the committed CSV holds only **2,267 of 3,802 cells** — the 1,535 all-MATCH cells a kappa
needs are absent from it. The generator also opens the database **read-write**
(`sqlite3.connect(db_path)` at `export_disagreement_pairs.py:70`, `concordance.py:81,174`).
The CI method **is** recoverable: `# Analytical SE (Fleiss, 1981)`, κ ± 1.96·se.

---

## What was built

### `engine/analysis/metrics.py` — one Cohen's kappa

READ, the module's own statement of the defect:

> The raters' own label distributions were gone before the function was entered, so that is not
> Cohen's kappa. It reduces to `kappa = 1 - 1 / (2 * p_o)`, a function of the observed agreement
> alone, carrying no information the percent-agreement column did not already carry.

`cohens_kappa(labels_a, labels_b)` takes two aligned label sequences; `p_e = Σ_c p_A(c)·p_B(c)`
over the union of labels either rater used. The Fleiss-1981 SE is retained unchanged, so the CI
method of the superseded figures is the CI method of these.

**The undefined case returns `nan`, not `1.0`.** When both raters used one label and it was the
same label, `p_e` is 1 and kappa is 0/0 — sklearn returns `nan` there too, and
`KappaResult.undefined_reason` says which case it was. Both raters constant on *different*
labels is **not** undefined: `p_e` is 0, `p_o` is 0, kappa is 0.0.

`FieldSummary` now carries **two agreement numbers**, deliberately: `percent_agreement` (the
scorer's verdict rate over decisive pairs) and `kappa_p_o` (verbatim label agreement, which is
what kappa corrects). They answer different questions and were previously one column.

Kappa is computed over **every** aligned pair including scorer-AMBIGUOUS ones — an ambiguous
verdict is a statement about the scorer's confidence, not about what the raters wrote, and the
labels are known either way.

### `engine/analysis/scoring.py` — three rules

**Numbers (S1).** `parse_number` is a grammar, not a deletion: optional qualifier (`n=`, `~`,
`approx`, `about`, `ca.`), sign, digits with optional thousands separators, optional decimal
fraction, optional `%`, optional trailing unit word. Exact equality, no tolerance. Both parse and
equal ⇒ MATCH; both parse and unequal ⇒ MISMATCH; exactly one parses ⇒ **AMBIGUOUS, never
MATCH**; neither parses ⇒ the text rules. A unit **marker** (`%`) is kept and differing markers
are AMBIGUOUS (ruling 4b); a unit **word** is discarded, because on a count "50 patients" and
"50 cases" are the same answer.

**Token boundary (S2).** Containment counts only as a contiguous run of whole tokens, punctuation
being a boundary rather than a character. `"da Vinci Xi"` inside `"da Vinci Xi (Intuitive
Surgical)"` still matches; `"cid"` inside `"acidosis"` does not.

**Polarity (S3).** One declared, documented, tested `NEGATION_CUES` frozenset. Cues are matched as
whole **tokens**, which is why multi-word negations need no entries — "did not" is caught by
"not", "failed to" by "failed", "lack of" by "lack". Checked **before** containment, because
containment is exactly how `"benefit"` and `"no benefit"` used to match: the negation makes the
shorter value a substring of its own contradiction.

**One correction made during Phase 2, against my own first draft.** Ruling 4b's own example
(`"12.5%"` vs `"12.5"`) still matched after the first implementation, because
`primary_outcome_metric` is a `free_text` field and tokenization drops both the decimal point and
the percent sign — `"12.5%"` and `"12.5"` both tokenize to `["12", "5"]`. The numeric rule now
runs **before** the token rules whenever **both** sides parse as numbers, on any field. Caught by
writing the ruling's example as a test.

### `engine/analysis/normalize.py` — dispatch only

Numeric dispatch is the codebook's `type`, replacing `_NUMERIC_FIELDS`. Absence comes from the
codebook's `absence_sentinels`, replacing `_NULL_SYNONYMS`. Normalization no longer interprets
numbers at all — `_normalize_numeric` is deleted, and text is canonicalized here while
`scoring.parse_number` is the one place that knows what a number is. **The review-id literal
(`_FALLBACK_REVIEW_ID`) is untouched — it is S9's.** The two remaining hardcoded lists are
annotated in place for S5b:

> `_PASSTHROUGH_NUMERIC_FIELDS` names `primary_outcome_value`, which the codebook types
> `free_text`; the branch skips lowercasing, so it is a third normalization behaviour with no
> declaration behind it. `_MULTI_VALUE_FIELDS` has no codebook source at all, and its
> `secondary_outcomes` entry is DEAD.

**The absence change is inert on this corpus, and that is worth stating plainly.** MEASURED on
the scratch copy:

| token | local spans | cloud spans | effect |
|---|---:|---:|---|
| `NR` | 176 | 79 | absence before and after |
| `N/A` | 1 | 0 | absence before and after |
| `NOT_FOUND` | 6 | 87 | absence before and after |
| **`NA`** | **0** | **0** | codebook sentinel the old set lacked |
| **`NOT FOUND`** | **0** | **0** | codebook sentinel the old set lacked |
| **`none`** | **0** | **0** | was absence, is now a value |
| `NOT REPORTED`, `""`, `N/R` | 0 | 0 | — |

**Zero rows move.** The divergence was real and is now closed; it changed no published number.

---

## The 63 kappas, regenerated

**Route (ruling 1).** `auto_backup` took a scratch copy of the live database — read-only on its
source — which fingerprinted **`f376562e…39e00`, IDENTICAL to the committed record**; the
codebook was copied beside it (sha `f4bd7b39…b128a`, identical); the **shipped generator's own
functions**, unmodified, were pointed at the copy and at new dated filenames. The live database
was never opened for write, and no `mode=ro` change was made to the analysis readers — that is
session 6's.

**Artifacts** (under `data/`, which is gitignored, so the committed record is this report):

```
disagreement_pairs_3arm_20260920T230645Z.{csv,xlsx,html}
disagreement_pairs_3arm_kappa_old_vs_new_20260920T230645Z.csv   (63 rows)
disagreement_pairs_3arm.SUPERSEDED.md                            (the sidecar note)
```

### Summary

| measure | value |
|---|---|
| rows | **63** (old: 63) |
| comparable rows (both defined) | 62 |
| **moved by more than 0.05** | **55** |
| **largest move** | `key_limitation / local_vs_o4mini`, **−11.5 → 0.0048** (Δ 11.5048) |
| **old values outside kappa's range [−1, 1]** | **4** |
| new values outside [−1, 1] | **0** |
| kappa's n, every row | **189** (the full grid for the 189 shared papers) |
| old n (scorer-decisive) | 5 – 189 |
| disagreement rows | 2,267 → **2,266** (10 left, 9 entered) |

**🔴 Four of the sixty-three published "kappas" were outside kappa's mathematical range.** No
Cohen's kappa can be less than −1. That is not an inaccuracy in the values; it is proof on the
face of the artifact that the column was not a kappa:

| field | arm pair | old value | p_o |
|---|---|---:|---:|
| `key_limitation` | local_vs_o4mini | **−11.5** | 0.0400 |
| `secondary_outcomes` | local_vs_sonnet | **−3.4167** | 0.1132 |
| `secondary_outcomes` | o4mini_vs_sonnet | **−1.8** | 0.1786 |
| `key_limitation` | local_vs_sonnet | **−1.5** | 0.2000 |

Each is `1 − 1/(2·p_o)` exactly.

**The two endpoint rows** (the ones M4's closed form could not describe):

| field | arm pair | old | new | why |
|---|---|---|---|---|
| `key_limitation` | o4mini_vs_sonnet | 0 | **0.0** | now a *genuine* zero: 361 distinct labels, p_o 0.0 |
| `field_1` | o4mini_vs_sonnet | **1** | **undefined (nan)** | **one** category — both arms absent on all 189 papers, so kappa is 0/0. The old code reported perfect reliability where nobody made a choice. |

**INFERRED, and the reason free-text kappas are all near zero:** with 285–361 distinct labels over
189 papers, `p_e` is tiny and κ ≈ p_o. That is correct Cohen's kappa; it is simply not an
informative statistic for free text, and the old formula's output *looked* more plausible than
the correct one does. The numbers are now honest about how little verbatim agreement there is.

### The 63 rows

🔴 = the old value was outside [−1, 1]. ⚠ = not a codebook field.

| # | field | arm pair | type | old κ | **new κ** | Δ | κ p_o | pct_agr | n(κ) | n(dec) | cats |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | `robot_platform` | local_vs_o4mini | free_text | 0.2817 | **0.1875** | -0.0942 | 0.195767 | 0.7019 | 189 | 104 | 285 |
| 2 | `robot_platform` | local_vs_sonnet | free_text | 0.4507 | **0.0531** | -0.3976 | 0.058201 | 0.9146 | 189 | 82 | 334 |
| 3 | `robot_platform` | o4mini_vs_sonnet | free_text | 0.2917 | **0.1023** | -0.1894 | 0.10582 | 0.7273 | 189 | 88 | 312 |
| 4 | `task_performed` | local_vs_o4mini | free_text | 0.1417 | **0.1626** | 0.0209 | 0.164021 | 0.5825 | 189 | 103 | 312 |
| 5 | `task_performed` | local_vs_sonnet | free_text | 0.3438 | **0.0** | -0.3438 | 0.0 | 0.7619 | 189 | 63 | 367 |
| 6 | `task_performed` | o4mini_vs_sonnet | free_text | 0.1774 | **0.0** | -0.1774 | 0.0 | 0.6078 | 189 | 51 | 355 |
| 7 | `comparison_to_human` | local_vs_o4mini | free_text | 0.2813 | **0.2472** | -0.0341 | 0.592593 | 0.6957 | 189 | 161 | 99 |
| 8 | `comparison_to_human` | local_vs_sonnet | free_text | 0.3167 | **0.1576** | -0.1591 | 0.42328 | 0.7339 | 189 | 124 | 155 |
| 9 | `comparison_to_human` | o4mini_vs_sonnet | free_text | 0.3444 | **0.1778** | -0.1666 | 0.42328 | 0.7627 | 189 | 118 | 152 |
| 10 | `primary_outcome_metric` | local_vs_o4mini | free_text | -0.25 | **0.1703** | 0.4203 | 0.179894 | 0.3953 | 189 | 129 | 258 |
| 11 | `primary_outcome_metric` | local_vs_sonnet | free_text | 0.2134 | **0.0356** | -0.1778 | 0.037037 | 0.6357 | 189 | 129 | 313 |
| 12 | `primary_outcome_metric` | o4mini_vs_sonnet | free_text | 0.1129 | **0.0361** | -0.0768 | 0.037037 | 0.5804 | 189 | 112 | 332 |
| 13 | `primary_outcome_value` | local_vs_o4mini | free_text | -0.625 | **0.1045** | 0.7295 | 0.121693 | 0.3289 | 189 | 152 | 302 |
| 14 | `primary_outcome_value` | local_vs_sonnet | free_text | -0.2128 | **0.0411** | 0.2539 | 0.042328 | 0.4174 | 189 | 115 | 347 |
| 15 | `primary_outcome_value` | o4mini_vs_sonnet | free_text | 0.0159 | **0.0826** | 0.0667 | 0.084656 | 0.5538 | 189 | 130 | 325 |
| 16 | `secondary_outcomes` | local_vs_o4mini | free_text | -0.7593 | **0.0651** | 0.8244 | 0.100529 | 0.2842 | 189 | 95 | 304 |
| 17 | `secondary_outcomes` | local_vs_sonnet | free_text | -3.4167 🔴 | **0.014** | 3.4307 | 0.015873 | 0.1132 | 189 | 53 | 343 |
| 18 | `secondary_outcomes` | o4mini_vs_sonnet | free_text | -1.8 🔴 | **0.0136** | 1.8136 | 0.015873 | 0.1636 | 189 | 55 | 335 |
| 19 | `key_limitation` | local_vs_o4mini | free_text | -11.5 🔴 | **0.0048** | 11.5048 | 0.005291 | 0.04 | 189 | 25 | 360 |
| 20 | `key_limitation` | local_vs_sonnet | free_text | -1.5 🔴 | **0.0** | 1.5 | 0.0 | 0.2 | 189 | 5 | 378 |
| 21 | `key_limitation` | o4mini_vs_sonnet | free_text | 0 | **0.0** | 0.0 | 0.0 | 0.0 | 189 | 18 | 361 |
| 22 | `field_1` ⚠ | local_vs_o4mini | free_text | 0.4973 | **0.0** | -0.4973 | 0.994709 | 0.9947 | 189 | 189 | 2 |
| 23 | `field_1` ⚠ | local_vs_sonnet | free_text | 0.4973 | **0.0** | -0.4973 | 0.994709 | 0.9947 | 189 | 189 | 2 |
| 24 | `field_1` ⚠ | o4mini_vs_sonnet | free_text | 1 | ****undef**** | — | 1.0 | 1.0 | 189 | 189 | 1 |
| 25 | `sample_size` | local_vs_o4mini | numeric | 0.1168 | **0.4388** | 0.322 | 0.534392 | 0.5401 | 189 | 187 | 60 |
| 26 | `sample_size` | local_vs_sonnet | numeric | -0.0739 | **0.3823** | 0.4562 | 0.439153 | 0.4439 | 189 | 187 | 76 |
| 27 | `sample_size` | o4mini_vs_sonnet | numeric | 0.1085 | **0.4837** | 0.3752 | 0.518519 | 0.5185 | 189 | 189 | 77 |
| 28 | `study_type` | local_vs_o4mini | categorical | 0.4892 | **0.4953** | 0.0061 | 0.978836 | 0.9788 | 189 | 189 | 6 |
| 29 | `study_type` | local_vs_sonnet | categorical | 0.4836 | **0.1383** | -0.3453 | 0.968254 | 0.9683 | 189 | 189 | 6 |
| 30 | `study_type` | o4mini_vs_sonnet | categorical | 0.4947 | **0.331** | -0.1637 | 0.989418 | 0.9894 | 189 | 189 | 4 |
| 31 | `surgical_domain` | local_vs_o4mini | categorical | -0.3056 | **0.2356** | 0.5412 | 0.380952 | 0.383 | 189 | 188 | 15 |
| 32 | `surgical_domain` | local_vs_sonnet | categorical | -0.3214 | **0.2378** | 0.5592 | 0.37037 | 0.3784 | 189 | 185 | 17 |
| 33 | `surgical_domain` | o4mini_vs_sonnet | categorical | 0.3199 | **0.5619** | 0.242 | 0.719577 | 0.7351 | 189 | 185 | 15 |
| 34 | `autonomy_level` | local_vs_o4mini | categorical | 0.2617 | **0.186** | -0.0757 | 0.677249 | 0.6772 | 189 | 189 | 9 |
| 35 | `autonomy_level` | local_vs_sonnet | categorical | 0.1637 | **0.2199** | 0.0562 | 0.597884 | 0.5979 | 189 | 189 | 8 |
| 36 | `autonomy_level` | o4mini_vs_sonnet | categorical | 0.0825 | **0.1727** | 0.0902 | 0.544974 | 0.545 | 189 | 189 | 8 |
| 37 | `country` | local_vs_o4mini | free_text | 0.3427 | **0.6729** | 0.3302 | 0.698413 | 0.7606 | 189 | 188 | 46 |
| 38 | `country` | local_vs_sonnet | free_text | 0.39 | **0.6113** | 0.2213 | 0.640212 | 0.8142 | 189 | 183 | 67 |
| 39 | `country` | o4mini_vs_sonnet | free_text | 0.3392 | **0.5631** | 0.2239 | 0.592593 | 0.7513 | 189 | 189 | 62 |
| 40 | `study_design` | local_vs_o4mini | categorical | -0.125 | **0.2207** | 0.3457 | 0.444444 | 0.4444 | 189 | 189 | 8 |
| 41 | `study_design` | local_vs_sonnet | categorical | 0.0455 | **0.3059** | 0.2604 | 0.52381 | 0.5238 | 189 | 189 | 7 |
| 42 | `study_design` | o4mini_vs_sonnet | categorical | 0.2125 | **0.4844** | 0.2719 | 0.634921 | 0.6349 | 189 | 189 | 7 |
| 43 | `system_maturity` | local_vs_o4mini | categorical | -0.3897 | **0.1805** | 0.5702 | 0.359788 | 0.3598 | 189 | 189 | 6 |
| 44 | `system_maturity` | local_vs_sonnet | categorical | 0.0644 | **0.2743** | 0.2099 | 0.534392 | 0.5344 | 189 | 189 | 7 |
| 45 | `system_maturity` | o4mini_vs_sonnet | categorical | 0.1 | **0.4086** | 0.3086 | 0.555556 | 0.5556 | 189 | 189 | 7 |
| 46 | `task_execute` | local_vs_o4mini | categorical | 0.3824 | **0.1783** | -0.2041 | 0.809524 | 0.8095 | 189 | 189 | 7 |
| 47 | `task_execute` | local_vs_sonnet | categorical | 0.4273 | **0.4271** | -0.0002 | 0.873016 | 0.873 | 189 | 189 | 6 |
| 48 | `task_execute` | o4mini_vs_sonnet | categorical | 0.3615 | **0.1612** | -0.2003 | 0.783069 | 0.7831 | 189 | 189 | 5 |
| 49 | `task_generate` | local_vs_o4mini | categorical | 0.3102 | **0.1663** | -0.1439 | 0.724868 | 0.7249 | 189 | 189 | 5 |
| 50 | `task_generate` | local_vs_sonnet | categorical | 0.1 | **0.0795** | -0.0205 | 0.555556 | 0.5556 | 189 | 189 | 5 |
| 51 | `task_generate` | o4mini_vs_sonnet | categorical | 0.1085 | **0.1648** | 0.0563 | 0.560847 | 0.5608 | 189 | 189 | 4 |
| 52 | `task_monitor` | local_vs_o4mini | categorical | 0.2786 | **0.097** | -0.1816 | 0.693122 | 0.6931 | 189 | 189 | 4 |
| 53 | `task_monitor` | local_vs_sonnet | categorical | 0.3 | **0.2288** | -0.0712 | 0.714286 | 0.7143 | 189 | 189 | 4 |
| 54 | `task_monitor` | o4mini_vs_sonnet | categorical | 0.244 | **0.2791** | 0.0351 | 0.661376 | 0.6614 | 189 | 189 | 4 |
| 55 | `task_select` | local_vs_o4mini | categorical | 0.2895 | **0.1337** | -0.1558 | 0.703704 | 0.7037 | 189 | 189 | 5 |
| 56 | `task_select` | local_vs_sonnet | categorical | 0.3392 | **0.221** | -0.1182 | 0.756614 | 0.7566 | 189 | 189 | 5 |
| 57 | `task_select` | o4mini_vs_sonnet | categorical | 0.3903 | **0.1605** | -0.2298 | 0.820106 | 0.8201 | 189 | 189 | 4 |
| 58 | `validation_setting` | local_vs_o4mini | categorical | 0.2347 | **0.339** | 0.1043 | 0.518519 | 0.6533 | 189 | 150 | 30 |
| 59 | `validation_setting` | local_vs_sonnet | categorical | 0.3257 | **0.4025** | 0.0768 | 0.57672 | 0.7415 | 189 | 147 | 31 |
| 60 | `validation_setting` | o4mini_vs_sonnet | categorical | 0.4111 | **0.5792** | 0.1681 | 0.714286 | 0.8491 | 189 | 159 | 16 |
| 61 | `clinical_readiness_assessment` | local_vs_o4mini | categorical | 0.37 | **0.1538** | -0.2162 | 0.793651 | 0.7937 | 189 | 189 | 5 |
| 62 | `clinical_readiness_assessment` | local_vs_sonnet | categorical | 0.4019 | **0.2482** | -0.1537 | 0.835979 | 0.836 | 189 | 189 | 5 |
| 63 | `clinical_readiness_assessment` | o4mini_vs_sonnet | categorical | 0.4057 | **0.3146** | -0.0911 | 0.84127 | 0.8413 | 189 | 189 | 5 |

---

## 🔴 `field_1` and `Title` — two parse-artefact field names (ruling 2)

MEASURED on the scratch copy, read-only:

| name | local spans | cloud spans | paper | value |
|---|---:|---:|---:|---|
| `field_1` | **1** | 0 | 719 | *"The paper presents a dynamic potential field method for robot path planning…"* — a model preamble |
| `Title` | **1** | 0 | 415 | *"Enhancing Left Ventricle Segmentation in Echocardiograms Using Anatomically Constrained CycleGAN…"* — the paper's title |

Neither is in the codebook; every one of the 20 codebook fields has spans. **`field_1` reached
the published figures and `Title` did not** — paper 415 is not shared across all three arms, and
the grid is built from the 189 papers that are. INFERRED: the difference is an accident of which
paper the artefact landed on, not a filter.

`field_1` appears in three of the 63 rows and is **kept**, so the old and new tables are
row-comparable. Its `o4mini_vs_sonnet` row is the undefined one above: both cloud arms are absent
on it for all 189 papers, which is exactly what a field that does not exist should look like.

---

## Tests (T1) — old assertion and new, side by side

| test | OLD | NEW |
|---|---|---|
| `test_substring_containment` | `assert "substring" in s.detail` on the da Vinci pair | same pair, `assert "containment" in s.detail` — the case the rule was written for, kept |
| *(new)* `test_containment_does_not_cross_word_boundaries` | — | `"cid"` vs `"acidosis"` and `"Raven"` vs `"Ravensbourne"` are **not** MATCH |
| *(new)* `test_containment_cannot_match_across_a_negation` | — | `"benefit"` vs `"no benefit"` is **not** MATCH, and the detail names the negation |
| `test_null_synonyms` | parametrized over a hardcoded set including `N/R` and `none` | parametrized over the **codebook's** sentinels, now including `NA` and `not found` |
| *(new)* `test_none_is_a_value_not_an_absence` | — | `normalize("key_limitation", "none") == "none"` |
| *(new)* `test_a_sentinel_the_codebook_does_not_declare_is_a_value` | — | `"N/R"` → `"n/r"` |
| `test_sample_size_strips_nonnumeric` | `normalize("sample_size", "n=42") == "42"` | **deleted.** Replaced by `test_numeric_normalization_no_longer_deletes_characters`: `"n=42"` stays `"n=42"`, `"2.5"` stays `"2.5"`, `"-5"` stays `"-5"` |
| `test_sample_size_with_spaces` | `normalize(" 100 patients ") == "100"` | folded into the same test: `"100 patients"` |
| *(new)* `test_the_numbers_still_compare_equal_through_score_pair` | — | what the old normalization was *for* still works one layer down; and `"2.5"` vs `"25"` is now MISMATCH |
| `TestCohensKappa::test_perfect_agreement` | `cohens_kappa([MATCH]*10).kappa == 1.0` | ten identical labels from **one** category is the undefined case (`nan`); **two** labels in perfect agreement gives 1.0 |
| `TestCohensKappa::test_no_agreement` | `== 0.0` "degenerate: p_e = 1.0" | total disagreement on two labels is **−1.0**, the floor |
| `TestCohensKappa::test_mixed_agreement` | `assert 0 < kr.kappa < 1.0` — **a range** | `assert kr.kappa == 0.7368` — **an exact value, equal to sklearn** |
| `TestCohensKappa::test_ambiguous_excluded_from_n` | `kr.n == 3  # AMBIGUOUS excluded` | renamed `test_every_pair_counts_including_scorer_ambiguity`: `kappa_n == 4`, `n == 3` |
| `TestCohensKappa::test_all_ambiguous` | `isnan(kappa)`, `n == 0` | scorer-ambiguous pairs still have labels, so kappa is **defined**; `percent_agreement` is the `nan` |
| `TestPipelineWithMockData::test_all_mismatch_categorical` | `kappa == 0.0  # degenerate: p_e = 1.0` | `kappa == −0.5` — three labels, identical marginals, zero agreement. Systematic disagreement is worse than chance and the old collapse could not express it |

**Files:** `tests/test_kappa.py` (13 tests, new), `tests/test_scoring_rules.py` (37 tests, new),
plus the rewrites above in `tests/test_concordance.py` and `tests/test_concordance_pipeline.py`.

### K2 — three fixtures against sklearn

| fixture | construction | true κ | engine | sklearn |
|---|---|---:|---:|---:|
| **A** | balanced 2×2, a=45 d=45 b=5 c=5; both marginals 50/50 | 0.8000 | **0.8000** | 0.8000 |
| **B** | one constant rater, 95% agreement | 0.0000 | **0.0000** | 0.0000 |
| **C** | 3 categories, **asymmetric** marginals A 60/30/10 vs B 63/28/9 | — | **0.4896030…** | 0.4896030245746692 |

Fixture C's expected value is **taken from sklearn, not hand-set** (`test_fixture_c_value_is_taken_from_sklearn_not_hand_set`), so the per-rater marginal
is proven rather than assumed. The old implementation returns 0.4444, 0.4737 and 0.3151 on the
three. `test_the_old_closed_form_is_gone` asserts no fixture's kappa still equals `1 − 1/(2·p_o)`.
`test_marginals_matter_not_just_agreement` pins the property the old code *could not have*: two
sets with identical `p_o` and different marginals now get different kappas.

### The frozen study's kappa still agrees

`analysis/eval/score_screen2f.py` is **not imported into** — it returns a bare rounded float and
`None` on the degenerate branch where this one returns `nan`, so "a pure import with identical
output" was not available. `test_agrees_with_the_frozen_screen2f_kappa_including_the_2g_value`
rebuilds the 2g P3 A-vs-D contingency table from `scoring.json` (86 papers) and asserts all three
implementations give **0.1258**, plus 1e-9 agreement on fixtures A, B and C. The one-predicate
guarantee is bought by check rather than by sharing, which is the honest trade when one side is
frozen.

---

## Gate table

| gate | requirement | measured | |
|---|---|---|---|
| **G1** | Phase 1 read-out delivered and acknowledged before any `engine/` change | delivered → ruling → first `engine/` write | ✅ |
| **G2** | three fixtures equal sklearn to 1e-9; the 2g P3 A-vs-D 0.1258 unchanged | A 0.8000, B 0.0000, C 0.4896030245746692, all within 1e-9; **0.1258** from the frozen scorer, the new kappa and sklearn | ✅ |
| **G3** | of the five Part A pairs only the da Vinci pair is MATCH; `"5"` vs `"50"` MISMATCH; `"benefit"` vs `"no benefit"` not MATCH | MISMATCH / MISMATCH / AMBIGUOUS / **MATCH** / AMBIGUOUS; da Vinci **MATCH** | ✅ |
| **G4** | 63 rows old-vs-new committed; March files byte-identical; SUPERSEDED note present; scratch fingerprint recorded and its deletion reported; `field_1` and sentinel counts in the report | 63-row table above; March sha256 unchanged (below); note written; scratch `f376562e…39e00`; counts above | ✅ |
| **G5** | standard gate green; new count stated; deselects 17; fingerprint exit 0 at open and close; `review.db` unchanged | **2,397 passed / 17 deselected** (+54) — 508/608/390/475/416, deselects 0/0/10/6/1; `--compare` exit 0 at open and close; `review.db` byte- and mtime-identical | ✅ |
| **G6** | T1 named with the old assertion and the new one side by side | the table above, 15 rows | ✅ |

**March files, byte-identical at open and close** — `…csv` `1772407daa939324fb586212db8dbfaab63f72f06b01e9dd297c2334c7b64b7c`, `…xlsx` `a181a895c9203991f501819572158a5319ec7e51804141eb56957c3b3cf1d994`, `…html` `34a4e86d9bf4a9340125b84f857edd7f0ab36f7af34f1223011db44d9f7e12f6`, all still mtime 2026‑03‑20 19:32.

---

## Inventory rows this session adds (for the architect's list)

1. **I-class, ARMED, closes in session 6** — *analysis readers open the live database read-write*:
   `engine/analysis/concordance.py` (×2) and `analysis/paper1/export_disagreement_pairs.py` (×1)
   call `sqlite3.connect(db_path)` with no `mode=ro`. Not changed here by ruling; the regeneration
   went through a scratch copy instead.
2. **A-class** — *parse-artefact field names stored as spans*: `field_1` (paper 719) and `Title`
   (paper 415), one span each, neither in the codebook; `field_1` has been in every published
   figure since March. Removal belongs to the unexpected-fields guard plus a one-time cleanup.
3. **For S5b** — *two hardcoded dispatch lists that are not the codebook's*:
   `_PASSTHROUGH_NUMERIC_FIELDS` (names a `free_text` field and skips lowercasing) and
   `_MULTI_VALUE_FIELDS` (no codebook source; its `secondary_outcomes` entry is dead code).

---

## What this does not claim

* **The judge was not re-run and must not be**, per the brief: the disagreement set moved by 19
  rows, so a judge run now would not be comparable with the one on record. That is session 6's.
* **No published figure other than the 63 kappas was regenerated.** The judge tables, the PI audit
  workbooks and the provenance census all still rest on the March disagreement set.
* **The free-text kappas are correct and uninformative.** With ~300 distinct labels over 189
  papers, `p_e` ≈ 0 and κ ≈ `p_o`. Reporting them is honest; treating them as reliability
  coefficients for free text would not be.
* **The absence-sentinel fix changed no current number** (zero spans carry `NA`, `NOT FOUND` or
  `none`). It is in scope because the regenerated kappas had to be computed under one absence
  vocabulary, not two.
* `analysis/eval/score_screen2f.py`, the PI audit's weighted kappa, the judge and its loader, the
  frozen surfaces, the live spec and the codebook were **not touched**.
