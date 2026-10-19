# 12s-U14B step 1 — R675(c)'s variant measurement

Run 2026-10-19 17:02:47–17:06:15 UTC on HEAD `49385013` (item 0; the locator as committed at
`2d8823e`, unedited). Script `variant_measurement.py`, output `variant_measurement_output.json`. The
12s_U14A reproducer is executed unchanged and its definitions reused; the copied 300-edit generation
is asserted to reproduce 12s_U14A's own sweep (6 not located, min 0.8136) before anything is measured.
I1 holds: the sweep is seeded (`random.Random(20261019)`).

The three locators: **today** — `engine.core.locator.locate` (autojunk on); **plain A** — every window
with `autojunk=False`; **variant** — every window with autojunk on as the module does it, the k = 5 best
re-scored with `autojunk=False`, the best of those.

## 1. Lost claims, 300 seeded 3-word edits

| | not located | min score | median score |
| --- | ---: | ---: | ---: |
| today | **6** | 0.8136 | 0.9883 |
| plain A | **0** | 0.9883 | 0.9883 |
| variant | **0** | 0.9883 | 0.9883 |

Claims the variant loses that plain A locates: **none**. Edits where the variant's chosen window differs
from plain A's: **0 of 300**.

## 2. The six claims today loses

| edit # | today | plain A (score, window) | variant (score, window) |
| ---: | --- | --- | --- |
| 70 | not located, 0.8240 | located, 0.9883, 17 | located, 0.9883, 17 |
| 108 | not located, 0.8318 | located, 0.9883, 17 | located, 0.9883, 17 |
| 128 | not located, 0.8188 | located, 0.9883, 17 | located, 0.9883, 17 |
| 200 | not located, 0.8188 | located, 0.9883, 17 | located, 0.9883, 17 |
| 288 | not located, 0.8136 | located, 0.9883, 17 | located, 0.9883, 17 |
| 298 | not located, 0.8214 | located, 0.9883, 17 | located, 0.9883, 17 |

## 3. Cost on the F11 text (156,196 characters; 383-character absent snippet; three runs each)

| | runs (s) | median (s) | × today | × 5.214 s |
| --- | --- | ---: | ---: | ---: |
| today | 5.184, 5.209, 5.237 | 5.209 | 1.000 | 0.999 |
| plain A | 43.249, 43.424, 43.689 | 43.424 | 8.336 | 8.328 |
| variant | 5.211, 5.240, 5.224 | **5.224** | **1.003** | **1.002** |

## 4. The condition and the proceed

R675(c): "on the committed 300-edit reproducer it recovers all 6 lost claims with no claim lost that plain
(A) locates, and its median cost on the F11 text is at most 1.5× today's 5.214 s."

- Recovers all 6, loses nothing plain A locates: **True** (0 lost; set difference empty).
- Median ≤ 1.5 × 5.214 s = 7.821 s: **True** (5.224 s).
- Window choice differs from plain A: 0, so not ambiguous.

**R675(c) condition met → variant.**
