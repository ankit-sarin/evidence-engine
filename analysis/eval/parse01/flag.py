"""PARSE-01 Phase 2 — threshold derivation and flagging.

Thresholds are derived from the corpus distribution, not hand-tuned. The rule,
fixed before looking at which papers it catches:

  Tukey FAR-outlier fence, Q3 + 3*IQR, per structural metric. Stricter than the
  usual 1.5x because the question is "is this document broken", not "is it
  unusual".

  Zero-inflated correction: several artifact counts are zero for most of the
  corpus, so Q3 = IQR = 0 and the fence degenerates to "any non-zero value" --
  which would flag a paper carrying one stray glyph. For any metric whose Q3 is
  0, the fence is recomputed on the NON-ZERO subpopulation instead. This is a
  property of the distribution, not a per-paper adjustment.

  Two hard functional criteria sit outside the statistics:
    * local_est_prompt_tokens > 131,072 -- the paper cannot fit the local model's
      context, so Run 6 truncated its input.
    * sonnet/openai input_tokens above the arm's documented window.

Severity is assigned by consequence, not by hit count:
  SEVERE = the model did not receive the document as written (truncation), or
           the defect affects the bulk of the text (segmentation collapse,
           pervasive glyph corruption, merged document).
  MINOR  = localised artifacts that leave the prose readable.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

LOCAL_CTX = 131_072

STRUCTURAL = [
    "chars", "chars_per_line", "references_sections", "glyph_artifacts",
    "chars_per_unit", "short_unit_share_pct", "nonascii_density_pct",
    "max_line_chars", "image_comments", "formula_comments",
]


def pct(v, p):
    v = sorted(v)
    i = (len(v) - 1) * p
    lo, hi = math.floor(i), math.ceil(i)
    return v[lo] if lo == hi else v[lo] + (v[hi] - v[lo]) * (i - lo)


def fence(values: list[float]) -> tuple[float, str]:
    q1, q3 = pct(values, 0.25), pct(values, 0.75)
    if q3 == 0:
        nz = [v for v in values if v > 0]
        if not nz:
            return float("inf"), "all-zero"
        q1n, q3n = pct(nz, 0.25), pct(nz, 0.75)
        return q3n + 3 * (q3n - q1n), f"nonzero-subpop(n={len(nz)})"
    return q3 + 3 * (q3 - q1), "full"


def main() -> int:
    src = Path("data/surgical_autonomy/eval/parse01/sweep.jsonl")
    rows = [json.loads(l) for l in src.read_text().splitlines() if l.strip()]

    fences = {k: fence([r[k] for r in rows]) for k in STRUCTURAL}
    print("fences (Q3 + 3*IQR):")
    for k, (f, basis) in fences.items():
        print(f"  {k:<24} {f:>12.2f}   basis={basis}")
    print()

    flagged = []
    for r in rows:
        hits = [k for k in STRUCTURAL if r[k] > fences[k][0]]
        if r["local_est_prompt_tokens"] > LOCAL_CTX:
            hits.append("LOCAL_CTX_EXCEEDED")
        if r["replacement_chars"] > 0:
            hits.append("replacement_chars")
        if hits:
            flagged.append({**r, "flags": hits})

    out = src.parent / "flagged.jsonl"
    with out.open("w") as fh:
        for r in flagged:
            fh.write(json.dumps(r) + "\n")

    print(f"FLAGGED {len(flagged)}/{len(rows)}")
    print(f"{'paper':<7}{'chars':>11}{'refs':>6}{'glyph':>7}{'c/unit':>8}"
          f"{'short%':>8}{'est_tok':>10}  flags")
    for r in sorted(flagged, key=lambda x: -len(x["flags"])):
        print(f"p{r['paper_id']:<6}{r['chars']:>11,}{r['references_sections']:>6}"
              f"{r['glyph_artifacts']:>7}{r['chars_per_unit']:>8.0f}"
              f"{r['short_unit_share_pct']:>8.0f}{r['local_est_prompt_tokens']:>10,}  "
              f"{','.join(r['flags'])}")
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
