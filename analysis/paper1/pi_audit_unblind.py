"""PI audit unblinding + precision scoring — STUB.

After the PI completes adjudication on the blinded workbook, this
module joins the adjudication decisions against the unblinding key,
computes per-class (SUPPORTED / PARTIALLY_SUPPORTED / UNSUPPORTED /
UNCLEAR) and per-arm precision, and writes a results markdown + CSV.

Stub exists so the code path is reserved and reviewable now. The real
implementation lands as its own task after PI adjudication returns.
"""

from __future__ import annotations


def unblind_and_compute_precision(
    adjudication_workbook_path: str,
    key_workbook_path: str,
    output_path: str,
) -> None:
    """Join completed adjudication workbook against the key; compute
    per-class and per-arm precision; write a results markdown + CSV.
    NOT YET IMPLEMENTED."""
    raise NotImplementedError("Implement after PI adjudication returns.")
