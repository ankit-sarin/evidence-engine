"""One-off: dump 28 UNSUPPORTED Pass 2 smoke verdicts as a markdown report.

Reads fabrication_verifications for the smoke run_id, joins each row to the
arm's extracted value (extractions.extracted_data for 'local',
cloud_extractions.extracted_data for cloud arms), and writes a human-readable
markdown file.
"""
from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

DB = Path("data/surgical_autonomy/review.db")
RUN_ID = "surgical_autonomy_pass2_smoke_20260421T122916Z"
PAIRS_CSV = Path("data/surgical_autonomy/exports/disagreement_pairs_3arm.csv")
OUT = Path("analysis/paper1/reports/pass2_smoke_unsupported_eyeball.md")

ARM_TO_COL = {
    "local": "local_value",
    "openai_o4_mini_high": "o4mini_value",
    "anthropic_sonnet_4_6": "sonnet_value",
}


def load_pairs_lookup() -> dict[tuple[str, str], dict]:
    lookup: dict[tuple[str, str], dict] = {}
    with PAIRS_CSV.open(newline="") as f:
        for r in csv.DictReader(f):
            lookup[(r["paper_id"], r["field_name"])] = r
    return lookup


def load_arm_value_from_pairs(pairs: dict, arm: str) -> str:
    col = ARM_TO_COL.get(arm)
    if col is None:
        return "<unknown arm>"
    val = pairs.get(col, "")
    if val is None or val == "":
        return "_(empty / not emitted)_"
    return val


def load_arm_value(conn: sqlite3.Connection, paper_id: str, arm: str, field: str) -> str:
    if arm == "local":
        row = conn.execute(
            "SELECT extracted_data FROM extractions WHERE paper_id=? "
            "ORDER BY id DESC LIMIT 1",
            (int(paper_id),),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT extracted_data FROM cloud_extractions "
            "WHERE paper_id=? AND arm=?",
            (int(paper_id), arm),
        ).fetchone()
    if not row or not row[0]:
        return "<missing>"
    try:
        data = json.loads(row[0])
    except json.JSONDecodeError:
        return "<unparseable>"
    val: object = "<field missing>"
    items: list = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for key in ("fields", "data", "extractions", "extracted_fields"):
            if isinstance(data.get(key), list):
                items = data[key]
                break
        else:
            if field in data:
                val = data[field]
    for item in items:
        if isinstance(item, dict) and item.get("field_name") == field:
            val = item.get("value", "<value missing>")
            break
    if isinstance(val, (dict, list)):
        val = json.dumps(val, ensure_ascii=False)
    return str(val)


def fmt(s: str | None, max_len: int = 400) -> str:
    if s is None:
        return "_(null)_"
    s = str(s).strip().replace("\n", " ").replace("|", "\\|")
    if len(s) > max_len:
        s = s[: max_len - 3] + "..."
    return s


def main() -> None:
    conn = sqlite3.connect(DB)
    pairs_lookup = load_pairs_lookup()
    rows = conn.execute(
        """
        SELECT paper_id, field_name, arm_name,
               verification_span, reasoning, fabrication_hypothesis,
               pre_check_short_circuit
        FROM fabrication_verifications
        WHERE judge_run_id=? AND verdict='UNSUPPORTED'
        ORDER BY CAST(paper_id AS INTEGER), field_name, arm_name
        """,
        (RUN_ID,),
    ).fetchall()

    lines: list[str] = []
    lines.append(f"# Pass 2 smoke — UNSUPPORTED verdicts ({len(rows)})")
    lines.append("")
    lines.append(f"**Run:** `{RUN_ID}`")
    lines.append("")
    lines.append(
        "One block per UNSUPPORTED verdict. `value` is what the arm extracted; "
        "`span` is what Gemma quoted from the source (null = nothing quotable "
        "found); `reasoning` is why Gemma ruled UNSUPPORTED; `hypothesis` is "
        "Gemma's guess at the fabrication mode."
    )
    lines.append("")

    for i, (paper_id, field, arm, span, reasoning, hypothesis, sc) in enumerate(rows, 1):
        pairs_row = pairs_lookup.get((str(paper_id), field))
        if pairs_row is not None:
            value = load_arm_value_from_pairs(pairs_row, arm)
        else:
            value = load_arm_value(conn, paper_id, arm, field)
        sc_marker = " · short_circuit" if sc else ""
        lines.append(f"## {i}. paper {paper_id} / `{field}` / **{arm}**{sc_marker}")
        lines.append("")
        lines.append(f"- **value:** {fmt(value)}")
        lines.append(f"- **span:** {fmt(span)}")
        lines.append(f"- **reasoning:** {fmt(reasoning)}")
        lines.append(f"- **hypothesis:** {fmt(hypothesis)}")
        lines.append("")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT} ({len(rows)} verdicts)")


if __name__ == "__main__":
    main()
