#!/usr/bin/env python3
"""Write one decision-log block from an rNNN.txt and its metadata, in the record's form (R746(e)(ii)).

usage, from the repository root:
  transcribe_ruling.py <rNNN.txt> <block number> <meta.json> [--check-plan <plan>] [--append <plan>]
With neither option the block goes to stdout. --check-plan compares it byte for byte with that block in the plan;
--append appends it, refusing unless the plan's last line is the ruling before it's "ends at" line (meta prev_ends).

The metadata (every value per use, R742(b)):
  tag          "R744"
  transcribed  the transcription date
  item         the clause's item text: "item 0 of the 12aa-U20B brief"
  beside       what the commit carries beside the block ("Block 139", "the lane-line copy it orders"), or null
  made_by      "PI"; made_date; session
  report       the report as the header names it, full hashes in backticks
  report_short the report as the row names it, short hashes
  sentence     {"form": "model", "followed": "12aa-U20B brief"}, or
               {"form": "block133", "resume": "12aa-U20P-R2", "brief": "12aa-U20B"} (R737(c))
  prev_ends    (--append only) the previous ruling's tag
The cell is 12q_close/render_cell.py's (R656(b)).
"""
import argparse, importlib.util, json, re, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("render_cell", REPO / "docs/session-reports/session-12/12q_close/render_cell.py")
rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)


def sentence(s: dict) -> str:
    if s["form"] == "model":
        return f"The {s['followed']} that followed the ruling in the paste is not part of the block."
    if s["form"] == "block133":
        return (f"The {s['resume']} resume that carried the ruling in substance, and the {s['brief']} brief "
                "whose paste carried its text, are not part of the block.")
    raise SystemExit(f"REFUSED: unknown sentence form {s['form']!r}")


def block(ruling: str, number: int, m: dict) -> str:
    beside = f" with {m['beside']}" if m.get("beside") else ""
    head = (f"**Block {number}.** *Transcribed {m['transcribed']} ({m['item']}), in its own commit{beside}, in the R597(e) "
            f"form:* ruling {m['tag']}, made by {m['made_by']} on architect recommendation on {m['made_date']} UTC, during "
            f"session {m['session']}, on {m['report']}. {sentence(m['sentence'])} The ruling as issued, with its line breaks:")
    row = (f"| {m['made_date']} | {rc.render_cell(ruling, m['tag'])} | {m['made_by']}, {m['made_date']}, during session "
           f"{m['session']}, on architect recommendation, on {m['report_short']} |")
    return (f"\n{head}\n\n```text\n{ruling}```\n\n| Date | Ruling | Made by |\n| --- | --- | --- |\n{row}\n\n"
            f"The decision log in this file ends at {m['tag']}.\n")


def in_plan(plan: str, number: int) -> str:
    start = plan.index(f"\n\n**Block {number}.**") + 1
    nxt = plan.find(f"\n\n**Block {number + 1}.**", start)
    return plan[start:] if nxt < 0 else plan[start:nxt + 1]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("ruling"); ap.add_argument("number", type=int); ap.add_argument("meta")
    ap.add_argument("--check-plan"); ap.add_argument("--append")
    a = ap.parse_args()
    ruling = Path(a.ruling).read_text(encoding="utf-8")
    meta = json.loads(Path(a.meta).read_text(encoding="utf-8"))
    text = block(ruling, a.number, meta)
    if a.check_plan:
        found = in_plan(Path(a.check_plan).read_text(encoding="utf-8"), a.number)
        same = found == text
        print(f"Block {a.number} ({meta['tag']}): byte-identical {same}; generated {len(text.encode())} B, plan {len(found.encode())} B")
        return 0 if same else 1
    if a.append:
        plan = Path(a.append).read_text(encoding="utf-8")
        if not plan.endswith(f"\nThe decision log in this file ends at {meta['prev_ends']}.\n"):
            sys.exit(f"REFUSED: the plan's last line is not the {meta['prev_ends']} ends-at line")
        last = max(int(x) for x in re.findall(r"^\*\*Block (\d+)\.\*\*", plan, flags=re.M))
        if a.number != last + 1:
            sys.exit(f"REFUSED: block {a.number} does not follow block {last}")
        Path(a.append).write_text(plan + text, encoding="utf-8")
        print(f"appended Block {a.number} ({meta['tag']}): +{text.count(chr(10))}")
        return 0
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
