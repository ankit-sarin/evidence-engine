"""The decision-log table cell for a ruling, under R656(b); committed once at the 12q closeout (R660(c)).
usage, from the repository root: render_cell.py <ruling.txt> <Rnnn>   (stdout: the cell, no newline)
The cell is the ruling's lines from the one beginning "<Rnnn>(a)" to the end, each stripped and joined
with single spaces. R656(b): in a decision-log table cell, "|" is written "\\|", and a backslash
immediately before a pipe is written "\\\\"; a backslash anywhere else is written as it is. R659(f)
accepts what that renders to under CommonMark ("\\\\" before "\\|" shows one backslash). This is the
function the scratch builders of Blocks 51–58 each carried a copy of (build_block51.py, build_blocks.py,
and the two build_item0.py); those copies stay in scratch, unused from here. Block 59 is its first use."""
import sys
PH = "\x00"


def render_cell(ruling: str, tag: str) -> str:
    lines = ruling.splitlines()
    start = next(i for i, l in enumerate(lines) if l.startswith(tag + "(a)"))
    cell = " ".join(l.strip() for l in lines[start:])
    return cell.replace("\\|", PH).replace("|", "\\|").replace(PH, "\\\\\\|")


if __name__ == "__main__":
    sys.stdout.write(render_cell(open(sys.argv[1], encoding="utf-8").read(), sys.argv[2]))
