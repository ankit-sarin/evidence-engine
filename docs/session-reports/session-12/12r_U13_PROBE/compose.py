"""12r-U13-PROBE — compose the two cross conditions by R669(b) option (ii). Writes only under out/.
usage: compose.py <render_dir> <paper_id>...
A rendered user turn is an instruction block (every line before the one line "## Numbered Paper Text")
and a numbered text (that line and every [Sn] line after it). A cross condition takes one tree's
instruction block and the other tree's numbered text, and rewrites the instruction block's two
text-bound count lines to the recipient text's unit count:
  line 4: "... [S<n>]. You cite evidence by unit"      ("[S<donor n>]" -> "[S<recipient n>]")
  line 7: "Every index must be a bare JSON integer between 1 and <n>. ..."   (the same)
No other line changes. Each replacement must occur exactly once in its line, and the donor count must
not occur anywhere else in the instruction block (else: refuse). Self-test first: recomposing each
tree's own block with its own text reproduces its render byte for byte.
Conditions: a = HEAD numbered text + 90f4697 instructions; b = 90f4697 numbered text + HEAD instructions.
Outputs: out/cond_<a|b>_p<id>.txt (the user turn), out/compose_p<id>.json (both rewritten lines, the
units, the prompt sha256, the would-send request hash)."""
import hashlib, json, os, re, sys
HERE = os.path.dirname(os.path.abspath(__file__)); OUT = os.path.join(HERE, "out")
sys.path.insert(0, HERE)
from probe import would_send

HEADER = "## Numbered Paper Text"


def split(text):
    lines = text.split("\n")
    i = [k for k, l in enumerate(lines) if l == HEADER]
    assert len(i) == 1, f"{len(i)} header lines"
    i = i[0]
    assert all(re.match(r"\[S\d+\] ", l) for l in lines[i + 1:] if l != ""), "non-unit line after header"
    n = sum(1 for l in lines[i + 1:] if re.match(r"\[S\d+\] ", l))
    return lines[:i], lines[i:], n


def rewrite(instr, n_from, n_to):
    out = list(instr)
    for idx, old, new in ((3, f"[S{n_from}]", f"[S{n_to}]"), (6, f"between 1 and {n_from}", f"between 1 and {n_to}")):
        assert out[idx].count(old) == 1, (idx, out[idx])
        out[idx] = out[idx].replace(old, new)
    rest = "\n".join(l for k, l in enumerate(out) if k not in (3, 6))
    assert not re.search(rf"\b{n_from}\b", rest), f"donor count {n_from} elsewhere in the block"
    return out, (out[3], out[6])


render, pids = sys.argv[1], sys.argv[2:]
os.makedirs(OUT, exist_ok=True)
for pid in pids:
    head = open(os.path.join(render, f"head_p{pid}.txt"), encoding="utf-8").read()
    g90 = open(os.path.join(render, f"g90_p{pid}.txt"), encoding="utf-8").read()
    hi, ht, hn = split(head); gi, gt, gn = split(g90)
    assert "\n".join(hi + ht) == head and "\n".join(gi + gt) == g90, "self-test: recomposition differs"
    rec = {"paper_id": int(pid), "units": {"head": hn, "g90f4697": gn}, "self_test": "identity recomposition equal"}
    for cond, instr, n_from, text, n_to, desc in (
            ("a", gi, gn, ht, hn, "HEAD numbered text + 90f4697 instructions"),
            ("b", hi, hn, gt, gn, "90f4697 numbered text + HEAD instructions")):
        new, lines = rewrite(instr, n_from, n_to)
        prompt = "\n".join(new + text)
        path = os.path.join(OUT, f"cond_{cond}_p{pid}.txt")
        open(path, "w", encoding="utf-8").write(prompt)
        rec[cond] = {"composition": desc, "rewritten_line_4": lines[0], "rewritten_line_7": lines[1],
                     "prompt_chars": len(prompt), "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
                     "would_send_request_hash": would_send(prompt)[1], "file": path}
    json.dump(rec, open(os.path.join(OUT, f"compose_p{pid}.json"), "w"), indent=1)
    print(json.dumps(rec, indent=1))
