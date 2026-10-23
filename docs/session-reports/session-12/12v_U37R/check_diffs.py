"""12v-U37R step 2b (I3) — the old and the new attempt-2 request of each case differ only by U37's text.
usage, from the repository root: check_diffs.py <requests_dir>   (stdout: JSON; exit 1 if any case fails)
Per case: model and every kwarg equal; message count, roles and the system message equal; the new user
turn is the old one with insertions only (difflib on characters: no replace or delete), and the inserted
text is exactly (R-1) one block between the "re-emit EVERY field … not a patch." sentence and the first
failed entry, starting "The schema has " and ending with the JUDGMENT line, and (R-2) one block at the
end, "\n\n" + the output section the prompt sends before the paper text."""
import difflib, json, os, sys

R = sys.argv[1]
out, ok_all = {}, True
for cid in sorted(os.listdir(R)):
    o = json.load(open(os.path.join(R, cid, "old.json")))["request"]
    n = json.load(open(os.path.join(R, cid, "new.json")))["request"]
    kw_equal = {k: v for k, v in o.items() if k != "messages"} == {k: v for k, v in n.items() if k != "messages"}
    mo, mn = o["messages"], n["messages"]
    roles_equal = [m["role"] for m in mo] == [m["role"] for m in mn] == ["system", "user"]
    system_equal = mo[0] == mn[0]
    a, b = mo[1]["content"], mn[1]["content"]
    ops = [op for op in difflib.SequenceMatcher(None, a, b, autojunk=False).get_opcodes() if op[0] != "equal"]
    inserts_only = all(op[0] == "insert" for op in ops)
    inserted = [b[j1:j2] for _, _, _, j1, j2 in ops]
    i_out = a.rfind("## Output\n", 0, a.index("## Numbered Paper Text"))
    first_output = a[i_out:a.index("\n\n## Numbered Paper Text", i_out)]
    r1 = [s for s in inserted if "The schema has " in s]
    r2 = [s for s in inserted if s.endswith(first_output)]
    shape_ok = (len(inserted) == 2 and len(r1) == 1 and len(r2) == 1
                and r1[0].rstrip("\n").split("\n")[-1].startswith("- JUDGMENT (")
                and r2[0] == "\n\n" + first_output and b.endswith(r2[0]))
    ok = kw_equal and roles_equal and system_equal and inserts_only and shape_ok
    ok_all &= ok
    out[cid] = {"kwargs_equal": kw_equal, "roles_equal": roles_equal, "system_equal": system_equal,
                "inserts_only": inserts_only, "n_inserts": len(inserted), "r1_chars": len(r1[0]) if r1 else None,
                "r2_chars": len(r2[0]) if r2 else None, "r1_r2_only": shape_ok, "ok": ok,
                "r1_text": r1[0] if r1 else None}
print(json.dumps({"all_ok": ok_all, "cases": out}, indent=1, sort_keys=True))
sys.exit(0 if ok_all else 1)
