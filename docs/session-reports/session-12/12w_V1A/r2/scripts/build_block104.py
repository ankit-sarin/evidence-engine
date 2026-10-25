import importlib.util
spec = importlib.util.spec_from_file_location("rc", "docs/session-reports/session-12/12q_close/render_cell.py")
rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
r = open("docs/session-reports/session-12/12w_V1A/r706.txt", encoding="utf-8").read()
plan = open(P, encoding="utf-8").read()
assert plan.endswith("The decision log in this file ends at R705.\n")
# R706(b): the dated note on INT-v12-1, appended to its row's last cell.
ROW_END = " | `12v_RSA.md` §Q3, F13 |\n"
lines = plan.splitlines(keepends=True)
idx = [i for i, l in enumerate(lines) if l.startswith("| INT-v12-1 | ")]
assert len(idx) == 1 and lines[idx[0]].endswith(ROW_END)
note = ("*Note 2026-10-25 (12w-V1A resume item 0, R706(b)):* the row's text is MEASURED (12w-V1A Q2, "
        "`a0f2323`), no longer INFERRED. V1 (attempt 2's feedback block as a third user message, in "
        "`run_pass1` only): elicited pin `d70bf688…59f0`, unchanged; attempt-2 request sha256 "
        "`123f6630…4db2` (HEAD `307077c9…71a3`); fails "
        "`test_elicitation_pipeline.py::test_the_elicited_requests_are_byte_identical`. V2 (the same "
        "change in `render_messages` only): elicited pin `8f66789a…ec0f`; request `307077c9…71a3`, "
        "unchanged; fails `test_prompt_hash_coverage.py::test_elicitation_pass1_hashes_both_attempts`. "
        "V3 (both): elicited pin `8f66789a…ec0f`; request `123f6630…4db2`; both tests fail. The "
        "non-elicited pin `d3672ab6…ff5c` throughout. The gap is the pin's: the fixture digest test sees "
        "request bytes in the suite, not in the pin (R706(b)); V2 moves the pin with no request byte "
        "changed (R706(c)). Class 2, P1 unchanged.")
l = lines[idx[0]]
lines[idx[0]] = l[: -len(" |\n")] + " " + note + " |\n"
plan = "".join(lines)
hdr = ("**Block 104.** *Transcribed 2026-10-25 (item 0 of the 12w-V1A resume), in its own commit with the "
       "INT-v12-1 note, in the R597(e) form:* ruling R706, made by PI on architect recommendation on "
       "2026-10-25 UTC, during session 12w, on the 12w-V1A report stopped at Q2 (record "
       "`a0f2323332f993558ecbbda1560c0205ed9738c6`). The 12w-V1A resume that followed the ruling in the "
       "paste is not part of the block. The ruling as issued, with its line breaks:")
cell = rc.render_cell(r, "R706")
made = "PI, 2026-10-25, during session 12w, on architect recommendation, on the 12w-V1A report stopped at Q2 (a0f2323)"
block = ("\n" + hdr + "\n\n```text\n" + r + "```\n\n| Date | Ruling | Made by |\n| --- | --- | --- |\n"
         f"| 2026-10-25 | {cell} | {made} |\n\nThe decision log in this file ends at R706.\n")
open(P, "w", encoding="utf-8").write(plan + block)
print("appended lines", block.count("\n"), "row line", idx[0] + 1)
