import importlib.util
spec = importlib.util.spec_from_file_location("rc", "docs/session-reports/session-12/12q_close/render_cell.py")
rc = importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
r = open("docs/session-reports/session-12/12w_V1A/r705.txt", encoding="utf-8").read()
plan = open(P, encoding="utf-8").read()
assert plan.endswith("The decision log in this file ends at R704.\n")
hdr = ("**Block 103.** *Transcribed 2026-10-25 (item 0 of the 12w-V1A brief), in its own commit, in the "
       "R597(e) form:* ruling R705, made by PI on architect recommendation on 2026-10-25 UTC, during session "
       "12w, on the 12w-U17B final report (read-out "
       "`c5ab50579bbad95246e3d54bb6769da7fa14c43c`). The 12w-V1A brief that followed the ruling in the "
       "paste is not part of the block. The ruling as issued, with its line breaks:")
cell = rc.render_cell(r, "R705")
made = "PI, 2026-10-25, during session 12w, on architect recommendation, on the 12w-U17B final report (c5ab505)"
block = ("\n" + hdr + "\n\n```text\n" + r + "```\n\n| Date | Ruling | Made by |\n| --- | --- | --- |\n"
         f"| 2026-10-25 | {cell} | {made} |\n\nThe decision log in this file ends at R705.\n")
open(P, "w", encoding="utf-8").write(plan + block)
print("appended lines", block.count("\n"))
