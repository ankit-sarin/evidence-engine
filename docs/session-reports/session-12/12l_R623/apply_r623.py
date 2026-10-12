"""12l item 0 (12l-U11 brief) — applies R623's transcription to the plan: Block 21, a dated note on
INT-i12-9, the "L (12l additions, R623)" table (INT-l12-24) and its coverage-map line. Run once
from the repository root; refuses if already applied. --recount <file> adds the recount paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "459575d:docs/session-reports/session-12/12l_U10/12l_U10.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "(12l item 0, R623), by script" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R623" not in s and "INT-l12-24" not in s, "already applied"

RULING = '''R623 — U10's findings (R509), on 12l_U10 (459575d).
(a) F1 (loaders that open no manifest check no codebook identity) and F2
    (load_codebook_beside's docstring states the disproved premise): one new row,
    Class 3, P7.
(b) F3: CLAUDE.md's list of open_run refusals gains "`CodebookReviewMismatch`
    (INT-i12-9)" after "`CloudArmNotEnabled` (S3g)", in the session-12l closeout
    commit, matched verbatim apart from line wraps.
(c) F4, F5, F6: no row.'''
flat = " ".join(RULING.split())
def clause(l): return re.search(rf"\({l}\) (.*?)(?= \([a-c]\) |$)", flat).group(1).strip()
assert s.endswith("during session 12l |\n")
s = s.rstrip("\n") + f'''

**Block 21.** *Transcribed 2026-10-12 (12l item 0 of the 12l-U11 brief), in its own commit with the dated note and the row it orders, in the R597(e) form:* ruling R623, made by PI on architect recommendation during session 12l on unit U10's report (`docs/session-reports/session-12/12l_U10/12l_U10.md`, `459575d`), carried in the 12l-U11 brief. The ruling as issued, with its line breaks:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe at item 0 as
R623 in Block 20's form)

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
RO = "`12l_U10.md`"
def note(rid, src, body):
    global s
    lines = s.split("\n"); i = [k for k, l in enumerate(lines) if l.startswith(f"| {rid} |")][0]
    assert lines[i].endswith(" |") and " | " not in body
    lines[i] = lines[i][:-2] + f" *Note 2026-10-12 (12l item 0, {src}):* {body} |"; s = "\n".join(lines)
def has(x): assert x in MD, x[:50]; return x
f1 = has("`load_codebook` and `load_codebook_beside` are called from about 40 sites in `engine/` and `scripts/` (exporters, the validators, concordance, the auditor's tokens, the cloud base). A reader pointed at a database whose neighbouring codebook is another review's uses it unrefused.")
f2 = has("\"No identity check: the path came from the database's own location, which IS the review.\" The file can be another review's; `open_run` is now where that is caught.")
f3 = has("\"…or refuses: `DirtyTree`, `PreManifestArm` (R59), `RetiredArm` (R21), `ArmPinMismatch` (R10, naming the differing keys), `CloudArmNotEnabled` (S3g), or a digest failure.\"")
note("INT-i12-9", "R623(b); U10 F3", f"U10 landed at `a332f36` (`open_run` refuses a codebook whose `review` is not `spec.review_id`, or is empty, with `CodebookReviewMismatch`, before anything is resolved or written). Its finding F3, as {RO} states it: \"CLAUDE.md's list of `open_run` refusals does not name the new one. {f3}\" R623(b): \"{clause('b')}\"")
rows = [("INT-l12-24", "F1 — the loaders that open no manifest still check no identity. " + f1 + " F2 — `load_codebook_beside`'s docstring gives as its reason the premise this row disproved. " + f2, "3", "P7", "R623(a)", f"{RO} \"R509 findings\", F1 and F2 (`engine/core/codebook.py::load_codebook`, `load_codebook_beside`). R623(a): \"{clause('a')}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12l additions, R623).** *Opened 2026-10-12 (12l item 0 of the 12l-U11 brief) under R509 and R623.* One row from unit U10's report ({RO}, `459575d`): `INT-l12-24` for its findings F1 and F2 together (R623(a)). The text cell is each finding's heading and the report's own sentences, quoted unchanged; class and package are R623's. Not rows, as ruled: F3 is a dated note of this date on INT-i12-9, with the CLAUDE.md text it orders for the session-12l closeout commit (R623(b)); F4, F5 and F6 open nothing (R623(c)).

| # | On disk (the unit report, quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-l12-23 | P7 — R622(a) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-l12-24 | P7 — R623(a) |\n")
open(P, "w", encoding="utf-8").write(s); print("applied")
