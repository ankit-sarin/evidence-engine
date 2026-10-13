"""12m-U12-C1b item 0 — applies R626's transcription to the plan: a dated note on R625
(R626(d)(1)), Block 24, the "L (12m additions, R626)" table (INT-m12-4) and its coverage-map line,
and a dated note on A-4b (R626(c)). Run once from the repository root; refuses if already applied.
--recount <file> adds the recount paragraph."""
import re, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "(12m-R626), by script" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R626" not in s and "INT-m12-4" not in s, "already applied"

RULING = '''R626(a) — Finding from 12m-U12-C1: tests/test_ollama_preflight.py::
  TestRequirePreflight::test_raises_on_failure patches ollama_chat and
  check_ollama_env but not ollama.ps, so preflight_check sends GET /api/ps to the live
  server (ollama_preflight.py:124) on every standard gate and nightly. New row
  INT-m12-4, Class 3, P1, closing in U12 commit 1. Evidence: the C1 stop report and
  its journal extracts in ~/scratch/12m-U12-C1/, copied beside the unit report.
R626(b) — Commit 1's Touches widen to that one test. It patches ollama.ps as its
  sibling tests do. The fixed test lands in commit 1.
R626(c) — The fence also covers port 11435 on the loopback addresses, because
  analysis/eval/run_qualgap01.py:94 targets it by default. The hard-coded 11434
  literals that ignore OLLAMA_HOST (extractor.py:753, ollama_client.py:353 and :388,
  run_screen2f.py:60) are A-4b's family. No new row; a dated note on A-4b lists them.
R626(d) — Dated corrections, append-only:
  (1) on R625: its MEASURED line "The standard gate run in session 12m made no Ollama
      request (journal)" is false. The 12m-OPEN gate window holds GET /api/ps at
      18:09:51 UTC from the test in (a). The same request appears in the 12l U10 and
      U11 gate windows (04:14:58, 04:49:45 UTC) and the 2026-10-12 nightly (09:09:33
      UTC). No model call is affected. R625(a)(1) stands: its basis is "no model
      call", which holds, and from commit 1 the fence enforces it.
  (2) an addendum file beside the read-out correcting §6's sentence in the same terms.
      The read-out itself is not edited.
R626(e) — The 18:02:05 UTC group (GET /api/version, /api/tags, /api/ps) in the
  unfenced 12m-OPEN gate window is not traced. The fenced gate's journal window is the
  measurement of record.'''
flat = " ".join(RULING.split())
assert " | " not in flat and s.endswith("during session 12m |\n")
def clause(l): return re.search(rf"R626\({l}\) — (.*?)(?= R626\([a-e]\) — |$)", flat).group(1).strip()
d1 = re.search(r"\(1\) (on R625: .*?) \(2\) ", flat).group(1)
EV = "`docs/session-reports/session-12/12m_R626/`"
s = s.rstrip("\n") + f'''

*Note 2026-10-13 (12m-U12-C1b item 0, R626(d)(1)), on R625 in Block 23 above — a correction, appended; the ruling's text is not edited:* R626(d)(1): \"{d1}\" Evidence: {EV} (`journal_12m_open_gate_window.txt`, `api_ps_20261012.txt`, `fenced_gate_failure_excerpt.txt`). The same sentence in the read-out (`12m_U12A.md` §6) is corrected by `docs/session-reports/session-12/12m_U12/12m_U12A_addendum_20261013.md` (R626(d)(2)).

**Block 24.** *Transcribed 2026-10-13 (item 0 of the 12m-U12-C1b brief), in its own commit with the row, the dated notes and the addendum it orders, in the R597(e) form:* ruling R626, made by PI on architect recommendation on 2026-10-13 UTC, during session 12m, on the 12m-U12-C1 stop report (unit U12, commit 1, stopped at its gate; no commit). The ruling as issued, with its line breaks:

```text
RULING R626 (PI, 2026-10-13 UTC, session 12m, on architect recommendation):

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-13 | {flat} | PI, 2026-10-13, during session 12m |
'''
a = clause("a")
finding = a[len("Finding from 12m-U12-C1: "):a.index(" New row INT-m12-4")]
rows = [("INT-m12-4", finding, "3", "P1, closing in unit U12 (commit 1)", "R626(a)",
         f"the 12m-U12-C1 stop report; {EV}: `fenced_gate_failure_excerpt.txt` (the fenced gate, 2026-10-13 03:15:21–03:29:40 UTC: the test refused with `GET http://127.0.0.1:11434/api/ps`), `api_ps_20261012.txt` and `journal_12m_open_gate_window.txt` (the request in earlier gate and nightly windows). R626(a): \"{a}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12m additions, R626).** *Opened 2026-10-13 (item 0 of the 12m-U12-C1b brief) under R509 and R626.* One row from the 12m-U12-C1 stop report: `INT-m12-4` (R626(a)). The text cell is R626(a)'s statement of the finding, quoted unchanged; class and package are R626's. Not rows, as ruled: the hard-coded 11434 addresses are a dated note of this date on A-4b (R626(c)); R626(d) is a dated correction under R625 in the decision log and an addendum beside the read-out; R626(e) opens nothing.

| # | On disk (R626(a), quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-m12-3 | P7 — R625(c) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-m12-4 | P1, closing in U12 — R626(a) |\n")
lines = s.split("\n")
i = [k for k, l in enumerate(lines) if l.startswith("| A-4b |") and l.count(" | ") > 3]; assert len(i) == 1 and lines[i[0]].endswith(" |")
lines[i[0]] = lines[i[0]][:-2] + f" *Note 2026-10-13 (12m-U12-C1b item 0, R626(c)):* this row's family, as the 12m-U12-C1 census found it — `httpx.get` sites that name the default address outright and do not follow `OLLAMA_HOST`: `engine/agents/extractor.py:753` (`http://127.0.0.1:11434/api/tags`), `engine/utils/ollama_client.py:353` and `:388` (`OLLAMA_BASE_URL = \"http://localhost:11434\"`, the digest read), `analysis/eval/run_screen2f.py:60` (`OLLAMA = \"http://localhost:11434\"`). `analysis/eval/run_qualgap01.py:94` targets `:11435` by default. No new row. R626(c): \"{clause('c')}\" |"
open(P, "w", encoding="utf-8").write("\n".join(lines)); print("applied")
