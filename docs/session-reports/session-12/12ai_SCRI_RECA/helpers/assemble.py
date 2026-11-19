"""12ai-SCRI-RECA step 3: join the census into OUT/12ai_SCRI_RECA.md, headed by every helper's path and sha256 and the run's
inputs. Writes only OUT/12ai_SCRI_RECA.md. usage: python3 -I -B assemble.py <OUT> <SRC>"""
import hashlib, subprocess, sys
from pathlib import Path
OUT, SRC = Path(sys.argv[1]), Path(sys.argv[2])
h = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
lines = ["# 12ai-SCRI-RECA — publication census of ~/scratch/12ai-scri/ (read-only; to scratch, not committed)", "",
         f"Written {subprocess.run(['date', '-u', '+%FT%TZ'], capture_output=True, text=True).stdout.strip()}. SRC `{SRC}`, its list "
         f"`SHA256SUMS` {h(SRC / 'SHA256SUMS')}. Every count below is a helper's; a classification by reading is marked CC's reading.", "",
         "Helpers (each run with `python3 -I -B`):", ""]
for p in sorted((OUT / "helpers").glob("*.py")): lines.append(f"- `{p}` sha256 `{h(p)}`")
lines += [f"- `{Path.home()}/projects/evidence-engine/docs/session-reports/session-12/12ah_OLRA/rec_scan.py` sha256 "
          f"`{h(Path.home() / 'projects/evidence-engine/docs/session-reports/session-12/12ah_OLRA/rec_scan.py')}` (the scan, 2b; unedited)",
          f"- `{Path.home()}/scratch/12ai-open/helpers/scri_check.py` sha256 `{h(Path.home() / 'scratch/12ai-open/helpers/scri_check.py')}` (2a)", "",
          "Tables beside this file: " + ", ".join(f"`{p.name}`" for p in sorted(OUT.glob("3*.tsv"))) + ".", "",
          "## Step 2 — scan and control", "", "```"] + (OUT / "2c_control.txt").read_text().splitlines()[-2:] + \
         [l for l in (OUT / "reca_credential_scan.txt").read_text().splitlines() if l.startswith("RESULT")] + ["```", ""]
lines += (OUT / "census_sections.md").read_text().splitlines()
(OUT / "12ai_SCRI_RECA.md").write_text("\n".join(lines) + "\n"); print(f"12ai_SCRI_RECA.md: {len(lines)} lines")
