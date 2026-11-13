# WR5-B — prototype brief checker, measured on past briefs

Task `evidence-engine WR5-B`, resumed at step 2 under the WR5-B ruling. Brief of record:
`docs/session-reports/WR5-B/brief_WR5-B.md` at `957f917`, with the deltas in `brief_WR5-B_R1.md`. Written 2026-11-13 by
Claude Code, session `c4f7c3c7-8e58-4a82-9b30-25d20e8fb125`. Read-only on everything inspected. No model load, no
`review.db` access, and nothing installed. **No hit is classified true or false here (G4).** The hits are printed for the
architect to read.

## 1. Item 0

| file | sha256 | bytes | lines | commit |
|---|---|---|---|---|
| `docs/session-reports/brief_WR5-A.md` | `dee1182c33918ac78db9da6e84cb53beca3fe69370345f0cab278fce6199e5e1` | 6113 | 110 | `22f8a638c5ee814edb8521808a2a0219f2b8f0ce` |
| `docs/session-reports/WR5-B/brief_WR5-B.md` | `9fa7df245e2e660d9d058c408ae64f6ea47be0553c60e5f951b837beedbd41cc` | 8973 | 162 | `957f917a47c5720977bae5b48e762b46b31d8391` |
| `docs/session-reports/WR5-B/brief_WR5-B_R1.md` | `99c50f3a9ebe22b303445c3c7c33c22bae37c5f5fb4e7f1103211c0ce44dab14` | 4632 | 88 | `886c6f9e8fa80a69894f77c858ce216585d9814f` |

The `_R1` name follows the existing file `docs/session-reports/session-12/12ae_U24B/brief_12ae_U24B_R1.md`, found by
`git ls-files | grep '_R1\.md$'`. Each file was written once by `~/scratch/wr5-b/write_brief.py`, which opens with
`open(out, "x")` and prints the hash straight after the write. The source is this session's transcript, JSONL lines 7,
447 and 518. The form is the text between the paste tags plus a final newline, the same form as `brief_12af_CLOSE.md`
against its archived paste.

## 2. B1 and the recounts

B1, as restated by ruling clause (a), holds. The five TASK titles are as quoted in the ruling: 766 `TASK [evidence-engine
12y-U18B]`, 862 `… 12y-U18B, resumed under R722]`, 909 `… 12y-U18B, resumed under R722 and R723]`, 1039 `… 12y-U18B,
resumed at step 2 under R724]`, 1517 `… 12y-U18C]`. M2 and M3 are accepted as reported (ruling (e)).

Recounts under the ruling's set rules, all from `brief_check.py measure` (§6):
- **S2: 19 files.** These are the committed `brief_*.md` files whose name does not begin `brief_WR5`.
- **S4: 3 files.**
- **S3: 373 turns.** 376 turns match M3's predicate, and 3 of them have a TASK title naming WR5 and are excluded (all three
  are in this session). The 376 is one more than M3's 375 because this session's `brief_WR5-B_R1` paste is now a turn.
- **S1: 5 turns.**

## 3. Checks

Every check's comment in the script quotes its clause from `STANDING_RULES.md`, read before the check was written. All 17
clause ids are in the file (an `awk` over `## <id>` headings found each one). Where a clause left a choice open, the
narrower reading was taken; it is stated in the comment and summarised here.

| check | clause | status | narrower reading taken |
|---|---|---|---|
| K1 | R730(c) | built | "full hash" = lowercase hex token of 33–39 or 41–47 chars with a digit and a letter; abbreviations and 64-char sha256 values are not full commit hashes |
| K2 | R783(d) | built | "the repository it belongs to" = the given repo **or** `~/claude-config` (`--also-repo`); hit only if neither holds the commit |
| K3 | R720(c) | built | a logical line with `git commit` and `&&`, plus a single `\|` or `tee`; markdown table rows excluded |
| K4 | R656(d) | built | interpreter word before a repo script path or `-m engine\|analysis\|scripts`; `-m pytest` not a repository script |
| K5 | R658(c) | built | rm/rmdir/unlink/shred (git rm included) or find -delete with a path-like argument not under `~/scratch/`; `$VAR` paths not resolvable, not flagged |
| K6 | R697(b) | built | pkill/killall with an argument, `kill $(pgrep …)`, `pgrep … \| xargs kill` |
| K7 | R676(d)(iii) | built | a pytest command with no `--basetemp`; gate wrappers and the bare standard gate are not ad hoc |
| K8 | R705(c), R725(c) | built | a `Touched:`/`Touches:` label at line start; the list is cut at Reads/STOP/DO NOT TOUCH; `engine/` named, `docs/inventory/` not |
| K9 | R667(e) | built | a free-standing numeral in an INFERRED section body; identifiers, item labels and numbers after a reference word (step, item, section, block, rule, phase, part, row, §) excluded |
| K10 | R723(a), R750(c), R772(c) | built | "exactly N" (numeral or number word ≤ twelve), or a stated "−" count |
| K11 | R733(a), R756(b) | built | READ/MEASURED items; the cited file = first file-like token in the item, else in the section header, resolved to a unique tree path; quotes of ≥ 3 words, split at "…", each piece whitespace-normalised |
| K12 | R731(b) | built | tokens under engine/ scripts/ tests/ docs/ analysis/ review_specs/; absent at the commit and not declared new (the token or a parent dir on a line holding "new") |
| K13 | R784(g)(i) | built | ≥ 12 consecutive words (lowercased, edge punctuation stripped) equal to 12 consecutive words of one fenced clause of `STANDING_RULES.md` at the commit |
| K14 | none (architect's addition) | built | `R<n>` or `R<n>(<letter>)` absent as a token from the plan at the commit |
| region rule | ruling (b) | built | split at the first line beginning `TASK [` after whitespace |

**NOT BUILT: none. UNPROVEN: none.** Every check has a passing control pair (§3a).

### 3a. Controls (G3)

Run with
`python3 -I ~/scratch/wr5-b/brief_check.py controls ~/scratch/wr5-b/controls --repo . --commit 886c6f9e8fa80a69894f77c858ce216585d9814f --also-repo ~/claude-config`,
rc 0. The output was saved as `~/scratch/wr5-b/controls_output.txt`, sha256 `8dba756c…38ce`:

```text
PASS  K10_hit.txt: K10 UNSPLIT:1 exactly 3
PASS  K10_miss.txt: K10 no hit
PASS  K11_hit.txt: K11 UNSPLIT:2 docs/session-reports/WR5-A_readout.md: "this sentence is not in the read-out anywhere"
PASS  K11_miss.txt: K11 no hit
PASS  K12_hit.txt: K12 UNSPLIT:1 docs/plan/NO_SUCH_FILE.md
PASS  K12_miss.txt: K12 no hit
PASS  K13_hit.txt: K13 UNSPLIT:1 R730(c): every full commit hash a brief or a ruling carries is 40 hexadecimal characters, 
PASS  K13_miss.txt: K13 no hit
PASS  K14_hit.txt: K14 UNSPLIT:1 R9999(a)
PASS  K14_miss.txt: K14 no hit
PASS  K1_hit.txt: K1 UNSPLIT:1 886c6f9e8fa80a69894f77c858ce216585d9814
PASS  K1_miss.txt: K1 no hit
PASS  K2_hit.txt: K2 UNSPLIT:1 0123456789abcdef0123456789abcdef01234567
PASS  K2_miss.txt: K2 no hit
PASS  K3_hit.txt: K3 UNSPLIT:1 2c. .venv/bin/python check.py | tee ev.txt && git commit -F msg.txt
PASS  K3_miss.txt: K3 no hit
PASS  K4_hit.txt: K4 UNSPLIT:1 python3 scripts/run_pipeline.py
PASS  K4_miss.txt: K4 no hit
PASS  K5_hit.txt: K5 UNSPLIT:1 rm -rf ~/projects/evidence-engine/data/tmp
PASS  K5_miss.txt: K5 no hit
PASS  K6_hit.txt: K6 UNSPLIT:1 pkill -f
PASS  K6_miss.txt: K6 no hit
PASS  K7_hit.txt: K7 UNSPLIT:1 .venv/bin/python -m pytest tests/test_x.py -q
PASS  K7_miss.txt: K7 no hit
PASS  K8_hit.txt: K8 UNSPLIT:1 engine/core/x.py; tests/test_x.py
PASS  K8_miss.txt: K8 no hit
PASS  K9_hit.txt: K9 UNSPLIT:2 E1. The suite collects 3591 tests.
PASS  K9_miss.txt: K9 no hit
PASS  REGION_hit.txt: split at line 2 (expected split)
PASS  REGION_miss.txt: unsplit (expected unsplit)
```

`check` mode's exit status: `brief_check.py check controls/K1_hit.txt` gave rc 1 (one K1 hit printed), and
`brief_check.py check controls/K10_miss.txt` gave rc 0.

The control fragments (`⏎` marks a newline):

- `K1_hit.txt`: `HEAD is 886c6f9e8fa80a69894f77c858ce216585d9814 as quoted.`
- `K1_miss.txt`: `HEAD is 886c6f9e8fa80a69894f77c858ce216585d9814f, short 886c6f9.`
- `K2_hit.txt`: `Parent 0123456789abcdef0123456789abcdef01234567.`
- `K2_miss.txt`: `Parent 886c6f9e8fa80a69894f77c858ce216585d9814f.`
- `K3_hit.txt`: `  2c. .venv/bin/python check.py | tee ev.txt && git commit -F msg.txt`
- `K3_miss.txt`: `  2c. .venv/bin/python check.py > ev.txt && git commit -F msg.txt`
- `K4_hit.txt`: `  run python3 scripts/run_pipeline.py --review x`
- `K4_miss.txt`: `  run .venv/bin/python scripts/run_pipeline.py --review x`
- `K5_hit.txt`: `  rm -rf ~/projects/evidence-engine/data/tmp`
- `K5_miss.txt`: `  rm -rf ~/scratch/wr5-b/tmp`
- `K6_hit.txt`: `  pkill -f run_gate`
- `K6_miss.txt`: `  kill 12345`
- `K7_hit.txt`: `  .venv/bin/python -m pytest tests/test_x.py -q`
- `K7_miss.txt`: `  .venv/bin/python -m pytest tests/test_x.py -q --basetemp ~/scratch/wr5-b/pt`
- `K8_hit.txt`: `  Touched: engine/core/x.py; tests/test_x.py⏎  STOP / DO NOT TOUCH: scripts/`
- `K8_miss.txt`: `  Touched: engine/core/x.py; docs/inventory/entry_points.json⏎  STOP / DO NOT TOUCH: scripts/`
- `K9_hit.txt`: `ASSUMPTION LEDGER (load-bearing only):⏎  INFERRED — expectation: E1. The suite collects 3591 tests.⏎CONTEXT: x`
- `K9_miss.txt`: `ASSUMPTION LEDGER (load-bearing only):⏎  INFERRED — expectation: E1. The suite collects every test at step 2; R722(a) holds.⏎CONTEXT: x`
- `K10_hit.txt`: `The diff carries exactly 3 lines.`
- `K10_miss.txt`: `The diff carries exactly the noted rows.`
- `K11_hit.txt`: `ASSUMPTION LEDGER (load-bearing only):⏎  READ (docs/session-reports/WR5-A_readout.md): R1. "this sentence is not in the read-out anywhere"⏎CONTEXT: x`
- `K11_miss.txt`: `ASSUMPTION LEDGER (load-bearing only):⏎  READ (docs/session-reports/WR5-A_readout.md): R1. "Every count below is⏎    followed by the command that produced it."⏎CONTEXT: x`
- `K12_hit.txt`: `  Reads: docs/plan/NO_SUCH_FILE.md`
- `K12_miss.txt`: `  Reads: docs/plan/STANDING_RULES.md⏎  Touches: docs/session-reports/WR5-C/ (new)⏎  Writes docs/session-reports/WR5-C/x.md`
- `K13_hit.txt`: `Note: every full commit hash a brief or a ruling carries is 40 hexadecimal characters, checked by the architect.`
- `K13_miss.txt`: `Note: every full commit hash a brief or a ruling carries is long, so CC checks it.`
- `K14_hit.txt`: `Under R9999(a), this holds.`
- `K14_miss.txt`: `Under R722(a), this holds.`
- `REGION_hit.txt`: `Before.⏎TASK [evidence-engine X] — a task⏎Body.`
- `REGION_miss.txt`: `Before.⏎See the TASK [evidence-engine X] title, quoted mid-line.`

## 4. Per-set tables

Every count below is from `summary.json` (§6), rendered by `render.py`. "Briefs hit" counts are split into PRE only, BRIEF only and both, which sum to "any" for the split texts. "Hits" counts PRE plus BRIEF, which sum to the total. Unsplit texts are counted apart (G6). S1 and S3 run the text-only checks (K1, K3–K10); S2 and S4 run all fourteen.

#### S1: 5 texts, 5 split, 0 unsplit, 0 with more than one TASK line

| check | briefs hit: PRE only | BRIEF only | both | any (of 5 split) | hits PRE | hits BRIEF | hits total | unsplit briefs hit (of 0) | unsplit hits |
|---|---|---|---|---|---|---|---|---|---|
| K1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K8 | 0 | 1 | 0 | 1 | 0 | 1 | 1 | 0 | 0 |
| K9 | 1 | 0 | 2 | 3 | 60 | 2 | 62 | 0 | 0 |
| K10 | 1 | 1 | 2 | 4 | 10 | 8 | 18 | 0 | 0 |

#### S2: 19 texts, 18 split, 1 unsplit, 0 with more than one TASK line

| check | briefs hit: PRE only | BRIEF only | both | any (of 18 split) | hits PRE | hits BRIEF | hits total | unsplit briefs hit (of 1) | unsplit hits |
|---|---|---|---|---|---|---|---|---|---|
| K1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K4 | 1 | 0 | 0 | 1 | 2 | 0 | 2 | 0 | 0 |
| K5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K7 | 1 | 0 | 0 | 1 | 1 | 0 | 1 | 0 | 0 |
| K8 | 0 | 1 | 0 | 1 | 0 | 1 | 1 | 0 | 0 |
| K9 | 2 | 0 | 16 | 18 | 295 | 48 | 343 | 1 | 4 |
| K10 | 10 | 1 | 6 | 17 | 28 | 13 | 41 | 1 | 2 |
| K11 | 3 | 0 | 15 | 18 | 299 | 41 | 340 | 1 | 16 |
| K12 | 0 | 1 | 1 | 2 | 1 | 3 | 4 | 0 | 0 |
| K13 | 1 | 0 | 0 | 1 | 1 | 0 | 1 | 0 | 0 |
| K14 | 0 | 2 | 0 | 2 | 0 | 4 | 4 | 1 | 9 |

#### S4: 3 texts, 3 split, 0 unsplit, 0 with more than one TASK line

| check | briefs hit: PRE only | BRIEF only | both | any (of 3 split) | hits PRE | hits BRIEF | hits total | unsplit briefs hit (of 0) | unsplit hits |
|---|---|---|---|---|---|---|---|---|---|
| K1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K8 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K9 | 0 | 1 | 0 | 1 | 0 | 1 | 1 | 0 | 0 |
| K10 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K11 | 0 | 1 | 0 | 1 | 0 | 1 | 1 | 0 | 0 |
| K12 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K13 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| K14 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

#### S3: 373 turns (3 WR5 turns excluded), 369 split, 4 unsplit, 0 with more than one TASK line, 66 taken as the whole turn (no paste block)

| check | briefs hit: PRE only | BRIEF only | both | any (of 369 split) | hits PRE | hits BRIEF | hits total | unsplit briefs hit (of 4) | unsplit hits |
|---|---|---|---|---|---|---|---|---|---|
| K1 | 2 | 6 | 2 | 10 | 6 | 12 | 18 | 0 | 0 |
| K3 | 1 | 0 | 0 | 1 | 1 | 0 | 1 | 0 | 0 |
| K4 | 1 | 27 | 1 | 29 | 3 | 38 | 41 | 1 | 1 |
| K5 | 1 | 0 | 0 | 1 | 1 | 0 | 1 | 0 | 0 |
| K6 | 0 | 4 | 1 | 5 | 1 | 5 | 6 | 0 | 0 |
| K7 | 1 | 12 | 0 | 13 | 1 | 13 | 14 | 0 | 0 |
| K8 | 0 | 76 | 0 | 76 | 0 | 76 | 76 | 0 | 0 |
| K9 | 31 | 175 | 73 | 279 | 1206 | 934 | 2140 | 1 | 2 |
| K10 | 23 | 93 | 49 | 165 | 141 | 297 | 438 | 1 | 1 |

S3 by month — each cell: briefs PRE-only/BRIEF-only/both=any; hits PRE+BRIEF=total; u = unsplit briefs/hits where any

| check | 2026-09 (n=163, split 162) | 2026-10 (n=186, split 183) | 2026-11 (n=24, split 24) |
|---|---|---|---|
| K1 | 0/1/0=1; 0+1=1 | 2/5/2=9; 6+11=17 | 0/0/0=0; 0+0=0 |
| K3 | 0/0/0=0; 0+0=0 | 1/0/0=1; 1+0=1 | 0/0/0=0; 0+0=0 |
| K4 | 0/18/0=18; 0+22=22 | 0/9/1=10; 1+16=17; u 1/1 | 1/0/0=1; 2+0=2 |
| K5 | 0/0/0=0; 0+0=0 | 1/0/0=1; 1+0=1 | 0/0/0=0; 0+0=0 |
| K6 | 0/0/0=0; 0+0=0 | 0/4/1=5; 1+5=6 | 0/0/0=0; 0+0=0 |
| K7 | 0/6/0=6; 0+7=7 | 0/6/0=6; 0+6=6 | 1/0/0=1; 1+0=1 |
| K8 | 0/52/0=52; 0+52=52 | 0/23/0=23; 0+23=23 | 0/1/0=1; 0+1=1 |
| K9 | 0/105/0=105; 0+367=367 | 28/70/52=150; 814+497=1311; u 1/2 | 3/0/21=24; 392+70=462 |
| K10 | 0/43/0=43; 0+64=64 | 12/49/38=99; 104+206=310; u 1/1 | 11/1/11=23; 37+27=64 |

S3 by the next ruling's title word (238 unmatched: 111 with no later ruling in the same transcript, 127 whose next ruling id has no title in the plan) — cell form as above

| check | accepted (n=74, split 74) | neither (n=24, split 24) | stop (n=26, split 26) | stop+accepted (n=11, split 11) | unmatched (n=238, split 234) |
|---|---|---|---|---|---|
| K1 | 1/2/0=3; 1+2=3 | 0/0/0=0; 0+0=0 | 1/3/1=5; 3+6=9 | 0/0/0=0; 0+0=0 | 0/1/1=2; 2+4=6 |
| K3 | 1/0/0=1; 1+0=1 | 0/0/0=0; 0+0=0 | 0/0/0=0; 0+0=0 | 0/0/0=0; 0+0=0 | 0/0/0=0; 0+0=0 |
| K4 | 0/1/0=1; 0+1=1 | 0/3/0=3; 0+7=7 | 1/1/1=3; 3+2=5 | 0/0/0=0; 0+0=0 | 0/22/0=22; 0+28=28; u 1/1 |
| K5 | 1/0/0=1; 1+0=1 | 0/0/0=0; 0+0=0 | 0/0/0=0; 0+0=0 | 0/0/0=0; 0+0=0 | 0/0/0=0; 0+0=0 |
| K6 | 0/3/1=4; 1+4=5 | 0/0/0=0; 0+0=0 | 0/1/0=1; 0+1=1 | 0/0/0=0; 0+0=0 | 0/0/0=0; 0+0=0 |
| K7 | 1/1/0=2; 1+1=2 | 0/4/0=4; 0+4=4 | 0/0/0=0; 0+0=0 | 0/0/0=0; 0+0=0 | 0/7/0=7; 0+8=8 |
| K8 | 0/1/0=1; 0+1=1 | 0/1/0=1; 0+1=1 | 0/3/0=3; 0+3=3 | 0/0/0=0; 0+0=0 | 0/71/0=71; 0+71=71 |
| K9 | 11/12/48=71; 699+203=902 | 4/13/4=21; 74+47=121 | 8/0/10=18; 186+20=206 | 2/0/8=10; 126+35=161 | 6/150/3=159; 121+629=750; u 1/2 |
| K10 | 14/18/28=60; 85+128=213 | 2/3/4=9; 12+12=24 | 6/5/8=19; 28+31=59 | 1/1/6=8; 10+15=25 | 0/66/3=69; 6+111=117; u 1/1 |


## 5. S1, per stop brief

From `summary.json` → `S1.per_brief`. All five turns were split, none had more than one TASK line, and each text was taken
from its paste block.

| turn (JSONL line, `dc9f61b7…jsonl`) | stop it ran into | checks hit in region BRIEF | checks hit in region PRE |
|---|---|---|---|
| 766 `12y-U18B` | R722(a) | K8, K9, K10 | K9, K10 |
| 862 `12y-U18B, resumed under R722` | R723(a) | K10 | K9, K10 |
| 909 `12y-U18B, resumed under R722 and R723` | R724(a) | K10 | none |
| 1039 `12y-U18B, resumed at step 2 under R724` | R725(a) | none | none |
| 1517 `12y-U18C` | R727(a) | K9 | K9, K10 |

The BRIEF-region hits on S1, verbatim from the hits file (check, line, matched text):

```text
766  K8  123  docs/plan/ENGINE_REFACTOR_PLAN.md (append only: Block 119; dated notes on A-4b and HK-ARCH); docs/session-repo…  [200-char cut in the file]
766  K9  203  to 023 (R721(b)), and report.
766  K10 187  exactly 3
766  K10 263  three "−" lines
766  K10 351  exactly 3
862  K10 203  exactly 3
862  K10 249  "−" count is 3
909  K10 25   exactly 3
909  K10 25   one "−" line
909  K10 29   exactly one
1517 K9  334  I2. The run context can carry digest_fn: open_run receives it (the 26 injecting
```

## Measured properties of the hits (not classifications)

`python3 ~/scratch/wr5-b/props.py ~/scratch/wr5-b/hits_S1_S2_S4.jsonl` (sha256 `e92b1842…b404`):

```text
S1 PRE: K9 hits 60, on a column-0 line 12; K11 hits 0, quote with leading/trailing space 0
S1 BRIEF: K9 hits 2, on a column-0 line 0; K11 hits 0, quote with leading/trailing space 0
S2 PRE: K9 hits 295, on a column-0 line 37; K11 hits 299, quote with leading/trailing space 49
S2 BRIEF: K9 hits 48, on a column-0 line 0; K11 hits 41, quote with leading/trailing space 1
S2 UNSPLIT: K9 hits 4, on a column-0 line 1; K11 hits 16, quote with leading/trailing space 5
S4 BRIEF: K9 hits 1, on a column-0 line 0; K11 hits 1, quote with leading/trailing space 0
```

- **K9's ledger reach.** The ledger runs to the next column-0 label, and a ruling's clause text (`R795(b) — …`, `` ```text
  ``) is not a label. So in region PRE, an INFERRED section of `ASSUMPTIONS IN THIS RULING` takes in the ruling clauses
  that follow it. The 12 and 37 column-0 PRE hits above are such lines.
- **K11's quote pairing.** Quotes pair on straight `"`, so a quote holding inner `"` marks splits into fragments. The 49 PRE
  quotes with an edge space are that shape.
- **K11's file resolution.** The cited file is the first file-like token in the item, which may not be the file the quote
  came from.
- **K11 coverage.** In S2, `K11_checked_quotes` 420 and `K11_unchecked_quotes` 83 (no resolvable file). In S4, 3 and 7.
- These are properties of the stated readings. **Nothing was changed after seeing them** (out of scope: "Tuning a check to
  reduce hits after seeing them: report first").

## Expectations

- **X2 (few of the five stop briefs hit): mismatch.** 4 of 5 are hit in region BRIEF, and 4 of 5 in any region (§5). 1039
  has no hit in either region.
- **X3 (PRE draws most of the K10 and K13 hits): mixed.**
  - K10 PRE/BRIEF hits: S1 10/8, S2 28/13, S3 141/297, S4 0/0. PRE holds the majority in S1 and S2, but not in S3.
  - K13: S2 1/0, S4 0/0. K13 is not run on S1 or S3, which are text-only.

## 6. Paths

Evidence directory `docs/session-reports/WR5-B/`:
- `brief_WR5-B.md`, `brief_WR5-B_R1.md`: item 0.
- `brief_check.py`: the prototype copy. sha256 `750ee3d2eb33a65781bcbef0faaf75d29fadd8b3da6530c3bece4071683f9e83`, equal
  to `~/scratch/wr5-b/brief_check.py`, which produced every number here (G5).
- `hits_S1_S2_S4.jsonl`: every hit on S1, S2 and S4, one JSON object per line (`set`, `brief`, `check`, `region`, `line`,
  `text`), the text verbatim up to 200 characters. sha256 `daba5f3c13667303e54f64a4d5a7bde5765ecc97137f34d7bb8953b8cd7c0cb8`.
  Lines: S1 81, S2 768, S4 2, by `collections.Counter` over the `set` field (851 total, `wc -l`). S3 is counts only.
- `WR5-B_readout.md`: this file. Its commit is the one that adds it; the hash is given in the final report.

The measurement call was
`python3 -I ~/scratch/wr5-b/brief_check.py measure --out ~/scratch/wr5-b/hits_S1_S2_S4.jsonl --summary ~/scratch/wr5-b/summary.json --repo . --also-repo ~/claude-config`,
run from the repository root at HEAD `886c6f9`. It took 6.1 s and returned rc 0. `summary.json` stays in scratch, sha256
`9dfd05c5…8e5b`. The tables in §4 were rendered from it by `~/scratch/wr5-b/render.py` (sha256 `dcfddf8a…6689`), which
asserts G6's sums for every row it prints.

## 7. Against the brief

1. **One committed lane brief has no `TASK [` line:** `brief_12ae_U24B_R1.md`. It is "unsplit" in S2, and its hits are in
   the unsplit columns.
2. **S3's ruling match is limited by the record.** 238 of 373 turns could not be matched. For 111, no later turn in the
   same transcript carries a new ruling id: a brief's ruling can arrive in the next session's transcript, and the match
   reads one transcript only. For 127, the next ruling's id has no `RULING [evidence-engine <id>] —` heading in the plan;
   earlier sessions' rulings are not in that form. Matching is by ruling id, from the next turn's paste.
3. **66 S3 turns carry no paste block**, so their whole turn text was checked.
4. **X2 does not hold.** See Expectations.

STOP-WR5-B-R1
