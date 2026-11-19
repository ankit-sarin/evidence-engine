# 12ah-OLRA — addendum 1 (Q11–Q13), under ruling 12ah-OLRA-R2

Run 2026-11-16 from 00:21:13 UTC, DGX `spark-59e4`. Read-only. No model load, no server request,
no lock acquisition, no pytest run, and nothing executed from dgx-infra or claude-config.
evidence-engine at `35a914b2bdd75147af104e5abfaea388eae3a057` throughout. Lines are quoted as
`path:line` at the HEAD of the repository named; "CC's inference" marks a reading, not a command.
Raw outputs beside this file: `q11_grep_raw.txt`, `q11_callers.txt`.

`brief_12ah_OLRA_R2.md` was written once and hashed straight after the write:
`0fa58829b5ee68dabb75c324a46c1f06b9e9999f7db27f26bea0c2c4bb4a8a66`.

## Y1–Y4

| | verdict | evidence |
|---|---|---|
| Y1 | holds | `systemctl show ollama --property=Environment` carries `OLLAMA_KEEP_ALIVE=30m` (read 00:07, R1 report §G4). A request that sends its own keep_alive is unaffected, because the engine sends `-1` on every resolver call (§Q11). That it overrides the host is Ollama's per-request precedence, which OLR-1 §E4 also states (CC's inference; not measured here) |
| Y2 | holds | `~/projects/dgx-infra/scripts/ollama_lane_models.txt:10-15`, quoted in §Q12 |
| Y3 | holds | `review_specs/surgical_autonomy.yaml:58` `  extractor: "deepseek-r1:32b"` and `:75` `    model: "deepseek-r1:32b"`; declared default `engine/core/review_spec.py:127` `extractor: str = Field(default="deepseek-r1:32b", …` |
| Y4 | holds, for the full sweep only | `~/projects/dgx-infra/scripts/ollama_health_check.sh:45-46` (Sunday UTC → `MODE=full`), `:146` `LOCKFILE="${OLLAMA_EXPERIMENT_LOCK:-$HOME/.ollama_experiment.lock}"`, `:154-157` `flock -n 9` → `HELD`, `:214-218` `SKIPPED — experiment lock held ($LOCKFILE)`. **Light mode does not consult the lock**: `:147-148` `if [[ "$MODE" == light ]]; then LOCK_STATE="NOT_CONSULTED"` (it sends GETs only, `:15`) |

---

## Q11. Keep-alive

**Which requests can load or hold a model.** Pattern over engine, scripts and analysis:
`_client\.(chat|generate|embed|embeddings|show|ps|pull|create)|ollama\.(chat|generate|embed|embeddings|Client|AsyncClient)\b|/api/(chat|generate|embed|embeddings|show|ps|tags|pull)|keep_alive`
→ 54 lines (`q11_grep_raw.txt`). The only load-capable send is `_client.chat`, at two places, both
inside `ollama_chat`:
`engine/utils/ollama_client.py:784` `_client.chat,` (every attempt) and `:955` `_client.chat,` (the
post-restart final attempt). Neither adds a keep_alive of its own. Both forward the caller's
`**kwargs` (`:783-788`, `:954-959`). The other requests are reads that load nothing:
`/api/tags` (`ollama_client.py:517`, `extractor.py:800`; `effective_config.py:24` "`/api/tags`,
which loads no model"), `/api/ps` (`ollama_client.py:571`) and `/api/show` (`ollama_client.py:289`).
That `/api/show` loads nothing is CC's inference from Ollama's API. A second pattern for direct
HTTP, `requests\.(post|get)|httpx\.(post|Client|AsyncClient|request)|11434|11435|urlopen`, finds
only literature and PDF fetches (Unpaywall, OpenAlex, publishers) and `analysis/eval/qualgap01.py:74`
`DEFAULT_HOST = "http://127.0.0.1:11435"`. **No other site sends Ollama a load-capable request.**

**Where the value comes from.** `engine/core/effective_config.py:311-312`:
`sent = {"model", "messages", "options", "keep_alive"}` / `keep_alive, sources["keep_alive"] =
_keep_alive(spec)`. That runs for **every** Ollama stage (no branch removes `keep_alive` from `sent`;
the pattern `sent *-=|discard\(|sent\.remove` finds none). `:266-268` reads it from the spec's
`ollama` block, and `:137-138` puts it in `kwargs()`. The live spec has no `ollama:` block
(`review_specs/surgical_autonomy.yaml`, pattern `keep_alive|ollama:`: no such site found), so the
value is the declared default **−1** with source `declared_default`. The manifest records it per
stage (`run_manifest.py:809` `"keep_alive": str(cfg.keep_alive) if "keep_alive" in cfg.sent_keys
else "unset",`).

**The quoted default and its comment:**
```
engine/core/review_spec.py:672: class OllamaRuntime(_SpecModel):
engine/core/review_spec.py:673:     """Request-level settings sent on every Ollama call."""
engine/core/review_spec.py:675:     keep_alive: Union[StrictInt, str] = Field(
engine/core/review_spec.py:676:         default=-1,
engine/core/review_spec.py:677:         description=(
engine/core/review_spec.py:678:             "Sent per request so the manifest's value is the one in force (the "
engine/core/review_spec.py:679:             "service's OLLAMA_KEEP_ALIVE lives in a systemd drop-in the manifest "
engine/core/review_spec.py:680:             "cannot see). -1 equals the service value, so behaviour is unchanged."
engine/core/review_spec.py:681:         ),
```
**The sentence the host change made untrue:** "-1 equals the service value, so behaviour is
unchanged." Its premise ("-1 equals the service value") is false since 00:06:51 UTC (the service
value is 30m). For the engine's own requests the consequence, unchanged behaviour, still holds,
because they keep sending −1. Measured against the host, though, the engine's requests now behave
differently from everyone else's (CC's inference: they pin residency indefinitely; finding 1). The
first sentence ("Sent per request so the manifest's value is the one in force …") still holds.

**Sites that send keep_alive −1** (through `**cfg.kwargs()`), with their entry points (reach from
`q2_entry_census.json` and the import lines named):

| site | stage | entry points |
|---|---|---|
| `engine/agents/screener.py:125` `response = ollama_chat(messages=build_messages(paper, spec, role=role), **cfg.kwargs())` | abstract_screen_primary / _verifier | `scripts/run_pipeline.py`, `scripts/screen_expanded.py` |
| `engine/agents/ft_screener.py:216-218`, `:226-228` `return_request_hash=True, **cfg.kwargs())` | ft_screen_primary / _verifier | `engine/agents/ft_screener.py` (FT runner) |
| `engine/agents/extractor.py:236-237`, `:344-346`, `:432-436` `**cfg.kwargs()` | extract_pass1, extract_pass2, extract_retry_snippet | `scripts/run_pipeline.py`. Pass 1 also from `analysis/eval/run_local_ab.py:114`, `run_local_abc.py:131` and `smoke_regression01.py:67` (`extract_pass1_reasoning(prompt, cfg=stage_config("extract_pass1", spec))`); pass 2 from `smoke_regression01.py:68` (`extract_pass2_structured`) |
| `engine/elicitation/pipeline.py:295-296` `**cfg.kwargs())` | elicitation_pass1 | `scripts/run_pipeline.py` (elicited spec) |
| `engine/agents/auditor.py:73-75` `messages=build_audit_messages(span, field_type=field_type), **cfg.kwargs())` | audit | `scripts/run_pipeline.py` |
| `engine/parsers/pdf_parser.py:280-285` `**cfg.kwargs(),` | vision_parse | `scripts/parse_expanded_corpus.py`, `scripts/run_pipeline.py` (CC's reading of the census rows) |
| `engine/acquisition/pdf_quality_check.py:105-110` `**cfg.kwargs(),` | pdf_quality | `engine/acquisition/pdf_quality_check.py` |
| `engine/utils/ollama_preflight.py:69-74` `**cfg.kwargs(),` | preflight | `engine/utils/ollama_preflight.py`, and `require_preflight` from the extract loop (`extractor.py:1147-1149`) and the screening/FT/audit paths |

**Paths that send no keep_alive** (hand-built kwargs, so the host default applies, now 30m).
Pattern `keep_alive` over analysis/ → no such site found. All of these send none:
- `analysis/eval/elicit01/runner.py:118-124` (`options=dict(OPTIONS), think=PASS1_THINK,`). Entry: itself.
- `analysis/eval/run_capture01.py:119-128` (`options=dict(OPTIONS), think=PASS1_THINK,`). Entry: itself.
- `analysis/eval/run_local_ab.py:115-119` (`kwargs = {"options": {"temperature": 0}, "think": False}`), the pass 2 call. Entry: itself (but see finding 3: it cannot import).
- `analysis/eval/run_local_abc.py:136-142` (`kwargs = {"options": {"temperature": 0}, "think": models.pass2_think}`), the pass 2 call. Entry: itself.
- `analysis/eval/run_qualgap01.py:117-118`, `:158-164`, `:231-235`. Entry: itself. Its host is a second server on :11435 by default (`run_qualgap01.py:263` `p.add_argument("--host", default=DEFAULT_HOST)`), not the service.
- `analysis/paper1/judge.py:116-124`, `:225-232` (`format=…, options={"temperature": …, "seed": seed, "num_ctx": …}`). Entries: `judge_cli.py:25`, `judge_codebook_smoke.py:82-83`, `pass2_full.py:32`, `pass2_smoke.py:32` (all `from analysis.paper1.judge import …`).
- `analysis/paper1/pass2_retry_single.py:215-221` (`options={"temperature": 0.0, "seed": seed, "num_ctx": DEFAULT_NUM_CTX}, think=False,`). Entry: itself.

---

## Q12. Models and lock

**The live resolved set** (live spec, falling back to declared defaults where the spec omits a block;
resolver `effective_config.py:314-364`):

| model | stage(s) | source |
|---|---|---|
| `qwen3:8b` | abstract_screen_primary | `surgical_autonomy.yaml:45` `  primary: "qwen3:8b"` |
| `gemma3:27b` | abstract_screen_verifier | `surgical_autonomy.yaml:46` `  verification: "gemma3:27b"` |
| `qwen3:32b` | ft_screen_primary | `surgical_autonomy.yaml:49` `  primary: "qwen3:32b"` |
| `gemma3:27b` | ft_screen_verifier | `surgical_autonomy.yaml:50` `  verifier: "gemma3:27b"` |
| `deepseek-r1:32b` | extract_pass1, extract_pass2, extract_retry_snippet, elicitation_pass1; arm `local_deepseek_r1_32b` | `surgical_autonomy.yaml:58`, `:63`, `:75` |
| `gemma3:27b` | audit | default `review_spec.py:688` `model: str = Field(default="gemma3:27b", …)` (no `auditor_model` in the live spec; `effective_config.py:332-336`) |
| `qwen2.5vl:7b` | vision_parse | default `review_spec.py:531-532` `vision_model: str = Field(` / `default="qwen2.5vl:7b",` (the spec has no `pdf_parsing` block) |
| `qwen2.5vl:7b` | pdf_quality | `surgical_autonomy.yaml:118` `  ai_model: "qwen2.5vl:7b"` (default `review_spec.py:724-725` the same) |
| (each of the above) | preflight | `stage_config("preflight", spec, model=model_name)`, `ollama_preflight.py:66` |

**Declared defaults that the live spec overrides** (named by the engine, used only by a spec that
omits the field): `review_spec.py:97` `verification: str = Field(default="qwen3:32b", …)` (live:
gemma3:27b) and `review_spec.py:105` `primary: str = Field(default="qwen3.5:27b", …)` (FT primary;
live: qwen3:32b). The lineage table names `qwen2.5vl:7b` and the five above
(`surgical_autonomy.yaml:106-110`); `qwen3.5:27b` is not in it.

**Analysis scripts:** `deepseek-r1:32b` in `analysis/eval/elicit01/runner.py:40`,
`run_capture01.py:60`, `run_local_abc.py:53` and `run_qualgap01.py:67` (`MODEL = "deepseek-r1:32b"`)
(extraction pass 1/2 studies). `gemma3:27b` in `analysis/paper1/judge.py:40`,
`judge_codebook_smoke.py:107`, `pass2_full.py:60`, `pass2_retry_single.py:58` and `pass2_smoke.py:59`
(`DEFAULT_MODEL = "gemma3:27b"`) (judge Pass 1/2). `run_local_ab.py:33-34` imports `MODEL` from
`engine.agents.extractor`, which defines none (finding 3). Pattern for model literals:
`(MODEL|model)[A-Z_]* *[:=] *"[a-z0-9.\-]+:[a-z0-9.\-]+"|default="[a-z0-9.\-]+:[a-z0-9.\-]+"`. It
also finds `engine/migrations/003_backfill_expanded_screening.py:28-29` (`qwen3:8b`, `gemma3:27b`),
a historical backfill that sends nothing.

**dgx-infra's lane file** (`~/projects/dgx-infra`, HEAD `dbc02fdd90a2d8b9d9ca95a77d6c1a383f0ea8d2`,
`scripts/` clean, level with origin; not edited):
```
scripts/ollama_lane_models.txt:10: # evidence-engine — OLR-1 section 3
scripts/ollama_lane_models.txt:11: qwen3:8b
scripts/ollama_lane_models.txt:12: qwen3:32b
scripts/ollama_lane_models.txt:13: qwen3.5:27b
scripts/ollama_lane_models.txt:14: gemma3:27b
scripts/ollama_lane_models.txt:15: qwen2.5vl:7b
```
and `:21` `deepseek-r1:32b` under `# llm-council — OLR-1 section 3` (`:17`). `:3-4`: "A model listed
twice / is checked once."

**Differences.**
- In the engine's use but **not in the Evidence Engine block**: `deepseek-r1:32b`, the extraction
  model (Y3). It is listed only under llm-council (`:21`), so the light check still covers it, but
  under another lane's name. If llm-council dropped it from its block, the engine's extraction
  model would leave the check without anyone touching this lane.
- In the Evidence Engine block but **not in the live resolved set**: `qwen3.5:27b` (`:13`). It is
  only the declared FT-primary default (`review_spec.py:105`), and the live spec overrides it.
- `qwen3:32b` is in both, as the live FT primary (not, as the default would have it, the abstract
  verifier).

**The lock.** `engine/utils/ollama_lock.py:74` `_DEFAULT_LOCK_PATH = Path.home() /
".ollama_experiment.lock"`; `:77-80` `override = os.environ.get("OLLAMA_EXPERIMENT_LOCK")` / `return
Path(override) if override else _DEFAULT_LOCK_PATH`. The health check uses the same:
`ollama_health_check.sh:146` `LOCKFILE="${OLLAMA_EXPERIMENT_LOCK:-$HOME/.ollama_experiment.lock}"`.
Both resolve to `/home/ankitsarin/.ollama_experiment.lock` and honour the same override variable.

---

## Q13. The close, read and not run

**This session's id:** `c72bc042-ed30-45b4-924f-2b0cddc440ea` (`CLAUDE_CODE_SESSION_ID`; equal to
`"sessionId"` in `~/.claude/sessions/3513716.json`, `CLAUDE_PID=3513716`). Its transcript is
`~/.claude/projects/-home-ankitsarin-projects-evidence-engine/c72bc042-ed30-45b4-924f-2b0cddc440ea.jsonl`.
Job-store record: `grep -rl <id> ~/.claude/jobs/` → none.

**/wrap** is `~/.claude/commands/wrap.md` → `~/claude-config/commands/wrap.md` (via the symlink
`~/.claude/commands -> /home/ankitsarin/claude-config/commands`; 421 lines, sha256 `66aa91b0…2d10`).
What it would do for this session:
- **Step 1** (`wrap.md:42-114`): no tasks to `TaskStop`, because this session armed none (CC's
  in-context knowledge). No job record. It counts the queued messages from the transcript.
- **Step 2, git** (`wrap.md:118-160`): evidence-engine is clean and level with nothing of this
  session's to commit, so **no commit and no push** ("For each repo this session actually worked
  in", `:120`). The scratch directory is outside every repository.
- **Step 4, global primer** (`wrap.md:181-201`): "The default is SKIP." No trigger fires for this
  session's own work (no service, unit, tool or convention changed by it), so the field reads
  `unchanged (no durable state delta)` (CC's reading).
- **Step 4b, the ledger: it WOULD commit another lane's row.** `wrap.md:226-230` "**This sub-step
  runs on every `/wrap`** … `PROJECT_LEDGER.md` accrues rows from *other* repos' hooks". `:241-247`
  "**Dirty** → confirm it is append-only … then commit and push it: `git -C ~/claude-config add
  PROJECT_LEDGER.md` / `git -C ~/claude-config commit -m "ledger: rows through <last timestamp>"`".
  `:253-255` "**No lane ownership applies here.** … committing another lane's rows takes nothing from
  that lane". In the state R2 describes (the surgical-cv row uncommitted), `/wrap` would have
  committed it as `ledger: rows through 2026-11-16T00:04:30Z` and pushed it. **That conflicts with
  R2-b** ("The surgical-cv row is not committed by this session"). It agrees with R654(d)'s general
  rule (`STANDING_RULES.md:81-83` "they are committed by this lane's next ledger step, named in its
  message"). **As found now, the point is moot:** another session committed that row at
  `1ae06141c32f3694d2693258406de38d5ab71eba` (2026-11-16T00:10:12Z, "ledger: rows through
  2026-11-16T00:04:30Z"), so claude-config's `PROJECT_LEDGER.md` is clean and step 4b would read
  `clean`.
- **Step 4c, project primer** (`wrap.md:257-312`): `~/projects/evidence-engine/primer.md` exists
  (195,942 B, 2026-11-14 22:08) and is ignored (`.gitignore:9:primer.md`, which also matches
  `primer-history/probe/primer.md`). If durable project state is judged changed, `/wrap` copies it to
  `primer-history/2026-11-16/primer.md` and **writes** the new primer, with no approval gate
  (`:271-275`). Whether this session's scratch-only read-out counts as a durable project state change
  is a judgement `wrap.md` leaves to the session (`:265-269`).
- **Step 5** (`wrap.md:316-421`): prints the closeout block, the audit-verify option, then
  "Run /exit now …" and the archive line.
- The only hook in play: `~/projects/evidence-engine/.git/hooks/post-commit` exists, but there would
  be no evidence-engine commit. `~/claude-config/.git/hooks/` has no non-sample hook, so a ledger
  commit there writes no further row.

**session-archive** is `~/projects/dgx-infra/bin/session-archive` (18 lines, `25671c8a…`
2026-09-05). It is a wrapper, `:16` `from session_audit.archive import main`, over
`session_audit/archive.py` (381 lines). What it writes: `archive.py:18-20` "The only mutation
performed is os.rename, twice at most, each with both / operands named in full. … no verb / here
removes anything: an archive is a MOVE". `:248-253`: the transcript moves to `archive_dir /
path.name`, then each job dir to `archive_dir / f"job-{job_dir.name}"`, with `archive_dir` defaulting
to `:73` `DEFAULT_ARCHIVE = Path.home() / "claude-session-archive"` (created `0o700`, `:80`, `:266`).
It refuses a LIVE session (`:224-225`). **It runs no git, so it commits and pushes nothing and does
not touch the ledger** (pattern `git|commit|push|ledger|PROJECT_LEDGER` over `archive.py`: no such
site found, other than words in docstrings). It would not commit the row.

**The archive command as it would be given after /exit** (`wrap.md:393`, with the id filled in):
```
archive after /exit :  ~/projects/dgx-infra/bin/session-archive c72bc042-ed30-45b4-924f-2b0cddc440ea
```
Neither command was run.

---

## Findings outside the questions

1. **The engine's −1 now pins its model while every no-keep-alive lane expires at 30m.** CC's
   inference from §Q11 and Y1: with one slot (`OLLAMA_MAX_LOADED_MODELS=1`), an engine model loaded
   last stays resident until another lane's request evicts it. The other lanes' models now free the
   slot after 30 minutes idle. Section 1 item 4 keeps −1 deliberately, and this is a property of
   that choice, not a defect.
2. **The engine's analysis harnesses send no keep_alive** (computed, §Q11), so their runs now take
   the host's 30m. A judge or eval run idle for more than 30 minutes between calls would now pay a
   cold load it did not pay before 00:06:51 (CC's inference).
3. **`analysis/eval/run_local_ab.py` cannot be imported** (computed by AST, not executed):
   `run_local_ab.py:33-34` `from engine.agents.extractor import ( / MODEL,`, and `extractor.py`
   assigns no top-level `MODEL` (AST scan of its module body: `[]`). CLAUDE.md records the constant
   as removed ("`extractor.MODEL` … gone — C1"). The entry census still lists it as reaching
   `ollama_chat` because the AST walk does not resolve the failing import. The docs/architecture
   lists naming `MODEL = "deepseek-r1:32b"` (`modules.md:146`, `pipeline.md:231`) are stale for the
   same reason.
4. **Y4 covers the full sweep only.** Computed: the light mode, now six days in seven, sends GETs and
   does not consult the lock (`ollama_health_check.sh:147-148`). That is harmless to a run, since GETs
   load nothing, but "stands down on the lock" is true only on Sundays (UTC).
5. **`/wrap` step 4b and R2-b disagree about other lanes' ledger rows** (computed, §Q13). `wrap.md`
   commits them on every `/wrap` with no lane ownership; R2-b forbade it for this session, and R654(d)
   assigns them to "this lane's next ledger step". The disagreement was mooted today by `1ae0614`. A
   later close of this session should say which rule governs if a foreign row reappears before it.
6. **claude-config moved during this session** (computed): `7c74c2b` → `0ed6266f2bd398dc388a1c4264141f890de24870`
   (2026-11-16T00:10:12Z, "primer: Ollama keep-alive 30m live; health check light/full live (OHC-1)",
   primer.md +2 inside §18) → `1ae06141c32f3694d2693258406de38d5ab71eba` (the same second, the
   surgical-cv ledger row). Both were pushed (0/0 with origin) by another session. Neither touches
   `primer.md` lines 41–45 (the diff hunk is `@@ -2488,3 +2488,5 @@`).

## Gates

Read at the STOP and reported in the final report: G1 (evidence-engine as at step 0), G2 (every answer
above carries a quoted line or a pattern), G3 (`sha256sum -c SHA256SUMS`), G4 (the Ollama journal over
this resume's window), G5 (R2-b's form).
