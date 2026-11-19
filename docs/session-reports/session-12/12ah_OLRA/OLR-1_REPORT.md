# OLR-1 — cross-lane Ollama read-out (read-only)

Run 2026-11-15 23:18:16Z → 23:27:10Z (`t_start.txt`, `t_end.txt`), DGX `spark-59e4`.
All raw outputs are in `~/scratch/ollama-OLR-1/` and are named per section (`s0_*`, `s1_*` …).
"MY COUNT" means I counted by reading. Every other count shows the command that produced it.

**Headline.** No restarts of Ollama have happened for 2.5 months. The last sudo restart was
2026-08-31 00:41:58. The service has been up since boot (2026-10-17 23:04:05, `NRestarts=0`).
Cross-lane interference now comes from **model eviction**, not restarts. `OLLAMA_MAX_LOADED_MODELS=1`
means that any lane asking for a different model (or, per hello-ai-v2's own comment, a different
`num_ctx`) evicts whatever is resident. The restart code paths still exist in evidence-engine,
behind the experiment flock and an opt-out env var.

---

## 0. Ollama service

`systemctl show ollama -p MainPID,ActiveEnterTimestamp,NRestarts,User` (start of audit):
```
MainPID=2564
NRestarts=0
User=ollama
ActiveEnterTimestamp=Sat 2026-10-17 23:04:05 UTC
```
Boot time is `2026-10-17 23:03:53` (`uptime -s`), so Ollama has run since boot.

`systemctl cat ollama` (in full):
```
# /etc/systemd/system/ollama.service
[Unit]
Description=Ollama Service
After=network-online.target

[Service]
ExecStart=/usr/local/bin/ollama serve
User=ollama
Group=ollama
Restart=always
RestartSec=3
Environment="PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin"

[Install]
WantedBy=default.target

# /etc/systemd/system/ollama.service.d/override.conf
[Service]
Environment="OLLAMA_KV_CACHE_TYPE=f16"
Environment="OLLAMA_MAX_LOADED_MODELS=1"
Environment="OLLAMA_NUM_PARALLEL=1"
Environment="OLLAMA_FLASH_ATTENTION=true"
Environment="OLLAMA_KEEP_ALIVE=-1"
```
This matches the ledger's LCH-1 quote. Pre-existing finding: the main unit has `RestartSec=3`,
but CLAUDE.md requires `RestartSec=10` in all units. It is an installer-owned unit, so I am only noting it.

`GET /api/version` → `{"version": "0.21.0"}`

`GET /api/ps`:
```
name gemma2:27b  size 19298629632  size_vram 19298629632  context_length 8192
expires_at "2319-02-25T21:55:22.884246082Z"
```
`GET /api/tags`: 20 models (`python3 -c ...` over `s0_api_tags.json`):
```
qwen3.6:35b-a3b 23.9 GB | qwen3.6:27b 17.4 | gpt-oss:20b 13.8 | gemma4:26b 18.0 | qwen3:30b-instruct 18.6
gemma4:31b 19.9 | qwen3.5:27b 17.4 | gemma3:27b 17.4 | qwen2.5vl:7b 6.0 | qwen3:8b 5.2 | qwen3:32b 20.2
minicpm-v:latest 5.5 | qwen3-vl:8b 6.1 | qwen2.5:7b-instruct 4.7 | deepseek-r1:32b 19.9 | mistral:7b 4.4
gemma2:27b 15.6 | qwen2.5:32b 19.9 | llama3.3:70b 42.5 | llama3.2:3b 2.0
```

**Who loaded the resident gemma2:27b (inference, from the journal).** Between 22:05:18 and 22:07:50
tonight, `journalctl -u ollama` shows six `loading model` events in under 3 minutes, rotating
through 33-, 65- and 47-layer models, each followed by a `POST "/api/chat"`. The last load is at 22:07:50.
`llm-council.service` has `ActiveEnterTimestamp=Sun 2026-11-15 22:37:39 UTC`, and the keep_alive commit
`ac63d50` is dated 22:12:50. So the load came from the **pre-LCH-1** llm-council process, which sent
no keep_alive. The server default `-1` applied, which gives the ~293-year `expires_at`. Nothing will
unload it until another model is requested. Ollama 0.21.0's runner PID is 3376434 (started ≈22:07;
`ps` etime 01:16:08 at 23:24).

---

## 1. Restart timeline

**B1 holds.** `/var/log/auth.log*` is `-rw-r----- syslog adm` and `id` shows group `4(adm)`.
`journalctl -u ollama` is readable, and its earliest line is `Jan 14 23:54:09 … Started ollama.service`.
Coverage is uneven. auth.log* starts at `2026-07-01T00:00:02` (`zcat -f auth.log.4.gz | head -1`),
while the ollama journal reaches back to 2026-01-14. **Restarts before July cannot be attributed.**

### 1a. sudo lines naming ollama
`zgrep -h 'sudo:' /var/log/auth.log* | grep -i ollama` → **79 lines** (`s1_sudo_ollama_raw.txt`).
- With `TTY=`: **4**. Without: **75** (`grep -c 'TTY='` / `grep -vc 'TTY='`).
- By COMMAND (`sed -n 's/.*COMMAND=//p' | sort | uniq -c`):
  ```
  75 /usr/bin/systemctl restart ollama
   1 /usr/bin/systemctl is-active ollama          ← "a password is required"
   1 /usr/bin/journalctl -b -1 -u ollama …        ← TTY=pts/4
   1 /usr/bin/cat /usr/share/ollama/.ollama/models/manifests/…/qwen3/8b   ← "a password is required"
   1 /usr/bin/cat /etc/systemd/system/ollama.service.d/override.conf      ← "a password is required"
  ```
  None of the 75 restarts carries "password is required" (`grep -c` → 0). The `is-active` call was
  refused. So the NOPASSWD rule covers `restart` and does **not** cover `is-active` (inferred from behaviour; the rule itself was not read).

**The 75 restarts, by PWD** (`sed -n 's/.*PWD=\([^ ]*\) .*/\1/p' | sort | uniq -c`):
```
74 /home/ankitsarin/projects/evidence-engine
 1 /home/ankitsarin/projects/inference-determinism
```
**By month × TTY** (awk over `s1_restart_lines.txt`):
```
2026-07 notty 46
2026-08 notty 26
2026-08 tty   3
(Sep, Oct, Nov: 0)
```
**Time-of-day** (`awk '{print substr($1,12,5)}' | sort | uniq -c`): **51 at 09:03 UTC**, and every
other minute appears once. Days with more than one restart: 07-27 (6), 07-28 (7), 07-29 (2), 07-30 (2), 07-31 (3),
08-01 (4), 08-30 (2).

**The 10 most recent restarts:**
```
2026-08-17T09:03:42  2026-08-18T09:03:39  2026-08-19T09:03:39  2026-08-20T09:03:41
2026-08-21T09:03:39  2026-08-22T09:03:39  2026-08-23T04:58:51  2026-08-30T07:08:06 (TTY=pts/0)
2026-08-30T23:54:00 (TTY=pts/0)  2026-08-31T00:41:58 (TTY=pts/0)
```
First 09:03 restart in auth coverage: 2026-07-01. Last: 2026-08-22.

### 1b. Journal stop/start and matching
`journalctl -u ollama -o short-iso-precise | grep -E 'systemd\[1\]: (Start|Stopp|ollama.service:)'`
gives 383 `Started`, 376 `Stopping`, and 2 `Failed with result 'oom-kill'` (`s1_journal_lifecycle.txt`).
I classified each start with `classify.py` (in the folder), output in `s1_start_classification.txt`:

```
journal starts: 383  range 2026-01-14 23:54:09 -> 2026-10-17 23:04:05
whole journal:   301 stop+start, no sudo line within ±60 s   (288 of these predate auth.log coverage)
                  75 stop+start matched to sudo restart (±60 s)
                   4 boot
                   2 auto-restart after failure (oom-kill)
                   1 start with no preceding stop/boot/failure (first journal line)
on/after 07-01:   75 matched to sudo | 6 no sudo | 1 boot
sudo restart lines: 75; matched to a journal stop within ±60 s: 75; unmatched: []
```
**Match rate in the covered window is 75/75 sudo lines → journal stop (100%).** The 6 starts without
a sudo line, read one by one:
| start | cause (evidence) |
|---|---|
| 2026-08-10 01:28:35 | `sudo[3585067]: … TTY=pts/1 … COMMAND=/usr/bin/apt install linux-modules-nvidia-580-open-6.17.0-1029-nvidia` at 01:27:16. Ollama stopped together with `lists.service` (a service restart during the package install), then the kernel logged `NVRM: API mismatch: the client 'ollama' … 580.173.02, but this kernel module has … 580.159.03` |
| 2026-08-10 01:31:10 | reboot (boot −5 first entry 2026-08-10 01:31:04) |
| 2026-08-23 07:22:11 | reboot (boot −4 07:22:07) |
| 2026-09-11 20:48:09 | reboot (boot −3 20:48:04) |
| 2026-09-11 21:09:13 | reboot (boot −2 21:09:10) |
| 2026-10-16 23:05:57 | reboot (boot −1 23:05:38) |
| (2026-10-17 23:04:05) | reboot, boot 0, the current process. The classifier counted it as "boot" |

The classifier put the five reboots under "stop+start" because the previous boot's shutdown
`Stopping` falls within 120 s. That is MY reading of the table above. Other causes in the
whole journal: **2026-03-10 06:30:28 and 06:36:34**, `A process of this unit has been killed by the OOM killer`
→ `Scheduled restart job, restart counter is at 1` / `2` (`s1_journal_failures.txt`). There are also two
`CUDA error: out of memory` lines (2026-03-01, 2026-03-14) with no restart attached.

**Pre-July (unattributable):** 288 stop+starts. MY COUNT from the `Stopping` time histogram before
2026-07: 70 at 09:03, 23 at 09:01, 3 at 09:02, the rest scattered. The 09:0x cluster first appears 2026-03-15.
The sudoers file is dated 2026-03-14 22:13:56 (§6), and March has 112 stop+starts. That pattern fits the
same nightly-suite cause, but it is **inference**, since no auth.log covers March.

**What fired at 09:03.** `crontab -l` → `0 9 * * * /bin/bash /home/ankitsarin/projects/evidence-engine/scripts/nightly_tests.sh`.
Journal on 2026-08-22: `09:00:01 CRON[2741776]: (ankitsarin) CMD (/bin/bash …/evidence-engine/scripts/nightly_tests.sh)`
→ `09:03:39 sudo[2742513]: ankitsarin : PWD=/home/ankitsarin/projects/evidence-engine ; USER=root ; COMMAND=/usr/bin/systemctl restart ollama`.
The cause is recorded in evidence-engine `tests/conftest.py:3-9`:
> "`test_timeout_logs_warning` patched the Ollama *client* but not the `subprocess` boundary, so when `ollama_chat`'s last-resort recovery fired it ran a real `sudo systemctl restart ollama`. A NOPASSWD sudoers rule for exactly that command let it through, so **every offline suite run restarted the production Ollama service** — confirmed by journal timestamp during QUALGAP-01 (14:56:09, 15:49:23) and again by that task's own acceptance-gate run (2026-08-23 04:58:52)."

It was fixed by `184d857 2026-08-23 test: fence the suite against service-manager calls (OPSFIX-01)`.
After that commit, the 09:03 restarts stop. The last one is 2026-08-22, and 2026-08-23 09:00 shows the cron with no restart.

**Attribution of the 75 (MY COUNT, with sources):**
| n | cause | basis |
|---:|---|---|
| 51 | nightly offline suite leaking the last-resort restart (09:03) | cron line + conftest.py:3-9 + stop after 184d857 |
| 3 | interactive suite runs (08-01 14:56:09, 15:49:23; 08-23 04:58:51) | timestamps quoted in conftest.py:8-9 |
| 3 | operator at a terminal, evidence-engine PWD (08-30 07:08, 23:54; 08-31 00:41) | `TTY=pts/0` |
| 1 | inference-determinism PWD, 07-27 17:24:43 | 35 s before its Arm P rerun started (`CONTAMINATED.md`: "started 2026-07-27 17:25:18 UTC"). **Inference:** a deliberate pre-run restart |
| 17 | evidence-engine, no TTY, not 09:03, Jul 27 – Aug 1 | **unattributed**. Consistent with proactive `restart_ollama()` (RESTART_EVERY_N=25) during extraction runs. inference-determinism `CONTAMINATED.md:36-38`: Arm P "was itself destroyed … by the Evidence Engine's own proactive `restart_ollama()` (`RESTART_EVERY_N=25`) issuing `systemctl restart ollama` while the arm was in flight" (fits 07-27 18:28/18:29) |

---

## 2. Restart sources in code and schedules

Scope (`s2_repos.txt`): every directory under `~/projects` is a git repo (12), plus `~/claude-config`.
`~/scripts` is the symlink `-> /home/ankitsarin/projects/dgx-infra/scripts`, so it is covered by
dgx-infra. Search: `git ls-files -z | xargs -0 grep -InHiE 'systemctl restart ollama|restart ollama|pkill.*ollama|ollama stop|ollama serve|keep_alive|OLLAMA_'`
→ 32,494 lines (`s2_grep_all.txt`). 29,694 are evidence-engine docs/session reports. I then added a
second search for list-form argv (`["sudo", "systemctl", "restart", "ollama"]`), which the string
patterns miss (`s2_restart_defs.txt`).

**G3 positive control:** `llm-council/council.py:94: keep_alive="10m",` and `council.py:167: keep_alive="10m",`, so it passes.

### Live restart call sites (production code; tests and docs excluded)
| repo | file:line | function | trigger | guard / comment |
|---|---|---|---|---|
| evidence-engine | `engine/agents/extractor.py:789-792` `subprocess.run(["sudo", "systemctl", "restart", "ollama"], timeout=30, check=True, capture_output=True)` | `restart_ollama(reason, papers_done)` (def :744) | **Proactive.** `extractor.py:1267-1268` `papers_since_restart += 1; if restart_every > 0 and papers_since_restart >= restart_every:` with `RESTART_EVERY_N = 25  # proactive Ollama restart interval (0 = disabled)` (:61), the default for `run_extraction` (:1000) and `_extract_selected` (:1107) | Docstring :747-748: "to fully clear CUDA context fragmentation (a simple model unload/reload is not sufficient)". Skips if `restart_disabled()` (env `EVIDENCE_ENGINE_NO_OLLAMA_RESTART`, :760) or `foreign_lock_held()` (:775). Failure is caught and the run continues (:1276-1280) |
| evidence-engine | `engine/utils/ollama_client.py:931-936` same argv | `_restart_ollama_and_retry(...)` (def :878) | **Last resort.** Called at :811 inside `except httpx.ReadTimeout` in `ollama_chat` | Same two gates (:908-921). Docstring: "an unguarded restart on this box killed the inference-determinism Arm P rerun." Then `time.sleep(10)` and one retry |
| evidence-engine | `analysis/paper1/pass2_full.py:257` `restart_ollama(reason="proactive", …)` | pass-2 loop | `if restart_every and processed > 0 and processed % restart_every == 0` (:255). CLI `--restart-every`, **default 0** (:712) | — |
| evidence-engine | `analysis/eval/elicit01/runner.py:193` | runner loop | `restart_ollama(reason="elicit01 cadence", papers_done=n)` (cadence = RESTART_EVERY_N, imported :35) | — |
| evidence-engine | `analysis/eval/run_capture01.py:227` | capture loop | `restart_ollama(reason="capture01 cadence", …)`. Docstring :20: "Proactive `restart_ollama()` every `RESTART_EVERY_N` calls to clear CUDA" | — |

No restart, `pkill` or `ollama stop` call exists in production code of any other repo.
The `ollama serve` hits are user-facing error strings only: `hello-ai-v2/backend/ollama_client.py:240`,
`hello-ai/app.py:245`, `surgical-cv/bench/bench_pipeline_models.py:174`. inference-determinism has
**no** restart code in tracked `.py`/`.sh` (`grep -iE 'systemctl|restart|unload'` → only
`keep_alive` and doc lines), so its 07-27 restart was not issued by its own tracked code. Tests
(`evidence-engine/tests/test_service_call_fence.py:48,54` and others) call restart only to
assert that the fence blocks it.

### Schedules (`s2_schedules.txt`)
`crontab -l`:
```
0 9 * * *    evidence-engine/scripts/nightly_tests.sh        (Ollama fenced since 184d857; STANDARD_GATE="not network and not ollama and not integration")
30 13 * * *  ~/scripts/morning_digest.sh
0 */6 * * *  ~/scripts/service_health_check.sh
0 7 * * *    ~/scripts/ollama_health_check.sh                ← loads every installed model, see §3
30 9 * * *   citation-mcp/harness/run_nightly.sh             (citation-mcp: zero Ollama hits in tracked files)
```
`/etc/cron.d/*` contains only stock entries (anacron, atop, e2scrub_all, sysstat). `/etc/cron.daily` is stock plus etckeeper.
None mention Ollama. System timers: stock ones plus `dgx-snapshot-{user,root,upload}` and
`dgx-backup-monitor`. The snapshot user tier GETs `/api/tags` only (`dgx-infra/snapshot/user_tier.py:51`).
User timers: `surgical-cv-health` and `surgical-cv-worker` (every 5 min), `surgical-cv-backup`
(03:17). **No timer or cron restarts Ollama.**

---

## 3. Ollama clients by lane

| lane | unit (state now) | client library | models (quoted config) | keep_alive | restarts Ollama? (where) | parallel calls? |
|---|---|---|---|---|---|---|
| **evidence-engine** | none deployed. Cron nightly (fenced). Runs are ad-hoc and long (`systemd-run --user` units per the global rule) | own wrapper `engine/utils/ollama_client.py` over httpx | `review_spec.py:96` `primary … default="qwen3:8b"`; `:97` `verification … "qwen3:32b"`; `:105` `"qwen3.5:27b"`; `:106` `"gemma3:27b"`; `:688` auditor `"gemma3:27b"`; `:532`/`:725` vision `"qwen2.5vl:7b"`. Extraction models come per spec from `extraction_models` (not enumerated) | **sent per request** from the spec. `review_spec.py:675-676` `keep_alive … Field(default=-1` ("-1 equals the service value") | **yes**: `extractor.py:790` (proactive, every 25), `ollama_client.py:932` (read-timeout recovery), plus the 3 analysis scripts above. Gated by flock and an env opt-out | no. `ThreadPoolExecutor(max_workers=1)` (`ollama_client.py:781,951`) is a wall-clock watchdog, not fan-out |
| **llm-council** | `llm-council.service` (system, running since 22:37:39) | `ollama` python (`ollama.chat` via `asyncio.to_thread`). Chairman uses `client.messages.create` with `CHAIRMAN_MODEL = "claude-opus-4-5-20251101"` (`config.py:109`, Anthropic, not Ollama) | `config.py:10-14`: `"llama3.3:70b"`, `"qwen2.5:32b"`, `"gemma2:27b"`, `"deepseek-r1:32b"`, `"mistral:7b"` | `"10m"` (`council.py:94,167`) | no (removed in `ac63d50`: "remove Ollama-restart button") | **yes**: `asyncio.gather(*tasks)` at `council.py:281,428,549,695` over up to 5 models. With NUM_PARALLEL=1 and MAX_LOADED=1 this serialises into a load per model, which matches the six loads in 3 min observed 22:05–22:08 |
| **hello-ai-v2** | `hello-ai.service` (system; ExecStart `…/hello-ai-v2/venv/bin/python`) | `ollama.Client(timeout=…)` (`backend/ollama_client.py:93,106`). Cloud fallback `CLOUD_MODEL = "claude-sonnet-5"` (`anthropic_client.py:24`) | user-selected from the `/api/tags` inventory (`backend/inventory.py`). `capabilities.py:41` `POLICY_EXCLUDED_LOOP_MODELS = frozenset({"llama3.3:70b"})` | **not set**, so it falls to server `-1` | no | none found (grep for gather/ThreadPool/Semaphore: no hits). Sets `options={…"num_ctx": OLLAMA_NUM_CTX}` with `OLLAMA_NUM_CTX = 32_768` (`base.py:44`). Comment `ollama_client.py:151-152`: "a different num_ctx on the same model is a different load" |
| **lists** | `lists.service` (system) | raw HTTP `http://localhost:11434/api/generate` (`app.py:168,222`) | `"qwen2.5:7b-instruct"` (:169), `"qwen3-vl:8b"` (:224) | not set (→ −1) | no | no |
| **operativereports** | `operativereports.service` (system) | `ollama` python | `app.py:278` `model="qwen2.5:32b"`; `report_generator.py:10` `DEFAULT_MODEL = "qwen2.5:32b"`; `ocr_engine.py:12` `MODEL_NAME = "minicpm-v"` | not set (→ −1). Options `temperature 0.1, num_predict 1024` (`app.py:283-286`) | no | no |
| **surgical-cv** | `surgical-cv-worker.service` (user, `python -m app.worker --once`, every 5 min) → `dispatch.py:132` `["verify", …]` → `pipeline/commands/verify.py:35` imports the diagnostician | `ollama.Client(timeout=_OLLAMA_CHAT_TIMEOUT_S)` (`pipeline/diagnostician.py:62`) | `diagnostician.py:30` `OLLAMA_MODEL = "qwen3:32b"` | not set (→ −1). Options `temperature`, `num_predict`. `format="json"`, `think=False` (:254-262) | no | no |
| **inference-determinism** | none (ad-hoc probes) | HTTP payloads (`probes/common.py:401`) | per probe (not enumerated) | **explicit `-1`**: `common.py:46 KEEP_ALIVE = -1`. `tests/test_probe.py:41 assert C.KEEP_ALIVE == -1` | no (in tracked code). 1 sudo restart from its PWD (§1) | no evidence of fan-out |
| **dgx-infra** (health cron) | cron `0 7 * * *` `ollama_health_check.sh` | `python3 -I ollama_check_one.py check <model> <timeout>` over HTTP | **every model in `ollama list`** (`ollama_health_check.sh:136,150`). That is 20 today, including `llama3.3:70b` | **`0`**: `ollama_check_one.py:40 KEEP_ALIVE = 0`, `num_ctx 4096` | no | no (serial loop). Stands down when the experiment flock is held (`:55-74`, "SKIPPED — experiment lock held") |
| hello-ai (v1) | none (the hello-ai unit runs v2) | `ollama.chat` (`app.py:229`) | `model_name` (UI) | not set | no | no |
| citation-mcp | `citation-mcp.service` | **no Ollama use** (0 hits) | — | — | — | — |

**Models shared across lanes** (MY COUNT from the table): `qwen3:32b` (evidence-engine verification, surgical-cv
diagnostician); `qwen2.5:32b` (llm-council, operativereports). With one model slot, any two distinct models from
any two lanes evict each other. `num_ctx` also differs by lane: hello-ai-v2 sends 32768, the health check 4096,
and the others send the model default. **Inference**, per hello-ai-v2's own comment: the same model can reload when lanes disagree on `num_ctx`.

**Cross-lane eviction, measured today.** `~/logs/ollama_health_20261115.log`: "Resident at start: none". 20 models
were loaded in turn 07:00 → `Done: Sun Nov 15 07:06:42 AM UTC 2026`. Each load evicts whatever was resident.
That evicted model reloads cold on its next use. `ollama_lock.py:4-6` records the earlier harm: "the 07:00 health cron
killed the attempt before it" (since guarded by the flock).

---

## 4. GPU and memory outside Ollama (`s4_gpu_mem.txt`)

`nvidia-smi`: `NVIDIA GB10 … Driver Version: 580.173.02 CUDA Version: 13.0`; Memory-Usage column **`Not Supported`**.
Processes:
```
22108   G /usr/lib/xorg/Xorg           345MiB
22745   G /usr/bin/gnome-shell         244MiB
3376434 C /usr/local/bin/ollama      18902MiB
```
`--query-compute-apps=pid,process_name,used_memory --format=csv` **is supported** (rc=0):
`3376434, /usr/local/bin/ollama, 18902 MiB`.

`free -g`: `Mem: 119 total, 39 used, 8 free, 80 buff/cache, 80 available; Swap: 15 total, 4 used, 11 free`.

Top 10 by RSS (`ps -eo pid,user,rss,etime,comm --sort=-rss`): `ollama` 3376434 1.6 GB RSS. Seven
`MainThread` processes of 0.43–1.13 GB each resolve (`readlink /proc/<pid>/exe`) to
`~/.vscode-server/.../server/node` (VS Code server and Pylance). Two `claude` processes are ~0.45 GB each.

`tmux ls` → `no server running on /tmp/tmux-1000/default`.

**GPU code outside Ollama** (`git ls-files … | grep -IlE 'import torch|\.cuda\(|device="cuda"|onnxruntime|CUDAExecutionProvider|h264_nvenc|hwaccel.*cuda|whisper.load_model|SentenceTransformer'`, tests/docs excluded):
- `surgical-cv/app/edit/preview_runner.py:98-104`: `nvenc_argv` → `"-hwaccel", "cuda", "-hwaccel_output_format", "cuda"`, `"scale_cuda=w=-2:h=480"`, `"-c:v", "h264_nvenc"`. The default encoder is `nvenc` (:124-128). This is GPU video decode and encode, **not PyTorch**.
- `operativereports/rag_engine.py:7,20` and `dgx-infra/tools/gen_chroma_baseline.py:38,64`: `SentenceTransformer(EMBEDDING_MODEL)` with no device argument. **Inference:** this would use CUDA only if the venv's torch has sm_121 support. CLAUDE.md says PyTorch does not support it yet, so CPU is likely.
- `lists/app.py:15`: `whisper.load_model("base.en", device="cpu")`, which is CPU.
- **No `import torch` / `.cuda()` in any repo's tracked production code.** Right now only Xorg, gnome-shell and ollama hold GPU memory.

**Unified memory (inference).** GB10 has no separate VRAM. The 18,902 MiB nvidia-smi attributes to ollama is
carved from the same 119 GiB that `free` reports, so "used" in `free` (39 GiB) already contains it, and
`size_vram` in `/api/ps` (19.30 GB) reports the same allocation from Ollama's side. nvidia-smi
cannot give a device-wide total ("Not Supported"), so GPU headroom can only be read from host
memory (`free`/`MemAvailable`, which is what earlyoom watches). A non-CUDA consumer of RAM, such as VS Code or page cache,
competes directly with model loads. Separately: `journalctl | grep -c NV_ERR_NO_MEMORY` across the whole
retained journal → **1971** (not broken down by date here; the global primer records 37 this boot, clustered in 07:00 health windows).

---

## 5. earlyoom (`s5_*`)

Unit `/usr/lib/systemd/system/earlyoom.service`, `EnvironmentFile=-/etc/default/earlyoom`, `ExecStart=/usr/bin/earlyoom $EARLYOOM_ARGS`, `MemoryMax=50M`.
`/etc/default/earlyoom`:
```
EARLYOOM_ARGS="-r 3600 -m 6 -s 100,100 --avoid '^(sshd|tailscaled|cloudflared|systemd|systemd-journal|NetworkManager|Xorg|gnome-shell|gdm.*)$' --prefer '^ollama$'"
```
Effective (journal 2026-10-18 05:43:03): `sending SIGTERM when mem <=  6.00% and swap <= 100.00%, SIGKILL when mem <=  3.00% and swap <= 100.00%`; `Preferring to kill process names that match regex '^ollama$'`.
**Inference:** `-s 100` makes the swap condition always true, so only `MemAvailable` ≤ 6% (≈7.4 GiB of 122,566 MiB) triggers a kill, and **ollama is the preferred victim**.
A kill of the runner or server drops every lane's in-flight request, the same as a restart does.

Kills: `journalctl -u earlyoom` has 706 lines, the first at `2026-10-18T05:42:57`, the day earlyoom was installed.
`grep -cE 'to process|sending SIG(TERM|KILL) to'` → **0**. The lowest periodic sample (hourly `-r 3600`, so sampled, not a true minimum) is
`2026-10-19T01:43:15 mem avail: 57413 of 122566 MiB (46.84%)`. The latest is `2026-11-15T22:50:12 mem avail: 82270 … (67.12%)`.
Other OOM events (not earlyoom): kernel log, current boot only, `2026-10-18T00:44:43 … Memory cgroup out of memory: Killed process 92825 (python3)` inside
`memcg=/user.slice/…/app.slice/run-u13` (a memory-capped transient user unit, not Ollama). Plus the 2026-03-10 ollama oom-kills ×2 (§1).

---

## 6. sudoers rule

Metadata only (`stat`, `ls -l /etc/sudoers.d/`):
```
-r--r----- root:root 65 2026-03-14 22:13:56.669540144 +0000 /etc/sudoers.d/ollama-restart
-r--r----- root root 83 Jun 16 19:59 surgical-cv-deploy
-r--r----- root root 1068 Jan 29 2024 README
```
`cat` → `Permission denied`. The content was not read. **Inferred from behaviour:** NOPASSWD for
`/usr/bin/systemctl restart ollama` (75 restarts and no "password is required"), but not
`is-active` (refused 2026-08-23 04:56:47).

**What breaks if the rule were removed or narrowed** (from §1–2):
1. `evidence-engine/engine/agents/extractor.py:790` `restart_ollama()`: sudo with no tty would fail. **Inference:** "a terminal is required…", bounded by `timeout=30` → `RuntimeError("Failed to restart Ollama: …")`. In `run_extraction` this is caught: "Proactive Ollama restart failed — continuing without restart" (:1276-1280). The proactive restart would then never happen.
2. `evidence-engine/engine/utils/ollama_client.py:932` `_restart_ollama_and_retry`: raises `_RestartEnded(DETAIL_RESTART_COMMAND_FAILED)` → `TimeoutError`. That is the same outcome as its existing refusal paths, minus the one retry after a restart.
3. `analysis/paper1/pass2_full.py:257` (only if `--restart-every` > 0), `analysis/eval/elicit01/runner.py:193`, `analysis/eval/run_capture01.py:227`: these call `restart_ollama`. I did not read whether they catch its `RuntimeError`.
4. Interactive operator restarts (3 in Aug, `TTY=pts/0`) would need a password. That is the normal sudo path, so nothing breaks; it just prompts.
5. The test suites already refuse any service call (conftest fence), so tests are unaffected.

No other lane, unit, cron or timer depends on the rule.

---

## 7. Policy options (not chosen)

Reload cost, **measured** (cold load + one short generate at num_ctx 4096; `~/logs/ollama_health_20261115.log`; not a benchmark):
`llama3.2:3b 4.5s · mistral:7b 4.9s · qwen2.5vl:7b 4.3s · qwen3:8b 6.9s · qwen3-vl:8b 16.6s · gemma3:27b 5.7s · gemma2:27b 21.2s · qwen3.5:27b 21.2s · qwen3:32b 23.3s · deepseek-r1:32b 20.9s · qwen2.5:32b 44.0s · qwen3.6:35b-a3b 13.8s · llama3.3:70b 153.7s`.
Journal `llama runner started in` values tonight: 2.59–15.65 s (runner only, excluding the first token).
Memory held ≈ on-disk size + KV cache. Measured once: gemma2:27b is 15.6 GB on disk and 19.30 GB resident at ctx 8192. With MAX_LOADED=1, at most one model is held at a time.

| option | touches | lanes affected | restart needed | risk |
|---|---|---|---|---|
| **(a) Finite host `OLLAMA_KEEP_ALIVE`** (e.g. `5m` / `30m` / `2h` instead of `-1`) | `/etc/systemd/system/ollama.service.d/override.conf` (sudo, etckeeper commit). Also `dgx-infra/scripts/morning_digest.sh:118` `[OLLAMA_KEEP_ALIVE]=-1` (perf-tier WARN) and the primer's pinned `Environment=` line | only lanes that **don't** send keep_alive: hello-ai-v2, lists, operativereports, surgical-cv, hello-ai v1. evidence-engine (spec −1), inference-determinism (−1), llm-council (10m) and the health check (0) override per request (**inference** from Ollama's per-request precedence) | yes. One Ollama restart, which drops in-flight requests. Pick a quiet window | 5m: frequent cold loads for intermittent apps (surgical-cv worker every 5 min → qwen3:32b ~23 s per load; hello-ai-v2 interactive). 30m/2h: memory freed only after idling; at most ~20 GB held (42.5 GB+ if 70B). −1 (today): a stale model is held forever (gemma2:27b now) |
| **(b) Each lane sets keep_alive per request** (as llm-council does) | each lane's repo + a redeploy of its unit (system units need sudo restart of that app, not Ollama) | per lane chosen. Candidates: hello-ai-v2, lists, operativereports, surgical-cv | the lane's own unit only. **No Ollama restart** | relies on every future lane remembering. Values still conflict across lanes (the last request's keep_alive wins for that model, **inference**). evidence-engine's default −1 is pinned into its manifest (`review_spec.py:675`), so changing it is a pin-affecting decision for that lane |
| **(c) Replace restarts with per-model unload** (`POST /api/generate {"model": X, "keep_alive": 0}`) in evidence-engine's `restart_ollama` / `_restart_ollama_and_retry` | evidence-engine code. Removes the sudo dependency | evidence-engine. Indirectly everyone, since in-flight requests survive an unload of a different model | none | **Shared-name caveat:** with one slot, the resident model may belong to another lane (`qwen3:32b` is shared with surgical-cv), so "unload my model" can unload theirs mid-use. And evidence-engine's premise "a simple model unload/reload is not sufficient" (`extractor.py:747-748`) to clear CUDA fragmentation is **unmeasured here**. The proactive restart's need is itself an open question (option e4) |
| **(d) Narrow or remove `/etc/sudoers.d/ollama-restart`** | `/etc/sudoers.d` (sudo, `visudo -c`, etckeeper; the file content must be read by root first) | evidence-engine callers in §6 (1–3). Operators get a password prompt | none for Ollama | removing it turns proactive/last-resort restarts into logged failures. Proactive ones are already caught; last-resort ones become a TimeoutError a retry earlier. Today it is still a standing capability for **any** process running as ankitsarin to drop every lane's requests without a terminal, with no tty in the log |
| **(e1) Health cron model sweep** | `dgx-infra/scripts/ollama_health_check.sh` (repo + push) | all lanes. Daily 07:00–07:07 it evicts whatever is resident and loads all 20 models incl. 70B | none | sweeping all models is what proves each loads. A narrower check (resident model only, or GET-only, or a model allowlist) proves less. It honours the experiment flock but not other lanes' interactive use |
| **(e2) Raise `OLLAMA_MAX_LOADED_MODELS`** (e.g. 2) | override.conf (sudo) | all, most of all llm-council (5-model fan-out) and the shared-name pairs | yes (Ollama restart) | doubles worst-case held memory toward ~60 GB (70B + 32B). earlyoom `--prefer '^ollama$'` at 6% makes ollama the victim under pressure |
| **(e3) Agree `num_ctx` per model across lanes** | lane repos | hello-ai-v2 (32768), health check (4096), others default | per lane | reduces same-model reloads (**inference**). May cost quality or headroom where a lane needs long context |
| **(e4) Set evidence-engine `RESTART_EVERY_N` to 0 by default** | evidence-engine (pin-affecting for its runs) | evidence-engine and every lane it would interrupt | none | if CUDA fragmentation is real, long extraction runs may degrade. It is currently suppressed only while another process holds the flock, and most lanes (llm-council, hello-ai-v2, lists, operativereports, surgical-cv) **never take the flock**, so their in-flight requests are unprotected |
| **(e5) earlyoom victim preference** | `/etc/default/earlyoom` (sudo) | all Ollama lanes | earlyoom only | `--prefer ollama` sacrifices the shared server first. The alternative is that a lane's own process dies first. It has never fired (§5) |
| **(e6) Attribution retention** | `/etc/logrotate.d/rsyslog` or journald (sudo) | none functionally | none | auth.log* keeps ~4.5 months, so 288 pre-July restarts are unattributable. sudo lines carry no PID/parent, so even within coverage, non-TTY callers are attributable only by PWD |

---

## Expectation verdicts

- **E1: PARTLY CONFIRMED, cause mismatched.** Premise correction first: the "75 without a TTY" are 75 sudo **lines**. 72 of them are restarts and 3 are refused non-restart commands (`is-active`, two `cat`s). The 75 **restarts** split 72 no-TTY + 3 TTY. 74/75 restarts came from evidence-engine's PWD (71 of the 72 no-TTY ones; the other was inference-determinism). But the dominant cause was **not** pipeline stages: 51 (09:03) + 3 were the **offline test suite** leaking the last-resort restart (`conftest.py:3-9`, fixed `184d857`, 2026-08-23). 17 are unattributed (consistent with proactive RESTART_EVERY_N=25). 1 was from inference-determinism's PWD. None since 2026-08-31.
- **E2: MISMATCH.** surgical-cv runs **no PyTorch**. Its GPU use outside Ollama is ffmpeg NVDEC/NVENC (`preview_runner.py:98-104`). No repo imports torch in tracked production code. sentence-transformers (operativereports, dgx-infra tool) has no device set. At audit time nvidia-smi shows only Xorg, gnome-shell and ollama.
- **E3: CONFIRMED.** One model (`gemma2:27b`, 19.30 GB, ctx 8192), `expires_at 2319-02-25T21:55:22Z`. Loaded 22:07:50 by the pre-LCH-1 llm-council (inference, §0).
- **E4: MISMATCH.** Besides llm-council (`"10m"`): evidence-engine sends keep_alive per request (default `-1`, `review_spec.py:675`), inference-determinism sends `-1` (`probes/common.py:46`), and the dgx-infra health check sends `0` (`ollama_check_one.py:40`). Lanes that don't set it: hello-ai-v2, lists, operativereports, surgical-cv, hello-ai v1.
- **E5: CONFIRMED.** 0 kill lines in the retained earlyoom journal (since its 2026-10-18 install).

## Gates

- **G1: PASS.** End: `MainPID=2564 NRestarts=0 ActiveEnterTimestamp=Sat 2026-10-17 23:04:05 UTC` (`s_g1_end.txt`, 23:27:10Z), the same as step 0. `journalctl -u ollama --since 23:18:16 | grep -E 'Stopping|Started|POST'` → no output.
- **G2: PASS.** Exactly one command touched port 11434:
  `for e in version ps tags; do curl -s -X GET --max-time 10 http://localhost:11434/api/$e …; done` (23:18:16Z).
  Ollama's own log for the audit window: `GET "/api/version"`, `GET "/api/ps"`, `GET "/api/tags"`, all at 23:18:16, and nothing else.
  (`systemctl show/cat ollama` and `journalctl` read systemd, not the server.)
- **G3: PASS.** `llm-council/council.py:94: keep_alive="10m",` and `:167`, found by the section-2 search.
- **G4: PASS, with disclosures.** Files written, all under `~/scratch/ollama-OLR-1/`: `t_start.txt t_end.txt s0_show_start.txt s0_cat.txt s0_api_{version,ps,tags}.json ollama_requests.log s1_sudo_ollama_raw.txt s1_restart_lines.txt s1_journal_lifecycle.txt s1_journal_failures.txt s1_boots.txt s1_start_classification.txt classify.py s2_repos.txt s2_grep_all.txt s2_restart_code.txt s2_restart_defs.txt s2_schedules.txt s3_client_files.txt s3_units.txt s4_gpu_mem.txt s5_earlyoom_cfg.txt s5_earlyoom_journal.txt s5_earlyoom_kills.txt s5_kernel_oom.txt s6_sudoers_meta.txt s_g1_end.txt REPORT.md`.
  Disclosures: (1) The session-startup `cd ~/claude-config && git pull --ff-only` ran (CLAUDE.md startup protocol, exempt under the auto-mode rule). It printed "Already up to date", so nothing was written. (2) Claude Code's harness wrote one background-task output file under its own `/tmp/claude-1000/…/tasks/`, for a journal grep that outlived the 120 s foreground limit. (3) Reads outside repos: `~/logs/ollama_health_2026111{4,5}.log` (dgx-infra cron output, host log), `/proc/<pid>/{exe,cwd}` for 7 PIDs, one `ls` of `hello-ai-v2/backend/` (a code directory). (4) The grep ran over **tracked** files only. In evidence-engine that includes a tracked `data/surgical_autonomy/extraction_codebook.yaml`. Its name appeared in a `git ls-files` listing and it was pattern-scanned, but no line of it surfaced and it was not opened. No data, corpus, video, upload, output or session folder was opened. No `.env` or `tunnel.env` was read.

## Decisions for the PI

1. Whether the host default `OLLAMA_KEEP_ALIVE=-1` stays or becomes finite, and if finite, which value, given that it only governs the four apps that send no keep_alive.
2. Whether every lane must send its own keep_alive per request, and whether evidence-engine's pinned default of `-1` is part of that rule.
3. Whether evidence-engine may keep restarting the shared server at all (proactive every 25 papers and on read timeout), or must switch to an unload or to no restart, given that most lanes never take the experiment flock that is its only guard.
4. Whether `/etc/sudoers.d/ollama-restart` is kept, narrowed or removed once decision 3 is made.
5. Whether the 07:00 health check keeps loading all 20 models daily, given that each load evicts whatever another lane has resident.
6. Whether `OLLAMA_MAX_LOADED_MODELS=1` stays, given llm-council's five-model fan-out and the two model names shared across lanes.
7. Whether earlyoom should keep preferring `ollama` as its first victim on a server every lane shares.

STOP-OLR1-READ
