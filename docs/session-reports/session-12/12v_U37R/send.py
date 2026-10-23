"""12v-U37R step 3a — send the recorded order of attempt-2 requests, once each, under the experiment lock,
outside any run manifest (R668(c)): no review is written and no run_calls row is made (ollama_chat records
a call only inside `active_run`, and none is opened). The 12r-U13-PROBE form.
usage, from the repository root: send.py <requests_dir> <order.json> <out_dir>
Before each call: the request file's hash is recomputed (engine request_hash over the kwargs the client is
handed) and must equal its recorded hash; the call is sent with return_request_hash=True and the hash the
client computed must equal it too. After each call the raw content and thinking are written, named by
sha256, BEFORE anything else is done with them. Between calls, the stop conditions of the brief are read:
NRestarts unchanged, no earlyoom kill line since the start, /api/ps naming only the stage model. A stop
condition ends the sends after the current call (recorded as `stopped`). One call per request, no retry:
a failed call is recorded with its error. At the end the stage model is unloaded (keep_alive 0) and
/api/ps read, still under the lock."""
import hashlib, inspect, json, os, subprocess, sys, time
from datetime import datetime, timezone

sys.path.insert(0, os.getcwd())
import httpx
from engine.core.run_manifest import request_hash
from engine.utils.ollama_client import ollama_chat
from engine.utils.ollama_lock import hold_experiment_lock

REQ, ORDER, OUT = sys.argv[1:4]
CONSUMED = {n for n, p in inspect.signature(ollama_chat).parameters.items()
            if p.kind is not p.VAR_KEYWORD and n not in ("model", "messages")}
os.makedirs(os.path.join(OUT, "responses"), exist_ok=True)
now = lambda: datetime.now(timezone.utc).isoformat()


def sh(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True).stdout


def nrestarts():
    return sh("systemctl", "show", "ollama", "-p", "NRestarts").strip()


def earlyoom_kills(since):
    out = sh("journalctl", "-u", "earlyoom", "--since", since, "--no-pager")
    return sum(1 for l in out.splitlines() if "sending SIGTERM to" in l or "sending SIGKILL to" in l)


def ps():
    return httpx.get("http://127.0.0.1:11434/api/ps", timeout=10).json()


def save(text):
    h = hashlib.sha256(text.encode("utf-8")).hexdigest()
    p = os.path.join(OUT, "responses", f"{h}.txt")
    with open(p, "w", encoding="utf-8") as fh:
        fh.write(text)
    return h


order = json.load(open(ORDER))["order"]
log = {"started_at": now(), "nrestarts_start": nrestarts(), "calls": [], "stopped": None}
since = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
with hold_experiment_lock():
    log["lock_acquired_at"] = now(); log["lock_pid"] = os.getpid()
    log["ps_before"] = ps()
    for i, item in enumerate(order):
        f = json.load(open(os.path.join(REQ, item["case"], f"{item['arm']}.json")))
        req = dict(f["request"]); model = req["model"]; messages = req.pop("messages")
        kw = {k: v for k, v in req.items() if k != "model"}
        h = request_hash({"model": model, "messages": messages, **{k: v for k, v in kw.items() if k not in CONSUMED}})
        assert h == f["request_hash"], (item, h, f["request_hash"])
        rec = {"seq": i, **item, "request_hash": h, "started_at": now()}
        t0 = time.monotonic()
        try:
            resp, sent_hash = ollama_chat(paper_id=f["paper_id"], messages=messages, model=model,
                                          return_request_hash=True, **kw)
            rec["wall_s"] = round(time.monotonic() - t0, 3)
            rec["sent_hash_equal"] = sent_hash == h
            raw = resp.message.content or ""
            thinking = getattr(resp.message, "thinking", None) or ""
            rec.update(ended_at=now(), raw_sha256=save(raw), raw_chars=len(raw),
                       thinking_sha256=save(thinking), thinking_chars=len(thinking),
                       done_reason=getattr(resp, "done_reason", None), eval_count=getattr(resp, "eval_count", None),
                       prompt_eval_count=getattr(resp, "prompt_eval_count", None),
                       load_duration_ns=getattr(resp, "load_duration", None),
                       total_duration_ns=getattr(resp, "total_duration", None))
        except Exception as exc:
            rec.update(ended_at=now(), wall_s=round(time.monotonic() - t0, 3), error=f"{type(exc).__name__}: {exc}")
        p = ps(); rec["ps_after"] = [m.get("name") for m in p.get("models", [])]
        rec["nrestarts"] = nrestarts(); rec["earlyoom_kills_since_start"] = earlyoom_kills(since)
        log["calls"].append(rec)
        with open(os.path.join(OUT, "send_log.json"), "w") as fh:
            json.dump(log, fh, indent=1)
        foreign = [n for n in rec["ps_after"] if n != model]
        if rec["nrestarts"] != log["nrestarts_start"] or rec["earlyoom_kills_since_start"] or foreign:
            log["stopped"] = {"after_seq": i, "nrestarts": rec["nrestarts"], "earlyoom": rec["earlyoom_kills_since_start"],
                              "foreign_resident": foreign}
            break
    unload = httpx.post("http://127.0.0.1:11434/api/generate", json={"model": order[0]["model"], "keep_alive": 0}, timeout=120)
    log["unload"] = {"at": now(), "status": unload.status_code}
    time.sleep(5)
    log["ps_after_unload"] = ps(); log["ps_after_unload_at"] = now()
    log["lock_released_at"] = now()
log["nrestarts_end"] = nrestarts(); log["ended_at"] = now()
with open(os.path.join(OUT, "send_log.json"), "w") as fh:
    json.dump(log, fh, indent=1)
open(os.path.join(OUT, "send_done.txt"), "w").write(log["ended_at"] + "\n")
