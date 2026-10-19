"""12r-U13-PROBE — send one rendered Pass-1 user turn to deepseek-r1:32b through the engine's own client,
outside any run, under the experiment lock; record the response's shape. Writes only under this
script's out/ directory; opens no database (the spec and codebook are read as files).
usage, from the evidence-engine repository root:
    probe.py --prompt <user_turn.txt> --label <label> [--expect-hash <sha256>] [--dry-run]
  --dry-run       compute the request hash the call would send and stop; no lock, no call.
  --expect-hash   refuse to send (exit 3) unless the would-send hash equals this value.
`probe_one(...)` is the same thing as a function, for run_phase2.py, which holds the lock across calls
(hold_experiment_lock is re-entrant: the nested acquire here takes no second flock).
The messages are HEAD's pass1_messages(prompt) (system turn + the user turn as given); model and options
are stage_config("elicitation_pass1", spec) from the ra_12q spec, the kwargs the run sent; the hash is
run_manifest.request_hash over {"model", "messages", **kwargs minus ollama_chat's consumed names}, the
value ollama_chat computes. Record (out/<label>.json): label, UTC start/end, request hash, done_reason,
eval_count and prompt_eval_count if the response carries them, response length, whether the JSON
between the first "{" and the last "}" parses, the field names present by codebook class, and the raw
response's sha256 (the raw text goes to out/raw_<sha256>.txt)."""
import argparse, hashlib, inspect, json, os, sys
from datetime import datetime, timezone

RA = os.path.expanduser("~/scratch/12r-u13/run/ra_12q")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
sys.path.insert(0, os.getcwd())
from engine.core.codebook import load_codebook
from engine.core.effective_config import stage_config
from engine.core.review_spec import load_review_spec
from engine.core.run_manifest import request_hash
from engine.elicitation import contracts as K
from engine.elicitation.pipeline import pass1_messages
from engine.utils.ollama_client import ollama_chat
from engine.utils.ollama_lock import hold_experiment_lock

SPEC = load_review_spec(os.path.join(RA, "spec.yaml"))
CB = load_codebook(os.path.join(RA, "extraction_codebook.yaml")).raw
CLASS = {f["name"]: f["field_class"] for f in CB["fields"]}
KW = stage_config("elicitation_pass1", SPEC).kwargs()
CONSUMED = {n for n, p in inspect.signature(ollama_chat).parameters.items()
            if p.kind is not p.VAR_KEYWORD and n not in ("model", "messages")}


def would_send(prompt: str) -> tuple[list[dict], str]:
    messages = pass1_messages(prompt)
    sent = {"model": KW["model"], "messages": messages,
            **{k: v for k, v in KW.items() if k not in CONSUMED and k != "model"}}
    return messages, request_hash(sent)


def probe_one(prompt_path: str, label: str, expect_hash: str | None = None) -> dict:
    prompt = open(prompt_path, encoding="utf-8").read()
    messages, h = would_send(prompt)
    if expect_hash and h != expect_hash:
        raise SystemExit(f"{label}: would-send hash {h} != expected {expect_hash}; not sent")
    os.makedirs(OUT, exist_ok=True)
    rec = {"label": label, "request_hash": h, "expect_hash": expect_hash, "prompt_file": prompt_path,
           "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest()}
    with hold_experiment_lock():
        rec["started_at"] = datetime.now(timezone.utc).isoformat()
        try:
            resp = ollama_chat(messages=messages, **KW)
        except BaseException as exc:      # record the refusal/timeout, then re-raise
            rec.update(ended_at=datetime.now(timezone.utc).isoformat(), error=f"{type(exc).__name__}: {exc}")
            json.dump(rec, open(os.path.join(OUT, f"{label}.json"), "w"), indent=1)
            raise
    raw = resp.message.content or ""
    rh = hashlib.sha256(raw.encode()).hexdigest()
    open(os.path.join(OUT, f"raw_{rh}.txt"), "w").write(raw)
    names = [str(e.get(K.KEY_FIELD)) for e in K.parse_container(raw)[0] if e.get(K.KEY_FIELD)]
    try:
        json.loads(raw[raw.index("{"):raw.rindex("}") + 1]); parses = True
    except (ValueError, json.JSONDecodeError):
        parses = False
    present = {c: sum(1 for n in names if CLASS.get(n) == c) for c in ("stated", "inferable", "judgment")}
    rec.update(ended_at=datetime.now(timezone.utc).isoformat(),
               done_reason=getattr(resp, "done_reason", None), eval_count=getattr(resp, "eval_count", None),
               prompt_eval_count=getattr(resp, "prompt_eval_count", None),
               load_duration_ns=getattr(resp, "load_duration", None),
               thinking_chars=len(getattr(resp.message, "thinking", None) or ""),
               raw_chars=len(raw), raw_sha256=rh, json_parses=parses, names_present=names,
               present_by_class=present,
               expected_by_class={c: sum(1 for v in CLASS.values() if v == c) for c in ("stated", "inferable", "judgment")},
               stated_only=present["stated"] >= 8 and present["inferable"] == 0 and present["judgment"] == 0)
    json.dump(rec, open(os.path.join(OUT, f"{label}.json"), "w"), indent=1)
    return rec


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--prompt", required=True); ap.add_argument("--label", required=True)
    ap.add_argument("--expect-hash"); ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    _, h = would_send(open(a.prompt, encoding="utf-8").read())
    print(json.dumps({"label": a.label, "would_send_request_hash": h, "expect_hash": a.expect_hash,
                      "equal": (h == a.expect_hash) if a.expect_hash else None, "kwargs": KW}, default=str))
    if a.expect_hash and h != a.expect_hash:
        sys.exit(3)
    if not a.dry_run:
        print(json.dumps(probe_one(a.prompt, a.label, a.expect_hash)))
