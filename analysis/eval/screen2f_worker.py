"""SCREEN-AUTH-01 Phase 2f smoke — one arm, one phase, one engine tree.

Run by `run_screen2f` as a separate process, never imported by it:

    python -P analysis/eval/screen2f_worker.py --arm A --root <tree> --spec <yaml> ...

with PYTHONPATH set to the arm's tree root. `-P` stops the interpreter putting
this file's directory first on sys.path, so `engine` can only come from
PYTHONPATH; the worker then asserts `engine.__file__` lies inside --root and
refuses otherwise. Arm A's tree is a worktree of 83defc5 — this is how it never
imports HEAD's package.

Every call goes through the arm's own `screen_paper`, exactly as the runner
calls it. The only interposition is a pass-through wrapper on the name
`ollama_chat` inside that module: it records the request hash, the call
kwargs, the raw response and its timing, then returns the real response
unchanged. The client's own retries happen inside the real call and are logged
from the client's warnings; the harness adds none.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from unittest import mock

HARNESS_DIR = Path(__file__).resolve().parent
RESTART_OPT_OUT_ENV = "EVIDENCE_ENGINE_NO_OLLAMA_RESTART"

EXIT_DONE, EXIT_PAUSED, EXIT_WRONG_TREE, EXIT_PRECONDITION = 0, 3, 4, 5


def _lib():
    # Appended, not prepended: this directory holds no `engine`, and the arm's
    # tree must win every other name too.
    if str(HARNESS_DIR) not in sys.path:
        sys.path.append(str(HARNESS_DIR))
    import screen2f

    return screen2f


class _WarningTap(logging.Handler):
    """Copies the client's WARNING records (timeouts, retries) into the current call."""

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.sink: list | None = None

    def emit(self, record):
        if self.sink is not None:
            self.sink.append({"levelname": record.levelname, "msg": record.getMessage()})


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--arm", required=True)
    ap.add_argument("--root", required=True, type=Path)
    ap.add_argument("--spec", required=True, type=Path)
    ap.add_argument("--phase", required=True, choices=["identity", "primary", "verifier"])
    ap.add_argument("--papers", type=Path)
    ap.add_argument("--ids", default="")
    ap.add_argument("--forced-verifier-ids", default="")
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--lead-minutes", type=int, default=None)
    args = ap.parse_args(argv)

    lib = _lib()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)
    log = logging.getLogger(f"screen2f.worker.{args.arm}").info
    os.environ[RESTART_OPT_OUT_ENV] = "1"

    root = args.root.resolve()
    import engine  # noqa: E402 — resolved from PYTHONPATH only (-P)

    engine_file = Path(engine.__file__).resolve()
    if not engine_file.is_relative_to(root):
        print(f"REFUSED: engine imported from {engine_file}, expected inside {root}", file=sys.stderr)
        return EXIT_WRONG_TREE

    from pydantic import ValidationError

    from engine.agents import screener
    from engine.core.review_spec import load_review_spec

    spec = load_review_spec(args.spec)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.phase == "identity":
        return _identity(lib, screener, spec, args, engine_file)

    tap = _WarningTap()
    logging.getLogger("engine.utils.ollama_client").addHandler(tap)
    real_chat = screener.ollama_chat

    def screen(paper: dict, pass_number: int, role: str) -> dict:
        model = spec.screening_models.primary if role == lib.PRIMARY else spec.screening_models.verification
        call = {"role": role, "pass_number": pass_number, "model": model,
                "started_utc": lib.utc_iso(), "client_warnings": []}
        tap.sink = call["client_warnings"]

        def passthrough(**kw):
            call["request_hash"] = lib.request_hash(kw.get("format"), kw.get("messages"))
            call["call_kwargs"] = {k: v for k, v in kw.items() if k not in ("messages", "format")}
            t0 = time.monotonic()
            resp = real_chat(**kw)
            call["client_wall_s"] = round(time.monotonic() - t0, 3)
            call["raw"] = resp.message.content
            call["metrics"] = {k: getattr(resp, k, None) for k in (
                "total_duration", "load_duration", "prompt_eval_count", "prompt_eval_duration",
                "eval_count", "eval_duration", "done_reason")}
            return resp

        out = {"decision": None, "rationale": None, "confidence": None, "parse_error": None, "error": None}
        t0 = time.monotonic()
        try:
            with mock.patch.object(screener, "ollama_chat", passthrough):
                if role == lib.PRIMARY:
                    d = screener.screen_paper(paper, spec, pass_number=pass_number, model=model)
                else:
                    d = screener.screen_paper(paper, spec, pass_number=1, model=model, role="verifier")
            out.update(decision=d.decision, rationale=d.rationale, confidence=d.confidence)
        except (json.JSONDecodeError, ValidationError) as exc:
            out["parse_error"] = f"{type(exc).__name__}: {exc}"[:1000]
        except Exception as exc:  # timeout after the client's retries, connection loss
            out["error"] = f"{type(exc).__name__}: {exc}"[:1000]
        finally:
            tap.sink = None
        call["wall_s"] = round(time.monotonic() - t0, 3)
        call["ended_utc"] = lib.utc_iso()
        return {**out, "call": call}

    lead = lib.DEFAULT_LEAD_MINUTES if args.lead_minutes is None else args.lead_minutes

    def blocked() -> bool:
        return lib.in_quiet_window(lib.now_utc(), lead)

    ids = [int(x) for x in args.ids.split(",") if x.strip()] or None
    papers = lib.load_papers(args.papers, ids)
    meta = {"arm": args.arm, "engine_file": str(engine_file), "spec": str(args.spec.resolve()),
            "screening_hash": spec.screening_hash()}

    if args.phase == "primary":
        status = lib.run_primary(papers, lib.Checkpoint(args.out_dir / "primary.jsonl"), screen,
                                 blocked=blocked, log=log, meta=meta)
    else:
        forced = [int(x) for x in args.forced_verifier_ids.split(",") if x.strip()]
        primary_results, _ = lib.Checkpoint(args.out_dir / "primary.jsonl").load()
        try:
            targets = lib.verifier_targets(papers, primary_results, forced)
        except ValueError as exc:
            print(f"REFUSED: {exc}", file=sys.stderr)
            return EXIT_PRECONDITION
        status = lib.run_verifier(targets, lib.Checkpoint(args.out_dir / "verifier.jsonl"), screen,
                                  blocked=blocked, log=log, meta=meta)
    log(f"phase {args.phase} status {status}")
    return EXIT_PAUSED if status == "paused" else EXIT_DONE


def _identity(lib, screener, spec, args, engine_file: Path) -> int:
    """Capture the placeholder-paper request for both roles. No model is called."""

    class _Captured(Exception):
        pass

    result = {"arm": args.arm, "engine_file": str(engine_file), "spec": str(args.spec.resolve()),
              "spec_sha256": lib.sha256_file(args.spec), "screening_hash": spec.screening_hash(),
              "models": {"primary": spec.screening_models.primary,
                         "verification": spec.screening_models.verification}}
    for role in (lib.PRIMARY, lib.VERIFIER):
        box: dict = {}

        def recorder(**kw):
            box.update(kw)
            raise _Captured()

        with mock.patch.object(screener, "ollama_chat", recorder):
            try:
                if role == lib.PRIMARY:
                    screener.screen_paper(dict(lib.PLACEHOLDER_PAPER), spec, 1)
                else:
                    screener.screen_paper(dict(lib.PLACEHOLDER_PAPER), spec, 2, role="verifier")
            except _Captured:
                pass
        result[role] = {
            "hash": lib.request_hash(box["format"], box["messages"]),
            "messages": box["messages"],
            "format": box["format"],
            # model is omitted: the R1/R2 captures call without one, and the model
            # per role is recorded above from the spec.
            "call_kwargs": {k: v for k, v in box.items() if k not in ("messages", "format", "model")},
        }
    (args.out_dir / "identity.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True) + "\n")
    print(json.dumps({"arm": args.arm, "primary": result["primary"]["hash"],
                      "verifier": result["verifier"]["hash"], "screening_hash": result["screening_hash"]}))
    return EXIT_DONE


if __name__ == "__main__":
    sys.exit(main())
