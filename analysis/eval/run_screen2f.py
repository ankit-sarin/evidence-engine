"""SCREEN-AUTH-01 Phase 2f smoke runner: 3 arms x the abstract-stage pipeline.

Phase-batched (ruling R1): every arm's primary phase on qwen3:8b, then every
arm's verifier phase on gemma3:27b — two model swaps, not one per paper. Each
arm-phase is a separate `screen2f_worker.py` process rooted in that arm's tree.

Which three arms run is an ARM SET (--arm-set, screen2f.ARM_SETS): "2f" as the
smoke ran, "2g" for the exclusion-basis re-smoke (A, B, D).

Rails, in order:
  * every arm's tree is measured and judged by its declared rule — a worktree at
    its commit and clean, or the repo tree at that commit with no uncommitted
    change under engine/ and review_specs/;
  * the experiment flock is taken non-blocking and held for the whole run, so
    the 07:00 health check stands down and no foreign restart path fires;
  * every worker runs with the restart opt-out set;
  * gate 1: the placeholder requests of all three arms are checked before any
    model call (ruling R4); a failure stops the run;
  * no paper starts inside 06:30–09:30 UTC (ruling R5). A paused worker is
    relaunched only after 09:30, once no pytest process is running and the
    server holds only a model this run loads, or nothing — a bounded wait with
    its own timeout outcome;
  * an optional --watch-file is stat'd before and after every phase, and a
    moved size or mtime stops the run.

Writes only under --out-dir, plus the --background log.

Usage:
    PYTHONPATH=. python -m analysis.eval.run_screen2f \\
        --out-dir docs/session-reports/screen-auth-2f-smoke/preflight \\
        --arm-set 2g --arm-root A=~/worktrees/ee-83defc5 --arm-root B=~/worktrees/ee-61326fa \\
        --ids 290,57,222 --forced-verifier-ids 222 \\
        --watch-file <path> [--background]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx

from analysis.eval import screen2f as lib
from engine.core.review_paths import spec_path_for
from engine.utils.background import maybe_background
from engine.utils.ollama_lock import hold_experiment_lock

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKER = Path(__file__).resolve().parent / "screen2f_worker.py"
DEFAULT_PAPERS = REPO_ROOT / "docs/session-reports/screen-auth-2f-smoke/papers_86.jsonl"
REVIEW = "surgical_autonomy"
#: Each arm's tree rule and expected placeholder hashes live in screen2f.ARM_SETS;
#: this module measures the git facts and lets lib.tree_problem judge them.
OLLAMA = "http://localhost:11434"
HARNESS_MODELS = {"qwen3:8b", "gemma3:27b"}
POLL_SECONDS = 60

logger = logging.getLogger("screen2f")


class Abort(RuntimeError):
    pass


def _git(root: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True, text=True).stdout.strip()


def ollama_ps() -> list[dict]:
    models = httpx.get(f"{OLLAMA}/api/ps", timeout=10).json().get("models", [])
    return [{"name": m.get("name"), "size": m.get("size"), "size_vram": m.get("size_vram"),
             "expires_at": m.get("expires_at"), "context_length": m.get("context_length")} for m in models]


def pytest_running() -> list[str]:
    """Command lines of pytest processes other than this one."""
    hits = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit() or int(proc.name) == os.getpid():
            continue
        try:
            argv = (proc / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        args = [a.decode(errors="replace") for a in argv if a]
        names = [Path(a).name for a in args]
        if "pytest" in names or "py.test" in names or any(
            args[i] == "-m" and i + 1 < len(args) and args[i + 1] == "pytest" for i in range(len(args))
        ):
            hits.append(" ".join(args)[:200])
    return hits


def stat_file(path: Path | None) -> dict | None:
    if path is None:
        return None
    st = path.stat()
    return {"size": st.st_size, "mtime_ns": st.st_mtime_ns}


def wait_for_resume(max_wait_min: int) -> None:
    """Block until the quiet window has ended and R5's resume conditions hold."""
    deadline = time.monotonic() + max_wait_min * 60
    while True:
        now = lib.now_utc()
        in_window = lib.in_quiet_window(now, lead_minutes=0)
        tests = pytest_running()
        loaded = {m["name"] for m in ollama_ps()}
        foreign = sorted(loaded - HARNESS_MODELS)
        if not in_window and not tests and not foreign:
            logger.info("RESUME conditions met at %s: outside window, no pytest, loaded=%s",
                        lib.utc_iso(now), sorted(loaded))
            return
        if time.monotonic() > deadline:
            raise Abort(f"RESUME_TIMEOUT after {max_wait_min} min: in_window={in_window} "
                        f"pytest={tests} foreign_models={foreign}")
        logger.info("waiting to resume: in_window=%s pytest=%d foreign_models=%s",
                    in_window, len(tests), foreign)
        time.sleep(POLL_SECONDS)


def run_worker(arm: lib.Arm, phase: str, out_dir: Path, args) -> int:
    arm_dir = out_dir / f"arm_{arm.name}"
    arm_dir.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "-P", str(WORKER), "--arm", arm.name, "--root", str(arm.root),
           "--spec", str(arm.spec), "--phase", phase, "--out-dir", str(arm_dir)]
    if phase != "identity":
        cmd += ["--papers", str(args.papers.resolve()), "--ids", args.ids,
                "--lead-minutes", str(args.lead_minutes)]
    if phase == "verifier":
        cmd += ["--forced-verifier-ids", args.forced_verifier_ids]
    env = {**os.environ, "PYTHONPATH": str(arm.root), "PYTHONUNBUFFERED": "1",
           "EVIDENCE_ENGINE_NO_OLLAMA_RESTART": "1"}
    with open(arm_dir / f"worker_{phase}.log", "a", encoding="utf-8") as logf:
        logf.write(f"==== {lib.utc_iso()} {' '.join(cmd)}\n")
        logf.flush()
        return subprocess.run(cmd, cwd=str(arm.root), env=env, stdout=logf, stderr=subprocess.STDOUT).returncode


def main(argv: list[str] | None = None) -> int:
    maybe_background("screen2f", review_name=REVIEW)
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--arm-set", default="2f", choices=sorted(lib.ARM_SETS))
    ap.add_argument("--arm-root", action="append", default=[], metavar="NAME=PATH",
                    help="worktree path for a worktree-sourced arm; repeatable")
    ap.add_argument("--arm-a-root", type=Path, help="alias for --arm-root A=PATH")
    ap.add_argument("--papers", type=Path, default=DEFAULT_PAPERS)
    ap.add_argument("--ids", default="", help="comma-separated paper ids; empty = all")
    ap.add_argument("--forced-verifier-ids", default="", help="pre-flight only")
    ap.add_argument("--watch-file", type=Path, default=None)
    ap.add_argument("--lead-minutes", type=int, default=lib.DEFAULT_LEAD_MINUTES)
    ap.add_argument("--max-resume-wait-min", type=int, default=240)
    ap.add_argument("--phases", default="primary,verifier")
    args = ap.parse_args(argv)

    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(out_dir / "run_log.txt", encoding="utf-8")
    fmt = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
    logging.Formatter.converter = time.gmtime
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.setLevel(logging.INFO)

    summary: dict = {"argv": sys.argv, "started_utc": lib.utc_iso(), "events": []}
    status = "FAILED"
    try:
        status = _run(args, out_dir, summary)
    except Abort as exc:
        logger.error("ABORT: %s", exc)
        summary["abort"] = str(exc)
        status = "ABORTED"
    except Exception as exc:
        logger.exception("CRASH: %s", exc)
        summary["abort"] = f"{type(exc).__name__}: {exc}"
        status = "CRASHED"
    finally:
        summary["ended_utc"] = lib.utc_iso()
        summary["status"] = status
        (out_dir / "run_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n")
        (out_dir / "DONE").write_text(status + "\n")
        logger.info("run status %s", status)
    return 0 if status == "COMPLETE" else 1


def _run(args, out_dir: Path, summary: dict) -> str:
    def event(kind: str, **kw):
        rec = {"at": lib.utc_iso(), "event": kind, **kw}
        summary["events"].append(rec)
        logger.info("%s %s", kind, json.dumps(kw, ensure_ascii=False))

    # Trees: every arm's tree is measured, then judged by lib.tree_problem.
    specs = lib.arm_specs(args.arm_set)
    roots = {}
    for item in args.arm_root:
        name, _, path = item.partition("=")
        if not path:
            raise Abort(f"--arm-root expects NAME=PATH, got {item!r}")
        roots[name] = Path(path)
    if args.arm_a_root:
        roots.setdefault("A", args.arm_a_root)
    missing = [s.name for s in specs if s.source == "worktree" and s.name not in roots]
    if missing:
        raise Abort(f"arm set {args.arm_set!r} needs --arm-root for {missing}")

    spec_rel = spec_path_for(REVIEW)
    arm_c_spec = None
    if any(s.spec_kind == "arm_c_variant" for s in specs):
        arm_c_spec = out_dir / "armC_spec.yaml"
        arm_c_spec.write_text(lib.make_arm_c_spec_text((REPO_ROOT / spec_rel).read_text(encoding="utf-8")),
                              encoding="utf-8")
    arms = lib.arms(REPO_ROOT, spec_rel, roots, arm_c_spec, args.arm_set)

    trees = {}
    for spec in specs:
        root = arms[spec.name].root
        facts = {
            "head": _git(root, "rev-parse", "HEAD"),
            "commit_sha": _git(REPO_ROOT, "rev-parse", f"{spec.commit}^{{commit}}"),
            "dirty": bool(_git(root, "status", "--porcelain")),
            "inputs_differ_from_commit": bool(subprocess.run(
                ["git", "-C", str(root), "diff", "--quiet", spec.commit, "--", *lib.INPUT_PATHS]).returncode),
            "inputs_uncommitted": bool(subprocess.run(
                ["git", "-C", str(root), "diff", "--quiet", "HEAD", "--", *lib.INPUT_PATHS]).returncode),
        }
        problem = lib.tree_problem(spec, facts)
        if problem:
            raise Abort(problem)
        trees[spec.name] = {"root": str(root), "head": facts["head"], "commit": spec.commit,
                            "rule": spec.rule, "spec": str(arms[spec.name].spec)}
        if spec.spec_kind == "arm_c_variant":
            trees[spec.name]["spec_sha256"] = lib.sha256_file(arms[spec.name].spec)
    summary["arm_set"] = args.arm_set
    summary["trees"] = trees
    summary["papers"] = {"path": str(args.papers.resolve()), "sha256": lib.sha256_file(args.papers)}
    summary["ollama_version"] = httpx.get(f"{OLLAMA}/api/version", timeout=10).json().get("version")
    summary["watch_file"] = {"path": str(args.watch_file) if args.watch_file else None,
                             "before": stat_file(args.watch_file)}

    with hold_experiment_lock(blocking=False):
        event("flock_acquired")

        # Gate 1.
        identities = {}
        for name, arm in arms.items():
            rc = run_worker(arm, "identity", out_dir, args)
            if rc != 0:
                raise Abort(f"identity worker for arm {name} exited {rc}")
            identities[name] = json.loads((out_dir / f"arm_{name}" / "identity.json").read_text())
        gate = lib.check_identity(identities, args.arm_set)
        (out_dir / "identity_check.json").write_text(json.dumps(gate, indent=2, ensure_ascii=False) + "\n")
        summary["identity"] = {n: {"primary": i["primary"]["hash"], "verifier": i["verifier"]["hash"],
                                   "screening_hash": i["screening_hash"], "engine_file": i["engine_file"],
                                   "models": i["models"]} for n, i in identities.items()}
        event("identity_gate", ok=gate["ok"])
        if not gate["ok"]:
            raise Abort("gate 1 failed: " + json.dumps([c for c in gate["checks"] if not c["ok"]]))

        for phase in [p for p in args.phases.split(",") if p]:
            event("phase_start", phase=phase, ollama_ps=ollama_ps())
            for name, arm in arms.items():
                while True:
                    t0 = lib.utc_iso()
                    rc = run_worker(arm, phase, out_dir, args)
                    event("worker_exit", phase=phase, arm=name, rc=rc, started=t0)
                    if rc == 0:
                        break
                    if rc == 3:
                        event("paused", phase=phase, arm=name)
                        wait_for_resume(args.max_resume_wait_min)
                        event("resumed", phase=phase, arm=name, ollama_ps=ollama_ps())
                        continue
                    raise Abort(f"worker {name}/{phase} exited {rc}")
                after = stat_file(args.watch_file)
                if after != summary["watch_file"]["before"]:
                    raise Abort(f"WATCH_FILE_MOVED: {summary['watch_file']['before']} -> {after}")
            event("phase_end", phase=phase, ollama_ps=ollama_ps())

    event("flock_released")
    summary["watch_file"]["after"] = stat_file(args.watch_file)
    if summary["watch_file"]["after"] != summary["watch_file"]["before"]:
        raise Abort("WATCH_FILE_MOVED at close")
    return "COMPLETE"


if __name__ == "__main__":
    sys.exit(main())
