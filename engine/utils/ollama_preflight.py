"""Ollama pre-flight health check — verify models are loaded and responsive.

Sends a minimal completion to each model before committing to a multi-hour
batch run. Reports load time, VRAM usage, and failures.
"""

import argparse
import logging
import time
from dataclasses import dataclass, field

import ollama

from engine.utils.ollama_client import ollama_chat

logger = logging.getLogger(__name__)

_VRAM_BUDGET_GB = 100.0  # usable VRAM on DGX Spark


@dataclass
class ModelResult:
    model: str
    status: str  # "ok" or "error"
    load_time_seconds: float = 0.0
    vram_used_gb: float = 0.0
    error_message: str = ""


@dataclass
class PreflightResult:
    success: bool
    models: list[ModelResult] = field(default_factory=list)
    total_vram_gb: float = 0.0
    error_summary: str = ""


def _get_model_vram_gb(model_name: str) -> float:
    """Query ollama ps for VRAM usage of a specific model."""
    try:
        ps = ollama.ps()
        for m in ps.get("models", []):
            if m.get("name", "").startswith(model_name.split(":")[0]):
                size_bytes = m.get("size", 0)
                return size_bytes / (1024**3)
    except Exception:
        pass
    return 0.0


def check_model(model_name: str, timeout: int = 30) -> ModelResult:
    """Send a minimal completion to verify model loads and responds."""
    start = time.time()
    try:
        ollama_chat(
            model=model_name,
            messages=[{"role": "user", "content": "Respond with OK"}],
            options={"temperature": 0, "num_predict": 4},
            max_retries=0,
            wall_timeout=60.0,
        )
        elapsed = time.time() - start
        vram = _get_model_vram_gb(model_name)
        return ModelResult(
            model=model_name, status="ok",
            load_time_seconds=round(elapsed, 1), vram_used_gb=round(vram, 1),
        )
    except Exception as exc:
        elapsed = time.time() - start
        return ModelResult(
            model=model_name, status="error",
            load_time_seconds=round(elapsed, 1),
            error_message=str(exc),
        )


def preflight_check(models: list[str], timeout: int = 30) -> PreflightResult:
    """Check all models and return aggregate result."""
    results = []
    for name in models:
        logger.info("Checking model: %s", name)
        result = check_model(name, timeout=timeout)
        if result.status == "ok":
            logger.info(
                "  %s: OK (%.1fs, %.1f GB VRAM)", name,
                result.load_time_seconds, result.vram_used_gb,
            )
        else:
            logger.error("  %s: FAILED — %s", name, result.error_message)
        results.append(result)

    # Query total VRAM from ollama ps
    total_vram = 0.0
    try:
        ps = ollama.ps()
        for m in ps.get("models", []):
            total_vram += m.get("size", 0) / (1024**3)
    except Exception:
        total_vram = sum(r.vram_used_gb for r in results)
    total_vram = round(total_vram, 1)

    failures = [r for r in results if r.status == "error"]
    success = len(failures) == 0
    error_summary = ""
    if failures:
        error_summary = "; ".join(
            f"{r.model}: {r.error_message}" for r in failures
        )
    if total_vram > _VRAM_BUDGET_GB:
        error_summary += (
            f" Total VRAM {total_vram} GB exceeds {_VRAM_BUDGET_GB} GB budget."
        )
        success = False

    return PreflightResult(
        success=success, models=results,
        total_vram_gb=total_vram, error_summary=error_summary,
    )


def require_preflight(models: list[str], runner_name: str, timeout: int = 30) -> None:
    """Run preflight check; abort with clear message on failure.

    Call this at the top of batch runners before the main loop.
    """
    result = preflight_check(models, timeout=timeout)
    if not result.success:
        msg = (
            f"{runner_name} pre-flight check failed: {result.error_summary}. "
            f"Aborting — fix model availability before retrying."
        )
        logger.error(msg)
        raise RuntimeError(msg)
    logger.info(
        "Pre-flight OK: %d models loaded, %.1f GB VRAM total",
        len(result.models), result.total_vram_gb,
    )


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Ollama model pre-flight health check")
    parser.add_argument("--models", nargs="+", required=True, help="Model names to check")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout per model (seconds)")
    args = parser.parse_args()

    result = preflight_check(args.models, timeout=args.timeout)

    print(f"\n{'=' * 50}")
    print("OLLAMA PRE-FLIGHT RESULTS")
    print(f"{'=' * 50}")
    for m in result.models:
        icon = "OK" if m.status == "ok" else "FAIL"
        line = f"  [{icon}] {m.model:25s} {m.load_time_seconds:>5.1f}s"
        if m.vram_used_gb:
            line += f"  {m.vram_used_gb:>5.1f} GB"
        if m.error_message:
            line += f"  — {m.error_message}"
        print(line)

    print(f"\n  Total VRAM: {result.total_vram_gb:.1f} / {_VRAM_BUDGET_GB:.0f} GB")
    print(f"  Status: {'PASS' if result.success else 'FAIL'}")

    if not result.success:
        print(f"\n  {result.error_summary}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
