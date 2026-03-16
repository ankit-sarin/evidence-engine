"""Periodic progress reporting for long extraction runs."""

import statistics
import sys
import time


def _fmt_duration(seconds: float) -> str:
    """Format seconds into a human-readable string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m{s:02d}s"
    h, remainder = divmod(int(seconds), 3600)
    m = remainder // 60
    return f"{h}h{m:02d}m"


class ProgressReporter:
    """Track and report progress during multi-paper extraction runs.

    Args:
        total_papers: Total number of papers to process.
        run_name: Label for this run (e.g., "Run 6 local", "Cloud o4-mini").
        file: Output stream (default: sys.stderr).
    """

    def __init__(
        self,
        total_papers: int,
        run_name: str,
        file=None,
    ):
        self.total = total_papers
        self.run_name = run_name
        self.file = file or sys.stderr
        self.completed = 0
        self.failed = 0
        self.elapsed_list: list[float] = []
        self._wall_start = time.monotonic()

    def report(self, paper_id: int, status: str, elapsed_seconds: float) -> None:
        """Record one paper's completion and print a progress line.

        Args:
            paper_id: The paper's DB id (used for display as EE-{id:03d}).
            status: Outcome string (e.g., "EXTRACTED", "FAILED", "SKIPPED").
            elapsed_seconds: Wall time spent on this paper.
        """
        self.completed += 1
        self.elapsed_list.append(elapsed_seconds)
        if status.upper() in ("FAILED", "EXTRACT_FAILED"):
            self.failed += 1

        pct = (self.completed / self.total * 100) if self.total else 0
        avg = statistics.mean(self.elapsed_list)
        remaining = self.total - self.completed
        eta = avg * remaining

        line = (
            f"[{self.run_name}] {self.completed}/{self.total} ({pct:.1f}%) | "
            f"Paper EE-{paper_id:03d} {status} | "
            f"{_fmt_duration(elapsed_seconds)} | "
            f"ETA ~{_fmt_duration(eta)} | "
            f"{self.failed} failed"
        )
        print(line, file=self.file, flush=True)

    def summary(self) -> str:
        """Print and return a final summary block."""
        wall = time.monotonic() - self._wall_start
        succeeded = self.completed - self.failed

        lines = [
            f"{'=' * 60}",
            f"[{self.run_name}] COMPLETE",
            f"  Total papers : {self.completed}",
            f"  Succeeded    : {succeeded}",
            f"  Failed       : {self.failed}",
            f"  Wall time    : {_fmt_duration(wall)}",
        ]

        if self.elapsed_list:
            mean_t = statistics.mean(self.elapsed_list)
            median_t = statistics.median(self.elapsed_list)
            max_t = max(self.elapsed_list)
            lines.append(f"  Mean/paper   : {_fmt_duration(mean_t)}")
            lines.append(f"  Median/paper : {_fmt_duration(median_t)}")
            lines.append(f"  Max/paper    : {_fmt_duration(max_t)}")

        lines.append(f"{'=' * 60}")

        block = "\n".join(lines)
        print(block, file=self.file, flush=True)
        return block
