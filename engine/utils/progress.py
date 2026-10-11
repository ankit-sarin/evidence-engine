"""Periodic progress reporting for long extraction runs."""

import logging
import statistics
import sys
import time

logger = logging.getLogger(__name__)


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
        self._stream_dead = False

    def _emit(self, text: str) -> None:
        """Write a progress line, or stop writing once the stream is gone.

        Under `--background` stderr is a pipe to `tee`; if its reader dies the
        write raises. Progress output is bookkeeping: a dead stream is logged
        once and the run carries on, with the counts still kept here."""
        if self._stream_dead:
            return
        try:
            print(text, file=self.file, flush=True)
        except (OSError, ValueError) as exc:
            self._stream_dead = True
            logger.warning("progress output stopped: the stream is gone (%s: %s); "
                           "the run continues and the counts are kept",
                           type(exc).__name__, exc)

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
        self._emit(line)

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
        self._emit(block)
        return block
