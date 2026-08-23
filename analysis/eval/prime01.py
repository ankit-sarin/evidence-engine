"""PRIME-01 — how quote-rich is each Pass-1 channel, and does it predict anchoring?

QUALGAP-01 established that Run 6's anchoring rode on Pass 2 being primed with a
quote-rich first-draft *answer* rather than with reasoning: on 0.17.7 the draft
(content) channel repeats the paper verbatim in 37.7% of its 8-word windows and
the thinking channel in 0.3%. This module measures that across every channel
still on disk and asks whether draft richness actually predicts the final
anchored rate.

**Zero model calls.** Every input is already on disk.

## The verbatim-window measure

Deliberately identical to the ad-hoc check QUALGAP-01 used, so the numbers are
comparable to the ones already reported (0.3% / 37.7%) rather than merely
similar:

    normalize the text, split to words, walk non-overlapping 8-word windows; a
    window scores if it appears verbatim in the normalized paper. On a hit,
    advance a full window (a matched run is counted once per window it fills);
    on a miss, advance one word. Rate = hits / (word_count // 8).

`test_prime01.py` pins the method against QUALGAP-01's published figures. It is
a coarse instrument on purpose: it asks "does this text repeat the paper", not
"is this a well-formed quotation", which is exactly the question priming raises.

## Channel availability — the gating fact

The task asked for 0.21.0 Pass-1 *drafts*. They do not exist. The eval runners
(`run_local_ab`, `run_local_abc`) store `raw_content` = the **Pass-2** response
and `think_chars` = an integer **length** of the Pass-1 trace; the trace text
itself is discarded. `record_call` telemetry would have held it but is never
reached, because those runners deliberately bypass `extract_paper()`. So the
0.21.0 draft was never captured — it is absent, not truncated.

One partial signal survives. SCHEMA-EVAL-01 ran on 2026-07-28, before the
REGRESSION-01 fix (2026-07-29), calling the pre-fix `extract_pass1_reasoning(prompt)`
whose whole-content fallback made `trace` *be* the draft. Its `think_chars` is
therefore a genuine measurement of **0.21.0 draft length** — length only, not
richness, and richness is what the pre-registered bands are defined on.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from analysis.provenance.normalize import normalize

WINDOW_WORDS = 8

# Same shape-detector `analyze_qualgap01.channel()` uses, so "does this draft
# enumerate fields with snippets" is answered the same way in both reports.
SNIPPET_RE = re.compile(
    r'["\'*]{0,3}source[_ ]snippet["\'*]{0,3}\s*[:=]\s*["“]([^"”]{10,})["”]',
    re.IGNORECASE,
)


def verbatim_window_rate(text: str | None, norm_paper: str) -> tuple[int, int]:
    """Return (hits, windows) for `text` against an already-normalized paper.

    See the module docstring for why the walk is asymmetric (advance a window on
    a hit, one word on a miss). Kept byte-for-byte equivalent to QUALGAP-01's
    check so the two studies' numbers can sit in the same table.
    """
    words = normalize(text or "").split()
    hits = 0
    i = 0
    while i + WINDOW_WORDS <= len(words):
        if " ".join(words[i:i + WINDOW_WORDS]) in norm_paper:
            hits += 1
            i += WINDOW_WORDS
        else:
            i += 1
    return hits, max(1, len(words) // WINDOW_WORDS)


@dataclass
class DocStats:
    """One document of one channel, measured."""

    paper_id: int
    channel: str
    chars: int
    hits: int
    windows: int
    fenced_json: bool
    snippet_labels: int

    @property
    def rate(self) -> float:
        return 100.0 * self.hits / self.windows if self.windows else 0.0

    def to_json(self) -> dict:
        return {
            "paper_id": self.paper_id, "channel": self.channel, "chars": self.chars,
            "hits": self.hits, "windows": self.windows, "rate_pct": round(self.rate, 1),
            "fenced_json": self.fenced_json, "snippet_labels": self.snippet_labels,
        }


def measure(paper_id: int, channel: str, text: str | None, norm_paper: str) -> DocStats:
    text = text or ""
    hits, windows = verbatim_window_rate(text, norm_paper)
    return DocStats(
        paper_id=paper_id, channel=channel, chars=len(text), hits=hits, windows=windows,
        fenced_json="```" in text,
        snippet_labels=len(SNIPPET_RE.findall(text)),
    )


# ── pure-python rank correlation ─────────────────────────────────────────


def _ranks(values: list[float]) -> list[float]:
    """Average ranks, ties shared — the standard Spearman treatment."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        shared = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = shared
        i = j + 1
    return ranks


def spearman(xs: list[float], ys: list[float]) -> float | None:
    """Spearman's rho. Pure Python — no scipy dependency for one coefficient."""
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    rx, ry = _ranks(xs), _ranks(ys)
    n = len(xs)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    dx = sum((a - mx) ** 2 for a in rx)
    dy = sum((b - my) ** 2 for b in ry)
    if dx <= 0 or dy <= 0:
        return None
    return num / (dx * dy) ** 0.5


# ── loaders ──────────────────────────────────────────────────────────────


def load_papers(parsed_dir: Path, pids) -> dict[int, str]:
    """paper_id -> normalized parsed text (the newest version on disk)."""
    out = {}
    for pid in pids:
        files = sorted(parsed_dir.glob(f"{pid}_v*.md"),
                       key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
        if files:
            out[pid] = normalize(files[-1].read_text())
    return out


def load_qualgap(path: Path) -> list[dict]:
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def load_run6_traces(db_path: Path, pids) -> dict[int, str]:
    conn = sqlite3.connect(f"file:{db_path}?immutable=1", uri=True)
    try:
        out = {}
        for pid in pids:
            row = conn.execute(
                "SELECT reasoning_trace FROM extractions WHERE paper_id=? ORDER BY id LIMIT 1",
                (pid,)).fetchone()
            if row and row[0]:
                out[pid] = row[0]
        return out
    finally:
        conn.close()


def load_run6_spans(db_path: Path, pids) -> dict[int, list[dict]]:
    conn = sqlite3.connect(f"file:{db_path}?immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        out = {}
        for pid in pids:
            rows = [dict(r) for r in conn.execute(
                "SELECT s.field_name, s.value, s.source_snippet FROM evidence_spans s "
                "JOIN extractions e ON e.id = s.extraction_id WHERE e.paper_id = ?", (pid,))]
            if rows:
                out[pid] = rows
        return out
    finally:
        conn.close()


def load_schema_eval1_draft_lengths(path: Path) -> dict[int, list[int]]:
    """0.21.0 draft *lengths* from the pre-fix SCHEMA-EVAL-01 run.

    Only valid because that run predates the REGRESSION-01 fix: `think_chars`
    there is `len(trace)` where `trace` was the whole content, i.e. the draft.
    Post-fix runs record thinking length under the same key and must not be
    mixed in. Length only — the text was not kept.
    """
    out: dict[int, list[int]] = {}
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        if r.get("ok") and r.get("think_chars"):
            out.setdefault(r["paper_id"], []).append(r["think_chars"])
    return out
