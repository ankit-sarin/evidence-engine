"""QUALGAP-01 — runtime version A/B: does Ollama 0.17.7 restore Run 6 quality?

SCHEMA-EVAL-02 measured the current production contract (condition B) at 38.8%
anchored against Run 6's 58.2% on the same 40 papers, same parsed text, same
`deepseek-r1:32b` blob. The response contract was worth ~3pp; the remaining
~19pp had one suspect left standing: the Ollama runtime itself went
**0.17.7 -> 0.21.0** on 2026-04-19, after Run 6 and before every measurement
since.

This module holds the cell definitions and the result store for the test. Two
cells, both on a user-space 0.17.7 server, both on the production array schema
(contract B), both on the SCHEMA-EVAL-02 sample:

    V1  think NOT passed   — Run 6-era call shape. `extract_pass1_reasoning(prompt)`
                             took no `think` kwarg at all before commit 9190e41,
                             so the runtime default decided.
    V2  think=True         — same runtime, current call shape.

**Pre-flight correction, measured not assumed.** The brief expected V1 to produce
inline `<think>` tags (`parse_branch=legacy-tags`). It does not. Raw HTTP probes
against this 0.17.7 server — no Python client in the path — show `message.thinking`
populated and zero `<think>` tags in content, with `think` omitted *and* with
`think=true`; `think=false` suppresses thinking entirely. 0.17.7 already used the
native channel, so the interface did not move at the 0.21.0 upgrade.

Two consequences, both carried into the report:

  * **V1 is not a Run 6 replication.** Run 6's stored `reasoning_trace` rows are
    first-draft *answers* — field lists with verbatim quoted snippets, some in
    fenced ```json blocks — which is what the pre-fix parser's whole-content
    fallback returned. Run 6's Pass 2 was primed from the **content** channel.
    V1 and V2 are primed from the **thinking** channel, like every post-fix run.
    So V1 holds the code path at "fixed" and moves only the runtime version,
    which is what the primary read needs; it just is not the Run 6 code path.
  * **V2 - V1 cannot isolate a thinking-mode mechanism on this runtime**, because
    omitting `think` and passing `think=True` reach the same channel. The cell is
    kept as pinned, and read additionally as a same-condition replication — the
    first internal noise band this study has had for the anchored rate.

The comparison arms are NOT re-run — they already exist:
  * SCHEMA-EVAL-02 condition B  (0.21.0, think=True, same 40 papers)
  * Run 6 stored extractions    (0.17.7-era production, same papers)

Pass 2 is `think=False` in both cells, which is what Run 6-era code hard-coded
and what SCHEMA-EVAL-02 used, so pass 2 is held constant and the cells differ
only in the pass-1 `think` argument.

Byte-identical prompts to SCHEMA-EVAL-02: the sample, `build_extraction_prompt`
and the pass-2 message list are all imported rather than restated.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from analysis.eval.schema_eval2 import Result

# Cell labels. Stored in the `condition` field of the shared Result record so
# the SCHEMA-EVAL-02 analysis helpers can be reused unchanged.
CELL_V1 = "V1_017_no_think"
CELL_V2 = "V2_017_think_true"
CELLS = (CELL_V1, CELL_V2)

# `think` argument for pass 1, per cell. None means "do not pass the kwarg at
# all" — which is the whole point of V1 and is not the same as think=False.
PASS1_THINK: dict[str, bool | None] = {CELL_V1: None, CELL_V2: True}

# The runtime under test. Asserted against /api/version before any spend: a
# silent fall-back onto the 0.21.0 production port would produce a confidently
# wrong answer to the only question this task asks.
EXPECTED_SERVER_VERSION = "0.17.7"
DEFAULT_HOST = "http://127.0.0.1:11435"


@dataclass
class RuntimeResult(Result):
    """SCHEMA-EVAL-02's record plus both Pass-1 channels, kept verbatim.

    Storing `pass1_content` alongside `pass1_trace` costs nothing at run time and
    buys the one comparison this study would otherwise be unable to make. Run 6
    did not read the thinking channel: its stored `reasoning_trace` rows are
    first-draft *answers* (structured field lists with quoted snippets), because
    the pre-REGRESSION-01 parser fell back to the whole content. So Run 6's real
    Pass-2 input was the content channel, and every post-fix run's is the thinking
    channel. Capturing both here makes that difference measurable offline, on
    these same 80 calls, with no additional spend.
    """

    pass1_content: str | None = None
    pass1_trace: str | None = None


@dataclass(frozen=True)
class Probe:
    """One pre-flight probe result — response shape, not quality."""

    cell: str
    server_version: str
    parse_branch: str | None
    thinking_chars: int | None
    content_chars: int
    has_think_tags: bool
    pass2_format_ok: bool
    pass2_keys: int
    latency_s: float
    error: str | None = None


def store_dir(review_dir: Path) -> Path:
    p = Path(review_dir) / "eval" / "qualgap01"
    p.mkdir(parents=True, exist_ok=True)
    return p


def append_result(review_dir: Path, label: str, result) -> Path:
    """Append one result immediately — a ~10 hour batch must not lose work."""
    out = store_dir(review_dir) / f"{label}.jsonl"
    with out.open("a") as fh:
        fh.write(json.dumps(result.to_json(), default=str) + "\n")
    return out


def read_results(review_dir: Path, pattern: str = "*.jsonl") -> list[dict]:
    rows = []
    for p in sorted(store_dir(review_dir).glob(pattern)):
        for line in p.read_text().splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows
