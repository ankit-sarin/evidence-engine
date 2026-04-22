"""Pass 2 24-triple smoke: stratified selection + 8-gate verification.

Reads the Pass 1 dry-run output, picks 14 high + 10 stratified medium
triples, runs run_pass2 against gemma3:27b, writes results to
fabrication_verifications under a new pass_number=2 judge_run_id
(suffix `pass2_smoke_<timestamp>`), and produces a markdown report
tagged with 8 pass/fail gates.

Usage:
    python -m analysis.paper1.pass2_smoke \\
        --review surgical_autonomy \\
        --pass1-run-id surgical_autonomy_pass1_20260420T093840Z_1d93b6e5 \\
        --pairs-csv data/surgical_autonomy/exports/disagreement_pairs_3arm.csv \\
        --codebook data/surgical_autonomy/extraction_codebook.yaml

The selection SQL is recorded verbatim in the report (§"selection")
so the 24 triples can be reproduced deterministically.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from analysis.paper1.judge import JudgeError, run_pass2
from analysis.paper1.judge_loader import (
    compute_codebook_sha256,
    load_ai_triples_csv,
    load_codebook,
)
from analysis.paper1.judge_prompts import arm_short_circuit_eligible
from analysis.paper1.judge_schema import (
    JudgeInput,
    PartiallySupportedVerdict,
    Pass2Result,
    SupportedVerdict,
    UnsupportedVerdict,
)
from analysis.paper1.judge_storage import (
    JudgeStorageError,
    complete_judge_run,
    create_judge_run,
    insert_pass2_verifications,
)
from engine.core.database import ReviewDatabase
from engine.utils.ollama_client import get_model_digest

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gemma3:27b"
PAPER_719 = "719"  # Bauzano 2010 — forces windowing path (125K tokens)


# ── Stratified selection ───────────────────────────────────────────


_SELECTION_SQL = """-- Smoke triple universe (run against judge_ratings
-- with pass1_run_id = :pass1_run_id):
--   high:   SELECT paper_id, field_name, field_type FROM judge_ratings
--           WHERE run_id=? AND pass1_fabrication_risk='high'
--           ORDER BY paper_id, field_name;
--   medium: SELECT paper_id, field_name, field_type FROM judge_ratings
--           WHERE run_id=? AND pass1_fabrication_risk='medium'
--           ORDER BY paper_id, field_name;
-- Stratification is applied in-Python (see pass2_smoke.select_triples).
-- Within each stratum, candidates are sorted by (paper_id ASC as int,
-- field_name ASC) and the first N unseen are taken."""


@dataclass(frozen=True)
class Candidate:
    paper_id: str
    field_name: str
    field_type: str
    risk: str  # 'high' or 'medium'

    def key(self) -> tuple:
        try:
            pid_sort = (0, int(self.paper_id))
        except ValueError:
            pid_sort = (1, self.paper_id)
        return pid_sort + (self.field_name,)


@dataclass
class SelectionResult:
    chosen: list[tuple[Candidate, str]]  # (candidate, stratum_label)
    gaps: list[str]                      # stratum labels that underfilled


def _fetch_candidates(
    db: ReviewDatabase, pass1_run_id: str, risk: str
) -> list[Candidate]:
    rows = db._conn.execute(
        """SELECT paper_id, field_name, field_type
           FROM judge_ratings
           WHERE run_id = ? AND pass1_fabrication_risk = ?""",
        (pass1_run_id, risk),
    ).fetchall()
    return [
        Candidate(r["paper_id"], r["field_name"], r["field_type"], risk)
        for r in rows
    ]


def _take_from(
    pool: list[Candidate],
    predicate,
    n: int,
    already_chosen: set[tuple[str, str]],
) -> list[Candidate]:
    filtered = [c for c in pool if predicate(c)]
    filtered.sort(key=Candidate.key)
    out: list[Candidate] = []
    for c in filtered:
        if len(out) >= n:
            break
        if (c.paper_id, c.field_name) in already_chosen:
            continue
        out.append(c)
        already_chosen.add((c.paper_id, c.field_name))
    return out


def select_triples(
    db: ReviewDatabase,
    pass1_run_id: str,
    judge_input_lookup: dict[tuple[str, str], JudgeInput],
) -> SelectionResult:
    """Apply the stratified plan and return the 24-row list + gap report."""
    high_pool = _fetch_candidates(db, pass1_run_id, "high")
    medium_pool = _fetch_candidates(db, pass1_run_id, "medium")

    chosen: list[tuple[Candidate, str]] = []
    gaps: list[str] = []
    already: set[tuple[str, str]] = set()

    # 1. All high triples (up to 14 — spec says include all).
    high_sorted = sorted(high_pool, key=Candidate.key)
    for c in high_sorted:
        chosen.append((c, "high"))
        already.add((c.paper_id, c.field_name))
    if len(high_sorted) < 14:
        gaps.append(f"high: only {len(high_sorted)} available, expected 14")

    # 2. task_performed GRANULARITY-saturated free-text (medium, 2).
    stratum_2 = _take_from(
        medium_pool,
        lambda c: c.field_name == "task_performed",
        2, already,
    )
    for c in stratum_2:
        chosen.append((c, "task_performed_saturated"))
    if len(stratum_2) < 2:
        gaps.append(
            f"task_performed_saturated: got {len(stratum_2)}/2"
        )

    # 3. robot_platform saturated free-text (medium, 2).
    stratum_3 = _take_from(
        medium_pool,
        lambda c: c.field_name == "robot_platform",
        2, already,
    )
    for c in stratum_3:
        chosen.append((c, "robot_platform_saturated"))
    if len(stratum_3) < 2:
        gaps.append(f"robot_platform_saturated: got {len(stratum_3)}/2")

    # 4. 2 categorical medium from any field.
    stratum_4 = _take_from(
        medium_pool,
        lambda c: c.field_type == "categorical",
        2, already,
    )
    for c in stratum_4:
        chosen.append((c, "categorical_any"))
    if len(stratum_4) < 2:
        gaps.append(f"categorical_any: got {len(stratum_4)}/2")

    # 5. 1 numeric (prefer sample_size).
    stratum_5 = _take_from(
        medium_pool,
        lambda c: c.field_type == "numeric" and c.field_name == "sample_size",
        1, already,
    )
    if not stratum_5:
        stratum_5 = _take_from(
            medium_pool, lambda c: c.field_type == "numeric", 1, already
        )
    for c in stratum_5:
        chosen.append((c, "numeric"))
    if len(stratum_5) < 1:
        gaps.append("numeric: got 0/1")

    # 6. 2 from paper 719 (forces windowing).
    stratum_6 = _take_from(
        medium_pool,
        lambda c: c.paper_id == PAPER_719,
        2, already,
    )
    for c in stratum_6:
        chosen.append((c, "paper719_windowing"))
    if len(stratum_6) < 2:
        gaps.append(f"paper719_windowing: got {len(stratum_6)}/2")

    # 7. 1 short-circuit eligible (at least one arm clean).
    def _is_short_circuit(c: Candidate) -> bool:
        inp = judge_input_lookup.get((c.paper_id, c.field_name))
        if inp is None:
            return False
        return any(arm_short_circuit_eligible(a) for a in inp.arms)

    stratum_7 = _take_from(
        medium_pool, _is_short_circuit, 1, already,
    )
    for c in stratum_7:
        chosen.append((c, "short_circuit_eligible"))
    if len(stratum_7) < 1:
        gaps.append("short_circuit_eligible: got 0/1")

    return SelectionResult(chosen=chosen, gaps=gaps)


# ── Execution ──────────────────────────────────────────────────────


@dataclass
class TripleExec:
    candidate: Candidate
    stratum: str
    wall_sec: float
    windowed: bool
    source_tokens: int
    verdicts_by_arm: dict[str, str]
    short_circuit_by_arm: dict[str, bool]
    verification_span_by_arm: dict[str, Optional[str]]
    reasoning_by_arm: dict[str, Optional[str]]
    fabrication_hypothesis_by_arm: dict[str, Optional[str]]
    overall_fabrication_detected: bool
    raw_response: str
    error: Optional[str] = None


def _paper_text(review_dir: Path, paper_id: str) -> Optional[str]:
    parsed_dir = review_dir / "parsed_text"
    md_files = sorted(parsed_dir.glob(f"{paper_id}_v*.md"), reverse=True)
    if not md_files:
        return None
    try:
        return md_files[0].read_text()
    except OSError:
        return None


def _new_run_id(review: str, tag: str = "") -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    middle = f"pass2_smoke_{tag}_" if tag else "pass2_smoke_"
    return f"{review}_{middle}{ts}"


def run_smoke(
    db: ReviewDatabase,
    pass1_run_id: str,
    judge_inputs: dict[tuple[str, str], JudgeInput],
    selection: SelectionResult,
    *,
    review: str,
    model: str,
    codebook_path: Path,
    codebook_sha: str,
    dry_run: bool,
    run_tag: str = "",
) -> tuple[str, list[TripleExec]]:
    run_id = _new_run_id(review, tag=run_tag)
    model_digest = get_model_digest(model) or model

    if not dry_run:
        create_judge_run(
            db, run_id=run_id,
            judge_model_name=model,
            judge_model_digest=model_digest,
            codebook_sha256=codebook_sha,
            pass_number=2,
            input_scope="AI_TRIPLES",
            run_config={
                "pass1_run_id": pass1_run_id,
                "codebook_path": str(codebook_path),
                "model": model,
                "n_selected": len(selection.chosen),
                "selection_gaps": selection.gaps,
            },
            notes=f"pass2 smoke (24 triples) linked to {pass1_run_id}",
        )
        logger.info("Created judge_run %s", run_id)

    review_dir = db.db_path.parent
    execs: list[TripleExec] = []
    attempted = succeeded = failed = 0

    for candidate, stratum in selection.chosen:
        attempted += 1
        key = (candidate.paper_id, candidate.field_name)
        inp = judge_inputs.get(key)
        if inp is None:
            execs.append(TripleExec(
                candidate=candidate, stratum=stratum,
                wall_sec=0.0, windowed=False, source_tokens=0,
                verdicts_by_arm={}, short_circuit_by_arm={},
                verification_span_by_arm={}, reasoning_by_arm={},
                fabrication_hypothesis_by_arm={},
                overall_fabrication_detected=False,
                raw_response="",
                error="JudgeInput lookup miss (not in pairs CSV after loader)",
            ))
            failed += 1
            continue

        source = _paper_text(review_dir, candidate.paper_id)
        if source is None:
            execs.append(TripleExec(
                candidate=candidate, stratum=stratum,
                wall_sec=0.0, windowed=False, source_tokens=0,
                verdicts_by_arm={}, short_circuit_by_arm={},
                verification_span_by_arm={}, reasoning_by_arm={},
                fabrication_hypothesis_by_arm={},
                overall_fabrication_detected=False,
                raw_response="",
                error=f"no parsed text for paper_id={candidate.paper_id}",
            ))
            failed += 1
            continue

        t0 = time.time()
        try:
            result: Pass2Result = run_pass2(
                inp, run_id=run_id, source_text=source, model=model
            )
            elapsed = time.time() - t0
        except JudgeError as exc:
            elapsed = time.time() - t0
            execs.append(TripleExec(
                candidate=candidate, stratum=stratum,
                wall_sec=elapsed, windowed=False, source_tokens=0,
                verdicts_by_arm={}, short_circuit_by_arm={},
                verification_span_by_arm={}, reasoning_by_arm={},
                fabrication_hypothesis_by_arm={},
                overall_fabrication_detected=False,
                raw_response="",
                error=f"{type(exc).__name__}: {exc}",
            ))
            failed += 1
            logger.error("FAIL %s/%s: %s", candidate.paper_id,
                         candidate.field_name, exc)
            continue

        # De-randomize verdicts for per-arm tables.
        verdicts_by_arm: dict[str, str] = {}
        span_by_arm: dict[str, Optional[str]] = {}
        reason_by_arm: dict[str, Optional[str]] = {}
        hyp_by_arm: dict[str, Optional[str]] = {}
        for v in result.pass2.arm_verdicts:
            arm_name = result.arm_permutation[v.arm_slot - 1]
            verdicts_by_arm[arm_name] = v.verdict
            span_by_arm[arm_name] = v.verification_span
            if isinstance(v, SupportedVerdict):
                reason_by_arm[arm_name] = v.reasoning
                hyp_by_arm[arm_name] = None
            elif isinstance(v, PartiallySupportedVerdict):
                reason_by_arm[arm_name] = v.reasoning
                hyp_by_arm[arm_name] = None
            elif isinstance(v, UnsupportedVerdict):
                reason_by_arm[arm_name] = v.reasoning
                hyp_by_arm[arm_name] = v.fabrication_hypothesis

        if not dry_run:
            try:
                insert_pass2_verifications(db, run_id, result)
            except JudgeStorageError as exc:
                failed += 1
                execs.append(TripleExec(
                    candidate=candidate, stratum=stratum,
                    wall_sec=elapsed,
                    windowed=result.source_text_windowed,
                    source_tokens=result.source_text_tokens,
                    verdicts_by_arm=verdicts_by_arm,
                    short_circuit_by_arm=result.pre_check_short_circuit_by_arm,
                    verification_span_by_arm=span_by_arm,
                    reasoning_by_arm=reason_by_arm,
                    fabrication_hypothesis_by_arm=hyp_by_arm,
                    overall_fabrication_detected=result.pass2.overall_fabrication_detected,
                    raw_response=result.raw_response,
                    error=f"insert_pass2_verifications failed: {exc}",
                ))
                logger.error("DB insert FAIL %s/%s: %s",
                             candidate.paper_id, candidate.field_name, exc)
                continue

        succeeded += 1
        execs.append(TripleExec(
            candidate=candidate, stratum=stratum,
            wall_sec=elapsed,
            windowed=result.source_text_windowed,
            source_tokens=result.source_text_tokens,
            verdicts_by_arm=verdicts_by_arm,
            short_circuit_by_arm=result.pre_check_short_circuit_by_arm,
            verification_span_by_arm=span_by_arm,
            reasoning_by_arm=reason_by_arm,
            fabrication_hypothesis_by_arm=hyp_by_arm,
            overall_fabrication_detected=result.pass2.overall_fabrication_detected,
            raw_response=result.raw_response,
        ))
        logger.info(
            "OK %s/%s [%s] %.1fs windowed=%s",
            candidate.paper_id, candidate.field_name, stratum,
            elapsed, result.source_text_windowed,
        )

    if not dry_run:
        try:
            complete_judge_run(
                db, run_id,
                n_triples_attempted=attempted,
                n_triples_succeeded=succeeded,
                n_triples_failed=failed,
            )
        except JudgeStorageError as exc:
            logger.error("complete_judge_run failed: %s", exc)

    return run_id, execs


# ── 8 gates ────────────────────────────────────────────────────────


@dataclass
class GateResult:
    name: str
    passed: bool
    detail: str


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    k = int(round((pct / 100.0) * (len(sorted_vals) - 1)))
    return sorted_vals[k]


def evaluate_gates(
    db: ReviewDatabase, run_id: str, execs: list[TripleExec], dry_run: bool
) -> list[GateResult]:
    out: list[GateResult] = []
    successes = [e for e in execs if e.error is None]
    failures = [e for e in execs if e.error is not None]

    # Gate 1 — parse cleanliness.
    parse_ok = 24 - len(failures)
    out.append(GateResult(
        name="1. parse_cleanliness",
        passed=parse_ok == 24,
        detail=f"{parse_ok}/24 triples parsed cleanly; {len(failures)} failures",
    ))

    # Gate 2 — invariant compliance (DB CHECK constraints).
    if dry_run:
        out.append(GateResult(
            name="2. invariant_compliance",
            passed=True,
            detail="dry-run — no DB writes to check",
        ))
    else:
        # Any UNSUPPORTED row missing reasoning or hypothesis would have
        # raised at insert time; the gate is whether any slipped through.
        bad = db._conn.execute(
            """SELECT COUNT(*) AS n
               FROM fabrication_verifications
               WHERE judge_run_id = ?
                 AND verdict = 'UNSUPPORTED'
                 AND (reasoning IS NULL OR TRIM(reasoning) = ''
                      OR fabrication_hypothesis IS NULL
                      OR TRIM(fabrication_hypothesis) = '')""",
            (run_id,),
        ).fetchone()["n"]
        out.append(GateResult(
            name="2. invariant_compliance",
            passed=bad == 0,
            detail=f"{bad} UNSUPPORTED rows violating reasoning/hypothesis NOT NULL",
        ))

    # Gate 3 — row count.
    if dry_run:
        expected = len(successes) * 3
        observed = sum(len(e.verdicts_by_arm) for e in successes)
        out.append(GateResult(
            name="3. row_count",
            passed=observed == expected,
            detail=f"dry-run: {observed} verdicts computed, expected {expected}",
        ))
    else:
        n = db._conn.execute(
            "SELECT COUNT(*) FROM fabrication_verifications WHERE judge_run_id = ?",
            (run_id,),
        ).fetchone()[0]
        out.append(GateResult(
            name="3. row_count",
            passed=n == 72,
            detail=f"{n} rows in fabrication_verifications (expected 72)",
        ))

    # Gate 4 — at least 1 UNSUPPORTED verdict.
    unsupp = sum(
        1 for e in successes for v in e.verdicts_by_arm.values()
        if v == "UNSUPPORTED"
    )
    out.append(GateResult(
        name="4. unsupported_present",
        passed=unsupp >= 1,
        detail=f"{unsupp} UNSUPPORTED verdicts across {len(successes)} triples",
    ))

    # Gate 5 — at least one short-circuit fires.
    sc_fires = sum(
        1 for e in successes for b in e.short_circuit_by_arm.values() if b
    )
    out.append(GateResult(
        name="5. short_circuit_firing",
        passed=sc_fires >= 1,
        detail=f"{sc_fires} arm-rows with short_circuit=True",
    ))

    # Gate 6 — at least 1 triple windowed.
    n_windowed = sum(1 for e in successes if e.windowed)
    out.append(GateResult(
        name="6. windowing_exercised",
        passed=n_windowed >= 1,
        detail=f"{n_windowed}/{len(successes)} triples used windowed source",
    ))

    # Gate 7 — latency p50 ≤ 75s, p95 ≤ 120s.
    latencies = sorted(e.wall_sec for e in successes if e.wall_sec > 0)
    p50 = _percentile(latencies, 50)
    p95 = _percentile(latencies, 95)
    out.append(GateResult(
        name="7. latency",
        passed=p50 <= 75.0 and p95 <= 120.0,
        detail=f"p50={p50:.1f}s (limit 75), p95={p95:.1f}s (limit 120)",
    ))

    # Gate 8 — no single arm is 100% SUPPORTED.
    by_arm: dict[str, dict[str, int]] = {}
    for e in successes:
        for arm, verdict in e.verdicts_by_arm.items():
            d = by_arm.setdefault(arm, {})
            d[verdict] = d.get(verdict, 0) + 1
    imbalanced: list[str] = []
    for arm, counts in by_arm.items():
        total = sum(counts.values())
        sup = counts.get("SUPPORTED", 0)
        if total >= 5 and sup == total:
            imbalanced.append(arm)
    out.append(GateResult(
        name="8. arm_verdict_balance",
        passed=len(imbalanced) == 0,
        detail=(
            f"arms 100% SUPPORTED: {imbalanced}" if imbalanced
            else f"no arm is 100% SUPPORTED (per-arm counts: "
            f"{ {a: dict(c) for a,c in by_arm.items()} })"
        ),
    ))

    return out


# ── Report ─────────────────────────────────────────────────────────


def _format_selection_table(execs: list[TripleExec]) -> str:
    lines = [
        "| # | paper_id | field_name | risk | field_type | stratum | windowed | sc_arms |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for i, e in enumerate(execs, 1):
        sc_arms = ",".join(
            a for a, v in e.short_circuit_by_arm.items() if v
        ) or "-"
        windowed = "Y" if e.windowed else "N"
        lines.append(
            f"| {i} | {e.candidate.paper_id} | {e.candidate.field_name} | "
            f"{e.candidate.risk} | {e.candidate.field_type} | {e.stratum} | "
            f"{windowed} | {sc_arms} |"
        )
    return "\n".join(lines)


def _format_per_triple_results(execs: list[TripleExec]) -> str:
    arms_order = ("local", "openai_o4_mini_high", "anthropic_sonnet_4_6")
    lines = [
        "| # | paper/field | latency | src_toks | local | o4mini | sonnet | overall_fab |",
        "|---|---|---:|---:|---|---|---|:---:|",
    ]
    for i, e in enumerate(execs, 1):
        if e.error:
            lines.append(
                f"| {i} | {e.candidate.paper_id}/{e.candidate.field_name} | ERR | - | - | - | - | ERR:{e.error[:40]} |"
            )
            continue
        cells = [e.verdicts_by_arm.get(a, "-") for a in arms_order]
        lines.append(
            f"| {i} | {e.candidate.paper_id}/{e.candidate.field_name} | "
            f"{e.wall_sec:.1f}s | {e.source_tokens} | "
            f"{cells[0]} | {cells[1]} | {cells[2]} | "
            f"{'Y' if e.overall_fabrication_detected else 'N'} |"
        )
    return "\n".join(lines)


def _verdict_by_arm_crosstab(execs: list[TripleExec]) -> str:
    arms_order = ("local", "openai_o4_mini_high", "anthropic_sonnet_4_6")
    counts: dict[str, dict[str, int]] = {a: {} for a in arms_order}
    for e in execs:
        if e.error:
            continue
        for arm, v in e.verdicts_by_arm.items():
            counts[arm][v] = counts[arm].get(v, 0) + 1
    lines = [
        "| arm | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total |",
        "|---|---:|---:|---:|---:|",
    ]
    for arm in arms_order:
        row = counts[arm]
        s = row.get("SUPPORTED", 0)
        p = row.get("PARTIALLY_SUPPORTED", 0)
        u = row.get("UNSUPPORTED", 0)
        lines.append(f"| {arm} | {s} | {p} | {u} | {s+p+u} |")
    return "\n".join(lines)


def _pick_example_traces(
    execs: list[TripleExec], max_each: int = 1
) -> str:
    want: list[tuple[str, str]] = [
        ("SUPPORTED", "supported"),
        ("UNSUPPORTED", "unsupported"),
    ]
    blocks: list[str] = []
    for target, label in want:
        for e in execs:
            if e.error:
                continue
            for arm, verdict in e.verdicts_by_arm.items():
                if verdict == target:
                    reasoning = e.reasoning_by_arm.get(arm)
                    hyp = e.fabrication_hypothesis_by_arm.get(arm)
                    span = e.verification_span_by_arm.get(arm)
                    blocks.append(
                        f"### {label} — {e.candidate.paper_id}/{e.candidate.field_name} (arm={arm})\n"
                        f"- verification_span: {span!r}\n"
                        f"- reasoning: {reasoning!r}\n"
                        f"- fabrication_hypothesis: {hyp!r}"
                    )
                    break
            if blocks and blocks[-1].startswith(f"### {label}"):
                break
    # Short-circuit example.
    for e in execs:
        if e.error:
            continue
        for arm, sc in e.short_circuit_by_arm.items():
            if sc and arm in e.verdicts_by_arm:
                blocks.append(
                    f"### short-circuit fired — {e.candidate.paper_id}/{e.candidate.field_name} (arm={arm})\n"
                    f"- verdict: {e.verdicts_by_arm[arm]}\n"
                    f"- verification_span: {e.verification_span_by_arm.get(arm)!r}\n"
                    f"- reasoning: {e.reasoning_by_arm.get(arm)!r}"
                )
                break
        else:
            continue
        break
    return "\n\n".join(blocks) if blocks else "_no qualifying traces found_"


def render_report(
    *,
    run_id: str,
    pass1_run_id: str,
    model: str,
    codebook_sha: str,
    execs: list[TripleExec],
    gates: list[GateResult],
    selection: SelectionResult,
) -> str:
    lines: list[str] = []
    lines.append(f"# Pass 2 smoke — `{run_id}`\n")
    lines.append(f"**Parent Pass 1 run:** `{pass1_run_id}`")
    lines.append(f"**Model:** `{model}` · codebook_sha256 `{codebook_sha}`\n")

    lines.append("## Gate pass/fail\n")
    for g in gates:
        status = "PASS" if g.passed else "FAIL"
        lines.append(f"- **{status}** · {g.name} — {g.detail}")
    all_pass = all(g.passed for g in gates)
    lines.append(
        f"\n**Overall: {'ALL GATES PASS — cleared for Task #10 full run' if all_pass else 'FAIL — do not proceed to full run'}**\n"
    )

    lines.append("## Selection\n")
    lines.append("Selection SQL (deterministic; stratified in Python):\n")
    lines.append("```sql")
    lines.append(_SELECTION_SQL.strip())
    lines.append("```")
    if selection.gaps:
        lines.append("\n**Stratum gaps:**")
        for gap in selection.gaps:
            lines.append(f"- {gap}")
    else:
        lines.append("\nAll strata populated to target count.")
    lines.append("")
    lines.append("### 24 selected triples\n")
    lines.append(_format_selection_table(execs))
    lines.append("")

    lines.append("## Per-triple results\n")
    lines.append(_format_per_triple_results(execs))
    lines.append("")

    lines.append("## Verdict × arm cross-tab\n")
    lines.append(_verdict_by_arm_crosstab(execs))
    lines.append("")

    # Short-circuit firing rate.
    n_sc = sum(
        1 for e in execs if not e.error
        for b in e.short_circuit_by_arm.values() if b
    )
    total_arm_rows = 3 * sum(1 for e in execs if not e.error)
    lines.append("## Short-circuit firing\n")
    if total_arm_rows:
        pct = n_sc / total_arm_rows * 100
        lines.append(
            f"- {n_sc}/{total_arm_rows} arm-rows eligible "
            f"({pct:.1f}%)"
        )
    else:
        lines.append("- no successful triples to count")
    lines.append("")

    lines.append("## Example reasoning traces\n")
    lines.append(_pick_example_traces(execs))
    lines.append("")

    # Anomalies: any failures, any stratum gaps, high latency.
    anomalies: list[str] = []
    for e in execs:
        if e.error:
            anomalies.append(
                f"- {e.candidate.paper_id}/{e.candidate.field_name} "
                f"({e.stratum}): {e.error}"
            )
    if selection.gaps:
        anomalies.extend(f"- stratum gap: {g}" for g in selection.gaps)
    succ = [e for e in execs if not e.error]
    if succ:
        slowest = max(succ, key=lambda e: e.wall_sec)
        if slowest.wall_sec > 120:
            anomalies.append(
                f"- slowest triple > 120s: "
                f"{slowest.candidate.paper_id}/{slowest.candidate.field_name} "
                f"= {slowest.wall_sec:.1f}s"
            )
    lines.append("## Anomalies\n")
    if anomalies:
        lines.extend(anomalies)
    else:
        lines.append("- none")
    lines.append("")

    return "\n".join(lines) + "\n"


# ── CLI ────────────────────────────────────────────────────────────


def _resolve_db(args) -> ReviewDatabase:
    if args.data_root is not None:
        return ReviewDatabase(args.review, data_root=args.data_root)
    return ReviewDatabase(args.review)


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="analysis.paper1.pass2_smoke",
        description="Run the Pass 2 24-triple smoke test.",
    )
    p.add_argument("--review", required=True)
    p.add_argument("--pass1-run-id", required=True)
    p.add_argument("--pairs-csv", required=True, type=Path)
    p.add_argument("--codebook", required=True, type=Path)
    p.add_argument("--out-dir", type=Path,
                   default=Path("analysis/paper1/reports"))
    p.add_argument("--log-dir", type=Path,
                   default=Path("analysis/paper1/logs"))
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--dry-run", action="store_true",
                   help="Select + print plan, skip Ollama/DB writes")
    p.add_argument("--data-root", type=Path, default=None)
    p.add_argument("--run-tag", default="",
                   help="Optional tag inserted into run_id, e.g. 'fixed' "
                        "→ <review>_pass2_smoke_fixed_<timestamp>")
    return p


def _setup_logging(log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_file = log_dir / f"pass2_smoke_{ts}.log"
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    root = logging.getLogger()
    if not root.handlers:
        root.setLevel(logging.INFO)
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        root.addHandler(sh)
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        root.addHandler(fh)
    return log_file


def run(argv: Optional[list[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    log_file = _setup_logging(args.log_dir)
    logger.info("pass2 smoke starting; log=%s", log_file)

    db = _resolve_db(args)
    codebook = load_codebook(args.codebook)
    codebook_sha = compute_codebook_sha256(args.codebook)

    logger.info("Loading pairs CSV to derive per-triple JudgeInput")
    inputs_list = load_ai_triples_csv(args.pairs_csv, db, codebook, limit=None)
    judge_inputs = {(i.paper_id, i.field_name): i for i in inputs_list}

    logger.info("Selecting 24 triples (14 high + 10 stratified medium)")
    selection = select_triples(db, args.pass1_run_id, judge_inputs)
    if not selection.chosen:
        print("No triples selected — aborting", file=sys.stderr)
        db.close()
        return 2

    if args.dry_run:
        print("=== DRY RUN — selection only ===")
        for i, (c, s) in enumerate(selection.chosen, 1):
            inp = judge_inputs.get((c.paper_id, c.field_name))
            sc_eligible = (
                [a.arm_name for a in inp.arms if arm_short_circuit_eligible(a)]
                if inp else []
            )
            print(
                f"{i:2d}. [{s}] paper={c.paper_id} field={c.field_name} "
                f"risk={c.risk} type={c.field_type} sc={sc_eligible}"
            )
        if selection.gaps:
            print("gaps:")
            for g in selection.gaps:
                print(f"  {g}")
        db.close()
        return 0

    run_id, execs = run_smoke(
        db, args.pass1_run_id, judge_inputs, selection,
        review=args.review, model=args.model,
        codebook_path=args.codebook, codebook_sha=codebook_sha,
        dry_run=False, run_tag=args.run_tag,
    )

    gates = evaluate_gates(db, run_id, execs, dry_run=False)

    md = render_report(
        run_id=run_id, pass1_run_id=args.pass1_run_id,
        model=args.model, codebook_sha=codebook_sha,
        execs=execs, gates=gates, selection=selection,
    )
    args.out_dir.mkdir(parents=True, exist_ok=True)
    report_path = args.out_dir / f"pass2_smoke_{run_id}.md"
    report_path.write_text(md)
    logger.info("Wrote report: %s", report_path)

    db.close()
    all_pass = all(g.passed for g in gates)
    print(f"\n{'ALL GATES PASS' if all_pass else 'GATE FAILURE'} — {report_path}")
    return 0 if all_pass else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
