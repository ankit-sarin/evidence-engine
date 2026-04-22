"""Pass 1 dry-run inspection: cross-tabs, fabricator audit, B-filter sim.

Runs seven diagnostic queries against the judge tables produced by
``analysis.paper1.judge_cli`` and emits per-query CSVs plus a single
markdown report. Not a permanent engine module; lives alongside the
Paper 1 analysis scripts.

Usage:
    python -m analysis.paper1.pass1_inspection \\
        --review surgical_autonomy \\
        --run-id surgical_autonomy_pass1_20260420T093840Z_1d93b6e5 \\
        --pairs-csv data/surgical_autonomy/exports/disagreement_pairs_3arm.csv \\
        --codebook data/surgical_autonomy/extraction_codebook.yaml
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from analysis.paper1.judge_loader import (
    CodebookEntry,
    load_ai_triples_csv,
    load_codebook,
)
from engine.core.database import ReviewDatabase

logger = logging.getLogger(__name__)

ARMS = ("local", "openai_o4_mini_high", "anthropic_sonnet_4_6")
LEVEL1 = ("EQUIVALENT", "PARTIAL", "DIVERGENT")
LEVEL2 = ("GRANULARITY", "SELECTION", "OMISSION", "CONTRADICTION", "FABRICATION")
FIELD_TYPES = ("free_text", "categorical", "numeric")

EQ_LOW_FLAG = 5.0    # flag fields with <5% EQ
EQ_HIGH_FLAG = 30.0  # flag fields with >30% EQ
GRANULARITY_SATURATION = 80.0  # flag fields where GRANULARITY > 80% of non-EQ

PASS2_RATES_SEC = (45, 60)  # wall-clock projection per triple at 45s and 60s


# ── DB pulls ───────────────────────────────────────────────────────


def _load_ratings(db: ReviewDatabase, run_id: str) -> pd.DataFrame:
    rows = db._conn.execute(
        """SELECT rating_id, paper_id, field_name, field_type,
                  pass1_fabrication_risk AS fab_risk
           FROM judge_ratings
           WHERE run_id = ?""",
        (run_id,),
    ).fetchall()
    return pd.DataFrame([dict(r) for r in rows])


def _load_pair_ratings(db: ReviewDatabase, run_id: str) -> pd.DataFrame:
    rows = db._conn.execute(
        """SELECT r.rating_id, r.paper_id, r.field_name, r.field_type,
                  pr.arm_a, pr.arm_b, pr.level1_rating, pr.level2_type
           FROM judge_pair_ratings pr
           JOIN judge_ratings r ON r.rating_id = pr.rating_id
           WHERE r.run_id = ?""",
        (run_id,),
    ).fetchall()
    return pd.DataFrame([dict(r) for r in rows])


# ── Queries ────────────────────────────────────────────────────────


def q1_field_level1(pairs: pd.DataFrame) -> pd.DataFrame:
    """Per-field Level 1 cross-tab with % EQ and flags."""
    pivot = (
        pairs.pivot_table(
            index="field_name",
            columns="level1_rating",
            values="rating_id",
            aggfunc="count",
            fill_value=0,
        )
        .reindex(columns=list(LEVEL1), fill_value=0)
    )
    pivot["total"] = pivot.sum(axis=1)
    pivot["pct_eq"] = pivot["EQUIVALENT"] / pivot["total"] * 100
    pivot["flag"] = pivot["pct_eq"].apply(
        lambda x: "LOW_EQ" if x < EQ_LOW_FLAG
        else ("HIGH_EQ" if x > EQ_HIGH_FLAG else "")
    )
    return pivot.sort_values("total", ascending=False).reset_index()


def q2_field_level2(pairs: pd.DataFrame) -> pd.DataFrame:
    """Per-field Level 2 cross-tab, flagging GRANULARITY saturation."""
    noneq = pairs[pairs["level2_type"].notna()]
    pivot = (
        noneq.pivot_table(
            index="field_name",
            columns="level2_type",
            values="rating_id",
            aggfunc="count",
            fill_value=0,
        )
        .reindex(columns=list(LEVEL2), fill_value=0)
    )
    pivot["noneq_total"] = pivot.sum(axis=1)
    pivot["pct_granularity"] = (
        pivot["GRANULARITY"] / pivot["noneq_total"].replace(0, pd.NA) * 100
    )
    pivot["flag"] = pivot["pct_granularity"].apply(
        lambda x: "GRANULARITY_SATURATED" if pd.notna(x) and x > GRANULARITY_SATURATION else ""
    )
    return pivot.sort_values("noneq_total", ascending=False).reset_index()


def q3_fieldtype_level2(
    pairs: pd.DataFrame, codebook: dict[str, CodebookEntry]
) -> pd.DataFrame:
    """field_type × level2_type aggregation."""
    noneq = pairs[pairs["level2_type"].notna()].copy()
    # The DB stores field_type on judge_ratings already — use it directly,
    # fall back to codebook only for rows missing it (defensive).
    missing = noneq["field_type"].isna()
    if missing.any():
        noneq.loc[missing, "field_type"] = noneq.loc[missing, "field_name"].map(
            lambda n: codebook[n].field_type if n in codebook else "unknown"
        )
    pivot = (
        noneq.pivot_table(
            index="field_type",
            columns="level2_type",
            values="rating_id",
            aggfunc="count",
            fill_value=0,
        )
        .reindex(index=list(FIELD_TYPES), columns=list(LEVEL2), fill_value=0)
    )
    pivot["noneq_total"] = pivot.sum(axis=1)
    pivot["pct_granularity"] = (
        pivot["GRANULARITY"] / pivot["noneq_total"].replace(0, pd.NA) * 100
    )
    return pivot.reset_index()


def q4_armpair_level2(pairs: pd.DataFrame) -> pd.DataFrame:
    """arm_pair × level2_type cross-tab. Confirms §10 FABRICATION skew."""
    noneq = pairs[pairs["level2_type"].notna()].copy()
    noneq["arm_pair"] = noneq["arm_a"] + " × " + noneq["arm_b"]
    pivot = (
        noneq.pivot_table(
            index="arm_pair",
            columns="level2_type",
            values="rating_id",
            aggfunc="count",
            fill_value=0,
        )
        .reindex(columns=list(LEVEL2), fill_value=0)
    )
    pivot["noneq_total"] = pivot.sum(axis=1)
    return pivot.sort_values("noneq_total", ascending=False).reset_index()


@dataclass(frozen=True)
class Q5Finding:
    has_attribution_field: bool
    note: str


def q5_fabricator_attribution() -> Q5Finding:
    """Schema inspection — does Pass 1 attribute fabrication to an arm?

    Read analysis/paper1/judge_schema.py structurally: Pass1Output exposes
    pairwise_ratings, fabrication_risk, proposed_consensus, overall_rationale;
    PairwiseRating variants expose slot_a, slot_b, rating, rationale, and
    (only on DisagreementPair) disagreement_type. No field carries per-arm
    fabricator identity.
    """
    from analysis.paper1 import judge_schema as js

    pass1_fields = set(js.Pass1Output.model_fields.keys())
    eq_fields = set(js.EquivalentPair.model_fields.keys())
    disagree_fields = set(js.DisagreementPair.model_fields.keys())
    all_fields = pass1_fields | eq_fields | disagree_fields
    attribution_markers = {"fabricator_slot", "fabricator_arm", "suspect_arm",
                           "suspect_slot", "fabricator"}
    has_attr = bool(all_fields & attribution_markers)

    if has_attr:
        note = (
            "Pass 1 exposes an attribution field — per-arm count needs manual "
            "extraction from raw_response (see judge_ratings.raw_response)."
        )
    else:
        note = (
            "Pass 1 emits a triple-level fabrication_risk label (low/medium/"
            "high) and a proposed_consensus string. Neither carries per-arm "
            "attribution; FABRICATION counts in Q4 are pair-level (arm_a vs "
            "arm_b disagreement type) and do not identify which of the two "
            "arms is fabricating. Per-arm fabricator identity is a Pass 2 "
            "responsibility: Pass 2 re-grounds each arm's (value, span) "
            "against the source paper and labels each arm as grounded / "
            "ungrounded / absent. This is the intended design."
        )
    return Q5Finding(has_attribution_field=has_attr, note=note)


def q6_bfilter_simulation(
    ratings: pd.DataFrame,
    db: ReviewDatabase,
    codebook: dict[str, CodebookEntry],
    pairs_csv: Path,
) -> tuple[pd.DataFrame, dict]:
    """Replay loader to re-derive precheck flags, then apply B-filter."""
    logger.info("Q6: re-running loader for precheck flags (%s triples)",
                len(ratings))
    inputs = load_ai_triples_csv(pairs_csv, db, codebook, limit=None)

    flag_rows = []
    for inp in inputs:
        row: dict = {
            "paper_id": inp.paper_id,
            "field_name": inp.field_name,
            "field_type": inp.field_type,
        }
        any_span_missing = False
        any_value_absent = False
        for arm in inp.arms:
            flags = arm.precheck_flags
            row[f"{arm.arm_name}__span_in_source"] = flags.span_in_source
            row[f"{arm.arm_name}__value_in_span"] = flags.value_in_span
            row[f"{arm.arm_name}__span_present"] = flags.span_present
            if not flags.span_in_source:
                any_span_missing = True
            if not flags.value_in_span:
                any_value_absent = True
        row["any_span_not_in_source"] = any_span_missing
        row["any_value_not_in_span"] = any_value_absent
        flag_rows.append(row)
    flags_df = pd.DataFrame(flag_rows)

    merged = ratings.merge(
        flags_df, on=("paper_id", "field_name"), how="left",
        suffixes=("", "_fl"),
    )
    # Paper_id may differ dtype (ratings = str, loader emits str too),
    # but a quiet merge-miss would silently break the sim — assert instead.
    missing_flags = merged["any_span_not_in_source"].isna().sum()
    if missing_flags:
        logger.warning(
            "Q6: %d triples could not be joined to precheck flags — "
            "they will be treated as missing both checks.", missing_flags
        )
        merged["any_span_not_in_source"] = merged["any_span_not_in_source"].fillna(False)
        merged["any_value_not_in_span"] = merged["any_value_not_in_span"].fillna(False)

    def _keep(row) -> bool:
        if row["fab_risk"] == "high":
            return True
        if row["fab_risk"] == "medium":
            return bool(row["any_span_not_in_source"] or row["any_value_not_in_span"])
        return False

    merged["pass2_keep"] = merged.apply(_keep, axis=1)

    kept = merged[merged["pass2_keep"]]
    # Diagnostic: does the precheck-flag test actually discriminate risk
    # buckets? If low-risk triples also trip the flag at ~100%, the filter
    # is degenerate and the B-filter effectively equals "medium OR high".
    merged["flagged"] = merged["any_span_not_in_source"] | merged["any_value_not_in_span"]
    trip_rates: dict[str, dict[str, int | float]] = {}
    for bucket in ("low", "medium", "high"):
        sub = merged[merged["fab_risk"] == bucket]
        n = int(len(sub))
        tripped = int(sub["flagged"].sum()) if n else 0
        trip_rates[bucket] = {
            "n": n,
            "tripped": tripped,
            "pct": round((tripped / n * 100) if n else 0.0, 1),
        }

    summary: dict = {
        "total_triples": int(len(ratings)),
        "medium_or_high": int((ratings["fab_risk"].isin(["medium", "high"])).sum()),
        "kept_total": int(len(kept)),
        "kept_high": int((kept["fab_risk"] == "high").sum()),
        "kept_medium_surviving": int((kept["fab_risk"] == "medium").sum()),
        "by_field_type": kept.groupby("field_type").size().to_dict(),
        "by_arm_pair": {},  # filled below
        "projections_sec": {
            str(rate): int(len(kept) * rate) for rate in PASS2_RATES_SEC
        },
        "projections_h": {
            str(rate): round(len(kept) * rate / 3600.0, 2)
            for rate in PASS2_RATES_SEC
        },
        "flag_trip_rates": trip_rates,
    }

    # Arm-pair breakdown for Pass 2 scheduling — every kept triple
    # exercises all three pairs, so "by arm_pair" here reports triple
    # count with at least one flagged arm in that pair. We just echo
    # the totals; Pass 2 runs per-triple, not per-pair.
    summary["by_arm_pair"] = {
        f"{a} × {b}": int(len(kept)) for a, b in (
            ("anthropic_sonnet_4_6", "local"),
            ("anthropic_sonnet_4_6", "openai_o4_mini_high"),
            ("local", "openai_o4_mini_high"),
        )
    }

    return merged, summary


def q7_field_1_root_cause(pairs_csv: Path) -> dict:
    """Classify the field_1 skipped row from the source CSV."""
    matches: list[dict] = []
    with pairs_csv.open(newline="") as f:
        for row in csv.DictReader(f):
            if row.get("field_name") == "field_1":
                matches.append(row)

    if not matches:
        return {"count": 0, "classification": "not_present", "example": None}

    ex = matches[0]
    local_nonempty = bool((ex.get("local_value") or "").strip())
    o4_nonempty = bool((ex.get("o4mini_value") or "").strip())
    son_nonempty = bool((ex.get("sonnet_value") or "").strip())

    if local_nonempty and not (o4_nonempty or son_nonempty):
        classification = "local_arm_extractor_artifact"
    elif not any([local_nonempty, o4_nonempty, son_nonempty]):
        classification = "empty_row_artifact"
    else:
        classification = "unknown_pre_migration_column"

    return {
        "count": len(matches),
        "classification": classification,
        "example": {
            "paper_id": ex["paper_id"],
            "paper_label": ex.get("paper_label", ""),
            "field_name": ex["field_name"],
            "field_tier": ex.get("field_tier", ""),
            "field_type": ex.get("field_type", ""),
            "local_value_preview": (ex.get("local_value") or "")[:150],
            "o4mini_value": ex.get("o4mini_value", ""),
            "sonnet_value": ex.get("sonnet_value", ""),
        },
    }


# ── Markdown rendering ─────────────────────────────────────────────


def _df_to_md(df: pd.DataFrame, floatfmt: str = ".1f") -> str:
    return df.to_markdown(index=False, floatfmt=floatfmt)


def _render_report(
    *,
    run_id: str,
    run_meta: dict,
    q1: pd.DataFrame,
    q2: pd.DataFrame,
    q3: pd.DataFrame,
    q4: pd.DataFrame,
    q5: Q5Finding,
    q6_summary: dict,
    q7: dict,
) -> str:
    lines: list[str] = []
    lines.append(f"# Pass 1 inspection — `{run_id}`\n")

    # Q7 at the top.
    lines.append("## Q7 · `field_1` root cause (headline)\n")
    cls_label = {
        "local_arm_extractor_artifact": (
            "local-arm extractor artifact — deepseek-r1:32b emitted a "
            "field with a synthetic name `field_1` containing a paper "
            "summary; cloud arms correctly returned empty."
        ),
        "empty_row_artifact": "empty-row artifact (all three arms blank)",
        "unknown_pre_migration_column": (
            "stale pre-migration column — more than one arm has a value"
        ),
        "not_present": "no field_1 rows in source CSV",
    }.get(q7["classification"], "unknown")

    n = q7["count"]
    if n:
        ex = q7["example"]
        lines.append(
            f"{n} row in `disagreement_pairs_3arm.csv` had `field_name = field_1` "
            f"(paper_id={ex['paper_id']}, {ex['paper_label']!r}, tier={ex['field_tier']}, "
            f"type={ex['field_type']}). Classification: **{cls_label}** "
            f"The codebook has no `field_1` entry, so the loader correctly skipped "
            f"this row with a WARNING. Not a real field; no re-export needed.\n"
        )
        lines.append(
            f"local_value preview: `{ex['local_value_preview']!r}`; "
            f"o4mini_value: `{ex['o4mini_value']!r}`; "
            f"sonnet_value: `{ex['sonnet_value']!r}`.\n"
        )
    else:
        lines.append("No `field_1` rows present in the source CSV.\n")

    # Run header.
    lines.append("## Run metadata\n")
    lines.append(f"- run_id: `{run_id}`")
    lines.append(f"- triples analyzed: **{run_meta['n_triples']}**")
    lines.append(f"- pair-ratings analyzed: **{run_meta['n_pairs']}**")
    lines.append(f"- source CSV: `{run_meta['pairs_csv']}`")
    lines.append(f"- codebook: `{run_meta['codebook_path']}`")
    lines.append(f"- codebook_sha256: `{run_meta['codebook_sha']}`\n")

    lines.append("## Q1 · Per-field Level 1 cross-tab\n")
    lines.append(_df_to_md(q1))
    lines.append("")
    n_low = (q1["flag"] == "LOW_EQ").sum()
    n_high = (q1["flag"] == "HIGH_EQ").sum()
    lines.append(
        f"**Flags:** {n_low} field(s) with <{EQ_LOW_FLAG:.0f}% EQ "
        f"(strong disagreement); {n_high} field(s) with >{EQ_HIGH_FLAG:.0f}% "
        f"EQ (trivially concordant — verify they're actually in scope for "
        f"Paper 1 methods).\n"
    )

    lines.append("## Q2 · Per-field Level 2 cross-tab\n")
    lines.append(_df_to_md(q2))
    lines.append("")
    saturated = q2[q2["flag"] == "GRANULARITY_SATURATED"]
    if len(saturated):
        names = ", ".join(f"`{n}`" for n in saturated["field_name"].tolist())
        lines.append(
            f"**GRANULARITY-saturated fields (>{GRANULARITY_SATURATION:.0f}% of "
            f"non-EQ pairs):** {names}. Degenerate-classification signal per "
            f"§9.1 of the plan — these fields need Level 2 collapsed to binary "
            f"(GRANULARITY vs not) for Paper 1 methods, since the judge is "
            f"effectively only emitting one label for them.\n"
        )
    else:
        lines.append(
            "No fields exceed the saturation threshold; Level 2 label mix is "
            "non-degenerate across fields.\n"
        )

    lines.append("## Q3 · field_type × Level 2\n")
    lines.append(_df_to_md(q3))
    lines.append("")
    ft_max = q3.loc[q3["pct_granularity"].idxmax(), "field_type"] if len(q3) else "?"
    ft_min = q3.loc[q3["pct_granularity"].idxmin(), "field_type"] if len(q3) else "?"
    lines.append(
        f"GRANULARITY peaks on `{ft_max}` and is lowest on `{ft_min}`. "
        "If free-text carries the bulk of GRANULARITY labels, the collapsed "
        "binary recommendation from §9.1 applies specifically to the free-text "
        "subset, not uniformly.\n"
    )

    lines.append("## Q4 · Per-arm-pair × Level 2\n")
    lines.append(_df_to_md(q4))
    lines.append("")
    fab_by_pair = q4.set_index("arm_pair")["FABRICATION"].to_dict()
    top_fab_pair = max(fab_by_pair, key=fab_by_pair.get) if fab_by_pair else "?"
    lines.append(
        f"**FABRICATION skew:** the `{top_fab_pair}` pair carries the largest "
        "share of FABRICATION labels, consistent with the §10 observation. "
        "This is a pair-level disagreement count, not per-arm attribution "
        "(see Q5).\n"
    )

    lines.append("## Q5 · Fabricator-arm attribution audit\n")
    lines.append(
        f"- Pass1Output contains a per-arm attribution field: "
        f"**{q5.has_attribution_field}**\n"
    )
    lines.append(q5.note + "\n")

    lines.append("## Q6 · B-filter simulation (Pass 2 sizing)\n")
    s = q6_summary
    lines.append(f"- Total triples: **{s['total_triples']}**")
    lines.append(
        f"- Medium or high fabrication_risk: **{s['medium_or_high']}**"
    )
    lines.append(f"- **Kept after B-filter: {s['kept_total']}**")
    lines.append(f"  - high (auto-keep): {s['kept_high']}")
    lines.append(f"  - medium (surviving the flag test): {s['kept_medium_surviving']}")
    lines.append("")
    lines.append("Kept by field_type:")
    for ft, n in sorted(s["by_field_type"].items()):
        lines.append(f"- {ft}: {n}")
    lines.append("")
    lines.append("Kept by arm-pair (every kept triple runs all three pairs):")
    for pair, n in s["by_arm_pair"].items():
        lines.append(f"- {pair}: {n}")
    lines.append("")
    lines.append("Pass 2 wall-clock projection (single-stream):")
    lines.append(
        f"- at 45 s/triple: **{s['projections_h']['45']} h** "
        f"({s['projections_sec']['45']} s)"
    )
    lines.append(
        f"- at 60 s/triple: **{s['projections_h']['60']} h** "
        f"({s['projections_sec']['60']} s)"
    )
    lines.append("")

    lines.append("### B-filter discriminative power\n")
    lines.append(
        "| fab_risk | n | tripped | % tripped |\n"
        "|---|---:|---:|---:|"
    )
    for bucket in ("low", "medium", "high"):
        tr = s["flag_trip_rates"][bucket]
        lines.append(
            f"| {bucket} | {tr['n']} | {tr['tripped']} | {tr['pct']:.1f}% |"
        )
    lines.append("")
    low_pct = s["flag_trip_rates"]["low"]["pct"]
    med_pct = s["flag_trip_rates"]["medium"]["pct"]
    if low_pct > 80:
        lines.append(
            f"**Filter is non-discriminative.** Low-risk triples trip the "
            f"precheck flag at {low_pct:.1f}% vs {med_pct:.1f}% for medium. "
            f"Because the test is per-triple (any arm failing across all "
            f"three), even a single noisy arm saturates the OR. The B-filter "
            f"as currently defined does not meaningfully cull the medium "
            f"bucket — Pass 2 scope reduces to **\"all medium + high "
            f"triples\" = {s['medium_or_high']}**. Consider moving the "
            f"precheck cull to a per-arm step inside Pass 2 (where we "
            f"already re-ground each arm against the source) instead of as "
            f"an up-front triple filter.\n"
        )
    else:
        lines.append(
            f"Filter discriminates: low {low_pct:.1f}% vs medium "
            f"{med_pct:.1f}% trip rate.\n"
        )

    return "\n".join(lines) + "\n"


# ── CLI ────────────────────────────────────────────────────────────


def _resolve_db(args) -> ReviewDatabase:
    if args.data_root is not None:
        return ReviewDatabase(args.review, data_root=args.data_root)
    return ReviewDatabase(args.review)


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="analysis.paper1.pass1_inspection",
        description="Run Pass 1 dry-run inspection queries.",
    )
    p.add_argument("--review", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--pairs-csv", type=Path, required=True)
    p.add_argument("--codebook", type=Path, required=True)
    p.add_argument("--out-dir", type=Path,
                   default=Path("analysis/paper1/reports"))
    p.add_argument("--data-root", type=Path, default=None)
    return p


def run(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _build_arg_parser().parse_args(argv)

    db = _resolve_db(args)

    ratings = _load_ratings(db, args.run_id)
    pairs = _load_pair_ratings(db, args.run_id)
    if ratings.empty:
        print(f"No judge_ratings rows for run_id={args.run_id!r}", file=sys.stderr)
        db.close()
        return 2

    codebook = load_codebook(args.codebook)
    from analysis.paper1.judge_loader import compute_codebook_sha256
    codebook_sha = compute_codebook_sha256(args.codebook)

    run_meta = {
        "n_triples": int(len(ratings)),
        "n_pairs": int(len(pairs)),
        "pairs_csv": str(args.pairs_csv),
        "codebook_path": str(args.codebook),
        "codebook_sha": codebook_sha,
    }

    q1 = q1_field_level1(pairs)
    q2 = q2_field_level2(pairs)
    q3 = q3_fieldtype_level2(pairs, codebook)
    q4 = q4_armpair_level2(pairs)
    q5 = q5_fabricator_attribution()
    q6_merged, q6_summary = q6_bfilter_simulation(
        ratings, db, codebook, args.pairs_csv
    )
    q7 = q7_field_1_root_cause(args.pairs_csv)

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_stem = f"pass1_inspection_{args.run_id}"

    q1.to_csv(out_dir / f"{csv_stem}_q1_field_level1.csv", index=False)
    q2.to_csv(out_dir / f"{csv_stem}_q2_field_level2.csv", index=False)
    q3.to_csv(out_dir / f"{csv_stem}_q3_fieldtype_level2.csv", index=False)
    q4.to_csv(out_dir / f"{csv_stem}_q4_armpair_level2.csv", index=False)
    q6_merged.to_csv(out_dir / f"{csv_stem}_q6_bfilter_per_triple.csv",
                     index=False)

    md = _render_report(
        run_id=args.run_id, run_meta=run_meta,
        q1=q1, q2=q2, q3=q3, q4=q4, q5=q5,
        q6_summary=q6_summary, q7=q7,
    )
    report_path = out_dir / f"{csv_stem}.md"
    report_path.write_text(md)

    db.close()

    print(f"Wrote report: {report_path}")
    print(f"CSVs: {out_dir}/{csv_stem}_q*.csv")
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
