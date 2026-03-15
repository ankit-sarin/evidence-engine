"""Human-readable concordance reports: terminal, CSV, and HTML."""

import csv
import html as html_mod
import logging
import math
from datetime import datetime, timezone
from pathlib import Path

from engine.analysis.concordance import ConcordanceReport, Disagreement
from engine.analysis.metrics import FieldSummary

logger = logging.getLogger(__name__)

# ── Tier lookup ──────────────────────────────────────────────────────

_TIER_CACHE: dict[str, int] | None = None


def _get_tier_map() -> dict[str, int]:
    global _TIER_CACHE
    if _TIER_CACHE is None:
        try:
            from engine.core.review_spec import load_review_spec
            spec = load_review_spec("review_specs/surgical_autonomy_v1.yaml")
            _TIER_CACHE = {f.name: f.tier for f in spec.extraction_schema.fields}
        except Exception:
            _TIER_CACHE = {}
    return _TIER_CACHE


def _tier(field_name: str) -> int:
    return _get_tier_map().get(field_name, 0)


def _is_nan(v: float) -> bool:
    return math.isnan(v)


def _fmt_kappa(k: float) -> str:
    return f"{k:.3f}" if not _is_nan(k) else "N/A"


def _fmt_pct(p: float) -> str:
    return f"{p:.1%}" if not _is_nan(p) else "N/A"


def _fmt_ci(lo: float, hi: float) -> str:
    if _is_nan(lo):
        return "N/A"
    return f"[{lo:.2f},{hi:.2f}]"


def _short_arm(arm: str) -> str:
    """Abbreviate arm name for terminal display."""
    _MAP = {
        "local": "Local",
        "openai_o4_mini_high": "o4-mini",
        "anthropic_sonnet_4_6": "Sonnet",
    }
    return _MAP.get(arm, arm[:12])


def _sorted_fields(summaries: dict[str, FieldSummary]) -> list[str]:
    """Sort field names by tier then alphabetically."""
    return sorted(summaries.keys(), key=lambda f: (_tier(f), f))


# ── Terminal output ──────────────────────────────────────────────────


def print_summary(reports: list[ConcordanceReport]) -> None:
    """Print compact summary table to stdout."""
    if not reports:
        print("No concordance reports to display.")
        return

    truncated = False
    display_reports = reports
    if len(reports) > 3:
        display_reports = reports[:3]
        truncated = True

    # Collect all field names across all reports
    all_fields: set[str] = set()
    for r in display_reports:
        all_fields.update(r.field_summaries.keys())
    sorted_fields = sorted(all_fields, key=lambda f: (_tier(f), f))

    # Header
    pair_labels = [
        f"{_short_arm(r.arm_a)} v {_short_arm(r.arm_b)}"
        for r in display_reports
    ]

    # Build column widths
    field_w = 30
    tier_w = 4
    pair_w = 22  # per pair: κ(6) + %Agr(6) + Amb(4) + spacing

    print()
    # Title line
    header_parts = [f"{'Field':<{field_w}}", f"{'T':>{tier_w}}"]
    for label in pair_labels:
        header_parts.append(f" | {label:^{pair_w}}")
    print("".join(header_parts))

    # Sub-header
    sub_parts = [f"{'':<{field_w}}", f"{'':>{tier_w}}"]
    for _ in display_reports:
        sub_parts.append(f" | {'κ':>6} {'%Agr':>6} {'Amb':>4}  ")
    print("".join(sub_parts))

    print("-" * (field_w + tier_w + len(display_reports) * (pair_w + 3)))

    # Data rows
    for fname in sorted_fields:
        t = _tier(fname)
        parts = [f"{fname:<{field_w}}", f"{t:>{tier_w}}"]
        for r in display_reports:
            fs = r.field_summaries.get(fname)
            if fs:
                k = _fmt_kappa(fs.kappa)
                p = _fmt_pct(fs.percent_agreement)
                a = str(fs.n_ambiguous)
                parts.append(f" | {k:>6} {p:>6} {a:>4}  ")
            else:
                parts.append(f" | {'---':>6} {'---':>6} {'---':>4}  ")
        print("".join(parts))

    # Footer: per-pair summary
    print("-" * (field_w + tier_w + len(display_reports) * (pair_w + 3)))

    footer_parts = [f"{'Mean κ (categorical)':<{field_w}}", f"{'':>{tier_w}}"]
    for r in display_reports:
        cat_kappas = [
            fs.kappa for fs in r.field_summaries.values()
            if not _is_nan(fs.kappa) and _is_categorical(fs.field_name)
        ]
        mean_k = sum(cat_kappas) / len(cat_kappas) if cat_kappas else float("nan")
        total_amb = sum(fs.n_ambiguous for fs in r.field_summaries.values())
        footer_parts.append(f" | {_fmt_kappa(mean_k):>6} {'':>6} {total_amb:>4}  ")
    print("".join(footer_parts))

    # Paper counts
    for r in display_reports:
        label = f"{_short_arm(r.arm_a)} v {_short_arm(r.arm_b)}"
        print(f"  {label}: {r.n_papers} shared, "
              f"{r.n_papers_a_only} only-A, {r.n_papers_b_only} only-B, "
              f"{len(r.disagreements)} disagreements")

    if truncated:
        print(f"\n  ({len(reports) - 3} additional arm-pairs omitted — see full output in analysis/)")

    print()


def _is_categorical(field_name: str) -> bool:
    try:
        from engine.analysis.normalize import _get_field_def
        fd = _get_field_def(field_name)
        return fd is not None and fd.type == "categorical"
    except Exception:
        return False


# ── CSV outputs ──────────────────────────────────────────────────────


def write_report(reports: list[ConcordanceReport], output_dir: Path) -> None:
    """Write concordance_summary.csv, disagreements.csv, and concordance_report.html."""
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_summary_csv(reports, output_dir / "concordance_summary.csv")
    _write_disagreements_csv(reports, output_dir / "disagreements.csv")
    _write_html(reports, output_dir / "concordance_report.html")
    logger.info("Reports written to %s", output_dir)


def _write_summary_csv(reports: list[ConcordanceReport], path: Path) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "arm_a", "arm_b", "field_name", "tier",
            "n", "n_agree", "n_disagree", "n_ambiguous",
            "pct_agreement", "kappa", "kappa_ci_lower", "kappa_ci_upper",
        ])
        for r in reports:
            for fname in _sorted_fields(r.field_summaries):
                fs = r.field_summaries[fname]
                writer.writerow([
                    r.arm_a, r.arm_b, fname, _tier(fname),
                    fs.n, fs.n_match, fs.n_mismatch, fs.n_ambiguous,
                    round(fs.percent_agreement, 4) if not _is_nan(fs.percent_agreement) else "",
                    round(fs.kappa, 4) if not _is_nan(fs.kappa) else "",
                    round(fs.ci_lower, 4) if not _is_nan(fs.ci_lower) else "",
                    round(fs.ci_upper, 4) if not _is_nan(fs.ci_upper) else "",
                ])


def _write_disagreements_csv(reports: list[ConcordanceReport], path: Path) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "arm_a", "arm_b", "paper_id", "field_name", "tier",
            "value_a", "value_b", "score", "detail",
        ])
        for r in reports:
            for d in r.disagreements:
                writer.writerow([
                    r.arm_a, r.arm_b, d.paper_id, d.field_name, _tier(d.field_name),
                    d.value_a or "", d.value_b or "", d.result, d.detail,
                ])


# ── HTML report ──────────────────────────────────────────────────────


def _esc(text: str | None) -> str:
    return html_mod.escape(str(text)) if text else "&mdash;"


def _kappa_class(k: float) -> str:
    if _is_nan(k):
        return "k-na"
    if k >= 0.80:
        return "k-good"
    if k >= 0.60:
        return "k-mod"
    return "k-poor"


def _write_html(reports: list[ConcordanceReport], path: Path) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Collect all fields
    all_fields: set[str] = set()
    for r in reports:
        all_fields.update(r.field_summaries.keys())
    sorted_fields = sorted(all_fields, key=lambda f: (_tier(f), f))

    html_parts = [f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Concordance Report</title>
<style>
  :root {{
    --bg: #EEF5F4; --text: #2C2C2C; --teal: #0A5E56; --terra: #B85D3A;
    --border: #c8d8d6; --card-bg: #fff;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'IBM Plex Sans', system-ui, sans-serif; background: var(--bg);
         color: var(--text); padding: 24px; max-width: 1200px; margin: 0 auto; }}
  h1 {{ font-family: 'Fraunces', serif; color: var(--teal); margin-bottom: 8px; font-size: 1.6rem; }}
  .meta {{ color: #666; font-size: 0.85rem; margin-bottom: 20px; }}
  .card {{ background: var(--card-bg); border-radius: 8px; padding: 20px; margin-bottom: 20px;
           box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
  .card h2 {{ font-size: 1.1rem; color: var(--teal); margin-bottom: 12px; cursor: pointer; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 0.85rem; }}
  th, td {{ padding: 6px 10px; border-bottom: 1px solid var(--border); text-align: left; }}
  th {{ background: var(--teal); color: #fff; font-weight: 600; position: sticky; top: 0; }}
  tr:hover {{ background: #e8f0ef; }}
  .tier-sep td {{ border-top: 2px solid var(--teal); }}
  .k-good {{ color: #1a7a1a; font-weight: 600; }}
  .k-mod {{ color: #b8860b; font-weight: 600; }}
  .k-poor {{ color: #c0392b; font-weight: 600; }}
  .k-na {{ color: #999; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .disag-table {{ font-size: 0.8rem; }}
  .disag-table td {{ max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  .badge {{ display: inline-block; padding: 2px 6px; border-radius: 3px; font-size: 0.75rem; font-weight: 600; }}
  .badge-mis {{ background: #fce4e4; color: #c0392b; }}
  .badge-amb {{ background: #fef9e7; color: #b8860b; }}
  details > summary {{ cursor: pointer; color: var(--teal); font-weight: 600; margin: 8px 0; }}
  .footer {{ text-align: center; color: #999; font-size: 0.8rem; margin-top: 24px; }}
</style>
</head>
<body>
<h1>Concordance Analysis Report</h1>
<p class="meta">Generated {_esc(now)}</p>
"""]

    # Summary table per arm-pair
    for r in reports:
        pair_label = f"{_esc(r.arm_a)} vs {_esc(r.arm_b)}"
        html_parts.append(f"""
<div class="card">
<h2>{pair_label}</h2>
<p style="font-size:0.85rem;margin-bottom:12px;">
  {r.n_papers} shared papers &middot; {r.n_papers_a_only} only in A &middot;
  {r.n_papers_b_only} only in B &middot; {len(r.disagreements)} disagreements
</p>
<table>
<tr><th>Field</th><th class="num">Tier</th><th class="num">&kappa;</th>
<th class="num">95% CI</th><th class="num">% Agree</th>
<th class="num">n</th><th class="num">Match</th><th class="num">Mis</th><th class="num">Amb</th></tr>
""")
        prev_tier = 0
        for fname in _sorted_fields(r.field_summaries):
            fs = r.field_summaries[fname]
            t = _tier(fname)
            tier_class = ' class="tier-sep"' if t != prev_tier and prev_tier != 0 else ""
            prev_tier = t
            kc = _kappa_class(fs.kappa)
            k_str = _fmt_kappa(fs.kappa)
            ci_str = _fmt_ci(fs.ci_lower, fs.ci_upper)
            p_str = _fmt_pct(fs.percent_agreement)
            html_parts.append(
                f'<tr{tier_class}><td>{_esc(fname)}</td>'
                f'<td class="num">{t}</td>'
                f'<td class="num {kc}">{k_str}</td>'
                f'<td class="num">{ci_str}</td>'
                f'<td class="num">{p_str}</td>'
                f'<td class="num">{fs.n}</td>'
                f'<td class="num">{fs.n_match}</td>'
                f'<td class="num">{fs.n_mismatch}</td>'
                f'<td class="num">{fs.n_ambiguous}</td></tr>\n'
            )

        html_parts.append("</table>\n")

        # Disagreements as expandable section
        if r.disagreements:
            html_parts.append(f"""
<details>
<summary>Disagreements ({len(r.disagreements)})</summary>
<table class="disag-table">
<tr><th>Paper</th><th>Field</th><th>Tier</th><th>Value A</th><th>Value B</th><th>Score</th><th>Detail</th></tr>
""")
            for d in r.disagreements:
                badge_cls = "badge-mis" if d.result == "MISMATCH" else "badge-amb"
                html_parts.append(
                    f'<tr><td>{d.paper_id}</td><td>{_esc(d.field_name)}</td>'
                    f'<td class="num">{_tier(d.field_name)}</td>'
                    f'<td title="{_esc(d.value_a)}">{_esc(_trunc(d.value_a, 50))}</td>'
                    f'<td title="{_esc(d.value_b)}">{_esc(_trunc(d.value_b, 50))}</td>'
                    f'<td><span class="badge {badge_cls}">{d.result}</span></td>'
                    f'<td title="{_esc(d.detail)}">{_esc(_trunc(d.detail, 60))}</td></tr>\n'
                )
            html_parts.append("</table>\n</details>\n")

        html_parts.append("</div>\n")

    html_parts.append("""
<p class="footer">Surgical Evidence Engine &middot; Concordance Analysis</p>
</body>
</html>
""")

    path.write_text("".join(html_parts))


def _trunc(v: str | None, maxlen: int = 40) -> str:
    if v is None:
        return ""
    return v[:maxlen] + "..." if len(v) > maxlen else v
