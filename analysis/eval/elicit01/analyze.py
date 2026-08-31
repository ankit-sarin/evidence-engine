"""ELICIT-01 analysis — COPY provenance ladder vs INDEX selection validity.

Reads only the ELICIT-01 JSONL, the persisted unit maps and parsed_text. No model
calls, no DB access.

The provenance ladder is imported unmodified from `analysis/provenance/`:
`classify_span` + `PaperIndex`. Note the one place the classifier reads `value`
(verified in this task's pre-flight): with an EMPTY snippet it selects
ABSENCE_DECLARED when the value is a codebook absence sentinel and MISSING_SNIPPET
otherwise. With a non-empty snippet the ladder is purely lexical on the snippet and
`value` is carried, not scored. Values are therefore passed through faithfully so
NOT_FOUND fields land in the right non-taxonomy class.

INDEX quotes are materialized from the unit map AFTER the run. A materialized quote
is ANCHORED by construction, so reporting "100% ANCHORED" would measure the
materializer, not the model. The headline for INDEX is the **selection validity
rate**; the ladder is run over materialized quotes only as an integrity check that
the round-trip holds.
"""

from __future__ import annotations

import argparse
import json
import statistics as st
import sys
from collections import Counter, defaultdict
from pathlib import Path

from analysis.eval.elicit01.prompts import (
    CONDITION_COPY, CONDITION_INDEX, parse_fields, stated_fields,
)
from analysis.provenance.classifier import PaperIndex, classify_span

ABSENT = "NOT_FOUND"


def load_rows(store: Path) -> list[dict]:
    p = store / "elicit01.jsonl"
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def field_entries(row: dict) -> tuple[dict, str]:
    entries, path = parse_fields(row.get("raw_content"))
    return {e.get("field_name"): e for e in entries if e.get("field_name")}, path


def is_absent(value) -> bool:
    return not str(value or "").strip() or str(value).strip().upper() == ABSENT


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="ELICIT-01 analysis")
    ap.add_argument("--review", default="surgical_autonomy")
    ap.add_argument("--data-root", default="data")
    ap.add_argument("--out", default="analysis_summary.json")
    args = ap.parse_args(argv)

    review_dir = Path(args.data_root) / args.review
    store = review_dir / "eval" / "elicit01"
    rows = [r for r in load_rows(store) if r.get("ok")]
    truncated = [r for r in load_rows(store) if r.get("truncated")]
    unit_maps = json.loads((store / "unit_maps.json").read_text())
    manifest = json.loads((store / "manifest.json").read_text())
    in_corpus = {p["paper_id"]: p["in_corpus"] for p in manifest["papers"]}

    names = [f["name"] for f in stated_fields(review_dir / "extraction_codebook.yaml")]
    papers: dict[int, PaperIndex] = {}

    def index_for(pid: int) -> PaperIndex:
        if pid not in papers:
            fs = sorted((review_dir / "parsed_text").glob(f"{pid}_v*.md"),
                        key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
            papers[pid] = PaperIndex.build(pid, fs[-1].read_text())
        return papers[pid]

    copy_ladder: Counter = Counter()
    copy_ladder_by_field: dict[str, Counter] = defaultdict(Counter)
    idx_valid = idx_invalid = idx_empty_cited = idx_malformed = 0
    idx_value_no_citation: Counter = Counter()      # addition 2
    copy_value_no_quote: Counter = Counter()        # addition 2, COPY analogue
    abstain_asymmetry: list[dict] = []              # addition 1
    eyeball: list[dict] = []                        # addition 3
    idx_invalid_examples: list[dict] = []
    idx_quote_lens: list[int] = []
    idx_roundtrip_ok = idx_roundtrip_bad = 0
    notfound = {CONDITION_COPY: Counter(), CONDITION_INDEX: Counter()}
    present: dict[str, dict[int, bool]] = {CONDITION_COPY: {}, CONDITION_INDEX: {}}
    presence_pairs: dict[str, list[tuple[bool, bool]]] = defaultdict(list)
    parse_paths: Counter = Counter()
    per_paper: dict[int, dict] = defaultdict(dict)
    field_coverage: Counter = Counter()

    by_key = {(r["paper_id"], r["condition"]): r for r in rows}

    for (pid, cond), row in sorted(by_key.items()):
        entries, path = field_entries(row)
        parse_paths[f"{cond}:{path}"] += 1
        field_coverage[cond] += len([n for n in names if n in entries])
        for name in names:
            e = entries.get(name)
            if e is None:
                continue
            absent = is_absent(e.get("value"))
            present[cond][(pid, name)] = not absent
            if absent:
                notfound[cond][name] += 1
            if cond == CONDITION_COPY:
                quotes = e.get("quotes") or []
                if not isinstance(quotes, list):
                    quotes = [quotes]
                if not quotes:
                    if not absent:
                        copy_value_no_quote[name] += 1
                    cls = classify_span("", e.get("value"), index_for(pid)).taxonomy_class
                    copy_ladder[cls] += 1
                    copy_ladder_by_field[name][cls] += 1
                for q in quotes:
                    if not isinstance(q, str):
                        continue
                    cls = classify_span(q, e.get("value"), index_for(pid)).taxonomy_class
                    copy_ladder[cls] += 1
                    copy_ladder_by_field[name][cls] += 1
            else:
                raw_idx = e.get("unit_indices")
                units = unit_maps[str(pid)]["units"]
                if raw_idx is None:
                    idx_malformed += 1
                    continue
                if not isinstance(raw_idx, list):
                    raw_idx = [raw_idx]
                if not raw_idx:
                    if not absent:
                        idx_empty_cited += 1
                        idx_value_no_citation[name] += 1
                    continue
                for ix in raw_idx:
                    if not isinstance(ix, int) or isinstance(ix, bool):
                        idx_malformed += 1
                        idx_invalid_examples.append(
                            {"paper_id": pid, "field": name, "index": ix, "why": "not_int"})
                        continue
                    if 1 <= ix <= len(units):
                        idx_valid += 1
                        q = units[ix - 1]
                        idx_quote_lens.append(len(q))
                        cls = classify_span(q, e.get("value"), index_for(pid)).taxonomy_class
                        if cls == "ANCHORED":
                            idx_roundtrip_ok += 1
                        else:
                            idx_roundtrip_bad += 1
                    else:
                        idx_invalid += 1
                        idx_invalid_examples.append(
                            {"paper_id": pid, "field": name, "index": ix,
                             "n_units": len(units), "why": "out_of_range"})
        per_paper[pid][cond] = {
            "latency_s": row.get("latency_s"),
            "prompt_eval_count": row.get("prompt_eval_count"),
            "eval_count": row.get("eval_count"),
            "fields_returned": len([n for n in names if n in entries]),
            "parse_path": path,
        }

    # ── addition 1: paired abstention asymmetry ───────────────────────────
    for (pid, name), copy_present in present[CONDITION_COPY].items():
        idx_present = present[CONDITION_INDEX].get((pid, name))
        if idx_present is None or copy_present == idx_present:
            continue
        abstain_asymmetry.append({
            "paper_id": pid, "field": name,
            "copy_answered": copy_present, "index_answered": idx_present,
            "direction": "INDEX_abstains_COPY_answers" if copy_present
                         else "COPY_abstains_INDEX_answers",
        })

    # ── addition 3: verbatim side-by-side eyeball material ────────────────
    strat = {p["paper_id"]: p["length_stratum"] for p in manifest["papers"]}
    for (pid, cond), row in sorted(by_key.items()):
        if cond != CONDITION_COPY:
            continue
        irow = by_key.get((pid, CONDITION_INDEX))
        if irow is None:
            continue
        centries, _ = field_entries(row)
        ientries, _ = field_entries(irow)
        units = unit_maps[str(pid)]["units"]
        for name in names:
            ce, ie = centries.get(name), ientries.get(name)
            if not ce or not ie:
                continue
            cq = [q for q in (ce.get("quotes") or []) if isinstance(q, str)]
            ix = [i for i in (ie.get("unit_indices") or [])
                  if isinstance(i, int) and not isinstance(i, bool) and 1 <= i <= len(units)]
            if not cq or not ix:
                continue
            verdict = classify_span(cq[0], ce.get("value"), index_for(pid)).taxonomy_class
            eyeball.append({
                "paper_id": pid, "stratum": strat.get(pid), "field": name,
                "copy_value": ce.get("value"), "index_value": ie.get("value"),
                "values_agree": str(ce.get("value")).strip() == str(ie.get("value")).strip(),
                "copy_quote": cq[0],
                "copy_verdict": verdict,
                "index_unit_ids": ix,
                "index_materialized": [f"[S{i}] {units[i-1]}" for i in ix],
                "kind": "clean" if (verdict == "ANCHORED" and
                                    str(ce.get("value")).strip() == str(ie.get("value")).strip())
                        else "disagreement",
            })

    for name in names:
        for pid in {p for p, _ in present[CONDITION_COPY]}:
            a = present[CONDITION_COPY].get((pid, name))
            b = present[CONDITION_INDEX].get((pid, name))
            if a is not None and b is not None:
                presence_pairs[name].append((a, b))

    paired = [p for p in per_paper.values() if CONDITION_COPY in p and CONDITION_INDEX in p]
    lat = {c: [p[c]["latency_s"] for p in paired if p[c]["latency_s"]] for c in (CONDITION_COPY, CONDITION_INDEX)}
    pe = {c: [p[c]["prompt_eval_count"] for p in paired if p[c]["prompt_eval_count"]] for c in (CONDITION_COPY, CONDITION_INDEX)}

    idx_total = idx_valid + idx_invalid + idx_malformed
    summary = {
        "study": "ELICIT-01",
        "n_rows": len(rows),
        "n_papers_paired": len(paired),
        "truncated_rows": len(truncated),
        "stated_fields": names,
        "parse_paths": dict(parse_paths),
        "field_coverage_total": dict(field_coverage),
        "copy_ladder_pooled": dict(copy_ladder),
        "copy_ladder_pct": {k: round(100.0 * v / sum(copy_ladder.values()), 1)
                            for k, v in copy_ladder.items()} if copy_ladder else {},
        "copy_ladder_by_field": {k: dict(v) for k, v in copy_ladder_by_field.items()},
        "index_selection": {
            "indices_total": idx_total,
            "valid": idx_valid,
            "out_of_range": idx_invalid,
            "malformed": idx_malformed,
            "validity_rate_pct": round(100.0 * idx_valid / idx_total, 1) if idx_total else None,
            "empty_citation_with_value": idx_empty_cited,
            "value_without_citation_by_field": dict(idx_value_no_citation),
            "materialized_quote_chars_median": int(st.median(idx_quote_lens)) if idx_quote_lens else None,
            "roundtrip_anchored": idx_roundtrip_ok,
            "roundtrip_not_anchored": idx_roundtrip_bad,
            "note": ("materialized quotes are ANCHORED by construction; roundtrip_* is an "
                     "integrity check on the materializer, not a model result. The headline "
                     "is validity_rate_pct."),
            "invalid_examples": idx_invalid_examples[:25],
        },
        "not_found_by_field": {c: dict(v) for c, v in notfound.items()},
        "value_without_citation": {
            "INDEX_by_field": dict(idx_value_no_citation),
            "INDEX_total": sum(idx_value_no_citation.values()),
            "COPY_analogue_by_field": dict(copy_value_no_quote),
            "COPY_analogue_total": sum(copy_value_no_quote.values()),
            "note": ("INDEX: a value was returned with no unit cited. Excluded from the "
                     "validity denominator (which counts indices, not fields) and reported "
                     "beside it. The COPY analogue is a value with an empty quote list, which "
                     "reaches the classifier's empty-snippet branch and lands in "
                     "MISSING_SNIPPET (or ABSENCE_DECLARED if the value is a codebook "
                     "absence sentinel)."),
        },
        "abstention_asymmetry": {
            "index_abstains_copy_answers": [a for a in abstain_asymmetry
                                            if a["direction"] == "INDEX_abstains_COPY_answers"],
            "copy_abstains_index_answers": [a for a in abstain_asymmetry
                                            if a["direction"] == "COPY_abstains_INDEX_answers"],
            "n_index_abstains": sum(1 for a in abstain_asymmetry
                                    if a["direction"] == "INDEX_abstains_COPY_answers"),
            "n_copy_abstains": sum(1 for a in abstain_asymmetry
                                   if a["direction"] == "COPY_abstains_INDEX_answers"),
        },
        "eyeball": eyeball,
        "presence_agreement_by_field": {
            n: {"pairs": len(v),
                "agree": sum(1 for a, b in v if a == b),
                "agree_pct": round(100.0 * sum(1 for a, b in v if a == b) / len(v), 1) if v else None,
                "copy_only": sum(1 for a, b in v if a and not b),
                "index_only": sum(1 for a, b in v if b and not a)}
            for n, v in presence_pairs.items()},
        "latency": {c: {"median_s": round(st.median(v), 1), "total_h": round(sum(v) / 3600, 2)}
                    for c, v in lat.items() if v},
        "prompt_inflation": {
            "copy_median_tokens": int(st.median(pe[CONDITION_COPY])) if pe[CONDITION_COPY] else None,
            "index_median_tokens": int(st.median(pe[CONDITION_INDEX])) if pe[CONDITION_INDEX] else None,
            "index_over_copy_median": (
                round(st.median(pe[CONDITION_INDEX]) / st.median(pe[CONDITION_COPY]), 3)
                if pe[CONDITION_COPY] and pe[CONDITION_INDEX] else None),
            "calibration_note": (
                "MEASURED token inflation of the INDEX rendering over COPY on the same paper. "
                "The manifest fit check predicted 1.03-1.05x from a character-length ratio; "
                "the measured token ratio is higher because [Sn] markers tokenize worse than "
                "they read. A future input-fit guard estimating from characters will "
                "under-predict INDEX-style prompts by roughly this margin."),
        },
        "non_corpus_papers": {
            "paper_ids": sorted(p for p, v in in_corpus.items() if not v),
            "note": "FT_SCREENED_OUT, retained for CAPTURE-01 comparability; see manifest",
        },
        "per_paper": {str(k): v for k, v in sorted(per_paper.items())},
    }

    out = store / args.out
    out.write_text(json.dumps(summary, indent=1))
    print(f"ELICIT-01: {len(rows)} ok rows, {len(paired)} paired papers, "
          f"{len(truncated)} truncated")
    print(f"  COPY ladder : {summary['copy_ladder_pct']}")
    ix = summary["index_selection"]
    print(f"  INDEX       : validity {ix['validity_rate_pct']}% "
          f"({ix['valid']}/{ix['indices_total']}), out-of-range {ix['out_of_range']}, "
          f"malformed {ix['malformed']}")
    print(f"  wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
