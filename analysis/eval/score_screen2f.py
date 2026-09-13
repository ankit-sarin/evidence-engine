"""SCREEN-AUTH-01 Phase 2f smoke — scoring, disagreement lists, descriptive read-out.

Reads the run directory `run_screen2f` wrote, the exported papers file and the
86-paper PI workbook. Writes CSV, JSON and Markdown under --out-dir. Calls no
model and opens no database.

Definitions, fixed here so the report can cite them:

  * final outcome: primary OUT or FLAGGED stands; primary IN takes the
    verifier's outcome (IN if it includes, FLAGGED otherwise). ERROR is a call
    that failed after the client's own retries.
  * PI-consistent final: IN for a PI include, OUT for a PI exclude. FLAGGED is
    never PI-consistent; it is a deferral to a human.
  * sensitivity is over PI includes, specificity over PI excludes, each under
    three treatments of FLAGGED: counted as include, counted as exclude, and
    dropped (decided papers only). ERROR papers are dropped from all three and
    counted.
  * d1≠d2 rate: papers whose two primary passes both parsed and disagreed,
    over papers whose two passes both parsed.
  * verifier overturn rate: primary-IN papers the verifier did not keep IN,
    over primary-IN papers with a verifier result.
  * agreement between arms: exact match on the three-valued outcome, with
    Cohen's kappa over the categories observed.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from collections import Counter
from pathlib import Path

from analysis.eval import screen2f as lib

PAIRS = (("A", "B"), ("B", "C"), ("A", "C"))
OUTCOME_ROWS = (lib.IN, lib.OUT, lib.FLAGGED, lib.ERROR)
DEFAULT_WORKBOOK = Path("data/surgical_autonomy/adjudication/specialty_rescreen_flagged_86.xlsx")


# ── inputs ────────────────────────────────────────────────────────────


def load_labels(workbook: Path) -> dict[int, dict]:
    import openpyxl

    wb = openpyxl.load_workbook(workbook, read_only=True, data_only=True)
    rows = list(wb.worksheets[0].iter_rows(values_only=True))
    wb.close()
    header = [str(h) for h in rows[0]]
    out = {}
    for r in rows[1:]:
        if not r or r[0] is None:
            continue
        d = dict(zip(header, r))
        out[int(d["paper_id"])] = {
            "ee": d["ee_identifier"], "title": d["title"] or "", "pi": str(d["PI_decision"]).strip().lower(),
            "pi_notes": d["PI_notes"] or "", "category": d["verifier_reason_category"] or "",
            "wb_primary": d["primary_decision"], "wb_primary_reasoning": d["primary_reasoning"] or "",
            "wb_verifier": d["verifier_decision"], "wb_verifier_reasoning": d["verifier_reasoning"] or "",
        }
    return out


def load_arm(run_dir: Path, arm: str) -> tuple[dict[int, dict], dict[int, dict]]:
    d = Path(run_dir) / f"arm_{arm}"
    primary, p_open = lib.Checkpoint(d / "primary.jsonl").load()
    verifier, v_open = lib.Checkpoint(d / "verifier.jsonl").load()
    if p_open or v_open:
        raise ValueError(f"arm {arm}: unfinished claims primary={sorted(p_open)} verifier={sorted(v_open)}")
    return primary, verifier


# ── per-arm records ───────────────────────────────────────────────────


def _call(rec: dict | None, i: int) -> dict:
    if not rec or len(rec["calls"]) <= i:
        return {}
    return rec["calls"][i]


def arm_records(labels: dict[int, dict], primary: dict[int, dict], verifier: dict[int, dict]) -> dict[int, dict]:
    out = {}
    for pid in labels:
        p = primary.get(pid)
        v = verifier.get(pid)
        if p is None:
            out[pid] = None
            continue
        v_counts = v is not None and not v.get("forced")
        final = lib.final_outcome(p["outcome"], v["outcome"] if v_counts else None)
        c1, c2, cv = _call(p, 0), _call(p, 1), _call(v, 0)
        out[pid] = {
            "primary_outcome": p["outcome"], "d1": p["d1"], "d2": p["d2"], "d1_ne_d2": p["d1_ne_d2"],
            "d1_rationale": c1.get("rationale"), "d2_rationale": c2.get("rationale"),
            "verifier_outcome": v["outcome"] if v else None, "verifier_decision": v.get("decision") if v else None,
            "verifier_rationale": cv.get("rationale"), "verifier_forced": bool(v and v.get("forced")),
            "final": final,
            "primary_parse_error": p["parse_error"], "primary_error": p["error"],
            "verifier_parse_error": bool(v and v["parse_error"]), "verifier_error": bool(v and v["error"]),
            "calls": list(p["calls"]) + (list(v["calls"]) if v else []),
            "reprocessed": bool(p.get("reprocessed_after_interrupt") or (v and v.get("reprocessed_after_interrupt"))),
        }
    return out


def check_call_pattern(labels: dict[int, dict], primary: dict[int, dict], verifier: dict[int, dict]) -> list[str]:
    """Gate 2 for one arm: the runner's call pattern, every paper once, verifier on IN only."""
    problems = []
    for pid in labels:
        p = primary.get(pid)
        if p is None:
            problems.append(f"paper {pid}: no primary result")
            continue
        calls = p["calls"]
        passes = [c["call"].get("pass_number") for c in calls]
        roles = {c["call"].get("role") for c in calls}
        first_failed = bool(calls and (calls[0].get("parse_error") or calls[0].get("error")))
        want = [1] if first_failed else [1, 2]
        if passes != want or roles != {lib.PRIMARY}:
            problems.append(f"paper {pid}: primary calls {passes} roles {sorted(roles)}, expected {want}")
        v = verifier.get(pid)
        if p["outcome"] == lib.IN and v is None:
            problems.append(f"paper {pid}: primary IN with no verifier result")
        if v is not None:
            if v.get("forced"):
                problems.append(f"paper {pid}: forced verifier call in a full run")
            if p["outcome"] != lib.IN:
                problems.append(f"paper {pid}: verifier called on primary {p['outcome']}")
            if [c["call"].get("role") for c in v["calls"]] != [lib.VERIFIER]:
                problems.append(f"paper {pid}: verifier calls {[c['call'].get('role') for c in v['calls']]}")
    extra = sorted(set(primary) - set(labels)) + sorted(set(verifier) - set(labels))
    if extra:
        problems.append(f"results for papers outside the labelled set: {extra}")
    return problems


# ── metrics ───────────────────────────────────────────────────────────


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float | None, float | None]:
    if n == 0:
        return None, None
    p = k / n
    den = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / den
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return round(max(0.0, centre - half), 4), round(min(1.0, centre + half), 4)


def confusion(recs: dict[int, dict], labels: dict[int, dict]) -> dict[str, dict[str, int]]:
    t = {o: {"include": 0, "exclude": 0} for o in OUTCOME_ROWS}
    t["MISSING"] = {"include": 0, "exclude": 0}
    for pid, r in recs.items():
        row = "MISSING" if r is None or r["final"] is None else r["final"]
        t[row][labels[pid]["pi"]] += 1
    return t


def binary_metrics(recs: dict[int, dict], labels: dict[int, dict], flagged_as: str | None) -> dict:
    """Sensitivity/specificity with FLAGGED counted as `flagged_as`, or dropped when None."""
    tp = fn = tn = fp = dropped_flagged = dropped_other = 0
    for pid, r in recs.items():
        final = None if r is None else r["final"]
        if final == lib.FLAGGED:
            if flagged_as is None:
                dropped_flagged += 1
                continue
            call = flagged_as
        elif final == lib.IN:
            call = "include"
        elif final == lib.OUT:
            call = "exclude"
        else:
            dropped_other += 1
            continue
        pi = labels[pid]["pi"]
        if pi == "include":
            tp, fn = tp + (call == "include"), fn + (call == "exclude")
        else:
            tn, fp = tn + (call == "exclude"), fp + (call == "include")
    sens = tp / (tp + fn) if tp + fn else None
    spec = tn / (tn + fp) if tn + fp else None
    return {
        "flagged_as": flagged_as or "dropped", "tp": tp, "fn": fn, "tn": tn, "fp": fp,
        "dropped_flagged": dropped_flagged, "dropped_error_or_missing": dropped_other,
        "sensitivity": None if sens is None else round(sens, 4), "sensitivity_ci95": wilson(tp, tp + fn),
        "specificity": None if spec is None else round(spec, 4), "specificity_ci95": wilson(tn, tn + fp),
        "balanced_accuracy": None if sens is None or spec is None else round((sens + spec) / 2, 4),
    }


def rates(recs: dict[int, dict]) -> dict:
    present = [r for r in recs.values() if r is not None]
    n = len(present)
    both = [r for r in present if r["d1"] is not None and r["d2"] is not None]
    prim_in = [r for r in present if r["primary_outcome"] == lib.IN and r["verifier_outcome"] is not None]
    calls = [c for r in present for c in r["calls"]]
    parse_calls = sum(1 for c in calls if c.get("parse_error"))
    return {
        "papers": n,
        "final_counts": dict(Counter(r["final"] for r in present)),
        "primary_counts": dict(Counter(r["primary_outcome"] for r in present)),
        "flag_rate": round(sum(r["final"] == lib.FLAGGED for r in present) / n, 4) if n else None,
        "d1_ne_d2": sum(r["d1_ne_d2"] for r in both), "d1_ne_d2_of": len(both),
        "d1_ne_d2_rate": round(sum(r["d1_ne_d2"] for r in both) / len(both), 4) if both else None,
        "verifier_overturned": sum(r["verifier_outcome"] != lib.IN for r in prim_in), "verifier_ran": len(prim_in),
        "verifier_overturn_rate": round(sum(r["verifier_outcome"] != lib.IN for r in prim_in) / len(prim_in), 4)
        if prim_in else None,
        "calls": len(calls), "parse_error_calls": parse_calls,
        "parse_error_call_rate": round(parse_calls / len(calls), 4) if calls else None,
        "papers_with_parse_error": sum(r["primary_parse_error"] or r["verifier_parse_error"] for r in present),
        "error_calls": sum(1 for c in calls if c.get("error")),
        "reprocessed_after_interrupt": sum(r["reprocessed"] for r in present),
    }


def cohen_kappa(xs: list[str], ys: list[str]) -> float | None:
    n = len(xs)
    if n == 0:
        return None
    cats = sorted(set(xs) | set(ys))
    po = sum(a == b for a, b in zip(xs, ys)) / n
    cx, cy = Counter(xs), Counter(ys)
    pe = sum(cx[c] * cy[c] for c in cats) / (n * n)
    if pe == 1:
        return 1.0 if po == 1 else None
    return round((po - pe) / (1 - pe), 4)


def agreement(rx: dict[int, dict], ry: dict[int, dict], key: str) -> dict:
    pids = [p for p in rx if rx[p] is not None and ry.get(p) is not None
            and rx[p][key] is not None and ry[p][key] is not None]
    xs, ys = [rx[p][key] for p in pids], [ry[p][key] for p in pids]
    agree = sum(a == b for a, b in zip(xs, ys))
    table = Counter(f"{a}->{b}" for a, b in zip(xs, ys))
    return {"n": len(pids), "agree": agree, "pct": round(agree / len(pids), 4) if pids else None,
            "kappa": cohen_kappa(xs, ys), "table": dict(sorted(table.items()))}


# ── rendering ─────────────────────────────────────────────────────────


def one_line(text: str | None, limit: int = 200) -> str:
    if not text:
        return ""
    s = re.sub(r"\s+", " ", str(text)).strip()
    return s if len(s) <= limit else s[: limit - 1] + "…"


def final_rationale(r: dict | None) -> str:
    """The rationale of the call that decided the final outcome."""
    if r is None:
        return ""
    if r["primary_outcome"] == lib.IN and r["verifier_outcome"] is not None:
        return "verifier: " + one_line(r["verifier_rationale"])
    if r["d1_ne_d2"]:
        return f"p1 {r['d1']}: {one_line(r['d1_rationale'], 120)} | p2 {r['d2']}: {one_line(r['d2_rationale'], 120)}"
    return "primary: " + one_line(r["d1_rationale"])


def pi_consistent(r: dict | None, pi: str) -> bool:
    return r is not None and r["final"] == (lib.IN if pi == "include" else lib.OUT)


def disagreement_lists(labels: dict[int, dict], recs: dict[str, dict[int, dict]]) -> dict[str, list[int]]:
    ids = list(labels)
    return {
        "any_arm_vs_PI": [p for p in ids if any(not pi_consistent(recs[a][p], labels[p]["pi"]) for a in lib.ARMS)],
        "A_vs_B": [p for p in ids if (recs["A"][p] or {}).get("final") != (recs["B"][p] or {}).get("final")],
        "B_vs_C": [p for p in ids if (recs["B"][p] or {}).get("final") != (recs["C"][p] or {}).get("final")],
    }


def disagreement_rows(pids: list[int], labels: dict[int, dict], recs: dict[str, dict[int, dict]]) -> list[dict]:
    rows = []
    for p in pids:
        lab = labels[p]
        row = {"paper_id": p, "ee_identifier": lab["ee"], "title": lab["title"], "PI_decision": lab["pi"],
               "PI_notes": one_line(lab["pi_notes"], 400)}
        for a in lib.ARMS:
            r = recs[a][p]
            row[f"{a}_primary"] = None if r is None else r["primary_outcome"]
            row[f"{a}_final"] = None if r is None else r["final"]
            row[f"{a}_rationale"] = final_rationale(r)
        rows.append(row)
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        if not rows:
            f.write("")
            return
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def _md_cell(v) -> str:
    return "" if v is None else str(v).replace("|", "\\|").replace("\n", " ")


def md_table(rows: list[dict], cols: list[str]) -> str:
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    out += ["| " + " | ".join(_md_cell(r.get(c)) for c in cols) + " |" for r in rows]
    return "\n".join(out)


def per_arm_rows(labels: dict[int, dict], recs: dict[int, dict]) -> list[dict]:
    rows = []
    for p, lab in labels.items():
        r = recs[p]
        calls = [] if r is None else r["calls"]
        prim = [c["call"] for c in calls if c["call"].get("role") == lib.PRIMARY]
        ver = [c["call"] for c in calls if c["call"].get("role") == lib.VERIFIER]
        rows.append({
            "paper_id": p, "ee_identifier": lab["ee"], "PI_decision": lab["pi"],
            "d1": r and r["d1"], "d1_rationale": one_line(r and r["d1_rationale"], 1000),
            "d2": r and r["d2"], "d2_rationale": one_line(r and r["d2_rationale"], 1000),
            "d1_ne_d2": r and r["d1_ne_d2"], "primary_outcome": r and r["primary_outcome"],
            "verifier_decision": r and r["verifier_decision"],
            "verifier_rationale": one_line(r and r["verifier_rationale"], 1000),
            "final_outcome": r and r["final"],
            "primary_parse_error": r and r["primary_parse_error"], "primary_error": r and r["primary_error"],
            "verifier_parse_error": r and r["verifier_parse_error"], "verifier_error": r and r["verifier_error"],
            "primary_request_hash": prim[0].get("request_hash") if prim else None,
            "verifier_request_hash": ver[0].get("request_hash") if ver else None,
            "primary_wall_s": round(sum(c.get("wall_s", 0) for c in prim), 3),
            "verifier_wall_s": round(sum(c.get("wall_s", 0) for c in ver), 3) if ver else None,
            "reprocessed_after_interrupt": r and r["reprocessed"],
        })
    return rows


# ── descriptive: the workbook's own columns vs arm A (2d) ─────────────


def workbook_vs_arm_a(labels: dict[int, dict], rec_a: dict[int, dict]) -> dict:
    prim_x = Counter()
    ver_x = Counter()
    prefix = Counter()
    for p, lab in labels.items():
        r = rec_a[p]
        if r is None:
            continue
        prim_x[f"wb {lab['wb_primary']} / A d1 {r['d1']} / A d2 {r['d2']}"] += 1
        if r["verifier_decision"] is not None:
            ver_x[f"wb {lab['wb_verifier']} / A verifier {r['verifier_decision']}"] += 1
        else:
            ver_x[f"wb {lab['wb_verifier']} / A verifier not called (primary {r['primary_outcome']})"] += 1
        wbp, wbv = one_line(lab["wb_primary_reasoning"], 10_000), one_line(lab["wb_verifier_reasoning"], 10_000)
        for name, wb_text, mine in (("primary_vs_d1", wbp, r["d1_rationale"]),
                                    ("primary_vs_d2", wbp, r["d2_rationale"]),
                                    ("verifier", wbv, r["verifier_rationale"])):
            if not wb_text or mine is None:
                prefix[f"{name}: not comparable"] += 1
                continue
            m = one_line(mine, 10_000)
            if m == wb_text:
                prefix[f"{name}: identical"] += 1
            elif m.startswith(wb_text):
                prefix[f"{name}: workbook text is a prefix of arm A"] += 1
            elif m[:40] == wb_text[:40]:
                prefix[f"{name}: first 40 chars equal only"] += 1
            else:
                prefix[f"{name}: different"] += 1
    wb_len = Counter(len(one_line(l["wb_primary_reasoning"], 10_000)) for l in labels.values())
    return {"primary_crosstab": dict(sorted(prim_x.items())), "verifier_crosstab": dict(sorted(ver_x.items())),
            "rationale_text_comparison": dict(sorted(prefix.items())),
            "workbook_primary_reasoning_length_top": wb_len.most_common(3)}


# ── timing ────────────────────────────────────────────────────────────


def timing(run_dir: Path, recs: dict[str, dict[int, dict]]) -> dict:
    by_model: dict[str, list[dict]] = {}
    for arm in lib.ARMS:
        for r in recs[arm].values():
            for c in (r or {}).get("calls", []):
                by_model.setdefault(c["call"].get("model"), []).append(c["call"])
    models = {}
    for m, calls in by_model.items():
        walls = [c["wall_s"] for c in calls if "wall_s" in c]
        loads = [(c.get("metrics") or {}).get("load_duration") or 0 for c in calls]
        models[m] = {"calls": len(calls), "wall_mean_s": round(statistics.mean(walls), 3),
                     "wall_median_s": round(statistics.median(walls), 3), "wall_max_s": round(max(walls), 3),
                     "wall_total_s": round(sum(walls), 1), "load_max_s": round(max(loads) / 1e9, 3),
                     "calls_with_load_over_1s": sum(l > 1e9 for l in loads)}
    summary = json.loads((Path(run_dir) / "run_summary.json").read_text())
    events = summary["events"]
    boundaries = [{"at": e["at"], "event": e["event"], "phase": e.get("phase"), "arm": e.get("arm"),
                   "ollama_ps": [(m["name"], m["size"], m.get("context_length")) for m in e.get("ollama_ps", [])]}
                  for e in events if "ollama_ps" in e]
    pauses = [{"event": e["event"], "at": e["at"], "phase": e.get("phase"), "arm": e.get("arm")}
              for e in events if e["event"] in ("paused", "resumed")]
    return {"per_model": models, "started_utc": summary["started_utc"], "ended_utc": summary["ended_utc"],
            "status": summary["status"], "watch_file": summary.get("watch_file"),
            "ollama_ps_at_boundaries": boundaries, "pauses": pauses, "trees": summary.get("trees"),
            "papers": summary.get("papers"), "identity": summary.get("identity"),
            "ollama_version": summary.get("ollama_version")}


# ── main ──────────────────────────────────────────────────────────────


def score(run_dir: Path, workbook: Path, out_dir: Path) -> dict:
    labels = load_labels(workbook)
    raw = {a: load_arm(run_dir, a) for a in lib.ARMS}
    recs = {a: arm_records(labels, *raw[a]) for a in lib.ARMS}
    result = {
        "definitions": __doc__.split("Definitions, fixed here so the report can cite them:")[1].strip(),
        "n": len(labels), "pi_counts": dict(Counter(l["pi"] for l in labels.values())),
        "call_pattern_problems": {a: check_call_pattern(labels, *raw[a]) for a in lib.ARMS},
        "arms": {},
        "agreement": {},
    }
    for a in lib.ARMS:
        result["arms"][a] = {
            "confusion_final_x_PI": confusion(recs[a], labels),
            "flagged_as_include": binary_metrics(recs[a], labels, "include"),
            "flagged_as_exclude": binary_metrics(recs[a], labels, "exclude"),
            "decided_only": binary_metrics(recs[a], labels, None),
            "rates": rates(recs[a]),
        }
    for x, y in PAIRS:
        result["agreement"][f"{x}-{y}"] = {"final": agreement(recs[x], recs[y], "final"),
                                          "primary": agreement(recs[x], recs[y], "primary_outcome")}
    lists = disagreement_lists(labels, recs)
    result["disagreement_counts"] = {k: len(v) for k, v in lists.items()}
    result["workbook_vs_arm_A"] = workbook_vs_arm_a(labels, recs["A"])
    result["timing"] = timing(run_dir, recs)

    out_dir.mkdir(parents=True, exist_ok=True)
    for a in lib.ARMS:
        write_csv(out_dir / f"arm_{a}_papers.csv", per_arm_rows(labels, recs[a]))
    score_rows = []
    for a in lib.ARMS:
        for key in ("flagged_as_include", "flagged_as_exclude", "decided_only"):
            m = result["arms"][a][key]
            score_rows.append({"arm": a, "treatment": m["flagged_as"], **{k: m[k] for k in (
                "tp", "fn", "tn", "fp", "dropped_flagged", "dropped_error_or_missing",
                "sensitivity", "specificity", "balanced_accuracy")},
                "sensitivity_ci95": m["sensitivity_ci95"], "specificity_ci95": m["specificity_ci95"]})
    write_csv(out_dir / "scoring.csv", score_rows)
    cols = ["paper_id", "ee_identifier", "title", "PI_decision", "PI_notes",
            "A_final", "A_rationale", "B_final", "B_rationale", "C_final", "C_rationale"]
    for name, pids in lists.items():
        rows = disagreement_rows(pids, labels, recs)
        write_csv(out_dir / f"disagreements_{name}.csv", rows)
        md_rows = [{**r, "title": one_line(r["title"], 90), "PI_notes": one_line(r["PI_notes"], 160),
                    **{f"{a}_rationale": one_line(r[f"{a}_rationale"], 180) for a in lib.ARMS}} for r in rows]
        (out_dir / f"disagreements_{name}.md").write_text(
            f"# 2f disagreement list — {name}\n\n{len(rows)} papers. Final outcome per arm; rationale is the "
            f"call that decided it. See scoring.json `definitions`.\n\n" + md_table(md_rows, cols) + "\n",
            encoding="utf-8")
    (out_dir / "scoring.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str) + "\n")
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK)
    ap.add_argument("--out-dir", required=True, type=Path)
    args = ap.parse_args(argv)
    result = score(args.run_dir, args.workbook, args.out_dir)
    brief = {a: {"rates": result["arms"][a]["rates"],
                 **{k: {m: result["arms"][a][k][m] for m in ("tp", "fn", "tn", "fp", "sensitivity", "specificity")}
                    for k in ("flagged_as_include", "flagged_as_exclude", "decided_only")}}
             for a in lib.ARMS}
    print(json.dumps({"call_pattern_problems": result["call_pattern_problems"], "arms": brief,
                      "agreement": {k: {kk: {"pct": vv["pct"], "kappa": vv["kappa"], "n": vv["n"]}
                                        for kk, vv in v.items()} for k, v in result["agreement"].items()},
                      "disagreement_counts": result["disagreement_counts"]}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
