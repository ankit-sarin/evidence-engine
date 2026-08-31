"""ELICIT-01 — sample manifest and the pre-flight input-fit check.

The fit check is a study pre-condition, not the production input-fit guard that
PARSE-01 queued for the fix phase. It exists so this run cannot silently repeat
CAPTURE-01's truncation: every one of the 76 prompts is rendered and sized
BEFORE any model call, against the enforced ceiling.

The ceiling is 131,072 -- `n_ctx_train` for deepseek-r1:32b. PARSE-01 established
that the configured and derived values (262,144; OLLAMA_CONTEXT_LENGTH=0) are the
wrong numbers to trust: the runtime clamps against the trained context, so a check
using the configured value would be wrong by a factor of two.

Sizing uses the WORST observed chars->tokens ratio (0.4288, from CAPTURE-01's
token-dense p719), not the median. A central estimate is the wrong instrument for
a safety check: it under-predicts exactly the token-dense text most likely to
truncate.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, asdict
from pathlib import Path

from analysis.eval.elicit01.prompts import (
    CONDITION_COPY, CONDITION_INDEX, build_copy_prompt, build_index_prompt,
)
from analysis.eval.elicit01.units import build_unit_map
from analysis.eval.schema_eval2 import select_sample

CEILING_TOKENS = 131_072          # n_ctx_train, the enforced clamp (PARSE-01)
WORST_RATIO = 0.4288              # worst observed tokens/char (CAPTURE-01 p719)
EXCLUDED = {415: "PARSE-01 MERGED_DOCUMENT (728-page proceedings volume); truncates at ceiling",
            719: "PARSE-01 EXTRACTION_FAILURE (font-glyph encoded, 8.84x inflation); truncates at ceiling"}


@dataclass
class ManifestEntry:
    paper_id: int
    length_stratum: str
    study_type: str
    corpus_status: str
    in_corpus: bool
    parsed_chars: int
    n_units: int
    copy_prompt_chars: int
    index_prompt_chars: int
    copy_est_tokens: int
    index_est_tokens: int
    copy_fits: bool
    index_fits: bool
    headroom_tokens: int


def newest_parsed(parsed_dir: Path, pid: int) -> Path | None:
    fs = sorted(parsed_dir.glob(f"{pid}_v*.md"), key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
    return fs[-1] if fs else None


def build(review_dir: Path, codebook: Path) -> tuple[list[ManifestEntry], dict]:
    sample = select_sample(review_dir)
    conn = sqlite3.connect(f"file:{review_dir/'review.db'}?immutable=1", uri=True)
    status = {r[0]: r[1] for r in conn.execute("SELECT id, status FROM papers")}
    extracted = {r[0] for r in conn.execute("SELECT DISTINCT paper_id FROM extractions")}
    conn.close()

    parsed = review_dir / "parsed_text"
    entries: list[ManifestEntry] = []
    unit_maps: dict[int, dict] = {}
    for p in sample:
        if p.paper_id in EXCLUDED:
            continue
        f = newest_parsed(parsed, p.paper_id)
        raw = f.read_text()
        um = build_unit_map(p.paper_id, raw)
        unit_maps[p.paper_id] = um.to_json()
        cp = build_copy_prompt(raw, codebook)
        ip = build_index_prompt(um.render(), um.n, codebook)
        ce, ie = int(len(cp) * WORST_RATIO), int(len(ip) * WORST_RATIO)
        entries.append(ManifestEntry(
            paper_id=p.paper_id, length_stratum=p.length_stratum, study_type=p.study_type,
            corpus_status=status.get(p.paper_id, "?"),
            in_corpus=p.paper_id in extracted,
            parsed_chars=len(raw), n_units=um.n,
            copy_prompt_chars=len(cp), index_prompt_chars=len(ip),
            copy_est_tokens=ce, index_est_tokens=ie,
            copy_fits=ce < CEILING_TOKENS, index_fits=ie < CEILING_TOKENS,
            headroom_tokens=CEILING_TOKENS - max(ce, ie),
        ))
    meta = {
        "ceiling_tokens": CEILING_TOKENS,
        "ratio_used": WORST_RATIO,
        "ratio_basis": "worst observed tokens/char, CAPTURE-01 p719 (not the median)",
        "excluded": {str(k): v for k, v in EXCLUDED.items()},
        "n": len(entries),
        "n_in_corpus": sum(1 for e in entries if e.in_corpus),
        "n_not_in_corpus": sum(1 for e in entries if not e.in_corpus),
    }
    return entries, {"meta": meta, "unit_maps": unit_maps}


def main() -> int:
    review_dir = Path("data/surgical_autonomy")
    cb = review_dir / "extraction_codebook.yaml"
    entries, extra = build(review_dir, cb)
    out = review_dir / "eval" / "elicit01"
    out.mkdir(parents=True, exist_ok=True)
    (out / "manifest.json").write_text(json.dumps(
        {"meta": extra["meta"], "papers": [asdict(e) for e in entries]}, indent=1))
    (out / "unit_maps.json").write_text(json.dumps(extra["unit_maps"]))

    m = extra["meta"]
    print(f"ELICIT-01 manifest: n={m['n']} "
          f"({m['n_in_corpus']} in corpus, {m['n_not_in_corpus']} not)")
    print(f"excluded: {sorted(EXCLUDED)}")
    print()
    worst = sorted(entries, key=lambda e: -max(e.copy_est_tokens, e.index_est_tokens))[:6]
    print(f"{'paper':<7}{'chars':>9}{'units':>7}{'COPY est':>10}{'INDEX est':>11}"
          f"{'inflation':>11}{'headroom':>10}  fits")
    for e in worst:
        infl = e.index_est_tokens / e.copy_est_tokens
        print(f"p{e.paper_id:<6}{e.parsed_chars:>9,}{e.n_units:>7}{e.copy_est_tokens:>10,}"
              f"{e.index_est_tokens:>11,}{infl:>10.2f}x{e.headroom_tokens:>10,}  "
              f"{'YES' if e.copy_fits and e.index_fits else 'NO'}")
    bad = [e for e in entries if not (e.copy_fits and e.index_fits)]
    print()
    print(f"FIT CHECK: {2*len(entries)} prompts, "
          f"{'ALL FIT' if not bad else 'FAILURES: ' + str([e.paper_id for e in bad])}")
    print(f"  max estimate {max(max(e.copy_est_tokens, e.index_est_tokens) for e in entries):,} "
          f"vs ceiling {CEILING_TOKENS:,}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
