"""12h RD-2 — Run 6 legacy local spans against their source texts. Read-only (mode=ro; files
opened for reading). usage, from the repository root:
    rd2_substring.py <review.db> <extraction_codebook.yaml>
(stdout: JSON; also writes rd2_spans.csv beside the script: one row per span.)

LABEL (R563): two-pass legacy snippets measure the corpus's text-pathology rate, not the
elicited path.

The six texts of R563, applied CUMULATIVELY and in R563's order, to the snippet and to the
source text alike. A span "matches at stage k" when stage-k(snippet) is non-empty and is an
exact, case-sensitive substring of stage-k(text).
  0 raw                  nothing applied
  1 whitespace           every run of whitespace -> one space; strip
  2 comment-stripped     engine.elicitation.units.COMMENT_RE (the engine's own pattern,
                         imported) -> one space; whitespace again
  3 de-hyphenated        a hyphen (ASCII "-" or U+2010) between two word characters and
                         followed by whitespace is removed with that whitespace: "auto- matic"
                         -> "automatic"
  4 ligature-folded      U+FB00..U+FB06 -> ff fi fl ffi ffl ft st
  5 soft-hyphen-removed  U+00AD removed; where it sat between word characters and was followed
                         by whitespace, that whitespace goes with it

Populations:
  headline   spans on papers with ONE parsed-text version, with a non-empty snippet
  empty      spans whose snippet is NULL or whitespace (not testable; counted)
  sentinel   spans whose value is a codebook absence sentinel or "No comparison reported"
             (reported apart, and the headline is given with and without them)
  D11        papers with more than one parsed-text version (455, 586, 699, 719): matched
             against each version side by side, excluded from the headline

Supplementary, beyond R563's six: the same spans under locator-1's own `normalize`
(engine.core.locator, imported: curly quotes, NFKC, glued-full-stop, lowercase, whitespace),
exact stage only, then + comment strip, + de-hyphenation, + soft-hyphen removal. The fuzzy
stage of locator-1 is NOT run here.
"""
import csv, json, os, re, sqlite3, sys
from collections import Counter, defaultdict

sys.path.insert(0, os.getcwd())
import yaml
from engine.elicitation.units import COMMENT_RE
from engine.core.locator import normalize as locator1_normalize
from engine.core.constants import INVALID_SNIPPET_RE
assert not any(m == "ollama" or m.startswith("ollama.") for m in sys.modules), "ollama imported"

HERE = os.path.dirname(os.path.abspath(__file__))
DB, CB = sys.argv[1], sys.argv[2]
cb = yaml.safe_load(open(CB))
SENTINELS = set(cb["absence_sentinels"]) | {"No comparison reported"}
CODEBOOK_FIELDS = [f["name"] for f in cb["fields"]]

WS = re.compile(r"\s+")
DEHYPH = re.compile(r"(?<=\w)[-‐]\s+(?=\w)")
LIG = str.maketrans({"ﬀ": "ff", "ﬁ": "fi", "ﬂ": "fl", "ﬃ": "ffi",
                     "ﬄ": "ffl", "ﬅ": "ft", "ﬆ": "st"})
SHY_JOIN = re.compile(r"(?<=\w)­\s*(?=\w)")

def ws(s): return WS.sub(" ", s).strip()
STAGES = ["raw", "whitespace", "comment_stripped", "de_hyphenated", "ligature_folded",
          "soft_hyphen_removed"]

def ladder(s):
    """The six cumulative forms of one string."""
    out = [s]
    s = ws(s); out.append(s)
    s = ws(COMMENT_RE.sub(" ", s)); out.append(s)
    s = DEHYPH.sub("", s); out.append(s)
    s = s.translate(LIG); out.append(s)
    s = ws(SHY_JOIN.sub("", s).replace("­", "")); out.append(s)
    return out

L1_STAGES = ["locator1_normalize", "+comment_stripped", "+de_hyphenated", "+soft_hyphen_removed"]
def l1_ladder(s):
    a = locator1_normalize(s)
    b = locator1_normalize(COMMENT_RE.sub(" ", s))
    c = DEHYPH.sub("", b)
    d = ws(SHY_JOIN.sub("", c).replace("­", ""))
    return [a, b, c, d]

c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
refs = defaultdict(list)
for pid, ver, path, parsed_at in c.execute(
        "SELECT r.paper_id, r.parsed_text_version, r.parsed_text_path, a.parsed_at "
        "FROM parsed_text_refs r LEFT JOIN full_text_assets a "
        "ON a.id = r.source_full_text_assets_id ORDER BY 1, 2"):
    refs[pid].append((ver, path, parsed_at))
D11 = sorted(p for p, v in refs.items() if len(v) > 1)

text_cache = {}
def forms(path):
    if path not in text_cache:
        raw = open(path, encoding="utf-8").read()
        text_cache[path] = (ladder(raw), l1_ladder(raw), raw)
    return text_cache[path]

def first_match(snip_forms, text_forms):
    hits = [bool(s) and s in t for s, t in zip(snip_forms, text_forms)]
    first = next((i for i, h in enumerate(hits) if h), None)
    return first, hits

spans = c.execute(
    "SELECT s.id, e.paper_id, s.field_name, s.value, s.source_snippet, s.audit_status, "
    "e.extracted_at, e.model FROM evidence_spans s JOIN extractions e ON e.id = s.extraction_id "
    "ORDER BY s.id").fetchall()

rows = []
for sid, pid, field, value, snippet, audit, extracted_at, model in spans:
    empty = not (snippet or "").strip()
    sentinel = (value or "").strip() in SENTINELS
    r = {"span_id": sid, "paper_id": pid, "field_name": field, "empty_snippet": empty,
         "sentinel_value": sentinel, "d11": pid in D11, "audit_status": audit,
         "snippet_chars": len(snippet or ""),
         "bridged": bool(snippet and INVALID_SNIPPET_RE.search(snippet))}
    if not empty:
        sf, sl1 = ladder(snippet), l1_ladder(snippet)
        for ver, path, parsed_at in refs[pid]:
            tf, tl1, raw = forms(path)
            first, hits = first_match(sf, tf)
            l1first, l1hits = first_match(sl1, tl1)
            key = f"v{ver}" if pid in D11 else "only"
            r[f"first_stage_{key}"] = first
            r[f"hits_{key}"] = hits
            r[f"l1_first_{key}"] = l1first
            r[f"parsed_after_extraction_{key}"] = bool(parsed_at and parsed_at > extracted_at)
            r[f"snippet_has_shy_{key}"] = "­" in snippet
            r[f"snippet_has_ligature_{key}"] = any(ch in snippet for ch in "ﬀﬁﬂﬃﬄﬅﬆ")
    rows.append(r)

with open(os.path.join(HERE, "rd2_spans.csv"), "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["span_id", "paper_id", "field_name", "empty_snippet", "sentinel_value", "d11",
                "audit_status", "snippet_chars", "bridged", "first_stage", "first_stage_v2",
                "first_stage_v3", "locator1_first_stage"])
    def nm(i, names=STAGES): return "" if i is None else names[i]
    for r in rows:
        w.writerow([r["span_id"], r["paper_id"], r["field_name"], int(r["empty_snippet"]),
                    int(r["sentinel_value"]), int(r["d11"]), r["audit_status"], r["snippet_chars"],
                    int(r["bridged"]),
                    nm(r.get("first_stage_only")) if "first_stage_only" in r else "",
                    nm(r.get("first_stage_v2")) if "first_stage_v2" in r else "",
                    nm(r.get("first_stage_v3")) if "first_stage_v3" in r else "",
                    nm(r.get("l1_first_only"), L1_STAGES) if "l1_first_only" in r else ""])


def table(sel, key="only", names=STAGES, pref="first_stage_"):
    """Cumulative matches by stage, with each transform's marginal gain."""
    n = len(sel)
    firsts = Counter(r.get(pref + key) for r in sel)
    out, cum = [], 0
    for i, name in enumerate(names):
        gain = firsts.get(i, 0); cum += gain
        out.append({"stage": name, "matched_cumulative": cum,
                    "share_cumulative": round(cum / n, 4) if n else None,
                    "marginal_gain": gain,
                    "marginal_gain_share_of_all": round(gain / n, 4) if n else None})
    return {"spans": n, "stages": out, "unmatched_after_all": n - cum,
            "unmatched_share": round((n - cum) / n, 4) if n else None}


def nonmono(sel, key="only"):
    return sum(1 for r in sel for a, b in zip(r[f"hits_{key}"], r[f"hits_{key}"][1:]) if a and not b)


single = [r for r in rows if not r["d11"]]
head = [r for r in single if not r["empty_snippet"]]
head_nosent = [r for r in head if not r["sentinel_value"]]
sent = [r for r in single if r["sentinel_value"]]
d11 = [r for r in rows if r["d11"]]
d11_ne = [r for r in d11 if not r["empty_snippet"]]
resid = [r for r in head if r["first_stage_only"] is None]

by_field = {}
for f in sorted({r["field_name"] for r in head}):
    by_field[f] = table([r for r in head if r["field_name"] == f])
by_field_compact = {f: {"spans": t["spans"],
                        "raw": t["stages"][0]["matched_cumulative"],
                        "gain_whitespace": t["stages"][1]["marginal_gain"],
                        "gain_comment": t["stages"][2]["marginal_gain"],
                        "gain_dehyphen": t["stages"][3]["marginal_gain"],
                        "gain_ligature": t["stages"][4]["marginal_gain"],
                        "gain_soft_hyphen": t["stages"][5]["marginal_gain"],
                        "matched_after_all": t["spans"] - t["unmatched_after_all"],
                        "share_after_all": round(1 - t["unmatched_share"], 4),
                        "in_codebook": f in CODEBOOK_FIELDS}
                    for f, t in by_field.items()}

print(json.dumps({
    "label": "two-pass legacy snippets measure the corpus's text-pathology rate, not the elicited path",
    "stages_in_order": STAGES,
    "spans_total": len(rows),
    "papers": len({r["paper_id"] for r in rows}),
    "extraction_models": dict(Counter(s[7] for s in spans)),
    "extracted_at_range": [min(s[6] for s in spans), max(s[6] for s in spans)],
    "field_names_not_in_codebook": sorted({r["field_name"] for r in rows} - set(CODEBOOK_FIELDS)),
    "sentinel_values_used": sorted(SENTINELS),
    "d11_papers": D11,
    "populations": {
        "all_spans": len(rows),
        "d11_spans": len(d11),
        "single_version_spans": len(single),
        "single_version_empty_snippet": sum(r["empty_snippet"] for r in single),
        "single_version_non_empty (headline)": len(head),
        "headline_sentinel_valued": sum(r["sentinel_value"] for r in head),
        "headline_not_sentinel_valued": len(head_nosent),
    },
    "empty_snippets": {
        "all": sum(r["empty_snippet"] for r in rows),
        "by_value": dict(Counter(s[3] for s, r in zip(spans, rows) if r["empty_snippet"]).most_common(12)),
        "sentinel_valued": sum(1 for r in rows if r["empty_snippet"] and r["sentinel_value"]),
    },
    "headline": table(head),
    "headline_excluding_sentinel_valued": table(head_nosent),
    "sentinel_valued_single_version": {
        "spans": len(sent), "empty_snippet": sum(r["empty_snippet"] for r in sent),
        "by_value": dict(Counter(s[3].strip() for s, r in zip(spans, rows)
                                 if r["sentinel_value"] and not r["d11"]).most_common()),
        "with_snippet": table([r for r in sent if not r["empty_snippet"]]),
    },
    "by_field": by_field_compact,
    "d11": {
        "spans": len(d11), "empty_snippet": sum(r["empty_snippet"] for r in d11),
        "against_v2": table(d11_ne, "v2"), "against_v3": table(d11_ne, "v3"),
        "per_paper": {str(p): {"spans_with_snippet": len([r for r in d11_ne if r["paper_id"] == p]),
                               "v2_matched_after_all": sum(1 for r in d11_ne if r["paper_id"] == p and r["first_stage_v2"] is not None),
                               "v3_matched_after_all": sum(1 for r in d11_ne if r["paper_id"] == p and r["first_stage_v3"] is not None),
                               "v2_raw": sum(1 for r in d11_ne if r["paper_id"] == p and r["first_stage_v2"] == 0),
                               "v3_raw": sum(1 for r in d11_ne if r["paper_id"] == p and r["first_stage_v3"] == 0)}
                      for p in D11},
        "v3_parsed_after_extraction": sorted({r["paper_id"] for r in d11_ne if r["parsed_after_extraction_v3"]}),
        "v2_parsed_after_extraction": sorted({r["paper_id"] for r in d11_ne if r["parsed_after_extraction_v2"]}),
    },
    "checks": {
        "headline_spans_whose_text_was_parsed_after_extraction":
            sum(1 for r in head if r["parsed_after_extraction_only"]),
        "matches_lost_by_a_later_stage (non-monotone)": nonmono(head),
        "headline_snippets_containing_a_soft_hyphen": sum(r["snippet_has_shy_only"] for r in head),
        "headline_snippets_containing_a_ligature": sum(r["snippet_has_ligature_only"] for r in head),
        "source_texts_containing_a_soft_hyphen": sum(1 for p, (_, _, raw) in text_cache.items() if "­" in raw),
        "source_texts_containing_a_ligature": sum(1 for p, (_, _, raw) in text_cache.items()
                                                  if any(ch in raw for ch in "ﬀﬁﬂﬃﬄﬅﬆ")),
        "source_texts_containing_a_comment": sum(1 for p, (_, _, raw) in text_cache.items() if COMMENT_RE.search(raw)),
        "source_texts_read": len(text_cache),
    },
    "residual_unmatched_headline": {
        "spans": len(resid),
        "bridged_with_ellipsis": sum(r["bridged"] for r in resid),
        "by_audit_status": dict(Counter(r["audit_status"] for r in resid)),
        "sentinel_valued": sum(r["sentinel_value"] for r in resid),
    },
    "headline_by_run6_audit_status": {a: table([r for r in head if r["audit_status"] == a])
                                      for a in sorted({r["audit_status"] for r in head})},
    "supplementary_locator1_exact_stage": table(head, names=L1_STAGES, pref="l1_first_"),
}, indent=1, sort_keys=True, ensure_ascii=False))
