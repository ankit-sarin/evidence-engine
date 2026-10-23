"""12v-RSA Q7 — how often INT-s12-1's precheck fires on Run 6's legacy local spans, and how often falsely.
Read-only: review.db opened mode=ro; parsed texts read through the engine's own resolver
(engine.core.parsed_text.resolve_parsed_text + read_parsed_text: the paper's current version, its bytes
verified against the recorded sha256 — the text the non-elicited extractor is handed at selection);
every socket connect refused; no model is called.
usage, from the repository root: freq_invalid_snippet.py <review.db> local|cloud   (stdout: JSON; writes nothing)
Populations:
  local — every evidence_spans row joined to extractions (Run 6's local arm; 12h_rd/rd2_substring.py's
          query). These snippets were written AFTER Run 6's own precheck and snippet retry, so a flagged
          snippet in it would have been retried or emptied before storage: it is the survivor population.
  cloud — every cloud_evidence_spans row joined to cloud_extractions (Run 6's two cloud arms). The cloud
          path runs no precheck (engine/cloud/ does not reference it), so these are model-typed snippets
          on the same corpus that no precheck filtered: a PROXY for what the precheck would see, from
          different models, not a measurement of the local arm.
For each span with a non-empty snippet:
  flagged       — extractor._has_invalid_snippet(snippet) is True (INVALID_SNIPPET_RE.search)
  falsely       — flagged, and the snippet is an exact, case-sensitive substring of the resolved text:
                  a verbatim quote of a passage that itself contains the ellipsis
  not_exact     — flagged, and not a raw substring
For the flagged spans that are raw substrings the production locator (engine.core.locator.locate, locator-1.1) is also run,
and its (located, kind, bridged) is tallied: bridged False with located True is the locator's own
reading that the quote is verbatim."""
import collections, json, os, socket, sqlite3, sys


def _refuse(*a, **k):
    raise OSError("12v-RSA: network refused")


socket.socket.connect = _refuse
sys.path.insert(0, os.getcwd())
from engine.agents.extractor import _has_invalid_snippet
from engine.core.locator import locate
from engine.core.parsed_text import ParsedTextError, read_parsed_text, resolve_parsed_text

DB, POP = sys.argv[1], sys.argv[2]
SQL = {"local": "SELECT s.id, e.paper_id, s.field_name, s.source_snippet FROM evidence_spans s "
              "JOIN extractions e ON e.id = s.extraction_id ORDER BY s.id",
       "cloud": "SELECT s.id, e.paper_id, s.field_name || ' @' || e.arm, s.source_snippet FROM cloud_evidence_spans s "
              "JOIN cloud_extractions e ON e.id = s.cloud_extraction_id ORDER BY s.id"}[POP]
c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
spans = c.execute(SQL).fetchall()
texts, refused = {}, {}
t = collections.Counter(); loc = collections.Counter(); marks = collections.Counter()
by_field = collections.defaultdict(collections.Counter); by_arm = collections.defaultdict(collections.Counter); cases = []
for sid, pid, field, snippet in spans:
    t["spans"] += 1
    if not (snippet or "").strip():
        t["empty_snippet"] += 1; continue
    t["with_snippet"] += 1
    if not _has_invalid_snippet(snippet):
        continue
    t["flagged"] += 1
    marks["unicode_ellipsis" if "…" in snippet else "three_dots"] += 1
    if pid not in texts and pid not in refused:
        try:
            texts[pid] = read_parsed_text(resolve_parsed_text(c, pid))
        except ParsedTextError as exc:
            refused[pid] = exc.reason_code
    if pid in refused:
        t["flagged_text_refused"] += 1; continue
    text = texts[pid]
    exact = snippet in text
    kind = "falsely" if exact else "not_exact"
    t[kind] += 1; by_field[field][kind] += 1; by_arm[field.split(" @")[-1] if POP == "cloud" else "local"][kind] += 1
    # the locator only on raw substrings: its exact stage is cheap there, while its fuzzy stage on a
    # non-exact snippet costs seconds each (12s_U14A F11) and is not needed for the false-flag count
    r = locate(text, snippet) if exact else None
    loc[(kind, r.located, r.kind, r.bridged) if r else (kind, "not run")] += 1
    cases.append({"span_id": sid, "paper_id": pid, "field_name": field, "snippet_chars": len(snippet),
                  "raw_substring": exact, "locate": [r.located, r.kind, r.bridged] if r else "not run"})
# Opportunity: a false flag needs the paper's own text to contain the ellipsis. Every paper of the
# population, its resolved text, INVALID_SNIPPET_RE match count.
from engine.core.constants import INVALID_SNIPPET_RE
opp = collections.Counter()
for pid in sorted({row[1] for row in spans}):
    if pid not in texts and pid not in refused:
        try:
            texts[pid] = read_parsed_text(resolve_parsed_text(c, pid))
        except ParsedTextError as exc:
            refused[pid] = exc.reason_code
    if pid in texts:
        n = len(INVALID_SNIPPET_RE.findall(texts[pid]))
        opp["papers"] += 1; opp["papers_text_with_ellipsis"] += bool(n); opp["ellipsis_matches_in_texts"] += n
c.close()
print(json.dumps({"opportunity": dict(opp),
    "population": POP, "totals": dict(t), "flagged_by_mark": dict(marks), "by_arm": {a: dict(v) for a, v in sorted(by_arm.items())},
    "flagged_by_field": {f: dict(v) for f, v in sorted(by_field.items())},
    "locate_on_flagged": {"|".join(map(str, k)): n for k, n in sorted(loc.items(), key=str)},
    "papers_with_flagged": len({x["paper_id"] for x in cases}), "text_refused": refused,
    "cases": cases}, indent=1, sort_keys=True))
