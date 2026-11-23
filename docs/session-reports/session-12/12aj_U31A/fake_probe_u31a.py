"""12aj-U31A — FAKE probe for U31's Phase A ("Cloud response repair does not invent a value"; INT-j12-18, INT-l12-2), on
12af_U30A/fake_probe_u30a.py's scratch setup: `scripts/run_cloud_extraction.run_arm` itself, driven on a fresh scratch database per
case with the OpenAI transport's SDK class replaced by a fake that returns a response in the SDK's own shape (`ChatCompletion`).
At HEAD:
  PART 1  the ten span shapes of Q2, each as the `sample_size` span of an otherwise complete twenty-field response: what
          `parse_response_to_spans` returns for it (value, snippet, _last_salvage), then what the run stores for it — the rows in
          cloud_extractions, cloud_evidence_spans, field_events and claim_inputs, the paper's run_calls, and the stored
          extracted_data's own span for that field (the response as received).
  PART 2  the acceptance test's three statements (U31's entry), each evaluated from PART 1's rows.
  PART 3  Q4's cut-off shapes: a response the transport reads as truncated (finish_reason "length"), in five forms.
  PART 4  two neighbours read by code order: the Anthropic transport's ''→None change met by the store's citation guard (the pure
          function `check_citations`, no transport), and the call form `scripts/backfill_cloud_spans.py` makes — the parser on an
          instance built by `__new__` — given a null value (the expression only; the script is not run and no database is opened).
Seams: a fresh scratch ReviewDatabase per case under a temp directory (one paper, with an eligibility event and one recorded parsed
text); the tracked codebook beside it; a spec copy whose `enabled_arms: []` names the openai arm; rm.git_state patched clean;
`cli.EXTRACTORS["openai"]` a thin subclass of the real transport passing a placeholder key (never reading the environment); the
transport module's `openai.OpenAI` replaced by the fake; `time.sleep` in the transport a no-op recorder; httpx.Client.send and
socket.socket.connect refusing recorders. No server, no provider, no key, no network, no live database; neither repair script is
imported or run.
usage, from the repository root: .venv/bin/python fake_probe_u31a.py <tmp dir>"""
import copy, json, logging, shutil, socket, sqlite3, sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path.cwd())); sys.path.insert(0, str(Path.cwd() / "tests"))
logging.disable(logging.CRITICAL)
import httpx, openai
from openai.types.chat import ChatCompletion
from engine.cloud import openai_extractor as oe
from engine.cloud.base import CloudExtractorBase
from engine.core import run_manifest as rm
from engine.core.citation_guard import LEGACY, check_citations
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from scripts import run_cloud_extraction as cli
from _event_store_fixture import seed_eligibility
from _parsed_text_fixture import write_parsed

TMP = Path(sys.argv[1]); TMP.mkdir(parents=True, exist_ok=True)
SPEC_PATH = Path("review_specs/surgical_autonomy.yaml"); CODEBOOK = Path("data/surgical_autonomy/extraction_codebook.yaml")
CBK = load_codebook(CODEBOOK)
REC = {"constructors": [], "creates": 0, "sleeps": [], "httpx_send": 0, "socket_connect": 0}
FIELD = "sample_size"                      # the codebook's one numeric field
QUOTE = "Forty patients were enrolled."    # present in the scratch paper text
PAPER_TEXT = f"A paper.\n\nSnippet for study_type.\n\n{QUOTE}\n"
ORDER = [f for tier in (1, 2, 3, 4) for f in CBK.fields_by_tier(tier)]


def base_span(f):
    if f["name"] == "study_type":
        return {"field_name": "study_type", "value": "Original Research", "source_snippet": "Snippet for study_type.", "confidence": 0.9, "tier": f["tier"]}
    return {"field_name": f["name"], "value": "NR", "source_snippet": "", "confidence": 0.9, "tier": f["tier"]}


def response_with(shape_span, drop=()):
    """The twenty-field response, the FIELD span replaced by `shape_span` (None = FIELD's span left as the base 'NR')."""
    fields = []
    for f in ORDER:
        if f["name"] in drop:
            continue
        fields.append(copy.deepcopy(shape_span) if (f["name"] == FIELD and shape_span is not None) else base_span(f))
    return {"fields": fields}


S = lambda **kw: {"field_name": FIELD, "confidence": 0.9, "tier": 1, **kw}
SHAPES = [
    ("1 no value key (snippet sent)",      S(source_snippet=QUOTE)),
    ("2 explicit null (snippet sent)",     S(value=None, source_snippet=QUOTE)),
    ("3 empty string (no snippet)",        S(value="", source_snippet="")),
    ("4 int 40",                           S(value=40, source_snippet=QUOTE)),
    ("5 float 40.5",                       S(value=40.5, source_snippet=QUOTE)),
    ("6 bool true",                        S(value=True, source_snippet=QUOTE)),
    ("7 list of strings",                  S(value=["40", "38"], source_snippet=QUOTE)),
    ("8 list of mixed types",              S(value=["40", 38, None, True], source_snippet=QUOTE)),
    ("9 dict",                             S(value={"n": 40}, source_snippet=QUOTE)),
    ("10 no source_snippet key (value '40')", S(value="40")),
]


def make_case(name):
    d = TMP / name; d.mkdir()
    db = ReviewDatabase("u31aprobe", data_root=d)
    db.add_papers([Citation(title="P1", source="pubmed", pmid="1")])
    pid = [r["id"] for r in db.get_papers_by_status("INGESTED")][0]
    seed_eligibility(db._conn, pid); write_parsed(db, pid, PAPER_TEXT, version=1)
    db._conn.commit(); db_path = Path(db.db_path); db.close()
    shutil.copy2(CODEBOOK, db_path.parent / "extraction_codebook.yaml")
    arm = next(a.name for a in load_review_spec(SPEC_PATH).arms if a.provider == "openai")
    text = SPEC_PATH.read_text(); assert text.count("enabled_arms: []") == 1
    spec_path = d / "spec.yaml"; spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arm}]"))
    return arm, db_path, spec_path, pid


def fake_openai(content, finish):
    def create(**payload):
        REC["creates"] += 1
        return ChatCompletion(**{"id": "c", "object": "chat.completion", "created": 0, "model": payload["model"],
                                 "choices": [{"index": 0, "finish_reason": finish, "message": {"role": "assistant", "content": content}}],
                                 "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})

    class FakeOpenAI:
        def __init__(self, **kw):
            REC["constructors"].append(kw)
            self.chat = type("C", (), {"completions": type("CC", (), {"create": staticmethod(create)})()})()
    return FakeOpenAI


class Probe(oe.OpenAIExtractor):
    def __init__(self, *a, **k):
        k["api_key"] = "placeholder-not-a-credential"
        super().__init__(*a, **k)


def short(v, n=70):
    s = repr(v); return s if len(s) <= n else s[:n] + "…"


def state(db_path, arm, pid):
    c = sqlite3.connect(db_path); c.row_factory = sqlite3.Row
    try:
        q = lambda s, *a: c.execute(s, a).fetchone()[0]
        ext = c.execute("SELECT id, extracted_data FROM cloud_extractions WHERE arm = ? AND paper_id = ?", (arm, pid)).fetchall()
        span = [tuple(r) for r in c.execute("SELECT s.value, s.source_snippet, s.notes FROM cloud_evidence_spans s JOIN cloud_extractions e "
                                             "ON e.id = s.cloud_extraction_id WHERE e.arm = ? AND e.paper_id = ? AND s.field_name = ?", (arm, pid, FIELD))]
        fe = [tuple(r) for r in c.execute("SELECT event_type, value, source_snippet FROM field_events WHERE arm = ? AND paper_id = ? AND field_name = ? "
                                           "ORDER BY event_id", (arm, pid, FIELD))]
        received = None
        if ext:
            data = json.loads(ext[0]["extracted_data"])
            flds = data.get("fields") if isinstance(data, dict) else None
            received = next((s for s in (flds or []) if isinstance(s, dict) and s.get("field_name") == FIELD), "(no fields list in extracted_data)")
        return {"cloud_extractions": len(ext),
                "cloud_evidence_spans": q("SELECT COUNT(*) FROM cloud_evidence_spans s JOIN cloud_extractions e ON e.id = s.cloud_extraction_id "
                                          "WHERE e.arm = ? AND e.paper_id = ?", arm, pid),
                "field_events": q("SELECT COUNT(*) FROM field_events WHERE arm = ? AND paper_id = ?", arm, pid),
                "claim_inputs": q("SELECT COUNT(*) FROM claim_inputs WHERE arm = ? AND paper_id = ?", arm, pid),
                "span": span, "field_event": fe, "received": received,
                "run_calls": [tuple(r) for r in c.execute("SELECT outcome, outcome_detail FROM run_calls WHERE paper_id = ? ORDER BY call_id", (pid,))],
                "manifest": [tuple(r) for r in c.execute("SELECT end_status, end_reason FROM run_manifests WHERE run_kind = 'extraction' ORDER BY run_id")]}
    finally:
        c.close()


def drive(name, content, finish="stop"):
    arm, db_path, spec_path, pid = make_case(name)
    before = REC["creates"]
    with patch.object(oe.openai, "OpenAI", fake_openai(content, finish)), patch.dict(cli.EXTRACTORS, {"openai": Probe}):
        try:
            cli.run_arm(arm, str(db_path), str(spec_path), None, None); left = "returned"
        except BaseException as e:   # noqa: BLE001 — the probe reports what left run_arm
            left = f"{type(e).__name__} propagated"
    st = state(db_path, arm, pid); st["left"] = left; st["sends"] = REC["creates"] - before
    return st


def parse_only(payload):
    """parse_response_to_spans at HEAD on a scratch-rooted base instance (no PROVIDER: no arm, no client)."""
    arm, db_path, spec_path, pid = make_case(f"parse_{parse_only.n}"); parse_only.n += 1
    base = CloudExtractorBase(str(db_path), str(spec_path))
    try:
        out = base.parse_response_to_spans(copy.deepcopy(payload))
        return out, base._last_salvage
    finally:
        base.close()
parse_only.n = 0


def report(st):
    return (f"    run_arm {st['left']}; provider sends {st['sends']}; run_calls (outcome, detail) {st['run_calls']}; manifest (end_status, end_reason) {st['manifest']}\n"
            f"    rows for the paper: cloud_extractions {st['cloud_extractions']}, cloud_evidence_spans {st['cloud_evidence_spans']}, field_events {st['field_events']}, "
            f"claim_inputs {st['claim_inputs']}\n"
            f"    {FIELD}: cloud_evidence_spans (value, source_snippet, notes) {st['span']}; field_events (event_type, value, source_snippet) {st['field_event']}\n"
            f"    {FIELD} in the stored extracted_data (the response as received): {short(st['received'], 150)}")


RESULTS = {}
with patch.object(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None)), \
     patch.object(httpx.Client, "send", lambda self, *a, **k: (REC.__setitem__("httpx_send", REC["httpx_send"] + 1), (_ for _ in ()).throw(RuntimeError("no network")))[1]), \
     patch.object(socket.socket, "connect", lambda self, *a, **k: (REC.__setitem__("socket_connect", REC["socket_connect"] + 1), (_ for _ in ()).throw(RuntimeError("no network")))[1]), \
     patch.object(oe.time, "sleep", lambda s: REC["sleeps"].append(s)):
    print(f"=== PART 1 — Q2's ten shapes, each as the {FIELD!r} span of an otherwise complete {len(ORDER)}-field response (FAKE)")
    for i, (label, span) in enumerate(SHAPES, 1):
        out, salvage = parse_only({"fields": [span]})
        parsed = [(s["value"], s["source_snippet"]) for s in out]
        st = drive(f"shape{i}", json.dumps(response_with(span)))
        RESULTS[i] = st
        print(f"--- shape {label}\n    sent: {short(span, 150)}\n    parse_response_to_spans → (value, source_snippet) {parsed}; _last_salvage {salvage!r}; spans returned {len(out)}")
        print(report(st) + "\n")

    print("=== PART 2 — the acceptance test's three statements at HEAD, from PART 1's rows (FAKE)")
    s1, s2, s6, s7, s9 = RESULTS[1], RESULTS[2], RESULTS[6], RESULTS[7], RESULTS[9]
    a = not s1["field_event"] and not s1["span"]
    print(f"  (i) 'A span with no value key yields no sentinel claim and its field is reported missing': {a} "
          f"— stored span {s1['span']}, claim {s1['field_event']}, sends {s1['sends']} (a field reported missing would be re-asked)")
    b = all(not RESULTS[k]["span"] for k in (6, 7, 8, 9))
    print(f"  (ii) 'a list, dict or bool value is rejected, not stored as its repr': {b} — stored values: bool {s6['span']}, list {s7['span']}, "
          f"mixed list {RESULTS[8]['span']}, dict {s9['span']}")
    c = bool(s2["span"]) and s2["span"][0][0] == CBK.canonical_absence_sentinel
    print(f"  (iii) 'an explicit JSON null still yields the canonical sentinel': {c} — stored span {s2['span']}\n")

    print("=== PART 3 — Q4: a response the transport reads as truncated (finish_reason 'length'), five forms (FAKE)")
    full = json.dumps(response_with(S(value="40", source_snippet=QUOTE)))
    CUTS = [
        ("T1 cut mid-string: the text is not valid JSON", full[: len(full) // 2]),
        ("T2 the JSON is whole (all fields) and the provider still says 'length'", full),
        ("T3 fenced ```json, the JSON whole, cut before the closing fence", "```json\n" + full),
        ("T4 valid JSON, the last field absent (19 of 20)", json.dumps(response_with(S(value="40", source_snippet=QUOTE), drop=(ORDER[-1]["name"],)))),
        ("T5 valid JSON, all field names present, the cut-off field's span holding no value key", json.dumps(response_with(S(source_snippet=QUOTE)))),
    ]
    for i, (label, content) in enumerate(CUTS, 1):
        try:
            data = json.loads(content); loads = "parses"
        except json.JSONDecodeError:
            data = {"fields": [], "raw": content}; loads = "JSONDecodeError → the transport passes {'fields': [], 'raw': <content>}"
        out, salvage = parse_only(data)
        st = drive(f"cut{i}", content, finish="length")
        print(f"--- {label}\n    json.loads(content) in the transport: {loads}; parse_response_to_spans returns {len(out)} spans; _last_salvage {salvage!r}")
        print(report(st) + "\n")

print("=== PART 4 — two neighbours (CODE ORDER, each a pure call; no transport, no database)")
anth = [{"field_name": FIELD, "value": None, "source_snippet": "", "confidence": 0.9, "tier": 1, "notes": "empty_string_to_null"}]
r = check_citations(anth, escape_token=CBK.escape_token, absence_sentinels=frozenset(s.strip().upper() for s in CBK.absence_sentinels),
                    mode=LEGACY, contract_unmet_token=CBK.raw.get("contract_unmet_token"))
print(f"  the Anthropic transport's ''→None span (anthropic_extractor.py, 'empty_string_to_null') at the store's citation guard: ok {r.ok}, offenders {r.offenders}")
bare = CloudExtractorBase.__new__(CloudExtractorBase)
for label, payload in (("a null value", {"fields": [{"field_name": FIELD, "value": None, "source_snippet": "", "confidence": 0.9, "tier": 1}]}),
                       ("no null value", {"fields": [{"field_name": FIELD, "value": "40", "source_snippet": "q", "confidence": 0.9, "tier": 1}]})):
    try:
        out = CloudExtractorBase.parse_response_to_spans(bare, payload); res = f"returned {len(out)} span(s)"
    except Exception as e:   # noqa: BLE001
        res = f"raised {type(e).__name__}: {e}"
    print(f"  CloudExtractorBase.parse_response_to_spans(CloudExtractorBase.__new__(CloudExtractorBase), <{label}>) — backfill_cloud_spans.py:58,66's call form: {res}")

print("\n=== the record")
print(f"SDK constructions recorded (FakeOpenAI, no client): {len(REC['constructors'])}, keywords {REC['constructors'][:1]}; provider creates "
      f"{REC['creates']}; sleeps {REC['sleeps']}; httpx sends {REC['httpx_send']}; socket connects {REC['socket_connect']}; real "
      f"openai.OpenAI never constructed (patched in every driven case; the parse-only instances have no PROVIDER and build no client); "
      f"scripts.reparse_cloud_spans / scripts.backfill_cloud_spans imported: {[m for m in sys.modules if 'reparse_cloud_spans' in m or 'backfill_cloud_spans' in m]}")
