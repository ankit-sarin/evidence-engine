"""12s-U14A — reproducers for INT-i12-7 (autojunk) and INT-i12-8 (the ellipsis regex on the snippet
alone), at HEAD, on disposable in-memory fixtures. Evidence, not a test.
usage, from the repository root: .venv/bin/python -I repro_u14a.py [--cost]   (stdout: JSON)
No database is opened and no model is called: socket.connect is replaced before any engine import,
so a network attempt raises instead of leaving the process. The engine modules are imported, never
edited; the autojunk=False figures come from `locate_like`, a copy of locate's window loop whose
one difference is the SequenceMatcher argument, checked against locate itself (autojunk on) on
every case before it is used."""
import json, random, socket, statistics, sys, time
from difflib import SequenceMatcher
from pathlib import Path

def _no_network(*a, **k): raise RuntimeError("network attempt in a reproducer")
socket.socket.connect = _no_network
sys.path.insert(0, str(Path.cwd()))

from engine.core import locator as L                      # noqa: E402
from engine.core.constants import INVALID_SNIPPET_RE      # noqa: E402

OUT = {"locator_version": L.LOCATOR_VERSION, "fuzzy_threshold": L.FUZZY_THRESHOLD}


def locate_like(text, snippet, *, autojunk):
    """locate()'s fuzzy branch with SequenceMatcher's autojunk set explicitly."""
    ns, nt = L.normalize(snippet), L.normalize(text)
    if ns in nt:
        return {"kind": L.EXACT, "score": 1.0, "located": True}
    sw, tw = ns.split(), nt.split(); size = len(sw); best = 0.0
    for i in range(max(1, len(tw) - size + 1)):
        r = SequenceMatcher(None, ns, " ".join(tw[i:i + size]), autojunk=autojunk).ratio()
        best = max(best, r)
    loc = best > L.FUZZY_THRESHOLD
    return {"kind": L.FUZZY if loc else L.NONE, "score": best, "located": loc}


def both(text, snippet):
    r = L.locate(text, snippet)
    on, off = locate_like(text, snippet, autojunk=True), locate_like(text, snippet, autojunk=False)
    assert (on["located"], on["kind"]) == (r.located, r.kind) and abs(on["score"] - r.score) < 1e-12, \
        "locate_like(autojunk=True) does not reproduce locate"
    return {"snippet_chars": len(snippet), "snippet_words": len(snippet.split()),
            "locate_module": {"located": r.located, "kind": r.kind, "score": round(r.score, 4),
                              "bridged": r.bridged},
            "autojunk_false": {"located": off["located"], "kind": off["kind"],
                               "score": round(off["score"], 4)}}


# ── INT-i12-7 ─────────────────────────────────────────────────────────
PASSAGE = (
    "We used the robotic platform to suture the small bowel in twelve adult pigs. The arm held a "
    "curved needle with braided nylon thread. Each knot was tied by the arm, then trimmed by a nurse "
    "after every run. We observed no leakage at either test pressure in any animal. The mean time "
    "per stitch was between six and nine seconds, and no procedure was stopped early or finished manually.")
BEFORE = ("The authors describe the experimental platform in detail and report its calibration "
          "procedure before the main experiments. ")
AFTER = (" The remaining results are presented in the following tables and are discussed with "
         "reference to previous reports.")
TEXT = BEFORE + PASSAGE + AFTER
EDITS = {"twelve": "eleven", "braided": "plaited", "nurse": "nurses"}


def edit(s, edits):
    w = s.split()
    return " ".join(edits.get(x, x) for x in w)


snip379 = edit(PASSAGE, EDITS)
changed = sum(a != b for a, b in zip(PASSAGE.split(), snip379.split()))
OUT["i12_7"] = {
    "row_text_figures": "379 characters, 3 of 70 words changed, 0.732 against 0.974 (12i read-out)",
    "passage_chars": len(PASSAGE), "passage_words": len(PASSAGE.split()), "words_changed": changed,
    "case_long_3_words_changed": both(TEXT, snip379),
    "case_long_0_words_changed": both(TEXT, PASSAGE),
}
short = PASSAGE[:150].rsplit(" ", 1)[0]
OUT["i12_7"]["case_150_same_edit"] = dict(both(TEXT, edit(short, EDITS)),
                                          words_changed=sum(a != b for a, b in zip(short.split(), edit(short, EDITS).split())))


def sweep_of(passage, n, seed):
    """n draws of 3 words changed at seeded positions ("zq" replaces each word's first letter)."""
    rng = random.Random(seed); words = passage.split(); rows = []
    for _ in range(n):
        w = list(words)
        for i in rng.sample(range(len(w)), 3):
            w[i] = "zq" + w[i][1:]
        s = " ".join(w); rows.append((s, both(TEXT, s)))
    on = [b["locate_module"]["score"] for _, b in rows]; off = [b["autojunk_false"]["score"] for _, b in rows]
    worst = min(rows, key=lambda r: r[1]["locate_module"]["score"])
    return {"n": n, "seed": seed, "snippet_chars": len(rows[0][0]),
            "autojunk_on": {"min": min(on), "median": statistics.median(on), "max": max(on),
                            "not_located": sum(not b["locate_module"]["located"] for _, b in rows)},
            "autojunk_off": {"min": min(off), "median": statistics.median(off), "max": max(off),
                             "not_located": sum(not b["autojunk_false"]["located"] for _, b in rows)},
            "worst_draw_autojunk_on": {"snippet": worst[0], **worst[1]}}


OUT["i12_7"]["sweep_long_3_words_changed"] = sweep_of(PASSAGE, 300, 20261019)
OUT["i12_7"]["sweep_150_3_words_changed"] = sweep_of(short, 300, 20261019)

# the existing fuzzy fixtures, each string checked to occur verbatim in its test file
T_LOC = ("The trial enrolled forty patients at two centres. The robot performed "
         "autonomous suturing on porcine tissue in ten trials. Outcomes were "
         "measured at six months.")
T_AUD = ("This study evaluates the Smart Tissue Autonomous Robot (STAR) system "
         "for autonomous suturing. A randomized controlled trial was conducted "
         "with 20 trials on porcine tissue. The system achieved Level 3 autonomy "
         "per the Yang et al. taxonomy. Suture placement accuracy was 95.2% "
         "compared to 91.0% for expert surgeons. No tissue damage events were "
         "reported during autonomous operation.")
FIXTURES = [
    ("tests/test_locator.py", T_LOC, "The robot performd autonomous suturing on porcine tisue in ten trials."),
    ("tests/test_locator.py", T_LOC, "Conducted in Norway by the authors of this study."),
    ("tests/test_locator.py", T_LOC, "Conducted in Norway."),
    ("tests/test_locator.py", T_LOC, "outcomes were measured at six month"),
    ("tests/test_locator.py", T_LOC, "Table.I shows"),
    ("tests/test_auditor.py", T_AUD, "20 trials on porcme tissue"),
    ("tests/test_auditor.py", T_AUD, "500 experiments on synthetic phantoms with laser guidance"),
    ("tests/test_auditor.py", T_AUD, "The experiment was performed on cadaveric specimens"),
    ("tests/test_auditor.py", T_AUD, "Cooking temperature affects protein denaturation"),
    ("tests/test_auditor.py", T_AUD, "This snippet does not exist in the paper text at all."),
]
fx = []
for path, text, s in FIXTURES:
    assert s in Path(path).read_text(encoding="utf-8"), (path, s)
    b = both(text, s)
    fx.append({"file": path, "snippet": s, "chars": b["snippet_chars"], "words": b["snippet_words"],
               "autojunk_on": [b["locate_module"]["located"], b["locate_module"]["score"]],
               "autojunk_off": [b["autojunk_false"]["located"], b["autojunk_false"]["score"]]})
OUT["fuzzy_fixtures"] = {"rows": fx, "max_chars": max(r["chars"] for r in fx),
                         "any_at_or_over_200": any(r["chars"] >= 200 for r in fx),
                         "outcome_differs_under_autojunk_false": sum(r["autojunk_on"] != r["autojunk_off"] for r in fx)}

# ── INT-i12-8 ─────────────────────────────────────────────────────────
from engine.agents.auditor import audit_span       # noqa: E402  (imports only; no call reaches a model)
from engine.agents.extractor import _has_invalid_snippet   # noqa: E402

T_U = ("Patients were followed for twelve months… Two were lost to follow-up. "
       "The device was removed in three cases. Complications were minor.")
T_A = ("The protocol was revised twice... Recruitment closed in March. "
       "The device was removed in three cases. Complications were minor.")
T_P = ("Patients were followed for twelve months. Two were lost to follow-up. "
       "The device was removed in three cases. Complications were minor.")
cases = {
    "verbatim_quote_with_unicode_ellipsis": (T_U, "Patients were followed for twelve months… Two were lost to follow-up."),
    "verbatim_quote_with_three_dots": (T_A, "The protocol was revised twice... Recruitment closed in March."),
    "control_same_text_quote_without_ellipsis": (T_U, "Two were lost to follow-up."),
    "bridging_snippet_ellipsis_not_in_text": (T_P, "Two were lost to follow-up ... Complications were minor."),
}
r8 = {}
for name, (text, s) in cases.items():
    r = L.locate(text, s)
    status, _why = audit_span({"field_name": "f", "value": "v", "source_snippet": s}, text, cfg=None) \
        if INVALID_SNIPPET_RE.search(s) else ("(not reached: the precheck does not fire)", "")
    r8[name] = {"snippet": s, "regex_fires_on_snippet": bool(INVALID_SNIPPET_RE.search(s)),
                "ellipsis_in_text": bool(INVALID_SNIPPET_RE.search(text)),
                "locate": {"located": r.located, "kind": r.kind, "score": round(r.score, 4),
                           "bridged": r.bridged},
                "extractor__has_invalid_snippet": _has_invalid_snippet(s),
                "auditor_audit_span": status}
OUT["i12_8"] = {"pattern": INVALID_SNIPPET_RE.pattern, "nfkc_of_U+2026": L.normalize("…"), "cases": r8}

# ── cost reading ──────────────────────────────────────────────────────
if "--cost" in sys.argv:
    N_CHARS = 156_196   # the largest auditable live text, 12g_RA-A.md row 3 (paper 11, v1)
    rng = random.Random(7); sents = [PASSAGE, BEFORE.strip(), AFTER.strip()] + [
        "The outcome measures were defined before enrolment and assessed by an independent reviewer.",
        "Statistical analysis used a two-sided test with a significance level of five percent.",
        "Table 2 summarises the baseline characteristics of the included specimens."]
    big = []
    while sum(map(len, big)) < N_CHARS:
        big.append(rng.choice(sents[1:]))
    big_text = " ".join(big)[:N_CHARS]
    absent = edit(PASSAGE, EDITS)       # not in big_text: every window is scored
    assert L.normalize(absent) not in L.normalize(big_text)
    t_on, t_off = [], []
    for _ in range(3):
        t0 = time.perf_counter(); L.locate(big_text, absent); t_on.append(time.perf_counter() - t0)
        t0 = time.perf_counter(); locate_like(big_text, absent, autojunk=False); t_off.append(time.perf_counter() - t0)
    OUT["cost"] = {"text_chars": len(big_text), "text_words": len(big_text.split()),
                   "snippet_chars": len(absent), "runs_s_autojunk_on_locate": [round(x, 3) for x in t_on],
                   "runs_s_autojunk_off": [round(x, 3) for x in t_off],
                   "median_s_autojunk_on": round(statistics.median(t_on), 3),
                   "median_s_autojunk_off": round(statistics.median(t_off), 3)}

print(json.dumps(OUT, indent=1, ensure_ascii=False))
