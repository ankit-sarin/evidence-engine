"""12s-U14B step 1 — R675(c)'s variant measurement, before any engine edit. Evidence, not a test.
usage, from the repository root: .venv/bin/python -I variant_measurement.py   (stdout: JSON)
The 12s_U14A reproducer is executed unchanged (its file is not edited) and its definitions reused:
PASSAGE, TEXT, the seeded 300-edit sweep and `locate_like`. Three locators are compared:
  today   — engine.core.locator.locate as committed at 2d8823e (autojunk on);
  plain A — every window scored with autojunk=False;
  variant — every window scored with autojunk on, as the module does it; the k = 5 best re-scored
            with autojunk=False; the best of those.
No database, no model; socket.connect is replaced before any engine import (inherited from U14A)."""
import contextlib, io, json, random, statistics, sys, time
from difflib import SequenceMatcher
from pathlib import Path

SRC = Path("docs/session-reports/session-12/12s_U14A/repro_u14a.py")
g = {"__name__": "repro_u14a", "__file__": str(SRC)}
argv, sys.argv = sys.argv, [str(SRC)]
with contextlib.redirect_stdout(io.StringIO()) as buf:
    exec(compile(SRC.read_text(encoding="utf-8"), str(SRC), "exec"), g)
sys.argv = argv
U14A = json.loads(buf.getvalue())
L, PASSAGE, TEXT, EDITS, edit = g["L"], g["PASSAGE"], g["TEXT"], g["EDITS"], g["edit"]
K = 5


def windows(text, snippet):
    ns, nt = L.normalize(snippet), L.normalize(text)
    sw, tw = ns.split(), nt.split(); size = len(sw)
    return ns, nt, [" ".join(tw[i:i + size]) for i in range(max(1, len(tw) - size + 1))]


def plain_a(text, snippet):
    ns, nt, ws = windows(text, snippet)
    if ns in nt:
        return {"located": True, "kind": L.EXACT, "score": 1.0, "window": None}
    best, at = 0.0, None
    for i, w in enumerate(ws):
        r = SequenceMatcher(None, ns, w, autojunk=False).ratio()
        if r > best:
            best, at = r, i
    return {"located": best > L.FUZZY_THRESHOLD, "kind": L.FUZZY if best > L.FUZZY_THRESHOLD else L.NONE,
            "score": best, "window": at}


def variant(text, snippet, k=K):
    ns, nt, ws = windows(text, snippet)
    if ns in nt:
        return {"located": True, "kind": L.EXACT, "score": 1.0, "window": None}
    on = [(SequenceMatcher(None, ns, w).ratio(), i) for i, w in enumerate(ws)]
    top = sorted(on, key=lambda t: (-t[0], t[1]))[:k]
    best, at = 0.0, None
    for _, i in sorted(top, key=lambda t: t[1]):
        r = SequenceMatcher(None, ns, ws[i], autojunk=False).ratio()
        if r > best:
            best, at = r, i
    return {"located": best > L.FUZZY_THRESHOLD, "kind": L.FUZZY if best > L.FUZZY_THRESHOLD else L.NONE,
            "score": best, "window": at}


# the 300 seeded edits: U14A's sweep_of generation, copied, and checked against U14A's own output
rng = random.Random(20261019); words = PASSAGE.split(); rows = []
for _ in range(300):
    w = list(words)
    for i in rng.sample(range(len(w)), 3):
        w[i] = "zq" + w[i][1:]
    s = " ".join(w)
    t = L.locate(TEXT, s); a = plain_a(TEXT, s); v = variant(TEXT, s)
    rows.append({"snippet": s, "today": [t.located, round(t.score, 4)], "plain_a": [a["located"], round(a["score"], 4), a["window"]],
                 "variant": [v["located"], round(v["score"], 4), v["window"]]})
today_lost = sum(not r["today"][0] for r in rows)
ref = U14A["i12_7"]["sweep_long_3_words_changed"]
assert today_lost == ref["autojunk_on"]["not_located"] and min(r["today"][1] for r in rows) == ref["autojunk_on"]["min"], \
    "the copied sweep does not reproduce 12s_U14A's"
differ = [r for r in rows if r["variant"][2] != r["plain_a"][2]]
lost_a = {i for i, r in enumerate(rows) if not r["plain_a"][0]}
lost_v = {i for i, r in enumerate(rows) if not r["variant"][0]}
recovered = [i for i, r in enumerate(rows) if not r["today"][0]]
OUT = {"k": K, "n": len(rows),
       "lost": {"today": today_lost, "plain_a": len(lost_a), "variant": len(lost_v)},
       "variant_loses_what_plain_a_locates": sorted(lost_v - lost_a),
       "today_lost_rows": [{"i": i, **{k: rows[i][k] for k in ("today", "plain_a", "variant")}} for i in recovered],
       "window_choice_differs": [{"i": rows.index(r), "plain_a": r["plain_a"], "variant": r["variant"]} for r in differ],
       "scores": {name: {"min": min(r[name][1] for r in rows), "median": statistics.median(r[name][1] for r in rows)}
                  for name in ("today", "plain_a", "variant")}}

# the F11 timing: U14A's text construction, copied
N_CHARS = 156_196
rng = random.Random(7); sents = [PASSAGE, g["BEFORE"].strip(), g["AFTER"].strip()] + [
    "The outcome measures were defined before enrolment and assessed by an independent reviewer.",
    "Statistical analysis used a two-sided test with a significance level of five percent.",
    "Table 2 summarises the baseline characteristics of the included specimens."]
big = []
while sum(map(len, big)) < N_CHARS:
    big.append(rng.choice(sents[1:]))
big_text = " ".join(big)[:N_CHARS]
absent = edit(PASSAGE, EDITS)
assert L.normalize(absent) not in L.normalize(big_text) and len(big_text) == N_CHARS
times = {"today": [], "plain_a": [], "variant": []}
for _ in range(3):
    for name, fn in (("today", lambda: L.locate(big_text, absent)), ("plain_a", lambda: plain_a(big_text, absent)),
                     ("variant", lambda: variant(big_text, absent))):
        t0 = time.perf_counter(); fn(); times[name].append(round(time.perf_counter() - t0, 3))
med = {k: statistics.median(v) for k, v in times.items()}
OUT["timing"] = {"text_chars": len(big_text), "snippet_chars": len(absent), "runs_s": times, "median_s": med,
                 "variant_over_today": round(med["variant"] / med["today"], 3),
                 "variant_over_5_214": round(med["variant"] / 5.214, 3),
                 "plain_a_over_today": round(med["plain_a"] / med["today"], 3)}
cond_recover = OUT["lost"]["variant"] == 0 and not OUT["variant_loses_what_plain_a_locates"]
cond_cost = med["variant"] <= 1.5 * 5.214
OUT["condition"] = {"recovers_all_and_loses_nothing_plain_a_locates": cond_recover,
                    "variant_median_le_1_5x_5_214": cond_cost, "window_choice_differs_count": len(differ),
                    "met": cond_recover and cond_cost}
print(json.dumps(OUT, indent=1, ensure_ascii=False))
