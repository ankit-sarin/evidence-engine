"""12s-U14B — the committed locator (locator-1.1) on 12s_U14A's evidence: the 300 seeded edits,
the INT-i12-7 and INT-i12-8 cases, and the F11 timing. Evidence, not a test.
usage, from the repository root: .venv/bin/python -I post_fix_check.py   (stdout: JSON)
The fixture strings are read from 12s_U14A/repro_u14a.py by AST (literal assignments only). That
script is not executed: its fidelity check asserts its autojunk-on copy equals `locate`, which held
for locator-1 and by design no longer holds for locator-1.1. No database, no model; socket.connect
is replaced before the engine import."""
import ast, json, random, socket, statistics, sys, time
from pathlib import Path

def _no_network(*a, **k): raise RuntimeError("network attempt in an evidence script")
socket.socket.connect = _no_network
sys.path.insert(0, str(Path.cwd()))
from engine.core import locator as L      # noqa: E402

SRC = Path("docs/session-reports/session-12/12s_U14A/repro_u14a.py")
g = {}
for node in ast.parse(SRC.read_text(encoding="utf-8")).body:
    if (isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id in {"PASSAGE", "BEFORE", "AFTER", "EDITS", "T_U", "T_A", "T_P"}):
        g[node.targets[0].id] = ast.literal_eval(node.value)
assert len(g) == 7, sorted(g)
PASSAGE, EDITS = g["PASSAGE"], g["EDITS"]
TEXT = g["BEFORE"] + PASSAGE + g["AFTER"]


def edit(s, edits):
    return " ".join(edits.get(x, x) for x in s.split())


OUT = {"locator_version": L.LOCATOR_VERSION, "fuzzy_threshold": L.FUZZY_THRESHOLD,
       "rescore_top_k": L.RESCORE_TOP_K}

rng = random.Random(20261019); words = PASSAGE.split(); res = []
for _ in range(300):
    w = list(words)
    for i in rng.sample(range(len(w)), 3):
        w[i] = "zq" + w[i][1:]
    res.append(L.locate(TEXT, " ".join(w)))
OUT["sweep_300"] = {"not_located": sum(not r.located for r in res),
                    "min": round(min(r.score for r in res), 4),
                    "median": round(statistics.median(r.score for r in res), 4)}
OUT["i12_7_cases"] = {"long_3_words": vars(L.locate(TEXT, edit(PASSAGE, EDITS))),
                      "long_unchanged": vars(L.locate(TEXT, PASSAGE))}
OUT["i12_8_cases"] = {k: vars(L.locate(t, s)) for k, (t, s) in {
    "verbatim_unicode_ellipsis": (g["T_U"], "Patients were followed for twelve months… Two were lost to follow-up."),
    "verbatim_three_dots": (g["T_A"], "The protocol was revised twice... Recruitment closed in March."),
    "control": (g["T_U"], "Two were lost to follow-up."),
    "bridging_on_text_without_ellipsis": (g["T_P"], "Two were lost to follow-up ... Complications were minor."),
}.items()}

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
runs = []
for _ in range(3):
    t0 = time.perf_counter(); L.locate(big_text, absent); runs.append(round(time.perf_counter() - t0, 3))
OUT["timing_f11"] = {"text_chars": len(big_text), "snippet_chars": len(absent), "runs_s": runs,
                     "median_s": statistics.median(runs)}
print(json.dumps(OUT, indent=1, ensure_ascii=False))
