"""L01-1: engine.core.locator.locate builds SequenceMatcher with autojunk at its
default (True). For a window >= 200 chars difflib treats every character that
makes up >1% of the window as 'popular' and will not anchor a match on it, so a
near-verbatim long snippet scores far below its true similarity.
Pure functions on synthetic strings; no database, no model."""
from difflib import SequenceMatcher
from engine.core.locator import locate, normalize, FUZZY_THRESHOLD

body = ("the robot completed the anastomosis without any human intervention and the "
        "surgeon remained at the console to observe the entire task as it was carried "
        "out on the porcine intestine in the same theatre on the same morning and the "
        "team noted that the needle and the thread were held in the usual manner as in "
        "the earlier animal series that the same team had done in the same centre")
text = "Background and aims are stated here. " + body + ". Further sections follow here."

def variant(n_edits):
    words = body.split()
    step = len(words) // (n_edits + 1)
    for k in range(1, n_edits + 1):
        words[k * step] = "that" if words[k * step] != "that" else "this"
    return " ".join(words)

print(f"snippet length (chars): {len(body)}   threshold: {FUZZY_THRESHOLD}")
for n in (1, 2, 3, 5):
    snip = variant(n)
    res = locate(text, snip)
    # the true similarity of the snippet to the passage it was taken from
    true = SequenceMatcher(None, normalize(snip), normalize(body), autojunk=False).ratio()
    junk = SequenceMatcher(None, normalize(snip), normalize(body)).ratio()
    print(f"{n} word(s) changed of {len(body.split())}: locate -> located={res.located} "
          f"kind={res.kind} score={res.score:.3f} | same pair autojunk=True {junk:.3f} "
          f"autojunk=False {true:.3f}")

short = "the robot completed the anastomosis without any human intervention and the surgeon remained"
s2 = short.replace("without", "with")
r = locate(text, s2)
print(f"control, short snippet ({len(s2)} chars, 1 word changed): located={r.located} "
      f"kind={r.kind} score={r.score:.3f}")
