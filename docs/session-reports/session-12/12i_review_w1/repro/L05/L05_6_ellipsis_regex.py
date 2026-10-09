"""INVALID_SNIPPET_RE fires on a snippet that is verbatim in the paper."""
from engine.core.constants import INVALID_SNIPPET_RE
from engine.core.locator import locate
paper = ("The surgeon noted that the robot “performed the anastomosis … without intervention” in all trials. "
         "Success was 9/10 (90%), i.e. the task completed...as planned.")
for snip in ("the robot “performed the anastomosis … without intervention” in all trials",
             "the task completed...as planned"):
    r = locate(paper, snip)
    print(repr(snip[:45]), "| regex hit:", bool(INVALID_SNIPPET_RE.search(snip)), "| located:", r.located, r.kind, "| bridged:", r.bridged)
