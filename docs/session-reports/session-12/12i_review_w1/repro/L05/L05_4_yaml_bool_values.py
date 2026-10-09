"""Unquoted yes/no in valid_values or absence_sentinels load as booleans; the loader accepts them."""
from _common import CODEBOOK, write, LIVE_SPEC
from engine.core.codebook import load_codebook
from engine.core.review_spec import load_review_spec
text = (CODEBOOK.replace('{value: "Yes",', '{value: Yes,').replace('{value: "No",', '{value: No,')
        .replace('absence_sentinels: ["NR", "NOT_FOUND"]', 'absence_sentinels: ["NR", "NOT_FOUND", NO, null]'))
p = write("bool.yaml", text)
cb = load_codebook(p)
print("enum_values('randomised') =", cb.enum_values("randomised"))
print("absence_sentinels =", cb.absence_sentinels)
print("is_absence_sentinel('NO') =", cb.is_absence_sentinel("NO"), "| ('False') =", cb.is_absence_sentinel("False"),
      "| ('None') =", cb.is_absence_sentinel("None"))
from engine.agents.extractor import build_extraction_prompt
try:
    prompt = build_extraction_prompt("PAPER", load_review_spec(LIVE_SPEC), p)
    line = [l for l in prompt.splitlines() if "True" in l or "False" in l]
    print("prompt lines naming the values:", line[:4])
except Exception as e:
    print("build_extraction_prompt raised:", type(e).__name__, e)
from engine.analysis.normalize import _build_prefix_map
try:
    _build_prefix_map(cb.enum_values("randomised"))
except Exception as e:
    print("normalize._build_prefix_map raised:", type(e).__name__, e)
