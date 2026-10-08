"""F11: what truncate_paper_text does (pure function, no model, no DB)."""
from engine.agents.ft_screener import truncate_paper_text, _SECTION_RE
from engine.core.constants import FT_MAX_TEXT_CHARS
import engine.agents.ft_screener as m, inspect

intro = "# Introduction\n" + ("Filler sentence about context. " * 1500)   # ~46k chars
methods = "\n# Methods\nMETHODS_MARKER the robot performed the task autonomously.\n"
results = "# Results\nRESULTS_MARKER success rate 97%.\n"
full = intro + methods + results
out = truncate_paper_text(full, title="T", abstract="A")
print("max_chars", FT_MAX_TEXT_CHARS, "| input", len(full), "| output", len(out))
print("A. Methods kept:", "METHODS_MARKER" in out, "| Results kept:", "RESULTS_MARKER" in out)
print("A. any omission marker in output:", any(k in out.lower() for k in ("truncat", "omitted", "[...]", "…")))
print("A. output is a pure prefix of header+body:", ("Title: T\n\nAbstract: A\n\n" + full).startswith(out))

# B. an early line starting with 'References' / 'Acknowledgements' cuts EARLIER than the budget
early = "# Introduction\nShort intro.\nReferences to prior work are given in Section 2.\n" + full
outb = truncate_paper_text(early, title="T", abstract="A")
print("B. early 'References ...' prose line: output", len(outb), "chars of", len(early), "->", repr(outb[-40:]))

# C. abstract longer than the budget: no body at all
outc = truncate_paper_text(full, title="T", abstract="x" * 40000)
print("C. 40k-char abstract: output", len(outc), "| contains any body:", "Introduction" in outc)

# D. the section regex the docstring's 'prioritizing' would need is never used
src = inspect.getsource(m)
print("D. _SECTION_RE occurrences in module source:", src.count("_SECTION_RE"), "(1 = definition only)")
