"""The Docling glyph-placeholder pattern, and nothing else.

Extracted from `parse_quality.py` by FONT-AUDIT-01 so that a module needing only
the pattern does not inherit `parse_quality`'s dependency chain. That chain is
real and deliberate -- `parse_quality` imports the segmenter from
`analysis.provenance.segment`, taking an explicit exception to the rule that the
study lane is not an engine dependency, because there must be exactly one
segmenter. A regex is not a segmenter, and `font_audit.py` needs the regex
without the exception, so the pattern lives here and `parse_quality` imports it.

**This module imports nothing.** That is the point of it, and
`tests/test_font_audit.py::test_font_audit_does_not_import_the_analysis_lane`
is what keeps it true.

**Two marker forms, both matched, and the second is not a typo.** Docling
normally writes `GLYPH<c=3,font=/JGFKKL+TimesNewRoman>`, and the stored `.md`
holds it HTML-escaped as `GLYPH&lt;c=3,font=/...&gt;` -- so a plain
``grep 'GLYPH<'`` over `parsed_text/` returns zero on most papers and undercounts
the rest (PARSE-GATE-09 §4). It also writes a bare `GLYPH<31>` carrying **no**
`c=` and **no** `font=`: measured in `618_v2.md`, where it stands for a diameter
sign set in a simple Type1 CMEX font that is not a composite font at all
(PARSE-GATE-10 §8.2). Both forms count as damage; only the first can be
attributed to a font.
"""

from __future__ import annotations

import re

#: Matches both marker forms Docling emits -- the raw one and the HTML-escaped
#: one the parsed markdown actually stores. Character-for-character the pattern
#: `analysis/eval/parse01/sweep.py` used, which is why `test_parse_quality.py`
#: can pin `compute_metrics` against that study's committed `sweep.jsonl`.
RE_GLYPH = re.compile(r"GLYPH<[^>]*>|GLYPH&lt;[^&]*&gt;")

#: Pulls the CID out of an *attributed* marker of either form. A marker that
#: does not match this is a bare `GLYPH<31>`: it is damage, it is countable, and
#: it cannot be charged to a font.
RE_GLYPH_ATTRIBUTED = re.compile(
    r"GLYPH(?:<|&lt;)c=(\d+),font=/([^>&]+)(?:>|&gt;)"
)
