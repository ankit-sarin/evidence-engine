"""Shared constants used across extraction and audit agents."""

import re

# Pattern detecting invalid snippets: ellipsis bridging or abbreviation.
# Matches: ... (3+ dots), [...], [U+2026], or the Unicode ellipsis character.
INVALID_SNIPPET_RE = re.compile(r"\[\.{3}\]|\[…\]|…|\.{3,}")

# Maximum token budget for full-text screening prompt (chars, ~4 chars/token)
FT_MAX_TEXT_CHARS = 32_000  # ~8,000 tokens
