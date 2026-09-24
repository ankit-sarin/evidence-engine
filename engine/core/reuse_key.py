"""S3d's extraction reuse key (R91).

`(arm, paper_id, parsed_text_sha256)`. The other components the plan's S3d
six-tuple named — codebook hash, prompt hash, model digest, options hash — are
NOT hashed in again: the arm carries them. An arm is pinned at its first
manifest to a tuple holding exactly those (`run_manifest.pin_tuple`), and a
later run resolving differently refuses (`ArmPinMismatch`, R10/R21; the four
refusals are pinned by name in `tests/test_run_manifest.py`, two of them by
R97). A changed configuration is therefore a new arm NAME, and the arm's name
is already in the key.

**Scheme (stable; changing it is a new scheme id, never an edit):**

    rk1:<sha256 of canonical_json({"arm": <str>, "paper_id": <int>,
                                   "parsed_text_sha256": <64 lowercase hex>})>

`canonical_json` is the one serialisation every manifest hash is taken over
(`engine.core.effective_config`); the key is not a second one.

**Consumer: session 9.** Nothing calls this in session 8 (R92). The extractor's
cut-over to the event writer uses it at selection — "skip if this arm already
asserted this paper under this text, else extract and supersede by event".
It is pure: no I/O, no database, no clock.
"""

from __future__ import annotations

import re

from engine.core.effective_config import sha256_canonical

SCHEME = "rk1"
_HEX64 = re.compile(r"[0-9a-f]{64}")


def reuse_key(arm_id: str, paper_id: int, parsed_text_sha256: str) -> str:
    """The reuse key of one (arm, paper, parsed text). Refuses a malformed part
    rather than hashing it: a key built from a wrong-typed paper id or an
    upper-case digest would silently never match the key built correctly."""
    if not isinstance(arm_id, str) or not arm_id:
        raise ValueError(f"reuse_key: arm_id must be a non-empty string, got {arm_id!r}")
    if isinstance(paper_id, bool) or not isinstance(paper_id, int):
        raise ValueError(f"reuse_key: paper_id must be an int, got {paper_id!r}")
    if not isinstance(parsed_text_sha256, str) or not _HEX64.fullmatch(parsed_text_sha256):
        raise ValueError("reuse_key: parsed_text_sha256 must be 64 lowercase hex "
                         f"characters, got {parsed_text_sha256!r}")
    digest = sha256_canonical({"arm": arm_id, "paper_id": paper_id,
                               "parsed_text_sha256": parsed_text_sha256})
    return f"{SCHEME}:{digest}"
