"""`/api/ps` for a fake client under a run, for `ollama_chat`'s loaded-context check (U20
commit 2, R744(c)): each model the active run declared, with the digest its manifest recorded,
loaded at the ceiling the fake's `show` gives. Built from a body the 12aa-U20P probe committed,
through the library's own response class, with only name, digest and context_length changed.

Outside a run it lists nothing, as the fakes' `ps` did before. Underscore-prefixed so pytest
does not collect it.
"""
from __future__ import annotations

import json
from pathlib import Path

import ollama

from engine.core import run_manifest as rm

_BODY = Path(__file__).resolve().parent.parent / "docs/session-reports/session-12/12aa_U20P/load/P3_ps.json"
TEMPLATE = json.loads(_BODY.read_text(encoding="utf-8"))["models"][0]


def loaded_ps(context_length: int = 131_072) -> ollama.ProcessResponse:
    active = rm._ACTIVE.get()
    if active is None:
        return ollama.ProcessResponse(models=[])
    conn, run_id = active
    rows = conn.execute("SELECT DISTINCT model_name, model_digest FROM run_stage_configs "
                        "WHERE run_id = ?", (run_id,)).fetchall()
    return ollama.ProcessResponse(models=[
        {**TEMPLATE, "name": row[0], "model": row[0], "digest": row[1], "context_length": context_length}
        for row in rows])
