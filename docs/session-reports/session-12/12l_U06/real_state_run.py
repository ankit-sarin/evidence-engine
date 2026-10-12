"""12l-U06 — R535's floor on real state (R560, RB-4; 12l_P1A §3 U06). One time, not part of the gate.

usage, from the repository root:
    real_state_run.py copy  <review_id>      make the throwaway copy (R235) and print its provenance
    real_state_run.py run   <review_id>      drive the three cases on the copy and print the results

`copy` — R235: a verified `auto_backup` of the live review, moved to `data/<review_id>/review.db`;
`spec.yaml` and `extraction_codebook.yaml` copied beside it with `review_id` / `review:` changed and
nothing else. Live is opened `mode=ro` by the backup; nothing is written to it.

`run` — on the copy only (refuses any database under `data/surgical_autonomy`): the real elicited
path, the real codebook, the papers' real parsed texts, the real manifest opener and event writer.
The model is a fake client at `ollama_client._client`; no model is called. Three runs, one per case,
each over the same papers (the smallest parsed texts, so the input-fit guard is not what is tested):

  A  Pass 1 answers with an empty response on both attempts.
  B  Pass 1 meets every contract; Pass 2 returns 5 of 20 fields on all three attempts
     (an exhausted record under the floor).
  C  Pass 1 meets every contract; Pass 2 returns 14 of 20 fields on all three attempts
     (an exhausted record at or above the floor).

The live spec has `elicitation: false`; the runs set it in memory (the copy's `spec.yaml` is R235's
copy and is not edited), which is the configuration Run 7 turns on.
"""
import dataclasses, hashlib, json, shutil, sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, ".")
LIVE = Path("data/surgical_autonomy")
PAPERS = 3


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def copy(review_id: str) -> dict:
    from engine.tools.db_fingerprint import fingerprint
    from engine.utils.db_backup import auto_backup
    dest = Path("data") / review_id
    assert review_id != "surgical_autonomy" and not dest.exists(), dest
    started = now()
    result = auto_backup(LIVE / "review.db", "u06-floor")          # mode=ro read of live, verified
    dest.mkdir(parents=True)
    shutil.move(str(result.path), dest / "review.db")              # nothing is left beside live
    spec = Path("review_specs/surgical_autonomy.yaml").read_text()
    assert spec.count("review_id: surgical_autonomy") == 1
    (dest / "spec.yaml").write_text(spec.replace("review_id: surgical_autonomy",
                                                 f"review_id: {review_id}"))
    cb = (LIVE / "extraction_codebook.yaml").read_text()
    assert cb.count('review: "surgical_autonomy"') == 1
    (dest / "extraction_codebook.yaml").write_text(
        cb.replace('review: "surgical_autonomy"', f'review: "{review_id}"'))
    fp = fingerprint(dest / "review.db")
    return {"copied": "data/surgical_autonomy/review.db (online backup API, verified against its "
                      "source), review_specs/surgical_autonomy.yaml, "
                      "data/surgical_autonomy/extraction_codebook.yaml",
            "to": str(dest), "started_utc": started, "finished_utc": now(),
            "source_overall_sha256": result.fingerprint["overall_sha256"],
            "copy_overall_sha256": fp["overall_sha256"], "copy_tables": len(fp["tables"]),
            "copy_equals_source": fp["overall_sha256"] == result.fingerprint["overall_sha256"]}


class FakeClient:
    """`ollama.Client` stand-in. Pass 1 (no `format`) answers `pass1`; Pass 2 (schema-constrained)
    answers the first `pass2_fields` codebook fields."""

    def __init__(self, cb, pass1: str, pass2_fields: int):
        from engine.agents.models import EvidenceSpan, ExtractionOutput
        self.pass1 = pass1
        self.pass2 = ExtractionOutput(fields=[
            EvidenceSpan(field_name=f["name"], value="v", source_snippet="typed by the model",
                         confidence=0.9, tier=f["tier"]) for f in cb.fields[:pass2_fields]
        ]).model_dump_json()
        self.calls = {"pass1": 0, "pass2": 0}
        self._client = SimpleNamespace(base_url="http://u06.invalid")

    def show(self, model):
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def chat(self, **kwargs):
        import engine.utils.ollama_client as oc
        second = kwargs.get("format") is not None
        self.calls["pass2" if second else "pass1"] += 1
        chars = oc.message_chars(kwargs.get("messages"))
        return SimpleNamespace(
            message=SimpleNamespace(content=self.pass2 if second else self.pass1,
                                    thinking="reasoning"),
            prompt_eval_count=max(1, int(chars * 0.3)), done_reason="stop", eval_count=10)


def good_pass1(cb) -> str:
    from engine.elicitation import classes as C
    out = []
    for f in cb.fields:
        cls = C.field_class(f["name"], cb.raw)
        if cls == C.STATED:
            out.append({"field_name": f["name"], "unit_indices": [1], "value": "v"})
        elif cls == C.INFERABLE:
            out.append({"field_name": f["name"], "unit_indices": [1],
                        "inference": "The cited unit fixes the value.", "value": "v"})
        else:
            out.append({"field_name": f["name"], "value": "v", "reasoning_steps": [
                {"step": "The cited unit describes it.", "unit_indices": [1]}]})
    return json.dumps({"fields": out})


def run(review_id: str) -> dict:
    import httpx
    import engine.utils.ollama_client as oc
    from engine.agents import extractor as E
    from engine.core import extraction_events as X
    from engine.core import run_manifest as rm
    from engine.core.codebook import load_codebook_beside
    from engine.core.database import ReviewDatabase
    from engine.core.review_spec import load_review_spec
    from engine.core.selection import select_for_extraction
    from engine.exporters.prisma import generate_prisma_flow, validate_prisma_counts
    from engine.tools.db_fingerprint import fingerprint

    root = Path("data") / review_id
    assert review_id != "surgical_autonomy" and (root / "review.db").exists()

    def refuse(*a, **k):
        raise AssertionError("a real HTTP request was attempted")
    httpx.Client.send = refuse
    oc.fetch_model_digest = lambda m: (_ for _ in ()).throw(AssertionError("digest fetch"))

    spec0 = load_review_spec(root / "spec.yaml")
    spec = spec0.model_copy(update={"extraction_models":
                                    spec0.extraction_models.model_copy(update={"elicitation": True})})
    arm = spec.extraction_models.arm
    db = ReviewDatabase(review_id)
    assert Path(db.db_path).resolve() == (root / "review.db").resolve()
    assert "surgical_autonomy" not in str(Path(db.db_path).resolve().relative_to(Path("data").resolve()))
    conn = db._conn
    cb = load_codebook_beside(db.db_path)
    git = rm.git_state()
    digests = {"deepseek-r1:32b": "edba8017331d15236e57480eb45406c0d721db77a4cdcf234df500fc2ad3960c",
               "gemma3:27b": "a418f5838eaf7fe2cfe0a3046c8384b68ba43a4435542c942f9db00a5f342203"}

    def selection():
        return select_for_extraction(conn, arm=arm)

    first = selection()
    chosen = tuple(sorted(first.to_extract, key=lambda p: (p[1].path.stat().st_size, p[0]))[:PAPERS])
    pids = [p for p, _ in chosen]
    texts = {p: {"path": ref.stored_path, "sha256": ref.sha256, "bytes": ref.path.stat().st_size,
                 "sha256_of_file_now": hashlib.sha256(ref.path.read_bytes()).hexdigest()}
             for p, ref in chosen}

    def one(label, pass1, pass2_fields):
        handle = rm.open_run(conn, spec, kind="extraction",
                             stages=[*E.extraction_stages(spec), "audit"], codebook=cb,
                             digest_fn=digests.__getitem__, git=git, host="12l-U06 real-state")
        client = FakeClient(cb, pass1, pass2_fields)
        oc._client = client
        oc.clear_ceiling_cache()
        sel = selection()
        bound = dataclasses.replace(sel, to_extract=tuple(p for p in sel.to_extract if p[0] in pids))
        out = {"run_id": handle.run_id, "selected": [p for p, _ in bound.to_extract]}
        try:
            with patch("engine.utils.ollama_preflight.require_preflight"):
                stats = E.run_extraction(db, spec, review_id, experiment_lock=False,
                                         restart_every=0, selection=bound, run_id=handle.run_id)
            out["ended"] = "completed"
            out["stats"] = {k: stats[k] for k in ("extracted", "failed", "stored_incomplete",
                                                  "incomplete_fields", "all_declined", "below_floor")}
            rm.close_run(conn, handle.run_id, "completed")
        except X.RunAborted as exc:
            out["ended"] = "aborted"
            out["abort"] = str(exc)
            rm.close_run(conn, handle.run_id, "aborted", reason=str(exc))
        out["calls"] = dict(client.calls)
        out["paper_events"] = [
            {"paper_id": r[0], "to_state": r[1], "reason_code": r[2],
             **{k: v for k, v in json.loads(r[3]).items()
                if k in ("held", "expected", "asserted", "declined", "contract_unmet",
                         "incomplete_fields")}}
            for r in conn.execute(
                "SELECT paper_id, to_state, reason_code, payload_json FROM paper_events "
                "WHERE run_id = ? AND event_type IN ('extracted', 'extraction_failed') "
                "ORDER BY event_id", (handle.run_id,))]
        out["claims_written"] = {r[0]: r[1] for r in conn.execute(
            "SELECT event_type, COUNT(*) FROM field_events WHERE run_id = ? GROUP BY 1",
            (handle.run_id,))}
        out["manifest_end"] = list(conn.execute(
            "SELECT end_status, end_reason FROM run_manifests WHERE run_id = ?",
            (handle.run_id,)).fetchone())
        again = [p for p, _ in selection().to_extract]
        out["selected_again_by_the_next_run"] = [p for p in pids if p in again]
        return out

    before = fingerprint(db.db_path)["overall_sha256"]
    results = {
        "A_pass1_empty_on_both_attempts": one("A", "", 0),
        "B_exhausted_record_under_the_floor": one("B", good_pass1(cb), 5),
        "C_exhausted_record_at_or_above_the_floor": one("C", good_pass1(cb), 14),
    }
    flow = generate_prisma_flow(db)
    check = validate_prisma_counts(db, flow)
    out = {
        "review_id": review_id, "database": db.db_path, "run_utc": now(),
        "git": {"commit": git.commit, "dirty": git.dirty},
        "spec": "data/%s/spec.yaml with extraction_models.elicitation set True in memory" % review_id,
        "arm": arm, "consecutive_failure_abort": E.CONSECUTIVE_FAILURE_ABORT,
        "papers": pids, "parsed_texts": texts,
        "arm_row": dict(zip(("configuration_marker", "pinned_run_id"), conn.execute(
            "SELECT configuration_marker, pinned_run_id FROM arms WHERE arm_name = ?",
            (arm,)).fetchone())),
        "cases": results,
        "prisma": {"n_eligible": flow["n_eligible"], "extraction_failed": flow["extraction_failed"],
                   "failure_reasons_extraction_failed": flow["failure_reasons"]["extraction_failed"],
                   "extracted_incomplete": flow["extracted_incomplete"],
                   "incomplete_fields": flow["incomplete_fields"],
                   "validation": {k: check[k] for k in ("valid", "errors") if k in check}},
        "copy_overall_sha256_before": before,
        "copy_overall_sha256_after": fingerprint(db.db_path)["overall_sha256"],
    }
    db.close()
    return out


if __name__ == "__main__":
    mode, review_id = sys.argv[1:3]
    print(json.dumps({"copy": copy, "run": run}[mode](review_id), indent=1, sort_keys=True,
                     ensure_ascii=False, default=str))
