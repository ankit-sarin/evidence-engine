"""L01-3: the writer accepts a reviewer decision that names competing DECISIONS
but no claim (R20's check is `not against_claims and not against_decisions`);
the reader then lets it clear the row-2 conflict and falls through to the
machine value - the correction's own value is not applied and no unresolved
state is reported. Synthetic database under a temp dir; no model, no network."""
import tempfile, shutil
from pathlib import Path
from engine.core.database import ReviewDatabase
from engine.core import events
from engine.core.effective import effective_value
from engine.core.reuse_key import reuse_key

tmp = Path(tempfile.mkdtemp(prefix="l01_3_"))
try:
    db = ReviewDatabase("synthetic_l01", data_root=tmp)
    conn = db._conn
    cols = [r[1] for r in conn.execute("PRAGMA table_info(papers)")]
    conn.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
                 "VALUES (1, 't', 'fixture', 'FT_ELIGIBLE', 'x', 'x')")
    run_id = conn.execute(
        "INSERT INTO run_manifests (run_uid, review_id, run_kind, git_commit, git_dirty, "
        "spec_hash, codebook_hash, codebook_sha256, library_versions_json, host, started_at, "
        "manifest_json, manifest_sha256) VALUES ('r', 'fixture', 'extraction', ?, 0, 'f', 'f', "
        "'f', '{}', 'h', '2026-01-01T00:00:00+00:00', '{}', 'f')", ("0" * 40,)).lastrowid
    events.register_arm(conn, "local", "model")
    conn.execute(
        "INSERT INTO run_stage_configs (run_id, stage, stage_kind, arm_name, provider, "
        "model_name, model_digest, options_json, options_hash, sent_keys_json, sources_json, "
        "keep_alive, format_schema_hash, prompt_hash) VALUES (?, 'fixture:local', "
        "'extract_pass2', 'local', 'ollama', 'm', ?, '{}', 'f', '[]', '{}', '-1', 'none', 'f')",
        (run_id, "0" * 64))
    conn.commit()
    F, S = "primary_outcome_value", frozenset({"NR"})
    sha = "a" * 64
    uid = events.mint_extraction_uid()
    cid = events.make_claim_id("local", uid, F)
    events.write_field_event(
        conn, event_type="asserted", paper_id=1, field_name=F, arm="local", claim_id=cid,
        extraction_uid=uid, value="5", source_snippet="q", actor_kind="model",
        actor_role="extractor", actor_name="m", run_id=run_id,
        presented_context_sha256="ctx",
        payload={"reuse_key": reuse_key("local", 1, sha), "parsed_text_sha256": sha,
                 "parsed_text_uid": "ptu", "context_chain": ["ctx"]})

    def review(etype, *, against=(), decisions=(), value=None, who="PI"):
        return events.write_field_event(
            conn, event_type=etype, paper_id=1, field_name=F, arm="local", claim_id=cid,
            value=value, actor_kind="human", actor_role="reviewer", actor_name=who,
            against_claims=against, against_decisions=decisions, run_id=run_id)

    def show(label):
        r = effective_value(conn, 1, F, "local", sentinels=S)
        print(f"{label:<58} value={r.value!r} state={r.state} row={r.rule_row}")

    show("machine claim '5'")
    a = review("human_corrected", against={cid}, value="6", who="PI-a")
    b = review("human_withdrew", against={cid}, who="PI-b")
    show("two conflicting reviewer decisions (row 2)")
    review("human_corrected", decisions={a, b}, value="7", who="PI-c")   # ACCEPTED by the writer
    show("resolving CORRECT to '7' naming both decisions, no claim")
finally:
    shutil.rmtree(tmp, ignore_errors=True)
