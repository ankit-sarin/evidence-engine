"""12u-U16A — R624(b)'s changed-text cases and U16's row (INT-i12-73) at HEAD, on one disposable
review under a scratch directory. Evidence, not a test.
usage, from the repository root: .venv/bin/python -I repro_u16a.py <scratch dir>   (stdout: JSON)

No model, no network, no service: socket.connect is replaced before any engine import, a
subprocess call naming a service manager or privilege escalation raises, both Ollama restart paths
are disarmed (EVIDENCE_ENGINE_NO_OLLAMA_RESTART), and the experiment lock is a file under <scratch
dir> (OLLAMA_EXPERIMENT_LOCK). The model passes are stubbed exactly as
tests/test_text_refusal_at_selection.py stubs them (extract_pass1_reasoning, extract_pass2_structured,
_validate_and_retry_snippets; require_preflight patched); semantic_verify is patched as
tests/test_audit_events.py patches it. Claims, events and runs are written by the engine's own
writers and the suite's fixture helpers (imported, not run as tests). Nothing under data/ is opened
except the live codebook and spec, read as files.

Part 1 — the R624(b) cases, one review, driven through run_pipeline's own stage functions
(_stage_extract, _stage_audit, _advance_extraction_workflow). Setup run A extracts and audits
papers 11–16 on text v1. Then:
  11  case (i)    — the recorded v1 file's bytes altered in place; the ref row unchanged.
  12  case (ii)   — a re-parse's shape: v2 recorded with new bytes (write_parsed, as the suite does).
  13  case (iii)  — the recorded v1 file removed.
  14  control     — untouched.
  15  case (ii-b) — v2 recorded with new bytes, and v2's file altered after run B's selection and
                    before the loop's re-read (U11 F2's window), by wrapping select_for_extraction.
  16  case (ii-s) — v2 recorded with byte-identical text (a new uid, the same sha256).
  17, 18          — added after run A and extracted by run B; before run B's audit, 17's file is
                    altered and 18's removed (an extracted paper whose text changes before audit).
Runs B and C each run the extract stage, the audit stage and the workflow advance.

Part 2 — INT-i12-73 on fresh reviews:
  u16_states   — the P1A acceptance test's world: three eligible papers, audited_ai / extracted /
                 extraction_failed (seeded by the fixture writer), then _advance_extraction_workflow.
  u16_declined — paper 21 extracted by _stage_extract; paper 22 added after it, its 20 claims all
                 `declined` and an `extracted` event, written by the engine's writers (the live spec
                 is not elicited, so no stub of its path produces `declined`); _stage_audit, then
                 _advance_extraction_workflow.
  u16_second_arm — the spec arm's claims on paper 31 plus a second pinned model arm's claims on
                 the same paper; _stage_audit (spec arm only), then the reader and the advance."""
import json, logging, os, shutil, socket, subprocess, sys
from pathlib import Path

ROOT = Path(sys.argv[1]).resolve(); ROOT.mkdir(parents=True, exist_ok=True)
os.environ["EVIDENCE_ENGINE_NO_OLLAMA_RESTART"] = "1"
os.environ["OLLAMA_EXPERIMENT_LOCK"] = str(ROOT / "experiment.lock")


def _no_network(*a, **k): raise RuntimeError("network attempt in a reproducer")
socket.socket.connect = _no_network
_BLOCK = {"systemctl", "sudo", "service", "su", "doas", "pkexec"}
for _name in ("run", "Popen", "call", "check_call", "check_output"):
    _orig = getattr(subprocess, _name)
    def _guard(*a, _o=_orig, **k):
        argv = a[0] if a else k.get("args")
        first = (argv.split()[0] if isinstance(argv, str) else str(list(argv)[0])) if argv else ""
        if os.path.basename(first) in _BLOCK:
            raise RuntimeError(f"service call in a reproducer: {argv!r}")
        return _o(*a, **k)
    setattr(subprocess, _name, _guard)

REPO = Path.cwd(); sys.path.insert(0, str(REPO)); sys.path.insert(0, str(REPO / "tests"))
from dataclasses import asdict                                      # noqa: E402
from unittest.mock import patch                                     # noqa: E402

import scripts.run_pipeline as rp                                   # noqa: E402
from engine.adjudication.workflow import ensure_workflow_table, is_stage_done  # noqa: E402
from engine.agents import audit_events as AE                        # noqa: E402
from engine.agents import extractor as E                            # noqa: E402
from engine.agents.auditor import AuditVerdict                      # noqa: E402
from engine.agents.models import EvidenceSpan, ExtractionResult     # noqa: E402
from engine.core import events                                      # noqa: E402
from engine.core.codebook import load_codebook                      # noqa: E402
from engine.core.database import ReviewDatabase                     # noqa: E402
from engine.core.effective import effective_state, effective_value, live_claim_events  # noqa: E402
from engine.core.parsed_text import ParsedTextError, read_parsed_bytes, resolve_parsed_text  # noqa: E402
from engine.core.review_spec import load_review_spec                # noqa: E402
from engine.core.reuse_key import reuse_key                         # noqa: E402
from engine.core.run_telemetry import read_run_events               # noqa: E402
from engine.exporters.prisma import generate_prisma_flow            # noqa: E402
from _event_store_fixture import (FIXTURE_CONTEXT_SHA, claim_identity, fixture_run,  # noqa: E402
                                  open_extraction_run, seed_eligibility, seed_processing)
from _parsed_text_fixture import write_parsed                       # noqa: E402

LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
SPEC = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")
ARM = SPEC.extraction_models.arm
NAMES = tuple(f["name"] for f in load_codebook(LIVE_CODEBOOK).fields)
TEXT = "The paper reports a trial. A clean sentence.\n"
TEXT2 = "The paper reports a trial, re-parsed. A clean sentence.\n"
SNIP = "A clean sentence."
HASH = "d" * 64
OUT = {"arm": ARM, "fields": len(NAMES)}

# ── capture: every WARNING+ line, and every pass-2 call by paper ──────────────
LOG: list[dict] = []
PHASE = {"name": "setup"}


class _H(logging.Handler):
    def emit(self, r):
        LOG.append({"phase": PHASE["name"], "level": r.levelname, "logger": r.name,
                    "msg": r.getMessage()[:400]})


logging.getLogger().addHandler(_H(level=logging.WARNING)); logging.getLogger().setLevel(logging.INFO)
CALLS: dict[str, list[int]] = {}


def pass2(prompt, trace, spec_, paper_id, **kw):
    CALLS.setdefault(PHASE["name"], []).append(paper_id)
    spans = [EvidenceSpan(field_name=n, value="RCT", source_snippet=SNIP, confidence=0.9, tier=1)
             for n in NAMES]
    return ExtractionResult(paper_id=paper_id, fields=spans, reasoning_trace="t",
                            model="deepseek-r1:32b", codebook_hash="h",
                            extracted_at="2026-10-22T00:00:00Z"), HASH


STUBS = [patch.object(E, "extract_pass1_reasoning", lambda *a, **k: ("trace", HASH)),
         patch.object(E, "extract_pass2_structured", pass2),
         patch.object(E, "_validate_and_retry_snippets", lambda fields, *a, **k: (fields, {}, ())),
         patch("engine.utils.ollama_preflight.require_preflight"),
         patch.object(AE, "semantic_verify", return_value=AuditVerdict(
             status="flagged", grep_found=False, reasoning="stub"))]
for s in STUBS:
    s.start()


def fresh(name):
    d = ROOT / name
    if d.exists():
        shutil.rmtree(d)
    db = ReviewDatabase("u16a", data_root=d)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    ensure_workflow_table(db._conn)
    return db


def add(db, pid, text=TEXT):
    db._conn.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
                     "VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
    seed_eligibility(db._conn, pid)
    p = write_parsed(db, pid, text)
    db._conn.commit()
    return p


def file_state(ref):
    try:
        read_parsed_bytes(ref); return "ok"
    except ParsedTextError as exc:
        return exc.reason_code


def snap(db, pid):
    c = db._conn
    s = effective_state(c, pid)
    ref = resolve_parsed_text(c, pid)
    live = live_claim_events(c, pid, ARM)
    keys = {r[0] for r in c.execute(
        "SELECT DISTINCT ci.reuse_key FROM field_events fe JOIN claim_inputs ci "
        "ON fe.extraction_uid = ci.extraction_uid WHERE fe.paper_id = ? AND fe.arm = ? "
        "AND fe.claim_id IN (%s)" % ",".join("?" * len(live)),
        (pid, ARM, *[e.claim_id for e in live])).fetchall()} if live else set()
    cur_key = reuse_key(ARM, pid, ref.sha256)
    ev = [list(r) for r in c.execute(
        "SELECT run_id, event_type, to_state, reason_code FROM paper_events WHERE paper_id = ? "
        "AND event_type <> 'adjudicated' ORDER BY event_id", (pid,))]
    v1 = c.execute("SELECT parsed_text_version, parsed_text_sha256 FROM parsed_text_refs "
                   "WHERE paper_id = ? ORDER BY parsed_text_version", (pid,)).fetchall()
    return {"processing": s.processing, "reason": s.processing_reason,
            "analysis_ready": s.analysis_ready, "in_corpus": s.in_corpus,
            "current_text": {"version": ref.version, "file": file_state(ref),
                             "versions_recorded": [[r[0], r[1][:12]] for r in v1]},
            "live_claims": len(live),
            "live_claims_by_kind": sorted({e.event_type for e in live}),
            "live_claim_keys_equal_current_key": (keys == {cur_key}) if keys else None,
            "superseded_events": c.execute(
                "SELECT COUNT(*) FROM field_events WHERE paper_id = ? AND event_type = 'superseded'",
                (pid,)).fetchone()[0],
            "located_events": c.execute(
                "SELECT COUNT(*) FROM field_events WHERE paper_id = ? AND event_type = "
                "'citation_located'", (pid,)).fetchone()[0],
            "study_type_effective": effective_value(c, pid, "study_type", ARM,
                                                    sentinels=frozenset()).value
                                    if "study_type" in NAMES else None,
            "paper_events": ev}


PRISMA_KEYS = None


def prisma(db):
    f = generate_prisma_flow(db)
    return {k: v for k, v in f.items() if not isinstance(v, (list, dict)) or len(v) < 12}


def stage_run(db, label, *, hook=None, between=None):
    """One run through run_pipeline's stage functions: extract, audit, workflow advance."""
    PHASE["name"] = label
    run_id = open_extraction_run(db, SPEC)
    real = rp.select_for_extraction
    if hook:
        def wrapped(conn, *, arm):
            sel = real(conn, arm=arm); hook(); return sel
        rp.select_for_extraction = wrapped
    try:
        ext = rp._stage_extract(db, SPEC, "u16a", run_id=run_id)
    finally:
        rp.select_for_extraction = real
    if between:
        between()
    aud = rp._stage_audit(db, "u16a", SPEC, run_id=run_id)
    adv = rp._advance_extraction_workflow(db._conn)
    tele = [e for e in read_run_events(Path(db.db_path).parent)
            if e["run_id"] == run_id and e["kind"] == "claimed_papers_text_unreadable"]
    keep = ("extracted", "failed", "skipped_asserted", "skipped_refused",
            "claimed_text_unreadable", "stored_incomplete", "below_floor", "all_declined")
    return {"run_id": run_id,
            "extract_stats": {k: ext.get(k) for k in keep},
            "pass2_calls_for": CALLS.get(label, []),
            "audit_report": {k: v for k, v in aud.items() if k != "elapsed"},
            "workflow_advance": adv,
            "ai_audit_stage_done": is_stage_done(db._conn, "AI_AUDIT_COMPLETE_STAGE"),
            "telemetry_claimed_text_unreadable": [e["payload"] for e in tele],
            "warnings": [l for l in LOG if l["phase"] == label]}


# ═════════ Part 1 — R624(b) ═════════
db = fresh("r624b")
P = {pid: add(db, pid) for pid in (11, 12, 13, 14, 15, 16)}
OUT["run_A"] = stage_run(db, "run_A")
PIDS = (11, 12, 13, 14, 15, 16, 17, 18)
OUT["after_A"] = {pid: snap(db, pid) for pid in (11, 12, 13, 14, 15, 16)}
OUT["prisma_after_A"] = prisma(db)

# the perturbations
P[11].write_text(TEXT + "An added line.\n")                 # (i)
write_parsed(db, 12, TEXT2); db._conn.commit()              # (ii)
P[13].unlink()                                              # (iii)
p15v2 = write_parsed(db, 15, TEXT2); db._conn.commit()      # (ii-b)
write_parsed(db, 16, TEXT); db._conn.commit()               # (ii-s)
P[17] = add(db, 17); P[18] = add(db, 18)
OUT["perturbed"] = {pid: snap(db, pid) for pid in PIDS}


def alter_15():
    p15v2.write_text(TEXT2 + "Changed after selection.\n")


def before_audit_B():
    P[17].write_text(TEXT + "Changed before audit.\n")
    P[18].unlink()


OUT["run_B"] = stage_run(db, "run_B", hook=alter_15, between=before_audit_B)
OUT["after_B"] = {pid: snap(db, pid) for pid in PIDS}
OUT["prisma_after_B"] = prisma(db)
OUT["run_C"] = stage_run(db, "run_C")
OUT["after_C"] = {pid: snap(db, pid) for pid in PIDS}
OUT["prisma_after_C"] = prisma(db)
OUT["r624b_db"] = str(db.db_path)
db.close()

# ═════════ Part 2 — INT-i12-73 ═════════
PHASE["name"] = "u16_states"
db = fresh("u16_states")
for pid in (1, 2, 3):
    add(db, pid)
run_id = open_extraction_run(db, SPEC)
seed_processing(db._conn, 1, "extracted", run_id=run_id)
seed_processing(db._conn, 1, "audited_ai", run_id=run_id)
seed_processing(db._conn, 2, "extracted", run_id=run_id)
seed_processing(db._conn, 3, "extraction_failed", reason_code="model_call_failed", run_id=run_id)
db._conn.commit()
OUT["u16_states"] = {"states": {p: effective_state(db._conn, p).processing for p in (1, 2, 3)},
                     "workflow_advance": rp._advance_extraction_workflow(db._conn),
                     "ai_audit_stage_done": is_stage_done(db._conn, "AI_AUDIT_COMPLETE_STAGE"),
                     "metadata": db._conn.execute(
                         "SELECT metadata FROM workflow_state WHERE stage_name = "
                         "'AI_AUDIT_COMPLETE_STAGE'").fetchone()[0]}
db.close()

# u16_declined: paper 22's only live claims are `declined`
db = fresh("u16_declined")
add(db, 21)
PHASE["name"] = "u16_declined"
run_id = open_extraction_run(db, SPEC)
ext = rp._stage_extract(db, SPEC, "u16a", run_id=run_id)      # paper 21 only
add(db, 22)                                                    # then 22, its claims all declined
ref = resolve_parsed_text(db._conn, 22)
for n in NAMES:
    events.write_field_event(
        db._conn, run_id=run_id, event_type="declined", paper_id=22, field_name=n, arm=ARM,
        extraction_uid=events.mint_extraction_uid(), actor_kind="model", actor_role="extractor",
        actor_name="deepseek-r1:32b",
        payload=claim_identity(ARM, 22, sha=ref.sha256, uid=ref.parsed_text_uid),
        presented_context_sha256=FIXTURE_CONTEXT_SHA, commit=False)
seed_processing(db._conn, 22, "extracted", run_id=run_id)
db._conn.commit()
before = {p: effective_state(db._conn, p).processing for p in (21, 22)}
aud = rp._stage_audit(db, "u16a", SPEC, run_id=run_id)
OUT["u16_declined"] = {"extract_stats": {k: ext.get(k) for k in ("extracted", "failed")},
                       "states_before_audit": before,
                       "paper_22_live_claim_kinds": sorted({e.event_type for e in
                                                            live_claim_events(db._conn, 22, ARM)}),
                       "audit_report": {k: v for k, v in aud.items() if k != "elapsed"},
                       "states_after_audit": {p: effective_state(db._conn, p).processing
                                              for p in (21, 22)},
                       "workflow_advance": rp._advance_extraction_workflow(db._conn),
                       "ai_audit_stage_done": is_stage_done(db._conn, "AI_AUDIT_COMPLETE_STAGE"),
                       "warnings": [l for l in LOG if l["phase"] == "u16_declined"]}
db.close()

# u16_second_arm: a second pinned model arm holds asserted claims on the same paper
PHASE["name"] = "u16_second_arm"
db = fresh("u16_second_arm")
add(db, 31)
run_id = open_extraction_run(db, SPEC)
ext = rp._stage_extract(db, SPEC, "u16a", run_id=run_id)
OTHER = "second_model_arm_fixture"
frun = fixture_run(db._conn, OTHER)
ref = resolve_parsed_text(db._conn, 31)
for n in NAMES[:3]:
    events.write_field_event(
        db._conn, run_id=frun, event_type="asserted", paper_id=31, field_name=n, arm=OTHER,
        value="Other", source_snippet="Not in the text at all.",
        extraction_uid=events.mint_extraction_uid(), actor_kind="model", actor_role="extractor",
        actor_name="fixture", payload=claim_identity(OTHER, 31, sha=ref.sha256,
                                                     uid=ref.parsed_text_uid),
        presented_context_sha256=FIXTURE_CONTEXT_SHA, commit=False)
db._conn.commit()
aud = rp._stage_audit(db, "u16a", SPEC, run_id=run_id)


def located(arm):
    return db._conn.execute("SELECT COUNT(*) FROM field_events WHERE paper_id = 31 AND arm = ? "
                            "AND event_type = 'citation_located'", (arm,)).fetchone()[0]


OUT["u16_second_arm"] = {"audit_report": {k: v for k, v in aud.items() if k != "elapsed"},
                         "processing": effective_state(db._conn, 31).processing,
                         "located_events": {ARM: located(ARM), OTHER: located(OTHER)},
                         "other_arm_live_asserted": len(live_claim_events(
                             db._conn, 31, OTHER, event_types=("asserted",))),
                         "workflow_advance": rp._advance_extraction_workflow(db._conn),
                         "ai_audit_stage_done": is_stage_done(db._conn,
                                                              "AI_AUDIT_COMPLETE_STAGE")}
db.close()

for s in STUBS:
    s.stop()
print(json.dumps(OUT, indent=1, default=str))
