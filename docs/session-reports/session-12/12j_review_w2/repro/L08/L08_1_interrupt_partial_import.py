"""L08-C1/C2: an interrupt (BaseException) mid-import is not handled by the importers'
`except Exception`; in the documented in-process invocation the half-written transaction
stays open on the connection and the next workflow READ commits it (C37's executescript).
Synthetic temp review only; no model, no network; git state and digest are stubbed."""
import json, shutil, sys, tempfile
from pathlib import Path
from unittest.mock import patch

REPO = Path("/home/ankitsarin/projects/evidence-engine")
HERE = Path(__file__).resolve().parent

from engine.adjudication import import_extraction_entry as ee
from engine.adjudication import import_screening_entry as se
from engine.adjudication.workflow import get_workflow_status
from engine.core import run_manifest as rm
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec

spec = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")
CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)


def fresh(tmp, name):
    d = ReviewDatabase(name, data_root=tmp / "data")
    shutil.copy2(HERE / "codebook_copy.yaml", Path(d.db_path).parent / "extraction_codebook.yaml")
    return d


def state(d, label):
    c = d._conn
    n = lambda t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    ptd = Path(d.db_path).parent / "parsed_text"
    man = [tuple(r) for r in c.execute("SELECT run_id, run_kind, end_status FROM run_manifests")]
    done = c.execute("SELECT COUNT(*) FROM workflow_state WHERE status='complete'").fetchone()[0]
    print(f"  [{label}] in_transaction={c.in_transaction} papers={n('papers')} "
          f"paper_events={n('paper_events')} refs={n('parsed_text_refs')} "
          f"files={sorted(p.name for p in ptd.iterdir())} manifests={man} stages_complete={done}")


def second_conn_papers(d):
    import sqlite3
    c = sqlite3.connect(f"file:{d.db_path}?mode=ro", uri=True)
    try:
        return [tuple(r) for r in c.execute("SELECT id, status FROM papers")]
    finally:
        c.close()


with tempfile.TemporaryDirectory(dir=HERE) as t:
    tmp = Path(t)
    # ---------- extraction entry ----------
    src = tmp / "in"; (src / "texts").mkdir(parents=True)
    papers = []
    for i in (1, 2, 3):
        (src / "texts" / f"t{i}.md").write_bytes(f"# Paper {i}\n\nBody text.\n".encode())
        papers.append({"title": f"Paper {i}", "pmid": f"P{i}", "doi": None,
                       "text_path": f"texts/t{i}.md"})
    (src / "entry.json").write_text(json.dumps({"source": "elsewhere", "papers": papers}))

    d = fresh(tmp, "ee_interrupt")
    real = ee.write_paper_event
    calls = {"n": 0}

    def interrupt(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 2:
            raise KeyboardInterrupt()      # Ctrl-C while entry 2 is being written
        return real(*a, **kw)

    print("EXTRACTION-ENTRY import, KeyboardInterrupt at entry 2 of 3")
    try:
        with patch.object(ee, "write_paper_event", interrupt):
            ee.import_extraction_entry(d, src / "entry.json", spec=spec, git=CLEAN,
                                       digest_fn=lambda m: "")
    except KeyboardInterrupt:
        print("  KeyboardInterrupt propagated to the caller (the REPL prompt returns)")
    state(d, "after interrupt, same connection")
    print("  committed papers seen by a second read-only connection:", second_conn_papers(d))
    print("  -> operator runs a workflow READ on the same ReviewDatabase: get_workflow_status(conn)")
    get_workflow_status(d._conn)
    state(d, "after the read")
    print("  committed papers seen by a second read-only connection:", second_conn_papers(d))
    from engine.core.effective import eligible_paper_ids
    print("  eligible_paper_ids (what extraction selects from):", sorted(eligible_paper_ids(d._conn)))
    print("  retry of the same import:")
    try:
        ee.import_extraction_entry(d, src / "entry.json", spec=spec, git=CLEAN,
                                   digest_fn=lambda m: "")
    except Exception as exc:
        print("   ", type(exc).__name__, "::", str(exc).replace(str(tmp), "<tmp>")[:420])
    d.close()

    # ---------- same interrupt, but the process dies (connection closed) ----------
    d = fresh(tmp, "ee_interrupt_exit")
    calls["n"] = 0
    print("\nEXTRACTION-ENTRY import, KeyboardInterrupt at entry 2, then the process exits")
    try:
        with patch.object(ee, "write_paper_event", interrupt):
            ee.import_extraction_entry(d, src / "entry.json", spec=spec, git=CLEAN,
                                       digest_fn=lambda m: "")
    except KeyboardInterrupt:
        pass
    d.close()                                  # process exit: the open transaction is rolled back
    d = ReviewDatabase("ee_interrupt_exit", data_root=tmp / "data")
    state(d, "reopened")
    try:
        ee.import_extraction_entry(d, src / "entry.json", spec=spec, git=CLEAN,
                                   digest_fn=lambda m: "")
    except Exception as exc:
        print("  retry:", type(exc).__name__, "::", str(exc).replace(str(tmp), "<tmp>")[:300])
    d.close()

    # ---------- screening entry ----------
    (src / "screen.json").write_text(json.dumps({"source": "elsewhere", "papers": [
        {"title": f"Citation {i}", "pmid": f"S{i}"} for i in (1, 2, 3)]}))
    d = fresh(tmp, "se_interrupt")
    real_ins = se.insert_paper_at_status
    calls["n"] = 0

    def interrupt_ins(conn, **kw):
        calls["n"] += 1
        if calls["n"] == 3:
            raise KeyboardInterrupt()
        return real_ins(conn, **kw)

    print("\nSCREENING-ENTRY import, KeyboardInterrupt at entry 3 of 3")
    try:
        with patch.object(se, "insert_paper_at_status", interrupt_ins):
            se.import_screening_entry(d, src / "screen.json", spec=spec, git=CLEAN,
                                      digest_fn=lambda m: "")
    except KeyboardInterrupt:
        pass
    state(d, "after interrupt, same connection")
    get_workflow_status(d._conn)
    state(d, "after get_workflow_status")
    print("  committed papers seen by a second read-only connection:", second_conn_papers(d))
    d.close()
