"""W1-F4-C2: run_pipeline's `except Exception` close commits whatever transaction the
failing stage left open; the interrupt close rolls it back first (test T8 pins only that).

Synthetic databases under this directory. Real run_pipeline, real _stage_search, real
ReviewDatabase.add_papers, real rm.close_run. The manifest is a real open_run of kind
'import' with zero stages (no digest, no network); search clients are stubbed.
"""
import logging, re, shutil, sqlite3, sys, tempfile
from pathlib import Path
from types import SimpleNamespace

REPO = Path("/home/ankitsarin/projects/evidence-engine")
sys.path.insert(0, str(REPO))
logging.disable(logging.CRITICAL)

from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook_beside
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
import scripts.run_pipeline as rp

work = Path(tempfile.mkdtemp(prefix="c2_", dir=Path(__file__).parent))


def cit(n, authors):
    return SimpleNamespace(pmid=str(700000 + n), doi=None, title=f"t{n}", abstract="a",
                           authors=authors, journal="j", year=2020, source="pubmed")


def scenario(name, stage_stub=None, batch=None, expect=Exception):
    rid = f"w1f4_{name}"
    txt = re.sub(r"(?m)^review_id:.*$", f"review_id: {rid}",
                 (REPO / "review_specs/surgical_autonomy.yaml").read_text())
    (work / f"{rid}.yaml").write_text(txt)
    spec = load_review_spec(work / f"{rid}.yaml")
    db = ReviewDatabase(rid, data_root=work)
    cb = re.sub(r"(?m)^review:.*$", f'review: "{rid}"',
                (REPO / "data/surgical_autonomy/extraction_codebook.yaml").read_text())
    (Path(db.db_path).parent / "extraction_codebook.yaml").write_text(cb)
    run_id = rm.open_run(db._conn, spec, kind="import", stages=(),
                         codebook=load_codebook_beside(db.db_path)).run_id
    rp.load_spec_for = lambda n, p=None: spec
    rp.ReviewDatabase = lambda n: db
    rp._open_run_manifest = lambda d, s, i, **k: run_id
    if batch is not None:                       # real _stage_search + real add_papers
        rp.search_pubmed = lambda s: batch
        rp.search_openalex = lambda s: []
        rp.deduplicate = lambda a, b: SimpleNamespace(
            unique_citations=a, stats={"duplicates_found": 0})
    else:
        rp._stage_search = stage_stub
    try:
        rp.run_pipeline(rid)
        raised = None
    except BaseException as exc:                # noqa: BLE001 - reproducer
        raised = type(exc).__name__
    conn = sqlite3.connect(f"file:{work / rid / 'review.db'}?mode=ro", uri=True)
    n = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    end = conn.execute("SELECT end_status, end_reason FROM run_manifests WHERE run_id = ?",
                       (run_id,)).fetchone()
    conn.close()
    print(f"{name:34s} raised={raised:18s} manifest end={end}  papers rows committed={n}")


def half(exc):
    def stub(d, *a, **k):
        d._conn.execute("INSERT INTO papers (title, source, status, created_at, updated_at) "
                        "VALUES ('t', 's', 'INGESTED', 'n', 'n')")
        raise exc
    return stub

# 1. the real writer: add_papers raises on the 3rd of 4 citations (json.dumps TypeError)
scenario("real_add_papers_midbatch_error",
         batch=[cit(1, ["A"]), cit(2, ["B"]), cit(3, {"not", "serialisable"}), cit(4, ["D"])])
# 2. T8's stub with an ordinary exception  3. T8 itself (the interrupt control)
scenario("stub_half_write_runtimeerror", stage_stub=half(RuntimeError("stage failed")))
scenario("stub_half_write_keyboardinterrupt", stage_stub=half(KeyboardInterrupt()))
shutil.rmtree(work)
