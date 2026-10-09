"""L03-C2/C3 reproducer: the collapse monitor (a) reads a field whose every value is an
absence sentinel as status OK, (b) reads one off-case spelling as a second level.
Synthetic scratch database through the tests' event-store fixture helper; read-only monitor."""
import logging, shutil, sys, tempfile
from pathlib import Path
logging.disable(logging.CRITICAL)
REPO = Path("/home/ankitsarin/projects/evidence-engine")
sys.path.insert(0, str(REPO / "tests"))
from _event_store_fixture import add_values
from engine.validators.distribution_monitor import check_distribution, run_post_extraction_check

def world(values):
    tmp = Path(tempfile.mkdtemp(dir=Path.cwd()))
    cb = tmp / "extraction_codebook.yaml"
    shutil.copy2(REPO / "data/surgical_autonomy/extraction_codebook.yaml", cb)
    db = tmp / "test.db"
    add_values(db, "local", "study_type", values)
    return tmp, db, cb

def show(label, values):
    tmp, db, cb = world(values)
    r = [x for x in check_distribution(db, "t", "local", cb) if x["field_name"] == "study_type"][0]
    s = run_post_extraction_check(db, "t", "local", cb, extracted_count=len(values))
    print(f"{label:<46} n={r['total_non_null']:>3} distinct={r['distinct_count']} status={r['status']:<12}"
          f" gate: skipped={s['skipped']} collapsed={s['collapsed']} (did not raise)")
    shutil.rmtree(tmp)

show("40 x 'Original Research' (control)", ["Original Research"] * 40) if False else None
try:
    show("40 x 'Original Research' (control)", ["Original Research"] * 40)
except Exception as e:
    print("40 x 'Original Research' (control)             -> raised", type(e).__name__, e)
show("40 x 'NR' (every paper absent)", ["NR"] * 40)
show("31 x 'NR' + 9 x 'Original Research'", ["NR"] * 31 + ["Original Research"] * 9)
show("39 x 'Original Research' + 1 x 'original research'", ["Original Research"] * 39 + ["original research"])
show("15 x 'Original Research' + 1 x 'Original Research.'", ["Original Research"] * 15 + ["Original Research."])
