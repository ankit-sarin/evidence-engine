"""12ai-U30B I3 probe (FAKE; on the tree as it stands): the cloud connection's in_transaction at Q2's rows 4, 7, 8, 9 and 12, and what is
committed after run_arm, with the acceptance file's own seams (tests/test_u30_cloud_interrupt.py: its fixtures' scratch review, fake SDK and
placeholder key), run in-process by pytest so the conftest fence is on. Row 4: run_calls' INSERT executed, its commit not reached; row 8:
the second span INSERT; rows 7, 9, 12 as cases 1, 2 and 6. No signal is sent.
usage, from the repo root: .venv/bin/python -I -B i3_probe.py <out file>"""
import sys, textwrap, pathlib, subprocess
out = pathlib.Path(sys.argv[1])
probe = pathlib.Path("tests") / "_tmp_i3_probe_u30b.py"
probe.write_text(textwrap.dedent('''
    import sqlite3, test_u30_cloud_interrupt as T
    class Nth(T.SeamConn):
        def __init__(self, real, trip, exc, after, seam, n):
            super().__init__(real, trip, exc, after, seam); self.n = n
        def execute(self, sql, *a, **k):
            if self.trip and not self.tripped and sql.lstrip().upper().startswith(self.trip):
                self.n -= 1
                if self.n > 0:
                    return self.real.execute(sql, *a, **k)
            return super().execute(sql, *a, **k)
    def _row(arm_case, monkeypatch, trip, after, seam, n=1, run=None):
        orig = T.SeamConn
        monkeypatch.setattr(T, "SeamConn", lambda real, trip_, exc, after_, seam_: Nth(real, trip_, exc, after_, seam_, n))
        left, db_path, arm, pids, spec_path = arm_case(trip=trip, exc=KeyboardInterrupt(), after=after, seam=seam, run=run)
        print(f"ROW {seam}: in_transaction at the seam = {T.SEAMS.get(seam)}; after run_arm: paper {T._paper(db_path, arm, pids[0])}, "
              f"run_calls {T._run_calls(db_path, pids[0])}, close {T._cloud_end(db_path)}")
    def test_row4(arm_case, monkeypatch): _row(arm_case, monkeypatch, "INSERT INTO RUN_CALLS", True, "row4_run_calls")
    def test_row7(arm_case, monkeypatch): _row(arm_case, monkeypatch, "INSERT INTO CLOUD_EVIDENCE_SPANS", False, "row7_span_insert")
    def test_row8(arm_case, monkeypatch): _row(arm_case, monkeypatch, "INSERT INTO CLOUD_EVIDENCE_SPANS", False, "row8_second_span", n=2)
    def test_row9(arm_case, monkeypatch): _row(arm_case, monkeypatch, "INSERT INTO FIELD_EVENTS", False, "row9_field_events")
    def test_row12(arm_case, monkeypatch): _row(arm_case, monkeypatch, "UPDATE RUN_MANIFESTS", True, "row12_close_update", run=lambda ex: None)
    from test_u30_cloud_interrupt import arm_case, no_network  # noqa: F401 — the fixtures
'''))
try:
    r = subprocess.run([".venv/bin/python", "-m", "pytest", str(probe), "-p", "no:cacheprovider", "-q", "-s"], capture_output=True, text=True)
finally:
    probe.unlink()
out.write_text("\n".join(l for l in r.stdout.splitlines() if "ROW " in l or "passed" in l or "failed" in l) + "\n")
print(out.read_text())
