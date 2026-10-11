"""L09b reproducer: what the two adjudication pages carry per paper.
Synthetic database under this directory only; DATA_ROOT patched on the two modules."""
import re, sqlite3
from pathlib import Path
import engine.adjudication.abstract_adjudication_html as A
import engine.adjudication.ft_adjudication_html as F

root = Path.cwd() / "synth_root"
(root / "synth").mkdir(parents=True, exist_ok=True)
dbp = root / "synth" / "review.db"
if dbp.exists():
    dbp.unlink()
c = sqlite3.connect(dbp)
c.executescript("""
CREATE TABLE papers(id INTEGER PRIMARY KEY, title, authors, year, doi, abstract, status, ee_identifier);
CREATE TABLE abstract_screening_decisions(id INTEGER PRIMARY KEY, paper_id, pass_number, decision, rationale, model);
CREATE TABLE abstract_verification_decisions(id INTEGER PRIMARY KEY, paper_id, model, decision, rationale);
CREATE TABLE ft_screening_decisions(id INTEGER PRIMARY KEY, paper_id, model, decision, reason_code, rationale);
CREATE TABLE ft_verification_decisions(id INTEGER PRIMARY KEY, paper_id, model, decision, rationale);
""")
long_abs = ("A" * 399) + " TAIL_MARKER: the decisive sentence is here, after character 400."
# paper 7: flagged because pass 1 = include and pass 2 = exclude (run_screening's else-branch)
c.execute("INSERT INTO papers VALUES (7,'Paper seven','X; Y',2020,'10.1/x',?, 'ABSTRACT_SCREEN_FLAGGED','EE-900')", (long_abs,))
c.execute("INSERT INTO abstract_screening_decisions VALUES (1,7,1,'include','P1_RATIONALE_INCLUDE','qwen3:8b')")
c.execute("INSERT INTO abstract_screening_decisions VALUES (2,7,2,'exclude','P2_RATIONALE_EXCLUDE','qwen3:8b')")
# paper 8: FT_FLAGGED with NO decision row (the 'no parsed text' route in run_ft_screening)
c.execute("INSERT INTO papers VALUES (8,'Paper eight','X',2021,NULL,?, 'FT_FLAGGED','EE-901')", (long_abs,))
c.commit(); c.close()

A.DATA_ROOT = root; F.DATA_ROOT = root
a_out, _ = A.generate_abstract_adjudication_html("synth", output_path=str(root / "a.html"))
f_out, _ = F.generate_ft_adjudication_html("synth", output_path=str(root / "f.html"))
a = a_out.read_text(); f = f_out.read_text()
card = lambda h, pid: h[h.index(f'id="card-{pid}"'):h.index('<div class="decision-row">', h.index(f'id="card-{pid}"'))]
ca, cf = card(a, 7), card(f, 8)
print("ABSTRACT page, paper 7 (pass1 include / pass2 exclude):")
print("  pass-1 rationale on card :", "P1_RATIONALE_INCLUDE" in ca)
print("  pass-2 rationale on card :", "P2_RATIONALE_EXCLUDE" in ca)
print("  word 'exclude' on card   :", "exclude" in ca.lower().replace("btn-exclude", ""))
print("  decisive tail of abstract:", "TAIL_MARKER" in ca, "| anywhere in file:", "TAIL_MARKER" in a)
print("  ee_identifier EE-900     :", "EE-900" in a, "| id shown:", re.search(r'class="paper-id">([^<]*)<', ca).group(1))
print("FT page, paper 8 (FT_FLAGGED, no decision row):")
print("  panel text               :", re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", cf[cf.index('class="panels"'):]))[:120])
print("  decisive tail of abstract:", "TAIL_MARKER" in f)
print("  any path/link to PDF or parsed text:", bool(re.search(r"\.pdf|parsed_text|\.md", cf)))
print("exported record shape (abstract):", re.search(r"output\.push\(\{(.*?)\}\);", a, re.S).group(1).split())
print("exported record shape (ft)      :", re.search(r"output\.push\(\{(.*?)\}\);", f, re.S).group(1).split())
print("FT papers_json embedded in page :", '"primary_reason_code"' in f)
