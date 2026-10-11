"""L14 reproducer — pure functions only; no database, no model, no network.
The fallback codebook (normalize._default_codebook) is replaced by a synthetic
stub so nothing under the repo's data/ is read."""
from types import SimpleNamespace
import engine.analysis.normalize as N

TYPES = {"sample_size": "numeric", "study_type": "categorical",
         "validation_setting": "categorical", "primary_outcome_value": "free_text",
         "key_limitation": "free_text"}
ENUMS = {"study_type": ["RCT", "Case series"],
         "validation_setting": ["Phantom", "In vivo (animal)", "Clinical"]}

class StubCodebook:
    absence_sentinels = ("NR", "N/A", "NA", "NOT_FOUND", "NOT FOUND", "NOT REPORTED")
    def view(self, name):
        return SimpleNamespace(name=name, type=TYPES[name], tier=1,
                               enum_values=ENUMS.get(name))
N._default_codebook = lambda: StubCodebook()
N._absence_sentinels.cache_clear()

from engine.analysis.concordance import align_arms
from engine.analysis.scoring import score_pair
from engine.analysis.metrics import cohens_kappa, field_summary

print("== C1: a cell with NO VALUE (load_arm leaves the key absent: missing / declined /")
print("   contract unmet / unresolved / withdrawn / out of scope) scored as an ABSENCE ANSWER")
# arm A: real answers. arm B: paper 1 study_type absent key (e.g. contract_unmet),
# paper 2 has no value in any field (e.g. extraction_failed), paper 3 fine.
arm_a = {1: {"study_type": "NR", "sample_size": "40"},
         2: {"study_type": "RCT", "sample_size": "12"},
         3: {"study_type": "RCT", "sample_size": "7"}}
arm_b = {1: {"sample_size": "40"},          # study_type: no value at all
         3: {"study_type": "RCT"}}          # sample_size: no value at all
aligned, a_only, b_only = align_arms(arm_a, arm_b)
for pid, f, va, vb in aligned:
    s = score_pair(f, va, vb)
    print(f"  paper {pid} {f:<12} A={va!r:<6} B={vb!r:<6} -> {s.result}: {s.detail}")
print(f"  papers only in A (never scored, no cell): {sorted(a_only)}")

print("\n== C2: decimal point is a token boundary, so a number 'is contained' in a different number")
for a, b in [("5", "0.5 mm mean error"), ("12", "mean 3.12 s per stitch"),
             ("5", "-5 degrees of deviation"), ("2", "level 2 of 5"), ("5", "level 2 of 5")]:
    s = score_pair("key_limitation", a, b)
    print(f"  {a!r} vs {b!r} -> {s.result}: {s.detail}")

print("\n== C3: the unit word is discarded on every field, not only on counts")
for f, a, b in [("primary_outcome_value", "5 mm", "5 cm"),
                ("primary_outcome_value", "1.2 mm", "1.2 N"),
                ("primary_outcome_value", "30 s", "30 min"),
                ("key_limitation", "3D", "3"),
                ("sample_size", "5k", "5")]:
    s = score_pair(f, a, b)
    print(f"  [{f}] {a!r} vs {b!r} -> {s.result}: {s.detail}")

print("\n== C4: the kappa CI")
for la, lb, why in [(["a", "b"], ["a", "b"], "n=2, perfect agreement"),
                    (["a"]*3 + ["b"], ["a"]*3 + ["b"], "n=4, perfect agreement"),
                    (["a", "b"], ["b", "a"], "n=2, total disagreement"),
                    (["a"]*9 + ["b"], ["a"]*8 + ["b"]*2, "n=10")]:
    k = cohens_kappa(la, lb)
    print(f"  {why:<26} kappa={k.kappa} CI=[{k.ci_lower}, {k.ci_upper}] p_o={k.p_o} p_e={k.p_e}")

print("\n== C5: verdict MATCH, kappa label DISAGREE (one-element set vs string)")
scores = [score_pair("validation_setting", "Phantom; NR", "Phantom"),
          score_pair("validation_setting", "Clinical", "Clinical")]
for s in scores:
    print(f"  {s.result}: norm_a={s.norm_a!r} norm_b={s.norm_b!r}")
fs = field_summary("validation_setting", scores)
print(f"  percent_agreement={fs.percent_agreement} kappa={fs.kappa} kappa_p_o={fs.kappa_p_o}")
