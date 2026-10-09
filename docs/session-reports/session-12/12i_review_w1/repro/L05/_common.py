"""Shared synthetic fixtures for the L05 reproducers (scratch only; no DB, no model, no network)."""
from pathlib import Path
REPO = Path("/home/ankitsarin/projects/evidence-engine")
HERE = Path(__file__).resolve().parent
LIVE_SPEC = REPO / "review_specs" / "surgical_autonomy.yaml"   # read-only source text

CODEBOOK = '''version: "1.0"
review: "alpha"
date: "2026-01-01"
escape_token: "NO_EVIDENCE_LOCATABLE"
contract_unmet_token: "CONTRACT_UNMET"
absence_sentinels: ["NR", "NOT_FOUND"]
canonical_absence_sentinel: "NR"
fields:
  - name: robot_platform
    type: free_text
    tier: 1
    definition: "The robot."
    instruction: "Name it."
    field_class: stated
    judge_rubric_family: free_text
  - name: randomised
    type: categorical
    tier: 1
    definition: "Was it randomised."
    instruction: "Classify it."
    field_class: inferable
    judge_rubric_family: categorical
    valid_values:
      - {value: "Yes", definition: "Randomised."}
      - {value: "No", definition: "Not randomised."}
'''
def write(name, text, newline=None):
    p = HERE / "tmp" / name
    p.parent.mkdir(exist_ok=True)
    with open(p, "w", newline=newline) as f:
        f.write(text)
    return p
