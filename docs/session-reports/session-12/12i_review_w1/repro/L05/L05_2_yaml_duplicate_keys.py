"""A key declared twice is accepted by both loaders; the earlier declaration silently never takes effect."""
from _common import CODEBOOK, write, LIVE_SPEC
from engine.core.codebook import load_codebook
from engine.core.review_spec import load_review_spec
# codebook: second top-level escape_token, and a second `definition` inside a field
text = CODEBOOK.replace('    definition: "The robot."\n', '    definition: "The robot."\n    definition: "SOMETHING ELSE ENTIRELY."\n')
text += 'escape_token: "SECOND_TOKEN"\n'
cb = load_codebook(write("dup_codebook.yaml", text))
print("codebook loaded; escape_token =", cb.escape_token, "| robot_platform.definition =", cb.field("robot_platform")["definition"])
# spec: the live spec's text plus a second low_yield_threshold and a second ft_screening_models block
base = LIVE_SPEC.read_text()
s0 = load_review_spec(LIVE_SPEC)
dup = base + '\nlow_yield_threshold: 19\nft_screening_models:\n  primary: "some-other-model:1b"\n'
s1 = load_review_spec(write("dup_spec.yaml", dup))
print("spec loaded; low_yield_threshold", s0.low_yield_threshold, "->", s1.low_yield_threshold)
print("ft primary", s0.ft_screening_models.primary, "->", s1.ft_screening_models.primary,
      "| ft verifier", s0.ft_screening_models.verifier, "->", s1.ft_screening_models.verifier)
