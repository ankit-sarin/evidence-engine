"""Per-class evidence elicitation (ELICIT-DESIGN-01).

Pass 1 deliberately elicits evidence as sentence-unit citations under per-class
contracts (STATED / INFERABLE / JUDGMENT); the engine materializes verbatim text
from the persisted unit map and primes Pass 2 with it.

The citation mechanism is the same for all three classes -- the model names unit
indices, never copies text. The classes differ in what must ACCOMPANY the
citations, not in how evidence is cited (architect ruling, ELICIT-01 exit).
"""
