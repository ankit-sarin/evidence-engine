# parse_quality fixtures

Mid-file excerpts of two real parsed documents of near-identical length and
opposite structure. **Mid-file, not head**, because every PyMuPDF- and
vision-parsed document opens with a `<!-- Page 1 -->` marker; a head excerpt
would pin the marker rather than the prose.

p455 and p561 are 59,964 and 59,931 characters — a 33-character difference on
~60 KB. Any length-based check treats them as the same document. They are the
worked example for why the gate measures structure.

| fixture | source | version | byte offset | excerpt bytes | sha256 |
|---|---|---:|---:|---:|---|
| `p455_shattered.md` | `data/surgical_autonomy/parsed_text/455_v2.md` | 2 | 29,982 | 1,018 | `e3640000a7ca24af831c20b75255981231f07ea83ace9255c44a9eaf28f904b2` |
| `p561_clean.md` | `data/surgical_autonomy/parsed_text/561_v2.md` | 2 | 29,965 | 1,000 | `89c48432fcc38153b78fedba5b734fc5d8d2a76e8f626a95e0324d29178dccab` |

Offsets are `len(raw) // 2` of the source file. Excerpts are 1,000 **characters**
sliced from that offset; the byte counts above differ from 1,000 where the slice
contains non-ASCII.

## Verdicts under the provisional defaults

| fixture | verdict |
|---|---|
| `p455_shattered.md` | FAIL: SHORT_UNIT_SHARE=94.8 (limit 50.0); CHARS_PER_UNIT=7.5 (limit 20.0) |
| `p561_clean.md` | PASS |

Regenerating these requires the gitignored corpus under `data/`. The sha256 above
is the contract: if a fixture is ever rebuilt and the digest changes, the source
parse changed and the pinned verdicts must be re-read, not re-blessed.
