"""The one repository root (U23, E-ROOT; R780, R781).

Computed from this file's own location, so it does not follow the working
directory. `DATA_ROOT`, `SPEC_ROOT`, `git_state` and the parsed-text store all
resolve under this object. This module imports nothing from `engine`, so anything
may import it without a cycle.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
