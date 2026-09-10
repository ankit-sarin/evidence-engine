"""INVENTORY-01 — the generated entry-point inventory and its drift guard.

T1 classification on a fixture tree · T2 drift detection · T3 runtime bound.
"""

import json
import time
from pathlib import Path

import pytest

from engine.tools import inventory

REPO_ROOT = Path(__file__).resolve().parent.parent


# ── T1: classification ───────────────────────────────────────────────


IDS = {"surgical_autonomy", "other_review"}


def _analyze(tmp_path, name, source):
    p = tmp_path / name
    p.write_text(source)
    return inventory.analyze_file(p, name, IDS)


def test_argparse_script_is_an_entry_point_with_its_flags(tmp_path):
    r = _analyze(tmp_path, "cli.py", '''
import argparse

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--review", required=True, help="Review id")
    p.add_argument("--spec", default=None, help="Override review_specs/<review>.yaml")
    p.add_argument("--limit", type=int, default=10)
    return p.parse_args()

if __name__ == "__main__":
    main()
''')
    assert r["entry_point"] is True
    assert r["has_main_guard"] is True
    flags = {tuple(f["names"]): f for f in r["flags"]}
    assert flags[("--review",)]["required"] is True
    assert flags[("--spec",)]["default"] == "None"
    assert flags[("--limit",)]["default"] == "10"
    # The help text mentions review_specs but is prose, not a constructed path.
    roles = {h["role"] for h in r["path_construction"]}
    assert roles == {"help_text"}
    assert r["unparsed"] == []


def test_dynamic_flag_name_is_reported_unparsed_not_omitted(tmp_path):
    """I1's escape hatch: a flag the tool cannot read must be visible."""
    r = _analyze(tmp_path, "dyn.py", '''
import argparse
FLAG = "--review"

def main():
    p = argparse.ArgumentParser()
    p.add_argument(FLAG, required=True)
    for name in ("--a", "--b"):
        p.add_argument(name)
''')
    assert len(r["unparsed"]) == 2
    reasons = " ".join(u["reason"] for u in r["unparsed"])
    assert "non-literal flag name" in reasons
    assert "FLAG" in reasons and "name" in reasons
    # and it did not silently record a flag it could not read
    assert r["flags"] == []


def test_format_string_path_builder_is_seen(tmp_path):
    """The class a filename grep cannot find."""
    r = _analyze(tmp_path, "build.py", '''
def go(review):
    return f"review_specs/{review}_v1.yaml"
''')
    hits = [h for h in r["path_construction"] if h["role"] == "code"]
    # Exactly one: the f-string as a whole, not its literal fragments too.
    assert len(hits) == 1, hits
    assert hits[0]["kind"] == "f-string"
    assert "review_specs" in hits[0]["text"]


def test_literal_path_and_f_string_are_distinguished(tmp_path):
    r = _analyze(tmp_path, "both.py", '''
LITERAL = "review_specs/surgical_autonomy.yaml"

def go(x):
    return f"data/{x}/review.db"
''')
    kinds = {h["kind"] for h in r["path_construction"] if h["role"] == "code"}
    assert kinds == {"literal", "f-string"}


def test_raw_yaml_load_is_recorded_with_its_target(tmp_path):
    r = _analyze(tmp_path, "y.py", '''
import yaml
from pathlib import Path

def load(codebook_path):
    return yaml.safe_load(Path(codebook_path).read_text())
''')
    assert len(r["yaml_loads"]) == 1
    y = r["yaml_loads"][0]
    assert y["call"] == "safe_load"
    assert "codebook_path" in y["target"]
    assert y["scope"] == "load"


def test_literal_review_id_is_found_and_prose_mentions_are_not(tmp_path):
    """Equality, not containment.

    A docstring or help string that MENTIONS a review is prose about the
    review; only a string that IS the review id is a hardcoded review.
    """
    r = _analyze(tmp_path, "lit.py", '''
"""Usage: --review surgical_autonomy"""
import argparse

DEFAULT_REVIEW = "surgical_autonomy"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--review", default="surgical_autonomy", help="e.g. surgical_autonomy")
''')
    assert r["review_id_constants"] == [
        {"name": "DEFAULT_REVIEW", "line": 5, "value": "surgical_autonomy"}
    ]
    hits = sorted((h["line"], h["role"]) for h in r["literal_review_ids"])
    assert hits == [(5, "code"), (9, "code")], hits


def test_reviewdatabase_order_is_per_scope_not_per_file(tmp_path):
    """A helper defined above main() must not make main() look db-first."""
    r = _analyze(tmp_path, "order.py", '''
from engine.core.database import ReviewDatabase
from engine.core.review_paths import load_spec_for

def helper(name):
    return ReviewDatabase(name)

def main():
    spec = load_spec_for("surgical_autonomy")
    db = ReviewDatabase("surgical_autonomy")
    return spec, db

def bad():
    db = ReviewDatabase("surgical_autonomy")
    spec = load_spec_for("surgical_autonomy")
    return spec, db
''')
    assert r["order"] == {"main": "spec_first", "bad": "db_first"}
    assert "helper" not in r["order"]  # no spec load in that scope at all


def test_data_root_kwarg_is_recorded(tmp_path):
    r = _analyze(tmp_path, "dr.py", '''
from engine.core.database import ReviewDatabase
a = ReviewDatabase("x")
b = ReviewDatabase("x", data_root=tmp)
''')
    assert [d["data_root_passed"] for d in r["review_database"]] == [False, True]


def test_syntax_error_is_reported_not_raised(tmp_path):
    r = _analyze(tmp_path, "broken.py", "def f(:\n    pass\n")
    assert len(r["unparsed"]) == 1
    assert "SyntaxError" in r["unparsed"][0]["reason"]
    assert r["entry_point"] is False


def test_review_ids_exclude_data_dirs_without_a_review_db(tmp_path):
    (tmp_path / "review_specs").mkdir()
    (tmp_path / "review_specs" / "alpha.yaml").write_text("review_id: alpha\n")
    (tmp_path / "data" / "alpha").mkdir(parents=True)
    (tmp_path / "data" / "alpha" / "review.db").write_text("")
    (tmp_path / "data" / "backups").mkdir()
    (tmp_path / "data" / "backups" / "old.db").write_text("")
    ids, excluded = inventory.review_ids(tmp_path)
    assert ids == ["alpha"]
    assert "backups" in excluded and "no review.db" in excluded["backups"]


# ── T2: drift ────────────────────────────────────────────────────────


def test_committed_inventory_is_in_sync_with_the_tree():
    """The whole point. If this is red, regenerate — do not edit the file."""
    message = inventory.drift(REPO_ROOT)
    assert message is None, message


def test_drift_names_the_first_differing_file(monkeypatch):
    committed = inventory.committed_inventory(REPO_ROOT)
    assert committed is not None
    victim = "engine/core/review_paths.py"
    assert victim in committed["files"]

    mutated = json.loads(json.dumps(committed))
    mutated["files"][victim]["flags"] = [{"names": ["--not-real"]}]
    monkeypatch.setattr(inventory, "committed_inventory", lambda *_a, **_k: mutated)

    message = inventory.drift(REPO_ROOT)
    assert message is not None
    assert victim in message
    assert "flags" in message


def test_drift_reports_a_file_missing_from_the_committed_inventory(monkeypatch):
    committed = inventory.committed_inventory(REPO_ROOT)
    mutated = json.loads(json.dumps(committed))
    victim = sorted(mutated["files"])[0]
    del mutated["files"][victim]
    monkeypatch.setattr(inventory, "committed_inventory", lambda *_a, **_k: mutated)
    message = inventory.drift(REPO_ROOT)
    assert message is not None and victim in message


def test_drift_ignores_the_generating_commit_hash():
    """meta moves with every commit; comparing it would fail on its own success."""
    raw = json.loads((REPO_ROOT / inventory.OUT_JSON).read_text())
    assert "generated_at_commit" in raw["meta"]
    assert "generated_at_commit" not in raw["data"]


def test_markdown_says_it_is_generated():
    md = (REPO_ROOT / inventory.OUT_MD).read_text()
    assert "GENERATED — DO NOT EDIT" in md
    assert "engine/tools/inventory.py" in md


# ── T3: runtime ──────────────────────────────────────────────────────


def test_full_scan_is_fast_enough_for_the_standard_gate():
    start = time.perf_counter()
    inventory.build_inventory(REPO_ROOT)
    elapsed = time.perf_counter() - start
    assert elapsed < 5.0, f"scan took {elapsed:.2f}s"


# ── The constraint that makes the tool safe ──────────────────────────


def test_the_tool_never_imports_a_scanned_module_or_opens_a_database():
    """AST only, asserted structurally.

    Importing a scanned module runs its module-level code, and
    ReviewDatabase.__init__ mkdirs its root — an inventory tool that modified
    the tree it inventories would be worse than no tool.
    """
    src = (REPO_ROOT / "engine/tools/inventory.py").read_text()
    for forbidden in ("importlib", "__import__", "exec(", "eval(",
                      "sqlite3", "ReviewDatabase("):
        assert forbidden not in src, f"inventory.py references {forbidden}"


def test_exception_and_log_messages_are_not_counted_as_path_construction(tmp_path):
    """Structural, from the callee — never from what the string looks like.

    A metric for "spec paths built by hand" that counts the error message
    telling an operator the naming convention has a floor above its target,
    and a metric that can never reach zero teaches the reader to ignore it.
    """
    r = _analyze(tmp_path, "msg.py", '''
import logging
logger = logging.getLogger(__name__)

class ReviewSpecError(ValueError):
    pass

def go(path, review):
    logger.warning("no spec at %s, expected review_specs/<id>.yaml", path)
    if not path:
        raise ReviewSpecError(f"Expected review_specs/{review}.yaml")
    return f"review_specs/{review}.yaml"
''')
    by_role = {}
    for h in r["path_construction"]:
        by_role.setdefault(h["role"], []).append(h["line"])
    assert by_role.get("message") and len(by_role["message"]) == 2
    assert len(by_role.get("code", [])) == 1, by_role
