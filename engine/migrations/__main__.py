"""CLI entry point for the migration runner (R222a).

    python -m engine.migrations <db_path> [--apply-pending] [--include-data]

Calls `runner.run(db_path, include_data=..., apply_pending=...)` exactly once
and prints its return verbatim. This is the sanctioned, sole caller in the tree
that passes `apply_pending=True` — the remedy text `PendingMigrations` names.

It opens nothing itself and performs no backup, embargo or exclusivity check —
those stay in the operator's pre-flight (the 10b brief), never in code.
"""

from __future__ import annotations

import argparse
import sys

from engine.migrations import runner


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m engine.migrations",
        description="Apply pending migrations to a review database.",
    )
    parser.add_argument("db_path", help="Path to the review.db to migrate.")
    parser.add_argument(
        "--apply-pending", action="store_true",
        help="Apply pending schema migrations to a database that already "
             "carries receipts (non-fresh). Without this flag, such a database "
             "refuses with PendingMigrations if anything is pending (R222). A "
             "fresh database (no receipts) always applies regardless of this "
             "flag.",
    )
    parser.add_argument(
        "--include-data", action="store_true",
        help="Also apply pending data migrations. Never applied on a fresh "
             "database regardless of this flag; see engine/migrations/README.md.",
    )
    args = parser.parse_args(argv)

    try:
        result = runner.run(
            args.db_path,
            include_data=args.include_data,
            apply_pending=args.apply_pending,
        )
    except Exception as exc:  # noqa: BLE001 - the message is the whole report
        print(str(exc), file=sys.stderr)
        return 1

    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
