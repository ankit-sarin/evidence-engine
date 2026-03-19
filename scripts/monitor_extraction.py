"""Watchdog: monitors extract_log.txt for stalls and reports progress."""

import argparse
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DEFAULT_REVIEW = "surgical_autonomy"
CHECK_INTERVAL = 20 * 60  # 20 minutes


def get_extracted_count() -> int:
    conn = sqlite3.connect(str(DB_PATH))
    count = conn.execute(
        "SELECT COUNT(*) FROM papers WHERE status = 'EXTRACTED'"
    ).fetchone()[0]
    conn.close()
    return count


def get_line_count() -> int:
    if not LOG_PATH.exists():
        return 0
    with open(LOG_PATH) as f:
        return sum(1 for _ in f)


def get_tail(n: int = 5) -> str:
    if not LOG_PATH.exists():
        return "(log file not found)"
    with open(LOG_PATH) as f:
        lines = f.readlines()
    return "".join(lines[-n:]).rstrip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor extraction progress")
    parser.add_argument("--review", default=DEFAULT_REVIEW, help=f"Review name (default: {DEFAULT_REVIEW})")
    args = parser.parse_args()

    if args.review == DEFAULT_REVIEW and "--review" not in " ".join(sys.argv):
        logging.warning("No --review specified, using default 'surgical_autonomy'.")

    global LOG_PATH, DB_PATH
    LOG_PATH = Path(f"data/{args.review}/extract_log.txt")
    DB_PATH = Path(f"data/{args.review}/review.db")

    print(f"Monitoring {LOG_PATH} every {CHECK_INTERVAL // 60} minutes")
    print("Press Ctrl+C to stop\n")

    prev_lines = get_line_count()

    while True:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur_lines = get_line_count()
        extracted = get_extracted_count()

        if cur_lines > prev_lines:
            print(f"[{now}] Extraction alive — {extracted} papers extracted so far")
        else:
            print(f"[{now}] WARNING: no new log output in {CHECK_INTERVAL // 60} minutes")
            print(f"         Extracted so far: {extracted}")
            print(f"         Last 5 lines of log:")
            for line in get_tail(5).splitlines():
                print(f"           {line}")
            print()

        prev_lines = cur_lines
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nMonitor stopped.")
