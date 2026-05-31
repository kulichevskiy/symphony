#!/usr/bin/env python3
"""One-time backfill for historical run termination telemetry."""

from __future__ import annotations

import argparse
from pathlib import Path

from symphony.db.termination_backfill import run_backfill


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill runs.termination_* columns from logs/{run_id}.log."
    )
    parser.add_argument("--db", required=True, type=Path, help="Path to state.sqlite")
    parser.add_argument(
        "--log-root",
        required=True,
        type=Path,
        help="Directory containing per-run logs named {run_id}.log",
    )
    args = parser.parse_args()

    result = run_backfill(
        db_path=args.db.expanduser(),
        log_root=args.log_root.expanduser(),
    )
    print(f"updated: {result.updated}")
    print("termination_kind\tcount")
    for kind, count in result.aggregate:
        print(f"{kind}\t{count}")


if __name__ == "__main__":
    main()
