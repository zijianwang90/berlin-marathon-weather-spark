#!/usr/bin/env python3
"""Quick raw-data profiler for the Berlin Marathon project.

This script is intentionally lightweight so the team can validate the raw
inputs before running the Spark ETL pipeline.
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from pathlib import Path


TIME_PATTERN = re.compile(r"^\d{2}:\d{2}:\d{2}$")


def summarize_results(path: Path) -> None:
    row_count = 0
    gender_counter: Counter[str] = Counter()
    age_counter: Counter[str] = Counter()
    invalid_time_count = 0

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row_count += 1
            gender_counter[row["GENDER"].strip()] += 1
            age_counter[row["AGE"].strip()] += 1
            if not TIME_PATTERN.match(row["TIME"].strip()):
                invalid_time_count += 1

    print(f"[results] rows={row_count}")
    print(f"[results] gender_counts={dict(gender_counter)}")
    print(f"[results] top_age_values={age_counter.most_common(15)}")
    print(f"[results] invalid_time_rows={invalid_time_count}")


def summarize_weather(path: Path) -> None:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    print(f"[weather] rows={len(rows)}")
    print(f"[weather] columns={reader.fieldnames}")
    if rows:
        print(f"[weather] sample={rows[0]}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile raw Berlin Marathon CSV files.")
    parser.add_argument(
        "--results",
        default="src/Berlin_Marathon_data_1974_2019.csv",
        help="Path to runner-level marathon results CSV.",
    )
    parser.add_argument(
        "--weather",
        default="src/Berlin_Marathon_weather_data_since_1974.csv",
        help="Path to weather summary CSV.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summarize_results(Path(args.results))
    summarize_weather(Path(args.weather))


if __name__ == "__main__":
    main()
