#!/usr/bin/env python3
"""Generates a root-level CSV report with total row counts per Bubble table."""

import csv
import os

from backup_utils import get_all_data_types, get_row_count

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_REPORT = os.path.join(BASE_DIR, "table_row_counts_latest.csv")


def write_csv_report(rows):
    """Writes the CSV report with only table name and row count."""
    with open(CSV_REPORT, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_ALL)
        writer.writerow(["table_name", "row_count"])
        writer.writerows(rows)


def main():
    all_types, enabled = get_all_data_types()
    print(f"Found {len(enabled)} enabled table(s) out of {len(all_types)} available data type(s).")

    rows = []
    for table_name in sorted(enabled):
        row_count = get_row_count(table_name)
        status = f"{row_count:,}" if row_count is not None else "N/A"
        print(f"  {table_name}: {status}")
        rows.append((table_name, row_count))

    write_csv_report(rows)

    print(f"\nCSV report saved: {CSV_REPORT}")


if __name__ == "__main__":
    main()
