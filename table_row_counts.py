#!/usr/bin/env python3
"""Generates a root-level report with total row counts per Bubble table."""

import csv
import os
import tempfile

from backup_utils import get_all_data_types, get_row_count, make_timestamp

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_REPORT = os.path.join(BASE_DIR, "table_row_counts_latest.csv")
MARKDOWN_REPORT = os.path.join(BASE_DIR, "table_row_counts_latest.md")


def atomic_write_text(filename, content):
    """Writes a text file atomically."""
    directory = os.path.dirname(filename) or "."
    os.makedirs(directory, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=".tmp-", suffix=".tmp", dir=directory)

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.replace(temp_path, filename)
    except Exception:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass
        raise


def write_csv_report(rows):
    """Writes the CSV report with only table name and row count."""
    fd, temp_path = tempfile.mkstemp(
        prefix=".tmp-",
        suffix=".csv",
        dir=BASE_DIR,
    )

    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, quoting=csv.QUOTE_ALL)
            writer.writerow(["table_name", "row_count"])
            writer.writerows(rows)
        os.replace(temp_path, CSV_REPORT)
    except Exception:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass
        raise


def write_markdown_report(rows, generated_at):
    """Writes the Markdown report with only table name and row count."""
    lines = [
        "# Bubble Table Row Counts",
        "",
        f"Generated at: {generated_at}",
        "",
        "| table_name | row_count |",
        "| --- | ---: |",
    ]

    for table_name, row_count in rows:
        count_display = "" if row_count is None else f"{row_count:,}"
        lines.append(f"| {table_name} | {count_display} |")

    lines.append("")
    atomic_write_text(MARKDOWN_REPORT, "\n".join(lines))


def main():
    generated_at = make_timestamp()
    all_types, enabled = get_all_data_types()
    print(f"Found {len(enabled)} enabled table(s) out of {len(all_types)} available data type(s).")

    rows = []
    for table_name in sorted(enabled):
        row_count = get_row_count(table_name)
        status = f"{row_count:,}" if row_count is not None else "N/A"
        print(f"  {table_name}: {status}")
        rows.append((table_name, row_count))

    write_csv_report(rows)
    write_markdown_report(rows, generated_at)

    print(f"\nCSV report saved: {CSV_REPORT}")
    print(f"Markdown report saved: {MARKDOWN_REPORT}")


if __name__ == "__main__":
    main()
