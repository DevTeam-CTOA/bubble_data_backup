#!/usr/bin/env python3
"""Backs up all Bubble data types to CSV files and refreshes the consolidated baseline."""

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from backup_utils import (
    LARGE_TABLE_ROW_THRESHOLD,
    MAX_CONCURRENT_LARGE_TABLES,
    MAX_CONCURRENT_TABLES,
    OUTPUT_DIR,
    collect_all_keys,
    ensure_output_dirs,
    fetch_complete_records,
    get_all_data_types,
    get_consolidated_path,
    get_row_count,
    is_large_table,
    make_date_folder,
    make_timestamp,
    write_csv,
    write_schema,
)


def fetch_row_counts(enabled_tables):
    """Fetches row counts concurrently to speed up table classification."""
    row_counts = {}
    max_workers = max(1, min(MAX_CONCURRENT_TABLES, len(enabled_tables)))

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="row-count") as executor:
        future_to_table = {
            executor.submit(get_row_count, table_name): table_name
            for table_name in enabled_tables
        }
        for future in as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                count = future.result()
            except Exception as exc:
                print(f"  {table_name}: FAILED to fetch row count ({exc})", file=sys.stderr)
                count = None

            row_counts[table_name] = count
            status = f"{count:,}" if count is not None else "N/A (not accessible)"
            print(f"  {table_name}: {status}")

    return row_counts


def backup_table(table_name, row_count, dated_output_dir, timestamp):
    """Backs up a single Bubble table to snapshot and consolidated CSVs."""
    print(f"[{table_name}] Starting backup...")
    records = fetch_complete_records(
        table_name,
        row_count=row_count,
        progress_label=table_name,
    )

    if records is None:
        return {"table_name": table_name, "success": False}

    headers = collect_all_keys(records, preferred_keys=["_id"])
    snapshot_filename = os.path.join(dated_output_dir, f"{table_name}_{timestamp}.csv")
    consolidated_filename = get_consolidated_path(table_name)

    num_cols = write_csv(records, snapshot_filename, headers)
    write_csv(records, consolidated_filename, headers)

    return {
        "table_name": table_name,
        "success": True,
        "snapshot_filename": snapshot_filename,
        "consolidated_filename": consolidated_filename,
        "record_count": len(records),
        "column_count": num_cols,
    }


def main():
    ensure_output_dirs()
    dated_output_dir = os.path.join(OUTPUT_DIR, make_date_folder())
    os.makedirs(dated_output_dir, exist_ok=True)
    timestamp = make_timestamp()

    all_types, enabled = get_all_data_types()
    enabled_tables = sorted(enabled)
    print(f"Found {len(all_types)} data types ({len(enabled)} enabled)\n")

    print("Fetching row counts...")
    row_counts = fetch_row_counts(enabled_tables)

    schema_filename = os.path.join(dated_output_dir, f"schema_{timestamp}.csv")
    write_schema(all_types, enabled, row_counts, schema_filename)
    print(f"\nSchema saved: {schema_filename}")

    large_tables = [
        table_name for table_name in enabled_tables
        if is_large_table(table_name, row_counts.get(table_name))
    ]
    regular_tables = [
        table_name for table_name in enabled_tables
        if table_name not in large_tables
    ]

    print(f"\nFull backup started — {len(enabled_tables)} table(s)")
    print(f"Output folder: {dated_output_dir}")
    print(f"Timestamp: {timestamp}\n")
    print(
        "Parallelism: "
        f"{MAX_CONCURRENT_TABLES} regular table(s) + "
        f"{MAX_CONCURRENT_LARGE_TABLES} large table(s)"
    )
    print(f"Large-table threshold: {LARGE_TABLE_ROW_THRESHOLD:,} rows")
    if large_tables:
        print(f"Large tables: {', '.join(large_tables)}")
    else:
        print("Large tables: none")
    print("")

    failed_tables = []
    future_to_table = {}

    executors = []
    if regular_tables:
        regular_workers = max(1, min(MAX_CONCURRENT_TABLES, len(regular_tables)))
        executors.append(
            ThreadPoolExecutor(
                max_workers=regular_workers,
                thread_name_prefix="backup-regular",
            )
        )
        for table_name in regular_tables:
            future = executors[-1].submit(
                backup_table,
                table_name,
                row_counts.get(table_name),
                dated_output_dir,
                timestamp,
            )
            future_to_table[future] = table_name

    if large_tables:
        large_workers = max(1, min(MAX_CONCURRENT_LARGE_TABLES, len(large_tables)))
        executors.append(
            ThreadPoolExecutor(
                max_workers=large_workers,
                thread_name_prefix="backup-large",
            )
        )
        for table_name in large_tables:
            future = executors[-1].submit(
                backup_table,
                table_name,
                row_counts.get(table_name),
                dated_output_dir,
                timestamp,
            )
            future_to_table[future] = table_name

    try:
        for future in as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                result = future.result()
            except Exception as exc:
                print(f"[{table_name}] FAILED — unexpected error: {exc}\n", file=sys.stderr)
                failed_tables.append(table_name)
                continue

            if not result["success"]:
                print(f"[{table_name}] FAILED — skipping\n")
                failed_tables.append(table_name)
                continue

            print(
                f"[{table_name}] Saved snapshot: {result['snapshot_filename']} "
                f"({result['record_count']} records, {result['column_count']} columns)"
            )
            print(
                f"[{table_name}] Updated consolidated baseline: "
                f"{result['consolidated_filename']}\n"
            )
    finally:
        for executor in executors:
            executor.shutdown(wait=True)

    if failed_tables:
        print("Full backup finished with failures.")
        print("Failed tables:")
        for dt in failed_tables:
            print(f"  - {dt}")
        sys.exit(1)

    print("Full backup complete!")


if __name__ == "__main__":
    main()
