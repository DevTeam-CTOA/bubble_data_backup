#!/usr/bin/env python3
"""Runs incremental Bubble backups based on Modified Date and consolidates by _id."""

import os
import sys

from backup_utils import (
    BUBBLE_MODIFIED_FIELD,
    OUTPUT_DIR,
    collect_all_keys,
    ensure_output_dirs,
    fetch_complete_records,
    fetch_records,
    format_bubble_datetime,
    get_all_data_types,
    get_consolidated_path,
    get_row_count,
    make_date_folder,
    make_timestamp,
    max_modified_at,
    merge_records_by_id,
    read_csv_records,
    watermark_with_overlap,
    write_csv,
    write_schema,
)


def build_incremental_constraints(last_modified_at):
    """Builds a Bubble Data API constraint for incremental fetches."""
    watermark = watermark_with_overlap(last_modified_at)
    if watermark is None:
        return None

    return [
        {
            "key": BUBBLE_MODIFIED_FIELD,
            "constraint_type": "greater than",
            "value": format_bubble_datetime(watermark),
        }
    ]


def main():
    ensure_output_dirs()
    dated_output_dir = os.path.join(OUTPUT_DIR, make_date_folder())
    os.makedirs(dated_output_dir, exist_ok=True)
    timestamp = make_timestamp()

    all_types, enabled = get_all_data_types()
    print(f"Found {len(all_types)} data types ({len(enabled)} enabled)\n")

    print("Fetching row counts...")
    row_counts = {}
    for dt in all_types:
        if dt in enabled:
            count = get_row_count(dt)
            row_counts[dt] = count
            status = f"{count:,}" if count is not None else "N/A (not accessible)"
            print(f"  {dt}: {status}")

    schema_filename = os.path.join(dated_output_dir, f"schema_incremental_{timestamp}.csv")
    write_schema(all_types, enabled, row_counts, schema_filename)
    print(f"\nSchema saved: {schema_filename}")

    print(f"\nIncremental backup started — {len(enabled)} table(s)")
    print(f"Output folder: {dated_output_dir}")
    print(f"Timestamp: {timestamp}\n")

    missing_baselines = []
    for dt in enabled:
        consolidated_path = get_consolidated_path(dt)
        if not os.path.exists(consolidated_path):
            missing_baselines.append((dt, consolidated_path))

    if missing_baselines:
        print("Incremental backup aborted: missing consolidated baselines.")
        for dt, consolidated_path in missing_baselines:
            print(f"  {dt}: {consolidated_path}")
        print("Run full_backup.py successfully before recurrent_backup.py.")
        sys.exit(1)

    had_failures = False

    for dt in enabled:
        print(f"[{dt}]")
        consolidated_path = get_consolidated_path(dt)

        existing_headers, existing_records = read_csv_records(consolidated_path)
        last_modified_at = max_modified_at(existing_records)
        constraints = build_incremental_constraints(last_modified_at)
        full_refresh = constraints is None

        if last_modified_at:
            print(f"  Watermark: {format_bubble_datetime(last_modified_at)}")
        else:
            print("  No usable Modified Date in the baseline. Running a full refresh for this table.")

        if full_refresh:
            delta_records = fetch_complete_records(
                dt,
                row_count=row_counts.get(dt),
                progress_label=f"{dt} incremental",
            )
        else:
            delta_records = fetch_records(
                dt,
                constraints=constraints,
                progress_label=f"{dt} incremental",
            )

        if delta_records is None:
            print("  FAILED — skipping\n")
            had_failures = True
            continue

        if not delta_records and not full_refresh:
            print("  No changes since the last watermark.\n")
            continue

        incremental_filename = os.path.join(dated_output_dir, f"{dt}_incremental_{timestamp}.csv")
        delta_headers = collect_all_keys(delta_records, preferred_keys=["_id"])
        write_csv(delta_records, incremental_filename, delta_headers)

        if full_refresh:
            merged_records = delta_records
        else:
            merged_records = merge_records_by_id(existing_records, delta_records)

        merged_headers = collect_all_keys(merged_records, preferred_keys=existing_headers or ["_id"])
        write_csv(merged_records, consolidated_path, merged_headers)

        if full_refresh:
            print(f"  Refresh saved: {incremental_filename} ({len(delta_records)} records)")
        else:
            print(f"  Delta saved: {incremental_filename} ({len(delta_records)} changed records)")
        print(f"  Consolidated snapshot updated: {consolidated_path} ({len(merged_records)} total records)\n")

    if had_failures:
        print("Incremental backup finished with failures. Check the logs above.")
        sys.exit(1)

    print("Incremental backup complete!")


if __name__ == "__main__":
    main()
