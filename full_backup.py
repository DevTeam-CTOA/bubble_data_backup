#!/usr/bin/env python3
"""Backs up all Bubble data types to CSV files and refreshes the consolidated baseline."""

import os

from backup_utils import (
    OUTPUT_DIR,
    collect_all_keys,
    ensure_output_dirs,
    get_all_data_types,
    get_consolidated_path,
    get_row_count,
    load_state,
    make_date_folder,
    make_timestamp,
    max_modified_at,
    save_state,
    update_table_state,
    fetch_records,
    write_csv,
    write_schema,
)


def main():
    ensure_output_dirs()
    dated_output_dir = os.path.join(OUTPUT_DIR, make_date_folder())
    os.makedirs(dated_output_dir, exist_ok=True)
    timestamp = make_timestamp()
    state = load_state()

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

    schema_filename = os.path.join(dated_output_dir, f"schema_{timestamp}.csv")
    write_schema(all_types, enabled, row_counts, schema_filename)
    print(f"\nSchema saved: {schema_filename}")

    print(f"\nFull backup started — {len(enabled)} table(s)")
    print(f"Output folder: {dated_output_dir}")
    print(f"Timestamp: {timestamp}\n")

    for dt in enabled:
        print(f"[{dt}]")
        records = fetch_records(dt)

        if records is None:
            print("  FAILED — skipping\n")
            continue

        headers = collect_all_keys(records, preferred_keys=["_id"])
        snapshot_filename = os.path.join(dated_output_dir, f"{dt}_{timestamp}.csv")
        consolidated_filename = get_consolidated_path(dt)

        num_cols = write_csv(records, snapshot_filename, headers)
        write_csv(records, consolidated_filename, headers)
        update_table_state(state, dt, max_modified_at(records), source_file=os.path.basename(snapshot_filename))

        print(
            f"  Saved snapshot: {snapshot_filename} ({len(records)} records, {num_cols} columns)"
        )
        print(f"  Updated consolidated baseline: {consolidated_filename}\n")

    save_state(state)
    print("Full backup complete!")


if __name__ == "__main__":
    main()
