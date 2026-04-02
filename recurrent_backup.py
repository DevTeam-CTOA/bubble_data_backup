#!/usr/bin/env python3
"""Runs incremental Bubble backups based on Modified Date and consolidates by _id."""

import os
import sys

from backup_utils import (
    BUBBLE_MODIFIED_FIELD,
    OUTPUT_DIR,
    collect_all_keys,
    ensure_output_dirs,
    ensure_table_baseline,
    fetch_records,
    format_bubble_datetime,
    get_all_data_types,
    get_consolidated_path,
    get_row_count,
    load_state,
    make_date_folder,
    make_timestamp,
    max_modified_at,
    merge_records_by_id,
    parse_bubble_datetime,
    save_state,
    seed_table_from_records,
    update_table_state,
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


def initial_full_seed(table_name, state, dated_output_dir, timestamp):
    """Seeds a table baseline with a one-time full fetch when none exists."""
    print("  No baseline found. Seeding this table with a full export.")
    records = fetch_records(table_name, progress_label=f"{table_name} seed")
    if records is None:
        return None

    snapshot_filename = os.path.join(dated_output_dir, f"{table_name}_{timestamp}.csv")
    headers = collect_all_keys(records, preferred_keys=["_id"])
    write_csv(records, snapshot_filename, headers)
    seed_table_from_records(state, table_name, records, snapshot_filename)
    print(f"  Seed snapshot saved: {snapshot_filename}")
    print(f"  Consolidated baseline created: {get_consolidated_path(table_name)}")
    return records


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

    schema_filename = os.path.join(dated_output_dir, f"schema_incremental_{timestamp}.csv")
    write_schema(all_types, enabled, row_counts, schema_filename)
    print(f"\nSchema saved: {schema_filename}")

    print(f"\nIncremental backup started — {len(enabled)} table(s)")
    print(f"Output folder: {dated_output_dir}")
    print(f"Timestamp: {timestamp}\n")

    had_failures = False

    for dt in enabled:
        print(f"[{dt}]")
        existing_headers, existing_records, consolidated_path = ensure_table_baseline(state, dt)

        if existing_records is None:
            seeded = initial_full_seed(dt, state, dated_output_dir, timestamp)
            if seeded is None:
                print("  FAILED — skipping\n")
                had_failures = True
                continue
            print()
            continue

        last_modified_at = state.get("tables", {}).get(dt, {}).get("last_modified_at")
        constraints = build_incremental_constraints(last_modified_at)

        if last_modified_at:
            print(f"  Watermark: {last_modified_at}")
        else:
            print("  No stored watermark. Using the baseline max Modified Date.")
            inferred = max_modified_at(existing_records)
            update_table_state(state, dt, inferred, source_file=os.path.basename(consolidated_path))
            last_modified_at = state.get("tables", {}).get(dt, {}).get("last_modified_at")
            constraints = build_incremental_constraints(last_modified_at)
            if last_modified_at:
                print(f"  Inferred watermark: {last_modified_at}")

        delta_records = fetch_records(
            dt,
            constraints=constraints,
            progress_label=f"{dt} incremental",
        )

        if delta_records is None:
            print("  FAILED — skipping\n")
            had_failures = True
            continue

        if not delta_records:
            print("  No changes since the last watermark.\n")
            continue

        incremental_filename = os.path.join(dated_output_dir, f"{dt}_incremental_{timestamp}.csv")
        delta_headers = collect_all_keys(delta_records, preferred_keys=["_id"])
        write_csv(delta_records, incremental_filename, delta_headers)

        merged_records = merge_records_by_id(existing_records, delta_records)
        merged_headers = collect_all_keys(merged_records, preferred_keys=existing_headers or ["_id"])
        write_csv(merged_records, consolidated_path, merged_headers)

        delta_max_dt = max_modified_at(delta_records)
        previous_max_dt = parse_bubble_datetime(last_modified_at)
        if previous_max_dt and delta_max_dt and previous_max_dt > delta_max_dt:
            delta_max_dt = previous_max_dt
        elif previous_max_dt and not delta_max_dt:
            delta_max_dt = previous_max_dt

        update_table_state(state, dt, delta_max_dt, source_file=os.path.basename(incremental_filename))

        print(f"  Delta saved: {incremental_filename} ({len(delta_records)} changed records)")
        print(f"  Consolidated snapshot updated: {consolidated_path} ({len(merged_records)} total records)\n")

    save_state(state)

    if had_failures:
        print("Incremental backup finished with failures. Check the logs above.")
        sys.exit(1)

    print("Incremental backup complete!")


if __name__ == "__main__":
    main()
