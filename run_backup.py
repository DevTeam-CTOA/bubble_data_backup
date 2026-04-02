#!/usr/bin/env python3
"""Backs up all Bubble data types to CSV files."""

import csv
import json
import os
import re
import requests
import sys
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

API_URL = os.environ["BUBBLE_API_URL"]
API_TOKEN = os.environ["BUBBLE_API_TOKEN"]
API_META = f"{API_URL}/meta"
API_BASE = f"{API_URL}/obj"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}
PAGE_SIZE = 100
OUTPUT_DIR = os.path.join(BASE_DIR, "generated_backups")


def make_timestamp():
    """Generates an ISO 8601 timestamp with GMT-3 offset, filesystem-safe."""
    tz = timezone(timedelta(hours=-3))
    now = datetime.now(tz)
    return now.strftime("%Y-%m-%dT%H-%M-%S%z")


def get_all_data_types():
    """Fetches all available data types from the /meta API."""
    print("Fetching available data types from API...")
    resp = requests.get(API_META, headers=HEADERS)

    if resp.status_code != 200:
        print(f"API error: {resp.status_code} - {resp.text}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()

    # Data types with Data API enabled ("get" field)
    enabled = set(data.get("get", []))

    # Data types referenced in workflows (custom.typename)
    raw = json.dumps(data)
    referenced = set(re.findall(r"custom\.(\w+)", raw))

    # Combine all
    all_types = sorted(enabled | referenced)

    return all_types, enabled


def get_row_count(data_type):
    """Gets the total row count for a data type with a single API call."""
    url = f"{API_BASE}/{data_type}"
    params = {"limit": 1, "cursor": 0}

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
        if resp.status_code != 200:
            # 401 = not accessible (privacy rules), just skip silently
            return None

        data = resp.json()["response"]
        results = data.get("results", [])
        remaining = data.get("remaining", 0)

        return len(results) + remaining
    except Exception:
        return None


def fetch_all(data_type):
    """Fetches all records for a data type, paginating in batches of 100."""
    url = f"{API_BASE}/{data_type}"
    all_records = []
    cursor = 0

    while True:
        params = {"limit": PAGE_SIZE, "cursor": cursor}
        resp = requests.get(url, headers=HEADERS, params=params)

        if resp.status_code != 200:
            print(f"  API error for '{data_type}': {resp.status_code} - {resp.text}", file=sys.stderr)
            return None

        data = resp.json()["response"]
        results = data["results"]
        remaining = data["remaining"]

        all_records.extend(results)
        print(f"  {len(all_records)} records (remaining: {remaining})")

        if remaining == 0:
            break

        cursor += PAGE_SIZE
        time.sleep(0.5)

    return all_records


def flatten_value(val):
    """Converts a value to a CSV-friendly string."""
    if val is None:
        return ""
    if isinstance(val, bool):
        return str(val).lower()
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)


def write_csv(records, filename):
    """Writes records to a CSV file."""
    all_keys = []
    seen = set()
    for rec in records:
        for key in rec.keys():
            if key not in seen:
                seen.add(key)
                all_keys.append(key)

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(all_keys)
        for rec in records:
            row = [flatten_value(rec.get(k)) for k in all_keys]
            writer.writerow(row)

    return len(all_keys)


def write_schema(all_types, enabled, row_counts, filename):
    """Writes the schema CSV file."""
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["data_type", "api_enabled", "row_count"])
        for dt in all_types:
            is_enabled = dt in enabled
            count = row_counts.get(dt, "")
            count_str = str(count) if count is not None else ""
            writer.writerow([dt, "yes" if is_enabled else "no", count_str])


def main():
    # Create date-specific subfolder
    tz = timezone(timedelta(hours=-3))
    date_folder = datetime.now(tz).strftime("%Y-%m-%d")
    dated_output_dir = os.path.join(OUTPUT_DIR, date_folder)
    os.makedirs(dated_output_dir, exist_ok=True)

    timestamp = make_timestamp()

    # Get all data types
    all_types, enabled = get_all_data_types()
    print(f"Found {len(all_types)} data types ({len(enabled)} enabled)\n")

    # Fetch row counts for enabled data types
    print("Fetching row counts...")
    row_counts = {}
    for dt in all_types:
        if dt in enabled:
            count = get_row_count(dt)
            row_counts[dt] = count
            status = f"{count:,}" if count is not None else "N/A (not accessible)"
            print(f"  {dt}: {status}")

    # Write schema file
    schema_filename = os.path.join(dated_output_dir, f"schema_{timestamp}.csv")
    write_schema(all_types, enabled, row_counts, schema_filename)
    print(f"\nSchema saved: {schema_filename}")

    # Backup all enabled data types
    print(f"\nBackup started — {len(enabled)} table(s)")
    print(f"Output folder: {dated_output_dir}")
    print(f"Timestamp: {timestamp}\n")

    for dt in enabled:
        print(f"[{dt}]")
        records = fetch_all(dt)

        if records is None:
            print(f"  FAILED — skipping\n")
            continue

        filename = os.path.join(dated_output_dir, f"{dt}_{timestamp}.csv")

        if not records:
            # Create empty CSV with just the header row
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                writer.writerow(["_id"])  # Minimal header for empty tables
            print(f"  Empty — saved: {filename} (0 records)\n")
        else:
            num_cols = write_csv(records, filename)
            print(f"  Saved: {filename} ({len(records)} records, {num_cols} columns)\n")

    print("Backup complete!")


if __name__ == "__main__":
    main()
