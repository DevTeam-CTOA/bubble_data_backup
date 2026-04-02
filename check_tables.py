#!/usr/bin/env python3
"""Queries the Bubble /meta API and generates schema.csv with all data types (enabled or not)."""

import csv
import json
import os
import re
import requests
import sys
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

API_URL = os.environ["BUBBLE_API_URL"]
API_TOKEN = os.environ["BUBBLE_API_TOKEN"]
API_META = f"{API_URL}/meta"
API_BASE = f"{API_URL}/obj"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}
OUTPUT_DIR = os.path.join(BASE_DIR, "generated_backups")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "schema.csv")
SELECTION_FILE = os.path.join(BASE_DIR, "backup_selected_data-types.csv")


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


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Fetching available data types...")
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

    # Fetch row counts for enabled data types
    print(f"\nFetching row counts for {len(enabled)} enabled data types...")
    row_counts = {}
    for dt in all_types:
        if dt in enabled:
            count = get_row_count(dt)
            row_counts[dt] = count
            status = f"{count:,}" if count is not None else "N/A (not accessible)"
            print(f"  {dt}: {status}")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["data_type", "api_enabled", "row_count"])
        for dt in all_types:
            is_enabled = dt in enabled
            count = row_counts.get(dt, "")
            count_str = str(count) if count is not None else ""
            writer.writerow([dt, "yes" if is_enabled else "no", count_str])

    print(f"\nSchema saved to: {OUTPUT_FILE}")
    print(f"Data types found: {len(all_types)} ({len(enabled)} enabled)\n")
    for dt in all_types:
        status = "Y" if dt in enabled else "N"
        count = row_counts.get(dt)
        if count is not None:
            print(f"  {status}  {dt:30s} ({count:,} rows)")
        else:
            print(f"  {status}  {dt}")

    # Create selection file if it doesn't exist
    if not os.path.exists(SELECTION_FILE):
        with open(SELECTION_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["data_type"])
        print(f"\nCreated empty selection file: {SELECTION_FILE}")
        print("Add the data types you want to back up (one per line).")


if __name__ == "__main__":
    main()
