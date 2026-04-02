#!/usr/bin/env python3
"""Backs up selected data types listed in backup_selected_data-types.csv."""

import csv
import json
import os
import requests
import sys
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

API_URL = os.environ["BUBBLE_API_URL"]
API_TOKEN = os.environ["BUBBLE_API_TOKEN"]
API_BASE = f"{API_URL}/obj"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}
PAGE_SIZE = 100
OUTPUT_DIR = os.path.join(BASE_DIR, "generated_backups")
SELECTION_FILE = os.path.join(BASE_DIR, "backup_selected_data-types.csv")


def make_timestamp():
    """Generates an ISO 8601 timestamp with GMT-3 offset, filesystem-safe."""
    tz = timezone(timedelta(hours=-3))
    now = datetime.now(tz)
    return now.strftime("%Y-%m-%dT%H-%M-%S%z")


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


def load_selected_types():
    """Reads selected data types from the CSV file."""
    try:
        with open(SELECTION_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [row["data_type"].strip() for row in reader if row["data_type"].strip()]
    except FileNotFoundError:
        print(f"File '{SELECTION_FILE}' not found.", file=sys.stderr)
        print("Run check_tables.py first and create the selection file.", file=sys.stderr)
        sys.exit(1)


def main():
    selected = load_selected_types()
    if not selected:
        print("No data types selected.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    timestamp = make_timestamp()
    print(f"Backup started — {len(selected)} table(s) selected")
    print(f"Timestamp: {timestamp}\n")

    for dt in selected:
        print(f"[{dt}]")
        records = fetch_all(dt)

        if records is None:
            print(f"  FAILED — skipping\n")
            continue

        filename = os.path.join(OUTPUT_DIR, f"{dt}_{timestamp}.csv")

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
