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
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}
OUTPUT_DIR = os.path.join(BASE_DIR, "generated_backups")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "schema.csv")
SELECTION_FILE = os.path.join(BASE_DIR, "backup_selected_data-types.csv")


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

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["data_type", "api_enabled"])
        for dt in all_types:
            writer.writerow([dt, "yes" if dt in enabled else "no"])

    print(f"Schema saved to: {OUTPUT_FILE}")
    print(f"Data types found: {len(all_types)} ({len(enabled)} enabled)\n")
    for dt in all_types:
        status = "Y" if dt in enabled else "N"
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
