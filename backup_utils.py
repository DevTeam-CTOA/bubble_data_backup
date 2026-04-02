#!/usr/bin/env python3
"""Shared helpers for Bubble backup scripts."""

import csv
import glob
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
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
CONSOLIDATED_DIR = os.path.join(OUTPUT_DIR, "consolidated")
STATE_FILE = os.path.join(OUTPUT_DIR, "incremental_state.json")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("BUBBLE_REQUEST_TIMEOUT_SECONDS", "60"))
INCREMENTAL_OVERLAP_SECONDS = int(os.getenv("BUBBLE_INCREMENTAL_OVERLAP_SECONDS", "300"))
BUBBLE_MODIFIED_FIELD = "Modified Date"
LOCAL_TZ = timezone(timedelta(hours=-3))


def make_timestamp():
    """Generates an ISO 8601 timestamp with GMT-3 offset, filesystem-safe."""
    now = datetime.now(LOCAL_TZ)
    return now.strftime("%Y-%m-%dT%H-%M-%S%z")


def make_date_folder():
    """Returns the current date folder in local timezone."""
    return datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")


def ensure_output_dirs():
    """Ensures the output directories exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(CONSOLIDATED_DIR, exist_ok=True)


def get_all_data_types():
    """Fetches all available data types from the /meta API."""
    print("Fetching available data types from API...")
    try:
        resp = requests.get(API_META, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.Timeout:
        print(
            f"Timeout while fetching API metadata after {REQUEST_TIMEOUT_SECONDS:.0f}s. Backup aborted.",
            file=sys.stderr,
        )
        sys.exit(1)
    except requests.RequestException as exc:
        print(f"Error fetching API metadata: {exc}", file=sys.stderr)
        sys.exit(1)

    if resp.status_code != 200:
        print(f"API error: {resp.status_code} - {resp.text}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    enabled = set(data.get("get", []))
    raw = json.dumps(data)
    referenced = set(re.findall(r"custom\.(\w+)", raw))
    all_types = sorted(enabled | referenced)
    return all_types, enabled


def get_row_count(data_type):
    """Gets the total row count for a data type with a single API call."""
    url = f"{API_BASE}/{data_type}"
    params = {"limit": 1, "cursor": 0}

    try:
        resp = requests.get(
            url,
            headers=HEADERS,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if resp.status_code != 200:
            return None

        data = resp.json()["response"]
        results = data.get("results", [])
        remaining = data.get("remaining", 0)
        return len(results) + remaining
    except requests.Timeout:
        print(
            f"  Timeout while counting rows for '{data_type}' after {REQUEST_TIMEOUT_SECONDS:.0f}s",
            file=sys.stderr,
        )
        return None
    except requests.RequestException:
        return None


def fetch_records(data_type, constraints=None, progress_label=None):
    """Fetches records for a data type, paginating in batches of 100."""
    url = f"{API_BASE}/{data_type}"
    all_records = []
    cursor = 0
    label = progress_label or data_type

    while True:
        params = {"limit": PAGE_SIZE, "cursor": cursor}
        if constraints:
            params["constraints"] = json.dumps(constraints)

        try:
            resp = requests.get(
                url,
                headers=HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.Timeout:
            print(
                f"  Timeout for '{label}' at cursor {cursor} after {REQUEST_TIMEOUT_SECONDS:.0f}s without response. "
                "Stopping this table and moving on.",
                file=sys.stderr,
            )
            return None
        except requests.RequestException as exc:
            print(f"  Request error for '{label}' at cursor {cursor}: {exc}", file=sys.stderr)
            return None

        if resp.status_code != 200:
            print(f"  API error for '{label}': {resp.status_code} - {resp.text}", file=sys.stderr)
            return None

        data = resp.json()["response"]
        results = data.get("results", [])
        remaining = data.get("remaining", 0)

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


def collect_all_keys(records, preferred_keys=None):
    """Collects CSV headers preserving first-seen order."""
    all_keys = []
    seen = set()

    for key in preferred_keys or []:
        if key not in seen:
            seen.add(key)
            all_keys.append(key)

    for rec in records:
        for key in rec.keys():
            if key not in seen:
                seen.add(key)
                all_keys.append(key)

    return all_keys


def write_csv(records, filename, all_keys=None):
    """Writes records to a CSV file."""
    headers = all_keys or collect_all_keys(records)
    if not headers:
        headers = ["_id"]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(headers)
        for rec in records:
            row = [flatten_value(rec.get(k)) for k in headers]
            writer.writerow(row)

    return len(headers)


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


def parse_bubble_datetime(value):
    """Parses Bubble's datetime strings into timezone-aware datetimes."""
    if not value:
        return None

    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def format_bubble_datetime(dt):
    """Formats a datetime for Bubble API constraints."""
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def max_modified_at(records):
    """Returns the greatest Modified Date found in records."""
    max_dt = None
    for rec in records:
        candidate = parse_bubble_datetime(rec.get(BUBBLE_MODIFIED_FIELD))
        if candidate and (max_dt is None or candidate > max_dt):
            max_dt = candidate
    return max_dt


def get_consolidated_path(table_name):
    """Returns the consolidated CSV path for a table."""
    ensure_output_dirs()
    return os.path.join(CONSOLIDATED_DIR, f"{table_name}.csv")


def load_state():
    """Loads incremental backup state from disk."""
    if not os.path.exists(STATE_FILE):
        return {"tables": {}}

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if "tables" not in data or not isinstance(data["tables"], dict):
        data["tables"] = {}
    return data


def save_state(state):
    """Persists incremental backup state."""
    ensure_output_dirs()
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False, sort_keys=True)


def update_table_state(state, table_name, last_modified_at, source_file=None):
    """Updates incremental state for a single table."""
    table_state = state.setdefault("tables", {}).setdefault(table_name, {})
    table_state["last_modified_at"] = format_bubble_datetime(last_modified_at) if last_modified_at else None
    if source_file:
        table_state["source_file"] = source_file


def read_csv_records(filename):
    """Reads CSV rows into memory preserving header order."""
    with open(filename, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        records = list(reader)
    return headers, records


def latest_full_snapshot_path(table_name):
    """Returns the latest non-incremental snapshot path for a table, if any."""
    pattern = os.path.join(OUTPUT_DIR, "*", f"{table_name}_*.csv")
    candidates = []
    for path in glob.glob(pattern):
        if not os.path.isfile(path):
            continue
        name = os.path.basename(path)
        if name.startswith("schema_"):
            continue
        if "_incremental_" in name:
            continue
        candidates.append(path)

    if not candidates:
        return None
    return sorted(candidates)[-1]


def bootstrap_table_from_snapshot(state, table_name, snapshot_path):
    """Copies an existing full snapshot into the consolidated area and seeds state."""
    consolidated_path = get_consolidated_path(table_name)
    shutil.copyfile(snapshot_path, consolidated_path)
    headers, records = read_csv_records(consolidated_path)
    max_dt = max_modified_at(records)
    update_table_state(state, table_name, max_dt, source_file=os.path.basename(snapshot_path))
    return headers, records, consolidated_path


def seed_table_from_records(state, table_name, records, source_filename):
    """Creates a consolidated baseline from fetched records."""
    consolidated_path = get_consolidated_path(table_name)
    headers = collect_all_keys(records, preferred_keys=["_id"])
    write_csv(records, consolidated_path, headers)
    update_table_state(state, table_name, max_modified_at(records), source_file=os.path.basename(source_filename))
    return headers, records, consolidated_path


def ensure_table_baseline(state, table_name):
    """Returns an existing consolidated baseline, bootstrapping it when possible."""
    consolidated_path = get_consolidated_path(table_name)
    if os.path.exists(consolidated_path):
        headers, records = read_csv_records(consolidated_path)
        table_state = state.setdefault("tables", {}).setdefault(table_name, {})
        if "last_modified_at" not in table_state:
            update_table_state(state, table_name, max_modified_at(records), source_file=os.path.basename(consolidated_path))
        return headers, records, consolidated_path

    snapshot_path = latest_full_snapshot_path(table_name)
    if snapshot_path:
        print(f"  Bootstrapping consolidated baseline from {snapshot_path}")
        return bootstrap_table_from_snapshot(state, table_name, snapshot_path)

    return None, None, consolidated_path


def merge_records_by_id(existing_records, delta_records):
    """Upserts delta records into an existing record list by _id."""
    merged = {}
    ordered_ids = []

    for rec in existing_records:
        record_id = rec.get("_id")
        key = record_id or f"__row__{len(ordered_ids)}"
        if key not in merged:
            ordered_ids.append(key)
        merged[key] = dict(rec)

    for rec in delta_records:
        record_id = rec.get("_id")
        key = record_id or f"__delta__{len(ordered_ids)}"
        if key not in merged:
            ordered_ids.append(key)
            merged[key] = dict(rec)
            continue

        # Treat the delta payload as the newest full representation of the row.
        merged[key] = dict(rec)

    return [merged[key] for key in ordered_ids]


def watermark_with_overlap(last_modified_at):
    """Returns the fetch watermark adjusted with a safety overlap."""
    dt = parse_bubble_datetime(last_modified_at)
    if dt is None:
        return None
    return dt - timedelta(seconds=INCREMENTAL_OVERLAP_SECONDS)
