#!/usr/bin/env python3
"""Shared helpers for Bubble backup scripts."""

import csv
import json
import os
import re
import sys
import tempfile
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
REQUEST_TIMEOUT_SECONDS = float(os.getenv("BUBBLE_REQUEST_TIMEOUT_SECONDS", "60"))
INCREMENTAL_OVERLAP_SECONDS = int(os.getenv("BUBBLE_INCREMENTAL_OVERLAP_SECONDS", "300"))
MAX_CONCURRENT_TABLES = int(os.getenv("MAX_CONCURRENT_TABLES", "10"))
LARGE_TABLE_ROW_THRESHOLD = int(os.getenv("LARGE_TABLE_ROW_THRESHOLD", "100000"))
MAX_CONCURRENT_LARGE_TABLES = int(os.getenv("MAX_CONCURRENT_LARGE_TABLES", "2"))
LARGE_TABLE_NAMES = {
    name.strip()
    for name in os.getenv("LARGE_TABLE_NAMES", "").split(",")
    if name.strip()
}
REQUEST_MAX_RETRIES = int(os.getenv("BUBBLE_REQUEST_MAX_RETRIES", "3"))
REQUEST_RETRY_BACKOFF_SECONDS = float(os.getenv("BUBBLE_REQUEST_RETRY_BACKOFF_SECONDS", "2"))
BUBBLE_CREATED_FIELD = "Created Date"
BUBBLE_MODIFIED_FIELD = "Modified Date"
MAX_BUBBLE_GET_ITEMS = int(os.getenv("MAX_BUBBLE_GET_ITEMS", "50000"))
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


def _request_with_retries(url, *, params=None, label, retry_on_statuses=None):
    """Performs a GET request with retry/backoff for transient failures."""
    retry_on_statuses = retry_on_statuses or {429, 500, 502, 503, 504}

    for attempt in range(REQUEST_MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                headers=HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.Timeout:
            resp = None
            error_message = (
                f"Timeout for '{label}' after {REQUEST_TIMEOUT_SECONDS:.0f}s"
            )
        except requests.RequestException as exc:
            resp = None
            error_message = f"Request error for '{label}': {exc}"
        else:
            if resp.status_code == 200:
                return resp

            if resp.status_code not in retry_on_statuses:
                return resp

            error_message = f"Transient API error for '{label}': {resp.status_code}"

        if attempt >= REQUEST_MAX_RETRIES:
            if resp is not None:
                print(f"  API error for '{label}': {resp.status_code} - {resp.text}", file=sys.stderr)
            else:
                print(f"  {error_message}", file=sys.stderr)
            return None

        backoff = REQUEST_RETRY_BACKOFF_SECONDS * (2 ** attempt)
        print(
            f"  {error_message}. Retrying in {backoff:.1f}s "
            f"({attempt + 1}/{REQUEST_MAX_RETRIES})...",
            file=sys.stderr,
        )
        time.sleep(backoff)


def _atomic_csv_write(filename, headers, rows):
    """Writes a CSV file atomically to avoid partial/corrupted outputs."""
    directory = os.path.dirname(filename) or "."
    os.makedirs(directory, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=".tmp-", suffix=".csv", dir=directory)

    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)
        os.replace(temp_path, filename)
    except Exception:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass
        raise


def get_all_data_types():
    """Fetches all available data types from the /meta API."""
    print("Fetching available data types from API...")
    resp = _request_with_retries(API_META, label="API metadata")
    if resp is None or resp.status_code != 200:
        if resp is not None:
            print(f"API error: {resp.status_code} - {resp.text}", file=sys.stderr)
        else:
            print("Unable to fetch API metadata. Backup aborted.", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    enabled = set(data.get("get", []))
    raw = json.dumps(data)
    referenced = set(re.findall(r"custom\.(\w+)", raw))
    all_types = sorted(enabled | referenced)
    return all_types, enabled


def _build_query_params(
    *,
    limit,
    cursor=0,
    constraints=None,
    sort_field=None,
    descending=None,
):
    """Builds Bubble Data API query parameters for list requests."""
    params = {"limit": limit, "cursor": cursor}
    if constraints:
        params["constraints"] = json.dumps(constraints)
    if sort_field:
        params["sort_field"] = sort_field
    if descending is not None:
        params["descending"] = str(bool(descending)).lower()
    return params


def _fetch_response(
    data_type,
    *,
    constraints=None,
    cursor=0,
    limit=PAGE_SIZE,
    progress_label=None,
    sort_field=None,
    descending=None,
):
    """Fetches one Bubble Data API page and returns the response payload."""
    url = f"{API_BASE}/{data_type}"
    label = progress_label or data_type
    params = _build_query_params(
        limit=limit,
        cursor=cursor,
        constraints=constraints,
        sort_field=sort_field,
        descending=descending,
    )

    resp = _request_with_retries(url, params=params, label=f"{label} at cursor {cursor}")
    if resp is None:
        return None
    if resp.status_code != 200:
        print(f"  API error for '{label}': {resp.status_code} - {resp.text}", file=sys.stderr)
        return None

    return resp.json()["response"]


def get_record_count(data_type, constraints=None, progress_label=None):
    """Gets the total row count for a data type or constrained search."""
    label = progress_label or data_type
    data = _fetch_response(
        data_type,
        constraints=constraints,
        cursor=0,
        limit=1,
        progress_label=f"{label} row count",
    )
    if data is None:
        return None

    results = data.get("results", [])
    remaining = data.get("remaining", 0)
    return len(results) + remaining


def get_row_count(data_type):
    """Gets the total row count for a data type with a single API call."""
    return get_record_count(data_type)


def fetch_records(
    data_type,
    constraints=None,
    progress_label=None,
    sort_field=None,
    descending=None,
):
    """Fetches records for a data type, paginating in batches of 100."""
    all_records = []
    cursor = 0
    label = progress_label or data_type

    while True:
        data = _fetch_response(
            data_type,
            constraints=constraints,
            cursor=cursor,
            limit=PAGE_SIZE,
            progress_label=label,
            sort_field=sort_field,
            descending=descending,
        )
        if data is None:
            return None

        results = data.get("results", [])
        remaining = data.get("remaining", 0)

        all_records.extend(results)
        print(f"  [{label}] {len(all_records)} records (remaining: {remaining})")

        if remaining == 0:
            break

        cursor += PAGE_SIZE
        time.sleep(0.5)

    return all_records


def _combine_constraints(*constraint_groups):
    """Combines multiple Bubble constraint lists into one list."""
    combined = []
    for group in constraint_groups:
        if group:
            combined.extend(group)
    return combined or None


def _make_date_constraint(field_name, constraint_type, dt):
    """Builds a single Bubble date constraint."""
    return {
        "key": field_name,
        "constraint_type": constraint_type,
        "value": format_bubble_datetime(dt),
    }


def _fetch_boundary_datetime(data_type, *, constraints=None, descending=False, progress_label=None):
    """Fetches the earliest or latest datetime for a constrained search."""
    label = progress_label or data_type
    data = _fetch_response(
        data_type,
        constraints=constraints,
        cursor=0,
        limit=1,
        progress_label=f"{label} boundary",
        sort_field=BUBBLE_CREATED_FIELD,
        descending=descending,
    )
    if data is None:
        return None
    records = data.get("results", [])
    if not records:
        return None
    return parse_bubble_datetime(records[0].get(BUBBLE_CREATED_FIELD))


def _fetch_partitioned_by_created_date(
    data_type,
    *,
    constraints=None,
    total_count=None,
    progress_label=None,
):
    """Fetches a full table by recursively splitting it on Created Date."""
    label = progress_label or data_type

    if total_count is None:
        total_count = get_record_count(
            data_type,
            constraints=constraints,
            progress_label=label,
        )
        if total_count is None:
            return None

    if total_count == 0:
        print(f"  [{label}] 0 records (remaining: 0)")
        return []

    if total_count <= MAX_BUBBLE_GET_ITEMS:
        return fetch_records(
            data_type,
            constraints=constraints,
            progress_label=label,
            sort_field=BUBBLE_CREATED_FIELD,
            descending=False,
        )

    print(
        f"  [{label}] {total_count} records exceed Bubble's {MAX_BUBBLE_GET_ITEMS:,} "
        f"item GET limit; splitting by {BUBBLE_CREATED_FIELD}..."
    )

    oldest_dt = _fetch_boundary_datetime(
        data_type,
        constraints=constraints,
        descending=False,
        progress_label=label,
    )
    newest_dt = _fetch_boundary_datetime(
        data_type,
        constraints=constraints,
        descending=True,
        progress_label=label,
    )

    if oldest_dt is None or newest_dt is None:
        print(
            f"  Unable to determine {BUBBLE_CREATED_FIELD} boundaries for '{label}'.",
            file=sys.stderr,
        )
        return None

    if oldest_dt == newest_dt:
        print(
            f"  [{label}] Cannot split further: all {total_count} records share the same "
            f"{BUBBLE_CREATED_FIELD} value {format_bubble_datetime(oldest_dt)}.",
            file=sys.stderr,
        )
        return None

    split_dt = oldest_dt + ((newest_dt - oldest_dt) / 2)
    split_label = format_bubble_datetime(split_dt)

    left_constraints = _combine_constraints(
        constraints,
        [_make_date_constraint(BUBBLE_CREATED_FIELD, "less than", split_dt)],
    )
    equal_constraints = _combine_constraints(
        constraints,
        [_make_date_constraint(BUBBLE_CREATED_FIELD, "equals", split_dt)],
    )
    right_constraints = _combine_constraints(
        constraints,
        [_make_date_constraint(BUBBLE_CREATED_FIELD, "greater than", split_dt)],
    )

    left_count = get_record_count(
        data_type,
        constraints=left_constraints,
        progress_label=f"{label} < {split_label}",
    )
    equal_count = get_record_count(
        data_type,
        constraints=equal_constraints,
        progress_label=f"{label} = {split_label}",
    )
    right_count = get_record_count(
        data_type,
        constraints=right_constraints,
        progress_label=f"{label} > {split_label}",
    )

    if left_count is None or equal_count is None or right_count is None:
        return None

    if left_count + equal_count + right_count != total_count:
        print(
            f"  [{label}] Partition count mismatch around {split_label}: "
            f"expected {total_count}, got {left_count + equal_count + right_count}.",
            file=sys.stderr,
        )
        return None

    if equal_count > MAX_BUBBLE_GET_ITEMS:
        print(
            f"  [{label}] Cannot split further at {split_label}: {equal_count} records "
            f"share the same {BUBBLE_CREATED_FIELD}.",
            file=sys.stderr,
        )
        return None

    print(
        f"  [{label}] Split at {split_label}: "
        f"left={left_count}, equal={equal_count}, right={right_count}"
    )

    all_records = []
    for branch_label, branch_constraints, branch_count, is_equality_branch in (
        (f"{label} < {split_label}", left_constraints, left_count, False),
        (f"{label} = {split_label}", equal_constraints, equal_count, True),
        (f"{label} > {split_label}", right_constraints, right_count, False),
    ):
        if branch_count == 0:
            continue

        if is_equality_branch:
            branch_records = fetch_records(
                data_type,
                constraints=branch_constraints,
                progress_label=branch_label,
                sort_field=BUBBLE_CREATED_FIELD,
                descending=False,
            )
        else:
            branch_records = _fetch_partitioned_by_created_date(
                data_type,
                constraints=branch_constraints,
                total_count=branch_count,
                progress_label=branch_label,
            )

        if branch_records is None:
            return None
        all_records.extend(branch_records)

    return all_records


def fetch_complete_records(data_type, row_count=None, progress_label=None):
    """Fetches a full table, splitting large datasets by Created Date when needed."""
    label = progress_label or data_type
    total_count = row_count

    if total_count is None:
        total_count = get_row_count(data_type)
        if total_count is None:
            return fetch_records(data_type, progress_label=label)

    if total_count == 0:
        print(f"  [{label}] 0 records (remaining: 0)")
        return []

    if total_count <= MAX_BUBBLE_GET_ITEMS:
        return fetch_records(data_type, progress_label=label)

    print(
        f"  [{label}] Using partitioned full fetch by {BUBBLE_CREATED_FIELD} "
        f"for {total_count:,} records."
    )
    return _fetch_partitioned_by_created_date(
        data_type,
        total_count=total_count,
        progress_label=label,
    )


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

    rows = ([flatten_value(rec.get(k)) for k in headers] for rec in records)
    _atomic_csv_write(filename, headers, rows)

    return len(headers)


def write_schema(all_types, enabled, row_counts, filename):
    """Writes the schema CSV file."""
    rows = []
    for dt in all_types:
        is_enabled = dt in enabled
        count = row_counts.get(dt, "")
        count_str = str(count) if count is not None else ""
        rows.append([dt, "yes" if is_enabled else "no", count_str])
    _atomic_csv_write(filename, ["data_type", "api_enabled", "row_count"], rows)


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


def is_large_table(table_name, row_count):
    """Returns whether a table should use the heavy-table concurrency pool."""
    if table_name in LARGE_TABLE_NAMES:
        return True
    if row_count is None:
        return False
    return row_count >= LARGE_TABLE_ROW_THRESHOLD


def read_csv_records(filename):
    """Reads CSV rows into memory preserving header order."""
    with open(filename, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        records = list(reader)
    return headers, records


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
    if last_modified_at is None:
        return None
    if isinstance(last_modified_at, str):
        last_modified_at = parse_bubble_datetime(last_modified_at)
    if last_modified_at is None:
        return None
    return last_modified_at - timedelta(seconds=INCREMENTAL_OVERLAP_SECONDS)
