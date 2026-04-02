#!/usr/bin/env python3
"""Backs up all Bubble data types to CSV files with concurrent processing."""

import asyncio
import csv
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

try:
    import aiohttp
except ImportError:
    print("Error: aiohttp is required. Install it with: pip3 install aiohttp")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

API_URL = os.environ["BUBBLE_API_URL"]
API_TOKEN = os.environ["BUBBLE_API_TOKEN"]
API_META = f"{API_URL}/meta"
API_BASE = f"{API_URL}/obj"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}
PAGE_SIZE = 100  # Bubble API max limit per request
OUTPUT_DIR = os.path.join(BASE_DIR, "generated_backups")
MAX_CONCURRENT_TABLES = int(os.getenv("MAX_CONCURRENT_TABLES", "5"))
LARGE_TABLE_ROW_THRESHOLD = int(os.getenv("LARGE_TABLE_ROW_THRESHOLD", "100000"))
MAX_CONCURRENT_LARGE_TABLES = int(os.getenv("MAX_CONCURRENT_LARGE_TABLES", "2"))


def make_timestamp():
    """Generates an ISO 8601 timestamp with GMT-3 offset, filesystem-safe."""
    tz = timezone(timedelta(hours=-3))
    now = datetime.now(tz)
    return now.strftime("%Y-%m-%dT%H-%M-%S%z")


async def get_all_data_types(session):
    """Fetches all available data types from the /meta API."""
    print("Fetching available data types from API...")

    try:
        async with session.get(API_META, headers=HEADERS, timeout=30) as resp:
            if resp.status != 200:
                text = await resp.text()
                print(f"API error: {resp.status} - {text}", file=sys.stderr)
                sys.exit(1)

            data = await resp.json()

            # Data types with Data API enabled ("get" field)
            enabled = set(data.get("get", []))

            # Data types referenced in workflows (custom.typename)
            raw = json.dumps(data)
            referenced = set(re.findall(r"custom\.(\w+)", raw))

            # Combine all
            all_types = sorted(enabled | referenced)

            return all_types, enabled
    except Exception as e:
        print(f"Error fetching data types: {e}", file=sys.stderr)
        sys.exit(1)


async def get_row_count(session, data_type):
    """Gets the total row count for a data type with a single API call."""
    url = f"{API_BASE}/{data_type}"
    params = {"limit": 1, "cursor": 0}

    try:
        async with session.get(url, headers=HEADERS, params=params, timeout=10) as resp:
            if resp.status != 200:
                # 401 = not accessible (privacy rules), just skip silently
                return None

            data = await resp.json()
            response_data = data["response"]
            results = response_data.get("results", [])
            remaining = response_data.get("remaining", 0)

            return len(results) + remaining
    except Exception:
        return None


async def fetch_to_jsonl(session, data_type, table_lock, temp_path):
    """Fetches all records and spools them to a JSONL temp file."""
    url = f"{API_BASE}/{data_type}"
    cursor = 0
    total_records = 0
    all_keys = []
    seen_keys = set()

    with open(temp_path, "w", encoding="utf-8") as temp_file:
        while True:
            params = {"limit": PAGE_SIZE, "cursor": cursor}

            try:
                async with session.get(url, headers=HEADERS, params=params, timeout=60) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        async with table_lock:
                            print(f"  API error for '{data_type}': {resp.status} - {text}", file=sys.stderr)
                        return None

                    data = await resp.json()
                    response_data = data["response"]
                    results = response_data["results"]
                    remaining = response_data["remaining"]

                    for rec in results:
                        for key in rec.keys():
                            if key not in seen_keys:
                                seen_keys.add(key)
                                all_keys.append(key)
                        temp_file.write(json.dumps(rec, ensure_ascii=False) + "\n")

                    total_records += len(results)
                    async with table_lock:
                        print(f"  [{data_type}] {total_records} records (remaining: {remaining})")

                    if remaining == 0:
                        break

                    cursor += PAGE_SIZE
                    await asyncio.sleep(0.5)  # Rate limiting
            except Exception as e:
                async with table_lock:
                    print(f"  Error fetching '{data_type}': {e}", file=sys.stderr)
                return None

    return total_records, all_keys


def flatten_value(val):
    """Converts a value to a CSV-friendly string."""
    if val is None:
        return ""
    if isinstance(val, bool):
        return str(val).lower()
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)


def write_csv_from_jsonl(jsonl_path, all_keys, filename):
    """Writes a CSV file from a JSONL temp file."""
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(all_keys)
        with open(jsonl_path, "r", encoding="utf-8") as jsonl_file:
            for line in jsonl_file:
                rec = json.loads(line)
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


def split_tables_by_size(tables, row_counts):
    """Splits tables into large and regular groups using known row counts."""
    large_tables = []
    regular_tables = []

    for data_type in tables:
        count = row_counts.get(data_type)
        if count is not None and count >= LARGE_TABLE_ROW_THRESHOLD:
            large_tables.append(data_type)
        else:
            regular_tables.append(data_type)

    large_tables.sort(key=lambda dt: row_counts.get(dt, 0), reverse=True)
    regular_tables.sort(key=lambda dt: row_counts.get(dt, 0), reverse=True)
    return large_tables, regular_tables


async def backup_table(session, data_type, dated_output_dir, timestamp, semaphore, table_lock):
    """Backs up a single table with semaphore control."""
    async with semaphore:
        filename = os.path.join(dated_output_dir, f"{data_type}_{timestamp}.csv")
        temp_file = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".jsonl",
            prefix=f"{data_type}_",
            dir=dated_output_dir,
            delete=False,
            encoding="utf-8",
        )
        temp_path = temp_file.name
        temp_file.close()

        async with table_lock:
            print(f"\n[{data_type}] Starting backup...")

        try:
            fetch_result = await fetch_to_jsonl(session, data_type, table_lock, temp_path)

            if fetch_result is None:
                async with table_lock:
                    print(f"[{data_type}] FAILED — skipping")
                return False

            total_records, all_keys = fetch_result

            if total_records == 0:
                # Create empty CSV with just the header row
                with open(filename, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                    writer.writerow(["_id"])  # Minimal header for empty tables
                async with table_lock:
                    print(f"[{data_type}] Empty — saved: {filename} (0 records)")
            else:
                num_cols = write_csv_from_jsonl(temp_path, all_keys, filename)
                async with table_lock:
                    print(f"[{data_type}] Saved: {filename} ({total_records} records, {num_cols} columns)")

            return True
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)


async def backup_group(session, tables, concurrency, dated_output_dir, timestamp, table_lock, label):
    """Backs up a group of tables with its own concurrency limit."""
    if not tables:
        return []

    print(f"\n{label} — {len(tables)} table(s), concurrency: {concurrency}")
    semaphore = asyncio.Semaphore(concurrency)
    tasks = [
        backup_table(session, dt, dated_output_dir, timestamp, semaphore, table_lock)
        for dt in tables
    ]
    return await asyncio.gather(*tasks)


async def main():
    # Create date-specific subfolder
    tz = timezone(timedelta(hours=-3))
    date_folder = datetime.now(tz).strftime("%Y-%m-%d")
    dated_output_dir = os.path.join(OUTPUT_DIR, date_folder)
    os.makedirs(dated_output_dir, exist_ok=True)

    timestamp = make_timestamp()

    # Create aiohttp session
    async with aiohttp.ClientSession() as session:
        # Get all data types
        all_types, enabled = await get_all_data_types(session)
        print(f"Found {len(all_types)} data types ({len(enabled)} enabled)\n")

        # Fetch row counts for enabled data types
        print("Fetching row counts...")
        row_counts = {}

        tasks = []
        for dt in all_types:
            if dt in enabled:
                tasks.append(get_row_count(session, dt))
            else:
                tasks.append(asyncio.sleep(0))  # Placeholder for non-enabled types

        results = await asyncio.gather(*tasks)

        for i, dt in enumerate(all_types):
            if dt in enabled:
                count = results[i]
                row_counts[dt] = count
                status = f"{count:,}" if count is not None else "N/A (not accessible)"
                print(f"  {dt}: {status}")

        # Write schema file
        schema_filename = os.path.join(dated_output_dir, f"schema_{timestamp}.csv")
        write_schema(all_types, enabled, row_counts, schema_filename)
        print(f"\nSchema saved: {schema_filename}")

        # Backup all enabled data types concurrently
        print(f"\nBackup started — {len(enabled)} table(s)")
        print(f"Output folder: {dated_output_dir}")
        print(f"Timestamp: {timestamp}")
        print(f"Max concurrent regular tables: {MAX_CONCURRENT_TABLES}")
        print(f"Large table threshold: {LARGE_TABLE_ROW_THRESHOLD:,} rows")
        print(f"Max concurrent large tables: {MAX_CONCURRENT_LARGE_TABLES}\n")

        table_lock = asyncio.Lock()  # Lock for synchronized console output
        enabled_tables = sorted(enabled)
        large_tables, regular_tables = split_tables_by_size(enabled_tables, row_counts)

        print(f"Large tables: {len(large_tables)}")
        print(f"Regular tables: {len(regular_tables)}")

        results = []
        results.extend(
            await backup_group(
                session,
                large_tables,
                MAX_CONCURRENT_LARGE_TABLES,
                dated_output_dir,
                timestamp,
                table_lock,
                "Phase 1: large tables first",
            )
        )
        results.extend(
            await backup_group(
                session,
                regular_tables,
                MAX_CONCURRENT_TABLES,
                dated_output_dir,
                timestamp,
                table_lock,
                "Phase 2: regular tables",
            )
        )

    print("\n" + "=" * 60)
    if all(results):
        print("Backup complete!")
    else:
        print("Backup finished with failures. Check the logs above.")
        sys.exit(1)
    print("=" * 60)


if __name__ == "__main__":
    # Check Python version
    if sys.version_info < (3, 7):
        print("Error: Python 3.7 or higher is required")
        sys.exit(1)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nBackup interrupted by user")
        sys.exit(1)
