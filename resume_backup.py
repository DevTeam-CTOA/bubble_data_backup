#!/usr/bin/env python3
"""Resumes and completes incomplete Bubble backups from a specific backup folder."""

import asyncio
import csv
import glob
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


def list_backup_folders():
    """Lists all available backup folders."""
    if not os.path.exists(OUTPUT_DIR):
        return []

    folders = []
    for item in os.listdir(OUTPUT_DIR):
        item_path = os.path.join(OUTPUT_DIR, item)
        if os.path.isdir(item_path) and re.match(r'^\d{4}-\d{2}-\d{2}$', item):
            folders.append(item)

    return sorted(folders, reverse=True)


def get_incomplete_tables(backup_folder):
    """Returns dict of incomplete tables with expected row counts."""
    dated_output_dir = os.path.join(OUTPUT_DIR, backup_folder)

    # Find the most recent schema file in this folder
    schema_pattern = os.path.join(dated_output_dir, "schema_*.csv")
    schema_files = sorted(glob.glob(schema_pattern), reverse=True)

    if not schema_files:
        print(f"No schema file found in {backup_folder}")
        return None

    schema_file = schema_files[0]
    print(f"Using schema: {os.path.basename(schema_file)}")

    # Read expected tables and row counts from schema
    expected = {}
    with open(schema_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['api_enabled'] == 'yes':
                table_name = row['data_type']
                row_count = int(row['row_count']) if row['row_count'] else None
                expected[table_name] = row_count

    # Find all backup CSV files in this folder
    csv_pattern = os.path.join(dated_output_dir, "*.csv")
    all_files = glob.glob(csv_pattern)

    # Count actual rows in each table backup
    actual = {}
    for filepath in all_files:
        filename = os.path.basename(filepath)

        # Skip schema files
        if filename.startswith("schema"):
            continue

        # Extract table name
        match = re.match(r"^(.+?)_\d{4}-\d{2}-\d{2}T.+\.csv$", filename)
        if not match:
            continue

        table_name = match.group(1)

        # Count CSV records, not physical lines, because fields may contain newlines.
        with open(filepath, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            line_count = sum(1 for _ in reader)

        # Keep the file with most records
        if table_name not in actual or line_count > actual[table_name]:
            actual[table_name] = line_count

    # Find incomplete or missing tables
    incomplete = {}
    missing = []

    for table_name, expected_count in expected.items():
        actual_count = actual.get(table_name, -1)

        if actual_count == -1:
            missing.append(table_name)
            incomplete[table_name] = expected_count
        elif expected_count is None:
            # If the original schema could not determine the table size, the safe
            # fallback is to re-export the table during resume.
            incomplete[table_name] = expected_count
        elif actual_count < expected_count:
            incomplete[table_name] = expected_count

    return incomplete, missing, expected, actual


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


def split_tables_by_size(row_counts):
    """Splits tables into large and regular groups using expected row counts."""
    large_tables = []
    regular_tables = []

    for data_type, count in row_counts.items():
        if count >= LARGE_TABLE_ROW_THRESHOLD:
            large_tables.append(data_type)
        else:
            regular_tables.append(data_type)

    large_tables.sort(key=lambda dt: row_counts[dt], reverse=True)
    regular_tables.sort(key=lambda dt: row_counts[dt], reverse=True)
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


async def resume_backup(backup_folder):
    """Resumes backup for incomplete tables in the specified folder."""
    dated_output_dir = os.path.join(OUTPUT_DIR, backup_folder)
    timestamp = make_timestamp()

    # Get incomplete tables
    status = get_incomplete_tables(backup_folder)
    if status is None:
        print("Cannot resume this backup without a schema file.")
        sys.exit(1)

    incomplete, missing, expected, actual = status

    print(f"\n{'='*60}")
    print("BACKUP STATUS")
    print(f"{'='*60}")
    print(f"Expected tables: {len(expected)}")
    print(f"Missing tables: {len(missing)}")
    print(f"Incomplete tables: {len(incomplete) - len(missing)}")
    print(f"Complete tables: {len(expected) - len(incomplete)}")
    print(f"{'='*60}\n")

    if missing:
        print("MISSING TABLES:")
        for table in sorted(missing):
            expected_count = expected[table]
            if expected_count is None:
                print(f"  - {table} (expected row count unknown)")
            else:
                print(f"  - {table} (expected {expected_count:,} rows)")
        print()

    if len(incomplete) > len(missing):
        print("INCOMPLETE TABLES:")
        for table in sorted(incomplete.keys()):
            if table not in missing:
                actual_count = actual.get(table, 0)
                expected_count = incomplete[table]
                if expected_count is None:
                    print(f"  - {table}: {actual_count:,} rows exported, expected row count unknown")
                else:
                    percentage = (actual_count / expected_count * 100) if expected_count > 0 else 0
                    print(f"  - {table}: {actual_count:,} / {expected_count:,} rows ({percentage:.1f}%)")
        print()

    if not incomplete:
        print("All tables are complete!")
        return

    # Backup incomplete tables
    print(f"\nResuming backup — {len(incomplete)} table(s)")
    print(f"Output folder: {dated_output_dir}")
    print(f"Timestamp: {timestamp}")
    print(f"Max concurrent regular tables: {MAX_CONCURRENT_TABLES}")
    print(f"Large table threshold: {LARGE_TABLE_ROW_THRESHOLD:,} rows")
    print(f"Max concurrent large tables: {MAX_CONCURRENT_LARGE_TABLES}\n")

    # Create aiohttp session
    async with aiohttp.ClientSession() as session:
        table_lock = asyncio.Lock()  # Lock for synchronized console output
        large_tables, regular_tables = split_tables_by_size(incomplete)

        print(f"Large tables pending: {len(large_tables)}")
        print(f"Regular tables pending: {len(regular_tables)}")

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
        print("Resume backup complete!")
    else:
        print("Resume backup finished with failures. Check the logs above.")
        sys.exit(1)
    print("=" * 60)


def main():
    # List available backup folders
    folders = list_backup_folders()

    if not folders:
        print("No backup folders found!")
        print(f"Expected format: {OUTPUT_DIR}/YYYY-MM-DD")
        sys.exit(1)

    print("Available backup folders:")
    for i, folder in enumerate(folders, 1):
        print(f"  {i}. {folder}")

    print()

    # Ask user which folder to resume
    while True:
        try:
            choice = input(f"Which backup folder to resume? (1-{len(folders)} or date YYYY-MM-DD): ").strip()

            # Check if it's a number
            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(folders):
                    backup_folder = folders[idx]
                    break
                else:
                    print(f"Invalid choice. Please enter a number between 1 and {len(folders)}")
            # Check if it's a date format
            elif re.match(r'^\d{4}-\d{2}-\d{2}$', choice):
                if choice in folders:
                    backup_folder = choice
                    break
                else:
                    print(f"Backup folder '{choice}' not found!")
            else:
                print("Invalid input. Please enter a number or date in YYYY-MM-DD format")
        except KeyboardInterrupt:
            print("\n\nCancelled by user")
            sys.exit(1)
        except EOFError:
            print("\n\nNo input received")
            sys.exit(1)

    print(f"\nSelected folder: {backup_folder}\n")

    # Run async resume backup
    try:
        asyncio.run(resume_backup(backup_folder))
    except KeyboardInterrupt:
        print("\n\nBackup interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    # Check Python version
    if sys.version_info < (3, 7):
        print("Error: Python 3.7 or higher is required")
        sys.exit(1)

    main()
