# Bubble Database Backup

CLI tool to back up Bubble.io Data API tables to CSV files.

## Features

- **Full backup**: Creates timestamped full snapshots for all enabled Bubble tables
- **Incremental backup**: Fetches only rows changed since the last watermark using `Modified Date`
- **Consolidated snapshots**: Maintains the latest merged CSV per table using `_id` upserts
- **Date-based organization**: Each backup creates a dated folder

## Setup

```bash
pip3 install -r requirements.txt
```

Create a `.env` file:

```bash
BUBBLE_API_URL=https://platform.cto.academy/version-live/api/1.1
BUBBLE_API_TOKEN=your_token_here
BUBBLE_REQUEST_TIMEOUT_SECONDS=60
BUBBLE_INCREMENTAL_OVERLAP_SECONDS=300
MAX_CONCURRENT_TABLES=10
LARGE_TABLE_ROW_THRESHOLD=100000
MAX_CONCURRENT_LARGE_TABLES=2
LARGE_TABLE_NAMES=answer,lecturestat,tanswer,tchoice,cards_userlog,error
```

## Usage

### Full Backup

```bash
python3 full_backup.py
```

The full backup:

1. Creates a dated folder in `generated_backups/`
2. Generates a schema file with all available data types and row counts
3. Exports enabled tables in parallel, using one concurrency limit for regular tables and another for large tables
4. Refreshes the consolidated baseline in `generated_backups/consolidated/`

### Incremental Backup

```bash
python3 recurrent_backup.py
```

The recurrent backup:

1. Reads the current consolidated baseline from `generated_backups/consolidated/<table>.csv`
2. Fetches only rows whose `Modified Date` is newer than the last watermark, with a safety overlap
3. Saves those changes as delta CSVs
4. Consolidates the deltas into `generated_backups/consolidated/<table>.csv` using `_id` upserts

`recurrent_backup.py` assumes `full_backup.py` has already been run at least once. If a consolidated baseline is missing, it stops that table and asks you to run the full backup first.

## Output Structure

```text
generated_backups/
├── 2026-04-02/
│   ├── schema_2026-04-02T10-30-15-0300.csv
│   ├── schema_incremental_2026-04-02T18-00-00-0300.csv
│   ├── user_2026-04-02T10-30-15-0300.csv
│   ├── user_incremental_2026-04-02T18-00-00-0300.csv
│   ├── course_2026-04-02T10-30-15-0300.csv
│   └── ...
├── consolidated/
│   ├── user.csv
│   └── course.csv
```

## Notes

- `full_backup.py` classifies a table as "large" when its `row_count` is at or above `LARGE_TABLE_ROW_THRESHOLD`, or when its name is explicitly listed in `LARGE_TABLE_NAMES`.
- The default full-backup parallelism is `10` regular tables plus `2` large tables. Tune `MAX_CONCURRENT_TABLES` and `MAX_CONCURRENT_LARGE_TABLES` based on Bubble API stability and local I/O pressure.
- Incremental consolidation uses `_id` upserts, not simple append, so updated Bubble rows overwrite prior versions.
- Deleted rows are not detected by the incremental flow alone. Run `full_backup.py` periodically to reconcile deletions and refresh the baseline.
- There is no resume flow anymore. If a run fails, just run `full_backup.py` or `recurrent_backup.py` again from the start.
