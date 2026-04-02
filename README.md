# Bubble Database Backup

CLI tool to back up Bubble.io Data API tables to CSV files.

## Features

- **Full backup**: Creates timestamped full snapshots for all enabled Bubble tables
- **Incremental backup**: Fetches only rows changed since the last watermark using `Modified Date`
- **Consolidated snapshots**: Maintains the latest merged CSV per table using `_id` upserts
- **Resume capability**: Can resume incomplete full backups from a specific date
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
```

## Usage

### Full Backup

```bash
python3 full_backup.py
```

`run_backup.py` remains as a compatibility alias for `full_backup.py`.

The full backup:

1. Creates a dated folder in `generated_backups/`
2. Generates a schema file with all available data types and row counts
3. Exports each enabled table to a timestamped CSV snapshot
4. Refreshes the consolidated baseline in `generated_backups/consolidated/`

### Incremental Backup

```bash
python3 recurrent_backup.py
```

The recurrent backup:

1. Reads the last saved watermark for each table from `generated_backups/incremental_state.json`
2. Fetches only rows whose `Modified Date` is newer than the last watermark, with a safety overlap
3. Saves those changes as delta CSVs
4. Consolidates the deltas into `generated_backups/consolidated/<table>.csv` using `_id` upserts

If no consolidated baseline exists for a table yet, `recurrent_backup.py` seeds that table with a one-time full export automatically.

### Resume Incomplete Full Backup

```bash
python3 resume_backup.py
```

This flow inspects an existing dated backup folder and completes only the missing or incomplete full-table exports.

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
└── incremental_state.json
```

## Notes

- Incremental consolidation uses `_id` upserts, not simple append, so updated Bubble rows overwrite prior versions.
- Deleted rows are not detected by the incremental flow alone. Run `full_backup.py` periodically to reconcile deletions and refresh the baseline.
- `resume_backup.py` remains focused on completing interrupted full backups.
