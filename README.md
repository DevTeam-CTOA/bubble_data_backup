# Bubble Database Backup

CLI tool to back up Bubble.io Data API tables to CSV files with concurrent processing.

## Features

- **Multitasking**: Backs up multiple tables concurrently
- **Large-table prioritization**: Big tables run first with lower concurrency to reduce RAM spikes
- **Resume capability**: Can resume incomplete backups from a specific date
- **Automatic pagination**: Handles Bubble's 100 record limit per request
- **Date-based organization**: Each backup creates a dated folder

## Setup

```bash
pip3 install aiohttp python-dotenv
```

Create a `.env` file:

```
BUBBLE_API_URL=https://platform.cto.academy/version-live/api/1.1
BUBBLE_API_TOKEN=your_token_here
```

## Usage

### Full Backup (from scratch)

Run a complete backup that creates a new dated folder:

```bash
python3 run_backup.py
```

This will:
1. Create a dated folder in `generated_backups/` (e.g., `generated_backups/2026-04-02/`)
2. Generate a schema file with all data types and row counts
3. Back up large tables first with lower concurrency, then the remaining tables

### Resume Incomplete Backup

Resume a previous backup that was interrupted or had incomplete tables:

```bash
python3 resume_backup.py
```

This will:
1. Show you available backup folders
2. Ask which folder to resume (you can enter the number or date like `2026-04-02`)
3. Analyze what's missing or incomplete
4. Complete only the missing/incomplete tables concurrently

## Output Structure

```
generated_backups/
└── 2026-04-02/
    ├── schema_2026-04-02T10-30-15-0300.csv
    ├── user_2026-04-02T10-30-15-0300.csv
    ├── course_2026-04-02T10-30-15-0300.csv
    └── ...
```

## Configuration

You can adjust concurrency with environment variables:

```bash
MAX_CONCURRENT_TABLES=5
MAX_CONCURRENT_LARGE_TABLES=2
LARGE_TABLE_ROW_THRESHOLD=100000
```
