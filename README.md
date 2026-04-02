# Bubble Database Backup

CLI tool to back up Bubble.io Data API tables to CSV files.

## Setup

```bash
pip install -r requirements.txt
```

Create a `.env` file:

```
BUBBLE_API_URL=https://platform.cto.academy/version-test/api/1.1
BUBBLE_API_TOKEN=your_token_here
```

## Usage

Simply run:

```bash
python3 run_backup.py
```

This will automatically:

1. **Create a dated folder** in `generated_backups/` (e.g., `generated_backups/2026-04-02/`)
2. **Generate a schema file** with all available data types and row counts (e.g., `schema_2026-04-02T10-30-15-0300.csv`)
3. **Back up all enabled data types** to individual CSV files (e.g., `user_2026-04-02T10-30-15-0300.csv`)

## Output Structure

```
generated_backups/
└── 2026-04-02/
    ├── schema_2026-04-02T10-30-15-0300.csv
    ├── user_2026-04-02T10-30-15-0300.csv
    ├── course_2026-04-02T10-30-15-0300.csv
    └── ...
```

Each backup run creates a new dated folder with timestamped files, making it easy to track and compare backups over time.
