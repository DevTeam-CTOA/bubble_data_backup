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

### 1. Discover available data types

```bash
python3 check_tables.py
```

Generates `generated_backups/schema.csv` listing all data types and whether their Data API is enabled.

### 2. Select data types to back up

Edit `backup_selected_data-types.csv`:

```csv
data_type
user
course
lecture
```

Only data types with `api_enabled: yes` in the schema can be backed up.

### 3. Run the backup

```bash
python3 run_backup.py
```

Generates one CSV per data type in `generated_backups/`, named `{data_type}_{timestamp}.csv`.
