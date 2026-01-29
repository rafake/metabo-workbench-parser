# Metaloader - Metabolomics Data Loader

Open-source tool for loading metabolomics data (Metabolomics Workbench / MetaboAnalyst exports) into PostgreSQL.

## Phase 1: Foundation

This is the initial phase focusing on the foundation:
- Database schema and migrations
- File ingestion with deduplication
- Import tracking
- CLI interface

**Note:** This phase does NOT parse metabolite data yet - that comes in later phases.

## Features

- **File Ingestion**: Process metabolomics data files with automatic type detection
- **Deduplication**: Global deduplication by SHA256 hash and file size
- **Import Tracking**: Track batches of file imports with status and notes
- **Database Migrations**: Proper Alembic migrations for schema management
- **Type Detection**: Heuristic detection of file formats (mwtab, HTML tables, Excel, etc.)
- **CLI Interface**: Simple command-line interface using Typer

## Supported File Types

Currently detects (but doesn't parse yet):
- `.txt` - mwtab format, results text files
- `.htm`, `.html` - metabolite tables
- `.csv`, `.tsv` - CSV/TSV files
- `.xlsx`, `.xlsm` - Excel files (including NMR binned data)
- `.zip` - Archive files
- `.pdf` - PDF documents

## Installation

### Prerequisites

1. **Python 3.10+** - Check version:
   ```bash
   python3 --version
   ```

2. **PostgreSQL 15+** - Install via Homebrew on macOS:
   ```bash
   brew install postgresql@15
   brew services start postgresql@15
   ```

3. **uv** - Modern Python package manager (recommended):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

   Or use pip:
   ```bash
   pip install uv
   ```

### Database Setup

1. Create PostgreSQL database:
   ```bash
   createdb metaloader
   ```

2. Create a `.env` file in the project root:
   ```bash
   cp .env.example .env
   ```

3. Edit `.env` and configure your database connection:
   ```bash
   DATABASE_URL=postgresql://postgres:postgres@localhost:5432/metaloader
   LOG_LEVEL=INFO
   ```

   **Format:** `postgresql://[user]:[password]@[host]:[port]/[database]`

### Project Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd metabo-workbench-parser
   ```

2. Install dependencies with uv:
   ```bash
   uv pip install -e .
   ```

   Or with pip:
   ```bash
   pip install -e .
   ```

3. Install development dependencies (optional):
   ```bash
   uv pip install -e ".[dev]"
   ```

## Usage

### 1. Test Database Connection

```bash
metaloader db ping
```

Expected output:
```
Testing database connection...
✓ Database connection successful!
Connected to: localhost:5432/metaloader
```

### 2. Initialize Database Schema

```bash
metaloader db init
```

This runs Alembic migrations to create all necessary tables:
- `imports` - Import batch tracking
- `files` - File registry with deduplication
- `studies`, `analyses`, `samples`, `features`, `measurements` - Placeholder tables

### 3. Ingest Files

Ingest a single file:

```bash
metaloader ingest-file /path/to/data/study_ST000001.txt
```

This will:
- Create a new import record
- Calculate SHA256 hash (streaming, memory-efficient)
- Detect file type heuristically
- Check for duplicates
- Store file metadata in database

Example output:
```
Ingesting file: /path/to/data/study_ST000001.txt
Created import: 550e8400-e29b-41d4-a716-446655440000
╭────────────────────┬───────────────────────────────────────────────╮
│ Property           │ Value                                         │
├────────────────────┼───────────────────────────────────────────────┤
│ Import ID          │ 550e8400-e29b-41d4-a716-446655440000          │
│ File ID            │ 660e9511-f3ac-52e5-b827-557766551111          │
│ Filename           │ study_ST000001.txt                            │
│ Detected Type      │ mwtab                                         │
│ SHA256             │ a1b2c3d4...                                   │
│ Size (bytes)       │ 45678                                         │
│ Status             │ New                                           │
╰────────────────────┴───────────────────────────────────────────────╯
✓ File ingested successfully!
```

Ingest to an existing import:

```bash
metaloader ingest-file /path/to/data/file2.html --import-id 550e8400-e29b-41d4-a716-446655440000
```

**Deduplication:** If the same file (same SHA256 + size) is ingested again, it will detect the duplicate and return the existing file ID without creating a new record.

### 4. Finalize Import

Mark an import as completed:

```bash
metaloader import finalize 550e8400-e29b-41d4-a716-446655440000 --status success --notes "Batch import completed"
```

Or mark as failed:

```bash
metaloader import finalize 550e8400-e29b-41d4-a716-446655440000 --status failed --notes "Error during processing"
```

### 5. Parse mwTab Files (Phase 2)

After ingesting files, parse mwTab files to extract samples, metabolites, and measurements.

**Find file_id from database:**

```sql
-- Connect to database
psql -d metaloader

-- List ingested mwTab files
SELECT id, filename, detected_type, created_at
FROM files
WHERE detected_type = 'mwtab'
ORDER BY created_at DESC
LIMIT 20;
```

**Parse using file_id (from database):**

```bash
metaloader parse mwtab <file-uuid>
```

**Parse using file path (direct):**

```bash
metaloader parse mwtab "/path/to/study_ST000315_AN000501.txt"
```

**Example output:**
```
Parsing mwTab: 660e9511-f3ac-52e5-b827-557766551111
Using file_id: 660e9511-f3ac-52e5-b827-557766551111
╭───────────────────────────────┬─────────────╮
│ Property                      │ Value       │
├───────────────────────────────┼─────────────┤
│ Study ID                      │ ST000315    │
│ Analysis ID                   │ AN000501    │
│                               │             │
│ Samples                       │             │
│   Processed                   │ 233         │
│   Created                     │ 233         │
│                               │             │
│ Features (Metabolites)        │             │
│   Processed                   │ 156         │
│   Created                     │ 156         │
│                               │             │
│ Measurements                  │             │
│   Processed                   │ 36348       │
│   Inserted/Updated            │ 36348       │
│                               │             │
│ Warnings                      │ 0           │
│ Mode                          │ Production  │
╰───────────────────────────────┴─────────────╯
✓ File parsed and data stored successfully!
```

**Dry run mode** (preview without writing):

```bash
metaloader parse mwtab <file-uuid-or-path> --dry-run
```

**What gets parsed:**

1. **Metadata**: `STUDY_ID`, `ANALYSIS_ID` from file header
2. **Samples**: From `SUBJECT_SAMPLE_FACTORS` section
   - `sample_uid` = `{study_id}:{sample_label}`
   - Raw factors string saved to `factors_raw`
3. **Metabolites/Features**: From `MS_METABOLITE_DATA` section
   - `feature_uid` = `{analysis_id}:met:{normalized_name}`
   - `feature_type` = 'metabolite'
4. **Measurements**: Values from MS data table
   - Linked by `(sample_uid, feature_uid)`
   - Units detected from `MS_METABOLITE_DATA:UNITS`
   - NA/empty values stored as NULL

**Idempotency**: Running the same file twice will update existing records (upsert), not create duplicates.

**Check parsed data:**

```sql
-- Overview of studies and analyses
SELECT s.study_id, a.analysis_id,
       COUNT(DISTINCT sam.id) as samples,
       COUNT(DISTINCT f.id) as features,
       COUNT(DISTINCT m.id) as measurements
FROM studies s
LEFT JOIN analyses a ON a.study_pk = s.id
LEFT JOIN samples sam ON sam.study_pk = s.id
LEFT JOIN features f ON f.feature_uid LIKE a.analysis_id || ':%'
LEFT JOIN measurements m ON m.sample_uid = sam.sample_uid
GROUP BY s.study_id, a.analysis_id;

-- View samples for a study
SELECT sample_uid, sample_label, factors_raw
FROM samples
WHERE study_pk = (SELECT id FROM studies WHERE study_id = 'ST000315')
LIMIT 10;

-- View metabolites for an analysis
SELECT feature_uid, name_raw, feature_type
FROM features
WHERE feature_uid LIKE 'AN000501:%'
LIMIT 10;

-- View measurements for a sample
SELECT f.name_raw, m.value, m.unit
FROM measurements m
JOIN features f ON f.feature_uid = m.feature_uid
WHERE m.sample_uid = 'ST000315:Sample1'
ORDER BY f.name_raw;

-- Count measurements per study
SELECT s.study_id, COUNT(*) as measurement_count
FROM measurements m
JOIN samples sam ON sam.sample_uid = m.sample_uid
JOIN studies s ON s.id = sam.study_pk
GROUP BY s.study_id;
```

## Database Schema

### Core Tables (Phase 1)

**imports**
- Tracks import batches
- Fields: `id`, `created_at`, `root_path`, `status`, `notes`
- Status: `running`, `success`, `failed`

**files**
- File registry with global deduplication
- Fields: `id`, `import_id`, `path_abs`, `path_rel`, `filename`, `ext`, `size_bytes`, `sha256`, `detected_type`, `created_at`
- Unique constraint: `(sha256, size_bytes)`
- Index on: `sha256`

### Data Tables (Phase 2)

**studies**
- Study metadata
- Fields: `id`, `study_id`, `created_at`
- Upsert by: `study_id`

**analyses**
- Analysis metadata
- Fields: `id`, `study_pk`, `analysis_id`, `file_id`, `created_at`
- Foreign keys: `study_pk` → `studies.id`, `file_id` → `files.id`
- Upsert by: `(study_pk, analysis_id)`

**samples**
- Sample information
- Fields: `id`, `study_pk`, `sample_label`, `sample_uid`, `factors_raw`, `created_at`
- Unique constraint: `sample_uid`
- `sample_uid` format: `{study_id}:{sample_label}`
- `factors_raw`: Raw factors string from mwTab (e.g., "Group:Exercise | Visit:1")

**sample_factors**
- Sample metadata key-value pairs (parsed from factors_raw)
- Fields: `id`, `sample_uid`, `factor_key`, `factor_value`, `created_at`
- Unique constraint: `(sample_uid, factor_key)`
- Foreign key: `sample_uid` → `samples.sample_uid` (ON DELETE CASCADE)

**features**
- Metabolite/feature definitions
- Fields: `id`, `feature_uid`, `feature_type`, `name_raw`, `created_at`
- Unique constraint: `feature_uid`
- `feature_uid` format: `{analysis_id}:met:{normalized_name}`
- `feature_type`: 'metabolite' for MS data

**measurements**
- Feature values per sample
- Fields: `id`, `sample_uid`, `feature_uid`, `value`, `unit`, `created_at`
- Unique constraint: `(sample_uid, feature_uid)` - enables upsert
- Foreign keys: `sample_uid` → `samples.sample_uid`, `feature_uid` → `features.feature_uid`
- `value`: Float, NULL for missing/NA values
- `unit`: Detected from `MS_METABOLITE_DATA:UNITS`

## File Type Detection

Heuristic rules:

| Pattern | Detected Type |
|---------|---------------|
| `.txt` containing `#METABOLOMICS WORKBENCH` | `mwtab` |
| `.htm/.html` containing `Metabolite_name` | `metabo_table_html` |
| Filename contains `_res.txt` | `results_txt` |
| `.xlsx/.xlsm` with `normalized binned data` in name | `nmr_binned_xlsx` |
| Others | `unknown` |

## Development

### Running Tests

```bash
pytest
```

Run with coverage:
```bash
pytest --cov=metaloader --cov-report=html
```

### Code Quality

Format and lint:
```bash
ruff check src/ tests/
ruff format src/ tests/
```

## Architecture

```
src/metaloader/
├── cli.py              # CLI commands (Typer)
├── config.py           # Configuration (env vars)
├── database.py         # SQLAlchemy setup
├── models.py           # Database models
├── parsers/
│   └── mwtab.py           # mwTab format parser
├── services/
│   ├── file_handler.py    # File processing logic
│   ├── import_service.py  # Import management
│   └── parse_service.py   # Parsing and data extraction
└── utils/
    ├── hashing.py         # SHA256 streaming
    └── type_detector.py   # File type detection

alembic/
├── env.py              # Alembic environment
└── versions/
    ├── 001_initial_schema.py  # Initial migration
    ├── 002_add_sample_factors.py  # Sample factors table
    └── 003_add_factors_raw_and_measurements_constraint.py  # Phase 2

tests/
├── test_hashing.py
├── test_type_detector.py
└── test_mwtab_parser.py
```

## Troubleshooting

### Database Connection Issues

1. Check PostgreSQL is running:
   ```bash
   brew services list | grep postgresql
   ```

2. Test connection manually:
   ```bash
   psql -h localhost -U postgres -d metaloader
   ```

3. Verify DATABASE_URL in `.env`:
   ```bash
   cat .env | grep DATABASE_URL
   ```

### Migration Issues

Reset database (⚠️ destroys all data):
```bash
dropdb metaloader
createdb metaloader
metaloader db init
```

## Roadmap

- ✅ **Phase 1**: Foundation
  - Database schema
  - File ingestion with deduplication
  - Import tracking

- ✅ **Phase 2**: mwTab Parsing (current)
  - Parse `SUBJECT_SAMPLE_FACTORS` section
  - Parse `MS_METABOLITE_DATA` section
  - Extract study/analysis/sample metadata
  - Create features and measurements
  - Batch upserts for performance
  - Idempotent: re-running updates existing data

- 📋 **Phase 3**: Additional Formats (planned)
  - NMR binned data parser (Excel)
  - HTML table parser
  - CSV/TSV parsers

- 📋 **Phase 4**: Analysis (planned)
  - Data validation
  - Quality checks
  - Export functionality
  - Data aggregation/normalization

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `pytest`
5. Submit a pull request

## License

MIT License - See LICENSE file for details

## Support

For issues and questions:
- Open an issue on GitHub
- Check existing issues for solutions

## Authors

- Your Name <your.email@example.com>