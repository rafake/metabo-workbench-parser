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

### Placeholder Tables (Phase 1)

These tables are created but not populated yet:
- `studies` - Study metadata
- `analyses` - Analysis metadata
- `samples` - Sample information
- `features` - Metabolite features
- `measurements` - Feature measurements per sample

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
├── services/
│   ├── file_handler.py    # File processing logic
│   └── import_service.py  # Import management
└── utils/
    ├── hashing.py         # SHA256 streaming
    └── type_detector.py   # File type detection

alembic/
├── env.py              # Alembic environment
└── versions/
    └── 001_initial_schema.py  # Initial migration

tests/
├── test_hashing.py
└── test_type_detector.py
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

- ✅ **Phase 1**: Foundation (current)
  - Database schema
  - File ingestion
  - Import tracking

- 🔄 **Phase 2**: Parsing (planned)
  - MWTAB parser
  - HTML table parser
  - Excel parser
  - Populate studies, samples, features, measurements

- 🔄 **Phase 3**: Analysis (planned)
  - Data validation
  - Quality checks
  - Export functionality

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