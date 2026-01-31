"""CLI application for metaloader."""

import logging
import sys
from pathlib import Path
from typing import Optional
from uuid import UUID

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table
from sqlalchemy.exc import OperationalError

from metaloader.config import config
from metaloader.database import get_db, test_connection, engine
from metaloader.models import Base
from metaloader.services.file_handler import FileHandler
from metaloader.services.import_service import ImportService
from metaloader.services.parse_service import ParseService
from metaloader.services.parse_ms_service import ParseMSService
from metaloader.services.parse_nmr_service import ParseNMRService
from metaloader.services.derive_service import DeriveService
from metaloader.services.ingest_dir_service import IngestDirService
from metaloader.services.parse_dir_service import ParseDirService
from metaloader.services.tagger_service import TaggerService
from metaloader.services.export_service import ExportService
from metaloader.qc import QCService, QCFilters
from metaloader.models import File

# Setup logging
logging.basicConfig(
    level=config.log_level,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)],
)
logger = logging.getLogger(__name__)

# CLI app
app = typer.Typer(help="Metaloader - Tool for loading metabolomics data into PostgreSQL")
db_app = typer.Typer(help="Database management commands")
parse_app = typer.Typer(help="Parse and extract data from files")
qc_app = typer.Typer(help="Quality control and data validation commands")
derive_app = typer.Typer(help="Derive computed columns from raw data")
files_app = typer.Typer(help="File management and tagging commands")
export_app = typer.Typer(help="Export data to various formats")
app.add_typer(db_app, name="db")
app.add_typer(parse_app, name="parse")
app.add_typer(qc_app, name="qc")
app.add_typer(derive_app, name="derive")
app.add_typer(files_app, name="files")
app.add_typer(export_app, name="export")

console = Console()


@db_app.command("ping")
def db_ping():
    """Test database connection."""
    console.print("[bold blue]Testing database connection...[/bold blue]")
    
    try:
        if test_connection():
            console.print("[bold green]✓ Database connection successful![/bold green]")
            console.print(f"Connected to: {config.db_url.split('@')[-1]}")  # Hide credentials
        else:
            console.print("[bold red]✗ Database connection failed[/bold red]")
            sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        sys.exit(1)


@db_app.command("init")
def db_init():
    """Initialize database schema (create tables)."""
    console.print("[bold blue]Initializing database schema...[/bold blue]")
    
    try:
        # Import alembic to run migrations
        from alembic import command
        from alembic.config import Config
        
        # Load alembic config
        alembic_cfg = Config("alembic.ini")
        
        # Run migrations to head
        command.upgrade(alembic_cfg, "head")
        
        console.print("[bold green]✓ Database schema initialized successfully![/bold green]")
    except ImportError:
        console.print("[bold yellow]⚠ Alembic not found, falling back to SQLAlchemy create_all[/bold yellow]")
        try:
            Base.metadata.create_all(bind=engine)
            console.print("[bold green]✓ Database tables created successfully![/bold green]")
        except Exception as e:
            console.print(f"[bold red]✗ Error creating tables: {e}[/bold red]")
            sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗ Error running migrations: {e}[/bold red]")
        sys.exit(1)


@app.command("ingest-file")
def ingest_file(
    file_path: Path = typer.Argument(..., help="Path to the file to ingest"),
    import_id: Optional[str] = typer.Option(None, "--import-id", help="UUID of existing import"),
):
    """Ingest a single file into the database."""
    console.print(f"[bold blue]Ingesting file: {file_path}[/bold blue]")
    
    # Validate file exists
    if not file_path.exists():
        console.print(f"[bold red]✗ Error: File not found: {file_path}[/bold red]")
        sys.exit(1)
    
    if not file_path.is_file():
        console.print(f"[bold red]✗ Error: Path is not a file: {file_path}[/bold red]")
        sys.exit(1)
    
    # Get absolute path
    file_path = file_path.absolute()
    
    try:
        # Get database session
        db = next(get_db())
        
        # Initialize services
        import_service = ImportService(db)
        file_handler = FileHandler(db)
        
        # Handle import_id
        import_record = None
        import_created = False
        
        if import_id:
            # Use existing import
            try:
                import_uuid = UUID(import_id)
                import_record = import_service.get_import(import_uuid)
                if not import_record:
                    console.print(f"[bold red]✗ Error: Import not found: {import_id}[/bold red]")
                    sys.exit(1)
            except ValueError:
                console.print(f"[bold red]✗ Error: Invalid UUID format: {import_id}[/bold red]")
                sys.exit(1)
        else:
            # Create new import
            root_path = str(file_path.parent)
            import_record = import_service.create_import(root_path=root_path, status="running")
            import_created = True
            console.print(f"[dim]Created import: {import_record.id}[/dim]")
        
        # Process file
        file_record, is_new = file_handler.process_file(
            file_path, import_record.id, Path(import_record.root_path) if import_record.root_path else None
        )
        
        # Update import status if we created it
        if import_created:
            import_service.update_status(import_record.id, "success")
        
        # Display results
        table = Table(title="File Ingestion Results")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Import ID", str(import_record.id))
        table.add_row("File ID", str(file_record.id))
        table.add_row("Filename", file_record.filename)
        table.add_row("Detected Type", file_record.detected_type)
        table.add_row("SHA256", file_record.sha256)
        table.add_row("Size (bytes)", str(file_record.size_bytes))
        table.add_row("Status", "Duplicate (existing)" if not is_new else "New")
        
        console.print(table)
        
        if not is_new:
            console.print("[bold yellow]⚠ File already exists in database (duplicate detected)[/bold yellow]")
        else:
            console.print("[bold green]✓ File ingested successfully![/bold green]")
        
    except ValueError as e:
        console.print(f"[bold red]✗ Validation error: {e}[/bold red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        logger.exception("Error during file ingestion")
        sys.exit(1)


@app.command("import")
def import_finalize(
    import_id: str = typer.Argument(..., help="UUID of the import to finalize"),
    status: str = typer.Option(..., "--status", help="Final status (success or failed)"),
    notes: str = typer.Option("", "--notes", help="Notes about the import"),
):
    """Finalize an import with status and notes."""
    console.print(f"[bold blue]Finalizing import: {import_id}[/bold blue]")
    
    # Validate status
    if status not in ["success", "failed"]:
        console.print("[bold red]✗ Error: Status must be 'success' or 'failed'[/bold red]")
        sys.exit(1)
    
    try:
        # Parse UUID
        import_uuid = UUID(import_id)
    except ValueError:
        console.print(f"[bold red]✗ Error: Invalid UUID format: {import_id}[/bold red]")
        sys.exit(1)
    
    try:
        # Get database session
        db = next(get_db())
        
        # Initialize service
        import_service = ImportService(db)
        
        # Finalize import
        import_record = import_service.finalize_import(import_uuid, status, notes)
        
        # Display results
        table = Table(title="Import Finalized")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Import ID", str(import_record.id))
        table.add_row("Status", import_record.status)
        table.add_row("Root Path", import_record.root_path or "N/A")
        table.add_row("Notes", import_record.notes or "N/A")
        table.add_row("Created At", str(import_record.created_at))
        
        console.print(table)
        console.print("[bold green]✓ Import finalized successfully![/bold green]")
        
    except ValueError as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        logger.exception("Error during import finalization")
        sys.exit(1)


def _is_uuid(value: str) -> bool:
    """Check if string is a valid UUID."""
    try:
        UUID(value)
        return True
    except ValueError:
        return False


@parse_app.command("mwtab")
def parse_mwtab(
    file_ref: str = typer.Argument(..., help="File ID (UUID) or path to mwTab file"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Don't write to database, only show stats"),
):
    """Parse mwTab file and extract samples, features, and measurements.

    FILE_REF can be:
    - A UUID from the files table (e.g., from 'metaloader ingest-file')
    - A direct path to an mwTab file
    """
    console.print(f"[bold blue]Parsing mwTab: {file_ref}[/bold blue]")

    if dry_run:
        console.print("[bold yellow]⚠ Dry run mode - not writing to database[/bold yellow]")

    # Determine if file_ref is UUID or path
    file_uuid: Optional[UUID] = None
    file_path: Optional[Path] = None

    if _is_uuid(file_ref):
        file_uuid = UUID(file_ref)
        console.print(f"[dim]Using file_id: {file_uuid}[/dim]")
    else:
        file_path = Path(file_ref)
        if not file_path.exists():
            console.print(f"[bold red]✗ Error: File not found: {file_path}[/bold red]")
            sys.exit(1)
        file_path = file_path.absolute()
        console.print(f"[dim]Using file path: {file_path}[/dim]")

    try:
        # Get database session
        db = next(get_db())

        # Initialize service
        parse_service = ParseService(db)

        # Parse file
        stats = parse_service.parse_mwtab_file(
            file_id=file_uuid,
            file_path=file_path,
            dry_run=dry_run
        )

        # Display results
        table = Table(title="mwTab Parse Results")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Study ID", stats.study_id or "N/A")
        table.add_row("Analysis ID", stats.analysis_id or "N/A")
        table.add_row("", "")
        table.add_row("[bold]Samples[/bold]", "")
        table.add_row("  Processed", str(stats.samples_processed))
        table.add_row("  Created", str(stats.samples_created))
        table.add_row("", "")
        table.add_row("[bold]Features (Metabolites)[/bold]", "")
        table.add_row("  Processed", str(stats.features_processed))
        table.add_row("  Created", str(stats.features_created))
        table.add_row("", "")
        table.add_row("[bold]Measurements[/bold]", "")
        table.add_row("  Processed", str(stats.measurements_processed))
        table.add_row("  Inserted/Updated", str(stats.measurements_inserted))
        table.add_row("", "")
        table.add_row("Warnings", str(stats.warnings_count))
        table.add_row("Mode", "Dry Run" if dry_run else "Production")

        console.print(table)

        if stats.warnings_count > 0:
            console.print(f"[bold yellow]⚠ {stats.warnings_count} warnings during parsing[/bold yellow]")

        if stats.measurements_processed == 0 and stats.features_processed == 0:
            console.print("[bold yellow]⚠ No MS_METABOLITE_DATA section found (may be NMR study)[/bold yellow]")

        if not dry_run:
            console.print("[bold green]✓ File parsed and data stored successfully![/bold green]")
        else:
            console.print("[bold blue]✓ Dry run completed - no data was written[/bold blue]")

    except ValueError as e:
        console.print(f"[bold red]✗ Validation error: {e}[/bold red]")
        sys.exit(1)
    except FileNotFoundError as e:
        console.print(f"[bold red]✗ File not found: {e}[/bold red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        logger.exception("Error during mwTab parsing")
        sys.exit(1)


@parse_app.command("mwtab-ms")
def parse_mwtab_ms(
    file_ref: str = typer.Argument(None, help="Path to mwTab file (or use --file-id)"),
    file_id: Optional[str] = typer.Option(None, "--file-id", help="UUID of file from database"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Don't write to database, only show stats"),
):
    """Parse mwTab file MS_METABOLITE_DATA section and store measurements.

    This command parses LC/GC-MS metabolite data with:
    - Column-level tracking (col_index)
    - Replicate detection (replicate_ix)
    - Idempotent inserts (no duplicates)

    Provide either a file path or --file-id to parse from database.
    """
    # Validate inputs
    if not file_ref and not file_id:
        console.print("[bold red]✗ Error: Provide either a file path or --file-id[/bold red]")
        sys.exit(1)

    if file_ref and file_id:
        console.print("[bold red]✗ Error: Provide either file path OR --file-id, not both[/bold red]")
        sys.exit(1)

    file_uuid: Optional[UUID] = None
    file_path: Optional[Path] = None

    if file_id:
        try:
            file_uuid = UUID(file_id)
        except ValueError:
            console.print(f"[bold red]✗ Error: Invalid UUID format: {file_id}[/bold red]")
            sys.exit(1)
        console.print(f"[bold blue]Parsing MS data from file_id: {file_uuid}[/bold blue]")
    else:
        file_path = Path(file_ref)
        if not file_path.exists():
            console.print(f"[bold red]✗ Error: File not found: {file_path}[/bold red]")
            sys.exit(1)
        file_path = file_path.absolute()
        console.print(f"[bold blue]Parsing MS data from: {file_path}[/bold blue]")

    if dry_run:
        console.print("[bold yellow]⚠ Dry run mode - not writing to database[/bold yellow]")

    try:
        # Get database session
        db = next(get_db())

        # If file_id provided, look up the file record
        if file_uuid:
            file_record = db.query(File).filter(File.id == file_uuid).first()
            if not file_record:
                console.print(f"[bold red]✗ Error: File not found in database: {file_uuid}[/bold red]")
                sys.exit(1)
            file_path = Path(file_record.path_abs)
            if not file_path.exists():
                console.print(f"[bold red]✗ Error: File path not found: {file_path}[/bold red]")
                sys.exit(1)

        # Initialize service
        parse_service = ParseMSService(db)

        # Parse file
        stats = parse_service.parse_file(
            file_path=file_path,
            file_id=file_uuid,
            dry_run=dry_run
        )

        # Display results
        table = Table(title="MS Metabolite Data Parse Results")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Study ID", stats.study_id or "N/A")
        table.add_row("Analysis ID", stats.analysis_id or "N/A")
        table.add_row("", "")
        table.add_row("[bold]Samples[/bold]", "")
        table.add_row("  Processed", str(stats.samples_processed))
        table.add_row("  Created", str(stats.samples_created))
        table.add_row("", "")
        table.add_row("[bold]Features (Metabolites)[/bold]", "")
        table.add_row("  Processed", str(stats.features_processed))
        table.add_row("  Created", str(stats.features_created))
        table.add_row("", "")
        table.add_row("[bold]Measurements[/bold]", "")
        table.add_row("  Processed", str(stats.measurements_processed))
        table.add_row("  Inserted", str(stats.measurements_inserted))
        table.add_row("  Skipped (conflict)", str(stats.measurements_skipped))
        table.add_row("", "")
        table.add_row("Warnings", str(stats.warnings_count))
        table.add_row("Mode", "Dry Run" if dry_run else "Production")

        console.print(table)

        if stats.warnings_count > 0:
            console.print(f"[bold yellow]⚠ {stats.warnings_count} warnings during parsing[/bold yellow]")

        if stats.measurements_skipped > 0:
            console.print(f"[dim]Note: {stats.measurements_skipped} measurements skipped (already exist)[/dim]")

        if not dry_run:
            console.print("[bold green]✓ MS data parsed and stored successfully![/bold green]")
        else:
            console.print("[bold blue]✓ Dry run completed - no data was written[/bold blue]")

    except ValueError as e:
        console.print(f"[bold red]✗ Validation error: {e}[/bold red]")
        sys.exit(1)
    except FileNotFoundError as e:
        console.print(f"[bold red]✗ File not found: {e}[/bold red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        logger.exception("Error during MS data parsing")
        sys.exit(1)


@parse_app.command("mwtab-nmr-binned")
def parse_mwtab_nmr_binned(
    file_ref: str = typer.Argument(None, help="Path to mwTab file (or use --file-id)"),
    file_id: Optional[str] = typer.Option(None, "--file-id", help="UUID of file from database"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Don't write to database, only show stats"),
):
    """Parse mwTab file NMR_BINNED_DATA section and store measurements.

    This command parses NMR binned data with:
    - Bin range (ppm) as features
    - Column-level tracking (col_index)
    - Replicate detection (replicate_ix)
    - Idempotent inserts (no duplicates)

    Provide either a file path or --file-id to parse from database.
    """
    # Validate inputs
    if not file_ref and not file_id:
        console.print("[bold red]✗ Error: Provide either a file path or --file-id[/bold red]")
        sys.exit(1)

    if file_ref and file_id:
        console.print("[bold red]✗ Error: Provide either file path OR --file-id, not both[/bold red]")
        sys.exit(1)

    file_uuid: Optional[UUID] = None
    file_path: Optional[Path] = None

    if file_id:
        try:
            file_uuid = UUID(file_id)
        except ValueError:
            console.print(f"[bold red]✗ Error: Invalid UUID format: {file_id}[/bold red]")
            sys.exit(1)
        console.print(f"[bold blue]Parsing NMR binned data from file_id: {file_uuid}[/bold blue]")
    else:
        file_path = Path(file_ref)
        if not file_path.exists():
            console.print(f"[bold red]✗ Error: File not found: {file_path}[/bold red]")
            sys.exit(1)
        file_path = file_path.absolute()
        console.print(f"[bold blue]Parsing NMR binned data from: {file_path}[/bold blue]")

    if dry_run:
        console.print("[bold yellow]⚠ Dry run mode - not writing to database[/bold yellow]")

    try:
        # Get database session
        db = next(get_db())

        # If file_id provided, look up the file record
        if file_uuid:
            file_record = db.query(File).filter(File.id == file_uuid).first()
            if not file_record:
                console.print(f"[bold red]✗ Error: File not found in database: {file_uuid}[/bold red]")
                sys.exit(1)
            file_path = Path(file_record.path_abs)
            if not file_path.exists():
                console.print(f"[bold red]✗ Error: File path not found: {file_path}[/bold red]")
                sys.exit(1)

        # Initialize service
        parse_service = ParseNMRService(db)

        # Parse file
        stats = parse_service.parse_file(
            file_path=file_path,
            file_id=file_uuid,
            dry_run=dry_run
        )

        # Display results
        table = Table(title="NMR Binned Data Parse Results")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Study ID", stats.study_id or "N/A")
        table.add_row("Analysis ID", stats.analysis_id or "N/A")
        table.add_row("", "")
        table.add_row("[bold]Samples[/bold]", "")
        table.add_row("  Processed", str(stats.samples_processed))
        table.add_row("  Created", str(stats.samples_created))
        table.add_row("", "")
        table.add_row("[bold]Features (NMR Bins)[/bold]", "")
        table.add_row("  Processed", str(stats.features_processed))
        table.add_row("  Created", str(stats.features_created))
        table.add_row("", "")
        table.add_row("[bold]Measurements[/bold]", "")
        table.add_row("  Processed", str(stats.measurements_processed))
        table.add_row("  Inserted", str(stats.measurements_inserted))
        table.add_row("  Skipped (conflict)", str(stats.measurements_skipped))
        table.add_row("", "")
        table.add_row("Warnings", str(stats.warnings_count))
        table.add_row("Mode", "Dry Run" if dry_run else "Production")

        console.print(table)

        if stats.warnings_count > 0:
            console.print(f"[bold yellow]⚠ {stats.warnings_count} warnings during parsing[/bold yellow]")

        if stats.measurements_skipped > 0:
            console.print(f"[dim]Note: {stats.measurements_skipped} measurements skipped (already exist)[/dim]")

        if not dry_run:
            console.print("[bold green]✓ NMR binned data parsed and stored successfully![/bold green]")
        else:
            console.print("[bold blue]✓ Dry run completed - no data was written[/bold blue]")

    except ValueError as e:
        console.print(f"[bold red]✗ Validation error: {e}[/bold red]")
        sys.exit(1)
    except FileNotFoundError as e:
        console.print(f"[bold red]✗ File not found: {e}[/bold red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        logger.exception("Error during NMR binned data parsing")
        sys.exit(1)


@qc_app.command("summary")
def qc_summary(
    study_id: Optional[str] = typer.Option(None, "--study-id", help="Filter by study ID (e.g., ST000106)"),
    analysis_id: Optional[str] = typer.Option(None, "--analysis-id", help="Filter by analysis ID (e.g., AN000175)"),
):
    """Generate QC summary report for measurements data.

    Shows data quality metrics including:
    - Measurement counts (total, null, non-null)
    - Duplicate detection
    - Special values (NaN, Inf)
    - Orphan records
    - Unit distribution
    - Sample statistics
    """
    console.print("[bold blue]Running QC Summary...[/bold blue]")

    # Show applied filters
    if study_id:
        console.print(f"[dim]Filter: study_id = {study_id}[/dim]")
    if analysis_id:
        console.print(f"[dim]Filter: analysis_id = {analysis_id}[/dim]")

    try:
        # Get database session
        db = next(get_db())

        # Initialize service
        qc_service = QCService(db)

        # Build filters
        filters = QCFilters(study_id=study_id, analysis_id=analysis_id)

        # Run QC
        results = qc_service.run_summary(filters)

        # === Main metrics table ===
        main_table = Table(title="QC Summary - Measurements", show_header=True, header_style="bold cyan")
        main_table.add_column("Metric", style="cyan", width=35)
        main_table.add_column("Value", style="green", justify="right")
        main_table.add_column("Status", justify="center")

        # Total measurements
        main_table.add_row(
            "Total Measurements",
            f"{results.total_measurements:,}",
            "[green]OK[/green]" if results.total_measurements > 0 else "[yellow]EMPTY[/yellow]"
        )

        # Non-null values
        main_table.add_row(
            "Non-NULL Values",
            f"{results.non_null_values:,}",
            ""
        )

        # Null values
        null_status = "[green]OK[/green]"
        if results.null_percent > 50:
            null_status = "[red]HIGH[/red]"
        elif results.null_percent > 20:
            null_status = "[yellow]WARN[/yellow]"

        main_table.add_row(
            "NULL Values",
            f"{results.null_count:,} ({results.null_percent:.2f}%)",
            null_status
        )

        main_table.add_row("", "", "")  # Separator

        # Duplicates
        dup_status = "[green]OK[/green]" if results.duplicate_pairs_count == 0 else "[red]ISSUE[/red]"
        main_table.add_row(
            "Duplicate (sample, feature) pairs",
            f"{results.duplicate_pairs_count:,}",
            dup_status
        )

        main_table.add_row("", "", "")  # Separator

        # Special values
        main_table.add_row(
            "NaN Values",
            f"{results.nan_count:,}",
            "[green]OK[/green]" if results.nan_count == 0 else "[yellow]WARN[/yellow]"
        )
        main_table.add_row(
            "+Infinity Values",
            f"{results.pos_inf_count:,}",
            "[green]OK[/green]" if results.pos_inf_count == 0 else "[yellow]WARN[/yellow]"
        )
        main_table.add_row(
            "-Infinity Values",
            f"{results.neg_inf_count:,}",
            "[green]OK[/green]" if results.neg_inf_count == 0 else "[yellow]WARN[/yellow]"
        )

        main_table.add_row("", "", "")  # Separator

        # Negative values
        neg_status = "[green]OK[/green]"
        if results.total_measurements > 0:
            neg_pct = (results.negative_values_count / results.total_measurements) * 100
            if neg_pct > 10:
                neg_status = "[yellow]WARN[/yellow]"
        main_table.add_row(
            "Negative Values",
            f"{results.negative_values_count:,}",
            neg_status
        )

        main_table.add_row("", "", "")  # Separator

        # Orphans
        main_table.add_row(
            "Orphan Samples (no FK match)",
            f"{results.orphan_sample_count:,}",
            "[green]OK[/green]" if results.orphan_sample_count == 0 else "[red]ISSUE[/red]"
        )
        main_table.add_row(
            "Orphan Features (no FK match)",
            f"{results.orphan_feature_count:,}",
            "[green]OK[/green]" if results.orphan_feature_count == 0 else "[red]ISSUE[/red]"
        )

        console.print(main_table)
        console.print()

        # === Sample stats table ===
        sample_table = Table(title="Sample Statistics", show_header=True, header_style="bold cyan")
        sample_table.add_column("Metric", style="cyan", width=35)
        sample_table.add_column("Value", style="green", justify="right")
        sample_table.add_column("Status", justify="center")

        sample_table.add_row(
            "Total Samples" + (f" (study: {study_id})" if study_id else ""),
            f"{results.samples_total:,}",
            ""
        )

        no_factors_status = "[green]OK[/green]"
        if results.samples_total > 0:
            no_factors_pct = (results.samples_no_factors / results.samples_total) * 100
            if no_factors_pct > 50:
                no_factors_status = "[yellow]WARN[/yellow]"

        sample_table.add_row(
            "Samples without factors_raw",
            f"{results.samples_no_factors:,}",
            no_factors_status
        )

        console.print(sample_table)
        console.print()

        # === Top units table ===
        if results.top_units:
            units_table = Table(title="Top 10 Units", show_header=True, header_style="bold cyan")
            units_table.add_column("#", style="dim", width=4)
            units_table.add_column("Unit", style="cyan")
            units_table.add_column("Count", style="green", justify="right")

            for i, (unit, count) in enumerate(results.top_units, 1):
                unit_display = f"[dim]{unit}[/dim]" if unit == "<NULL>" else unit
                units_table.add_row(str(i), unit_display, f"{count:,}")

            console.print(units_table)
            console.print()

        # === Top NULL features table ===
        if results.top_null_features:
            null_features_table = Table(
                title="Top 10 Features with NULL Values",
                show_header=True,
                header_style="bold cyan"
            )
            null_features_table.add_column("#", style="dim", width=4)
            null_features_table.add_column("Feature UID", style="cyan")
            null_features_table.add_column("NULL Count", style="yellow", justify="right")

            for i, (feature_uid, count) in enumerate(results.top_null_features, 1):
                # Truncate long feature UIDs
                display_uid = feature_uid if len(feature_uid) <= 60 else f"{feature_uid[:57]}..."
                null_features_table.add_row(str(i), display_uid, f"{count:,}")

            console.print(null_features_table)
            console.print()

        # Final status
        issues = []
        if results.duplicate_pairs_count > 0:
            issues.append(f"{results.duplicate_pairs_count} duplicate pairs")
        if results.orphan_sample_count > 0:
            issues.append(f"{results.orphan_sample_count} orphan samples")
        if results.orphan_feature_count > 0:
            issues.append(f"{results.orphan_feature_count} orphan features")
        if results.null_percent > 50:
            issues.append(f"high NULL rate ({results.null_percent:.1f}%)")

        if issues:
            console.print(f"[bold yellow]⚠ Issues found: {', '.join(issues)}[/bold yellow]")
        else:
            console.print("[bold green]✓ QC Summary completed - no critical issues found[/bold green]")

    except Exception as e:
        console.print(f"[bold red]✗ Error running QC: {e}[/bold red]")
        logger.exception("Error during QC summary")
        sys.exit(1)


@derive_app.command("categories")
def derive_categories(
    study_id: Optional[str] = typer.Option(None, "--study-id", help="Filter by study ID (e.g., ST000106)"),
    file_id: Optional[str] = typer.Option(None, "--file-id", help="UUID of specific file to process"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Don't write to database, only show what would change"),
    limit: Optional[int] = typer.Option(None, "--limit", help="Limit number of records to process"),
):
    """Derive category columns from raw data.

    This command computes and stores:
    - files.device: LCMS, GCMS, NMR, MS (from file content/metadata)
    - samples.exposure: OB, CON (from sample factors)
    - samples.sample_matrix: Serum, Urine, Feces, CSF (from sample factors or file paths)

    The operation is idempotent - running multiple times won't duplicate data.
    """
    console.print("[bold blue]Deriving category columns...[/bold blue]")

    # Show applied filters
    if study_id:
        console.print(f"[dim]Filter: study_id = {study_id}[/dim]")
    if file_id:
        console.print(f"[dim]Filter: file_id = {file_id}[/dim]")
    if limit:
        console.print(f"[dim]Limit: {limit} records[/dim]")
    if dry_run:
        console.print("[bold yellow]⚠ Dry run mode - no changes will be saved[/bold yellow]")

    # Parse file_id if provided
    file_uuid: Optional[UUID] = None
    if file_id:
        try:
            file_uuid = UUID(file_id)
        except ValueError:
            console.print(f"[bold red]✗ Error: Invalid UUID format: {file_id}[/bold red]")
            sys.exit(1)

    try:
        # Get database session
        db = next(get_db())

        # Initialize service
        derive_service = DeriveService(db)

        # Run derivation
        stats = derive_service.derive_all(
            study_id=study_id,
            file_id=file_uuid,
            dry_run=dry_run,
            limit=limit
        )

        # Display results
        console.print()

        # === Device stats table ===
        device_table = Table(title="Device Derivation (files)", show_header=True, header_style="bold cyan")
        device_table.add_column("Metric", style="cyan", width=30)
        device_table.add_column("Count", style="green", justify="right")

        device_table.add_row("Files processed", f"{stats.files_processed:,}")
        device_table.add_row("Device set", f"{stats.files_device_set:,}")
        device_table.add_row("Already had device", f"{stats.files_device_already_set:,}")
        device_table.add_row("Could not determine", f"{stats.files_device_unknown:,}")

        console.print(device_table)
        console.print()

        # === Exposure stats table ===
        exposure_table = Table(title="Exposure Derivation (samples)", show_header=True, header_style="bold cyan")
        exposure_table.add_column("Metric", style="cyan", width=30)
        exposure_table.add_column("Count", style="green", justify="right")

        exposure_table.add_row("Samples processed", f"{stats.samples_processed:,}")
        exposure_table.add_row("Exposure set", f"{stats.samples_exposure_set:,}")
        exposure_table.add_row("Already had exposure", f"{stats.samples_exposure_already_set:,}")
        exposure_table.add_row("Could not determine", f"{stats.samples_exposure_unknown:,}")
        if stats.samples_exposure_conflict > 0:
            exposure_table.add_row("[yellow]Conflicts (warning)[/yellow]", f"[yellow]{stats.samples_exposure_conflict:,}[/yellow]")

        console.print(exposure_table)
        console.print()

        # === Matrix stats table ===
        matrix_table = Table(title="Sample Matrix Derivation (samples)", show_header=True, header_style="bold cyan")
        matrix_table.add_column("Metric", style="cyan", width=30)
        matrix_table.add_column("Count", style="green", justify="right")

        matrix_table.add_row("Matrix set", f"{stats.samples_matrix_set:,}")
        matrix_table.add_row("Already had matrix", f"{stats.samples_matrix_already_set:,}")
        matrix_table.add_row("Could not determine", f"{stats.samples_matrix_unknown:,}")
        if stats.samples_matrix_conflict > 0:
            matrix_table.add_row("[yellow]Conflicts (warning)[/yellow]", f"[yellow]{stats.samples_matrix_conflict:,}[/yellow]")

        console.print(matrix_table)
        console.print()

        # Show warnings if any
        if stats.warnings:
            console.print(f"[bold yellow]⚠ {len(stats.warnings)} warnings:[/bold yellow]")
            for warning in stats.warnings[:10]:  # Show first 10
                console.print(f"  [yellow]• {warning}[/yellow]")
            if len(stats.warnings) > 10:
                console.print(f"  [dim]... and {len(stats.warnings) - 10} more[/dim]")
            console.print()

        # Final status
        if dry_run:
            console.print("[bold blue]✓ Dry run completed - no data was written[/bold blue]")
        else:
            total_set = stats.files_device_set + stats.samples_exposure_set + stats.samples_matrix_set
            console.print(f"[bold green]✓ Category derivation completed - {total_set:,} values set[/bold green]")

    except Exception as e:
        console.print(f"[bold red]✗ Error during derivation: {e}[/bold red]")
        logger.exception("Error during category derivation")
        sys.exit(1)


@app.command("ingest-dir")
def ingest_dir(
    directory: Path = typer.Argument(..., help="Directory to ingest recursively"),
    import_notes: Optional[str] = typer.Option(None, "--import-notes", help="Notes for the import record"),
    include_extensions: Optional[str] = typer.Option(
        None, "--include-extensions",
        help="Comma-separated list of extensions to include (e.g., '.txt,.csv')"
    ),
    max_files: Optional[int] = typer.Option(None, "--max-files", help="Maximum number of files to process"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Don't write to database, only show what would be ingested"),
):
    """Ingest all files from a directory recursively.

    Scans the directory recursively and registers all files with supported extensions
    in the database. Files are deduplicated by SHA256+size.

    Default extensions: .txt, .htm, .html, .csv, .tsv, .xlsx, .xlsm, .zip, .pdf
    """
    console.print(f"[bold blue]Ingesting directory: {directory}[/bold blue]")

    # Validate directory
    if not directory.exists():
        console.print(f"[bold red]✗ Error: Directory not found: {directory}[/bold red]")
        sys.exit(1)

    if not directory.is_dir():
        console.print(f"[bold red]✗ Error: Path is not a directory: {directory}[/bold red]")
        sys.exit(1)

    directory = directory.absolute()

    # Parse extensions
    extensions = None
    if include_extensions:
        extensions = {ext.strip() for ext in include_extensions.split(",")}
        console.print(f"[dim]Extensions: {', '.join(sorted(extensions))}[/dim]")

    if max_files:
        console.print(f"[dim]Max files: {max_files}[/dim]")

    if dry_run:
        console.print("[bold yellow]Dry run mode - no data will be written[/bold yellow]")

    try:
        # Get database session
        db = next(get_db())

        # Initialize service
        ingest_service = IngestDirService(db)

        # Run ingestion
        stats = ingest_service.ingest_directory(
            directory=directory,
            import_notes=import_notes,
            include_extensions=extensions,
            max_files=max_files,
            dry_run=dry_run,
        )

        # Display results
        console.print()

        # Main stats table
        table = Table(title="Directory Ingestion Results")
        table.add_column("Metric", style="cyan", width=25)
        table.add_column("Value", style="green", justify="right")

        if stats.import_id:
            table.add_row("Import ID", str(stats.import_id))
        table.add_row("Root Path", stats.root_path)
        table.add_row("", "")
        table.add_row("Files found", f"{stats.files_found:,}")
        table.add_row("Files processed", f"{stats.files_processed:,}")
        table.add_row("  New", f"{stats.files_new:,}")
        table.add_row("  Duplicate", f"{stats.files_duplicate:,}")
        table.add_row("  Skipped", f"{stats.files_skipped:,}")
        table.add_row("  Errors", f"{stats.files_error:,}")

        console.print(table)
        console.print()

        # Type distribution
        if stats.by_type:
            type_table = Table(title="Files by Detected Type")
            type_table.add_column("Type", style="cyan")
            type_table.add_column("Count", style="green", justify="right")
            for dtype, count in sorted(stats.by_type.items(), key=lambda x: -x[1]):
                type_table.add_row(dtype, f"{count:,}")
            console.print(type_table)
            console.print()

        # Extension distribution
        if stats.by_extension:
            ext_table = Table(title="Files by Extension")
            ext_table.add_column("Extension", style="cyan")
            ext_table.add_column("Count", style="green", justify="right")
            for ext, count in sorted(stats.by_extension.items(), key=lambda x: -x[1]):
                ext_table.add_row(ext, f"{count:,}")
            console.print(ext_table)
            console.print()

        # Show errors if any
        if stats.errors:
            console.print(f"[bold yellow]Errors ({len(stats.errors)}):[/bold yellow]")
            for error in stats.errors[:10]:
                console.print(f"  [yellow]{error}[/yellow]")
            if len(stats.errors) > 10:
                console.print(f"  [dim]... and {len(stats.errors) - 10} more[/dim]")
            console.print()

        # Final status
        if dry_run:
            console.print("[bold blue]Dry run completed - no data was written[/bold blue]")
        else:
            console.print(f"[bold green]Directory ingestion completed - {stats.files_new:,} new files ingested[/bold green]")

    except ValueError as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        logger.exception("Error during directory ingestion")
        sys.exit(1)


@app.command("parse-dir")
def parse_dir(
    directory: Path = typer.Argument(..., help="Directory to parse recursively"),
    only_types: Optional[str] = typer.Option(
        None, "--only-types",
        help="Only parse these detected types (comma-separated, e.g., 'mwtab,mwtab_ms')"
    ),
    skip_types: Optional[str] = typer.Option(
        None, "--skip-types",
        help="Skip these detected types (comma-separated)"
    ),
    fail_fast: bool = typer.Option(False, "--fail-fast", help="Stop on first error"),
    max_files: Optional[int] = typer.Option(None, "--max-files", help="Maximum number of files to parse"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Don't write to database, only show what would be parsed"),
):
    """Parse all supported files in a directory recursively.

    Scans the directory for parsable files and extracts their data.
    Supported types: mwtab, mwtab_ms, mwtab_nmr_binned
    """
    console.print(f"[bold blue]Parsing directory: {directory}[/bold blue]")

    # Validate directory
    if not directory.exists():
        console.print(f"[bold red]✗ Error: Directory not found: {directory}[/bold red]")
        sys.exit(1)

    if not directory.is_dir():
        console.print(f"[bold red]✗ Error: Path is not a directory: {directory}[/bold red]")
        sys.exit(1)

    directory = directory.absolute()

    # Parse type filters
    only_types_set = None
    skip_types_set = None

    if only_types:
        only_types_set = {t.strip() for t in only_types.split(",")}
        console.print(f"[dim]Only types: {', '.join(sorted(only_types_set))}[/dim]")

    if skip_types:
        skip_types_set = {t.strip() for t in skip_types.split(",")}
        console.print(f"[dim]Skip types: {', '.join(sorted(skip_types_set))}[/dim]")

    if max_files:
        console.print(f"[dim]Max files: {max_files}[/dim]")

    if fail_fast:
        console.print("[dim]Fail fast mode enabled[/dim]")

    if dry_run:
        console.print("[bold yellow]Dry run mode - no data will be written[/bold yellow]")

    try:
        # Get database session
        db = next(get_db())

        # Initialize service
        parse_service = ParseDirService(db)

        # Run parsing
        stats = parse_service.parse_directory(
            directory=directory,
            only_types=only_types_set,
            skip_types=skip_types_set,
            fail_fast=fail_fast,
            max_files=max_files,
            dry_run=dry_run,
        )

        # Display results
        _display_parse_dir_results(stats, dry_run)

    except ValueError as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        sys.exit(1)
    except RuntimeError as e:
        console.print(f"[bold red]✗ Error (fail-fast): {e}[/bold red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        logger.exception("Error during directory parsing")
        sys.exit(1)


@app.command("parse-import")
def parse_import(
    import_id: str = typer.Argument(..., help="UUID of the import to parse"),
    only_types: Optional[str] = typer.Option(
        None, "--only-types",
        help="Only parse these detected types (comma-separated, e.g., 'mwtab,mwtab_ms')"
    ),
    skip_types: Optional[str] = typer.Option(
        None, "--skip-types",
        help="Skip these detected types (comma-separated)"
    ),
    fail_fast: bool = typer.Option(False, "--fail-fast", help="Stop on first error"),
    max_files: Optional[int] = typer.Option(None, "--max-files", help="Maximum number of files to parse"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Don't write to database, only show what would be parsed"),
):
    """Parse all pending files from an import.

    Parses files with parse_status='pending' or 'failed' from the specified import.
    Updates parse_status to 'success' or 'failed' after processing.
    """
    console.print(f"[bold blue]Parsing import: {import_id}[/bold blue]")

    # Validate UUID
    try:
        import_uuid = UUID(import_id)
    except ValueError:
        console.print(f"[bold red]✗ Error: Invalid UUID format: {import_id}[/bold red]")
        sys.exit(1)

    # Parse type filters
    only_types_set = None
    skip_types_set = None

    if only_types:
        only_types_set = {t.strip() for t in only_types.split(",")}
        console.print(f"[dim]Only types: {', '.join(sorted(only_types_set))}[/dim]")

    if skip_types:
        skip_types_set = {t.strip() for t in skip_types.split(",")}
        console.print(f"[dim]Skip types: {', '.join(sorted(skip_types_set))}[/dim]")

    if max_files:
        console.print(f"[dim]Max files: {max_files}[/dim]")

    if fail_fast:
        console.print("[dim]Fail fast mode enabled[/dim]")

    if dry_run:
        console.print("[bold yellow]Dry run mode - no data will be written[/bold yellow]")

    try:
        # Get database session
        db = next(get_db())

        # Initialize service
        parse_service = ParseDirService(db)

        # Run parsing
        stats = parse_service.parse_import(
            import_id=import_uuid,
            only_types=only_types_set,
            skip_types=skip_types_set,
            fail_fast=fail_fast,
            max_files=max_files,
            dry_run=dry_run,
        )

        # Display results
        _display_parse_dir_results(stats, dry_run)

    except ValueError as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        sys.exit(1)
    except RuntimeError as e:
        console.print(f"[bold red]✗ Error (fail-fast): {e}[/bold red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        logger.exception("Error during import parsing")
        sys.exit(1)


def _display_parse_dir_results(stats, dry_run: bool):
    """Display bulk parsing results."""
    console.print()

    # Main stats table
    table = Table(title="Bulk Parse Results")
    table.add_column("Metric", style="cyan", width=25)
    table.add_column("Value", style="green", justify="right")

    table.add_row("Files total", f"{stats.files_total:,}")
    table.add_row("Files parsed", f"{stats.files_parsed:,}")
    table.add_row("  Success", f"{stats.files_success:,}")
    table.add_row("  Failed", f"{stats.files_failed:,}")
    table.add_row("  Skipped", f"{stats.files_skipped:,}")
    table.add_row("", "")
    table.add_row("Samples created", f"{stats.samples_created:,}")
    table.add_row("Features created", f"{stats.features_created:,}")
    table.add_row("Measurements inserted", f"{stats.measurements_inserted:,}")

    console.print(table)
    console.print()

    # Type distribution
    if stats.by_type:
        type_table = Table(title="Parsed by Type")
        type_table.add_column("Type", style="cyan")
        type_table.add_column("Count", style="green", justify="right")
        for dtype, count in sorted(stats.by_type.items(), key=lambda x: -x[1]):
            type_table.add_row(dtype, f"{count:,}")
        console.print(type_table)
        console.print()

    # Show errors if any
    if stats.errors:
        console.print(f"[bold yellow]Errors ({len(stats.errors)}):[/bold yellow]")
        for error in stats.errors[:10]:
            console.print(f"  [yellow]{error}[/yellow]")
        if len(stats.errors) > 10:
            console.print(f"  [dim]... and {len(stats.errors) - 10} more[/dim]")
        console.print()

    # Final status
    if dry_run:
        console.print("[bold blue]Dry run completed - no data was written[/bold blue]")
    else:
        console.print(f"[bold green]Bulk parsing completed - {stats.files_success:,} files parsed successfully[/bold green]")


# =============================================================================
# FILES COMMANDS
# =============================================================================

@files_app.command("tag")
def files_tag(
    import_id: Optional[str] = typer.Option(None, "--import-id", help="Tag files from this import"),
    file_id: Optional[str] = typer.Option(None, "--file-id", help="Tag a single file by ID"),
    all_files: bool = typer.Option(False, "--all", help="Tag all files in database"),
    overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite existing tag values"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Don't write to database, only show what would change"),
):
    """Tag files with inferred category values.

    Automatically infers and sets the following columns on files:
    - device: LCMS, GCMS, NMR (based on detected_type and path patterns)
    - sample_type: Serum, Urine, Feces, CSF (based on path/filename)
    - exposure: OB, CON (based on path/filename)
    - platform: ESI_pos, HILIC, QQQ, etc. (based on path/filename)

    Tagging is idempotent: existing values are preserved unless --overwrite is used.

    Examples:
        metaloader files tag --all
        metaloader files tag --import-id abc123
        metaloader files tag --file-id xyz789 --overwrite
    """
    console.print("[bold blue]Tagging files with category values[/bold blue]")

    # Validate inputs
    if not any([import_id, file_id, all_files]):
        console.print("[bold red]✗ Error: Must specify --import-id, --file-id, or --all[/bold red]")
        sys.exit(1)

    if sum([bool(import_id), bool(file_id), all_files]) > 1:
        console.print("[bold red]✗ Error: Specify only one of --import-id, --file-id, or --all[/bold red]")
        sys.exit(1)

    # Parse UUIDs
    import_uuid: Optional[UUID] = None
    file_uuid: Optional[UUID] = None

    if import_id:
        try:
            import_uuid = UUID(import_id)
        except ValueError:
            console.print(f"[bold red]✗ Error: Invalid UUID format: {import_id}[/bold red]")
            sys.exit(1)
        console.print(f"[dim]Tagging files from import: {import_uuid}[/dim]")

    if file_id:
        try:
            file_uuid = UUID(file_id)
        except ValueError:
            console.print(f"[bold red]✗ Error: Invalid UUID format: {file_id}[/bold red]")
            sys.exit(1)
        console.print(f"[dim]Tagging file: {file_uuid}[/dim]")

    if all_files:
        console.print("[dim]Tagging all files in database[/dim]")

    if overwrite:
        console.print("[bold yellow]⚠ Overwrite mode: existing values will be replaced[/bold yellow]")

    if dry_run:
        console.print("[bold yellow]⚠ Dry run mode: no data will be written[/bold yellow]")

    try:
        # Get database session
        db = next(get_db())

        # Initialize service
        tagger_service = TaggerService(db)

        # Run tagging
        stats = tagger_service.tag_files(
            import_id=import_uuid,
            file_id=file_uuid,
            tag_all=all_files,
            overwrite=overwrite,
            dry_run=dry_run,
        )

        # Display results
        console.print()
        table = Table(title="Tagging Results")
        table.add_column("Metric", style="cyan", width=25)
        table.add_column("Value", style="green", justify="right")

        table.add_row("Files processed", f"{stats.files_processed:,}")
        table.add_row("Files updated", f"{stats.files_updated:,}")
        table.add_row("Files skipped", f"{stats.files_skipped:,}")
        table.add_row("", "")
        table.add_row("[bold]Tags set:[/bold]", "")
        table.add_row("  Device", f"{stats.device_set:,}")
        table.add_row("  Exposure", f"{stats.exposure_set:,}")
        table.add_row("  Sample type", f"{stats.sample_type_set:,}")
        table.add_row("  Platform", f"{stats.platform_set:,}")

        console.print(table)
        console.print()

        # Show warnings if any
        if stats.warnings:
            console.print(f"[bold yellow]Warnings ({len(stats.warnings)}):[/bold yellow]")
            for warning in stats.warnings[:10]:
                console.print(f"  [yellow]{warning}[/yellow]")
            if len(stats.warnings) > 10:
                console.print(f"  [dim]... and {len(stats.warnings) - 10} more[/dim]")
            console.print()

        # Final status
        if dry_run:
            console.print("[bold blue]Dry run completed - no data was written[/bold blue]")
        else:
            console.print(f"[bold green]✓ Tagging complete: {stats.files_updated:,} files updated[/bold green]")

    except ValueError as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        logger.exception("Error during file tagging")
        sys.exit(1)


# =============================================================================
# EXPORT COMMANDS
# =============================================================================

@export_app.command("parquet")
def export_parquet(
    out: Path = typer.Option(..., "--out", help="Output Parquet file path"),
    file_id: Optional[str] = typer.Option(None, "--file-id", help="Filter by file ID"),
    import_id: Optional[str] = typer.Option(None, "--import-id", help="Filter by import ID"),
    study_id: Optional[str] = typer.Option(None, "--study-id", help="Filter by study ID"),
    feature_type: Optional[str] = typer.Option(None, "--feature-type", help="Filter by feature type (metabolite, nmr_bin, etc.)"),
    chunk_size: int = typer.Option(200000, "--chunk-size", help="Number of rows per chunk"),
    preview: bool = typer.Option(False, "--preview", help="Preview first 10 rows instead of exporting"),
    count_only: bool = typer.Option(False, "--count", help="Only count rows, don't export"),
):
    """Export measurement data to Parquet format.

    Exports long-format measurement data with file categories, sample info,
    feature info, and measurement values. Data is streamed in chunks to
    avoid loading the entire dataset into memory.

    Output columns:
    - file_id, path_rel, detected_type
    - device, exposure, sample_type, platform (category tags)
    - sample_uid, sample_label
    - feature_uid, feature_type, feature_name, refmet_name
    - value, unit, col_index, replicate_ix
    - study_id, analysis_id (if available)
    - created_at

    Examples:
        metaloader export parquet --out data.parquet
        metaloader export parquet --out data.parquet --import-id abc123
        metaloader export parquet --out nmr.parquet --feature-type nmr_bin
        metaloader export parquet --out data.parquet --preview
    """
    console.print("[bold blue]Exporting measurement data to Parquet[/bold blue]")

    # Parse UUIDs
    file_uuid: Optional[UUID] = None
    import_uuid: Optional[UUID] = None

    if file_id:
        try:
            file_uuid = UUID(file_id)
        except ValueError:
            console.print(f"[bold red]✗ Error: Invalid UUID format for --file-id: {file_id}[/bold red]")
            sys.exit(1)
        console.print(f"[dim]Filtering by file: {file_uuid}[/dim]")

    if import_id:
        try:
            import_uuid = UUID(import_id)
        except ValueError:
            console.print(f"[bold red]✗ Error: Invalid UUID format for --import-id: {import_id}[/bold red]")
            sys.exit(1)
        console.print(f"[dim]Filtering by import: {import_uuid}[/dim]")

    if study_id:
        console.print(f"[dim]Filtering by study: {study_id}[/dim]")

    if feature_type:
        console.print(f"[dim]Filtering by feature type: {feature_type}[/dim]")

    try:
        # Initialize export service with engine
        export_service = ExportService(engine)

        # Count only mode
        if count_only:
            console.print("[dim]Counting rows...[/dim]")
            row_count = export_service.get_row_count(
                file_id=file_uuid,
                import_id=import_uuid,
                feature_type=feature_type,
                study_id=study_id,
            )
            console.print(f"[bold green]Total rows: {row_count:,}[/bold green]")
            return

        # Preview mode
        if preview:
            console.print("[dim]Fetching preview...[/dim]")
            preview_df = export_service.get_export_preview(
                file_id=file_uuid,
                import_id=import_uuid,
                feature_type=feature_type,
                study_id=study_id,
                limit=10,
            )

            if preview_df.empty:
                console.print("[bold yellow]No data found matching filters[/bold yellow]")
                return

            # Display preview as table
            table = Table(title="Export Preview (first 10 rows)")
            for col in preview_df.columns:
                table.add_column(col, style="cyan", overflow="fold", max_width=20)

            for _, row in preview_df.iterrows():
                table.add_row(*[str(v) if v is not None else "NULL" for v in row.values])

            console.print(table)
            console.print(f"[dim]Showing {len(preview_df)} of potentially many more rows[/dim]")
            return

        # Full export
        console.print(f"[dim]Output: {out}[/dim]")
        console.print(f"[dim]Chunk size: {chunk_size:,}[/dim]")

        stats = export_service.export_parquet(
            output_path=out,
            file_id=file_uuid,
            import_id=import_uuid,
            feature_type=feature_type,
            study_id=study_id,
            chunk_size=chunk_size,
        )

        # Display results
        console.print()
        table = Table(title="Export Results")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Output file", stats.output_path)
        table.add_row("Total rows", f"{stats.total_rows:,}")
        table.add_row("Chunks written", f"{stats.total_chunks:,}")
        table.add_row("File size", f"{stats.file_size_bytes / (1024*1024):.2f} MB")

        console.print(table)

        if stats.total_rows == 0:
            console.print("[bold yellow]⚠ No data exported - check your filters[/bold yellow]")
        else:
            console.print(f"[bold green]✓ Export complete: {stats.total_rows:,} rows written to {out}[/bold green]")

    except Exception as e:
        console.print(f"[bold red]✗ Error: {e}[/bold red]")
        logger.exception("Error during Parquet export")
        sys.exit(1)


if __name__ == "__main__":
    app()
