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
app.add_typer(db_app, name="db")
app.add_typer(parse_app, name="parse")
app.add_typer(qc_app, name="qc")

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


if __name__ == "__main__":
    app()
