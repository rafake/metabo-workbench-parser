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
app.add_typer(db_app, name="db")
app.add_typer(parse_app, name="parse")

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


@parse_app.command("mwtab")
def parse_mwtab(
    file_id: str = typer.Option(..., "--file-id", help="UUID of file to parse"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Don't write to database, only show stats"),
):
    """Parse mwTab file and extract sample factors."""
    console.print(f"[bold blue]Parsing mwTab file: {file_id}[/bold blue]")

    if dry_run:
        console.print("[bold yellow]⚠ Dry run mode - not writing to database[/bold yellow]")

    try:
        # Parse UUID
        file_uuid = UUID(file_id)
    except ValueError:
        console.print(f"[bold red]✗ Error: Invalid UUID format: {file_id}[/bold red]")
        sys.exit(1)

    try:
        # Get database session
        db = next(get_db())

        # Initialize service
        parse_service = ParseService(db)

        # Parse file
        stats = parse_service.parse_mwtab_file(file_uuid, dry_run=dry_run)

        # Display results
        table = Table(title="mwTab Parse Results")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Study ID", stats.study_id or "N/A")
        table.add_row("Analysis ID", stats.analysis_id or "N/A")
        table.add_row("Samples Processed", str(stats.samples_processed))
        table.add_row("Factors Written", str(stats.factors_written))
        table.add_row("Warnings", str(stats.warnings_count))
        table.add_row("Skipped", str(stats.skipped_count))
        table.add_row("Mode", "Dry Run" if dry_run else "Production")

        console.print(table)

        if stats.warnings_count > 0:
            console.print(f"[bold yellow]⚠ {stats.warnings_count} warnings during parsing[/bold yellow]")

        if stats.skipped_count > 0:
            console.print(f"[bold yellow]⚠ {stats.skipped_count} samples skipped due to errors[/bold yellow]")

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


if __name__ == "__main__":
    app()
