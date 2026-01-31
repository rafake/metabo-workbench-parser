"""Service for bulk directory ingestion."""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Set
from uuid import UUID

from sqlalchemy.orm import Session

from metaloader.models import Import, File
from metaloader.services.file_handler import FileHandler
from metaloader.services.import_service import ImportService
from metaloader.utils.type_detector import validate_file_extension

logger = logging.getLogger(__name__)


@dataclass
class IngestDirStats:
    """Statistics from directory ingestion."""
    import_id: Optional[UUID] = None
    root_path: str = ""
    files_found: int = 0
    files_processed: int = 0
    files_new: int = 0
    files_duplicate: int = 0
    files_skipped: int = 0
    files_error: int = 0
    errors: List[str] = field(default_factory=list)
    by_type: dict = field(default_factory=dict)  # detected_type -> count
    by_extension: dict = field(default_factory=dict)  # extension -> count


class IngestDirService:
    """Service for ingesting files from a directory recursively."""

    # Default allowed extensions
    DEFAULT_EXTENSIONS = {".txt", ".htm", ".html", ".csv", ".tsv", ".xlsx", ".xlsm", ".zip", ".pdf"}

    def __init__(self, db: Session):
        self.db = db
        self.import_service = ImportService(db)
        self.file_handler = FileHandler(db)

    def ingest_directory(
        self,
        directory: Path,
        import_notes: Optional[str] = None,
        include_extensions: Optional[Set[str]] = None,
        max_files: Optional[int] = None,
        dry_run: bool = False,
    ) -> IngestDirStats:
        """Ingest all files from a directory recursively.

        Args:
            directory: Root directory to scan
            import_notes: Optional notes for the import record
            include_extensions: Set of extensions to include (default: all allowed)
            max_files: Maximum number of files to process
            dry_run: If True, don't write to database

        Returns:
            IngestDirStats with operation statistics

        Raises:
            ValueError: If directory doesn't exist or is not a directory
        """
        if not directory.exists():
            raise ValueError(f"Directory not found: {directory}")

        if not directory.is_dir():
            raise ValueError(f"Path is not a directory: {directory}")

        directory = directory.absolute()
        stats = IngestDirStats(root_path=str(directory))

        # Determine extensions to process
        extensions = include_extensions or self.DEFAULT_EXTENSIONS
        extensions = {ext.lower() if ext.startswith('.') else f'.{ext}'.lower() for ext in extensions}

        # Collect files recursively
        files_to_process = self._collect_files(directory, extensions, max_files, stats)
        stats.files_found = len(files_to_process)

        if dry_run:
            logger.info(f"Dry run: would process {stats.files_found} files")
            return stats

        # Create import record
        import_record = self.import_service.create_import(
            root_path=str(directory),
            status="running"
        )
        stats.import_id = import_record.id
        logger.info(f"Created import record: {import_record.id}")

        # Process files
        for file_path in files_to_process:
            try:
                file_record, is_new = self.file_handler.process_file(
                    file_path, import_record.id, directory
                )
                stats.files_processed += 1

                if is_new:
                    stats.files_new += 1
                else:
                    stats.files_duplicate += 1

                # Track by type and extension
                detected_type = file_record.detected_type
                ext = file_record.ext
                stats.by_type[detected_type] = stats.by_type.get(detected_type, 0) + 1
                stats.by_extension[ext] = stats.by_extension.get(ext, 0) + 1

            except ValueError as e:
                stats.files_skipped += 1
                logger.warning(f"Skipped file {file_path}: {e}")
            except Exception as e:
                stats.files_error += 1
                error_msg = f"Error processing {file_path}: {e}"
                stats.errors.append(error_msg)
                logger.error(error_msg)

        # Finalize import
        if stats.files_error > 0:
            status = "failed" if stats.files_new == 0 else "success"
            notes = f"{stats.files_new} new, {stats.files_duplicate} dup, {stats.files_error} errors"
        else:
            status = "success"
            notes = f"{stats.files_new} new, {stats.files_duplicate} duplicates"

        if import_notes:
            notes = f"{import_notes}; {notes}"

        self.import_service.finalize_import(import_record.id, status, notes)
        logger.info(f"Finalized import {import_record.id}: {status}")

        return stats

    def _collect_files(
        self,
        directory: Path,
        extensions: Set[str],
        max_files: Optional[int],
        stats: IngestDirStats
    ) -> List[Path]:
        """Collect files from directory recursively.

        Args:
            directory: Directory to scan
            extensions: Set of allowed extensions
            max_files: Maximum number of files to collect
            stats: Stats object to update with skip counts

        Returns:
            List of file paths to process
        """
        files = []

        for file_path in directory.rglob("*"):
            if not file_path.is_file():
                continue

            # Check extension
            ext = file_path.suffix.lower()
            if ext not in extensions:
                stats.files_skipped += 1
                continue

            files.append(file_path)

            if max_files and len(files) >= max_files:
                logger.info(f"Reached max_files limit: {max_files}")
                break

        # Sort files for deterministic processing
        files.sort()
        return files

    def get_import_files(
        self,
        import_id: UUID,
        parse_status: Optional[str] = None
    ) -> List[File]:
        """Get files belonging to an import.

        Args:
            import_id: UUID of the import
            parse_status: Optional filter by parse_status

        Returns:
            List of File records
        """
        query = self.db.query(File).filter(File.import_id == import_id)

        if parse_status:
            query = query.filter(File.parse_status == parse_status)

        return query.order_by(File.created_at).all()
