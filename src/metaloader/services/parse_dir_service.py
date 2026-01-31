"""Service for bulk parsing of files."""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set
from uuid import UUID

from sqlalchemy.orm import Session

from metaloader.models import File
from metaloader.services.parse_service import ParseService, ParseStats
from metaloader.services.parse_ms_service import ParseMSService, ParseMSStats
from metaloader.services.parse_nmr_service import ParseNMRService, ParseNMRStats
from metaloader.utils.type_detector import detect_file_type

logger = logging.getLogger(__name__)


@dataclass
class ParseDirStats:
    """Statistics from directory/bulk parsing."""
    files_total: int = 0
    files_parsed: int = 0
    files_success: int = 0
    files_failed: int = 0
    files_skipped: int = 0  # unsupported type or already parsed
    samples_created: int = 0
    features_created: int = 0
    measurements_inserted: int = 0
    errors: List[str] = field(default_factory=list)
    by_type: Dict[str, int] = field(default_factory=dict)  # detected_type -> count parsed


# Mapping of detected_type to parser type
PARSABLE_TYPES = {
    "mwtab": "mwtab",  # Handles both MS and NMR text files
    "mwtab_ms": "mwtab_ms",  # MS-specific parsing
    "mwtab_nmr_binned": "mwtab_nmr_binned",  # NMR binned data
}


class ParseDirService:
    """Service for bulk parsing of files from directory or import."""

    def __init__(self, db: Session):
        self.db = db
        self.parse_service = ParseService(db)
        self.parse_ms_service = ParseMSService(db)
        self.parse_nmr_service = ParseNMRService(db)

    def parse_directory(
        self,
        directory: Path,
        only_types: Optional[Set[str]] = None,
        skip_types: Optional[Set[str]] = None,
        fail_fast: bool = False,
        max_files: Optional[int] = None,
        dry_run: bool = False,
    ) -> ParseDirStats:
        """Parse all supported files in a directory.

        Args:
            directory: Directory to scan for parsable files
            only_types: Only parse these detected_types (e.g., {"mwtab"})
            skip_types: Skip these detected_types
            fail_fast: Stop on first error
            max_files: Maximum number of files to parse
            dry_run: If True, don't write to database

        Returns:
            ParseDirStats with operation statistics

        Raises:
            ValueError: If directory doesn't exist
        """
        if not directory.exists():
            raise ValueError(f"Directory not found: {directory}")

        if not directory.is_dir():
            raise ValueError(f"Path is not a directory: {directory}")

        directory = directory.absolute()
        stats = ParseDirStats()

        # Collect parsable files
        files_to_parse = []
        for file_path in directory.rglob("*"):
            if not file_path.is_file():
                continue

            detected_type = detect_file_type(file_path)

            # Check if type is parsable
            if detected_type not in PARSABLE_TYPES:
                continue

            # Apply filters
            if only_types and detected_type not in only_types:
                stats.files_skipped += 1
                continue

            if skip_types and detected_type in skip_types:
                stats.files_skipped += 1
                continue

            files_to_parse.append((file_path, detected_type))

            if max_files and len(files_to_parse) >= max_files:
                logger.info(f"Reached max_files limit: {max_files}")
                break

        stats.files_total = len(files_to_parse)

        if dry_run:
            logger.info(f"Dry run: would parse {stats.files_total} files")
            for _, dt in files_to_parse:
                stats.by_type[dt] = stats.by_type.get(dt, 0) + 1
            return stats

        # Parse each file
        for file_path, detected_type in files_to_parse:
            try:
                parse_stats = self._parse_file(file_path, detected_type)
                stats.files_parsed += 1
                stats.files_success += 1
                stats.by_type[detected_type] = stats.by_type.get(detected_type, 0) + 1

                # Aggregate stats
                stats.samples_created += getattr(parse_stats, 'samples_created', 0)
                stats.features_created += getattr(parse_stats, 'features_created', 0)
                stats.measurements_inserted += getattr(parse_stats, 'measurements_inserted', 0)

            except Exception as e:
                stats.files_failed += 1
                error_msg = f"Error parsing {file_path}: {e}"
                stats.errors.append(error_msg)
                logger.error(error_msg)

                if fail_fast:
                    raise RuntimeError(error_msg) from e

        return stats

    def parse_import(
        self,
        import_id: UUID,
        only_types: Optional[Set[str]] = None,
        skip_types: Optional[Set[str]] = None,
        fail_fast: bool = False,
        max_files: Optional[int] = None,
        dry_run: bool = False,
    ) -> ParseDirStats:
        """Parse all pending files from an import.

        Args:
            import_id: UUID of the import to process
            only_types: Only parse these detected_types
            skip_types: Skip these detected_types
            fail_fast: Stop on first error
            max_files: Maximum number of files to parse
            dry_run: If True, don't write to database

        Returns:
            ParseDirStats with operation statistics

        Raises:
            ValueError: If import not found
        """
        from metaloader.models import Import

        # Verify import exists
        import_record = self.db.query(Import).filter(Import.id == import_id).first()
        if not import_record:
            raise ValueError(f"Import not found: {import_id}")

        stats = ParseDirStats()

        # Query files with parse_status='pending' or failed to retry
        query = (
            self.db.query(File)
            .filter(File.import_id == import_id)
            .filter(File.parse_status.in_(['pending', 'failed']))
        )

        # Apply type filters
        if only_types:
            query = query.filter(File.detected_type.in_(only_types))

        if skip_types:
            query = query.filter(~File.detected_type.in_(skip_types))

        # Order and limit
        query = query.order_by(File.created_at)

        if max_files:
            query = query.limit(max_files)

        files = query.all()
        stats.files_total = len(files)

        if dry_run:
            logger.info(f"Dry run: would parse {stats.files_total} files from import {import_id}")
            for f in files:
                if f.detected_type in PARSABLE_TYPES:
                    stats.by_type[f.detected_type] = stats.by_type.get(f.detected_type, 0) + 1
                else:
                    stats.files_skipped += 1
            return stats

        # Parse each file
        for file_record in files:
            detected_type = file_record.detected_type

            # Check if type is parsable
            if detected_type not in PARSABLE_TYPES:
                self._update_file_status(file_record, "skipped", "Unsupported file type")
                stats.files_skipped += 1
                continue

            try:
                file_path = Path(file_record.path_abs)
                if not file_path.exists():
                    raise FileNotFoundError(f"File not found: {file_path}")

                parse_stats = self._parse_file_with_id(file_path, detected_type, file_record.id)
                self._update_file_status(file_record, "success")

                stats.files_parsed += 1
                stats.files_success += 1
                stats.by_type[detected_type] = stats.by_type.get(detected_type, 0) + 1

                # Aggregate stats
                stats.samples_created += getattr(parse_stats, 'samples_created', 0)
                stats.features_created += getattr(parse_stats, 'features_created', 0)
                stats.measurements_inserted += getattr(parse_stats, 'measurements_inserted', 0)

            except Exception as e:
                error_msg = str(e)
                self._update_file_status(file_record, "failed", error_msg)

                stats.files_failed += 1
                full_error = f"Error parsing {file_record.filename}: {error_msg}"
                stats.errors.append(full_error)
                logger.error(full_error)

                if fail_fast:
                    raise RuntimeError(full_error) from e

        return stats

    def _parse_file(self, file_path: Path, detected_type: str):
        """Parse a file by its detected type (no file_id).

        Returns:
            ParseStats or MSParseStats or NMRParseStats
        """
        if detected_type == "mwtab":
            return self.parse_service.parse_mwtab_file(file_path=file_path)
        elif detected_type == "mwtab_ms":
            return self.parse_ms_service.parse_file(file_path=file_path)
        elif detected_type == "mwtab_nmr_binned":
            return self.parse_nmr_service.parse_file(file_path=file_path)
        else:
            raise ValueError(f"Unsupported file type: {detected_type}")

    def _parse_file_with_id(self, file_path: Path, detected_type: str, file_id: UUID):
        """Parse a file by its detected type (with file_id for tracking).

        Returns:
            ParseStats or MSParseStats or NMRParseStats
        """
        if detected_type == "mwtab":
            return self.parse_service.parse_mwtab_file(file_id=file_id)
        elif detected_type == "mwtab_ms":
            return self.parse_ms_service.parse_file(file_path=file_path, file_id=file_id)
        elif detected_type == "mwtab_nmr_binned":
            return self.parse_nmr_service.parse_file(file_path=file_path, file_id=file_id)
        else:
            raise ValueError(f"Unsupported file type: {detected_type}")

    def _update_file_status(
        self,
        file_record: File,
        status: str,
        error: Optional[str] = None
    ) -> None:
        """Update file's parse status.

        Args:
            file_record: File record to update
            status: New status (pending, success, failed, skipped)
            error: Optional error message
        """
        file_record.parse_status = status
        file_record.parse_error = error
        file_record.parsed_at = datetime.now(timezone.utc)
        self.db.commit()
