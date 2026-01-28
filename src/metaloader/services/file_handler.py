"""File handling service for processing and storing file records."""

import logging
from pathlib import Path
from typing import Optional, Tuple
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from metaloader.models import File
from metaloader.utils.hashing import calculate_sha256
from metaloader.utils.type_detector import detect_file_type, validate_file_extension

logger = logging.getLogger(__name__)


class FileHandler:
    """Handler for file processing and storage."""

    def __init__(self, db: Session):
        self.db = db

    def check_duplicate(self, sha256: str, size_bytes: int) -> Optional[File]:
        """Check if file already exists in database by sha256 and size.
        
        Args:
            sha256: SHA256 hash of the file
            size_bytes: File size in bytes
            
        Returns:
            Existing File record if found, None otherwise
        """
        return (
            self.db.query(File)
            .filter(File.sha256 == sha256, File.size_bytes == size_bytes)
            .first()
        )

    def process_file(
        self, file_path: Path, import_id: UUID, root_path: Optional[Path] = None
    ) -> Tuple[File, bool]:
        """Process a file and create database record.
        
        Args:
            file_path: Absolute path to the file
            import_id: UUID of the import this file belongs to
            root_path: Optional root path for calculating relative path
            
        Returns:
            Tuple of (File record, is_new) where is_new is False if file was duplicate
            
        Raises:
            FileNotFoundError: If file does not exist
            ValueError: If file extension is not allowed
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if not file_path.is_file():
            raise ValueError(f"Path is not a file: {file_path}")

        # Validate extension
        if not validate_file_extension(file_path):
            raise ValueError(
                f"File extension '{file_path.suffix}' not allowed. "
                "Allowed: .txt, .htm, .html, .csv, .tsv, .xlsx, .xlsm, .zip, .pdf"
            )

        # Calculate file properties
        size_bytes = file_path.stat().st_size
        sha256 = calculate_sha256(file_path)
        detected_type = detect_file_type(file_path)

        # Check for duplicates
        existing_file = self.check_duplicate(sha256, size_bytes)
        if existing_file:
            logger.info(
                f"File already exists in database: {file_path.name} "
                f"(existing file_id: {existing_file.id})"
            )
            return existing_file, False

        # Calculate relative path if root_path provided
        path_rel = None
        if root_path:
            try:
                path_rel = str(file_path.relative_to(root_path))
            except ValueError:
                # File is not relative to root_path
                path_rel = None

        # Create new file record
        file_record = File(
            import_id=import_id,
            path_rel=path_rel,
            path_abs=str(file_path.absolute()),
            filename=file_path.name,
            ext=file_path.suffix.lower(),
            size_bytes=size_bytes,
            sha256=sha256,
            detected_type=detected_type,
        )

        try:
            self.db.add(file_record)
            self.db.commit()
            self.db.refresh(file_record)
            logger.info(
                f"Successfully processed file: {file_path.name} "
                f"(file_id: {file_record.id}, type: {detected_type})"
            )
            return file_record, True
        except IntegrityError as e:
            self.db.rollback()
            # Race condition: another process inserted the same file
            existing_file = self.check_duplicate(sha256, size_bytes)
            if existing_file:
                logger.info(
                    f"File was inserted by another process: {file_path.name} "
                    f"(existing file_id: {existing_file.id})"
                )
                return existing_file, False
            # Some other integrity error
            raise e
