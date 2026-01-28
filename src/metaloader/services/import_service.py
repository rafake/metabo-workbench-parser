"""Import management service."""

import logging
from typing import Optional
from uuid import UUID

from sqlalchemy.orm import Session

from metaloader.models import Import

logger = logging.getLogger(__name__)


class ImportService:
    """Service for managing import records."""

    def __init__(self, db: Session):
        self.db = db

    def create_import(
        self, root_path: Optional[str] = None, status: str = "running"
    ) -> Import:
        """Create a new import record.
        
        Args:
            root_path: Optional root path for this import
            status: Initial status (default: "running")
            
        Returns:
            Created Import record
        """
        import_record = Import(root_path=root_path, status=status)
        self.db.add(import_record)
        self.db.commit()
        self.db.refresh(import_record)
        logger.info(f"Created import record: {import_record.id}")
        return import_record

    def get_import(self, import_id: UUID) -> Optional[Import]:
        """Get import record by ID.
        
        Args:
            import_id: UUID of the import
            
        Returns:
            Import record if found, None otherwise
        """
        return self.db.query(Import).filter(Import.id == import_id).first()

    def update_status(
        self, import_id: UUID, status: str, notes: Optional[str] = None
    ) -> Import:
        """Update import status and notes.
        
        Args:
            import_id: UUID of the import
            status: New status ("running", "success", or "failed")
            notes: Optional notes to add
            
        Returns:
            Updated Import record
            
        Raises:
            ValueError: If import not found or status is invalid
        """
        valid_statuses = {"running", "success", "failed"}
        if status not in valid_statuses:
            raise ValueError(
                f"Invalid status: {status}. Must be one of: {', '.join(valid_statuses)}"
            )

        import_record = self.get_import(import_id)
        if not import_record:
            raise ValueError(f"Import not found: {import_id}")

        import_record.status = status
        if notes is not None:
            import_record.notes = notes

        self.db.commit()
        self.db.refresh(import_record)
        logger.info(f"Updated import {import_id} status to: {status}")
        return import_record

    def finalize_import(self, import_id: UUID, status: str, notes: str) -> Import:
        """Finalize import with status and notes.
        
        Args:
            import_id: UUID of the import
            status: Final status ("success" or "failed")
            notes: Notes about the import
            
        Returns:
            Updated Import record
        """
        return self.update_status(import_id, status, notes)
