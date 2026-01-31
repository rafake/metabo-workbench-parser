"""Service for tagging files with inferred categories."""

import logging
from dataclasses import dataclass, field
from typing import List, Optional
from uuid import UUID

from sqlalchemy.orm import Session

from metaloader.models import File
from metaloader.utils.tagger import infer_all_tags

logger = logging.getLogger(__name__)


@dataclass
class TagStats:
    """Statistics from tagging operation."""
    files_processed: int = 0
    files_updated: int = 0
    files_skipped: int = 0  # Already had values and --overwrite not set
    device_set: int = 0
    exposure_set: int = 0
    sample_type_set: int = 0
    platform_set: int = 0
    warnings: List[str] = field(default_factory=list)


class TaggerService:
    """Service for tagging files with category values."""

    def __init__(self, db: Session):
        self.db = db

    def tag_files(
        self,
        import_id: Optional[UUID] = None,
        file_id: Optional[UUID] = None,
        tag_all: bool = False,
        overwrite: bool = False,
        dry_run: bool = False,
    ) -> TagStats:
        """Tag files with inferred category values.

        Args:
            import_id: Tag only files from this import
            file_id: Tag only this specific file
            tag_all: Tag all files in database
            overwrite: Overwrite existing values (otherwise skip files with values)
            dry_run: Don't write to database, just show what would change

        Returns:
            TagStats with operation statistics

        Raises:
            ValueError: If no filter specified (must use one of import_id, file_id, or tag_all)
        """
        if not any([import_id, file_id, tag_all]):
            raise ValueError(
                "Must specify one of: --import-id, --file-id, or --all"
            )

        stats = TagStats()

        # Build query
        query = self.db.query(File)

        if file_id:
            query = query.filter(File.id == file_id)
        elif import_id:
            query = query.filter(File.import_id == import_id)
        # else: tag_all - no filter

        # Get files
        files = query.all()
        stats.files_processed = len(files)

        if stats.files_processed == 0:
            logger.info("No files found to tag")
            return stats

        logger.info(f"Processing {stats.files_processed} files")

        # Process each file
        for file_record in files:
            updated = self._tag_file(file_record, overwrite, dry_run, stats)
            if updated:
                stats.files_updated += 1
            else:
                stats.files_skipped += 1

        # Commit if not dry run
        if not dry_run and stats.files_updated > 0:
            self.db.commit()
            logger.info(f"Committed changes for {stats.files_updated} files")

        return stats

    def _tag_file(
        self,
        file_record: File,
        overwrite: bool,
        dry_run: bool,
        stats: TagStats
    ) -> bool:
        """Tag a single file with inferred categories.

        Args:
            file_record: File to tag
            overwrite: Overwrite existing values
            dry_run: Don't actually update
            stats: Stats object to update

        Returns:
            True if file was updated, False if skipped
        """
        # Check if file already has all values and overwrite not set
        has_device = file_record.device is not None
        has_exposure = file_record.exposure is not None
        has_sample_type = file_record.sample_type is not None
        has_platform = file_record.platform is not None

        if not overwrite and all([has_device, has_exposure, has_sample_type, has_platform]):
            logger.debug(f"Skipping {file_record.filename}: already tagged")
            return False

        # Infer tags
        tags = infer_all_tags(
            path_rel=file_record.path_rel,
            filename=file_record.filename,
            detected_type=file_record.detected_type
        )

        # Record warnings
        for warning in tags.warnings:
            warning_msg = f"{file_record.filename}: {warning}"
            stats.warnings.append(warning_msg)
            logger.warning(warning_msg)

        # Track what we're updating
        updated = False

        # Update device
        if tags.device is not None:
            if overwrite or not has_device:
                if not dry_run:
                    file_record.device = tags.device
                stats.device_set += 1
                updated = True
                logger.debug(f"{file_record.filename}: device -> {tags.device}")

        # Update exposure
        if tags.exposure is not None:
            if overwrite or not has_exposure:
                if not dry_run:
                    file_record.exposure = tags.exposure
                stats.exposure_set += 1
                updated = True
                logger.debug(f"{file_record.filename}: exposure -> {tags.exposure}")

        # Update sample_type
        if tags.sample_type is not None:
            if overwrite or not has_sample_type:
                if not dry_run:
                    file_record.sample_type = tags.sample_type
                stats.sample_type_set += 1
                updated = True
                logger.debug(f"{file_record.filename}: sample_type -> {tags.sample_type}")

        # Update platform
        if tags.platform is not None:
            if overwrite or not has_platform:
                if not dry_run:
                    file_record.platform = tags.platform
                stats.platform_set += 1
                updated = True
                logger.debug(f"{file_record.filename}: platform -> {tags.platform}")

        return updated
