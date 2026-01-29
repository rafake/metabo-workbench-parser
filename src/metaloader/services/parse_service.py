"""Service for parsing and storing metabolomics data."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from metaloader.models import File, Study, Analysis, Sample, SampleFactor
from metaloader.parsers.mwtab import MwTabParser, MwTabParseResult

logger = logging.getLogger(__name__)


@dataclass
class ParseStats:
    """Statistics from parsing operation."""
    study_id: Optional[str]
    analysis_id: Optional[str]
    samples_processed: int
    factors_written: int
    warnings_count: int
    skipped_count: int


class ParseService:
    """Service for parsing metabolomics files and storing results."""

    def __init__(self, db: Session):
        self.db = db

    def parse_mwtab_file(self, file_id: UUID, dry_run: bool = False) -> ParseStats:
        """Parse mwTab file and store results in database.
        
        Args:
            file_id: UUID of file record in database
            dry_run: If True, don't write to database
            
        Returns:
            ParseStats with operation statistics
            
        Raises:
            ValueError: If file not found or not mwtab type
            FileNotFoundError: If file path doesn't exist
        """
        # Get file record
        file_record = self.db.query(File).filter(File.id == file_id).first()
        if not file_record:
            raise ValueError(f"File not found: {file_id}")
        
        if file_record.detected_type != "mwtab":
            raise ValueError(
                f"File is not mwtab type (detected: {file_record.detected_type})"
            )
        
        file_path = Path(file_record.path_abs)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found at path: {file_path}")
        
        logger.info(f"Parsing mwTab file: {file_path}")
        
        # Parse file
        parser = MwTabParser(file_path)
        result = parser.parse()
        
        # Validate metadata
        if not result.metadata.study_id or not result.metadata.analysis_id:
            raise ValueError(
                f"Missing required metadata. "
                f"study_id={result.metadata.study_id}, "
                f"analysis_id={result.metadata.analysis_id}"
            )
        
        logger.info(
            f"Parsed: study_id={result.metadata.study_id}, "
            f"analysis_id={result.metadata.analysis_id}, "
            f"samples={len(result.samples)}"
        )
        
        # Store results (unless dry run)
        factors_written = 0
        skipped = 0
        
        if not dry_run:
            factors_written, skipped = self._store_parse_results(result)
        else:
            logger.info("Dry run mode - not writing to database")
            # Count factors for stats
            for sample_data in result.samples:
                factors_written += len(sample_data.factors)
        
        stats = ParseStats(
            study_id=result.metadata.study_id,
            analysis_id=result.metadata.analysis_id,
            samples_processed=len(result.samples),
            factors_written=factors_written,
            warnings_count=len(result.warnings),
            skipped_count=skipped
        )
        
        return stats

    def _store_parse_results(self, result: MwTabParseResult) -> tuple[int, int]:
        """Store parse results in database with upserts.
        
        Args:
            result: Parse result to store
            
        Returns:
            Tuple of (factors_written, skipped_count)
        """
        try:
            # Upsert study
            study = self._upsert_study(result.metadata.study_id)
            
            # Upsert analysis
            analysis = self._upsert_analysis(
                result.metadata.analysis_id, study.id
            )
            
            # Process samples and factors
            factors_written = 0
            skipped = 0
            
            for sample_data in result.samples:
                try:
                    # Create sample_uid
                    sample_uid = MwTabParser.create_sample_uid(
                        result.metadata.study_id,
                        result.metadata.analysis_id,
                        sample_data.sample_label
                    )
                    
                    # Upsert sample
                    sample = self._upsert_sample(
                        sample_uid,
                        sample_data.sample_label,
                        study.id
                    )
                    
                    # Upsert factors
                    for key, value in sample_data.factors.items():
                        self._upsert_sample_factor(sample_uid, key, value)
                        factors_written += 1
                    
                except Exception as e:
                    logger.error(
                        f"Error processing sample '{sample_data.sample_label}': {e}"
                    )
                    skipped += 1
                    continue
            
            # Commit transaction
            self.db.commit()
            logger.info(
                f"Successfully stored: "
                f"study={result.metadata.study_id}, "
                f"analysis={result.metadata.analysis_id}, "
                f"factors={factors_written}"
            )
            
            return factors_written, skipped
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error storing parse results: {e}")
            raise

    def _upsert_study(self, study_id: str) -> Study:
        """Upsert study record.
        
        Args:
            study_id: Study ID
            
        Returns:
            Study record
        """
        # Try to find existing
        study = self.db.query(Study).filter(Study.study_id == study_id).first()
        
        if not study:
            # Create new
            study = Study(study_id=study_id)
            self.db.add(study)
            self.db.flush()
            logger.debug(f"Created study: {study_id}")
        else:
            logger.debug(f"Using existing study: {study_id}")
        
        return study

    def _upsert_analysis(self, analysis_id: str, study_pk: UUID) -> Analysis:
        """Upsert analysis record.
        
        Args:
            analysis_id: Analysis ID
            study_pk: Study primary key
            
        Returns:
            Analysis record
        """
        # Try to find existing
        analysis = (
            self.db.query(Analysis)
            .filter(
                Analysis.analysis_id == analysis_id,
                Analysis.study_pk == study_pk
            )
            .first()
        )
        
        if not analysis:
            # Create new
            analysis = Analysis(analysis_id=analysis_id, study_pk=study_pk)
            self.db.add(analysis)
            self.db.flush()
            logger.debug(f"Created analysis: {analysis_id}")
        else:
            logger.debug(f"Using existing analysis: {analysis_id}")
        
        return analysis

    def _upsert_sample(
        self, sample_uid: str, sample_label: str, study_pk: UUID
    ) -> Sample:
        """Upsert sample record.
        
        Args:
            sample_uid: Unique sample identifier
            sample_label: Original sample label
            study_pk: Study primary key
            
        Returns:
            Sample record
        """
        # Try to find existing
        sample = (
            self.db.query(Sample)
            .filter(Sample.sample_uid == sample_uid)
            .first()
        )
        
        if not sample:
            # Create new
            sample = Sample(
                sample_uid=sample_uid,
                sample_label=sample_label,
                study_pk=study_pk
            )
            self.db.add(sample)
            self.db.flush()
            logger.debug(f"Created sample: {sample_uid}")
        else:
            # Update label if changed
            if sample.sample_label != sample_label:
                sample.sample_label = sample_label
                logger.debug(f"Updated sample label: {sample_uid}")
        
        return sample

    def _upsert_sample_factor(
        self, sample_uid: str, factor_key: str, factor_value: str
    ) -> None:
        """Upsert sample factor using PostgreSQL ON CONFLICT.
        
        Args:
            sample_uid: Sample UID
            factor_key: Factor key
            factor_value: Factor value
        """
        # Use PostgreSQL INSERT ... ON CONFLICT DO UPDATE
        stmt = insert(SampleFactor).values(
            sample_uid=sample_uid,
            factor_key=factor_key,
            factor_value=factor_value
        )
        
        stmt = stmt.on_conflict_do_update(
            constraint='uq_sample_factor',
            set_={'factor_value': factor_value}
        )
        
        self.db.execute(stmt)
        logger.debug(f"Upserted factor: {sample_uid} -> {factor_key}={factor_value}")
