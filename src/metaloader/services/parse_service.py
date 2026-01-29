"""Service for parsing and storing metabolomics data."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text

from metaloader.models import File, Study, Analysis, Sample, Feature, Measurement, SampleFactor
from metaloader.parsers.mwtab import MwTabParser, MwTabParseResult, is_mwtab_file

logger = logging.getLogger(__name__)

# Batch size for bulk inserts
BATCH_SIZE = 5000


@dataclass
class ParseStats:
    """Statistics from parsing operation."""
    study_id: Optional[str]
    analysis_id: Optional[str]
    samples_processed: int
    samples_created: int
    features_processed: int
    features_created: int
    measurements_processed: int
    measurements_inserted: int
    measurements_updated: int
    warnings_count: int


class ParseService:
    """Service for parsing metabolomics files and storing results."""

    def __init__(self, db: Session):
        self.db = db

    def parse_mwtab_file(
        self, 
        file_id: Optional[UUID] = None, 
        file_path: Optional[Path] = None,
        dry_run: bool = False
    ) -> ParseStats:
        """Parse mwTab file and store results in database.
        
        Args:
            file_id: UUID of file record in database (mutually exclusive with file_path)
            file_path: Direct path to file (mutually exclusive with file_id)
            dry_run: If True, don't write to database
            
        Returns:
            ParseStats with operation statistics
            
        Raises:
            ValueError: If file not found or not mwtab type
            FileNotFoundError: If file path doesn't exist
        """
        # Resolve file path
        resolved_path: Path
        file_record: Optional[File] = None
        
        if file_id:
            file_record = self.db.query(File).filter(File.id == file_id).first()
            if not file_record:
                raise ValueError(f"File not found: {file_id}")
            
            if file_record.detected_type != "mwtab":
                raise ValueError(
                    f"File is not mwtab type (detected: {file_record.detected_type})"
                )
            
            resolved_path = Path(file_record.path_abs)
        elif file_path:
            resolved_path = file_path
            if not is_mwtab_file(resolved_path):
                raise ValueError("File does not appear to be mwTab format")
        else:
            raise ValueError("Either file_id or file_path must be provided")
        
        if not resolved_path.exists():
            raise FileNotFoundError(f"File not found at path: {resolved_path}")
        
        logger.info(f"Parsing mwTab file: {resolved_path}")
        
        # Parse file
        parser = MwTabParser(resolved_path)
        result = parser.parse()
        
        # Validate metadata
        if not result.metadata.study_id:
            raise ValueError("Missing required metadata: study_id")
        
        if not result.metadata.analysis_id:
            raise ValueError("Missing required metadata: analysis_id")
        
        logger.info(
            f"Parsed: study_id={result.metadata.study_id}, "
            f"analysis_id={result.metadata.analysis_id}, "
            f"samples={len(result.samples)}, metabolites={len(result.metabolites)}"
        )
        
        # Store results (unless dry run)
        stats = ParseStats(
            study_id=result.metadata.study_id,
            analysis_id=result.metadata.analysis_id,
            samples_processed=len(result.samples),
            samples_created=0,
            features_processed=len(result.metabolites),
            features_created=0,
            measurements_processed=0,
            measurements_inserted=0,
            measurements_updated=0,
            warnings_count=len(result.warnings)
        )
        
        if not dry_run:
            self._store_parse_results(result, file_record, stats)
        else:
            logger.info("Dry run mode - not writing to database")
            # Count potential measurements
            stats.measurements_processed = len(result.metabolites) * len(result.sample_columns)
        
        return stats

    def _store_parse_results(
        self, result: MwTabParseResult, file_record: Optional[File], stats: ParseStats
    ) -> None:
        """Store parse results in database with upserts."""
        try:
            # 1. Upsert study
            study = self._upsert_study(result.metadata.study_id)
            
            # 2. Upsert analysis
            analysis = self._upsert_analysis(
                result.metadata.analysis_id, 
                study.id,
                file_record.id if file_record else None
            )
            
            # 3. Build sample label to sample_uid mapping
            # Include both SUBJECT_SAMPLE_FACTORS samples and metabolite data columns
            all_sample_labels: Set[str] = set()
            
            # From SUBJECT_SAMPLE_FACTORS
            sample_factors_map: Dict[str, str] = {}  # sample_label -> factors_raw
            for sample_data in result.samples:
                all_sample_labels.add(sample_data.sample_label)
                sample_factors_map[sample_data.sample_label] = sample_data.factors_raw
            
            # From MS_METABOLITE_DATA columns
            for label in result.sample_columns:
                all_sample_labels.add(label)
            
            # 4. Upsert samples
            sample_uid_map: Dict[str, str] = {}  # sample_label -> sample_uid
            samples_created = 0
            
            for sample_label in all_sample_labels:
                sample_uid = MwTabParser.create_sample_uid(
                    result.metadata.study_id, sample_label
                )
                factors_raw = sample_factors_map.get(sample_label)
                
                is_new = self._upsert_sample(
                    sample_uid, sample_label, study.id, factors_raw
                )
                sample_uid_map[sample_label] = sample_uid
                if is_new:
                    samples_created += 1
            
            stats.samples_created = samples_created
            
            # 5. Process metabolites and measurements
            if result.metabolites:
                features_created, measurements_inserted, measurements_updated = (
                    self._process_metabolite_data(
                        result, sample_uid_map, result.metadata.units
                    )
                )
                stats.features_created = features_created
                stats.measurements_inserted = measurements_inserted
                stats.measurements_updated = measurements_updated
                stats.measurements_processed = len(result.metabolites) * len(result.sample_columns)
            
            # Commit transaction
            self.db.commit()
            logger.info(
                f"Successfully stored: study={result.metadata.study_id}, "
                f"samples={stats.samples_created} new, "
                f"features={stats.features_created} new, "
                f"measurements={stats.measurements_inserted} inserted, "
                f"{stats.measurements_updated} updated"
            )
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error storing parse results: {e}")
            raise

    def _upsert_study(self, study_id: str) -> Study:
        """Upsert study record."""
        study = self.db.query(Study).filter(Study.study_id == study_id).first()
        
        if not study:
            study = Study(study_id=study_id)
            self.db.add(study)
            self.db.flush()
            logger.debug(f"Created study: {study_id}")
        else:
            logger.debug(f"Using existing study: {study_id}")
        
        return study

    def _upsert_analysis(
        self, analysis_id: str, study_pk: UUID, file_id: Optional[UUID] = None
    ) -> Analysis:
        """Upsert analysis record."""
        analysis = (
            self.db.query(Analysis)
            .filter(
                Analysis.analysis_id == analysis_id,
                Analysis.study_pk == study_pk
            )
            .first()
        )
        
        if not analysis:
            analysis = Analysis(
                analysis_id=analysis_id, 
                study_pk=study_pk,
                file_id=file_id
            )
            self.db.add(analysis)
            self.db.flush()
            logger.debug(f"Created analysis: {analysis_id}")
        else:
            # Update file_id if provided
            if file_id and not analysis.file_id:
                analysis.file_id = file_id
            logger.debug(f"Using existing analysis: {analysis_id}")
        
        return analysis

    def _upsert_sample(
        self, 
        sample_uid: str, 
        sample_label: str, 
        study_pk: UUID,
        factors_raw: Optional[str] = None
    ) -> bool:
        """Upsert sample record.
        
        Returns:
            True if sample was created, False if it already existed
        """
        sample = (
            self.db.query(Sample)
            .filter(Sample.sample_uid == sample_uid)
            .first()
        )
        
        if not sample:
            sample = Sample(
                sample_uid=sample_uid,
                sample_label=sample_label,
                study_pk=study_pk,
                factors_raw=factors_raw
            )
            self.db.add(sample)
            self.db.flush()
            logger.debug(f"Created sample: {sample_uid}")
            return True
        else:
            # Update if changed
            if sample.sample_label != sample_label:
                sample.sample_label = sample_label
            if factors_raw and sample.factors_raw != factors_raw:
                sample.factors_raw = factors_raw
            logger.debug(f"Using existing sample: {sample_uid}")
            return False

    def _process_metabolite_data(
        self,
        result: MwTabParseResult,
        sample_uid_map: Dict[str, str],
        units: Optional[str]
    ) -> tuple[int, int, int]:
        """Process metabolite data with batch upserts.
        
        Returns:
            Tuple of (features_created, measurements_inserted, measurements_updated)
        """
        features_created = 0
        measurements_inserted = 0
        measurements_updated = 0
        
        # Track feature_uid -> feature_uid mapping
        feature_uid_set: Set[str] = set()
        
        # First pass: create all features
        for metabolite in result.metabolites:
            feature_uid = MwTabParser.create_feature_uid(
                result.metadata.analysis_id, metabolite.metabolite_name
            )
            
            if feature_uid not in feature_uid_set:
                is_new = self._upsert_feature(
                    feature_uid, metabolite.metabolite_name
                )
                feature_uid_set.add(feature_uid)
                if is_new:
                    features_created += 1
        
        # Flush to ensure features exist
        self.db.flush()
        
        # Second pass: batch insert measurements
        measurement_batch: List[dict] = []
        
        for metabolite in result.metabolites:
            feature_uid = MwTabParser.create_feature_uid(
                result.metadata.analysis_id, metabolite.metabolite_name
            )
            
            for sample_label, value in metabolite.values.items():
                sample_uid = sample_uid_map.get(sample_label)
                if not sample_uid:
                    logger.warning(f"Sample not found for label: {sample_label}")
                    continue
                
                measurement_batch.append({
                    'sample_uid': sample_uid,
                    'feature_uid': feature_uid,
                    'value': value,
                    'unit': units
                })
                
                # Process batch when full
                if len(measurement_batch) >= BATCH_SIZE:
                    ins, upd = self._batch_upsert_measurements(measurement_batch)
                    measurements_inserted += ins
                    measurements_updated += upd
                    measurement_batch = []
        
        # Process remaining batch
        if measurement_batch:
            ins, upd = self._batch_upsert_measurements(measurement_batch)
            measurements_inserted += ins
            measurements_updated += upd
        
        return features_created, measurements_inserted, measurements_updated

    def _upsert_feature(self, feature_uid: str, name_raw: str) -> bool:
        """Upsert feature record.
        
        Returns:
            True if feature was created, False if it already existed
        """
        feature = (
            self.db.query(Feature)
            .filter(Feature.feature_uid == feature_uid)
            .first()
        )
        
        if not feature:
            feature = Feature(
                feature_uid=feature_uid,
                feature_type='metabolite',
                name_raw=name_raw
            )
            self.db.add(feature)
            logger.debug(f"Created feature: {feature_uid}")
            return True
        else:
            # Update name_raw if it was empty
            if not feature.name_raw and name_raw:
                feature.name_raw = name_raw
            return False

    def _batch_upsert_measurements(self, batch: List[dict]) -> tuple[int, int]:
        """Batch upsert measurements using PostgreSQL INSERT ON CONFLICT.
        
        Returns:
            Tuple of (inserted_count, updated_count)
        """
        if not batch:
            return 0, 0
        
        # Use PostgreSQL INSERT ... ON CONFLICT DO UPDATE
        stmt = insert(Measurement).values(batch)
        
        # On conflict, update value only if new value is not NULL
        stmt = stmt.on_conflict_do_update(
            constraint='uq_measurement_sample_feature',
            set_={
                'value': text('COALESCE(EXCLUDED.value, measurements.value)'),
                'unit': text('COALESCE(EXCLUDED.unit, measurements.unit)')
            }
        )
        
        # Execute and get row count
        result = self.db.execute(stmt)
        
        # Note: PostgreSQL doesn't easily distinguish inserts vs updates in ON CONFLICT
        # We'll estimate based on affected rows
        # For simplicity, count all as "processed" - exact split would require extra queries
        total = result.rowcount if result.rowcount else len(batch)
        
        # Since we can't easily tell inserts from updates, we'll count as inserts
        # The actual split would require checking existing records first
        return total, 0

    def _upsert_sample_factors(
        self, result: MwTabParseResult, sample_uid_map: Dict[str, str]
    ) -> int:
        """Upsert sample factors from parsed data.
        
        Returns:
            Count of factor entries written
        """
        factors_written = 0
        
        for sample_data in result.samples:
            sample_uid = sample_uid_map.get(sample_data.sample_label)
            if not sample_uid:
                continue
            
            for key, value in sample_data.factors.items():
                stmt = insert(SampleFactor).values(
                    sample_uid=sample_uid,
                    factor_key=key,
                    factor_value=value
                )
                
                stmt = stmt.on_conflict_do_update(
                    constraint='uq_sample_factor',
                    set_={'factor_value': value}
                )
                
                self.db.execute(stmt)
                factors_written += 1
        
        return factors_written
