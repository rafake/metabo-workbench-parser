"""Service for parsing and storing NMR binned data."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Set
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from metaloader.models import Study, Analysis, Sample, Feature, Measurement
from metaloader.parsers.mwtab_nmr import MwTabNMRParser, NMRMetadata, NMRSampleFactorInfo

logger = logging.getLogger(__name__)

# Batch size for bulk inserts (keep small to avoid PostgreSQL lock exhaustion)
BATCH_SIZE = 1000


@dataclass
class ParseNMRStats:
    """Statistics from NMR parsing operation."""
    study_id: Optional[str] = None
    analysis_id: Optional[str] = None
    samples_processed: int = 0
    samples_created: int = 0
    features_processed: int = 0
    features_created: int = 0
    measurements_processed: int = 0
    measurements_inserted: int = 0
    measurements_skipped: int = 0
    warnings_count: int = 0


class ParseNMRService:
    """Service for parsing NMR binned data and storing results."""

    def __init__(self, db: Session):
        self.db = db

    def parse_file(
        self,
        file_path: Path,
        file_id: Optional[UUID] = None,
        dry_run: bool = False
    ) -> ParseNMRStats:
        """Parse mwTab file for NMR_BINNED_DATA and store results.

        Args:
            file_path: Path to mwTab file
            file_id: UUID of file record in database (optional)
            dry_run: If True, don't write to database

        Returns:
            ParseNMRStats with operation statistics
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        logger.info(f"Parsing NMR binned data from: {file_path}")

        # Initialize parser
        parser = MwTabNMRParser(file_path)

        # First pass: metadata and sample factors
        metadata, sample_factors = parser.parse_metadata_and_samples()

        if not metadata.study_id:
            raise ValueError("Missing required metadata: study_id")
        if not metadata.analysis_id:
            raise ValueError("Missing required metadata: analysis_id")

        # Get unique samples from NMR data
        nmr_samples = parser.get_unique_sample_uids(metadata, sample_factors)

        logger.info(
            f"Parsed metadata: study={metadata.study_id}, analysis={metadata.analysis_id}, "
            f"units={metadata.units}, samples={len(nmr_samples)}"
        )

        stats = ParseNMRStats(
            study_id=metadata.study_id,
            analysis_id=metadata.analysis_id,
            samples_processed=len(nmr_samples),
            warnings_count=len(parser.warnings)
        )

        if dry_run:
            # Dry run: count measurements
            for _ in parser.iter_measurements(metadata, sample_factors):
                stats.measurements_processed += 1
            logger.info(f"Dry run: {stats.measurements_processed} measurements found")
            return stats

        # Store results
        try:
            self._store_results(
                metadata=metadata,
                nmr_samples=nmr_samples,
                parser=parser,
                sample_factors=sample_factors,
                file_id=file_id,
                stats=stats
            )
            self.db.commit()  # Final commit for any remaining data
            logger.info(
                f"Successfully stored: samples={stats.samples_created} new, "
                f"features={stats.features_created} new, "
                f"measurements={stats.measurements_inserted} inserted, "
                f"{stats.measurements_skipped} skipped (conflict)"
            )
        except Exception as e:
            # Note: partial data may have been committed in batches
            self.db.rollback()
            logger.error(f"Error storing results: {e}")
            raise

        return stats

    def _store_results(
        self,
        metadata: NMRMetadata,
        nmr_samples: Dict[str, NMRSampleFactorInfo],
        parser: MwTabNMRParser,
        sample_factors: Dict[str, NMRSampleFactorInfo],
        file_id: Optional[UUID],
        stats: ParseNMRStats
    ) -> None:
        """Store parsed results in database."""
        # 1. Upsert study
        study = self._upsert_study(metadata.study_id)

        # 2. Upsert analysis
        analysis = self._upsert_analysis(metadata.analysis_id, study.id, file_id)

        # 3. Upsert samples
        for sample_uid, sample_info in nmr_samples.items():
            is_new = self._upsert_sample(
                sample_uid=sample_uid,
                sample_label=sample_info.sample_label,
                study_pk=study.id,
                factors_raw=sample_info.factors_raw or None
            )
            if is_new:
                stats.samples_created += 1

        # Flush samples before measurements (FK constraint)
        self.db.flush()

        # 4. Process measurements in batches
        # Track features we've seen
        feature_uids_seen: Set[str] = set()
        feature_batch: list = []
        measurement_batch: list = []
        batch_count = 0

        for measurement in parser.iter_measurements(metadata, sample_factors):
            stats.measurements_processed += 1

            # Create/track feature
            if measurement.feature_uid not in feature_uids_seen:
                feature_batch.append({
                    'feature_uid': measurement.feature_uid,
                    'feature_type': 'nmr_bin',
                    'name_raw': measurement.bin_range,
                    'refmet_name': None,
                    'analysis_id': metadata.analysis_id
                })
                feature_uids_seen.add(measurement.feature_uid)

                # Flush feature batch
                if len(feature_batch) >= BATCH_SIZE:
                    created = self._batch_upsert_features(feature_batch)
                    stats.features_created += created
                    feature_batch = []
                    self.db.commit()  # Commit to release locks

            # Add measurement
            measurement_batch.append({
                'sample_uid': measurement.sample_uid,
                'feature_uid': measurement.feature_uid,
                'value': measurement.value,
                'unit': metadata.units,
                'file_id': file_id,
                'col_index': measurement.col_index,
                'replicate_ix': measurement.replicate_ix
            })

            # Flush measurement batch - commit after each batch to release locks
            if len(measurement_batch) >= BATCH_SIZE:
                inserted, skipped = self._batch_insert_measurements(measurement_batch)
                stats.measurements_inserted += inserted
                stats.measurements_skipped += skipped
                measurement_batch = []
                batch_count += 1
                self.db.commit()  # Commit to release locks
                if batch_count % 10 == 0:
                    logger.info(f"Progress: {stats.measurements_processed} processed, {stats.measurements_inserted} inserted")

        # Flush remaining batches
        if feature_batch:
            created = self._batch_upsert_features(feature_batch)
            stats.features_created += created

        if measurement_batch:
            inserted, skipped = self._batch_insert_measurements(measurement_batch)
            stats.measurements_inserted += inserted
            stats.measurements_skipped += skipped

        stats.features_processed = len(feature_uids_seen)

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
        self,
        analysis_id: str,
        study_pk: UUID,
        file_id: Optional[UUID] = None
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
            True if sample was created, False if existed
        """
        sample = self.db.query(Sample).filter(Sample.sample_uid == sample_uid).first()

        if not sample:
            sample = Sample(
                sample_uid=sample_uid,
                sample_label=sample_label,
                study_pk=study_pk,
                factors_raw=factors_raw
            )
            self.db.add(sample)
            logger.debug(f"Created sample: {sample_uid}")
            return True
        else:
            # Update factors if we have them and sample doesn't
            if factors_raw and not sample.factors_raw:
                sample.factors_raw = factors_raw
            return False

    def _batch_upsert_features(self, batch: list) -> int:
        """Batch upsert features using PostgreSQL INSERT ON CONFLICT.

        Returns:
            Number of new features created
        """
        if not batch:
            return 0

        # Use savepoint for error isolation
        savepoint = self.db.begin_nested()

        try:
            # PostgreSQL INSERT ... ON CONFLICT DO NOTHING on feature_uid
            stmt = insert(Feature).values(batch)
            stmt = stmt.on_conflict_do_nothing(index_elements=['feature_uid'])

            result = self.db.execute(stmt)
            savepoint.commit()

            created = result.rowcount if result.rowcount else 0
            return created

        except Exception as e:
            savepoint.rollback()
            logger.warning(f"Batch feature insert failed, trying individual: {e}")

            # Fallback to individual
            created = 0
            for item in batch:
                feature = (
                    self.db.query(Feature)
                    .filter(Feature.feature_uid == item['feature_uid'])
                    .first()
                )

                if not feature:
                    feature = Feature(
                        feature_uid=item['feature_uid'],
                        feature_type=item['feature_type'],
                        name_raw=item['name_raw'],
                        refmet_name=item.get('refmet_name'),
                        analysis_id=item.get('analysis_id')
                    )
                    self.db.add(feature)
                    created += 1

            self.db.flush()
            return created

    def _batch_insert_measurements(self, batch: list) -> tuple:
        """Batch insert measurements, checking for duplicates.

        Handles both:
        - New file-based uniqueness: (file_id, col_index, feature_uid)
        - Legacy uniqueness: (sample_uid, feature_uid)

        Returns:
            Tuple of (inserted_count, skipped_count)
        """
        if not batch:
            return 0, 0

        inserted = 0
        skipped = 0

        file_id = batch[0].get('file_id')

        # Build sets of existing keys for duplicate checking
        # 1. Check file-based duplicates (if file_id present)
        existing_file_keys: set = set()
        if file_id:
            existing_rows = (
                self.db.query(Measurement.col_index, Measurement.feature_uid)
                .filter(Measurement.file_id == file_id)
                .all()
            )
            for row in existing_rows:
                existing_file_keys.add((row.col_index, row.feature_uid))

        # 2. Check legacy (sample_uid, feature_uid) duplicates
        # Get unique sample_uid/feature_uid pairs from this batch
        batch_sample_features = set((item['sample_uid'], item['feature_uid']) for item in batch)

        existing_legacy_keys: set = set()
        if batch_sample_features:
            # Query existing measurements for these sample/feature combinations
            for sample_uid, feature_uid in batch_sample_features:
                existing = (
                    self.db.query(Measurement)
                    .filter(
                        Measurement.sample_uid == sample_uid,
                        Measurement.feature_uid == feature_uid
                    )
                    .first()
                )
                if existing:
                    existing_legacy_keys.add((sample_uid, feature_uid))

        # Filter out duplicates
        to_insert = []
        for item in batch:
            # Check file-based key
            file_key = (item['col_index'], item['feature_uid'])
            # Check legacy key
            legacy_key = (item['sample_uid'], item['feature_uid'])

            if file_key in existing_file_keys:
                skipped += 1
            elif legacy_key in existing_legacy_keys:
                skipped += 1
            else:
                to_insert.append(item)
                # Track for within-batch dedup
                existing_file_keys.add(file_key)
                existing_legacy_keys.add(legacy_key)

        if not to_insert:
            return inserted, skipped

        # Insert in smaller batches to avoid memory issues
        MINI_BATCH = 500
        for i in range(0, len(to_insert), MINI_BATCH):
            mini_batch = to_insert[i:i + MINI_BATCH]

            savepoint = self.db.begin_nested()
            try:
                self.db.execute(insert(Measurement).values(mini_batch))
                savepoint.commit()
                inserted += len(mini_batch)
            except Exception as e:
                savepoint.rollback()
                logger.warning(f"Mini-batch insert failed: {e}")

                # Fallback to individual inserts
                for item in mini_batch:
                    item_sp = self.db.begin_nested()
                    try:
                        self.db.add(Measurement(**item))
                        item_sp.commit()
                        inserted += 1
                    except Exception:
                        item_sp.rollback()
                        skipped += 1

        return inserted, skipped
