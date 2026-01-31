"""Service for deriving category columns from raw data."""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy import func

from metaloader.models import File, Sample, SampleFactor, Measurement, Analysis

logger = logging.getLogger(__name__)

# Maximum bytes to scan for device detection heuristics
MAX_SCAN_BYTES = 32 * 1024  # 32KB


@dataclass
class DeriveStats:
    """Statistics from derive operation."""
    # Files/device stats
    files_processed: int = 0
    files_device_set: int = 0
    files_device_already_set: int = 0
    files_device_unknown: int = 0

    # Samples/exposure stats
    samples_processed: int = 0
    samples_exposure_set: int = 0
    samples_exposure_already_set: int = 0
    samples_exposure_unknown: int = 0
    samples_exposure_conflict: int = 0

    # Samples/matrix stats
    samples_matrix_set: int = 0
    samples_matrix_already_set: int = 0
    samples_matrix_unknown: int = 0
    samples_matrix_conflict: int = 0

    # Warnings
    warnings: List[str] = field(default_factory=list)


class DeriveService:
    """Service for deriving category columns."""

    # Device detection patterns (case-insensitive)
    GCMS_PATTERNS = [
        r'\bGC[-_\s]?MS\b',
        r'\bGCMS\b',
        r'\bgas\s+chromatograph',
        r'\bGC\s+mass\s+spectrom',
    ]

    LCMS_PATTERNS = [
        r'\bLC[-_\s]?MS\b',
        r'\bLCMS\b',
        r'\bliquid\s+chromatograph',
        r'\bHPLC[-_\s]?MS\b',
        r'\bUHPLC\b',
        r'\bUPLC\b',
    ]

    NMR_PATTERNS = [
        r'\bNMR\b',
        r'\bnuclear\s+magnetic\s+resonance\b',
        r'\b1H[-_\s]?NMR\b',
        r'\b13C[-_\s]?NMR\b',
    ]

    # Exposure detection keys (case-insensitive)
    EXPOSURE_KEYS = frozenset([
        'group', 'cohort', 'exposure', 'casecontrol', 'case_control',
        'obesity', 'status', 'condition', 'treatment', 'phenotype',
        'constitution', 'bmi', 'category', 'class', 'diagnosis'
    ])

    # Exposure value mappings (case-insensitive)
    OB_PATTERNS = ['ob', 'obese', 'obesity', 'case', 'overweight', 'bmi>']
    CON_PATTERNS = ['con', 'control', 'lean', 'normal', 'healthy', 'reference']

    # Values that should NOT trigger exposure mapping (study types, not obesity categories)
    EXPOSURE_EXCLUSIONS = frozenset([
        'exercise', 'acute', 'pool', 'pooled', 'qc', 'blank', 'standard',
        'baseline', 'treatment', 'intervention'
    ])

    # Study names that contain obesity-related words but are NOT individual classifications
    EXPOSURE_STUDY_NAMES = frozenset([
        'obesity and hdl function',  # Study name, not individual classification
    ])

    # Sample matrix detection keys (case-insensitive)
    MATRIX_KEYS = frozenset([
        'matrix', 'sample_type', 'sampletype', 'biofluid', 'specimen',
        'biospecimen', 'sample', 'tissue', 'material', 'samplesource',
        'sample_source', 'source', 'bodyfluid', 'body_fluid'
    ])

    # Sample matrix value mappings (case-insensitive)
    MATRIX_MAPPINGS = {
        'Serum': ['serum', 'blood serum', 'plasma', 'blood'],
        'Urine': ['urine', 'urinary'],
        'Feces': ['feces', 'faeces', 'stool', 'fecal', 'faecal'],
        'CSF': ['csf', 'cerebrospinal', 'spinal fluid'],
        'Tissue': ['tissue', 'mammary', 'liver', 'muscle', 'adipose'],
    }

    def __init__(self, db: Session):
        self.db = db

    def derive_all(
        self,
        study_id: Optional[str] = None,
        file_id: Optional[UUID] = None,
        dry_run: bool = False,
        limit: Optional[int] = None
    ) -> DeriveStats:
        """Derive all category columns.

        Args:
            study_id: Optional study ID filter (e.g., 'ST000106')
            file_id: Optional specific file UUID
            dry_run: If True, don't write to database
            limit: Optional limit on records to process

        Returns:
            DeriveStats with operation statistics
        """
        stats = DeriveStats()

        try:
            # 1. Derive device for files
            self._derive_device(stats, file_id, limit, dry_run)

            # 2. Derive exposure for samples
            self._derive_exposure(stats, study_id, limit, dry_run)

            # 3. Derive sample_matrix for samples
            self._derive_sample_matrix(stats, study_id, limit, dry_run)

            if not dry_run:
                self.db.commit()
                logger.info("All category derivations committed successfully")
            else:
                self.db.rollback()
                logger.info("Dry run completed - no changes committed")

        except Exception as e:
            self.db.rollback()
            logger.error(f"Error during derivation: {e}")
            raise

        return stats

    def _derive_device(
        self,
        stats: DeriveStats,
        file_id: Optional[UUID],
        limit: Optional[int],
        dry_run: bool
    ) -> None:
        """Derive device column for files."""
        logger.info("Deriving device for files...")

        # Build query
        query = self.db.query(File)
        if file_id:
            query = query.filter(File.id == file_id)
        if limit:
            query = query.limit(limit)

        files = query.all()

        for file in files:
            stats.files_processed += 1

            # Skip if already set
            if file.device:
                stats.files_device_already_set += 1
                continue

            # Detect device
            device = self._detect_device(file)

            if device:
                if not dry_run:
                    file.device = device
                stats.files_device_set += 1
                logger.debug(f"File {file.id}: device={device}")

                # Also update related analyses
                self._update_analyses_device(file.id, device, dry_run)
            else:
                stats.files_device_unknown += 1

        logger.info(
            f"Device derivation: {stats.files_processed} processed, "
            f"{stats.files_device_set} set, {stats.files_device_already_set} already set, "
            f"{stats.files_device_unknown} unknown"
        )

    def _detect_device(self, file: File) -> Optional[str]:
        """Detect device type from file metadata and content.

        Returns:
            'LCMS', 'GCMS', 'NMR', 'MS', or None
        """
        # 1. Check detected_type for NMR
        detected_type_lower = (file.detected_type or '').lower()
        if 'nmr' in detected_type_lower:
            return 'NMR'

        # 2. Check file path/name for hints
        path_text = f"{file.path_abs or ''} {file.filename or ''}".lower()

        if any(re.search(p, path_text, re.IGNORECASE) for p in self.NMR_PATTERNS):
            return 'NMR'

        # 3. For mwtab files, scan content for device hints
        if file.detected_type in ('mwtab', 'mwtab_ms', 'text/plain'):
            device = self._scan_file_for_device(file.path_abs)
            if device:
                return device

        # 4. Check path for LC/GC hints
        if any(re.search(p, path_text, re.IGNORECASE) for p in self.GCMS_PATTERNS):
            return 'GCMS'
        if any(re.search(p, path_text, re.IGNORECASE) for p in self.LCMS_PATTERNS):
            return 'LCMS'

        # 5. If it's an MS file but can't determine type
        if 'ms' in detected_type_lower or 'metabolite' in detected_type_lower:
            return 'MS'

        return None

    def _scan_file_for_device(self, file_path: str) -> Optional[str]:
        """Scan first N bytes of file for device hints."""
        if not file_path:
            return None

        path = Path(file_path)
        if not path.exists():
            return None

        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read(MAX_SCAN_BYTES).lower()

            # Check for NMR
            if any(re.search(p, content, re.IGNORECASE) for p in self.NMR_PATTERNS):
                return 'NMR'

            # Check for GCMS
            if any(re.search(p, content, re.IGNORECASE) for p in self.GCMS_PATTERNS):
                return 'GCMS'

            # Check for LCMS
            if any(re.search(p, content, re.IGNORECASE) for p in self.LCMS_PATTERNS):
                return 'LCMS'

            # If MS_METABOLITE_DATA present but no specific type
            if 'ms_metabolite_data' in content:
                return 'MS'

        except Exception as e:
            logger.warning(f"Could not scan file {file_path}: {e}")

        return None

    def _update_analyses_device(
        self,
        file_id: UUID,
        device: str,
        dry_run: bool
    ) -> None:
        """Update device for analyses linked to this file."""
        if dry_run:
            return

        analyses = self.db.query(Analysis).filter(Analysis.file_id == file_id).all()
        for analysis in analyses:
            if not analysis.device:
                analysis.device = device
                logger.debug(f"Analysis {analysis.analysis_id}: device={device}")

    def _derive_exposure(
        self,
        stats: DeriveStats,
        study_id: Optional[str],
        limit: Optional[int],
        dry_run: bool
    ) -> None:
        """Derive exposure column for samples."""
        logger.info("Deriving exposure for samples...")

        # Build query
        query = self.db.query(Sample)
        if study_id:
            query = query.filter(Sample.sample_uid.like(f"{study_id}:%"))
        if limit:
            query = query.limit(limit)

        samples = query.all()

        for sample in samples:
            stats.samples_processed += 1

            # Skip if already set
            if sample.exposure:
                stats.samples_exposure_already_set += 1
                continue

            # Get factors for this sample
            factors = self._get_sample_factors(sample.sample_uid)

            # Also parse factors_raw if present
            if sample.factors_raw:
                parsed = self._parse_factors_raw(sample.factors_raw)
                factors.update(parsed)

            # Derive exposure
            exposure, conflict = self._derive_exposure_value(factors, sample.sample_uid)

            if conflict:
                stats.samples_exposure_conflict += 1
                stats.warnings.append(f"Exposure conflict for sample {sample.sample_uid}")

            if exposure:
                if not dry_run:
                    sample.exposure = exposure
                stats.samples_exposure_set += 1
                logger.debug(f"Sample {sample.sample_uid}: exposure={exposure}")
            else:
                stats.samples_exposure_unknown += 1

        logger.info(
            f"Exposure derivation: {stats.samples_processed} processed, "
            f"{stats.samples_exposure_set} set, {stats.samples_exposure_already_set} already set, "
            f"{stats.samples_exposure_unknown} unknown, {stats.samples_exposure_conflict} conflicts"
        )

    def _get_sample_factors(self, sample_uid: str) -> Dict[str, str]:
        """Get all factors for a sample as a dict."""
        factors = {}
        rows = (
            self.db.query(SampleFactor)
            .filter(SampleFactor.sample_uid == sample_uid)
            .all()
        )
        for row in rows:
            factors[row.factor_key.lower()] = row.factor_value
        return factors

    def _parse_factors_raw(self, factors_raw: str) -> Dict[str, str]:
        """Parse factors_raw string into dict."""
        factors = {}
        if not factors_raw or factors_raw == '-':
            return factors

        for pair in factors_raw.split('|'):
            pair = pair.strip()
            if ':' in pair:
                key, value = pair.split(':', 1)
                factors[key.strip().lower()] = value.strip()

        return factors

    def _derive_exposure_value(
        self,
        factors: Dict[str, str],
        sample_uid: str
    ) -> Tuple[Optional[str], bool]:
        """Derive exposure value from factors.

        Returns:
            Tuple of (exposure_value, has_conflict)
        """
        ob_score = 0
        con_score = 0

        for key, value in factors.items():
            # Check if key is relevant for exposure
            key_normalized = key.replace('_', '').replace('-', '').replace(' ', '')
            if not any(ek in key_normalized for ek in self.EXPOSURE_KEYS):
                continue

            value_lower = value.lower()

            # Skip known study names that are not individual classifications
            if value_lower in self.EXPOSURE_STUDY_NAMES:
                continue

            # Skip if value is primarily about excluded categories (not obesity-related)
            # Only skip if exclusion word is present AND no OB/CON word is present
            has_exclusion = any(excl in value_lower for excl in self.EXPOSURE_EXCLUSIONS)
            has_ob_word = any(p in value_lower for p in ['obese', 'obesity', 'lean'])
            if has_exclusion and not has_ob_word:
                continue

            # Check for OB patterns
            for pattern in self.OB_PATTERNS:
                if pattern in value_lower:
                    # Exact match gets higher score
                    if value_lower == pattern:
                        ob_score += 10
                    else:
                        ob_score += 5
                    break

            # Check for CON patterns
            for pattern in self.CON_PATTERNS:
                if pattern in value_lower:
                    if value_lower == pattern:
                        con_score += 10
                    else:
                        con_score += 5
                    break

        # Determine result
        if ob_score > 0 and con_score > 0:
            # Conflict - choose the higher score
            logger.warning(
                f"Exposure conflict for {sample_uid}: OB={ob_score}, CON={con_score}"
            )
            if ob_score > con_score:
                return 'OB', True
            elif con_score > ob_score:
                return 'CON', True
            else:
                return None, True

        if ob_score > 0:
            return 'OB', False
        if con_score > 0:
            return 'CON', False

        return None, False

    def _derive_sample_matrix(
        self,
        stats: DeriveStats,
        study_id: Optional[str],
        limit: Optional[int],
        dry_run: bool
    ) -> None:
        """Derive sample_matrix column for samples."""
        logger.info("Deriving sample_matrix for samples...")

        # Build query
        query = self.db.query(Sample)
        if study_id:
            query = query.filter(Sample.sample_uid.like(f"{study_id}:%"))
        if limit:
            query = query.limit(limit)

        samples = query.all()

        for sample in samples:
            # Skip if already set
            if sample.sample_matrix:
                stats.samples_matrix_already_set += 1
                continue

            # Get factors for this sample
            factors = self._get_sample_factors(sample.sample_uid)

            # Also parse factors_raw if present
            if sample.factors_raw:
                parsed = self._parse_factors_raw(sample.factors_raw)
                factors.update(parsed)

            # Derive from factors first
            matrix, conflict = self._derive_matrix_value(factors, sample.sample_uid)

            if conflict:
                stats.samples_matrix_conflict += 1
                stats.warnings.append(f"Matrix conflict for sample {sample.sample_uid}")

            # Fallback to file path if no matrix found
            if not matrix and not conflict:
                matrix = self._derive_matrix_from_files(sample.sample_uid)

            if matrix:
                if not dry_run:
                    sample.sample_matrix = matrix
                stats.samples_matrix_set += 1
                logger.debug(f"Sample {sample.sample_uid}: sample_matrix={matrix}")
            else:
                stats.samples_matrix_unknown += 1

        logger.info(
            f"Matrix derivation: {stats.samples_processed} processed, "
            f"{stats.samples_matrix_set} set, {stats.samples_matrix_already_set} already set, "
            f"{stats.samples_matrix_unknown} unknown, {stats.samples_matrix_conflict} conflicts"
        )

    def _derive_matrix_value(
        self,
        factors: Dict[str, str],
        sample_uid: str
    ) -> Tuple[Optional[str], bool]:
        """Derive sample_matrix value from factors.

        Returns:
            Tuple of (matrix_value, has_conflict)
        """
        found_matrices: Set[str] = set()

        for key, value in factors.items():
            # Check if key is relevant for matrix
            key_normalized = key.replace('_', '').replace('-', '').replace(' ', '')
            if not any(mk in key_normalized for mk in self.MATRIX_KEYS):
                continue

            value_lower = value.lower()

            # Check against matrix mappings
            for matrix_name, patterns in self.MATRIX_MAPPINGS.items():
                for pattern in patterns:
                    if pattern in value_lower:
                        found_matrices.add(matrix_name)
                        break

        if len(found_matrices) > 1:
            logger.warning(
                f"Matrix conflict for {sample_uid}: {found_matrices}"
            )
            return None, True

        if len(found_matrices) == 1:
            return found_matrices.pop(), False

        return None, False

    def _derive_matrix_from_files(self, sample_uid: str) -> Optional[str]:
        """Derive matrix from associated file paths."""
        # Get distinct file paths for this sample
        file_paths = (
            self.db.query(File.path_abs)
            .join(Measurement, Measurement.file_id == File.id)
            .filter(Measurement.sample_uid == sample_uid)
            .distinct()
            .all()
        )

        found_matrices: Set[str] = set()

        for (path,) in file_paths:
            if not path:
                continue

            path_lower = path.lower()

            for matrix_name, patterns in self.MATRIX_MAPPINGS.items():
                for pattern in patterns:
                    # Check if pattern appears as directory component
                    if f'/{pattern}/' in path_lower or f'/{pattern}' in path_lower:
                        found_matrices.add(matrix_name)
                        break

        if len(found_matrices) > 1:
            logger.warning(
                f"Matrix conflict from files for {sample_uid}: {found_matrices}"
            )
            return None

        if len(found_matrices) == 1:
            return found_matrices.pop()

        return None


# Helper functions for testing/CLI usage

def derive_device(value: str) -> Optional[str]:
    """Standalone function to test device derivation logic."""
    value_lower = value.lower()

    # NMR patterns (check first - highest priority)
    nmr_patterns = [r'\bnmr\b', r'\bnuclear\s+magnetic']
    for p in nmr_patterns:
        if re.search(p, value_lower, re.IGNORECASE):
            return 'NMR'

    # GCMS patterns (check before generic MS)
    gcms_patterns = [
        r'\bgc[-_\s]?ms\b', r'\bgcms\b', r'\bgas\s+chromatograph',
        r'\bgc[/]ms\b', r'\bgc\s+mass\s*spec', r'\bgc[-_\s]?tof'
    ]
    for p in gcms_patterns:
        if re.search(p, value_lower, re.IGNORECASE):
            return 'GCMS'

    # LCMS patterns (check before generic MS)
    lcms_patterns = [
        r'\blc[-_\s]?ms\b', r'\blcms\b', r'\bliquid\s+chromatograph',
        r'\bhplc', r'\buhplc\b', r'\buplc\b', r'\blc[/]ms', r'\blc[-_\s]?tof'
    ]
    for p in lcms_patterns:
        if re.search(p, value_lower, re.IGNORECASE):
            return 'LCMS'

    # Generic MS (last resort)
    if 'ms' in value_lower or 'mass spec' in value_lower:
        return 'MS'

    return None


def derive_exposure(value: str) -> Optional[str]:
    """Standalone function to test exposure derivation logic."""
    value_lower = value.lower()

    ob_patterns = ['ob', 'obese', 'obesity', 'case', 'overweight']
    con_patterns = ['con', 'control', 'lean', 'normal', 'healthy']

    for p in ob_patterns:
        if p in value_lower:
            return 'OB'

    for p in con_patterns:
        if p in value_lower:
            return 'CON'

    return None


def derive_matrix(value: str) -> Optional[str]:
    """Standalone function to test matrix derivation logic."""
    value_lower = value.lower()

    mappings = {
        'Serum': ['serum', 'plasma', 'blood'],
        'Urine': ['urine', 'urinary'],
        'Feces': ['feces', 'faeces', 'stool', 'fecal'],
        'CSF': ['csf', 'cerebrospinal'],
        'Tissue': ['tissue', 'mammary', 'liver', 'muscle', 'adipose'],
    }

    for matrix_name, patterns in mappings.items():
        for p in patterns:
            if p in value_lower:
                return matrix_name

    return None
