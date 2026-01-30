"""Streaming parser for mwTab NMR_BINNED_DATA section."""

import logging
import re
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class NMRMetadata:
    """Metadata extracted from mwTab file for NMR binned data."""
    study_id: Optional[str] = None
    analysis_id: Optional[str] = None
    units: Optional[str] = None


@dataclass
class NMRSampleColumn:
    """Represents a sample column in NMR_BINNED_DATA."""
    col_index: int
    sample_uid: str
    sample_label: str
    replicate_ix: int = 1


@dataclass
class NMRMeasurement:
    """Single measurement from NMR_BINNED_DATA."""
    col_index: int
    sample_uid: str
    feature_uid: str
    bin_range: str  # Original bin range string (e.g., "(0.000,0.040)")
    value: Optional[float]
    replicate_ix: int


@dataclass
class NMRSampleFactorInfo:
    """Sample factor information from SUBJECT_SAMPLE_FACTORS."""
    subject: str
    sample_label: str
    factors_raw: str


class MwTabNMRParser:
    """Streaming parser for mwTab NMR_BINNED_DATA.

    Parses line-by-line to minimize memory usage for large files.
    """

    # Known non-sample column names for NMR binned data
    SKIP_COLUMNS = frozenset([
        'samples', 'factors', 'bin range(ppm)', 'bin_range', 'bin',
        'ppm_range', 'ppm', 'chemical_shift', 'chemical shift',
        'bucket', 'bucket_id'
    ])

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.warnings: List[str] = []

    def parse_metadata_and_samples(self) -> Tuple[NMRMetadata, Dict[str, NMRSampleFactorInfo]]:
        """First pass: extract metadata and sample factors.

        Returns:
            Tuple of (NMRMetadata, dict of sample_label -> NMRSampleFactorInfo)
        """
        metadata = NMRMetadata()
        sample_factors: Dict[str, NMRSampleFactorInfo] = {}

        with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
            in_subject_sample_factors = False

            for line in f:
                line = line.rstrip('\n\r')

                if not line.strip():
                    continue

                # Extract metadata from first line
                if line.startswith('#METABOLOMICS WORKBENCH'):
                    study_match = re.search(r'STUDY_ID:(\S+)', line)
                    if study_match:
                        metadata.study_id = study_match.group(1)

                    analysis_match = re.search(r'ANALYSIS_ID:(\S+)', line)
                    if analysis_match:
                        metadata.analysis_id = analysis_match.group(1)
                    continue

                # Standalone STUDY_ID
                if line.startswith('STUDY_ID:') and not metadata.study_id:
                    metadata.study_id = line.split(':', 1)[1].strip()
                    continue

                # Standalone ANALYSIS_ID
                if line.startswith('ANALYSIS_ID:') and not metadata.analysis_id:
                    metadata.analysis_id = line.split(':', 1)[1].strip()
                    continue

                # Units for NMR binned data
                if line.startswith('NMR_BINNED_DATA:UNITS'):
                    parts = line.split('\t')
                    if len(parts) > 1:
                        metadata.units = parts[-1].strip()
                    else:
                        parts = line.split(':')
                        if len(parts) > 1:
                            metadata.units = parts[-1].strip()
                    continue

                # Section detection
                if line.startswith('#SUBJECT_SAMPLE_FACTORS'):
                    in_subject_sample_factors = True
                    continue

                if line.startswith('#') or line.startswith('NMR_BINNED_DATA_START'):
                    in_subject_sample_factors = False
                    if line.startswith('NMR_BINNED_DATA_START'):
                        break  # Done with metadata and samples

                # Parse sample factors
                if in_subject_sample_factors and line.startswith('SUBJECT_SAMPLE_FACTORS'):
                    parts = line.split('\t')
                    if len(parts) >= 4:
                        subject = parts[1].strip()
                        sample_label = parts[2].strip()
                        factors_raw = parts[3].strip() if len(parts) > 3 else ''

                        if sample_label:
                            sample_factors[sample_label] = NMRSampleFactorInfo(
                                subject=subject,
                                sample_label=sample_label,
                                factors_raw=factors_raw
                            )

        logger.info(
            f"NMR Metadata: study_id={metadata.study_id}, analysis_id={metadata.analysis_id}, "
            f"units={metadata.units}, sample_factors={len(sample_factors)}"
        )

        return metadata, sample_factors

    def iter_measurements(
        self,
        metadata: NMRMetadata,
        sample_factors: Dict[str, NMRSampleFactorInfo]
    ) -> Iterator[NMRMeasurement]:
        """Second pass: stream measurements from NMR_BINNED_DATA.

        Yields measurements one by one to minimize memory usage.

        Args:
            metadata: Parsed metadata (for creating feature_uid)
            sample_factors: Dict of sample_label -> NMRSampleFactorInfo

        Yields:
            NMRMeasurement objects
        """
        with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
            in_nmr_data = False
            columns: List[NMRSampleColumn] = []
            bin_range_col_idx = 0

            for line_num, line in enumerate(f, 1):
                line = line.rstrip('\n\r')

                if not line.strip():
                    continue

                if line.startswith('NMR_BINNED_DATA_START'):
                    in_nmr_data = True
                    continue

                if line.startswith('NMR_BINNED_DATA_END'):
                    break

                if not in_nmr_data:
                    continue

                parts = line.split('\t')

                # First data line: header row with "Bin range(ppm)" or similar
                if not columns and parts:
                    first_col_lower = parts[0].lower().strip()
                    # Check if this looks like a header row
                    if self._is_header_row(first_col_lower):
                        columns, bin_range_col_idx = self._parse_header_row(
                            parts,
                            metadata.study_id or 'UNKNOWN',
                            metadata.analysis_id or 'UNKNOWN'
                        )
                        logger.debug(f"Parsed {len(columns)} sample columns for NMR data")
                        continue

                # Second row might be "Factors" - skip it
                if columns and parts and parts[0].lower().strip() == 'factors':
                    continue

                # Data row: bin_range + values
                if columns:
                    yield from self._parse_data_row(
                        parts, columns, bin_range_col_idx,
                        metadata.analysis_id or 'UNKNOWN', line_num
                    )

    def _is_header_row(self, first_col: str) -> bool:
        """Check if this is a header row."""
        header_indicators = [
            'samples', 'bin range', 'bin_range', 'ppm',
            'bucket', 'chemical_shift', 'chemical shift'
        ]
        return any(ind in first_col for ind in header_indicators)

    def _parse_header_row(
        self,
        parts: List[str],
        study_id: str,
        analysis_id: str
    ) -> Tuple[List[NMRSampleColumn], int]:
        """Parse the header row for NMR binned data.

        Returns:
            Tuple of (sample_columns, bin_range_col_idx)
        """
        columns: List[NMRSampleColumn] = []
        bin_range_col_idx = 0

        # Track replicates (same sample_uid appearing multiple times)
        sample_uid_counts: Dict[str, int] = defaultdict(int)

        for i, header in enumerate(parts):
            header_clean = header.strip()
            header_lower = header_clean.lower()

            # Skip known non-sample columns
            if header_lower in self.SKIP_COLUMNS:
                # First column is typically the bin range
                if i == 0 or 'bin' in header_lower or 'ppm' in header_lower:
                    bin_range_col_idx = i
                continue

            # This is a sample column
            sample_uid = self._create_sample_uid(study_id, analysis_id, header_clean)
            sample_uid_counts[sample_uid] += 1
            replicate_ix = sample_uid_counts[sample_uid]

            columns.append(NMRSampleColumn(
                col_index=i,
                sample_uid=sample_uid,
                sample_label=header_clean,
                replicate_ix=replicate_ix
            ))

        return columns, bin_range_col_idx

    def _parse_data_row(
        self,
        parts: List[str],
        columns: List[NMRSampleColumn],
        bin_range_col_idx: int,
        analysis_id: str,
        line_num: int
    ) -> Iterator[NMRMeasurement]:
        """Parse a single data row and yield measurements."""
        if len(parts) <= bin_range_col_idx:
            return

        bin_range = parts[bin_range_col_idx].strip()
        if not bin_range:
            return

        feature_uid = self._create_feature_uid(analysis_id, bin_range)

        for col in columns:
            raw_value = parts[col.col_index].strip() if col.col_index < len(parts) else ''
            value = self._parse_value(raw_value)

            yield NMRMeasurement(
                col_index=col.col_index,
                sample_uid=col.sample_uid,
                feature_uid=feature_uid,
                bin_range=bin_range,
                value=value,
                replicate_ix=col.replicate_ix
            )

    def _parse_value(self, raw_value: str) -> Optional[float]:
        """Parse value string to float."""
        if not raw_value or raw_value.upper() in ('NA', 'N/A', 'NULL', '-', '.', ''):
            return None

        try:
            return float(raw_value)
        except ValueError:
            cleaned = raw_value.replace(',', '').replace(' ', '')
            try:
                return float(cleaned)
            except ValueError:
                self.warnings.append(f"Could not parse value: {raw_value}")
                return None

    @staticmethod
    def _create_sample_uid(study_id: str, analysis_id: str, sample_label: str) -> str:
        """Create stable sample_uid for NMR data.

        Format: {study_id}:{analysis_id}:s:{sha1(sample_label)[:12]}
        """
        label_hash = hashlib.sha1(sample_label.encode()).hexdigest()[:12]
        return f"{study_id}:{analysis_id}:s:{label_hash}"

    @staticmethod
    def _create_feature_uid(analysis_id: str, bin_range: str) -> str:
        """Create stable feature_uid for NMR bin.

        Format: {analysis_id}:nmrbin:{sha1(bin_range)[:12]}
        """
        bin_hash = hashlib.sha1(bin_range.encode()).hexdigest()[:12]
        return f"{analysis_id}:nmrbin:{bin_hash}"

    def get_unique_sample_uids(
        self,
        metadata: NMRMetadata,
        sample_factors: Dict[str, NMRSampleFactorInfo]
    ) -> Dict[str, NMRSampleFactorInfo]:
        """Get unique sample_uids from NMR data.

        Scans NMR_BINNED_DATA header to find all unique samples.
        Returns dict of sample_uid -> NMRSampleFactorInfo
        """
        samples: Dict[str, NMRSampleFactorInfo] = {}
        study_id = metadata.study_id or 'UNKNOWN'
        analysis_id = metadata.analysis_id or 'UNKNOWN'

        with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
            in_nmr_data = False

            for line in f:
                line = line.rstrip('\n\r')

                if line.startswith('NMR_BINNED_DATA_START'):
                    in_nmr_data = True
                    continue

                if not in_nmr_data:
                    continue

                parts = line.split('\t')

                # Header row
                if parts and self._is_header_row(parts[0].lower().strip()):
                    for header in parts[1:]:
                        header_clean = header.strip()
                        header_lower = header_clean.lower()

                        if header_lower in self.SKIP_COLUMNS:
                            continue

                        sample_uid = self._create_sample_uid(study_id, analysis_id, header_clean)

                        if sample_uid not in samples:
                            # Look up factors from SUBJECT_SAMPLE_FACTORS
                            factor_info = sample_factors.get(header_clean)
                            if factor_info:
                                samples[sample_uid] = NMRSampleFactorInfo(
                                    subject=factor_info.subject,
                                    sample_label=header_clean,
                                    factors_raw=factor_info.factors_raw
                                )
                            else:
                                samples[sample_uid] = NMRSampleFactorInfo(
                                    subject='',
                                    sample_label=header_clean,
                                    factors_raw=''
                                )
                    break

        return samples
