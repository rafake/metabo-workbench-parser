"""Streaming parser for mwTab MS_METABOLITE_DATA section."""

import logging
import re
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class MSMetadata:
    """Metadata extracted from mwTab file for MS data."""
    study_id: Optional[str] = None
    analysis_id: Optional[str] = None
    units: Optional[str] = None


@dataclass
class SampleColumn:
    """Represents a sample column in MS_METABOLITE_DATA."""
    col_index: int
    sample_uid: str
    factors: Optional[str] = None
    replicate_ix: int = 1


@dataclass
class MSMeasurement:
    """Single measurement from MS_METABOLITE_DATA."""
    col_index: int
    sample_uid: str
    feature_uid: str
    feature_name_raw: str
    refmet_name: Optional[str]
    value: Optional[float]
    replicate_ix: int


@dataclass
class SampleFactorInfo:
    """Sample factor information from SUBJECT_SAMPLE_FACTORS."""
    subject: str
    sample_label: str
    factors_raw: str


class MwTabMSParser:
    """Streaming parser for mwTab MS_METABOLITE_DATA.

    Parses line-by-line to minimize memory usage for large files.
    """

    # Known non-sample column names
    SKIP_COLUMNS = frozenset([
        'samples', 'factors', 'metabolite_name', 'metabolite',
        'compound_name', 'compound', 'name', 'refmet_name', 'refmet',
        'pubchem_id', 'kegg_id', 'hmdb_id', 'inchi_key', 'retention_time',
        'retention_index', 'm/z', 'mz', 'mass'
    ])

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.warnings: List[str] = []

    def parse_metadata_and_samples(self) -> Tuple[MSMetadata, Dict[str, SampleFactorInfo]]:
        """First pass: extract metadata and sample factors.

        Returns:
            Tuple of (MSMetadata, dict of sample_label -> SampleFactorInfo)
        """
        metadata = MSMetadata()
        sample_factors: Dict[str, SampleFactorInfo] = {}

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

                # Units
                if line.startswith('MS_METABOLITE_DATA:UNITS'):
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

                if line.startswith('#') or line.startswith('MS_METABOLITE_DATA_START'):
                    in_subject_sample_factors = False
                    if line.startswith('MS_METABOLITE_DATA_START'):
                        break  # Done with metadata and samples

                # Parse sample factors
                if in_subject_sample_factors and line.startswith('SUBJECT_SAMPLE_FACTORS'):
                    parts = line.split('\t')
                    if len(parts) >= 4:
                        subject = parts[1].strip()
                        sample_label = parts[2].strip()
                        factors_raw = parts[3].strip() if len(parts) > 3 else ''

                        if sample_label:
                            sample_factors[sample_label] = SampleFactorInfo(
                                subject=subject,
                                sample_label=sample_label,
                                factors_raw=factors_raw
                            )

        logger.info(
            f"Metadata: study_id={metadata.study_id}, analysis_id={metadata.analysis_id}, "
            f"units={metadata.units}, sample_factors={len(sample_factors)}"
        )

        return metadata, sample_factors

    def iter_measurements(
        self,
        metadata: MSMetadata,
        sample_factors: Dict[str, SampleFactorInfo]
    ) -> Iterator[MSMeasurement]:
        """Second pass: stream measurements from MS_METABOLITE_DATA.

        Yields measurements one by one to minimize memory usage.

        Args:
            metadata: Parsed metadata (for creating feature_uid)
            sample_factors: Dict of sample_label -> SampleFactorInfo

        Yields:
            MSMeasurement objects
        """
        with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
            in_ms_data = False
            columns: List[SampleColumn] = []
            metabolite_col_idx = 0
            refmet_col_idx: Optional[int] = None

            for line_num, line in enumerate(f, 1):
                line = line.rstrip('\n\r')

                if not line.strip():
                    continue

                if line.startswith('MS_METABOLITE_DATA_START'):
                    in_ms_data = True
                    continue

                if line.startswith('MS_METABOLITE_DATA_END'):
                    break

                if not in_ms_data:
                    continue

                parts = line.split('\t')

                # First data line: "Samples" header row
                if not columns and parts and parts[0].lower().strip() == 'samples':
                    columns, metabolite_col_idx, refmet_col_idx = self._parse_header_row(
                        parts, metadata.study_id or 'UNKNOWN'
                    )
                    logger.debug(f"Parsed {len(columns)} sample columns")
                    continue

                # Second row might be "Factors" - skip it
                if columns and parts and parts[0].lower().strip() == 'factors':
                    # Optionally update column factors here
                    self._update_column_factors(columns, parts)
                    continue

                # Data row: metabolite + values
                if columns:
                    yield from self._parse_data_row(
                        parts, columns, metabolite_col_idx, refmet_col_idx,
                        metadata.analysis_id or 'UNKNOWN', line_num
                    )

    def _parse_header_row(
        self,
        parts: List[str],
        study_id: str
    ) -> Tuple[List[SampleColumn], int, Optional[int]]:
        """Parse the Samples header row.

        Returns:
            Tuple of (sample_columns, metabolite_col_idx, refmet_col_idx)
        """
        columns: List[SampleColumn] = []
        metabolite_col_idx = 0
        refmet_col_idx: Optional[int] = None

        # Track replicates (same sample_uid appearing multiple times)
        sample_uid_counts: Dict[str, int] = defaultdict(int)

        for i, header in enumerate(parts):
            header_clean = header.strip()
            header_lower = header_clean.lower()

            # Skip known non-sample columns
            if header_lower in self.SKIP_COLUMNS:
                if header_lower in ('metabolite_name', 'metabolite', 'compound_name', 'compound', 'name'):
                    metabolite_col_idx = i
                elif header_lower in ('refmet_name', 'refmet'):
                    refmet_col_idx = i
                continue

            # This is a sample column
            sample_uid = self._create_sample_uid(study_id, header_clean)
            sample_uid_counts[sample_uid] += 1
            replicate_ix = sample_uid_counts[sample_uid]

            columns.append(SampleColumn(
                col_index=i,
                sample_uid=sample_uid,
                replicate_ix=replicate_ix
            ))

        return columns, metabolite_col_idx, refmet_col_idx

    def _update_column_factors(self, columns: List[SampleColumn], parts: List[str]) -> None:
        """Update column factors from Factors row."""
        for col in columns:
            if col.col_index < len(parts):
                factor_val = parts[col.col_index].strip()
                if factor_val and factor_val != '-':
                    col.factors = factor_val

    def _parse_data_row(
        self,
        parts: List[str],
        columns: List[SampleColumn],
        metabolite_col_idx: int,
        refmet_col_idx: Optional[int],
        analysis_id: str,
        line_num: int
    ) -> Iterator[MSMeasurement]:
        """Parse a single data row and yield measurements."""
        if len(parts) <= metabolite_col_idx:
            return

        metabolite_name = parts[metabolite_col_idx].strip()
        if not metabolite_name:
            return

        refmet_name: Optional[str] = None
        if refmet_col_idx is not None and refmet_col_idx < len(parts):
            refmet_val = parts[refmet_col_idx].strip()
            if refmet_val and refmet_val not in ('-', 'NA', 'N/A', ''):
                refmet_name = refmet_val

        feature_uid = self._create_feature_uid(analysis_id, metabolite_name, refmet_name)

        for col in columns:
            raw_value = parts[col.col_index].strip() if col.col_index < len(parts) else ''
            value = self._parse_value(raw_value)

            yield MSMeasurement(
                col_index=col.col_index,
                sample_uid=col.sample_uid,
                feature_uid=feature_uid,
                feature_name_raw=metabolite_name,
                refmet_name=refmet_name,
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
    def _create_sample_uid(study_id: str, sample_label: str) -> str:
        """Create stable sample_uid."""
        return f"{study_id}:{sample_label}"

    @staticmethod
    def _create_feature_uid(
        analysis_id: str,
        name_raw: str,
        refmet_name: Optional[str] = None
    ) -> str:
        """Create stable feature_uid.

        Format: {analysis_id}:met:{normalized_name}

        If name is very long, use a hash to keep it manageable.
        """
        # Normalize name
        normalized = name_raw.strip().lower()
        normalized = re.sub(r'\s+', ' ', normalized)

        # If name is too long (>100 chars), use hash
        if len(normalized) > 100:
            hash_input = f"{name_raw}|{refmet_name or ''}"
            name_hash = hashlib.md5(hash_input.encode()).hexdigest()[:16]
            return f"{analysis_id}:met:{name_hash}"

        # Replace problematic characters
        normalized = re.sub(r'[^a-z0-9._\-,()` ]', '_', normalized)
        normalized = re.sub(r'_+', '_', normalized)
        normalized = normalized.strip('_')

        return f"{analysis_id}:met:{normalized}"

    def get_unique_sample_uids(
        self,
        metadata: MSMetadata,
        sample_factors: Dict[str, SampleFactorInfo]
    ) -> Dict[str, SampleFactorInfo]:
        """Get unique sample_uids from MS data (third pass if needed).

        This scans MS_METABOLITE_DATA header to find all unique samples.
        Returns dict of sample_uid -> SampleFactorInfo (with factors from SUBJECT_SAMPLE_FACTORS if available)
        """
        samples: Dict[str, SampleFactorInfo] = {}
        study_id = metadata.study_id or 'UNKNOWN'

        with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
            in_ms_data = False

            for line in f:
                line = line.rstrip('\n\r')

                if line.startswith('MS_METABOLITE_DATA_START'):
                    in_ms_data = True
                    continue

                if not in_ms_data:
                    continue

                parts = line.split('\t')

                # Samples header row
                if parts and parts[0].lower().strip() == 'samples':
                    for header in parts[1:]:
                        header_clean = header.strip()
                        header_lower = header_clean.lower()

                        if header_lower in self.SKIP_COLUMNS:
                            continue

                        sample_uid = self._create_sample_uid(study_id, header_clean)

                        if sample_uid not in samples:
                            # Look up factors from SUBJECT_SAMPLE_FACTORS
                            factor_info = sample_factors.get(header_clean)
                            if factor_info:
                                samples[sample_uid] = SampleFactorInfo(
                                    subject=factor_info.subject,
                                    sample_label=header_clean,
                                    factors_raw=factor_info.factors_raw
                                )
                            else:
                                samples[sample_uid] = SampleFactorInfo(
                                    subject='',
                                    sample_label=header_clean,
                                    factors_raw=''
                                )
                    break

        return samples
