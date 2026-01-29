"""Parser for mwTab format (Metabolomics Workbench)."""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class MwTabMetadata:
    """Metadata extracted from mwTab file."""
    study_id: Optional[str] = None
    analysis_id: Optional[str] = None
    units: Optional[str] = None


@dataclass
class SampleFactorData:
    """Sample with its factors."""
    subject: str
    sample_label: str
    factors_raw: str  # Raw factors string
    factors: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetaboliteRow:
    """Single metabolite row from MS_METABOLITE_DATA."""
    metabolite_name: str
    values: Dict[str, Optional[float]]  # sample_label -> value


@dataclass
class MwTabParseResult:
    """Result of parsing mwTab file."""
    metadata: MwTabMetadata
    samples: List[SampleFactorData]
    metabolites: List[MetaboliteRow]
    sample_columns: List[str]  # Column headers (sample labels) from metabolite data
    warnings: List[str]


class MwTabParser:
    """Parser for mwTab format files."""

    # Patterns for detecting metabolite name column
    METABOLITE_COL_PATTERNS = [
        r'^metabolite_name$',
        r'^metabolite$',
        r'^compound_name$',
        r'^compound$',
        r'^name$',
    ]

    def __init__(self, file_path: Path):
        """Initialize parser with file path.
        
        Args:
            file_path: Path to mwTab file
        """
        self.file_path = file_path
        self.warnings: List[str] = []

    def parse(self) -> MwTabParseResult:
        """Parse mwTab file and extract metadata, samples, and metabolite data.

        Returns:
            MwTabParseResult with metadata, samples, metabolites, and warnings
        """
        metadata = MwTabMetadata()
        samples: List[SampleFactorData] = []
        metabolites: List[MetaboliteRow] = []
        sample_columns: List[str] = []

        with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
            in_subject_sample_factors = False
            in_ms_metabolite_data = False
            metabolite_headers: List[str] = []
            metabolite_name_col_idx: int = 0

            for line in f:
                line = line.rstrip('\n\r')

                # Skip empty lines
                if not line.strip():
                    continue

                # Check first line for inline STUDY_ID and ANALYSIS_ID
                if line.startswith('#METABOLOMICS WORKBENCH'):
                    self._extract_inline_metadata(line, metadata)
                    continue

                # Extract STUDY_ID (standalone line format)
                if line.startswith('STUDY_ID:') and not metadata.study_id:
                    metadata.study_id = line.split(':', 1)[1].strip()
                    logger.debug(f"Found STUDY_ID: {metadata.study_id}")
                    continue

                # Extract ANALYSIS_ID (standalone line format)
                if line.startswith('ANALYSIS_ID:') and not metadata.analysis_id:
                    metadata.analysis_id = line.split(':', 1)[1].strip()
                    logger.debug(f"Found ANALYSIS_ID: {metadata.analysis_id}")
                    continue

                # Detect units
                if line.startswith('MS_METABOLITE_DATA:UNITS'):
                    units = line.split('\t')[-1].strip() if '\t' in line else line.split(':', 1)[-1].strip()
                    if units:
                        metadata.units = units
                        logger.debug(f"Found units: {metadata.units}")
                    continue

                # Section detection
                if line.startswith('#SUBJECT_SAMPLE_FACTORS'):
                    in_subject_sample_factors = True
                    in_ms_metabolite_data = False
                    logger.debug("Entering SUBJECT_SAMPLE_FACTORS section")
                    continue

                if line.startswith('MS_METABOLITE_DATA_START'):
                    in_subject_sample_factors = False
                    in_ms_metabolite_data = True
                    metabolite_headers = []
                    logger.debug("Entering MS_METABOLITE_DATA section")
                    continue

                if line.startswith('MS_METABOLITE_DATA_END'):
                    in_ms_metabolite_data = False
                    logger.debug("Exiting MS_METABOLITE_DATA section")
                    continue

                # New section starts
                if line.startswith('#'):
                    in_subject_sample_factors = False
                    in_ms_metabolite_data = False
                    continue

                # Parse SUBJECT_SAMPLE_FACTORS data
                if in_subject_sample_factors and line.startswith('SUBJECT_SAMPLE_FACTORS'):
                    sample_data = self._parse_sample_factor_line(line)
                    if sample_data:
                        samples.append(sample_data)
                    continue

                # Parse MS_METABOLITE_DATA
                if in_ms_metabolite_data:
                    if not metabolite_headers:
                        # First line is header
                        metabolite_headers = line.split('\t')
                        metabolite_name_col_idx = self._find_metabolite_column(metabolite_headers)
                        # Sample columns are all columns except the metabolite name column
                        sample_columns = [h for i, h in enumerate(metabolite_headers) if i != metabolite_name_col_idx]
                        logger.debug(f"Found {len(sample_columns)} sample columns in metabolite data")
                    else:
                        # Data row
                        metabolite_row = self._parse_metabolite_row(
                            line, metabolite_headers, metabolite_name_col_idx
                        )
                        if metabolite_row:
                            metabolites.append(metabolite_row)

        logger.info(
            f"Parsed: study_id={metadata.study_id}, "
            f"analysis_id={metadata.analysis_id}, "
            f"samples={len(samples)}, metabolites={len(metabolites)}"
        )

        return MwTabParseResult(
            metadata=metadata,
            samples=samples,
            metabolites=metabolites,
            sample_columns=sample_columns,
            warnings=self.warnings
        )

    def _extract_inline_metadata(self, line: str, metadata: MwTabMetadata) -> None:
        """Extract STUDY_ID and ANALYSIS_ID from first line."""
        study_match = re.search(r'STUDY_ID:(\S+)', line)
        if study_match:
            metadata.study_id = study_match.group(1)
            logger.debug(f"Found STUDY_ID (inline): {metadata.study_id}")

        analysis_match = re.search(r'ANALYSIS_ID:(\S+)', line)
        if analysis_match:
            metadata.analysis_id = analysis_match.group(1)
            logger.debug(f"Found ANALYSIS_ID (inline): {metadata.analysis_id}")

    def _parse_sample_factor_line(self, line: str) -> Optional[SampleFactorData]:
        """Parse a single SUBJECT_SAMPLE_FACTORS data line."""
        parts = line.split('\t')

        if len(parts) < 4:
            self.warnings.append(f"Invalid SUBJECT_SAMPLE_FACTORS line (too few fields): {line[:100]}")
            return None

        subject = parts[1].strip()
        sample_label = parts[2].strip()
        factors_str = parts[3].strip()

        if not sample_label:
            self.warnings.append("Empty sample label, skipping")
            return None

        factors = self.parse_factors_string(factors_str)

        return SampleFactorData(
            subject=subject,
            sample_label=sample_label,
            factors_raw=factors_str,
            factors=factors
        )

    def _find_metabolite_column(self, headers: List[str]) -> int:
        """Find the column index containing metabolite names."""
        # Try exact matches first
        for i, header in enumerate(headers):
            header_lower = header.lower().strip()
            for pattern in self.METABOLITE_COL_PATTERNS:
                if re.match(pattern, header_lower):
                    logger.debug(f"Found metabolite column '{header}' at index {i}")
                    return i

        # Default to first column
        logger.debug("Using first column as metabolite name column")
        return 0

    def _parse_metabolite_row(
        self, line: str, headers: List[str], metabolite_col_idx: int
    ) -> Optional[MetaboliteRow]:
        """Parse a single metabolite data row."""
        parts = line.split('\t')

        if len(parts) < 2:
            return None

        # Get metabolite name
        if metabolite_col_idx >= len(parts):
            self.warnings.append(f"Metabolite column index out of range: {line[:50]}")
            return None

        metabolite_name = parts[metabolite_col_idx].strip()
        if not metabolite_name:
            return None

        # Parse values
        values: Dict[str, Optional[float]] = {}
        for i, header in enumerate(headers):
            if i == metabolite_col_idx:
                continue

            if i >= len(parts):
                values[header] = None
                continue

            raw_value = parts[i].strip()
            parsed_value = self._parse_value(raw_value)
            values[header] = parsed_value

        return MetaboliteRow(
            metabolite_name=metabolite_name,
            values=values
        )

    def _parse_value(self, raw_value: str) -> Optional[float]:
        """Parse a value string to float, handling NA and empty values."""
        if not raw_value or raw_value.upper() in ('NA', 'N/A', 'NULL', '-', '.', ''):
            return None

        try:
            return float(raw_value)
        except ValueError:
            # Try removing common separators
            cleaned = raw_value.replace(',', '').replace(' ', '')
            try:
                return float(cleaned)
            except ValueError:
                self.warnings.append(f"Could not parse value: {raw_value}")
                return None

    def parse_factors_string(self, factors_str: str) -> Dict[str, str]:
        """Parse factors string into key-value pairs."""
        factors = {}

        if not factors_str or factors_str == '-':
            return factors

        factor_pairs = factors_str.split('|')

        for pair in factor_pairs:
            pair = pair.strip()
            if not pair:
                continue

            if ':' not in pair:
                self.warnings.append(f"Factor without colon, skipping: {pair}")
                continue

            key, value = pair.split(':', 1)
            key = key.strip()
            value = value.strip()

            if not key:
                continue

            factors[key] = value

        return factors

    @staticmethod
    def normalize_sample_label(sample_label: str) -> str:
        """Normalize sample label for creating stable sample_uid."""
        normalized = sample_label.strip()
        normalized = normalized.replace(' ', '_')
        normalized = re.sub(r'[^A-Za-z0-9._-]', '_', normalized)
        normalized = re.sub(r'_+', '_', normalized)
        normalized = normalized.strip('_')
        return normalized

    @staticmethod
    def normalize_feature_name(name_raw: str) -> str:
        """Normalize feature name for creating stable feature_uid.
        
        Rules:
        - trim
        - collapse whitespace to single space
        - lowercase
        """
        normalized = name_raw.strip()
        normalized = re.sub(r'\s+', ' ', normalized)
        normalized = normalized.lower()
        return normalized

    @staticmethod
    def create_sample_uid(study_id: str, sample_label: str) -> str:
        """Create stable sample_uid.
        
        Format: {study_id}:{sample_label}
        """
        return f"{study_id}:{sample_label}"

    @staticmethod
    def create_feature_uid(analysis_id: str, name_raw: str) -> str:
        """Create stable feature_uid.
        
        Format: {analysis_id}:met:{normalized_name}
        """
        normalized = MwTabParser.normalize_feature_name(name_raw)
        return f"{analysis_id}:met:{normalized}"


def is_mwtab_file(file_path: Path) -> bool:
    """Check if file is an mwTab format file."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            # Read first few lines
            for _ in range(10):
                line = f.readline()
                if not line:
                    break
                if '#METABOLOMICS WORKBENCH' in line.upper():
                    return True
        return False
    except Exception:
        return False
