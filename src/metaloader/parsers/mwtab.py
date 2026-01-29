"""Parser for mwTab format (Metabolomics Workbench)."""

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class MwTabMetadata:
    """Metadata extracted from mwTab file."""
    study_id: Optional[str] = None
    analysis_id: Optional[str] = None


@dataclass
class SampleFactorData:
    """Sample with its factors."""
    subject: str
    sample_label: str
    factors: Dict[str, str]  # key -> value


@dataclass
class MwTabParseResult:
    """Result of parsing mwTab file."""
    metadata: MwTabMetadata
    samples: List[SampleFactorData]
    warnings: List[str]


class MwTabParser:
    """Parser for mwTab format files."""

    def __init__(self, file_path: Path):
        """Initialize parser with file path.
        
        Args:
            file_path: Path to mwTab file
        """
        self.file_path = file_path
        self.warnings: List[str] = []

    def parse(self) -> MwTabParseResult:
        """Parse mwTab file and extract metadata and sample factors.

        Returns:
            MwTabParseResult with metadata, samples, and warnings
        """
        metadata = MwTabMetadata()
        samples: List[SampleFactorData] = []

        with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
            # First pass: extract metadata
            for line in f:
                line = line.rstrip('\n\r')

                # Check first line for inline STUDY_ID and ANALYSIS_ID
                # Format: #METABOLOMICS WORKBENCH ... STUDY_ID:ST000315 ANALYSIS_ID:AN000501 ...
                if line.startswith('#METABOLOMICS WORKBENCH'):
                    study_match = re.search(r'STUDY_ID:(\S+)', line)
                    if study_match:
                        metadata.study_id = study_match.group(1)
                        logger.debug(f"Found STUDY_ID (inline): {metadata.study_id}")

                    analysis_match = re.search(r'ANALYSIS_ID:(\S+)', line)
                    if analysis_match:
                        metadata.analysis_id = analysis_match.group(1)
                        logger.debug(f"Found ANALYSIS_ID (inline): {metadata.analysis_id}")

                # Extract STUDY_ID (standalone line format)
                elif line.startswith('STUDY_ID:'):
                    if not metadata.study_id:  # Don't override if already found
                        metadata.study_id = line.split(':', 1)[1].strip()
                        logger.debug(f"Found STUDY_ID: {metadata.study_id}")

                # Extract ANALYSIS_ID (standalone line format)
                elif line.startswith('ANALYSIS_ID:'):
                    if not metadata.analysis_id:  # Don't override if already found
                        metadata.analysis_id = line.split(':', 1)[1].strip()
                        logger.debug(f"Found ANALYSIS_ID: {metadata.analysis_id}")

                # Check if we reached SUBJECT_SAMPLE_FACTORS section
                elif line.startswith('#SUBJECT_SAMPLE_FACTORS'):
                    logger.debug("Found SUBJECT_SAMPLE_FACTORS section header")
                    # Now parse the data lines
                    samples = self._parse_subject_sample_factors(f)
                    break

        return MwTabParseResult(
            metadata=metadata,
            samples=samples,
            warnings=self.warnings
        )

    def _parse_subject_sample_factors(self, file_handle) -> List[SampleFactorData]:
        """Parse SUBJECT_SAMPLE_FACTORS section.
        
        Args:
            file_handle: Open file handle positioned after section header
            
        Returns:
            List of SampleFactorData
        """
        samples: List[SampleFactorData] = []
        
        for line in file_handle:
            line = line.rstrip('\n\r')
            
            # Stop at next section or empty line
            if line.startswith('#') or not line.strip():
                break
            
            # Data lines start with SUBJECT_SAMPLE_FACTORS
            if line.startswith('SUBJECT_SAMPLE_FACTORS'):
                sample_data = self._parse_sample_factor_line(line)
                if sample_data:
                    samples.append(sample_data)
        
        logger.info(f"Parsed {len(samples)} sample factor entries")
        return samples

    def _parse_sample_factor_line(self, line: str) -> Optional[SampleFactorData]:
        """Parse a single SUBJECT_SAMPLE_FACTORS data line.
        
        Format: SUBJECT_SAMPLE_FACTORS\t<subject>\t<sample>\t<FACTORS>\t...
        
        Args:
            line: Tab-separated line
            
        Returns:
            SampleFactorData or None if parsing fails
        """
        parts = line.split('\t')
        
        if len(parts) < 4:
            self.warnings.append(f"Invalid SUBJECT_SAMPLE_FACTORS line (too few fields): {line[:100]}")
            logger.warning(f"Skipping line with too few fields: {line[:100]}")
            return None
        
        # parts[0] = "SUBJECT_SAMPLE_FACTORS"
        # parts[1] = subject (can be "-")
        # parts[2] = sample label
        # parts[3] = factors string
        
        subject = parts[1].strip()
        sample_label = parts[2].strip()
        factors_str = parts[3].strip()
        
        if not sample_label:
            self.warnings.append("Empty sample label, skipping")
            logger.warning(f"Skipping entry with empty sample label")
            return None
        
        # Parse factors string
        factors = self.parse_factors_string(factors_str)
        
        if not factors:
            self.warnings.append(f"No valid factors for sample '{sample_label}': {factors_str}")
            logger.warning(f"No valid factors parsed for sample '{sample_label}'")
            # Still return the sample even with empty factors
        
        return SampleFactorData(
            subject=subject,
            sample_label=sample_label,
            factors=factors
        )

    def parse_factors_string(self, factors_str: str) -> Dict[str, str]:
        """Parse factors string into key-value pairs.
        
        Format examples:
        - "Group:Exercise"
        - "Constitution:Lean | Visit:1 | Sampling time:0 min"
        
        Args:
            factors_str: Factors string with | separators
            
        Returns:
            Dictionary of factor_key -> factor_value
        """
        factors = {}
        
        if not factors_str or factors_str == '-':
            return factors
        
        # Split by pipe
        factor_pairs = factors_str.split('|')
        
        for pair in factor_pairs:
            pair = pair.strip()
            
            if not pair:
                continue
            
            # Split by first colon
            if ':' not in pair:
                self.warnings.append(f"Factor without colon, skipping: {pair}")
                logger.warning(f"Skipping factor without colon: {pair}")
                continue
            
            key, value = pair.split(':', 1)
            key = key.strip()
            value = value.strip()
            
            if not key:
                self.warnings.append(f"Empty factor key, skipping: {pair}")
                logger.warning(f"Skipping factor with empty key: {pair}")
                continue
            
            factors[key] = value
        
        return factors

    @staticmethod
    def normalize_sample_label(sample_label: str) -> str:
        """Normalize sample label for creating stable sample_uid.
        
        Rules:
        - Strip whitespace
        - Replace spaces with underscores
        - Remove non-alphanumeric characters (except . _ -)
        
        Args:
            sample_label: Raw sample label
            
        Returns:
            Normalized label
        """
        # Strip
        normalized = sample_label.strip()
        
        # Replace spaces with underscores
        normalized = normalized.replace(' ', '_')
        
        # Keep only alphanumeric, dot, underscore, hyphen
        # Replace everything else with underscore
        normalized = re.sub(r'[^A-Za-z0-9._-]', '_', normalized)
        
        # Collapse multiple underscores
        normalized = re.sub(r'_+', '_', normalized)
        
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        
        return normalized

    @staticmethod
    def create_sample_uid(study_id: str, analysis_id: str, sample_label: str) -> str:
        """Create stable sample_uid.
        
        Format: {study_id}:{analysis_id}:{normalized_sample_label}
        
        Args:
            study_id: Study ID (e.g., ST000315)
            analysis_id: Analysis ID (e.g., AN000501)
            sample_label: Raw sample label
            
        Returns:
            Stable sample_uid
        """
        normalized = MwTabParser.normalize_sample_label(sample_label)
        return f"{study_id}:{analysis_id}:{normalized}"
