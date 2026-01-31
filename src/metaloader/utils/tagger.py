"""Heuristic functions for inferring file categories from paths and metadata.

This module provides testable functions for inferring:
- Device type (LCMS, GCMS, NMR)
- Sample type (Serum, Urine, Feces, CSF)
- Exposure (OB, CON)
- Platform (ESI_pos, ESI_neg, HILIC, QQQ, QTOF, etc.)
"""

import re
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class TagResult:
    """Result of tagging operation with optional warning."""
    value: Optional[str]
    warning: Optional[str] = None


# =============================================================================
# Device inference
# =============================================================================

# NMR indicators in detected_type
NMR_DETECTED_TYPES = frozenset([
    'nmr_binned_xlsx',
    'nmr_binned_xlsm',
    'mwtab_nmr_binned',
])

# MS indicators in detected_type
MS_DETECTED_TYPES = frozenset([
    'mwtab',
    'mwtab_ms',
    'metabo_table_html',
])

# GC-MS patterns in path/filename
GC_PATTERNS = [
    r'\bGC[-_\s]?MS\b',
    r'\bGCMS\b',
    r'\bGC[-_]TOF\b',
    r'\b_GC_',
    r'\bGC[-_\s]?EI\b',
    r'/GC/',
]

# NMR patterns in path/filename (backup if detected_type doesn't indicate)
NMR_PATTERNS = [
    r'\bNMR\b',
    r'\b1H[-_]?NMR\b',
    r'\b13C[-_]?NMR\b',
    r'/NMR/',
]


def infer_device(
    path_rel: Optional[str],
    filename: Optional[str],
    detected_type: Optional[str]
) -> Optional[str]:
    """Infer device type from file metadata.

    Priority:
    1. detected_type indicates NMR -> NMR
    2. detected_type indicates MS:
       - path/filename contains GC patterns -> GCMS
       - otherwise -> LCMS
    3. path/filename patterns as fallback
    4. None if uncertain

    Args:
        path_rel: Relative path to file
        filename: Filename
        detected_type: Detected file type

    Returns:
        Device type: 'NMR', 'LCMS', 'GCMS', or None
    """
    # Normalize inputs
    path_rel = (path_rel or '').lower()
    filename = (filename or '').lower()
    detected_type = detected_type or ''

    combined = f"{path_rel} {filename}"

    # 1. Check detected_type for NMR
    if detected_type in NMR_DETECTED_TYPES:
        return 'NMR'

    # 2. Check detected_type for MS
    if detected_type in MS_DETECTED_TYPES:
        # Check for GC-MS patterns
        for pattern in GC_PATTERNS:
            if re.search(pattern, combined, re.IGNORECASE):
                return 'GCMS'
        # Default to LC-MS for MS files
        return 'LCMS'

    # 3. Fallback: check path/filename patterns
    # Check NMR patterns
    for pattern in NMR_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return 'NMR'

    # Check GC patterns
    for pattern in GC_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return 'GCMS'

    # Check generic MS/LC patterns
    if re.search(r'\bLC[-_\s]?MS\b|\bLCMS\b|\bHPLC\b|\bUPLC\b|\bUHPLC\b', combined, re.IGNORECASE):
        return 'LCMS'

    return None


# =============================================================================
# Sample type inference
# =============================================================================

# Sample type patterns (order matters - more specific first)
SAMPLE_TYPE_PATTERNS = {
    'Serum': [
        r'\bserum\b',
        r'\bplasma\b',
        r'\bblood\b',
    ],
    'Urine': [
        r'\burine\b',
        r'\burinary\b',
    ],
    'Feces': [
        r'\bfeces\b',
        r'\bfaeces\b',
        r'\bstool\b',
        r'\bfecal\b',
        r'\bfaecal\b',
    ],
    'CSF': [
        r'\bcsf\b',
        r'\bcerebrospinal\b',
    ],
}


def infer_sample_type(
    path_rel: Optional[str],
    filename: Optional[str]
) -> Optional[str]:
    """Infer sample type from file path and name.

    Args:
        path_rel: Relative path to file
        filename: Filename

    Returns:
        Sample type: 'Serum', 'Urine', 'Feces', 'CSF', or None
    """
    # Normalize and combine
    path_rel = (path_rel or '').lower()
    filename = (filename or '').lower()
    combined = f"{path_rel} {filename}"

    for sample_type, patterns in SAMPLE_TYPE_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, combined, re.IGNORECASE):
                return sample_type

    return None


# =============================================================================
# Exposure inference
# =============================================================================

# OB (obese/case) patterns
OB_PATTERNS = [
    r'\bOB\b',
    r'\bobese\b',
    r'\bobesity\b',
    r'\boverweight\b',
    r'\bcase\b',
    r'\bhigh[-_\s]?BMI\b',
    r'\bBMI[-_]?[>3]\d',  # BMI>30, BMI_35, etc.
]

# CON (control/lean) patterns
CON_PATTERNS = [
    r'\bCON\b',
    r'\bcontrol\b',
    r'\blean\b',
    r'\bnormal\b',
    r'\bhealthy\b',
    r'\breference\b',
    r'\blow[-_\s]?BMI\b',
]


def infer_exposure(
    path_rel: Optional[str],
    filename: Optional[str]
) -> TagResult:
    """Infer exposure type from file path and name.

    Args:
        path_rel: Relative path to file
        filename: Filename

    Returns:
        TagResult with:
        - value: 'OB', 'CON', or None
        - warning: Set if both OB and CON patterns found (conflict)
    """
    # Normalize and combine
    path_rel = (path_rel or '').lower()
    filename = (filename or '').lower()
    combined = f"{path_rel} {filename}"

    has_ob = any(re.search(p, combined, re.IGNORECASE) for p in OB_PATTERNS)
    has_con = any(re.search(p, combined, re.IGNORECASE) for p in CON_PATTERNS)

    if has_ob and has_con:
        # Conflict - both found
        return TagResult(
            value=None,
            warning=f"Conflicting exposure patterns in path: found both OB and CON indicators"
        )
    elif has_ob:
        return TagResult(value='OB')
    elif has_con:
        return TagResult(value='CON')
    else:
        return TagResult(value=None)


# =============================================================================
# Platform inference
# =============================================================================

# Platform patterns and their canonical names
PLATFORM_PATTERNS = [
    # Ionization mode
    (r'\bESI[-_\s]?pos(?:itive)?\b', 'ESI_pos'),
    (r'\bESI[-_\s]?neg(?:ative)?\b', 'ESI_neg'),
    (r'\bpositive[-_\s]?mode\b', 'ESI_pos'),
    (r'\bnegative[-_\s]?mode\b', 'ESI_neg'),
    (r'\b\+ESI\b', 'ESI_pos'),
    (r'\b-ESI\b', 'ESI_neg'),
    (r'\bAPCI[-_\s]?pos\b', 'APCI_pos'),
    (r'\bAPCI[-_\s]?neg\b', 'APCI_neg'),

    # Chromatography
    (r'\bHILIC\b', 'HILIC'),
    (r'\bRP\b', 'RP'),  # Reverse Phase
    (r'\bC18\b', 'C18'),
    (r'\bC8\b', 'C8'),

    # Mass analyzer
    (r'\bQQQ\b', 'QQQ'),
    (r'\btriple[-_\s]?quad\b', 'QQQ'),
    (r'\bQTOF\b', 'QTOF'),
    (r'\bQ[-_]?TOF\b', 'QTOF'),
    (r'\bOrbitrap\b', 'Orbitrap'),
    (r'\bTripleTOF\b', 'TripleTOF'),
    (r'\bTOF\b', 'TOF'),

    # LC methods
    (r'\bUPLC\b', 'UPLC'),
    (r'\bUHPLC\b', 'UHPLC'),
    (r'\bHPLC\b', 'HPLC'),
]


def infer_platform(
    path_rel: Optional[str],
    filename: Optional[str]
) -> Optional[str]:
    """Infer analytical platform details from file path and name.

    Extracts information about ionization mode, chromatography, and mass analyzer.

    Args:
        path_rel: Relative path to file
        filename: Filename

    Returns:
        Platform string (e.g., 'ESI_pos', 'HILIC_neg', 'QQQ') or None
    """
    # Normalize and combine
    path_rel = (path_rel or '').lower()
    filename = (filename or '').lower()
    combined = f"{path_rel} {filename}"

    found_platforms = []

    for pattern, platform_name in PLATFORM_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            if platform_name not in found_platforms:
                found_platforms.append(platform_name)

    if not found_platforms:
        return None

    # Return joined platforms (most specific first: ionization, chromatography, analyzer)
    # Limit to avoid overly long strings
    return '_'.join(found_platforms[:3])


# =============================================================================
# Combined tagging
# =============================================================================

@dataclass
class FileTags:
    """All inferred tags for a file."""
    device: Optional[str]
    sample_type: Optional[str]
    exposure: Optional[str]
    platform: Optional[str]
    warnings: list


def infer_all_tags(
    path_rel: Optional[str],
    filename: Optional[str],
    detected_type: Optional[str]
) -> FileTags:
    """Infer all category tags for a file.

    Args:
        path_rel: Relative path to file
        filename: Filename
        detected_type: Detected file type

    Returns:
        FileTags with all inferred values and any warnings
    """
    warnings = []

    device = infer_device(path_rel, filename, detected_type)
    sample_type = infer_sample_type(path_rel, filename)

    exposure_result = infer_exposure(path_rel, filename)
    exposure = exposure_result.value
    if exposure_result.warning:
        warnings.append(exposure_result.warning)

    platform = infer_platform(path_rel, filename)

    return FileTags(
        device=device,
        sample_type=sample_type,
        exposure=exposure,
        platform=platform,
        warnings=warnings
    )
