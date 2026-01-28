"""File type detection using heuristics."""

import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def detect_file_type(file_path: Path) -> str:
    """Detect file type using heuristics.
    
    Detection rules:
    - .txt files containing "#METABOLOMICS WORKBENCH" -> "mwtab"
    - .htm/.html files containing "Metabolite_name" -> "metabo_table_html"
    - files with "_res.txt" in name -> "results_txt"
    - .xlsx/.xlsm files with "normalized binned data" in name (case-insensitive) -> "nmr_binned_xlsx"
    - otherwise -> "unknown"
    
    Args:
        file_path: Path to the file
        
    Returns:
        Detected file type string
    """
    ext = file_path.suffix.lower()
    filename = file_path.name
    filename_lower = filename.lower()
    
    # Check for _res.txt pattern
    if "_res.txt" in filename_lower:
        return "results_txt"
    
    # Check for NMR binned data XLSX/XLSM
    if ext in [".xlsx", ".xlsm"] and "normalized binned data" in filename_lower:
        return "nmr_binned_xlsx"
    
    # For .txt files, check content for MWTAB marker
    if ext == ".txt":
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                # Read first few lines to check for marker
                for _ in range(50):  # Check first 50 lines
                    line = f.readline()
                    if not line:
                        break
                    if "#METABOLOMICS WORKBENCH" in line.upper():
                        return "mwtab"
        except Exception as e:
            logger.warning(f"Error reading file {file_path} for type detection: {e}")
    
    # For .htm/.html files, check for table content
    if ext in [".htm", ".html"]:
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                # Read reasonable chunk for HTML detection
                content = f.read(50000)  # Read first 50KB
                if "Metabolite_name" in content or "metabolite_name" in content.lower():
                    return "metabo_table_html"
        except Exception as e:
            logger.warning(f"Error reading file {file_path} for type detection: {e}")
    
    return "unknown"


def validate_file_extension(file_path: Path) -> bool:
    """Validate that file has an allowed extension.
    
    Allowed extensions: .txt, .htm, .html, .csv, .tsv, .xlsx, .xlsm, .zip, .pdf
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if extension is allowed, False otherwise
    """
    allowed_extensions = {".txt", ".htm", ".html", ".csv", ".tsv", ".xlsx", ".xlsm", ".zip", ".pdf"}
    ext = file_path.suffix.lower()
    return ext in allowed_extensions
