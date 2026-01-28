"""Tests for file type detection utility."""

import tempfile
from pathlib import Path

import pytest

from metaloader.utils.type_detector import detect_file_type, validate_file_extension


def test_detect_mwtab():
    """Test detection of MWTAB files."""
    content = """#METABOLOMICS WORKBENCH
STUDY_ID:ST000001
ANALYSIS_ID:AN000001
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(content)
        temp_path = Path(f.name)
    
    try:
        result = detect_file_type(temp_path)
        assert result == "mwtab"
    finally:
        temp_path.unlink()


def test_detect_results_txt():
    """Test detection of results text files."""
    content = "Sample data"
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='_res.txt', delete=False) as f:
        f.write(content)
        temp_path = Path(f.name)
    
    try:
        result = detect_file_type(temp_path)
        assert result == "results_txt"
    finally:
        temp_path.unlink()


def test_detect_nmr_binned_xlsx():
    """Test detection of NMR binned XLSX files."""
    # Create a file with the pattern in name
    with tempfile.NamedTemporaryFile(suffix='_Normalized Binned Data.xlsx', delete=False) as f:
        temp_path = Path(f.name)
    
    try:
        result = detect_file_type(temp_path)
        assert result == "nmr_binned_xlsx"
    finally:
        temp_path.unlink()


def test_detect_nmr_binned_xlsm():
    """Test detection of NMR binned XLSM files."""
    with tempfile.NamedTemporaryFile(suffix='_normalized binned data.xlsm', delete=False) as f:
        temp_path = Path(f.name)
    
    try:
        result = detect_file_type(temp_path)
        assert result == "nmr_binned_xlsx"
    finally:
        temp_path.unlink()


def test_detect_metabo_table_html():
    """Test detection of metabolite table HTML files."""
    content = """<html>
<head><title>Results</title></head>
<body>
<table>
<tr><th>Metabolite_name</th><th>Value</th></tr>
<tr><td>Glucose</td><td>123.45</td></tr>
</table>
</body>
</html>"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
        f.write(content)
        temp_path = Path(f.name)
    
    try:
        result = detect_file_type(temp_path)
        assert result == "metabo_table_html"
    finally:
        temp_path.unlink()


def test_detect_unknown_txt():
    """Test that unknown txt files return 'unknown'."""
    content = "Just some random text without markers"
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(content)
        temp_path = Path(f.name)
    
    try:
        result = detect_file_type(temp_path)
        assert result == "unknown"
    finally:
        temp_path.unlink()


def test_detect_unknown_csv():
    """Test that CSV files return 'unknown' (not specifically detected yet)."""
    content = "col1,col2,col3\n1,2,3\n4,5,6"
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(content)
        temp_path = Path(f.name)
    
    try:
        result = detect_file_type(temp_path)
        assert result == "unknown"
    finally:
        temp_path.unlink()


def test_validate_extension_allowed():
    """Test validation of allowed extensions."""
    allowed = ['.txt', '.htm', '.html', '.csv', '.tsv', '.xlsx', '.xlsm', '.zip', '.pdf']
    
    for ext in allowed:
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as f:
            temp_path = Path(f.name)
        
        try:
            assert validate_file_extension(temp_path) is True
        finally:
            temp_path.unlink()


def test_validate_extension_not_allowed():
    """Test validation of disallowed extensions."""
    disallowed = ['.exe', '.py', '.sh', '.bat', '.docx', '.pptx']
    
    for ext in disallowed:
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as f:
            temp_path = Path(f.name)
        
        try:
            assert validate_file_extension(temp_path) is False
        finally:
            temp_path.unlink()


def test_validate_extension_case_insensitive():
    """Test that extension validation is case-insensitive."""
    with tempfile.NamedTemporaryFile(suffix='.TXT', delete=False) as f:
        temp_path = Path(f.name)
    
    try:
        assert validate_file_extension(temp_path) is True
    finally:
        temp_path.unlink()
