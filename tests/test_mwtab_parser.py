"""Tests for mwTab parser."""

import tempfile
from pathlib import Path

import pytest

from metaloader.parsers.mwtab import MwTabParser


def test_parse_factors_string_single_factor():
    """Test parsing single factor."""
    parser = MwTabParser(Path("/tmp/dummy"))
    
    factors_str = "Group:Exercise"
    factors = parser.parse_factors_string(factors_str)
    
    assert len(factors) == 1
    assert factors["Group"] == "Exercise"


def test_parse_factors_string_multiple_factors():
    """Test parsing multiple factors with pipe separator."""
    parser = MwTabParser(Path("/tmp/dummy"))
    
    factors_str = "Constitution:Lean | Visit:1 | Sampling time:0 min"
    factors = parser.parse_factors_string(factors_str)
    
    assert len(factors) == 3
    assert factors["Constitution"] == "Lean"
    assert factors["Visit"] == "1"
    assert factors["Sampling time"] == "0 min"


def test_parse_factors_string_with_spaces():
    """Test parsing factors with various spacing."""
    parser = MwTabParser(Path("/tmp/dummy"))
    
    factors_str = "  Group : Exercise  |  Age : 25  "
    factors = parser.parse_factors_string(factors_str)
    
    assert len(factors) == 2
    assert factors["Group"] == "Exercise"
    assert factors["Age"] == "25"


def test_parse_factors_string_empty():
    """Test parsing empty factors string."""
    parser = MwTabParser(Path("/tmp/dummy"))
    
    factors = parser.parse_factors_string("")
    assert len(factors) == 0
    
    factors = parser.parse_factors_string("-")
    assert len(factors) == 0


def test_parse_factors_string_no_colon():
    """Test that factors without colon are skipped."""
    parser = MwTabParser(Path("/tmp/dummy"))
    
    factors_str = "Group:Exercise | InvalidFactor | Age:30"
    factors = parser.parse_factors_string(factors_str)
    
    # Should skip InvalidFactor
    assert len(factors) == 2
    assert "Group" in factors
    assert "Age" in factors
    assert len(parser.warnings) > 0


def test_parse_factors_string_empty_key():
    """Test that factors with empty key are skipped."""
    parser = MwTabParser(Path("/tmp/dummy"))
    
    factors_str = ":NoKey | Group:Exercise"
    factors = parser.parse_factors_string(factors_str)
    
    assert len(factors) == 1
    assert "Group" in factors
    assert len(parser.warnings) > 0


def test_parse_factors_string_multiple_colons():
    """Test parsing factor with multiple colons in value."""
    parser = MwTabParser(Path("/tmp/dummy"))
    
    factors_str = "Time:12:30:45"
    factors = parser.parse_factors_string(factors_str)
    
    assert len(factors) == 1
    assert factors["Time"] == "12:30:45"


def test_normalize_sample_label_basic():
    """Test basic sample label normalization."""
    result = MwTabParser.normalize_sample_label("Sample 123")
    assert result == "Sample_123"


def test_normalize_sample_label_special_chars():
    """Test normalization with special characters."""
    result = MwTabParser.normalize_sample_label("6018 post B S_87")
    assert result == "6018_post_B_S_87"


def test_normalize_sample_label_remove_invalid():
    """Test removal of invalid characters."""
    result = MwTabParser.normalize_sample_label("Sample#123@Test!")
    assert result == "Sample_123_Test_"


def test_normalize_sample_label_collapse_underscores():
    """Test collapsing multiple underscores."""
    result = MwTabParser.normalize_sample_label("Sample___123___Test")
    assert result == "Sample_123_Test"


def test_normalize_sample_label_strip_underscores():
    """Test stripping leading/trailing underscores."""
    result = MwTabParser.normalize_sample_label("__Sample_123__")
    assert result == "Sample_123"


def test_normalize_sample_label_preserve_allowed():
    """Test that allowed characters are preserved."""
    result = MwTabParser.normalize_sample_label("Sample-123.456_Test")
    assert result == "Sample-123.456_Test"


def test_create_sample_uid():
    """Test creation of sample UID."""
    uid = MwTabParser.create_sample_uid("ST000315", "AN000501", "6018 post B S_87")
    
    assert uid == "ST000315:AN000501:6018_post_B_S_87"


def test_parse_mwtab_file():
    """Test parsing a complete mwTab file."""
    # Create temporary mwTab file
    content = """#METABOLOMICS WORKBENCH
STUDY_ID:ST000315
ANALYSIS_ID:AN000501
#SUBJECT_SAMPLE_FACTORS:SUBJECT(optional)[tab]SAMPLE[tab]FACTORS(NAME:VALUE pairs separated by |)[tab]Additional sample data
SUBJECT_SAMPLE_FACTORS\t-\t6018 post B S_87\tGroup:Exercise | Visit:1
SUBJECT_SAMPLE_FACTORS\t-\t6018 post A S_88\tGroup:Control | Visit:1
SUBJECT_SAMPLE_FACTORS\t-\t6019 post B S_89\tGroup:Exercise | Visit:2
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(content)
        temp_path = Path(f.name)
    
    try:
        parser = MwTabParser(temp_path)
        result = parser.parse()
        
        # Check metadata
        assert result.metadata.study_id == "ST000315"
        assert result.metadata.analysis_id == "AN000501"
        
        # Check samples
        assert len(result.samples) == 3
        
        # Check first sample
        sample1 = result.samples[0]
        assert sample1.subject == "-"
        assert sample1.sample_label == "6018 post B S_87"
        assert len(sample1.factors) == 2
        assert sample1.factors["Group"] == "Exercise"
        assert sample1.factors["Visit"] == "1"
        
        # Check second sample
        sample2 = result.samples[1]
        assert sample2.sample_label == "6018 post A S_88"
        assert sample2.factors["Group"] == "Control"
        
    finally:
        temp_path.unlink()


def test_parse_mwtab_file_missing_metadata():
    """Test parsing file with missing metadata."""
    content = """#METABOLOMICS WORKBENCH
#SUBJECT_SAMPLE_FACTORS:
SUBJECT_SAMPLE_FACTORS\t-\tSample1\tGroup:Test
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(content)
        temp_path = Path(f.name)
    
    try:
        parser = MwTabParser(temp_path)
        result = parser.parse()
        
        # Should have None for missing metadata
        assert result.metadata.study_id is None
        assert result.metadata.analysis_id is None
        
    finally:
        temp_path.unlink()


def test_parse_sample_factor_line_invalid():
    """Test parsing invalid factor line."""
    parser = MwTabParser(Path("/tmp/dummy"))
    
    # Too few fields
    line = "SUBJECT_SAMPLE_FACTORS\tSubject1"
    result = parser._parse_sample_factor_line(line)
    
    assert result is None
    assert len(parser.warnings) > 0


def test_parse_sample_factor_line_empty_label():
    """Test parsing line with empty sample label."""
    parser = MwTabParser(Path("/tmp/dummy"))
    
    line = "SUBJECT_SAMPLE_FACTORS\t-\t\tGroup:Test"
    result = parser._parse_sample_factor_line(line)
    
    assert result is None
    assert len(parser.warnings) > 0
