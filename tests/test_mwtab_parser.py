"""Tests for mwTab parser."""

import tempfile
from pathlib import Path

import pytest

from metaloader.parsers.mwtab import MwTabParser, is_mwtab_file


class TestNormalization:
    """Tests for normalization functions."""

    def test_normalize_sample_label_basic(self):
        """Test basic sample label normalization."""
        result = MwTabParser.normalize_sample_label("Sample 123")
        assert result == "Sample_123"

    def test_normalize_sample_label_special_chars(self):
        """Test normalization with special characters."""
        result = MwTabParser.normalize_sample_label("6018 post B S_87")
        assert result == "6018_post_B_S_87"

    def test_normalize_sample_label_collapse_underscores(self):
        """Test collapsing multiple underscores."""
        result = MwTabParser.normalize_sample_label("Sample___123___Test")
        assert result == "Sample_123_Test"

    def test_normalize_feature_name_basic(self):
        """Test basic feature name normalization."""
        result = MwTabParser.normalize_feature_name("  Glucose  ")
        assert result == "glucose"

    def test_normalize_feature_name_whitespace(self):
        """Test feature name normalization with multiple whitespace."""
        result = MwTabParser.normalize_feature_name("  L-Lactic   Acid  ")
        assert result == "l-lactic acid"

    def test_normalize_feature_name_unicode(self):
        """Test feature name normalization preserves unicode."""
        result = MwTabParser.normalize_feature_name("β-Alanine")
        assert result == "β-alanine"

    def test_create_sample_uid(self):
        """Test sample UID creation."""
        uid = MwTabParser.create_sample_uid("ST000315", "6018 post B S_87")
        assert uid == "ST000315:6018 post B S_87"

    def test_create_feature_uid(self):
        """Test feature UID creation."""
        uid = MwTabParser.create_feature_uid("AN000501", "L-Lactic Acid")
        assert uid == "AN000501:met:l-lactic acid"


class TestFactorsParsing:
    """Tests for factors string parsing."""

    def test_parse_factors_string_single(self):
        """Test parsing single factor."""
        parser = MwTabParser(Path("/tmp/dummy"))
        factors = parser.parse_factors_string("Group:Exercise")
        assert len(factors) == 1
        assert factors["Group"] == "Exercise"

    def test_parse_factors_string_multiple(self):
        """Test parsing multiple factors with pipe separator."""
        parser = MwTabParser(Path("/tmp/dummy"))
        factors = parser.parse_factors_string("Constitution:Lean | Visit:1 | Sampling time:0 min")
        assert len(factors) == 3
        assert factors["Constitution"] == "Lean"
        assert factors["Visit"] == "1"
        assert factors["Sampling time"] == "0 min"

    def test_parse_factors_string_empty(self):
        """Test parsing empty factors string."""
        parser = MwTabParser(Path("/tmp/dummy"))
        factors = parser.parse_factors_string("")
        assert len(factors) == 0

        factors = parser.parse_factors_string("-")
        assert len(factors) == 0

    def test_parse_factors_string_no_colon(self):
        """Test that factors without colon are skipped."""
        parser = MwTabParser(Path("/tmp/dummy"))
        factors = parser.parse_factors_string("Group:Exercise | InvalidFactor | Age:30")
        assert len(factors) == 2
        assert "Group" in factors
        assert "Age" in factors


class TestValueParsing:
    """Tests for value parsing."""

    def test_parse_value_number(self):
        """Test parsing numeric value."""
        parser = MwTabParser(Path("/tmp/dummy"))
        assert parser._parse_value("123.45") == 123.45

    def test_parse_value_na(self):
        """Test parsing NA values."""
        parser = MwTabParser(Path("/tmp/dummy"))
        assert parser._parse_value("NA") is None
        assert parser._parse_value("N/A") is None
        assert parser._parse_value("") is None
        assert parser._parse_value("-") is None

    def test_parse_value_with_comma(self):
        """Test parsing value with comma separator."""
        parser = MwTabParser(Path("/tmp/dummy"))
        assert parser._parse_value("1,234.56") == 1234.56

    def test_parse_value_invalid(self):
        """Test parsing invalid value."""
        parser = MwTabParser(Path("/tmp/dummy"))
        result = parser._parse_value("not_a_number")
        assert result is None
        assert len(parser.warnings) > 0


class TestMsMetaboliteDataDetection:
    """Tests for MS_METABOLITE_DATA section detection."""

    def test_find_metabolite_column_exact_match(self):
        """Test finding metabolite column with exact match."""
        parser = MwTabParser(Path("/tmp/dummy"))
        headers = ["Metabolite_name", "Sample1", "Sample2"]
        idx = parser._find_metabolite_column(headers)
        assert idx == 0

    def test_find_metabolite_column_case_insensitive(self):
        """Test finding metabolite column case-insensitively."""
        parser = MwTabParser(Path("/tmp/dummy"))
        headers = ["metabolite", "Sample1", "Sample2"]
        idx = parser._find_metabolite_column(headers)
        assert idx == 0

    def test_find_metabolite_column_default_first(self):
        """Test defaulting to first column."""
        parser = MwTabParser(Path("/tmp/dummy"))
        headers = ["Compound", "Sample1", "Sample2"]
        idx = parser._find_metabolite_column(headers)
        assert idx == 0


class TestFullFileParsing:
    """Tests for full file parsing."""

    def test_parse_mwtab_with_ms_data(self):
        """Test parsing mwTab file with MS_METABOLITE_DATA."""
        content = """#METABOLOMICS WORKBENCH test STUDY_ID:ST000001 ANALYSIS_ID:AN000001
VERSION	1
#SUBJECT_SAMPLE_FACTORS:	SUBJECT	SAMPLE	FACTORS
SUBJECT_SAMPLE_FACTORS	-	Sample1	Group:Control
SUBJECT_SAMPLE_FACTORS	-	Sample2	Group:Treatment
MS_METABOLITE_DATA:UNITS	Peak area
MS_METABOLITE_DATA_START
Metabolite_name	Sample1	Sample2
Glucose	100.5	200.3
Lactate	50.2	NA
MS_METABOLITE_DATA_END
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(content)
            temp_path = Path(f.name)

        try:
            parser = MwTabParser(temp_path)
            result = parser.parse()

            # Check metadata
            assert result.metadata.study_id == "ST000001"
            assert result.metadata.analysis_id == "AN000001"
            assert result.metadata.units == "Peak area"

            # Check samples
            assert len(result.samples) == 2
            assert result.samples[0].sample_label == "Sample1"
            assert result.samples[0].factors["Group"] == "Control"

            # Check metabolites
            assert len(result.metabolites) == 2
            assert result.metabolites[0].metabolite_name == "Glucose"
            assert result.metabolites[0].values["Sample1"] == 100.5
            assert result.metabolites[0].values["Sample2"] == 200.3
            assert result.metabolites[1].metabolite_name == "Lactate"
            assert result.metabolites[1].values["Sample2"] is None  # NA

            # Check sample columns
            assert "Sample1" in result.sample_columns
            assert "Sample2" in result.sample_columns

        finally:
            temp_path.unlink()

    def test_parse_mwtab_without_ms_data(self):
        """Test parsing mwTab file without MS_METABOLITE_DATA (NMR study)."""
        content = """#METABOLOMICS WORKBENCH test STUDY_ID:ST000002 ANALYSIS_ID:AN000002
VERSION	1
#SUBJECT_SAMPLE_FACTORS:	SUBJECT	SAMPLE	FACTORS
SUBJECT_SAMPLE_FACTORS	-	Sample1	Group:Control
#NMR
NM:INSTRUMENT_NAME	Bruker
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(content)
            temp_path = Path(f.name)

        try:
            parser = MwTabParser(temp_path)
            result = parser.parse()

            assert result.metadata.study_id == "ST000002"
            assert len(result.samples) == 1
            assert len(result.metabolites) == 0  # No MS data

        finally:
            temp_path.unlink()

    def test_parse_mwtab_inline_metadata(self):
        """Test parsing with inline STUDY_ID and ANALYSIS_ID."""
        content = """#METABOLOMICS WORKBENCH file.txt STUDY_ID:ST000315 ANALYSIS_ID:AN000501
VERSION	1
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(content)
            temp_path = Path(f.name)

        try:
            parser = MwTabParser(temp_path)
            result = parser.parse()

            assert result.metadata.study_id == "ST000315"
            assert result.metadata.analysis_id == "AN000501"

        finally:
            temp_path.unlink()


class TestIsMwtabFile:
    """Tests for mwTab file detection."""

    def test_is_mwtab_file_true(self):
        """Test detecting valid mwTab file."""
        content = """#METABOLOMICS WORKBENCH test
VERSION	1
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(content)
            temp_path = Path(f.name)

        try:
            assert is_mwtab_file(temp_path) is True
        finally:
            temp_path.unlink()

    def test_is_mwtab_file_false(self):
        """Test detecting non-mwTab file."""
        content = """This is just a regular text file.
Nothing special here.
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(content)
            temp_path = Path(f.name)

        try:
            assert is_mwtab_file(temp_path) is False
        finally:
            temp_path.unlink()

    def test_is_mwtab_file_nonexistent(self):
        """Test with non-existent file."""
        assert is_mwtab_file(Path("/nonexistent/file.txt")) is False
