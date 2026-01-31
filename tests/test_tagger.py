"""Tests for tagger heuristic functions."""

import pytest

from metaloader.utils.tagger import (
    infer_device,
    infer_sample_type,
    infer_exposure,
    infer_platform,
    infer_all_tags,
    TagResult,
    FileTags,
)


class TestInferDevice:
    """Tests for infer_device function."""

    def test_nmr_detected_type(self):
        """NMR detected types should return NMR."""
        assert infer_device(None, "file.xlsx", "nmr_binned_xlsx") == "NMR"
        assert infer_device(None, "file.xlsm", "nmr_binned_xlsm") == "NMR"
        assert infer_device("/data/nmr/", "file.txt", "mwtab_nmr_binned") == "NMR"

    def test_ms_detected_type_default_lcms(self):
        """MS detected types without GC indicators should return LCMS."""
        assert infer_device(None, "metabolites.txt", "mwtab") == "LCMS"
        assert infer_device(None, "data.txt", "mwtab_ms") == "LCMS"
        assert infer_device("/data/analysis/", "results.html", "metabo_table_html") == "LCMS"

    def test_ms_with_gc_patterns_returns_gcms(self):
        """MS detected types with GC indicators should return GCMS."""
        assert infer_device("/data/GCMS/", "file.txt", "mwtab") == "GCMS"
        assert infer_device("/data/GC-MS/", "file.txt", "mwtab_ms") == "GCMS"
        # GC patterns need word boundary - _GC_ works due to explicit pattern
        assert infer_device(None, "study_GC_results.txt", "mwtab") == "GCMS"
        assert infer_device("/GC/data/", "file.txt", "mwtab") == "GCMS"
        # GCMS as word with boundary
        assert infer_device(None, "GCMS results.txt", "mwtab_ms") == "GCMS"

    def test_path_patterns_fallback_nmr(self):
        """NMR patterns in path should return NMR when detected_type is unknown."""
        assert infer_device("/data/NMR/", "file.xlsx", "unknown") == "NMR"
        # NMR as word boundary works with space or slash
        assert infer_device(None, "1H-NMR data.xlsx", "excel") == "NMR"
        assert infer_device("/NMR/analysis/", "file.csv", "csv") == "NMR"

    def test_path_patterns_fallback_gcms(self):
        """GC patterns in path should return GCMS when detected_type is unknown."""
        assert infer_device("/data/GC-MS/", "file.csv", "csv") == "GCMS"
        # GCMS needs word boundary
        assert infer_device(None, "GCMS results.csv", "csv") == "GCMS"
        assert infer_device("/GC/data/", "results.xlsx", "excel") == "GCMS"

    def test_path_patterns_fallback_lcms(self):
        """LC-MS patterns in path should return LCMS when detected_type is unknown."""
        assert infer_device("/data/LCMS/", "file.csv", "csv") == "LCMS"
        # LC-MS needs word boundary - use space separator
        assert infer_device(None, "LC-MS results.csv", "csv") == "LCMS"
        assert infer_device("/HPLC/data/", "results.xlsx", "excel") == "LCMS"
        assert infer_device("/UPLC/", "metabolites.csv", "csv") == "LCMS"

    def test_unknown_returns_none(self):
        """Unknown file types should return None."""
        assert infer_device(None, "data.csv", "csv") is None
        assert infer_device("/data/analysis/", "results.xlsx", "excel") is None
        assert infer_device(None, None, None) is None

    def test_case_insensitive(self):
        """Pattern matching should be case insensitive."""
        assert infer_device("/DATA/NMR/", "FILE.xlsx", "unknown") == "NMR"
        assert infer_device("/data/gcms/", "file.txt", "mwtab") == "GCMS"


class TestInferSampleType:
    """Tests for infer_sample_type function."""

    def test_serum(self):
        """Serum patterns should be detected."""
        assert infer_sample_type("/study/serum/", "file.txt") == "Serum"
        # Word boundary requires space or path separator
        assert infer_sample_type(None, "serum metabolites.csv") == "Serum"
        assert infer_sample_type("/plasma/", "data.txt") == "Serum"
        assert infer_sample_type("/blood/samples/", "data.xlsx") == "Serum"

    def test_urine(self):
        """Urine patterns should be detected."""
        assert infer_sample_type("/study/urine/", "file.txt") == "Urine"
        assert infer_sample_type(None, "urine metabolites.csv") == "Urine"
        assert infer_sample_type("/urinary/", "data.txt") == "Urine"

    def test_feces(self):
        """Feces patterns should be detected."""
        assert infer_sample_type("/study/feces/", "file.txt") == "Feces"
        assert infer_sample_type(None, "faeces metabolites.csv") == "Feces"
        assert infer_sample_type("/stool/", "data.txt") == "Feces"
        assert infer_sample_type("/fecal/samples/", "data.xlsx") == "Feces"
        assert infer_sample_type("/faecal/", "data.txt") == "Feces"

    def test_csf(self):
        """CSF patterns should be detected."""
        assert infer_sample_type("/study/csf/", "file.txt") == "CSF"
        assert infer_sample_type(None, "csf metabolites.csv") == "CSF"
        assert infer_sample_type("/cerebrospinal/", "data.txt") == "CSF"

    def test_unknown(self):
        """Unknown sample types should return None."""
        assert infer_sample_type("/study/tissue/", "file.txt") is None
        assert infer_sample_type(None, "metabolites.csv") is None
        assert infer_sample_type(None, None) is None

    def test_case_insensitive(self):
        """Pattern matching should be case insensitive."""
        assert infer_sample_type("/STUDY/SERUM/", "FILE.txt") == "Serum"
        assert infer_sample_type("/urine/", "DATA.csv") == "Urine"


class TestInferExposure:
    """Tests for infer_exposure function."""

    def test_ob_patterns(self):
        """OB/obese patterns should be detected."""
        result = infer_exposure("/study/OB/", "file.txt")
        assert result.value == "OB"
        assert result.warning is None

        result = infer_exposure(None, "obese subjects.csv")
        assert result.value == "OB"

        result = infer_exposure("/obesity/", "data.txt")
        assert result.value == "OB"

        result = infer_exposure("/overweight/samples/", "data.xlsx")
        assert result.value == "OB"

    def test_con_patterns(self):
        """CON/control patterns should be detected."""
        result = infer_exposure("/study/CON/", "file.txt")
        assert result.value == "CON"
        assert result.warning is None

        result = infer_exposure(None, "control subjects.csv")
        assert result.value == "CON"

        result = infer_exposure("/lean/", "data.txt")
        assert result.value == "CON"

        result = infer_exposure("/healthy/samples/", "data.xlsx")
        assert result.value == "CON"

    def test_conflict_returns_none_with_warning(self):
        """Both OB and CON patterns should return None with warning."""
        # Both patterns need proper word boundaries
        result = infer_exposure("/OB/vs/CON/", "file.txt")
        assert result.value is None
        assert result.warning is not None
        assert "Conflicting" in result.warning

        result = infer_exposure("/obese/", "control comparison.csv")
        assert result.value is None
        assert result.warning is not None

    def test_unknown(self):
        """Unknown exposure should return None without warning."""
        result = infer_exposure("/study/data/", "file.txt")
        assert result.value is None
        assert result.warning is None

        result = infer_exposure(None, None)
        assert result.value is None
        assert result.warning is None

    def test_case_insensitive(self):
        """Pattern matching should be case insensitive."""
        result = infer_exposure("/STUDY/ob/", "FILE.txt")
        assert result.value == "OB"

        result = infer_exposure("/control/", "DATA.csv")
        assert result.value == "CON"


class TestInferPlatform:
    """Tests for infer_platform function."""

    def test_ionization_mode(self):
        """Ionization mode patterns should be detected."""
        # ESI_pos as path segment or with word boundary
        assert infer_platform("/ESI_pos/", "data.csv") == "ESI_pos"
        assert infer_platform("/ESI_neg/", "results.csv") == "ESI_neg"
        assert infer_platform("/positive_mode/", "data.csv") == "ESI_pos"
        assert infer_platform("/negative_mode/", "data.csv") == "ESI_neg"

    def test_chromatography(self):
        """Chromatography patterns should be detected."""
        assert infer_platform("/HILIC/", "metabolites.csv") == "HILIC"
        assert infer_platform("/RP/column/", "data.csv") == "RP"
        assert infer_platform("/C18/", "column.csv") == "C18"

    def test_mass_analyzer(self):
        """Mass analyzer patterns should be detected."""
        assert infer_platform("/QQQ/", "results.csv") == "QQQ"
        assert infer_platform("/triple_quad/", "data.csv") == "QQQ"
        assert infer_platform("/QTOF/", "metabolites.csv") == "QTOF"
        assert infer_platform("/Orbitrap/", "data.csv") == "Orbitrap"
        assert infer_platform("/TOF/", "results.csv") == "TOF"

    def test_lc_methods(self):
        """LC method patterns should be detected."""
        assert infer_platform("/UPLC/", "data.csv") == "UPLC"
        assert infer_platform("/UHPLC/", "results.csv") == "UHPLC"
        assert infer_platform("/HPLC/", "data.csv") == "HPLC"

    def test_multiple_platforms_combined(self):
        """Multiple platform indicators should be combined."""
        result = infer_platform("/ESI_pos/HILIC/QQQ/", "data.csv")
        assert result is not None
        assert "ESI_pos" in result
        assert "HILIC" in result
        assert "QQQ" in result

    def test_unknown(self):
        """Unknown platform should return None."""
        assert infer_platform(None, "data.csv") is None
        assert infer_platform("/study/", "metabolites.txt") is None
        assert infer_platform(None, None) is None

    def test_case_insensitive(self):
        """Pattern matching should be case insensitive."""
        assert infer_platform("/esi_pos/", "data.csv") == "ESI_pos"
        assert infer_platform("/hilic/", "DATA.CSV") == "HILIC"


class TestInferAllTags:
    """Tests for infer_all_tags function."""

    def test_all_tags_detected(self):
        """All tags should be detected when patterns present."""
        tags = infer_all_tags(
            path_rel="/OB/serum/NMR/ESI_pos/",
            filename="data.txt",
            detected_type="nmr_binned_xlsx"
        )

        assert isinstance(tags, FileTags)
        assert tags.device == "NMR"
        assert tags.sample_type == "Serum"
        assert tags.exposure == "OB"
        assert tags.platform == "ESI_pos"
        assert isinstance(tags.warnings, list)

    def test_partial_tags(self):
        """Only some tags detected when partial patterns present."""
        tags = infer_all_tags(
            path_rel="/data/serum/",
            filename="metabolites.txt",
            detected_type="mwtab"
        )

        assert tags.device == "LCMS"
        assert tags.sample_type == "Serum"
        assert tags.exposure is None
        assert tags.platform is None

    def test_no_tags(self):
        """No tags detected when no patterns present."""
        tags = infer_all_tags(
            path_rel="/unknown/",
            filename="data.txt",
            detected_type="unknown"
        )

        assert tags.device is None
        assert tags.sample_type is None
        assert tags.exposure is None
        assert tags.platform is None

    def test_warnings_collected(self):
        """Warnings should be collected in tags.warnings."""
        tags = infer_all_tags(
            path_rel="/OB/vs/CON/",
            filename="data.txt",
            detected_type="mwtab"
        )

        assert tags.exposure is None
        assert len(tags.warnings) == 1
        assert "Conflicting" in tags.warnings[0]

    def test_null_inputs(self):
        """Should handle None inputs gracefully."""
        tags = infer_all_tags(
            path_rel=None,
            filename=None,
            detected_type=None
        )

        assert tags.device is None
        assert tags.sample_type is None
        assert tags.exposure is None
        assert tags.platform is None
        assert tags.warnings == []


class TestTagResult:
    """Tests for TagResult dataclass."""

    def test_tag_result_with_value(self):
        """TagResult should store value correctly."""
        result = TagResult(value="OB")
        assert result.value == "OB"
        assert result.warning is None

    def test_tag_result_with_warning(self):
        """TagResult should store warning correctly."""
        result = TagResult(value=None, warning="Test warning")
        assert result.value is None
        assert result.warning == "Test warning"
