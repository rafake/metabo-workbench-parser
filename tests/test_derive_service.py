"""Tests for derive service category derivation logic."""

import pytest

from metaloader.services.derive_service import (
    derive_device,
    derive_exposure,
    derive_matrix,
)


class TestDeriveDevice:
    """Tests for device derivation logic."""

    def test_derive_device_nmr_exact(self):
        """Test NMR detection with exact match."""
        assert derive_device("NMR") == "NMR"
        assert derive_device("1H-NMR") == "NMR"
        assert derive_device("13C-NMR") == "NMR"

    def test_derive_device_nmr_description(self):
        """Test NMR detection from description."""
        assert derive_device("Nuclear Magnetic Resonance") == "NMR"
        assert derive_device("This is an NMR study") == "NMR"

    def test_derive_device_lcms_exact(self):
        """Test LC-MS detection with exact match."""
        assert derive_device("LC-MS") == "LCMS"
        assert derive_device("LCMS") == "LCMS"
        assert derive_device("LC_MS") == "LCMS"

    def test_derive_device_lcms_description(self):
        """Test LC-MS detection from description."""
        assert derive_device("Liquid Chromatography") == "LCMS"
        assert derive_device("HPLC-MS analysis") == "LCMS"
        assert derive_device("UHPLC method") == "LCMS"
        assert derive_device("UPLC based") == "LCMS"

    def test_derive_device_gcms_exact(self):
        """Test GC-MS detection with exact match."""
        assert derive_device("GC-MS") == "GCMS"
        assert derive_device("GCMS") == "GCMS"
        assert derive_device("GC_MS") == "GCMS"

    def test_derive_device_gcms_description(self):
        """Test GC-MS detection from description."""
        assert derive_device("Gas Chromatography") == "GCMS"
        assert derive_device("GC mass spectrometry") == "GCMS"

    def test_derive_device_ms_generic(self):
        """Test generic MS detection."""
        assert derive_device("MS metabolomics") == "MS"
        assert derive_device("mass spectrometry") == "MS"

    def test_derive_device_unknown(self):
        """Test unknown device."""
        assert derive_device("random text") is None
        assert derive_device("") is None

    def test_derive_device_case_insensitive(self):
        """Test case insensitivity."""
        assert derive_device("nmr") == "NMR"
        assert derive_device("lc-ms") == "LCMS"
        assert derive_device("gc-ms") == "GCMS"

    def test_derive_device_priority(self):
        """Test that NMR takes priority over others."""
        # NMR should be detected first in mixed content
        assert derive_device("NMR and LC-MS combined") == "NMR"


class TestDeriveExposure:
    """Tests for exposure derivation logic."""

    def test_derive_exposure_obese_exact(self):
        """Test OB detection with exact matches."""
        assert derive_exposure("OB") == "OB"
        assert derive_exposure("Obese") == "OB"
        assert derive_exposure("Obesity") == "OB"

    def test_derive_exposure_obese_variations(self):
        """Test OB detection with variations."""
        assert derive_exposure("case") == "OB"
        assert derive_exposure("overweight") == "OB"

    def test_derive_exposure_control_exact(self):
        """Test CON detection with exact matches."""
        assert derive_exposure("CON") == "CON"
        assert derive_exposure("Control") == "CON"
        assert derive_exposure("Lean") == "CON"

    def test_derive_exposure_control_variations(self):
        """Test CON detection with variations."""
        assert derive_exposure("Normal") == "CON"
        assert derive_exposure("Healthy") == "CON"

    def test_derive_exposure_unknown(self):
        """Test unknown exposure."""
        assert derive_exposure("random") is None
        assert derive_exposure("") is None
        assert derive_exposure("Treatment A") is None

    def test_derive_exposure_case_insensitive(self):
        """Test case insensitivity."""
        assert derive_exposure("OBESE") == "OB"
        assert derive_exposure("CONTROL") == "CON"
        assert derive_exposure("lean") == "CON"

    def test_derive_exposure_partial_match(self):
        """Test partial matches in longer strings."""
        assert derive_exposure("Obese group") == "OB"
        assert derive_exposure("Control group") == "CON"
        assert derive_exposure("healthy volunteer") == "CON"


class TestDeriveMatrix:
    """Tests for sample matrix derivation logic."""

    def test_derive_matrix_serum(self):
        """Test Serum detection."""
        assert derive_matrix("Serum") == "Serum"
        assert derive_matrix("Blood serum") == "Serum"
        assert derive_matrix("plasma") == "Serum"  # Plasma maps to Serum

    def test_derive_matrix_urine(self):
        """Test Urine detection."""
        assert derive_matrix("Urine") == "Urine"
        assert derive_matrix("urinary sample") == "Urine"

    def test_derive_matrix_feces(self):
        """Test Feces detection."""
        assert derive_matrix("Feces") == "Feces"
        assert derive_matrix("Faeces") == "Feces"
        assert derive_matrix("Stool") == "Feces"
        assert derive_matrix("fecal sample") == "Feces"

    def test_derive_matrix_csf(self):
        """Test CSF detection."""
        assert derive_matrix("CSF") == "CSF"
        assert derive_matrix("Cerebrospinal fluid") == "CSF"
        assert derive_matrix("cerebrospinal") == "CSF"

    def test_derive_matrix_unknown(self):
        """Test unknown matrix."""
        assert derive_matrix("random") is None
        assert derive_matrix("") is None
        assert derive_matrix("tissue") is None  # Not in our mapping

    def test_derive_matrix_case_insensitive(self):
        """Test case insensitivity."""
        assert derive_matrix("SERUM") == "Serum"
        assert derive_matrix("URINE") == "Urine"
        assert derive_matrix("feces") == "Feces"
        assert derive_matrix("csf") == "CSF"

    def test_derive_matrix_partial_match(self):
        """Test partial matches in longer strings."""
        assert derive_matrix("Human serum sample") == "Serum"
        assert derive_matrix("Morning urine collection") == "Urine"


class TestDerivationEdgeCases:
    """Tests for edge cases in derivation logic."""

    def test_whitespace_handling(self):
        """Test handling of whitespace."""
        assert derive_device("  NMR  ") == "NMR"
        assert derive_exposure("  Obese  ") == "OB"
        assert derive_matrix("  Serum  ") == "Serum"

    def test_unicode_handling(self):
        """Test handling of unicode characters."""
        # Should handle gracefully even with unicode
        assert derive_device("NMR β-spectrum") == "NMR"

    def test_special_characters(self):
        """Test handling of special characters."""
        assert derive_device("LC-MS/MS") == "LCMS"
        assert derive_device("GC/MS") == "GCMS"

    def test_numbers_in_strings(self):
        """Test strings containing numbers."""
        assert derive_device("1H-NMR at 600MHz") == "NMR"
        assert derive_exposure("Group1_Obese") == "OB"


class TestCombinedScenarios:
    """Integration tests for realistic scenarios."""

    def test_metabolomics_workbench_typical_values(self):
        """Test with typical Metabolomics Workbench values."""
        # Typical study descriptions
        assert derive_device("Reverse Phase LC-MS positive ion mode") == "LCMS"
        assert derive_device("GC-TOF MS") == "GCMS"
        assert derive_device("1H NMR spectroscopy") == "NMR"

        # Typical factor values
        assert derive_exposure("Case") == "OB"
        assert derive_exposure("Control") == "CON"
        assert derive_exposure("Lean") == "CON"

        # Typical matrix values
        assert derive_matrix("Human serum") == "Serum"
        assert derive_matrix("24h urine") == "Urine"
        assert derive_matrix("Fecal sample") == "Feces"
