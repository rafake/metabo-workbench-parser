"""Tests for SHA256 hashing utility."""

import tempfile
from pathlib import Path

import pytest

from metaloader.utils.hashing import calculate_sha256


def test_calculate_sha256_small_file():
    """Test SHA256 calculation for small file."""
    content = b"Hello, World!"
    expected_hash = "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"
    
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(content)
        temp_path = Path(f.name)
    
    try:
        result = calculate_sha256(temp_path)
        assert result == expected_hash
    finally:
        temp_path.unlink()


def test_calculate_sha256_large_file():
    """Test SHA256 calculation for larger file (tests streaming)."""
    # Create 1MB of data
    content = b"A" * (1024 * 1024)
    
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(content)
        temp_path = Path(f.name)
    
    try:
        result = calculate_sha256(temp_path)
        # This is the known SHA256 of 1MB of 'A' characters
        expected_hash = "ad782ecdac770fc6eb9bc551d6d4e703ccbb6a47d5a5456b6cd552f9f2896d78"
        assert result == expected_hash
    finally:
        temp_path.unlink()


def test_calculate_sha256_empty_file():
    """Test SHA256 calculation for empty file."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        temp_path = Path(f.name)
    
    try:
        result = calculate_sha256(temp_path)
        # SHA256 of empty string
        expected_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        assert result == expected_hash
    finally:
        temp_path.unlink()


def test_calculate_sha256_file_not_found():
    """Test that FileNotFoundError is raised for non-existent file."""
    fake_path = Path("/tmp/nonexistent_file_xyz123.txt")
    
    with pytest.raises(FileNotFoundError):
        calculate_sha256(fake_path)


def test_calculate_sha256_directory():
    """Test that ValueError is raised when path is a directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dir_path = Path(temp_dir)
        
        with pytest.raises(ValueError, match="not a file"):
            calculate_sha256(dir_path)


def test_calculate_sha256_consistency():
    """Test that multiple calculations give same result."""
    content = b"Test content for consistency"
    
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(content)
        temp_path = Path(f.name)
    
    try:
        hash1 = calculate_sha256(temp_path)
        hash2 = calculate_sha256(temp_path)
        assert hash1 == hash2
    finally:
        temp_path.unlink()
