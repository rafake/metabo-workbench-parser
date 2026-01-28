"""Streaming SHA256 hash calculation for files."""

import hashlib
from pathlib import Path


def calculate_sha256(file_path: Path, chunk_size: int = 8192) -> str:
    """Calculate SHA256 hash of a file using streaming to avoid loading entire file into memory.
    
    Args:
        file_path: Path to the file
        chunk_size: Size of chunks to read (default 8KB)
        
    Returns:
        Hexadecimal SHA256 hash string
        
    Raises:
        FileNotFoundError: If file does not exist
        PermissionError: If file cannot be read
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if not file_path.is_file():
        raise ValueError(f"Path is not a file: {file_path}")
    
    sha256_hash = hashlib.sha256()
    
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            sha256_hash.update(chunk)
    
    return sha256_hash.hexdigest()
