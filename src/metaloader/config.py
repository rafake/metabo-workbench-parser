"""Configuration module for database connection and environment variables."""

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()


class Config:
    """Application configuration."""

    def __init__(self):
        self.database_url: str = os.getenv("DATABASE_URL", "")
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")

        if not self.database_url:
            raise ValueError(
                "DATABASE_URL environment variable is required. "
                "Please set it in your environment or create a .env file."
            )

    @property
    def db_url(self) -> str:
        """Get database URL."""
        return self.database_url


# Global config instance
config = Config()
