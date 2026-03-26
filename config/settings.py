"""Configuration and environment settings for the strategy engine."""

import os
from typing import Optional
from pathlib import Path

import dotenv


# Load .env file from project root if it exists
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_ENV_FILE = _PROJECT_ROOT / ".env"
if _ENV_FILE.exists():
    dotenv.load_dotenv(_ENV_FILE)


class DatabaseConfig:
    """Database connection configuration."""

    @staticmethod
    def get_connection_string() -> str:
        """
        Build a PostgreSQL connection string from environment variables.
        
        Priority:
        1. DATABASE_URL env var (if set, use as-is)
        2. Individual components (DB_USER, DB_PASSWORD, DB_HOST, etc.)
        
        Raises:
            ValueError: If required credentials are missing.
        """
        # Check if full connection string is provided
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            return database_url
        
        # Build from individual components
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "strategy_engine")
        
        if not user or not password:
            raise ValueError(
                "Missing database credentials. Set DB_USER and DB_PASSWORD "
                "environment variables, or provide DATABASE_URL."
            )
        
        return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db_name}"

    @staticmethod
    def get_schema() -> str:
        """Get the database schema name."""
        return os.getenv("DB_SCHEMA", "strategy_engine")


if __name__ == "__main__":
    # Quick test: print connection string (with password masked)
    try:
        conn_str = DatabaseConfig.get_connection_string()
        masked = conn_str.replace(
            os.getenv("DB_PASSWORD", "???"), 
            "***"
        ) if os.getenv("DB_PASSWORD") else conn_str
        print(f"✓ Connection string loaded (masked): {masked}")
        print(f"✓ Schema: {DatabaseConfig.get_schema()}")
    except ValueError as e:
        print(f"✗ Error: {e}")

